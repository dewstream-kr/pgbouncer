/* PgBouncer RW/RO routing integration glue. SPDX-License-Identifier: ISC */
#include "bouncer.h"
#include "rwro_pgbouncer.h"

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * This file intentionally avoids adding fields to PgSocket/PgDatabase.  The
 * selected target and query-sent guard are stored in a tiny side table keyed by
 * PgSocket*.  That keeps the invasive bouncer.h patch optional while still
 * allowing objects.c/server.c/client.c to enforce safe fallback semantics.
 */

typedef struct PendingLine {
	char *name;
	char *line;
} PendingLine;

typedef struct PendingList {
	PendingLine *items;
	size_t count;
	size_t cap;
} PendingList;

typedef struct SocketMeta {
	PgSocket *server;
	PgbServerTarget *target;
	bool query_sent;
	bool role_check_running;
	bool role_check_done;
	bool role_check_saw_row;
	bool role_check_pg_is_in_recovery;
	bool role_check_transaction_read_only;
	struct SocketMeta *next;
} SocketMeta;

static PgbRoutingRegistry active_registry;
static PgbRoutingRegistry loading_registry;
static bool loading_config;
static PendingList pending_groups;
static PendingList pending_routes;
static SocketMeta *socket_meta_list;

static char *rwro_strdup(const char *s)
{
	size_t len;
	char *out;
	if (!s)
		return NULL;
	len = strlen(s) + 1;
	out = malloc(len);
	if (!out)
		return NULL;
	memcpy(out, s, len);
	return out;
}

static void pending_list_clear(PendingList *list)
{
	size_t i;
	if (!list)
		return;
	for (i = 0; i < list->count; i++) {
		free(list->items[i].name);
		free(list->items[i].line);
	}
	free(list->items);
	memset(list, 0, sizeof(*list));
}

static bool pending_list_add(PendingList *list, const char *name, const char *line)
{
	PendingLine *items;
	if (!list || !name || !line)
		return false;
	if (list->count == list->cap) {
		size_t cap = list->cap ? list->cap * 2 : 16;
		items = realloc(list->items, cap * sizeof(*items));
		if (!items)
			return false;
		list->items = items;
		list->cap = cap;
	}
	list->items[list->count].name = rwro_strdup(name);
	list->items[list->count].line = rwro_strdup(line);
	if (!list->items[list->count].name || !list->items[list->count].line) {
		free(list->items[list->count].name);
		free(list->items[list->count].line);
		return false;
	}
	list->count++;
	return true;
}

static void socket_meta_clear_all(void)
{
	SocketMeta *m = socket_meta_list;
	while (m) {
		SocketMeta *next = m->next;
		free(m);
		m = next;
	}
	socket_meta_list = NULL;
}

static SocketMeta *socket_meta_find(PgSocket *server, bool create)
{
	SocketMeta *m;
	for (m = socket_meta_list; m; m = m->next) {
		if (m->server == server)
			return m;
	}
	if (!create)
		return NULL;
	m = calloc(1, sizeof(*m));
	if (!m)
		return NULL;
	m->server = server;
	m->next = socket_meta_list;
	socket_meta_list = m;
	return m;
}

void rwro_detach_server(PgSocket *server)
{
	SocketMeta **pp = &socket_meta_list;
	while (*pp) {
		SocketMeta *m = *pp;
		if (m->server == server) {
			*pp = m->next;
			free(m);
			return;
		}
		pp = &m->next;
	}
}

void rwro_config_setup(void)
{
	pgb_routing_init(&active_registry);
	pgb_routing_init(&loading_registry);
	loading_config = false;
}

void rwro_config_cleanup(void)
{
	pending_list_clear(&pending_groups);
	pending_list_clear(&pending_routes);
	socket_meta_clear_all();
	pgb_routing_free(&loading_registry);
	pgb_routing_free(&active_registry);
	loading_config = false;
}

void rwro_config_begin(void)
{
	pending_list_clear(&pending_groups);
	pending_list_clear(&pending_routes);
	pgb_routing_free(&loading_registry);
	pgb_routing_init(&loading_registry);
	loading_config = true;
}

void rwro_config_abort(void)
{
	pending_list_clear(&pending_groups);
	pending_list_clear(&pending_routes);
	pgb_routing_free(&loading_registry);
	pgb_routing_init(&loading_registry);
	loading_config = false;
}

static PgbRoutingRegistry *load_registry(void)
{
	return loading_config ? &loading_registry : &active_registry;
}

PgbRoutingRegistry *rwro_active_registry(void)
{
	return &active_registry;
}

static bool streq_ignore_case(const char *a, const char *b)
{
	while (*a && *b) {
		if (tolower((unsigned char)*a) != tolower((unsigned char)*b))
			return false;
		a++;
		b++;
	}
	return *a == '\0' && *b == '\0';
}

static bool is_route_db_key(const char *key)
{
	static const char *keys[] = {
		"server_group", "group",
		"route", "role_check",
		"lb_policy", "policy", "rw_policy",
		"rw_fallback", "fallback",
		"skip_unavailable", "ro_skip_unavailable",
		"fallback_group", "ro_fallback_group",
		"fallback_to_primary", "ro_fallback_to_primary",
		NULL
	};
	int i;
	if (!key)
		return false;
	for (i = 0; keys[i]; i++) {
		if (streq_ignore_case(key, keys[i]))
			return true;
	}
	return false;
}

static bool append_fragment(char *dst, size_t dstlen, size_t *off,
	const char *start, size_t len)
{
	if (*off + len + 2 >= dstlen)
		return false;
	if (*off > 0)
		dst[(*off)++] = ' ';
	memcpy(dst + *off, start, len);
	*off += len;
	dst[*off] = '\0';
	return true;
}

/* Strip routing-only keys before delegating to upstream parse_database(). */
static bool filter_database_connstr(const char *connstr, char *out, size_t outlen,
	bool *has_route_key)
{
	const char *p = connstr;
	size_t off = 0;
	out[0] = '\0';
	*has_route_key = false;

	while (p && *p) {
		const char *frag_start;
		const char *key_start;
		const char *key_end;
		const char *frag_end;
		char keybuf[128];
		size_t keylen;

		while (*p && isspace((unsigned char)*p))
			p++;
		if (!*p)
			break;

		frag_start = p;
		key_start = p;
		while (*p && *p != '=' && !isspace((unsigned char)*p))
			p++;
		key_end = p;
		while (*p && isspace((unsigned char)*p))
			p++;
		if (*p != '=') {
			/* Let the upstream parser report the original syntax problem. */
			return append_fragment(out, outlen, &off, frag_start, strlen(frag_start));
		}
		p++;
		while (*p && isspace((unsigned char)*p))
			p++;
		if (*p == '\'') {
			p++;
			while (*p) {
				if (*p == '\'') {
					if (p[1] == '\'') {
						p += 2;
						continue;
					}
					p++;
					break;
				}
				p++;
			}
		} else {
			while (*p && !isspace((unsigned char)*p))
				p++;
		}
		frag_end = p;

		keylen = (size_t)(key_end - key_start);
		if (keylen >= sizeof(keybuf))
			keylen = sizeof(keybuf) - 1;
		memcpy(keybuf, key_start, keylen);
		keybuf[keylen] = '\0';

		if (is_route_db_key(keybuf)) {
			*has_route_key = true;
		} else if (!append_fragment(out, outlen, &off, frag_start,
			(size_t)(frag_end - frag_start))) {
			return false;
		}
	}
	return true;
}

bool rwro_parse_database(void *base, const char *name, const char *connstr)
{
	char filtered[4096];
	bool has_route_key = false;

	if (!filter_database_connstr(connstr, filtered, sizeof(filtered), &has_route_key)) {
		log_error("[databases] %s: cannot filter rw/ro routing options", name);
		return false;
	}

	if (!parse_database(base, name, filtered))
		return false;

	if (has_route_key && !pending_list_add(&pending_routes, name, connstr)) {
		log_error("[databases] %s: out of memory while saving rw/ro route", name);
		return false;
	}
	return true;
}

bool rwro_parse_server(void *base, const char *name, const char *connstr)
{
	char err[PGB_ROUTING_ERR_MAX] = "";
	(void)base;
	if (!pgb_parse_target_line(load_registry(), name, connstr, err, sizeof(err))) {
		log_error("[servers] %s: %s", name, err);
		return false;
	}
	return true;
}

bool rwro_parse_server_group(void *base, const char *name, const char *connstr)
{
	(void)base;
	if (!pending_list_add(&pending_groups, name, connstr)) {
		log_error("[server_groups] %s: out of memory while saving group", name);
		return false;
	}
	return true;
}

bool rwro_config_finish(void)
{
	size_t i;
	char err[PGB_ROUTING_ERR_MAX];

	if (!loading_config)
		return true;

	for (i = 0; i < pending_groups.count; i++) {
		err[0] = '\0';
		if (!pgb_parse_group_line(&loading_registry, pending_groups.items[i].name,
			pending_groups.items[i].line, err, sizeof(err))) {
			log_error("[server_groups] %s: %s", pending_groups.items[i].name, err);
			rwro_config_abort();
			return false;
		}
	}

	for (i = 0; i < pending_routes.count; i++) {
		err[0] = '\0';
		if (!pgb_parse_database_route_line(&loading_registry, pending_routes.items[i].name,
			pending_routes.items[i].line, err, sizeof(err))) {
			log_error("[databases] %s: %s", pending_routes.items[i].name, err);
			rwro_config_abort();
			return false;
		}
	}

	pgb_routing_free(&active_registry);
	active_registry = loading_registry;
	memset(&loading_registry, 0, sizeof(loading_registry));
	pgb_routing_init(&loading_registry);
	pending_list_clear(&pending_groups);
	pending_list_clear(&pending_routes);
	loading_config = false;

	if (active_registry.route_count > 0) {
		log_info("rw/ro routing loaded: %zu targets, %zu groups, %zu database routes",
			active_registry.target_count, active_registry.group_count,
			active_registry.route_count);
	}
	return true;
}

PgbDatabaseRoute *rwro_route_for_database_name(const char *database_name)
{
	if (!database_name)
		return NULL;
	return pgb_find_database_route(&active_registry, database_name);
}

PgbDatabaseRoute *rwro_route_for_database(PgDatabase *db)
{
	if (!db)
		return NULL;
	return rwro_route_for_database_name(db->name);
}

bool rwro_database_has_route(PgDatabase *db)
{
	return rwro_route_for_database(db) != NULL;
}

bool rwro_select_target_for_database(PgDatabase *db, PgbServerTarget **target_out,
	char *err, size_t errlen)
{
	PgbDatabaseRoute *route;
	PgbServerTarget *target;

	if (target_out)
		*target_out = NULL;
	if (err && errlen)
		err[0] = '\0';
	if (!db || !target_out)
		return false;

	route = rwro_route_for_database(db);
	if (!route)
		return true; /* no RW/RO route: use upstream db->host/db->port path */

	target = pgb_select_target(route->primary_group, route->route_role, route->skip_unavailable);
	if (!target) {
		if (err && errlen)
			snprintf(err, errlen, "no usable rw/ro target for database=%s group=%s route=%s",
				db->name,
				route->primary_group ? route->primary_group->name : "<none>",
				pgb_route_role_to_string(route->route_role));
		route->counters.fallback_rejected_count++;
		return false;
	}
	pgb_record_target_selected(route->primary_group, target);
	route->counters.selected_count++;
	*target_out = target;
	return true;
}

void rwro_attach_server_target(PgSocket *server, PgbServerTarget *target)
{
	SocketMeta *m;
	if (!server || !target)
		return;
	m = socket_meta_find(server, true);
	if (!m)
		return;
	m->target = target;
	m->query_sent = false;
	m->role_check_running = false;
	m->role_check_done = false;
	m->role_check_saw_row = false;
}

const char *rwro_server_target_host(PgSocket *server)
{
	SocketMeta *m = socket_meta_find(server, false);
	return (m && m->target) ? m->target->host : NULL;
}

int rwro_server_target_port(PgSocket *server, int fallback_port)
{
	SocketMeta *m = socket_meta_find(server, false);
	return (m && m->target && m->target->port > 0) ? m->target->port : fallback_port;
}

bool rwro_pool_can_retry_now(PgPool *pool)
{
	PgbDatabaseRoute *route;
	if (!pool || !pool->db)
		return false;
	route = rwro_route_for_database(pool->db);
	return route && route->rw_fallback == PGB_FALLBACK_CONNECT_ONLY;
}

void rwro_record_connect_attempt(PgSocket *server)
{
	SocketMeta *m = socket_meta_find(server, false);
	if (m && m->target)
		pgb_record_connect_attempt(m->target);
}

void rwro_record_connect_success(PgSocket *server)
{
	SocketMeta *m = socket_meta_find(server, false);
	if (m && m->target)
		pgb_record_connect_success(m->target);
}

void rwro_record_connect_failure(PgSocket *server, const char *reason)
{
	SocketMeta *m = socket_meta_find(server, false);
	if (m && m->target)
		pgb_record_connect_failure(m->target, reason);
}

void rwro_record_login_failure(PgSocket *server, const char *reason)
{
	SocketMeta *m = socket_meta_find(server, false);
	if (m && m->target)
		pgb_record_login_failure(m->target, reason);
}

static PgbServerTarget *select_fallback_target(PgbDatabaseRoute *route, PgbServerTarget *failed)
{
	PgbServerGroup *group;
	PgbServerTarget **candidates;
	size_t i, n;

	if (!route || !route->primary_group)
		return NULL;
	group = route->primary_group;
	if (route->fallback_group)
		group = route->fallback_group;

	candidates = calloc(group->target_count ? group->target_count : 1, sizeof(*candidates));
	if (!candidates)
		return NULL;
	n = pgb_build_candidate_order(group, route->route_role, true, candidates, group->target_count);
	for (i = 0; i < n; i++) {
		if (candidates[i] != failed) {
			PgbServerTarget *target = candidates[i];
			free(candidates);
			return target;
		}
	}
	free(candidates);
	return NULL;
}

bool rwro_try_prepare_fallback(PgSocket *server, PgbFailureKind failure,
	bool transaction_in_progress, const char *reason, PgbServerTarget **next_target_out)
{
	SocketMeta *m = socket_meta_find(server, false);
	PgbDatabaseRoute *route;
	PgbServerTarget *next;

	if (next_target_out)
		*next_target_out = NULL;
	if (!server || !server->pool || !server->pool->db || !m || !m->target)
		return false;

	route = rwro_route_for_database(server->pool->db);
	if (!route)
		return false;

	if (!pgb_should_fallback(failure, m->query_sent, transaction_in_progress, route->rw_fallback)) {
		pgb_record_fallback(m->target, NULL, false, reason);
		route->counters.fallback_rejected_count++;
		return false;
	}

	switch (failure) {
	case PGB_FAIL_CONNECT:
		pgb_record_connect_failure(m->target, reason);
		pgb_set_target_state(m->target, PGB_TARGET_DOWN, reason);
		break;
	case PGB_FAIL_LOGIN:
		pgb_record_login_failure(m->target, reason);
		pgb_set_target_state(m->target, PGB_TARGET_DOWN, reason);
		break;
	case PGB_FAIL_ROLE_CHECK:
		pgb_set_target_state(m->target, PGB_TARGET_PROBING, reason);
		break;
	default:
		break;
	}

	next = select_fallback_target(route, m->target);
	if (!next) {
		pgb_record_fallback(m->target, NULL, false, "no fallback target");
		route->counters.fallback_rejected_count++;
		return false;
	}

	pgb_record_fallback(m->target, next, true, reason);
	route->counters.fallback_from_count++;
	if (next_target_out)
		*next_target_out = next;
	return true;
}

void rwro_mark_query_sent(PgSocket *server)
{
	SocketMeta *m = socket_meta_find(server, false);
	if (m)
		m->query_sent = true;
}

bool rwro_query_sent(PgSocket *server)
{
	SocketMeta *m = socket_meta_find(server, false);
	return m ? m->query_sent : false;
}

bool rwro_role_check_needed(PgSocket *server)
{
	PgbDatabaseRoute *route;
	SocketMeta *m;
	if (!server || !server->pool || !server->pool->db)
		return false;
	if (server->pool->db->admin || server->replication)
		return false;
	m = socket_meta_find(server, false);
	if (!m || !m->target || m->role_check_done)
		return false;
	route = rwro_route_for_database(server->pool->db);
	return route && route->role_check_enabled;
}

const char *rwro_role_check_sql(PgSocket *server)
{
	PgbDatabaseRoute *route = server && server->pool ? rwro_route_for_database(server->pool->db) : NULL;
	return route ? pgb_role_check_sql(route->role_check_role) : NULL;
}

void rwro_role_check_begin(PgSocket *server)
{
	SocketMeta *m = socket_meta_find(server, true);
	if (!m)
		return;
	m->role_check_running = true;
	m->role_check_done = false;
	m->role_check_saw_row = false;
	m->role_check_pg_is_in_recovery = false;
	m->role_check_transaction_read_only = false;
}

bool rwro_role_check_running(PgSocket *server)
{
	SocketMeta *m = socket_meta_find(server, false);
	return m && m->role_check_running;
}

static bool bool_text_value(const char *value, uint32_t len)
{
	if (len == 0 || !value)
		return false;
	return value[0] == 't' || value[0] == 'T' || value[0] == '1' || value[0] == 'o' || value[0] == 'O';
}

static bool parse_role_check_datarow(PgSocket *server, PktHdr *pkt, const char **reason_out)
{
	SocketMeta *m = socket_meta_find(server, false);
	uint16_t cols;
	uint32_t len0, len1;
	const char *v0, *v1;
	if (!m)
		return false;
	/* mbuf_get_* advances pkt->data.  The caller skips the full packet after
	 * this hook, and the DataRow is not needed elsewhere. */
	if (!mbuf_get_uint16be(&pkt->data, &cols) || cols < 2) {
		*reason_out = "malformed role-check DataRow";
		return false;
	}
	if (!mbuf_get_uint32be(&pkt->data, &len0) || len0 == (uint32_t)-1 ||
		!mbuf_get_chars(&pkt->data, len0, &v0)) {
		*reason_out = "malformed pg_is_in_recovery value";
		return false;
	}
	if (!mbuf_get_uint32be(&pkt->data, &len1) || len1 == (uint32_t)-1 ||
		!mbuf_get_chars(&pkt->data, len1, &v1)) {
		*reason_out = "malformed transaction_read_only value";
		return false;
	}
	m->role_check_pg_is_in_recovery = bool_text_value(v0, len0);
	m->role_check_transaction_read_only = bool_text_value(v1, len1);
	m->role_check_saw_row = true;
	return true;
}

bool rwro_role_check_handle_packet(PgSocket *server, PktHdr *pkt,
	bool *done_out, bool *ok_out, const char **reason_out)
{
	SocketMeta *m = socket_meta_find(server, false);
	PgbDatabaseRoute *route = server && server->pool ? rwro_route_for_database(server->pool->db) : NULL;
	PgbTargetRole observed;

	*done_out = false;
	*ok_out = false;
	*reason_out = NULL;
	if (!m || !route)
		return false;

	switch (pkt->type) {
	case PqMsg_RowDescription:
	case PqMsg_CommandComplete:
	case PqMsg_ParameterStatus:
		return true;
	case PqMsg_DataRow:
		return parse_role_check_datarow(server, pkt, reason_out);
	case PqMsg_ErrorResponse:
		*done_out = true;
		*ok_out = false;
		*reason_out = "role-check query failed";
		return true;
	case PqMsg_ReadyForQuery:
		*done_out = true;
		if (!m->role_check_saw_row) {
			*ok_out = false;
			*reason_out = "role-check query returned no row";
			return true;
		}
		observed = pgb_observed_role_from_pg_status(m->role_check_pg_is_in_recovery,
			m->role_check_transaction_read_only);
		pgb_record_role_check(m->target, observed, route->role_check_role);
		m->target->observed_role = observed;
		*ok_out = pgb_observed_role_satisfies_route(observed, route->role_check_role);
		if (!*ok_out)
			*reason_out = "server role does not satisfy route";
		return true;
	default:
		return true;
	}
}

void rwro_role_check_finish(PgSocket *server, bool ok, const char *reason)
{
	SocketMeta *m = socket_meta_find(server, false);
	if (!m)
		return;
	m->role_check_running = false;
	m->role_check_done = ok;
	if (!ok && m->target)
		pgb_set_target_state(m->target, PGB_TARGET_PROBING, reason ? reason : "role check failed");
}

size_t rwro_show_server_targets(char *dst, size_t dstlen)
{
	return pgb_format_server_targets(&active_registry, dst, dstlen);
}

size_t rwro_show_server_groups(char *dst, size_t dstlen)
{
	return pgb_format_server_groups(&active_registry, dst, dstlen);
}

size_t rwro_show_route_stats(char *dst, size_t dstlen)
{
	return pgb_format_route_stats(&active_registry, dst, dstlen);
}

bool rwro_include_target_cmd(const char *target_name, char *err, size_t errlen)
{
	PgbServerTarget *target = pgb_find_target(&active_registry, target_name);
	if (!target) {
		if (err && errlen)
			snprintf(err, errlen, "target not found: %s", target_name);
		return false;
	}
	pgb_include_target(target);
	return true;
}

bool rwro_exclude_target_cmd(const char *target_name, char *err, size_t errlen)
{
	PgbServerTarget *target = pgb_find_target(&active_registry, target_name);
	if (!target) {
		if (err && errlen)
			snprintf(err, errlen, "target not found: %s", target_name);
		return false;
	}
	pgb_exclude_target(target, "admin exclude");
	return true;
}

bool rwro_drain_target_cmd(const char *target_name, char *err, size_t errlen)
{
	PgbServerTarget *target = pgb_find_target(&active_registry, target_name);
	if (!target) {
		if (err && errlen)
			snprintf(err, errlen, "target not found: %s", target_name);
		return false;
	}
	pgb_drain_target(target, "admin drain");
	return true;
}
