/* PgBouncer RW/RO routing core. SPDX-License-Identifier: ISC */
#include "routing.h"

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void copy_text(char *dst, size_t dstlen, const char *src)
{
	if (dstlen == 0)
		return;
	snprintf(dst, dstlen, "%s", src ? src : "");
}

static bool ci_eq(const char *a, const char *b)
{
	if (!a || !b)
		return false;
	while (*a && *b) {
		if (tolower((unsigned char)*a) != tolower((unsigned char)*b))
			return false;
		a++;
		b++;
	}
	return *a == '\0' && *b == '\0';
}

static char *trim(char *s)
{
	char *e;
	if (!s)
		return s;
	while (isspace((unsigned char)*s))
		s++;
	e = s + strlen(s);
	while (e > s && isspace((unsigned char)e[-1]))
		*--e = '\0';
	return s;
}

static void set_error(char *err, size_t errlen, const char *fmt, ...)
{
	va_list ap;
	if (!err || errlen == 0)
		return;
	va_start(ap, fmt);
	vsnprintf(err, errlen, fmt, ap);
	va_end(ap);
}

static int parse_int_or_default(const char *s, int def)
{
	char *end = NULL;
	long value;
	if (!s || *s == '\0')
		return def;
	value = strtol(s, &end, 10);
	if (!end || *trim(end) != '\0')
		return def;
	return (int)value;
}

static bool grow_ptr_array(void ***array, size_t *cap, size_t need)
{
	void **new_array;
	size_t new_cap;
	if (*cap >= need)
		return true;
	new_cap = *cap ? *cap * 2 : 8;
	while (new_cap < need)
		new_cap *= 2;
	new_array = realloc(*array, new_cap * sizeof(void *));
	if (!new_array)
		return false;
	*array = new_array;
	*cap = new_cap;
	return true;
}

static size_t appendf(char *buf, size_t buflen, size_t off, const char *fmt, ...)
{
	va_list ap;
	int written;
	if (!buf || buflen == 0 || off >= buflen)
		return off;
	va_start(ap, fmt);
	written = vsnprintf(buf + off, buflen - off, fmt, ap);
	va_end(ap);
	if (written < 0)
		return off;
	if ((size_t)written >= buflen - off)
		return buflen - 1;
	return off + (size_t)written;
}

static char *next_token(char **cursor)
{
	char *p;
	char *start;
	if (!cursor || !*cursor)
		return NULL;
	p = *cursor;
	while (*p && isspace((unsigned char)*p))
		p++;
	if (*p == '\0') {
		*cursor = p;
		return NULL;
	}
	start = p;
	if (*p == '\'' || *p == '"') {
		char quote = *p++;
		start = p;
		while (*p && *p != quote)
			p++;
		if (*p)
			*p++ = '\0';
	} else {
		while (*p && !isspace((unsigned char)*p))
			p++;
		if (*p)
			*p++ = '\0';
	}
	*cursor = p;
	return trim(start);
}

static char *next_csv_item(char **cursor)
{
	char *p;
	char *start;
	if (!cursor || !*cursor)
		return NULL;
	p = *cursor;
	while (*p && (*p == ',' || isspace((unsigned char)*p)))
		p++;
	if (*p == '\0') {
		*cursor = NULL;
		return NULL;
	}
	start = p;
	while (*p && *p != ',')
		p++;
	if (*p)
		*p++ = '\0';
	*cursor = p;
	return trim(start);
}

void pgb_routing_init(PgbRoutingRegistry *registry)
{
	if (registry)
		memset(registry, 0, sizeof(*registry));
}

void pgb_routing_free(PgbRoutingRegistry *registry)
{
	size_t i;
	if (!registry)
		return;
	for (i = 0; i < registry->target_count; i++)
		free(registry->targets[i]);
	for (i = 0; i < registry->group_count; i++) {
		free(registry->groups[i]->targets);
		free(registry->groups[i]);
	}
	for (i = 0; i < registry->route_count; i++)
		free(registry->routes[i]);
	free(registry->targets);
	free(registry->groups);
	free(registry->routes);
	memset(registry, 0, sizeof(*registry));
}

const char *pgb_target_role_to_string(PgbTargetRole role)
{
	switch (role) {
	case PGB_TARGET_ROLE_ANY:
		return "any";
	case PGB_TARGET_ROLE_RW:
		return "rw";
	case PGB_TARGET_ROLE_RO:
		return "ro";
	case PGB_TARGET_ROLE_STANDBY:
		return "standby";
	case PGB_TARGET_ROLE_UNKNOWN:
	default:
		return "unknown";
	}
}

const char *pgb_route_role_to_string(PgbRouteRole role)
{
	switch (role) {
	case PGB_ROUTE_ANY:
		return "any";
	case PGB_ROUTE_READ_WRITE:
		return "read-write";
	case PGB_ROUTE_READ_ONLY:
		return "read-only";
	case PGB_ROUTE_STANDBY_ONLY:
		return "standby";
	case PGB_ROUTE_UNKNOWN:
	default:
		return "unknown";
	}
}

const char *pgb_target_state_to_string(PgbTargetState state)
{
	switch (state) {
	case PGB_TARGET_UP:
		return "up";
	case PGB_TARGET_DOWN:
		return "down";
	case PGB_TARGET_EXCLUDED:
		return "excluded";
	case PGB_TARGET_DRAINING:
		return "draining";
	case PGB_TARGET_PROBING:
		return "probing";
	default:
		return "unknown";
	}
}

const char *pgb_lb_policy_to_string(PgbLbPolicy policy)
{
	switch (policy) {
	case PGB_LB_ROUND_ROBIN:
		return "round-robin";
	case PGB_LB_PRIORITY:
		return "priority";
	case PGB_LB_FIRST:
		return "first";
	default:
		return "unknown";
	}
}

const char *pgb_failure_kind_to_string(PgbFailureKind failure)
{
	switch (failure) {
	case PGB_FAIL_CONNECT:
		return "connect";
	case PGB_FAIL_LOGIN:
		return "login";
	case PGB_FAIL_ROLE_CHECK:
		return "role-check";
	case PGB_FAIL_SERVER_ERROR_BEFORE_QUERY:
		return "server-error-before-query";
	case PGB_FAIL_QUERY_ALREADY_SENT:
		return "query-already-sent";
	case PGB_FAIL_TRANSACTION_IN_PROGRESS:
		return "transaction-in-progress";
	case PGB_FAIL_COMMIT_UNKNOWN:
		return "commit-unknown";
	case PGB_FAIL_SERIALIZATION_OR_DEADLOCK:
		return "serialization-or-deadlock";
	case PGB_FAIL_OTHER:
	default:
		return "other";
	}
}

bool pgb_parse_target_role(const char *value, PgbTargetRole *role_out)
{
	PgbTargetRole role = PGB_TARGET_ROLE_UNKNOWN;
	if (ci_eq(value, "any"))
		role = PGB_TARGET_ROLE_ANY;
	else if (ci_eq(value, "rw") || ci_eq(value, "read-write") || ci_eq(value, "primary"))
		role = PGB_TARGET_ROLE_RW;
	else if (ci_eq(value, "ro") || ci_eq(value, "read-only") || ci_eq(value, "readonly") || ci_eq(value, "replica"))
		role = PGB_TARGET_ROLE_RO;
	else if (ci_eq(value, "standby") || ci_eq(value, "standby-only"))
		role = PGB_TARGET_ROLE_STANDBY;
	if (role_out)
		*role_out = role;
	return role != PGB_TARGET_ROLE_UNKNOWN;
}

bool pgb_parse_route_role(const char *value, PgbRouteRole *role_out)
{
	PgbRouteRole role = PGB_ROUTE_UNKNOWN;
	if (ci_eq(value, "any"))
		role = PGB_ROUTE_ANY;
	else if (ci_eq(value, "rw") || ci_eq(value, "write") || ci_eq(value, "read-write"))
		role = PGB_ROUTE_READ_WRITE;
	else if (ci_eq(value, "ro") || ci_eq(value, "read-only") || ci_eq(value, "readonly"))
		role = PGB_ROUTE_READ_ONLY;
	else if (ci_eq(value, "standby") || ci_eq(value, "standby-only"))
		role = PGB_ROUTE_STANDBY_ONLY;
	if (role_out)
		*role_out = role;
	return role != PGB_ROUTE_UNKNOWN;
}

bool pgb_parse_target_state(const char *value, PgbTargetState *state_out)
{
	PgbTargetState state;
	if (ci_eq(value, "up"))
		state = PGB_TARGET_UP;
	else if (ci_eq(value, "down"))
		state = PGB_TARGET_DOWN;
	else if (ci_eq(value, "excluded") || ci_eq(value, "exclude"))
		state = PGB_TARGET_EXCLUDED;
	else if (ci_eq(value, "draining") || ci_eq(value, "drain"))
		state = PGB_TARGET_DRAINING;
	else if (ci_eq(value, "probing") || ci_eq(value, "probe"))
		state = PGB_TARGET_PROBING;
	else
		return false;
	if (state_out)
		*state_out = state;
	return true;
}

bool pgb_parse_lb_policy(const char *value, PgbLbPolicy *policy_out)
{
	PgbLbPolicy policy;
	if (ci_eq(value, "round-robin") || ci_eq(value, "round_robin") || ci_eq(value, "rr"))
		policy = PGB_LB_ROUND_ROBIN;
	else if (ci_eq(value, "priority") || ci_eq(value, "prio"))
		policy = PGB_LB_PRIORITY;
	else if (ci_eq(value, "first"))
		policy = PGB_LB_FIRST;
	else
		return false;
	if (policy_out)
		*policy_out = policy;
	return true;
}

bool pgb_parse_bool(const char *value, bool *value_out)
{
	if (ci_eq(value, "1") || ci_eq(value, "true") || ci_eq(value, "yes") || ci_eq(value, "on")) {
		if (value_out)
			*value_out = true;
		return true;
	}
	if (ci_eq(value, "0") || ci_eq(value, "false") || ci_eq(value, "no") || ci_eq(value, "off")) {
		if (value_out)
			*value_out = false;
		return true;
	}
	return false;
}

PgbServerTarget *pgb_find_target(PgbRoutingRegistry *registry, const char *name)
{
	size_t i;
	if (!registry || !name)
		return NULL;
	for (i = 0; i < registry->target_count; i++) {
		if (ci_eq(registry->targets[i]->name, name))
			return registry->targets[i];
	}
	return NULL;
}

PgbServerGroup *pgb_find_group(PgbRoutingRegistry *registry, const char *name)
{
	size_t i;
	if (!registry || !name)
		return NULL;
	for (i = 0; i < registry->group_count; i++) {
		if (ci_eq(registry->groups[i]->name, name))
			return registry->groups[i];
	}
	return NULL;
}

PgbDatabaseRoute *pgb_find_database_route(PgbRoutingRegistry *registry, const char *database)
{
	size_t i;
	if (!registry || !database)
		return NULL;
	for (i = 0; i < registry->route_count; i++) {
		if (ci_eq(registry->routes[i]->database, database))
			return registry->routes[i];
	}
	return NULL;
}

PgbServerTarget *pgb_register_target(PgbRoutingRegistry *registry, const char *name,
	const char *host, int port, PgbTargetRole role, int priority, int weight, bool enabled)
{
	PgbServerTarget *target;
	if (!registry || !name || *name == '\0')
		return NULL;
	target = pgb_find_target(registry, name);
	if (!target) {
		if (!grow_ptr_array((void ***)&registry->targets, &registry->target_cap, registry->target_count + 1))
			return NULL;
		target = calloc(1, sizeof(*target));
		if (!target)
			return NULL;
		registry->targets[registry->target_count++] = target;
	}
	copy_text(target->name, sizeof(target->name), name);
	copy_text(target->host, sizeof(target->host), host && *host ? host : name);
	target->port = port > 0 ? port : 5432;
	target->configured_role = role;
	target->observed_role = PGB_TARGET_ROLE_UNKNOWN;
	target->state = enabled ? PGB_TARGET_UP : PGB_TARGET_DOWN;
	target->enabled = enabled;
	target->excluded = !enabled;
	target->draining = false;
	target->priority = priority;
	target->weight = weight > 0 ? weight : 1;
	target->last_error[0] = '\0';
	return target;
}

PgbServerGroup *pgb_register_group(PgbRoutingRegistry *registry, const char *name,
	PgbTargetRole role, PgbLbPolicy policy)
{
	PgbServerGroup *group;
	if (!registry || !name || *name == '\0')
		return NULL;
	group = pgb_find_group(registry, name);
	if (!group) {
		if (!grow_ptr_array((void ***)&registry->groups, &registry->group_cap, registry->group_count + 1))
			return NULL;
		group = calloc(1, sizeof(*group));
		if (!group)
			return NULL;
		registry->groups[registry->group_count++] = group;
	}
	copy_text(group->name, sizeof(group->name), name);
	group->configured_role = role;
	group->policy = policy;
	return group;
}

bool pgb_group_add_target(PgbServerGroup *group, PgbServerTarget *target)
{
	size_t i;
	if (!group || !target)
		return false;
	for (i = 0; i < group->target_count; i++) {
		if (group->targets[i] == target)
			return true;
	}
	if (!grow_ptr_array((void ***)&group->targets, &group->target_cap, group->target_count + 1))
		return false;
	group->targets[group->target_count++] = target;
	return true;
}

PgbDatabaseRoute *pgb_register_database_route(PgbRoutingRegistry *registry,
	const char *database, PgbServerGroup *primary_group, PgbRouteRole route_role,
	PgbRouteRole role_check_role, PgbLbPolicy policy, bool role_check_enabled,
	bool skip_unavailable, PgbFallbackMode rw_fallback,
	PgbServerGroup *fallback_group, bool fallback_to_primary)
{
	PgbDatabaseRoute *route;
	if (!registry || !database || *database == '\0' || !primary_group)
		return NULL;
	route = pgb_find_database_route(registry, database);
	if (!route) {
		if (!grow_ptr_array((void ***)&registry->routes, &registry->route_cap, registry->route_count + 1))
			return NULL;
		route = calloc(1, sizeof(*route));
		if (!route)
			return NULL;
		registry->routes[registry->route_count++] = route;
	}
	copy_text(route->database, sizeof(route->database), database);
	route->primary_group = primary_group;
	route->fallback_group = fallback_group;
	route->route_role = route_role;
	route->role_check_role = role_check_role == PGB_ROUTE_ANY ? route_role : role_check_role;
	route->policy = policy;
	route->role_check_enabled = role_check_enabled;
	route->skip_unavailable = skip_unavailable;
	route->rw_fallback = rw_fallback;
	route->fallback_to_primary = fallback_to_primary;
	primary_group->policy = policy;
	return route;
}

bool pgb_target_is_usable(const PgbServerTarget *target, bool skip_unavailable)
{
	if (!target || !target->enabled || target->excluded || target->draining)
		return false;
	if (target->state == PGB_TARGET_EXCLUDED || target->state == PGB_TARGET_DRAINING)
		return false;
	if (skip_unavailable && target->state == PGB_TARGET_DOWN)
		return false;
	return true;
}

bool pgb_observed_role_satisfies_route(PgbTargetRole observed_role, PgbRouteRole route_role)
{
	if (route_role == PGB_ROUTE_ANY || route_role == PGB_ROUTE_UNKNOWN)
		return true;
	if (observed_role == PGB_TARGET_ROLE_UNKNOWN || observed_role == PGB_TARGET_ROLE_ANY)
		return true;
	if (route_role == PGB_ROUTE_READ_WRITE)
		return observed_role == PGB_TARGET_ROLE_RW;
	if (route_role == PGB_ROUTE_READ_ONLY)
		return observed_role == PGB_TARGET_ROLE_RO || observed_role == PGB_TARGET_ROLE_STANDBY;
	if (route_role == PGB_ROUTE_STANDBY_ONLY)
		return observed_role == PGB_TARGET_ROLE_STANDBY;
	return false;
}

bool pgb_target_matches_route(const PgbServerTarget *target, PgbRouteRole route_role)
{
	PgbTargetRole role;
	if (!target)
		return false;
	role = target->observed_role != PGB_TARGET_ROLE_UNKNOWN ?
		target->observed_role : target->configured_role;
	return pgb_observed_role_satisfies_route(role, route_role);
}

PgbTargetRole pgb_observed_role_from_pg_status(bool pg_is_in_recovery, bool transaction_read_only)
{
	if (pg_is_in_recovery)
		return PGB_TARGET_ROLE_STANDBY;
	if (transaction_read_only)
		return PGB_TARGET_ROLE_RO;
	return PGB_TARGET_ROLE_RW;
}

static int cmp_target_priority(const void *a, const void *b)
{
	const PgbServerTarget *ta = *(const PgbServerTarget * const *)a;
	const PgbServerTarget *tb = *(const PgbServerTarget * const *)b;
	if (ta->priority < tb->priority)
		return -1;
	if (ta->priority > tb->priority)
		return 1;
	return strcmp(ta->name, tb->name);
}

size_t pgb_build_candidate_order(PgbServerGroup *group, PgbRouteRole route_role,
	bool skip_unavailable, PgbServerTarget **out, size_t out_count)
{
	size_t i;
	size_t count = 0;
	size_t start = 0;
	if (!group || !out || out_count == 0)
		return 0;
	if (group->target_count == 0)
		return 0;
	if (group->policy == PGB_LB_ROUND_ROBIN)
		start = group->rr_counter % group->target_count;
	for (i = 0; i < group->target_count; i++) {
		size_t idx = group->policy == PGB_LB_ROUND_ROBIN ?
			(start + i) % group->target_count : i;
		PgbServerTarget *target = group->targets[idx];
		if (!pgb_target_is_usable(target, skip_unavailable))
			continue;
		if (!pgb_target_matches_route(target, route_role))
			continue;
		out[count++] = target;
		if (count == out_count)
			break;
	}
	if (group->policy == PGB_LB_PRIORITY && count > 1)
		qsort(out, count, sizeof(*out), cmp_target_priority);
	return count;
}

PgbServerTarget *pgb_select_target(PgbServerGroup *group, PgbRouteRole route_role,
	bool skip_unavailable)
{
	PgbServerTarget *candidate = NULL;
	PgbServerTarget *candidates[256];
	size_t count;
	if (!group)
		return NULL;
	count = pgb_build_candidate_order(group, route_role, skip_unavailable,
		candidates, sizeof(candidates) / sizeof(candidates[0]));
	if (count == 0) {
		group->counters.fallback_rejected_count++;
		return NULL;
	}
	candidate = candidates[0];
	if (group->policy == PGB_LB_ROUND_ROBIN)
		group->rr_counter++;
	pgb_record_target_selected(group, candidate);
	return candidate;
}

void pgb_record_target_selected(PgbServerGroup *group, PgbServerTarget *target)
{
	time_t now = time(NULL);
	if (group)
		group->counters.selected_count++;
	if (target) {
		target->counters.selected_count++;
		target->last_selected_at = now;
	}
}

void pgb_record_connect_attempt(PgbServerTarget *target)
{
	if (target)
		target->counters.connect_attempt_count++;
}

void pgb_record_connect_success(PgbServerTarget *target)
{
	if (!target)
		return;
	target->counters.connect_ok_count++;
	target->last_connect_ok_at = time(NULL);
	if (target->state == PGB_TARGET_DOWN || target->state == PGB_TARGET_PROBING)
		target->state = PGB_TARGET_UP;
	target->last_error[0] = '\0';
}

void pgb_record_connect_failure(PgbServerTarget *target, const char *reason)
{
	if (!target)
		return;
	target->counters.connect_fail_count++;
	target->last_connect_fail_at = time(NULL);
	if (target->state != PGB_TARGET_EXCLUDED && target->state != PGB_TARGET_DRAINING)
		target->state = PGB_TARGET_DOWN;
	copy_text(target->last_error, sizeof(target->last_error), reason ? reason : "connect failed");
}

void pgb_record_login_failure(PgbServerTarget *target, const char *reason)
{
	if (!target)
		return;
	target->counters.login_fail_count++;
	pgb_record_connect_failure(target, reason ? reason : "login failed");
}

void pgb_record_role_check(PgbServerTarget *target, PgbTargetRole observed_role,
	PgbRouteRole expected_route)
{
	if (!target)
		return;
	target->counters.role_check_count++;
	target->observed_role = observed_role;
	target->last_role_check_at = time(NULL);
	if (!pgb_observed_role_satisfies_route(observed_role, expected_route)) {
		target->counters.role_mismatch_count++;
		if (target->state != PGB_TARGET_EXCLUDED && target->state != PGB_TARGET_DRAINING)
			target->state = PGB_TARGET_PROBING;
		copy_text(target->last_error, sizeof(target->last_error), "role mismatch");
	} else {
		if (target->state == PGB_TARGET_PROBING || target->state == PGB_TARGET_DOWN)
			target->state = PGB_TARGET_UP;
		target->last_error[0] = '\0';
	}
}

void pgb_record_fallback(PgbServerTarget *from, PgbServerTarget *to,
	bool accepted, const char *reason)
{
	if (from) {
		if (accepted)
			from->counters.fallback_from_count++;
		else
			from->counters.fallback_rejected_count++;
		if (reason)
			copy_text(from->last_error, sizeof(from->last_error), reason);
	}
	if (to && accepted)
		to->counters.fallback_to_count++;
}

void pgb_set_target_state(PgbServerTarget *target, PgbTargetState state, const char *reason)
{
	if (!target)
		return;
	target->state = state;
	target->excluded = state == PGB_TARGET_EXCLUDED;
	target->draining = state == PGB_TARGET_DRAINING;
	if (state == PGB_TARGET_EXCLUDED)
		target->enabled = false;
	else if (state != PGB_TARGET_DOWN)
		target->enabled = true;
	copy_text(target->last_error, sizeof(target->last_error), reason ? reason : "");
}

void pgb_include_target(PgbServerTarget *target)
{
	if (!target)
		return;
	target->enabled = true;
	target->excluded = false;
	target->draining = false;
	if (target->state == PGB_TARGET_EXCLUDED || target->state == PGB_TARGET_DRAINING)
		target->state = PGB_TARGET_PROBING;
	target->last_error[0] = '\0';
}

void pgb_exclude_target(PgbServerTarget *target, const char *reason)
{
	pgb_set_target_state(target, PGB_TARGET_EXCLUDED, reason ? reason : "excluded");
}

void pgb_drain_target(PgbServerTarget *target, const char *reason)
{
	pgb_set_target_state(target, PGB_TARGET_DRAINING, reason ? reason : "draining");
}

bool pgb_should_fallback(PgbFailureKind failure, bool query_sent,
	bool transaction_in_progress, PgbFallbackMode mode)
{
	if (mode != PGB_FALLBACK_CONNECT_ONLY)
		return false;
	if (query_sent || transaction_in_progress)
		return false;
	switch (failure) {
	case PGB_FAIL_CONNECT:
	case PGB_FAIL_LOGIN:
	case PGB_FAIL_ROLE_CHECK:
	case PGB_FAIL_SERVER_ERROR_BEFORE_QUERY:
		return true;
	default:
		return false;
	}
}

bool pgb_parse_target_line(PgbRoutingRegistry *registry, const char *name,
	const char *line, char *err, size_t errlen)
{
	char tmp[1024];
	char host[PGB_ROUTING_HOST_MAX] = "";
	char *cursor;
	char *token;
	int port = 5432;
	int priority = 100;
	int weight = 1;
	bool enabled = true;
	PgbTargetRole role = PGB_TARGET_ROLE_ANY;
	PgbTargetState state = PGB_TARGET_UP;
	PgbServerTarget *target;
	if (!registry || !name || !line) {
		set_error(err, errlen, "invalid target line");
		return false;
	}
	copy_text(tmp, sizeof(tmp), line);
	cursor = tmp;
	while ((token = next_token(&cursor)) != NULL) {
		char *eq = strchr(token, '=');
		char *key;
		char *value;
		bool bool_value;
		if (!eq) {
			copy_text(host, sizeof(host), token);
			continue;
		}
		*eq = '\0';
		key = trim(token);
		value = trim(eq + 1);
		if (ci_eq(key, "host"))
			copy_text(host, sizeof(host), value);
		else if (ci_eq(key, "port"))
			port = parse_int_or_default(value, 5432);
		else if (ci_eq(key, "role")) {
			if (!pgb_parse_target_role(value, &role)) {
				set_error(err, errlen, "bad target role %s", value);
				return false;
			}
		} else if (ci_eq(key, "priority"))
			priority = parse_int_or_default(value, 100);
		else if (ci_eq(key, "weight"))
			weight = parse_int_or_default(value, 1);
		else if (ci_eq(key, "enabled")) {
			if (!pgb_parse_bool(value, &enabled)) {
				set_error(err, errlen, "bad enabled value %s", value);
				return false;
			}
		} else if (ci_eq(key, "excluded")) {
			if (!pgb_parse_bool(value, &bool_value)) {
				set_error(err, errlen, "bad excluded value %s", value);
				return false;
			}
			if (bool_value)
				state = PGB_TARGET_EXCLUDED;
		} else if (ci_eq(key, "state")) {
			if (!pgb_parse_target_state(value, &state)) {
				set_error(err, errlen, "bad target state %s", value);
				return false;
			}
		} else {
			set_error(err, errlen, "unknown target option %s", key);
			return false;
		}
	}
	target = pgb_register_target(registry, name, host[0] ? host : name,
		port, role, priority, weight, enabled);
	if (!target) {
		set_error(err, errlen, "cannot register target %s", name);
		return false;
	}
	pgb_set_target_state(target, state, NULL);
	return true;
}

static bool group_add_csv(PgbRoutingRegistry *registry, PgbServerGroup *group,
	char *list, char *err, size_t errlen)
{
	char *cursor = list;
	char *item;
	while ((item = next_csv_item(&cursor)) != NULL) {
		PgbServerTarget *target;
		if (*item == '\0')
			continue;
		target = pgb_find_target(registry, item);
		if (!target) {
			set_error(err, errlen, "unknown target %s", item);
			return false;
		}
		if (!pgb_group_add_target(group, target)) {
			set_error(err, errlen, "cannot add target %s", item);
			return false;
		}
	}
	return true;
}

bool pgb_parse_group_line(PgbRoutingRegistry *registry, const char *name,
	const char *line, char *err, size_t errlen)
{
	char tmp[1024];
	char *cursor;
	char *token;
	PgbTargetRole role = PGB_TARGET_ROLE_ANY;
	PgbLbPolicy policy = PGB_LB_ROUND_ROBIN;
	PgbServerGroup *group;
	if (!registry || !name || !line) {
		set_error(err, errlen, "invalid group line");
		return false;
	}
	group = pgb_register_group(registry, name, role, policy);
	if (!group) {
		set_error(err, errlen, "cannot register group %s", name);
		return false;
	}
	group->target_count = 0;
	copy_text(tmp, sizeof(tmp), line);
	cursor = tmp;
	while ((token = next_token(&cursor)) != NULL) {
		char *eq = strchr(token, '=');
		if (eq) {
			char *key;
			char *value;
			*eq = '\0';
			key = trim(token);
			value = trim(eq + 1);
			if (ci_eq(key, "targets")) {
				if (!group_add_csv(registry, group, value, err, errlen))
					return false;
			} else if (ci_eq(key, "role") || ci_eq(key, "route")) {
				if (!pgb_parse_target_role(value, &role)) {
					set_error(err, errlen, "bad group role %s", value);
					return false;
				}
				group->configured_role = role;
			} else if (ci_eq(key, "policy") || ci_eq(key, "lb_policy")) {
				if (!pgb_parse_lb_policy(value, &policy)) {
					set_error(err, errlen, "bad group policy %s", value);
					return false;
				}
				group->policy = policy;
			} else {
				set_error(err, errlen, "unknown group option %s", key);
				return false;
			}
		} else {
			if (!group_add_csv(registry, group, token, err, errlen))
				return false;
		}
	}
	if (group->target_count == 0) {
		set_error(err, errlen, "group %s has no targets", name);
		return false;
	}
	return true;
}

bool pgb_parse_database_route_line(PgbRoutingRegistry *registry, const char *database,
	const char *line, char *err, size_t errlen)
{
	char tmp[2048];
	char *cursor;
	char *token;
	PgbServerGroup *primary_group = NULL;
	PgbServerGroup *fallback_group = NULL;
	PgbRouteRole route_role = PGB_ROUTE_ANY;
	PgbRouteRole role_check_role = PGB_ROUTE_ANY;
	PgbLbPolicy policy = PGB_LB_ROUND_ROBIN;
	PgbFallbackMode fallback = PGB_FALLBACK_NONE;
	bool role_check_enabled = false;
	bool skip_unavailable = true;
	bool fallback_to_primary = false;
	if (!registry || !database || !line) {
		set_error(err, errlen, "invalid database route line");
		return false;
	}
	copy_text(tmp, sizeof(tmp), line);
	cursor = tmp;
	while ((token = next_token(&cursor)) != NULL) {
		char *eq = strchr(token, '=');
		char *key;
		char *value;
		bool bool_value;
		if (!eq)
			continue;
		*eq = '\0';
		key = trim(token);
		value = trim(eq + 1);
		if (ci_eq(key, "server_group") || ci_eq(key, "group")) {
			primary_group = pgb_find_group(registry, value);
			if (!primary_group) {
				set_error(err, errlen, "unknown server_group %s", value);
				return false;
			}
		} else if (ci_eq(key, "fallback_group") || ci_eq(key, "ro_fallback_group")) {
			fallback_group = pgb_find_group(registry, value);
			if (!fallback_group) {
				set_error(err, errlen, "unknown fallback_group %s", value);
				return false;
			}
		} else if (ci_eq(key, "route")) {
			if (!pgb_parse_route_role(value, &route_role)) {
				set_error(err, errlen, "bad route %s", value);
				return false;
			}
		} else if (ci_eq(key, "role_check")) {
			if (pgb_parse_bool(value, &bool_value)) {
				role_check_enabled = bool_value;
			} else if (ci_eq(value, "none")) {
				role_check_enabled = false;
				role_check_role = PGB_ROUTE_ANY;
			} else {
				if (!pgb_parse_route_role(value, &role_check_role)) {
					set_error(err, errlen, "bad role_check %s", value);
					return false;
				}
				role_check_enabled = role_check_role != PGB_ROUTE_ANY;
			}
		} else if (ci_eq(key, "lb_policy") || ci_eq(key, "policy") || ci_eq(key, "rw_policy")) {
			if (!pgb_parse_lb_policy(value, &policy)) {
				set_error(err, errlen, "bad policy %s", value);
				return false;
			}
		} else if (ci_eq(key, "rw_fallback")) {
			if (ci_eq(value, "connect-only") || ci_eq(value, "connect_only"))
				fallback = PGB_FALLBACK_CONNECT_ONLY;
			else if (ci_eq(value, "none") || ci_eq(value, "0"))
				fallback = PGB_FALLBACK_NONE;
			else {
				set_error(err, errlen, "bad rw_fallback %s", value);
				return false;
			}
		} else if (ci_eq(key, "skip_unavailable") || ci_eq(key, "ro_skip_unavailable")) {
			if (!pgb_parse_bool(value, &skip_unavailable)) {
				set_error(err, errlen, "bad skip_unavailable %s", value);
				return false;
			}
		} else if (ci_eq(key, "fallback_to_primary") || ci_eq(key, "ro_fallback_to_primary")) {
			if (!pgb_parse_bool(value, &fallback_to_primary)) {
				set_error(err, errlen, "bad fallback_to_primary %s", value);
				return false;
			}
		} else {
			/* Keep normal PgBouncer database parameters untouched. */
			continue;
		}
	}
	if (!primary_group) {
		set_error(err, errlen, "database %s needs server_group", database);
		return false;
	}
	if (!pgb_register_database_route(registry, database, primary_group, route_role,
		role_check_role, policy, role_check_enabled, skip_unavailable, fallback,
		fallback_group, fallback_to_primary)) {
		set_error(err, errlen, "cannot register database route %s", database);
		return false;
	}
	return true;
}

size_t pgb_format_server_targets(PgbRoutingRegistry *registry, char *buf, size_t buflen)
{
	size_t off = 0;
	size_t i;
	if (!buf || buflen == 0)
		return 0;
	buf[0] = '\0';
	off = appendf(buf, buflen, off,
		"target\thost\tport\tconfigured_role\tobserved_role\tstate\tenabled\tpriority\tweight\tselected\tconnect_attempt\tconnect_ok\tconnect_fail\trole_mismatch\tfallback_from\tfallback_to\tlast_error\n");
	if (!registry)
		return off;
	for (i = 0; i < registry->target_count; i++) {
		PgbServerTarget *t = registry->targets[i];
		off = appendf(buf, buflen, off,
			"%s\t%s\t%d\t%s\t%s\t%s\t%d\t%d\t%d\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%s\n",
			t->name, t->host, t->port,
			pgb_target_role_to_string(t->configured_role),
			pgb_target_role_to_string(t->observed_role),
			pgb_target_state_to_string(t->state), t->enabled ? 1 : 0,
			t->priority, t->weight,
			(unsigned long long)t->counters.selected_count,
			(unsigned long long)t->counters.connect_attempt_count,
			(unsigned long long)t->counters.connect_ok_count,
			(unsigned long long)t->counters.connect_fail_count,
			(unsigned long long)t->counters.role_mismatch_count,
			(unsigned long long)t->counters.fallback_from_count,
			(unsigned long long)t->counters.fallback_to_count,
			t->last_error);
	}
	return off;
}

size_t pgb_format_server_groups(PgbRoutingRegistry *registry, char *buf, size_t buflen)
{
	size_t off = 0;
	size_t i;
	size_t j;
	if (!buf || buflen == 0)
		return 0;
	buf[0] = '\0';
	off = appendf(buf, buflen, off, "group\trole\tpolicy\ttarget_count\ttargets\tselected\n");
	if (!registry)
		return off;
	for (i = 0; i < registry->group_count; i++) {
		PgbServerGroup *g = registry->groups[i];
		off = appendf(buf, buflen, off, "%s\t%s\t%s\t%zu\t",
			g->name, pgb_target_role_to_string(g->configured_role),
			pgb_lb_policy_to_string(g->policy), g->target_count);
		for (j = 0; j < g->target_count; j++)
			off = appendf(buf, buflen, off, "%s%s", j ? "," : "", g->targets[j]->name);
		off = appendf(buf, buflen, off, "\t%llu\n", (unsigned long long)g->counters.selected_count);
	}
	return off;
}

size_t pgb_format_route_stats(PgbRoutingRegistry *registry, char *buf, size_t buflen)
{
	size_t off = 0;
	size_t i;
	if (!buf || buflen == 0)
		return 0;
	buf[0] = '\0';
	off = appendf(buf, buflen, off,
		"database\troute\trole_check_route\tgroup\tfallback_group\tpolicy\tfallback_mode\trole_check\tskip_unavailable\tselected\tconnect_fail\trole_mismatch\tfallback_rejected\n");
	if (!registry)
		return off;
	for (i = 0; i < registry->route_count; i++) {
		PgbDatabaseRoute *r = registry->routes[i];
		off = appendf(buf, buflen, off,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%llu\t%llu\t%llu\t%llu\n",
			r->database, pgb_route_role_to_string(r->route_role),
			pgb_route_role_to_string(r->role_check_role),
			r->primary_group ? r->primary_group->name : "",
			r->fallback_group ? r->fallback_group->name : "",
			pgb_lb_policy_to_string(r->policy),
			r->rw_fallback == PGB_FALLBACK_CONNECT_ONLY ? "connect-only" : "none",
			r->role_check_enabled ? 1 : 0,
			r->skip_unavailable ? 1 : 0,
			(unsigned long long)r->counters.selected_count,
			(unsigned long long)r->counters.connect_fail_count,
			(unsigned long long)r->counters.role_mismatch_count,
			(unsigned long long)r->counters.fallback_rejected_count);
	}
	return off;
}

const char *pgb_role_check_sql(PgbRouteRole role)
{
	(void)role;
	return "SELECT pg_is_in_recovery() AS in_recovery, "
	       "current_setting('transaction_read_only') IN ('on','true','1') AS transaction_read_only";
}
