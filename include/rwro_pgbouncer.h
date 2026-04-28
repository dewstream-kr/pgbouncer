/* PgBouncer RW/RO routing glue API. SPDX-License-Identifier: ISC */
#ifndef PGBOUNCER_RWRO_PGBOUNCER_H
#define PGBOUNCER_RWRO_PGBOUNCER_H

#include <stdbool.h>
#include <stddef.h>

#include "rwro_route.h"

/* These are defined by PgBouncer's bouncer.h.  Keeping forward declarations
 * here lets the header be parsed before bouncer.h in some integration layouts. */
typedef struct PgDatabase PgDatabase;
typedef struct PgPool PgPool;
typedef struct PgSocket PgSocket;
typedef struct PktHdr PktHdr;

/* main.c config/lifecycle hooks */
void rwro_config_setup(void);
void rwro_config_cleanup(void);
void rwro_config_begin(void);
bool rwro_config_finish(void);
void rwro_config_abort(void);

/* CfSect.set_key callbacks */
bool rwro_parse_database(void *base, const char *name, const char *connstr);
bool rwro_parse_server(void *base, const char *name, const char *connstr);
bool rwro_parse_server_group(void *base, const char *name, const char *connstr);

/* Registry lookup */
PgbRoutingRegistry *rwro_active_registry(void);
PgbDatabaseRoute *rwro_route_for_database_name(const char *database_name);
PgbDatabaseRoute *rwro_route_for_database(PgDatabase *db);
bool rwro_database_has_route(PgDatabase *db);

/* objects.c connection selection hooks */
bool rwro_select_target_for_database(PgDatabase *db, PgbServerTarget **target_out,
	char *err, size_t errlen);
void rwro_attach_server_target(PgSocket *server, PgbServerTarget *target);
void rwro_detach_server(PgSocket *server);
const char *rwro_server_target_host(PgSocket *server);
int rwro_server_target_port(PgSocket *server, int fallback_port);
bool rwro_pool_can_retry_now(PgPool *pool);

/* server.c/accounting/fallback hooks */
void rwro_record_connect_attempt(PgSocket *server);
void rwro_record_connect_success(PgSocket *server);
void rwro_record_connect_failure(PgSocket *server, const char *reason);
void rwro_record_login_failure(PgSocket *server, const char *reason);
bool rwro_try_prepare_fallback(PgSocket *server, PgbFailureKind failure,
	bool transaction_in_progress, const char *reason, PgbServerTarget **next_target_out);

/* client.c safety guard hook */
void rwro_mark_query_sent(PgSocket *server);
bool rwro_query_sent(PgSocket *server);

/* Role-check state machine for server.c. */
bool rwro_role_check_needed(PgSocket *server);
const char *rwro_role_check_sql(PgSocket *server);
void rwro_role_check_begin(PgSocket *server);
bool rwro_role_check_running(PgSocket *server);
bool rwro_role_check_handle_packet(PgSocket *server, PktHdr *pkt,
	bool *done_out, bool *ok_out, const char **reason_out);
void rwro_role_check_finish(PgSocket *server, bool ok, const char *reason);

/* Admin console helpers.  The formatters emit tab-separated text. */
size_t rwro_show_server_targets(char *dst, size_t dstlen);
size_t rwro_show_server_groups(char *dst, size_t dstlen);
size_t rwro_show_route_stats(char *dst, size_t dstlen);
bool rwro_include_target_cmd(const char *target_name, char *err, size_t errlen);
bool rwro_exclude_target_cmd(const char *target_name, char *err, size_t errlen);
bool rwro_drain_target_cmd(const char *target_name, char *err, size_t errlen);

#endif /* PGBOUNCER_RWRO_PGBOUNCER_H */
