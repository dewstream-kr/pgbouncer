/* PgBouncer RW/RO routing core. SPDX-License-Identifier: ISC */
#ifndef PGBOUNCER_RWRO_ROUTING_H
#define PGBOUNCER_RWRO_ROUTING_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#define PGB_ROUTING_NAME_MAX 64
#define PGB_ROUTING_HOST_MAX 256
#define PGB_ROUTING_ERR_MAX 256

typedef enum PgbTargetRole {
	PGB_TARGET_ROLE_ANY = 0,
	PGB_TARGET_ROLE_RW,
	PGB_TARGET_ROLE_RO,
	PGB_TARGET_ROLE_STANDBY,
	PGB_TARGET_ROLE_UNKNOWN
} PgbTargetRole;

typedef enum PgbRouteRole {
	PGB_ROUTE_ANY = 0,
	PGB_ROUTE_READ_WRITE,
	PGB_ROUTE_READ_ONLY,
	PGB_ROUTE_STANDBY_ONLY,
	PGB_ROUTE_UNKNOWN
} PgbRouteRole;

typedef enum PgbTargetState {
	PGB_TARGET_UP = 0,
	PGB_TARGET_DOWN,
	PGB_TARGET_EXCLUDED,
	PGB_TARGET_DRAINING,
	PGB_TARGET_PROBING
} PgbTargetState;

typedef enum PgbLbPolicy {
	PGB_LB_ROUND_ROBIN = 0,
	PGB_LB_PRIORITY,
	PGB_LB_FIRST
} PgbLbPolicy;

typedef enum PgbFallbackMode {
	PGB_FALLBACK_NONE = 0,
	PGB_FALLBACK_CONNECT_ONLY
} PgbFallbackMode;

typedef enum PgbFailureKind {
	PGB_FAIL_CONNECT = 0,
	PGB_FAIL_LOGIN,
	PGB_FAIL_ROLE_CHECK,
	PGB_FAIL_SERVER_ERROR_BEFORE_QUERY,
	PGB_FAIL_QUERY_ALREADY_SENT,
	PGB_FAIL_TRANSACTION_IN_PROGRESS,
	PGB_FAIL_COMMIT_UNKNOWN,
	PGB_FAIL_SERIALIZATION_OR_DEADLOCK,
	PGB_FAIL_OTHER
} PgbFailureKind;

typedef struct PgbRoutingCounters {
	uint64_t selected_count;
	uint64_t connect_attempt_count;
	uint64_t connect_ok_count;
	uint64_t connect_fail_count;
	uint64_t login_fail_count;
	uint64_t role_check_count;
	uint64_t role_mismatch_count;
	uint64_t fallback_from_count;
	uint64_t fallback_to_count;
	uint64_t fallback_rejected_count;
} PgbRoutingCounters;

typedef struct PgbServerTarget {
	char name[PGB_ROUTING_NAME_MAX];
	char host[PGB_ROUTING_HOST_MAX];
	int port;
	PgbTargetRole configured_role;
	PgbTargetRole observed_role;
	PgbTargetState state;
	bool enabled;
	bool excluded;
	bool draining;
	int priority;
	int weight;
	PgbRoutingCounters counters;
	time_t last_selected_at;
	time_t last_connect_ok_at;
	time_t last_connect_fail_at;
	time_t last_role_check_at;
	char last_error[PGB_ROUTING_ERR_MAX];
} PgbServerTarget;

typedef struct PgbServerGroup {
	char name[PGB_ROUTING_NAME_MAX];
	PgbTargetRole configured_role;
	PgbLbPolicy policy;
	PgbServerTarget **targets;
	size_t target_count;
	size_t target_cap;
	uint32_t rr_counter;
	PgbRoutingCounters counters;
} PgbServerGroup;

typedef struct PgbDatabaseRoute {
	char database[PGB_ROUTING_NAME_MAX];
	PgbServerGroup *primary_group;
	PgbServerGroup *fallback_group;
	PgbRouteRole route_role;
	PgbRouteRole role_check_role;
	PgbLbPolicy policy;
	bool role_check_enabled;
	bool skip_unavailable;
	PgbFallbackMode rw_fallback;
	bool fallback_to_primary;
	PgbRoutingCounters counters;
} PgbDatabaseRoute;

typedef struct PgbRoutingRegistry {
	PgbServerTarget **targets;
	size_t target_count;
	size_t target_cap;
	PgbServerGroup **groups;
	size_t group_count;
	size_t group_cap;
	PgbDatabaseRoute **routes;
	size_t route_count;
	size_t route_cap;
} PgbRoutingRegistry;

void pgb_routing_init(PgbRoutingRegistry *registry);
void pgb_routing_free(PgbRoutingRegistry *registry);

const char *pgb_target_role_to_string(PgbTargetRole role);
const char *pgb_route_role_to_string(PgbRouteRole role);
const char *pgb_target_state_to_string(PgbTargetState state);
const char *pgb_lb_policy_to_string(PgbLbPolicy policy);
const char *pgb_failure_kind_to_string(PgbFailureKind failure);

bool pgb_parse_target_role(const char *value, PgbTargetRole *role_out);
bool pgb_parse_route_role(const char *value, PgbRouteRole *role_out);
bool pgb_parse_target_state(const char *value, PgbTargetState *state_out);
bool pgb_parse_lb_policy(const char *value, PgbLbPolicy *policy_out);
bool pgb_parse_bool(const char *value, bool *value_out);

PgbServerTarget *pgb_find_target(PgbRoutingRegistry *registry, const char *name);
PgbServerGroup *pgb_find_group(PgbRoutingRegistry *registry, const char *name);
PgbDatabaseRoute *pgb_find_database_route(PgbRoutingRegistry *registry, const char *database);

PgbServerTarget *pgb_register_target(PgbRoutingRegistry *registry, const char *name,
	const char *host, int port, PgbTargetRole role, int priority, int weight, bool enabled);
PgbServerGroup *pgb_register_group(PgbRoutingRegistry *registry, const char *name,
	PgbTargetRole role, PgbLbPolicy policy);
bool pgb_group_add_target(PgbServerGroup *group, PgbServerTarget *target);
PgbDatabaseRoute *pgb_register_database_route(PgbRoutingRegistry *registry,
	const char *database, PgbServerGroup *primary_group, PgbRouteRole route_role,
	PgbRouteRole role_check_role, PgbLbPolicy policy, bool role_check_enabled,
	bool skip_unavailable, PgbFallbackMode rw_fallback,
	PgbServerGroup *fallback_group, bool fallback_to_primary);

bool pgb_target_is_usable(const PgbServerTarget *target, bool skip_unavailable);
bool pgb_target_matches_route(const PgbServerTarget *target, PgbRouteRole route_role);
bool pgb_observed_role_satisfies_route(PgbTargetRole observed_role, PgbRouteRole route_role);
PgbTargetRole pgb_observed_role_from_pg_status(bool pg_is_in_recovery, bool transaction_read_only);

size_t pgb_build_candidate_order(PgbServerGroup *group, PgbRouteRole route_role,
	bool skip_unavailable, PgbServerTarget **out, size_t out_count);
PgbServerTarget *pgb_select_target(PgbServerGroup *group, PgbRouteRole route_role,
	bool skip_unavailable);

void pgb_record_target_selected(PgbServerGroup *group, PgbServerTarget *target);
void pgb_record_connect_attempt(PgbServerTarget *target);
void pgb_record_connect_success(PgbServerTarget *target);
void pgb_record_connect_failure(PgbServerTarget *target, const char *reason);
void pgb_record_login_failure(PgbServerTarget *target, const char *reason);
void pgb_record_role_check(PgbServerTarget *target, PgbTargetRole observed_role,
	PgbRouteRole expected_route);
void pgb_record_fallback(PgbServerTarget *from, PgbServerTarget *to,
	bool accepted, const char *reason);
void pgb_set_target_state(PgbServerTarget *target, PgbTargetState state, const char *reason);
void pgb_include_target(PgbServerTarget *target);
void pgb_exclude_target(PgbServerTarget *target, const char *reason);
void pgb_drain_target(PgbServerTarget *target, const char *reason);

bool pgb_should_fallback(PgbFailureKind failure, bool query_sent,
	bool transaction_in_progress, PgbFallbackMode mode);

bool pgb_parse_target_line(PgbRoutingRegistry *registry, const char *name,
	const char *line, char *err, size_t errlen);
bool pgb_parse_group_line(PgbRoutingRegistry *registry, const char *name,
	const char *line, char *err, size_t errlen);
bool pgb_parse_database_route_line(PgbRoutingRegistry *registry, const char *database,
	const char *line, char *err, size_t errlen);

size_t pgb_format_server_targets(PgbRoutingRegistry *registry, char *buf, size_t buflen);
size_t pgb_format_server_groups(PgbRoutingRegistry *registry, char *buf, size_t buflen);
size_t pgb_format_route_stats(PgbRoutingRegistry *registry, char *buf, size_t buflen);

const char *pgb_role_check_sql(PgbRouteRole role);

#ifdef __cplusplus
}
#endif
#endif
