PgBouncer RW/RO Routing 확장 기능 (운영 가이드)
===========================================

개요
----

본 PgBouncer는 기본 upstream 기능에 더해 아래 기능이 추가된 커스텀 버전입니다.

 * Read-Write / Read-Only 서버 분리
 * Read-Only 서버 대상 Round-Robin 분산
 * Read-Write 서버 대상 Priority 기반 선택
 * PostgreSQL Role 기반 서버 검증 (Primary / Standby)
 * Write 실패 시 안전한 fallback (연결 단계에서만)
 * Routing 상태 조회용 Admin View 제공

주의사항
--------

 * 본 기능은 PgBouncer 공식 버전에 존재하지 않는 커스텀 기능입니다.
 * 반드시 패치된 바이너리에서만 동작합니다.
 * 기본 PgBouncer에서는 설정 적용 시 오류 발생합니다.

아키텍처
--------

Application은 Read / Write를 분리하여 접속해야 합니다.

    Application
      ├─ Write → PgBouncer(app_rw) → Primary
      └─ Read  → PgBouncer(app_ro) → Replica (RR 분산)

중요:

 * PgBouncer는 SQL을 분석하지 않음
 * 어떤 DB로 보낼지는 "접속 database 이름(app_rw / app_ro)" 기준으로 결정됨

설정 방법
---------

1. server 그룹 정의

    [server_groups]  
    appdb_rw = pg1,pg2  
    appdb_ro = pg3,pg4,pg5  

2. 서버 등록

    [servers]  
    pg1 = host=10.0.1.11 port=5432 role=rw priority=1 enabled=1  
    pg2 = host=10.0.1.12 port=5432 role=rw priority=2 enabled=1  
  
    pg3 = host=10.0.2.11 port=5432 role=ro weight=1 enabled=1  
    pg4 = host=10.0.2.12 port=5432 role=ro weight=1 enabled=1  
    pg5 = host=10.0.2.13 port=5432 role=ro weight=1 enabled=1  

3. database routing 설정

    [databases]  
    app_rw = dbname=appdb server_group=appdb_rw route=read-write role_check=read-write rw_fallback=connect_only pool_mode=transaction  
    app_ro = dbname=appdb server_group=appdb_ro route=read-only role_check=standby lb_policy=round-robin pool_mode=transaction  

동작 방식
---------

Read 요청 (app_ro):

 * Replica 대상 Round-Robin 분산
 * 장애 서버 자동 제외
 * Standby 여부 검증 후 사용

Write 요청 (app_rw):

 * Priority 기반 서버 선택 (1 → 2 → ...)
 * Primary 여부 검증 후 사용
 * 연결 실패 시 다음 후보로 fallback

Write Fallback 정책 (중요)
-------------------------

Write fallback은 아래 경우에만 수행됩니다.

 * 서버 connect 실패
 * 로그인 실패
 * role check 실패 (standby 연결된 경우)

아래 경우에는 fallback 수행하지 않습니다.

 * 쿼리가 이미 서버로 전송된 경우
 * 트랜잭션이 시작된 경우
 * commit 결과가 불확실한 경우

이유:

 * 중복 INSERT/UPDATE 방지
 * 트랜잭션 정합성 보장
 * 데이터 손상 방지

운영 시나리오
-------------

1. Primary 장애 발생

    pg1 DOWN  
    pg2 승격 (Primary)  

    → PgBouncer가 자동으로 pg2를 선택하는 조건:

       - pg2가 appdb_rw 그룹에 등록되어 있어야 함
       - role_check 결과 pg2가 read-write 상태여야 함

    동작 흐름:

       1. pg1 연결 실패
       2. pg2 연결 시도
       3. role check 수행
       4. pg2가 primary이면 사용

    즉, "자동 연결"은 맞지만
    → 미리 RW 그룹에 후보로 등록되어 있어야 가능

    신규 서버가 자동으로 추가되는 구조는 아님

2. Replica 일부 장애

    pg3 DOWN

    → pg4, pg5로 자동 분산

3. Replica 전체 장애

    → Read 요청 실패 (Primary fallback 없음 - 권장 설정)

애플리케이션 설정 가이드
------------------------

핵심:

 * Write / Read datasource를 반드시 분리해야 함
 * PgBouncer는 SQL을 보고 판단하지 않음

1. Python (psycopg2)

    WRITE:
        conn = psycopg2.connect(
            host="pgbouncer-host",
            port=6432,
            dbname="app_rw",
            user="user",
            password="password"
        )

    READ:
        conn = psycopg2.connect(
            host="pgbouncer-host",
            port=6432,
            dbname="app_ro",
            user="user",
            password="password"
        )

2. Java (HikariCP)

    WRITE:
        jdbc:postgresql://pgbouncer-host:6432/app_rw

    READ:
        jdbc:postgresql://pgbouncer-host:6432/app_ro

3. C (libpq)

    WRITE:
        conninfo = "host=pgbouncer-host port=6432 dbname=app_rw user=xxx password=xxx";

    READ:
        conninfo = "host=pgbouncer-host port=6432 dbname=app_ro user=xxx password=xxx";

중요:

 * app_ro에 INSERT 보내면 에러 발생
 * app_rw에 SELECT 보내면 Primary에서 실행됨

운영 명령어
-----------

Routing 상태 조회:

    SHOW SERVER_TARGETS;
    SHOW SERVER_GROUPS;
    SHOW ROUTE_STATS;

서버 제어:

    EXCLUDE TARGET appdb_rw pg1;
    INCLUDE TARGET appdb_rw pg1;

    DRAIN TARGET appdb_ro pg3;

설명:

 * EXCLUDE → 신규 연결 제외
 * INCLUDE → 다시 포함
 * DRAIN → 기존 연결 종료까지 대기 후 제거

권장 설정
---------

 * pool_mode = transaction
 * rw_fallback = connect_only
 * ro_fallback_to_primary = 0 (권장)

주의할 점
---------

 * PgBouncer는 SQL을 분석하지 않음
 * 반드시 애플리케이션에서 Read/Write 분리 필요
 * Write fallback은 자동 재시도가 아님 (Application retry 필요)

한계
----

 * Multi-primary 구조 지원 안 함
 * Query 기반 routing 미지원
 * Transaction 중 failover 지원 안 함

결론
----

본 구조는 다음 목적에 적합합니다.

 * PostgreSQL Primary + Replica 구조
 * Read 부하 분산
 * Primary 장애 시 빠른 연결 전환
 * 최소한의 PgBouncer 변경으로 안정성 확보
