CREATE OR REPLACE PACKAGE dbms_mview AUTHID CURRENT_USER IS

  PROCEDURE purge_log(
    master              IN   VARCHAR2,
    num                 IN   BINARY_INTEGER := 1,
    flag                IN   VARCHAR2       := 'NOP',
    purge_log_parallel  IN   BINARY_INTEGER := 0);

  PROCEDURE refresh(
    list                   IN     VARCHAR2,
    method                 IN     VARCHAR2       := NULL,
    rollback_seg           IN     VARCHAR2       := NULL,
    push_deferred_rpc      IN     BOOLEAN        := true,
    refresh_after_errors   IN     BOOLEAN        := false,
    purge_option           IN     BINARY_INTEGER := 1,
    parallelism            IN     BINARY_INTEGER := 0,
    heap_size              IN     BINARY_INTEGER := 0,
    atomic_refresh         IN     BOOLEAN        := true,
    nested                 IN     BOOLEAN        := false,
    out_of_place           IN     BOOLEAN        := false,
    skip_ext_data          IN     BOOLEAN        := false,
    refresh_parallel       IN     BINARY_INTEGER := 0,
    nested_refresh_mode    IN     VARCHAR2       := NULL,
    -- TEMPORARY v2 (task 0036): async=FALSE blocks until the batch completes;
    -- TRUE returns immediately after enqueue (prior 4.6.0 async behaviour).
    async                  IN     BOOLEAN        := FALSE);

  PROCEDURE refresh(
    tab                    IN     DBMS_UTILITY.UNCL_ARRAY,
    method                 IN     VARCHAR2       := NULL,
    rollback_seg           IN     VARCHAR2       := NULL,
    push_deferred_rpc      IN     BOOLEAN        := true,
    refresh_after_errors   IN     BOOLEAN        := false,
    purge_option           IN     BINARY_INTEGER := 1,
    parallelism            IN     BINARY_INTEGER := 0,
    heap_size              IN     BINARY_INTEGER := 0,
    atomic_refresh         IN     BOOLEAN        := true,
    nested                 IN     BOOLEAN        := false,
    out_of_place           IN     BOOLEAN        := false,
    skip_ext_data          IN     BOOLEAN        := false,
    refresh_parallel       IN     BINARY_INTEGER := 0,
    nested_refresh_mode    IN     VARCHAR2       := NULL,
    -- TEMPORARY v2 (task 0036): async=FALSE blocks until the batch completes;
    -- TRUE returns immediately after enqueue (prior 4.6.0 async behaviour).
    async                  IN     BOOLEAN        := FALSE);

  FUNCTION refresh_report(
    refresh_id          IN     NUMBER        DEFAULT NULL,
    mv_name             IN     VARCHAR2      DEFAULT NULL,
    tenant_id           IN     NUMBER        DEFAULT NULL,
    format              IN     VARCHAR2      DEFAULT 'TEXT')
  RETURN CLOB;

  PROCEDURE kill_refresh(
    refresh_id              IN   BINARY_INTEGER);

  PROCEDURE set_refresh_params(
    mv_name        IN  VARCHAR2,
    parameter_name    IN  VARCHAR2,
    parameter_value   IN  VARCHAR2
  );

END dbms_mview;
//
