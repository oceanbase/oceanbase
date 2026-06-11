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
    nested_refresh_mode    IN     VARCHAR2       := NULL);

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
    nested_refresh_mode    IN     VARCHAR2       := NULL);

  PROCEDURE set_refresh_params(
    mv_name        IN  VARCHAR2,
    parameter_name    IN  VARCHAR2,
    parameter_value   IN  VARCHAR2
  );

  FUNCTION explain_refresh(
    mv_name                IN     VARCHAR2,
    method                 IN     VARCHAR2       := NULL,
    nested                 IN     BOOLEAN        := false,
    tenant_id              IN     BINARY_INTEGER := 0
  ) RETURN CLOB;

END dbms_mview;
//
