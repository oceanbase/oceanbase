CREATE OR REPLACE PACKAGE BODY dbms_mview IS

  PROCEDURE tab_to_comma(
    tab                   IN     DBMS_UTILITY.UNCL_ARRAY,
    list                  OUT    VARCHAR2)
  IS
  tab_idx     BINARY_INTEGER;
  tab_val     VARCHAR2(128);
  BEGIN
    IF tab IS NULL OR tab.count = 0 THEN
      DBMS_OUTPUT.PUT_LINE('tab empty');
      RETURN;
    END IF;

    tab_idx := tab.FIRST;
    WHILE tab_idx IS NOT NULL LOOP
      tab_val := tab(tab_idx);
      IF tab_idx = tab.FIRST THEN
        list := tab_val;
      ELSE
        list := list || ',' || tab_val;
      END IF;
      tab_idx := tab.next(tab_idx);
    END LOOP;
  END;

  PROCEDURE do_purge_log(
    master                IN     VARCHAR2,
    num                   IN     BINARY_INTEGER := 1,
    flag                  IN     VARCHAR2       := 'NOP',
    purge_log_parallel    IN     BINARY_INTEGER := 0);
  PRAGMA INTERFACE(C, DBMS_MVIEW_PURGE_LOG);

  PROCEDURE purge_log(
    master                IN     VARCHAR2,
    num                   IN     BINARY_INTEGER := 1,
    flag                  IN     VARCHAR2       := 'NOP',
    purge_log_parallel    IN     BINARY_INTEGER := 0)
  IS
  BEGIN
    COMMIT;
    do_purge_log(master, num, flag, purge_log_parallel);
  END;

  PROCEDURE do_refresh(
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
    -- TEMPORARY v2 (task 0036): async param, see dbms_mview.sql
    async                  IN     BOOLEAN        := FALSE,
    force                  IN     BOOLEAN        := FALSE);
  PRAGMA INTERFACE(C, DBMS_MVIEW_REFRESH);

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
    -- TEMPORARY v2 (task 0036): async param, see dbms_mview.sql
    async                  IN     BOOLEAN        := FALSE,
    force                  IN     BOOLEAN        := FALSE)
  IS
  BEGIN
    COMMIT;
    do_refresh(list,
               method,
               rollback_seg,
               push_deferred_rpc,
               refresh_after_errors,
               purge_option,
               parallelism,
               heap_size,
               atomic_refresh,
               nested,
               out_of_place,
               skip_ext_data,
               refresh_parallel,
               nested_refresh_mode,
               async,
               force);
  END;

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
    -- TEMPORARY v2 (task 0036): async param, see dbms_mview.sql
    async                  IN     BOOLEAN        := FALSE,
    force                  IN     BOOLEAN        := FALSE)
  IS
  list                VARCHAR2(4000);
  BEGIN
    COMMIT;
    tab_to_comma(tab, list);
    do_refresh(list,
               method,
               rollback_seg,
               push_deferred_rpc,
               refresh_after_errors,
               purge_option,
               parallelism,
               heap_size,
               atomic_refresh,
               nested,
               out_of_place,
               skip_ext_data,
               refresh_parallel,
               nested_refresh_mode,
               async,
               force);
  END;

  FUNCTION refresh_report(
    refresh_id          IN     NUMBER        DEFAULT NULL,
    mv_name             IN     VARCHAR2      DEFAULT NULL,
    tenant_id           IN     NUMBER        DEFAULT NULL,
    format              IN     VARCHAR2      DEFAULT 'TEXT')
  RETURN CLOB;
  PRAGMA INTERFACE(C, DBMS_MVIEW_REFRESH_REPORT);

  PROCEDURE do_kill_refresh(refresh_id IN BINARY_INTEGER);
  PRAGMA INTERFACE(C, DBMS_MVIEW_KILL_REFRESH);

  PROCEDURE kill_refresh(refresh_id IN BINARY_INTEGER)
  IS
  BEGIN
    COMMIT;
    do_kill_refresh(refresh_id);
  END;

  PROCEDURE set_refresh_params(
    mv_name           IN  VARCHAR2,
    parameter_name    IN  VARCHAR2,
    parameter_value   IN  VARCHAR2);
  PRAGMA INTERFACE(C, DBMS_MVIEW_SET_REFRESH_PARAMS);

END dbms_mview;
//
