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

  -- ------------------------------------------------------------------------
  -- purge_log

  PROCEDURE do_purge_log(
    master                IN     VARCHAR2,
    num                   IN     BINARY_INTEGER := 1,
    flag                  IN     VARCHAR2       := 'NOP',
    purge_log_parallel    IN     BINARY_INTEGER := 1);
  PRAGMA INTERFACE(C, DBMS_MVIEW_PURGE_LOG);

  PROCEDURE purge_log(
    master                IN     VARCHAR2,
    num                   IN     BINARY_INTEGER := 1,
    flag                  IN     VARCHAR2       := 'NOP',
    purge_log_parallel    IN     BINARY_INTEGER := 1)
  IS
  BEGIN
    COMMIT;
    do_purge_log(master, num, flag, purge_log_parallel);
  END;

  -- ------------------------------------------------------------------------
  -- refresh

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
    refresh_parallel       IN     BINARY_INTEGER := 1);
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
    refresh_parallel       IN     BINARY_INTEGER := 1)
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
               refresh_parallel);
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
    refresh_parallel       IN     BINARY_INTEGER := 1)
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
               refresh_parallel);
  END;

END dbms_mview;
//
