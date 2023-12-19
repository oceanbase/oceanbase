CREATE OR REPLACE PACKAGE BODY dbms_mview

  -- ------------------------------------------------------------------------
  -- purge_log

  PROCEDURE do_purge_log(
    IN     master_name            VARCHAR(65535));
  PRAGMA INTERFACE(C, DBMS_MVIEW_MYSQL_PURGE_LOG);

  PROCEDURE purge_log(
    IN     master_name            VARCHAR(65535))
  BEGIN
    COMMIT;
    CALL do_purge_log(master_name);
  END;

  -- ------------------------------------------------------------------------
  -- refresh

  PROCEDURE do_refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     parallelism            INT            DEFAULT 1);
  PRAGMA INTERFACE(C, DBMS_MVIEW_MYSQL_REFRESH);

  PROCEDURE refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     parallelism            INT            DEFAULT 1)
  BEGIN
    COMMIT;
    CALL do_refresh(mv_name, method, parallelism);
  END;

END dbms_mview;
