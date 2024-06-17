CREATE OR REPLACE PACKAGE BODY dbms_mview

  -- ------------------------------------------------------------------------
  -- purge_log

  PROCEDURE do_purge_log(
    IN     master_name            VARCHAR(65535),
    IN     purge_log_parallel     INT            DEFAULT 1);
  PRAGMA INTERFACE(C, DBMS_MVIEW_MYSQL_PURGE_LOG);

  PROCEDURE purge_log(
    IN     master_name            VARCHAR(65535),
    IN     purge_log_parallel     INT            DEFAULT 1)
  BEGIN
    COMMIT;
    CALL do_purge_log(master_name, purge_log_parallel);
  END;

  -- ------------------------------------------------------------------------
  -- refresh

  PROCEDURE do_refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 1);
  PRAGMA INTERFACE(C, DBMS_MVIEW_MYSQL_REFRESH);

  PROCEDURE refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 1)
  BEGIN
    COMMIT;
    CALL do_refresh(mv_name, method, refresh_parallel);
  END;

END dbms_mview;
