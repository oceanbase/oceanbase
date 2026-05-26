CREATE OR REPLACE PACKAGE BODY dbms_mview

  PROCEDURE do_purge_log(
    IN     master_name            VARCHAR(65535),
    IN     purge_log_parallel     INT            DEFAULT 0);
  PRAGMA INTERFACE(C, DBMS_MVIEW_MYSQL_PURGE_LOG);

  PROCEDURE purge_log(
    IN     master_name            VARCHAR(65535),
    IN     purge_log_parallel     INT            DEFAULT 0)
  BEGIN
    COMMIT;
    CALL do_purge_log(master_name, purge_log_parallel);
  END;

  PROCEDURE do_refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 0,
    IN     nested                 BOOLEAN        DEFAULT FALSE,
    IN     nested_refresh_mode    VARCHAR(65535) DEFAULT NULL,
    -- TEMPORARY v2 (task 0036): async param, see dbms_mview_mysql.sql
    IN     async                  BOOLEAN        DEFAULT FALSE);
  PRAGMA INTERFACE(C, DBMS_MVIEW_MYSQL_REFRESH);

  PROCEDURE refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 0,
    IN     nested                 BOOLEAN        DEFAULT FALSE,
    IN     nested_refresh_mode    VARCHAR(65535) DEFAULT NULL,
    -- TEMPORARY v2 (task 0036): async param, see dbms_mview_mysql.sql
    IN     async                  BOOLEAN        DEFAULT FALSE)
  BEGIN
    DECLARE EXIT HANDLER for SQLWARNING
    BEGIN
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT = 'mview refresh failed with sql warning exception';
    END;
    COMMIT;
    SET @ob_dbmsmview_cno=null;
    SET @ob_dbmsmview_errno=null;
    SET @ob_dbmsmview_p4=null;
    CALL do_refresh(mv_name, method, refresh_parallel, nested, nested_refresh_mode, async);
  END;

  FUNCTION refresh_report(
    IN     refresh_id             INT            DEFAULT NULL,
    IN     mv_name                VARCHAR(65535) DEFAULT NULL,
    IN     tenant_id              INT            DEFAULT NULL,
    IN     format                 VARCHAR(65535) DEFAULT 'TEXT')
  RETURN TEXT;
  PRAGMA INTERFACE(C, DBMS_MVIEW_MYSQL_REFRESH_REPORT);

  PROCEDURE do_kill_refresh(
    IN     refresh_id              BIGINT);
  PRAGMA INTERFACE(C, DBMS_MVIEW_MYSQL_KILL_REFRESH);

  PROCEDURE kill_refresh(
    IN     refresh_id              BIGINT)
  BEGIN
    COMMIT;
    CALL do_kill_refresh(refresh_id);
  END;

  PROCEDURE set_refresh_params(
    IN     mv_name                VARCHAR(65535),
    IN     parameter_name         VARCHAR(65535),
    IN     parameter_value        VARCHAR(65535));
  PRAGMA INTERFACE(C, DBMS_MVIEW_MYSQL_SET_REFRESH_PARAMS);

END dbms_mview;
