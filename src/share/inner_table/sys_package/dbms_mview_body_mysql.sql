CREATE OR REPLACE PACKAGE BODY dbms_mview

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

  PROCEDURE do_refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 0,
    IN     nested                 BOOLEAN        DEFAULT FALSE,
    IN     nested_refresh_mode    VARCHAR(65535) DEFAULT NULL);
  PRAGMA INTERFACE(C, DBMS_MVIEW_MYSQL_REFRESH);

  PROCEDURE refresh(
    IN     mv_name                VARCHAR(65535),
    IN     method                 VARCHAR(65535) DEFAULT NULL,
    IN     refresh_parallel       INT            DEFAULT 0,
    IN     nested                 BOOLEAN        DEFAULT FALSE,
    IN     nested_refresh_mode    VARCHAR(65535) DEFAULT NULL)
  BEGIN
    COMMIT;
    CALL do_refresh(mv_name, method, refresh_parallel, nested, nested_refresh_mode);
    GET DIAGNOSTICS @ob_dbmsmview_cno = NUMBER;
    IF @ob_dbmsmview_cno >= 1 THEN
      GET DIAGNOSTICS CONDITION 1 @ob_dbmsmview_errno = MYSQL_ERRNO, @ob_dbmsmview_p4 = MESSAGE_TEXT;
      SIGNAL SQLSTATE 'HY000' SET MESSAGE_TEXT = @ob_dbmsmview_p4, MYSQL_ERRNO = @ob_dbmsmview_errno;
    END IF;
  END;

END dbms_mview;
