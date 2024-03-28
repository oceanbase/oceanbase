#package_name: dbms_workload_repository
#author: jiajingzhe.jjz

CREATE OR REPLACE PACKAGE BODY dbms_workload_repository

  PROCEDURE CREATE_SNAPSHOT (
    flush_level            VARCHAR(64) DEFAULT 'TYPICAL'
  );
  PRAGMA INTERFACE(c, WR_CREATE_SNAPSHOT);

  PROCEDURE DROP_SNAPSHOT_RANGE(
    low_snap_id    INT,
    high_snap_id   INT);
  PRAGMA INTERFACE(C, WR_DROP_SNAPSHOT_RANGE);

  PROCEDURE MODIFY_SNAPSHOT_SETTINGS(
    retention     INT    DEFAULT  NULL,
    interval      INT    DEFAULT  NULL,
    topnsql       INT    DEFAULT  NULL);
  PRAGMA INTERFACE(C, WR_MODIFY_SNAPSHOT_SETTINGS);

  FUNCTION ASH_REPORT_TEXT(BTIME         DATETIME,
                           ETIME         DATETIME,
                           SQL_ID        VARCHAR(64)  DEFAULT NULL,
                           TRACE_ID      VARCHAR(64)  DEFAULT NULL,
                           WAIT_CLASS    VARCHAR(64)  DEFAULT NULL,
                           SVR_IP        VARCHAR(64)  DEFAULT NULL,
                           SVR_PORT      INT          DEFAULT NULL
  )RETURN TEXT;
  PRAGMA INTERFACE(C, GENERATE_ASH_REPORT_TEXT);

  PROCEDURE ASH_REPORT(
    BTIME         DATETIME,
    ETIME         DATETIME,
    SQL_ID        VARCHAR(64)  DEFAULT NULL,
    TRACE_ID      VARCHAR(64)  DEFAULT NULL,
    WAIT_CLASS    VARCHAR(64)  DEFAULT NULL,
    REPORT_TYPE   VARCHAR(64)  DEFAULT 'text',
    SVR_IP        VARCHAR(64)  DEFAULT NULL,
    SVR_PORT      INT          DEFAULT NULL)
  BEGIN
    IF LOWER(REPORT_TYPE) = 'text' THEN
        SELECT DBMS_WORKLOAD_REPOSITORY.ASH_REPORT_TEXT(BTIME, ETIME,SQL_ID,TRACE_ID,WAIT_CLASS, SVR_IP, SVR_PORT) AS REPORT ;
    ELSE
        SELECT "Other formats are not currently supported besides TEXT" AS Message;
    END IF;
  END ;

END dbms_workload_repository;
