#package_name:dbms_workload_repository
#author: jiajingzhe.jjz

-- only support SYS tenant
CREATE OR REPLACE PACKAGE dbms_workload_repository AUTHID CURRENT_USER

  PROCEDURE CREATE_SNAPSHOT(flush_level VARCHAR(64) DEFAULT 'TYPICAL');

  PROCEDURE DROP_SNAPSHOT_RANGE(
    low_snap_id    INT,
    high_snap_id   INT);

  PROCEDURE MODIFY_SNAPSHOT_SETTINGS(
    retention        INT    DEFAULT  NULL,
    interval         INT    DEFAULT  NULL,
    topnsql          INT    DEFAULT  NULL,
    sqlstat_interval INT    DEFAULT  NULL);

  FUNCTION ASH_REPORT_TEXT(BTIME         TIMESTAMP,
                           ETIME         TIMESTAMP,
                           SQL_ID        VARCHAR(64)  DEFAULT NULL,
                           TRACE_ID      VARCHAR(64)  DEFAULT NULL,
                           WAIT_CLASS    VARCHAR(64)  DEFAULT NULL,
                           SVR_IP        VARCHAR(64)  DEFAULT NULL,
                           SVR_PORT      INT          DEFAULT NULL,
                           TENANT_ID     INT          DEFAULT NULL,
                           REPORT_TYPE   VARCHAR(64)  DEFAULT 'text'
  )RETURN TEXT ;

  PROCEDURE ASH_REPORT(BTIME         TIMESTAMP,
                       ETIME         TIMESTAMP,
                       SQL_ID        VARCHAR(64)  DEFAULT NULL,
                       TRACE_ID      VARCHAR(64)  DEFAULT NULL,
                       WAIT_CLASS    VARCHAR(64)  DEFAULT NULL,
                       REPORT_TYPE   VARCHAR(64)  DEFAULT 'text',
                       SVR_IP        VARCHAR(64)  DEFAULT NULL,
                       SVR_PORT      INT          DEFAULT NULL,
                       TENANT_ID     INT          DEFAULT NULL);

END dbms_workload_repository;
