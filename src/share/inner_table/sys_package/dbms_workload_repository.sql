#package_name:dbms_workload_repository
#author:xiaochu.yh

CREATE OR REPLACE PACKAGE dbms_workload_repository AUTHID CURRENT_USER AS

  PROCEDURE ASH_REPORT(BTIME         IN TIMESTAMP,
                       ETIME         IN TIMESTAMP,
                       SQL_ID        IN VARCHAR2  DEFAULT NULL,
                       TRACE_ID      IN VARCHAR2  DEFAULT NULL,
                       WAIT_CLASS    IN VARCHAR2  DEFAULT NULL,
                       REPORT_TYPE   IN VARCHAR2  DEFAULT 'text',
                       SVR_IP        IN VARCHAR2  DEFAULT NULL,
                       SVR_PORT      IN NUMBER    DEFAULT NULL,
                       TENANT_ID     IN NUMBER    DEFAULT NULL
                     );

  FUNCTION ASH_REPORT_TEXT(L_BTIME     IN TIMESTAMP,
                           L_ETIME       IN TIMESTAMP,
                           SQL_ID        IN VARCHAR2  DEFAULT NULL,
                           TRACE_ID      IN VARCHAR2  DEFAULT NULL,
                           WAIT_CLASS    IN VARCHAR2  DEFAULT NULL,
                           SVR_IP        IN VARCHAR2  DEFAULT NULL,
                           SVR_PORT      IN NUMBER    DEFAULT NULL,
                           TENANT_ID     IN NUMBER    DEFAULT NULL,
                           REPORT_TYPE   IN VARCHAR2  DEFAULT 'text'
                          )
  RETURN CLOB;

END dbms_workload_repository;
