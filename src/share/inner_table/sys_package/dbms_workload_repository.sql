-- package_name:dbms_workload_repository
-- author:xiaochu.yh


--
-- 完整 PL 包参考文档：https://docs.oracle.com/database/121/ARPLS/d_workload_repos.htm#ARPLS093
--
CREATE OR REPLACE PACKAGE dbms_workload_repository AUTHID CURRENT_USER AS

  -- Type declare
  -- SUBTYPE OUTPUT_TYPE IS VARCHAR2(4000 CHAR);
  TYPE awrrpt_text_type_table IS TABLE OF VARCHAR2(4096 CHAR) INDEX BY BINARY_INTEGER;
  RPT_ROWS awrrpt_text_type_table;


  TYPE SEC_REC IS RECORD(
    TITLE      VARCHAR2(4000 CHAR),
    TITLE_TYPE VARCHAR2(1 CHAR)
  );

  TYPE SEC_REC_TAB IS TABLE OF SEC_REC
    INDEX BY BINARY_INTEGER;

  TEST_ROW SEC_REC_TAB;

  FUNCTION ash_report_text(l_btime         IN DATE,
                           l_etime         IN DATE,
                           l_sql_id        IN VARCHAR2  DEFAULT NULL,
                           l_wait_class    IN VARCHAR2  DEFAULT NULL
                          )
  RETURN awrrpt_text_type_table;

END dbms_workload_repository;
