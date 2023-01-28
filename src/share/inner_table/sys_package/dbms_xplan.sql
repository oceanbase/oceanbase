#package_name:dbms_xplan
#author:zhenling.zzg

CREATE OR REPLACE PACKAGE dbms_xplan AUTHID CURRENT_USER AS

    --  CONSTANTS
    DEFAULT_INENTIFIER  constant VARCHAR2(20) := '';
    DEFAULT_LEVEL       constant INT := 1;

    --  TYPES
    type dbms_xplan_type_table is table of varchar2(4000);

    -- FUNCTIONS and PROCEDURES
    PROCEDURE enable_opt_trace(
        sql_id          IN VARCHAR2 DEFAULT '',
        identifier      IN VARCHAR2 DEFAULT DEFAULT_INENTIFIER,
        level           IN INT DEFAULT DEFAULT_LEVEL
    );

    PROCEDURE disable_opt_trace;

    PROCEDURE set_opt_trace_parameter(
        sql_id          IN VARCHAR2 DEFAULT '',
        identifier      IN VARCHAR2 DEFAULT DEFAULT_INENTIFIER,
        level           IN INT DEFAULT DEFAULT_LEVEL
    );

    -- display plan table`s plan
    function display(table_name   varchar2 default 'PLAN_TABLE',
                     statement_id varchar2 default null,
                     format       varchar2 default 'TYPICAL',-- 'BASIC', 'TYPICAL', 'ALL', 'ADVANCED'
                     filter_preds varchar2 default null)
    return dbms_xplan_type_table;

    -- display sql plan table`s plan
    function display_cursor(sql_id		 varchar2 default null,
                            plan_id      integer default 0,
                            format		 varchar2 default 'TYPICAL')
    return dbms_xplan_type_table;

    -- display base line plan
    function display_sql_plan_baseline(
               sql_handle   varchar2  default  NULL,
               plan_name    varchar2  default  NULL,
               format       varchar2  default  'TYPICAL')
    return dbms_xplan_type_table;

END dbms_xplan;