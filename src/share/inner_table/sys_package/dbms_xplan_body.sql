# package_name : dbms_xplan
# author : zhenling.zzg

CREATE OR REPLACE PACKAGE BODY dbms_xplan  AS

    PROCEDURE enable_opt_trace(
        sql_id          IN VARCHAR2 DEFAULT '',
        identifier      IN VARCHAR2 DEFAULT DEFAULT_INENTIFIER,
        level           IN INT DEFAULT DEFAULT_LEVEL
    );
    PRAGMA INTERFACE(C, ENABLE_OPT_TRACE);

    PROCEDURE disable_opt_trace;
    PRAGMA INTERFACE(C, DISABLE_OPT_TRACE);

    PROCEDURE set_opt_trace_parameter(
        sql_id          IN VARCHAR2 DEFAULT '',
        identifier      IN VARCHAR2 DEFAULT DEFAULT_INENTIFIER,
        level           IN INT DEFAULT DEFAULT_LEVEL
    );
    PRAGMA INTERFACE(C, SET_OPT_TRACE_PARAMETER);

    -- display plan table`s plan
    function display(table_name   varchar2 default 'PLAN_TABLE',
                     statement_id varchar2 default null,
                     format       varchar2 default 'TYPICAL',-- 'BASIC', 'TYPICAL', 'ALL'
                     filter_preds varchar2 default null)
    return dbms_xplan_type_table;
    PRAGMA INTERFACE(C, DISPLAY);

    -- display sql plan table`s plan
    function display_cursor(sql_id		 varchar2 default null,
                            plan_id      integer default 0,
                            format		 varchar2 default 'TYPICAL')
    return dbms_xplan_type_table;
    PRAGMA INTERFACE(C, DISPLAY_CURSOR);

    -- display base line plan
    function display_sql_plan_baseline(
               sql_handle   varchar2  default  NULL,
               plan_name    varchar2  default  NULL,
               format       varchar2  default  'TYPICAL')
    return dbms_xplan_type_table;
    PRAGMA INTERFACE(C, DISPLAY_SQL_PLAN_BASELINE);

END dbms_xplan;