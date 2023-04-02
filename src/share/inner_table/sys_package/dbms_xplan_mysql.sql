#package_name:dbms_xplan
#author:zhenling.zzg

CREATE OR REPLACE PACKAGE dbms_xplan  AUTHID CURRENT_USER

    --  CONSTANTS
    DECLARE DEFAULT_INENTIFIER      VARCHAR(20)    DEFAULT '';
    DECLARE DEFAULT_LEVEL           DECIMAL        DEFAULT 1;

    --  TYPES

    -- FUNCTIONS and PROCEDURES
    PROCEDURE enable_opt_trace(
        sql_id          VARCHAR(32)  DEFAULT '',
        identifier      VARCHAR(20)  DEFAULT DEFAULT_INENTIFIER,
        level           DECIMAL      DEFAULT DEFAULT_LEVEL
    );

    PROCEDURE disable_opt_trace();

    PROCEDURE set_opt_trace_parameter(
        sql_id          VARCHAR(32)  DEFAULT '',
        identifier      VARCHAR(20)  DEFAULT DEFAULT_INENTIFIER,
        level           DECIMAL      DEFAULT DEFAULT_LEVEL
    );

    -- display plan table`s plan
    -- function display(table_name   VARCHAR(32) default 'PLAN_TABLE',
    --                  statement_id VARCHAR(32) default null,
    --                  format       VARCHAR(32) default 'TYPICAL', -- 'BASIC', 'TYPICAL', 'ALL', 'ADVANCED'
    --                  filter_preds VARCHAR(255) default null)
    -- return text;

    -- display sql plan table`s plan
    -- function display_cursor(sql_id		 VARCHAR(32) default null,
    --                         plan_id      DECIMAL default 0,
    --                         format		 VARCHAR(32) default 'TYPICAL')
    -- return text;

    -- display base line plan
    -- function display_sql_plan_baseline(
    --            sql_handle   VARCHAR(32)  default  NULL,
    --            plan_name    VARCHAR(32)  default  NULL,
    --            format       VARCHAR(32)  default  'TYPICAL')
    -- return text;

END dbms_xplan;