# package_name : dbms_xplan
# author : zhenling.zzg

CREATE OR REPLACE PACKAGE BODY dbms_xplan

    PROCEDURE enable_opt_trace(
        sql_id          VARCHAR(32)  DEFAULT '',
        identifier      VARCHAR(20)  DEFAULT DEFAULT_INENTIFIER,
        level           DECIMAL      DEFAULT DEFAULT_LEVEL
    );
    PRAGMA INTERFACE(C, ENABLE_OPT_TRACE);

    PROCEDURE disable_opt_trace();
    PRAGMA INTERFACE(C, DISABLE_OPT_TRACE);

    PROCEDURE set_opt_trace_parameter(
        sql_id          VARCHAR(32)  DEFAULT '',
        identifier      VARCHAR(20)  DEFAULT DEFAULT_INENTIFIER,
        level           DECIMAL      DEFAULT DEFAULT_LEVEL
    );
    PRAGMA INTERFACE(C, SET_OPT_TRACE_PARAMETER);

    function display(format       VARCHAR(32) default 'TYPICAL',
                     statement_id VARCHAR(32) default null,
                     table_name   VARCHAR(32) default 'PLAN_TABLE',
                     filter_preds VARCHAR(255) default null)
    return text;
    PRAGMA INTERFACE(C, DISPLAY);

    function display_cursor(plan_id      DECIMAL default 0,
                            format		 VARCHAR(32) default 'TYPICAL',
                            svr_ip       VARCHAR(64) default null,
                            svr_port     DECIMAL default 0,
                            tenant_id	 DECIMAL default 0
                        )
    return text;
    PRAGMA INTERFACE(C, DISPLAY_CURSOR);

    function display_sql_plan_baseline(sql_handle   VARCHAR(32)  default  NULL,
                                        plan_name    VARCHAR(32)  default  NULL,
                                        format       VARCHAR(32)  default  'TYPICAL',
                                        svr_ip       VARCHAR(64) default null,
                                        svr_port     DECIMAL default 0,
                                        tenant_id	 DECIMAL default 0
                                    )
    return text;
    PRAGMA INTERFACE(C, DISPLAY_SQL_PLAN_BASELINE);

    function display_active_session_plan(
                session_id   DECIMAL default 0,
                format       VARCHAR(32)  default  'TYPICAL',
                svr_ip       VARCHAR(64) default null,
                svr_port     DECIMAL default 0
                )
    return text;
    PRAGMA INTERFACE(C, DISPLAY_ACTIVE_SESSION_PLAN);

END dbms_xplan;
