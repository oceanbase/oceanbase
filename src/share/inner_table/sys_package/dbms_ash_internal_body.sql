-- package_name:dbms_ash_internal
-- author:xiaochu.yh

CREATE OR REPLACE PACKAGE BODY dbms_ash_internal AS

  FUNCTION  IN_MEMORY_ASH_VIEW_SQL
    RETURN  VARCHAR2
  IS
    RETVAL    VARCHAR2(30000);
  BEGIN
    RETVAL := 'SELECT a.sample_id, a.sample_time, ' ||
              '       a.svr_ip, ' ||
              '       a.svr_port, ' ||
              '       a.con_id, ' ||
              '       a.user_id, ' ||
              '       a.session_id, '   ||
              '       a.session_type, ' ||
              '       a.session_state, ' ||
              '       a.sql_id, ' ||
              '       plan_id, ' ||
              '       a.trace_id, ' ||
              '       nvl(a.event, ''CPU + Wait for CPU'') as event, ' ||
              '       nvl(a.event_no, 1) as event_no, ' ||
              '       a.p1, a.p1text, ' ||
              '       a.p2, a.p2text, ' ||
              '       a.p3, a.p3text, ' ||
              '       nvl(a.wait_class, ''CPU'') as wait_class, ' ||
              '       nvl(a.wait_class_id, 9999) as wait_class_id, ' ||
              '       a.time_waited, ' ||
              '       a.sql_plan_line_id, ' ||
              '       a.in_parse, ' ||
              '       a.in_pl_parse, ' ||
              '       a.in_plan_cache, ' ||
              '       a.in_sql_optimize, ' ||
              '       a.in_sql_execution, ' ||
              '       a.in_px_execution, ' ||
              '       a.in_sequence_load, ' ||
              '       a.in_committing, ' ||
              '       a.in_storage_read, ' ||
              '       a.in_storage_write, ' ||
              '       a.in_remote_das_execution, ' ||
              '       a.module, a.action, a.client_id ' ||
              'FROM GV$ACTIVE_SESSION_HISTORY a ' ||
              'WHERE  1=1 ';
    RETURN RETVAL;
  END IN_MEMORY_ASH_VIEW_SQL;

  FUNCTION  ASH_VIEW_SQL
    RETURN  VARCHAR2
  IS
    RETVAL  VARCHAR2(30000);
  BEGIN
    RETVAL := 'SELECT * FROM    (' || IN_MEMORY_ASH_VIEW_SQL ||
              '            and  a.sample_time between :ash_mem_btime and :ash_mem_etime ' ||
              '         ) unified_ash ' ||
              'WHERE  sample_time between :ash_begin_time ' ||
              '                        and :ash_end_time ' ||
              '  AND   (:ash_sql_id IS NULL ' ||
              '         OR sql_id like :ash_sql_id) ' ||
              '  AND   (:ash_trace_id IS NULL ' ||
              '         OR trace_id like :ash_trace_id) ' ||
              '  AND   (:ash_wait_class IS NULL ' ||
              '         OR wait_class like :ash_wait_class) ' ||
              '  AND   (:ash_module IS NULL ' ||
              '         OR module like :ash_module) ' ||
              '  AND   (:ash_action IS NULL ' ||
              '         OR action like :ash_action) ' ||
              '  AND   (:ash_client_id IS NULL ' ||
              '         OR client_id like :ash_client_id)';
       RETURN RETVAL;
  END ASH_VIEW_SQL;

END dbms_ash_internal;
