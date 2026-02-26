#package_name:dbms_ash_internal
#author:xiaochu.yh

CREATE OR REPLACE PACKAGE BODY dbms_ash_internal AS
  FUNCTION  IN_MEMORY_ASH_VIEW_SQL RETURN  VARCHAR2 IS BEGIN
    RETURN 'SELECT sample_id, '                                   ||
           '       sample_time, '                                 ||
           '       svr_ip, '                                      ||
           '       svr_port, '                                    ||
           '       con_id, '                                      ||
           '       user_id, '                                     ||
           '       session_id, '                                  ||
           '       session_type, '                                ||
           '       session_state, '                               ||
           '       top_level_sql_id, '                            ||
           '       sql_id, '                                      ||
           '       plan_id, '                                     ||
           '       trace_id, '                                    ||
           '       nvl(event, ''CPU + Wait for CPU'') as event, ' ||
           '       nvl(event_no, 1) as event_no, '                ||
           '       p1, '                                          ||
           '       p1text, '                                      ||
           '       p2, '                                          ||
           '       p2text, '                                      ||
           '       p3, '                                          ||
           '       p3text, '                                      ||
           '       nvl(wait_class, ''CPU'') as wait_class, '      ||
           '       nvl(wait_class_id, 9999) as wait_class_id, '   ||
           '       time_waited, '                                 ||
           '       sql_plan_line_id, '                            ||
           '       in_parse, '                                    ||
           '       in_pl_parse, '                                 ||
           '       in_plan_cache, '                               ||
           '       in_sql_optimize, '                             ||
           '       in_sql_execution, '                            ||
           '       in_px_execution, '                             ||
           '       in_sequence_load, '                            ||
           '       in_committing, '                               ||
           '       in_storage_read, '                             ||
           '       in_storage_write, '                            ||
           '       in_remote_das_execution, '                     ||
           '       in_plsql_execution, '                          ||
           '       in_plsql_compilation, '                        ||
           '       plsql_entry_object_id, '                       ||
           '       plsql_entry_subprogram_id, '                   ||
           '       plsql_entry_subprogram_name, '                 ||
           '       plsql_object_id, '                             ||
           '       plsql_subprogram_id, '                         ||
           '       plsql_subprogram_name, '                       ||
           '       module, '                                      ||
           '       action, '                                      ||
           '       client_id '                                    ||
           'FROM GV$ACTIVE_SESSION_HISTORY '                      ||
           'WHERE  0 = 0 ';
  END IN_MEMORY_ASH_VIEW_SQL;

  FUNCTION  ASH_VIEW_SQL RETURN  VARCHAR2 IS BEGIN
    RETURN 'SELECT * FROM (' || IN_MEMORY_ASH_VIEW_SQL                                    ||
           '                AND (sample_time between :ash_mem_btime AND :ash_mem_etime) ' ||
           '              ) unified_ash '                                                 ||
           'WHERE sample_time between :ash_begin_time AND :ash_end_time '                 ||
           '      AND (:ash_sql_id IS NULL OR sql_id LIKE :ash_sql_id) '                  ||
           '      AND (:ash_trace_id IS NULL OR trace_id LIKE :ash_trace_id) '            ||
           '      AND (:ash_wait_class IS NULL OR wait_class LIKE :ash_wait_class) '      ||
           '      AND (:ash_module IS NULL OR module LIKE :ash_module) '                  ||
           '      AND (:ash_action IS NULL OR action LIKE :ash_action) '                  ||
           '      AND (:ash_client_id IS NULL OR client_id LIKE :ash_client_id)';
  END ASH_VIEW_SQL;

END dbms_ash_internal;
