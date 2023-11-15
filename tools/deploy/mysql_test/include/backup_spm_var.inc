--disable_query_log
--disable_result_log
if ($TENANT == 'mysql') {
  let $use_spb_bk = query_get_value(select value from oceanbase.__tenant_virtual_session_variable where VARIABLE_NAME = 'optimizer_use_sql_plan_baselines',value, 1);
  let $cap_spb_bk = query_get_value(select value from oceanbase.__tenant_virtual_session_variable where VARIABLE_NAME = 'optimizer_capture_sql_plan_baselines',value, 1);
  set global optimizer_use_sql_plan_baselines = 'OFF';
  set global optimizer_capture_sql_plan_baselines = 'OFF';
  set session optimizer_use_sql_plan_baselines = 'OFF';
  set session optimizer_capture_sql_plan_baselines = 'OFF';
}
if ($TENANT == 'oracle') {
  let $use_spb_bk = query_get_value(select value from sys.TENANT_VIRTUAL_SESSION_VARIABLE where VARIABLE_NAME = 'optimizer_use_sql_plan_baselines',VALUE, 1);
  let $cap_spb_bk = query_get_value(select value from sys.TENANT_VIRTUAL_SESSION_VARIABLE where VARIABLE_NAME = 'optimizer_capture_sql_plan_baselines',VALUE, 1);
  set global optimizer_use_sql_plan_baselines = 'OFF';
  set global optimizer_capture_sql_plan_baselines = 'OFF';
  set session optimizer_use_sql_plan_baselines = 'OFF';
  set session optimizer_capture_sql_plan_baselines = 'OFF';
}
--enable_result_log
--enable_query_log
