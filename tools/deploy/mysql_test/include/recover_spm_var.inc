--disable_query_log
--disable_result_log
if ($TENANT == 'mysql') {
  eval set global optimizer_use_sql_plan_baselines = '$use_spb_bk';
  eval set global optimizer_capture_sql_plan_baselines = '$cap_spb_bk';
  eval set session optimizer_use_sql_plan_baselines = '$use_spb_bk';
  eval set session optimizer_capture_sql_plan_baselines = '$cap_spb_bk';
}
if ($TENANT == 'oracle') {
  eval set global optimizer_use_sql_plan_baselines = '$use_spb_bk';
  eval set global optimizer_capture_sql_plan_baselines = '$cap_spb_bk';
  eval set session optimizer_use_sql_plan_baselines = '$use_spb_bk';
  eval set session optimizer_capture_sql_plan_baselines = '$cap_spb_bk';
}
--enable_result_log
--enable_query_log
