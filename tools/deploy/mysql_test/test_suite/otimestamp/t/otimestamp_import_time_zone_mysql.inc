
#--echo
#--echo ########### import time zone info ###########

--disable_query_log
--disable_result_log
--disable_warnings

connect (conn_admin_tz, $OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection conn_admin_tz;
set session ob_query_timeout = 20000000;
select count(*) c1 from oceanbase.__all_tenant_time_zone;
select count(*) c1 from oceanbase.__all_tenant_time_zone_name;
select count(*) c1 from oceanbase.__all_tenant_time_zone_transition;
select count(*) c1 from oceanbase.__all_tenant_time_zone_transition_type;
delete from oceanbase.__all_tenant_time_zone;
delete from oceanbase.__all_tenant_time_zone_name;
delete from oceanbase.__all_tenant_time_zone_transition;
delete from oceanbase.__all_tenant_time_zone_transition_type;
alter system change tenant mysql ;
sleep 2;

--source mysql_test/test_suite/time_zone/t/time_zone_info.sql

sleep 10;
disconnect conn_admin_tz;
connection default;

--enable_query_log
--enable_result_log
--enable_warnings
