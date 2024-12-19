
#--echo
#--echo ########### import srs data ###########

--disable_query_log
--disable_result_log
--disable_warnings

connect (conn_admin, $OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection conn_admin;
let $c1 = query_get_value(select count(*) as cnt from oceanbase.__all_spatial_reference_systems, cnt, 1);
# TODO@dazhi: import to mysql tenant
#alter system change tenant mysql ;
#sleep 2;

if ($c1 != 5160)
{
  --source mysql_test/test_suite/geometry/t/default_srs_data_mysql.sql
  sleep 30;
}

# TODO@dazhi: use mysql tenant
#disconnect conn_admin;
#connection default;

--enable_query_log
--enable_result_log
--enable_warnings
