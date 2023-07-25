
#--echo
#--echo ########### import time zone info ###########

--disable_query_log
--disable_result_log
--disable_warnings

connect (conn_admin_tz, $OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection conn_admin_tz;
alter system change tenant mysql;

connect (conn_mysql,$OBMYSQL_MS0,root@mysql,,*NO-ONE*,$OBMYSQL_PORT);
connection conn_mysql;

--error 0, 1298
set time_zone = 'Asia/Shanghai';
let $result= query_get_value(select @@time_zone as c1 from dual, c1, 1);
if ($result != 'Asia/Shanghai')
{
  connection conn_admin_tz;
  --source mysql_test/test_suite/time_zone/t/time_zone_info.sql
  --disable_query_log
  --disable_result_log
  sleep 5;
  let $test_count=1;
  connection conn_mysql;
  while ($test_count < 30)
  {
    --error 0, 1298
    set time_zone = 'Asia/Shanghai';
    let $result= query_get_value(select @@time_zone as c1 from dual, c1, 1);
    if ($result != 'Asia/Shanghai') {
      sleep 1;
      inc $test_count;
    }
    if ($result == 'Asia/Shanghai') {
      let $test_count = 30;
    }
  }
}

--enable_query_log
disconnect conn_admin_tz;
disconnect conn_mysql;
connection default;

--enable_result_log
--enable_warnings
