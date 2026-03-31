connect (obsys,$OBMYSQL_MS0,root,"",oceanbase,$OBMYSQL_PORT);
connection obsys;
--disable_abort_on_error
--disable_query_log
--disable_result_log
alter proxyconfig set server_protocol = 'OceanBase 2.0';
alter proxyconfig set client_session_id_version = 2;
--enable_abort_on_error
--enable_query_log
--enable_result_log
disconnect obsys;
disconnect default;
connect (default,$OBMYSQL_MS0,admin@mysql,admin,test,$OBMYSQL_PORT);