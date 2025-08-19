connect (wenyu_sys,$OBMYSQL_MS0,root,"",oceanbase,$OBMYSQL_PORT);
connection wenyu_sys;
--disable_abort_on_error
--disable_query_log
--disable_result_log
alter proxyconfig set client_session_id_version=1;
alter proxyconfig set server_protocol='Auto';
--enable_abort_on_error
--enable_query_log
--enable_result_log
disconnect wenyu_sys;
disconnect default;
connect (default,$OBMYSQL_MS0,admin@mysql,admin,test,$OBMYSQL_PORT);