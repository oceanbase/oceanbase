
connection default;

--disable_query_log
--disable_result_log
select * from proxy_mock_table_for_pc;
--enable_query_log
--enable_result_log

