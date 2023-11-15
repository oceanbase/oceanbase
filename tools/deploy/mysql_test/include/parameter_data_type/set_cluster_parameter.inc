--disable_query_log
let $para_value = query_get_value(select value from oceanbase.__all_virtual_tenant_parameter_stat where svr_ip = host_ip() and svr_port = rpc_port() and name = '$para_name', value, 1);
eval alter system set $para_name = '$para_value';
--enable_query_log
