## 功能: 获取指定租户的ip&port
# 1.最好使用前sleep下，否则tenant_schema有可能还未刷到server上导致连接不上
# 2.如果分布于多个server，取unit_id最小的
## 用法:
# 1.设置待连接的tenant_name
# 2.source本文件
# 3.connect
## 模板:
# sleep 3;
# let $__tenant_name__ = 'tt1';
# --source mysql_test/include/get_tenant_server.inc
# connect (conn1,$TE_SERVER_IP,root@tt1,,*NO-ONE*,$TE_SERVER_PORT);


--disable_query_log
--disable_result_log
connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys;
let $__tenant_id__  = `select tenant_id from oceanbase.__all_tenant where tenant_name = $__tenant_name__ limit 1`;
let $__resource_pool_id__ = `select resource_pool_id from oceanbase.__all_resource_pool where tenant_id = $__tenant_id__ limit 1`;

let $__svr_ip__ = `select svr_ip from oceanbase.__all_unit where resource_pool_id = $__resource_pool_id__ limit 1`;
let $__svr_port__ = `select svr_port from oceanbase.__all_unit where resource_pool_id = $__resource_pool_id__ limit 1`;

let $__inner_port__ = `select inner_port from oceanbase.__all_server where svr_ip = '$__svr_ip__' and svr_port = $__svr_port__ limit 1`;

let $TE_SERVER_IP = $__svr_ip__;
let $TE_SERVER_PORT = $__inner_port__;
disconnect obsys;
connection default;
--enable_query_log
--enable_result_log
