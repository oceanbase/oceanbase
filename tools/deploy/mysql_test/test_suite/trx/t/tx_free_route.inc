## -------
##  common routines to prepare table and ls topology for tx_free_route test
## -------

connection sys;

# create tenant for test

--disable_warnings
create resource unit if not exists tx_free_route_unit max_cpu=1, memory_size='2G';
--enable_warnings
--disable_abort_on_error
--disable_query_log
--disable_result_log
let $i=0;
let $breakloop=0;
while($breakloop == 0) {
  create resource pool tx_free_route_pool unit='tx_free_route_unit',unit_num=1,zone_list=('zone1','zone2');
  if ($mysql_errno == 0) {
    let $breakloop=1;
  }
  if ($mysql_errno != 0) {
    --inc $i
    if ($i > 20) {
      --echo "create resource pool failed : retry times=$i, error no=$mysql_errno"
      let $breakloop=1;
    }
    --sleep 1
  }
}
--enable_abort_on_error
--enable_query_log
--enable_result_log

eval create tenant tx_free_route_tenant replica_num=2, resource_pool_list=('tx_free_route_pool'),primary_zone=RANDOM set ob_tcp_invited_nodes='%',ob_compatibility_mode='$compat_mode';

let $tenant_id=`select tenant_id from oceanbase.__all_tenant where tenant_name = 'tx_free_route_tenant'`;

# check tenant has multiple ls

let $breakLoop=0;
let $i=0;
let $fail = 0;
while ($breakLoop == 0) {
 --inc $i
 let $lsCnt=`select count(ls_id) from oceanbase.__all_virtual_ls where tenant_id = $tenant_id and ls_id != 1 and status = "NORMAL" and flag not like "%DUPLICATE%"`;
 if ($lsCnt > 1) {
    --inc $breakLoop
 }
 if ($i >= 50) {
    --inc $fail
    --inc $breakLoop
 }
 --sleep 0.2
}

if ($fail > 0) {
   --echo "tenant normal ls not distributed to multiple server"
   eval select * from oceanbase.__all_virtual_ls where tenant_id = $tenant_id;
}

if ($compat_mode == 'mysql') {
   connect (conn0,$OBMYSQL_MS0,root@tx_free_route_tenant,,test,$OBMYSQL_PORT);
   connect (conn1,$OBMYSQL_MS0,root@tx_free_route_tenant,,test,$OBMYSQL_PORT);
   connect (conn2,$OBMYSQL_MS0,root@tx_free_route_tenant,,test,$OBMYSQL_PORT);
}

if ($compat_mode == 'oracle') {
   connect (conn0,$OBMYSQL_MS0,sys@tx_free_route_tenant,,sys,$OBMYSQL_PORT);
   connect (conn1,$OBMYSQL_MS0,sys@tx_free_route_tenant,,sys,$OBMYSQL_PORT);
   connect (conn2,$OBMYSQL_MS0,sys@tx_free_route_tenant,,sys,$OBMYSQL_PORT);
}

connection conn0;

if ($compat_mode == 'mysql') {
  --disable_warnings
  drop table if exists t;
  --enable_warnings
}

if ($compat_mode == 'oracle') {
  --disable_abort_on_error
  --disable_result_log
  drop table t purge;
  --enable_abort_on_error
  --enable_result_log
}

create table t(id int primary key, v int default 0) partition by range(id)
 (partition p0 values less than (10000), partition p1 values less than (3000000000));

# insert rows to ensure tablet created
--disable_result_log
--disable_query_log
insert into t(id) values(9999),(99999);
commit;
--enable_result_log
--enable_query_log

# check tablet of target table distributed to multiple ls
connection sys;
let $table_id=`select table_id from oceanbase.__all_virtual_table where table_name = 't' and tenant_id = $tenant_id`;
let $breakLoop=0;
let $i=0;
let $fail=0;
while ($breakLoop == 0)
{
  --inc $i
  let $lsCnt=`select count(distinct ls_id) from oceanbase.__all_virtual_tablet_to_ls where tenant_id = $tenant_id and table_id = $table_id`;
  if ($lsCnt > 1) {
    --inc $breakLoop
  }
  if ($i >= 50) {
    --inc $fail
    --inc $breakLoop
  }
 --sleep 0.2
}

if ($fail > 0) {
  --echo "error, table should distributed to all ls"
  eval select * from oceanbase.__all_virtual_tablet_to_ls where tenant_id = $tenant_id and table_id = $table_id;
  eval select * from oceanbase.__all_virtual_ls where tenant_id = $tenant_id;
}

# check ls leader distributed to multiple server
let $check_ts=`select now()`;
let $svrCnt=`select count(distinct svr_ip,svr_port) from oceanbase.__all_virtual_ls_info where tenant_id = $tenant_id and ls_state = 'LEADER' and ls_id in (select distinct ls_id from oceanbase.__all_virtual_tablet_to_ls where tenant_id = $tenant_id and table_id = $table_id)`;
if ($svrCnt <= 1) {
  --echo "ls leader not distributed to all server, check_ts=$check_ts, table_id=$table_id";
  eval select * from oceanbase.__all_virtual_tablet_to_ls where tenant_id = $tenant_id and table_id = $table_id;
  eval select * from oceanbase.__all_virtual_ls_meta_table where tenant_id = $tenant_id and role = 1 order by ls_id;
  eval select * from oceanbase.__all_virtual_ls where tenant_id = $tenant_id;
  eval select * from oceanbase.__all_virtual_ls_info where tenant_id = $tenant_id;
}
