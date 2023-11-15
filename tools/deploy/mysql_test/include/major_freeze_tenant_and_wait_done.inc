# set __freeze_tenant_name__ outside
# set __now_time__ outside

--disable_query_log
--disable_result_log

connect (obsys_major,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_major;

eval alter system major freeze tenant $__freeze_tenant_name__;

let $__tenant_id__ = query_get_value(select tenant_id from oceanbase.__all_tenant where tenant_name = $__freeze_tenant_name__, tenant_id, 1);
let $__i__= 600;
while($__i__ > 0)
{
  sleep 1;
  dec $__i__;
  let $__finish_major__= query_get_value(select count(*) as cnt from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id = $__tenant_id__ and last_finish_time > '$__now_time__', cnt, 1);
  if ($__finish_major__ == 1)
  {
    let $__i__ = -5;
  }
}

if ($__i__ != -5)
{
  --echo major freeze failed
}

disconnect obsys_major;
connection default;

--enable_query_log
--enable_result_log