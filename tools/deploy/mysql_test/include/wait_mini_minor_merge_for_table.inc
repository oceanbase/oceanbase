# set __table_id__ outside
# set __tenant_id__ outside
# set __now_time__ outside

--disable_query_log
--disable_result_log

connect (obsys_minor,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_minor;
let $i=300;
while ($i > 0)
{
  sleep 1;
  dec $i;
  let $cur_minor_count = query_get_value(select count(*) > 0 as c from oceanbase.__all_virtual_tablet_compaction_history where type = 'MINOR_MERGE' and tenant_id = $__tenant_id__ and tablet_id in (select tablet_id from oceanbase.__all_virtual_tablet_to_ls where table_id = $__table_id__ and tenant_id = $__tenant_id__) and finish_time >= '$__now_time__', c, 1);
  if ($cur_minor_count > 0)
  {
    let $i = -5;
  }
}

if ($i != -5)
{
  --echo mini minor failed
}

disconnect obsys_minor;
connection default;

--enable_query_log
--enable_result_log
