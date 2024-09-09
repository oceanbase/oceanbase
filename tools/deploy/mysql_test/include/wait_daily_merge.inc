--disable_query_log
--disable_result_log
connect (obsys_ydm,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_ydm;

#reduce wait major compation time cost
alter system set ob_compaction_schedule_interval = '10s' tenant sys;
alter system set ob_compaction_schedule_interval = '10s' tenant all_user;
alter system set ob_compaction_schedule_interval = '10s' tenant all_meta;

let $__i__= 1200;
sleep 3;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;
    let $__frozen_scn__ = query_get_value(select /*+query_timeout(100000000)*/sum(frozen_scn/1000) as sum_value from oceanbase.CDB_OB_MAJOR_COMPACTION, sum_value, 1);
    let $__merged_version__ = query_get_value(select /*+query_timeout(100000000)*/sum(last_scn/1000) as sum_value from oceanbase.CDB_OB_MAJOR_COMPACTION, sum_value, 1);
    if($__frozen_scn__ == $__merged_version__)
    {
        let $__i__ = -5;
    }
}

if($__i__ != -5)
{
    --echo major freeze failed
    select * from oceanbase.CDB_OB_MAJOR_COMPACTION;
    let $__tenant_id__ = query_get_value(select tenant_id from oceanbase.CDB_OB_MAJOR_COMPACTION where last_scn != frozen_scn, tenant_id, 1);
    let $__frozen_scn__ = query_get_value(select frozen_scn from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id = $__tenant_id__, frozen_scn, 1);
    eval select * from oceanbase.__all_virtual_server_compaction_event_history where tenant_id = $__tenant_id__ and compaction_scn = $__frozen_scn__;
    select * from oceanbase.__all_virtual_compaction_diagnose_info;
}
sleep 2;

disconnect obsys_ydm;
connection default;
--enable_query_log
--enable_result_log
