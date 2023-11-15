#--disable_query_log
#--disable_result_log
connect (obsys_ydm,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_ydm;
let $__i__= 600;
sleep 3;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;
    let $__frozen_scn__ = query_get_value(select /*+query_timeout(100000000)*/sum(frozen_scn/1000) as sum_value from oceanbase.CDB_OB_MAJOR_COMPACTION, sum_value, 1);
    let $__merged_version__ = query_get_value(select /*+query_timeout(100000000)*/sum(last_scn/1000) as sum_value from oceanbase.CDB_OB_MAJOR_COMPACTION, sum_value, 1);
    #--echo sum_frozen_scn=$__frozen_scn__
    #--echo sum_merged_version=$__merged_version__
    #--echo remain_time_s=$__i__
    #--echo =======================================
    # all tenant should major merge complete
    if($__frozen_scn__ == $__merged_version__)
    {
        let $__i__ = -5;
    }
}

###判断集群状态，先注释掉
#let $__j__= 180;
#while($__j__ > 0)
#{
#    sleep 1;
#    dec $__j__;
#    let $__global_stat__ = query_get_value(select comment from oceanbase.__all_cluster_stat where name='global_stat',comment, 1);
#    if($__global_stat__ == NORMAL)
#    {
#        let $__j__ = -5;
#    }
#}

#if($__j__ != -5)
#{
#    --echo major freeze failed
#}

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
