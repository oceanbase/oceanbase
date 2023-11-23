#--disable_query_log
#--disable_result_log
connection sys_conn;
let $timeout_s = 600;
if ($merge_stage == 0) {
    ## check minor_freeze after major_freeze is started
    let $__i__= $timeout_s;
    while($__i__ > 0)
    {
        sleep 1;
        dec $__i__;
        let $cur_minor_freeze_count = query_get_value(select count(1) as val from __all_rootservice_event_history where event = 'root_minor_freeze', val, 1);
        if ($cur_minor_freeze_count > $minor_freeze_count_ori) {
            let $__i__ = -5;
        }
    }
    if($__i__ != -5)
    {
        --echo failed
    }
    
    ## check broadcast in freeze_info_detector
    let $__i__= $timeout_s;
    while($__i__ > 0)
    {
        sleep 1;
        dec $__i__;
        let $cur_frozen_scn_1 = query_get_value(select frozen_scn from __all_freeze_info order by frozen_scn desc limit 1, frozen_scn, 1);
        let $cur_frozen_scn_2 = query_get_value(select frozen_scn from __all_merge_info where tenant_id = $sys_tenant_id, frozen_scn, 1);
        let $ret1 = query_get_value(select $cur_frozen_scn_1 > $start_frozen_scn as val, val, 1);
        let $ret2 = query_get_value(select $cur_frozen_scn_2 > $start_frozen_scn as val, val, 1);
        if ($ret1 == 1) {
            if ($ret2 == 1) {
                let $__i__ = -5;
            }
        }
    }
    if($__i__ != -5)
    {
        --echo merge_stage = 0, failed to check broadcast in freeze_info_detector
    }

    ## check last_merged_version
    let $__i__= $timeout_s;
    while($__i__ > 0)
    {
        sleep 1;
        dec $__i__;
        let $cur_frozen_scn = query_get_value(select frozen_scn from __all_zone_merge_info where tenant_id = $sys_tenant_id, frozen_scn, 1);
        let $cur_last_merged_scn = query_get_value(select last_merged_scn from __all_zone_merge_info where tenant_id = $sys_tenant_id, last_merged_scn, 1);
        if ($cur_frozen_scn == $cur_last_merged_scn) {
            let $__i__ = -5;
        }
    }
    if($__i__ != -5)
    {
        --echo merge_stage = 0, failed to check last_merged_version
    }

    ## check is_merging
    let $__i__= $timeout_s;
    while($__i__ > 0)
    {
        sleep 1;
        dec $__i__;
        let $cur_is_merging = query_get_value(select is_merging from __all_zone_merge_info where tenant_id = $sys_tenant_id, is_merging, 1);
        if ($cur_is_merging == 0) {
            let $__i__ = -5;
        }
    }
    if($__i__ != -5)
    {
        --echo merge_stage = 0, failed to check is_merging
    }

    ## check merge_status
    let $__i__= $timeout_s;
    while($__i__ > 0)
    {
        sleep 1;
        dec $__i__;
        let $cur_merge_status = query_get_value(select merge_status from __all_zone_merge_info where tenant_id = $sys_tenant_id, merge_status, 1);
        if ($cur_merge_status == 0) {
            let $__i__ = -5;
        }
    }
    if($__i__ != -5)
    {
        --echo merge_stage = 0, failed to check merge_status
    }
}

if ($merge_stage == 1) {
    ## check is_merging
    let $__i__= $timeout_s;
    while($__i__ > 0)
    {
        sleep 1;
        dec $__i__;
        let $cur_is_merging = query_get_value(select is_merging from __all_zone_merge_info where tenant_id = $sys_tenant_id, is_merging, 1);
        if ($cur_is_merging == 1) {
            let $__i__ = -5;
        }
    }
    if($__i__ != -5)
    {
        --echo merge_stage = 1, failed to check is_merging
    }

    ## check merge_status
    let $__i__= $timeout_s;
    while($__i__ > 0)
    {
        sleep 1;
        dec $__i__;
        let $cur_merge_status = query_get_value(select merge_status from __all_zone_merge_info where tenant_id = $sys_tenant_id, merge_status, 1);
        if ($cur_merge_status >= 1) {
            let $__i__ = -5;
        }
    }
    if($__i__ != -5)
    {
        --echo merge_stage = 1, failed to check merge_status
    }

    ## check frozen_scn
    let $__i__= $timeout_s;
    while($__i__ > 0)
    {
        sleep 1;
        dec $__i__;
        let $cur_frozen_scn = query_get_value(select frozen_scn from __all_zone_merge_info where tenant_id = $sys_tenant_id, frozen_scn, 1);
        let $ret = query_get_value(select $cur_frozen_scn > $frozen_scn_ori as val, val, 1);
        if ($ret == 1) {
            let $__i__ = -5;
        }
    }
    if($__i__ != -5)
    {
        --echo merge_stage = 1, failed to check frozen_scn
    }

    ## check broadcast_version
    let $__i__= $timeout_s;
    while($__i__ > 0)
    {
        sleep 1;
        dec $__i__;
        let $cur_broadcast_scn = query_get_value(select broadcast_scn from __all_zone_merge_info where tenant_id = $sys_tenant_id, broadcast_scn, 1);
        let $ret = query_get_value(select $cur_broadcast_scn > $broadcast_scn_ori as val, val, 1);
        if ($ret == 1) {
            let $__i__ = -5;
        }
    }
    if($__i__ != -5)
    {
        --echo merge_stage = 1, failed to check broadcast_version
    }
}

connection default;
#--enable_query_log
#--enable_result_log
