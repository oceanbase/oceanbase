tenant_ids = {}
print_to_client(usage())

tenant_ids = get_tenant_id_list()

print_to_client("tenant_cnt:", #tenant_ids)
print_to_client("tenant_id memory_limit")
for i=1,#tenant_ids do
    limit = get_tenant_mem_limit(tenant_ids[i])
    print_to_client(tenant_ids[i], limit)
end

print_to_client("rpc packet in bytes", get_tenant_sysstat_by_name(1, "rpc packet in bytes"), get_tenant_sysstat_by_id(1001, 10001))

print_to_client("memory usage", get_tenant_sysstat_by_name(1, "memory usage"), get_tenant_sysstat_by_id(1001, 140003))

para = {}
para["limit"] = {1, 10}
para["dump"] = true

sessions = select_processlist()
print_to_client("session_cnt:", #sessions)
for i=1,#sessions do
    print_to_client(sessions[i]['id'],
                    sessions[i]['user'],
                    sessions[i]['tenant'],
                    sessions[i]['host'],
                    sessions[i]['db'],
                    sessions[i]['command'],
                    sessions[i]['sql_id'],
                    sessions[i]['time'],
                    sessions[i]['state'],
                    sessions[i]['info'],
                    sessions[i]['sql_port'],
                    sessions[i]['proxy_sessid'],
                    sessions[i]['master_sessid'],
                    sessions[i]['user_client_ip'],
                    sessions[i]['user_host'],
                    sessions[i]['trans_id'],
                    sessions[i]['thread_id'],
                    sessions[i]['ssl_cipher'],
                    sessions[i]['trace_id'])
end

para["select"] = {"id", "tenant", "command"}
select_processlist(para)

sysstats = select_sysstat()
print_to_client("sysstat_cnt:", #sysstats)
for i=1,10 do
    print_to_client(sysstats[i]['tenant_id'],
                    sysstats[i]['statistic'],
                    sysstats[i]['value'],
                    sysstats[i]['value_type'],
                    sysstats[i]['stat_id'],
                    sysstats[i]['name'],
                    sysstats[i]['class'],
                    sysstats[i]['can_visible'])
end

para["select"] = {"tenant_id", "statistic", "value"}
select_sysstat(para)

memory_info = select_memory_info()
print_to_client("memory_info_cnt", #memory_info)
for i=1,10 do
    print_to_client(memory_info[i]['tenant_id'],
                    memory_info[i]['ctx_id'],
                    memory_info[i]['ctx_name'],
                    memory_info[i]['label'],
                    memory_info[i]['hold'],
                    memory_info[i]['used'],
                    memory_info[i]['count'])
end

para["select"] = {"tenant_id", "ctx_name", "label", "hold"}
select_memory_info(para)

tenant_ctx_memory_info = select_tenant_ctx_memory_info()
print_to_client("tenant_ctx_memory_info_cnt", #tenant_ctx_memory_info)
for i=1,10 do
    print_to_client(tenant_ctx_memory_info[i]['tenant_id'],
                    tenant_ctx_memory_info[i]['ctx_id'],
                    tenant_ctx_memory_info[i]['ctx_name'],
                    tenant_ctx_memory_info[i]['hold'],
                    tenant_ctx_memory_info[i]['used'])
end

para["select"] = {"tenant_id", "ctx_id", "ctx_name", "hold"}
select_tenant_ctx_memory_info(para)

trans_stat = select_trans_stat()
print_to_client("trans_stat_cnt", #trans_stat)
for i=1,#trans_stat do
    print_to_client(trans_stat[i]['tenant_id'],
                    trans_stat[i]['trans_type'],
                    trans_stat[i]['trans_id'],
                    trans_stat[i]['session_id'],
                    trans_stat[i]['scheduler_addr'],
                    trans_stat[i]['is_decided'],
                    trans_stat[i]['ls_id'],
                    trans_stat[i]['participants'],
                    trans_stat[i]['trans_consistency'],
                    trans_stat[i]['ctx_create_time'],
                    trans_stat[i]['expired_time'],
                    trans_stat[i]['refer_cnt'],
                    trans_stat[i]['sql_no'],
                    trans_stat[i]['state'],
                    trans_stat[i]['part_trans_action'],
                    trans_stat[i]['lock_for_read_retry_count'],
                    trans_stat[i]['trans_ctx_addr'],
                    trans_stat[i]['trans_ctx_id'],
                    trans_stat[i]['pending_log_size'],
                    trans_stat[i]['flushed_log_size'],
                    trans_stat[i]['role'],
                    trans_stat[i]['is_exiting'],
                    trans_stat[i]['coord'],
                    trans_stat[i]['last_request_time'],
                    trans_stat[i]['gtrid'],
                    trans_stat[i]['bqual'],
                    trans_stat[i]['format_id'])
end

para["select"] = {"tenant_id", "trans_id", "session_id"}
select_trans_stat(para)

-- tenant_disk_stat = select_tenant_disk_stat()
-- print_to_client("tenant_disk_stat_cnt", #tenant_disk_stat)
-- for i=1,#tenant_disk_stat do
--     print_to_client(tenant_disk_stat[i]['tenant_id'],
--                     tenant_disk_stat[i]['zone'],
--                     tenant_disk_stat[i]['block_type'],
--                     tenant_disk_stat[i]['block_size'])
-- end
-- 
-- para["limit"] = {5}
-- para["select"] = None
-- select_tenant_disk_stat(para)

sql_workarea_active = select_sql_workarea_active()
print_to_client("sql_workarea_active_cnt", #sql_workarea_active)
for i=1,#sql_workarea_active do
    print_to_client(sql_workarea_active[i]['plan_id'],
                    sql_workarea_active[i]['sql_id'],
                    sql_workarea_active[i]['sql_exec_id'],
                    sql_workarea_active[i]['operation_type'],
                    sql_workarea_active[i]['operation_id'],
                    sql_workarea_active[i]['sid'],
                    sql_workarea_active[i]['active_time'],
                    sql_workarea_active[i]['work_area_size'],
                    sql_workarea_active[i]['expect_size'],
                    sql_workarea_active[i]['actual_mem_used'],
                    sql_workarea_active[i]['max_mem_used'],
                    sql_workarea_active[i]['number_passes'],
                    sql_workarea_active[i]['tempseg_size'],
                    sql_workarea_active[i]['tenant_id'],
                    sql_workarea_active[i]['policy'])
end

para["select"] = {"plan_id", "sql_id"}
select_sql_workarea_active(para)

sys_task_status = select_sys_task_status()
print_to_client("sys_task_status_cnt", #sys_task_status)
for i=1,#sys_task_status do
    print_to_client(sys_task_status[i]['start_time'],
                    sys_task_status[i]['task_type'],
                    sys_task_status[i]['task_id'],
                    sys_task_status[i]['tenant_id'],
                    sys_task_status[i]['comment'],
                    sys_task_status[i]['is_cancel'])
end

para["select"] = {"start_time", "tenant_id"}
select_sys_task_status(para)

dump_tenant_info = select_dump_tenant_info()
print_to_client("dump_tenant_info_cnt", #dump_tenant_info)
for i=1,#dump_tenant_info do
    print_to_client(dump_tenant_info[i]['tenant_id'],
                    dump_tenant_info[i]['compat_mode'],
                    dump_tenant_info[i]['unit_min_cpu'],
                    dump_tenant_info[i]['unit_max_cpu'],
                    dump_tenant_info[i]['slice'],
                    dump_tenant_info[i]['remain_slice'],
                    dump_tenant_info[i]['token_cnt'],
                    dump_tenant_info[i]['ass_token_cnt'],
                    dump_tenant_info[i]['lq_tokens'],
                    dump_tenant_info[i]['used_lq_tokens'],
                    dump_tenant_info[i]['stopped'],
                    dump_tenant_info[i]['idle_us'],
                    dump_tenant_info[i]['recv_hp_rpc_cnt'],
                    dump_tenant_info[i]['recv_np_rpc_cnt'],
                    dump_tenant_info[i]['recv_lp_rpc_cnt'],
                    dump_tenant_info[i]['recv_mysql_cnt'],
                    dump_tenant_info[i]['recv_task_cnt'],
                    dump_tenant_info[i]['recv_large_req_cnt'],
                    dump_tenant_info[i]['recv_large_queries'],
                    dump_tenant_info[i]['actives'],
                    dump_tenant_info[i]['workers'],
                    dump_tenant_info[i]['lq_warting_workers'],
                    dump_tenant_info[i]['req_queue_total_size'],
                    dump_tenant_info[i]['queue_0'],
                    dump_tenant_info[i]['queue_1'],
                    dump_tenant_info[i]['queue_2'],
                    dump_tenant_info[i]['queue_3'],
                    dump_tenant_info[i]['queue_4'],
                    dump_tenant_info[i]['queue_5'],
                    dump_tenant_info[i]['large_queued'])
end

para["select"] = {"tenant_id", "unit_min_cpu", "unit_max_cpu"}
select_dump_tenant_info(para)

disk_stat = select_disk_stat()
print_to_client("disk_stat_cnt", #disk_stat)
for i=1,#disk_stat do
    print_to_client(disk_stat[i]['total_size'],
                    disk_stat[i]['used_size'],
                    disk_stat[i]['free_size'],
                    disk_stat[i]['is_disk_valid'])
end

para["select"] = {"total_size", "used_size"}
select_disk_stat(para)

tenant_memory_info = select_tenant_memory_info()
print_to_client("tenant_memory_info_cnt", #tenant_memory_info)
for i=1,#tenant_memory_info do
    print_to_client(tenant_memory_info[i]['tenant_id'],
                    tenant_memory_info[i]['hold'],
                    tenant_memory_info[i]['limit'])
end

para["select"] = {"tenant_id", "hold"}
select_tenant_memory_info(para)
