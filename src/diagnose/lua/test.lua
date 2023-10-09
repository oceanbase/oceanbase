tenant_ids = {}
print_to_client(usage())

print_to_client("now:", now())

tenant_ids = get_tenant_id_list()

print_to_client("tenant_cnt:", #tenant_ids)
print_to_client("tenant_id memory_limit")
for i=1,#tenant_ids do
    limit = get_tenant_mem_limit(tenant_ids[i])
    print_to_client(tenant_ids[i], limit)
end

print_to_client("rpc packet in bytes", get_tenant_sysstat_by_name(1, "rpc packet in bytes"), get_tenant_sysstat_by_id(1001, 10001))

print_to_client("memory usage", get_tenant_sysstat_by_name(1, "memory usage"), get_tenant_sysstat_by_id(1001, 140003))

-- to limit 10 rows
-- para["limit"] = {10}

-- equal to {10}
-- para["limit"] = {0, 10}

-- using dump mode, recommended
-- para["dump"] = true

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "id",
    "user",
    "tenant",
    "host",
    "db",
    "command",
    "sql_id",
    "time",
    "state",
    "info",
    "sql_port",
    "proxy_sessid",
    "master_sessid",
    "user_client_ip",
    "user_host",
    "trans_id",
    "thread_id",
    "ssl_cipher",
    "trace_id",
    "trans_state",
    "total_time",
    "retry_cnt",
    "retry_info",
    "action",
    "module",
    "client_info"
}
print_to_client("select_processlist")
select_processlist(para)

para = {}
para["limit"] = {10}
para["dump"] = true
para["select"] = {
    "tenant_id",
    "statistic",
    "value",
    "value_type",
    "stat_id",
    "name",
    "class",
    "can_visible"
}
print_to_client("select_sysstat")
select_sysstat(para)

para = {}
para["limit"] = {10}
para["dump"] = true
para["select"] = {"tenant_id", "ctx_name", "label", "hold"}
print_to_client("select_memory_info")
select_memory_info(para)

para = {}
para["limit"] = {10}
para["dump"] = true
para["select"] = {
    "tenant_id",
    "ctx_id",
    "ctx_name",
    "hold",
    "used",
    "limit"
}
print_to_client("select_tenant_ctx_memory_info")
select_tenant_ctx_memory_info(para)

para = {}
para["limit"] = {10}
para["dump"] = true
para["select"] = {
    "tenant_id",
    "tx_type",
    "tx_id",
    "session_id",
    "scheduler_addr",
    "is_decided",
    "ls_id",
    "participants",
    "tx_ctx_create_time",
    "expired_time",
    "ref_cnt",
    "last_op_sn",
    "pending_write",
    "state",
    "part_tx_action",
    "tx_ctx_addr",
    "mem_ctx_id",
    "pending_log_size",
    "flushed_log_size",
    "role_state",
    "is_exiting",
    "coord",
    "last_request_ts",
    "gtrid",
    "bqual",
    "format_id"
}
print_to_client("select_trans_stat")
select_trans_stat(para)

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "plan_id",
    "sql_id",
    "sql_exec_id",
    "operation_type",
    "operation_id",
    "sid",
    "active_time",
    "work_area_size",
    "expect_size",
    "actual_mem_used",
    "max_mem_used",
    "number_passes",
    "tempseg_size",
    "tenant_id",
    "policy"
}
print_to_client("select_sql_workarea_active")
select_sql_workarea_active(para)

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "start_time",
    "task_type",
    "task_id",
    "tenant_id",
    "comment",
    "is_cancel"
}
print_to_client("select_sys_task_status")
select_sys_task_status(para)

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "tenant_id",
    "compat_mode",
    "unit_min_cpu",
    "unit_max_cpu",
    "slice",
    "remain_slice",
    "token_cnt",
    "ass_token_cnt",
    "lq_tokens",
    "used_lq_tokens",
    "stopped",
    "idle_us",
    "recv_hp_rpc_cnt",
    "recv_np_rpc_cnt",
    "recv_lp_rpc_cnt",
    "recv_mysql_cnt",
    "recv_task_cnt",
    "recv_large_req_cnt",
    "recv_large_queries",
    "actives",
    "workers",
    "lq_warting_workers",
    "req_queue_total_size",
    "queue_0",
    "queue_1",
    "queue_2",
    "queue_3",
    "queue_4",
    "queue_5",
    "large_queued"
}
print_to_client("select_dump_tenant_info")
select_dump_tenant_info(para)

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "total_size",
    "used_size",
    "free_size",
    "is_disk_valid",
    "disk_error_begin_ts"
}
print_to_client("select_disk_stat")
select_disk_stat(para)

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "tenant_id",
    "hold",
    "limit"
}
print_to_client("select_tenant_memory_info")
select_tenant_memory_info(para)

print_to_client("show_log_probe")
print_to_client(show_log_probe())

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "mod_name",
    "mod_type",
    "alloc_count",
    "alloc_size",
    "back_trace"
}
print_to_client("select_mem_leak_checker_info")
select_mem_leak_checker_info(para)

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "tenant_id",
    "merge_type",
    "ls_id",
    "tablet_id",
    "status",
    "create_time",
    "diagnose_info"
}
print_to_client("select_compaction_diagnose_info")
select_compaction_diagnose_info(para)

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "tenant_id",
    "task_id",
    "module",
    "type",
    "ret",
    "status",
    "gmt_create",
    "gmt_modified",
    "retry_cnt",
    "warning_info"
}
print_to_client("select_dag_warning_history")
select_dag_warning_history(para)

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "tenant_id",
    "refreshed_schema_version",
    "received_schema_version",
    "schema_count",
    "schema_size",
    "min_sstable_schema_version"
}
print_to_client("select_server_schema_info")
select_server_schema_info(para)

para = {}
para["limit"] = {10}
para["dump"] = true
para["select"] = {
    "tenant_id",
    "slot_id",
    "schema_version",
    "schema_count",
    "total_ref_cnt",
    "ref_info"
}
print_to_client("select_schema_slot")
select_schema_slot(para)

para = {}
para["limit"] = {}
para["dump"] = true
para["select"] = {
    "tname",
    "tid",
    "thread_base",
    "loop_ts",
    "latch_hold",
    "latch_wait",
    "trace_id",
    "status",
    "wait_event"
}
print_to_client("dump_thread_info")
dump_thread_info(para)

para = {}
para["limit"] = {0, 3}
para["dump"] = true
para["select"] = {
    "tenant_id",
    "ctx_id",
    "mod_name",
    "back_trace",
    "ctx_name",
    "alloc_count",
    "alloc_bytes"
}
print_to_client("select_malloc_sample_info")
select_malloc_sample_info(para)

set_system_tenant_limit_mode(1)
