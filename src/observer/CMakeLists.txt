ob_set_subtarget(ob_server ALONE
  ob_server.cpp
  ob_service.cpp
)

ob_set_subtarget(ob_server common
  ob_dump_task_generator.cpp
  ob_heartbeat.cpp
  ob_inner_sql_rpc_proxy.cpp
  ob_inner_sql_rpc_processor.cpp
  ob_inner_sql_connection.cpp
  ob_inner_sql_connection_pool.cpp
  ob_resource_inner_sql_connection_pool.cpp
  ob_inner_sql_read_context.cpp
  ob_inner_sql_result.cpp
  ob_lease_state_mgr.cpp
  ob_req_time_service.cpp
  ob_root_service_monitor.cpp
  ob_rpc_extra_payload.cpp
  ob_rpc_intrusion_detect.cpp
  ob_rpc_processor_simple.cpp
  ob_server_event_history_table_operator.cpp
  ob_server_reload_config.cpp
  ob_server_schema_updater.cpp
  ob_server_struct.cpp
  ob_server_utils.cpp
  ob_signal_handle.cpp
  ob_sql_client_decorator.cpp
  ob_srv_deliver.cpp
  ob_srv_network_frame.cpp
  ob_srv_xlator.cpp
  ob_srv_xlator_partition.cpp
  ob_srv_xlator_primary.cpp
  ob_srv_xlator_rootserver.cpp
  ob_srv_xlator_storage.cpp
  ob_tenant_duty_task.cpp
  ob_uniq_task_queue.cpp
  ob_startup_accel_task_handler.cpp
  ob_srv_rpc_handler.cpp
  ob_check_params.cpp
)

ob_set_subtarget(ob_server common_mixed
  report/ob_ls_table_updater.cpp
  report/ob_tablet_table_updater.cpp
  report/ob_tenant_meta_checker.cpp
  report/ob_server_meta_table_checker.cpp
)

ob_set_subtarget(ob_server dbms_job
  dbms_job/ob_dbms_job_executor.cpp
  dbms_job/ob_dbms_job_master.cpp
  dbms_job/ob_dbms_job_rpc_processor.cpp
  dbms_job/ob_dbms_job_rpc_proxy.cpp
  dbms_job/ob_dbms_job_utils.cpp
  dbms_scheduler/ob_dbms_sched_job_utils.cpp
  dbms_scheduler/ob_dbms_sched_job_executor.cpp
  dbms_scheduler/ob_dbms_sched_job_master.cpp
  dbms_scheduler/ob_dbms_sched_job_rpc_processor.cpp
  dbms_scheduler/ob_dbms_sched_job_rpc_proxy.cpp
  dbms_scheduler/ob_dbms_sched_table_operator.cpp
 )

ob_set_subtarget(ob_server mysql
  mysql/ob_async_cmd_driver.cpp
  mysql/ob_async_plan_driver.cpp
  mysql/ob_eliminate_task.cpp
  mysql/ob_mysql_end_trans_cb.cpp
  mysql/ob_mysql_request_manager.cpp
  mysql/ob_mysql_result_set.cpp
  mysql/ob_query_driver.cpp
  mysql/ob_query_response_time.cpp
  mysql/ob_query_retry_ctrl.cpp
  mysql/ob_sync_cmd_driver.cpp
  mysql/ob_sync_plan_driver.cpp
  mysql/obmp_base.cpp
  mysql/obmp_change_user.cpp
  mysql/obmp_connect.cpp
  mysql/obmp_disconnect.cpp
  mysql/obmp_init_db.cpp
  mysql/obmp_packet_sender.cpp
  mysql/obmp_ping.cpp
  mysql/obmp_query.cpp
  mysql/obmp_process_info.cpp
  mysql/obmp_process_kill.cpp
  mysql/obmp_debug.cpp
  mysql/obmp_refresh.cpp
  mysql/obmp_reset_connection.cpp
  mysql/obmp_statistic.cpp
  mysql/obmp_stmt_close.cpp
  mysql/obmp_stmt_execute.cpp
  mysql/obmp_stmt_fetch.cpp
  mysql/obmp_stmt_get_piece_data.cpp
  mysql/obmp_stmt_prepare.cpp
  mysql/obmp_stmt_prexecute.cpp
  mysql/obmp_stmt_reset.cpp
  mysql/obmp_stmt_send_long_data.cpp
  mysql/obmp_stmt_send_piece_data.cpp
  mysql/obmp_utils.cpp
  mysql/obmp_auth_response.cpp
  mysql/obsm_conn_callback.cpp
  mysql/obsm_handler.cpp
  mysql/obsm_row.cpp
  mysql/obsm_utils.cpp
  mysql/obmp_set_option.cpp
  mysql/ob_feedback_proxy_utils.cpp
)

ob_set_subtarget(ob_server net
  net/ob_ingress_bw_alloc_service.cpp
  net/ob_net_endpoint_ingress_rpc_proccessor.cpp
  net/ob_net_endpoint_ingress_rpc_struct.cpp
  net/ob_rpc_reverse_keepalive.cpp
)

ob_set_subtarget(ob_server omt
  omt/ob_multi_level_queue.cpp
  omt/ob_multi_tenant.cpp
  omt/ob_retry_queue.cpp
  omt/ob_tenant.cpp
  omt/ob_tenant_config.cpp
  omt/ob_tenant_config_mgr.cpp
  omt/ob_tenant_meta.cpp
  omt/ob_tenant_node_balancer.cpp
  omt/ob_tenant_timezone.cpp
  omt/ob_tenant_timezone_mgr.cpp
  omt/ob_th_worker.cpp
  omt/ob_worker_processor.cpp
  omt/ob_multi_tenant_operator.cpp
  omt/ob_tenant_srs.cpp
)

ob_set_subtarget(ob_server table
  table/htable_filter_lex.cxx
  table/htable_filter_tab.cxx
  table/ob_htable_filter_operator.cpp
  table/ob_htable_filter_parser.cpp
  table/ob_htable_filters.cpp
  table/ob_table_filter.cpp
  table/ob_htable_utils.cpp
  table/ob_table_batch_execute_processor.cpp
  table/ob_table_end_trans_cb.cpp
  table/ob_table_execute_processor.cpp
  table/ob_table_query_and_mutate_processor.cpp
  table/ob_table_query_processor.cpp
  table/ob_table_rpc_processor.cpp
  table/ob_table_query_async_processor.cpp
  table/ob_table_service.cpp
  table/ob_htable_lock_mgr.cpp
  table/ob_table_context.cpp
  table/ob_table_executor.cpp
  table/ob_table_delete_executor.cpp
  table/ob_table_insert_executor.cpp
  table/ob_table_insert_up_executor.cpp
  table/ob_table_lock_executor.cpp
  table/ob_table_modify_executor.cpp
  table/ob_table_replace_executor.cpp
  table/ob_table_scan_executor.cpp
  table/ob_table_update_executor.cpp
  table/ob_table_executor_factory.cpp
  table/ob_table_cg_service.cpp
  table/ob_table_cache.cpp
  table/ob_table_session_pool.cpp
  table/ob_table_op_wrapper.cpp
  table/ob_table_query_common.cpp
  table/ob_table_direct_load_processor.cpp
  table/ob_table_aggregation.cpp
  table/ttl/ob_ttl_service.cpp
  table/ttl/ob_tenant_ttl_manager.cpp
  table/ttl/ob_tenant_tablet_ttl_mgr.cpp
  table/ttl/ob_table_ttl_task.cpp
  table/ttl/ob_table_ttl_executor.cpp
  table/ob_table_move_response.cpp
  table/ob_table_connection_mgr.cpp
  table/redis/ob_redis_meta.cpp
)

ob_set_subtarget(ob_server table_load
  table_load/backup/v_1_4/ob_table_load_backup_macro_block_meta_v_1_4.cpp
  table_load/backup/v_1_4/ob_table_load_backup_macro_block_reader_v_1_4.cpp
  table_load/backup/v_1_4/ob_table_load_backup_macro_block_scanner_v_1_4.cpp
  table_load/backup/v_1_4/ob_table_load_backup_micro_block_scanner_v_1_4.cpp
  table_load/backup/v_1_4/ob_table_load_backup_partition_scanner_v_1_4.cpp
  table_load/backup/v_1_4/ob_table_load_backup_row_reader_v_1_4.cpp
  table_load/backup/v_1_4/ob_table_load_backup_table_v_1_4.cpp
  table_load/backup/v_1_4/ob_table_load_backup_util_v_1_4.cpp
  table_load/backup/ob_table_load_backup_file_util.cpp
  table_load/backup/ob_table_load_backup_table.cpp
  table_load/client/ob_table_direct_load_rpc_executor.cpp
  table_load/client/ob_table_direct_load_rpc_proxy.cpp
  table_load/client/ob_table_direct_load_rpc_struct.cpp
  table_load/control/ob_table_load_control_rpc_executor.cpp
  table_load/control/ob_table_load_control_rpc_proxy.cpp
  table_load/control/ob_table_load_control_rpc_struct.cpp
  table_load/resource/ob_table_load_resource_manager.cpp
  table_load/resource/ob_table_load_resource_processor.cpp
  table_load/resource/ob_table_load_resource_rpc_executor.cpp
  table_load/resource/ob_table_load_resource_rpc_proxy.cpp
  table_load/resource/ob_table_load_resource_rpc_struct.cpp
  table_load/resource/ob_table_load_resource_service.cpp
  table_load/ob_table_load_assigned_memory_manager.cpp
  table_load/ob_table_load_assigned_task_manager.cpp
  table_load/ob_table_load_autoinc_nextval.cpp
  table_load/ob_table_load_bucket.cpp
  table_load/ob_table_load_client_service.cpp
  table_load/ob_table_load_client_task.cpp
  table_load/ob_table_load_coordinator_ctx.cpp
  table_load/ob_table_load_coordinator_trans.cpp
  table_load/ob_table_load_coordinator.cpp
  table_load/ob_table_load_error_row_handler.cpp
  table_load/ob_table_load_general_table_compactor.cpp
  table_load/ob_table_load_manager.cpp
  table_load/ob_table_load_merger.cpp
  table_load/ob_table_load_obj_cast.cpp
  table_load/ob_table_load_parallel_merge_ctx.cpp
  table_load/ob_table_load_parallel_merge_table_compactor.cpp
  table_load/ob_table_load_partition_calc.cpp
  table_load/ob_table_load_partition_location.cpp
  table_load/ob_table_load_rpc_processor.cpp
  table_load/ob_table_load_schema.cpp
  table_load/ob_table_load_service.cpp
  table_load/ob_table_load_store_ctx.cpp
  table_load/ob_table_load_store_trans.cpp
  table_load/ob_table_load_store_trans_px_writer.cpp
  table_load/ob_table_load_store.cpp
  table_load/ob_table_load_struct.cpp
  table_load/ob_table_load_table_compactor.cpp
  table_load/ob_table_load_table_ctx.cpp
  table_load/ob_table_load_task_scheduler.cpp
  table_load/ob_table_load_task.cpp
  table_load/ob_table_load_time_convert.cpp
  table_load/ob_table_load_trans_bucket_writer.cpp
  table_load/ob_table_load_trans_ctx.cpp
  table_load/ob_table_load_trans_store.cpp
  table_load/ob_table_load_utils.cpp
  table_load/ob_table_load_mem_compactor.cpp
  table_load/ob_table_load_exec_ctx.cpp
  table_load/ob_table_load_instance.cpp
  table_load/ob_table_load_multiple_heap_table_compactor.cpp
  table_load/ob_table_load_redef_table.cpp
  table_load/ob_table_load_stat.cpp
)

ob_set_subtarget(ob_server virtual_table
  virtual_table/ob_agent_table_base.cpp
  virtual_table/ob_agent_virtual_table.cpp
  virtual_table/ob_all_concurrency_object_pool.cpp
  virtual_table/ob_all_data_type_class_table.cpp
  virtual_table/ob_all_data_type_table.cpp
  virtual_table/ob_all_disk_stat.cpp
  virtual_table/ob_all_latch.cpp
  virtual_table/ob_all_plan_cache_stat.cpp
  virtual_table/ob_all_virtual_bad_block_table.cpp
  virtual_table/ob_all_virtual_compaction_diagnose_info.cpp
  virtual_table/ob_all_virtual_server_compaction_event_history.cpp
  virtual_table/ob_all_virtual_compaction_suggestion.cpp
  virtual_table/ob_all_virtual_tablet_compaction_info.cpp
  virtual_table/ob_all_virtual_dag.cpp
  virtual_table/ob_all_virtual_dag_warning_history.cpp
  virtual_table/ob_all_virtual_dblink_info.cpp
  virtual_table/ob_all_virtual_diag_index_scan.cpp
  virtual_table/ob_all_virtual_dtl_channel.cpp
  virtual_table/ob_all_virtual_dtl_first_cached_buffer.cpp
  virtual_table/ob_all_virtual_dtl_memory.cpp
  virtual_table/ob_all_virtual_dump_tenant_info.cpp
  virtual_table/ob_all_virtual_dup_ls_lease_mgr.cpp
  virtual_table/ob_all_virtual_dup_ls_tablets.cpp
  virtual_table/ob_all_virtual_dup_ls_tablet_set.cpp
  virtual_table/ob_all_virtual_engine_table.cpp
  virtual_table/ob_all_virtual_files_table.cpp
  virtual_table/ob_all_virtual_id_service.cpp
  virtual_table/ob_all_virtual_io_stat.cpp
  virtual_table/ob_all_virtual_kvcache_store_memblock.cpp
  virtual_table/ob_all_virtual_load_data_stat.cpp
  virtual_table/ob_all_virtual_lock_wait_stat.cpp
  virtual_table/ob_all_virtual_long_ops_status.cpp
  virtual_table/ob_all_virtual_tenant_snapshot_ls_replica.cpp
  virtual_table/ob_all_virtual_tenant_snapshot_ls_replica_history.cpp
  virtual_table/ob_all_virtual_ls_info.cpp
  virtual_table/ob_all_virtual_ls_snapshot.cpp
  virtual_table/ob_all_virtual_timestamp_service.cpp
  virtual_table/ob_all_virtual_tracepoint_info.cpp
  virtual_table/ob_all_virtual_transaction_freeze_checkpoint.cpp
  virtual_table/ob_all_virtual_transaction_checkpoint.cpp
  virtual_table/ob_all_virtual_checkpoint.cpp
  virtual_table/ob_all_virtual_macro_block_marker_status.cpp
  virtual_table/ob_all_virtual_tablet_buffer_info.cpp
  virtual_table/ob_all_virtual_mds_node_stat.cpp
  virtual_table/ob_all_virtual_malloc_sample_info.cpp
  virtual_table/ob_all_virtual_mds_event_history.cpp
  virtual_table/ob_all_virtual_memory_context_stat.cpp
  virtual_table/ob_all_virtual_memory_info.cpp
  virtual_table/ob_all_virtual_memstore_info.cpp
  virtual_table/ob_all_virtual_minor_freeze_info.cpp
  virtual_table/ob_all_virtual_obj_lock.cpp
  virtual_table/ob_all_virtual_storage_meta_memory_status.cpp
  virtual_table/ob_all_virtual_tablet_pointer_status.cpp
  virtual_table/ob_all_virtual_tablet_sstable_macro_info.cpp
  virtual_table/ob_all_virtual_tablet_store_stat.cpp
  virtual_table/ob_all_virtual_proxy_base.cpp
  virtual_table/ob_all_virtual_proxy_partition.cpp
  virtual_table/ob_all_virtual_proxy_partition_info.cpp
  virtual_table/ob_all_virtual_proxy_routine.cpp
  virtual_table/ob_all_virtual_proxy_schema.cpp
  virtual_table/ob_all_virtual_proxy_sub_partition.cpp
  virtual_table/ob_all_virtual_ps_item_info.cpp
  virtual_table/ob_all_virtual_ps_stat.cpp
  virtual_table/ob_all_virtual_px_target_monitor.cpp
  virtual_table/ob_all_virtual_px_worker_stat.cpp
  virtual_table/ob_all_virtual_px_p2p_datahub.cpp
  virtual_table/ob_all_virtual_dtl_interm_result_monitor.cpp
  virtual_table/ob_all_virtual_raid_stat.cpp
  virtual_table/ob_all_virtual_ls_archive_stat.cpp
  virtual_table/ob_all_virtual_server_blacklist.cpp
  virtual_table/ob_all_virtual_server_compaction_progress.cpp
  virtual_table/ob_all_virtual_tablet_stat.cpp
  virtual_table/ob_all_virtual_server_object_pool.cpp
  virtual_table/ob_all_virtual_server_schema_info.cpp
  virtual_table/ob_all_virtual_schema_memory.cpp
  virtual_table/ob_all_virtual_schema_slot.cpp
  virtual_table/ob_all_virtual_session_event.cpp
  virtual_table/ob_all_virtual_session_info.cpp
  virtual_table/ob_all_virtual_session_stat.cpp
  virtual_table/ob_all_virtual_session_wait.cpp
  virtual_table/ob_all_virtual_session_wait_history.cpp
  virtual_table/ob_all_virtual_sql_workarea_active.cpp
  virtual_table/ob_all_virtual_sql_workarea_histogram.cpp
  virtual_table/ob_all_virtual_sql_workarea_history_stat.cpp
  virtual_table/ob_all_virtual_sql_workarea_memory_info.cpp
  virtual_table/ob_all_virtual_sys_event.cpp
  virtual_table/ob_all_virtual_sys_parameter_stat.cpp
  virtual_table/ob_all_virtual_sys_stat.cpp
  virtual_table/ob_all_virtual_sys_task_status.cpp
  virtual_table/ob_all_virtual_table_mgr.cpp
  virtual_table/ob_all_virtual_unit.cpp
  virtual_table/ob_all_virtual_tablet_ddl_kv_info.cpp
  virtual_table/ob_all_virtual_ddl_sim_point_stat.cpp
  virtual_table/ob_all_virtual_tablet_info.cpp
  virtual_table/ob_all_virtual_server.cpp
  virtual_table/ob_all_virtual_tablet_compaction_history.cpp
  virtual_table/ob_all_virtual_tablet_compaction_progress.cpp
  virtual_table/ob_all_virtual_io_status.cpp
  virtual_table/ob_all_virtual_tenant_ctx_memory_info.cpp
  virtual_table/ob_all_virtual_tenant_memory_info.cpp
  virtual_table/ob_all_virtual_tenant_memstore_allocator_info.cpp
  virtual_table/ob_all_virtual_tenant_parameter_info.cpp
  virtual_table/ob_all_virtual_tenant_parameter_stat.cpp
  virtual_table/ob_all_virtual_tx_stat.cpp
  virtual_table/ob_all_virtual_tx_lock_stat.cpp
  virtual_table/ob_all_virtual_tx_scheduler_stat.cpp
  virtual_table/ob_all_virtual_tx_ctx_mgr_stat.cpp
  virtual_table/ob_all_virtual_tx_data_table.cpp
  virtual_table/ob_all_virtual_tx_data.cpp
  virtual_table/ob_all_virtual_weak_read_stat.cpp
  virtual_table/ob_all_virtual_log_stat.cpp
  virtual_table/ob_all_virtual_arbitration_member_info.cpp
  virtual_table/ob_all_virtual_arbitration_service_status.cpp
  virtual_table/ob_all_virtual_apply_stat.cpp
  virtual_table/ob_all_virtual_replay_stat.cpp
  virtual_table/ob_all_virtual_ha_diagnose.cpp
  virtual_table/ob_global_variables.cpp
  virtual_table/ob_gv_sql.cpp
  virtual_table/ob_gv_sql_audit.cpp
  virtual_table/ob_all_virtual_tenant_memstore_info.cpp
  virtual_table/ob_information_check_constraints_table.cpp
  virtual_table/ob_information_columns_table.cpp
  virtual_table/ob_information_global_status_table.cpp
  virtual_table/ob_information_kvcache_table.cpp
  virtual_table/ob_all_virtual_storage_leak_info.cpp
  virtual_table/ob_information_parameters_table.cpp
  virtual_table/ob_information_partitions_table.cpp
  virtual_table/ob_information_referential_constraints_table.cpp
  virtual_table/ob_information_schema_privileges_table.cpp
  virtual_table/ob_information_session_status_table.cpp
  virtual_table/ob_information_session_variables_table.cpp
  virtual_table/ob_information_table_constraints_table.cpp
  virtual_table/ob_information_table_privileges_table.cpp
  virtual_table/ob_information_triggers_table.cpp
  virtual_table/ob_information_user_privileges_table.cpp
  virtual_table/ob_information_query_response_time.cpp
  virtual_table/ob_information_schema_enable_roles_table.cpp
  virtual_table/ob_iterate_private_virtual_table.cpp
  virtual_table/ob_iterate_virtual_table.cpp
  virtual_table/ob_mem_leak_checker_info.cpp
  virtual_table/ob_mysql_db_table.cpp
  virtual_table/ob_mysql_proc_table.cpp
  virtual_table/ob_mysql_user_table.cpp
  virtual_table/ob_plan_cache_plan_explain.cpp
  virtual_table/ob_session_variables.cpp
  virtual_table/ob_show_create_database.cpp
  virtual_table/ob_show_create_procedure.cpp
  virtual_table/ob_show_create_table.cpp
  virtual_table/ob_show_create_tablegroup.cpp
  virtual_table/ob_show_create_trigger.cpp
  virtual_table/ob_show_database_status.cpp
  virtual_table/ob_show_grants.cpp
  virtual_table/ob_show_processlist.cpp
  virtual_table/ob_show_tenant_status.cpp
  virtual_table/ob_table_columns.cpp
  virtual_table/ob_table_index.cpp
  virtual_table/ob_tenant_all_tables.cpp
  virtual_table/ob_tenant_show_restore_preview.cpp
  virtual_table/ob_tenant_show_tables.cpp
  virtual_table/ob_tenant_virtual_charset.cpp
  virtual_table/ob_tenant_virtual_privilege.cpp
  virtual_table/ob_tenant_virtual_collation.cpp
  virtual_table/ob_tenant_virtual_concurrent_limit_sql.cpp
  virtual_table/ob_tenant_virtual_current_tenant.cpp
  virtual_table/ob_tenant_virtual_event_name.cpp
  virtual_table/ob_tenant_virtual_get_object_definition.cpp
  virtual_table/ob_tenant_virtual_outline.cpp
  virtual_table/ob_tenant_virtual_statname.cpp
  virtual_table/ob_tenant_virtual_warning.cpp
  virtual_table/ob_virtual_data_access_service.cpp
  virtual_table/ob_virtual_obrpc_send_stat.cpp
  virtual_table/ob_virtual_open_cursor_table.cpp
  virtual_table/ob_virtual_proxy_server_stat.cpp
  virtual_table/ob_virtual_proxy_sys_variable.cpp
  virtual_table/ob_virtual_sql_monitor.cpp
  virtual_table/ob_virtual_sql_monitor_statname.cpp
  virtual_table/ob_virtual_sql_plan_monitor.cpp
  virtual_table/ob_virtual_ash.cpp
  virtual_table/ob_virtual_sql_plan_statistics.cpp
  virtual_table/ob_virtual_table_iterator_factory.cpp
  virtual_table/ob_all_virtual_dml_stats.cpp
  virtual_table/ob_all_virtual_archive_dest_status.cpp
  virtual_table/ob_all_virtual_ls_log_restore_status.cpp
  virtual_table/ob_virtual_span_info.cpp
  virtual_table/ob_virtual_show_trace.cpp
  virtual_table/ob_all_virtual_sql_plan.cpp
  virtual_table/ob_all_virtual_opt_stat_gather_monitor.cpp
  virtual_table/ob_all_virtual_thread.cpp
  virtual_table/ob_all_virtual_cgroup_config.cpp
  virtual_table/ob_virtual_flt_config.cpp
  virtual_table/ob_all_virtual_kv_connection.cpp
  virtual_table/ob_all_virtual_tenant_resource_limit.cpp
  virtual_table/ob_all_virtual_tenant_resource_limit_detail.cpp
  virtual_table/ob_all_virtual_activity_metrics.cpp
  virtual_table/ob_all_virtual_checkpoint_diagnose_info.cpp
  virtual_table/ob_all_virtual_checkpoint_diagnose_memtable_info.cpp
  virtual_table/ob_all_virtual_nic_info.cpp
  virtual_table/ob_all_virtual_storage_ha_error_diagnose.cpp
  virtual_table/ob_all_virtual_storage_ha_perf_diagnose.cpp
  virtual_table/ob_all_virtual_sys_variable_default_value.cpp
  virtual_table/ob_all_virtual_tenant_scheduler_running_job.cpp
  virtual_table/ob_all_virtual_compatibility_control.cpp
)

ob_server_add_target(ob_server)

set_source_files_properties(table/htable_filter_lex.cxx PROPERTIES COMPILE_FLAGS -Wno-null-conversion)

add_library(oceanbase_static
  STATIC
  EXCLUDE_FROM_ALL
  ${CMAKE_CURRENT_BINARY_DIR}/ob_version.cpp)

target_link_libraries(oceanbase_static
  PUBLIC ob_base "${ob_objects}" ob_sql_static ob_storage_static ob_share_static ${ob_close_modules_static_name} oblib objit)

if (OB_GPERF_MODE)
  target_link_libraries(oceanbase_static
  PRIVATE libprofiler.a)
endif()

if (OB_SO_CACHE)
  add_library(oceanbase SHARED IMPORTED GLOBAL)
  set_target_properties(oceanbase PROPERTIES
    IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/liboceanbase.so"
    IMPORTED_LINK_INTERFACE_LIBRARIES ob_base)
else()
  add_library(oceanbase
    SHARED
    EXCLUDE_FROM_ALL
    ${CMAKE_CURRENT_BINARY_DIR}/ob_version.cpp)
  target_link_libraries(oceanbase
    PUBLIC ob_base
    PRIVATE
    -Wl,-z,notext
    -Wl,--whole-archive
    "${ob_objects}" "${oblib_object_libraries}"
    ob_sql_static
    ob_storage_static
    ob_share_static
    ${ob_close_modules_static_name}
    -Wl,--no-whole-archive
    -static-libgcc -static-libstdc++
    objit)
endif()

if (NOT OB_USE_ASAN)
  set(link_malloc_hook malloc_hook)
endif()

add_library(ob_main OBJECT main.cpp)
config_ccls_flag(ob_main)
target_link_libraries(ob_main PUBLIC ob_base)
add_executable(observer)
target_link_libraries(observer
  PRIVATE
  -Wl,--start-group
  ob_main
  ob_malloc
  oceanbase_static
  ob_sql_static
  ob_storage_static
  ob_share_static
  ${ob_close_modules_static_name}
  -Wl,--end-group
  -static-libgcc
  -static-libstdc++
  ${link_malloc_hook}
)

execute_process(
  COMMAND sh -c "cd ${CMAKE_SOURCE_DIR} && git rev-parse HEAD"
  OUTPUT_STRIP_TRAILING_WHITESPACE
  OUTPUT_VARIABLE GIT_REVISION)
execute_process(
  COMMAND sh -c "cd ${CMAKE_SOURCE_DIR} && git rev-parse --abbrev-ref HEAD"
  OUTPUT_STRIP_TRAILING_WHITESPACE
  OUTPUT_VARIABLE GIT_BRANCH)
if (NOT CMAKE_BUILD_TYPE)
  set(BUILD_FLAGS "default" CACHE INTERNAL "build flags")
else()
  set(BUILD_FLAGS "${CMAKE_BUILD_TYPE}" CACHE INTERNAL "build flags")
endif()

if (OB_ENABLE_BOLT)
  add_custom_command(
    OUTPUT observer_bolt
    COMMAND ${CMAKE_SOURCE_DIR}/cmake/script/do_bolt_opt --build_dir=${CMAKE_BINARY_DIR}
    DEPENDS observer)
  add_custom_target(fake_custom_target ALL
    DEPENDS observer_bolt)
endif()

configure_file(../share/ob_version.cpp.in ob_version.cpp)
