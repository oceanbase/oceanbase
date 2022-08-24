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
  mysql/obmp_ping.cpp
  mysql/obmp_query.cpp
  mysql/obmp_statistic.cpp
  mysql/obmp_stmt_close.cpp
  mysql/obmp_stmt_execute.cpp
  mysql/obmp_stmt_prepare.cpp
  mysql/obmp_stmt_send_long_data.cpp
  mysql/obmp_stmt_send_long_data.h
  mysql/obmp_utils.cpp
  mysql/obsm_handler.cpp
  mysql/obsm_row.cpp
  mysql/obsm_utils.cpp
)

ob_set_subtarget(ob_server common
  ob_cache_size_calculator.cpp
  ob_heartbeat.cpp
  ob_index_status_reporter.cpp
  ob_inner_sql_connection.cpp
  ob_inner_sql_connection_pool.cpp
  ob_inner_sql_read_context.cpp
  ob_inner_sql_result.cpp
  ob_lease_state_mgr.cpp
  ob_partition_table_checker.cpp
  ob_partition_table_updater.cpp
  ob_partition_location_updater.cpp
  ob_rebuild_flag_reporter.cpp
  ob_root_service_monitor.cpp
  ob_rpc_extra_payload.cpp
  ob_rpc_processor_simple.cpp
  ob_server_memory_cutter.cpp
  ob_server.cpp
  ob_server_event_history_table_operator.cpp
  ob_server_reload_config.cpp
  ob_server_schema_updater.cpp
  ob_server_struct.cpp
  ob_server_utils.cpp
  ob_service.cpp
  ob_signal_handle.cpp
  ob_dump_task_generator.cpp
  ob_sql_client_decorator.cpp
  ob_srv_deliver.cpp
  ob_srv_network_frame.cpp
  ob_rpc_intrusion_detect.cpp
  ob_srv_xlator_partition.cpp
  ob_srv_xlator_primary.cpp
  ob_srv_xlator_rootserver.cpp
  ob_srv_xlator_storage.cpp
  ob_srv_xlator.cpp
  ob_sstable_checksum_updater.cpp
  ob_pg_partition_meta_table_updater.cpp
  ob_tenant_duty_task.cpp
  ob_uniq_task_queue.cpp
  ob_req_time_service.cpp
)

ob_set_subtarget(ob_server omt
  omt/ob_multi_level_queue.cpp
  omt/ob_retry_queue.cpp
  omt/ob_cgroup_ctrl.cpp
  omt/ob_multi_tenant.cpp
  omt/ob_tenant.cpp
  omt/ob_tenant_node_balancer.cpp
  omt/ob_th_worker.cpp
  omt/ob_token_calcer.cpp
  omt/ob_worker_pool.cpp
  omt/ob_worker_processor.cpp
  omt/ob_tenant_config.cpp
  omt/ob_tenant_config_mgr.cpp
  omt/ob_tenant_timezone.cpp
  omt/ob_tenant_timezone_mgr.cpp
)

ob_set_subtarget(ob_server vt
  virtual_table/ob_agent_table_base.cpp
  virtual_table/ob_agent_virtual_table.cpp
  virtual_table/ob_all_concurrency_object_pool.cpp
  virtual_table/ob_all_data_type_class_table.cpp
  virtual_table/ob_all_data_type_table.cpp
  virtual_table/ob_all_disk_stat.cpp
  virtual_table/ob_all_latch.cpp
  virtual_table/ob_all_plan_cache_stat.cpp
  virtual_table/ob_all_virtual_ps_stat.cpp
  virtual_table/ob_all_virtual_ps_item_info.cpp
  virtual_table/ob_all_storage_stat.cpp
  virtual_table/ob_all_virtual_dtl_channel.cpp
  virtual_table/ob_all_virtual_dtl_memory.cpp
  virtual_table/ob_all_virtual_dtl_first_cached_buffer.cpp
  virtual_table/ob_all_virtual_sql_workarea_history_stat.cpp
  virtual_table/ob_all_virtual_sql_workarea_active.cpp
  virtual_table/ob_all_virtual_sql_workarea_histogram.cpp
  virtual_table/ob_all_virtual_sql_workarea_memory_info.cpp
  virtual_table/ob_all_virtual_bad_block_table.cpp
  virtual_table/ob_all_virtual_clog_stat.cpp
  virtual_table/ob_all_virtual_diag_index_scan.cpp
  virtual_table/ob_all_virtual_duplicate_partition_mgr_stat.cpp
  virtual_table/ob_all_virtual_election_group_info.cpp
  virtual_table/ob_all_virtual_election_info.cpp
  virtual_table/ob_all_virtual_election_mem_stat.cpp
  virtual_table/ob_all_virtual_election_event_history.cpp
  virtual_table/ob_all_virtual_election_priority.cpp
  virtual_table/ob_all_virtual_engine_table.cpp
  virtual_table/ob_all_virtual_files_table.cpp
  virtual_table/ob_all_virtual_io_stat.cpp
  virtual_table/ob_all_virtual_lock_wait_stat.cpp
  virtual_table/ob_all_virtual_long_ops_status.cpp
  virtual_table/ob_all_virtual_macro_block_marker_status.cpp
  virtual_table/ob_all_virtual_memory_info.cpp
  virtual_table/ob_all_virtual_memstore_info.cpp
  virtual_table/ob_all_virtual_partition_amplification_stat.cpp
  virtual_table/ob_all_virtual_partition_audit.cpp
  virtual_table/ob_all_virtual_partition_item.cpp
  virtual_table/ob_all_virtual_partition_location.cpp
  virtual_table/ob_all_virtual_partition_migration_status.cpp
  virtual_table/ob_all_virtual_partition_replay_status.cpp
  virtual_table/ob_all_virtual_partition_split_info.cpp
  virtual_table/ob_all_virtual_partition_sstable_macro_info.cpp
  virtual_table/ob_all_virtual_partition_sstable_merge_info.cpp
  virtual_table/ob_all_virtual_partition_store_info.cpp
  virtual_table/ob_all_virtual_partition_table_store_stat.cpp
  virtual_table/ob_all_virtual_memory_context_stat.cpp
  virtual_table/ob_all_virtual_dump_tenant_info.cpp
  virtual_table/ob_all_virtual_proxy_base.cpp
  virtual_table/ob_all_virtual_proxy_partition.cpp
  virtual_table/ob_all_virtual_proxy_partition_info.cpp
  virtual_table/ob_all_virtual_proxy_route.cpp
  virtual_table/ob_all_virtual_proxy_schema.cpp
  virtual_table/ob_all_virtual_proxy_sub_partition.cpp
  virtual_table/ob_all_virtual_px_worker_stat.cpp
  virtual_table/ob_all_virtual_server_blacklist.cpp
  virtual_table/ob_all_virtual_server_clog_stat.cpp
  virtual_table/ob_all_virtual_server_memory_info.cpp
  virtual_table/ob_all_virtual_server_object_pool.cpp
  virtual_table/ob_all_virtual_server_schema_info.cpp
  virtual_table/ob_all_virtual_session_event.cpp
  virtual_table/ob_all_virtual_session_stat.cpp
  virtual_table/ob_all_virtual_session_wait.cpp
  virtual_table/ob_all_virtual_session_wait_history.cpp
  virtual_table/ob_all_virtual_sys_event.cpp
  virtual_table/ob_all_virtual_sys_parameter_stat.cpp
  virtual_table/ob_all_virtual_sys_stat.cpp
  virtual_table/ob_all_virtual_sys_task_status.cpp
  virtual_table/ob_all_virtual_table_mgr.cpp
  virtual_table/ob_all_virtual_raid_stat.cpp
  virtual_table/ob_all_virtual_tenant_disk_stat.cpp
  virtual_table/ob_all_virtual_tenant_memstore_allocator_info.cpp
  virtual_table/ob_all_virtual_trans_audit.cpp
  virtual_table/ob_all_virtual_weak_read_stat.cpp
  virtual_table/ob_all_virtual_trans_lock_stat.cpp
  virtual_table/ob_all_virtual_trans_mem_stat.cpp
  virtual_table/ob_all_virtual_trans_mgr_stat.cpp
  virtual_table/ob_all_virtual_trans_result_info.cpp
  virtual_table/ob_all_virtual_trans_sql_audit.cpp
  virtual_table/ob_all_virtual_trans_stat.cpp
  virtual_table/ob_all_virtual_zone_stat.cpp
  virtual_table/ob_global_variables.cpp
  virtual_table/ob_gv_partition_info.cpp
  virtual_table/ob_all_virtual_pg_partition_info.cpp
  virtual_table/ob_gv_sql.cpp
  virtual_table/ob_gv_sql_audit.cpp
  virtual_table/ob_gv_tenant_memstore_info.cpp
  virtual_table/ob_information_columns_table.cpp
  virtual_table/ob_information_global_status_table.cpp
  virtual_table/ob_information_kvcache_table.cpp
  virtual_table/ob_information_parameters_table.cpp
  virtual_table/ob_information_partitions_table.cpp
  virtual_table/ob_information_schema_privileges_table.cpp
  virtual_table/ob_information_session_status_table.cpp
  virtual_table/ob_information_session_variables_table.cpp
  virtual_table/ob_information_referential_constraints_table.cpp
  virtual_table/ob_information_table_constraints_table.cpp
  virtual_table/ob_information_table_privileges_table.cpp
  virtual_table/ob_information_user_privileges_table.cpp
  virtual_table/ob_information_query_response_time.cpp
  virtual_table/ob_iterate_virtual_table.cpp
  virtual_table/ob_mem_leak_checker_info.cpp
  virtual_table/ob_mysql_db_table.cpp
  virtual_table/ob_mysql_proc_table.cpp
  virtual_table/ob_mysql_user_table.cpp
  virtual_table/ob_partition_sstable_image_info_table.cpp
  virtual_table/ob_plan_cache_plan_explain.cpp
  virtual_table/ob_session_variables.cpp
  virtual_table/ob_show_create_database.cpp
  virtual_table/ob_show_create_table.cpp
  virtual_table/ob_show_create_tablegroup.cpp
  virtual_table/ob_show_database_status.cpp
  virtual_table/ob_show_grants.cpp
  virtual_table/ob_show_interm_result.cpp
  virtual_table/ob_show_processlist.cpp
  virtual_table/ob_show_tenant_status.cpp
  virtual_table/ob_table_columns.cpp
  virtual_table/ob_table_index.cpp
  virtual_table/ob_tenant_all_tables.cpp
  virtual_table/ob_tenant_partition_stat.cpp
  virtual_table/ob_tenant_show_tables.cpp
  virtual_table/ob_tenant_show_tables.h
  virtual_table/ob_tenant_show_restore_preview.cpp
  virtual_table/ob_tenant_show_restore_preview.h
  virtual_table/ob_tenant_virtual_charset.cpp
  virtual_table/ob_tenant_virtual_collation.cpp
  virtual_table/ob_tenant_virtual_concurrent_limit_sql.cpp
  virtual_table/ob_tenant_virtual_current_tenant.cpp
  virtual_table/ob_tenant_virtual_event_name.cpp
  virtual_table/ob_tenant_virtual_get_object_definition.cpp
  virtual_table/ob_tenant_virtual_outline.cpp
  virtual_table/ob_tenant_virtual_statname.cpp
  virtual_table/ob_tenant_virtual_warning.cpp
  virtual_table/ob_virtual_table_iterator_factory.cpp
  virtual_table/ob_virtual_data_access_service.cpp
  virtual_table/ob_virtual_obrpc_send_stat.cpp
  virtual_table/ob_virtual_proxy_server_stat.cpp
  virtual_table/ob_virtual_proxy_sys_variable.cpp
  virtual_table/ob_virtual_sql_monitor.cpp
  virtual_table/ob_virtual_sql_plan_monitor.cpp
  virtual_table/ob_virtual_sql_monitor_statname.cpp
  virtual_table/ob_virtual_sql_plan_statistics.cpp
  virtual_table/ob_virtual_trace_log.cpp
  virtual_table/ob_all_virtual_tenant_parameter_stat.cpp
  virtual_table/ob_all_virtual_tenant_parameter_info.cpp
  virtual_table/ob_all_virtual_deadlock_stat.cpp
  virtual_table/ob_all_virtual_pg_backup_log_archive_status.cpp
  virtual_table/ob_all_virtual_server_backup_log_archive_status.cpp
  virtual_table/ob_all_virtual_pg_log_archive_stat.cpp
  virtual_table/ob_all_virtual_table_modifications.cpp
  virtual_table/ob_all_virtual_timestamp_service.cpp
  virtual_table/ob_all_virtual_trans_table_status.cpp
  virtual_table/ob_virtual_open_cursor_table.cpp
  virtual_table/ob_all_virtual_reserved_table_mgr.cpp
  virtual_table/ob_all_virtual_dag_warning_history.cpp
  virtual_table/ob_all_virtual_backupset_history_mgr.cpp
  virtual_table/ob_all_virtual_backup_clean_info.h
  virtual_table/ob_all_virtual_backup_clean_info.cpp
)

ob_set_subtarget(ob_server table
  table/ob_table_batch_execute_processor.cpp		
  table/ob_table_end_trans_cb.cpp		
  table/ob_table_execute_processor.cpp		
  table/ob_table_query_processor.cpp		
  table/ob_table_rpc_processor.cpp		
  table/ob_table_service.cpp		
  table/ob_table_api_row_iterator.cpp
  table/ob_table_query_and_mutate_processor.cpp
  table/ob_htable_filter_operator.cpp
  table/ob_htable_filter_parser.cpp
  table/ob_htable_utils.cpp
  table/ob_htable_filters.cpp
  table/htable_filter_tab.cxx
  table/htable_filter_lex.cxx
  table/ob_table_query_sync_processor.cpp
  table/ob_table_ttl_manager.cpp
  table/ob_table_ttl_task.cpp
)

set_source_files_properties(table/htable_filter_lex.cxx PROPERTIES COMPILE_FLAGS -Wno-null-conversion)

ob_server_add_pchs(observer
  ob_server_struct.h
  ob_uniq_task_queue.h
  mysql/ob_ra_queue.h
  mysql/ob_mysql_end_trans_cb.h
)

ob_server_add_target(ob_server)

ob_server_add_target(ob_version
${CMAKE_CURRENT_BINARY_DIR}/ob_version.cpp
)

add_library(oceanbase_static
  STATIC
  EXCLUDE_FROM_ALL)

target_link_libraries(oceanbase_static
  PUBLIC ob_version ob_base "${ob_objects}" ob_sql_server_parser_objects oblib easy)

if (OB_GPERF_MODE)
  target_link_libraries(oceanbase_static
  PRIVATE libprofiler.a)
endif()

set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -lmariadb -laio")

if (OB_SO_CACHE)
  add_library(oceanbase SHARED IMPORTED GLOBAL)
  set_target_properties(oceanbase PROPERTIES
    IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/liboceanbase.so")
  target_link_libraries(oceanbase
    INTERFACE
    ob_base
    ${LGPL_DEPS}
  )
else()
  add_library(oceanbase
    SHARED
    EXCLUDE_FROM_ALL)
  target_link_libraries(oceanbase
    PUBLIC
    ob_base
    ${LGPL_DEPS}
    PRIVATE
    -Wl,-z,notext
    "${ob_objects}" "${oblib_object_libraries}"
    -static-libgcc
    -static-libstdc++
    ob_version
    ob_sql_server_parser_objects
    oblib easy)
endif()

ob_add_target(ob_main main.cpp)
target_link_libraries(ob_main PUBLIC ob_base)
enable_server_pch(ob_main)

if (OB_STATIC_LINK_LGPL_DEPS)
  set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -l:libmariadbclient.a -l:libaio.a")
endif()

add_executable(observer)

if (NOT OB_USE_ASAN) 
  set(link_malloc_hook malloc_hook)
endif()

target_link_libraries(observer
  PRIVATE
  ob_main
  -Wl,--start-group
  oceanbase_static
  ${link_malloc_hook}
  ${LGPL_DEPS}
  oblib
  easy
  -Wl,--end-group
  -static-libgcc
  -static-libstdc++
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
configure_file(../share/ob_version.cpp.in ob_version.cpp)
