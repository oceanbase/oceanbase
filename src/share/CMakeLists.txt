ob_set_subtarget(ob_share allocator
  allocator/ob_fifo_arena.cpp
  allocator/ob_gmemstore_allocator.cpp
  allocator/ob_handle_list.cpp
  allocator/ob_memstore_allocator_mgr.cpp
  allocator/ob_tenant_mutil_allocator.cpp
  allocator/ob_tenant_mutil_allocator_mgr.cpp
)

ob_set_subtarget(ob_share cache
  cache/ob_kv_storecache.cpp
  cache/ob_kvcache_inst_map.cpp
  cache/ob_kvcache_map.cpp
  cache/ob_kvcache_store.cpp
  cache/ob_kvcache_struct.cpp
  cache/ob_working_set_mgr.cpp
)

ob_set_subtarget(ob_share client_feedback
  client_feedback/ob_client_feedback_basic.cpp
  client_feedback/ob_client_feedback_manager.cpp
  client_feedback/ob_feedback_int_struct.cpp
  client_feedback/ob_feedback_partition_struct.cpp
)

ob_set_subtarget(ob_share config
  config/ob_common_config.cpp
  config/ob_config.cpp
  config/ob_config_helper.cpp
  config/ob_config_manager.cpp
  config/ob_reload_config.cpp
  config/ob_server_config.cpp
  config/ob_system_config.cpp
  config/ob_system_config_key.cpp
)

file(GLOB SCHEMA_CPPS "inner_table/ob_inner_table_schema.*.cpp")
ob_set_subtarget(ob_share inner_table
  inner_table/ob_inner_table_schema_misc.ipp
  ${SCHEMA_CPPS}
)

ob_set_subtarget(ob_share common
  ob_alive_server_tracer.cpp
  ob_cluster_switchover_info.cpp
  ob_cluster_sync_status.cpp
  ob_cluster_info_proxy.cpp
  ob_autoincrement_param.cpp
  ob_autoincrement_service.cpp
  ob_cascad_member.cpp
  ob_cascad_member_list.cpp
  ob_cluster_version.cpp
  ob_core_table_proxy.cpp
  ob_ddl_task_executor.cpp
  ob_debug_sync.cpp
  ob_dml_sql_splicer.cpp
  ob_encryption_util.cpp
  ob_encrypt_kms.cpp
  ob_errno.cpp
  ob_event_history_table_operator.cpp
  ob_freeze_info_proxy.cpp
  ob_get_compat_mode.cpp
  ob_global_stat_proxy.cpp
  ob_i_data_access_service.cpp
  ob_index_build_stat.cpp
  ob_index_builder_util.cpp
  ob_index_checksum.cpp
  ob_index_status_table_operator.cpp
  ob_index_task_table_operator.cpp
  ob_index_trans_status_reporter.cpp
  ob_inner_config_root_addr.cpp
  ob_kv_parser.cpp
  ob_leader_election_waiter.cpp
  ob_lease_struct.cpp
  ob_list_parser.cpp
  ob_locality_info.cpp
  ob_locality_parser.cpp
  ob_locality_priority.cpp
  ob_locality_table_operator.cpp
  ob_max_id_fetcher.cpp
  ob_multi_cluster_util.cpp
  ob_partition_modify.cpp
  ob_primary_zone_util.cpp
  ob_remote_sql_proxy.cpp
  ob_remote_server_provider.cpp
  ob_replica_info.cpp
  ob_rpc_share.cpp
  ob_debug_sync_point.cpp
  ob_rpc_struct.cpp
  ob_rs_mgr.cpp
  ob_scanner.cpp
  ob_schema_status_proxy.cpp
  ob_server_blacklist.h ob_server_blacklist.cpp
  ob_server_locality_cache.cpp
  ob_server_status.cpp
  ob_simple_batch.cpp
  ob_snapshot_table_proxy.cpp
  ob_sstable_checksum_operator.cpp
  ob_pg_partition_meta_table_operator.cpp
  ob_task_define.cpp
  ob_tenant_memstore_info_operator.cpp
  ob_tenant_mgr.cpp
  ob_time_utility2.cpp
  ob_time_zone_info_manager.cpp
  ob_unique_index_row_transformer.cpp
  ob_unit_getter.cpp
  ob_unit_stat_table_operator.cpp
  ob_unit_table_operator.cpp
  ob_gts_info.cpp
  ob_gts_table_operator.cpp
  ob_upgrade_utils.cpp
  ob_virtual_table_iterator.cpp
  ob_virtual_table_projector.cpp
  ob_virtual_table_scanner_iterator.cpp
  ob_web_service_root_addr.cpp
  ob_worker.cpp
  ob_zone_info.cpp
  ob_zone_table_operation.cpp
  ob_replica_wrs_info.cpp
  ob_thread_mgr.cpp
  ob_cluster_type.cpp
  ob_proposal_id.cpp
  ob_sstable_checksum_iterator.cpp
  ob_priv_common.cpp
  ob_bg_thread_monitor.cpp
)

ob_set_subtarget(ob_share object
  object/ob_obj_cast.cpp
)

ob_set_subtarget(ob_share datum
  datum/ob_datum.cpp
  datum/ob_datum_funcs.cpp
)

ob_set_subtarget(ob_share part
  part/ob_part_mgr_ad.cpp
)

ob_set_subtarget(ob_share partition_table
  partition_table/ob_united_pt_operator.cpp
  partition_table/ob_inmemory_partition_table.cpp
  partition_table/ob_location_update_task.cpp
  partition_table/ob_partition_info.cpp
  partition_table/ob_partition_location.cpp
  partition_table/ob_partition_location_cache.cpp
  partition_table/ob_partition_table_iterator.cpp
  partition_table/ob_partition_table_operator.cpp
  partition_table/ob_partition_table_proxy.cpp
  partition_table/ob_persistent_partition_table.cpp
  partition_table/ob_remote_partition_table_operator.cpp
  partition_table/ob_replica_filter.cpp
  partition_table/ob_rpc_partition_table.cpp
  partition_table/ob_tenant_partition_container.cpp
)

ob_set_subtarget(ob_share rc
  rc/ob_context.cpp
  rc/ob_tenant_base.cpp
)

ob_set_subtarget(ob_share redolog
  redolog/ob_log_disk_manager.cpp
  redolog/ob_log_file_reader.cpp
  redolog/ob_log_file_store.cpp
  redolog/ob_log_store_factory.cpp
)

ob_set_subtarget(ob_share diagnosis
  diagnosis/ob_sql_plan_monitor_node_list.cpp
  diagnosis/ob_sql_monitor_statname.cpp
)

ob_set_subtarget(ob_share restore
  restore/ob_restore_args.cpp
  restore/ob_restore_base_reader.cpp
  restore/ob_restore_uri_parser.cpp
)

ob_set_subtarget(ob_share rpc
  rpc/ob_batch_processor.cpp
  rpc/ob_batch_proxy.cpp
  rpc/ob_batch_rpc.cpp
  rpc/ob_blacklist_proxy.cpp
  rpc/ob_blacklist_req_processor.cpp
  rpc/ob_blacklist_resp_processor.cpp
)

ob_set_subtarget(ob_share scheduler
  scheduler/ob_dag_scheduler.cpp
  scheduler/ob_sys_task_stat.cpp
)

ob_set_subtarget(ob_share schema
  schema/ob_column_schema.cpp
  schema/ob_constraint.cpp
  schema/ob_database_sql_service.cpp
  schema/ob_dblink_mgr.cpp
  schema/ob_dblink_sql_service.cpp
  schema/ob_ddl_sql_service.cpp
  schema/ob_multi_version_schema_service.cpp
  schema/ob_outline_mgr.cpp
  schema/ob_outline_sql_service.cpp
  schema/ob_part_mgr_util.cpp
  schema/ob_partition_sql_helper.cpp
  schema/ob_priv_mgr.cpp
  schema/ob_priv_sql_service.cpp
  schema/ob_profile_sql_service.cpp
  schema/ob_profile_mgr.cpp
  schema/ob_schema_store.cpp
  schema/ob_schema_cache.cpp
  schema/ob_schema_getter_guard.cpp
  schema/ob_schema_macro_define.cpp
  schema/ob_schema_mem_mgr.cpp
  schema/ob_schema_mgr.cpp
  schema/ob_schema_mgr_cache.cpp
  schema/ob_schema_printer.cpp
  schema/ob_schema_retrieve_utils.ipp
  schema/ob_schema_service.cpp
  schema/ob_schema_service_rpc_proxy.cpp
  schema/ob_schema_service_sql_impl.cpp
  schema/ob_schema_struct.cpp
  schema/ob_schema_utils.cpp
  schema/ob_sequence_mgr.cpp
  schema/ob_sequence_sql_service.cpp
  schema/ob_server_schema_service.cpp
  schema/ob_synonym_mgr.cpp
  schema/ob_synonym_sql_service.cpp
  schema/ob_sys_variable_mgr.cpp
  schema/ob_sys_variable_sql_service.cpp
  schema/ob_table_dml_param.cpp
  schema/ob_table_iter.cpp
  schema/ob_table_param.cpp
  schema/ob_table_schema.cpp
  schema/ob_table_sql_service.cpp
  schema/ob_tablegroup_sql_service.cpp
  schema/ob_tenant_sql_service.cpp
  schema/ob_udf.cpp
  schema/ob_udf_mgr.cpp
  schema/ob_udf_sql_service.cpp
  schema/ob_user_sql_service.cpp
  schema/ob_error_info.cpp
  schema/ob_dependency_info.cpp
)

ob_set_subtarget(ob_share sequence
  sequence/ob_sequence_cache.cpp
  sequence/ob_sequence_ddl_proxy.cpp
  sequence/ob_sequence_dml_proxy.cpp
  sequence/ob_sequence_option.cpp
  sequence/ob_sequence_option_builder.cpp
)

ob_set_subtarget(ob_share resource_manager
  resource_manager/ob_resource_manager_proxy.cpp
  resource_manager/ob_resource_manager.cpp
  resource_manager/ob_resource_plan_manager.cpp
  resource_manager/ob_resource_mapping_rule_manager.cpp
  resource_manager/ob_resource_plan_info.cpp
)

ob_set_subtarget(ob_share stat
  stat/ob_col_stat_sql_service.cpp
  stat/ob_column_stat.cpp
  stat/ob_column_stat_cache.cpp
  stat/ob_opt_column_stat.cpp
  stat/ob_opt_column_stat_cache.cpp
  stat/ob_opt_stat_manager.cpp
  stat/ob_opt_stat_service.cpp
  stat/ob_opt_stat_sql_service.cpp
  stat/ob_opt_table_stat.cpp
  stat/ob_opt_table_stat_cache.cpp
  stat/ob_stat_manager.cpp
  stat/ob_table_stat_cache.cpp
  stat/ob_user_tab_col_statistics.cpp
)

ob_set_subtarget(ob_share variable
  system_variable/ob_system_variable.cpp
  system_variable/ob_system_variable_factory.cpp
  system_variable/ob_system_variable_init.cpp
  parameter/ob_parameter_seed.ipp
  parameter/ob_parameter_attr.cpp
)

ob_set_subtarget(ob_share backup
  backup/ob_backup_struct.cpp
  backup/ob_backup_info_mgr.cpp
  backup/ob_backup_manager.cpp
  backup/ob_backup_operator.cpp
  backup/ob_backup_path.cpp
  backup/ob_log_archive_backup_info_mgr.cpp
  backup/ob_backup_scheduler.cpp
  backup/ob_pg_backup_task_updater.cpp
  backup/ob_tenant_backup_task_updater.cpp
  backup/ob_extern_backup_info_mgr.cpp
  backup/ob_physical_restore_info.cpp
  backup/ob_physical_restore_table_operator.cpp
  backup/ob_backup_file_lock_mgr.cpp
  backup/ob_backup_lease_info_mgr.cpp
  backup/ob_tenant_name_mgr.cpp
  backup/ob_tenant_backup_clean_info_updater.cpp
  backup/ob_validate_scheduler.cpp
  backup/ob_validate_task_updater.cpp
  backup/ob_tenant_validate_task_updater.cpp
  backup/ob_pg_validate_task_updater.cpp
  backup/ob_validate_operator.cpp
)

ob_set_subtarget(ob_share interrupt
  interrupt/ob_global_interrupt_call.cpp
  interrupt/ob_interrupt_rpc_proxy.cpp
)

ob_server_add_pchs(share
  scheduler/ob_sys_task_stat.h
  scheduler/ob_dag_scheduler.h
  ob_errno.h
  mysql_errno.h
  ob_worker.h
  ob_define.h
  object/ob_obj_cast.h
  ob_encryption_util.h
  schema/ob_sys_priv_type.h
  schema/ob_priv_type.h
  schema/ob_obj_priv_type.h
  system_variable/ob_sys_var_class_type.h
  partition_table/ob_partition_info.h
  system_variable/ob_system_variable_init.h
  system_variable/ob_system_variable.h
  system_variable/ob_system_variable_factory.h
  system_variable/ob_system_variable_alias.h
  sequence/ob_sequence_option.h
  ob_replica_info.h
  ob_priv_common.h
  ob_max_id_fetcher.h
  schema/ob_table_param.h
  schema/ob_schema_struct.h
  ob_get_compat_mode.h
  ob_duplicate_scope_define.h
  parameter/ob_parameter_attr.h
  config/ob_system_config_key.h
  config/ob_config_helper.h
  config/ob_config.h
  config/ob_system_config_value.h
  config/ob_system_config.h
  parameter/ob_parameter_macro.h
  config/ob_server_config.h
  config/ob_common_config.h
  ob_cluster_version.h
  cache/ob_kvcache_struct.h
  ob_i_tenant_mgr.h
  cache/ob_kvcache_inst_map.h
  cache/ob_cache_utils.h
  cache/ob_kvcache_store.h
  inner_table/ob_inner_table_schema_constants.h
  cache/ob_working_set_mgr.h
  cache/ob_kvcache_map.h
  cache/ob_kv_storecache.h
  inner_table/ob_inner_table_schema.h
  schema/ob_udf.h
  schema/ob_constraint.h
  ob_storage_format.h
  schema/ob_table_schema.h
  schema/ob_column_schema.h
  ob_task_define.h
  schema/ob_schema_service.h
  schema/ob_udf_mgr.h
  schema/ob_sys_variable_mgr.h
  schema/ob_synonym_mgr.h
  schema/ob_sequence_mgr.h
  schema/ob_schema_mgr.h
  schema/ob_schema_mgr_cache.h
  schema/ob_profile_mgr.h
  schema/ob_priv_mgr.h
  schema/ob_outline_mgr.h
  schema/ob_dblink_mgr.h
  schema/ob_schema_store.h
  schema/ob_schema_mem_mgr.h
  schema/ob_schema_cache.h
  part/ob_part_mgr.h
  schema/ob_server_schema_service.h
  schema/ob_schema_getter_guard.h
  schema/ob_multi_version_schema_service.h
  ob_i_sql_expression.h
  datum/ob_datum.h
  ob_force_print_log.h
  backup/ob_backup_struct.h
  ob_alive_server_tracer.h
  diagnosis/ob_sql_monitor_statname.h
  diagnosis/ob_sql_plan_monitor_node_list.h
  ob_i_data_access_service.h
  ob_core_table_proxy.h
  ob_dml_sql_splicer.h
  ob_zone_info.h
  partition_table/ob_partition_location.h
  ob_cluster_type.h
  ob_cluster_sync_status.h
  ob_web_service_root_addr.h
  ob_root_addr_agent.h
  ob_lease_struct.h
  ob_cluster_info_proxy.h
  restore/ob_restore_args.h
  ob_server_status.h
  ob_debug_sync_point.h
  ob_debug_sync.h
  backup/ob_physical_restore_info.h
  ob_partition_modify.h
  ob_freeze_info_proxy.h
  ob_simple_batch.h
  ob_replica_wrs_info.h
  ob_cluster_switchover_info.h
  schema/ob_error_info.h
  schema/ob_dependency_info.h
  ob_rpc_struct.h
  ob_multi_cluster_util.h
  ob_proposal_id.h
  stat/ob_table_stat.h
  ob_thread_pool.h
  redolog/ob_log_disk_manager.h
  redolog/ob_log_file_store.h
  rc/ob_context.h
  partition_table/ob_ipartition_table.h
  ob_thread_mgr.h
  ob_thread_define.h
  ob_check_stop_provider.h
  rc/ob_tenant_base.h
  partition_table/ob_inmemory_partition_table.h
  partition_table/ob_rpc_partition_table.h
  partition_table/ob_persistent_partition_table.h
  partition_table/ob_partition_table_operator.h
  ob_unit_table_operator.h
  ob_unit_stat_table_operator.h
  ob_unit_stat.h
  ob_unit_getter.h
  ob_rs_mgr.h
  ob_inner_config_root_addr.h
  backup/ob_backup_manager.h
  backup/ob_backup_operator.h
  backup/ob_backup_info_mgr.h
  schema/ob_table_dml_param.h
  ob_srv_rpc_proxy.h
  partition_table/ob_partition_location_cache.h
  schema/ob_schema_utils.h
  datum/ob_datum_funcs.h
  ob_time_zone_info_manager.h
  ob_server_locality_cache.h
  ob_autoincrement_param.h
  stat/ob_opt_table_stat.h
  stat/ob_opt_column_stat.h
  stat/ob_opt_stat_sql_service.h
  stat/ob_opt_stat_manager.h
  client_feedback/ob_feedback_type_define.h
  client_feedback/ob_client_feedback_basic.h
  client_feedback/ob_feedback_partition_struct.h
  client_feedback/ob_feedback_int_struct.h
  client_feedback/ob_client_feedback_manager.h
  rpc/ob_batch_proxy.h
  ob_common_rpc_proxy.h
)

ob_server_add_target(ob_share)
