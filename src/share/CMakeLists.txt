ob_set_subtarget(ob_share ALONE
  ob_rpc_struct.cpp
  ob_tenant_mgr.cpp
  datum/ob_datum_funcs.cpp
  schema/ob_schema_service_sql_impl.cpp
  parameter/ob_parameter_attr.cpp
  vector/expr_cmp_func.cpp
  aggregate/sum.cpp
  aggregate/processor.cpp
  aggregate/single_row.cpp
  aggregate/approx_count_distinct.cpp
)

file(GLOB SCHEMA_CPPS "inner_table/ob_inner_table_schema.*.cpp")
ob_set_subtarget(ob_share SCHEMA_CPPS
  ${SCHEMA_CPPS}
)

ob_set_subtarget(ob_share allocator
  allocator/ob_fifo_arena.cpp
  allocator/ob_memstore_allocator.cpp
  allocator/ob_handle_list.cpp
  allocator/ob_mds_allocator.cpp
  allocator/ob_shared_memory_allocator_mgr.cpp
  allocator/ob_tenant_mutil_allocator.cpp
  allocator/ob_tenant_mutil_allocator_mgr.cpp
  allocator/ob_tx_data_allocator.cpp
)

ob_set_subtarget(ob_share backup
  backup/ob_archive_piece.cpp
  backup/ob_archive_struct.cpp
  backup/ob_archive_compatible.cpp
  backup/ob_archive_mode.cpp
  backup/ob_archive_persist_helper.cpp
  backup/ob_backup_clean_operator.cpp
  backup/ob_backup_clean_struct.cpp
  backup/ob_backup_clean_util.cpp
  backup/ob_backup_connectivity.cpp
  backup/ob_backup_io_adapter.cpp
  backup/ob_backup_path.cpp
  backup/ob_backup_server_mgr.cpp
  backup/ob_backup_struct.cpp
  backup/ob_tenant_archive_mgr.cpp
  backup/ob_tenant_archive_round.cpp
  backup/ob_backup_data_table_operator.cpp
  backup/ob_backup_helper.cpp
  backup/ob_archive_persist_helper.cpp
  backup/ob_backup_serialize_provider.cpp
  backup/ob_backup_store.cpp
  backup/ob_archive_store.cpp
  backup/ob_archive_checkpoint.cpp
  backup/ob_archive_path.cpp
  backup/ob_backup_config.cpp
  backup/ob_log_restore_config.cpp
  backup/ob_log_restore_struct.cpp
  backup/ob_archive_checkpoint_mgr.cpp
)

ob_set_subtarget(ob_share cache
  cache/ob_kv_storecache.cpp
  cache/ob_kvcache_inst_map.cpp
  cache/ob_kvcache_map.cpp
  cache/ob_kvcache_store.cpp
  cache/ob_kvcache_struct.cpp
  cache/ob_working_set_mgr.cpp
  cache/ob_kvcache_hazard_version.cpp
  cache/ob_kvcache_pre_warmer.cpp
  cache/ob_kvcache_pointer_swizzle.cpp
)

ob_set_subtarget(ob_share common
  ob_alive_server_tracer.cpp
  ob_all_server_tracer.cpp
  ob_arbitration_service_status.cpp
  ob_autoincrement_param.cpp
  ob_autoincrement_service.cpp
  ob_bg_thread_monitor.cpp
  ob_cascad_member.cpp
  ob_cascad_member_list.cpp
  ob_cluster_role.cpp
  ob_cluster_version.cpp
  ob_column_checksum_error_operator.cpp
  ob_core_table_proxy.cpp
  ob_ddl_checksum.cpp
  ob_ddl_sim_point.cpp
  ob_ddl_common.cpp
  ob_ddl_error_message_table_operator.cpp
  ob_ddl_task_executor.cpp
  ob_debug_sync.cpp
  ob_debug_sync_point.cpp
  ob_device_manager.cpp
  ob_disk_usage_table_operator.cpp
  ob_dml_sql_splicer.cpp
  ob_encryption_struct.cpp
  ob_encryption_util_os.cpp
  ob_errno.cpp
  ob_event_history_table_operator.cpp
  ob_tenant_id_schema_version.cpp
  ob_freeze_info_proxy.cpp
  ob_gais_client.cpp
  ob_gais_msg.cpp
  ob_gais_rpc.cpp
  ob_get_compat_mode.cpp
  ob_global_autoinc_service.cpp
  ob_global_context_operator.cpp
  ob_global_merge_table_operator.cpp
  ob_global_stat_proxy.cpp
  ob_gts_info.cpp
  ob_i_tablet_scan.cpp
  ob_index_builder_util.cpp
  ob_fts_index_builder_util.cpp
  ob_inner_config_root_addr.cpp
  ob_io_device_helper.cpp
  ob_kv_parser.cpp
  ob_log_restore_proxy.cpp
  ob_label_security_os.cpp
  ob_leader_election_waiter.cpp
  ob_lease_struct.cpp
  ob_heartbeat_handler.cpp
  ob_heartbeat_struct.cpp
  ob_list_parser.cpp
  ob_local_device.cpp
  ob_locality_info.cpp
  ob_locality_parser.cpp
  ob_locality_priority.cpp
  ob_locality_table_operator.cpp
  ob_ls_id.cpp
  ob_table_range.cpp
  ob_max_id_fetcher.cpp
  ob_occam_time_guard.cpp
  ob_order_perserving_encoder.cpp
  ob_partition_modify.cpp
  ob_primary_zone_util.cpp
  ob_priv_common.cpp
  ob_proposal_id.cpp
  ob_plugin_helper.cpp
  ob_replica_info.cpp
  ob_resource_limit.cpp
  ob_root_addr_agent.cpp
  ob_rpc_share.cpp
  ob_rs_mgr.cpp
  ob_scanner.cpp
  ob_schema_status_proxy.cpp
  ob_server_blacklist.cpp
  ob_server_locality_cache.cpp
  ob_server_status.cpp
  ob_server_table_operator.cpp
  ob_service_epoch_proxy.cpp
  ob_share_util.cpp
  ob_simple_batch.cpp
  ob_simple_mem_limit_getter.cpp
  ob_snapshot_table_proxy.cpp
  ob_tablet_autoincrement_param.cpp
  ob_tablet_autoincrement_service.cpp
  ob_tablet_checksum_iterator.cpp
  ob_tablet_checksum_operator.cpp
  ob_tablet_replica_checksum_operator.cpp
  ob_tablet_meta_table_compaction_operator.cpp
  ob_task_define.cpp
  ob_tenant_info_proxy.cpp
  ob_tenant_role.cpp
  ob_tenant_switchover_status.cpp
  ob_tenant_mem_limit_getter.cpp
  ob_tenant_memstore_info_operator.cpp
  ob_thread_mgr.cpp
  ob_time_utility2.cpp
  ob_time_zone_info_manager.cpp
  ob_unique_index_row_transformer.cpp
  ob_unit_getter.cpp
  ob_unit_table_operator.cpp
  ob_upgrade_utils.cpp
  ob_virtual_table_iterator.cpp
  ob_virtual_table_projector.cpp
  ob_virtual_table_scanner_iterator.cpp
  ob_web_service_root_addr.cpp
  ob_zone_info.cpp
  ob_freeze_info_manager.cpp
  ob_zone_merge_info.cpp
  ob_zone_merge_table_operator.cpp
  ob_zone_table_operation.cpp
  ob_rootservice_list.cpp
  ob_admin_dump_helper.cpp
  ob_inner_kv_table_operator.cpp
  ob_inner_table_operator.cpp
  ob_standby_upgrade.cpp
  ob_primary_standby_service.cpp
  ob_common_id.cpp
  ob_cluster_event_history_table_operator.cpp
  scn.cpp
  ob_throttling_utils.cpp
  ob_storage_ha_diagnose_struct.cpp
  ob_storage_ha_diagnose_operator.cpp
  ob_compatibility_control.cpp
)

ob_set_subtarget(ob_share common_mixed
  client_feedback/ob_client_feedback_basic.cpp
  client_feedback/ob_client_feedback_manager.cpp
  client_feedback/ob_feedback_int_struct.cpp
  client_feedback/ob_feedback_partition_struct.cpp
  datum/ob_datum.cpp
  diagnosis/ob_sql_monitor_statname.cpp
  diagnosis/ob_sql_plan_monitor_node_list.cpp
  interrupt/ob_global_interrupt_call.cpp
  interrupt/ob_interrupt_rpc_proxy.cpp
  location_cache/ob_location_service.cpp
  location_cache/ob_location_struct.cpp
  location_cache/ob_location_update_task.cpp
  location_cache/ob_ls_location_service.cpp
  location_cache/ob_ls_location_map.cpp
  location_cache/ob_tablet_ls_service.cpp
  location_cache/ob_tablet_ls_map.cpp
  location_cache/ob_tablet_location_broadcast.cpp
  location_cache/ob_vtable_location_service.cpp
  location_cache/ob_tablet_location_refresh_service.cpp
  inner_table/ob_inner_table_schema.vt.cpp
  inner_table/ob_inner_table_schema.lob.cpp
  inner_table/ob_inner_table_schema_misc.ipp
  object/ob_obj_cast.cpp
  parameter/ob_parameter_seed.ipp
  ratelimit/ob_rl_mgr.cpp
  ratelimit/ob_rl_rpc.cpp
  ratelimit/ob_rl_struct.cpp
  rc/ob_context.cpp
  rc/ob_tenant_base.cpp
  restore/ob_import_table_struct.cpp
  restore/ob_import_util.cpp
  restore/ob_recover_table_util.cpp
  restore/ob_restore_uri_parser.cpp
  restore/ob_ls_restore_status.cpp
  restore/ob_restore_type.cpp
  restore/ob_physical_restore_info.cpp
  restore/ob_physical_restore_table_operator.cpp
  restore/ob_log_restore_source.cpp
  restore/ob_log_restore_source_mgr.cpp
  restore/ob_restore_table_operator.cpp
  restore/ob_recover_table_persist_helper.cpp
  restore/ob_restore_persist_helper.cpp
  restore/ob_import_table_persist_helper.cpp
  restore/ob_import_item_format_provider.cpp
  restore/ob_import_schema_item.hpp
  restore/ob_import_partition_item.cpp
  restore/ob_import_table_item.cpp
  restore/ob_remap_schema_item.hpp
  restore/ob_import_remap_arg.cpp
  restore/ob_import_table_arg.cpp
  restore/ob_import_arg.cpp
  sequence/ob_sequence_cache.cpp
  sequence/ob_sequence_ddl_proxy.cpp
  sequence/ob_sequence_dml_proxy.cpp
  sequence/ob_sequence_option.cpp
  sequence/ob_sequence_option_builder.cpp
  system_variable/ob_nls_system_variable.cpp
  system_variable/ob_system_variable.cpp
  system_variable/ob_system_variable_factory.cpp
  system_variable/ob_system_variable_init.cpp
  ob_lob_access_utils.cpp
  ob_json_access_utils.cpp
  table/ob_table.cpp
  table/ob_table_rpc_struct.cpp
  table/ob_table_load_define.cpp
  table/ob_table_load_dml_stat.cpp
  table/ob_table_load_row.cpp
  transfer/ob_transfer_info.cpp
  transfer/ob_transfer_task_operator.cpp
  detect/ob_detect_manager.cpp
  detect/ob_detectable_id.cpp
  detect/ob_detect_callback.cpp
  detect/ob_detect_rpc_proxy.cpp
  detect/ob_detect_rpc_processor.cpp
  detect/ob_detect_manager_utils.cpp
  table/ob_table_load_sql_statistics.cpp
  tenant_snapshot/ob_tenant_snapshot_id.cpp
  tenant_snapshot/ob_tenant_snapshot_table_operator.cpp
  table/ob_ttl_util.cpp
  table/ob_table_config_util.cpp
  restore/ob_tenant_clone_table_operator.cpp
  index_usage/ob_index_usage_info_mgr.cpp
  index_usage/ob_index_usage_report_task.cpp
  table/ob_table_util.cpp
  scheduler/ob_tenant_dag_scheduler.cpp
  scheduler/ob_sys_task_stat.cpp
  scheduler/ob_dag_warning_history_mgr.cpp
  compaction/ob_compaction_locality_cache.cpp
  compaction/ob_table_ckm_items.cpp
  compaction/ob_compaction_time_guard.cpp
  compaction/ob_schedule_batch_size_mgr.cpp
  partition_table/ob_partition_location.cpp
  longops_mgr/ob_ddl_longops.cpp
  longops_mgr/ob_i_longops.cpp
  longops_mgr/ob_longops_mgr.cpp
  ash/ob_active_sess_hist_list.cpp
  ash/ob_active_sess_hist_task.cpp
  throttle/ob_share_throttle_define.cpp
  throttle/ob_throttle_common.cpp
  restore/ob_restore_data_mode.cpp
)

ob_set_subtarget(ob_share tablet
  tablet/ob_tablet_info.cpp
  tablet/ob_tablet_table_operator.cpp
  tablet/ob_tablet_to_ls_operator.cpp
  tablet/ob_tablet_table_iterator.cpp
  tablet/ob_tablet_to_ls_iterator.cpp
  tablet/ob_tablet_filter.cpp
  tablet/ob_tablet_to_table_history_operator.cpp
  tablet/ob_tenant_tablet_to_ls_map.cpp
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

ob_set_subtarget(ob_share deadlock
  deadlock/ob_deadlock_detector_common_define.cpp
  deadlock/ob_deadlock_detector_mgr.cpp
  deadlock/ob_deadlock_detector_rpc.cpp
  deadlock/ob_deadlock_inner_table_service.cpp
  deadlock/ob_deadlock_key_wrapper.cpp
  deadlock/ob_deadlock_message.cpp
  deadlock/ob_lcl_scheme/ob_lcl_batch_sender_thread.cpp
  deadlock/ob_lcl_scheme/ob_lcl_message.cpp
  deadlock/ob_lcl_scheme/ob_lcl_node.cpp
  deadlock/ob_lcl_scheme/ob_lcl_utils.cpp
)

ob_set_subtarget(ob_share ls
  ls/ob_inmemory_ls_table.cpp
  ls/ob_ls_info.cpp
  ls/ob_ls_operator.cpp
  ls/ob_ls_status_operator.cpp
  ls/ob_ls_life_manager.cpp
  ls/ob_ls_table.cpp
  ls/ob_ls_table_operator.cpp
  ls/ob_persistent_ls_table.cpp
  ls/ob_rpc_ls_table.cpp
  ls/ob_ls_table_iterator.cpp
  ls/ob_ls_replica_filter.cpp
  ls/ob_ls_recovery_stat_operator.cpp
  ls/ob_ls_creator.cpp
  ls/ob_ls_election_reference_info_operator.cpp
  ls/ob_ls_log_stat_info.cpp
)

ob_set_subtarget(ob_share redolog
  redolog/ob_clog_switch_write_callback.cpp
  redolog/ob_log_file_group.cpp
  redolog/ob_log_file_handler.cpp
  redolog/ob_log_file_reader.cpp
  redolog/ob_log_policy.cpp
)

ob_set_subtarget(ob_share rpc
  rpc/ob_batch_processor.cpp
  rpc/ob_batch_proxy.cpp
  rpc/ob_batch_rpc.cpp
  rpc/ob_blacklist_proxy.cpp
  rpc/ob_blacklist_req_processor.cpp
  rpc/ob_blacklist_resp_processor.cpp
)

ob_set_subtarget(ob_share resource_manager
  resource_manager/ob_cgroup_ctrl.cpp
  resource_manager/ob_resource_manager.cpp
  resource_manager/ob_resource_manager_proxy.cpp
  resource_manager/ob_resource_mapping_rule_manager.cpp
  resource_manager/ob_resource_col_mapping_rule_manager.cpp
  resource_manager/ob_resource_plan_info.cpp
  resource_manager/ob_resource_plan_manager.cpp
)

ob_set_subtarget(ob_share schema
  schema/ob_column_schema.cpp
  schema/ob_constraint.cpp
  schema/ob_database_sql_service.cpp
  schema/ob_dblink_mgr.cpp
  schema/ob_dblink_sql_service.cpp
  schema/ob_ddl_sql_service.cpp
  schema/ob_dependency_info.cpp
  schema/ob_directory_mgr.cpp
  schema/ob_directory_sql_service.cpp
  schema/ob_error_info.cpp
  schema/ob_keystore_mgr.cpp
  schema/ob_keystore_sql_service.cpp
  schema/ob_label_se_policy_mgr.cpp
  schema/ob_label_se_policy_sql_service.cpp
  schema/ob_mock_fk_parent_table_mgr.cpp
  schema/ob_multi_version_schema_service.cpp
  schema/ob_outline_mgr.cpp
  schema/ob_outline_sql_service.cpp
  schema/ob_package_info.cpp
  schema/ob_package_mgr.cpp
  schema/ob_part_mgr_util.cpp
  schema/ob_partition_sql_helper.cpp
  schema/ob_priv_mgr.cpp
  schema/ob_priv_sql_service.cpp
  schema/ob_profile_mgr.cpp
  schema/ob_profile_sql_service.cpp
  schema/ob_rls_mgr.cpp
  schema/ob_rls_sql_service.cpp
  schema/ob_routine_info.cpp
  schema/ob_routine_mgr.cpp
  schema/ob_routine_sql_service.cpp
  schema/ob_schema_cache.cpp
  schema/ob_schema_macro_define.cpp
  schema/ob_schema_mem_mgr.cpp
  schema/ob_schema_mgr.cpp
  schema/ob_schema_mgr_cache.cpp
  schema/ob_schema_printer.cpp
  schema/ob_schema_retrieve_utils.ipp
  schema/ob_schema_service.cpp
  schema/ob_schema_service_rpc_proxy.cpp
  schema/ob_schema_store.cpp
  schema/ob_schema_utils.cpp
  schema/ob_security_audit_mgr.cpp
  schema/ob_security_audit_sql_service.cpp
  schema/ob_sequence_mgr.cpp
  schema/ob_synonym_mgr.cpp
  schema/ob_synonym_sql_service.cpp
  schema/ob_sys_variable_mgr.cpp
  schema/ob_sys_variable_sql_service.cpp
  schema/ob_table_dml_param.cpp
  schema/ob_table_iter.cpp
  schema/ob_table_param.cpp
  schema/ob_table_sql_service.cpp
  schema/ob_tablegroup_sql_service.cpp
  schema/ob_tablespace_mgr.cpp
  schema/ob_tablespace_sql_service.cpp
  schema/ob_tenant_schema_service.cpp
  schema/ob_tenant_sql_service.cpp
  schema/ob_trigger_info.cpp
  schema/ob_trigger_mgr.cpp
  schema/ob_trigger_sql_service.cpp
  schema/ob_udf.cpp
  schema/ob_udf_mgr.cpp
  schema/ob_udf_sql_service.cpp
  schema/ob_udt_info.cpp
  schema/ob_udt_mgr.cpp
  schema/ob_udt_sql_service.cpp
  schema/ob_user_sql_service.cpp
  schema/ob_context_sql_service.cpp
  schema/ob_context_mgr.cpp
  schema/ob_context_ddl_proxy.cpp
  schema/ob_ddl_epoch.cpp
  schema/ob_ddl_trans_controller.cpp
  schema/ob_schema_getter_guard.cpp
  schema/ob_schema_struct.cpp
  schema/ob_sequence_sql_service.cpp
  schema/ob_server_schema_service.cpp
  schema/ob_table_schema.cpp
  schema/ob_latest_schema_guard.cpp
  schema/ob_mlog_info.cpp
  schema/ob_mview_info.cpp
  schema/ob_mview_refresh_stats_params.cpp
  schema/ob_mview_refresh_stats.cpp
)

ob_set_subtarget(ob_share stat
  stat/ob_basic_stats_estimator.cpp
  stat/ob_opt_stat_monitor_manager.cpp
  stat/ob_dbms_stats_copy_table_stats.cpp
  stat/ob_dbms_stats_executor.cpp
  stat/ob_dbms_stats_export_import.cpp
  stat/ob_dbms_stats_history_manager.cpp
  stat/ob_dbms_stats_lock_unlock.cpp
  stat/ob_dbms_stats_preferences.cpp
  stat/ob_dbms_stats_utils.cpp
  stat/ob_hybrid_hist_estimator.cpp
  stat/ob_incremental_stat_estimator.cpp
  stat/ob_opt_column_stat.cpp
  stat/ob_opt_osg_column_stat.cpp
  stat/ob_opt_column_stat_cache.cpp
  stat/ob_opt_stat_manager.cpp
  stat/ob_opt_stat_service.cpp
  stat/ob_opt_stat_sql_service.cpp
  stat/ob_opt_table_stat.cpp
  stat/ob_opt_table_stat_cache.cpp
  stat/ob_stat_define.cpp
  stat/ob_stat_item.cpp
  stat/ob_stats_estimator.cpp
  stat/ob_topk_hist_estimator.cpp
  stat/ob_index_stats_estimator.cpp
  stat/ob_dbms_stats_maintenance_window.cpp
  stat/ob_opt_stat_gather_stat.cpp
  stat/ob_opt_ds_stat.cpp
  stat/ob_opt_ds_stat_cache.cpp
  stat/ob_dbms_stats_gather.cpp
  stat/ob_opt_system_stat.cpp
  stat/ob_opt_system_stat_cache.cpp
)

ob_set_subtarget(ob_share external_table
  external_table/ob_external_table_file_mgr.cpp
  external_table/ob_external_table_file_rpc_processor.cpp
  external_table/ob_external_table_file_task.cpp
  external_table/ob_external_table_utils.cpp
)

ob_set_subtarget(ob_share io
  io/ob_io_define.cpp
  io/io_schedule/ob_io_mclock.cpp
  io/ob_io_struct.cpp
  io/ob_io_calibration.cpp
  io/ob_io_manager.cpp
)

ob_set_subtarget(ob_share unit
  unit/ob_resource_pool.cpp
  unit/ob_unit_config.cpp
  unit/ob_unit_resource.cpp
  unit/ob_unit_info.cpp
)
ob_set_subtarget(ob_share balance
  ob_balance_define.cpp
  balance/ob_balance_job_table_operator.cpp
  balance/ob_balance_task_table_operator.cpp
  balance/ob_balance_task_helper_operator.cpp
  balance/ob_transfer_partition_task_table_operator.cpp
)
ob_set_subtarget(ob_share resource_limit_calculator
  resource_limit_calculator/ob_resource_commmon.cpp
  resource_limit_calculator/ob_resource_limit_calculator.cpp
)

if (OB_ERRSIM)
 ob_set_subtarget(ob_share errsim_module
   errsim_module/ob_tenant_errsim_module_mgr.cpp
   errsim_module/ob_tenant_errsim_event_mgr.cpp
   errsim_module/ob_errsim_module_interface_imp.cpp
 )
endif()

ob_set_subtarget(ob_share text_analysis
  text_analysis/ob_text_analyzer.cpp
  text_analysis/ob_token_stream.cpp
)

ob_set_subtarget(ob_share throttle
  throttle/ob_throttle_common.cpp
)

ob_set_subtarget(ob_share wr
  wr/ob_wr_service.cpp
  wr/ob_wr_task.cpp
  wr/ob_wr_snapshot_rpc_processor.cpp
  wr/ob_wr_collector.cpp
)

ob_set_subtarget(ob_share vector
  vector/ob_vector_base.cpp
  vector/ob_fixed_length_vector.cpp
  vector/ob_continuous_vector.cpp
  vector/ob_discrete_vector.cpp
  vector/ob_uniform_vector.cpp
  vector/ob_uniform_base.cpp
  vector/ob_fixed_length_base.cpp
  vector/ob_discrete_base.cpp
  vector/ob_continuous_base.cpp
)

ob_set_subtarget(ob_share aggregate
  aggregate/iaggregate.cpp
  aggregate/count.cpp
  aggregate/min_max.cpp
  aggregate/sys_bit.cpp
)

ob_add_new_object_target(ob_share ob_share)

target_compile_options(ob_share PRIVATE)
#target_compile_options(ob_share PRIVATE -Wshorten-64-to-32)
target_link_libraries(ob_share PUBLIC ob_base)

add_library(ob_share_static
  STATIC
  EXCLUDE_FROM_ALL)

target_link_libraries(ob_share_static
  PUBLIC ob_share)
