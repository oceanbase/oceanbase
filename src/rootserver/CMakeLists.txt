ob_set_subtarget(ob_rootserver ALONE
  ob_ddl_service.cpp
  ob_ddl_service_launcher.cpp
  ob_root_service.cpp
  ddl_task/ob_ddl_redefinition_task.cpp
  ob_unit_manager.cpp
  ob_heartbeat_service.cpp
  ob_lonely_table_cleaner.cpp
)

ob_set_subtarget(ob_rootserver backup
  backup/ob_archive_scheduler_service.cpp
  backup/ob_backup_base_service.cpp
  backup/ob_backup_clean_ls_task_mgr.cpp
  backup/ob_backup_clean_scheduler.cpp
  backup/ob_backup_clean_task_mgr.cpp
  backup/ob_backup_service.cpp
  backup/ob_backup_schedule_task.cpp
  backup/ob_backup_task_scheduler.cpp
  backup/ob_tenant_archive_scheduler.cpp
  backup/ob_backup_data_scheduler.cpp
  backup/ob_backup_data_set_task_mgr.cpp
  backup/ob_backup_proxy.cpp
  backup/ob_backup_data_ls_task_mgr.cpp
  backup/ob_backup_table_list_mgr.cpp
  backup/ob_backup_param_operator.cpp
)

ob_set_subtarget(ob_rootserver common
  ob_bootstrap.cpp
  ob_admin_drtask_util.cpp
  ob_disaster_recovery_task_table_operator.cpp
  ob_balance_group_ls_stat_operator.cpp
  ob_disaster_recovery_info.cpp
  ob_disaster_recovery_worker.cpp
  ob_disaster_recovery_service.cpp
  ob_disaster_recovery_task.cpp
  ob_disaster_recovery_task_mgr.cpp
  ob_disaster_recovery_task_utils.cpp
  ob_migrate_unit_finish_checker.cpp
  ob_rootservice_util_checker.cpp
  ob_alter_primary_zone_checker.cpp
  ob_all_server_checker.cpp
  ob_all_server_task.cpp
  ob_balance_info.cpp
  ob_create_inner_schema_executor.cpp
  ob_ddl_help.cpp
  ob_ddl_operator.cpp
  ob_ddl_sql_generator.cpp
  ob_index_builder.cpp
  ob_mlog_builder.cpp
  ob_locality_util.cpp
  ob_resource_weight_parser.cpp
  ob_root_balancer.cpp
  ob_partition_balance.cpp
  ob_root_minor_freeze.cpp
  ob_root_utils.cpp
  ob_root_inspection.cpp
  ob_rs_event_history_table_operator.cpp
  ob_tenant_event_history_table_operator.cpp
  ob_rs_job_table_operator.cpp
  ob_rs_reentrant_thread.cpp
  ob_rs_thread_checker.cpp
  ob_schema2ddl_sql.cpp
  ob_empty_server_checker.cpp
  ob_lost_replica_checker.cpp
  ob_server_manager.cpp
  ob_server_zone_op_service.cpp
  ob_snapshot_info_manager.cpp
  ob_tablet_creator.cpp
  ob_tablet_drop.cpp
  ob_table_creator.cpp
  ob_thread_idling.cpp
  ob_unit_placement_strategy.cpp
  ob_unit_stat_manager.cpp
  ob_upgrade_executor.cpp
  ob_vertical_partition_builder.cpp
  ob_vtable_location_getter.cpp
  ob_zone_manager.cpp
  ob_zone_storage_manager.cpp
  ob_zone_unit_provider.cpp
  ob_tenant_info_loader.cpp
  ob_tenant_thread_helper.cpp
  ob_ls_recovery_reportor.cpp
  ob_standby_schema_refresh_trigger.cpp
  ob_tenant_info_loader.cpp
  ob_create_standby_from_net_actor.cpp
  ob_primary_ls_service.cpp
  ob_balance_ls_primary_zone.cpp
  ob_common_ls_service.cpp
  ob_ls_service_helper.cpp
  ob_tenant_transfer_service.cpp
  ob_transfer_partition_task.cpp
  ob_tenant_balance_service.cpp
  ob_ls_balance_helper.cpp
  ob_balance_task_execute_service.cpp
  ob_server_balancer.cpp
  ob_system_admin_util.cpp
  ob_update_rs_list_task.cpp
  ob_upgrade_storage_format_version_executor.cpp
  ob_schema_history_recycler.cpp
  ob_alter_locality_finish_checker.cpp
  ob_lob_piece_builder.cpp
  ob_lob_meta_builder.cpp
  ob_ls_recovery_stat_handler.cpp
  ob_shrink_expand_resource_pool_checker.cpp
  ob_split_partition_helper.cpp
  ob_transfer_partition_command.cpp
  ob_partition_exchange.cpp
  ob_service_name_command.cpp
  ob_catalog_ddl_operator.cpp
  ob_catalog_ddl_service.cpp
  ob_tenant_ddl_service.cpp
  ob_tenant_parallel_create_executor.cpp
  ob_load_sys_package_task.cpp
  ob_root_rebuild_tablet.cpp
  ob_alter_table_constraint_checker.cpp
)

ob_set_subtarget(ob_rootserver balance
  balance/ob_all_balance_group_builder.cpp
  balance/ob_balance_group_define.cpp
  balance/ob_balance_group_info.cpp
  balance/ob_ls_balance_group_info.cpp
  balance/ob_tenant_ls_balance_group_info.cpp
  balance/ob_ls_all_part_builder.cpp
  balance/ob_partition_balance_helper.cpp
)

ob_set_subtarget(ob_rootserver ddl_task
  ddl_task/ob_column_redefinition_task.cpp
  ddl_task/ob_constraint_task.cpp
  ddl_task/ob_ddl_retry_task.cpp
  ddl_task/ob_ddl_scheduler.cpp
  ddl_task/ob_ddl_single_replica_executor.cpp
  ddl_task/ob_ddl_task.cpp
  ddl_task/ob_drop_index_task.cpp
  ddl_task/ob_drop_fts_index_task.cpp
  ddl_task/ob_drop_vec_index_task.cpp
  ddl_task/ob_drop_primary_key_task.cpp
  ddl_task/ob_index_build_task.cpp
  ddl_task/ob_build_mview_task.cpp
  ddl_task/ob_fts_index_build_task.cpp
  ddl_task/ob_modify_autoinc_task.cpp
  ddl_task/ob_table_redefinition_task.cpp
  ddl_task/ob_partition_split_task.cpp
  ddl_task/ob_recover_restore_table_task.cpp
  ddl_task/ob_ddl_tablet_scheduler.cpp
  ddl_task/ob_drop_vec_ivf_index_task.cpp
  ddl_task/ob_rebuild_index_task.cpp
  ddl_task/ob_vec_index_build_task.cpp
  ddl_task/ob_sys_ddl_util.cpp
  ddl_task/ob_vec_ivf_index_build_task.cpp
  ddl_task/ob_drop_lob_task.cpp
)

ob_set_subtarget(ob_rootserver parallel_ddl
  parallel_ddl/ob_ddl_helper.cpp
  parallel_ddl/ob_create_table_helper.cpp
  parallel_ddl/ob_create_view_helper.cpp
  parallel_ddl/ob_index_name_checker.cpp
  parallel_ddl/ob_tablet_balance_allocator.cpp
  parallel_ddl/ob_set_comment_helper.cpp
  parallel_ddl/ob_create_index_helper.cpp
  parallel_ddl/ob_update_index_status_helper.cpp
)

ob_set_subtarget(ob_rootserver freeze
  freeze/ob_tenant_major_freeze.cpp
  freeze/ob_major_merge_scheduler.cpp
  freeze/ob_major_merge_progress_checker.cpp
  freeze/ob_freeze_info_detector.cpp
  freeze/ob_freeze_reentrant_thread.cpp
  freeze/ob_tenant_all_zone_merge_strategy.cpp
  freeze/ob_tenant_major_merge_strategy.cpp
  freeze/ob_zone_merge_manager.cpp
  freeze/ob_major_merge_info_manager.cpp
  freeze/ob_daily_major_freeze_launcher.cpp
  freeze/ob_checksum_validator.cpp
  freeze/ob_major_freeze_service.cpp
  freeze/ob_major_freeze_rpc_define.cpp
  freeze/ob_major_freeze_helper.cpp
  freeze/ob_major_freeze_util.cpp
  freeze/ob_major_merge_progress_util.cpp
  freeze/ob_fts_checksum_validate_util.cpp
)

ob_set_subtarget(ob_rootserver restore
  restore/ob_import_table_job_scheduler.cpp
  restore/ob_recover_table_initiator.cpp
  restore/ob_import_table_task_generator.cpp
  restore/ob_recover_table_job_scheduler.cpp
  restore/ob_restore_service.cpp
  restore/ob_restore_scheduler.cpp
  restore/ob_restore_util.cpp
  restore/ob_tenant_clone_util.cpp
  restore/ob_clone_scheduler.cpp
  restore/ob_restore_common_util.cpp
)

ob_set_subtarget(ob_rootserver virtual_table
  virtual_table/ob_core_meta_table.cpp
  virtual_table/ob_virtual_core_inner_table.cpp
  virtual_table/ob_all_virtual_backup_task_scheduler_stat.cpp
  virtual_table/ob_all_virtual_ls_replica_task_plan.cpp
)

ob_set_subtarget(ob_rootserver tenant_snapshot
  tenant_snapshot/ob_tenant_snapshot_scheduler.cpp
  tenant_snapshot/ob_tenant_snapshot_util.cpp
)

ob_set_subtarget(ob_rootserver mview
  mview/ob_mview_maintenance_service.cpp
  mview/ob_mlog_maintenance_task.cpp
  mview/ob_mview_maintenance_task.cpp
  mview/ob_mview_refresh_stats_maintenance_task.cpp
  mview/ob_mview_timer_task.cpp
  mview/ob_mview_dependency_service.cpp
  mview/ob_mview_push_refresh_scn_task.cpp
  mview/ob_mview_push_snapshot_task.cpp
  mview/ob_replica_safe_check_task.cpp
  mview/ob_collect_mv_merge_info_task.cpp
  mview/ob_mview_clean_snapshot_task.cpp
  mview/ob_mview_update_cache_task.cpp
  mview/ob_mview_mds_op_task.cpp
  mview/ob_mview_alter_service.cpp
)

ob_set_subtarget(ob_rootserver direct_load
  direct_load/ob_direct_load_partition_exchange.cpp
)

ob_set_subtarget(ob_rootserver standby
  standby/ob_standby_service.cpp
  standby/ob_recovery_ls_service.cpp
  standby/ob_tenant_role_transition_service.cpp
)

ob_set_subtarget(ob_rootserver truncate_info
  truncate_info/ob_truncate_info_service.cpp
)

ob_set_subtarget(ob_rootserver pl_ddl
  pl_ddl/ob_pl_ddl_service.cpp
  pl_ddl/ob_pl_ddl_operator.cpp
)

ob_server_add_target(ob_rootserver)
