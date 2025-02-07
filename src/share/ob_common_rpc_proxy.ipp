#include "rpc/obrpc/ob_rpc_proxy_macros.h"

  // rootservice provided
  RPC_S(PRZ renew_lease, obrpc::OB_RENEW_LEASE, (oceanbase::share::ObLeaseRequest), share::ObLeaseResponse);
//  RPC_S(PRZ cluster_heartbeat, obrpc::OB_CLUSTER_HB, (oceanbase::share::ObClusterAddr), obrpc::ObStandbyHeartBeatRes);
//  RPC_S(PRZ cluster_regist, obrpc::OB_CLUSTER_REGIST, (oceanbase::obrpc::ObRegistClusterArg), obrpc::ObRegistClusterRes);
//  RPC_S(PRZ get_schema_snapshot, obrpc::OB_GET_SCHEMA_SNAPSHOT, (oceanbase::obrpc::ObSchemaSnapshotArg), obrpc::ObSchemaSnapshotRes);
  RPC_S(PR5 report_sys_ls, obrpc::OB_REPORT_SYS_LS, (share::ObLSReplica));
  RPC_S(PR5 remove_sys_ls, obrpc::OB_REMOVE_SYS_LS, (obrpc::ObRemoveSysLsArg));
  RPC_S(PR5 fetch_location, obrpc::OB_FETCH_LOCATION, (obrpc::ObFetchLocationArg), ObFetchLocationResult);
  // RPC_S(PR5 merge_finish, obrpc::OB_MERGE_FINISH, (ObMergeFinishArg));
  RPC_S(PR5 broadcast_ds_action, obrpc::OB_BROADCAST_DS_ACTION, (ObDebugSyncActionArg));
  RPC_S(PR5 check_dangling_replica_finish, obrpc::OB_CHECK_DANGLING_REPLICA_FINISH, (ObCheckDanglingReplicaFinishArg));
  // high priority for fetch_alive_server, make sure it will not be blocked while network partition
  RPC_S(PR3 fetch_alive_server, obrpc::OB_FETCH_ALIVE_SERVER, (ObFetchAliveServerArg), ObFetchAliveServerResult);
  // RPC_S(PR5 fetch_active_server_status, obrpc::OB_FETCH_ACTIVE_SERVER_STATUS, (ObFetchAliveServerArg), ObFetchActiveServerAddrResult);

  RPC_S(PRD create_tenant, obrpc::OB_CREATE_TENANT, (ObCreateTenantArg), UInt64);
  RPC_S(PRD create_tenant_end, obrpc::OB_CREATE_TENANT_END, (ObCreateTenantEndArg));
  RPC_S(PRD drop_tenant, obrpc::OB_DROP_TENANT, (ObDropTenantArg));
  RPC_S(PRD modify_tenant, obrpc::OB_MODIFY_TENANT, (ObModifyTenantArg));
  RPC_S(PRD lock_tenant, obrpc::OB_LOCK_TENANT, (ObLockTenantArg));
  RPC_S(PRD add_system_variable, obrpc::OB_ADD_SYSVAR, (ObAddSysVarArg));
  RPC_S(PRD modify_system_variable, obrpc::OB_MODIFY_SYSVAR, (ObModifySysVarArg));
  RPC_S(PRD create_database, obrpc::OB_CREATE_DATABASE, (ObCreateDatabaseArg), UInt64);
  RPC_S(PRD create_tablegroup, obrpc::OB_CREATE_TABLEGROUP, (ObCreateTablegroupArg), UInt64);
  RPC_S(PRD create_table, obrpc::OB_CREATE_TABLE, (ObCreateTableArg), ObCreateTableRes);
  RPC_S(PRD recover_restore_table_ddl, obrpc::OB_RECOVER_RESTORE_TABLE_DDL, (ObRecoverRestoreTableDDLArg));
  RPC_S(PRD parallel_create_table, obrpc::OB_PARALLEL_CREATE_TABLE, (ObCreateTableArg), ObCreateTableRes);
  RPC_S(PRD alter_table, obrpc::OB_ALTER_TABLE, (ObAlterTableArg), ObAlterTableRes);
  RPC_S(PRD set_comment, obrpc::OB_PARALLEL_SET_COMMENT, (ObSetCommentArg), ObParallelDDLRes);
  RPC_S(PRD split_global_index_tablet, obrpc::OB_SPLIT_GLOBAL_INDEX_TABLET, (ObAlterTableArg));
  RPC_S(PRD create_hidden_table, obrpc::OB_CREATE_HIDDEN_TABLE, (obrpc::ObCreateHiddenTableArg), ObCreateHiddenTableRes);
  RPC_S(PRD alter_database, obrpc::OB_ALTER_DATABASE, (ObAlterDatabaseArg));
  RPC_S(PRD drop_database, obrpc::OB_DROP_DATABASE, (ObDropDatabaseArg), ObDropDatabaseRes);
  RPC_S(PRD drop_tablegroup, obrpc::OB_DROP_TABLEGROUP, (ObDropTablegroupArg));
  RPC_S(PRD alter_tablegroup, obrpc::OB_ALTER_TABLEGROUP, (ObAlterTablegroupArg));
  RPC_S(PRD drop_table, obrpc::OB_DROP_TABLE, (ObDropTableArg), ObDDLRes);
  RPC_S(PRD rename_table, obrpc::OB_RENAME_TABLE, (ObRenameTableArg));
  RPC_S(PRD truncate_table, obrpc::OB_TRUNCATE_TABLE, (ObTruncateTableArg), ObDDLRes);
  RPC_S(PRD truncate_table_v2, obrpc::OB_TRUNCATE_TABLE_V2, (ObTruncateTableArg), ObDDLRes);
  RPC_S(PRD create_aux_index, obrpc::OB_CREATE_AUX_INDEX, (obrpc::ObCreateAuxIndexArg), obrpc::ObCreateAuxIndexRes);
  RPC_S(PRD create_index, obrpc::OB_CREATE_INDEX, (ObCreateIndexArg), ObAlterTableRes);
  RPC_S(PRD parallel_create_index, obrpc::OB_PARALLEL_CREATE_INDEX, (ObCreateIndexArg), ObAlterTableRes);
  RPC_S(PRD drop_index, obrpc::OB_DROP_INDEX, (ObDropIndexArg), ObDropIndexRes);
  RPC_S(PRD drop_index_on_failed, obrpc::OB_DROP_INDEX_ON_FAILED, (ObDropIndexArg), ObDropIndexRes);
  RPC_S(PRD rebuild_vec_index, obrpc::OB_REBUILD_VEC_INDEX, (ObRebuildIndexArg), ObAlterTableRes);
  RPC_S(PRD create_mlog, obrpc::OB_CREATE_MLOG, (ObCreateMLogArg), ObCreateMLogRes);
  RPC_S(PRD flashback_index, obrpc::OB_FLASHBACK_INDEX, (ObFlashBackIndexArg));
  RPC_S(PRD purge_index, obrpc::OB_PURGE_INDEX, (ObPurgeIndexArg));
  RPC_S(PRD create_table_like, obrpc::OB_CREATE_TABLE_LIKE, (ObCreateTableLikeArg));
  RPC_S(PRD flashback_table_from_recyclebin, obrpc::OB_FLASHBACK_TABLE_FROM_RECYCLEBIN, (ObFlashBackTableFromRecyclebinArg));
  RPC_S(PRD flashback_table_to_time_point, obrpc::OB_FLASHBACK_TABLE_TO_SCN, (ObFlashBackTableToScnArg));
  RPC_S(PRD purge_table, obrpc::OB_PURGE_TABLE, (ObPurgeTableArg));
  RPC_S(PRD flashback_database, obrpc::OB_FLASHBACK_DATABASE, (ObFlashBackDatabaseArg));
  RPC_S(PRD purge_database, obrpc::OB_PURGE_DATABASE, (ObPurgeDatabaseArg));
  RPC_S(PRD flashback_tenant, obrpc::OB_FLASHBACK_TENANT, (ObFlashBackTenantArg));
  RPC_S(PRD purge_tenant, obrpc::OB_PURGE_TENANT, (ObPurgeTenantArg));
  RPC_S(PRD purge_expire_recycle_objects, obrpc::OB_PURGE_EXPIRE_RECYCLE_OBJECTS, (ObPurgeRecycleBinArg), Int64);
  RPC_S(PRD optimize_table, obrpc::OB_OPTIMIZE_TABLE, (ObOptimizeTableArg));
  RPC_S(PRD schema_revise, obrpc::OB_SCHEMA_REVISE, (ObSchemaReviseArg));
  RPC_S(PRD execute_ddl_task, obrpc::OB_EXECUTE_DDL_TASK, (ObAlterTableArg), common::ObSArray<uint64_t>);
  RPC_S(PRD maintain_obj_dependency_info, obrpc::OB_MAINTAIN_OBJ_DEPENDENCY_INFO, (ObDependencyObjDDLArg));
  RPC_S(PRD mview_complete_refresh, obrpc::OB_MVIEW_COMPLETE_REFRESH, (obrpc::ObMViewCompleteRefreshArg), obrpc::ObMViewCompleteRefreshRes);
  RPC_S(PRD exchange_partition, obrpc::OB_EXCHANGE_PARTITION, (ObExchangePartitionArg), ObAlterTableRes);

  //----Definitions for managing privileges----
  RPC_S(PRD create_user, obrpc::OB_CREATE_USER, (ObCreateUserArg), common::ObSArray<int64_t>);
  RPC_S(PRD drop_user, obrpc::OB_DROP_USER, (ObDropUserArg), common::ObSArray<int64_t>);
  RPC_S(PRD alter_role, obrpc::OB_ALTER_ROLE, (ObAlterRoleArg));
  RPC_S(PRD rename_user, obrpc::OB_RENAME_USER, (ObRenameUserArg), common::ObSArray<int64_t>);
  RPC_S(PRD set_passwd, obrpc::OB_SET_PASSWD, (ObSetPasswdArg));
  RPC_S(PRD grant, obrpc::OB_GRANT, (ObGrantArg));
  //RPC_S(PRD standby_grant, obrpc::OB_STANDBY_GRANT, (ObStandbyGrantArg));
  RPC_S(PRD revoke_user, obrpc::OB_REVOKE_USER, (ObRevokeUserArg));
  RPC_S(PRD lock_user, obrpc::OB_LOCK_USER, (ObLockUserArg), common::ObSArray<int64_t>);
  RPC_S(PRD revoke_database, obrpc::OB_REVOKE_DB, (ObRevokeDBArg));
  RPC_S(PRD revoke_table, obrpc::OB_REVOKE_TABLE, (ObRevokeTableArg));
  RPC_S(PRD revoke_routine, obrpc::OB_REVOKE_ROUTINE, (ObRevokeRoutineArg));
  RPC_S(PRD revoke_sys_priv, obrpc::OB_REVOKE_SYSPRIV, (ObRevokeSysPrivArg));
  RPC_S(PRD alter_user_profile, obrpc::OB_ALTER_USER_PROFILE, (ObAlterUserProfileArg));
  RPC_S(PRD handle_security_audit, obrpc::OB_SECURITY_AUDIT, (ObSecurityAuditArg));
  //----End of definitions for managing privileges----

  //----Definitions for managing outlines----
  RPC_S(PRD create_outline, obrpc::OB_CREATE_OUTLINE, (ObCreateOutlineArg));
  RPC_S(PRD alter_outline, obrpc::OB_ALTER_OUTLINE, (ObAlterOutlineArg));
  RPC_S(PRD drop_outline, obrpc::OB_DROP_OUTLINE, (ObDropOutlineArg));
  //----End of definitions for managing outlines----

  RPC_S(PRD create_routine, obrpc::OB_CREATE_ROUTINE, (ObCreateRoutineArg));
  RPC_S(PRD create_routine_with_res, obrpc::OB_CREATE_ROUTINE_WITH_RES, (ObCreateRoutineArg), ObRoutineDDLRes);
  RPC_S(PRD drop_routine, obrpc::OB_DROP_ROUTINE, (ObDropRoutineArg));
  RPC_S(PRD alter_routine, obrpc::OB_ALTER_ROUTINE, (ObCreateRoutineArg));
  RPC_S(PRD alter_routine_with_res, obrpc::OB_ALTER_ROUTINE_WITH_RES, (ObCreateRoutineArg), ObRoutineDDLRes);

  RPC_S(PRD create_udt, obrpc::OB_CREATE_UDT, (ObCreateUDTArg));
  RPC_S(PRD create_udt_with_res, obrpc::OB_CREATE_UDT_WITH_RES, (ObCreateUDTArg), ObRoutineDDLRes);
  RPC_S(PRD drop_udt, obrpc::OB_DROP_UDT, (ObDropUDTArg));

  //----Definitions for managing dblinks----
  RPC_S(PRD create_dblink, obrpc::OB_CREATE_DBLINK, (ObCreateDbLinkArg));
  RPC_S(PRD drop_dblink, obrpc::OB_DROP_DBLINK, (ObDropDbLinkArg));
  //----End of definitions for managing dblinks----

  //----Definitions for managing synonyms----
  RPC_S(PRD create_synonym, obrpc::OB_CREATE_SYNONYM, (ObCreateSynonymArg));
  RPC_S(PRD drop_synonym, obrpc::OB_DROP_SYNONYM, (ObDropSynonymArg));
  //----End of definitions for managing synonyms----

  //----Definitions for managing plan_baselines----
  RPC_S(PR5 accept_plan_baseline, obrpc::OB_RS_ACCEPT_PLAN_BASELINE, (ObModifyPlanBaselineArg));
  RPC_S(PRD cancel_evolve_task, obrpc::OB_RS_CANCEL_EVOLVE_TASK, (ObModifyPlanBaselineArg));
  RPC_S(PR5 admin_load_baseline, obrpc::OB_ADMIN_LOAD_BASELINE, (ObLoadPlanBaselineArg));
  RPC_S(PR5 admin_load_baseline_v2, obrpc::OB_ADMIN_LOAD_BASELINE_V2, (ObLoadPlanBaselineArg), ObLoadBaselineRes);
  // RPC_S(PRD drop_plan_baseline, obrpc::OB_DROP_PLAN_BASELINE, (ObDropPlanBaselineArg));

  //----End of definitions for managing plan_baselines----

  //----Definitions for managing udf----
  RPC_S(PRD create_udf, obrpc::OB_CREATE_USER_DEFINED_FUNCTION, (ObCreateUserDefinedFunctionArg));
  RPC_S(PRD drop_udf, obrpc::OB_DROP_USER_DEFINED_FUNCTION, (ObDropUserDefinedFunctionArg));
  //----End of definitions for managing udf----

  //----Definitions for managing sequence----
  RPC_S(PRD do_sequence_ddl, obrpc::OB_DO_SEQUENCE_DDL, (ObSequenceDDLArg));
  //----End of definitions for managing sequence----

  //tablespace
  RPC_S(PRD do_tablespace_ddl, obrpc::OB_DO_TABLESPACE_DDL, (ObTablespaceDDLArg));

  //----Definitions for managing keystore----
  RPC_S(PRD do_keystore_ddl, obrpc::OB_DO_KEYSTORE_DDL, (ObKeystoreDDLArg));
  //----End of definitions for managing keystore----

  //----Definitions for managing label security----
  RPC_S(PRD handle_label_se_policy_ddl, obrpc::OB_HANDLE_LABEL_SE_POLICY_DDL, (ObLabelSePolicyDDLArg));
  RPC_S(PRD handle_label_se_component_ddl, obrpc::OB_HANDLE_LABEL_SE_COMPONENT_DDL, (ObLabelSeComponentDDLArg));
  RPC_S(PRD handle_label_se_label_ddl, obrpc::OB_HANDLE_LABEL_SE_LABEL_DDL, (ObLabelSeLabelDDLArg));
  RPC_S(PRD handle_label_se_user_level_ddl, obrpc::OB_HANDLE_LABEL_SE_USER_LEVEL_DDL, (ObLabelSeUserLevelDDLArg));
  //----End of definitions for managing label security----

  //----Definitions for managing profile----
  RPC_S(PRD do_profile_ddl, obrpc::OB_DO_PROFILE_DDL, (ObProfileDDLArg));
  //----End of definitions for managing profile----

  RPC_S(PRD create_package, obrpc::OB_CREATE_PACKAGE, (ObCreatePackageArg));
  RPC_S(PRD create_package_with_res, obrpc::OB_CREATE_PACKAGE_WITH_RES, (ObCreatePackageArg), ObRoutineDDLRes);
  RPC_S(PRD alter_package, obrpc::OB_ALTER_PACKAGE, (ObAlterPackageArg));
  RPC_S(PRD alter_package_with_res, obrpc::OB_ALTER_PACKAGE_WITH_RES, (ObAlterPackageArg), ObRoutineDDLRes);
  RPC_S(PRD drop_package, obrpc::OB_DROP_PACKAGE, (ObDropPackageArg));

  RPC_S(PRD create_trigger, obrpc::OB_CREATE_TRIGGER, (ObCreateTriggerArg));
  RPC_S(PRD create_trigger_with_res, obrpc::OB_CREATE_TRIGGER_WITH_RES, (ObCreateTriggerArg), ObCreateTriggerRes);
  RPC_S(PRD alter_trigger, obrpc::OB_ALTER_TRIGGER, (ObAlterTriggerArg));
  RPC_S(PRD alter_trigger_with_res, obrpc::OB_ALTER_TRIGGER_WITH_RES, (ObAlterTriggerArg), ObRoutineDDLRes);
  RPC_S(PRD drop_trigger, obrpc::OB_DROP_TRIGGER, (ObDropTriggerArg));

  RPC_S(PR5 execute_bootstrap, OB_EXECUTE_BOOTSTRAP, (ObBootstrapArg));
  RPC_S(PR5 refresh_config, OB_REFRESH_CONFIG);

  RPC_S(PR5 create_resource_unit, obrpc::OB_CREATE_RESOURCE_UNIT, (ObCreateResourceUnitArg));
  RPC_S(PR5 alter_resource_unit, obrpc::OB_ALTER_RESOURCE_UNIT, (ObAlterResourceUnitArg));
  RPC_S(PR5 drop_resource_unit, obrpc::OB_DROP_RESOURCE_UNIT, (ObDropResourceUnitArg));
  RPC_S(PRD clone_resource_pool, obrpc::OB_CLONE_RESOURCE_POOL, (ObCloneResourcePoolArg));
  RPC_S(PRD create_resource_pool, obrpc::OB_CREATE_RESOURCE_POOL, (ObCreateResourcePoolArg));
  RPC_S(PRD alter_resource_pool, obrpc::OB_ALTER_RESOURCE_POOL, (ObAlterResourcePoolArg));
  RPC_S(PRD drop_resource_pool, obrpc::OB_DROP_RESOURCE_POOL, (ObDropResourcePoolArg));
  RPC_S(PRD split_resource_pool, obrpc::OB_SPLIT_RESOURCE_POOL, (ObSplitResourcePoolArg));
  RPC_S(PRD merge_resource_pool, obrpc::OB_MERGE_RESOURCE_POOL, (ObMergeResourcePoolArg));
  RPC_S(PRD alter_resource_tenant, obrpc::OB_ALTER_RESOURCE_TENANT, (ObAlterResourceTenantArg));
  RPC_S(PRD update_index_status, obrpc::OB_UPDATE_INDEX_TABLE_STATUS, (ObUpdateIndexStatusArg));
  RPC_S(PRD update_mview_status, obrpc::OB_UPDATE_MVIEW_TABLE_STATUS, (ObUpdateMViewStatusArg));
  RPC_S(PRD parallel_update_index_status, obrpc::OB_PARALLEL_UPDATE_INDEX_STATUS, (ObUpdateIndexStatusArg), ObParallelDDLRes);
  RPC_S(PRD drop_lob, obrpc::OB_DROP_LOB, (ObDropLobArg));

  // define system admin rpc (alter system ...)
  RPC_S(PR5 root_minor_freeze, obrpc::OB_ROOT_MINOR_FREEZE, (ObRootMinorFreezeArg));
  RPC_S(PR5 admin_switch_replica_role, obrpc::OB_ADMIN_SWITCH_REPLICA_ROLE, (ObAdminSwitchReplicaRoleArg));
  RPC_S(PR5 admin_switch_rs_role, obrpc::OB_ADMIN_SWITCH_RS_ROLE, (ObAdminSwitchRSRoleArg));
  RPC_S(PR5 admin_drop_replica, obrpc::OB_ADMIN_DROP_REPLICA, (ObAdminDropReplicaArg));
  RPC_S(PR5 admin_change_replica, obrpc::OB_ADMIN_CHANGE_REPLICA, (ObAdminChangeReplicaArg));
  RPC_S(PR5 admin_migrate_replica, obrpc::OB_ADMIN_MIGRATE_REPLICA, (ObAdminMigrateReplicaArg));
  RPC_S(PR5 admin_report_replica, obrpc::OB_ADMIN_REPORT_REPLICA, (ObAdminReportReplicaArg));
  RPC_S(PR5 admin_recycle_replica, obrpc::OB_ADMIN_RECYCLE_REPLICA, (ObAdminRecycleReplicaArg));
  RPC_S(PR5 admin_merge, obrpc::OB_ADMIN_MERGE, (ObAdminMergeArg));
  RPC_S(PR5 admin_recovery, obrpc::OB_ADMIN_RECOVERY, (ObAdminRecoveryArg));
  RPC_S(PR5 admin_clear_roottable, obrpc::OB_ADMIN_CLEAR_ROOTTABLE, (ObAdminClearRoottableArg));
  RPC_S(PR5 admin_refresh_schema, obrpc::OB_ADMIN_REFRESH_SCHEMA, (ObAdminRefreshSchemaArg));
  RPC_S(PR5 admin_refresh_memory_stat, obrpc::OB_ADMIN_REFRESH_MEMORY_STAT, (ObAdminRefreshMemStatArg));
  RPC_S(PR5 admin_wash_memory_fragmentation, obrpc::OB_ADMIN_WASH_MEMORY_FRAGMENTATION, (ObAdminWashMemFragmentationArg));
  RPC_S(PR5 admin_refresh_io_calibration, obrpc::OB_ADMIN_REFRESH_IO_CALIBRATION, (ObAdminRefreshIOCalibrationArg));
  RPC_S(PR5 admin_set_config, obrpc::OB_ADMIN_SET_CONFIG, (ObAdminSetConfigArg));
  RPC_S(PR5 admin_clear_location_cache, obrpc::OB_ADMIN_CLEAR_LOCATION_CACHE, (ObAdminClearLocationCacheArg));
  RPC_S(PR5 admin_reload_unit, obrpc::OB_ADMIN_RELOAD_UNIT);
  RPC_S(PR5 admin_reload_server, obrpc::OB_ADMIN_RELOAD_SERVER);
  RPC_S(PR5 admin_reload_zone, obrpc::OB_ADMIN_RELOAD_ZONE);
  RPC_S(PR5 admin_clear_merge_error, obrpc::OB_ADMIN_CLEAR_MERGE_ERROR, (ObAdminMergeArg));
  RPC_S(PR5 admin_migrate_unit, obrpc::OB_ADMIN_MIGRATE_UNIT, (ObAdminMigrateUnitArg));
  RPC_S(PR5 admin_alter_ls_replica, obrpc::OB_ADMIN_ALTER_LS_REPLICA, (ObAdminAlterLSReplicaArg));
  RPC_S(PRD admin_upgrade_virtual_schema, obrpc::OB_ADMIN_UPGRADE_VIRTUAL_SCHEMA);
  RPC_S(PRD run_job, obrpc::OB_RUN_JOB, (ObRunJobArg));
  RPC_S(PRD run_upgrade_job, obrpc::OB_RUN_UPGRADE_JOB, (ObUpgradeJobArg));
  RPC_S(PRD upgrade_table_schema, obrpc::OB_UPGRADE_TABLE_SCHEMA, (ObUpgradeTableSchemaArg));
  RPC_S(PR5 admin_flush_cache, obrpc::OB_ADMIN_FLUSH_CACHE, (ObAdminFlushCacheArg));
  RPC_S(PR5 admin_upgrade_cmd, obrpc::OB_ADMIN_UPGRADE_CMD, (Bool));
  RPC_S(PR5 admin_rolling_upgrade_cmd, obrpc::OB_ADMIN_ROLLING_UPGRADE_CMD, (ObAdminRollingUpgradeArg));
  RPC_S(PR5 admin_clear_balance_task, obrpc::OB_ADMIN_FLUSH_BALANCE_INFO, (ObAdminClearBalanceTaskArg));
  RPC_S(PRD get_tenant_schema_versions, obrpc::OB_GET_TENANT_SCHEMA_VERSIONS, (ObGetSchemaArg), obrpc::ObTenantSchemaVersions);
  // RPC_S(PRD update_freeze_schema_version, obrpc::OB_UPDATE_FREEZE_SCHEMA_VERSIONS, (Int64), obrpc::ObTenantSchemaVersions);

  RPC_S(PR5 add_server, obrpc::OB_ADD_SERVER, (ObAdminServerArg));
  RPC_S(PR5 delete_server, obrpc::OB_DELETE_SERVER, (ObAdminServerArg));
  RPC_S(PR5 cancel_delete_server, obrpc::OB_CANCEL_DELETE_SERVER, (ObAdminServerArg));
  RPC_S(PR5 start_server, obrpc::OB_START_SERVER, (ObAdminServerArg));
  RPC_S(PR5 stop_server, obrpc::OB_STOP_SERVER, (ObAdminServerArg));
  RPC_S(PR5 add_zone, obrpc::OB_ADD_ZONE, (ObAdminZoneArg));
  RPC_S(PR5 delete_zone, obrpc::OB_DELETE_ZONE, (ObAdminZoneArg));
  RPC_S(PR5 start_zone, obrpc::OB_START_ZONE, (ObAdminZoneArg));
  RPC_S(PR5 stop_zone, obrpc::OB_STOP_ZONE, (ObAdminZoneArg));
  RPC_S(PR5 alter_zone, obrpc::OB_ALTER_ZONE, (ObAdminZoneArg));
  RPC_S(PR5 add_storage, obrpc::OB_ADD_STORAGE, (ObAdminStorageArg));
  RPC_S(PR5 drop_storage, obrpc::OB_DROP_STORAGE, (ObAdminStorageArg));
  RPC_S(PR5 alter_storage, obrpc::OB_ALTER_STORAGE, (ObAdminStorageArg));
  RPC_S(PR5 admin_set_tracepoint, obrpc::OB_RS_SET_TP, (ObAdminSetTPArg));
  RPC_S(PR5 refresh_time_zone_info, obrpc::OB_REFRESH_TIME_ZONE_INFO, (ObRefreshTimezoneArg));
  RPC_S(PR5 request_time_zone_info, obrpc::OB_REQUEST_TIME_ZONE_INFO, (common::ObRequestTZInfoArg), common::ObRequestTZInfoResult);
  RPC_S(PR5 calc_column_checksum_response, obrpc::OB_CALC_COLUMN_CHECKSUM_RESPONSE, (obrpc::ObCalcColumnChecksumResponseArg));
  RPC_S(PR5 build_ddl_single_replica_response, obrpc::OB_DDL_BUILD_SINGLE_REPLICA_RESPONSE, (obrpc::ObDDLBuildSingleReplicaResponseArg));
  RPC_S(PRD clean_splitted_tablet, obrpc::OB_CLEAN_SPLITTED_TABLET, (ObCleanSplittedTabletArg));
  RPC_S(PR5 send_auto_split_tablet_task_request, obrpc::OB_AUTO_SPLIT_TABLET_TASK_REQUEST, (obrpc::ObAutoSplitTabletBatchArg), obrpc::ObAutoSplitTabletBatchRes);
  RPC_S(PR5 cancel_ddl_task, obrpc::OB_CANCEL_DDL_TASK, (obrpc::ObCancelDDLTaskArg));
  RPC_S(PR5 start_redef_table, obrpc::OB_START_REDEF_TABLE, (ObStartRedefTableArg), ObStartRedefTableRes);
  RPC_S(PR5 copy_table_dependents, obrpc::OB_COPY_TABLE_DEPENDENTS, (ObCopyTableDependentsArg));
  RPC_S(PR5 finish_redef_table, obrpc::OB_FINISH_REDEF_TABLE, (ObFinishRedefTableArg));
  RPC_S(PR5 abort_redef_table, obrpc::OB_ABORT_REDEF_TABLE, (obrpc::ObAbortRedefTableArg));
  RPC_S(PR5 update_ddl_task_active_time, obrpc::OB_UPDATE_DDL_TASK_ACTIVE_TIME, (obrpc::ObUpdateDDLTaskActiveTimeArg));

  RPC_S(PR5 disaster_recovery_task_reply, OB_DISASTER_RECOVERY_TASK_REPLY, (ObDRTaskReplyResult));
  RPC_S(PR5 backup_compl_log_res, obrpc::OB_BACKUP_COMPL_LOG_RES, (ObBackupTaskRes));

  RPC_S(PRD commit_alter_tenant_locality, OB_COMMIT_ALTER_TENANT_LOCALITY, (rootserver::ObCommitAlterTenantLocalityArg));
  RPC_S(PR5 update_stat_cache, OB_RS_UPDATE_STAT_CACHE, (ObUpdateStatCacheArg));


  RPC_S(PRD force_create_sys_table, OB_FORCE_CREATE_SYS_TABLE, (obrpc::ObForceCreateSysTableArg));
  RPC_S(PRD force_set_locality, OB_FORCE_SET_LOCALITY, (obrpc::ObForceSetLocalityArg));
  //RPC_S(PR5 get_cluster_info, obrpc::OB_GET_CLUSTER_INFO, (obrpc::ObGetClusterInfoArg), share::ObClusterInfo);
  //RPC_S(PRD log_nop_operation, obrpc::OB_LOG_DDL_NOP_OPERATOR, (obrpc::ObDDLNopOpreatorArg));
  RPC_S(PRD broadcast_schema, OB_BROADCAST_SCHEMA, (obrpc::ObBroadcastSchemaArg));
  //RPC_S(PR5 get_switchover_status, OB_GET_SWITCHOVER_STATUS, obrpc::ObGetSwitchoverStatusRes);
  RPC_S(PR5 get_recycle_schema_versions, OB_GET_RECYCLE_SCHEMA_VERSIONS, (obrpc::ObGetRecycleSchemaVersionsArg), obrpc::ObGetRecycleSchemaVersionsResult);

  // backup and restore
  RPC_S(PRD physical_restore_tenant, OB_PHYSICAL_RESTORE_TENANT, (obrpc::ObPhysicalRestoreTenantArg), obrpc::Int64);
  RPC_S(PRD rebuild_index_in_restore, OB_REBUILD_INDEX_IN_RESTORE, (obrpc::ObRebuildIndexInRestoreArg));
  RPC_S(PR5 archive_log, obrpc::OB_ARCHIVE_LOG, (ObArchiveLogArg));
  RPC_S(PRD backup_database, obrpc::OB_BACKUP_DATABASE, (ObBackupDatabaseArg)); // use ddl thread
  RPC_S(PR5 backup_manage, obrpc::OB_BACKUP_MANAGE, (ObBackupManageArg));
  RPC_S(PR5 backup_delete, obrpc::OB_BACKUP_CLEAN, (obrpc::ObBackupCleanArg));
  RPC_S(PR5 delete_policy, obrpc::OB_DELETE_POLICY, (obrpc::ObDeletePolicyArg));
  RPC_S(PR5 recover_table, obrpc::OB_RECOVER_TABLE, (obrpc::ObRecoverTableArg));
  //RPC_S(PRD standby_upgrade_virtual_schema, obrpc::OB_UPGRADE_STANDBY_SCHEMA,
  //          (ObDDLNopOpreatorArg)); // use ddl thread
  RPC_S(PR5 check_backup_scheduler_working, obrpc::OB_CHECK_BACKUP_SCHEDULER_WORKING, Bool);
  RPC_S(PR5 send_physical_restore_result, obrpc::OB_PHYSICAL_RESTORE_RES, (obrpc::ObPhysicalRestoreResult));
  RPC_S(PRD clone_tenant, obrpc::OB_CLONE_TENANT, (ObCloneTenantArg), ObCloneTenantRes);

  // auto part ddl
  RPC_S(PRD create_restore_point, obrpc::OB_CREATE_RESTORE_POINT, (ObCreateRestorePointArg));
  RPC_S(PRD drop_restore_point, obrpc::OB_DROP_RESTORE_POINT, (ObDropRestorePointArg));

  RPC_S(PR5 flush_opt_stat_monitoring_info, obrpc::OB_RS_FLUSH_OPT_STAT_MONITORING_INFO, (obrpc::ObFlushOptStatArg));

  //----Definitions for directory object----
  RPC_S(PRD create_directory, obrpc::OB_CREATE_DIRECTORY, (ObCreateDirectoryArg));
  RPC_S(PRD drop_directory, obrpc::OB_DROP_DIRECTORY, (ObDropDirectoryArg));
  //----End of definitions for directory object----

  //----Definitions for Application Context----
  RPC_S(PRD do_context_ddl, obrpc::OB_DO_CONTEXT_DDL, (ObContextDDLArg));
  //----End of definitions for Application Context----

  //----Definitions for sync rewrite rules----
  RPC_S(PR5 admin_sync_rewrite_rules, obrpc::OB_ADMIN_SYNC_REWRITE_RULES, (ObSyncRewriteRuleArg));
  //----End of Definitions for sync rewrite rules----

#ifdef OB_BUILD_ARBITRATION
  RPC_S(PR5 admin_add_arbitration_service, obrpc::OB_ADMIN_ADD_ARBITRATION_SERVICE, (ObAdminAddArbitrationServiceArg));
  RPC_S(PR5 admin_remove_arbitration_service, obrpc::OB_ADMIN_REMOVE_ARBITRATION_SERVICE, (ObAdminRemoveArbitrationServiceArg));
  RPC_S(PR5 admin_replace_arbitration_service, obrpc::OB_ADMIN_REPLACE_ARBITRATION_SERVICE, (ObAdminReplaceArbitrationServiceArg));
  RPC_S(PR5 remove_cluster_info_from_arb_server, obrpc::OB_REMOVE_CLUSTER_INFO_FROM_ARB_SERVER, (ObRemoveClusterInfoFromArbServerArg));
#endif
  //----Definitions for managing row level security----
  RPC_S(PRD handle_rls_policy_ddl, obrpc::OB_HANDLE_RLS_POLICY_DDL, (ObRlsPolicyDDLArg));
  RPC_S(PRD handle_rls_group_ddl, obrpc::OB_HANDLE_RLS_GROUP_DDL, (ObRlsGroupDDLArg));
  RPC_S(PRD handle_rls_context_ddl, obrpc::OB_HANDLE_RLS_CONTEXT_DDL, (ObRlsContextDDLArg));
  //----End of definitions for managing row level security----

  RPC_S(PRD recompile_all_views_batch, obrpc::OB_RECOMPILE_ALL_VIEWS_BATCH, (ObRecompileAllViewsBatchArg));
  RPC_S(PRD try_add_dep_infos_for_synonym_batch, obrpc::OB_TRY_ADD_DEP_INFOS_FOR_SYNONYM_BATCH, (ObTryAddDepInofsForSynonymBatchArg));
#ifdef OB_BUILD_TDE_SECURITY
  RPC_S(PR5 get_root_key, obrpc::OB_GET_ROOT_KEY, (obrpc::ObRootKeyArg), obrpc::ObRootKeyResult);
  RPC_S(PR5 reload_master_key, obrpc::OB_RELOAD_MASTER_KEY, (obrpc::ObReloadMasterKeyArg), obrpc::ObReloadMasterKeyResult);
#endif
  RPC_S(PRD alter_user_proxy, obrpc::OB_ALTER_USER_PROXY, (ObAlterUserProxyArg), obrpc::ObAlterUserProxyRes);
  //Rebuild Tablet
  RPC_S(PR5 root_rebuild_tablet, obrpc::OB_ROOT_REBUILD_TABLET, (ObRebuildTabletArg));
