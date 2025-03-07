#include "rpc/obrpc/ob_rpc_proxy_macros.h"

  // special usage when can't deliver request.
  RPC_S(PR5 nouse, OB_ERROR_PACKET);

  RPC_S(PR5 set_config, OB_SET_CONFIG, (common::ObString));
  RPC_S(PR5 get_config, OB_GET_CONFIG, common::ObString);
  RPC_S(PR5 set_tenant_config, OB_SET_TENANT_CONFIG, (obrpc::ObTenantConfigArg));
  RPC_S(PR1 get_diagnose_args, OB_GET_DIAGNOSE_ARGS, common::ObString);


  RPC_AP(PR5 notify_tenant_server_unit_resource, OB_NOTIFY_TENANT_SERVER_UNIT_RESOURCE, (obrpc::TenantServerUnitConfig));
  RPC_AP(PR5 get_wrs_info, OB_GET_WRS_INFO, (ObGetWRSArg), ObGetWRSResult);
  RPC_AP(PR5 stop_write, OB_PARTITION_STOP_WRITE, (obrpc::Int64), obrpc::Int64);
  RPC_AP(PR5 check_log, OB_PARTITION_CHECK_LOG, (obrpc::Int64), obrpc::Int64);
  RPC_AP(PR5 check_frozen_scn, OB_CHECK_FROZEN_SCN, (obrpc::ObCheckFrozenScnArg));
  RPC_AP(PR5 get_min_sstable_schema_version, OB_GET_MIN_SSTABLE_SCHEMA_VERSION,
      (obrpc::ObGetMinSSTableSchemaVersionArg), obrpc::ObGetMinSSTableSchemaVersionRes);
  RPC_AP(PR5 init_tenant_config, OB_INIT_TENANT_CONFIG,
      (obrpc::ObInitTenantConfigArg), obrpc::ObInitTenantConfigRes);
  RPC_AP(PR3 get_leader_locations, OB_GET_LEADER_LOCATIONS,
      (obrpc::ObGetLeaderLocationsArg), obrpc::ObGetLeaderLocationsResult);
  RPC_S(PR5 fetch_sys_ls, OB_FETCH_SYS_LS,
        share::ObLSReplica);
  RPC_S(PR5 broadcast_rs_list, OB_BROADCAST_RS_LIST, (obrpc::ObRsListArg));
  // Rpc bulk interface for replication, migration, etc.
  RPC_S(PR5 delete_backup_ls_task, OB_DELETE_BACKUP_LS_TASK, (ObLSBackupCleanArg));
  RPC_S(PR5 backup_ls_data, OB_BACKUP_LS_DATA, (ObBackupDataArg));
  RPC_S(PR5 backup_completing_log, OB_BACKUP_COMPL_LOG, (ObBackupComplLogArg));
  RPC_S(PR5 backup_build_index, OB_BACKUP_BUILD_INDEX, (ObBackupBuildIdxArg));
  RPC_S(PR5 backup_meta, OB_BACKUP_META, (ObBackupMetaArg));
  RPC_S(PR5 backup_fuse_tablet_meta, OB_BACKUP_FUSE_TABLET_META, (ObBackupFuseTabletMetaArg));
  RPC_S(PR5 check_backup_task_exist, OB_CHECK_BACKUP_TASK_EXIST, (ObBackupCheckTaskArg), obrpc::Bool);
  RPC_S(PR5 report_backup_over, OB_BACKUP_LS_DATA_RES, (ObBackupTaskRes));
  RPC_S(PR5 report_backup_clean_over, OB_DELETE_BACKUP_LS_TASK_RES, (ObBackupTaskRes));
  RPC_S(PR5 notify_archive, OB_NOTIFY_ARCHIVE, (ObNotifyArchiveArg));

  // ls disaster recovery rpc
  RPC_S(PR5 disaster_recovery_task_reply, OB_DISASTER_RECOVERY_TASK_REPLY, (ObDRTaskReplyResult));
  RPC_S(PR5 ls_cancel_replica_task, OB_LS_CANCEL_REPLICA_TASK, (ObLSCancelReplicaTaskArg));
  RPC_S(PR5 ls_migrate_replica, OB_LS_MIGRATE_REPLICA, (ObLSMigrateReplicaArg));
  RPC_S(PR5 ls_add_replica, OB_LS_ADD_REPLICA, (ObLSAddReplicaArg));
  RPC_S(PR5 ls_type_transform, OB_LS_TYPE_TRANSFORM, (ObLSChangeReplicaArg));
  RPC_S(PR5 ls_remove_paxos_replica, OB_LS_REMOVE_PAXOS_REPLICA, (ObLSDropPaxosReplicaArg));
  RPC_S(PR5 ls_remove_nonpaxos_replica, OB_LS_REMOVE_NONPAXOS_REPLICA, (ObLSDropNonPaxosReplicaArg));
  RPC_S(PR5 ls_modify_paxos_replica_number, OB_LS_MODIFY_PAXOS_REPLICA_NUMBER, (ObLSModifyPaxosReplicaNumberArg));
  RPC_S(PR5 ls_check_dr_task_exist, OB_LS_CHECK_DR_TASK_EXIST, (ObDRTaskExistArg), obrpc::Bool);
  RPC_S(PR5 ob_exec_drtask_obadmin_command, OB_EXEC_DRTASK_OBADMIN_COMMAND, (ObAdminCommandArg));
#ifdef OB_BUILD_ARBITRATION
  RPC_S(PR5 add_arb, OB_ADD_ARB, (ObAddArbArg), obrpc::ObAddArbResult);
  RPC_S(PR5 remove_arb, OB_REMOVE_ARB, (ObRemoveArbArg), obrpc::ObRemoveArbResult);
#endif
  RPC_S(PR5 checkpoint_slog, OB_CHECKPOINT_SLOG, (ObCheckpointSlogArg));

  RPC_AP(PR5 minor_freeze, OB_MINOR_FREEZE, (ObMinorFreezeArg), obrpc::Int64);
  RPC_AP(PR5 check_schema_version_elapsed, OB_CHECK_SCHEMA_VERSION_ELAPSED, (ObCheckSchemaVersionElapsedArg), ObCheckSchemaVersionElapsedResult);
  RPC_AP(PR5 check_memtable_cnt, OB_CHECK_MEMTABLE_CNT, (ObCheckMemtableCntArg), ObCheckMemtableCntResult);
  RPC_AP(PR5 check_medium_compaction_info_list_cnt, OB_CHECK_MEDIUM_INFO_LIST_CNT, (ObCheckMediumCompactionInfoListArg), ObCheckMediumCompactionInfoListResult);
  RPC_AP(PR5 check_modify_time_elapsed, OB_CHECK_MODIFY_TIME_ELAPSED, (ObCheckModifyTimeElapsedArg), ObCheckModifyTimeElapsedResult);

  RPC_AP(PR5 check_ddl_tablet_merge_status, OB_DDL_CHECK_TABLET_MERGE_STATUS, (ObDDLCheckTabletMergeStatusArg), ObDDLCheckTabletMergeStatusResult);
  RPC_S(PR5 switch_leader, OB_SWITCH_LEADER, (ObSwitchLeaderArg));
  RPC_S(PR5 batch_switch_rs_leader, OB_BATCH_SWITCH_RS_LEADER, (ObAddr));
  RPC_S(PR5 get_partition_count, OB_GET_PARTITION_COUNT,
        ObGetPartitionCountResult);
  RPC_AP(PR5 switch_schema, OB_SWITCH_SCHEMA, (ObSwitchSchemaArg), obrpc::ObSwitchSchemaResult);
  RPC_S(PR5 refresh_memory_stat, OB_REFRESH_MEMORY_STAT);
  RPC_S(PR5 wash_memory_fragmentation, OB_WASH_MEMORY_FRAGMENTATION);
  RPC_S(PR5 bootstrap, OB_BOOTSTRAP, (ObBootstrapArg));
  RPC_S(PR5 check_server_empty, OB_CHECK_SERVER_EMPTY, (ObCheckServerEmptyArg), Bool);
  RPC_S(PR5 check_server_empty_with_result, OB_CHECK_SERVER_EMPTY_WITH_RESULT, (ObCheckServerEmptyArg), ObCheckServerEmptyResult);
  RPC_S(PR5 prepare_server_for_adding_server, OB_PREPARE_SERVER_FOR_ADDING_SERVER, (ObPrepareServerForAddingServerArg), ObPrepareServerForAddingServerResult);
  RPC_S(PR5 check_server_machine_status, OB_CHECK_SERVER_MACHINE_STATUS, (ObCheckServerMachineStatusArg), ObCheckServerMachineStatusResult);
  RPC_S(PR5 check_deployment_mode_match, OB_CHECK_DEPLOYMENT_MODE, (ObCheckDeploymentModeArg), Bool);
#ifdef OB_BUILD_TDE_SECURITY
  RPC_S(PR5 wait_master_key_in_sync, OB_WAIT_MASTER_KEY_IN_SYNC, (ObWaitMasterKeyInSyncArg));
#endif
  RPC_S(PR5 notify_create_tenant_user_ls, OB_NOTIFY_CREATE_TENANT_USER_LS, (obrpc::UInt64));
  RPC_S(PR5 report_replica, OB_REPORT_REPLICA);
  RPC_S(PR5 recycle_replica, OB_RECYCLE_REPLICA);
  RPC_S(PR5 clear_location_cache, OB_CLEAR_LOCATION_CACHE);
  RPC_S(PR5 refresh_sync_value, OB_REFRESH_SYNC_VALUE, (ObAutoincSyncArg));
  RPC_S(PR5 sync_auto_increment, OB_SYNC_AUTO_INCREMENT, (ObAutoincSyncArg));
  RPC_S(PR5 clear_autoinc_cache, OB_CLEAR_AUTOINC_CACHE, (ObAutoincSyncArg));
  RPC_S(PR5 dump_memtable, OB_DUMP_MEMTABLE, (ObDumpMemtableArg));
  RPC_S(PR5 dump_tx_data_memtable, OB_DUMP_TX_DATA_MEMTABLE, (ObDumpTxDataMemtableArg));
  RPC_S(PR5 dump_single_tx_data, OB_DUMP_SINGLE_TX_DATA, (ObDumpSingleTxDataArg));
  RPC_S(PR5 halt_all_prewarming, OB_FORCE_PURGE_MEMTABLE);
  RPC_AP(PR5 halt_all_prewarming_async, OB_FORCE_PURGE_MEMTABLE_ASYNC, (obrpc::UInt64));
  RPC_S(PR5 set_debug_sync_action, OB_SET_DS_ACTION, (obrpc::ObDebugSyncActionArg));
  RPC_S(PR5 request_heartbeat, OB_REQUEST_HEARTBEAT, share::ObLeaseRequest);
  RPC_S(PR5 check_partition_table, OB_CHECK_PARTITION_TABLE);
  RPC_S(PR5 execute_io_benchmark, OB_EXECUTE_IO_BENCHMARK);
  RPC_S(PR5 refresh_io_calibration, OB_REFRESH_IO_CALIBRATION, (obrpc::ObRefreshIOCalibrationArg));
  RPC_S(PR5 update_baseline_schema_version, OB_UPDATE_BASELINE_SCHEMA_VERSION, (obrpc::Int64));
  RPC_AP(PR1 detect_master_rs_ls, OB_DETECT_MASTER_RS_LS,
        (obrpc::ObDetectMasterRsArg), obrpc::ObDetectMasterRsLSResult);
  RPC_AP(PR1 get_root_server_status, OB_GET_ROOT_SERVER_ROLE,
        (obrpc::ObDetectMasterRsArg), obrpc::ObGetRootserverRoleResult);
  RPC_S(PR5 sync_partition_table, OB_SYNC_PARTITION_TABLE, (obrpc::Int64));
  RPC_S(PR5 flush_cache, OB_FLUSH_CACHE, (ObFlushCacheArg));
  RPC_S(PR5 set_tracepoint, OB_SET_TP, (obrpc::ObAdminSetTPArg));
  RPC_S(PR5 kill_session, OB_KILL_SESSION, (sql::ObKillSessionArg));
  RPC_S(PR5 cancel_sys_task, OB_CANCEL_SYS_TASK, (obrpc::ObCancelTaskArg));
  RPC_S(PR5 set_disk_valid, OB_SET_DISK_VALID, (ObSetDiskValidArg));
  RPC_S(PR5 add_disk, OB_ADD_DISK, (ObAdminAddDiskArg));
  RPC_S(PR5 drop_disk, OB_DROP_DISK, (ObAdminDropDiskArg));
  RPC_S(PR5 force_switch_ilog_file, OB_FORCE_SWITCH_ILOG_FILE, (ObForceSwitchILogFileArg));
  RPC_S(PR5 force_set_all_as_single_replica, OB_FORCE_SET_ALL_AS_SINGLE_REPLICA, (ObForceSetAllAsSingleReplicaArg));
  RPC_S(PR5 force_set_server_list, OB_FORCE_SET_SERVER_LIST, (ObForceSetServerListArg), obrpc::ObForceSetServerListResult);
  RPC_S(PR5 calc_column_checksum_request, OB_CALC_COLUMN_CHECKSUM_REQUEST, (ObCalcColumnChecksumRequestArg), obrpc::ObCalcColumnChecksumRequestRes);
  RPC_AP(PR5 build_ddl_single_replica_request, OB_DDL_BUILD_SINGLE_REPLICA_REQUEST, (obrpc::ObDDLBuildSingleReplicaRequestArg), obrpc::ObDDLBuildSingleReplicaRequestResult);
  RPC_S(PR5 build_split_tablet_data_start_request, OB_SPLIT_TABLET_DATA_START_REQUEST, (obrpc::ObTabletSplitStartArg), obrpc::ObTabletSplitStartResult);
  RPC_S(PR5 build_split_tablet_data_finish_request, OB_SPLIT_TABLET_DATA_FINISH_REQUEST, (obrpc::ObTabletSplitFinishArg), obrpc::ObTabletSplitFinishResult);
  RPC_S(PR5 freeze_split_src_tablet, OB_FREEZE_SPLIT_SRC_TABLET, (obrpc::ObFreezeSplitSrcTabletArg), obrpc::ObFreezeSplitSrcTabletRes);
  RPC_S(PR5 prepare_tablet_split_task_ranges, OB_PREPARE_TABLET_SPLIT_TASK_RANGES, (obrpc::ObPrepareSplitRangesArg), obrpc::ObPrepareSplitRangesRes);
  RPC_S(PR5 check_and_cancel_ddl_complement_dag, OB_CHECK_AND_CANCEL_DDL_COMPLEMENT_DAG, (ObDDLBuildSingleReplicaRequestArg), Bool);
  RPC_S(PR5 fetch_split_tablet_info, OB_FETCH_SPLIT_TABLET_INFO, (obrpc::ObFetchSplitTabletInfoArg), obrpc::ObFetchSplitTabletInfoRes);
  RPC_S(PR5 check_and_cancel_delete_lob_meta_row_dag, OB_CHECK_AND_CANCEL_DELETE_LOB_META_ROW_DAG, (ObDDLBuildSingleReplicaRequestArg), Bool);
  RPC_S(PR5 fetch_tablet_autoinc_seq_cache, OB_FETCH_TABLET_AUTOINC_SEQ_CACHE, (obrpc::ObFetchTabletSeqArg), obrpc::ObFetchTabletSeqRes);
  RPC_AP(PR5 batch_get_tablet_autoinc_seq, OB_BATCH_GET_TABLET_AUTOINC_SEQ, (obrpc::ObBatchGetTabletAutoincSeqArg), obrpc::ObBatchGetTabletAutoincSeqRes);
  RPC_AP(PR5 batch_set_tablet_autoinc_seq, OB_BATCH_SET_TABLET_AUTOINC_SEQ, (obrpc::ObBatchSetTabletAutoincSeqArg), obrpc::ObBatchSetTabletAutoincSeqRes);
  RPC_S(PR5 set_tablet_autoinc_seq, OB_SET_TABLET_AUTOINC_SEQ, (obrpc::ObBatchSetTabletAutoincSeqArg), obrpc::ObBatchSetTabletAutoincSeqRes);
  RPC_AP(PR5 clear_tablet_autoinc_seq_cache, OB_CLEAR_TABLET_AUTOINC_SEQ_CACHE, (obrpc::ObClearTabletAutoincSeqCacheArg), obrpc::Int64);
  RPC_S(PR5 batch_get_tablet_binding, OB_BATCH_GET_TABLET_BINDING, (obrpc::ObBatchGetTabletBindingArg), obrpc::ObBatchGetTabletBindingRes);
  RPC_S(PR5 batch_get_tablet_split, OB_BATCH_GET_TABLET_SPLIT, (obrpc::ObBatchGetTabletSplitArg), obrpc::ObBatchGetTabletSplitRes);
  RPC_S(PRD force_create_sys_table, OB_FORCE_CREATE_SYS_TABLE, (ObForceCreateSysTableArg));
  RPC_S(PRD schema_revise, OB_SCHEMA_REVISE, (ObSchemaReviseArg));
  RPC_S(PRD force_set_locality, OB_FORCE_SET_LOCALITY, (ObForceSetLocalityArg));
  RPC_S(PR5 force_disable_blacklist, OB_FORCE_DISABLE_BLACKLIST);
  RPC_S(PR5 force_enable_blacklist, OB_FORCE_ENABLE_BLACKLIST);
  RPC_S(PR5 force_clear_srv_blacklist, OB_FORCE_CLEAR_BLACKLIST);
  RPC_S(PR5 notify_create_duplicate_ls, OB_NOTIFY_CREATE_DUPLICATE_LS, (obrpc::ObCreateDupLSArg), obrpc::ObCreateDupLSResult);

  RPC_S(PR5 update_local_stat_cache, obrpc::OB_SERVER_UPDATE_STAT_CACHE, (ObUpdateStatCacheArg));
  // The optimizer estimates the number of rows
  RPC_S(PR5 estimate_partition_rows, OB_ESTIMATE_PARTITION_ROWS, (ObEstPartArg), ObEstPartRes);

  RPC_AP(PR1 ha_gts_ping_request, OB_HA_GTS_PING_REQUEST, (ObHaGtsPingRequest), ObHaGtsPingResponse);
  RPC_AP(PR1 ha_gts_get_request, OB_HA_GTS_GET_REQUEST, (ObHaGtsGetRequest));
  RPC_AP(PR1 ha_gts_get_response, OB_HA_GTS_GET_RESPONSE, (ObHaGtsGetResponse));
  RPC_AP(PR1 ha_gts_heartbeat, OB_HA_GTS_HEARTBEAT, (ObHaGtsHeartbeat));
  RPC_S(PR1 ha_gts_update_meta, OB_HA_GTS_UPDATE_META, (ObHaGtsUpdateMetaRequest),
      ObHaGtsUpdateMetaResponse);
  RPC_S(PR1 ha_gts_change_member, OB_HA_GTS_CHANGE_MEMBER, (ObHaGtsChangeMemberRequest),
      ObHaGtsChangeMemberResponse);
  RPC_S(PR5 get_tenant_refreshed_schema_version, OB_GET_TENANT_REFRESHED_SCHEMA_VERSION,
        (ObGetTenantSchemaVersionArg), ObGetTenantSchemaVersionResult);
  RPC_S(PR5 update_tenant_memory, OB_UPDATE_TENANT_MEMORY, (obrpc::ObTenantMemoryArg));
  RPC_S(PR5 renew_in_zone_hb, OB_RENEW_IN_ZONE_HB, (share::ObInZoneHbRequest), share::ObInZoneHbResponse);
  RPC_S(PR5 pre_process_server_status, OB_PRE_PROCESS_SERVER, (obrpc::ObPreProcessServerArg));
#ifdef OB_BUILD_TDE_SECURITY
  RPC_S(PR5 get_master_key, OB_GET_MASTER_KEY, (obrpc::Int64), ObGetMasterKeyResultArg);
  RPC_AP(PR5 restore_key, OB_RESTORE_KEY, (obrpc::ObRestoreKeyArg), obrpc::ObRestoreKeyResult);
  RPC_AP(PR5 set_root_key, OB_SET_ROOT_KEY, (obrpc::ObRootKeyArg), obrpc::ObRootKeyResult);
  RPC_AP(PR5 clone_key, OB_CLONE_KEY, (obrpc::ObCloneKeyArg), obrpc::ObCloneKeyResult);
  RPC_AP(PR5 trim_key_list, OB_TRIM_KEY_LIST, (obrpc::ObTrimKeyListArg), obrpc::ObTrimKeyListResult);
#endif
  RPC_S(PR5 handle_part_trans_ctx, OB_HANDLE_PART_TRANS_CTX, (obrpc::ObTrxToolArg), ObTrxToolRes);
  RPC_S(PR5 flush_local_opt_stat_monitoring_info, obrpc::OB_SERVER_FLUSH_OPT_STAT_MONITORING_INFO, (obrpc::ObFlushOptStatArg));
#ifdef OB_BUILD_TDE_SECURITY
  RPC_S(PR5 dump_tenant_cache_master_key, OB_DUMP_TENANT_CACHE_MASTER_KEY, (obrpc::UInt64), ObDumpCacheMasterKeyResultArg);
#endif
  RPC_AP(PR5 set_member_list, OB_SET_MEMBER_LIST, (obrpc::ObSetMemberListArgV2), obrpc::ObSetMemberListResult);
  RPC_AP(PR5 create_ls, OB_CREATE_LS, (obrpc::ObCreateLSArg), obrpc::ObCreateLSResult);
#ifdef OB_BUILD_ARBITRATION
  RPC_S(PR5 create_arb, OB_CREATE_ARB, (obrpc::ObCreateArbArg), obrpc::ObCreateArbResult);
  RPC_S(PR5 delete_arb, OB_DELETE_ARB, (obrpc::ObDeleteArbArg), obrpc::ObDeleteArbResult);
  RPC_S(PR5 arb_gc_notify, OB_ARB_GC_NOTIFY, (obrpc::ObArbGCNotifyArg), obrpc::ObArbGCNotifyResult);
  RPC_S(PR5 force_clear_arb_cluster_info, OB_LOG_FORCE_CLEAR_ARB_CLUSTER_INFO, (obrpc::ObForceClearArbClusterInfoArg));
  RPC_S(PR5 arb_cluster_op, OB_ARB_CLUSTER_OP, (obrpc::ObArbClusterOpArg), obrpc::ObArbClusterOpResult);
#endif
  RPC_AP(PR5 create_tablet, OB_CREATE_TABLET, (obrpc::ObBatchCreateTabletArg), obrpc::ObCreateTabletBatchRes);
  RPC_AP(PR5 batch_broadcast_schema, OB_BATCH_BROADCAST_SCHEMA, (obrpc::ObBatchBroadcastSchemaArg), obrpc::ObBatchBroadcastSchemaResult);
  RPC_AP(PR5 drop_tablet, OB_DROP_TABLET, (obrpc::ObBatchRemoveTabletArg), obrpc::ObRemoveTabletRes);
  RPC_AP(PR5 lock_table, OB_TABLE_LOCK_TASK, (transaction::tablelock::ObTableLockTaskRequest),
         transaction::tablelock::ObTableLockTaskResult);
  RPC_AP(PR4 unlock_table, OB_HIGH_PRIORITY_TABLE_LOCK_TASK, (transaction::tablelock::ObTableLockTaskRequest),
         transaction::tablelock::ObTableLockTaskResult);
  RPC_AP(PR5 batch_lock_obj, OB_BATCH_TABLE_LOCK_TASK, (transaction::tablelock::ObLockTaskBatchRequest<transaction::tablelock::ObLockParam>),
         transaction::tablelock::ObTableLockTaskResult);
  RPC_AP(PR4 batch_unlock_obj, OB_HIGH_PRIORITY_BATCH_TABLE_LOCK_TASK, (transaction::tablelock::ObLockTaskBatchRequest<transaction::tablelock::ObLockParam>),
         transaction::tablelock::ObTableLockTaskResult);
  RPC_AP(PR5 batch_replace_lock_obj, OB_BATCH_REPLACE_TABLE_LOCK_TASK, (transaction::tablelock::ObLockTaskBatchRequest<transaction::tablelock::ObReplaceLockParam>),
         transaction::tablelock::ObTableLockTaskResult);
  RPC_S(PR4 admin_remove_lock_op, OB_REMOVE_OBJ_LOCK, (transaction::tablelock::ObAdminRemoveLockOpArg));
  RPC_S(PR4 admin_update_lock_op, OB_UPDATE_OBJ_LOCK, (transaction::tablelock::ObAdminUpdateLockOpArg));
  RPC_S(PR5 remote_write_ddl_redo_log, OB_REMOTE_WRITE_DDL_REDO_LOG, (obrpc::ObRpcRemoteWriteDDLRedoLogArg));
  RPC_S(PR5 remote_write_ddl_commit_log, OB_REMOTE_WRITE_DDL_COMMIT_LOG, (obrpc::ObRpcRemoteWriteDDLCommitLogArg), obrpc::Int64);
  #ifdef OB_BUILD_SHARED_STORAGE
  RPC_S(PR5 remote_write_ddl_finsih_log, OB_REMOTE_WRITE_DDL_FINISH_LOG, (obrpc::ObRpcRemoteWriteDDLFinishLogArg));
  RPC_S(PR5 get_ss_macro_block, OB_GET_SS_MACRO_BLOCK, (obrpc::ObGetSSMacroBlockArg), obrpc::ObGetSSMacroBlockResult);
  RPC_S(PR5 sync_hot_micro_key, OB_SYNC_HOT_MICRO_KEY, (obrpc::ObLSSyncHotMicroKeyArg));
  RPC_S(PR5 get_ss_phy_block_info, OB_GET_SS_PHY_BLOCK_INFO, (obrpc::ObGetSSPhyBlockInfoArg), obrpc::ObGetSSPhyBlockInfoResult);
  RPC_S(PR5 get_ss_micro_block_meta, OB_GET_SS_MICRO_BLOCK_META, (obrpc::ObGetSSMicroBlockMetaArg), obrpc::ObGetSSMicroBlockMetaResult);
  RPC_S(PR5 get_ss_macro_block_by_uri, OB_GET_SS_MACRO_BLOCK_BY_URI, (obrpc::ObGetSSMacroBlockByURIArg), obrpc::ObGetSSMacroBlockByURIResult);
  RPC_S(PR5 del_ss_tablet_meta, OB_DEL_SS_TABLET_META, (obrpc::ObDelSSTabletMetaArg));
  RPC_S(PR5 enable_ss_micro_cache, OB_ENABLE_SS_MICRO_CACHE, (obrpc::ObEnableSSMicroCacheArg));
  RPC_S(PR5 get_ss_micro_cache_info, OB_GET_SS_MICRO_CACHE_INFO, (obrpc::ObGetSSMicroCacheInfoArg), obrpc::ObGetSSMicroCacheInfoResult);
  RPC_S(PR5 clear_ss_micro_cache, OB_CLEAR_SS_MICRO_CACHE, (obrpc::ObClearSSMicroCacheArg));
  RPC_S(PR5 del_ss_local_tmpfile, OB_DEL_SS_LOCAL_TMPFILE, (obrpc::ObDelSSLocalTmpFileArg));
  RPC_S(PR5 del_ss_local_major, OB_DEL_SS_LOCAL_MAJOR, (obrpc::ObDelSSLocalMajorArg));
  RPC_S(PR5 calibrate_ss_disk_space, OB_CALIBRATE_SS_DISK_SPACE, (obrpc::ObCalibrateSSDiskSpaceArg));
  RPC_S(PR5 del_ss_tablet_micro, OB_DEL_SS_TABLET_MICRO, (obrpc::ObDelSSTabletMicroArg));
  RPC_S(PR5 set_ss_ckpt_compressor, OB_SET_SS_CKPT_COMPRESSOR, (obrpc::ObSetSSCkptCompressorArg));
  #endif
  RPC_S(PR5 remote_write_ddl_inc_commit_log, OB_REMOTE_WRITE_DDL_INC_COMMIT_LOG, (obrpc::ObRpcRemoteWriteDDLIncCommitLogArg), ObRpcRemoteWriteDDLIncCommitLogRes);
  RPC_S(PR5 check_ls_can_offline, OB_CHECK_LS_CAN_OFFLINE, (obrpc::ObCheckLSCanOfflineArg));
  RPC_S(PR5 clean_sequence_cache, obrpc::OB_CLEAN_SEQUENCE_CACHE, (obrpc::UInt64), obrpc::ObSeqCleanCacheRes);
  RPC_S(PR5 register_tx_data, OB_REGISTER_TX_DATA, (ObRegisterTxDataArg), ObRegisterTxDataResult);
  RPC_S(PR5 query_ls_is_valid_member, OB_QUERY_LS_IS_VALID_MEMBER, (ObQueryLSIsValidMemberRequest),
      ObQueryLSIsValidMemberResponse);
  RPC_S(PR5 check_backup_dest_connectivity, OB_CHECK_BACKUP_DEST_CONNECTIVITY, (ObCheckBackupConnectivityArg));
  RPC_AP(PR1 get_ls_access_mode, OB_GET_LS_ACCESS_MODE, (obrpc::ObGetLSAccessModeInfoArg), obrpc::ObLSAccessModeInfo);
  RPC_AP(PR1 change_ls_access_mode, OB_CHANGE_LS_ACCESS_MODE, (obrpc::ObLSAccessModeInfo), obrpc::ObChangeLSAccessModeRes);
#ifdef OB_BUILD_ARBITRATION
  RPC_S(PR5 svr_accept_plan_baseline, obrpc::OB_SERVER_ACCEPT_PLAN_BASELINE, (obrpc::ObModifyPlanBaselineArg));
  RPC_S(PR5 svr_cancel_evolve_task, obrpc::OB_SERVER_CANCEL_EVOLVE_TASK, (obrpc::ObModifyPlanBaselineArg));
  RPC_S(PR5 load_baseline, OB_LOAD_BASELINE, (ObLoadPlanBaselineArg));
  RPC_S(PR5 load_baseline_v2, OB_LOAD_BASELINE_V2, (ObLoadPlanBaselineArg), obrpc::ObLoadBaselineRes);
#endif
  RPC_S(PR5 estimate_tablet_block_count, OB_ESTIMATE_TABLET_BLOCK_COUNT, (ObEstBlockArg), ObEstBlockRes);
  RPC_S(PR5 gen_unique_id, OB_GEN_UNIQUE_ID, (obrpc::UInt64), share::ObCommonID);
  RPC_S(PR5 start_transfer_task, OB_START_TRANSFER_TASK, (ObStartTransferTaskArg));
  RPC_S(PR5 finish_transfer_task, OB_FINISH_TRANSFER_TASK, (ObFinishTransferTaskArg));
  RPC_AP(PR1 get_ls_sync_scn, OB_GET_LS_SYNC_SCN, (obrpc::ObGetLSSyncScnArg), obrpc::ObGetLSSyncScnRes);
  RPC_AP(PR5 refresh_tenant_info, OB_REFRESH_TENANT_INFO, (obrpc::ObRefreshTenantInfoArg), obrpc::ObRefreshTenantInfoRes);
  RPC_S(PR5 sync_rewrite_rules, OB_SYNC_REWRITE_RULES, (ObSyncRewriteRuleArg));
  RPC_AP(PR1 get_ls_replayed_scn, OB_GET_LS_REPLAYED_SCN, (ObGetLSReplayedScnArg), obrpc::ObGetLSReplayedScnRes);
  RPC_S(PR5 force_set_ls_as_single_replica, OB_LOG_FORCE_SET_LS_AS_SINGLE_REPLICA, (obrpc::ObForceSetLSAsSingleReplicaArg));
  RPC_AP(PR5 net_endpoint_predict_ingress, OB_PREDICT_INGRESS_BW, (obrpc::ObNetEndpointPredictIngressArg), obrpc::ObNetEndpointPredictIngressRes);
  RPC_AP(PR5 net_endpoint_set_ingress, OB_SET_INGRESS_BW, (obrpc::ObNetEndpointSetIngressArg), obrpc::ObNetEndpointSetIngressRes);
  RPC_S(PR5 session_info_verification, OB_SESS_INFO_VERIFICATION, (ObSessInfoVerifyArg), ObSessionInfoVeriRes);
  RPC_S(PR5 detect_session_alive, OB_DETECT_SESSION_ALIVE, (obrpc::UInt64), obrpc::Bool);
  RPC_AP(PRZ handle_heartbeat, OB_SEND_HEARTBEAT, (share::ObHBRequest), share::ObHBResponse);
  RPC_AP(PR5 get_server_resource_info, OB_GET_SERVER_RESOURCE_INFO, (obrpc::ObGetServerResourceInfoArg), obrpc::ObGetServerResourceInfoResult);
  RPC_AP(PR5 notify_switch_leader, OB_NOTIFY_SWITCH_LEADER, (obrpc::ObNotifySwitchLeaderArg));
  RPC_AP(PR5 update_tenant_info_cache, OB_UPDATE_TENANT_INFO_CACHE, (obrpc::ObUpdateTenantInfoCacheArg), obrpc::ObUpdateTenantInfoCacheRes);
  RPC_AP(PR5 broadcast_consensus_version, OB_BROADCAST_CONSENSUS_VERSION, (obrpc::ObBroadcastConsensusVersionArg), obrpc::ObBroadcastConsensusVersionRes);
  RPC_S(PR5 direct_load_control, OB_DIRECT_LOAD_CONTROL, (observer::ObDirectLoadControlRequest), observer::ObDirectLoadControlResult);
  RPC_AP(PR5 check_storage_operation_status, OB_CHECK_STORAGE_OPERATION_STATUS, (obrpc::ObCheckStorageOperationStatusArg), obrpc::ObCheckStorageOperationStatusResult);
  RPC_S(PR5 direct_load_resource, OB_DIRECT_LOAD_RESOURCE, (observer::ObDirectLoadResourceOpRequest), observer::ObDirectLoadResourceOpResult);
  RPC_S(PR5 dispatch_ttl, OB_TABLE_TTL, (obrpc::ObTTLRequestArg), obrpc::ObTTLResponseArg);
  RPC_S(PR5 admin_unlock_member_list_op, OB_HA_UNLOCK_MEMBER_LIST, (obrpc::ObAdminUnlockMemberListOpArg));
  RPC_S(PR5 notify_tenant_snapshot_scheduler, OB_NOTIFY_TENANT_SNAPSHOT_SCHEDULER, (obrpc::ObNotifyTenantSnapshotSchedulerArg), obrpc::ObNotifyTenantSnapshotSchedulerResult);
  RPC_AP(PR5 inner_create_tenant_snapshot, OB_INNER_CREATE_TENANT_SNAPSHOT, (obrpc::ObInnerCreateTenantSnapshotArg), obrpc::ObInnerCreateTenantSnapshotResult);
  RPC_AP(PR5 inner_drop_tenant_snapshot, OB_INNER_DROP_TENANT_SNAPSHOT, (obrpc::ObInnerDropTenantSnapshotArg), obrpc::ObInnerDropTenantSnapshotResult);
  RPC_AP(PR5 flush_ls_archive, OB_FLUSH_LS_ARCHIVE, (obrpc::ObFlushLSArchiveArg), obrpc::Int64);
  RPC_S(PR5 notify_clone_scheduler, OB_NOTIFY_CLONE_SCHEDULER, (obrpc::ObNotifyCloneSchedulerArg), obrpc::ObNotifyCloneSchedulerResult);
  RPC_S(PR5 notify_tenant_thread, OB_NOTIFY_TENANT_THREAD, (obrpc::ObNotifyTenantThreadArg));
  RPC_AP(PR5 tablet_major_freeze, OB_TABLET_MAJOR_FREEZE, (ObTabletMajorFreezeArg), obrpc::Int64);
  RPC_AP(PR5 shared_storage_net_throt_predict, OB_SHARED_STORAGE_NET_THROT_PREDICT, (obrpc::ObSSNTEndpointArg), obrpc::ObSharedDeviceResourceArray);
  RPC_AP(PR5 shared_storage_net_throt_set, OB_SHARED_STORAGE_NET_THROT_SET, (obrpc::ObSharedDeviceResourceArray), obrpc::ObSSNTSetRes);
  RPC_AP(PR5 kill_client_session, OB_KILL_CLIENT_SESSION, (ObKillClientSessionArg), ObKillClientSessionRes);
  RPC_S(PR5 client_session_create_time, OB_CLIENT_SESSION_CONNECT_TIME, (ObClientSessionCreateTimeAndAuthArg), ObClientSessionCreateTimeAndAuthRes);
  RPC_AP(PR5 tablet_location_send, OB_TABLET_LOCATION_BROADCAST, (obrpc::ObTabletLocationSendArg), obrpc::ObTabletLocationSendResult);
  RPC_AP(PR5 all_server_tracer_send, OB_ALL_SERVER_TRACER_BROADCAST, (obrpc::ObRefreshTenantInfoArg), obrpc::Int64);
  RPC_S(PR5 cancel_gather_stats, OB_CANCEL_GATHER_STATS, (ObCancelGatherStatsArg));
  RPC_S(PR5 force_set_tenant_log_disk, OB_LOG_FORCE_SET_TENANT_LOG_DISK, (obrpc::ObForceSetTenantLogDiskArg));
  RPC_S(PR5 dump_server_usage, OB_FORCE_DUMP_SERVER_USAGE, (obrpc::ObDumpServerUsageRequest), obrpc::ObDumpServerUsageResult);
  RPC_S(PR5 change_external_storage_dest, obrpc::OB_CHANGE_EXTERNAL_STORAGE_DEST, (ObAdminSetConfigArg));
  RPC_AP(PR5 refresh_service_name, OB_REFRESH_SERVICE_NAME, (obrpc::ObRefreshServiceNameArg), obrpc::ObRefreshServiceNameRes);
  RPC_AP(PR5 get_tenant_logical_resource, OB_CAL_STANDBY_TENANT_PHY_RESOURCE, (obrpc::ObGetTenantResArg), obrpc::ObTenantLogicalRes);
  RPC_S(PR5 phy_res_calculate_by_unit, OB_CAL_UNIT_PHY_RESOURCE, (obrpc::Int64), share::ObMinPhyResourceResult);
  RPC_S(PR5 rpc_reverse_keepalive, OB_RPC_REVERSE_KEEPALIVE, (obrpc::ObRpcReverseKeepaliveArg), obrpc::ObRpcReverseKeepaliveResp);
  RPC_AP(PR5 kill_query_client_session, OB_KILL_QUERY_CLIENT_SESSION, (ObKillQueryClientSessionArg), obrpc::Int64);
  RPC_AP(PR5 collect_mv_merge_info, OB_COLLECT_MV_MERGE_INFO, (obrpc::ObCollectMvMergeInfoArg), obrpc::ObCollectMvMergeInfoResult);
  RPC_S(PR5 fetch_stable_member_list, OB_FETCH_STABLE_MEMBER_LIST, (obrpc::ObFetchStableMemberListArg), obrpc::ObFetchStableMemberListInfo);
  RPC_S(PR5 notify_shared_storage_info, OB_NOTIFY_SHARED_STORAGE_INFO, (obrpc::ObNotifySharedStorageInfoArg), obrpc::ObNotifySharedStorageInfoResult);
  RPC_S(PR5 notify_ls_restore_finish, OB_NOTIFY_LS_RESTORE_FINISH, (obrpc::ObNotifyLSRestoreFinishArg));
  RPC_S(PR5 notify_start_archive, OB_NOTIFY_START_ARCHIVE, (obrpc::ObNotifyStartArchiveArg));
  RPC_S(PR5 ha_rebuild_tablet, OB_HA_REBUILD_TABLET, (obrpc::ObRebuildTabletArg));
