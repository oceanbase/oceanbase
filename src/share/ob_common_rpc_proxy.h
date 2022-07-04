/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEABASE_SHARE_OB_COMMON_RPC_PROXY_H_
#define _OCEABASE_SHARE_OB_COMMON_RPC_PROXY_H_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_lease_struct.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_rs_mgr.h"
#include "share/ob_time_zone_info_manager.h"
#include "common/storage/ob_freeze_define.h"  // for ObFrozenStatus
#include "rootserver/ob_alter_locality_checker.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace obrpc {
class ObCommonRpcProxy : public obrpc::ObRpcProxy {
public:
  DEFINE_TO(ObCommonRpcProxy);

  // rootservice provided
  RPC_S(PRZ renew_lease, obrpc::OB_RENEW_LEASE, (oceanbase::share::ObLeaseRequest), share::ObLeaseResponse);
  RPC_S(PRZ cluster_heartbeat, obrpc::OB_CLUSTER_HB, (oceanbase::share::ObClusterAddr), obrpc::ObStandbyHeartBeatRes);
  RPC_S(
      PRZ cluster_regist, obrpc::OB_CLUSTER_REGIST, (oceanbase::obrpc::ObRegistClusterArg), obrpc::ObRegistClusterRes);
  RPC_S(PRZ get_schema_snapshot, obrpc::OB_GET_SCHEMA_SNAPSHOT, (oceanbase::obrpc::ObSchemaSnapshotArg),
      obrpc::ObSchemaSnapshotRes);
  RPC_S(PR5 get_root_partition, obrpc::OB_GET_ROOT_PARTITION, share::ObPartitionInfo);
  RPC_S(PR5 report_root_partition, obrpc::OB_REPORT_ROOT_PARTITION, (share::ObPartitionReplica));
  RPC_S(PR5 remove_root_partition, obrpc::OB_REMOVE_ROOT_PARTITION, (common::ObAddr));
  RPC_S(PR5 rebuild_root_partition, obrpc::OB_REBUILD_ROOT_PARTITION, (common::ObAddr));
  RPC_S(PR5 clear_rebuild_root_partition, obrpc::OB_CLEAR_REBUILD_ROOT_PARTITION, (common::ObAddr));
  RPC_S(PR5 fetch_location, obrpc::OB_FETCH_LOCATION, (UInt64), common::ObSArray<share::ObPartitionLocation>);
  RPC_S(PR5 merge_finish, obrpc::OB_MERGE_FINISH, (ObMergeFinishArg));
  RPC_S(PR5 merge_error, obrpc::OB_MERGE_ERROR, (ObMergeErrorArg));
  RPC_S(PR5 admin_rebuild_replica, obrpc::OB_ADMIN_REBUILD_REPLICA, (ObAdminRebuildReplicaArg));
  RPC_S(PR5 broadcast_ds_action, obrpc::OB_BROADCAST_DS_ACTION, (ObDebugSyncActionArg));
  RPC_S(PR5 sync_pt_finish, obrpc::OB_SYNC_PT_FINISH, (ObSyncPartitionTableFinishArg));
  RPC_S(PR5 sync_pg_pt_finish, obrpc::OB_SYNC_PG_PT_FINISH, (ObSyncPGPartitionMTFinishArg));
  RPC_S(PR5 check_dangling_replica_finish, obrpc::OB_CHECK_DANGLING_REPLICA_FINISH, (ObCheckDanglingReplicaFinishArg));
  // high priority for fetch_alive_server, make sure it will not be blocked while network partition
  RPC_S(PR3 fetch_alive_server, obrpc::OB_FETCH_ALIVE_SERVER, (ObFetchAliveServerArg), ObFetchAliveServerResult);

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
  RPC_S(PRD alter_table, obrpc::OB_ALTER_TABLE, (ObAlterTableArg), ObAlterTableRes);
  RPC_S(PRD alter_database, obrpc::OB_ALTER_DATABASE, (ObAlterDatabaseArg));
  RPC_S(PRD drop_database, obrpc::OB_DROP_DATABASE, (ObDropDatabaseArg), UInt64);
  RPC_S(PRD drop_tablegroup, obrpc::OB_DROP_TABLEGROUP, (ObDropTablegroupArg));
  RPC_S(PRD alter_tablegroup, obrpc::OB_ALTER_TABLEGROUP, (ObAlterTablegroupArg));
  RPC_S(PRD drop_table, obrpc::OB_DROP_TABLE, (ObDropTableArg));
  RPC_S(PRD rename_table, obrpc::OB_RENAME_TABLE, (ObRenameTableArg));
  RPC_S(PRD truncate_table, obrpc::OB_TRUNCATE_TABLE, (ObTruncateTableArg));
  RPC_S(PRD create_index, obrpc::OB_CREATE_INDEX, (ObCreateIndexArg), ObAlterTableRes);
  RPC_S(PRD drop_index, obrpc::OB_DROP_INDEX, (ObDropIndexArg));
  RPC_S(PRD flashback_index, obrpc::OB_FLASHBACK_INDEX, (ObFlashBackIndexArg));
  RPC_S(PRD purge_index, obrpc::OB_PURGE_INDEX, (ObPurgeIndexArg));
  RPC_S(PRD create_table_like, obrpc::OB_CREATE_TABLE_LIKE, (ObCreateTableLikeArg));
  RPC_S(PRD flashback_table_from_recyclebin, obrpc::OB_FLASHBACK_TABLE_FROM_RECYCLEBIN,
      (ObFlashBackTableFromRecyclebinArg));
  RPC_S(PRD purge_table, obrpc::OB_PURGE_TABLE, (ObPurgeTableArg));
  RPC_S(PRD flashback_database, obrpc::OB_FLASHBACK_DATABASE, (ObFlashBackDatabaseArg));
  RPC_S(PRD purge_database, obrpc::OB_PURGE_DATABASE, (ObPurgeDatabaseArg));
  RPC_S(PRD flashback_tenant, obrpc::OB_FLASHBACK_TENANT, (ObFlashBackTenantArg));
  RPC_S(PRD purge_tenant, obrpc::OB_PURGE_TENANT, (ObPurgeTenantArg));
  RPC_S(PRD purge_expire_recycle_objects, obrpc::OB_PURGE_EXPIRE_RECYCLE_OBJECTS, (ObPurgeRecycleBinArg), Int64);
  RPC_S(PRD optimize_table, obrpc::OB_OPTIMIZE_TABLE, (ObOptimizeTableArg));
  RPC_S(PR5 submit_build_index_task, obrpc::OB_SUBMIT_BUILD_INDEX_TASK, (ObSubmitBuildIndexTaskArg));

  //----Definitions for managing privileges----
  RPC_S(PRD create_user, obrpc::OB_CREATE_USER, (ObCreateUserArg), common::ObSArray<int64_t>);
  RPC_S(PRD drop_user, obrpc::OB_DROP_USER, (ObDropUserArg), common::ObSArray<int64_t>);
  RPC_S(PRD rename_user, obrpc::OB_RENAME_USER, (ObRenameUserArg), common::ObSArray<int64_t>);
  RPC_S(PRD set_passwd, obrpc::OB_SET_PASSWD, (ObSetPasswdArg));
  RPC_S(PRD grant, obrpc::OB_GRANT, (ObGrantArg));
  RPC_S(PRD standby_grant, obrpc::OB_STANDBY_GRANT, (ObStandbyGrantArg));
  RPC_S(PRD revoke_user, obrpc::OB_REVOKE_USER, (ObRevokeUserArg));
  RPC_S(PRD lock_user, obrpc::OB_LOCK_USER, (ObLockUserArg), common::ObSArray<int64_t>);
  RPC_S(PRD revoke_database, obrpc::OB_REVOKE_DB, (ObRevokeDBArg));
  RPC_S(PRD revoke_table, obrpc::OB_REVOKE_TABLE, (ObRevokeTableArg));
  RPC_S(PRD revoke_sys_priv, obrpc::OB_REVOKE_SYSPRIV, (ObRevokeSysPrivArg));
  RPC_S(PRD alter_user_profile, obrpc::OB_ALTER_USER_PROFILE, (ObAlterUserProfileArg));
  //----End of definitions for managing privileges----

  //----Definitions for managing outlines----
  RPC_S(PRD create_outline, obrpc::OB_CREATE_OUTLINE, (ObCreateOutlineArg));
  RPC_S(PRD alter_outline, obrpc::OB_ALTER_OUTLINE, (ObAlterOutlineArg));
  RPC_S(PRD drop_outline, obrpc::OB_DROP_OUTLINE, (ObDropOutlineArg));
  //----End of definitions for managing outlines----

  //----Definitions for managing dblinks----
  RPC_S(PRD create_dblink, obrpc::OB_CREATE_DBLINK, (ObCreateDbLinkArg));
  RPC_S(PRD drop_dblink, obrpc::OB_DROP_DBLINK, (ObDropDbLinkArg));
  //----End of definitions for managing dblinks----

  //----Definitions for managing synonyms----
  RPC_S(PRD create_synonym, obrpc::OB_CREATE_SYNONYM, (ObCreateSynonymArg));
  RPC_S(PRD drop_synonym, obrpc::OB_DROP_SYNONYM, (ObDropSynonymArg));
  //----End of definitions for managing synonyms----

  //----Definitions for managing plan_baselines----
  RPC_S(PRD create_plan_baseline, obrpc::OB_CREATE_PLAN_BASELINE, (ObCreatePlanBaselineArg));
  RPC_S(PRD drop_plan_baseline, obrpc::OB_DROP_PLAN_BASELINE, (ObDropPlanBaselineArg));
  RPC_S(PRD alter_plan_baseline, obrpc::OB_ALTER_PLAN_BASELINE, (ObAlterPlanBaselineArg));
  RPC_S(PR5 admin_load_baseline, obrpc::OB_ADMIN_LOAD_BASELINE, (ObAdminLoadBaselineArg));
  //----End of definitions for managing plan_baselines----

  //----Definitions for managing udf----
  RPC_S(PRD create_udf, obrpc::OB_CREATE_USER_DEFINED_FUNCTION, (ObCreateUserDefinedFunctionArg));
  RPC_S(PRD drop_udf, obrpc::OB_DROP_USER_DEFINED_FUNCTION, (ObDropUserDefinedFunctionArg));
  //----End of definitions for managing udf----

  //----Definitions for managing sequence----
  RPC_S(PRD do_sequence_ddl, obrpc::OB_DO_SEQUENCE_DDL, (ObSequenceDDLArg));
  //----End of definitions for managing sequence----

  //----Definitions for managing profile----
  RPC_S(PRD do_profile_ddl, obrpc::OB_DO_PROFILE_DDL, (ObProfileDDLArg));
  //----End of definitions for managing profile----

  RPC_S(PR5 execute_bootstrap, OB_EXECUTE_BOOTSTRAP, (ObBootstrapArg));
  RPC_S(PR5 refresh_config, OB_REFRESH_CONFIG);
  RPC_S(PR5 get_frozen_version, obrpc::OB_GET_FROZEN_VERSION, Int64);
  RPC_S(PR5 get_frozen_status, obrpc::OB_GET_FROZEN_STATUS, (Int64), storage::ObFrozenStatus);

  RPC_S(PR5 create_resource_unit, obrpc::OB_CREATE_RESOURCE_UNIT, (ObCreateResourceUnitArg));
  RPC_S(PR5 alter_resource_unit, obrpc::OB_ALTER_RESOURCE_UNIT, (ObAlterResourceUnitArg));
  RPC_S(PR5 drop_resource_unit, obrpc::OB_DROP_RESOURCE_UNIT, (ObDropResourceUnitArg));
  RPC_S(PRD create_resource_pool, obrpc::OB_CREATE_RESOURCE_POOL, (ObCreateResourcePoolArg));
  RPC_S(PRD alter_resource_pool, obrpc::OB_ALTER_RESOURCE_POOL, (ObAlterResourcePoolArg));
  RPC_S(PRD drop_resource_pool, obrpc::OB_DROP_RESOURCE_POOL, (ObDropResourcePoolArg));
  RPC_S(PRD split_resource_pool, obrpc::OB_SPLIT_RESOURCE_POOL, (ObSplitResourcePoolArg));
  RPC_S(PRD merge_resource_pool, obrpc::OB_MERGE_RESOURCE_POOL, (ObMergeResourcePoolArg));

  RPC_S(PRD root_minor_freeze, obrpc::OB_ROOT_MINOR_FREEZE, (ObRootMinorFreezeArg));
  RPC_S(PRD root_major_freeze, obrpc::OB_ROOT_MAJOR_FREEZE, (ObRootMajorFreezeArg));
  RPC_S(PRD root_split_partition, obrpc::OB_ROOT_SPLIT_PARTITION, (ObRootSplitPartitionArg));
  RPC_S(PRD update_index_status, obrpc::OB_UPDATE_INDEX_TABLE_STATUS, (ObUpdateIndexStatusArg));
  RPC_S(PR5 get_root_server_status, obrpc::OB_GET_ROLE, ObGetRootserverRoleResult);
  RPC_S(PR5 get_master_root_server, obrpc::OB_GET_MASTER_RS, ObGetRootserverRoleResult);

  // define system admin rpc (alter system ...)
  RPC_S(PR5 admin_switch_replica_role, obrpc::OB_ADMIN_SWITCH_REPLICA_ROLE, (ObAdminSwitchReplicaRoleArg));
  RPC_S(PR5 admin_switch_rs_role, obrpc::OB_ADMIN_SWITCH_RS_ROLE, (ObAdminSwitchRSRoleArg));
  RPC_S(PR5 admin_drop_replica, obrpc::OB_ADMIN_DROP_REPLICA, (ObAdminDropReplicaArg));
  RPC_S(PR5 admin_change_replica, obrpc::OB_ADMIN_CHANGE_REPLICA, (ObAdminChangeReplicaArg));
  RPC_S(PR5 admin_migrate_replica, obrpc::OB_ADMIN_MIGRATE_REPLICA, (ObAdminMigrateReplicaArg));
  RPC_S(PR5 admin_report_replica, obrpc::OB_ADMIN_REPORT_REPLICA, (ObAdminReportReplicaArg));
  RPC_S(PR5 admin_recycle_replica, obrpc::OB_ADMIN_RECYCLE_REPLICA, (ObAdminRecycleReplicaArg));
  RPC_S(PR5 admin_merge, obrpc::OB_ADMIN_MERGE, (ObAdminMergeArg));
  RPC_S(PR5 admin_clear_roottable, obrpc::OB_ADMIN_CLEAR_ROOTTABLE, (ObAdminClearRoottableArg));
  RPC_S(PR5 admin_refresh_schema, obrpc::OB_ADMIN_REFRESH_SCHEMA, (ObAdminRefreshSchemaArg));
  RPC_S(PR5 admin_refresh_memory_stat, obrpc::OB_ADMIN_REFRESH_MEMORY_STAT, (ObAdminRefreshMemStatArg));
  RPC_S(PR5 admin_set_config, obrpc::OB_ADMIN_SET_CONFIG, (ObAdminSetConfigArg));
  RPC_S(PR5 admin_clear_location_cache, obrpc::OB_ADMIN_CLEAR_LOCATION_CACHE, (ObAdminClearLocationCacheArg));
  RPC_S(PR5 admin_reload_gts, obrpc::OB_ADMIN_RELOAD_GTS);
  RPC_S(PR5 admin_reload_unit, obrpc::OB_ADMIN_RELOAD_UNIT);
  RPC_S(PR5 admin_reload_server, obrpc::OB_ADMIN_RELOAD_SERVER);
  RPC_S(PR5 admin_reload_zone, obrpc::OB_ADMIN_RELOAD_ZONE);
  RPC_S(PR5 admin_clear_merge_error, obrpc::OB_ADMIN_CLEAR_MERGE_ERROR);
  RPC_S(PR5 admin_migrate_unit, obrpc::OB_ADMIN_MIGRATE_UNIT, (ObAdminMigrateUnitArg));
  RPC_S(PRD admin_upgrade_virtual_schema, obrpc::OB_ADMIN_UPGRADE_VIRTUAL_SCHEMA);
  RPC_S(PRD run_job, obrpc::OB_RUN_JOB, (ObRunJobArg));
  RPC_S(PRD run_upgrade_job, obrpc::OB_RUN_UPGRADE_JOB, (ObUpgradeJobArg));
  RPC_S(PR5 admin_flush_cache, obrpc::OB_ADMIN_FLUSH_CACHE, (ObAdminFlushCacheArg));
  RPC_S(PR5 admin_upgrade_cmd, obrpc::OB_ADMIN_UPGRADE_CMD, (Bool));
  RPC_S(PR5 admin_rolling_upgrade_cmd, obrpc::OB_ADMIN_ROLLING_UPGRADE_CMD, (ObAdminRollingUpgradeArg));
  RPC_S(PR5 restore_tenant, obrpc::OB_RESTORE_TENANT, (ObRestoreTenantArg));
  RPC_S(PRD restore_partitions, obrpc::OB_RESTORE_PARTITIONS, (ObRestorePartitionsArg));
  RPC_S(PR5 admin_clear_balance_task, obrpc::OB_ADMIN_FLUSH_BALANCE_INFO, (ObAdminClearBalanceTaskArg));
  RPC_S(PRD alter_cluster_attr, obrpc::OB_ALTER_CLUSTER_ATTR_DDL, (ObAlterClusterInfoArg));
  RPC_S(PRD get_tenant_schema_versions, obrpc::OB_GET_TENANT_SCHEMA_VERSIONS, (ObGetSchemaArg),
      obrpc::ObTenantSchemaVersions);
  RPC_S(PRD get_cluster_stats, obrpc::OB_GET_CLUSTER_STATS, obrpc::ObClusterTenantStats);
  RPC_S(PRD update_freeze_schema_version, obrpc::OB_UPDATE_FREEZE_SCHEMA_VERSIONS, (Int64),
      obrpc::ObTenantSchemaVersions);
  RPC_S(PRD alter_cluster, obrpc::OB_ALTER_CLUSTER, (ObAdminClusterArg));
  RPC_S(PRD cluster_action_verify, obrpc::OB_CLUSTER_ACTION_VERIFY, (ObClusterActionVerifyArg));
  RPC_S(PRD update_standby_cluster_info, obrpc::OB_UPDATE_STANDBY_CLUSTER_INFO, (share::ObClusterAddr));
  RPC_S(PR5 alter_cluster_info, obrpc::OB_ALTER_CLUSTER_INFO, (ObAlterClusterInfoArg));

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
  RPC_S(PR5 admin_set_tracepoint, obrpc::OB_RS_SET_TP, (ObAdminSetTPArg));
  RPC_S(PR5 refresh_time_zone_info, obrpc::OB_REFRESH_TIME_ZONE_INFO, (ObRefreshTimezoneArg));
  RPC_S(PR5 request_time_zone_info, obrpc::OB_REQUEST_TIME_ZONE_INFO, (common::ObRequestTZInfoArg),
      common::ObRequestTZInfoResult);
  RPC_S(PR5 check_unique_index_response, obrpc::OB_CHECK_UNIQUE_INDEX_RESPONSE, (obrpc::ObCheckUniqueIndexResponseArg));
  RPC_S(PR5 calc_column_checksum_response, obrpc::OB_CALC_COLUMN_CHECKSUM_RESPONSE,
      (obrpc::ObCalcColumnChecksumResponseArg));
  RPC_S(PR5 check_gts_replica_enough_when_stop_server, obrpc::OB_CHECK_GTS_REPLICA_FOR_STOP_SERVER,
      (obrpc::ObCheckGtsReplicaStopServer), Bool);
  RPC_S(PR5 check_gts_replica_enough_when_stop_zone, obrpc::OB_CHECK_GTS_REPLICA_FOR_STOP_ZONE,
      (obrpc::ObCheckGtsReplicaStopZone), Bool);

  RPC_S(PR5 add_replica_res, OB_ADD_REPLICA_RES, (ObAddReplicaRes));
  RPC_S(PR5 migrate_replica_res, OB_MIGRATE_REPLICA_RES, (ObMigrateReplicaRes));
  RPC_S(PR5 rebuild_replica_res, OB_REBUILD_REPLICA_RES, (ObRebuildReplicaRes));
  RPC_S(PR5 restore_replica_res, OB_RESTORE_REPLICA_RES, (ObRestoreReplicaRes));
  RPC_S(PR5 physical_restore_replica_res, OB_PHYSICAL_RESTORE_REPLICA_RES, (ObPhyRestoreReplicaRes));
  RPC_S(PR5 change_replica_res, OB_CHANGE_REPLICA_RES, (ObChangeReplicaRes));
  RPC_S(PR5 validate_backup_res, OB_VALIDATE_BACKUP_RES, (ObValidateRes));
  RPC_S(PR5 add_replica_batch_res, OB_ADD_REPLICA_BATCH_RES, (ObAddReplicaBatchRes));
  RPC_S(PR5 migrate_replica_batch_res, OB_MIGRATE_REPLICA_BATCH_RES, (ObMigrateReplicaBatchRes));
  RPC_S(PR5 rebuild_replica_batch_res, OB_REBUILD_REPLICA_BATCH_RES, (ObRebuildReplicaBatchRes));
  RPC_S(PR5 copy_sstable_batch_res, OB_COPY_SSTABLE_BATCH_RES, (ObCopySSTableBatchRes));
  RPC_S(PR5 change_replica_batch_res, OB_CHANGE_REPLICA_BATCH_RES, (ObChangeReplicaBatchRes));
  RPC_S(PR5 observer_copy_local_index_sstable, OB_TMP_OBSERVER_COPY_LOCAL_INDEX_SSTABLE,
      (ObServerCopyLocalIndexSSTableArg));
  RPC_S(PR5 backup_replica_batch_res, OB_BACKUP_REPLICA_BATCH_RES, (ObBackupBatchRes));
  RPC_S(PR5 validate_backup_batch_res, OB_VALIDATE_BACKUP_BATCH_RES, (ObValidateBatchRes));
  RPC_S(PR5 standby_cutdata_batch_task_res, OB_STANDBY_CUTDATA_BATCH_TASK_RES, (ObStandbyCutDataBatchTaskRes));
  RPC_S(PR5 backup_archive_log_batch_res, OB_BACKUP_ARCHIVE_LOG_BATCH_RES, (ObBackupArchiveLogBatchRes));
  RPC_S(PR5 backup_backupset_batch_res, OB_BACKUP_BACKUPSET_BATCH_RES, (ObBackupBackupsetBatchRes));

  RPC_S(
      PRD commit_alter_tenant_locality, OB_COMMIT_ALTER_TENANT_LOCALITY, (rootserver::ObCommitAlterTenantLocalityArg));
  RPC_S(PRD commit_alter_table_locality, OB_COMMIT_ALTER_TABLE_LOCALITY, (rootserver::ObCommitAlterTableLocalityArg));
  RPC_S(PRD commit_alter_tablegroup_locality, OB_COMMIT_ALTER_TABLEGROUP_LOCALITY,
      (rootserver::ObCommitAlterTablegroupLocalityArg));

  RPC_S(PR5 update_stat_cache, OB_RS_UPDATE_STAT_CACHE, (ObUpdateStatCacheArg));

  RPC_S(PRD force_create_sys_table, OB_FORCE_CREATE_SYS_TABLE, (obrpc::ObForceCreateSysTableArg));
  RPC_S(PRD force_set_locality, OB_FORCE_SET_LOCALITY, (obrpc::ObForceSetLocalityArg));
  RPC_S(PR5 get_cluster_info, obrpc::OB_GET_CLUSTER_INFO, (obrpc::ObGetClusterInfoArg), share::ObClusterInfo);
  RPC_S(PRD log_nop_operation, obrpc::OB_LOG_DDL_NOP_OPERATOR, (obrpc::ObDDLNopOpreatorArg));
  RPC_S(PRD finish_replay_schema, obrpc::OB_FINISH_REPLAY_SCHEMA, (obrpc::ObFinishReplayArg));
  RPC_S(PRD update_table_schema_version, obrpc::OB_UPDATE_TABLE_SCHEMA_VERSION, (obrpc::ObUpdateTableSchemaVersionArg));
  RPC_S(PRD finish_schema_split, OB_FINISH_SCHEMA_SPLIT, (obrpc::ObFinishSchemaSplitArg));
  RPC_S(PRD broadcast_schema, OB_BROADCAST_SCHEMA, (obrpc::ObBroadcastSchemaArg));
  RPC_S(PRD gen_next_schema_version, OB_GEN_NEXT_SCHEMA_VERSION, (obrpc::ObDDLArg));
  RPC_S(PR5 get_switchover_status, OB_GET_SWITCHOVER_STATUS, obrpc::ObGetSwitchoverStatusRes);
  RPC_S(PR5 check_merge_finish, OB_CHECK_MERGE_FINISH, (obrpc::ObCheckMergeFinishArg));
  RPC_S(PR5 check_cluster_valid_to_add, OB_CHECK_CLUSTER_VALID_TO_ADD, (obrpc::ObCheckAddStandbyArg));
  RPC_S(PR5 get_recycle_schema_versions, OB_GET_RECYCLE_SCHEMA_VERSIONS, (obrpc::ObGetRecycleSchemaVersionsArg),
      obrpc::ObGetRecycleSchemaVersionsResult);

  RPC_S(PR5 check_standby_can_access, OB_CHECK_STANDBY_CAN_ACCESS, (obrpc::ObCheckStandbyCanAccessArg), Bool);

  // backup and restore
  RPC_S(PRD physical_restore_tenant, OB_PHYSICAL_RESTORE_TENANT, (obrpc::ObPhysicalRestoreTenantArg));
  RPC_S(PRD rebuild_index_in_restore, OB_REBUILD_INDEX_IN_RESTORE, (obrpc::ObRebuildIndexInRestoreArg));
  RPC_S(PRD force_drop_schema, OB_FORCE_DROP_SCHEMA, (obrpc::ObForceDropSchemaArg));
  RPC_S(PR5 archive_log, obrpc::OB_ARCHIVE_LOG, (ObArchiveLogArg));
  RPC_S(PRD backup_database, obrpc::OB_BACKUP_DATABASE, (ObBackupDatabaseArg));  // use ddl thread
  RPC_S(PR5 backup_manage, obrpc::OB_BACKUP_MANAGE, (ObBackupManageArg));
  RPC_S(PRD backup_backupset, obrpc::OB_BACKUP_BACKUPSET, (ObBackupBackupsetArg));
  RPC_S(PRD backup_archive_log, obrpc::OB_BACKUP_ARCHIVE_LOG, (ObBackupArchiveLogArg));
  RPC_S(PRD backup_backuppiece, obrpc::OB_BACKUP_BACKUPPIECE, (ObBackupBackupPieceArg));
  RPC_S(PRD standby_upgrade_virtual_schema, obrpc::OB_UPGRADE_STANDBY_SCHEMA,
      (ObDDLNopOpreatorArg));  // use ddl thread
  RPC_S(PRD modify_schema_in_restore, OB_MODIFY_SCHEMA_IN_RESTORE, (obrpc::ObRestoreModifySchemaArg));

  RPC_S(PR5 check_backup_scheduler_working, obrpc::OB_CHECK_BACKUP_SCHEDULER_WORKING, Bool);
  RPC_S(PR5 send_physical_restore_result, obrpc::OB_PHYSICAL_RESTORE_RES, (obrpc::ObPhysicalRestoreResult));

  // auto part ddl
  RPC_S(PRD execute_range_part_split, obrpc::OB_EXECUTE_RANGE_PART_SPLIT, (obrpc::ObExecuteRangePartSplitArg));
  RPC_S(PRD create_restore_point, obrpc::OB_CREATE_RESTORE_POINT, (ObCreateRestorePointArg));
  RPC_S(PRD drop_restore_point, obrpc::OB_DROP_RESTORE_POINT, (ObDropRestorePointArg));

  // table api
  RPC_S(PRD table_ttl, obrpc::OB_TABLE_TTL, (ObTableTTLArg));
  RPC_S(PRD ttl_response, obrpc::OB_TTL_RESPONSE, (ObTTLResponseArg));
public:
  void set_rs_mgr(share::ObRsMgr& rs_mgr)
  {
    rs_mgr_ = &rs_mgr;
  }

  // send to rs, only need set rs_mgr, no need set dst_server
  ObCommonRpcProxy to_rs(share::ObRsMgr& rs_mgr) const
  {
    ObCommonRpcProxy proxy = this->to();
    proxy.set_rs_mgr(rs_mgr);
    return proxy;
  }

  // send to addr, if failed, it will not retry to send to rs according to rs_mgr
  // the interface emphasize no retry
  ObCommonRpcProxy to_addr(const ::oceanbase::common::ObAddr& dst) const
  {
    ObCommonRpcProxy proxy = this->to(dst);
    proxy.reset_rs_mgr();
    return proxy;
  }

private:
  void reset_rs_mgr()
  {
    rs_mgr_ = NULL;
  }

protected:
#define CALL_WITH_RETRY(call_stmt)                                                                                    \
  common::ObAddr rs;                                                                                                  \
  do {                                                                                                                \
    int ret = common::OB_SUCCESS;                                                                                     \
    rs.reset();                                                                                                       \
    if (NULL != rs_mgr_) {                                                                                            \
      if (GCONF.cluster_id != dst_cluster_id_ && OB_INVALID_CLUSTER_ID != dst_cluster_id_) {                          \
        if (OB_FAIL(rs_mgr_->get_remote_cluster_master_rs(dst_cluster_id_, rs))) {                                    \
          SHARE_LOG(WARN, "failed to get master rs", K(ret), K(dst_cluster_id_));                                     \
        }                                                                                                             \
      } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs))) {                                                      \
        SHARE_LOG(WARN, "get master root service failed", K(ret));                                                    \
      }                                                                                                               \
      if (OB_FAIL(ret)) {                                                                                             \
      } else {                                                                                                        \
        set_server(rs);                                                                                               \
      }                                                                                                               \
    }                                                                                                                 \
    if (OB_SUCC(ret)) {                                                                                               \
      ret = call_stmt;                                                                                                \
      const int64_t RETRY_TIMES = 3;                                                                                  \
      for (int64_t i = 0; (common::OB_RS_NOT_MASTER == ret || common::OB_SERVER_IS_INIT == ret) && NULL != rs_mgr_ && \
                          i < RETRY_TIMES;                                                                            \
           ++i) {                                                                                                     \
        if (GCONF.cluster_id != dst_cluster_id_ && OB_INVALID_CLUSTER_ID != dst_cluster_id_) {                        \
          if (OB_FAIL(rs_mgr_->get_remote_cluster_master_rs(dst_cluster_id_, rs))) {                                  \
            SHARE_LOG(WARN, "failed to get master rs", K(ret), K(dst_cluster_id_));                                   \
          }                                                                                                           \
        } else if (OB_FAIL(rs_mgr_->renew_master_rootserver())) {                                                     \
          SHARE_LOG(WARN, "renew_master_rootserver failed", K(ret), "retry", i);                                      \
        } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs))) {                                                    \
          SHARE_LOG(WARN, "get master root service failed", K(ret), "retry", i);                                      \
        }                                                                                                             \
        if (OB_FAIL(ret)) {                                                                                           \
        } else {                                                                                                      \
          set_server(rs);                                                                                             \
          ret = call_stmt;                                                                                            \
        }                                                                                                             \
      }                                                                                                               \
    }                                                                                                                 \
    return ret;                                                                                                       \
  } while (false)

  template <typename Input, typename Out>
  int rpc_call(ObRpcPacketCode pcode, const Input& args, Out& result, Handle* handle, const ObRpcOpts& opts)
  {
    CALL_WITH_RETRY(ObRpcProxy::rpc_call(pcode, args, result, handle, opts));
  }

  template <typename Input>
  int rpc_call(ObRpcPacketCode pcode, const Input& args, Handle* handle, const ObRpcOpts& opts)
  {
    CALL_WITH_RETRY(ObRpcProxy::rpc_call(pcode, args, handle, opts));
  }

  template <typename Output>
  int rpc_call(ObRpcPacketCode pcode, Output& result, Handle* handle, const ObRpcOpts& opts)
  {
    CALL_WITH_RETRY(ObRpcProxy::rpc_call(pcode, result, handle, opts));
  }

  int rpc_call(ObRpcPacketCode pcode, Handle* handle, const ObRpcOpts& opts)
  {
    CALL_WITH_RETRY(ObRpcProxy::rpc_call(pcode, handle, opts));
  }

  template <typename Input>
  int rpc_post(ObRpcPacketCode pcode, const Input& args, rpc::frame::ObReqTransport::AsyncCB* cb, const ObRpcOpts& opts)
  {
    CALL_WITH_RETRY(rpc_post(pcode, args, cb, opts));
  }

  int rpc_post(ObRpcPacketCode pcode, rpc::frame::ObReqTransport::AsyncCB* cb, const ObRpcOpts& opts)
  {
    CALL_WITH_RETRY(rpc_post(pcode, cb, opts));
  }
#undef CALL_WIRTH_RETRY

private:
  share::ObRsMgr* rs_mgr_;
};  // end of class ObCommonRpcProxy

}  // namespace obrpc
}  // end of namespace oceanbase

#endif /* _OCEABASE_SHARE_OB_COMMON_RPC_PROXY_H_ */
