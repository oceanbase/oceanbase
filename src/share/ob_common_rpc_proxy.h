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
#include "share/partition_table/ob_partition_location.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_rs_mgr.h"
#include "share/ob_time_zone_info_manager.h"
#include "common/storage/ob_freeze_define.h" // for ObFrozenStatus
#include "rootserver/ob_alter_locality_finish_checker.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "share/ls/ob_ls_info.h"

namespace oceanbase
{
namespace rootserver
{
class ObCommitAlterTenantLocalityArg;
}
namespace obrpc
{
class ObCommonRpcProxy
    : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObCommonRpcProxy);

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
  RPC_S(PRD create_hidden_table, obrpc::OB_CREATE_HIDDEN_TABLE, (obrpc::ObCreateHiddenTableArg), ObCreateHiddenTableRes);
  RPC_S(PRD alter_database, obrpc::OB_ALTER_DATABASE, (ObAlterDatabaseArg));
  RPC_S(PRD drop_database, obrpc::OB_DROP_DATABASE, (ObDropDatabaseArg), ObDropDatabaseRes);
  RPC_S(PRD drop_tablegroup, obrpc::OB_DROP_TABLEGROUP, (ObDropTablegroupArg));
  RPC_S(PRD alter_tablegroup, obrpc::OB_ALTER_TABLEGROUP, (ObAlterTablegroupArg));
  RPC_S(PRD drop_table, obrpc::OB_DROP_TABLE, (ObDropTableArg), ObDDLRes);
  RPC_S(PRD rename_table, obrpc::OB_RENAME_TABLE, (ObRenameTableArg));
  RPC_S(PRD truncate_table, obrpc::OB_TRUNCATE_TABLE, (ObTruncateTableArg), ObDDLRes);
  RPC_S(PRD truncate_table_v2, obrpc::OB_TRUNCATE_TABLE_V2, (ObTruncateTableArg), ObDDLRes);
  RPC_S(PRD generate_aux_index_schema, obrpc::OB_GENERATE_AUX_INDEX_SCHEMA, (obrpc::ObGenerateAuxIndexSchemaArg), obrpc::ObGenerateAuxIndexSchemaRes);
  RPC_S(PRD create_index, obrpc::OB_CREATE_INDEX, (ObCreateIndexArg), ObAlterTableRes);
  RPC_S(PRD drop_index, obrpc::OB_DROP_INDEX, (ObDropIndexArg), ObDropIndexRes);
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
  RPC_S(PR5 admin_set_tracepoint, obrpc::OB_RS_SET_TP, (ObAdminSetTPArg));
  RPC_S(PR5 refresh_time_zone_info, obrpc::OB_REFRESH_TIME_ZONE_INFO, (ObRefreshTimezoneArg));
  RPC_S(PR5 request_time_zone_info, obrpc::OB_REQUEST_TIME_ZONE_INFO, (common::ObRequestTZInfoArg), common::ObRequestTZInfoResult);
  RPC_S(PR5 calc_column_checksum_response, obrpc::OB_CALC_COLUMN_CHECKSUM_RESPONSE, (obrpc::ObCalcColumnChecksumResponseArg));
  RPC_S(PR5 build_ddl_single_replica_response, obrpc::OB_DDL_BUILD_SINGLE_REPLICA_RESPONSE, (obrpc::ObDDLBuildSingleReplicaResponseArg));
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
public:
  void set_rs_mgr(share::ObRsMgr &rs_mgr)
  {
    rs_mgr_ = &rs_mgr;
  }

  //send to rs, only need set rs_mgr, no need set dst_server
  ObCommonRpcProxy to_rs(share::ObRsMgr &rs_mgr) const
  {
    ObCommonRpcProxy proxy = this->to();
    proxy.set_rs_mgr(rs_mgr);
    return proxy;
  }

  //send to addr, if failed, it will not retry to send to rs according to rs_mgr
  //the interface emphasize no retry
  ObCommonRpcProxy to_addr(const ::oceanbase::common::ObAddr &dst) const
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

#define CALL_WITH_RETRY(call_stmt)                                      \
  common::ObAddr rs;                                                    \
  do {                                                                  \
    int ret = common::OB_SUCCESS;                                       \
    const bool use_remote_rs = GCONF.cluster_id != dst_cluster_id_ && OB_INVALID_CLUSTER_ID != dst_cluster_id_;\
    rs.reset();                                                         \
    if (NULL != rs_mgr_) {                                              \
      if (use_remote_rs) {                           \
        if (OB_FAIL(rs_mgr_->get_master_root_server(dst_cluster_id_, rs))) {  \
          if (OB_ENTRY_NOT_EXIST != ret) {                                                      \
            SHARE_LOG(WARN, "failed to get remote master rs", KR(ret), K_(dst_cluster_id));     \
          } else if (OB_FAIL(rs_mgr_->renew_master_rootserver(dst_cluster_id_))) {       \
            SHARE_LOG(WARN, "failed to renew remote master rs", KR(ret), K_(dst_cluster_id));   \
          } else if (OB_FAIL(rs_mgr_->get_master_root_server(dst_cluster_id_, rs))) {    \
            SHARE_LOG(WARN, "failed to get remote master rs", KR(ret), K_(dst_cluster_id));     \
          }                                                                                    \
        }                                                               \
      } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs))) {        \
        SHARE_LOG(WARN, "failed to get master rs", KR(ret));            \
      }                                                                 \
      if (OB_FAIL(ret)) {                                               \
      } else {                                                          \
        set_server(rs);                                                 \
      }                                                                 \
    }                                                                   \
    if (OB_SUCC(ret)) {                                                 \
      ret = call_stmt;                                                  \
      const int64_t RETRY_TIMES = 3;                                    \
      for (int64_t i = 0;                                               \
           (common::OB_RS_NOT_MASTER == ret                             \
            || common::OB_SERVER_IS_INIT == ret                         \
            || (use_remote_rs && OB_FAIL(ret)))                         \
           && NULL != rs_mgr_ && i < RETRY_TIMES;                       \
           ++i) {                                                       \
        if (use_remote_rs) {                         \
          if (OB_FAIL(rs_mgr_->renew_master_rootserver(dst_cluster_id_))) {  \
            SHARE_LOG(WARN, "failed to get master rs", K(ret), K(dst_cluster_id_));          \
          } else if (OB_FAIL(rs_mgr_->get_master_root_server(dst_cluster_id_, rs))) {  \
            SHARE_LOG(WARN, "failed to get remote master rs", KR(ret), K_(dst_cluster_id));     \
          }                                                            \
        } else if (OB_FAIL(rs_mgr_->renew_master_rootserver())) {              \
          SHARE_LOG(WARN, "renew_master_rootserver failed", K(ret), "retry", i); \
        } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs))) {      \
          SHARE_LOG(WARN, "get master root service failed", K(ret), "retry", i); \
        }                                                               \
        if (OB_FAIL(ret)) {                                             \
        } else {                                                        \
          set_server(rs);                                               \
          ret = call_stmt;                                              \
        }                                                               \
      }                                                                 \
    }                                                                   \
    return ret;                                                         \
  } while (false)

  template <typename Input, typename Out>
  int rpc_call(ObRpcPacketCode pcode, const Input &args,
               Out &result, Handle *handle, const ObRpcOpts &opts)
  {
    CALL_WITH_RETRY(ObRpcProxy::rpc_call(pcode, args, result, handle, opts));
  }

  template <typename Input>
  int rpc_post(ObRpcPacketCode pcode, const Input &args,
               rpc::frame::ObReqTransport::AsyncCB *cb, const ObRpcOpts &opts)
  {
    CALL_WITH_RETRY(rpc_post(pcode, args, cb, opts));
  }

  int rpc_post(
      ObRpcPacketCode pcode,
      rpc::frame::ObReqTransport::AsyncCB *cb,
      const ObRpcOpts &opts)
  {
    CALL_WITH_RETRY(rpc_post(pcode, cb, opts));
  }
#undef CALL_WIRTH_RETRY

private:
  share::ObRsMgr *rs_mgr_;
}; // end of class ObCommonRpcProxy

} // end of namespace share
} // end of namespace oceanbase

#endif /* _OCEABASE_SHARE_OB_COMMON_RPC_PROXY_H_ */
