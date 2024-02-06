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

#include "share/interrupt/ob_interrupt_rpc_proxy.h"
#include "observer/ob_srv_xlator.h"

#include "share/ob_tenant_mgr.h"
#include "share/schema/ob_schema_service_rpc_proxy.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "share/rpc/ob_batch_processor.h"
#include "share/rpc/ob_blacklist_req_processor.h"
#include "share/rpc/ob_blacklist_resp_processor.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "sql/engine/cmd/ob_kill_executor.h"
#include "sql/engine/cmd/ob_load_data_rpc.h"
#include "sql/engine/px/ob_px_rpc_processor.h"
#include "sql/dtl/ob_dtl_rpc_processor.h"
#include "storage/tx/ob_trans_rpc.h"
#include "storage/tx/ob_gts_rpc.h"
#include "storage/tx/ob_dup_table_rpc.h"
#include "storage/tx/ob_ts_response_handler.h"
#include "storage/tx/wrs/ob_weak_read_service_rpc_define.h"  // weak_read_service
#include "rootserver/ob_rs_rpc_processor.h"
#include "observer/ob_rpc_processor_simple.h"
#include "observer/ob_srv_task.h"

#include "observer/table/ob_table_rpc_processor.h"
#include "observer/table/ob_table_execute_processor.h"
#include "observer/table/ob_table_batch_execute_processor.h"
#include "observer/table/ob_table_query_processor.h"
#include "observer/table/ob_table_query_and_mutate_processor.h"

/* given the efficiency of compiling, we split all rpc records into multiple files.
 * when more than 200 records appears in a single file, another split is recommended
 */

using namespace oceanbase;
using namespace oceanbase::observer;
using namespace oceanbase::lib;
using namespace oceanbase::rpc;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::obrpc;
using namespace oceanbase::obmysql;

void oceanbase::observer::init_srv_xlator_for_rootserver(ObSrvRpcXlator *xlator) {
    // rootservice provided
    RPC_PROCESSOR(rootserver::ObRpcRenewLeaseP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcReportSysLSP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRemoveSysLSP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcFetchLocationP, *gctx_.root_service_);
    // RPC_PROCESSOR(rootserver::ObRpcMergeFinishP, *gctx_.root_service_);

    RPC_PROCESSOR(rootserver::ObBroadcastDSActionP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcFetchAliveServerP, *gctx_.root_service_);
    // RPC_PROCESSOR(rootserver::ObRpcFetchActiveServerStatusP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRefreshTimeZoneInfoP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRequestTimeZoneInfoP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObCheckDanglingReplicaFinishP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDDLBuildSingleReplicaResponseP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCancelDDLTaskP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCalcColumnChecksumResponseP, *gctx_.root_service_);
    //RPC_PROCESSOR(rootserver::ObRpcFetchSchemaP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCommitAlterTenantLocalityP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateResourceUnitP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterResourceUnitP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropResourceUnitP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateResourcePoolP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterResourcePoolP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropResoucePoolP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcSplitResourcePoolP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcMergeResourcePoolP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterResourceTenantP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateTenantP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateTenantEndP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcModifyTenantP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcLockTenantP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropTenantP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAddSysVarP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcModifySysVarP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateDatabaseP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterDatabaseP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropDatabaseP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateTablegroupP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropTablegroupP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterTablegroupP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRecoverRestoreTableDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcParallelCreateTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcStartRedefTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCopyTableDependentsP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcFinishRedefTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAbortRedefTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcUpdateDDLTaskActiveTimeP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateHiddenTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRenameTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcTruncateTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcTruncateTableV2P, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateIndexP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropIndexP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateTableLikeP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcExecuteBootstrapP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRefreshConfigP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRootMinorFreezeP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObUpdateIndexTableStatusP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateOutlineP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterOutlineP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropOutlineP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateRoutineP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropRoutineP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterRoutineP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateDbLinkP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropDbLinkP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreatePackageP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterPackageP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropPackageP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateTriggerP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterTriggerP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropTriggerP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateSynonymP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropSynonymP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateUserDefinedFunctionP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropUserDefinedFunctionP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDoSequenceDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCreateUDTP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropUDTP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcOptimizeTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcSchemaReviseP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDoKeystoreDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcHandleLabelSePolicyDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcHandleLabelSeComponentDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcHandleLabelSeLabelDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcHandleLabelSeUserLevelDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDoTablespaceDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDoProfileDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcSecurityAuditP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcExecuteDDLTaskP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcMaintainObjDependencyInfoP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDoContextDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRecompileAllViewsBatchP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcTryAddDepInfosForSynonymBatchP, *gctx_.root_service_);
    //ob_admin
    RPC_PROCESSOR(rootserver::ObForceCreateSysTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObForceSetLocalityP, *gctx_.root_service_);

    //recyclebin related
    RPC_PROCESSOR(rootserver::ObRpcFlashBackTableFromRecyclebinP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcFlashBackIndexP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcPurgeTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcPurgeIndexP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcFlashBackDatabaseP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcPurgeDatabaseP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcFlashBackTenantP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcPurgeTenantP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcPurgeExpireRecycleObjectsP, *gctx_.root_service_);
    //privilege
    RPC_PROCESSOR(rootserver::ObRpcCreateUserP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropUserP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRenameUserP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcSetPasswdP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcGrantP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRevokeUserP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcLockUserP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRevokeDBP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRevokeTableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRevokeSysPrivP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterRoleP, *gctx_.root_service_);

    //profile
    RPC_PROCESSOR(rootserver::ObRpcAlterUserProfileP, *gctx_.root_service_);
    // server related
    RPC_PROCESSOR(rootserver::ObRpcAddServerP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDeleteServerP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCancelDeleteServerP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcStartServerP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcStopServerP, *gctx_.root_service_);
    // zone related
    RPC_PROCESSOR(rootserver::ObRpcAddZoneP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDeleteZoneP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcStartZoneP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcStopZoneP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAlterZoneP, *gctx_.root_service_);

    // system admin commnad
    RPC_PROCESSOR(rootserver::ObRpcAdminSwitchReplicaRoleP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminSwitchRSRoleP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminDropReplicaP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminChangeReplicaP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminMigrateReplicaP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminReportReplicaP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminRecycleReplicaP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminMergeP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminRecoveryP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminClearRoottableP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminRefreshSchemaP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminRefreshMemStatP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminWashMemFragmentationP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminSetConfigP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminClearLocationCacheP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminReloadUnitP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminReloadServerP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminReloadZoneP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminClearMergeErrorP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminMigrateUnitP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminUpgradeVirtualSchemaP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRunJobP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminRefreshIOCalibrationP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRunUpgradeJobP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcUpgradeTableSchemaP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminFlushCacheP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminUpgradeCmdP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminRollingUpgradeCmdP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminSetTPP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminFlushBalanceInfoP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObGetTenantSchemaVersionsP, *gctx_.root_service_);

    //update optimizer statistic
    RPC_PROCESSOR(rootserver::ObRpcUpdateStatCacheP, *gctx_.root_service_);


    RPC_PROCESSOR(rootserver::ObBroadcastSchemaP, *gctx_.root_service_);


    //for upgrade
    RPC_PROCESSOR(ObGetTenantSchemaVersionP, gctx_);
    RPC_PROCESSOR(rootserver::ObRpcFlashBackTableToScnP, *gctx_.root_service_);

    RPC_PROCESSOR(rootserver::ObRpcCreateRestorePointP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropRestorePointP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObGetRecycleSchemaVersionsP, *gctx_.root_service_);
    // backup and restore
    RPC_PROCESSOR(rootserver::ObRpcPhysicalRestoreTenantP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRebuildIndexInRestoreP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObArchiveLogP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObBackupDatabaseP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObBackupManageP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObBackupCleanP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObDeletePolicyP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRecoverTableP, *gctx_.root_service_);

    // auto part ddl

    RPC_PROCESSOR(rootserver::ObRpcFlushOptStatMonitoringInfoP, *gctx_.root_service_);

    RPC_PROCESSOR(rootserver::ObRpcCreateDirectoryP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDropDirectoryP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcDisasterRecoveryTaskReplyP, *gctx_.root_service_);

#ifdef OB_BUILD_SPM
    // sql plan baseline
    RPC_PROCESSOR(rootserver::ObRpcAcceptPlanBaselineP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcCancelEvolveTaskP, *gctx_.root_service_)
    RPC_PROCESSOR(rootserver::ObRpcAdminLoadBaselineP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminLoadBaselineV2P, *gctx_.root_service_);
    // arb service related
    RPC_PROCESSOR(rootserver::ObRpcAdminAddArbitrationServiceP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminRemoveArbitrationServiceP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcAdminReplaceArbitrationServiceP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcRemoveClusterInfoFromArbServerP, *gctx_.root_service_);
#endif

    RPC_PROCESSOR(rootserver::ObRpcAdminSyncRewriteRulesP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcHandleRlsPolicyDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcHandleRlsGroupDDLP, *gctx_.root_service_);
    RPC_PROCESSOR(rootserver::ObRpcHandleRlsContextDDLP, *gctx_.root_service_);
#ifdef OB_BUILD_TDE_SECURITY
    RPC_PROCESSOR(rootserver::ObGetRootKeyP, *gctx_.root_service_);
#endif

}
