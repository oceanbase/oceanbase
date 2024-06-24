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

#ifndef OCEANBASE_ROOTSERVER_OB_RS_RPC_PROCESSOR_H_
#define OCEANBASE_ROOTSERVER_OB_RS_RPC_PROCESSOR_H_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/ob_common_rpc_proxy.h"
#include "ob_root_service.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/schema/ob_ddl_sql_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace rootserver
{
inline bool is_parallel_ddl(const obrpc::ObRpcPacketCode pcode)
{
  return obrpc::OB_TRUNCATE_TABLE_V2 == pcode
         || obrpc::OB_PARALLEL_CREATE_TABLE == pcode;
}

// precondition: enable_ddl = false
inline bool is_allow_when_disable_ddl(const obrpc::ObRpcPacketCode pcode, const obrpc::ObDDLArg *ddl_arg)
{
  bool bret = false;
  if (OB_ISNULL(ddl_arg)) {
  } else if (obrpc::OB_COMMIT_ALTER_TENANT_LOCALITY == pcode
             || obrpc::OB_SCHEMA_REVISE == pcode // for upgrade
             || obrpc::OB_UPGRADE_TABLE_SCHEMA == pcode
             || ((obrpc::OB_MODIFY_TENANT == pcode
                  || obrpc::OB_MODIFY_SYSVAR == pcode
                  || obrpc::OB_DO_KEYSTORE_DDL == pcode
                  || obrpc::OB_GRANT == pcode
                  || obrpc::OB_DO_PROFILE_DDL == pcode)
                 && ddl_arg->is_allow_when_disable_ddl())) {
    bret = true;
  }
  return bret;
}

inline bool is_allow_when_create_tenant(const obrpc::ObRpcPacketCode pcode)
{
  bool bret = false;
  if (obrpc::OB_CREATE_TENANT == pcode
      || obrpc::OB_DROP_TENANT == pcode
      || obrpc::OB_MODIFY_TENANT == pcode
      || obrpc::OB_LOCK_TENANT == pcode
      || obrpc::OB_COMMIT_ALTER_TENANT_LOCALITY == pcode
      || obrpc::OB_CREATE_TENANT_END == pcode) {
    bret = true;
  }
  return bret;
}
inline bool is_allow_when_drop_tenant(const obrpc::ObRpcPacketCode pcode)
{
  bool bret = false;
  if (obrpc::OB_DROP_TENANT == pcode
      || obrpc::OB_MODIFY_TENANT == pcode
      || obrpc::OB_LOCK_TENANT == pcode
      || obrpc::OB_COMMIT_ALTER_TENANT_LOCALITY == pcode
      || obrpc::OB_DROP_TABLE == pcode
      || obrpc::OB_DROP_TENANT == pcode
      || obrpc::OB_DROP_DATABASE == pcode
      || obrpc::OB_DROP_TABLEGROUP == pcode
      || obrpc::OB_DROP_INDEX == pcode
      || obrpc::OB_DROP_VIEW == pcode
      || obrpc::OB_PURGE_TABLE == pcode
      || obrpc::OB_PURGE_DATABASE == pcode
      || obrpc::OB_PURGE_EXPIRE_RECYCLE_OBJECTS == pcode
      || obrpc::OB_PURGE_INDEX == pcode
      || obrpc::OB_DROP_USER == pcode
      || obrpc::OB_DROP_OUTLINE == pcode
      || obrpc::OB_DROP_SYNONYM == pcode
      || obrpc::OB_DROP_ROUTINE == pcode
      || obrpc::OB_DROP_PACKAGE == pcode
      || obrpc::OB_DROP_USER_DEFINED_FUNCTION == pcode
      || obrpc::OB_DROP_UDT == pcode
      || obrpc::OB_DROP_TRIGGER == pcode) {
    bret = true;
  }
  return bret;
}

class ObRootServerRPCProcessorBase
{
public:
  ObRootServerRPCProcessorBase(ObRootService &rs, const bool full_service, const bool major_freeze_done, const bool is_ddl_like, obrpc::ObDDLArg *arg)
      : root_service_(rs), full_service_(full_service), major_freeze_done_(major_freeze_done), is_ddl_like_(is_ddl_like), ddl_arg_(arg) {}
protected:
  int process_(const obrpc::ObRpcPacketCode pcode) __attribute__((noinline))
  {
    int ret = common::OB_SUCCESS;
    if (OB_LIKELY(THE_RS_TRACE != nullptr)) {
      THE_RS_TRACE->reset();
    }
    if (root_service_.in_service()) {
      if (full_service_ && !root_service_.is_full_service()) {
        ret = OB_SERVER_IS_INIT;
        RS_LOG(WARN, "RS is initializing, can not process this request",
            K(ret), K(full_service_), K(root_service_.is_full_service()), K(pcode));
      } else if (major_freeze_done_ && !root_service_.is_major_freeze_done()) {
        ret = OB_SERVER_IS_INIT;
        RS_LOG(WARN, "RS major freeze not finished, can not process ddl request",
            K(ret), K(pcode));
      } else if (is_ddl_like_
                 && (!GCONF.enable_ddl && !is_allow_when_disable_ddl(pcode, ddl_arg_))) {
        ret = OB_OP_NOT_ALLOW;
        RS_LOG(WARN, "ddl operation not allow, can not process this request", K(ret), K(pcode));
      } else {
        if (is_ddl_like_) {
          if (OB_ISNULL(ddl_arg_)) {
            ret = OB_MISS_ARGUMENT;
            RS_LOG(WARN, "Arg is empty, can not process this request", K(ret), K(pcode));
          } else if (OB_INVALID_TENANT_ID == ddl_arg_->exec_tenant_id_) {
            ret = OB_INVALID_ARGUMENT;
            RS_LOG(WARN, "exec tenant id is invalid", K(ret), "arg", *ddl_arg_);
          } else if (common::STANDBY_CLUSTER == ObClusterInfoGetter::get_cluster_role_v2()
                     && !ddl_arg_->is_allow_in_standby()) {
            ret = OB_OP_NOT_ALLOW;
            RS_LOG(WARN, "ddl operation not allow in standby", KR(ret), KPC(ddl_arg_));
          } else {
            auto *tsi_value = GET_TSI(share::schema::TSIDDLVar);
            // used for parallel ddl
            auto *tsi_generator = GET_TSI(share::schema::TSISchemaVersionGenerator);
            if (OB_ISNULL(tsi_value)) {
              ret = OB_ERR_UNEXPECTED;
              RS_LOG(WARN, "Failed to get TSIDDLVar", K(ret), K(pcode));
            } else if (OB_ISNULL(tsi_generator)) {
              ret = OB_ERR_UNEXPECTED;
              RS_LOG(WARN, "Failed to get TSISchemaVersionGenerator", KR(ret), K(pcode));
            } else {
              tsi_generator->reset();
              tsi_value->exec_tenant_id_ = ddl_arg_->exec_tenant_id_;
              tsi_value->ddl_id_str_ = NULL;
              const common::ObString &ddl_id_str = ddl_arg_->ddl_id_str_;
              if (!ddl_id_str.empty()) {
                bool is_exists = false;
                if (OB_FAIL(share::schema::ObSchemaServiceSQLImpl::check_ddl_id_exist(root_service_.get_sql_proxy(),
                    ddl_arg_->exec_tenant_id_, ddl_id_str, is_exists))) {
                  RS_LOG(WARN, "Failed to check_ddl_id_status", K(ret), K(ddl_id_str));
                } else if (is_exists) {
                  ret = OB_SYNC_DDL_DUPLICATE;
                  LOG_USER_ERROR(OB_SYNC_DDL_DUPLICATE, ddl_id_str.length(), ddl_id_str.ptr());
                  RS_LOG(WARN, "Duplicated ddl id", K(ret), K(ddl_id_str));
                } else {
                  tsi_value->ddl_id_str_ = const_cast<common::ObString *>(&ddl_id_str);
                }
              }

              if (OB_SUCC(ret) && OB_SYS_TENANT_ID != ddl_arg_->exec_tenant_id_) {
                // check tenant status
                const int64_t tenant_id = ddl_arg_->exec_tenant_id_;
                share::schema::ObSchemaGetterGuard schema_guard;
                const share::schema::ObTenantSchema *tenant_schema = NULL;
                if (OB_FAIL(root_service_.get_schema_service().get_tenant_schema_guard(tenant_id, schema_guard))) {
                  RS_LOG(WARN, "failed to get schema guard", K(ret), K(tenant_id));
                } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
                  RS_LOG(WARN, "failed to get tenant_schema", K(ret), K(tenant_id));
                } else if (OB_ISNULL(tenant_schema)) {
                  ret = OB_ERR_UNEXPECTED;
                  RS_LOG(WARN, "tenant schema is null", K(ret), K(tenant_id));
                } else if (tenant_schema->is_dropping()) {
                  if (!is_allow_when_drop_tenant(pcode)) {
                    ret = OB_OP_NOT_ALLOW;
                    RS_LOG(WARN, "ddl operation during dropping tenant not allowed", K(ret), K(ddl_arg_));
                    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "ddl operation during dropping tenant");
                  }
                } else if (tenant_schema->is_creating()) {
                  if (!is_allow_when_create_tenant(pcode)) {
                    ret = OB_OP_NOT_ALLOW;
                    RS_LOG(WARN, "ddl operation during creating tenant not allowed", K(ret), K(ddl_arg_));
                    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "ddl operation during creating tenant");
                  }
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          int64_t start_ts = ObTimeUtility::current_time();
          bool with_ddl_lock = false;
          if (is_ddl_like_) {
            if (is_parallel_ddl(pcode)) {
              if (OB_FAIL(root_service_.get_ddl_service().ddl_rlock())) {
                RS_LOG(WARN, "root service ddl lock fail", K(ret), K(ddl_arg_));
              }
            } else {
              if (OB_FAIL(root_service_.get_ddl_service().ddl_wlock())) {
                RS_LOG(WARN, "root service ddl lock fail", K(ret), K(ddl_arg_));
              }
            }
            if (OB_SUCC(ret)) {
              with_ddl_lock = true;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(leader_process())) {
            RS_LOG(WARN, "process failed", K(ret));
            if (!root_service_.in_service()) {
              RS_LOG(WARN, "root service stoped, overwrite return code",
                  "from", ret, "to", OB_RS_SHUTDOWN);
              ret = OB_RS_SHUTDOWN;
            }
            EVENT_ADD(RS_RPC_FAIL_COUNT, 1);
          } else {
            EVENT_ADD(RS_RPC_SUCC_COUNT, 1);
          }
          if (with_ddl_lock) {
            int tmp_ret = root_service_.get_ddl_service().ddl_unlock();
            if (tmp_ret != OB_SUCCESS) {
              RS_LOG(WARN, "root service ddl unlock fail", K(tmp_ret), K(ddl_arg_));
              if (OB_SUCC(ret)) {
                ret = tmp_ret;
              }
            }
          }
          RS_LOG(INFO, "[DDL] execute ddl like stmt", K(ret),
                 "cost", ObTimeUtility::current_time() - start_ts, KPC_(ddl_arg));
        }
      }
    } else {
      if (OB_FAIL(follower_process())) {
        RS_LOG(WARN, "follower process failed", K(ret), K(pcode));
      }
    }
    return ret;
  }

  virtual int leader_process() = 0;
  virtual int follower_process()
  {
    RS_LOG_RET(WARN, common::OB_RS_NOT_MASTER, "not master rootserver");
    return common::OB_RS_NOT_MASTER;
  }
protected:
  ObRootService &root_service_;
  const bool full_service_;
  const bool major_freeze_done_;
  const bool is_ddl_like_;
  const obrpc::ObDDLArg *ddl_arg_;
};

template <obrpc::ObRpcPacketCode pcode>
class ObRootServerRPCProcessor
    : public obrpc::ObCommonRpcProxy::Processor<pcode>, public ObRootServerRPCProcessorBase
{
public:
  ObRootServerRPCProcessor(ObRootService &rs, const bool full_service, const bool major_freeze_done, const bool is_ddl_like, obrpc::ObDDLArg *arg = NULL)
      : ObRootServerRPCProcessorBase(rs, full_service, major_freeze_done, is_ddl_like, arg) {}
protected:
  virtual int before_process()
  {
    common::ObThreadFlags::set_rs_flag();
    return OB_SUCCESS;
  }

  virtual int process()
  {
    return process_(pcode);
  }

  virtual int after_process(int error_code)
  {
    UNUSED(error_code);
    common::ObThreadFlags::cancel_rs_flag();
    return OB_SUCCESS;
  }
};

#define DEFINE_RS_RPC_PROCESSOR_(pcode, pname, stmt, full_service, major_freeze_done, is_ddl_like, arg)         \
  class pname : public ObRootServerRPCProcessor<pcode>                                        \
  {                                                                                           \
  public:                                                                                     \
    explicit pname(ObRootService &rs)                                                         \
      : ObRootServerRPCProcessor<pcode>(rs, full_service, major_freeze_done, is_ddl_like, arg) {}               \
  protected:                                                                                  \
    virtual int leader_process() {                   \
      return root_service_.stmt; }                   \
  };

// RPC need rs in full service status (RS restart task success)
#define DEFINE_RS_RPC_PROCESSOR(pcode, pname, stmt) DEFINE_RS_RPC_PROCESSOR_(pcode, pname, stmt, true, false, false, NULL)

// RPC do not need full service
#define DEFINE_LIMITED_RS_RPC_PROCESSOR(pcode, pname, stmt) DEFINE_RS_RPC_PROCESSOR_(pcode, pname, stmt, false, false, false, NULL)

// DDL RPC need rs in full service status and major freeze done
#define DEFINE_DDL_RS_RPC_PROCESSOR(pcode, pname, stmt) DEFINE_RS_RPC_PROCESSOR_(pcode, pname, stmt, true, true, true, &arg_)

DEFINE_LIMITED_RS_RPC_PROCESSOR(obrpc::OB_RENEW_LEASE, ObRpcRenewLeaseP, renew_lease(arg_, result_));
DEFINE_LIMITED_RS_RPC_PROCESSOR(obrpc::OB_REPORT_SYS_LS, ObRpcReportSysLSP, report_sys_ls(arg_));
DEFINE_LIMITED_RS_RPC_PROCESSOR(obrpc::OB_REMOVE_SYS_LS, ObRpcRemoveSysLSP, remove_sys_ls(arg_));
DEFINE_LIMITED_RS_RPC_PROCESSOR(obrpc::OB_EXECUTE_BOOTSTRAP, ObRpcExecuteBootstrapP, execute_bootstrap(arg_));
// check server_refreshed_ flag in rootservice
DEFINE_LIMITED_RS_RPC_PROCESSOR(obrpc::OB_FETCH_ALIVE_SERVER, ObRpcFetchAliveServerP, fetch_alive_server(arg_, result_));

// DEFINE_RS_RPC_PROCESSOR(obrpc::OB_MERGE_FINISH, ObRpcMergeFinishP, merge_finish(arg_));
// DEFINE_RS_RPC_PROCESSOR(obrpc::OB_FETCH_ACTIVE_SERVER_STATUS, ObRpcFetchActiveServerStatusP, fetch_active_server_status(arg_, result_));

DEFINE_RS_RPC_PROCESSOR(obrpc::OB_DISASTER_RECOVERY_TASK_REPLY, ObRpcDisasterRecoveryTaskReplyP, disaster_recovery_task_reply(arg_));

DEFINE_RS_RPC_PROCESSOR(obrpc::OB_BROADCAST_DS_ACTION, ObBroadcastDSActionP, broadcast_ds_action(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_FETCH_LOCATION, ObRpcFetchLocationP, fetch_location(arg_, result_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_SET_CONFIG, ObRpcAdminSetConfigP, admin_set_config(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_FLUSH_BALANCE_INFO, ObRpcAdminFlushBalanceInfoP, admin_clear_balance_task(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_COPY_TABLE_DEPENDENTS, ObRpcCopyTableDependentsP, copy_table_dependents(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_FINISH_REDEF_TABLE, ObRpcFinishRedefTableP, finish_redef_table(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ABORT_REDEF_TABLE, ObRpcAbortRedefTableP, abort_redef_table(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_UPDATE_DDL_TASK_ACTIVE_TIME, ObRpcUpdateDDLTaskActiveTimeP, update_ddl_task_active_time(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_START_REDEF_TABLE, ObRpcStartRedefTableP, start_redef_table(arg_, result_));

// ddl rpc processors
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_HIDDEN_TABLE, ObRpcCreateHiddenTableP, create_hidden_table(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_COMMIT_ALTER_TENANT_LOCALITY, ObRpcCommitAlterTenantLocalityP, commit_alter_tenant_locality(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_TENANT, ObRpcCreateTenantP, create_tenant(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_TENANT_END, ObRpcCreateTenantEndP, create_tenant_end(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_TENANT, ObRpcDropTenantP, drop_tenant(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_MODIFY_TENANT, ObRpcModifyTenantP, modify_tenant(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_LOCK_TENANT, ObRpcLockTenantP, lock_tenant(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ADD_SYSVAR, ObRpcAddSysVarP, add_system_variable(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_MODIFY_SYSVAR, ObRpcModifySysVarP, modify_system_variable(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_DATABASE, ObRpcCreateDatabaseP, create_database(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_DATABASE, ObRpcAlterDatabaseP, alter_database(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_DATABASE, ObRpcDropDatabaseP, drop_database(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_TABLEGROUP, ObRpcCreateTablegroupP, create_tablegroup(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_TABLEGROUP, ObRpcDropTablegroupP, drop_tablegroup(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_TABLEGROUP, ObRpcAlterTablegroupP, alter_tablegroup(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_TABLE, ObRpcCreateTableP, create_table(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_RECOVER_RESTORE_TABLE_DDL, ObRpcRecoverRestoreTableDDLP, recover_restore_table_ddl(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_PARALLEL_CREATE_TABLE, ObRpcParallelCreateTableP, parallel_create_table(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_TABLE, ObRpcAlterTableP, alter_table(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_EXCHANGE_PARTITION, ObRpcExchangePartitionP, exchange_partition(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_TABLE, ObRpcDropTableP, drop_table(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_RENAME_TABLE, ObRpcRenameTableP, rename_table(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_TRUNCATE_TABLE, ObRpcTruncateTableP, truncate_table(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_TRUNCATE_TABLE_V2, ObRpcTruncateTableV2P, truncate_table_v2(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_GENERATE_AUX_INDEX_SCHEMA, ObRpcGenerateAuxIndexSchemaP, generate_aux_index_schema(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_INDEX, ObRpcCreateIndexP, create_index(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_INDEX, ObRpcDropIndexP, drop_index(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_MLOG, ObRpcCreateMLogP, create_mlog(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_TABLE_LIKE, ObRpcCreateTableLikeP, create_table_like(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_USER, ObRpcCreateUserP, create_user(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_USER, ObRpcDropUserP, drop_user(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_RENAME_USER, ObRpcRenameUserP, rename_user(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_SET_PASSWD, ObRpcSetPasswdP, set_passwd(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_GRANT, ObRpcGrantP, grant(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_REVOKE_USER, ObRpcRevokeUserP, revoke_user(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_LOCK_USER, ObRpcLockUserP, lock_user(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_USER_PROFILE, ObRpcAlterUserProfileP, alter_user_profile(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_SECURITY_AUDIT, ObRpcSecurityAuditP, handle_security_audit(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_REVOKE_DB, ObRpcRevokeDBP, revoke_database(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_REVOKE_TABLE, ObRpcRevokeTableP, revoke_table(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_REVOKE_ROUTINE, ObRpcRevokeRoutineP, revoke_routine(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_REVOKE_SYSPRIV, ObRpcRevokeSysPrivP, revoke_syspriv(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_UPDATE_INDEX_TABLE_STATUS, ObUpdateIndexTableStatusP, update_index_status(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_UPDATE_MVIEW_TABLE_STATUS, ObRpcUpdateMViewTableStatusP, update_mview_status(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_FLASHBACK_TABLE_FROM_RECYCLEBIN, ObRpcFlashBackTableFromRecyclebinP, flashback_table_from_recyclebin(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_FLASHBACK_INDEX, ObRpcFlashBackIndexP, flashback_index(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_PURGE_TABLE, ObRpcPurgeTableP, purge_table(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_PURGE_INDEX, ObRpcPurgeIndexP, purge_index(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_FLASHBACK_DATABASE, ObRpcFlashBackDatabaseP, flashback_database(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_PURGE_DATABASE, ObRpcPurgeDatabaseP, purge_database(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_FLASHBACK_TENANT, ObRpcFlashBackTenantP, flashback_tenant(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_PURGE_TENANT, ObRpcPurgeTenantP, purge_tenant(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_PURGE_EXPIRE_RECYCLE_OBJECTS, ObRpcPurgeExpireRecycleObjectsP, purge_expire_recycle_objects(arg_, result_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_FLASHBACK_TABLE_TO_SCN, ObRpcFlashBackTableToScnP, flashback_table_to_time_point(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_GET_TENANT_SCHEMA_VERSIONS, ObGetTenantSchemaVersionsP,
                        get_tenant_schema_versions(arg_, result_));
// DEFINE_RS_RPC_PROCESSOR(obrpc::OB_UPDATE_FREEZE_SCHEMA_VERSIONS, ObUpdateFreezeSchemaVersionsP,
//                         update_freeze_schema_versions(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_OPTIMIZE_TABLE, ObRpcOptimizeTableP, optimize_table(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_SCHEMA_REVISE, ObRpcSchemaReviseP, schema_revise(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DO_KEYSTORE_DDL, ObRpcDoKeystoreDDLP, do_keystore_ddl(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DO_TABLESPACE_DDL, ObRpcDoTablespaceDDLP, do_tablespace_ddl(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_EXECUTE_DDL_TASK, ObRpcExecuteDDLTaskP, execute_ddl_task(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_MAINTAIN_OBJ_DEPENDENCY_INFO, ObRpcMaintainObjDependencyInfoP, maintain_obj_dependency_info(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_MVIEW_COMPLETE_REFRESH, ObRpcMViewCompleteRefreshP, mview_complete_refresh(arg_, result_));

DEFINE_RS_RPC_PROCESSOR(obrpc::OB_REFRESH_CONFIG, ObRpcRefreshConfigP, refresh_config());
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ROOT_MINOR_FREEZE, ObRpcRootMinorFreezeP, root_minor_freeze(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_CREATE_RESOURCE_UNIT, ObRpcCreateResourceUnitP, create_resource_unit(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ALTER_RESOURCE_UNIT, ObRpcAlterResourceUnitP, alter_resource_unit(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_DROP_RESOURCE_UNIT, ObRpcDropResourceUnitP, drop_resource_unit(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_CLONE_RESOURCE_POOL, ObRpcCloneResourcePoolP, clone_resource_pool(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_CREATE_RESOURCE_POOL, ObRpcCreateResourcePoolP, create_resource_pool(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ALTER_RESOURCE_POOL, ObRpcAlterResourcePoolP, alter_resource_pool(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_DROP_RESOURCE_POOL, ObRpcDropResoucePoolP, drop_resource_pool(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_SPLIT_RESOURCE_POOL, ObRpcSplitResourcePoolP, split_resource_pool(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_MERGE_RESOURCE_POOL, ObRpcMergeResourcePoolP, merge_resource_pool(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ALTER_RESOURCE_TENANT, ObRpcAlterResourceTenantP, alter_resource_tenant(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADD_SERVER, ObRpcAddServerP, add_server(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_DELETE_SERVER, ObRpcDeleteServerP, delete_server(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_CANCEL_DELETE_SERVER, ObRpcCancelDeleteServerP, cancel_delete_server(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_START_SERVER, ObRpcStartServerP, start_server(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_STOP_SERVER, ObRpcStopServerP, stop_server(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADD_ZONE, ObRpcAddZoneP, add_zone(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_DELETE_ZONE, ObRpcDeleteZoneP, delete_zone(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_START_ZONE, ObRpcStartZoneP, start_zone(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_STOP_ZONE, ObRpcStopZoneP, stop_zone(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ALTER_ZONE, ObRpcAlterZoneP, alter_zone(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_CHECK_DANGLING_REPLICA_FINISH, ObCheckDanglingReplicaFinishP, check_dangling_replica_finish(arg_));


DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_OUTLINE, ObRpcCreateOutlineP, create_outline(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_OUTLINE, ObRpcAlterOutlineP, alter_outline(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_OUTLINE, ObRpcDropOutlineP, drop_outline(arg_));

DEFINE_RS_RPC_PROCESSOR(obrpc::OB_CREATE_RESTORE_POINT, ObRpcCreateRestorePointP, create_restore_point(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_DROP_RESTORE_POINT, ObRpcDropRestorePointP, drop_restore_point(arg_));
//routine ddl

DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_ROUTINE, ObRpcCreateRoutineP, create_routine(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_ROUTINE_WITH_RES, ObRpcCreateRoutineWithResP, create_routine_with_res(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_ROUTINE, ObRpcDropRoutineP, drop_routine(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_ROUTINE, ObRpcAlterRoutineP, alter_routine(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_ROUTINE_WITH_RES, ObRpcAlterRoutineWithResP, alter_routine_with_res(arg_, result_));

//udt ddl
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_UDT, ObRpcCreateUDTP, create_udt(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_UDT_WITH_RES, ObRpcCreateUDTWithResP, create_udt_with_res(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_UDT, ObRpcDropUDTP, drop_udt(arg_));

DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_SYNONYM, ObRpcCreateSynonymP, create_synonym(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_SYNONYM, ObRpcDropSynonymP, drop_synonym(arg_));

DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_DBLINK, ObRpcCreateDbLinkP, create_dblink(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_DBLINK, ObRpcDropDbLinkP, drop_dblink(arg_));

DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_USER_DEFINED_FUNCTION, ObRpcCreateUserDefinedFunctionP, create_user_defined_function(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_USER_DEFINED_FUNCTION, ObRpcDropUserDefinedFunctionP, drop_user_defined_function(arg_));

//package ddl
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_PACKAGE, ObRpcCreatePackageP, create_package(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_PACKAGE_WITH_RES, ObRpcCreatePackageWithResP, create_package_with_res(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_PACKAGE, ObRpcAlterPackageP, alter_package(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_PACKAGE_WITH_RES, ObRpcAlterPackageWithResP, alter_package_with_res(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_PACKAGE, ObRpcDropPackageP, drop_package(arg_));

DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_TRIGGER, ObRpcCreateTriggerP, create_trigger(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_TRIGGER_WITH_RES, ObRpcCreateTriggerWithResP, create_trigger_with_res(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_TRIGGER, ObRpcAlterTriggerP, alter_trigger(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_TRIGGER_WITH_RES, ObRpcAlterTriggerWithResP, alter_trigger_with_res(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_TRIGGER, ObRpcDropTriggerP, drop_trigger(arg_));

//profile ddl
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DO_PROFILE_DDL, ObRpcDoProfileDDLP, do_profile_ddl(arg_));


// Alter role ddl
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_ROLE, ObRpcAlterRoleP, alter_role(arg_));

DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_SWITCH_REPLICA_ROLE, ObRpcAdminSwitchReplicaRoleP, admin_switch_replica_role(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_SWITCH_RS_ROLE, ObRpcAdminSwitchRSRoleP, admin_switch_rs_role(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_DROP_REPLICA, ObRpcAdminDropReplicaP, admin_drop_replica(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_CHANGE_REPLICA, ObRpcAdminChangeReplicaP, admin_change_replica(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_MIGRATE_REPLICA, ObRpcAdminMigrateReplicaP, admin_migrate_replica(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_REPORT_REPLICA, ObRpcAdminReportReplicaP, admin_report_replica(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_RECYCLE_REPLICA, ObRpcAdminRecycleReplicaP, admin_recycle_replica(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_MERGE, ObRpcAdminMergeP, admin_merge(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_RECOVERY, ObRpcAdminRecoveryP, admin_recovery(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_CLEAR_ROOTTABLE, ObRpcAdminClearRoottableP, admin_clear_roottable(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_REFRESH_SCHEMA, ObRpcAdminRefreshSchemaP, admin_refresh_schema(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_CLEAR_LOCATION_CACHE, ObRpcAdminClearLocationCacheP, admin_clear_location_cache(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_REFRESH_MEMORY_STAT, ObRpcAdminRefreshMemStatP, admin_refresh_memory_stat(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_WASH_MEMORY_FRAGMENTATION, ObRpcAdminWashMemFragmentationP, admin_wash_memory_fragmentation(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_REFRESH_IO_CALIBRATION, ObRpcAdminRefreshIOCalibrationP, admin_refresh_io_calibration(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_RELOAD_UNIT, ObRpcAdminReloadUnitP, admin_reload_unit());
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_RELOAD_SERVER, ObRpcAdminReloadServerP, admin_reload_server());
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_RELOAD_ZONE, ObRpcAdminReloadZoneP, admin_reload_zone());
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_CLEAR_MERGE_ERROR, ObRpcAdminClearMergeErrorP, admin_clear_merge_error(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_MIGRATE_UNIT, ObRpcAdminMigrateUnitP, admin_migrate_unit(arg_));
#ifdef OB_BUILD_ARBITRATION
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_ADD_ARBITRATION_SERVICE, ObRpcAdminAddArbitrationServiceP, admin_add_arbitration_service(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_REMOVE_ARBITRATION_SERVICE, ObRpcAdminRemoveArbitrationServiceP, admin_remove_arbitration_service(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_REPLACE_ARBITRATION_SERVICE, ObRpcAdminReplaceArbitrationServiceP, admin_replace_arbitration_service(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_REMOVE_CLUSTER_INFO_FROM_ARB_SERVER, ObRpcRemoveClusterInfoFromArbServerP, remove_cluster_info_from_arb_server(arg_));
#endif
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_UPGRADE_VIRTUAL_SCHEMA, ObRpcAdminUpgradeVirtualSchemaP, admin_upgrade_virtual_schema());
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_RUN_JOB, ObRpcRunJobP, run_job(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_RUN_UPGRADE_JOB, ObRpcRunUpgradeJobP, run_upgrade_job(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_FLUSH_CACHE, ObRpcAdminFlushCacheP, admin_flush_cache(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_UPGRADE_CMD, ObRpcAdminUpgradeCmdP, admin_upgrade_cmd(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_ROLLING_UPGRADE_CMD, ObRpcAdminRollingUpgradeCmdP, admin_rolling_upgrade_cmd(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_RS_SET_TP, ObRpcAdminSetTPP, admin_set_tracepoint(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_REFRESH_TIME_ZONE_INFO, ObRpcRefreshTimeZoneInfoP, refresh_time_zone_info(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_REQUEST_TIME_ZONE_INFO, ObRpcRequestTimeZoneInfoP, request_time_zone_info(arg_, result_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_RS_UPDATE_STAT_CACHE, ObRpcUpdateStatCacheP, update_stat_cache(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_CALC_COLUMN_CHECKSUM_RESPONSE, ObRpcCalcColumnChecksumResponseP, calc_column_checksum_repsonse(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_DDL_BUILD_SINGLE_REPLICA_RESPONSE, ObRpcDDLBuildSingleReplicaResponseP, build_ddl_single_replica_response(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_CANCEL_DDL_TASK, ObRpcCancelDDLTaskP, cancel_ddl_task(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_FORCE_CREATE_SYS_TABLE, ObForceCreateSysTableP, force_create_sys_table(arg_));
class ObForceSetLocalityP : public ObRootServerRPCProcessor<obrpc::OB_FORCE_SET_LOCALITY>
{
public:
  explicit ObForceSetLocalityP(ObRootService &rs)
    : ObRootServerRPCProcessor<obrpc::OB_FORCE_SET_LOCALITY>(rs, true, false, false, NULL) {}
protected:
  virtual int leader_process() { return root_service_.force_set_locality(arg_); }
  int before_process() {
    int ret = OB_SUCCESS;
    if (OB_SUCC(ObRootServerRPCProcessor<obrpc::OB_FORCE_SET_LOCALITY>::before_process())) {
      ret = req_->is_from_unix_domain()? OB_SUCCESS : OB_NOT_SUPPORTED;
    }
    return ret;
  }
};

//sequence ddl
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DO_SEQUENCE_DDL, ObRpcDoSequenceDDLP, do_sequence_ddl(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_BROADCAST_SCHEMA, ObBroadcastSchemaP, broadcast_schema(arg_));
// only for upgrade
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_GET_RECYCLE_SCHEMA_VERSIONS, ObGetRecycleSchemaVersionsP, get_recycle_schema_versions(arg_, result_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_UPGRADE_TABLE_SCHEMA, ObRpcUpgradeTableSchemaP, upgrade_table_schema(arg_));
//label security ddl
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_HANDLE_LABEL_SE_POLICY_DDL, ObRpcHandleLabelSePolicyDDLP, handle_label_se_policy_ddl(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_HANDLE_LABEL_SE_COMPONENT_DDL, ObRpcHandleLabelSeComponentDDLP, handle_label_se_component_ddl(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_HANDLE_LABEL_SE_LABEL_DDL, ObRpcHandleLabelSeLabelDDLP, handle_label_se_label_ddl(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_HANDLE_LABEL_SE_USER_LEVEL_DDL, ObRpcHandleLabelSeUserLevelDDLP, handle_label_se_user_level_ddl(arg_));

// backup and restore
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_PHYSICAL_RESTORE_TENANT, ObRpcPhysicalRestoreTenantP, physical_restore_tenant(arg_, result_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_REBUILD_INDEX_IN_RESTORE, ObRpcRebuildIndexInRestoreP, rebuild_index_in_restore(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ARCHIVE_LOG, ObArchiveLogP, handle_archive_log(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_BACKUP_DATABASE, ObBackupDatabaseP, handle_backup_database(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_BACKUP_MANAGE, ObBackupManageP, handle_backup_manage(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_BACKUP_CLEAN, ObBackupCleanP, handle_backup_delete(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_DELETE_POLICY, ObDeletePolicyP, handle_delete_policy(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_PHYSICAL_RESTORE_RES, ObRpcPhysicalRestoreResultP, send_physical_restore_result(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_RECOVER_TABLE, ObRecoverTableP, handle_recover_table(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_CLONE_TENANT, ObRpcCloneTenantP, clone_tenant(arg_, result_));

DEFINE_RS_RPC_PROCESSOR(obrpc::OB_RS_FLUSH_OPT_STAT_MONITORING_INFO, ObRpcFlushOptStatMonitoringInfoP, flush_opt_stat_monitoring_info(arg_));

// directory object
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_CREATE_DIRECTORY, ObRpcCreateDirectoryP, create_directory(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DROP_DIRECTORY, ObRpcDropDirectoryP, drop_directory(arg_));

// context object
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_DO_CONTEXT_DDL, ObRpcDoContextDDLP, do_context_ddl(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_RECOMPILE_ALL_VIEWS_BATCH, ObRpcRecompileAllViewsBatchP, recompile_all_views_batch(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_TRY_ADD_DEP_INFOS_FOR_SYNONYM_BATCH, ObRpcTryAddDepInfosForSynonymBatchP,try_add_dep_infos_for_synonym_batch(arg_));
#ifdef OB_BUILD_SPM
// sql plan baseline
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_RS_ACCEPT_PLAN_BASELINE, ObRpcAcceptPlanBaselineP, accept_plan_baseline(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_RS_CANCEL_EVOLVE_TASK, ObRpcCancelEvolveTaskP, cancel_evolve_task(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_LOAD_BASELINE, ObRpcAdminLoadBaselineP, admin_load_baseline(arg_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_LOAD_BASELINE_V2, ObRpcAdminLoadBaselineV2P, admin_load_baseline_v2(arg_, result_));

#endif

DEFINE_RS_RPC_PROCESSOR(obrpc::OB_ADMIN_SYNC_REWRITE_RULES, ObRpcAdminSyncRewriteRulesP, admin_sync_rewrite_rules(arg_));
// row level security ddl
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_HANDLE_RLS_POLICY_DDL, ObRpcHandleRlsPolicyDDLP, handle_rls_policy_ddl(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_HANDLE_RLS_GROUP_DDL, ObRpcHandleRlsGroupDDLP, handle_rls_group_ddl(arg_));
DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_HANDLE_RLS_CONTEXT_DDL, ObRpcHandleRlsContextDDLP, handle_rls_context_ddl(arg_));
#ifdef OB_BUILD_TDE_SECURITY
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_GET_ROOT_KEY, ObGetRootKeyP, handle_get_root_key(arg_, result_));
DEFINE_RS_RPC_PROCESSOR(obrpc::OB_RELOAD_MASTER_KEY, ObReloadMasterKeyP, reload_master_key(arg_, result_));
#endif

DEFINE_DDL_RS_RPC_PROCESSOR(obrpc::OB_ALTER_USER_PROXY, ObRpcAlterUserProxyP, alter_user_proxy(arg_, result_));

#undef DEFINE_RS_RPC_PROCESSOR_
#undef DEFINE_RS_RPC_PROCESSOR
#undef DEFINE_LIMITED_RS_RPC_PROCESSOR

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_RS_RPC_PROCESSOR_H_
