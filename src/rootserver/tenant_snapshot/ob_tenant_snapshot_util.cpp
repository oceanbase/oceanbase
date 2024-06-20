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

#define USING_LOG_PREFIX RS

#include "ob_tenant_snapshot_util.h"
#include "rootserver/ob_ls_service_helper.h" //ObTenantLSInfo
#include "share/backup/ob_tenant_archive_mgr.h"
#include "share/balance/ob_balance_job_table_operator.h" //ObBalanceJob
#include "share/balance/ob_balance_task_table_operator.h" //ObBalanceTaskArray
#include "share/balance/ob_balance_task_helper_operator.h" //ObBalanceTaskHelper
#include "share/location_cache/ob_location_service.h"
#include "share/ls/ob_ls_operator.h" //ObLSAttrOperator
#include "share/ob_global_stat_proxy.h" //ObGlobalStatProxy
#include "share/tenant_snapshot/ob_tenant_snapshot_table_operator.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tablelock/ob_lock_utils.h" //ObInnerTableLockUtil
#include "storage/tablelock/ob_lock_inner_connection_util.h" // for ObInnerConnectionLockUtil
#include "observer/ob_inner_sql_connection.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::transaction::tablelock;

static const char* conflict_case_with_clone_strs[] = {
  "UPGRADE",
  "TRANSFER",
  "MODIFY_RESOURCE_POOL",
  "MODIFY_UNIT",
  "MODIFY_LS",
  "MODIFY_REPLICA",
  "MODIFY_TENANT_ROLE_OR_SWITCHOVER_STATUS",
  "DELAY_DROP_TENANT",
  "STANDBY_UPGRADE",
  "STANDBY_TRANSFER",
  "STANDBY_MODIFY_LS"
};

const char* ObConflictCaseWithClone::get_case_name_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(conflict_case_with_clone_strs) == (int64_t)MAX_CASE_NAME,
                "conflict_case_with_clone_strs string array size mismatch enum ConflictCaseWithClone count");
  const char *str = NULL;
  if (case_name_ > INVALID_CASE_NAME && case_name_ < MAX_CASE_NAME) {
    str = conflict_case_with_clone_strs[static_cast<int64_t>(case_name_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid ConflictCaseWithClone", K_(case_name));
  }
  return str;
}

int64_t ObConflictCaseWithClone::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(case_name));
  J_OBJ_END();
  return pos;
}

const char* ObTenantSnapshotUtil::TENANT_SNAP_OP_PRINT_ARRAY[] =
{
  "create tenant snapshot",
  "drop tenant snapshot",
  "clone tenant",
  "fork tenant",
};

const char* ObTenantSnapshotUtil::get_op_print_str(const TenantSnapshotOp &op)
{
  STATIC_ASSERT(ARRAYSIZEOF(TENANT_SNAP_OP_PRINT_ARRAY) == static_cast<int64_t>(TenantSnapshotOp::MAX),
                "type string array size mismatch with enum tenant snapshot operation count");
  const char* str = "";
  if (op >= TenantSnapshotOp::CREATE_OP && op < TenantSnapshotOp::MAX) {
    str = TENANT_SNAP_OP_PRINT_ARRAY[static_cast<int8_t>(op)];
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid tenant snapshot op", K(op));
  }
  return str;
}

int ObTenantSnapshotUtil::create_tenant_snapshot(const ObString &tenant_name,
                                                 const ObString &tenant_snapshot_name,
                                                 uint64_t &tenant_id,
                                                 ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive create tenant snapshot request", K(tenant_name), K(tenant_snapshot_name));
  tenant_id = OB_INVALID_TENANT_ID;
  tenant_snapshot_id.reset();
  uint64_t source_tenant_id = OB_INVALID_TENANT_ID;
  ObTenantSnapshotID tmp_tenant_snapshot_id;
  ObSqlString new_snapshot_name;

  if (OB_UNLIKELY(tenant_name.empty()
                  || tenant_snapshot_name.length() > OB_MAX_TENANT_SNAPSHOT_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_name), K(tenant_snapshot_name));
  } else if (OB_FAIL(get_tenant_id(tenant_name, source_tenant_id))) {
    LOG_WARN("get tenant id failed", KR(ret), K(tenant_name));
  } else if (!is_user_tenant(source_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("non-user tenant", KR(ret), K(source_tenant_id));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "non-user tenant id");
  } else if (OB_FAIL(check_source_tenant_info(source_tenant_id, CREATE_OP))) {
    LOG_WARN("check source tenant info failed", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(check_log_archive_ready(source_tenant_id, tenant_name))) {
    LOG_WARN("check log archive ready failed", KR(ret), K(source_tenant_id));
  } else if (tenant_snapshot_name.empty() &&
             OB_FAIL(rootserver::ObTenantSnapshotUtil::generate_tenant_snapshot_name(
                                                      source_tenant_id, new_snapshot_name, false))) {
    LOG_WARN("failed to generate new tenant snapshot name", KR(ret), K(source_tenant_id));
  } else if (!tenant_snapshot_name.empty() &&
             OB_FAIL(new_snapshot_name.assign(tenant_snapshot_name.ptr(),
                                              tenant_snapshot_name.length()))) {
    LOG_WARN("failed to assign", KR(ret), K(tenant_snapshot_name));
  } else if (OB_UNLIKELY(new_snapshot_name.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid snapshot name", KR(ret), K(new_snapshot_name));
  } else if (OB_FAIL(rootserver::ObTenantSnapshotUtil::generate_tenant_snapshot_id(
                                                      source_tenant_id, tmp_tenant_snapshot_id))) {
    LOG_WARN("failed to generate snapshot id", KR(ret), K(source_tenant_id));
  } else if (!tmp_tenant_snapshot_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid snapshot id", KR(ret), K(tmp_tenant_snapshot_id));
  } else {
    ObMySQLTransaction trans;
    ObTenantSnapStatus original_global_state_status = ObTenantSnapStatus::MAX;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(source_tenant_id)))) {
      LOG_WARN("failed to start trans", KR(ret), K(source_tenant_id));
    } else if (OB_FAIL(trylock_tenant_snapshot_simulated_mutex(trans, source_tenant_id,
                          TenantSnapshotOp::CREATE_OP, OB_INVALID_ID /*owner_job_id*/,
                          original_global_state_status))) {
      if (OB_TENANT_SNAPSHOT_LOCK_CONFLICT != ret) {
        LOG_WARN("trylock tenant snapshot simulated mutex failed", KR(ret), K(source_tenant_id));
      } else {
        LOG_WARN("GLOBAL_STATE snapshot lock conflict", KR(ret), K(source_tenant_id),
                                                            K(original_global_state_status));
        LOG_USER_ERROR(OB_TENANT_SNAPSHOT_LOCK_CONFLICT, "there may be other tenant snapshot operation in progress");
      }
    } else if (OB_FAIL(check_tenant_has_no_conflict_tasks(source_tenant_id))) {
      LOG_WARN("fail to check tenant has conflict tasks", KR(ret), K(source_tenant_id));
    } else if (OB_FAIL(add_create_tenant_snapshot_task(trans, source_tenant_id,
                                                       new_snapshot_name.string(),
                                                       tmp_tenant_snapshot_id))) {
      LOG_WARN("add create tenant snapshot task failed", KR(ret), K(source_tenant_id),
                                                         K(new_snapshot_name),
                                                         K(tmp_tenant_snapshot_id));
    } else {
      tenant_id = source_tenant_id;
      tenant_snapshot_id = tmp_tenant_snapshot_id;
    }

    DEBUG_SYNC(AFTER_LOCK_SNAPSHOT_MUTEX);
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
        ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      }
    }
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_scheduler(source_tenant_id))) {
      LOG_WARN("notify tenant snapshot scheduler failed", KR(tmp_ret), K(source_tenant_id));
    }
  }
  return ret;
}

// create tenant without global lock
int ObTenantSnapshotUtil::create_fork_tenant_snapshot(ObMySQLTransaction &trans,
                                                      const uint64_t target_tenant_id,
                                                      const ObString &target_tenant_name,
                                                      const ObString &tenant_snapshot_name,
                                                      const ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive create fork tenant snapshot request", K(target_tenant_id), K(tenant_snapshot_id),
                                                          K(tenant_snapshot_name));

  if (!is_user_tenant(target_tenant_id) || target_tenant_name.empty() ||
                      tenant_snapshot_name.empty() || !tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_tenant_id), K(target_tenant_name),
                                 K(tenant_snapshot_name), K(tenant_snapshot_id));
  } else if (OB_FAIL(check_source_tenant_info(target_tenant_id, CREATE_OP))) {
    LOG_WARN("check source tenant info failed", KR(ret), K(target_tenant_id));
  } else if (OB_FAIL(check_log_archive_ready(target_tenant_id, target_tenant_name))) {
    LOG_WARN("check log archive ready failed", KR(ret), K(target_tenant_id), K(target_tenant_name));
  } else if (OB_FAIL(add_create_tenant_snapshot_task(trans, target_tenant_id,
                                                     tenant_snapshot_name,
                                                     tenant_snapshot_id))) {
    LOG_WARN("add create tenant snapshot task failed", KR(ret), K(target_tenant_id),
                                                       K(tenant_snapshot_id),
                                                       K(tenant_snapshot_name));
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_scheduler(target_tenant_id))) {
      LOG_WARN("notify tenant snapshot scheduler failed", KR(tmp_ret), K(target_tenant_id));
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::drop_tenant_snapshot(const ObString &tenant_name,
                                               const ObString &tenant_snapshot_name)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive drop tenant snapshot request", K(tenant_name), K(tenant_snapshot_name));
  uint64_t target_tenant_id = OB_INVALID_TENANT_ID;

  if (OB_UNLIKELY(tenant_name.empty() || tenant_snapshot_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_name), K(tenant_snapshot_name));
  } else if (OB_FAIL(get_tenant_id(tenant_name, target_tenant_id))) {
    LOG_WARN("get tenant id failed", KR(ret), K(tenant_name));
  } else if (!is_user_tenant(target_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("non-user tenant", KR(ret), K(target_tenant_id));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "non-user tenant id");
  } else if (OB_FAIL(check_source_tenant_info(target_tenant_id, DROP_OP))) {
    LOG_WARN("check source tenant info failed", KR(ret), K(target_tenant_id));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(target_tenant_id)))) {
      LOG_WARN("failed to start trans", KR(ret), K(target_tenant_id));
    } else if (OB_FAIL(add_drop_tenant_snapshot_task(trans, target_tenant_id, tenant_snapshot_name))) {
      LOG_WARN("add drop tenant snapshot task failed", KR(ret), K(target_tenant_id), K(tenant_snapshot_name));
    } else if (OB_FAIL(recycle_tenant_snapshot_ls_replicas(trans, target_tenant_id, tenant_snapshot_name))) {
      LOG_WARN("fail to recycle tenant snapshot ls replicas", KR(ret), K(tenant_name),
                                                              K(target_tenant_id),
                                                              K(tenant_snapshot_name));
    }

    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
        ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      }
    }
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_scheduler(target_tenant_id))) {
      LOG_WARN("notify tenant snapshot scheduler failed", KR(tmp_ret), K(target_tenant_id));
    }
  }

  return ret;
}

int ObTenantSnapshotUtil::get_tenant_id(const ObString &tenant_name,
                                        uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;

  if (OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_TENANT_NAME;
    LOG_WARN("invalid tenant name", KR(ret), K(tenant_name));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, tenant_schema))) {
    LOG_WARN("failed to get tenant info", KR(ret), K(tenant_name));
  } else if (NULL == tenant_schema) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(tenant_name));
  } else {
    tenant_id = tenant_schema->get_tenant_id();
  }
  return ret;
}

int ObTenantSnapshotUtil::check_source_tenant_info(const uint64_t tenant_id,
                                                   const TenantSnapshotOp op)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool status_satisfied = false;
  bool compatibility_satisfied = false;
  bool role_satisfied = false;
  ObAllTenantInfo all_tenant_info;
  ObSqlString print_str;

  if (OB_UNLIKELY(TenantSnapshotOp::MAX == op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(op));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant snapshot operation for non-user-tenant is not allowed", KR(ret), K(tenant_id));
    if (TenantSnapshotOp::RESTORE_OP == op || TenantSnapshotOp::FORK_OP == op) {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "clone non-user-tenant is");
    } else {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant snapshot operation for non-user-tenant is");
    }
  } else if (OB_FAIL(check_tenant_status(tenant_id, status_satisfied))) {
    LOG_WARN("check tenant status failed", KR(ret), K(tenant_id));
  } else if (!status_satisfied) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant status is not valid", KR(ret), K(tenant_id), K(status_satisfied));
    if (OB_TMP_FAIL(print_str.assign_fmt("source tenant status is not valid, %s", get_op_print_str(op)))) {
      LOG_WARN("assign failed", KR(tmp_ret), K(get_op_print_str(op)));
    } else {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, print_str.ptr());
    }
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_clone_tenant(tenant_id, compatibility_satisfied))) {
    LOG_WARN("check tenant compatibility failed", KR(ret), K(tenant_id));
  } else if (!compatibility_satisfied) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant data version is below 4.3", KR(ret), K(tenant_id), K(compatibility_satisfied));
    if (OB_TMP_FAIL(print_str.assign_fmt("source tenant data version is below 4.3, %s", get_op_print_str(op)))) {
      LOG_WARN("assign failed", KR(tmp_ret), K(get_op_print_str(op)));
    } else {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, print_str.ptr());
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_status(const uint64_t tenant_id,
                                              bool &is_satisfied)
{
  int ret = OB_SUCCESS;
  is_satisfied = false;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_schema = NULL;
  const ObSimpleTenantSchema *meta_tenant_schema = NULL;
  uint64_t meta_tenant_id = OB_INVALID_TENANT_ID;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || OB_ISNULL(GCTX.schema_service_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant schema is null", KR(ret), K(tenant_id));
  } else if (!tenant_schema->is_normal()) {
  } else if (tenant_schema->is_in_recyclebin()) {
  } else if (FALSE_IT(meta_tenant_id = gen_meta_tenant_id(tenant_id))) {
  } else if (OB_FAIL(schema_guard.get_tenant_info(meta_tenant_id, meta_tenant_schema))) {
    LOG_WARN("fail to get tenant info", KR(ret), K(meta_tenant_id));
  } else if (OB_ISNULL(meta_tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("meta tenant schema is null", KR(ret), K(meta_tenant_id));
  } else if (!meta_tenant_schema->is_normal()) {
  } else {
    is_satisfied = true;
  }

  return ret;
}

int ObTenantSnapshotUtil::check_log_archive_ready(const uint64_t tenant_id, const ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr round_attr;
  int64_t fake_incarnation = 1;

  if (OB_INVALID_TENANT_ID == tenant_id || tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_name));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret));
  } else if (OB_FAIL(ObTenantArchiveMgr::get_tenant_current_round(tenant_id, fake_incarnation, round_attr))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "log archive is not ready, create tenant snapshot");
    } else {
      LOG_WARN("failed to get cur log archive round", KR(ret), K(tenant_id));
    }
  } else if (ObArchiveRoundState::Status::DOING != round_attr.state_.status_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("log archive is not ready", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "log archive is not ready, create tenant snapshot");
  } else {
    SCN min_ls_ckpt_scn;
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      uint64_t min_ls_checkpoint_scn = 0;
      if (OB_FAIL(sql.assign_fmt("SELECT MIN(checkpoint_scn) as min_ckpt_scn FROM %s "
                                 "WHERE tenant_id = %ld",
                                 OB_ALL_VIRTUAL_LS_INFO_TNAME, tenant_id))) {
        LOG_WARN("assign sql failed", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(sql), K(tenant_id));
      } else if (nullptr == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next row", KR(ret), K(sql));
      } else {
        EXTRACT_UINT_FIELD_MYSQL(*result, "min_ckpt_scn", min_ls_checkpoint_scn, uint64_t);
        if (OB_UNLIKELY(OB_ERR_NULL_VALUE == ret)) {
          ret = OB_EAGAIN;
          LOG_WARN("fail to get result from virtual table, "
                   "the searching might be timeout", KR(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(min_ls_ckpt_scn.convert_for_inner_table_field(min_ls_checkpoint_scn))) {
          LOG_WARN("fail to convert scn", KR(ret), K(min_ls_checkpoint_scn));
        }
      }
    }

    if (OB_SUCC(ret) && min_ls_ckpt_scn < round_attr.start_scn_) {
      ret = OB_ERR_TENANT_SNAPSHOT;
      LOG_WARN("ls checkpoint scn is smaller than archive start scn", KR(ret),
                                                                      K(round_attr.start_scn_),
                                                                      K(min_ls_ckpt_scn));
      ObSqlString str;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(str.assign_fmt("Current checkpoint scn is smaller than the start_scn of "
                                     "archive log, creating tenant snapshot is not allowed. "
                                     "You could execute 'ALTER SYSTEM MINOR FREEZE TENANT = %.*s' "
                                     "to trigger a minor compaction for advancing the checkpoint "
                                     "and then wait for a minute before executing again.",
                                     tenant_name.length(), tenant_name.ptr()))) {
        LOG_WARN("fail to append format", KR(tmp_ret), K(tenant_id));
      } else {
        LOG_USER_ERROR(OB_ERR_TENANT_SNAPSHOT, str.string().length(), str.ptr());
      }
    }
  }

  return ret;
}

//*********************************************************************
//We use the record (GLOBAL_STATE) in the inner table __all_tenant_snapshots
//  to simulate a mutex for tenant snapshot.
//Note: drop tenant snapshot operation no need to trylock.
//      special_item is the memory structure of GLOBAL_STATE.
//*********************************************************************
int ObTenantSnapshotUtil::trylock_tenant_snapshot_simulated_mutex(ObMySQLTransaction &trans,
                                                                  const uint64_t tenant_id,
                                                                  const TenantSnapshotOp op,
                                                                  const int64_t owner_job_id,
                                                                  ObTenantSnapStatus &original_global_state_status)
{
  int ret = OB_SUCCESS;
  original_global_state_status = ObTenantSnapStatus::MAX;
  ObTenantSnapItem special_item;
  bool lock_conflict = true;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id)
                  || (CREATE_OP != op && RESTORE_OP != op && FORK_OP != op))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(op));
  } else if (OB_FAIL(check_tenant_snapshot_simulated_mutex_(trans, tenant_id,
                                                  special_item, lock_conflict))) {
    LOG_WARN("check simulated mutex failed", KR(ret), K(tenant_id));
  } else if (FALSE_IT(original_global_state_status = special_item.get_status())) {
  } else if (lock_conflict) {
    if (0 < owner_job_id && special_item.get_owner_job_id() == owner_job_id) {
      // has been locked by clone job
      lock_conflict = false;
      LOG_INFO("has been locked by one's own clone job before", KR(ret), K(tenant_id),
                                                K(owner_job_id), K(special_item));
    } else {
      ret = OB_TENANT_SNAPSHOT_LOCK_CONFLICT;
      LOG_WARN("GLOBAL_STATE snapshot lock conflict", KR(ret), K(tenant_id));
    }
  } else if (FALSE_IT(special_item.set_owner_job_id(owner_job_id))) {
  } else if (OB_FAIL(lock_(trans, tenant_id, op, special_item))) {
    LOG_WARN("lock_ failed", KR(ret), K(tenant_id), K(op), K(special_item));
  }

  return ret;
}

int ObTenantSnapshotUtil::unlock_tenant_snapshot_simulated_mutex_from_clone_release_task(ObMySQLTransaction &trans,
                                                                                         const uint64_t tenant_id,
                                                                                         const int64_t owner_job_id,
                                                                                         const ObTenantSnapStatus &old_status,
                                                                                         bool &is_already_unlocked)
{
  int ret = OB_SUCCESS;
  const SCN snapshot_scn = SCN::invalid_scn();
  is_already_unlocked = false;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id)) ||
      OB_INVALID_ID == owner_job_id ||
      ObTenantSnapStatus::MAX == old_status) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(owner_job_id), K(tenant_id), K(old_status));
  } else if (OB_FAIL(unlock_(trans, tenant_id, owner_job_id, old_status, snapshot_scn, is_already_unlocked))) {
    LOG_WARN("fail to unlock", KR(ret), K(tenant_id), K(owner_job_id), K(old_status), K(snapshot_scn));
  } else if (is_already_unlocked) {
    // curent owner_job_id of global_lock is different from given id,
    // which means the global_lock of current releasing job has been already unlocked.
    // ret = OB_SUCCESS;
  }

  return ret;
}

int ObTenantSnapshotUtil::unlock_tenant_snapshot_simulated_mutex_from_snapshot_task(ObMySQLTransaction &trans,
                                                                                    const uint64_t tenant_id,
                                                                                    const ObTenantSnapStatus &old_status,
                                                                                    const SCN &snapshot_scn)
{
  int ret = OB_SUCCESS;
  const int64_t owner_job_id = OB_INVALID_ID;
  bool is_conflicted_owner_job_id = true;

  //NOTE: snapshot_scn may be invalid in case of failure to create snapshot.
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || ObTenantSnapStatus::MAX == old_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(old_status), K(snapshot_scn));
  } else if (OB_FAIL(unlock_(trans, tenant_id, owner_job_id, old_status, snapshot_scn, is_conflicted_owner_job_id))) {
    LOG_WARN("fail to unlock", KR(ret), K(tenant_id), K(owner_job_id), K(old_status), K(snapshot_scn));
  } else if (is_conflicted_owner_job_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job_id is different from owner_job_id of global lock", KR(ret), K(owner_job_id));
  }

  return ret;
}

//*********************************************************************
//if the status of the record (GLOBAL_STATE) is "NORMAL", it means
//  the simulated mutex is not locked,
//  and we can lock it (change the status) later to prevent other tenant snapshot operations.
//or else, it means the simulated mutex is locked,
//  and we can not do tenant snapshot operation.
//Note: At the beginning, if the record (GLOBAL_STATE) is not exist, We have to insert it.
//*********************************************************************
int ObTenantSnapshotUtil::check_tenant_snapshot_simulated_mutex_(ObMySQLTransaction &trans,
                                                                 const uint64_t tenant_id,
                                                                 ObTenantSnapItem &special_item,
                                                                 bool &is_conflict)
{
  int ret = OB_SUCCESS;
  special_item.reset();
  is_conflict = true;
  ObTenantSnapshotTableOperator table_op;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  //get special item(GLOBAL_STATE)
  } else if (OB_FAIL(table_op.init(tenant_id, &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_op.get_tenant_snap_item(
                        ObTenantSnapshotID(ObTenantSnapshotTableOperator::GLOBAL_STATE_ID),
                        true /*for update*/,
                        special_item))) {
    if (OB_TENANT_SNAPSHOT_NOT_EXIST != ret) {
      LOG_WARN("failed to get special tenant snapshot item", KR(ret), K(tenant_id));
    } else {
      ret = OB_SUCCESS;
      special_item.reset();
      ObTenantSnapItem tmp_special_item;
      //insert special item(GLOBAL_STATE)
      if (OB_FAIL(tmp_special_item.init(tenant_id,                           /*tenant_id*/
                                ObTenantSnapshotID(ObTenantSnapshotTableOperator::GLOBAL_STATE_ID), /*snapshot_id*/
                                ObTenantSnapshotTableOperator::GLOBAL_STATE_NAME, /*snapshot_name*/
                                ObTenantSnapStatus::NORMAL,          /*status*/
                                SCN::invalid_scn(),                  /*snapshot_scn*/
                                SCN::invalid_scn(),                  /*clog_start_scn*/
                                ObTenantSnapType::AUTO,              /*type*/
                                OB_INVALID_TIMESTAMP,                /*create_time*/
                                0,                                   /*data_version*/
                                OB_INVALID_ID                        /*owner_job_id*/))) {
        LOG_WARN("failed to init special item", KR(ret), K(tenant_id));
      } else if (OB_FAIL(table_op.insert_tenant_snap_item(tmp_special_item))) {
        LOG_WARN("failed to insert special snapshot item", KR(ret), K(tmp_special_item));
        if (OB_TENANT_SNAPSHOT_EXIST == ret) {
          ret = OB_TENANT_SNAPSHOT_LOCK_CONFLICT;
        }
      //get special item(GLOBAL_STATE)
      } else if (OB_FAIL(table_op.get_tenant_snap_item(
                        ObTenantSnapshotID(ObTenantSnapshotTableOperator::GLOBAL_STATE_ID),
                        true /*for update*/,
                        special_item))) {
        LOG_WARN("failed to get special tenant snapshot item", KR(ret), K(tenant_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (ObTenantSnapStatus::NORMAL == special_item.get_status()) {
      is_conflict = false;
    }
  }

  return ret;
}

int ObTenantSnapshotUtil::unlock_(ObMySQLTransaction &trans,
                                  const uint64_t tenant_id,
                                  const int64_t owner_job_id,
                                  const ObTenantSnapStatus &old_status,
                                  const SCN &snapshot_scn,
                                  bool &is_conflicted_owner_job_id)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotTableOperator table_op;
  ObTenantSnapItem global_lock;
  is_conflicted_owner_job_id = false;

  if (OB_FAIL(table_op.init(tenant_id, &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_op.get_tenant_snap_item(
                     ObTenantSnapshotID(ObTenantSnapshotTableOperator::GLOBAL_STATE_ID),
                     true /*for update*/,
                     global_lock))) {
    LOG_WARN("fail to get global_lock", KR(ret), K(tenant_id));
  } else if (owner_job_id != global_lock.get_owner_job_id()) {
    is_conflicted_owner_job_id = true;
    LOG_INFO("job_id is different from owner_job_id of global lock", K(owner_job_id), K(global_lock));
  } else if (OB_FAIL(table_op.update_special_tenant_snap_item(
                     ObTenantSnapshotID(ObTenantSnapshotTableOperator::GLOBAL_STATE_ID),
                     old_status,                    /*old status*/
                     ObTenantSnapStatus::NORMAL,    /*new status*/
                     OB_INVALID_ID,                 /*owner job id*/
                     snapshot_scn))) {
    LOG_WARN("update special tenant snapshot item failed", KR(ret));
  }

  return ret;
}

//we can lock (change the status of GLOBAL_STATE) to prevent other tenant snapshot operations.
int ObTenantSnapshotUtil::lock_(ObMySQLTransaction &trans,
                                const uint64_t tenant_id,
                                const TenantSnapshotOp op,
                                const ObTenantSnapItem &special_item)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotTableOperator table_op;
  ObTenantSnapStatus new_status = ObTenantSnapStatus::MAX;
  share::SCN gts_scn = SCN::invalid_scn();
  int64_t new_create_time = OB_INVALID_TIMESTAMP;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !special_item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(special_item));
  } else {
    if (CREATE_OP == op || FORK_OP == op) {
      new_create_time = ObTimeUtility::current_time();
      if (CREATE_OP == op) {
        new_status = ObTenantSnapStatus::CREATING;
      } else { // FORK_OP == op
        new_status = ObTenantSnapStatus::CLONING;
      }
    } else if (RESTORE_OP == op) {
      new_status = ObTenantSnapStatus::CLONING;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tenant snapshot operation", KR(ret), K(tenant_id), K(op));
    }
  }

  if (FAILEDx(table_op.init(tenant_id, &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_op.update_special_tenant_snap_item(
                        special_item.get_tenant_snapshot_id(),
                        ObTenantSnapStatus::NORMAL,       /*old status*/
                        new_status,                       /*new status*/
                        special_item.get_owner_job_id(),  /*owner job id*/
                        gts_scn,                          /*new snapshot scn*/
                        new_create_time                   /*new create time*/
                        ))) {
    LOG_WARN("update special tenant snapshot item failed", KR(ret),
                                          K(special_item), K(new_status));
  }

  return ret;
}

int ObTenantSnapshotUtil::add_create_tenant_snapshot_task(ObMySQLTransaction &trans,
                                                          const uint64_t tenant_id,
                                                          const ObString &snapshot_name,
                                                          const ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  int64_t create_time = OB_INVALID_TIMESTAMP;
  ObTenantSnapshotTableOperator table_op;
  ObTenantSnapItem item;
  common::ObCurTraceId::TraceId trace_id;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || snapshot_name.empty()
                  || snapshot_name.length() > OB_MAX_TENANT_SNAPSHOT_NAME_LENGTH
                  || !tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_name), K(tenant_snapshot_id));
  } else if (OB_FAIL(check_and_get_data_version(tenant_id, data_version))) {
    LOG_WARN("fail to check and get data version or tenant is in upgrading procedure", KR(ret), K(tenant_id));
  } else if (FALSE_IT(create_time = ObTimeUtility::current_time())) {
  } else if (OB_FAIL(item.init(tenant_id,                          /*tenant_id*/
                               tenant_snapshot_id,                 /*snapshot_id*/
                               snapshot_name,                      /*snapshot_name*/
                               ObTenantSnapStatus::CREATING,       /*status*/
                               SCN::invalid_scn(),                 /*snapshot_scn*/
                               SCN::invalid_scn(),                 /*clog_start_scn*/
                               ObTenantSnapType::MANUAL,           /*type*/
                               create_time,                        /*create_time*/
                               data_version,                       /*data_version*/
                               OB_INVALID_ID                       /*owner_job_id*/))) {
    LOG_WARN("failed to init item", KR(ret), K(tenant_id), K(tenant_snapshot_id),
                                    K(snapshot_name), K(create_time), K(data_version));
  } else if (OB_FAIL(table_op.init(tenant_id, &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_op.insert_tenant_snap_item(item))) {
    LOG_WARN("failed to insert snapshot item", KR(ret), K(item));
  } else {
    ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
    if (nullptr != cur_trace_id) {
      trace_id = *cur_trace_id;
    } else {
      trace_id.init(GCONF.self_addr_);
    }
    ObTenantSnapJobItem job_item(tenant_id,
                                 tenant_snapshot_id,
                                 ObTenantSnapOperation::CREATE,
                                 trace_id);
    if (OB_FAIL(table_op.insert_tenant_snap_job_item(job_item))) {
      LOG_WARN("fail to insert tenant snapshot job", KR(ret), K(job_item));
    }
  }

  return ret;
}

int ObTenantSnapshotUtil::add_drop_tenant_snapshot_task(ObMySQLTransaction &trans,
                                                        const uint64_t tenant_id,
                                                        const ObString &snapshot_name)
{
  int ret = OB_SUCCESS;
  ObTenantSnapItem item;
  ObTenantSnapshotTableOperator table_op;
  common::ObCurTraceId::TraceId trace_id;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || snapshot_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_name));
  } else if (OB_FAIL(table_op.init(tenant_id, &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_op.get_tenant_snap_item(snapshot_name, true, item))) {
    LOG_WARN("failed to get tenant snap item", KR(ret), K(tenant_id), K(snapshot_name));
  } else if (ObTenantSnapStatus::NORMAL != item.get_status()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not allowed for current snapshot operation", KR(ret), K(tenant_id), K(item.get_status()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "there may be other operation on the same tenant snapshot, drop tenant snapshot");
  } else if (OB_FAIL(table_op.update_tenant_snap_item(snapshot_name,
                                                      ObTenantSnapStatus::NORMAL,
                                                      ObTenantSnapStatus::DELETING))) {
    LOG_WARN("update tenant snapshot status failed", KR(ret), K(tenant_id), K(snapshot_name));
  } else {
    ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
    if (nullptr != cur_trace_id) {
      trace_id = *cur_trace_id;
    } else {
      trace_id.init(GCONF.self_addr_);
    }
    ObTenantSnapJobItem job_item(tenant_id,
                                 item.get_tenant_snapshot_id(),
                                 ObTenantSnapOperation::DELETE,
                                 trace_id);
    if (OB_FAIL(table_op.insert_tenant_snap_job_item(job_item))) {
      LOG_WARN("fail to insert tenant snapshot job", KR(ret), K(job_item));
    }
  }

  return ret;
}

int ObTenantSnapshotUtil::get_tenant_snapshot_info(common::ObISQLClient &sql_client,
                                                   const uint64_t source_tenant_id,
                                                   const ObString &snapshot_name,
                                                   share::ObTenantSnapItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  ObTenantSnapshotTableOperator table_op;
  if (OB_UNLIKELY(!is_user_tenant(source_tenant_id) || snapshot_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(source_tenant_id), K(snapshot_name));
  } else if (OB_FAIL(table_op.init(source_tenant_id, &sql_client))) {
    LOG_WARN("failed to init table op", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(table_op.get_tenant_snap_item(snapshot_name, false, item))) {
    LOG_WARN("failed to get snapshot item", KR(ret), K(snapshot_name));
  }
  return ret;
}

int ObTenantSnapshotUtil::get_tenant_snapshot_info(common::ObISQLClient &sql_client,
                                                   const uint64_t source_tenant_id,
                                                   const ObTenantSnapshotID &snapshot_id,
                                                   share::ObTenantSnapItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  ObTenantSnapshotTableOperator table_op;
  if (OB_UNLIKELY(!is_user_tenant(source_tenant_id) || !snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(source_tenant_id), K(snapshot_id));
  } else if (OB_FAIL(table_op.init(source_tenant_id, &sql_client))) {
    LOG_WARN("failed to init table op", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(table_op.get_tenant_snap_item(snapshot_id, false, item))) {
    LOG_WARN("failed to get snapshot item", KR(ret), K(snapshot_id));
  }
  return ret;
}

int ObTenantSnapshotUtil::add_clone_tenant_task(ObMySQLTransaction &trans,
                                                const uint64_t tenant_id,
                                                const share::ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotTableOperator table_op;
  ObTenantSnapItem snap_item;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_snapshot_id));
  } else if (OB_FAIL(table_op.init(tenant_id, &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_op.get_tenant_snap_item(tenant_snapshot_id, true, snap_item))) {
    LOG_WARN("failed to get snapshot item", KR(ret), K(tenant_snapshot_id));
  } else if (ObTenantSnapStatus::NORMAL != snap_item.get_status()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not allowed for current snapshot operation", KR(ret), K(tenant_id), K(snap_item.get_status()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "there may be other operation on the same tenant snapshot, clone tenant");
  } else if (OB_FAIL(table_op.update_tenant_snap_item(tenant_snapshot_id,
                                                      ObTenantSnapStatus::NORMAL,   /*old_status*/
                                                      ObTenantSnapStatus::CLONING /*new_status*/))) {
    LOG_WARN("update tenant snapshot status failed", KR(ret), K(tenant_id), K(tenant_snapshot_id));
  }

  return ret;
}

int ObTenantSnapshotUtil::add_clone_tenant_task(ObMySQLTransaction &trans,
                                                const ObTenantSnapItem &snap_item)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotTableOperator table_op;

  if (!snap_item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snap_item));
  } else if (ObTenantSnapStatus::NORMAL != snap_item.get_status()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not allowed for current snapshot operation", KR(ret), K(snap_item));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "there may be other operation on the same tenant snapshot, clone tenant");
  } else if (OB_FAIL(table_op.init(snap_item.get_tenant_id(), &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(snap_item));
  } else if (OB_FAIL(table_op.update_tenant_snap_item(snap_item.get_tenant_snapshot_id(),
                                                      ObTenantSnapStatus::NORMAL,   /*old_status*/
                                                      ObTenantSnapStatus::CLONING /*new_status*/))) {
    LOG_WARN("update tenant snapshot status failed", KR(ret), K(snap_item));
  }

  return ret;
}

int ObTenantSnapshotUtil::generate_tenant_snapshot_name(const uint64_t tenant_id,
                                                        ObSqlString &tenant_snapshot_name,
                                                        bool is_inner)
{
  int ret = OB_SUCCESS;
  tenant_snapshot_name.reset();
  share::SCN gts_scn = SCN::invalid_scn();

  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id, GCONF.rpc_timeout, gts_scn))) {
    LOG_WARN("fail to get gts sync", KR(ret), K(tenant_id));
  } else if (!gts_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected gts_scn", KR(ret), K(tenant_id), K(gts_scn));
  } else if (OB_FAIL(tenant_snapshot_name.assign_fmt(is_inner ? "_inner_snapshot$%lu" :
                                                                "snapshot$%lu",
                                                     gts_scn.get_val_for_gts()
                                                     ))) {
    LOG_WARN("failed to assign_fmt", KR(ret), K(gts_scn));
  }

  return ret;
}

int ObTenantSnapshotUtil::generate_tenant_snapshot_id(const uint64_t tenant_id,
                                                      ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  tenant_snapshot_id.reset();
  share::SCN gts_scn = SCN::invalid_scn();

  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret));
  } else if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id, GCONF.rpc_timeout, gts_scn))) {
    LOG_WARN("fail to get gts sync", KR(ret), K(tenant_id));
  } else if (!gts_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected gts_scn", KR(ret), K(tenant_id), K(gts_scn));
  } else {
    tenant_snapshot_id = gts_scn.get_val_for_tx();
  }

  return ret;
}

int ObTenantSnapshotUtil::check_and_get_data_version(const uint64_t tenant_id,
                                                     uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  data_version = 0;
  uint64_t current_data_version = 0;
  uint64_t target_data_version = 0;
  uint64_t compatible_version = 0;
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::UPGRADE);
  ObAllTenantInfo all_tenant_info;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(GCTX.sql_proxy_), K(tenant_id));
  } else if (OB_FAIL(ObShareUtil::fetch_current_data_version(*GCTX.sql_proxy_, tenant_id, current_data_version))) {
    LOG_WARN("fail to get current data version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compatible_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
  } else if (current_data_version != compatible_version) {
    ret = OB_CONFLICT_WITH_CLONE;
    LOG_WARN("source tenant is in upgrading procedure, can not do clone", KR(ret), K(tenant_id),
             K(current_data_version), K(compatible_version));
    LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                         tenant_id, GCTX.sql_proxy_, false/*for_update*/, all_tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (all_tenant_info.is_standby()) {
    data_version = current_data_version;
  } else if (all_tenant_info.is_primary()) {
    if (OB_FAIL(check_tenant_is_in_upgrading_procedure_(tenant_id, target_data_version))) {
      LOG_WARN("fail to check or tenant is in upgrading procedure", KR(ret), K(tenant_id));
    } else {
      data_version = current_data_version;
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not clone tenant with source tenant neither primary nor standby",
             KR(ret), K(tenant_id), K(all_tenant_info));
  }
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_has_snapshot(common::ObISQLClient &sql_client,
                                                    const uint64_t tenant_id,
                                                    bool &has_snapshot)
{
  int ret = OB_SUCCESS;
  has_snapshot = false;
  ObTenantSnapshotTableOperator snap_op;
  ObArray<ObTenantSnapItem> items;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(snap_op.init(tenant_id, &sql_client))) {
    LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(snap_op.get_all_user_tenant_snap_items(items))) {
    LOG_WARN("failed to get all user snapshot items", KR(ret), K(tenant_id));
  } else if (!items.empty()) {
    has_snapshot = true;
  }
  return ret;
}

int ObTenantSnapshotUtil::notify_scheduler(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader_addr;
  obrpc::ObNotifyTenantSnapshotSchedulerArg arg;
  arg.set_tenant_id(tenant_id);
  obrpc::ObNotifyTenantSnapshotSchedulerResult res;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
              GCONF.cluster_id, gen_meta_tenant_id(tenant_id), ObLSID(ObLSID::SYS_LS_ID), leader_addr))) {
    LOG_WARN("failed to get leader address", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr).by(tenant_id).notify_tenant_snapshot_scheduler(arg, res))) {
    LOG_WARN("failed to notify tenant snapshot scheduler", KR(ret), K(leader_addr), K(arg));
  } else {
    int res_ret = res.get_result();
    if (OB_SUCCESS != res_ret) {
      ret = res_ret;
      LOG_WARN("the result of notify scheduler failed", KR(res_ret), K(leader_addr), K(arg));
    }
  }

  return ret;
}

int ObTenantSnapshotUtil::recycle_tenant_snapshot_ls_replicas(common::ObISQLClient &sql_client,
                                                              const uint64_t tenant_id,
                                                              const ObString &tenant_snapshot_name)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotTableOperator snap_op;
  ObTenantSnapItem item;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || tenant_snapshot_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_snapshot_name));
  } else if (OB_FAIL(snap_op.init(tenant_id, &sql_client))) {
    LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(snap_op.get_tenant_snap_item(tenant_snapshot_name, false /*need_lock*/, item))) {
    LOG_WARN("fail to get tenant snap item", KR(ret), K(tenant_id), K(tenant_snapshot_name));
  } else if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant snapshot item", KR(ret), K(item));
  } else if (OB_FAIL(snap_op.archive_tenant_snap_ls_replica_history(item.get_tenant_snapshot_id()))) {
    LOG_WARN("fail to archive tenant snap ls replica", KR(ret), K(item));
  } else if (OB_FAIL(snap_op.eliminate_tenant_snap_ls_replica_history())) {
    LOG_WARN("fail to eliminate tenant snap ls replica history", KR(ret));
  }

  return ret;
}

int ObTenantSnapshotUtil::recycle_tenant_snapshot_ls_replicas(common::ObISQLClient &sql_client,
                                                              const uint64_t tenant_id,
                                                              const share::ObTenantSnapshotID &tenant_snapshot_id)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotTableOperator snap_op;
  ObTenantSnapItem item;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !tenant_snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_snapshot_id));
  } else if (OB_FAIL(snap_op.init(tenant_id, &sql_client))) {
    LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(snap_op.archive_tenant_snap_ls_replica_history(tenant_snapshot_id))) {
    LOG_WARN("fail to archive tenant snap ls replica", KR(ret), K(item));
  } else if (OB_FAIL(snap_op.eliminate_tenant_snap_ls_replica_history())) {
    LOG_WARN("fail to eliminate tenant snap ls replica history", KR(ret));
  }

  return ret;
}

int ObTenantSnapshotUtil::check_tenant_in_cloning_procedure_in_trans_(
    const uint64_t tenant_id,
    bool &is_tenant_in_cloning)
{
  int ret = OB_SUCCESS;
  is_tenant_in_cloning = true;
  common::ObMySQLTransaction trans;
  int64_t meta_tenant_id = OB_INVALID_TENANT_ID;
  bool tenant_snapshot_table_exist = false;
  // is_tenant_in_cloning = false if one of conditions below is satisfied
  //   (1) tenant is not up to version 4.3, __all_tenant_snapshot not exists, clone is not supported
  //   (2) line with snapshot_id = 0 in __all_tenant_snapshot not exists
  //   (3) line with snapshot_id = 0 in __all_tenant_snapshot exists but is NORMAL status
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    is_tenant_in_cloning = false;
    LOG_TRACE("sys and meta tenant can not in cloning procedure", K(tenant_id));
  } else if (OB_FAIL(check_snapshot_table_exists_(tenant_id, tenant_snapshot_table_exist))) {
    LOG_WARN("fail to check __all_tenant_snapshot table exists", KR(ret), K(tenant_id));
  } else if (!tenant_snapshot_table_exist) {
    is_tenant_in_cloning = false;
    LOG_INFO("tenant snapshot table not exists, tenant is not cloning", K(tenant_id));
  } else {
    meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret));
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, meta_tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(tenant_id), K(meta_tenant_id));
    } else if (OB_FAIL(inner_check_tenant_in_cloning_procedure_in_trans_(
                           trans, tenant_id, is_tenant_in_cloning))) {
      LOG_WARN("fail to inner check tenant in cloning procedure", KR(ret), K(tenant_id));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::inner_check_tenant_in_cloning_procedure_in_trans_(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    bool &is_tenant_in_cloning)
{
  int ret = OB_SUCCESS;
  is_tenant_in_cloning = true;
  int64_t user_tenant_id = OB_INVALID_TENANT_ID;
  ObTenantSnapshotTableOperator tenant_snapshot_table_operator;
  ObTenantSnapItem tenant_snapshot_item;
  bool lock_line = true;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    user_tenant_id = gen_user_tenant_id(tenant_id);
    if (OB_FAIL(tenant_snapshot_table_operator.init(user_tenant_id, &trans))) {
      LOG_WARN("fail to init tenant snapshot table operator", KR(ret), K(tenant_id), K(user_tenant_id));
    // 1. lock __all_tenant_snapshot where snapshot_id = 0
    } else if (OB_FAIL(tenant_snapshot_table_operator.get_tenant_snap_item(
                      ObTenantSnapshotID(ObTenantSnapshotTableOperator::GLOBAL_STATE_ID),
                      lock_line,
                      tenant_snapshot_item))) {
      if (OB_TENANT_SNAPSHOT_NOT_EXIST == ret) {
        // good, there is no snapshot need creating for this tenant
        ret = OB_SUCCESS;
        is_tenant_in_cloning = false;
        LOG_TRACE("snapshot with GLOBAL_STATE_ID not exists, tenant is not cloning", K(tenant_id));
      } else {
        LOG_WARN("fail to get snapshot item from __all_tenant_snapshot", KR(ret), K(tenant_id));
      }
    } else if (ObTenantSnapStatus::NORMAL != tenant_snapshot_item.get_status()) {
      is_tenant_in_cloning = true;
      LOG_INFO("snapshot with GLOBAL_STATE_ID is not in NORMAL status, tenant is in cloning procedure",
               K(tenant_id), K(tenant_snapshot_item));
    } else {
      // good, there is no ls snapshot exists for this tenant
      is_tenant_in_cloning = false;
      LOG_TRACE("snapshot with GLOBAL_STATE_ID is in NORMAL status,"
               " tenant is not in cloning procedure", K(tenant_id), K(user_tenant_id),
               K(tenant_snapshot_item));
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::check_snapshot_table_exists_(
    const uint64_t user_tenant_id,
    bool &tenant_snapshot_table_exist)
{
  // we check whether __all_tenant_snapshot table exists
  // STEP 1: check data version
  //    if data_version already up to 4.3, table must exist
  // STEP 2: check schema
  //    if data_version below 4.3, we check schema next
  //    if table exists in schema, table must exist
  // STEP 3: check __all_table
  //    if schema not refreshed, check __all_table
  int ret = OB_SUCCESS;
  bool is_compatible_with_clone = false;
  tenant_snapshot_table_exist = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(user_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id));
  // STEP 1: check data version first
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_clone_tenant(user_tenant_id, is_compatible_with_clone))) {
    LOG_WARN("fail to check compat version for clone tenant", KR(ret), K(user_tenant_id));
  } else if (is_compatible_with_clone) {
    // good, data version up to 4.3
    tenant_snapshot_table_exist = true;
  } else {
    // STEP 2: check schema
    ObSchemaGetterGuard meta_schema_guard;
    const ObSimpleTableSchemaV2 *snapshot_table_schema = NULL;
    uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null");
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(meta_tenant_id, meta_schema_guard))) {
      LOG_WARN("get schema guard failed", KR(ret));
    } else if (OB_FAIL(meta_schema_guard.get_simple_table_schema(meta_tenant_id, OB_ALL_TENANT_SNAPSHOT_TID, snapshot_table_schema))) {
      LOG_WARN("fail to get snapshot table schema", KR(ret), K(user_tenant_id), K(meta_tenant_id));
    } else if (OB_ISNULL(snapshot_table_schema)) {
      tenant_snapshot_table_exist = false;
    } else {
      // good, table exists
      tenant_snapshot_table_exist = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (tenant_snapshot_table_exist) {
    // good, table exists
  } else {
    // STEP 3: check __all_table
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql("CloneTable");
      uint64_t exec_tenant_id = gen_meta_tenant_id(user_tenant_id);
      int64_t row_count = 0;
      if (OB_FAIL(sql.assign_fmt("SELECT count(*) as COUNT FROM %s "
                                 "WHERE tenant_id = 0 AND table_id = %ld",
                                 OB_ALL_TABLE_TNAME, OB_ALL_TENANT_SNAPSHOT_TID))) {
        LOG_WARN("failed to assign sql", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, GCONF.cluster_id, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql), K(user_tenant_id), K(exec_tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("empty result set", KR(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "COUNT", row_count, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to get int from result", KR(ret));
        } else if (row_count == 0) {
          tenant_snapshot_table_exist = false;
        } else {
          tenant_snapshot_table_exist = true;
        }
      }
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::cancel_existed_clone_job_if_need(
    const uint64_t tenant_id,
    const ObConflictCaseWithClone &case_to_check)
{
  int ret = OB_SUCCESS;
  bool clone_already_finish = false; // not used
  uint64_t meta_tenant_id = OB_INVALID_TENANT_ID;
  bool is_cloning = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
      || OB_UNLIKELY(!case_to_check.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(case_to_check));
  } else {
    common::ObMySQLTransaction trans;
    meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret));
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, meta_tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(tenant_id), K(meta_tenant_id));
    } else if (OB_FAIL(check_standby_tenant_not_in_cloning_procedure(trans, tenant_id, is_cloning))) {
      LOG_WARN("fail to check standby tenant whether in cloning procedure", KR(ret), K(tenant_id));
    } else if (is_cloning) {
      ObCancelCloneJobReason reason;
      if (OB_FAIL(reason.init_by_conflict_case(case_to_check))) {
        LOG_WARN("fail to inti cancel reason by conflict case", KR(ret), K(case_to_check));
      } else if (OB_FAIL(ObTenantCloneUtil::cancel_clone_job_by_source_tenant_id(
                             *GCTX.sql_proxy_, tenant_id, reason, clone_already_finish))) {
        LOG_WARN("fail to cancel clone job by source tenant id", KR(ret), K(tenant_id), K(reason));
      }
    } else {
      // This function will cancel existed clone job
      // If no clone job exists, there is no need to cancel, just do nothing
      LOG_TRACE("there is no clone job to cancel", K(tenant_id));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::check_standby_tenant_not_in_cloning_procedure(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    bool &is_cloning)
{
  int ret = OB_SUCCESS;
  is_cloning = false;
  bool is_compatible = false;
  int64_t check_begin_time = ObTimeUtility::current_time();
  LOG_TRACE("start to check whether standby tenant is in cloning procedure", K(tenant_id));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    is_cloning = false;
    LOG_TRACE("sys and meta tenant can not in cloning procedure", K(tenant_id));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_clone_standby_tenant(
                         tenant_id, is_compatible))) {
    LOG_WARN("fail to check compatible version for clone standby tenant", KR(ret), K(tenant_id));
  } else if (!is_compatible) {
    // tenant can not be in cloning procedure, do nothing
    is_cloning = false;
    LOG_TRACE("standby tenant not in 432 version, can not in clonine procedure", KR(ret), K(tenant_id));
  } else if (OB_FAIL(inner_check_tenant_in_cloning_procedure_in_trans_(
                         trans, tenant_id, is_cloning))) {
    LOG_WARN("fail to inner check tenant in cloning procedure", KR(ret), K(tenant_id));
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_TRACE("finish check whether standby tenant is in cloning procedure", KR(ret), K(tenant_id),
            K(is_cloning), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_not_in_cloning_procedure(
    const uint64_t tenant_id,
    const ObConflictCaseWithClone &case_to_check)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("start to check whether tenant is in cloning procedure", K(tenant_id), K(case_to_check));
  bool tenant_is_in_cloning_procedure = true;
  int64_t check_begin_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
      || OB_UNLIKELY(!case_to_check.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(case_to_check));
  } else if (OB_FAIL(check_tenant_in_cloning_procedure_in_trans_(
                         tenant_id,
                         tenant_is_in_cloning_procedure))) {
    LOG_WARN("fail to check tenant in cloning procedure in trans", KR(ret), K(tenant_id));
  } else if (tenant_is_in_cloning_procedure) {
    ret = OB_CONFLICT_WITH_CLONE;
    LOG_WARN("tenant is in cloning procedure, some operations is not allowed for now", KR(ret),
             K(tenant_id), K(tenant_is_in_cloning_procedure));
    LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, CLONE_PROCEDURE_STR, case_to_check.get_case_name_str());
  } else {
    // good, tenant not in cloning procedure
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_TRACE("finish check whether tenant is in cloning procedure", KR(ret), K(tenant_id),
            K(case_to_check), K(tenant_is_in_cloning_procedure), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_has_no_conflict_tasks(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t check_begin_time = ObTimeUtility::current_time();
  uint64_t data_version = 0;
  ObAllTenantInfo all_tenant_info;
  bool is_compatible_to_clone = false;
  LOG_INFO("begin to check whether tenant has conflict tasks", K(tenant_id));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("sys or meta tenant can not in clone procedure, clone not allowed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_clone_tenant_with_tenant_role(
                         tenant_id, is_compatible_to_clone))) {
    LOG_WARN("fail to check tenant compatible with clone tenant", KR(ret), K(tenant_id));
  } else if (!is_compatible_to_clone) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not clone tenant with tenant role not expected", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_is_in_dropping_procedure_(tenant_id))) {
    LOG_WARN("fail to check tenant in dropping procedure", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_is_in_modify_resource_pool_procedure_(tenant_id))) {
    LOG_WARN("fail to check tenant in modify resource pool  procedure", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_is_in_modify_unit_procedure_(tenant_id))) {
    LOG_WARN("fail to check tenant in modify unit procedure", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_is_in_modify_replica_procedure_(tenant_id))) {
    LOG_WARN("fail to check tenant in modify replica procedure", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_is_in_switchover_procedure_(tenant_id))) {
    LOG_WARN("fail to check tenant in switchover procedure", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, GCTX.sql_proxy_,
                     false/*for_update*/, all_tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (all_tenant_info.is_primary()) {
    if (OB_FAIL(check_tenant_is_in_upgrading_procedure_(tenant_id, data_version))) {
      LOG_WARN("fail to check tenant in upgrading procedure", KR(ret), K(tenant_id));
    } else if (OB_FAIL(check_tenant_is_in_transfer_procedure_(tenant_id))) {
      LOG_WARN("fail to check tenant in transfer procedure", KR(ret), K(tenant_id));
    } else if (OB_FAIL(check_tenant_is_in_modify_ls_procedure_(tenant_id))) {
      LOG_WARN("fail to check tenant in modify ls procedure", KR(ret), K(tenant_id));
    }
    //TODO@jingyu.cr: need to consider these cases:
    //   (1) conflict check for arbitration service status
  } else if (all_tenant_info.is_standby()) {
    if (OB_FAIL(check_standby_tenant_has_no_conflict_tasks_(tenant_id))) {
      LOG_WARN("fail to check conflict tasks for standby tenant", KR(ret), K(tenant_id));
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not clone tenant with source tenant neither primary nor standby",
             KR(ret), K(tenant_id), K(all_tenant_info));
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether tenant has conflict tasks", KR(ret),
           K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_standby_tenant_has_no_conflict_tasks_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObLSRecoveryStatOperator recovery_op;
  ObLSRecoveryStat ls_recovery_stat;
  ObMySQLTransaction trans;
  // for standby tenant, we try to make positive check for upgrade/transfer/ls-modify
  // upgrade operation will be double checked just before finish creating snapshot
  // in function ObTenantSnapshotScheduler::check_data_version_before_finish_snapshot_creation_
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (OB_FAIL(recovery_op.get_ls_recovery_stat(
                         tenant_id, SYS_LS, true/* for update */, ls_recovery_stat, trans))) {
    LOG_WARN("failed to get SYS ls recovery stat", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_standby_tenant_is_in_transfer_procedure_(tenant_id))) {
    LOG_WARN("fail to check standby tenant whether in transfer or modify ls procedure", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_standby_tenant_is_in_modify_ls_procedure_(tenant_id))) {
    LOG_WARN("fail to check standby tenant whether in modify ls procedure", KR(ret), K(tenant_id));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::check_standby_tenant_is_in_modify_ls_procedure_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t check_begin_time = ObTimeUtility::current_time();
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::STANDBY_MODIFY_LS);
  LOG_INFO("begin to check whether standby tenant is in modify_ls procedure", K(tenant_id));

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    share::ObLSStatusOperator ls_status_operator;
    common::ObArray<share::ObLSStatusInfo> ls_status_info_array;
    if (OB_FAIL(ls_status_operator.get_all_ls_status_by_order(tenant_id, ls_status_info_array, *GCTX.sql_proxy_))) {
      LOG_WARN("fail to get all ls status", KR(ret), K(tenant_id));
    } else if (ls_status_info_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_status_info_array has no member", KR(ret), K(ls_status_info_array));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_info_array.count(); ++i) {
        share::ObLSStatusInfo &ls_status_info = ls_status_info_array.at(i);
        if (!ls_status_info.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), K(ls_status_info));
        } else if (ls_status_info.ls_is_creating()
                   || ls_status_info.ls_is_created()
                   || ls_status_info.ls_is_create_abort()) {
          ret = OB_CONFLICT_WITH_CLONE;
          LOG_WARN("ls not in normal status, conflict with clone procedure", KR(ret), K(ls_status_info));
          LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
        } else {
          LOG_TRACE("ls status not conflict with clone procedure", KR(ret), K(ls_status_info));
        }
      }
    }
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether standby tenant is in modify ls procedure",
           KR(ret), K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_standby_tenant_is_in_transfer_procedure_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t check_begin_time = ObTimeUtility::current_time();
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::STANDBY_TRANSFER);
  ObBalanceTaskArray balance_tasks;
  ObArray<ObBalanceTaskHelper> ls_balance_tasks;
  SCN max_scn;
  max_scn.set_max();
  LOG_INFO("begin to check whether standby tenant is in transfer procedure", K(tenant_id));

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(ObBalanceTaskTableOperator::load_task(tenant_id, balance_tasks, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to load task from __all_balance_task", KR(ret), K(tenant_id));
  } else if (0 != balance_tasks.count()) {
    ret = OB_CONFLICT_WITH_CLONE;
    LOG_WARN("balance task is running, can not clone standby tenant", KR(ret), K(tenant_id), K(balance_tasks));
    LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
  } else if (OB_FAIL(ObBalanceTaskHelperTableOperator::load_tasks_order_by_scn(
                         tenant_id, *GCTX.sql_proxy_, max_scn, ls_balance_tasks))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // good, no transfer task exists
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to load task from __all_balance_task_helper", KR(ret), K(tenant_id));
    }
  } else {
    // load_tasks_order_by_scn return OB_SUCCESS means task array not empty
    ret = OB_CONFLICT_WITH_CLONE;
    LOG_WARN("balance task in task_helper is running, can not clone standby tenant",
             KR(ret), K(tenant_id), K(ls_balance_tasks));
    LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether standby tenant is in transfer procedure",
           KR(ret), K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_is_in_dropping_procedure_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t check_begin_time = ObTimeUtility::current_time();
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::DELAY_DROP_TENANT);
  schema::ObTenantStatus tenant_status = ObTenantStatus::TENANT_STATUS_MAX;
  LOG_INFO("begin to check whether tenant is dropping", K(tenant_id));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(inner_lock_line_for_all_tenant_table_(tenant_id, *GCTX.sql_proxy_, tenant_status))) {
    LOG_WARN("fail to lock line in __all_table table", KR(ret), K(tenant_id));
  } else if (!is_tenant_normal(tenant_status)) {
    ret = OB_CONFLICT_WITH_CLONE;
    LOG_WARN("source tenant is in dropping procedure, can not do clone",
             KR(ret), K(tenant_id), K(tenant_status));
    LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether tenant is dropping", KR(ret), K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_is_in_upgrading_procedure_(
    const uint64_t tenant_id,
    uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  data_version = 0;
  int64_t check_begin_time = ObTimeUtility::current_time();
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::UPGRADE);
  LOG_INFO("begin to check whether tenant is upgrading", K(tenant_id));
  common::ObMySQLTransaction trans;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else {
    ObGlobalStatProxy proxy(trans, tenant_id);
    uint64_t target_data_version = 0;
    uint64_t current_data_version = 0;
    if (OB_FAIL(proxy.get_target_data_version(true/*for_update*/, target_data_version))) {
      LOG_WARN("fail to get target data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(proxy.get_current_data_version(current_data_version))) {
      LOG_WARN("fail to get current data version", KR(ret), K(tenant_id));
    } else if (current_data_version != target_data_version) {
      ret = OB_CONFLICT_WITH_CLONE;
      LOG_WARN("source tenant is in upgrading procedure, can not do clone", KR(ret), K(tenant_id),
               K(current_data_version), K(target_data_version));
      LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
    } else {
      data_version = target_data_version;
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether tenant is upgrading", KR(ret), K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_is_in_transfer_procedure_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t check_begin_time = ObTimeUtility::current_time();
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::TRANSFER);
  LOG_INFO("begin to check whether tenant is transfer", K(tenant_id));
  common::ObMySQLTransaction trans;
  ObBalanceJob job;
  int64_t start_time = OB_INVALID_TIMESTAMP;  // not used
  int64_t finish_time = OB_INVALID_TIMESTAMP; // not used
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObInnerTableLockUtil::lock_inner_table_in_trans(
                         trans,
                         tenant_id,
                         OB_ALL_BALANCE_JOB_TID,
                         EXCLUSIVE, false/*is_from_sql*/))) {
    LOG_WARN("lock inner table failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(
                         tenant_id,
                         false/*for_update*/,
                         trans,
                         job,
                         start_time,
                         finish_time))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // good, no balance job exists
    } else {
      LOG_WARN("failed to get balance job", KR(ret), K(tenant_id));
    }
  } else {
    // job exists, tenant is doing transfer, clone tenant related operations not allowed
    // TODO@jingyu.cr: we can control more precisely by checking __all_balance_task,
    //                 some operations like ls alter is allowed
    ret = OB_CONFLICT_WITH_CLONE;
    LOG_WARN("source tenant is in balance procedure, can not do clone", KR(ret), K(tenant_id), K(job));
    LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether tenant is transfer", KR(ret), K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_is_in_modify_resource_pool_procedure_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t check_begin_time = ObTimeUtility::current_time();
  LOG_INFO("begin to check whether tenant is transfer", K(tenant_id));
  ObSqlString sql("FetchPool");
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id = %lu FOR UPDATE",
                                   OB_ALL_RESOURCE_POOL_TNAME, tenant_id))) {
    LOG_WARN("append_fmt failed", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      common::sqlclient::ObMySQLResult *res = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(result, GCONF.cluster_id, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr());
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret));
      } else if (OB_FAIL(res->next())) {
        if (OB_ITER_END == ret) {
          // rewrite error code
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("expect at least one row in __all_resource_pool", KR(ret), K(sql));
        } else {
          LOG_WARN("iterate next result fail", KR(ret), K(sql));
        }
      } else {
        // good, we can acquire lock on __all_resource_pool
        // can continue clone related procedure
      }
    }
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether tenant is modify resource pool", KR(ret), K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_is_in_modify_ls_procedure_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  int64_t check_begin_time = ObTimeUtility::current_time();
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::MODIFY_LS);
  LOG_INFO("begin to check whether tenant is modifing ls", K(tenant_id));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else {
    ObLSAttrOperator ls_table_operator(tenant_id, GCTX.sql_proxy_);
    ObLSAttr sys_ls_attr;
    ObArray<share::ObLSAttr> ls_array;
    // 1. lock sys ls in __all_ls under user tenant
    if (OB_FAIL(ls_table_operator.get_ls_attr(SYS_LS, true/*for_update*/, trans, sys_ls_attr))) {
      LOG_WARN("failed to load sys ls status", KR(ret));
    } else if (OB_FAIL(ls_table_operator.get_all_ls_by_order(ls_array))) {
      LOG_WARN("failed to get all_ls by order", KR(ret));
    } else if (ls_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_array has no member", KR(ret), K(ls_array));
    } else {
    // TODO@jingyu.cr: make sure whether change ls status from creating to create_abort
    // 2. let ls info is consistent in__all_ls and __all_ls_status
    // 3. adjust ls status in __all_ls_status
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_array.count(); ++i) {
        const share::ObLSAttr &ls_info = ls_array.at(i);
        if (!ls_info.ls_is_normal()) {
          ret = OB_CONFLICT_WITH_CLONE;
          LOG_WARN("source tenant has ls not in normal status, can not do clone", KR(ret), K(tenant_id), K(ls_info));
          LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
        }
      }
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether tenant is modify ls", KR(ret), K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_is_in_modify_replica_procedure_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  const int64_t timeout = GCONF.internal_sql_execute_timeout;
  observer::ObInnerSQLConnection *conn = NULL;
  ObSqlString sql("FetchDRTask");
  int64_t task_cnt = 0;
  int64_t check_begin_time = ObTimeUtility::current_time();
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::MODIFY_REPLICA);
  LOG_INFO("begin to check whether tenant is modifing ls", K(tenant_id));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(gen_meta_tenant_id(tenant_id),
                                                           OB_ALL_LS_REPLICA_TASK_TID,
                                                           EXCLUSIVE,
                                                           timeout,
                                                           conn))) {
    LOG_WARN("lock dest table failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.append_fmt("SELECT count(*) as task_count FROM %s",
                                   OB_ALL_LS_REPLICA_TASK_TNAME))) {
    LOG_WARN("append_fmt failed", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(trans.read(res, GCONF.cluster_id, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr());
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("result next failed", K(ret), K(tenant_id));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "task_count", task_cnt, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to extract int from result", KR(ret));
        } else if (0 < task_cnt) {
          ret = OB_CONFLICT_WITH_CLONE;
          LOG_WARN("replica task is running now, can not create snapshot or clone tenant", KR(ret), K(tenant_id));
          LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
        }
      }
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether tenant is modify replica", KR(ret), K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_is_in_switchover_procedure_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  ObAllTenantInfo tenant_info;
  int64_t check_begin_time = ObTimeUtility::current_time();
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::MODIFY_TENANT_ROLE_OR_SWITCHOVER_STATUS);
  LOG_INFO("begin to check whether tenant is switchover", K(tenant_id));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                         tenant_id,
                         &trans,
                         true/*for_update*/,
                         tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (!tenant_info.is_normal_status()) {
    ret = OB_CONFLICT_WITH_CLONE;
    LOG_WARN("tenant is not NORMAL, create snapshot or clone tenant not allowed",
             KR(ret), K(tenant_id), K(tenant_info));
    LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
  } else if (!tenant_info.is_standby() && !tenant_info.is_primary()) {
    ret = OB_CONFLICT_WITH_CLONE;
    LOG_WARN("tenant is not PRIMARY or STANDBY, create snapshot or clone tenant not allowed",
             KR(ret), K(tenant_id), K(tenant_info));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether tenant is switchover", KR(ret), K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_tenant_is_in_modify_unit_procedure_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  ObSqlString sql("FetchUnit");
  int64_t check_begin_time = ObTimeUtility::current_time();
  LOG_INFO("begin to check whether tenant is modifing unit", K(tenant_id));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE resource_pool_id in "
                                   "(SELECT resource_pool_id FROM %s WHERE tenant_id = %lu) FOR UPDATE",
                                   OB_ALL_UNIT_TNAME, OB_ALL_RESOURCE_POOL_TNAME, tenant_id))) {
    LOG_WARN("append_fmt failed", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(trans.read(result, GCONF.cluster_id, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed");
      } else if (OB_FAIL(check_unit_infos_(*result.get_result(), tenant_id))) {
        LOG_WARN("construct log stream info failed", KR(ret), K(tenant_id));
      }
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  LOG_INFO("finish check whether tenant is modifing unit", KR(ret), K(tenant_id), K(check_begin_time), K(cost));
  return ret;
}

int ObTenantSnapshotUtil::check_unit_infos_(
    common::sqlclient::ObMySQLResult &res,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObUnit unit_info;
  int64_t unit_count = 0;
  ObConflictCaseWithClone case_to_check(rootserver::ObConflictCaseWithClone::MODIFY_UNIT);
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(res.next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next result failed", KR(ret));
        }
        break;
      }
      unit_info.reset();
      if (OB_FAIL(ObUnitTableOperator::read_unit(res, unit_info))) {
        LOG_WARN("fail to construct unit info", KR(ret));
      } else if (!unit_info.is_active_status()) {
        ret = OB_CONFLICT_WITH_CLONE;
        LOG_WARN("unit is not active, can not do clone tenant related operations", KR(ret), K(unit_info));
        LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
      } else if (unit_info.migrate_from_server_.is_valid()) {
        ret = OB_CONFLICT_WITH_CLONE;
        LOG_WARN("unit is migrating, can not do clone tenant related operations", KR(ret), K(unit_info));
        LOG_USER_ERROR(OB_CONFLICT_WITH_CLONE, tenant_id, case_to_check.get_case_name_str(), CLONE_PROCEDURE_STR);
      } else {
        unit_count++;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (0 >= unit_count) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("no valid unit exists", KR(ret));
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::lock_unit_for_tenant(
    common::ObISQLClient &client,
    const ObUnit &unit,
    uint64_t &tenant_id_to_lock)
{
  int ret = OB_SUCCESS;
  tenant_id_to_lock = OB_INVALID_TENANT_ID;
  ObMySQLTransaction trans;

  if (OB_UNLIKELY(!unit.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      const uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
      ObSqlString lock_sql("UnitTableLoc");
      if (OB_FAIL(lock_sql.assign_fmt("SELECT * FROM %s WHERE unit_id = %lu FOR UPDATE",
                                      OB_ALL_UNIT_TNAME, unit.unit_id_))) {
        LOG_WARN("fail to construct lock sql", KR(ret), K(unit));
      } else if (OB_FAIL(trans.read(res, exec_tenant_id, lock_sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(exec_tenant_id), K(lock_sql));
      } else {
        ObUnitTableOperator unit_table_operator;
        ObArray<uint64_t> input_pool_ids;
        ObArray<share::ObResourcePool> input_pools;
        if (OB_FAIL(unit_table_operator.init(*GCTX.sql_proxy_))) {
          LOG_WARN("failed to init unit operator", KR(ret));
        } else if (OB_FAIL(input_pool_ids.push_back(unit.resource_pool_id_))) {
          LOG_WARN("fail to add resource pool id into array", KR(ret), K(unit));
        } else if (OB_FAIL(unit_table_operator.get_resource_pools(input_pool_ids, input_pools))) {
          LOG_WARN("fail to get resource pool by input pool id", KR(ret), K(input_pool_ids));
        } else if (OB_UNLIKELY(1 < input_pools.count())) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("expect single resource pool", KR(ret), K(unit), K(input_pool_ids), K(input_pools));
        } else {
          tenant_id_to_lock = input_pools.at(0).tenant_id_;
        }
      }
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::lock_resource_pool_for_tenant(
    common::ObISQLClient &client,
    const share::ObResourcePool &resource_pool)
{
  int ret = OB_SUCCESS;
  ObSqlString lock_sql("PoolLock");
  if (OB_UNLIKELY(!resource_pool.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(resource_pool));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(resource_pool.tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("resource pool tenant id is invalid", KR(ret), K(resource_pool));
  } else if (is_sys_tenant(resource_pool.tenant_id_) || is_meta_tenant(resource_pool.tenant_id_)) {
    // do nothing
  } else if (OB_FAIL(lock_sql.assign_fmt("SELECT * FROM %s WHERE resource_pool_id = %lu FOR UPDATE",
                                         OB_ALL_RESOURCE_POOL_TNAME, resource_pool.resource_pool_id_))) {
    LOG_WARN("fail to construct lock sql", KR(ret), K(resource_pool));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      const uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
      if (OB_FAIL(client.read(res, exec_tenant_id, lock_sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(exec_tenant_id), K(lock_sql));
      }
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::lock_status_for_tenant(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id_to_check)
{
  int ret = OB_SUCCESS;
  schema::ObTenantStatus tenant_status = ObTenantStatus::TENANT_STATUS_MAX; // not used
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_to_check))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id_to_check));
  } else if (!is_user_tenant(tenant_id_to_check)) {
    // do nothing
  } else if (OB_FAIL(inner_lock_line_for_all_tenant_table_(tenant_id_to_check, trans, tenant_status))) {
    LOG_WARN("fail to lock line in __all_table table", KR(ret), K(tenant_id_to_check));
  }
  return ret;
}

int ObTenantSnapshotUtil::inner_lock_line_for_all_tenant_table_(
    const uint64_t tenant_id_to_lock,
    ObISQLClient &sql_proxy,
    share::schema::ObTenantStatus &tenant_status)
{
  int ret = OB_SUCCESS;
  tenant_status = ObTenantStatus::TENANT_STATUS_MAX;
  ObSqlString sql("TenantLock");
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_to_lock))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id_to_lock));
  } else if (OB_FAIL(sql.append_fmt("SELECT status FROM %s WHERE tenant_id = %lu FOR UPDATE",
                                   OB_ALL_TENANT_TNAME, tenant_id_to_lock))) {
    LOG_WARN("append_fmt failed", KR(ret), K(tenant_id_to_lock));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      common::sqlclient::ObMySQLResult *res = NULL;
      if (OB_FAIL(sql_proxy.read(result, GCONF.cluster_id, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr());
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret));
      } else if (OB_FAIL(res->next())) {
        if (OB_ITER_END == ret) {
          // rewrite error code
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("expect at least one row in __all_tenant", KR(ret), K(sql));
        } else {
          LOG_WARN("iterate next result fail", KR(ret), K(sql));
        }
      } else {
        ObString status_str;
        EXTRACT_VARCHAR_FIELD_MYSQL(*res, "status", status_str);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(schema::get_tenant_status(status_str, tenant_status))) {
            LOG_WARN("get tenant status failed", KR(ret), K(status_str));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantSnapshotUtil::check_current_and_target_data_version(
    const uint64_t tenant_id,
    uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  data_version = 0;
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::UPGRADE);
  uint64_t data_version_in_tenant_param_table = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObGlobalStatProxy proxy(*GCTX.sql_proxy_, tenant_id);
    uint64_t target_data_version = 0;
    uint64_t current_data_version = 0;
    if (OB_FAIL(proxy.get_target_data_version(false/*for_update*/, target_data_version))) {
      LOG_WARN("fail to get target data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(proxy.get_current_data_version(current_data_version))) {
      LOG_WARN("fail to get current data version", KR(ret), K(tenant_id));
    } else if (target_data_version != current_data_version) {
      ret = OB_CONFLICT_WITH_CLONE;
      LOG_WARN("source tenant is in upgrading procedure, can not do clone", KR(ret),
               K(tenant_id), K(current_data_version), K(target_data_version));
    } else if (OB_FAIL(ObShareUtil::fetch_current_data_version(
                         *GCTX.sql_proxy_, tenant_id, data_version_in_tenant_param_table))) {
      LOG_WARN("fail to fetch current data version from tenant parameter table", KR(ret), K(tenant_id));
    } else if (data_version_in_tenant_param_table != current_data_version) {
      ret = OB_CONFLICT_WITH_CLONE;
      LOG_WARN("source tenant is in upgrading procedure, can not do clone", KR(ret),
               K(tenant_id), K(current_data_version), K(data_version_in_tenant_param_table));
    } else {
      data_version = current_data_version;
    }
  }
  return ret;
}
