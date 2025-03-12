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

#define USING_LOG_PREFIX STANDBY
#include "ob_tenant_role_transition_service.h"
#include "logservice/ob_log_service.h"
#include "rootserver/ob_cluster_event.h"// CLUSTER_EVENT_ADD_CONTROL
#include "rootserver/ob_tenant_event_def.h" // TENANT_EVENT
#include "rootserver/ob_ls_service_helper.h" // ObLSServiceHelper
#include "rootserver/ob_empty_server_checker.h" // ObEmptyServerChecker
#include "share/ob_global_stat_proxy.h"//ObGlobalStatProxy
#include "storage/tx/ob_timestamp_service.h"  // ObTimestampService
#include "rootserver/standby/ob_standby_service.h" // ObStandbyService
#include "share/balance/ob_balance_task_helper_operator.h"//ObBalanceTaskHelper
#include "share/oracle_errno.h"//oracle error code
#include "rootserver/ob_service_name_command.h"

using namespace oceanbase::common::sqlclient;

namespace oceanbase
{
using namespace share;
using namespace palf;
using namespace common;
using namespace tenant_event;

namespace rootserver
{
/*
  The macro's usage scenario: It is used in the process of switchover to primary,
  to translate the connection information and certain error checking messages
  from the source primary tenant into USER ERROR,
  facilitating the troubleshooting of cross-tenant connection issues.
*/
#define SOURCE_TENANT_CHECK_USER_ERROR_FOR_SWITCHOVER_TO_PRIMARY                                                                          \
  int tmp_ret = OB_SUCCESS;                                                                                                               \
  ObSqlString str;                                                                                                                        \
  switch (ret) {                                                                                                                          \
    case -ER_TABLEACCESS_DENIED_ERROR:                                                                                                    \
    case OB_ERR_NO_TABLE_PRIVILEGE:                                                                                                       \
    case -OER_TABLE_OR_VIEW_NOT_EXIST:                                                                                                    \
    case -ER_DBACCESS_DENIED_ERROR:                                                                                                       \
    case OB_ERR_NO_DB_PRIVILEGE:                                                                                                          \
    case OB_ERR_NULL_VALUE:                                                                                                               \
    case OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH:                                                                                               \
    case -ER_CONNECT_FAILED:                                                                                                              \
    case OB_PASSWORD_WRONG:                                                                                                               \
    case -ER_ACCESS_DENIED_ERROR:                                                                                                         \
    case OB_ERR_NO_LOGIN_PRIVILEGE:                                                                                                       \
    case -OER_INTERNAL_ERROR_CODE:                                                                                                        \
      if (OB_TMP_FAIL(str.assign_fmt("query primary failed(original error code: %d), switchover to primary is", ret))) {                         \
        LOG_WARN("tenant role trans user error str assign failed");                                                                       \
      } else {                                                                                                                            \
        ret = OB_OP_NOT_ALLOW;                                                                                                            \
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, str.ptr());                                                                                       \
      }                                                                                                                                   \
      break;                                                                                                                               \
    case -ER_ACCOUNT_HAS_BEEN_LOCKED:                                                                                                      \
      ret = OB_OP_NOT_ALLOW;                                                                                                              \
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "primary tenant's user account is locked, switchover to primary is");                                                       \
      break;                                                                                                                     \
    case OB_ERR_TENANT_IS_LOCKED:                                                                                                         \
      ret = OB_OP_NOT_ALLOW;                                                                                                              \
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "primary tenant is locked, switchover to primary is");                                                       \
      break;                                                                                                                              \
    case OB_SOURCE_LS_STATE_NOT_MATCH:                                                                                                    \
      LOG_USER_ERROR(OB_SOURCE_LS_STATE_NOT_MATCH);                                                                                       \
      break;                                                                                                                              \
  }


const char* const ObTenantRoleTransitionConstants::SWITCH_TO_PRIMARY_LOG_MOD_STR = "SWITCH_TO_PRIMARY";
const char* const ObTenantRoleTransitionConstants::SWITCH_TO_STANDBY_LOG_MOD_STR = "SWITCH_TO_STANDBY";
const char* const ObTenantRoleTransitionConstants::RESTORE_TO_STANDBY_LOG_MOD_STR = "RESTORE_TO_STANDBY";

///////////ObTenantRoleTransCostDetail/////////////////
const char* ObTenantRoleTransCostDetail::type_to_str(CostType type) const
{
  static const char *strs[] = { "WAIT_LOG_SYNC", "WAIT_BALANCE_TASK", "FLASHBACK_LOG",
      "WAIT_LOG_END", "CHANGE_ACCESS_MODE" };
  STATIC_ASSERT(MAX_COST_TYPE == ARRAYSIZEOF(strs), "status string array size mismatch");
  const char* str = "UNKNOWN";
  if (type < 0 || type >= MAX_COST_TYPE) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid type", K(type));
  } else {
    str = strs[type];
  }
  return str;
}
void ObTenantRoleTransCostDetail::add_cost(CostType type, int64_t cost)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type < 0 || type >= MAX_COST_TYPE || cost < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(type), K(cost));
  } else {
    cost_type_[type] = cost;
  }
}
int64_t ObTenantRoleTransCostDetail::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t counted_cost = 0;
  for (int i = 0 ; i < MAX_COST_TYPE; i++) {
    if (cost_type_[i] > 0) {
      counted_cost += cost_type_[i];
      CostType type = static_cast<CostType>(i);
      BUF_PRINTF("%s: %ld, ", type_to_str(type), cost_type_[i]);
    }
  }
  BUF_PRINTF("OTHERS: %ld", end_ - start_ - counted_cost);
  return pos;
}

///////////ObTenantRoleTransAllLSInfo/////////////////
int ObTenantRoleTransAllLSInfo::init()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ObLSStatus::OB_LS_MAX_STATUS; i++) {
    all_ls_[i].reset();
  }
  return ret;
}
int ObTenantRoleTransAllLSInfo::add_ls(const ObLSID &ls_id, const ObLSStatus status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || status < 0 || status >= ObLSStatus::OB_LS_MAX_STATUS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(status));
  } else if (OB_FAIL(all_ls_[status].push_back(ls_id))) {
    LOG_WARN("fail to push back", KR(ret), K(ls_id));
  }
  return ret;
}
int64_t ObTenantRoleTransAllLSInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  bool meet_first = false;
  for (int64_t i = 0; i < ObLSStatus::OB_LS_MAX_STATUS; i++) {
    ObLSStatus status = static_cast<ObLSStatus>(i);
    if (all_ls_[i].count() > 0) {
      if (!meet_first) {
        BUF_PRINTF("%s: ", ls_status_to_str(status));
        meet_first = true;
      } else {
        BUF_PRINTF("; %s: ", ls_status_to_str(status));
      }
      int64_t arr_end = all_ls_[i].count() - 1;
      for (int64_t j=0; j < arr_end; j++) {
        BUF_PRINTF("%ld, ", all_ls_[i].at(j).id());
      }
      BUF_PRINTF("%ld", all_ls_[i].at(arr_end).id());
    }
  }
  if (!is_valid()) BUF_PRINTF("NULL");
  return pos;
}

bool ObTenantRoleTransAllLSInfo::is_valid() const {
  for (int64_t i = 0; i < ObLSStatus::OB_LS_MAX_STATUS; ++i) {
    if (all_ls_[i].count() > 0) {
      return true;
    }
  }
  return false;
}

////////////ObTenantRoleTransNonSyncInfo//////////////
int ObTenantRoleTransNonSyncInfo::init(const ObArray<obrpc::ObCheckpoint> &switchover_checkpoints)
{
  int ret = OB_SUCCESS;
  not_sync_checkpoints_.reset();
  is_sync_ = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < switchover_checkpoints.count(); i++) {
    const obrpc::ObCheckpoint &checkpoint = switchover_checkpoints.at(i);
    if (!checkpoint.is_sync_to_latest()) {
      is_sync_ = false;
      LOG_WARN("ls not sync, keep waiting", KR(ret), K(checkpoint));
      if (OB_FAIL(not_sync_checkpoints_.push_back(checkpoint))) {
        LOG_WARN("fail to push back", KR(ret), K(checkpoint), K(not_sync_checkpoints_));
      }
    }
  }
  return ret;
}

int64_t ObTenantRoleTransNonSyncInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t ls_num = not_sync_checkpoints_.count();
  BUF_PRINTF("NON_SYNC_LS_CNT: %ld; TOP_%ld: ", ls_num, MAX_PRINT_LS_NUM);
  if(ls_num > 0) {
    int64_t arr_end = MAX_PRINT_LS_NUM > ls_num ? ls_num - 1 : MAX_PRINT_LS_NUM - 1;
    for (int64_t i = 0; i < arr_end; i++) {
      const obrpc::ObCheckpoint &checkpoint = not_sync_checkpoints_.at(i);
      BUF_PRINTO(checkpoint);
      J_COMMA();
    }
    J_OBJ(not_sync_checkpoints_.at(arr_end));
  }
  return pos;
}

////////////ObTenantRoleTransitionService//////////////

int ObTenantRoleTransitionService::init(
      uint64_t tenant_id,
      const obrpc::ObSwitchTenantArg::OpType &switch_optype,
      const bool is_verify,
      common::ObMySQLProxy *sql_proxy,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      ObTenantRoleTransCostDetail *cost_detail,
      ObTenantRoleTransAllLSInfo *all_ls_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy)
      || OB_ISNULL(rpc_proxy)
      || OB_ISNULL(cost_detail)
      || OB_ISNULL(all_ls_info)
      || OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy), KP(rpc_proxy), K(tenant_id),
        K(switch_optype), KP(cost_detail), KP(all_ls_info));
  } else {
    sql_proxy_= sql_proxy;
    rpc_proxy_ = rpc_proxy;
    tenant_id_ = tenant_id;
    switch_optype_ = switch_optype;
    switchover_epoch_ = OB_INVALID_VERSION;
    so_scn_.set_min();
    cost_detail_ = cost_detail;
    all_ls_info_ = all_ls_info;
    has_restore_source_ = false;
    is_verify_ = is_verify;
  }
  return ret;
}
int ObTenantRoleTransitionService::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(rpc_proxy_) || OB_ISNULL(cost_detail_) || OB_ISNULL(all_ls_info_)
      || OB_UNLIKELY(!is_user_tenant(tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_),
        KP(cost_detail_), KP(all_ls_info_));
  }
  return ret;
}
int ObTenantRoleTransitionService::failover_to_primary()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[ROLE_TRANSITION] start to failover to primary", KR(ret), K(is_verify_), K(tenant_id_));
  const int64_t start_service_time = ObTimeUtility::current_time();
  ObAllTenantInfo tenant_info;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_FAIL(check_tenant_server_online_())) {
    LOG_WARN("fail to check whether the tenant's servers are online", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_,
                                                    false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
  } else if (tenant_info.is_primary()) {
    LOG_INFO("is primary tenant, no need failover");
  } else if (OB_UNLIKELY(!tenant_info.get_recovery_until_scn().is_valid_and_not_min())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("invalid recovery_until_scn", KR(ret), K(tenant_info));
    TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW, "recovery_until_scn is invalid", switch_optype_);
  } else if (FALSE_IT(switchover_epoch_ = tenant_info.get_switchover_epoch())) {
  } else if (tenant_info.is_normal_status()) {
    if (OB_FAIL(do_failover_to_primary_(tenant_info))) {
      LOG_WARN("fail to do failover to primary", KR(ret), K(tenant_info));
    }
  } else if (tenant_info.is_prepare_flashback_for_failover_to_primary_status()
        || tenant_info.is_prepare_flashback_for_switch_to_primary_status()) {
    //prepare flashback
    if (!is_verify_ && OB_FAIL(do_prepare_flashback_(tenant_info))) {
      LOG_WARN("fail to prepare flashback", KR(ret), K(tenant_info));
    }
  } else if (tenant_info.is_flashback_status()) {
    if (!is_verify_ && OB_FAIL(do_flashback_())) {
      LOG_WARN("fail to flashback", KR(ret), K(tenant_info));
    }
  } else if (tenant_info.is_switching_to_primary_status()) {
    if (!is_verify_ && OB_FAIL(do_switch_access_mode_to_append(tenant_info, share::PRIMARY_TENANT_ROLE))) {
      LOG_WARN("fail to switch access mode", KR(ret), K(tenant_info));
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switchover status not match", KR(ret), K(is_verify_), K(tenant_info), K_(tenant_id));
    TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW, "switchover status not match", switch_optype_);
  }

  if (OB_FAIL(ret)) {
  } else if (!is_verify_) {
    (void)broadcast_tenant_info(ObTenantRoleTransitionConstants::SWITCH_TO_PRIMARY_LOG_MOD_STR);
    ObBroadcastSchemaArg arg;
    arg.tenant_id_ = tenant_id_;
    if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy is null", KR(ret), KP(GCTX.rs_mgr_), KP(GCTX.rs_rpc_proxy_));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to_rs(*GCTX.rs_mgr_).broadcast_schema(arg))) {
      LOG_WARN("failed to broadcast schema", KR(ret), K(arg));
    }
  }

  const int64_t cost = ObTimeUtility::current_time() - start_service_time;
  LOG_INFO("[ROLE_TRANSITION] finish failover to primary", KR(ret), K(tenant_info), K(is_verify_), K(cost));
  return ret;
}
ERRSIM_POINT_DEF(ERRSIM_TENANT_ROLE_TRANS_WAIT_SYNC_ERROR);
int ObTenantRoleTransitionService::do_failover_to_primary_(const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo new_tenant_info;
  (void) new_tenant_info.assign(tenant_info);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY != switch_optype_
                         && obrpc::ObSwitchTenantArg::OpType::FAILOVER_TO_PRIMARY != switch_optype_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected switch tenant action", KR(ret),
             K_(switch_optype), K(tenant_info), K_(tenant_id));
  } else if (OB_UNLIKELY(!(tenant_info.is_normal_status())
      || tenant_info.is_primary()
      || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K(switchover_epoch_));
  } else if (obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY == switch_optype_
      && OB_FAIL(wait_sys_ls_sync_to_latest_until_timeout_(tenant_id_, new_tenant_info))) {
    LOG_WARN("fail to execute wait_sys_ls_sync_to_latest_until_timeout_", KR(ret), K_(tenant_id), K(new_tenant_info));
    SOURCE_TENANT_CHECK_USER_ERROR_FOR_SWITCHOVER_TO_PRIMARY;
  }
  /*The switchover to primary verify command ends here.
    This command cannot update the switchover status nor execute the further logic.
    We update the switchover status right after sys ls being synced, The reason is as follows:
        The tenant fetches log with reference to tenant_sync_scn + 3s.
        If two ls' sync_scn have an extremely large difference,
        e.g. tenant_sync_scn = ls_1001 sync_scn + 3s << ls_1002 sync_scn,
        there is a possibility that ls_1002's log cannot be fetched completely.
    To ensure all ls' log are fetched completely, we update the switchover status as PREPARE_xxx.
    Then the tenant fetching log will no longer utilize tenant_sync_scn + 3s as a reference point.
  **/
  if (OB_FAIL(ret) || is_verify_) {
  } else if (OB_FAIL(clear_service_name_())) {
    LOG_WARN("fail to execute clear_service_name", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(
      tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
      share::STANDBY_TENANT_ROLE, tenant_info.get_switchover_status(),
      obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY == switch_optype_ ?
      share::PREPARE_FLASHBACK_FOR_SWITCH_TO_PRIMARY_SWITCHOVER_STATUS :
      share::PREPARE_FLASHBACK_FOR_FAILOVER_TO_PRIMARY_SWITCHOVER_STATUS,
      switchover_epoch_))) {
    LOG_WARN("failed to update tenant role", KR(ret), K(tenant_id_), K(tenant_info));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_,
      false, new_tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(new_tenant_info.get_switchover_epoch() != switchover_epoch_)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover is concurrency", KR(ret), K(switchover_epoch_), K(new_tenant_info));
  } else if (OB_FAIL(do_prepare_flashback_(new_tenant_info))) {
    LOG_WARN("failed to prepare flashback", KR(ret), K(new_tenant_info));
  }
  return ret;
}

// operation
// 1. create abort all ls
// 2. take all ls to flashback
// 3. get all ls max sync point
int ObTenantRoleTransitionService::do_prepare_flashback_(share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_PREPARE_FLASHBACK);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!(tenant_info.is_prepare_flashback_for_failover_to_primary_status()
                           || tenant_info.is_prepare_flashback_for_switch_to_primary_status()))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switchover status not match, switch tenant is not allowed", KR(ret), K(tenant_info));
    TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW, "switchover status not match", switch_optype_);
  } else if (OB_UNLIKELY(switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K(switchover_epoch_));
  } else if (obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY == switch_optype_) {
    if (OB_FAIL(do_prepare_flashback_for_switch_to_primary_(tenant_info))) {
      LOG_WARN("failed to do_prepare_flashback_for_switch_to_primary_", KR(ret), K(tenant_info));
    }
  } else if (obrpc::ObSwitchTenantArg::OpType::FAILOVER_TO_PRIMARY == switch_optype_) {
    if (OB_FAIL(double_check_service_name_(tenant_info))) {
      // do double check here
      // so_status is not normal, service name related commands is not allowed
      LOG_WARN("fail to execute double_check_service_name_", KR(ret), K(tenant_info));
    } else if (OB_FAIL(do_prepare_flashback_for_failover_to_primary_(tenant_info))) {
      LOG_WARN("failed to do_prepare_flashback_for_failover_to_primary_", KR(ret), K(tenant_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected switch tenant action", KR(ret),
             K_(switch_optype), K(tenant_info), K_(tenant_id));
  }

  DEBUG_SYNC(BEFORE_DO_FLASHBACK);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(do_flashback_())) {
    LOG_WARN("failed to prepare flashback", KR(ret), K(tenant_info));
  }
  return ret;
}
int ObTenantRoleTransitionService::do_prepare_flashback_for_switch_to_primary_(
    share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObLSStatusOperator status_op;
  DEBUG_SYNC(PREPARE_FLASHBACK_FOR_SWITCH_TO_PRIMARY);
  LOG_INFO("start to do_prepare_flashback_for_switch_to_primary_", KR(ret), K_(tenant_id));

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_prepare_flashback_for_switch_to_primary_status())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switchover status not match, switch to primary not allow", KR(ret), K(tenant_info));
    TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW, "switchover status not match", switch_optype_);
  } else if (OB_UNLIKELY(obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY != switch_optype_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation type is not SWITCH_TO_PRIMARY", KR(ret), K(switch_optype_));
  } else if (OB_UNLIKELY(switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K_(switchover_epoch));
  } else if (OB_FAIL(status_op.create_abort_ls_in_switch_tenant(
      tenant_id_, tenant_info.get_switchover_status(),
      tenant_info.get_switchover_epoch(), *sql_proxy_))) {
    // SYS LS has been synced, all current CTREATING/CTREATED LS cannot become NORMAL
    // These LS should become CREATE_ABORT, otherwise tenant cannot be synced
    LOG_WARN("failed to create abort ls", KR(ret), K_(tenant_id), K(tenant_info));
  } else if (OB_FAIL(wait_tenant_sync_to_latest_until_timeout_(tenant_id_, tenant_info))) {
    LOG_WARN("fail to execute wait_tenant_sync_to_latest_until_timeout_", KR(ret), K(tenant_info));
    SOURCE_TENANT_CHECK_USER_ERROR_FOR_SWITCHOVER_TO_PRIMARY;
  } else if (OB_SUCC(ret) && ERRSIM_TENANT_ROLE_TRANS_WAIT_SYNC_ERROR) {
    ret = ERRSIM_TENANT_ROLE_TRANS_WAIT_SYNC_ERROR;
    SOURCE_TENANT_CHECK_USER_ERROR_FOR_SWITCHOVER_TO_PRIMARY;
    LOG_WARN("errsim wait_tenant_sync_to_latest_until_timeout", K(ret));
  }
  if (FAILEDx(wait_ls_balance_task_finish_())) {
    LOG_WARN("failed to wait ls balance task finish", KR(ret));
  } else if (OB_FAIL(switchover_update_tenant_status(tenant_id_,
                                              true /* switch_to_primary */,
                                              tenant_info.get_tenant_role(),
                                              tenant_info.get_switchover_status(),
                                              share::FLASHBACK_SWITCHOVER_STATUS,
                                              tenant_info.get_switchover_epoch(),
                                              tenant_info))) {
    LOG_WARN("failed to switchover_update_tenant_status", KR(ret), K_(tenant_id), K(tenant_info));
  }
  return ret;
}

int ObTenantRoleTransitionService::do_prepare_flashback_for_failover_to_primary_(
    share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K_(tenant_id), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_prepare_flashback_for_failover_to_primary_status())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switchover status not match, failover to primary not allow", KR(ret), K(tenant_info));
    TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW, "switchover status not match", switch_optype_);
  } else if (OB_UNLIKELY(switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K_(switchover_epoch));
  } else if (OB_FAIL(OB_STANDBY_SERVICE.do_recover_tenant(tenant_id_,
                     share::PREPARE_FLASHBACK_FOR_FAILOVER_TO_PRIMARY_SWITCHOVER_STATUS,
                     obrpc::ObRecoverTenantArg::RecoverType::CANCEL,
                     SCN::min_scn()))) {
    LOG_WARN("failed to do_recover_tenant", KR(ret), K_(tenant_id));
    // reset error code and USER_ERROR to avoid print recover error log
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "recover cancel failed, failover to primary");
  } else if (OB_FAIL(wait_ls_balance_task_finish_())) {
    LOG_WARN("failed to wait ls balance task finish", KR(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_switchover_status(
                     tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
                     tenant_info.get_switchover_status(), share::FLASHBACK_SWITCHOVER_STATUS))) {
    LOG_WARN("failed to update tenant switchover status", KR(ret), K_(tenant_id), K(tenant_info));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
             tenant_id_, sql_proxy_, false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(tenant_info.get_switchover_epoch() != switchover_epoch_)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover is concurrency", KR(ret), K(tenant_info), K_(switchover_epoch));
  }

  return ret;
}

int ObTenantRoleTransitionService::clear_service_name_()
{
  int ret = OB_SUCCESS;
  ObArray<ObServiceName> all_service_names;
  int64_t epoch = 0;
  if (obrpc::ObSwitchTenantArg::OpType::FAILOVER_TO_PRIMARY != switch_optype_) {
    // do nothing
  } else if (OB_FAIL(ObServiceNameProxy::check_is_service_name_enabled(tenant_id_))) {
    if (OB_NOT_SUPPORTED == ret) {
      ret = OB_SUCCESS;
    }
    LOG_WARN("service_name is not enabled, no need to execute clear_service_name", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObServiceNameProxy::select_all_service_names_with_epoch(tenant_id_, epoch, all_service_names))) {
    LOG_WARN("fail to execute select_all_service_names_with_epoch", KR(ret), K(tenant_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_service_names.count(); i++) {
      const ObServiceName &service_name = all_service_names.at(i);
      if (OB_FAIL(ObServiceNameCommand::stop_service(tenant_id_, service_name.get_service_name_str()))) {
        LOG_WARN("fail to execute stop_service", KR(ret), K(tenant_id_), K(service_name));
      } else if (OB_FAIL(ObServiceNameCommand::delete_service(tenant_id_, service_name.get_service_name_str()))) {
        LOG_WARN("fail to execute delete_service", KR(ret), K(tenant_id_), K(service_name));
      }
    }
  }
  return ret;
}
int ObTenantRoleTransitionService::double_check_service_name_(const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObServiceName> all_service_names;
  int64_t service_num = 0;
  if (obrpc::ObSwitchTenantArg::OpType::FAILOVER_TO_PRIMARY != switch_optype_) {
    // do nothing
  } else if (OB_FAIL(ObServiceNameProxy::check_is_service_name_enabled(tenant_id_))) {
    if (OB_NOT_SUPPORTED == ret) {
      ret = OB_SUCCESS;
    }
    LOG_WARN("service_name is not enabled, no need to execute double_check_service_name_", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(tenant_info.is_normal_status())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not allowed to do double check when switchover status is NORMAL", KR(ret), K(tenant_info));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObServiceNameProxy::get_tenant_service_name_num(*GCTX.sql_proxy_, tenant_id_, service_num))) {
    LOG_WARN("fail to execute get_tenant_service_name_num", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(0 != service_num)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("the tenant should have zero service_name", KR(ret), K(service_num));
  }
  return ret;
}

int ObTenantRoleTransitionService::do_flashback_()
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  logservice::ObLogService *log_service = NULL;
  ObLSStatusOperator status_op;
  ObAllTenantInfo tenant_info;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                 tenant_id_, sql_proxy_, false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!tenant_info.is_flashback_status() ||
                         switchover_epoch_ !=
                             tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K(switchover_epoch_));
  } else if (obrpc::ObSwitchTenantArg::OpType::FAILOVER_TO_PRIMARY == switch_optype_
        && OB_FAIL(status_op.create_abort_ls_in_switch_tenant(
              tenant_id_, tenant_info.get_switchover_status(),
              tenant_info.get_switchover_epoch(), *sql_proxy_))) {
    LOG_WARN("failed to create abort ls", KR(ret), K_(tenant_id), K(tenant_info));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(
                 ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to get MTL log_service", KR(ret), K(tenant_id_));
  } else {
    int64_t begin_time = ObTimeUtility::current_time();
    if (OB_FAIL(log_service->flashback(tenant_id_, tenant_info.get_sync_scn(), ctx.get_timeout()))) {
      LOG_WARN("failed to flashback", KR(ret), K(tenant_id_), K(tenant_info));
    }
    int64_t log_flashback = ObTimeUtility::current_time() - begin_time;
    if (OB_LIKELY(NULL != cost_detail_)) {
      (void) cost_detail_->add_cost(ObTenantRoleTransCostDetail::LOG_FLASHBACK, log_flashback);
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    CLUSTER_EVENT_ADD_LOG(ret, "flashback end",
        "tenant id", tenant_id_,
        "switchover#", tenant_info.get_switchover_epoch(),
        "flashback_scn#", tenant_info.get_sync_scn());
    ObAllTenantInfo new_tenant_info;
    if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_switchover_status(
            tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
            tenant_info.get_switchover_status(), share::SWITCHING_TO_PRIMARY_SWITCHOVER_STATUS))) {
      LOG_WARN("failed to update tenant role", KR(ret), K(tenant_id_), K(tenant_info));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_,
                                                      false, new_tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(new_tenant_info.get_switchover_epoch() != tenant_info.get_switchover_epoch())) {
      ret = OB_NEED_RETRY;
      LOG_WARN("switchover is concurrency", KR(ret), K(tenant_info), K(new_tenant_info));
    } else if (OB_FAIL(do_switch_access_mode_to_append(new_tenant_info, share::PRIMARY_TENANT_ROLE))) {
      LOG_WARN("failed to prepare flashback", KR(ret), K(new_tenant_info));
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::do_switch_access_mode_to_append(
    const share::ObAllTenantInfo &tenant_info,
    const share::ObTenantRole &target_tenant_role)
{
  int ret = OB_SUCCESS;
  int64_t begin_time = ObTimeUtility::current_time();
  palf::AccessMode access_mode = logservice::ObLogService::get_palf_access_mode(target_tenant_role);
  SCN ref_scn;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_switching_to_primary_status()
        || target_tenant_role == tenant_info.get_tenant_role()
        || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info),
        K(target_tenant_role), K(switchover_epoch_));
  } else if (OB_FAIL(get_tenant_ref_scn_(tenant_info.get_sync_scn(), ref_scn))) {
    LOG_WARN("failed to get tenant ref_scn", KR(ret));
    //TODO(yaoying):xianming
  } else if (OB_FAIL(get_all_ls_status_and_change_access_mode_(
      access_mode,
      ref_scn,
      share::SCN::min_scn()))) {
    LOG_WARN("fail to execute get_all_ls_status_and_change_access_mode_", KR(ret),
        K(tenant_info), K(access_mode), K(ref_scn));
  } else {
    common::ObMySQLTransaction trans;
    share::ObAllTenantInfo cur_tenant_info;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
    if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id), K_(tenant_id));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, &trans, true, cur_tenant_info))) {
      LOG_WARN("failed to load all tenant info", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(tenant_info.get_switchover_status() != cur_tenant_info.get_switchover_status()
                           || tenant_info.get_switchover_epoch() != cur_tenant_info.get_switchover_epoch())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("Tenant status changed by concurrent operation, switch to primary is not allowed",
              KR(ret), K(tenant_info), K(cur_tenant_info));
      TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW,
          "tenant status changed by concurrent operation", switch_optype_);
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role_in_trans(
        tenant_id_, trans, tenant_info.get_switchover_epoch(),
        share::PRIMARY_TENANT_ROLE, tenant_info.get_switchover_status(),
        share::NORMAL_SWITCHOVER_STATUS, switchover_epoch_))) {
      LOG_WARN("failed to update tenant switchover status", KR(ret), K(tenant_id_), K(tenant_info), K(cur_tenant_info));
    } else if (cur_tenant_info.get_recovery_until_scn().is_max()) {
      LOG_INFO("recovery_until_scn already is max_scn", KR(ret), K_(tenant_id), K(cur_tenant_info));
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_recovery_until_scn(
                  tenant_id_, trans, switchover_epoch_, SCN::max_scn()))) {
      LOG_WARN("failed to update_tenant_recovery_until_scn", KR(ret), K_(tenant_id), K(tenant_info), K(cur_tenant_info));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  if (OB_LIKELY(NULL != cost_detail_)) {
    int64_t log_mode_change = ObTimeUtility::current_time() - begin_time;
    (void) cost_detail_->add_cost(ObTenantRoleTransCostDetail::CHANGE_ACCESS_MODE, log_mode_change);
  }
  return ret;
}

int ObTenantRoleTransitionService::get_tenant_ref_scn_(const share::SCN &sync_scn, share::SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  ref_scn = sync_scn;
  if (!sync_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sync_scn));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
    share::ObLSStatusInfoArray status_info_array;
    ObLSStatusOperator status_op;
    ObLSRecoveryStatOperator recovery_op;
    ObLSRecoveryStat ls_recovery_stat;
    START_TRANSACTION(sql_proxy_, exec_tenant_id)
    if (FAILEDx(status_op.get_all_ls_status_by_order_for_switch_tenant(tenant_id_,
            false/* ignore_need_create_abort */, status_info_array, trans))) {
      LOG_WARN("fail to get_all_ls_status_by_order", KR(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(0 == status_info_array.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ls status", KR(ret), K(tenant_id_), K(status_info_array));
    }
    for (int64_t i = 0; i < status_info_array.count() && OB_SUCC(ret); ++i) {
      const share::ObLSStatusInfo &status_info = status_info_array.at(i);
      //Get the largest sync_scn of all LS by Lock and read sync_scn of all LSs
      //Ensure that the syncpoint reports of all LS have been submitted
      //When each LS reports the synchronization progress, it will lock and check whether it is rolled back
      if (OB_FAIL(recovery_op.get_ls_recovery_stat(status_info.tenant_id_, status_info.ls_id_,
              true, ls_recovery_stat, trans))) {
        LOG_WARN("failed to get ls recovery stat", KR(ret), K(status_info));
      } else if (ls_recovery_stat.get_sync_scn() > ref_scn || ls_recovery_stat.get_create_scn() > ref_scn) {
        ref_scn = share::SCN::max(ls_recovery_stat.get_sync_scn(), ls_recovery_stat.get_create_scn());
      }
    }

    END_TRANSACTION(trans);
  }
  return ret;
}

int ObTenantRoleTransitionService::wait_ls_balance_task_finish_()
{
  int ret = OB_SUCCESS;
  int64_t begin_time = ObTimeUtility::current_time();
  uint64_t compat_version = 0;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else {
    uint64_t meta_compat_version = 0;
    uint64_t user_compat_version = 0;

    ObGlobalStatProxy global_proxy(*sql_proxy_, gen_meta_tenant_id(tenant_id_));
    ObGlobalStatProxy user_global_proxy(*sql_proxy_, tenant_id_);
    if (OB_FAIL(global_proxy.get_current_data_version(meta_compat_version))) {
      LOG_WARN("failed to get current data version", KR(ret), K(tenant_id_));
    } else if (meta_compat_version < DATA_VERSION_4_2_0_0) {
      //if tenant version is less than 4200, no need check
      //Regardless of the data_version change and switchover concurrency scenario
      LOG_INFO("data version is smaller than 4200, no need check", K(meta_compat_version));
    } else if (OB_FAIL(user_global_proxy.get_current_data_version(user_compat_version))) {
      LOG_WARN("failed to get current data version", KR(ret), K(tenant_id_));
    } else if (user_compat_version < DATA_VERSION_4_2_0_0) {
      //if tenant version is less than 4200, no need check
      //Ignore the situation where the accurate version is not obtained because the readable_scn is behind.
      LOG_INFO("data version is smaller than 4200, no need check", K(user_compat_version));
    } else {
      bool is_finish = false;
      ObArray<ObBalanceTaskHelper> ls_balance_tasks;
      ObBalanceTaskArray balance_task_array;
      share::ObAllTenantInfo cur_tenant_info;
      int tmp_ret = OB_SUCCESS;
      SCN max_scn;
      max_scn.set_max();
      while (!THIS_WORKER.is_timeout() && OB_SUCC(ret) && !is_finish) {
        if (FALSE_IT(ret = ObBalanceTaskHelperTableOperator::load_tasks_order_by_scn(tenant_id_,
                *sql_proxy_, max_scn, ls_balance_tasks))) {
        } else if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          balance_task_array.reset();
          /*
             __all_balance_task_helper表被清空不代表没有transfer任务，例子：
             租户A是主库，发起了一轮负载均衡，balance_task表里进入transfer状态但是没有结束的时候切换成备库。
             租户B是备库，在切成主库的时候会在回放到最新的时候，把TRANSFER_BEGIN的给清理掉。
             如果租户B作为主库的时候也执行了几轮transfer任务但是还是没有结束掉balance_task表的transfer状态。
             这个时候租户A在切成主库的时候是没有办法判断自己是否有transfer的发生。
             为了解决这个问题，我们去读取了__all_balance_task表，如果这个表中有处于transfer状态的任务，则一定要等回放到最新。
             同时这里也有一个问题：可读点会不会特别落后，我们从两个方面论述A租户可读点不会有问题
             1. 如果可以清理__all_balance_task_helper表，则可读点一定越过了表中的记录,即使B新开启了一轮负载均衡任务，那也不会有问题
             2. 单纯的经过主切备，可读点会推高越过最新的GTS，所以肯定可以读到最新的transfer任务*/
          if (OB_FAIL(ObBalanceTaskTableOperator::load_need_transfer_task(
                  tenant_id_, balance_task_array, *sql_proxy_))) {
            LOG_WARN("failed to load need transfer task", KR(ret), K(tenant_id_));
          } else if (0 == balance_task_array.count()) {
            is_finish = true;
            LOG_INFO("balance task finish", K(tenant_id_));
          } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                  tenant_id_, sql_proxy_, false, cur_tenant_info))) {
            LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
          } else if (cur_tenant_info.get_sync_scn() == cur_tenant_info.get_readable_scn()) {
            is_finish = true;
            for (int64_t i = 0; OB_SUCC(ret) && i < balance_task_array.count() && !is_finish; ++i) {
              const ObBalanceTask &task = balance_task_array.at(i);
              if (OB_FAIL(ObLSServiceHelper::check_transfer_task_replay(tenant_id_,
                      task.get_src_ls_id(), task.get_dest_ls_id(), cur_tenant_info.get_sync_scn(),
                      is_finish))) {
                LOG_WARN("failed to check transfer task replay", KR(ret), K(cur_tenant_info), K(task));
              } else if (!is_finish) {
                LOG_INFO("has transfe task, and not replay to newest", K(task));
              }
            }//end for
            if (OB_SUCC(ret) && is_finish) {
            LOG_INFO("has transfer task, and replay to newest", KR(ret), K(cur_tenant_info));
            }
          }
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to pop task", KR(ret), K(tenant_id_));
        }

        if (OB_SUCC(ret) && !is_finish) {
          if (OB_TMP_FAIL(notify_recovery_ls_service_())) {
            LOG_WARN("failed to notify recovery ls service", KR(tmp_ret));
          }
          usleep(100L * 1000L);
          LOG_INFO("has balance task not finish", K(ls_balance_tasks),
              K(balance_task_array), K(cur_tenant_info));
        }
      }
      if (OB_SUCC(ret)) {
        if (THIS_WORKER.is_timeout() || !is_finish) {
          ret = OB_TIMEOUT;
          LOG_WARN("failed to wait ls balance task finish", KR(ret), K(is_finish));
        }
      }
    }
  }
  if (OB_LIKELY(NULL != cost_detail_)) {
    int64_t wait_balance_task = ObTimeUtility::current_time() - begin_time;
    (void) cost_detail_->add_cost(ObTenantRoleTransCostDetail::WAIT_BALANCE_TASK, wait_balance_task);
  }
  return ret;
}

int ObTenantRoleTransitionService::notify_recovery_ls_service_()
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else {
    ObAddr leader;
    ObNotifyTenantThreadArg arg;
    const int64_t timeout = ctx.get_timeout();
    if (OB_FAIL(GCTX.location_service_->get_leader(
            GCONF.cluster_id, tenant_id_, SYS_LS, false, leader))) {
      LOG_WARN("failed to get leader", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(arg.init(tenant_id_, obrpc::ObNotifyTenantThreadArg::RECOVERY_LS_SERVICE))) {
      LOG_WARN("failed to init arg", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(rpc_proxy_->to(leader).timeout(timeout)
          .group_id(share::OBCG_DBA_COMMAND).by(tenant_id_)
          .notify_tenant_thread(arg))) {
      LOG_WARN("failed to notify tenant thread", KR(ret),
          K(leader), K(tenant_id_), K(timeout), K(arg));
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(GCTX.location_service_->nonblock_renew(
              GCONF.cluster_id, tenant_id_, SYS_LS))) {
        LOG_WARN("failed to renew location", KR(ret), KR(tmp_ret), K(tenant_id_));
      }
    }
  }

  return ret;
}

int ObTenantRoleTransitionService::do_switch_access_mode_to_raw_rw(
    const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  int64_t begin_time = ObTimeUtility::current_time();
  palf::AccessMode target_access_mode = logservice::ObLogService::get_palf_access_mode(STANDBY_TENANT_ROLE);
  ObLSStatusOperator status_op;
  share::ObLSStatusInfoArray sys_info_array;
  ObLSStatusInfo sys_status_info;
  share::SCN sys_ls_sync_scn = SCN::min_scn();
  bool is_sys_ls_sync_to_latest = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_switching_to_standby_status()
        || !tenant_info.is_standby()
        || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info),
        K(switchover_epoch_));
  } else if (OB_FAIL(status_op.create_abort_ls_in_switch_tenant(
                     tenant_id_, tenant_info.get_switchover_status(),
                     tenant_info.get_switchover_epoch(), *sql_proxy_))) {
    LOG_WARN("failed to create abort ls", KR(ret), K(tenant_info));
  } else if (OB_FAIL(status_op.get_ls_status_info(tenant_id_, SYS_LS, sys_status_info, *sql_proxy_))) {
    LOG_WARN("fail to get sys ls status info", KR(ret), K_(tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(sys_info_array.push_back(sys_status_info))) {
    LOG_WARN("fail to push back", KR(ret), K(sys_status_info));
  } else if (OB_FAIL(change_ls_access_mode_(sys_info_array, target_access_mode, SCN::base_scn(), share::SCN::min_scn()))) {
    // step 1: change sys ls access mode
    LOG_WARN("fail to execute change_ls_access_mode_", KR(ret), K(sys_info_array),
        K(target_access_mode));
  } else if (OB_FAIL(get_sys_ls_sync_scn_(
      tenant_id_,
      false /*need_check_sync_to_latest*/,
      sys_ls_sync_scn,
      is_sys_ls_sync_to_latest))) {
    LOG_WARN("fail to get sys_ls_sync_scn", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(get_all_ls_status_and_change_access_mode_(
      target_access_mode,
      SCN::base_scn(),
      sys_ls_sync_scn))) {
    // step 2: let user ls wait some time until their end scn being is larger than sys ls's
    //         then change user ls access mode
    // note: there's no need to remove sys ls in arg status_info_array, it will be removed in change_ls_access_mode_
    LOG_WARN("fail to execute get_all_ls_status_and_change_access_mode_", KR(ret), K(tenant_info),
        K(target_access_mode), K(sys_ls_sync_scn));
  }

  if (OB_LIKELY(NULL != cost_detail_)) {
    int64_t log_mode_change = ObTimeUtility::current_time() - begin_time;
    int64_t change_access_mode = log_mode_change - cost_detail_->get_wait_log_end();
    (void) cost_detail_->add_cost(ObTenantRoleTransCostDetail::CHANGE_ACCESS_MODE, change_access_mode);
  }
  DEBUG_SYNC(AFTER_CHANGE_ACCESS_MODE);
  return ret;
}

int ObTenantRoleTransitionService::get_all_ls_status_and_change_access_mode_(
    const palf::AccessMode target_access_mode,
    const share::SCN &ref_scn,
    const share::SCN &sys_ls_sync_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSStatusOperator status_op;
  share::ObLSStatusInfoArray status_info_array;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_FAIL(status_op.get_all_ls_status_by_order_for_switch_tenant(
      tenant_id_,
      false/* ignore_need_create_abort */,
      status_info_array,
      *sql_proxy_))) {
    LOG_WARN("fail to get_all_ls_status_by_order", KR(ret), K_(tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(change_ls_access_mode_(status_info_array, target_access_mode, ref_scn, sys_ls_sync_scn))) {
    // ref_scn and target_access_mode will be checked in this func
    LOG_WARN("fail to execute change_ls_access_mode_", KR(ret), K(status_info_array),
        K(target_access_mode), K(ref_scn), K(sys_ls_sync_scn));
  }
  if (OB_TMP_FAIL(ls_status_stats_when_change_access_mode_(status_info_array))) {
    LOG_WARN("fail to gather ls status", KR(ret), KR(tmp_ret), K(status_info_array));
  }
  return ret;
}

int ObTenantRoleTransitionService::change_ls_access_mode_(
    const share::ObLSStatusInfoArray &status_info_array,
    const palf::AccessMode target_access_mode,
    const share::SCN &ref_scn,
    const share::SCN &sys_ls_sync_scn)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(palf::AccessMode::INVALID_ACCESS_MODE == target_access_mode 
                         || OB_INVALID_VERSION == switchover_epoch_
                         || !ref_scn.is_valid_and_not_min())) {
    // no need to check sys_ls_sync_scn, since this is only valid when it's needed
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_access_mode), K(switchover_epoch_), K(ref_scn));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    ObArray<ObLSAccessModeInfo> ls_mode_info;
    ObArray<ObLSAccessModeInfo> need_change_info;
    //ignore error, try until success
    bool need_retry = true;
    do {
      ls_mode_info.reset();
      need_change_info.reset();
      //1. get current access mode
      if (ctx.is_timeouted()) {
        need_retry = false;
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", KR(ret));
      } else if (OB_FAIL(get_ls_access_mode_(status_info_array, ls_mode_info))) {
        LOG_WARN("failed to get ls access mode", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_mode_info.count(); ++i) {
          const ObLSAccessModeInfo &info = ls_mode_info.at(i);
          if (info.get_access_mode() == target_access_mode) {
            //nothing, no need change
          } else if (OB_FAIL(need_change_info.push_back(info))) {
            LOG_WARN("failed to assign", KR(ret), K(i), K(info));
          }
        }
      }//end for
      //2. check epoch not change
      if (OB_SUCC(ret) && need_change_info.count() > 0) {
        ObAllTenantInfo new_tenant_info;
        if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                tenant_id_, sql_proxy_, false, new_tenant_info))) {
          LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
        } else if (switchover_epoch_ != new_tenant_info.get_switchover_epoch()) {
          need_retry = false;
          ret = OB_NEED_RETRY;
          LOG_WARN("epoch change, no need change access mode", KR(ret),
              K(switchover_epoch_), K(new_tenant_info),
              K(target_access_mode));
        }
      }
      //3. change access mode
      if (OB_SUCC(ret) && need_change_info.count() > 0) {
        if (OB_FAIL(do_change_ls_access_mode_(need_change_info,
                target_access_mode, ref_scn, sys_ls_sync_scn))) {
          LOG_WARN("failed to change ls access mode", KR(ret), K(need_change_info),
              K(target_access_mode), K(ref_scn), K(sys_ls_sync_scn));
        }
      }
      if (OB_SUCC(ret)) {
        break;
      }
    } while (need_retry);

  }
  LOG_INFO("[ROLE_TRANSITION] finish change ls mode", KR(ret), K(tenant_id_),
      K(target_access_mode), K(ref_scn));
  return ret;
}

int ObTenantRoleTransitionService::ls_status_stats_when_change_access_mode_(
    const share::ObLSStatusInfoArray &status_info_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
      const ObLSStatusInfo &status = status_info_array.at(i);
      const ObLSID &ls_id = status.get_ls_id();
      const ObLSStatus &ls_status = status.get_status();
      if (OB_FAIL(all_ls_info_->add_ls(ls_id, ls_status))) {
        LOG_WARN("fail to push back", KR(ret), K(ls_id), KPC(all_ls_info_));
      }
    }
    FLOG_INFO("gather ls_status_stats", KR(ret), K(all_ls_info_), K(status_info_array));
  }
  return ret;
}

template <typename ARRAY_L, typename ARRAY_R>
int do_nonblock_renew(const ARRAY_L &array_l, const ARRAY_R &array_r, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || 0 > array_l.count()
                  || 0 > array_r.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(array_l), K(array_r));
  } else {
    for (int64_t i = 0; i < array_l.count(); ++i) {
      const ObLSID ls_id = array_l.at(i).get_ls_id();
      bool found = false;
      FOREACH_CNT_X(r, array_r, !found) {
        if (OB_ISNULL(r)) {
          // ignore ret
          LOG_WARN("r is null", KR(ret), K(array_r));
        } else if (*r == ls_id) {
          found = true;
        }
      }
      if (!found) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS !=(tmp_ret = GCTX.location_service_->nonblock_renew(
                  GCONF.cluster_id, tenant_id, ls_id))) {
          LOG_WARN("failed to renew location", KR(ret), K(tenant_id), K(ls_id));
        }
      }
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::get_ls_access_mode_(
    const share::ObLSStatusInfoArray &status_info_array,
    ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info)
{
  int ret = OB_SUCCESS;
  ls_access_info.reset();
  ObArray<share::ObLSID> success_ls_ids;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else {
      ObAddr leader;
      ObGetLSAccessModeProxy proxy(
          *rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_access_mode);
      obrpc::ObGetLSAccessModeInfoArg arg;
      ObArray<int> return_code_array;
      int tmp_ret = OB_SUCCESS;
      const uint64_t group_id = share::OBCG_DBA_COMMAND;
      for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
        return_code_array.reset();
        const ObLSStatusInfo &info = status_info_array.at(i);
        const int64_t timeout = ctx.get_timeout();
        if (OB_FAIL(GCTX.location_service_->get_leader(
          GCONF.cluster_id, tenant_id_, info.ls_id_, false, leader))) {
          LOG_WARN("failed to get leader", KR(ret), K(tenant_id_), K(info));
        } else if (OB_FAIL(arg.init(tenant_id_, info.ls_id_))) {
          LOG_WARN("failed to init arg", KR(ret), K(tenant_id_), K(info));
        // use meta rpc process thread
        } else if (OB_FAIL(proxy.call(leader, timeout, GCONF.cluster_id, tenant_id_, group_id, arg))) {
          //can not ignore error of each ls
          LOG_WARN("failed to send rpc", KR(ret), K(leader), K(timeout),
              K(tenant_id_), K(arg), K(group_id));
        }
        if (OB_FAIL(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(GCTX.location_service_->nonblock_renew(
                  GCONF.cluster_id, tenant_id_, info.ls_id_))) {
            LOG_WARN("failed to renew location", KR(ret), KR(tmp_ret), K(tenant_id_), K(info));
          }
        }
      }//end for

      //get result
      if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
        LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_FAIL(ret)) {
        //no need to process return code
      } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
        LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
      } else if (status_info_array.count() != return_code_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rpc count not equal to result count", KR(ret),
                 K(return_code_array.count()), K(status_info_array.count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
          ret = return_code_array.at(i);
          if (OB_FAIL(ret)) {
            LOG_WARN("send rpc is failed", KR(ret), K(i));
          } else {
            const auto *result = proxy.get_results().at(i);
            if (OB_ISNULL(result)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("result is null", KR(ret), K(i));
            } else if (OB_FAIL(ls_access_info.push_back(*result))) {
              LOG_WARN("failed to push back info", KR(ret), KPC(result));
            } else if (OB_TMP_FAIL(success_ls_ids.push_back(result->get_ls_id()))) {
              LOG_WARN("fail to push back", KR(ret), KR(tmp_ret), K(success_ls_ids), KPC(result));
            }
          }
          LOG_INFO("[ROLE_TRANSITION] get ls access mode", KR(ret), K(arg));
        }

        if (OB_FAIL(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(do_nonblock_renew(status_info_array, success_ls_ids, tenant_id_))) {
            LOG_WARN("failed to renew location", KR(ret), KR(tmp_ret), K(tenant_id_), K(status_info_array), K(success_ls_ids));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::do_change_ls_access_mode_(
    const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
    const palf::AccessMode target_access_mode,
    const SCN &ref_scn,
    const share::SCN &sys_ls_sync_scn)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  ObArray<share::ObLSID> success_ls_ids;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(0== ls_access_info.count()
                         || palf::AccessMode::INVALID_ACCESS_MODE == target_access_mode
                         || !ref_scn.is_valid_and_not_min())) {
    // no need to check sys_ls_sync_scn, since this is only valid when it's needed
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_access_mode), K(ls_access_info), K(ref_scn));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx,
                                                          GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else {
    ObChangeLSAccessModeProxy proxy(*rpc_proxy_, &obrpc::ObSrvRpcProxy::change_ls_access_mode);
    ObAddr leader;
    obrpc::ObLSAccessModeInfo arg;
    const uint64_t group_id = share::OBCG_DBA_COMMAND;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_access_info.count(); ++i) {
      const obrpc::ObLSAccessModeInfo &info = ls_access_info.at(i);
      const int64_t timeout = ctx.get_timeout();
      if (OB_FAIL(GCTX.location_service_->get_leader(
          GCONF.cluster_id, tenant_id_, info.get_ls_id(), false, leader))) {
          LOG_WARN("failed to get leader", KR(ret), K(tenant_id_), K(info));
      // use meta rpc process thread
      } else if (OB_FAIL(arg.init(tenant_id_, info.get_ls_id(), info.get_mode_version(),
              target_access_mode, ref_scn, sys_ls_sync_scn))) {
        LOG_WARN("failed to init arg", KR(ret), K(info), K(target_access_mode), K(ref_scn), K(sys_ls_sync_scn));
      } else if (OB_FAIL(proxy.call(leader, timeout, GCONF.cluster_id, tenant_id_, group_id, arg))) {
        //can not ignore of each ls
        LOG_WARN("failed to send rpc", KR(ret), K(arg), K(timeout),
            K(tenant_id_), K(info), K(group_id));
      }
    }//end for
    //result
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;
    const int64_t rpc_count = ls_access_info.count();
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
    } else if (rpc_count != return_code_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc count not equal to result count", KR(ret),
               K(rpc_count), K(return_code_array.count()));
    } else {
      int64_t ls_wait_sync_scn_max = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        ret = return_code_array.at(i);
        const obrpc::ObChangeLSAccessModeRes *result = proxy.get_results().at(i);
        const obrpc::ObLSAccessModeInfo &info = ls_access_info.at(i);
        if (OB_FAIL(ret)) {
          LOG_WARN("send rpc is failed", KR(ret), K(i));
        } else if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(i));
        } else if (OB_FAIL(result->get_result())) {
          LOG_WARN("failed to change ls mode", KR(ret), KPC(result));
        } else if (OB_TMP_FAIL(success_ls_ids.push_back(result->get_ls_id()))) {
          LOG_WARN("fail to push back", KR(ret), KR(tmp_ret), K(success_ls_ids), K(result));
        }
        if (OB_FAIL(ret)) {
        } else {
          int64_t wait_scn_t = result->get_wait_sync_scn_cost();
          ls_wait_sync_scn_max =
              ls_wait_sync_scn_max > wait_scn_t ? ls_wait_sync_scn_max : wait_scn_t;
        }
        LOG_INFO("[ROLE_TRANSITION] change ls access mode", KR(ret), K(info), KPC(result), K(proxy.get_dests()));
      }// end for
      if (OB_LIKELY(NULL != cost_detail_)) {
        (void) cost_detail_->add_cost(ObTenantRoleTransCostDetail::WAIT_LOG_END, ls_wait_sync_scn_max);
      }
      if (OB_FAIL(ret)) {
        if (OB_TMP_FAIL(do_nonblock_renew(ls_access_info, success_ls_ids, tenant_id_))) {
          LOG_WARN("failed to renew location", KR(ret), KR(tmp_ret), K(tenant_id_), K(ls_access_info), K(success_ls_ids));
        }
      }
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::check_and_update_sys_ls_recovery_stat_in_switchover_(
    const uint64_t tenant_id,
    const bool switch_to_primary,
    ObMySQLTransaction &trans,
    const SCN &max_sys_ls_sync_scn/* SYS LS real max sync scn */,
    const SCN &target_tenant_sync_scn/* tenant target sync scn in switchover */)
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  ObLSRecoveryStat ls_recovery_stat;
  ObLSRecoveryStatOperator recovery_op;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id)
      || max_sys_ls_sync_scn > target_tenant_sync_scn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(max_sys_ls_sync_scn),
        K(target_tenant_sync_scn));
  }
  // Get latest SYS LS sync scn in table, which means SYS log recovery point
  else if (OB_FAIL(recovery_op.get_ls_recovery_stat(tenant_id, SYS_LS,
      false /* for update */, ls_recovery_stat, trans))) {
    LOG_WARN("failed to get SYS ls recovery stat", KR(ret), K(tenant_id));
  } else if (max_sys_ls_sync_scn < ls_recovery_stat.get_sync_scn()) {
    // SYS LS sync scn can not be greater than real max sync scn
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected SYS LS sync scn in table, it is greater than max sync scn",
        K(tenant_id), K(max_sys_ls_sync_scn), K(ls_recovery_stat), K(switch_to_primary));
  }
  // When switchover from STANDBY to PRIMARY,
  // SYS LS sync scn in table must be same with its real max sync scn
  else if (switch_to_primary && max_sys_ls_sync_scn != ls_recovery_stat.get_sync_scn()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SYS LS sync scn in table is not latest, unexpected error", K(tenant_id),
        K(switch_to_primary), K(ls_recovery_stat), K(max_sys_ls_sync_scn));
  } else if (max_sys_ls_sync_scn == ls_recovery_stat.get_sync_scn()) {
    need_update = false;
  } else {
    // SYS LS recovery point should be updated to latest during switchover.
    need_update = true;
    if (OB_FAIL(recovery_op.update_sys_ls_sync_scn(tenant_id, trans, max_sys_ls_sync_scn))) {
      LOG_WARN("update sys ls sync scn failed", KR(ret), K(tenant_id), K(max_sys_ls_sync_scn),
          K(ls_recovery_stat), K(switch_to_primary));
    }
  }

  LOG_INFO("check and update SYS LS sync scn in table", KR(ret), K(need_update), K(tenant_id),
      K(target_tenant_sync_scn), K(max_sys_ls_sync_scn), K(switch_to_primary), K(ls_recovery_stat));
  return ret;
}

int ObTenantRoleTransitionService::switchover_update_tenant_status(
    const uint64_t tenant_id,
    const bool switch_to_primary,
    const ObTenantRole &new_role,
    const ObTenantSwitchoverStatus &old_status,
    const ObTenantSwitchoverStatus &new_status,
    const int64_t old_switchover_epoch,
    ObAllTenantInfo &new_tenant_info)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  SCN max_checkpoint_scn = SCN::min_scn();
  SCN max_sys_ls_sync_scn = SCN::min_scn();
  ObMySQLTransaction trans;
  ObLSStatusOperator status_op;
  share::ObLSStatusInfoArray status_info_array;
  common::ObArray<obrpc::ObCheckpoint> switchover_checkpoints;
  bool is_sync_to_latest = false;
  SCN final_sync_scn;
  final_sync_scn.set_min();

  if (OB_UNLIKELY(!is_user_tenant(tenant_id)
                  || !new_role.is_valid()
                  || !old_status.is_valid()
                  || !new_status.is_valid()
                  || OB_INVALID_VERSION == old_switchover_epoch)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(new_role), K(old_status), K(new_status), K(old_switchover_epoch));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  // switch_to_primary: ignore_need_create_abort = true
  // switch_to_standby: ignore_need_create_abort = false. There should be no LS that needs to create abort
  } else if (OB_FAIL(status_op.get_all_ls_status_by_order_for_switch_tenant(tenant_id,
                      switch_to_primary/* ignore_need_create_abort */, status_info_array, *sql_proxy_))) {
    LOG_WARN("failed to get_all_ls_status_by_order", KR(ret), K(tenant_id), KP(sql_proxy_));
  } else if (OB_UNLIKELY(0 >= status_info_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls list is null", KR(ret), K(tenant_id), K(status_info_array));
  } else if (OB_FAIL(get_checkpoints_by_rpc(tenant_id, status_info_array, false/* check_sync_to_latest */,
                                             switchover_checkpoints))) {
    LOG_WARN("fail to get_checkpoints_by_rpc", KR(ret), K(tenant_id), K(status_info_array));
  } else if (OB_FAIL(get_max_checkpoint_scn_(switchover_checkpoints, max_checkpoint_scn))) {
    LOG_WARN("fail to get_max_checkpoint_scn_", KR(ret), K(tenant_id), K(switchover_checkpoints));
  } else if (OB_FAIL(get_sys_ls_sync_scn_(switchover_checkpoints, max_sys_ls_sync_scn, is_sync_to_latest))) {
    LOG_WARN("failed to get_sys_ls_sync_scn_", KR(ret), K(switchover_checkpoints));
  } else if (OB_UNLIKELY(!max_checkpoint_scn.is_valid_and_not_min() || !max_sys_ls_sync_scn.is_valid_and_not_min())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid checkpoint_scn", KR(ret), K(tenant_id), K(max_checkpoint_scn),
                                       K(max_sys_ls_sync_scn), K(switchover_checkpoints));
  } else {
    ObAllTenantInfo tmp_tenant_info;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    /**
     * @description:
     * In order to make timestamp monotonically increasing before and after switch tenant role
     * set readable_scn to gts_upper_limit:
     *    MAX{MAX_SYS_LS_LOG_TS + 2 * preallocated_range}
     * set sync_snc to
     *    MAX{MAX{LS max_log_ts}, MAX_SYS_LS_LOG_TS + 2 * preallocated_range}
     * Because replayable point is not support, set replayable_scn = sync_scn
     */
    const SCN gts_upper_limit = transaction::ObTimestampService::get_sts_start_scn(max_sys_ls_sync_scn);
    final_sync_scn = MAX(max_checkpoint_scn, gts_upper_limit);
    const SCN final_replayable_scn = final_sync_scn;
    SCN final_readable_scn = SCN::min_scn();
    SCN final_recovery_until_scn = SCN::min_scn();
    if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                       tenant_id, &trans, true /* for update */, tmp_tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(old_switchover_epoch != tmp_tenant_info.get_switchover_epoch()
                           || old_status != tmp_tenant_info.get_switchover_status())) {
      ret = OB_NEED_RETRY;
      LOG_WARN("switchover may concurrency, need retry", KR(ret), K(old_switchover_epoch),
               K(old_status), K(tmp_tenant_info));
    } else if (OB_FAIL(check_and_update_sys_ls_recovery_stat_in_switchover_(tenant_id,
        switch_to_primary, trans, max_sys_ls_sync_scn, final_sync_scn))) {
      LOG_WARN("fail to check and update sys ls recovery stat in switchover", KR(ret), K(tenant_id),
          K(switch_to_primary), K(max_sys_ls_sync_scn));
    } else {
      if (switch_to_primary) {
        // switch_to_primary
        // Does not change STS
        final_readable_scn = tmp_tenant_info.get_readable_scn();
        // To prevent unexpected sync log, set recovery_until_scn = sync_scn
        final_recovery_until_scn = final_sync_scn;
      } else {
        // switch_to_standby
        // STS >= GTS
        final_readable_scn = gts_upper_limit;
        // Does not change recovery_until_scn, it should be MAX_SCN,
        // also check it when just entering switch_to_standby
        final_recovery_until_scn = tmp_tenant_info.get_recovery_until_scn();
        if (OB_UNLIKELY(!final_recovery_until_scn.is_max())) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("recovery_until_scn is not max_scn ", KR(ret), K(tenant_id),
                   K(final_recovery_until_scn), K(tmp_tenant_info));
          TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW,
              "recovery_until_scn is not max_scn", switch_optype_);
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_status(tenant_id,
                                                                  trans,
                                                                  new_role,
                                                                  old_status,
                                                                  new_status,
                                                                  final_sync_scn,
                                                                  final_replayable_scn,
                                                                  final_readable_scn,
                                                                  final_recovery_until_scn,
                                                                  old_switchover_epoch))) {
      LOG_WARN("failed to update_tenant_status", KR(ret), K(tenant_id), K(new_role),
               K(old_status), K(new_status), K(final_sync_scn), K(final_replayable_scn),
               K(final_readable_scn), K(final_recovery_until_scn), K(old_switchover_epoch));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                    tenant_id, &trans, false /* for update */, new_tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
    }
  }

  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, KR(temp_ret));
      ret = OB_SUCC(ret) ? temp_ret : ret;
    }
  }
  if (OB_SUCC(ret)) {
    so_scn_ = final_sync_scn;
  }
  CLUSTER_EVENT_ADD_LOG(ret, "update tenant status",
      "tenant id", tenant_id,
      "old switchover#", old_switchover_epoch,
      "new switchover#", new_tenant_info.get_switchover_epoch(),
      K(new_role), K(old_status), K(new_status));
  return ret;
}

int ObTenantRoleTransitionService::wait_sys_ls_sync_to_latest_until_timeout_(
    const uint64_t tenant_id,
    ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  bool only_check_sys_ls = true;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY != switch_optype_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only switchover to primary can execute this logic", KR(ret), K(tenant_id), K(switch_optype_));
  } else if (OB_UNLIKELY(!tenant_info.is_normal_status())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant switchover status is not normal", KR(ret), K(tenant_id), K(tenant_info));
  } else if (OB_FAIL(check_restore_source_for_switchover_to_primary_(tenant_id_))) {
    LOG_WARN("fail to check restore source", KR(ret), K_(tenant_id));
  } else if (!has_restore_source_) {
    LOG_INFO("no restore source", K(tenant_id), K(tenant_info));
  } else if (OB_FAIL(check_sync_to_latest_do_while_(tenant_info, only_check_sys_ls))) {
    LOG_WARN("fail to check whether sys ls is synced", KR(ret), K(tenant_info));
  }
  return ret;
}

int ObTenantRoleTransitionService::wait_tenant_sync_to_latest_until_timeout_(
    const uint64_t tenant_id,
    const ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  bool only_check_sys_ls = false;
  int64_t begin_time = ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY != switch_optype_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only switchover to primary can execute this logic", KR(ret), K(tenant_id), K(switch_optype_));
  } else if (OB_UNLIKELY(!tenant_info.is_prepare_flashback_for_switch_to_primary_status())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant switchover status is not prepare", KR(ret), K(tenant_id), K(tenant_info));
  } else if (OB_FAIL(check_restore_source_for_switchover_to_primary_(tenant_id_))) {
    LOG_WARN("fail to check restore source", KR(ret), K_(tenant_id));
  } else if (!has_restore_source_) {
    LOG_INFO("no restore source", K(tenant_id), K(tenant_info));
  } else if (OB_FAIL(check_sync_to_latest_do_while_(tenant_info, only_check_sys_ls))) {
    LOG_WARN("fail to check whether all ls are synced", KR(ret), K(tenant_id), K(tenant_info));
  }
  int64_t wait_log_sync = ObTimeUtility::current_time() - begin_time;
  LOG_INFO("wait tenant sync to latest", KR(ret), K(has_restore_source_), K(wait_log_sync));
  if (OB_LIKELY(NULL != cost_detail_)) {
    (void) cost_detail_->add_cost(ObTenantRoleTransCostDetail::WAIT_LOG_SYNC, wait_log_sync);
  }
  CLUSTER_EVENT_ADD_LOG(ret, "wait sync to latest end",
      "tenant id", tenant_id,
      "switchover#", tenant_info.get_switchover_epoch(),
      "finished", OB_SUCC(ret) ? "yes" : "no",
      "cost sec", wait_log_sync / SEC_UNIT);
  return ret;
}

int ObTenantRoleTransitionService::check_restore_source_for_switchover_to_primary_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObLogRestoreSourceMgr restore_source_mgr;
  ObLogRestoreSourceItem item;
  ObSqlString standby_source_value;
  ObRestoreSourceServiceAttr service_attr;
  has_restore_source_ = true;
  ObLogRestoreSourceType restore_type;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(restore_source_mgr.init(tenant_id, sql_proxy_))) {
    LOG_WARN("failed to init restore_source_mgr", KR(ret), K(tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(restore_source_mgr.get_source(item))) {
    LOG_WARN("failed to get_source", KR(ret), K(tenant_id), K(tenant_id));
    if (OB_ENTRY_NOT_EXIST == ret) {
      // When restore_source fails, in order to proceed switchover. If no restore_source is set,
      // do not check sync with restore_source
      LOG_INFO("failed to get_source", KR(ret), K(tenant_id), K(tenant_id));
      has_restore_source_ = false;
      ret = OB_SUCCESS;
    }
  } else if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log restore source item is invalid");
  } else if (share::is_location_log_source_type(item.type_)) {
    // archive mode, cannot check whether previous primary tenant becomes standby
  } else if (OB_FAIL(standby_source_value.assign(item.value_))) {
    LOG_WARN("fail to assign standby source value", K(item.value_));
  } else if (OB_FAIL(service_attr.parse_service_attr_from_str(standby_source_value))) {
    LOG_WARN("fail to parse service attr", K(item), K(standby_source_value));
  } else {
    // net service mode, check whether previous primary tenant becomes standby
    share::ObTenantRole tenant_role;
    share::schema::ObTenantStatus tenant_status;
    ObTenantSwitchoverStatus switchover_status;
    SMART_VAR(share::ObLogRestoreProxyUtil, proxy_util) {
      if (OB_FAIL(proxy_util.init_with_service_attr(tenant_id, &service_attr))) {
        LOG_WARN("fail to init proxy_util", KR(ret), K(service_attr));
      } else if (OB_FAIL(proxy_util.get_tenant_info(tenant_role, tenant_status, switchover_status))) {
        LOG_WARN("fail to get tenant info", KR(ret), K(service_attr));
      } else if (OB_UNLIKELY(!tenant_role.is_standby())) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("tenant role not match", KR(ret), K(tenant_role), K(service_attr));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "log restore source is primary, switchover to primary is");
      } else if (OB_UNLIKELY(share::schema::ObTenantStatus::TENANT_STATUS_NORMAL != tenant_status)) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("tenant status not match", KR(ret), K(tenant_status), K(service_attr));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "log restore source is not in normal status, switchover to primary is");
      } else if (OB_UNLIKELY(!switchover_status.is_normal_status())) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("tenant switchover status not match", KR(ret), K(switchover_status), K(service_attr));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "log restore source is not in normal switchover status, switchover to primary is");
      }
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::check_sync_to_latest_do_while_(
    const ObAllTenantInfo &tenant_info,
    const bool only_check_sys_ls)
{
  int ret = OB_SUCCESS;
  bool is_synced = false;
  const uint64_t tenant_id = tenant_info.get_tenant_id();
  while (!THIS_WORKER.is_timeout() && !logservice::ObLogRestoreHandler::need_fail_when_switch_to_primary(ret)) {
    bool is_all_ls_synced = false;
    bool is_sys_ls_synced = false;
    ret = OB_SUCCESS;
    if (OB_FAIL(check_sync_to_latest_(tenant_id, only_check_sys_ls, tenant_info, is_sys_ls_synced, is_all_ls_synced))) {
      LOG_WARN("fail to execute check_sync_to_latest_", KR(ret), K(tenant_id), K(only_check_sys_ls), K(tenant_info));
    } else {
      is_synced = only_check_sys_ls ? is_sys_ls_synced : is_all_ls_synced;
      if (is_synced) {
        LOG_INFO("sync to latest", K(tenant_id), K(only_check_sys_ls), K(is_synced),
            K(is_sys_ls_synced), K(is_all_ls_synced));
        break;
      } else {
        LOG_WARN("not sync to latest, wait a while", K(tenant_id), K(only_check_sys_ls));
      }
    }
    usleep(10L * 1000L);
  }
  if (logservice::ObLogRestoreHandler::need_fail_when_switch_to_primary(ret)) {
  } else if (THIS_WORKER.is_timeout() || !is_synced) {
    // return NOT_ALLOW instead of timeout
    if (OB_SUCC(ret)) {
      ret = OB_TIMEOUT; // to print in err_msg
    }
    ObSqlString err_msg;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(err_msg.assign_fmt("wait tenant sync to latest failed(original error code: %d), switchover to primary is", ret))) {
      LOG_WARN("fail to assign error msg", KR(ret), KR(tmp_ret));
    } else {
      // convert OB_TIMEOUT or other failure code to OB_OP_NOT_ALLOW
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("has not sync to latest, can not swithover to primary", KR(ret), K(only_check_sys_ls));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg.ptr());
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("finish check sync to latest", K(only_check_sys_ls), K(is_synced));
  }
  return ret;
}
int ObTenantRoleTransitionService::check_sync_to_latest_(
    const uint64_t tenant_id,
    const bool only_check_sys_ls,
    const ObAllTenantInfo &tenant_info,
    bool &is_sys_ls_synced,
    bool &is_all_ls_synced)
{
  int ret = OB_SUCCESS;
  int64_t begin_ts = ObTimeUtility::current_time();
  is_all_ls_synced = false;
  ObLSStatusOperator ls_status_op;
  share::ObLSStatusInfoArray all_ls_status_array;
  share::ObLSStatusInfoArray sys_ls_status_array;
  common::ObArray<obrpc::ObCheckpoint> switchover_checkpoints;
  ObLSRecoveryStatOperator ls_recovery_operator;
  ObLSRecoveryStat sys_ls_recovery_stat;
  SCN sys_ls_sync_scn = SCN::min_scn();
  bool sys_ls_sync_has_all_log = false;
  share::ObLSStatusInfo ls_status;
  ObTenantRoleTransNonSyncInfo non_sync_info;
  LOG_INFO("start to check_sync_to_latest", KR(ret), K(tenant_id), K(tenant_info));
  is_sys_ls_synced = false;
  if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(tenant_id, SYS_LS,
        false/*for_update*/, sys_ls_recovery_stat, *sql_proxy_))) {
    LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sys_ls_sync_scn_(
      tenant_id,
      true /*need_check_sync_to_latest*/,
      sys_ls_sync_scn,
      sys_ls_sync_has_all_log))) {
    LOG_WARN("failed to get_sys_ls_sync_scn_", KR(ret));
  } else {
    is_sys_ls_synced = (sys_ls_sync_scn.is_valid_and_not_min() && sys_ls_sync_has_all_log
        && sys_ls_recovery_stat.get_sync_scn() == sys_ls_sync_scn);
  }
  if (OB_FAIL(ret)) {
  } else if (only_check_sys_ls) {
    // do nothing
  } else if (OB_UNLIKELY(!is_sys_ls_synced)) {
    // we have checked sys ls when only_check_sys_ls is true,
    // now it should be synced
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys ls not sync", KR(ret), K(only_check_sys_ls), K(is_sys_ls_synced), K(tenant_info));
  } else if (OB_FAIL(ls_status_op.get_all_ls_status_by_order_for_switch_tenant(tenant_id,
                      true/* ignore_need_create_abort */, all_ls_status_array, *sql_proxy_))) {
    LOG_WARN("failed to get_all_ls_status_by_order", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_checkpoints_by_rpc(tenant_id, all_ls_status_array, true/* check_sync_to_latest */,
                                             switchover_checkpoints))) {
    LOG_WARN("fail to get_checkpoints_by_rpc", KR(ret), K(tenant_id), K(all_ls_status_array));
  } else if (switchover_checkpoints.count() != all_ls_status_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect checkpoints count", KR(ret), K(switchover_checkpoints), K(tenant_id), K(tenant_info),
                                       K(all_ls_status_array));
  } else {
    lib::ob_sort(switchover_checkpoints.begin(), switchover_checkpoints.end());
    if (OB_FAIL(non_sync_info.init(switchover_checkpoints))) {
      LOG_WARN("fail to init non_sync_info", KR(ret), K(switchover_checkpoints));
    } else {
      is_all_ls_synced = non_sync_info.is_sync();
    }
  }
  int64_t cost = ObTimeUtility::current_time() - begin_ts;
  LOG_INFO("check sync to latest", KR(ret), K(is_verify_), K(tenant_id), K(cost), K(only_check_sys_ls),
      K(is_sys_ls_synced), K(is_all_ls_synced), K(non_sync_info));
  if (is_verify_ || only_check_sys_ls) {
  } else if (REACH_TIME_INTERVAL(PRINT_INTERVAL) || is_all_ls_synced) {
    TENANT_EVENT(tenant_id, TENANT_ROLE_CHANGE, WAIT_LOG_SYNC, begin_ts,
        ret, cost, is_sys_ls_synced ? "YES" : "NO",
        is_all_ls_synced ? "YES" : "NO", non_sync_info);
    CLUSTER_EVENT_ADD_LOG(ret, "wait tenant sync from latest",
        "tenant id", tenant_id,
        "is sync", is_all_ls_synced ? "yes" : "no",
        "switchover#", tenant_info.get_switchover_epoch(),
        "finished", OB_SUCC(ret) ? "yes" : "no",
        "checkpoint", switchover_checkpoints,
        "cost sec", cost / SEC_UNIT);
  }
  return ret;
}

int ObTenantRoleTransitionService::get_checkpoints_by_rpc(const uint64_t tenant_id,
                                                     const share::ObLSStatusInfoIArray &status_info_array,
                                                     const bool check_sync_to_latest,
                                                     ObIArray<obrpc::ObCheckpoint> &checkpoints)
{
  int ret = OB_SUCCESS;
  checkpoints.reset();

  LOG_INFO("start to get_checkpoints_by_rpc", KR(ret), K(tenant_id), K(check_sync_to_latest), K(status_info_array));
  if (!is_user_tenant(tenant_id) || 0 >= status_info_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(status_info_array));
  } else if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.location_service_), KP(GCTX.srv_rpc_proxy_));
  } else {
    ObAddr leader;
    ObGetLSSyncScnProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_sync_scn);
    obrpc::ObGetLSSyncScnArg arg;
    //由于在check_sync_to_latest，需要给上游发RPC或者SQL获取准确的end_scn，所以会存在嵌套
    //RPC的概率，OBCG_DBA_COMMAND这个队列是需要的时候创建，个数和租户的CPU相关，如果发生
    //嵌套RPC的话，可能会出现资源型饿死的可能性。
    //在不需要检查check_sync_to_latest使用OBCG_DBA_COMMAND，否则为了避免嵌套RPC，使用NORMAL队列
    const uint64_t group_id = check_sync_to_latest ? 0 : share::OBCG_DBA_COMMAND;
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
      const ObLSStatusInfo &info = status_info_array.at(i);
      const int64_t timeout_us = !THIS_WORKER.is_timeout_ts_valid() ?
        GCONF.rpc_timeout : THIS_WORKER.get_timeout_remain();
      if (OB_FAIL(GCTX.location_service_->get_leader(
        GCONF.cluster_id, tenant_id, info.ls_id_, false, leader))) {
        LOG_WARN("failed to get leader", KR(ret), K(tenant_id), K(info));
      } else if (OB_FAIL(arg.init(tenant_id, info.ls_id_, check_sync_to_latest))) {
        LOG_WARN("failed to init arg", KR(ret), K(tenant_id), K(info));
      // use meta rpc process thread
      } else if (OB_FAIL(proxy.call(leader, timeout_us, GCONF.cluster_id, tenant_id, group_id, arg))) {
        LOG_WARN("failed to send rpc", KR(ret), K(leader), K(timeout_us),
            K(tenant_id), K(arg), K(group_id));
      }
    }//end for
    //get result
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
    } else if (status_info_array.count() != return_code_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc count not equal to result count", KR(ret),
                K(return_code_array.count()), K(return_code_array.count()));
    } else {
      ObGetLSSyncScnRes res;
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        tmp_ret = return_code_array.at(i);
        if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
          LOG_WARN("get checkpoints: send rpc failed", KR(ret), KR(tmp_ret), K(i));
          const obrpc::ObGetLSSyncScnArg &arg = proxy.get_args().at(i);
          if (OB_SUCCESS !=(tmp_ret = GCTX.location_service_->nonblock_renew(
                    GCONF.cluster_id, tenant_id, arg.get_ls_id()))) {
            LOG_WARN("failed to renew location", KR(tmp_ret), K(tenant_id), K(arg));
          }
        } else {
          const auto *result = proxy.get_results().at(i);
          const ObAddr &leader = proxy.get_dests().at(i);
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", KR(ret), K(i));
          } else {
            ObCheckpoint checkpoint(result->get_ls_id(), result->get_cur_sync_scn(), result->get_cur_restore_source_next_scn());
            if (OB_FAIL(checkpoints.push_back(checkpoint))) {
              LOG_WARN("failed to push back checkpoint", KR(ret), K(checkpoint));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::get_max_checkpoint_scn_(
  const ObIArray<obrpc::ObCheckpoint> &checkpoints, SCN &max_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  max_checkpoint_scn.set_min();
  if (OB_UNLIKELY(0 >= checkpoints.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("checkpoint list is null", KR(ret), K(checkpoints));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < checkpoints.count(); ++i) {
      const obrpc::ObCheckpoint &checkpoint = checkpoints.at(i);
      if (max_checkpoint_scn < checkpoint.get_cur_sync_scn()) {
        max_checkpoint_scn = checkpoint.get_cur_sync_scn();
      }
    }
  }

  return ret;
}

int ObTenantRoleTransitionService::get_sys_ls_sync_scn_(
    const uint64_t tenant_id,
    const bool need_check_sync_to_latest,
    share::SCN &sys_ls_sync_scn,
    bool &is_sync_to_latest)
{
  int ret = OB_SUCCESS;
  share::ObLSStatusInfoArray sys_ls_status_array;
  common::ObArray<obrpc::ObCheckpoint> switchover_checkpoints;
  ObLSStatusOperator ls_status_op;
  share::ObLSStatusInfo ls_status;
  sys_ls_sync_scn = SCN::min_scn();
  is_sync_to_latest = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_FAIL(ls_status_op.get_ls_status_info(tenant_id, SYS_LS, ls_status, *sql_proxy_))) {
    LOG_WARN("failed to get ls status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_ls_status_array.push_back(ls_status))) {
    LOG_WARN("fail to push back", KR(ret), K(ls_status), K(sys_ls_status_array));
  } else if (OB_FAIL(get_checkpoints_by_rpc(tenant_id, sys_ls_status_array, need_check_sync_to_latest,
                                            switchover_checkpoints))) {
    LOG_WARN("fail to get_checkpoints_by_rpc", KR(ret), K(tenant_id), K(sys_ls_status_array));
  } else if (1 != switchover_checkpoints.count() || SYS_LS != switchover_checkpoints.at(0).get_ls_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("checkpoints count is not 1 or not sys ls", KR(ret), K(switchover_checkpoints),
        K(tenant_id), K(sys_ls_status_array));
  } else {
    sys_ls_sync_scn = switchover_checkpoints.at(0).get_cur_sync_scn();
    is_sync_to_latest = switchover_checkpoints.at(0).is_sync_to_latest();
  }
  return ret;
}

int ObTenantRoleTransitionService::get_sys_ls_sync_scn_(
  const ObIArray<obrpc::ObCheckpoint> &checkpoints,
  SCN &sys_ls_sync_scn,
  bool &is_sync_to_latest)
{
  int ret = OB_SUCCESS;
  sys_ls_sync_scn.reset();
  is_sync_to_latest = false;
  if (0 >= checkpoints.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("checkpoints count is 0", KR(ret), K(checkpoints));
  } else {
    bool found = false;
    for (int64_t i = 0; i < checkpoints.count() && OB_SUCC(ret); i++) {
      const auto &checkpoint = checkpoints.at(i);
      if (checkpoint.get_ls_id().is_sys_ls()) {
        sys_ls_sync_scn = checkpoint.get_cur_sync_scn();
        is_sync_to_latest = checkpoint.is_sync_to_latest();
        found = true;
        break;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_ERROR("fail to get sys_ls_sync_scn", KR(ret), K(checkpoints));
    }
  }
  return ret;
}

void ObTenantRoleTransitionService::broadcast_tenant_info(const char* const log_mode)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT_US = ObTenantRoleTransitionConstants::TENANT_INFO_LEASE_TIME_US; // tenant info lease is 200ms
  int64_t timeout_us = 0;
  const int64_t timeout_remain = THIS_WORKER.get_timeout_remain();
  ObUnitTableOperator unit_operator;
  common::ObArray<ObUnit> units;
  const int64_t begin_time = ObTimeUtility::current_time();
  ObArray<int> return_code_array;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_ISNULL(GCTX.server_tracer_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.server_tracer_), KP(GCTX.srv_rpc_proxy_));
  } else if (0 > timeout_remain) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid timeout_remain", KR(ret), K(timeout_remain));
  } else if (INT64_MAX == timeout_remain || timeout_remain > DEFAULT_TIMEOUT_US) {
    timeout_us = DEFAULT_TIMEOUT_US;
  } else {
    timeout_us = timeout_remain;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
    LOG_WARN("failed to init unit operator", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(tenant_id_, units))) {
    LOG_WARN("failed to get tenant unit", KR(ret), K_(tenant_id));
  } else {
    //no need user special group OBCG_DBA_COMMAND
    ObRefreshTenantInfoProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::refresh_tenant_info);
    int64_t rpc_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
      obrpc::ObRefreshTenantInfoArg arg;
      const ObUnit &unit = units.at(i);
      bool alive = true;
      int64_t trace_time = 0;
      if (OB_FAIL(GCTX.server_tracer_->is_alive(unit.server_, alive, trace_time))) {
        LOG_WARN("check server alive failed", KR(ret), K(unit));
      } else if (!alive) {
        //not send to alive
      } else if (OB_FAIL(arg.init(tenant_id_))) {
        LOG_WARN("failed to init arg", KR(ret), K_(tenant_id));
      // use meta rpc process thread
      } else if (OB_FAIL(proxy.call(unit.server_, timeout_us, GCONF.cluster_id, gen_meta_tenant_id(tenant_id_), arg))) {
        LOG_WARN("failed to send rpc", KR(ret), K(unit), K(timeout_us), K_(tenant_id), K(arg));
      } else {
        rpc_count++;
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
    } else if (rpc_count != return_code_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc count not equal to result count", KR(ret),
               K(rpc_count), K(return_code_array.count()));
    } else {
      ObRefreshTenantInfoRes res;
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        ret = return_code_array.at(i);
        if (OB_FAIL(ret)) {
          LOG_WARN("send rpc is failed", KR(ret), K(i), K(proxy.get_dests()));
        } else {
          LOG_INFO("refresh_tenant_info success", KR(ret), K(i), K(proxy.get_dests()));
        }
      }
    }
  }
  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  LOG_INFO("broadcast_tenant_info finished", KR(ret), K(log_mode), K_(tenant_id), K(cost_time),
           K(units), K(return_code_array));
  return ;
}

int ObTenantRoleTransitionService::check_tenant_server_online_()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArray<ObAddr> temporary_offline_servers;
  ObArray<ObAddr> permanent_offline_servers;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(sql.append_fmt("select distinct svr_ip, svr_port from %s "
      "where (svr_ip, svr_port) in  (select u.svr_ip, u.svr_port from %s as u "
      "join %s as r on r.resource_pool_id=u.resource_pool_id where tenant_id=%ld) and status != 'ACTIVE'",
      OB_ALL_SERVER_TNAME, OB_ALL_UNIT_TNAME, OB_ALL_RESOURCE_POOL_TNAME, tenant_id_))) {
    LOG_WARN("fail to append sql", KR(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to read the tenant's online servers", KR(ret), K(sql), K(tenant_id_));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql result is null", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(construct_offline_servers_(*result, temporary_offline_servers, permanent_offline_servers))) {
        LOG_WARN("fail to construct offline servers", KR(ret), K(tenant_id_));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 != temporary_offline_servers.count()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant has units on temporary offline servers", KR(ret), K(tenant_id_), K(temporary_offline_servers));
    TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW,
        "the tenant has units on temporary offline servers", switch_optype_);
  } else if (0 != permanent_offline_servers.count()) {
    bool exists = false;
    if (OB_FAIL(ObEmptyServerChecker::check_if_tenant_ls_replicas_exist_in_servers(
        tenant_id_,
        permanent_offline_servers,
        exists))) {
      LOG_WARN("fail to check if the tenant's LS replicas exist in permanent_offline_servers",
          KR(ret), K(tenant_id_), K(permanent_offline_servers));
      if (OB_LEADER_NOT_EXIST == ret) {
        ret = OB_OP_NOT_ALLOW;
        TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW,
            "the tenant has LS replicas without leader", switch_optype_);
      }
    } else if (exists) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("the tenant has LS replicas on at least one of the permanent offline servers",
          KR(ret), K(tenant_id_), K(exists), K(permanent_offline_servers));
      TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW,
          "the tenant has LS replicas on at least one of the permanent offline servers", switch_optype_);
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::construct_offline_servers_(
    common::sqlclient::ObMySQLResult &res,
    ObArray<ObAddr> &temporary_offline_servers,
    ObArray<ObAddr> &permanent_offline_servers)
{
  int ret = OB_SUCCESS;
  ObAddr server;
  temporary_offline_servers.reset();
  permanent_offline_servers.reset();
  bool is_offline = false;
  while (OB_SUCC(ret)) {
    server.reset();
    char svr_ip[OB_IP_STR_BUFF] = "";
    int64_t svr_port = 0;
    int64_t tmp_real_str_len = 0;
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("result next failed", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else {
      EXTRACT_STRBUF_FIELD_MYSQL(res, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
      EXTRACT_INT_FIELD_MYSQL(res, "svr_port", svr_port, int64_t);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(false == server.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid server address", KR(ret), K(svr_ip), K(svr_port));
    } else if (OB_FAIL(SVR_TRACER.check_server_permanent_offline(server, is_offline))) {
      LOG_WARN("fail to check whether the server is permanent offline", KR(ret), K(tenant_id_), K(server));
    } else if (is_offline) {
      if (OB_FAIL(permanent_offline_servers.push_back(server))) {
        LOG_WARN("fail to push back server", KR(ret), K(server));
      }
    } else if (OB_FAIL(temporary_offline_servers.push_back(server))) {
      LOG_WARN("fail to push back server", KR(ret), K(server));
    }
  }
  if (0 != temporary_offline_servers.count() || 0 != permanent_offline_servers.count()) {
    LOG_INFO("the tenant has offline_servers", KR(ret), K(temporary_offline_servers), K(permanent_offline_servers));
  }
  return ret;
}

}
}
