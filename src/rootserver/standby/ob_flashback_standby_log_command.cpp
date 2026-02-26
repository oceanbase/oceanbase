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

#include "ob_flashback_standby_log_command.h"

#include "observer/ob_server_struct.h"
#include "share/ob_server_check_utils.h"
#include "share/restore/ob_log_restore_source_mgr.h"  // ObLogRestoreSourceMgr
#include "share/ob_flashback_log_scn_table_operator.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "rootserver/ob_service_name_command.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/standby/ob_standby_service.h"
#include "logservice/restoreservice/ob_log_restore_service.h"
#include "logservice/ob_log_service.h"
#include "rootserver/ob_tenant_event_def.h" // TENANT_EVENT


namespace oceanbase
{
using namespace share;
using namespace tenant_event;
namespace rootserver
{
int ObFlashbackStandbyLogTimeCostDetail::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != start_timestamp_ || 0 != end_timestamp_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(start_timestamp_), K(end_timestamp_));
  }
  for (int64_t i = 0; i < MAX_COST_TYPE && OB_SUCC(ret); ++i) {
    if (OB_UNLIKELY(0 != cost_[i])) {
      ret = OB_INIT_TWICE;
      LOG_WARN("init twice", KR(ret), K(i), K(cost_[i]));
    }
  }
  if (OB_SUCC(ret)) {
    start_timestamp_ = ObTimeUtility::current_time();
  }
  return ret;
}
int ObFlashbackStandbyLogTimeCostDetail::end() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == start_timestamp_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(start_timestamp_));
  } else {
    end_timestamp_ = ObTimeUtility::current_time();
    int64_t sum = 0;
    for (int64_t i = INVALID_COST_TYPE + 1; i < MAX_COST_TYPE; ++i) {
      sum += cost_[i];
    }
    cost_[OTHERS] = end_timestamp_ - start_timestamp_ - sum;
  }
  return ret;
}
int ObFlashbackStandbyLogTimeCostDetail::set_cost(const CostType cost_type, const int64_t time_cost)
{
  int ret = OB_SUCCESS;
  // not check time_cost >= 0 here due to possible time drift
  if (OB_UNLIKELY(cost_type <= INVALID_COST_TYPE || cost_type >= MAX_COST_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(cost_type), K(time_cost));
  } else {
    cost_[cost_type] = time_cost;
  }
  return ret;
}
const char* ObFlashbackStandbyLogTimeCostDetail::type_to_str(const CostType type) const
{
  static const char *strs[] = { "INVALID_COST_TYPE",
                                "CLEAR_SERVICE_NAME",
                                "CLEAR_FETCHED_LOG_CACHE",
                                "FLASHBACK_LOG",
                                "CHANGE_ACCESS_MODE",
                                "OTHERS"};
  STATIC_ASSERT(MAX_COST_TYPE == ARRAYSIZEOF(strs), "status string array size mismatch");
  const char* str = "INVALID_COST_TYPE";
  if (type <= INVALID_COST_TYPE || type >= MAX_COST_TYPE) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid type", K(type));
  } else {
    str = strs[type];
  }
  return str;
}
int64_t ObFlashbackStandbyLogTimeCostDetail::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t start = INVALID_COST_TYPE + 1;
  int64_t end = MAX_COST_TYPE - 1;
  for (int64_t i = start; i < end; ++i) {
    CostType type = static_cast<CostType>(i);
    BUF_PRINTF("%s: %ld, ", type_to_str(type), cost_[i]);
  }
  CostType type = static_cast<CostType>(end);
  BUF_PRINTF("%s: %ld", type_to_str(type), cost_[end]);
  return pos;
}
int ObFlashbackStandbyLogEvent::start(const uint64_t tenant_id, const share::SCN &flashback_log_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!flashback_log_scn.is_valid_and_not_min() || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(flashback_log_scn), K(tenant_id));
  } else if (OB_FAIL(cost_detail_.start())) {
    LOG_WARN("fail to start cost_detail", KR(ret));
  } else if (OB_FAIL(all_flashback_ls_.init())) {
    LOG_WARN("fail to init all_flashback_ls_", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    flashback_log_scn_ = flashback_log_scn;
    op_start_tenant_info_.reset();
    op_end_tenant_info_.reset();
  }
  return ret;
}
int ObFlashbackStandbyLogEvent::end(const share::ObAllTenantInfo &op_end_tenant_info, const int end_ret)
{
  int ret = OB_SUCCESS;
  // op_end_tenant_info might be invalid
  // try best to print tenant event
  if (OB_UNLIKELY(!is_user_tenant(tenant_id_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(cost_detail_.end())) {
    LOG_WARN("fail to end cost_detail", KR(ret));
  } else {
    op_end_tenant_info_.assign(op_end_tenant_info);
    end_ret_ = end_ret;
    int64_t end_timestamp = cost_detail_.get_end_timestamp();
    int64_t cost = cost_detail_.get_end_timestamp() - cost_detail_.get_start_timestamp();
    uint64_t flashback_log_scn = flashback_log_scn_.get_val_for_inner_table_field();
    TENANT_EVENT(tenant_id_, TENANT_ROLE_CHANGE, FLASHBACK_STANDBY_LOG, end_timestamp, end_ret_, cost,
        flashback_log_scn, op_start_tenant_info_, op_end_tenant_info_,
        all_flashback_ls_, cost_detail_);
  }
  return ret;
}
int ObFlashbackStandbyLogEvent::set_op_start_tenant_info(const share::ObAllTenantInfo &op_start_tenant_info) {
  int ret = OB_SUCCESS;
  // invalid op_start_tenant_info_ means that the op fails at load teant_info
  op_start_tenant_info_.assign(op_start_tenant_info);
  return ret;
}
int ObFlashbackStandbyLogEvent::add_all_flashback_ls(const share::ObLSStatusInfoArray &status_info_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < status_info_array.size() && OB_SUCC(ret); ++i) {
    const ObLSStatusInfo &status_info = status_info_array[i];
    if (OB_FAIL(all_flashback_ls_.add_ls(status_info.get_ls_id(), status_info.get_status()))) {
      LOG_WARN("fail to add ls", KR(ret), K(status_info));
    }
  }
  return ret;
}
int ObFlashbackStandbyLogCommand::execute(const share::ObFlashbackStandbyLogArg &arg)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  const uint64_t tenant_id = arg.get_tenant_id();
  const SCN &flashback_log_scn = arg.get_flashback_log_scn();
  bool is_flashback_log_table_enabled = false;
  if (OB_FAIL(event_.start(tenant_id, flashback_log_scn))) {
    LOG_WARN("fail to start event", KR(ret), K(tenant_id), K(flashback_log_scn));
  } else if (OB_FAIL(ObFlashbackLogSCNTableOperator::check_is_flashback_log_table_enabled(tenant_id))) {
    LOG_WARN("flashback standby log is not enabled", KR(ret), K(tenant_id));
  } else if (FALSE_IT(is_flashback_log_table_enabled = true)) {
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, GCTX.sql_proxy_, false, tenant_info))) {
    LOG_WARN("fail to load tenant info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(event_.set_op_start_tenant_info(tenant_info))) {
    LOG_WARN("fail to set op_start_tenant_info", KR(ret), K(tenant_info));
  } else if (!tenant_info.get_switchover_status().is_normal_status()
      && !tenant_info.get_switchover_status().is_flashback_and_stay_standby_status()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switchover status not matched", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant is executing other commands, FLASHBACK STANDBY LOG is");
  } else if (OB_FAIL(ObServerCheckUtils::check_tenant_server_online(tenant_id, "FLASHBACK STANDBY LOG"))) {
    LOG_WARN("fail to check whether the tenant's servers are online", KR(ret), K(tenant_id));
  } else if (tenant_info.get_switchover_status().is_normal_status()
      && OB_FAIL(process_normal_status_(tenant_id, flashback_log_scn, tenant_info))) {
    LOG_WARN("fail to process normal status", KR(ret), K(tenant_id));
  } else if (tenant_info.get_switchover_status().is_flashback_and_stay_standby_status()
      && OB_FAIL(process_flashback_status_(tenant_id, flashback_log_scn, tenant_info))) {
        LOG_WARN("fail to process flashback status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, GCTX.sql_proxy_, false, tenant_info))) {
    LOG_WARN("fail to load tenant info", KR(ret), K(tenant_id));
  }

  int tmp_ret = OB_SUCCESS;
  if (is_flashback_log_table_enabled && OB_TMP_FAIL(event_.end(tenant_info, ret))) {
    LOG_WARN("fail to end event", KR(ret), KR(tmp_ret), K(tenant_info));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObFlashbackStandbyLogCommand::process_normal_status_(
    const uint64_t tenant_id,
    const SCN &flashback_log_scn,
    ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t new_switchover_epoch = OB_INVALID_VERSION;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !flashback_log_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(flashback_log_scn));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    int64_t start_timestamp = ObTimeUtility::current_time();
    if (OB_FAIL(ObServiceNameCommand::clear_service_name(tenant_id))) {
      LOG_WARN("fail to clear service name", KR(ret), K(tenant_id));
    }
    int64_t cost =ObTimeUtility::current_time() - start_timestamp;
    (void) event_.set_clear_service_time_cost(cost);
  }

  if (FAILEDx(trans.start(GCTX.sql_proxy_, exec_tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, &trans, true, tenant_info))) {
    LOG_WARN("fail to load tenant info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_requirements_for_normal_status_(tenant_info, flashback_log_scn, &trans))) {
    LOG_WARN("fail to check requirement", KR(ret), K(tenant_info), K(flashback_log_scn));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role_in_trans(tenant_id, trans,
    tenant_info.get_switchover_epoch(), share::STANDBY_TENANT_ROLE, tenant_info.get_switchover_status(),
    share::FLASHBACK_AND_STAY_STANDBY_STATUS, new_switchover_epoch))) {
    LOG_WARN("fail to update tenant role", KR(ret), K(tenant_info));
  } else if (OB_FAIL(ObFlashbackLogSCNTableOperator::insert_flashback_log_scn(tenant_id,
      new_switchover_epoch, share::FLASHBACK_STANDBY_LOG_TYPE, flashback_log_scn, &trans))) {
    LOG_WARN("fail to insert flashback log scn", KR(ret), K(tenant_id), K(new_switchover_epoch), K(flashback_log_scn));
  }
  if (OB_UNLIKELY(!trans.is_started())) {
    LOG_WARN("the transaction is not started");
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("fail to commit the transaction", KR(ret), KR(tmp_ret), K(tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, GCTX.sql_proxy_, false, tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(tenant_info.get_switchover_epoch() != new_switchover_epoch)) {
      ret = OB_NEED_RETRY;
      LOG_WARN("concurrency problem: the switchover_epoch has been changed", KR(ret), K(tenant_info), K(new_switchover_epoch));
    }
  }
  return ret;
}
ERRSIM_POINT_DEF(ERRSIM_AFTER_FLASHBACK);
int ObFlashbackStandbyLogCommand::process_flashback_status_(
    const uint64_t tenant_id,
    const SCN &flashback_log_scn,
    ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  int64_t new_switchover_epoch = OB_INVALID_VERSION;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(check_requirements_for_flashback_status_(tenant_info, flashback_log_scn))) {
    LOG_WARN("fail to check_requirements_for_flashback_status_", KR(ret), K(tenant_info), K(flashback_log_scn));
  } else if (OB_FAIL(clear_fetched_log_cache_(tenant_id))) {
    LOG_WARN("fail to clear fetched log cache", KR(ret), K(tenant_id));
  } else if (OB_FAIL(do_flashback_(tenant_id, flashback_log_scn))) {
    LOG_WARN("fail to do flashback", KR(ret), K(tenant_id), K(flashback_log_scn));
  } else if (OB_UNLIKELY(ERRSIM_AFTER_FLASHBACK)) {
    ret = ERRSIM_AFTER_FLASHBACK;
    LOG_WARN("ERRSIM_AFTER_FLASHBACK opened", KR(ret));
  } else if (OB_FAIL(change_ls_access_mode_back_to_raw_rw_(tenant_id, tenant_info.get_switchover_epoch()))) {
    LOG_WARN(" fail to do change ls access mode back to rw", KR(ret), K(tenant_info));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(tenant_id, GCTX.sql_proxy_,
      tenant_info.get_switchover_epoch(), share::STANDBY_TENANT_ROLE, tenant_info.get_switchover_status(),
      share::NORMAL_SWITCHOVER_STATUS, new_switchover_epoch))) {
    LOG_WARN("fail to update tenant role", KR(ret), K(tenant_info));
  }
  return ret;
}

int ObFlashbackStandbyLogCommand::check_requirements_for_normal_status_(
  const share::ObAllTenantInfo &tenant_info,
  const share::SCN &flashback_log_scn,
  ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_info.get_tenant_id();
  ObTenantStatus tenant_status = TENANT_STATUS_MAX;
  if (OB_UNLIKELY(!tenant_info.is_valid() || !flashback_log_scn.is_valid_and_not_min() || OB_ISNULL(proxy))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_info), K(flashback_log_scn), KP(proxy));
  } else if (OB_FAIL(standby::ObStandbyService::get_tenant_status(tenant_id, tenant_status))) {
    LOG_WARN("failed to get tenant status", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_tenant_normal(tenant_status))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant status is not normal", KR(ret), K(tenant_id), K(tenant_status));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant status is not normal, FLASHBACK STANDBY LOG is");
  } else if (OB_UNLIKELY(!tenant_info.is_standby())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not standby tenant, FLASHBACK STANDBY LOG is not allowed", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant is not a standby tenant, FLASHBACK STANDBY LOG is");
  } else if (OB_UNLIKELY(!tenant_info.get_switchover_status().is_normal_status())) {
    ret = OB_NEED_RETRY;
    LOG_WARN("concurrency problem: the switchover_status has been changed", KR(ret), K(tenant_info));
  } else if (tenant_info.get_sync_scn() != tenant_info.get_recovery_until_scn()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("sync_scn != recovery_until_scn", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "RECOVER STANDBY CANCEL command has not been executed, FLASHBACK STANDBY LOG is");
  } else if (flashback_log_scn < tenant_info.get_sync_scn()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("flashback_log_scn < sync_scn", KR(ret), K(tenant_info), K(flashback_log_scn));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The speicified flashback_log_scn is smaller than the tenant's sync_scn , FLASHBACK STANDBY LOG is");
  }
  return ret;
}

int ObFlashbackStandbyLogCommand::check_requirements_for_flashback_status_(
  const share::ObAllTenantInfo &tenant_info,
  const share::SCN &flashback_log_scn)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_info.get_tenant_id();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_valid() || !flashback_log_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_info), K(flashback_log_scn));
  } else if (OB_UNLIKELY(!tenant_info.get_switchover_status().is_flashback_and_stay_standby_status())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("switchover status not matched", KR(ret), K(tenant_info));
  } else if (OB_FAIL(check_restore_source_empty_(tenant_id, GCTX.sql_proxy_))) {
    LOG_WARN("fail to double check whether log restore source is empty", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObFlashbackLogSCNTableOperator::check_flashback_log_scn_based_on_switchover_epoch(
      tenant_id, tenant_info.get_switchover_epoch(), flashback_log_scn, GCTX.sql_proxy_))) {
    LOG_WARN("fail to check_flashback_log_scn_based_on_switchover_epoch", KR(ret), K(tenant_info), K(flashback_log_scn));
  }
  return ret;
}

int ObFlashbackStandbyLogCommand::check_restore_source_empty_(const uint64_t tenant_id, ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  ObLogRestoreSourceMgr restore_source_mgr;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not user tenant", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_FAIL(restore_source_mgr.init(tenant_id, proxy))) {
    LOG_WARN("failed to init restore_source_mgr", KR(ret), K(tenant_id));
  } else {
    ObLogRestoreSourceItem item;
    int check_ret = restore_source_mgr.get_source(item);
    if (OB_SUCCESS == check_ret) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("log restore source is not empty", KR(ret), K(tenant_id), K(item));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "LOG_RESTORE_SOURCE is not empty, FLASHBACK STANDBY LOG is");
    } else if (OB_ENTRY_NOT_EXIST == check_ret) {
      // check ok
    } else {
      ret = check_ret;
      LOG_WARN("fail to check log restore source", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObFlashbackStandbyLogCommand::clear_fetched_log_cache_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t start_timestamp = ObTimeUtility::current_time();
  ObArray<ObAddr> tenant_online_servers;
  ObClearFetchedLogCacheArg arg;
  ObArray<int> return_code_array;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObServerCheckUtils::check_offline_and_get_tenants_servers(tenant_id,
      false /* allow_temp_offline */, tenant_online_servers, "FLASHBACK STANDBY LOG"))) {
    // ensure the tenant has no units on temp. offline servers
    LOG_WARN("fail to execute check_and_get_tenants_online_servers", KR(ret), K(tenant_id));
  } else if (OB_FAIL(arg.init(tenant_id))) {
    LOG_WARN("fail to init arg", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.srv_rpc_proxy_ is null", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else {
    const uint64_t group_id = share::OBCG_DBA_COMMAND;
    ObClearFetchedLogCacheProxy proxy(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::clear_fetched_log_cache);
    int tmp_ret = OB_SUCCESS;
    ObTimeoutCtx ctx;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_online_servers.count(); i++) {
      const ObAddr &server = tenant_online_servers.at(i);
      if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
      } else if (OB_FAIL(proxy.call(server, ctx.get_timeout(), GCONF.cluster_id, tenant_id, group_id, arg))) {
        LOG_WARN("failed to send rpc", KR(ret), KR(tmp_ret), K(server), K(ctx), K(tenant_id), K(arg));
      }
    }
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    if (FAILEDx(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
    }
    ARRAY_FOREACH_X(proxy.get_results(), idx, cnt, OB_SUCC(ret)) {
      const ObClearFetchedLogCacheRes *result = proxy.get_results().at(idx);
      const ObAddr &dest_addr = proxy.get_dests().at(idx);
      ret = return_code_array.at(idx);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to send rpc", KR(ret), K(dest_addr), K(idx));
        if (OB_TENANT_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), KP(result));
      } else if (OB_UNLIKELY(!result->is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid result", KR(ret), KPC(result), K(dest_addr));
      }
    }
  }
  int64_t cost =ObTimeUtility::current_time() - start_timestamp;
  FLOG_INFO("clear_fetched_log_cache", KR(ret), K(tenant_id), K(tenant_online_servers), K(cost));
  event_.set_clear_fetched_log_cache_cost(cost);
  return ret;
}

int ObFlashbackStandbyLogCommand::clear_local_fetched_log_cache(
    const ObClearFetchedLogCacheArg &arg,
    ObClearFetchedLogCacheRes &res)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.get_tenant_id();
  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  LOG_TRACE("receive a clear_local_fetched_log_cache request", K(arg));
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invaild", KR(ret), K(arg));
  } else if (tenant_id != MTL_ID() && OB_FAIL(guard.switch_to(tenant_id))) {
    LOG_WARN("switch tenant failed", KR(ret), K(arg));
  } else {
    logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
    if (OB_ISNULL(log_service) || OB_ISNULL(log_service->get_log_restore_service())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log_service or log_restore_service is null", KR(ret), KP(log_service));
    } else if (OB_FAIL(log_service->get_log_restore_service()->restart())) {
      LOG_WARN("fail to restart log_restore_service", KR(ret), K(arg));
    } else if (OB_FAIL(res.init(tenant_id, GCONF.self_addr_))) {
      LOG_WARN("fail to init res", KR(ret), K(tenant_id), K(GCONF.self_addr_));
    }
  }
  const int64_t time_cost = ::oceanbase::common::ObTimeUtility::current_time() - now;
  FLOG_INFO("clear_local_fetched_log_cache", KR(ret), K(arg), K(res), K(time_cost));
  return ret;
}

int ObFlashbackStandbyLogCommand::do_flashback_(
    const uint64_t tenant_id,
    const share::SCN &flashback_log_scn)
{
  int ret = OB_SUCCESS;
  int64_t start_timestamp = ObTimeUtility::current_time();
  ObTimeoutCtx ctx;
  logservice::ObLogService *log_service = NULL;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !flashback_log_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(flashback_log_scn));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to get MTL log_service", KR(ret), K(tenant_id));
  } else if (OB_FAIL(log_service->flashback(tenant_id, flashback_log_scn, ctx.get_timeout()))) {
    LOG_WARN("fail to flashback", KR(ret), K(tenant_id));
  }
  int64_t cost =ObTimeUtility::current_time() - start_timestamp;
  (void) event_.set_flashback_log_time_cost(cost);
  return ret;
}

int ObFlashbackStandbyLogCommand::change_ls_access_mode_back_to_raw_rw_(const uint64_t tenant_id, const uint64_t switchover_epoch)
{
  int ret = OB_SUCCESS;
  int64_t start_timestamp = ObTimeUtility::current_time();
  share::ObLSStatusInfoArray status_info_array;
  ObLSStatusOperator status_op;
  palf::AccessMode target_access_mode = logservice::ObLogService::get_palf_access_mode(STANDBY_TENANT_ROLE);
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || OB_INVALID_VERSION == switchover_epoch)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(switchover_epoch));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ or GCTX.srv_rpc_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(status_op.get_all_ls_status_by_order_for_flashback_log(
      tenant_id, status_info_array, *GCTX.sql_proxy_))) {
    LOG_WARN("fail to get_all_ls_status_by_order_for_flashback_log", KR(ret), K(tenant_id), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(event_.add_all_flashback_ls(status_info_array))) {
    LOG_WARN("fail to add all flashback ls", KR(ret), K(status_info_array));
  } else {
    ObLSAccessModeModifier ls_access_mode_modifier(tenant_id, switchover_epoch, SCN::base_scn(),
        share::SCN::min_scn(), target_access_mode, &status_info_array, GCTX.sql_proxy_, GCTX.srv_rpc_proxy_);
    if (OB_FAIL(ls_access_mode_modifier.change_ls_access_mode())) {
      LOG_WARN("fail to change ls access mode", KR(ret), K(tenant_id));
    }
  }
  int64_t cost =ObTimeUtility::current_time() - start_timestamp;
  (void) event_.set_change_access_mode_time_cost(cost);
  return ret;
}
}
}