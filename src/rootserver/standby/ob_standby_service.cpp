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

#include "ob_standby_service.h"              // ObStandbyService

#include "rootserver/ob_cluster_event.h"          // CLUSTER_EVENT_ADD_CONTROL
#include "rootserver/ob_tenant_event_def.h" // TENANT_EVENT
#include "rootserver/ob_ls_service_helper.h"//ObTenantLSInfo
#include "share/ob_global_stat_proxy.h"//ObGlobalStatProxy
#include "share/backup/ob_backup_config.h" // ObBackupConfigParserMgr
#include "storage/high_availability/ob_transfer_lock_utils.h" // ObMemberListLockUtils
#include "share/ob_license_utils.h"
#include "share/ob_sync_standby_dest_operator.h"
#include "src/rootserver/standby/ob_protection_mode_utils.h"
#include "share/ob_server_check_utils.h"
#include "share/ob_flashback_standby_log_struct.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace oceanbase;
using namespace common;
using namespace obrpc;
using namespace share;
using namespace rootserver;
using namespace storage;
using namespace tenant_event;
namespace standby
{

static share::ObLSID get_ls_id_from_elem(const share::ObLSStatusInfo &status_info)
{
  return status_info.ls_id_;
}

static share::ObLSID get_ls_id_from_elem(const share::ObLSID &input_ls_id)
{
  return input_ls_id;
}

template <typename LSStatusArray>
int ObStandbyService::get_checkpoints_by_rpc(
    const uint64_t tenant_id,
    const LSStatusArray &status_info_array,
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
      const auto &ls_status = status_info_array.at(i);
      const share::ObLSID ls_id = get_ls_id_from_elem(ls_status);
      const int64_t timeout_us = !THIS_WORKER.is_timeout_ts_valid() ?
        GCONF.rpc_timeout : THIS_WORKER.get_timeout_remain();
      if (OB_FAIL(GCTX.location_service_->get_leader(
        GCONF.cluster_id, tenant_id, ls_id, false, leader))) {
        LOG_WARN("failed to get leader", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(arg.init(tenant_id, ls_id, check_sync_to_latest))) {
        LOG_WARN("failed to init arg", KR(ret), K(tenant_id), K(ls_id));
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
  LOG_INFO("finish get_checkpoints_by_rpc", KR(ret), K(tenant_id), K(check_sync_to_latest),
      K(status_info_array), K(checkpoints));
  return ret;
}

template int ObStandbyService::get_checkpoints_by_rpc<share::ObLSStatusInfoIArray>(
    const uint64_t tenant_id,
    const share::ObLSStatusInfoIArray &status_info_array,
    const bool check_sync_to_latest,
    ObIArray<obrpc::ObCheckpoint> &checkpoints);

template int ObStandbyService::get_checkpoints_by_rpc<ObIArray<share::ObLSID> >(
    const uint64_t tenant_id,
    const ObIArray<share::ObLSID> &status_info_array,
    const bool check_sync_to_latest,
    ObIArray<obrpc::ObCheckpoint> &checkpoints);

int ObStandbyService::init(
           ObMySQLProxy *sql_proxy,
           share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy)
      || OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy), KP(schema_service));
  } else {
    sql_proxy_ = sql_proxy;
    schema_service_ = schema_service;
    inited_ = true;
  }
  return ret;
}

void ObStandbyService::destroy()
{
  if (OB_UNLIKELY(!inited_)) {
    LOG_INFO("ObStandbyService has been destroyed", K_(inited));
  } else {
    LOG_INFO("ObStandbyService begin to destroy", K_(inited));
    sql_proxy_ = NULL;
    schema_service_ = NULL;
    inited_ = false;
    LOG_INFO("ObStandbyService destroyed", K_(inited));
  }
}

int ObStandbyService::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Member variables is NULL", KR(ret), KP(sql_proxy_), KP(schema_service_));
  }
  return ret;
}
#define PRINT_TENANT_INFO(tenant_info, tenant_info_buf) \
    do { \
        int64_t pos = 0; \
        size_t tenant_buf_size = sizeof(tenant_info_buf) / sizeof(tenant_info_buf[0]); \
        if ((tenant_info).is_valid()) { \
            (void)databuff_print_multi_objs(tenant_info_buf, tenant_buf_size, pos, tenant_info); \
        } else { \
            (void)databuff_printf(tenant_info_buf, tenant_buf_size, pos, "NULL"); \
        } \
    } while(0)

void ObStandbyService::tenant_event_start_(
    const uint64_t switch_tenant_id, const obrpc::ObSwitchTenantArg &arg, int ret,
    int64_t begin_ts, const share::ObAllTenantInfo &tenant_info)
{
  char tenant_info_buf[MAX_TENANT_EVENT_VALUE_LENGTH] = "";
  PRINT_OBJ_INFO(tenant_info, tenant_info_buf);
  switch (arg.get_op_type()) {
      case ObSwitchTenantArg::SWITCH_TO_PRIMARY :
        TENANT_EVENT(switch_tenant_id, TENANT_ROLE_CHANGE, SWITCHOVER_TO_PRIMARY_START, begin_ts,
            ret, 0, ObHexEscapeSqlStr(arg.get_stmt_str()), ObHexEscapeSqlStr(tenant_info_buf));
        break;
      case ObSwitchTenantArg::SWITCH_TO_STANDBY :
        TENANT_EVENT(switch_tenant_id, TENANT_ROLE_CHANGE, SWITCHOVER_TO_STANDBY_START, begin_ts,
            ret, 0, ObHexEscapeSqlStr(arg.get_stmt_str()), ObHexEscapeSqlStr(tenant_info_buf));
        break;
      case ObSwitchTenantArg::FAILOVER_TO_PRIMARY :
        TENANT_EVENT(switch_tenant_id, TENANT_ROLE_CHANGE, FAILOVER_TO_PRIMARY_START, begin_ts,
            ret, 0, ObHexEscapeSqlStr(arg.get_stmt_str()), ObHexEscapeSqlStr(tenant_info_buf));
        break;
      default :break;
    }
}

void ObStandbyService::tenant_event_end_(
    const uint64_t switch_tenant_id, const obrpc::ObSwitchTenantArg &arg,
    int ret, int64_t cost, int64_t end_ts, const share::SCN switch_scn,
    ObTenantRoleTransCostDetail &cost_detail, ObTenantRoleTransAllLSInfo &all_ls)
{
  share::ObAllTenantInfo tenant_info;
  if (!THIS_WORKER.is_timeout()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObAllTenantInfoProxy::load_tenant_info(
      switch_tenant_id,
      sql_proxy_,
      false,
      tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(switch_tenant_id));
    }
  }
  char tenant_info_buf[MAX_TENANT_EVENT_VALUE_LENGTH] = "";
  PRINT_OBJ_INFO(tenant_info, tenant_info_buf);
  switch (arg.get_op_type()) {
    case ObSwitchTenantArg::SWITCH_TO_PRIMARY :
      TENANT_EVENT(switch_tenant_id, TENANT_ROLE_CHANGE, SWITCHOVER_TO_PRIMARY_END, end_ts,
          ret, cost, ObHexEscapeSqlStr(arg.get_stmt_str()), ObHexEscapeSqlStr(tenant_info_buf), switch_scn.get_val_for_inner_table_field(), cost_detail, all_ls);
      break;
    case ObSwitchTenantArg::SWITCH_TO_STANDBY :
      TENANT_EVENT(switch_tenant_id, TENANT_ROLE_CHANGE, SWITCHOVER_TO_STANDBY_END, end_ts,
          ret, cost, ObHexEscapeSqlStr(arg.get_stmt_str()), ObHexEscapeSqlStr(tenant_info_buf), switch_scn.get_val_for_inner_table_field(), cost_detail, all_ls);
      break;
    case ObSwitchTenantArg::FAILOVER_TO_PRIMARY :
      TENANT_EVENT(switch_tenant_id, TENANT_ROLE_CHANGE, FAILOVER_TO_PRIMARY_END, end_ts,
          ret, cost, ObHexEscapeSqlStr(arg.get_stmt_str()), ObHexEscapeSqlStr(tenant_info_buf), switch_scn.get_val_for_inner_table_field(), cost_detail, all_ls);
      break;
    default :break;
  }
}

int ObStandbyService::check_tenant_can_set_protection_mode_(ObMySQLTransaction &trans,
  const uint64_t user_tenant_id, const ObProtectionMode &target_protection_mode,
  const ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObProtectionModeUtils protectioin_mode_utils;
  bool is_sync = false;
  bool is_empty = false;
  ObSyncStandbyDestStruct sync_standby_dest_struct;
  ObProtectionStat protection_stat;
  ObTimeoutCtx ctx;
  // 1. primary tenant
  if (!tenant_info.is_primary()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant is not primary", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant is not primary, set protection mode is");
  // 2. not in switchover
  } else if (!tenant_info.is_normal_status()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant is in switchover status", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant is in switchover status, set protection mode is");
  } else if (OB_FAIL(protection_stat.init(tenant_info))) {
    LOG_WARN("failed to init protection stat", KR(ret), K(tenant_info));
  // 3. tenant is not downgrading if planning to change to maximum protection mode
  } else if (protection_stat.is_sync_to_async() && target_protection_mode.is_maximum_protection()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("protection stat is sync to async, cannot set protection mode", KR(ret), K(user_tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "protection stat is sync to async, set protection mode is");
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  }
  // 4. standby tenant is sync
  if (!target_protection_mode.is_maximum_protection() || OB_FAIL(ret)) {
  } else if (OB_FAIL(protectioin_mode_utils.init(user_tenant_id))) {
    LOG_WARN("failed to init protection mode utils", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(protectioin_mode_utils.wait_standby_tenant_sync(ctx))) {
    if (OB_TIMEOUT == ret) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("wait standby tenant sync timeout", KR(ret), K(user_tenant_id));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "wait standby tenant sync timeout, set protection mode is");
    } else {
      LOG_WARN("failed to wait standby tenant sync", KR(ret), K(user_tenant_id));
    }
  // 5. no ls is creating
  } else if (OB_FAIL(ObProtectionModeUtils::check_ls_status_for_upgrade_protection_level(
      user_tenant_id, trans))) {
    LOG_WARN("failed to check no ls is creating for set protection mode", KR(ret), K(user_tenant_id));
  }
  // 6. check sync standby dest is not empty
  if (OB_FAIL(ret)) {
  } else if (protection_stat.get_protection_mode().is_maximum_availability()) {
    // if current protection mode is maximum availability, sync_standby_dest is always valid
  } else if (target_protection_mode.is_maximum_performance()) {
    // if target protection mode is maximum performance, no need to check sync standby dest
  } else if (OB_FAIL(ObProtectionModeUtils::get_tenant_sync_standby_dest(user_tenant_id, trans,
      true/*for_update*/, is_empty, sync_standby_dest_struct))) {
    LOG_WARN("failed to get tenant sync standby dest", KR(ret), K(user_tenant_id));
  } else if (is_empty || !sync_standby_dest_struct.is_valid()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant sync standby dest is invalid or empty", KR(ret), K(user_tenant_id), K(is_empty), K(sync_standby_dest_struct));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant sync standby dest is invalid or empty, set protection mode is");
  }
  return ret;
}

int ObStandbyService::set_protection_mode(const share::ObSetProtectionModeArg &arg)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  ObMySQLTransaction trans;
  const uint64_t user_tenant_id = arg.get_tenant_id();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  ObProtectionMode target_protection_mode = arg.get_protection_mode();
  ObProtectionLevel target_protection_level;
  ObProtectionMode current_protection_mode;
  ObProtectionLevel current_protection_level;
  int64_t new_switchover_epoch = OB_INVALID_VERSION;
  bool protection_mode_enable = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (OB_FAIL(ObProtectionModeUtils::check_tenant_data_version_for_protection_mode(
          user_tenant_id, protection_mode_enable))) {
    LOG_WARN("failed to check tenant data version for protection mode", KR(ret), K(user_tenant_id));
  } else if (!protection_mode_enable) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant is not upgraded, set protection mode is not allowed", KR(ret), K(user_tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant is not upgraded, set protection mode");
  } else if (GCONF.enable_logservice) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("set protection mode in logservice is not supported", KR(ret), K(user_tenant_id),
        K(GCONF.enable_logservice));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set protection mode in logservice");
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg), K(user_tenant_id));
  } else if (!is_user_tenant(user_tenant_id)) {
    // checked in ObSetProtectionModeResolver::resolve
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant is not user tenant", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, meta_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(user_tenant_id, &trans, true/*for_update*/, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(user_tenant_id));
  } else if (OB_UNLIKELY(!tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant info is not valid", KR(ret), K(tenant_info));
  } else if (OB_FAIL(check_tenant_can_set_protection_mode_(trans, user_tenant_id,
    target_protection_mode, tenant_info))) {
    LOG_WARN("failed to check tenant can set protection mode", KR(ret), K(user_tenant_id), K(target_protection_mode), K(tenant_info));
  } else if (FALSE_IT(current_protection_mode = tenant_info.get_protection_mode())) {
  } else if (FALSE_IT(current_protection_level = tenant_info.get_protection_level())) {
  } else if (current_protection_mode == target_protection_mode) {
    LOG_INFO("protection mode is already the same, no need to set", KR(ret), K(user_tenant_id),
      K(arg), K(tenant_info));
  } else if (OB_FAIL(ObProtectionStat::get_next_protection_level(current_protection_mode, current_protection_level,
    target_protection_mode, target_protection_level))) {
    LOG_WARN("failed to get next protection level", KR(ret), K(current_protection_mode),
      K(current_protection_level), K(target_protection_mode));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_protection_mode_and_level(
      user_tenant_id, tenant_info.get_tenant_role(), &trans, tenant_info.get_switchover_epoch(), target_protection_mode,
      target_protection_level, tenant_info.get_switchover_epoch()/* min switchover epoch */,
      new_switchover_epoch))) {
    LOG_WARN("failed to update tenant protection mode and level", KR(ret), K(user_tenant_id),
      K(target_protection_mode), K(target_protection_level), K(tenant_info));
  } else if (new_switchover_epoch == tenant_info.get_switchover_epoch()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover epoch is not changed, need retry", KR(ret), K(user_tenant_id), K(tenant_info), K(new_switchover_epoch));
    LOG_USER_ERROR(OB_NEED_RETRY, "switchover epoch is not changed, need retry");
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", KR(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  if (FAILEDx(ObProtectionModeUtils::wait_protection_stat_steady(
          user_tenant_id, target_protection_mode, sql_proxy_))) {
    LOG_WARN("failed to wait protection stat steady", KR(ret), K(user_tenant_id));
  } else {
    LOG_INFO("protection stat is steady", KR(ret), K(user_tenant_id));
  }
  return ret;
}

int ObStandbyService::switch_tenant(const obrpc::ObSwitchTenantArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t begin_ts = ObTimeUtility::current_time();
  uint64_t switch_tenant_id = OB_INVALID_ID;
  bool is_verify = arg.get_is_verify();
  uint64_t compat_version = 0;
  ObAllTenantInfo tenant_info;
  share::SCN switch_scn = SCN::min_scn();
  ObTenantRoleTransCostDetail cost_detail;
  ObTenantRoleTransAllLSInfo all_ls;
  cost_detail.set_start(begin_ts);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(get_target_tenant_id(arg.get_tenant_name(), arg.get_exec_tenant_id(), switch_tenant_id))) {
    LOG_WARN("failed to get_target_tenant_id", KR(ret), K(switch_tenant_id), K(arg));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(switch_tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(arg));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_1_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Tenant COMPATIBLE is below 4.1.0.0, switch tenant is not supported", KR(ret));
    TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_NOT_SUPPORTED, "Tenant COMPATIBLE is below 4.1.0.0", arg.get_op_type());
  } else if (OB_UNLIKELY(is_verify && !(compat_version >= DATA_VERSION_4_3_3_0
      || (compat_version >= DATA_VERSION_4_2_2_0 && compat_version < DATA_VERSION_4_3_0_0)
      || (compat_version >= MOCK_DATA_VERSION_4_2_1_8 && compat_version < DATA_VERSION_4_2_2_0)))) {
    ret = common::OB_NOT_SUPPORTED;
    LOG_WARN("only (version >= 4_2_1_8 and version < 4_2_2_0) "
      "or version >= 4_2_2_0 and version < 4_3_0_0 "
      "or version >= 4_3_3_0 support this operation", KR(ret), K(compat_version));
  } else if (OB_FAIL(check_if_tenant_status_is_normal_(switch_tenant_id, arg.get_op_type()))) {
    LOG_WARN("fail to check if tenant status is normal", KR(ret), K(switch_tenant_id), K(arg));
  } else if (OB_FAIL(all_ls.init())) {
    LOG_WARN("fail to init all_ls", KR(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
      switch_tenant_id,
      sql_proxy_,
      false,
      tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(switch_tenant_id));
  } else {
    if (!is_verify) {
      (void) tenant_event_start_(switch_tenant_id, arg, ret, begin_ts, tenant_info);
    }

    switch (arg.get_op_type()) {
      case ObSwitchTenantArg::SWITCH_TO_PRIMARY :
        if (OB_FAIL(switch_to_primary(switch_tenant_id, arg.get_op_type(), is_verify,
            switch_scn, cost_detail, all_ls))) {
          LOG_WARN("failed to switch_to_primary", KR(ret), K(switch_tenant_id), K(arg));
        }
        break;
      case ObSwitchTenantArg::SWITCH_TO_STANDBY :
        if (OB_FAIL(switch_to_standby(switch_tenant_id, arg.get_op_type(), is_verify,
            tenant_info, switch_scn, cost_detail, all_ls))) {
          LOG_WARN("failed to switch_to_standby", KR(ret), K(switch_tenant_id), K(arg));
        }
        break;
      case ObSwitchTenantArg::FAILOVER_TO_PRIMARY :
        if (OB_FAIL(failover_to_primary(switch_tenant_id, arg.get_op_type(), is_verify,
            tenant_info, switch_scn, cost_detail, all_ls))) {
          LOG_WARN("failed to failover_to_primary", KR(ret), K(switch_tenant_id), K(arg));
        }
        break;
      default :
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unkown op_type", KR(ret), K(arg));
    }
    // reset return code to TIMEOUT, to prevent the error code which not user unfriendly
    if (THIS_WORKER.is_timeout() && OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
      ret = OB_TIMEOUT;
    }
    int64_t end_ts = ObTimeUtility::current_time();
    int64_t cost = end_ts - begin_ts;
    cost_detail.set_end(end_ts);
    FLOG_INFO("switch tenant end", KR(ret), K(arg), K(cost), K(cost_detail), K(all_ls));
    if (!is_verify) {
      (void) tenant_event_end_(switch_tenant_id, arg, ret, cost, end_ts, switch_scn, cost_detail, all_ls);
    }
  }
  return ret;
}

int ObStandbyService::failover_to_primary(
    const uint64_t tenant_id,
    const obrpc::ObSwitchTenantArg::OpType &switch_optype,
    const bool is_verify,
    const share::ObAllTenantInfo &tenant_info,
    share::SCN &switch_scn,
    ObTenantRoleTransCostDetail &cost_detail,
    ObTenantRoleTransAllLSInfo &all_ls)
{
  int ret = OB_SUCCESS;
  ObTenantRoleTransitionService role_transition_service;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_UNLIKELY(obrpc::ObSwitchTenantArg::OpType::INVALID == switch_optype)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid switch_optype", KR(ret), K(switch_optype));
  } else if (OB_FAIL(role_transition_service.init(
      tenant_id,
      switch_optype,
      is_verify,
      sql_proxy_,
      GCTX.srv_rpc_proxy_,
      &cost_detail,
      &all_ls))) {
    LOG_WARN("fail to init role_transition_service", KR(ret), K(tenant_id), K(switch_optype),
        KP(sql_proxy_), KP(GCTX.srv_rpc_proxy_), K(cost_detail), K(all_ls));
  } else if (tenant_info.is_primary() && tenant_info.is_normal_status()) {
    LOG_INFO("already is primary tenant, no need switch", K(tenant_info));
  } else if (tenant_info.get_restore_data_mode().is_remote_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant restore data mode is remote, failover is not allowed", KR(ret), K(tenant_id), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant restore data mode is remote. Operation is");
  } else {
    if (OB_FAIL(role_transition_service.failover_to_primary())) {
      LOG_WARN("failed to failover to primary", KR(ret), K(tenant_id));
    }
    switch_scn = tenant_info.get_sync_scn();
  }

  return ret;
}

int ObStandbyService::get_target_tenant_id(
    const ObString &tenant_name,
    const uint64_t exec_tenant_id,
    uint64_t &switch_tenant_id)
{
  int ret = OB_SUCCESS;
  switch_tenant_id = OB_INVALID_ID;
  if (OB_INVALID_TENANT_ID == exec_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(exec_tenant_id), KR(ret));
  } else if (tenant_name.empty()) {
    if (!is_user_tenant(exec_tenant_id)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("can't operate tenant without tenant name using SYS/meta tenant session", KR(ret), K(tenant_name), K(exec_tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "omitting tenant name is ");
    } else {
      switch_tenant_id = exec_tenant_id;
    }
  } else {
    // tenant_name not empty
    if (OB_SYS_TENANT_ID != exec_tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("can't specify tenant name using user tenant session", KR(ret), K(tenant_name), K(exec_tenant_id));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name, please don't specify tenant name");
    } else {
      if (OB_ISNULL(schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid schema service", KR(ret), KP(schema_service_));
      } else {
        share::schema::ObSchemaGetterGuard guard;
        if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
          LOG_WARN("get_schema_guard failed", KR(ret));
        } else if (OB_FAIL(guard.get_tenant_id(tenant_name, switch_tenant_id))) {
          LOG_WARN("get_tenant_id failed", KR(ret), K(tenant_name), K(exec_tenant_id));
        } else if (OB_UNLIKELY(!is_valid_tenant_id(switch_tenant_id) || !is_user_tenant(switch_tenant_id))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("only support switch user tenant", KR(ret), K(tenant_name), K(exec_tenant_id), K(switch_tenant_id));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name, only support operating user tenant");
        }
      }
    }
  }
  return ret;
}

int ObStandbyService::recover_tenant(const obrpc::ObRecoverTenantArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t begin_time = ObTimeUtility::current_time();
  uint64_t tenant_id = OB_INVALID_ID;
  const char *alter_cluster_event = "recover_tenant";
  uint64_t compat_version = 0;
  CLUSTER_EVENT_ADD_CONTROL_START(ret, alter_cluster_event, "stmt_str", arg.get_stmt_str());

  if (OB_FAIL(ObLicenseUtils::check_standby_allowed())) {
    LOG_WARN("check standby allowed failed", KR(ret));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(get_target_tenant_id(arg.get_tenant_name(), arg.get_exec_tenant_id(), tenant_id))) {
    LOG_WARN("failed to get_target_tenant_id", KR(ret), K(tenant_id), K(arg));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Tenant COMPATIBLE is below 4.1.0.0, recover tenant is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Tenant COMPATIBLE is below 4.1.0.0, recover tenant is");
  } else if (OB_FAIL(do_recover_tenant(tenant_id, share::NORMAL_SWITCHOVER_STATUS, arg.get_type(),
                                       arg.get_recovery_until_scn()))) {
    LOG_WARN("failed to do_recover_tenant", KR(ret), K(tenant_id), K(arg));
  }

  int64_t cost = ObTimeUtility::current_time() - begin_time;
  CLUSTER_EVENT_ADD_CONTROL_FINISH(ret, alter_cluster_event,
      K(cost),
      "stmt_str", arg.get_stmt_str());

  return ret;
}

int ObStandbyService::get_tenant_status(
    const uint64_t tenant_id,
    ObTenantStatus &status)
{
  int ret = OB_SUCCESS;
  status = TENANT_STATUS_MAX;
  if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support get user tenant status", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant id, only support operating user tenant");
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
                  "SELECT status FROM %s WHERE tenant_id = %lu",
                   OB_ALL_TENANT_TNAME, tenant_id))) {
        LOG_WARN("assign sql string failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(result.get_result()->next())) {
        LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(sql));
        if (OB_ITER_END == ret) {
          ret = OB_TENANT_NOT_EXIST;
        }
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(tenant_id), K(sql));
      } else {
        ObString tenant_status_str("");
        ObString default_tenant_status_str("NORMAL");
        bool skip_null_error = false;

        EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result.get_result(), "status", tenant_status_str,
            skip_null_error, ObSchemaService::g_ignore_column_retrieve_error_, default_tenant_status_str);

        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get result", KR(ret), K(tenant_id), K(sql));
        } else if (OB_FAIL(schema::get_tenant_status(tenant_status_str, status))) {
          LOG_WARN("fail to get tenant status", KR(ret), K(tenant_status_str), K(tenant_id), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObStandbyService::check_if_tenant_status_is_normal_(const uint64_t tenant_id, const RoleTransType op_type)
{
  int ret = OB_SUCCESS;
  ObTenantStatus tenant_status = TENANT_STATUS_MAX;
  if (OB_FAIL(get_tenant_status(tenant_id, tenant_status))) {
    LOG_WARN("failed to get tenant status", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_tenant_normal(tenant_status))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant status is not normal", KR(ret), K(tenant_id), K(tenant_status));
    TENANT_ROLE_TRANS_USER_ERR_WITH_SUFFIX(OB_OP_NOT_ALLOW, "tenant status is not normal", op_type);
  }
  return ret;
}

int ObStandbyService::do_recover_tenant(
    const uint64_t tenant_id,
    const share::ObTenantSwitchoverStatus &working_sw_status,
    const obrpc::ObRecoverTenantArg::RecoverType &recover_type,
    const share::SCN &recovery_until_scn)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  uint64_t tenant_version = 0;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  common::ObMySQLTransaction trans;
  ObTenantStatus tenant_status = TENANT_STATUS_MAX;
  ObLSRecoveryStatOperator ls_recovery_operator;
  ObLSRecoveryStat sys_ls_recovery;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (!obrpc::ObRecoverTenantArg::is_valid(recover_type, recovery_until_scn)
             || !working_sw_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(recover_type), K(recovery_until_scn), KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_status(tenant_id, tenant_status))) {
    LOG_WARN("failed to get tenant status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_version))) {
    LOG_WARN("failed to get tenant min version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id), K(tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, &trans, true, tenant_info))) {
    LOG_WARN("failed to load all tenant info", KR(ret), K(tenant_id));
  } else if (!tenant_info.is_standby()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant role is not STANDBY", K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant role is not STANDBY, recover is");
  } else if (OB_UNLIKELY(tenant_info.get_switchover_status().is_flashback_and_stay_standby_status()
      && obrpc::ObRecoverTenantArg::RecoverType::CANCEL != recover_type)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("recover when the tenant's switchover_status is not allowed",
        KR(ret), K(recover_type), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW,"Recover when the tenant's switchover_status is "
        "'FLASHBACK AND STAY STANDBY' is");
  } else if (tenant_info.get_switchover_status() != working_sw_status) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("unexpected tenant switchover status", KR(ret), K(working_sw_status), K(tenant_info));
  } else if (obrpc::ObRecoverTenantArg::RecoverType::UNTIL == recover_type
              && recovery_until_scn < tenant_info.get_sync_scn()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("recover before tenant sync_scn is not allow", KR(ret), K(tenant_info),
             K(tenant_id), K(recover_type), K(recovery_until_scn));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "recover before tenant sync_scn sync_scn is");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_version))) {
    LOG_WARN("failed to get tenant min version", KR(ret), K(tenant_id));
  } else if (tenant_version >= DATA_VERSION_4_2_1_0) {
    //noting, no need check sys ls recovery stat
  } else if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(tenant_id, share::SYS_LS,
                     true /*for_update*/, sys_ls_recovery, trans))) {
    LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id));
  } else if (obrpc::ObRecoverTenantArg::RecoverType::UNTIL == recover_type
              && recovery_until_scn < sys_ls_recovery.get_sync_scn()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("recover before SYS LS sync_scn is not allow", KR(ret), K(tenant_info),
             K(tenant_id), K(recover_type), K(recovery_until_scn), K(sys_ls_recovery));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "recover before tenant SYS LS sync_scn is");
  }
  if (OB_FAIL(ret)) {
  } else if (is_tenant_normal(tenant_status)) {
    const SCN &recovery_until_scn_to_set = obrpc::ObRecoverTenantArg::RecoverType::UNTIL == recover_type ?
      recovery_until_scn : SCN::max(tenant_info.get_sync_scn(), sys_ls_recovery.get_sync_scn());
    if (tenant_info.get_recovery_until_scn() == recovery_until_scn_to_set) {
      LOG_WARN("recovery_until_scn is same with original", KR(ret), K(tenant_info), K(tenant_id),
               K(recover_type), K(recovery_until_scn));
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_recovery_until_scn(
                  tenant_id, trans, tenant_info.get_switchover_epoch(), recovery_until_scn_to_set))) {
      LOG_WARN("failed to update_tenant_recovery_until_scn", KR(ret), K(tenant_id), K(recover_type),
               K(recovery_until_scn), K(recovery_until_scn_to_set));
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant status is not normal, recover is not allowed", KR(ret), K(tenant_id),
             K(recover_type), K(recovery_until_scn), K(tenant_status));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant status is not normal, recover is");
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObStandbyService::switch_to_primary(
    const uint64_t tenant_id,
    const obrpc::ObSwitchTenantArg::OpType &switch_optype,
    const bool is_verify,
    share::SCN &switch_scn,
    ObTenantRoleTransCostDetail &cost_detail,
    ObTenantRoleTransAllLSInfo &all_ls)
{
  int ret = OB_SUCCESS;
  int64_t begin_time = ObTimeUtility::current_time();
  ObTenantRoleTransitionService role_transition_service;
  ObAllTenantInfo tenant_info;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(sql_proxy_));
  } else if (OB_UNLIKELY(obrpc::ObSwitchTenantArg::OpType::INVALID == switch_optype)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid switch_optype", KR(ret), K(switch_optype));
  } else if (OB_FAIL(role_transition_service.init(
      tenant_id,
      switch_optype,
      is_verify,
      sql_proxy_,
      GCTX.srv_rpc_proxy_,
      &cost_detail,
      &all_ls))) {
    LOG_WARN("fail to init role_transition_service", KR(ret), K(tenant_id), K(switch_optype),
        KP(sql_proxy_), KP(GCTX.srv_rpc_proxy_), K(cost_detail), K(all_ls));
  } else {
    (void)role_transition_service.set_switchover_epoch(tenant_info.get_switchover_epoch());
    if (OB_FAIL(role_transition_service.failover_to_primary())) {
      LOG_WARN("fail to failover to primary", KR(ret), K(tenant_id));
    }
    switch_scn = role_transition_service.get_so_scn();
  }
  return ret;
}

int ObStandbyService::switch_to_standby(
    const uint64_t tenant_id,
    const obrpc::ObSwitchTenantArg::OpType &switch_optype,
    const bool is_verify,
    share::ObAllTenantInfo &tenant_info,
    share::SCN &switch_scn,
    ObTenantRoleTransCostDetail &cost_detail,
    ObTenantRoleTransAllLSInfo &all_ls)
{
  int ret = OB_SUCCESS;
  const int32_t group_id = share::OBCG_DBA_COMMAND;
  ObProtectionStat protection_stat;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_UNLIKELY(obrpc::ObSwitchTenantArg::OpType::INVALID == switch_optype)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid switch_optype", KR(ret), K(switch_optype));
  } else if (tenant_info.is_standby() && tenant_info.is_normal_status()) {
    LOG_INFO("already is standby tenant, no need switch", K(tenant_id), K(tenant_info));
  } else {
    switch(tenant_info.get_switchover_status().value()) {
      case share::ObTenantSwitchoverStatus::NORMAL_STATUS: {
        if (OB_FAIL(ret)) {
        } else if (!tenant_info.is_primary()) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("unexpected tenant role", KR(ret), K(tenant_info));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant role is not PRIMARY, switchover to standby is");
        } else if (OB_UNLIKELY(!tenant_info.get_recovery_until_scn().is_max())) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("recovery_until_scn has been changed ", KR(ret), K(tenant_id), K(tenant_info));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "recovery_until_scn has been changed, switchover to standby is");
        } else if (OB_FAIL(protection_stat.init(tenant_info))) {
          LOG_WARN("failed to init protection stat", KR(ret), K(tenant_info));
        } else if (!protection_stat.is_steady()) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("protection stat is not steady, switchover to standby is not allowed", KR(ret),
              K(tenant_info), K(protection_stat));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "protection stat is not steady, switchover to standby is");
        } else if (is_verify) {
          // skip
        } else if (OB_FAIL(update_tenant_status_before_sw_to_standby_(
                            tenant_info.get_switchover_status(),
                            tenant_info.get_tenant_role(),
                            tenant_info.get_switchover_epoch(),
                            tenant_id,
                            tenant_info))) {
          LOG_WARN("failed to update_tenant_status_before_sw_to_standby_", KR(ret), K(tenant_info),
                            K(tenant_id));
        }
      }
      case share::ObTenantSwitchoverStatus::PREPARE_SWITCHING_TO_STANDBY_STATUS: {
        if (OB_FAIL(ret) || is_verify) {
        } else if (OB_FAIL(switch_to_standby_prepare_ls_status_(tenant_id,
                                                                tenant_info.get_switchover_status(),
                                                                tenant_info.get_switchover_epoch(),
                                                                tenant_info))) {
          LOG_WARN("failed to switch_to_standby_prepare_ls_status_", KR(ret), K(tenant_id), K(tenant_info));
        }
      }
      case share::ObTenantSwitchoverStatus::SWITCHING_TO_STANDBY_STATUS: {
        ObTenantRoleTransitionService role_transition_service;
        if (OB_FAIL(ret) || is_verify) {
        } else if (OB_FAIL(role_transition_service.init(
            tenant_id,
            switch_optype,
            is_verify,
            sql_proxy_,
            GCTX.srv_rpc_proxy_,
            &cost_detail,
            &all_ls))) {
          LOG_WARN("fail to init role_transition_service", KR(ret), K(tenant_id), K(switch_optype),
              KP(sql_proxy_), KP(GCTX.srv_rpc_proxy_), K(cost_detail), K(all_ls));
        } else {
          uint64_t compat_version = 0;
          ObGlobalStatProxy global_proxy(*sql_proxy_, gen_meta_tenant_id(tenant_id));
          (void)role_transition_service.set_switchover_epoch(tenant_info.get_switchover_epoch());
          if (OB_FAIL(role_transition_service.do_switch_access_mode_to_raw_rw(tenant_info))) {
            LOG_WARN("failed to do_switch_access_mode", KR(ret), K(tenant_id), K(tenant_info));
          } else if (OB_FAIL(global_proxy.get_current_data_version(compat_version))) {
            LOG_WARN("failed to get current data version", KR(ret), K(tenant_id));
          } else if (compat_version < DATA_VERSION_4_2_0_0) {
            //Regardless of the data_version change and switchover concurrency scenario,
            //if there is concurrency, the member_list lock that has not been released by the operation and maintenance process
          } else if (OB_FAIL(ObMemberListLockUtils::unlock_member_list_when_switch_to_standby(tenant_id, group_id, *sql_proxy_))) {
            LOG_WARN("failed to unlock member list when switch to standby", K(ret), K(tenant_id));
          }
          if (FAILEDx(role_transition_service.switchover_update_tenant_status(tenant_id,
                                                     false /* switch_to_standby */,
                                                     share::STANDBY_TENANT_ROLE,
                                                     tenant_info.get_switchover_status(),
                                                     share::NORMAL_SWITCHOVER_STATUS,
                                                     tenant_info.get_switchover_epoch(),
                                                     tenant_info))) {
            LOG_WARN("fail to switchover_update_tenant_status", KR(ret), K(tenant_id), K(tenant_info));
          } else {
            (void)role_transition_service.broadcast_tenant_info(
                  ObTenantRoleTransitionConstants::SWITCH_TO_STANDBY_LOG_MOD_STR);
          }
          switch_scn = role_transition_service.get_so_scn();
        }
        break;
      }
      default: {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("switchover status not match", KR(ret), K(tenant_info), K(tenant_id));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "switchover status not match, switchover to standby is");
        break;
      }
    }
  }

  return ret;
}

int ObStandbyService::update_tenant_status_before_sw_to_standby_(
    const ObTenantSwitchoverStatus cur_switchover_status,
    const ObTenantRole cur_tenant_role,
    const int64_t cur_switchover_epoch,
    const uint64_t tenant_id,
    ObAllTenantInfo &new_tenant_info)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObAllTenantInfo tenant_info;
  int64_t new_switchover_ts = common::OB_INVALID_TIMESTAMP;

  if (OB_UNLIKELY(!cur_switchover_status.is_valid()
                  || !cur_tenant_role.is_valid()
                  || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(cur_switchover_status), K(cur_tenant_role));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else {
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                    tenant_id, &trans, true, tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(!tenant_info.get_recovery_until_scn().is_max())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("recovery_until_scn has been changed ", KR(ret), K(tenant_id), K(tenant_info));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "recovery_until_scn has been changed, switchover to standby is");
    } else if (cur_switchover_status != tenant_info.get_switchover_status()) {
      ret = OB_NEED_RETRY;
      LOG_WARN("tenant not expect switchover status", KR(ret), K(tenant_info), K(cur_switchover_status));
    } else if (cur_tenant_role != tenant_info.get_tenant_role()) {
      ret = OB_NEED_RETRY;
      LOG_WARN("tenant not expect tenant role", KR(ret), K(tenant_info), K(cur_tenant_role));
    } else if (cur_switchover_epoch != tenant_info.get_switchover_epoch()) {
      ret = OB_NEED_RETRY;
      LOG_WARN("tenant not expect switchover epoch", KR(ret), K(tenant_info), K(cur_switchover_epoch));
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role_in_trans(
                  tenant_id, trans, cur_switchover_epoch,
                  PRIMARY_TENANT_ROLE, cur_switchover_status,
                  share::PREP_SWITCHING_TO_STANDBY_SWITCHOVER_STATUS, new_switchover_ts))) {
      LOG_WARN("failed to update tenant role", KR(ret), K(tenant_id), K(cur_switchover_epoch), K(tenant_info));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                    tenant_id, &trans, true, new_tenant_info))) {
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
  CLUSTER_EVENT_ADD_LOG(ret, "update tenant before switchover to standby",
      "tenant id", tenant_id,
      "old switchover#", cur_switchover_epoch,
      "new switchover#", tenant_info.get_switchover_epoch(),
      K(cur_switchover_status), K(cur_tenant_role));
  return ret;
}

int ObStandbyService::switch_to_standby_prepare_ls_status_(
    const uint64_t tenant_id,
    const ObTenantSwitchoverStatus &status,
    const int64_t switchover_epoch,
    ObAllTenantInfo &new_tenant_info)
{
  int ret = OB_SUCCESS;
  ObLSAttr sys_ls_attr;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *tenant_schema = NULL;
  int64_t new_switchover_epoch = OB_INVALID_VERSION;

  if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service_ is NULL", KR(ret));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(!status.is_prepare_switching_to_standby_status())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switchover status not match, switchover to standby not allow", KR(ret), K(status));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "switchover status not match, switchover to standby is");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant ids", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
  } else {
    ObTenantLSInfo tenant_stat(GCTX.sql_proxy_, tenant_schema, tenant_id);
    /* lock SYS_LS to get accurate LS list, then fix ls status to make ls status consistency
       between __all_ls&__all_ls_status.
       Refer to ls operator, insert/update/delete of ls table are executed in the SYS_LS lock
       and normal switchover status */
    if (OB_FAIL(ObLSServiceHelper::process_status_to_steady(true/* lock_sys_ls */,
                                   share::PREP_SWITCHING_TO_STANDBY_SWITCHOVER_STATUS,
                                   switchover_epoch,
                                   tenant_stat))) {
      LOG_WARN("failed to process_ls_status_missmatch", KR(ret));
    } else if (FALSE_IT(DEBUG_SYNC(BEFORE_SWITCHING_TO_STANDBY))) {
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(
                    tenant_id, sql_proxy_, switchover_epoch,
                    share::STANDBY_TENANT_ROLE, status,
                    share::SWITCHING_TO_STANDBY_SWITCHOVER_STATUS, new_switchover_epoch))) {
      LOG_WARN("failed to update tenant role", KR(ret), K(tenant_id), K(switchover_epoch));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                       tenant_id, sql_proxy_, false, new_tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(new_tenant_info.get_switchover_epoch() != new_switchover_epoch)) {
      ret = OB_NEED_RETRY;
      LOG_WARN("switchover is concurrency", KR(ret), K(switchover_epoch), K(new_tenant_info));
    }

    DEBUG_SYNC(SWITCHING_TO_STANDBY);
  }

  return ret;
}

int ObStandbyService::write_upgrade_barrier_log(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(write_barrier_log_(transaction::ObTxDataSourceType::STANDBY_UPGRADE,
                                 trans, tenant_id, data_version))) {
    LOG_WARN("fail to write upgrade barrier log", K(ret), K(tenant_id), K(data_version));
  }

  return ret;
}

int ObStandbyService::write_upgrade_data_version_barrier_log(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;

  if (ODV_MGR.is_enable_compatible_monotonic() &&
          OB_FAIL(write_barrier_log_(transaction::ObTxDataSourceType::STANDBY_UPGRADE_DATA_VERSION,
                                     trans, tenant_id, data_version))) {
    LOG_WARN("fail to write upgrade data_version barrier log", K(ret), K(tenant_id),
             K(data_version));
  }

  return ret;
}

int ObStandbyService::write_barrier_log_(
    const transaction::ObTxDataSourceType type,
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  ObStandbyUpgrade primary_data_version(data_version);
  observer::ObInnerSQLConnection *inner_conn =
      static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (OB_ISNULL(inner_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection or trans service is null", KR(ret), KP(inner_conn));
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not user tenant_id", KR(ret), K(tenant_id));
  } else if (!ObClusterVersion::check_version_valid_(data_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data_version", KR(ret), K(data_version));
  } else {
    const int64_t length = primary_data_version.get_serialize_size();
    char *buf = NULL;
    int64_t pos = 0;
    ObArenaAllocator allocator("StandbyUpgrade");
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", KR(ret), K(length));
    } else if (OB_FAIL(primary_data_version.serialize(buf, length, pos))) {
      LOG_WARN("failed to serialize", KR(ret), K(primary_data_version), K(length), K(pos));
    } else if (OB_UNLIKELY(pos > length)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("serialize error", KR(ret), K(pos), K(length), K(primary_data_version));
    } else if (OB_FAIL(inner_conn->register_multi_data_source(
                   tenant_id, SYS_LS, type, buf, length))) {
      LOG_WARN("failed to register tx data", KR(ret), K(tenant_id));
    }
    LOG_INFO("write_barrier_log finished", KR(ret), K(tenant_id), K(type),
             K(primary_data_version), K(length), KPHEX(buf, length));
  }
  return ret;
}

int ObStandbyService::check_can_create_standby_tenant(
    const common::ObString &log_restore_source,
    ObCompatibilityMode &compat_mode)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  share::ObBackupConfigParserMgr config_parser_mgr;
  common::ObSqlString name;
  common::ObSqlString value;
  compat_mode = ObCompatibilityMode::OCEANBASE_MODE;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (OB_FAIL(name.assign("log_restore_source"))) {
    LOG_WARN("assign sql failed", KR(ret));
  } else if (OB_FAIL(value.assign(log_restore_source))) {
    LOG_WARN("fail to assign value", KR(ret), K(log_restore_source));
  } else if (OB_FAIL(config_parser_mgr.init(name, value, 1002 /* fake_user_tenant_id */))) {
    LOG_WARN("fail to init backup config parser mgr", KR(ret), K(name), K(value));
  } else if (OB_FAIL(config_parser_mgr.only_check_before_update(compat_mode))) {
    LOG_WARN("fail to only_check_before_update", KR(ret), K(name), K(value));
  }

  LOG_INFO("[CREATE STANDBY TENANT] check can create standby tenant", KR(ret), K(log_restore_source),
           K(compat_mode), "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObStandbyService::wait_create_standby_tenant_end(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  bool is_dropped = false;
  const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t user_schema_version = OB_INVALID_VERSION;
  int64_t meta_schema_version = OB_INVALID_VERSION;
  const ObSimpleTenantSchema *tenant_schema = nullptr;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    while (OB_SUCC(ret)) {
      ObSchemaGetterGuard schema_guard;
      if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("failed to wait creating standby tenant end", KR(ret), K(tenant_id));
      } else if (OB_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(meta_tenant_id, is_dropped))) {
        LOG_WARN("meta tenant has been dropped", KR(ret), K(meta_tenant_id));
      } else if (is_dropped) {
        ret = OB_TENANT_HAS_BEEN_DROPPED;
        LOG_WARN("meta tenant has been dropped", KR(ret), K(meta_tenant_id));
      } else if (OB_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(user_tenant_id, is_dropped))) {
        LOG_WARN("user tenant has been dropped", KR(ret), K(user_tenant_id));
      } else if (is_dropped) {
        ret = OB_TENANT_HAS_BEEN_DROPPED;
        LOG_WARN("user tenant has been dropped", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
        LOG_WARN("failed to get schema guard", KR(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_info(user_tenant_id, tenant_schema))) {
        LOG_WARN("failed to get tenant info", KR(ret), K(user_tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_schema is null", KR(ret), K(user_tenant_id));
      } else if (tenant_schema->is_creating_standby_tenant_status()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(check_ls_restore_status_(user_tenant_id))) {
          LOG_WARN("failed to check ls restore_status", KR(ret), KR(tmp_ret), K(user_tenant_id));
          if (OB_CREATE_STANDBY_TENANT_FAILED == tmp_ret) {
            ret = OB_SUCC(ret) ? tmp_ret : ret;
          }
        }
      } else if (tenant_schema->is_normal()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(GSCHEMASERVICE.get_tenant_refreshed_schema_version(
            meta_tenant_id, meta_schema_version))) {
          if (OB_ENTRY_NOT_EXIST != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("get refreshed schema version failed", KR(ret), K(meta_tenant_id));
          }
        } else if (OB_TMP_FAIL(GSCHEMASERVICE.get_tenant_refreshed_schema_version(
            user_tenant_id, user_schema_version))) {
          if (OB_ENTRY_NOT_EXIST != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("get refreshed schema version failed", KR(ret), K(user_tenant_id));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (ObSchemaService::is_formal_version(meta_schema_version)
                  && ObSchemaService::is_formal_version(user_schema_version)) {
          break;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tenant status", KR(ret), K(user_tenant_id), K(meta_tenant_id), KPC(tenant_schema));
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "unexpected tenant status, create standby tenant failed");
      }

      if (OB_FAIL(ret)) {
      } else {
        LOG_INFO("[CREATE STANDBY TENANT] wait create standby tenant end", K(tenant_id), K(user_tenant_id),
              K(meta_tenant_id), K(meta_schema_version), K(user_schema_version), KPC(tenant_schema));
        ob_usleep(1000 * 1000L); // 1s
      }
    }
  }

  LOG_INFO("[CREATE STANDBY TENANT] finish to wait create standby tenant end", KR(ret), K(tenant_id),
           "cost", ObTimeUtility::current_time() - start_ts, K(user_tenant_id),
            K(meta_tenant_id), K(meta_schema_version), K(user_schema_version), KPC(tenant_schema));
  return ret;
}

int ObStandbyService::check_ls_restore_status_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(inited));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret), K(tenant_id));
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt("SELECT LS_ID, SYNC_STATUS, SYNC_SCN FROM %s WHERE TENANT_ID = '%lu' and SYNC_STATUS !='NORMAL' and ls_id = %ld",
                                 OB_V_OB_LS_LOG_RESTORE_STATUS_TNAME, tenant_id, SYS_LS.id()))) {
        LOG_WARN("fail to generate sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy->read(result, sql.ptr()))) {
        LOG_WARN("read V$OB_LS_LOG_RESTORE_STATUS failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(result.get_result()->next())) {
        if (OB_ITER_END == ret) {
          // there isn't abnormal ls
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get result next failed", KR(ret), K(tenant_id), K(sql));
        }
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(tenant_id), K(sql));
      } else {
        int64_t ls_id = 0;
        ObString sync_status_str;
        SCN sync_scn;
        logservice::RestoreSyncStatus restore_status;
        uint64_t sync_scn_val = OB_INVALID_SCN_VAL;
        EXTRACT_INT_FIELD_MYSQL(*result.get_result(), "LS_ID", ls_id, int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result.get_result(), "SYNC_STATUS", sync_status_str);
        EXTRACT_UINT_FIELD_MYSQL(*result.get_result(), "SYNC_SCN", sync_scn_val, uint64_t);

        restore_status = logservice::str_to_restore_sync_status(sync_status_str);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get result", KR(ret), K(tenant_id), K(sql));
        } else if (OB_FAIL(sync_scn.convert_for_inner_table_field(sync_scn_val))) {
          LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(sync_scn_val));
        } else if (is_valid_restore_status_for_creating_standby(restore_status)) {
          LOG_INFO("is valid restore status", K(restore_status), K(sync_scn_val));
        } else {
          LOG_WARN("get LS restore status", KR(ret), K(tenant_id), K(ls_id), K(sync_status_str), K(sync_scn), K(sql));
          ret = OB_CREATE_STANDBY_TENANT_FAILED;
          std::string fail_reason = "SYS LS sync status is abnormal: "
                                    + std::string(sync_status_str.ptr(), sync_status_str.length())
                                    + ", please check V$OB_LS_LOG_RESTORE_STATUS";
          LOG_USER_ERROR(OB_CREATE_STANDBY_TENANT_FAILED, fail_reason.c_str());
        }
      }
    }
  }

  return ret;
}

int ObStandbyService::clear_fetched_log_cache(const uint64_t tenant_id)
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
  int64_t cost = ObTimeUtility::current_time() - start_timestamp;
  FLOG_INFO("clear_fetched_log_cache", KR(ret), K(tenant_id), K(tenant_online_servers), K(cost));
  return ret;
}

}
}
