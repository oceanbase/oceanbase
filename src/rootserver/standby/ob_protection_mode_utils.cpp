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

#include "ob_protection_mode_utils.h"
#include "src/share/ob_log_restore_proxy.h"
#include "src/share/restore/ob_log_restore_source_mgr.h"
#include "src/share/backup/ob_log_restore_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "src/share/ob_server_struct.h"
#include "src/share/ob_sync_standby_dest_operator.h"
#include "src/share/ls/ob_ls_status_operator.h"
#include "src/rootserver/standby/ob_tenant_role_transition_service.h"
#include "src/rootserver/standby/ob_standby_service.h"
#include "src/share/ob_sync_standby_status_operator.h"
#define USING_LOG_PREFIX STANDBY

namespace oceanbase {
namespace standby {

ERRSIM_POINT_DEF(ERRSIM_WAIT_PROTECTION_STAT_STEADY_SLEEP);

int ObProtectionModeUtils::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", KR(ret), K_(inited));
  } else if (!is_user_tenant(user_tenant_id_) || !is_valid_tenant_id(user_tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id_));
  } else if (!restore_proxy_util_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore proxy util is not initialized", KR(ret));
  }
  return ret;
}

int ObProtectionModeUtils::get_sync_standby_status_attr(const uint64_t user_tenant_id,
    const int64_t switchover_epoch, share::ObSyncStandbyStatusAttr &sync_standby_status_attr)
{
  int ret = OB_SUCCESS;
  share::ObAllTenantInfo tenant_info;
  share::ObSyncStandbyDestStruct sync_standby_dest_struct;
  bool is_empty = false;
  share::ObProtectionStat protection_stat;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  ObMySQLTransaction trans;
  uint64_t standby_cluster_id = OB_INVALID_CLUSTER_ID;
  uint64_t standby_tenant_id = OB_INVALID_TENANT_ID;
  if (!is_user_tenant(user_tenant_id) || !is_valid_tenant_id(user_tenant_id)
      || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, meta_tenant_id))) {
    LOG_WARN("failed to start transaction", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(share::ObAllTenantInfoProxy::load_tenant_info(user_tenant_id, &trans, true/* for_update */, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(user_tenant_id));
  } else if (tenant_info.get_switchover_epoch() != switchover_epoch) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover epoch is not equal, need retry", KR(ret), K(user_tenant_id),
      K(tenant_info), K(switchover_epoch));
  } else if (OB_FAIL(protection_stat.init(tenant_info))) {
    LOG_WARN("failed to init protection stat", KR(ret), K(tenant_info));
  } else if (!protection_stat.get_protection_mode().is_sync_mode()) {
    // use invalid cluster id or tenant id
  } else if (OB_FAIL(share::ObSyncStandbyDestOperator::read_sync_standby_dest(trans, meta_tenant_id,
     false/* for_update */, is_empty, sync_standby_dest_struct))) {
    LOG_WARN("failed to read sync standby status", KR(ret), K(user_tenant_id));
  } else if (is_empty) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sync standby dest is empty", KR(ret), K(user_tenant_id), K(tenant_info));
  } else {
    const share::ObRestoreSourceServiceUser &user = sync_standby_dest_struct.restore_source_service_attr_.user_;
    standby_cluster_id = user.cluster_id_;
    standby_tenant_id = user.tenant_id_;
  }
  if (FAILEDx(sync_standby_status_attr.init(standby_cluster_id, standby_tenant_id, protection_stat))) {
    LOG_WARN("failed to init sync standby status attr", KR(ret), K(user_tenant_id), K(protection_stat));
  }
  if (trans.is_started()) {
    int tmp_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to end transaction", KR(ret), KR(tmp_ret), K(user_tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

bool ObProtectionModeUtils::check_cluster_version_for_protection_mode()
{
  return GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_2_1;
}

int ObProtectionModeUtils::check_tenant_data_version_for_protection_mode(const uint64_t tenant_id, bool &enable)
{
  int ret = OB_SUCCESS;
  uint64_t meta_data_version = 0;
  uint64_t user_data_version = 0;
  enable = false;
  if (!is_user_tenant(tenant_id) || !is_valid_tenant_id(tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("no user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, user_data_version))) {
    LOG_WARN("failed to get user data version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), meta_data_version))) {
    LOG_WARN("failed to get meta data version", KR(ret), K(tenant_id));
  } else if (meta_data_version < get_protection_mode_data_version() ||
      user_data_version < get_protection_mode_data_version()) {
    enable = false;
  } else {
    enable = true;
  }
  return ret;
}

int ObProtectionModeUtils::wait_protection_stat_steady(
    const uint64_t tenant_id,
    const share::ObProtectionMode &expected_protection_mode,
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (!is_user_tenant(tenant_id) || !is_valid_tenant_id(tenant_id)
      || !expected_protection_mode.is_valid() || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(expected_protection_mode), KP(sql_proxy));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    bool protection_stat_steady = false;
    ObAllTenantInfo tenant_info;
    ObProtectionStat protection_stat;
    while (OB_SUCC(ret) && !protection_stat_steady) {
      if (ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait protection level change timeout", KR(ret), K(tenant_id));
      } else if (OB_UNLIKELY(ERRSIM_WAIT_PROTECTION_STAT_STEADY_SLEEP)) {
        ob_usleep(100_ms);
      } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                     tenant_id, sql_proxy, false/*for_update*/, tenant_info))) {
        LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
      } else if (OB_FAIL(protection_stat.init(tenant_info))) {
        LOG_WARN("failed to init protection stat", KR(ret), K(tenant_info));
      } else if (protection_stat.get_protection_mode() != expected_protection_mode) {
        ret = OB_NEED_RETRY;
        LOG_WARN("protection mode changed while waiting protection stat steady",
            KR(ret), K(tenant_id), K(expected_protection_mode), K(protection_stat), K(tenant_info));
        LOG_USER_ERROR(OB_NEED_RETRY, "protection mode changed while waiting protection level steady");
      } else if (protection_stat.is_steady()) {
        protection_stat_steady = true;
      } else {
        if (OB_TMP_FAIL(rootserver::ObTenantRoleTransitionService::notify_thread(
                gen_meta_tenant_id(tenant_id), obrpc::ObNotifyTenantThreadArg::PROTECTION_MODE_MGR))) {
          LOG_WARN("failed to notify protection_mode_mgr", KR(ret), KR(tmp_ret), K(tenant_id));
        }
        if (REACH_TIME_INTERVAL(1_s)) {
          LOG_INFO("protection stat is not steady, need wait", KR(ret), K(tenant_id), K(tenant_info));
        }
        ob_usleep(100_ms);
      }
    }
  }
  return ret;
}

int ObProtectionModeUtils::wait_standby_tenant_sync(const ObTimeoutCtx &ctx)
{
  int ret = OB_SUCCESS;
  bool is_sync = false;
  while (!is_sync && OB_SUCC(ret)) {
    if (ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait standby tenant sync timeout", KR(ret));
    } else if (OB_FAIL(check_standby_tenant_sync(is_sync))) {
      LOG_WARN("failed to check standby tenant sync", KR(ret));
    }
  }
  return ret;
}

int ObProtectionModeUtils::check_standby_tenant_sync(bool &is_sync)
{
  int ret = OB_SUCCESS;
  share::ObAllTenantInfo standby_tenant_info;
  share::ObAllTenantInfo primary_tenant_info;
  ObTenantStatus standby_tenant_status;
  share::SCN primary_tenant_sync_scn;
  share::SCN standby_tenant_sync_scn;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(share::ObAllTenantInfoProxy::load_tenant_info(user_tenant_id_, GCTX.sql_proxy_,
      false/* for_update */, primary_tenant_info))) {
    LOG_WARN("failed to load primary tenant info", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(restore_proxy_util_.get_tenant_info(standby_tenant_info, standby_tenant_status))) {
    LOG_WARN("failed to get tenant info", KR(ret), K(user_tenant_id_));
  } else if (!standby_tenant_info.get_recovery_until_scn().is_max()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("recovery_until_scn is not max_scn", KR(ret), K(user_tenant_id_), K(standby_tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "standby tenant recovery_until_scn is not max_scn, set protection mode is");
  } else {
    primary_tenant_sync_scn = primary_tenant_info.get_sync_scn();
    const int64_t DIFF_TIME_NS = 5LL * 1000LL * 1000LL * 1000LL; // 5s
    share::SCN standby_tenant_sync_scn_low_bound = share::SCN::minus(primary_tenant_sync_scn, DIFF_TIME_NS);
    if (standby_tenant_sync_scn_low_bound > standby_tenant_info.get_sync_scn()) {
      is_sync = false;
      if (REACH_TIME_INTERVAL(1_s)) {
        LOG_INFO("standby tenant is not sync", KR(ret), K(user_tenant_id_),
          K(primary_tenant_sync_scn), K(standby_tenant_info));
      }
    } else {
      is_sync = true;
    }
  }
  return ret;
}

int ObProtectionModeUtils::get_all_ls_largest_end_scn(const uint64_t user_tenant_id, share::SCN &largest_scn)
{
  int ret = OB_SUCCESS;
  share::SCN smallest_scn;
  if (OB_FAIL(get_all_ls_end_scn_info_(user_tenant_id, largest_scn, smallest_scn))) {
    LOG_WARN("failed to get all ls end scn info", KR(ret), K(user_tenant_id));
  }
  return ret;
}

int ObProtectionModeUtils::get_all_ls_end_scn_info_(const uint64_t user_tenant_id,
  share::SCN &largest_scn, share::SCN &smallest_scn)
{
  int ret = OB_SUCCESS;
  largest_scn = share::SCN::min_scn();
  smallest_scn = share::SCN::max_scn();
  rootserver::ObLSStatusOperator status_op;
  share::ObLSStatusInfoArray status_info_array;
  ObArray<obrpc::ObCheckpoint> checkpoints;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(status_op.get_all_ls_status_by_order_for_protection_mode(user_tenant_id,
     false/* ignore_wait_offline */, status_info_array, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to get all ls status by order for switch tenant", KR(ret), K(user_tenant_id));
  } else if (OB_UNLIKELY(0 >= status_info_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls list is null", KR(ret), K(user_tenant_id), K(status_info_array));
  } else if (OB_FAIL(ObStandbyService::get_checkpoints_by_rpc<share::ObLSStatusInfoIArray>(
                         user_tenant_id,
                         status_info_array,
                         false/* check_sync_to_latest */,
                         checkpoints))) {
    LOG_WARN("failed to get checkpoints by rpc", KR(ret), K(user_tenant_id), K(status_info_array));
  } else {
    for (int64_t i = 0; i < checkpoints.count(); ++i) {
      if (checkpoints.at(i).get_cur_sync_scn() > largest_scn) {
        largest_scn = checkpoints.at(i).get_cur_sync_scn();
      }
      if (checkpoints.at(i).get_cur_sync_scn() < smallest_scn) {
        smallest_scn = checkpoints.at(i).get_cur_sync_scn();
      }
    }
  }
  return ret;
}

int ObProtectionModeUtils::get_tenant_restore_source(const uint64_t user_tenant_id, bool &is_empty,
  share::ObRestoreSourceServiceAttr &restore_source_service_attr)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (!is_user_tenant(user_tenant_id) || !is_valid_tenant_id(user_tenant_id) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(user_tenant_id)))) {
    LOG_WARN("failed to start transaction", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(get_tenant_restore_source(user_tenant_id, trans, false/* for_update */, is_empty, restore_source_service_attr))) {
    LOG_WARN("failed to get tenant restore source", KR(ret), K(user_tenant_id));
  }
  if (trans.is_started()) {
    int tmp_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to end transaction", KR(ret), KR(tmp_ret), K(user_tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObProtectionModeUtils::get_tenant_restore_source(const uint64_t user_tenant_id,
  ObMySQLTransaction &trans, const bool for_update, bool &is_empty,
  share::ObRestoreSourceServiceAttr &restore_source_service_attr)
{
  int ret = OB_SUCCESS;
  share::ObLogRestoreSourceMgr restore_source_mgr;
  share::ObLogRestoreSourceItem restore_source_item;
  if (OB_FAIL(restore_source_mgr.init(user_tenant_id, &trans))) {
    LOG_WARN("failed to init restore source mgr", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(restore_source_mgr.get_source_for_update(restore_source_item, trans))) {
    LOG_WARN("failed to get restore source item for update", KR(ret), K(user_tenant_id));
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_empty = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(restore_source_service_attr.parse_service_attr_from_item(restore_source_item))) {
    LOG_WARN("failed to parse service attr from str", KR(ret), K(restore_source_item));
  } else {
    is_empty = false;
  }
  return ret;
}

int ObProtectionModeUtils::get_tenant_sync_standby_dest(const uint64_t user_tenant_id,
  ObISQLClient &client, const bool for_update, bool &is_empty,
  share::ObSyncStandbyDestStruct &sync_standby_dest_struct, int64_t *ora_rowscn)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(user_tenant_id) || !is_valid_tenant_id(user_tenant_id)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(share::ObSyncStandbyDestOperator::read_sync_standby_dest(client,
      gen_meta_tenant_id(user_tenant_id), for_update, is_empty, sync_standby_dest_struct, ora_rowscn))) {
    LOG_WARN("failed to read sync standby dest", KR(ret), K(user_tenant_id));
  }
  return ret;
}

int ObProtectionModeUtils::get_tenant_sync_standby_dest(const uint64_t user_tenant_id,
  bool &is_empty, share::ObSyncStandbyDestStruct &sync_standby_dest_struct, int64_t *ora_rowscn)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(user_tenant_id) || !is_valid_tenant_id(user_tenant_id)
    || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(get_tenant_sync_standby_dest(user_tenant_id, *GCTX.sql_proxy_, false/*for_update*/,
      is_empty, sync_standby_dest_struct, ora_rowscn))) {
    LOG_WARN("failed to get tenant sync standby dest", KR(ret), K(user_tenant_id));
  }
  return ret;
}

int ObProtectionModeUtils::init(const uint64_t user_tenant_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(inited_));
  } else {
    share::ObSyncStandbyDestStruct sync_standby_dest_struct;
    bool is_empty = false;
    if (OB_FAIL(get_tenant_sync_standby_dest(user_tenant_id, is_empty, sync_standby_dest_struct))) {
      LOG_WARN("failed to get tenant sync standby dest", KR(ret), K(user_tenant_id));
    } else if (is_empty || !sync_standby_dest_struct.is_valid()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("tenant sync standby dest is invalid or empty", KR(ret), K(user_tenant_id));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tenant sync standby dest is invalid or empty, set protection mode is");
    } else if (OB_FAIL(restore_proxy_util_.init_with_service_attr(user_tenant_id,
        &sync_standby_dest_struct.restore_source_service_attr_))) {
      LOG_WARN("failed to init restore proxy util", KR(ret), K(user_tenant_id),
        K(sync_standby_dest_struct));
    } else {
      user_tenant_id_ = user_tenant_id;
      inited_ = true;
      LOG_INFO("protection mode utils is inited", KR(ret), K(inited_), K(user_tenant_id_));
    }
  }
  return ret;
}

int ObProtectionModeUtils::get_log_restore_source(bool &is_empty, share::ObRestoreSourceServiceAttr &restore_source_service_attr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(restore_proxy_util_.get_log_restore_source(is_empty, restore_source_service_attr))) {
    LOG_WARN("failed to get log restore source", KR(ret));
  }
  return ret;
}

bool ObProtectionModeUtils::check_user_is_self(
  const share::ObRestoreSourceServiceAttr &restore_source_service_attr) const
{
  return restore_source_service_attr.user_.cluster_id_ == GCONF.cluster_id &&
    restore_source_service_attr.user_.tenant_id_ == user_tenant_id_;
}

int ObProtectionModeUtils::get_ls_ids_for_protection_mode(const uint64_t user_tenant_id,
    ObISQLClient &client, ObIArray<share::ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  rootserver::ObLSStatusOperator ls_status_operator;
  rootserver::ObLSStatusInfoArray ls_status_info_array;
  ls_ids.reset();
  if (OB_FAIL(ls_status_operator.get_all_ls_status_by_order_for_protection_mode(user_tenant_id,
     false/* ignore_wait_offline */, ls_status_info_array, client))) {
    LOG_WARN("failed to get all ls status by order for protection mode", KR(ret), K(user_tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_info_array.count(); ++i) {
      const rootserver::ObLSStatusInfo &info = ls_status_info_array.at(i);
      if (OB_FAIL(ls_ids.push_back(info.get_ls_id()))) {
        LOG_WARN("failed to push back ls access info", KR(ret), K(info));
      }
    }
  }
  return ret;
}

int ObProtectionModeUtils::check_ls_status_for_upgrade_protection_level(
    const uint64_t user_tenant_id,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  rootserver::ObLSStatusOperator ls_status_operator;
  rootserver::ObLSStatusInfoArray ls_status_array;
  if (OB_FAIL(ls_status_operator.get_all_ls_status_by_order(user_tenant_id, ls_status_array,
      client))) {
    LOG_WARN("failed to get all ls status", KR(ret), K(user_tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_array.count(); ++i) {
      const rootserver::ObLSStatusInfo &ls_status_info = ls_status_array.at(i);
      if (ls_status_info.ls_is_creating()) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("ls status is creating, cannot set protection mode", KR(ret), K(user_tenant_id));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "ls status is creating, set protection mode is");
      }
    }
  }
  return ret;
}

int ObProtectionModeUtils::check_sync_same_with_restore_source(const uint64_t user_tenant_id,
    bool &equal)
{
  int ret = OB_SUCCESS;
  bool log_restore_source_is_empty = false;
  bool sync_standby_dest_is_empty = false;
  share::ObRestoreSourceServiceAttr restore_source_service_attr;
  share::ObSyncStandbyDestStruct sync_standby_dest;
  if (OB_FAIL(get_tenant_restore_source(user_tenant_id, log_restore_source_is_empty, restore_source_service_attr))) {
    LOG_WARN("failed to get tenant restore source", KR(ret));
  } else if (OB_FAIL(get_tenant_sync_standby_dest(user_tenant_id, sync_standby_dest_is_empty, sync_standby_dest))) {
    LOG_WARN("failed to get sync standby dest", KR(ret));
  } else {
    equal = (!log_restore_source_is_empty && !sync_standby_dest_is_empty &&
      sync_standby_dest.restore_source_service_attr_.user_.is_same_tenant(
        restore_source_service_attr.user_));
  }
  return ret;
}

int ObProtectionModeUtils::get_tenant_info(share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObTenantStatus tenant_status;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(restore_proxy_util_.get_tenant_info(tenant_info, tenant_status))) {
    LOG_WARN("failed to get tenant info", KR(ret), K(user_tenant_id_));
  }
  return ret;
}

int ObProtectionModeUtils::check_log_restore_source_is_self()
{
  int ret = OB_SUCCESS;
  // sync_standby_dest's log_restore_source should be self
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else {
    bool is_empty = false;
    share::ObRestoreSourceServiceAttr restore_source_service_attr;
    if (OB_FAIL(get_log_restore_source(is_empty, restore_source_service_attr))) {
      LOG_WARN("failed to get log restore source", KR(ret));
    } else if (is_empty) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("log restore source is empty", KR(ret));
    } else if (!check_user_is_self(restore_source_service_attr)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("log restore source is not self", KR(ret), K(restore_source_service_attr));
    }
  }
  return ret;
}

} // namespace standby
} // namespace oceanbase