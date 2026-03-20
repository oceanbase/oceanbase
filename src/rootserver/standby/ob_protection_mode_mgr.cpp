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

#include "rootserver/standby/ob_protection_mode_mgr.h"
#include "share/config/ob_server_config.h"
#include "share/rc/ob_tenant_base.h"
#include "deps/oblib/src/common/ob_version_def.h"
#include "share/ob_sync_standby_dest_utils.h"
#include "share/ob_sync_standby_dest_operator.h"
#include "share/ob_sync_standby_status_operator.h"
#include "logservice/ob_log_service.h"
#include "rootserver/standby/ob_tenant_role_transition_service.h"
#include "rootserver/standby/ob_protection_mode_utils.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_ls_service_helper.h"
#include "src/share/ls/ob_ls_status_operator.h"
#include "share/ls/ob_ls_recovery_stat_operator.h"
#define USING_LOG_PREFIX STANDBY
#define PROTECTION_LOG_PREFIX "[PROTECTION_MODE_MGR] "
namespace oceanbase
{
namespace standby
{

static int call_get_ls_standby_sync_scn_rpc(
    rootserver::ObGetLSStandbySyncScnProxy &proxy,
    const uint64_t user_tenant_id,
    const share::ObLSID &ls_id,
    obrpc::ObGetLSStandbySyncScnArg &arg)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader;
  const int64_t timeout_us = GCONF.rpc_timeout;
  if (OB_FAIL(GCTX.location_service_->nonblock_get_leader(
      GCONF.cluster_id, user_tenant_id, ls_id, leader))) {
    LOG_WARN("failed to get leader", KR(ret), K(ls_id));
  } else if (!leader.is_valid()) {
    ret = OB_LS_LOCATION_NOT_EXIST;
    LOG_WARN("leader is invalid", KR(ret), K(ls_id), K(leader));
  } else if (OB_FAIL(arg.init(user_tenant_id, ls_id))) {
    LOG_WARN("failed to init arg", KR(ret), K(ls_id));
  } else if (OB_FAIL(proxy.call(leader, timeout_us, GCONF.cluster_id, user_tenant_id,
      share::OBCG_DBA_COMMAND, arg))) {
    LOG_WARN("failed to call get_ls_standby_sync_scn", KR(ret), K(leader), K(timeout_us), K(ls_id));
  }
  return ret;
}

bool ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo::is_ls_ts_valid() const
{
  return ls_id_.is_valid() && ls_add_ts_ > 0;
}

bool ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo::is_valid() const
{
  return is_ls_ts_valid()
    && standby_sync_scn_.is_valid()
    && palf_end_scn_.is_valid();
}

int ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo::assign(const ObLSStandbySyncScnInfo &other)
{
  int ret = OB_SUCCESS;
  ls_id_ = other.ls_id_;
  standby_sync_scn_ = other.standby_sync_scn_;
  palf_end_scn_ = other.palf_end_scn_;
  ls_add_ts_ = other.ls_add_ts_;
  return ret;
}
int ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo::init(
    const obrpc::ObGetLSStandbySyncScnRes &res, const int64_t ls_add_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(res));
  } else {
    ls_id_ = res.get_ls_id();
    standby_sync_scn_ = res.get_standby_sync_scn();
    palf_end_scn_ = res.get_palf_sync_scn();
    ls_add_ts_ = ls_add_ts;
  }
  return ret;
}

int ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo::init(
    const share::ObLSRecoveryStat &ls_recovery,
    const int64_t ls_add_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_recovery.is_valid() || ls_add_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_recovery), K(ls_add_ts));
  } else {
    ls_id_ = ls_recovery.get_ls_id();
    standby_sync_scn_ = ls_recovery.get_sync_scn();
    palf_end_scn_ = ls_recovery.get_sync_scn();
    ls_add_ts_ = ls_add_ts;
  }
  return ret;
}

int ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo::init_empty(
  const share::ObLSID &ls_id, const int64_t ls_add_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ls_add_ts <= 0) || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_add_ts), K(ls_id));
  } else {
    ls_id_ = ls_id;
    standby_sync_scn_.reset();
    palf_end_scn_.reset();
    ls_add_ts_ = ls_add_ts;
  }
  return ret;
}
ObMAProtectionLevelAutoSwitchHelper::ObMAProtectionLevelAutoSwitchHelper()
  : standby_cluster_id_(OB_INVALID_CLUSTER_ID),
    standby_tenant_id_(OB_INVALID_TENANT_ID),
    init_ts_(OB_INVALID_TIMESTAMP),
    last_upgrade_check_fail_ts_(OB_INVALID_TIMESTAMP),
    inited_(false),
    lock_(ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK),
    ls_standby_sync_scn_info_array_()
{}

int ObMAProtectionLevelAutoSwitchHelper::build_init_ls_sync_info_array_(
    const int64_t now,
    ObIArray<ObLSStandbySyncScnInfo> &ls_sync_info_array) const
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  ObArray<share::ObLSRecoveryStat> ls_recovery_array;
  ls_sync_info_array.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(user_tenant_id) || !is_user_tenant(user_tenant_id) || now <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(now));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null, skip init ls standby sync info", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    share::ObLSRecoveryStatOperator recovery_op;
    if (OB_FAIL(recovery_op.get_all_ls_recovery_stat_for_ma_by_order(user_tenant_id, *GCTX.sql_proxy_, ls_recovery_array))) {
      LOG_WARN("failed to get all ls recovery stat for ma", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_recovery_array.count(); ++i) {
        const share::ObLSRecoveryStat &ls_recovery = ls_recovery_array.at(i);
        ObLSStandbySyncScnInfo ls_info;
        if (OB_FAIL(ls_info.init(ls_recovery, now))) {
          LOG_WARN("failed to init ls standby sync info", KR(ret), K(ls_recovery), K(now));
        } else if (OB_FAIL(ls_sync_info_array.push_back(ls_info))) {
          LOG_WARN("failed to push back ls standby sync info", KR(ret), K(ls_recovery), K(ls_info), K(now));
        }
      }
    }
  }
  return ret;
}

int ObMAProtectionLevelAutoSwitchHelper::init(
    const uint64_t standby_cluster_id,
    const uint64_t standby_tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSStandbySyncScnInfo> ls_sync_info_array;
  const int64_t now = ObTimeUtility::current_time();
  if (OB_UNLIKELY(standby_cluster_id == OB_INVALID_CLUSTER_ID
      || !is_valid_tenant_id(standby_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(standby_cluster_id), K(standby_tenant_id));
  } else if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = build_init_ls_sync_info_array_(now, ls_sync_info_array))) {
      LOG_WARN("failed to build init ls sync info array, continue init", KR(tmp_ret), K(now));
    }
    ObLatchWGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
    standby_cluster_id_ = standby_cluster_id;
    standby_tenant_id_ = standby_tenant_id;
    init_ts_ = now;
    last_upgrade_check_fail_ts_ = now;
    if (OB_FAIL(ls_standby_sync_scn_info_array_.assign(ls_sync_info_array))) {
      LOG_WARN("failed to assign ls standby sync info array", KR(ret), K(ls_sync_info_array));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

void ObMAProtectionLevelAutoSwitchHelper::reset()
{
  ObLatchWGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
  standby_cluster_id_ = OB_INVALID_CLUSTER_ID;
  standby_tenant_id_ = OB_INVALID_TENANT_ID;
  init_ts_ = OB_INVALID_TIMESTAMP;
  last_upgrade_check_fail_ts_ = OB_INVALID_TIMESTAMP;
  ls_standby_sync_scn_info_array_.reset();
  inited_ = false;
}

bool ObMAProtectionLevelAutoSwitchHelper::match_standby_dest(
    const uint64_t standby_cluster_id,
    const uint64_t standby_tenant_id) const
{
  ObLatchRGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
  return inited_
      && standby_cluster_id_ == standby_cluster_id
      && standby_tenant_id_ == standby_tenant_id;
}

void ObMAProtectionLevelAutoSwitchHelper::dump() const
{
  ObLatchRGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
  LOG_INFO(PROTECTION_LOG_PREFIX "dump ma protection level auto switch helper",
      K_(standby_cluster_id), K_(standby_tenant_id), KTIME_(init_ts),
      KTIME_(last_upgrade_check_fail_ts), K_(inited), K_(ls_standby_sync_scn_info_array));
}

int ObMAProtectionLevelAutoSwitchHelper::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(inited));
  }
  return ret;
}

int ObMAProtectionLevelAutoSwitchHelper::get_new_ls_standby_sync_info_(const share::ObLSID &ls_id, const ObLSStandbySyncScnInfo &old_ls_info,
    const obrpc::ObGetLSStandbySyncScnRes &res, ObLSStandbySyncScnInfo &new_ls_info, const int64_t now) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || now <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(now));
  } else if (res.is_valid()) {
    // get ls standby sync scn successfully
    if (res.get_ls_id() != ls_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ls id", KR(ret), K(ls_id), K(res));
    } else if (old_ls_info.is_valid()
        && old_ls_info.standby_sync_scn_ >= res.get_standby_sync_scn()) {
      LOG_WARN("standby_sync_scn fallback or not change", KR(ret), K(old_ls_info), K(res));
      if (OB_FAIL(new_ls_info.assign(old_ls_info))) {
        LOG_WARN("failed to assign old ls info", KR(ret), K(old_ls_info));
      }
    } else if (OB_FAIL(new_ls_info.init(res, now))) {
      LOG_WARN("failed to init new ls info", KR(ret), K(res));
    }
  } else if (old_ls_info.is_ls_ts_valid()) {
    // failed to get ls standby sync scn, but we have old ls info, use it
    if (old_ls_info.ls_id_ != ls_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ls id", KR(ret), K(ls_id), K(old_ls_info));
    } else if (OB_FAIL(new_ls_info.assign(old_ls_info))) {
      LOG_WARN("failed to assign old ls info", KR(ret), K(old_ls_info));
    }
  } else {
    // the first time to get ls standby sync scn, we don't have old ls info
    if (OB_FAIL(new_ls_info.init_empty(ls_id, now))) {
      LOG_WARN("failed to init empty ls info", KR(ret), K(ls_id), K(now));
    }
  }
  return ret;
}

int ObMAProtectionLevelAutoSwitchHelper::get_ls_sync_res_by_ls_id_(const share::ObLSID &ls_id,
    const ObIArray<int> &return_code,
    const ObIArray<obrpc::ObGetLSStandbySyncScnArg> &args,
    const ObIArray<const obrpc::ObGetLSStandbySyncScnRes *> &res_array,
    obrpc::ObGetLSStandbySyncScnRes &res) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || args.count() != return_code.count()
      || args.count() != res_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(args.count()), K(return_code.count()),
        K(res_array.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < args.count(); ++i) {
      if (args.at(i).get_ls_id() == ls_id) {
        found = true;
        const obrpc::ObGetLSStandbySyncScnRes *return_res = res_array.at(i);
        if (OB_FAIL(return_code.at(i))) {
          LOG_WARN("failed to get ls standby sync_scn", KR(ret), K(ls_id), K(args.at(i)));
        } else if (OB_ISNULL(return_res) || !return_res->is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("result is invalid", KR(ret), KPC(return_res), K(ls_id));
        } else if (OB_FAIL(res.assign(*return_res))) {
          LOG_WARN("failed to assign res", KR(ret), KPC(return_res), K(ls_id));
        }
      }
    }
  }
  return ret;
}

int ObMAProtectionLevelAutoSwitchHelper::get_sync_info_by_ls_id_(
    const share::ObLSID &ls_id,
    const ObIArray<ObLSStandbySyncScnInfo> &sync_info_array,
    ObLSStandbySyncScnInfo &sync_info) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < sync_info_array.count(); ++i) {
    const ObLSStandbySyncScnInfo &current_info = sync_info_array.at(i);
    if (current_info.ls_id_ == ls_id) {
      found = true;
      if (OB_FAIL(sync_info.assign(current_info))) {
        LOG_WARN("failed to assign sync info", KR(ret), K(sync_info), K(current_info));
      }
    }
  }
  return ret;
}

int ObMAProtectionLevelAutoSwitchHelper::update_sync_info_array_(const ObIArray<int> &return_code,
    const ObIArray<obrpc::ObGetLSStandbySyncScnArg> &args,
    const ObIArray<const obrpc::ObGetLSStandbySyncScnRes *> &res_array,
    const ObIArray<ObLSStatusInfo> &ls_status_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(args.count() != return_code.count() || args.count() != res_array.count()
      || args.count() > ls_status_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array count not match", KR(ret), K(args.count()), K(return_code.count()),
        K(res_array.count()));
  } else {
    int64_t now = ObTimeUtility::current_time();
    ObArray<ObLSStandbySyncScnInfo> new_sync_info_array;
    ObLatchWGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
    if (OB_FAIL(check_inner_stat_())) {
      LOG_WARN("failed to check inner stat", KR(ret));
    } else {
      for (int64_t i = 0; i < ls_status_array.count() && OB_SUCC(ret); ++i) {
        const share::ObLSID ls_id = ls_status_array.at(i).get_ls_id();
        ObGetLSStandbySyncScnRes res;
        ObLSStandbySyncScnInfo old_sync_info;
        ObLSStandbySyncScnInfo new_sync_info;
        // ignore error, invalid value will be processed in get_new_ls_standby_sync_info_
        if (OB_TMP_FAIL(get_ls_sync_res_by_ls_id_(ls_id, return_code, args, res_array, res))) {
          LOG_WARN("failed to get ls standby sync_scn by ls id", KR(tmp_ret), K(ls_id));
        }
        // ignore error, invalid value will be processed in get_new_ls_standby_sync_info_
        if (OB_TMP_FAIL(get_sync_info_by_ls_id_(ls_id, ls_standby_sync_scn_info_array_, old_sync_info))) {
          LOG_WARN("failed to get old sync info by ls id", KR(tmp_ret), K(ls_id));
        }
        if (OB_FAIL(get_new_ls_standby_sync_info_(ls_id, old_sync_info, res, new_sync_info, now))) {
          LOG_WARN("failed to get new ls standby sync info", KR(ret), K(ls_id));
        } else if (OB_FAIL(new_sync_info_array.push_back(new_sync_info))) {
          LOG_WARN("failed to push back new sync info", KR(ret), K(new_sync_info));
        }
      }
    }
    if (FAILEDx(ls_standby_sync_scn_info_array_.assign(new_sync_info_array))) {
      LOG_WARN("failed to assign new sync info array", KR(ret), K(new_sync_info_array));
    }
  }
  return ret;
}

int ObMAProtectionLevelAutoSwitchHelper::refresh_ls_standby_sync_scn()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
  rootserver::ObLSStatusOperator status_op;
  rootserver::ObLSStatusInfoArray status_info_array;
  const int64_t now = ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.location_service_),
        KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(status_op.get_all_ls_status_by_order_for_protection_mode(user_tenant_id,
      true /*ignore_wait_offline*/, status_info_array, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to get all ls status by order for protection mode", KR(ret));
  } else {
    rootserver::ObGetLSStandbySyncScnProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_standby_sync_scn);
    ObArray<int> return_code_array;
    ObArray<ObGetLSStandbySyncScnArg> args;
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
      const share::ObLSID ls_id = status_info_array.at(i).get_ls_id();
      obrpc::ObGetLSStandbySyncScnArg arg;
      if (OB_TMP_FAIL(call_get_ls_standby_sync_scn_rpc(proxy, user_tenant_id, ls_id, arg))) {
        LOG_WARN("failed to call get ls standby sync scn rpc", KR(tmp_ret), K(ls_id));
      } else if (OB_FAIL(args.push_back(arg))) {
        LOG_WARN("failed to push_back arg", KR(ret), K(arg));
      }
    } // end for
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
      LOG_WARN("failed to wait all get ls standby sync scn rpc", KR(ret), KR(tmp_ret));
    } else if (OB_FAIL(ret)) {
    } else if (args.count() != return_code_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("args and return_code_array count not match", KR(ret), K(args), K(return_code_array));
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("failed to check return cnt", KR(ret), K(return_code_array.count()));
    } else if (OB_FAIL(update_sync_info_array_(return_code_array, args, proxy.get_results(), status_info_array))) {
      LOG_WARN("failed to update sync info array", KR(ret));
    }
  }
  return ret;
}

bool ObMAProtectionLevelAutoSwitchHelper::need_downgrade_by_ls_(const ObLSStandbySyncScnInfo &ls_info,
    const int64_t now, const int64_t net_timeout_us) const
{
  bool bret = false;
  const share::SCN standby_sync_scn = ls_info.standby_sync_scn_;
  if (OB_UNLIKELY(net_timeout_us <= 0 || now <= 0 || !ls_info.is_ls_ts_valid())) {
    bret = true;
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", K(net_timeout_us), K(now), K(ls_info));
    // standby_sync_scn may be now+20s, so we need to check ls_add_ts first
  } else if (ls_info.ls_add_ts_ <= 0 || now - ls_info.ls_add_ts_ > net_timeout_us) {
    // standby_sync_scn not updated for a long time
    bret = true;
  } else if (standby_sync_scn.is_valid_and_not_min()) {
    const int64_t standby_sync_ts = standby_sync_scn.convert_to_ts(true /*ignore_invalid*/);
    if (standby_sync_ts <= 0 || now - standby_sync_ts > net_timeout_us) {
      bret = true;
    }
  }
  return bret;
}

bool ObMAProtectionLevelAutoSwitchHelper::can_upgrade_by_ls_(const ObLSStandbySyncScnInfo &ls_info,
    const int64_t now, const int64_t net_timeout_us) const
{
  bool bret = true;
  const share::SCN standby_sync_scn = ls_info.standby_sync_scn_;
  if (OB_UNLIKELY(net_timeout_us <= 0 || now <= 0 || !ls_info.ls_id_.is_valid() || ls_info.ls_add_ts_ == init_ts_)) {
    bret = false;
  } else if (standby_sync_scn.is_valid_and_not_min()) {
    const int64_t standby_sync_ts = standby_sync_scn.convert_to_ts(true /*ignore_invalid*/);
    bret = standby_sync_ts > 0 && now - standby_sync_ts <= net_timeout_us;
  } else {
    bret = false;
  }
  return bret;
}

bool ObMAProtectionLevelAutoSwitchHelper::need_downgrade(const int64_t net_timeout_us) const
{
  int ret = OB_SUCCESS;
  bool bret = false;
  const int64_t now = ObTimeUtility::current_time();
  ObLatchRGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat for downgrade check", KR(ret));
  } else if (net_timeout_us <= 0) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid net timeout us", K(net_timeout_us));
  } else {
    for (int64_t i = 0; !bret && i < ls_standby_sync_scn_info_array_.count(); ++i) {
      const ObLSStandbySyncScnInfo &ls_info = ls_standby_sync_scn_info_array_.at(i);
      if (need_downgrade_by_ls_(ls_info, now, net_timeout_us)) {
        bret = true;
        LOG_INFO(PROTECTION_LOG_PREFIX "need downgrade for ls standby sync scn check",
            K(ls_info), KTIME(now), K(net_timeout_us));
      }
    }
  }
  return bret;
}

bool ObMAProtectionLevelAutoSwitchHelper::can_upgrade(
    const int64_t net_timeout_us,
    const int64_t health_check_time_us)
{
  int ret = OB_SUCCESS;
  bool can_upgrade = false;
  ObLatchWGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat for upgrade check", KR(ret));
  } else if (net_timeout_us <= 0) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid net timeout us", K(net_timeout_us));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    bool check_pass = true;
    for (int64_t i = 0; check_pass && i < ls_standby_sync_scn_info_array_.count(); ++i) {
      const ObLSStandbySyncScnInfo &ls_info = ls_standby_sync_scn_info_array_.at(i);
      if (!can_upgrade_by_ls_(ls_info, now, net_timeout_us)) {
        check_pass = false;
        last_upgrade_check_fail_ts_ = now;
        if (REACH_THREAD_TIME_INTERVAL(10_s)) {
          LOG_INFO(PROTECTION_LOG_PREFIX "upgrade check failed by ls standby sync scn",
              K(ls_info), K(now), K(net_timeout_us));
        }
      }
    }
    if (check_pass) {
      if (last_upgrade_check_fail_ts_ <= 0) {
        // Keep the first pass as warm-up by recording fail timestamp with current time.
        last_upgrade_check_fail_ts_ = now;
      } else {
        can_upgrade = now - last_upgrade_check_fail_ts_ > health_check_time_us;
      }
    }
  }
  return can_upgrade;
}

ObProtectionModeChangeHelper::ObProtectionModeChangeHelper()
  : user_tenant_id_(OB_INVALID_TENANT_ID), proposal_id_(OB_INVALID_VERSION),
    meta_protection_stat_(), inited_(false) {}

ObProtectionModeChangeHelper::~ObProtectionModeChangeHelper()
{
  reset();
}

int ObProtectionModeChangeHelper::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(user_tenant_id_) || !is_user_tenant(user_tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(user_tenant_id_));
  } else if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(user_tenant_id));
  }
  return ret;
}
int ObProtectionModeChangeHelper::splite_sys_ls_from_all_ls_(
  const ObIArray<obrpc::ObLSAccessModeInfo> &all_ls_access_info,
  ObIArray<obrpc::ObLSAccessModeInfo> &sys_ls_access_info,
  ObIArray<obrpc::ObLSAccessModeInfo> &user_ls_access_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_ls_access_info.count(); i++) {
      if (all_ls_access_info.at(i).get_ls_id() == SYS_LS) {
        if (OB_FAIL(sys_ls_access_info.push_back(all_ls_access_info.at(i)))) {
          LOG_WARN("failed to push back sys ls access info", KR(ret),
            K(all_ls_access_info.at(i)));
        }
      } else {
        if (OB_FAIL(user_ls_access_info.push_back(all_ls_access_info.at(i)))) {
          LOG_WARN("failed to push back user ls access info", KR(ret),
            K(all_ls_access_info.at(i)));
        }
      }
    }
  }
  return ret;
}
int ObProtectionModeChangeHelper::init(const uint64_t tenant_id,
    const share::ObProtectionStat &meta_protection_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_protection_stat.is_valid() || !is_valid_tenant_id(tenant_id)
        || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta protection stat", KR(ret), K(meta_protection_stat));
  } else if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else {
    meta_protection_stat_ = meta_protection_stat;
    user_tenant_id_ = tenant_id;
    if (OB_FAIL(get_proposal_id_(proposal_id_))) {
      LOG_WARN("failed to get palf stat", KR(ret), K_(user_tenant_id));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

void ObProtectionModeChangeHelper::reset()
{
  user_tenant_id_ = OB_INVALID_TENANT_ID;
  proposal_id_ = OB_INVALID_VERSION;
  meta_protection_stat_.reset();
  inited_ = false;
}

int ObProtectionModeChangeHelper::check_status_not_changed_() const
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  ObProtectionStat protection_stat;
  if (OB_FAIL(check_self_leader_())) {
    LOG_WARN("meta tenant leader changed", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(user_tenant_id_, GCTX.sql_proxy_,
          false/*for_update*/, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(protection_stat.init(tenant_info))) {
    LOG_WARN("failed to init protection stat", KR(ret), K(tenant_info));
  } else if (!(protection_stat == meta_protection_stat_)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("protection stat changed", KR(ret), K(protection_stat), K(meta_protection_stat_));
  }
  return ret;
}

int ObProtectionModeChangeHelper::check_self_leader_() const
{
  int ret = OB_SUCCESS;
  int64_t proposal_id = OB_INVALID_VERSION;
  if (OB_FAIL(get_proposal_id_(proposal_id))) {
    LOG_WARN("failed to get palf stat", KR(ret), K_(user_tenant_id));
  } else if (proposal_id != proposal_id_) {
    ret = OB_NOT_MASTER;
    LOG_WARN("leader changed", KR(ret), K_(user_tenant_id), K(proposal_id), K(proposal_id_));
  }
  return ret;
}

int ObProtectionModeChangeHelper::get_proposal_id_(int64_t &proposal_id) const
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id_);
  common::ObRole role = FOLLOWER;
  if (OB_FAIL(rootserver::ObRootUtils::get_proposal_id(meta_tenant_id, SYS_LS, proposal_id, role))) {
    LOG_WARN("failed to get proposal id", KR(ret), K(meta_tenant_id), K(SYS_LS));
  } else if (!is_strong_leader(role)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("SYS_LS is not leader", KR(ret), K(meta_tenant_id), K(SYS_LS), K(role));
  }
  return ret;
}

int ObProtectionModeChangeHelper::get_ls_ids_(
    ObISQLClient &client, ObIArray<ObLSID> &ls_ids) const
{
  int ret = OB_SUCCESS;
  ls_ids.reset();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret), K_(user_tenant_id));
  } else if (OB_FAIL(ObProtectionModeUtils::get_ls_ids_for_protection_mode(
      user_tenant_id_, client, ls_ids))) {
    LOG_WARN("failed to get ls ids for protection mode", KR(ret), K_(user_tenant_id));
  }
  return ret;
}

int ObProtectionModeChangeHelper::get_sync_mode_(const ObIArray<ObLSID> &ls_ids,
  ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_status_not_changed_())) {
    LOG_WARN("failed to check self leader", KR(ret));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(rootserver::ObLSLogModeModifier::get_ls_access_mode(user_tenant_id_, ls_ids, ls_access_info))) {
    LOG_WARN("failed to get ls access mode", KR(ret), K(user_tenant_id_), K(ls_ids));
  }
  return ret;
}

int ObProtectionModeChangeHelper::set_sync_mode_(
  const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info, const palf::SyncMode &sync_mode,
  const share::SCN &ref_scn, ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array) const
{
  int ret = OB_SUCCESS;
  ObSyncStandbyStatusAttr protection_log;
  if (OB_FAIL(check_status_not_changed_())) {
    LOG_WARN("failed to check self leader", KR(ret));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (ls_access_info.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_access_info is empty", KR(ret), K(ls_access_info));
  } else if (OB_FAIL(ObProtectionModeUtils::get_sync_standby_status_attr(user_tenant_id_,
      meta_protection_stat_.get_switchover_epoch(), protection_log))) {
    LOG_WARN("failed to get sync standby status attr", KR(ret), K(user_tenant_id_), K(meta_protection_stat_));
  } else if (OB_FAIL(rootserver::ObLSLogModeModifier::change_ls_sync_mode(
      user_tenant_id_, ls_access_info, sync_mode, ref_scn,
      protection_log, change_ls_sync_mode_res_array))) {
    LOG_WARN("failed to set sync mode", KR(ret), K(user_tenant_id_), K(ls_access_info));
  }
  return ret;
}

int ObProtectionModeChangeHelper::check_gts_advanced_(const ObTimeoutCtx &ctx) const
{
  int ret = OB_SUCCESS;
  share::SCN gts;
  share::SCN largest_end_scn;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(ObProtectionModeUtils::get_all_ls_largest_end_scn(user_tenant_id_, largest_end_scn))) {
    LOG_WARN("failed to get all ls largest end scn", KR(ret), K(user_tenant_id_));
  } else {
    while (OB_SUCC(ret)) {
      if (ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait gts advanced timeout", KR(ret), K(user_tenant_id_));
      } else if (OB_FAIL(check_status_not_changed_())) {
        LOG_WARN("failed to check self leader", KR(ret));
      } else if (OB_FAIL(ObLSAttrOperator::get_tenant_gts(user_tenant_id_, gts))) {
        LOG_WARN("failed to get tenant gts", KR(ret), K(user_tenant_id_));
      } else if (gts > largest_end_scn) {
        FLOG_INFO(PROTECTION_LOG_PREFIX "gts advanced, can update protection level", KR(ret), K(gts), K(largest_end_scn));
        break;
      } else {
        LOG_INFO(PROTECTION_LOG_PREFIX "failed to check gts advanced, need wait", KR(ret),
            K(user_tenant_id_), K(gts), K(largest_end_scn));
        ob_usleep(1_s);
      }
    }
  }
  return ret;
}

int ObProtectionModeChangeHelper::check_all_ls_set_sync_mode_(
  const ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array,
  const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
  bool &all_ls_set_sync_mode) const
{
  int ret = OB_SUCCESS;
  all_ls_set_sync_mode = true;
  for (int64_t i = 0; all_ls_set_sync_mode && OB_SUCC(ret) && i < ls_access_info.count(); ++i) {
    ObLSID ls_id = ls_access_info.at(i).get_ls_id();
    bool found = false;
    for (int64_t j = 0; !found && OB_SUCC(ret) && j < change_ls_sync_mode_res_array.count(); ++j) {
      const obrpc::ObChangeLSSyncModeRes &res = change_ls_sync_mode_res_array.at(j);
      if (res.get_result() != OB_SUCCESS) {
        // ignore failed result
      } else if (res.get_ls_id() == ls_id) {
        found = true;
      }
    }
    if (!found) {
      all_ls_set_sync_mode = false;
      LOG_WARN("failed to set sync mode", KR(ret), K(ls_id));
    }
  }
  if (OB_FAIL(ret)) {
    all_ls_set_sync_mode = false;
  }
  return ret;
}

int ObProtectionModeChangeHelper::set_sync_mode_until_suceess_(
  const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
  const palf::SyncMode &sync_mode,
  const share::SCN &ref_scn,
  const int64_t timeout,
  ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array) const
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  ObArray<ObLSID> ls_ids;
  ObSyncStandbyStatusAttr protection_log;
  if (timeout <= 0) {
    ret = OB_TIMEOUT;
    LOG_WARN("already timeout", KR(ret), K(timeout));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else if (OB_FAIL(ObProtectionModeUtils::get_sync_standby_status_attr(user_tenant_id_,
      meta_protection_stat_.get_switchover_epoch(), protection_log))) {
    LOG_WARN("failed to get sync standby status attr", KR(ret), K(user_tenant_id_), K(meta_protection_stat_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_access_info.count(); ++i) {
      const obrpc::ObLSAccessModeInfo &info = ls_access_info.at(i);
      if (OB_FAIL(ls_ids.push_back(info.get_ls_id()))) {
        LOG_WARN("failed to push back ls id", KR(ret), K(info));
      }
    }
    if (FAILEDx(rootserver::ObLSLogModeModifier::change_ls_sync_mode_until_success(user_tenant_id_,
        ls_ids, sync_mode, ref_scn, protection_log, meta_protection_stat_.get_switchover_epoch(),
        change_ls_sync_mode_res_array))) {
      LOG_WARN("failed to change ls sync mode until success", KR(ret), K(user_tenant_id_), K(ls_ids),
          K(sync_mode), K(ref_scn));
    }
  }
  return ret;
}

int ObProtectionModeChangeHelper::downgrade_protection_mode_set_sync_mode_(
  const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
  const palf::SyncMode &target_sync_mode,
  const ObTimeoutCtx &ctx,
  const share::SCN &ref_scn,
  ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array) const
{
  int ret = OB_SUCCESS;
  // for requests to set sync_mode to PRE_ASYNC, we need to set SYS_LS to PRE_ASYNC first, so first_ls_access_info is sys_ls_access_info and second_ls_access_info is user_ls_access_info
  // for requests to set sync_mode to ASYNC, we need to set USER_LS to PRE_ASYNC first, so first_ls_access_info is user_ls_access_info and second_ls_access_info is sys_ls_access_info
  ObArray<obrpc::ObLSAccessModeInfo> sys_ls_access_info;
  ObArray<obrpc::ObLSAccessModeInfo> user_ls_access_info;
  ObIArray<obrpc::ObLSAccessModeInfo> &first_ls_access_info =
    target_sync_mode == palf::SyncMode::PRE_ASYNC ? sys_ls_access_info : user_ls_access_info;
  ObIArray<obrpc::ObLSAccessModeInfo> &second_ls_access_info =
    target_sync_mode == palf::SyncMode::PRE_ASYNC ? user_ls_access_info : sys_ls_access_info;
  ObArray<obrpc::ObChangeLSSyncModeRes> change_first_ls_sync_mode_res_array;
  ObArray<obrpc::ObChangeLSSyncModeRes> change_second_ls_sync_mode_res_array;
  if (OB_FAIL(check_status_not_changed_())) {
    LOG_WARN("failed to check self leader", KR(ret));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (target_sync_mode != palf::SyncMode::PRE_ASYNC && target_sync_mode != palf::SyncMode::ASYNC) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target sync mode", KR(ret), K(target_sync_mode));
  } else if (OB_FAIL(splite_sys_ls_from_all_ls_(ls_access_info, sys_ls_access_info, user_ls_access_info))) {
    LOG_WARN("failed to splite sys ls from all ls", KR(ret));
  } else if (OB_FAIL(set_sync_mode_until_suceess_(first_ls_access_info, target_sync_mode,
        ref_scn, ctx.get_abs_timeout() - ObTimeUtility::current_time(), change_first_ls_sync_mode_res_array))) {
    LOG_WARN("failed to set sync mode", KR(ret), K(first_ls_access_info), K(ref_scn));
  } else {
    share::SCN max_scn = SCN::min_scn();
    for (int64_t i = 0; i < change_first_ls_sync_mode_res_array.count(); ++i) {
      ObChangeLSSyncModeRes &res = change_first_ls_sync_mode_res_array.at(i);
      if (res.get_special_log_scn() > max_scn) {
        max_scn = res.get_special_log_scn();
      }
    }
    // max_scn may equal to ref_scn
    if (max_scn < ref_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected max scn", KR(ret), K(max_scn), K(ref_scn));
    } else if (OB_FAIL(check_status_not_changed_())) {
      LOG_WARN("failed to check self leader", KR(ret));
    } else if (OB_FAIL(set_sync_mode_until_suceess_(second_ls_access_info, target_sync_mode,
        max_scn, ctx.get_abs_timeout() - ObTimeUtility::current_time(), change_second_ls_sync_mode_res_array))) {
      LOG_WARN("failed to set sync mode", KR(ret), K(second_ls_access_info));
    } else if (OB_FAIL(change_ls_sync_mode_res_array.assign(change_first_ls_sync_mode_res_array))) {
      LOG_WARN("failed to assign change ls sync mode res array", KR(ret), K(change_first_ls_sync_mode_res_array));
    } else if (OB_FAIL(append(change_ls_sync_mode_res_array, change_second_ls_sync_mode_res_array))) {
      LOG_WARN("failed to append change ls sync mode res array", KR(ret), K(change_second_ls_sync_mode_res_array));
    }
  }
  return ret;
}
// tow situations:
// first:
//   1. A get tenant_info --> MPT/MPF
//   2. A send change sync mode to SYNC
//   3. B set tenant_info --> MPF/PRE-MPF
//   4. B set get sync mode --> ASYNC
//   B should change sync mode before change tenant_info to MPF/MPF because of change sync mode operation in 2
// second:
//   1. A get tenant_info --> MPF/PRE-MPF
//   2. A downgrade to ASYNC
//   3. A try to set tenant_info to MPF/MPF
//   4. B get tenant_info --> MPF/PRE-MPF
//   5. B change sync_mode to PRE-ASYNC
//   user may get tenant_info in MPF/MPF while sys ls in PRE-ASYNC
// so we should advance switchover epoch before change sync mode to PRE-ASYNC to avoid thread A's tenant_info change operation
// TODO(shouju.zyp for MPT): can use service_epoch to avoid situation B
int ObProtectionModeChangeHelper::downgrade_protection_mode(int64_t &new_switchover_epoch)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  ObArray<ObLSID> ls_ids;
  share::ObAllTenantInfo tenant_info;
  ObArray<obrpc::ObLSAccessModeInfo> all_ls_access_info;
  ObArray<obrpc::ObChangeLSSyncModeRes> change_ls_sync_mode_res_array;
  share::SCN largest_end_scn;
  // 1. update switchover epoch to avoid other thread update it
  // 1. set SYS_LS to pre_async
  // 3. set other LS to pre_async
  // 4. set other LS to async
  // 5. set SYS_LS to async
  // 4. change meta to MA/RE or MPF/MPF
  // check self leader between each step
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret), K_(user_tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K_(user_tenant_id), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(check_status_not_changed_())) {
    LOG_WARN("failed to check self leader", KR(ret));
  } else if (OB_FAIL(get_ls_ids_(*GCTX.sql_proxy_, ls_ids))) {
    LOG_WARN("failed to renew ls ids", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else if (OB_FAIL(get_sync_mode_(ls_ids, all_ls_access_info))) {
    LOG_WARN("failed to get ls access mode", KR(ret));
  } else if (OB_FAIL(share::ObAllTenantInfoProxy::update_tenant_protection_mode_and_level(
    user_tenant_id_, ObTenantRole(ObTenantRole::PRIMARY_TENANT), GCTX.sql_proxy_,
    meta_protection_stat_.get_switchover_epoch(), meta_protection_stat_.get_protection_mode(),
    meta_protection_stat_.get_protection_level(),
    meta_protection_stat_.get_switchover_epoch()/* min switchover epoch */, new_switchover_epoch))) {
    LOG_WARN("failed to advance switchover epoch", KR(ret), K(meta_protection_stat_));
  } else if (new_switchover_epoch <= meta_protection_stat_.get_switchover_epoch()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected new switchover epoch", KR(ret), K(meta_protection_stat_), K(new_switchover_epoch));
  } else if (FALSE_IT(meta_protection_stat_.set_switchover_epoch(new_switchover_epoch))) {
  } else if (OB_FAIL(ObProtectionModeUtils::get_all_ls_largest_end_scn(user_tenant_id_, largest_end_scn))) {
    LOG_WARN("failed to get all ls largest end scn", KR(ret));
  } else if (OB_FAIL(downgrade_protection_mode_set_sync_mode_(all_ls_access_info, palf::SyncMode::PRE_ASYNC,
      ctx, largest_end_scn, change_ls_sync_mode_res_array))) {
    LOG_WARN("failed to set sync mode", KR(ret), K(all_ls_access_info), K(largest_end_scn));
  } else if (OB_FAIL(get_access_info_from_result_(palf::SyncMode::PRE_ASYNC,
      change_ls_sync_mode_res_array, all_ls_access_info))) {
    LOG_WARN("failed to get access info from result", KR(ret), K(all_ls_access_info), K(change_ls_sync_mode_res_array));
  } else if (OB_FAIL(downgrade_protection_mode_set_sync_mode_(all_ls_access_info, palf::SyncMode::ASYNC,
      ctx, largest_end_scn, change_ls_sync_mode_res_array))) {
    LOG_WARN("failed to set sync mode", KR(ret), K(all_ls_access_info), K(largest_end_scn));
  } else if (OB_FAIL(check_status_not_changed_())) {
    LOG_WARN("failed to check self leader", KR(ret));
  } else if (OB_FAIL(ObProtectionModeMgr::advance_meta_tenant_protection_stat(
      gen_meta_tenant_id(user_tenant_id_), meta_protection_stat_, GCTX.sql_proxy_))) {
    LOG_WARN("failed to advance meta tenant protection stat", KR(ret), K(user_tenant_id_), K(meta_protection_stat_));
  }
  return ret;
}

int ObProtectionModeChangeHelper::get_access_info_from_result_(const palf::SyncMode target_sync_mode,
    const ObIArray<obrpc::ObChangeLSSyncModeRes> &results,
    ObIArray<obrpc::ObLSAccessModeInfo> &access_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (access_info.count() != results.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(access_info.count()), K(results.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < access_info.count(); ++i) {
      obrpc::ObLSAccessModeInfo &info = access_info.at(i);
      obrpc::ObLSAccessModeInfo new_info;
      bool found = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < results.count() && !found; ++j) {
        const obrpc::ObChangeLSSyncModeRes &res = results.at(j);
        if (res.get_ls_id() == info.get_ls_id()) {
          found = true;
          if (OB_FAIL(new_info.init(info.get_tenant_id(), info.get_ls_id(),
              res.get_mode_version(), info.get_access_mode(), info.get_ref_scn(),
              info.get_sys_ls_end_scn(), target_sync_mode))) {
            LOG_WARN("failed to init new info", KR(ret), K(info));
          } else if (OB_FAIL(info.assign(new_info))) {
            LOG_WARN("failed to assign new info", KR(ret), K(new_info));
          }
        }
      }
      if (!found) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ls id", KR(ret), K(info.get_ls_id()), K(access_info));
      }
    }
  }
  return ret;
}

int ObProtectionModeChangeHelper::upgrade_protection_mode() const
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  ObArray<ObLSID> ls_ids;
  const int64_t timeout = GCONF.internal_sql_execute_timeout;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret), K_(user_tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || !meta_protection_stat_.get_protection_level().is_maximum_performance()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(GCTX.sql_proxy_), K(meta_protection_stat_));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, timeout))) {
    LOG_WARN("failed to set default timeout ctx", KR(ret), K(timeout));
  } else if (OB_FAIL(get_ls_ids_(*GCTX.sql_proxy_, ls_ids))) {
    LOG_WARN("failed to get ls ids", KR(ret));
  } else {
    ObArray<obrpc::ObLSAccessModeInfo> ls_access_infos;
    ObArray<obrpc::ObChangeLSSyncModeRes> change_ls_sync_mode_res_array;
    if (OB_FAIL(check_status_not_changed_())) {
      LOG_WARN("failed to check self leader", KR(ret));
    } else if (OB_FAIL(get_sync_mode_(ls_ids, ls_access_infos))) {
      LOG_WARN("failed to get sync mode", KR(ret));
    } else if (OB_FAIL(set_sync_mode_until_suceess_(ls_access_infos, palf::SyncMode::SYNC,
            SCN::min_scn(), GCONF.internal_sql_execute_timeout, change_ls_sync_mode_res_array))) {
      LOG_WARN("failed to set sync mode", KR(ret), K(ls_access_infos));
    } else if (OB_FAIL(check_gts_advanced_(ctx))) {
      LOG_WARN("gts not advanced", KR(ret), K(user_tenant_id_), K(ls_ids));
    } else if (OB_FAIL(ObProtectionModeMgr::advance_meta_tenant_protection_stat(
        gen_meta_tenant_id(user_tenant_id_), meta_protection_stat_, GCTX.sql_proxy_))) {
      LOG_WARN("failed to advance meta tenant protection stat", KR(ret), K(user_tenant_id_), K(meta_protection_stat_));
    }
  }
  return ret;
}

ObSyncStandbyDestCache::ObSyncStandbyDestCache()
  : inited_(false),
    cache_last_update_ts_(OB_INVALID_TIMESTAMP),
    sync_standby_dest_ora_rowscn_(0),
    is_empty_(true),
    lock_(ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK),
    sync_standby_dest_()
{}

int ObSyncStandbyDestCache::init()
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else {
    cache_last_update_ts_ = OB_INVALID_TIMESTAMP;
    sync_standby_dest_ora_rowscn_ = 0;
    inited_ = true;
  }
  return ret;
}

void ObSyncStandbyDestCache::reset()
{
  ObLatchWGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
  inited_ = false;
  cache_last_update_ts_ = OB_INVALID_TIMESTAMP;
  sync_standby_dest_ora_rowscn_ = 0;
  is_empty_ = true;
  sync_standby_dest_.reset();
}

int ObSyncStandbyDestCache::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(inited));
  }
  return ret;
}

int ObSyncStandbyDestCache::clear_cache()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else {
    ObLatchWGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
    cache_last_update_ts_ = OB_INVALID_TIMESTAMP;
    sync_standby_dest_ora_rowscn_ = 0;
    is_empty_ = true;
    sync_standby_dest_.reset();
  }
  return ret;
}

bool ObSyncStandbyDestCache::need_refresh_cache_(const int64_t now) const
{
  return cache_last_update_ts_ <= 0
      || now - cache_last_update_ts_ >= CACHE_UPDATE_INTERVAL_US;
}

int ObSyncStandbyDestCache::get_sync_standby_dest(
    bool &is_empty,
    share::ObSyncStandbyDestStruct &sync_standby_dest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else {
    bool cached_is_empty = false;
    bool need_refresh_cache = true;
    ObSyncStandbyDestStruct cached_sync_standby_dest;
    {
      ObLatchRGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
      cached_is_empty = is_empty_;
      need_refresh_cache = need_refresh_cache_(ObTimeUtility::current_time());
      if (cached_is_empty) {
        cached_sync_standby_dest.reset();
      } else if (OB_FAIL(cached_sync_standby_dest.assign(sync_standby_dest_))) {
        LOG_WARN("failed to assign sync standby dest", KR(ret), K_(sync_standby_dest));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (need_refresh_cache) {
      const uint64_t user_tenant_id = gen_user_tenant_id(MTL_ID());
      bool latest_is_empty = false;
      int64_t latest_sync_standby_dest_ora_rowscn = 0;
      ObSyncStandbyDestStruct latest_sync_standby_dest;
      if (OB_FAIL(ObProtectionModeUtils::get_tenant_sync_standby_dest(
          user_tenant_id, latest_is_empty, latest_sync_standby_dest, &latest_sync_standby_dest_ora_rowscn))) {
        LOG_WARN("failed to get tenant sync standby dest", KR(ret), K(user_tenant_id));
      } else {
        ObLatchWGuard guard(lock_, ObLatchIds::MA_SYNC_MODE_SWITCH_LOCK);
        const bool has_cache_before_update = cache_last_update_ts_ > 0;
        if (has_cache_before_update && latest_sync_standby_dest_ora_rowscn > 0
            && sync_standby_dest_ora_rowscn_ > latest_sync_standby_dest_ora_rowscn) {
          LOG_INFO("skip sync standby dest cache update due to ora_rowscn rollback",
              K(user_tenant_id), K(sync_standby_dest_ora_rowscn_), K(latest_sync_standby_dest_ora_rowscn));
        } else {
          cache_last_update_ts_ = ObTimeUtility::current_time();
          sync_standby_dest_ora_rowscn_ = latest_sync_standby_dest_ora_rowscn;
          is_empty_ = latest_is_empty;
          if (is_empty_) {
            sync_standby_dest_.reset();
            cached_sync_standby_dest.reset();
            cached_is_empty = is_empty_;
          } else if (OB_FAIL(sync_standby_dest_.assign(latest_sync_standby_dest))) {
            LOG_WARN("failed to assign sync standby dest", KR(ret), K(latest_sync_standby_dest));
          } else if (OB_FAIL(cached_sync_standby_dest.assign(sync_standby_dest_))) {
            LOG_WARN("failed to assign sync standby dest", KR(ret), K_(sync_standby_dest));
          } else {
            cached_is_empty = is_empty_;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_empty = cached_is_empty;
      if (is_empty) {
        sync_standby_dest.reset();
      } else if (OB_FAIL(sync_standby_dest.assign(cached_sync_standby_dest))) {
        LOG_WARN("failed to assign sync standby dest", KR(ret), K(cached_sync_standby_dest));
      }
    }
  }
  return ret;
}

ObProtectionModeMgr::ObProtectionModeMgr()
  : user_tenant_id_(OB_INVALID_TENANT_ID), inited_(false), sync_standby_dest_cache_() {}

ObProtectionModeMgr::~ObProtectionModeMgr() {}

int ObProtectionModeMgr::init()
{
  int ret = OB_SUCCESS;
  user_tenant_id_ = gen_user_tenant_id(MTL_ID());
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(sync_standby_dest_cache_.init())) {
    LOG_WARN("failed to init sync standby dest cache", KR(ret), K_(user_tenant_id));
  } else if (OB_FAIL(ObTenantThreadHelper::create("ProtectionModeMgr",
          lib::TGDefIDs::ProtectionModeMgr, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObProtectionModeMgr::destroy()
{
  ObTenantThreadHelper::destroy();
  sync_standby_dest_cache_.reset();
  user_tenant_id_ = OB_INVALID_TENANT_ID;
  inited_ = false;
}

void ObProtectionModeMgr::do_work()
{
  int ret = OB_SUCCESS;
  const uint64_t thread_idx = get_thread_idx();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(wait_tenant_schema_and_version_ready_(user_tenant_id_,
      ObProtectionModeUtils::get_protection_mode_data_version()))) {
    LOG_WARN("failed to wait tenant schema version ready", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(wait_tenant_schema_and_version_ready_(meta_tenant_id,
     ObProtectionModeUtils::get_protection_mode_data_version()))) {
    LOG_WARN("failed to wait tenant schema version ready", KR(ret), K(meta_tenant_id));
  } else {
    while (!has_set_stop()) {
      int64_t idle_interval = PROTECTION_MODE_MGR_IDLE_INTERVAL;
      share::ObProtectionStat meta_protection_stat;
      bool sync_standby_dest_is_empty = false;
      share::ObSyncStandbyDestStruct sync_standby_dest_struct;
      ObCurTraceId::init(GCONF.self_addr_);
      DEBUG_SYNC(STOP_PROTECTION_MODE_MGR);
      bool status_changed = false;
      ret = OB_SUCCESS;
      MTL_SWITCH(user_tenant_id_) {
        if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
          ret = OB_NEED_RETRY;
          idle_interval = PROTECTION_MODE_MGR_IDLE_INTERVAL * 10;
          if (REACH_THREAD_TIME_INTERVAL(1_min)) {
            FLOG_INFO(PROTECTION_LOG_PREFIX "tenant is not primary tenant", KR(ret), K(user_tenant_id_));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(load_protection_mode_stat_(meta_protection_stat))) {
        LOG_WARN("failed to load protection mode stat", KR(ret), K(user_tenant_id_));
      } else if (meta_protection_stat.get_protection_mode().is_maximum_performance() &&
          meta_protection_stat.get_protection_level().is_maximum_performance()) {
        // MPF/MPF, need to sleep to reduce the resource usage
        idle_interval = PROTECTION_MODE_MGR_IDLE_INTERVAL * 10;
      }
      if (FAILEDx(sync_standby_dest_cache_.get_sync_standby_dest(
          sync_standby_dest_is_empty, sync_standby_dest_struct))) {
        LOG_WARN("failed to get tenant sync standby dest from cache", KR(ret), K(user_tenant_id_));
      } else if (0 == thread_idx) {
        process_upgrade_thread_(meta_protection_stat, sync_standby_dest_is_empty,
            sync_standby_dest_struct, status_changed);
      } else if (1 == thread_idx) {
        process_downgrade_thread_(meta_protection_stat, sync_standby_dest_is_empty,
            sync_standby_dest_struct, status_changed);
      }
      if (OB_FAIL(ret)) {
      } else if (status_changed) {
        // status_changed, need to speedup current thread to advance protection_level
        idle_interval = PROTECTION_MODE_MGR_IDLE_INTERVAL / 10;
      }
      LOG_INFO(PROTECTION_LOG_PREFIX "finish one round", KR(ret), K(meta_tenant_id),
          K(status_changed), K(thread_idx));
      idle(idle_interval);
    }
    if (thread_idx == 1) {
      ma_switch_helper_.reset();
    }
  }
}

int ObProtectionModeMgr::trigger_ma_upgrade_(const share::ObProtectionStat &meta_protection_stat) const
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObAllTenantInfo tenant_info;
  if (!is_valid_tenant_id(user_tenant_id_) || !is_user_tenant(user_tenant_id_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id_), KP(GCTX.sql_proxy_));
  } else if (!meta_protection_stat.get_protection_level().is_resynchronization()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected protection level", KR(ret), K(meta_protection_stat));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(user_tenant_id_)))) {
    LOG_WARN("failed to start transaction", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(user_tenant_id_, &trans, true/*for_update*/, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(user_tenant_id_));
  } else if (OB_UNLIKELY(!tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant info is not valid", KR(ret), K(tenant_info));
  } else if (tenant_info.get_switchover_epoch() != meta_protection_stat.get_switchover_epoch()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("tenant switchover epoch is not equal to meta protection stat switchover epoch", KR(ret), K(tenant_info), K(meta_protection_stat));
  } else if (OB_FAIL(ObProtectionModeUtils::check_ls_status_for_upgrade_protection_level(
      user_tenant_id_, trans))) {
    LOG_WARN("failed to check ls status for upgrade protection mode", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(advance_meta_tenant_protection_stat(gen_meta_tenant_id(user_tenant_id_),
      meta_protection_stat, &trans))) {
    LOG_WARN("failed to advance meta tenant protection stat", KR(ret), K(user_tenant_id_));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end transaction", KR(tmp_ret), K(user_tenant_id_));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

void ObProtectionModeMgr::process_upgrade_thread_(share::ObProtectionStat &meta_protection_stat,
    const bool sync_standby_dest_is_empty,
    const share::ObSyncStandbyDestStruct &sync_standby_dest_struct, bool &status_changed)
{
  int ret = OB_SUCCESS;
  bool status_changed1 = false;
  bool status_changed2 = false;
  if (OB_FAIL(ma_try_upgrade_from_re_(meta_protection_stat, sync_standby_dest_is_empty,
      sync_standby_dest_struct, status_changed1))) {
    // overwrite ret
    LOG_WARN("failed to try ma upgrade from resync", KR(ret));
  }
  if (OB_FAIL(advance_upgrade_protection_mode_(meta_protection_stat, status_changed2))) {
    // overwrite ret
    LOG_WARN("failed to advance upgrade protection mode", KR(ret));
  }
  status_changed = status_changed1 || status_changed2;
}

int ObProtectionModeMgr::ma_try_upgrade_from_re_(share::ObProtectionStat &meta_protection_stat,
    const bool sync_standby_dest_is_empty,
    const share::ObSyncStandbyDestStruct &sync_standby_dest_struct, bool &status_changed)
{
  int ret = OB_SUCCESS;
  status_changed = false;
  if (meta_protection_stat.get_protection_level().is_resynchronization()) {
    if (sync_standby_dest_is_empty || !sync_standby_dest_struct.is_valid()) {
      ret = OB_NEED_RETRY;
      LOG_WARN("tenant sync standby dest is invalid for ma upgrade", KR(ret), K(user_tenant_id_),
          K(sync_standby_dest_is_empty), K(sync_standby_dest_struct));
    } else {
      const int64_t net_timeout_us = sync_standby_dest_struct.get_service_net_timeout();
      const int64_t health_check_time_us = sync_standby_dest_struct.get_service_health_check_time();
      if (!ma_switch_helper_.can_upgrade(net_timeout_us, health_check_time_us)) {
        if (REACH_THREAD_TIME_INTERVAL(3_s)) {
          ma_switch_helper_.dump();
        }
      } else if (OB_FAIL(trigger_ma_upgrade_(meta_protection_stat))) {
        LOG_WARN("failed to advance meta tenant protection stat", KR(ret));
      } else {
        ma_switch_helper_.dump();
        FLOG_INFO(PROTECTION_LOG_PREFIX "ma upgrade success", KR(ret), K(meta_protection_stat));
        status_changed = true;
        if (OB_FAIL(load_protection_mode_stat_(meta_protection_stat))) {
          LOG_WARN("failed to load protection mode stat", KR(ret));
        }
      }
    }
  }
  return ret;
}

void ObProtectionModeMgr::process_downgrade_thread_(share::ObProtectionStat &meta_protection_stat,
    const bool sync_standby_dest_is_empty,
    const share::ObSyncStandbyDestStruct &sync_standby_dest_struct, bool &status_changed)
{
  int ret = OB_SUCCESS;
  bool status_changed1 = false;
  bool status_changed2 = false;
  if (OB_FAIL(ma_refresh_standby_sync_scn_(meta_protection_stat, sync_standby_dest_is_empty,
      sync_standby_dest_struct))) {
    // overwrite ret
    LOG_WARN("failed to refresh ma standby sync scn", KR(ret));
  }
  if (OB_FAIL(ma_downgrade_level_if_sync_lag_(meta_protection_stat, sync_standby_dest_is_empty,
      sync_standby_dest_struct, status_changed1))) {
    // overwrite ret
    LOG_WARN("failed to check sync lag and downgrade ma level", KR(ret));
  }
  if (OB_FAIL(advance_downgrade_protection_mode_(meta_protection_stat, status_changed2))) {
    // overwrite ret
    LOG_WARN("failed to advance downgrade protection mode", KR(ret));
  }
  status_changed = status_changed1 || status_changed2;
}

int ObProtectionModeMgr::ma_refresh_standby_sync_scn_(
    const share::ObProtectionStat &meta_protection_stat,
    const bool sync_standby_dest_is_empty,
    const share::ObSyncStandbyDestStruct &sync_standby_dest_struct)
{
  int ret = OB_SUCCESS;
  if (!meta_protection_stat.get_protection_mode().is_maximum_availability()) {
    if (ma_switch_helper_.is_inited()) {
      ma_switch_helper_.reset();
    }
  } else if (sync_standby_dest_is_empty || !sync_standby_dest_struct.is_valid()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("tenant sync standby dest is invalid for ma refresh", KR(ret), K(user_tenant_id_),
        K(sync_standby_dest_is_empty), K(sync_standby_dest_struct));
  } else {
    const share::ObRestoreSourceServiceUser &standby_user =
        sync_standby_dest_struct.restore_source_service_attr_.user_;
    const uint64_t standby_cluster_id = standby_user.cluster_id_;
    const uint64_t standby_tenant_id = standby_user.tenant_id_;
    if (OB_UNLIKELY(standby_cluster_id == OB_INVALID_CLUSTER_ID
        || !is_valid_tenant_id(standby_tenant_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid sync standby dest user", KR(ret), K(sync_standby_dest_struct));
    } else if (ma_switch_helper_.is_inited()
        && !ma_switch_helper_.match_standby_dest(standby_cluster_id, standby_tenant_id)) {
      ma_switch_helper_.reset();
      if (OB_FAIL(ma_switch_helper_.init(standby_cluster_id, standby_tenant_id))) {
        LOG_WARN("failed to reinit ma_switch_helper_ after standby tenant switched", KR(ret),
            K_(user_tenant_id), K(standby_cluster_id), K(standby_tenant_id));
      } else {
        LOG_INFO(PROTECTION_LOG_PREFIX "reinit ma_switch_helper_ because standby user changed",
            K_(user_tenant_id), K(standby_cluster_id), K(standby_tenant_id));
      }
    } else if (!ma_switch_helper_.is_inited()
        && OB_FAIL(ma_switch_helper_.init(standby_cluster_id, standby_tenant_id))) {
      LOG_WARN("failed to init ma_switch_helper_", KR(ret), K_(user_tenant_id),
          K(standby_cluster_id), K(standby_tenant_id));
    } else if (OB_FAIL(ma_switch_helper_.refresh_ls_standby_sync_scn())) {
      LOG_WARN("failed to refresh ls standby sync scn", KR(ret));
    }
  }
  return ret;
}

int ObProtectionModeMgr::ma_downgrade_level_if_sync_lag_(share::ObProtectionStat &meta_protection_stat,
    const bool sync_standby_dest_is_empty,
    const share::ObSyncStandbyDestStruct &sync_standby_dest_struct, bool &status_changed)
{
  int ret = OB_SUCCESS;
  status_changed = false;
  if (meta_protection_stat.get_protection_mode().is_maximum_availability()) {
    if (meta_protection_stat.get_protection_level().is_maximum_availability()
        || meta_protection_stat.get_protection_level().is_maximum_performance()) {
      if (sync_standby_dest_is_empty || !sync_standby_dest_struct.is_valid()) {
        ret = OB_NEED_RETRY;
        LOG_WARN("tenant sync standby dest is invalid for ma downgrade", KR(ret), K(user_tenant_id_),
            K(sync_standby_dest_is_empty), K(sync_standby_dest_struct));
      } else {
        const int64_t net_timeout_us = sync_standby_dest_struct.get_service_net_timeout();
        if (!ma_switch_helper_.is_inited()) {
          ret = OB_NOT_INIT;
          LOG_WARN("ma_switch_helper_ is not inited", KR(ret));
        } else if (ma_switch_helper_.need_downgrade(net_timeout_us)) {
          FLOG_INFO(PROTECTION_LOG_PREFIX "protection level need downgrade", KR(ret), K(meta_protection_stat));
          ma_switch_helper_.dump();
          int64_t new_switchover_epoch = OB_INVALID_VERSION;
          if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_protection_mode_and_level(user_tenant_id_,
                ObTenantRole(ObTenantRole::PRIMARY_TENANT), GCTX.sql_proxy_,
                meta_protection_stat.get_switchover_epoch(), ObProtectionMode(ObProtectionMode::MAXIMUM_AVAILABILITY_MODE),
                ObProtectionLevel(ObProtectionLevel::PRE_MAXIMUM_PERFORMANCE_LEVEL),
                meta_protection_stat.get_switchover_epoch(),
                new_switchover_epoch))) {
            LOG_WARN("failed to advance meta tenant protection stat", KR(ret), K(user_tenant_id_), K(meta_protection_stat));
          } else {
            ma_switch_helper_.dump();
            FLOG_INFO(PROTECTION_LOG_PREFIX "ma downgrade success", KR(ret), K(meta_protection_stat));
            status_changed = true;
            if (OB_FAIL(load_protection_mode_stat_(meta_protection_stat))) {
              LOG_WARN("failed to load protection mode stat", KR(ret));
            }
          }
        } else {
          if (REACH_THREAD_TIME_INTERVAL(10_s)) {
            ma_switch_helper_.dump();
          }
        }
      }
    }
  }
  return ret;
}


int ObProtectionModeMgr::load_protection_mode_stat_(
    share::ObProtectionStat &meta_protection_stat) const
{
  int ret = OB_SUCCESS;
  share::ObAllTenantInfo tenant_info;
  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id_) || !is_valid_tenant_id(user_tenant_id_)
    || OB_ISNULL(GCTX.sql_proxy_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id_), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(share::ObAllTenantInfoProxy::load_tenant_info(user_tenant_id_, GCTX.sql_proxy_,
     false/*for_update*/, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(user_tenant_id_));
  } else if (OB_UNLIKELY(!tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant info is not valid", KR(ret), K(tenant_info));
  } else if (!tenant_info.is_normal_status()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("tenant info is not normal status", KR(ret), K(tenant_info));
  } else if (OB_FAIL(meta_protection_stat.init(tenant_info))) {
    LOG_WARN("failed to init meta protection stat", KR(ret), K(tenant_info));
  }
  return ret;
}

int ObProtectionModeMgr::advance_upgrade_protection_mode_(
    share::ObProtectionStat &meta_protection_stat, bool &status_changed) const
{
  int ret = OB_SUCCESS;
  share::ObProtectionStat user_protection_stat;
  share::ObSyncStandbyStatusOperator sync_standby_status_operator(user_tenant_id_, GCTX.sql_proxy_);
  ObProtectionModeChangeHelper upgrade_helper;
  status_changed = false;
  if (OB_FAIL(sync_standby_status_operator.read_sync_standby_status(*GCTX.sql_proxy_,
        false/*for_update*/, user_protection_stat))) {
    LOG_WARN("failed to read sync standby status", KR(ret), K(user_tenant_id_));
  } else if (!(user_protection_stat == meta_protection_stat)) {
    status_changed = true;
    // situation:
    //   A set protection mode with switchover_epoch T1
    //   A switch to standby
    //   B switch to primary with switchover_epoch T2
    //   T2 may be less than T1 if A and B is not on the same server
    // so we need to update switchover_epoch in __all_tenant_info to make it larger than __all_sync_standby_status
    if (user_protection_stat.get_switchover_epoch() >= meta_protection_stat.get_switchover_epoch()) {
      ObProtectionStat tmp_meta_protection_stat;
      if (OB_FAIL(update_meta_switchover_epoch_by_user_(user_tenant_id_, meta_protection_stat,
              user_protection_stat, tmp_meta_protection_stat))) {
        LOG_WARN("failed to update meta switchover epoch", KR(ret), K(user_tenant_id_), K(user_protection_stat));
      } else {
        meta_protection_stat = tmp_meta_protection_stat;
      }
    }
    if (FAILEDx(notify_protection_stat_for_standby_tenant(user_tenant_id_, meta_protection_stat,
            user_protection_stat))) {
      LOG_WARN("failed to process protection mode to steady", KR(ret), K(user_tenant_id_),
          K(meta_protection_stat), K(user_protection_stat));
    } else {
      FLOG_INFO(PROTECTION_LOG_PREFIX "notify protection stat for standby tenant success", KR(ret), K(user_tenant_id_),
        K(meta_protection_stat), K(user_protection_stat));
    }
  } else if (meta_protection_stat.is_async_to_sync()) {
    // upgrade
    if (OB_FAIL(upgrade_helper.init(user_tenant_id_, meta_protection_stat))) {
      LOG_WARN("failed to init upgrade downgrade helper", KR(ret), K(user_tenant_id_),
        K(meta_protection_stat));
    } else if (OB_FAIL(upgrade_helper.upgrade_protection_mode())) {
      LOG_WARN("failed to upgrade protection mode", KR(ret), K(user_tenant_id_), K(meta_protection_stat));
    } else {
      status_changed = true;
      FLOG_INFO(PROTECTION_LOG_PREFIX "upgrade protection mode success", KR(ret), K(user_tenant_id_),
        K(meta_protection_stat));
    }
  }
  return ret;
}

int ObProtectionModeMgr::advance_downgrade_protection_mode_(
    share::ObProtectionStat &meta_protection_stat, bool &status_changed) const
{
  int ret = OB_SUCCESS;
  ObProtectionModeChangeHelper downgrade_helper;
  status_changed = false;
  if (meta_protection_stat.is_sync_to_async()) {
    int64_t new_switchover_epoch = OB_INVALID_VERSION;
    if (OB_FAIL(downgrade_helper.init(user_tenant_id_, meta_protection_stat))) {
      LOG_WARN("failed to init upgrade downgrade helper", KR(ret), K(user_tenant_id_), K(meta_protection_stat));
    } else if (OB_FAIL(downgrade_helper.downgrade_protection_mode(new_switchover_epoch))) {
      LOG_WARN("failed to downgrade protection mode", KR(ret), K(user_tenant_id_), K(meta_protection_stat));
    } else {
      status_changed = true;
      FLOG_INFO(PROTECTION_LOG_PREFIX "downgrade protection mode success", KR(ret), K(user_tenant_id_),
          K(meta_protection_stat));
    }
  }
  return ret;
}

int ObProtectionModeMgr::clear_sync_standby_dest_cache()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(inited), K_(user_tenant_id));
  } else if (OB_FAIL(sync_standby_dest_cache_.clear_cache())) {
    LOG_WARN("failed to clear sync standby dest cache", KR(ret), K_(user_tenant_id));
  } else {
    LOG_INFO(PROTECTION_LOG_PREFIX "clear sync standby dest cache", KR(ret), K_(user_tenant_id));
  }
  return ret;
}

int ObProtectionModeMgr::update_meta_switchover_epoch_by_user_(const uint64_t user_tenant_id,
    const share::ObProtectionStat &meta_protection_stat,
    const share::ObProtectionStat &user_protection_stat,
    share::ObProtectionStat &new_meta_protection_stat)
{
  int ret = OB_SUCCESS;
  int64_t new_switchover_epoch = OB_INVALID_TIMESTAMP;
  if (!is_user_tenant(user_tenant_id) || OB_ISNULL(GCTX.sql_proxy_)
      || !user_protection_stat.is_valid() || !meta_protection_stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), KP(GCTX.sql_proxy_),
        K(user_protection_stat), K(meta_protection_stat));
  } else if (meta_protection_stat.get_switchover_epoch() > user_protection_stat.get_switchover_epoch()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta switchover_epoch is larger than user switchover_epoch, no need to update",
        KR(ret), K(user_tenant_id), K(user_protection_stat), K(meta_protection_stat));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_protection_mode_and_level(user_tenant_id,
          ObTenantRole(ObTenantRole::PRIMARY_TENANT), GCTX.sql_proxy_,
          meta_protection_stat.get_switchover_epoch(), meta_protection_stat.get_protection_mode(),
          meta_protection_stat.get_protection_level(),
          user_protection_stat.get_switchover_epoch() + 1, new_switchover_epoch))) {
    LOG_WARN("failed to update tenant protection mode and level", KR(ret), K(user_tenant_id),
        K(user_protection_stat), K(meta_protection_stat));
  } else if (new_switchover_epoch <= user_protection_stat.get_switchover_epoch() ||
      new_switchover_epoch <= meta_protection_stat.get_switchover_epoch()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("switchover_epoch is too small", KR(ret), K(new_switchover_epoch),
        K(user_protection_stat), K(meta_protection_stat));
  } else if (OB_FAIL(new_meta_protection_stat.init(meta_protection_stat.get_protection_mode(),
          meta_protection_stat.get_protection_level(), new_switchover_epoch))) {
    LOG_WARN("failed to init new meta protection_stat", KR(ret), K(meta_protection_stat),
        K(new_switchover_epoch));
  } else {
    FLOG_INFO(PROTECTION_LOG_PREFIX "change meta tenant switchover_epoch according to user", KR(ret),
        K(user_tenant_id), K(meta_protection_stat), K(user_protection_stat), K(new_meta_protection_stat));
  }
  return ret;
}

// change user tenant according to meta tenant
int ObProtectionModeMgr::notify_protection_stat_for_standby_tenant(const uint64_t user_tenant_id,
    const share::ObProtectionStat &meta_protection_stat,
    const share::ObProtectionStat &user_protection_stat)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  share::ObSyncStandbyDestStruct sync_standby_dest_struct;
  bool is_sync_standby_dest_empty = false;
  share::ObSyncStandbyStatusAttr sync_standby_status_attr;
  share::ObSyncStandbyStatusOperator sync_standby_status_operator(user_tenant_id, GCTX.sql_proxy_);
  if (!is_user_tenant(user_tenant_id) || !is_valid_tenant_id(user_tenant_id)
    || !meta_protection_stat.is_valid() || !user_protection_stat.is_valid()
    || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id), K(meta_protection_stat),
      K(user_protection_stat));
  } else if (meta_protection_stat == user_protection_stat) {
    // do nothing
    LOG_TRACE(PROTECTION_LOG_PREFIX "protection mode is already steady, do nothing", KR(ret), K(user_tenant_id),
      K(meta_protection_stat), K(user_protection_stat));
  } else if (meta_protection_stat.get_switchover_epoch() <= user_protection_stat.get_switchover_epoch()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta tenant's switchover_epoch should be larger than user_protection_stat's",
        KR(ret), K(user_tenant_id), K(meta_protection_stat), K(user_protection_stat));
  } else if (meta_protection_stat.get_protection_level().is_pre_maximum_performance()) {
    LOG_TRACE(PROTECTION_LOG_PREFIX "protection level is in pre maximum performance, do nothing", KR(ret), K(user_tenant_id),
      K(meta_protection_stat), K(user_protection_stat));
  } else if (OB_FAIL(ObProtectionModeUtils::get_sync_standby_status_attr(user_tenant_id,
      meta_protection_stat.get_switchover_epoch(), sync_standby_status_attr))) {
      LOG_WARN("failed to get sync standby status attr", KR(ret), K(user_tenant_id), K(meta_protection_stat));
  } else if (OB_FAIL(sync_standby_status_operator.update_sync_standby_status(user_protection_stat,
      sync_standby_status_attr))) {
    LOG_WARN("failed to update sync standby status", KR(ret), K(user_tenant_id), K(meta_protection_stat),
      K(user_protection_stat), K(sync_standby_status_attr));
  } else {
    LOG_INFO(PROTECTION_LOG_PREFIX "update sync standby status success", KR(ret), K(user_tenant_id), K(meta_protection_stat),
      K(user_protection_stat), K(sync_standby_status_attr));
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_BEFORE_ADVANCE_META_TENANT_PROTECTION_STAT);
int ObProtectionModeMgr::advance_meta_tenant_protection_stat(const uint64_t meta_tenant_id,
  const share::ObProtectionStat &meta_protection_stat, ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  int64_t new_switchover_epoch = OB_INVALID_VERSION;
  share::ObProtectionStat next_meta_protection_stat;
  const uint64_t user_tenant_id = gen_user_tenant_id(meta_tenant_id);
  DEBUG_SYNC(BEFORE_ADVANCE_META_TENANT_PROTECTION_STAT);
  if (OB_FAIL(ERRSIM_BEFORE_ADVANCE_META_TENANT_PROTECTION_STAT)) {
    LOG_WARN("ERRSIM_BEFORE_ADVANCE_META_TENANT_PROTECTION_STAT", KR(ret));
  } else if (!is_meta_tenant(meta_tenant_id) || !is_valid_tenant_id(meta_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(meta_protection_stat.get_next_protection_stat(next_meta_protection_stat))) {
    LOG_WARN("failed to get next meta protection stat", KR(ret), K(meta_protection_stat));
  } else if (next_meta_protection_stat == meta_protection_stat) {
    // do nothing
    LOG_TRACE(PROTECTION_LOG_PREFIX "next meta protection stat is already the same as current, do nothing",
      KR(ret), K(meta_tenant_id), K(meta_protection_stat), K(next_meta_protection_stat));
  } else if (OB_FAIL(share::ObAllTenantInfoProxy::update_tenant_protection_mode_and_level(
    user_tenant_id, ObTenantRole(ObTenantRole::PRIMARY_TENANT), proxy, meta_protection_stat.get_switchover_epoch(),
    next_meta_protection_stat.get_protection_mode(), next_meta_protection_stat.get_protection_level(),
    meta_protection_stat.get_switchover_epoch()/* min switchover epoch */, new_switchover_epoch))) {
    LOG_WARN("failed to update tenant protection mode and level", KR(ret), K(meta_tenant_id), K(meta_protection_stat));
  } else {
    FLOG_INFO(PROTECTION_LOG_PREFIX "update tenant protection mode and level success", KR(ret),
     K(meta_tenant_id), K(meta_protection_stat), K(next_meta_protection_stat), K(new_switchover_epoch));
  }
  return ret;
}
#undef PROTECTION_LOG_PREFIX
} // namespace standby
} // namespace oceanbase
