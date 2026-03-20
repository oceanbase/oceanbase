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
#include "rootserver/standby/ob_ls_log_mode_modifier.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "share/ob_share_util.h"
#include "share/config/ob_server_config.h"
#include "share/ob_tenant_info_proxy.h"
#include "share/location_cache/ob_location_service.h"
#include "rootserver/standby/ob_protection_mode_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::palf;

namespace oceanbase
{
using namespace share;
using namespace palf;
using namespace common;
namespace rootserver
{

using namespace standby;

template <typename T>
ObLSID get_ls_id(const T &t)
{
  return t.get_ls_id();
}
template <>
ObLSID get_ls_id(const ObLSID &ls_id)
{
  return ls_id;
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
      const ObLSID ls_id = get_ls_id(array_l.at(i));
      if (!has_exist_in_array(array_r, ls_id)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(GCTX.location_service_->nonblock_renew(GCONF.cluster_id, tenant_id, ls_id))) {
          LOG_WARN("failed to renew location", KR(tmp_ret), K(tenant_id), K(ls_id));
        }
      }
    }
  }
  return ret;
}


int ObLSLogModeModifier::change_ls_access_mode()
{
  int ret = OB_SUCCESS;
  ObArray<ObLSID> ls_ids;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("invalid inner arg", KR(ret));
  } else if (OB_ISNULL(status_info_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status info array is null", KR(ret));
  } else {
    bool protection_mode_enabled = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array_->count(); ++i) {
      const ObLSStatusInfo &status = status_info_array_->at(i);
      if (OB_FAIL(ls_ids.push_back(status.get_ls_id()))) {
        LOG_WARN("failed to push back ls id", KR(ret), K(status));
      }
    }
    if (FAILEDx(ObProtectionModeUtils::check_tenant_data_version_for_protection_mode(tenant_id_,
            protection_mode_enabled))) {
      LOG_WARN("failed to check protection mode enabled", KR(ret), K(tenant_id_));
    } else if (protection_mode_enabled) {
      ObSyncStandbyStatusAttr protection_log;
      ObArray<obrpc::ObChangeLSSyncModeRes> change_ls_sync_mode_res_array;
      if (OB_FAIL(ObProtectionModeUtils::get_sync_standby_status_attr(tenant_id_, switchover_epoch_,
        protection_log))) {
        LOG_WARN("failed to get sync standby status attr", KR(ret), K(tenant_id_), K(switchover_epoch_));
      } else if (OB_FAIL(change_ls_sync_mode_until_success(tenant_id_, ls_ids, target_sync_mode_,
          ref_scn_, protection_log, switchover_epoch_, change_ls_sync_mode_res_array,
          false/*force_check_result*/))) {
        LOG_WARN("failed to change ls sync mode until success", KR(ret), K(tenant_id_), K(ls_ids),
            K(target_sync_mode_), K(ref_scn_), K(protection_log), K(switchover_epoch_));
      }
    }
    if (FAILEDx(change_ls_access_mode_until_success(tenant_id_, ls_ids, target_access_mode_,
        ref_scn_, sys_ls_sync_scn_, switchover_epoch_))) {
      LOG_WARN("failed to change ls access mode until success", KR(ret), K(tenant_id_), K(ls_ids),
          K(target_access_mode_), K(ref_scn_), K(sys_ls_sync_scn_), K(switchover_epoch_));
    }
  }
  LOG_INFO("[CHANGE_ACCESS_SYNC_MODE] finish change ls mode", KR(ret), K(tenant_id_), K(target_access_mode_), K(ref_scn_));
  return ret;
}


bool check_target_equal(const obrpc::ObLSAccessModeInfo &info, const palf::AccessMode &target_mode)
{
  return info.get_access_mode() == target_mode;
}

bool check_target_equal(const obrpc::ObLSAccessModeInfo &info, const palf::SyncMode &target_mode)
{
  return info.get_sync_mode() == target_mode;
}
template<typename TARGET_MODE, typename Arg, typename Res>
int ObLSLogModeModifier::change_ls_mode_until_success(const uint64_t tenant_id,
    const ObIArray<share::ObLSID> &ls_ids, const TARGET_MODE &target_mode, const SCN &ref_scn,
    const Arg &arg, const int64_t switchover_epoch, const bool force_check_result,
    ObIArray<Res> &results)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<ObLSID> success_ls_ids;
  ObTimeoutCtx ctx;
  if (!is_user_tenant(tenant_id) || !is_valid_tenant_id(tenant_id) || ls_ids.empty()
      || !ref_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_ids), K(ref_scn));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else if (OB_FAIL(success_ls_ids.create(hash::cal_next_prime(ls_ids.count())))) {
    LOG_WARN("failed to create check ls ids", KR(ret), K(ls_ids));
  } else {
    bool need_retry = true;
    ObArray<obrpc::ObLSAccessModeInfo> ls_mode_info;
    ObArray<obrpc::ObLSAccessModeInfo> need_change_info;
    ObAllTenantInfo new_tenant_info;
    ObArray<Res> tmp_results;
    do {
      ls_mode_info.reset();
      need_change_info.reset();
      tmp_results.reset();
      //1. get current access mode
      if (ctx.is_timeouted()) {
        need_retry = false;
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", KR(ret));
      } else if (OB_FAIL(get_ls_access_mode(tenant_id, ls_ids, ls_mode_info))) {
        LOG_WARN("failed to get ls access mode", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_mode_info.count(); ++i) {
          const obrpc::ObLSAccessModeInfo &info = ls_mode_info.at(i);
          bool need_check = false;
          // for sync mode, sync log may not be written, even if sync mode is right.
          // so we need to check result is success
          if (check_target_equal(info, target_mode)) {
            need_check = (force_check_result &&
              OB_HASH_EXIST != success_ls_ids.exist_refactored(info.get_ls_id()));
          } else {
            need_check = true;
          }
          if (need_check && OB_FAIL(need_change_info.push_back(info))) {
            LOG_WARN("failed to assign", KR(ret), K(i), K(info));
          }
        }
        LOG_INFO("ls need to change ls mode", KR(ret), K(need_change_info));
        //2. check epoch not change
        if (OB_SUCC(ret) && need_change_info.count() > 0) {
          if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                  tenant_id, GCTX.sql_proxy_, false, new_tenant_info))) {
            LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
          } else if (switchover_epoch != new_tenant_info.get_switchover_epoch()) {
            need_retry = false;
            ret = OB_NEED_RETRY;
            LOG_WARN("epoch change, no need change access mode", KR(ret),
                K(switchover_epoch), K(new_tenant_info), K(target_mode));
          }
        }
        //3. change access mode
        if (OB_SUCC(ret) && need_change_info.count() > 0) {
          if (OB_FAIL(change_ls_mode(tenant_id, need_change_info, target_mode, ref_scn, arg, tmp_results))) {
            LOG_WARN("failed to change ls access mode", KR(ret), K(need_change_info));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < tmp_results.count(); ++i) {
              const Res &result = tmp_results.at(i);
              if (result.get_result() != OB_SUCCESS) {
                LOG_WARN("failed to change ls access mode", KR(ret), K(result));
              } else if (OB_FAIL(results.push_back(result))) {
                LOG_WARN("failed to push back result", KR(ret), K(result));
              } else if (OB_FAIL(success_ls_ids.set_refactored(result.get_ls_id()))) {
                LOG_WARN("failed to set success ls id", KR(ret), K(result));
              }
            }
          }
        }
        if (OB_SUCC(ret) && (need_change_info.empty() || success_ls_ids.size() == ls_ids.count())) {
          break;
        }
        ob_usleep(100_ms);
      }//end if
    } while (need_retry);
  }
  return ret;
}

int ObLSLogModeModifier::change_ls_sync_mode_until_success(const uint64_t tenant_id,
  const ObIArray<share::ObLSID> &ls_ids, const palf::SyncMode &target_sync_mode, const SCN &ref_scn,
  const share::ObSyncStandbyStatusAttr &protection_log, const int64_t switchover_epoch,
  ObIArray<obrpc::ObChangeLSSyncModeRes> &results, const bool force_check_result)
{
  return change_ls_mode_until_success<palf::SyncMode, share::ObSyncStandbyStatusAttr, obrpc::ObChangeLSSyncModeRes>(
    tenant_id, ls_ids, target_sync_mode, ref_scn, protection_log, switchover_epoch,
    force_check_result, results);
}
int ObLSLogModeModifier::change_ls_access_mode_until_success(const uint64_t tenant_id,
  const ObIArray<share::ObLSID> &ls_ids, const palf::AccessMode &target_access_mode, const SCN &ref_scn,
  const SCN &sys_ls_sync_scn, const int64_t switchover_epoch)
{
  ObArray<obrpc::ObChangeLSAccessModeRes> results;
  return change_ls_mode_until_success<palf::AccessMode, SCN, obrpc::ObChangeLSAccessModeRes>(
    tenant_id, ls_ids, target_access_mode, ref_scn, sys_ls_sync_scn, switchover_epoch, false, results);
}
int ObLSLogModeModifier::change_ls_mode(const uint64_t tenant_id,
  const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
  const palf::SyncMode target_sync_mode, const SCN &ref_scn,
  const share::ObSyncStandbyStatusAttr &protection_log,
  ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array)
{
  return change_ls_sync_mode(tenant_id, ls_access_info, target_sync_mode, ref_scn, protection_log,
     change_ls_sync_mode_res_array);
}

int ObLSLogModeModifier::change_ls_mode(const uint64_t tenant_id,
  const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
  const palf::AccessMode target_access_mode, const SCN &ref_scn,
  const SCN &sys_ls_sync_scn,
  ObIArray<obrpc::ObChangeLSAccessModeRes> &change_ls_access_mode_res_array)
{
  return change_ls_access_mode(tenant_id, ls_access_info, target_access_mode, ref_scn,
     sys_ls_sync_scn, change_ls_access_mode_res_array);
}

template<typename Res>
int check_success(int tmp_ret, const Res &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tmp_ret)) {
    LOG_WARN("tmp_ret is not success", KR(ret), K(tmp_ret), K(result));
  } else if (OB_FAIL(result.get_result())) {
    LOG_WARN("result is not success", KR(ret), K(result));
  }
  return ret;
}

template<>
int check_success<obrpc::ObLSAccessModeInfo>(int tmp_ret, const obrpc::ObLSAccessModeInfo &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tmp_ret)) {
    LOG_WARN("tmp_ret is not success", KR(ret), K(tmp_ret), K(result));
  }
  return ret;
}

int ObLSLogModeModifier::get_ls_access_mode(const uint64_t tenant_id, const ObIArray<ObLSID> &ls_ids,
  ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info)
{
  int ret = OB_SUCCESS;
  ls_access_info.reset();
  if (!is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id) || ls_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_ids));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.location_service_), KP(GCTX.srv_rpc_proxy_));
  } else {
    ObGetLSAccessModeProxy proxy(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_access_mode);
    ObArray<obrpc::ObGetLSAccessModeInfoArg> args;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.count(); ++i) {
      obrpc::ObGetLSAccessModeInfoArg arg;
      if (OB_FAIL(arg.init(tenant_id, ls_ids.at(i)))) {
        LOG_WARN("failed to init arg", KR(ret), K(tenant_id), K(ls_ids.at(i)));
      } else if (OB_FAIL(args.push_back(arg))) {
        LOG_WARN("failed to push back arg", KR(ret), K(arg));
      }
    }
    if (FAILEDx(call_and_renew_location(tenant_id, args, proxy, ls_access_info))) {
      LOG_WARN("failed to call and renew location", KR(ret), K(tenant_id), K(ls_ids));
    } else if (ls_access_info.count() != ls_ids.count()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("ls access info count not equal to ls ids count", KR(ret), K(ls_access_info.count()), K(ls_ids.count()));
    } else {
      LOG_INFO("[CHANGE_ACCESS_SYNC_MODE] get ls access mode", KR(ret), K(tenant_id), K(ls_ids), K(ls_access_info));
    }
  }
  return ret;
}

int ObLSLogModeModifier::get_ls_access_mode(ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSID> ls_ids;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else {
    FOREACH_CNT_X(status, *status_info_array_, OB_SUCC(ret)) {
      if (OB_ISNULL(status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("status is null", KR(ret), KP(status));
      } else if (OB_FAIL(ls_ids.push_back(status->get_ls_id()))) {
        LOG_WARN("failed to push back ls id", KR(ret), KPC(status));
      }
    }
    if (FAILEDx(get_ls_access_mode(tenant_id_, ls_ids, ls_access_info))) {
      LOG_WARN("failed to get ls access mode", KR(ret), K(tenant_id_), K(ls_ids));
    }
  }
  return ret;
}

int ObLSLogModeModifier::change_ls_sync_mode_and_check_result(
  const uint64_t tenant_id,
  const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
  const palf::SyncMode target_sync_mode,
  const share::ObSyncStandbyStatusAttr &protection_log,
  const SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  ObArray<obrpc::ObChangeLSSyncModeRes> change_ls_sync_mode_res_array;
  if (OB_FAIL(change_ls_sync_mode(tenant_id, ls_access_info, target_sync_mode, ref_scn,
      protection_log, change_ls_sync_mode_res_array))) {
    LOG_WARN("failed to change ls sync mode", KR(ret), K(tenant_id), K(ls_access_info));
  } else if (change_ls_sync_mode_res_array.count() != ls_access_info.count()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("change ls sync mode res array count not equal to ls access info count", KR(ret),
      K(change_ls_sync_mode_res_array.count()), K(ls_access_info.count()));
  } else {
    FOREACH_X(change_ls_sync_mode_res, change_ls_sync_mode_res_array, OB_SUCC(ret)) {
      if (OB_ISNULL(change_ls_sync_mode_res)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("change ls sync mode res is null or failed", KR(ret), KPC(change_ls_sync_mode_res));
      } else if (OB_FAIL(change_ls_sync_mode_res->get_result())) {
        LOG_WARN("failed to change sync mode", KR(ret), KPC(change_ls_sync_mode_res));
      }
    }
  }
  return ret;
}

template<typename Arg, typename Res, typename Proxy>
int ObLSLogModeModifier::call_and_renew_location(const uint64_t tenant_id,
  const ObIArray<Arg> &args,
  Proxy &proxy,
  ObIArray<Res> &results)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  ObArray<share::ObLSID> success_ls_ids;
  results.reset();
  ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
  if (OB_UNLIKELY(0 == args.count() || !is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(args), K(tenant_id));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.location_service_));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else {
    ObAddr leader;
    const uint64_t group_id = share::OBCG_DBA_COMMAND;
    for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
      const Arg &arg = args.at(i);
      const int64_t timeout = ctx.get_timeout();
      if (arg.get_tenant_id() != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_id not match", KR(ret), K(arg), K(tenant_id));
      } else if (OB_FAIL(GCTX.location_service_->get_leader(
          GCONF.cluster_id, tenant_id, get_ls_id(arg), false, leader))) {
          LOG_WARN("failed to get leader", KR(ret), K(tenant_id), K(arg));
      // use meta rpc process thread
      } else if (OB_FAIL(proxy.call(leader, timeout, GCONF.cluster_id, tenant_id, group_id, arg))) {
        //can not ignore of each ls
        LOG_WARN("failed to send rpc", KR(ret), K(arg), K(timeout),
            K(tenant_id), K(arg), K(group_id));
      } else {
        FLOG_INFO("[CHANGE_ACCESS_SYNC_MODE] send log mode rpc", KR(ret), K(arg), K(timeout), K(tenant_id), K(group_id));
      }
    }//end for
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;
    const int64_t rpc_count = args.count();
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
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        const Res *result = proxy.get_results().at(i);
        const Arg &arg = args.at(i);
        tmp_ret = return_code_array.at(i);
        if (OB_ISNULL(result) || OB_TMP_FAIL(tmp_ret)) {
          tmp_ret = OB_TMP_FAIL(tmp_ret) ? tmp_ret : OB_ERR_UNEXPECTED;
          LOG_WARN("failed to check success", KR(ret), K(tmp_ret), K(arg), KPC(result));
          ROOTSERVICE_EVENT_ADD("ls_log_mode", "ls_log_mode_rpc_failed", K(tenant_id), K(tmp_ret),
            K(arg), KPC(result), KPC(trace_id));
        } else if (OB_FAIL(results.push_back(*result))) {
          LOG_WARN("failed to push back result", KR(ret), K(arg), KPC(result));
        } else if (OB_TMP_FAIL(check_success(tmp_ret, *result))) {
          LOG_WARN("rpc result is not success", KR(ret), K(tmp_ret), K(arg), KPC(result));
        } else if (OB_TMP_FAIL(success_ls_ids.push_back(get_ls_id(arg)))) {
          LOG_WARN("fail to push back", KR(ret), KR(tmp_ret), K(success_ls_ids));
        }
      }// end for
    }
    FLOG_INFO("[CHANGE_ACCESS_SYNC_MODE] call ls log rpc", KR(ret), K(args), K(results),
        K(success_ls_ids), K(proxy.get_dests()));
    if (OB_FAIL(ret) || success_ls_ids.count() != args.count()) {
      if (OB_TMP_FAIL(do_nonblock_renew(args, success_ls_ids, tenant_id))) {
        LOG_WARN("failed to renew location", KR(ret), KR(tmp_ret), K(tenant_id), K(args), K(success_ls_ids));
      }
    }
  }
  if (OB_SUCC(ret) && args.count() != results.count()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("args count not equal to results count", KR(ret), K(args.count()), K(results.count()));
  }
  return ret;
}

template<typename Arg, typename Res, typename Proxy>
int ObLSLogModeModifier::do_change_mode_(const uint64_t tenant_id,
  const ObIArray<Arg> &args, Proxy &proxy,
  ObIArray<Res> &results)
{
  int ret = OB_SUCCESS;
  results.reset();
  if (OB_UNLIKELY(0 == args.count() || !is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(args), K(tenant_id));
  } else if (OB_FAIL(call_and_renew_location(tenant_id, args, proxy, results))) {
    LOG_WARN("failed to call and renew location", KR(ret), K(tenant_id), K(args));
  } else {
    FLOG_INFO("[CHANGE_ACCESS_SYNC_MODE] change ls log mode", KR(ret), K(args), K(results),
        K(proxy.get_dests()));
  }
  return ret;
}


int ObLSLogModeModifier::change_ls_access_mode(
  const uint64_t tenant_id,
  const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
  const palf::AccessMode target_access_mode,
  const SCN &ref_scn,
  const SCN &sys_ls_sync_scn,
  ObIArray<obrpc::ObChangeLSAccessModeRes> &change_ls_access_mode_res_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == ls_access_info.count() || OB_ISNULL(GCTX.srv_rpc_proxy_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_access_info), KP(GCTX.srv_rpc_proxy_));
  } else {
    ObChangeLSAccessModeProxy proxy(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::change_ls_access_mode);
    ObArray<obrpc::ObLSAccessModeInfo> access_mode_info_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_access_info.count(); ++i) {
      obrpc::ObLSAccessModeInfo arg;
      const obrpc::ObLSAccessModeInfo &info = ls_access_info.at(i);
      if (OB_FAIL(arg.init(tenant_id, info.get_ls_id(), info.get_mode_version(),
              target_access_mode, ref_scn, sys_ls_sync_scn, info.get_sync_mode()))) {
        LOG_WARN("failed to init arg", KR(ret), K(info), K(target_access_mode), K(ref_scn), K(sys_ls_sync_scn));
      } else if (OB_FAIL(access_mode_info_array.push_back(arg))) {
        LOG_WARN("failed to push back arg", KR(ret), K(arg));
      }
    }
    if (FAILEDx(do_change_mode_(tenant_id, access_mode_info_array, proxy,
        change_ls_access_mode_res_array))) {
      LOG_WARN("failed to change ls access mode", KR(ret), K(tenant_id), K(ls_access_info));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_MPT_BEFORE_SET_LS_TO_PRE_ASYNC);
ERRSIM_POINT_DEF(ERRSIM_MPT_BEFORE_SET_LS_TO_ASYNC);
ERRSIM_POINT_DEF(ERRSIM_MPT_BEFORE_SET_LS_TO_SYNC);

int ObLSLogModeModifier::change_ls_sync_mode(
  const uint64_t tenant_id,
  const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
  const palf::SyncMode target_sync_mode,
  const SCN &ref_scn,
  const share::ObSyncStandbyStatusAttr &protection_log,
  ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
  if (OB_UNLIKELY(0 == ls_access_info.count() || OB_ISNULL(GCTX.srv_rpc_proxy_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_access_info), KP(GCTX.srv_rpc_proxy_));
  } else {
    if (target_sync_mode == palf::SyncMode::PRE_ASYNC) {
      DEBUG_SYNC(MPT_BEFORE_SET_LS_TO_PRE_ASYNC);
      OZ (ERRSIM_MPT_BEFORE_SET_LS_TO_PRE_ASYNC);
    } else if (target_sync_mode == palf::SyncMode::ASYNC) {
      DEBUG_SYNC(MPT_BEFORE_SET_LS_TO_ASYNC);
      OZ (ERRSIM_MPT_BEFORE_SET_LS_TO_ASYNC);
    } else if (target_sync_mode == palf::SyncMode::SYNC) {
      DEBUG_SYNC(MPT_BEFORE_SET_LS_TO_SYNC);
      OZ (ERRSIM_MPT_BEFORE_SET_LS_TO_SYNC);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid target sync mode", KR(ret), K(target_sync_mode));
    }
    ObChangeLSSyncModeProxy proxy(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::change_ls_sync_mode);
    ObArray<obrpc::ObChangeLSSyncModeArg> sync_mode_arg_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_access_info.count(); ++i) {
      obrpc::ObChangeLSSyncModeArg arg;
      const obrpc::ObLSAccessModeInfo &info = ls_access_info.at(i);
      if (OB_FAIL(arg.init(tenant_id, info.get_ls_id(), info.get_mode_version(),
              ref_scn, target_sync_mode, protection_log))) {
        LOG_WARN("failed to init arg", KR(ret), K(info), K(ref_scn), K(target_sync_mode), K(protection_log));
      } else if (OB_FAIL(sync_mode_arg_array.push_back(arg))) {
        LOG_WARN("failed to push back arg", KR(ret), K(arg));
      }
    }
    if (FAILEDx(do_change_mode_(tenant_id, sync_mode_arg_array, proxy, change_ls_sync_mode_res_array))) {
      LOG_WARN("failed to change ls sync mode", KR(ret), K(tenant_id), K(ls_access_info));
    } else if (ls_access_info.count() != change_ls_sync_mode_res_array.count() || ls_access_info.count() != sync_mode_arg_array.count()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("ls access info count not equal to change ls sync mode res array count or sync mode arg array count", KR(ret),
        K(ls_access_info.count()), K(change_ls_sync_mode_res_array.count()), K(sync_mode_arg_array.count()));
    } else {
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < change_ls_sync_mode_res_array.count(); ++i) {
        const obrpc::ObChangeLSSyncModeRes &result = change_ls_sync_mode_res_array.at(i);
        if (OB_TMP_FAIL(result.get_result())) {
          ROOTSERVICE_EVENT_ADD("ls_log_mode", "change_ls_sync_mode_failed", K(tenant_id),
            "pre_access_info", ls_access_info.at(i), "sync_mode_arg", sync_mode_arg_array.at(i),
            K(target_sync_mode), K(result), KPC(trace_id));
        } else {
          ROOTSERVICE_EVENT_ADD("ls_log_mode", "change_ls_sync_mode_success", K(tenant_id),
            "pre_access_info", ls_access_info.at(i), "sync_mode_arg", sync_mode_arg_array.at(i),
            K(target_sync_mode), K(result), KPC(trace_id));
        }
      }
    }
  }
  return ret;
}

int ObLSLogModeModifier::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id_) || OB_INVALID_VERSION == switchover_epoch_
      || !ref_scn_.is_valid_and_not_min() || palf::AccessMode::INVALID_ACCESS_MODE == target_access_mode_)
      || OB_ISNULL(sql_proxy_) || OB_ISNULL(rpc_proxy_) || OB_ISNULL(status_info_array_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inner arg", KR(ret), K(tenant_id_), K(switchover_epoch_), K(ref_scn_), K(target_access_mode_),
        KP(sql_proxy_), KP(rpc_proxy_), K(status_info_array_));
  }
  return ret;
}

}// namespace rootserver
}// namespace oceanbase
