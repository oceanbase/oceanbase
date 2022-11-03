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
#include "ob_tenant_role_transition_service.h"
#include "logservice/palf/log_define.h"
#include "logservice/ob_log_service.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"//ObChangeLSAccessModeProxy
#include "share/ob_rpc_struct.h"//ObLSAccessModeInfo
#include "observer/ob_server_struct.h"//GCTX
#include "share/location_cache/ob_location_service.h"//get ls leader
#include "share/ob_schema_status_proxy.h"//set_schema_status

namespace oceanbase
{
using namespace share;
namespace rootserver
{
////////////LSAccessModeInfo/////////////////
int ObTenantRoleTransitionService::LSAccessModeInfo::init(
    uint64_t tenant_id, const ObLSID &ls_id, const ObAddr &addr,
    const int64_t mode_version,
    const palf::AccessMode &access_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid() || !addr.is_valid()
                  || palf::INVALID_PROPOSAL_ID == mode_version
                  || palf::AccessMode::INVALID_ACCESS_MODE == access_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(addr),
    K(access_mode), K(mode_version));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    leader_addr_ = addr;
    mode_version_ = mode_version;
    access_mode_ = access_mode;
  }
  return ret;
}

int ObTenantRoleTransitionService::LSAccessModeInfo::assign(const LSAccessModeInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    leader_addr_ = other.leader_addr_;
    mode_version_ = other.mode_version_; 
    access_mode_ = other.access_mode_;
  }
  return ret;
}

void ObTenantRoleTransitionService::LSAccessModeInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  leader_addr_.reset();
  access_mode_ = palf::AccessMode::INVALID_ACCESS_MODE;
  mode_version_ = palf::INVALID_PROPOSAL_ID; 
} 
////////////ObTenantRoleTransitionService//////////////
int ObTenantRoleTransitionService::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(rpc_proxy_) ||
      OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  }
  return ret;
} 

int ObTenantRoleTransitionService::failover_to_primary()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[ROLE_TRANSITION] start to failover to primary", KR(ret), K(tenant_id_));
  const int64_t start_service_time = ObTimeUtility::current_time();
  ObAllTenantInfo tenant_info;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_,
                                                    false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
  } else if (tenant_info.is_primary()) {
    LOG_INFO("is primary tenant, no need failover");
  } else if (FALSE_IT(switchover_epoch_ = tenant_info.get_switchover_epoch())) {
  } else if (tenant_info.is_normal_status()) {
    //do failover to primary
    if (OB_FAIL(do_failover_to_primary_(tenant_info))) {
      LOG_WARN("failed to do failover to primary", KR(ret), K(tenant_info));
    }
  } else if (tenant_info.is_prepare_flashback_status()) {
    //prepare flashback
    if (OB_FAIL(do_prepare_flashback_(tenant_info))) {
      LOG_WARN("failed to prepare flashback", KR(ret), K(tenant_info));
    }
  } else if (tenant_info.is_flashback_status()) {
    if (OB_FAIL(do_flashback_(tenant_info))) {
      LOG_WARN("failed to flashback", KR(ret), K(tenant_info));
    }
  } else if (tenant_info.is_switching_status()) {
    if (OB_FAIL(do_switch_access_mode_(tenant_info, share::PRIMARY_TENANT_ROLE))) {
      LOG_WARN("failed to switch access mode", KR(ret), K(tenant_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant info not expected", KR(ret), K(tenant_info));
  }
  const int64_t cost = ObTimeUtility::current_time() - start_service_time;
  LOG_INFO("[ROLE_TRANSITION] finish failover to primary", KR(ret), K(tenant_info), K(cost));
  return ret;
}
  
int ObTenantRoleTransitionService::do_failover_to_primary_(const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo new_tenant_info;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_normal_status()
                 || tenant_info.is_primary()
                 || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K(switchover_epoch_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(
                 tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
                 share::STANDBY_TENANT_ROLE, share::PREPARE_FLASHBACK_SWITCHOVER_STATUS, switchover_epoch_))) {
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
int ObTenantRoleTransitionService::do_prepare_flashback_(const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(
        !tenant_info.is_prepare_flashback_status()
        || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K(switchover_epoch_));
  } else if (OB_FAIL(try_create_abort_ls_(tenant_info.get_switchover_status()))) {
    LOG_WARN("failed to create abort ls", KR(ret), K(tenant_info));
  }
  //TODO flashback access mode not ready
  if (FAILEDx(update_tenant_stat_info_())) {
    LOG_WARN("failed to update tenant stat info", KR(ret));
  }

  DEBUG_SYNC(BEFORE_DO_FLASHBACK);

  if (OB_SUCC(ret)) {
    ObAllTenantInfo new_tenant_info;
    if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_switchover_status(
           tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
           tenant_info.get_switchover_status(), share::FLASHBACK_SWITCHOVER_STATUS))) {
      LOG_WARN("failed to update tenant switchover status", KR(ret), K(tenant_id_), K(tenant_info));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
           tenant_id_, sql_proxy_, false, new_tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(new_tenant_info.get_switchover_epoch() != tenant_info.get_switchover_epoch())) {
      ret = OB_NEED_RETRY;
      LOG_WARN("switchover is concurrency", KR(ret), K(tenant_info), K(new_tenant_info));
    } else if (OB_FAIL(do_flashback_(new_tenant_info))) {
      LOG_WARN("failed to prepare flashback", KR(ret), K(new_tenant_info));
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::do_flashback_(const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_flashback_status()
        || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K(switchover_epoch_));
  } else {
    //1. flashback log TODO
    ObAllTenantInfo new_tenant_info;
    if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_switchover_status(
            tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
            tenant_info.get_switchover_status(), share::SWITCHING_SWITCHOVER_STATUS))) {
      LOG_WARN("failed to update tenant role", KR(ret), K(tenant_id_), K(tenant_info));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_,
                                                      false, new_tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(new_tenant_info.get_switchover_epoch() != tenant_info.get_switchover_epoch())) {
      ret = OB_NEED_RETRY;
      LOG_WARN("switchover is concurrency", KR(ret), K(tenant_info), K(new_tenant_info));
    } else if (OB_FAIL(do_switch_access_mode_(new_tenant_info, share::PRIMARY_TENANT_ROLE))) {
      LOG_WARN("failed to prepare flashback", KR(ret), K(new_tenant_info));
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::do_switch_access_mode_(
    const share::ObAllTenantInfo &tenant_info,
    const share::ObTenantRole &target_tenant_role) 
{
  int ret = OB_SUCCESS;
  palf::AccessMode access_mode = logservice::ObLogService::get_palf_access_mode(target_tenant_role); 
  const int64_t ref_ts = tenant_info.get_ref_scn();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_switching_status()
        || target_tenant_role == tenant_info.get_tenant_role()
        || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info),
        K(target_tenant_role), K(switchover_epoch_));
  } else if (OB_FAIL(change_ls_access_mode_(access_mode, ref_ts))) {
    LOG_WARN("failed to get access mode", KR(ret), K(access_mode), K(ref_ts), K(tenant_info));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(
          tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
          share::PRIMARY_TENANT_ROLE, share::NORMAL_SWITCHOVER_STATUS, switchover_epoch_))) {
    LOG_WARN("failed to update tenant switchover status", KR(ret), K(tenant_id_), K(tenant_info));
  }
  return ret;
}

int ObTenantRoleTransitionService::try_create_abort_ls_(const share::ObTenantSwitchoverStatus &status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("switchover stauts not valid", KR(ret), K(status));
  } else {
    ObMySQLTransaction trans;
    share::ObLSStatusInfoArray status_info_array;
    ObLSStatusOperator status_op;
    ObAllTenantInfo tenant_info;
    const uint64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(tenant_id_);
    if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
            tenant_id_, &trans, true, tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(switchover_epoch_ != tenant_info.get_switchover_epoch()
          || status != tenant_info.get_switchover_status())) {
      ret = OB_NEED_RETRY;
      LOG_WARN("switchover may concurrency, need retry", KR(ret), K(switchover_epoch_), K(tenant_info));
    } else if (OB_FAIL(status_op.get_all_ls_status_by_order(
                 tenant_id_, status_info_array, trans))) {
      LOG_WARN("failed to update ls status", KR(ret), K(tenant_id_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
      const ObLSStatusInfo &info = status_info_array.at(i);
      if (info.ls_is_created() || info.ls_is_creating()) {
        if (OB_FAIL(status_op.update_ls_status_in_trans(
                tenant_id_, info.ls_id_, info.status_, share::OB_LS_CREATE_ABORT, trans))) {
          LOG_WARN("failed to update ls status", KR(ret), K(tenant_id_), K(info));
        }
      }
    }//end for
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  LOG_INFO("[ROLE_TRANSITION] finish create abort ls", KR(ret), K(tenant_id_));
  return ret;
}

int ObTenantRoleTransitionService::update_tenant_stat_info_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else {
    //TODO, get all ls sync_ts_ns, update __all_tenant_info,
    //the new sync_ts_ns cannot larger than recovery_scn and sync_ts_ns of sys_ls
  }
  LOG_INFO("[ROLE_TRANSITION] finish update tenant stat info", KR(ret), K(tenant_id_));
  return ret;
}

int ObTenantRoleTransitionService::change_ls_access_mode_(palf::AccessMode target_access_mode,
                             int64_t ref_ts)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(palf::AccessMode::INVALID_ACCESS_MODE == target_access_mode 
                         || OB_INVALID_VERSION == switchover_epoch_
                         || OB_LS_INVALID_SCN_VALUE == ref_ts)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_access_mode), K(switchover_epoch_), K(ref_ts));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    ObArray<LSAccessModeInfo> ls_mode_info;
    ObArray<LSAccessModeInfo> need_change_info;
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
      } else if (OB_FAIL(get_ls_access_mode_(ls_mode_info))) {
        LOG_WARN("failed to get ls access mode", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_mode_info.count(); ++i) {
          const LSAccessModeInfo &info = ls_mode_info.at(i);
          if (info.access_mode_ == target_access_mode) {
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
                target_access_mode, ref_ts))) {
          LOG_WARN("failed to change ls access mode", KR(ret), K(need_change_info),
              K(target_access_mode), K(ref_ts));
        }
      }
      if (OB_SUCC(ret)) {
        break;
      }
    } while (need_retry);

  }
  LOG_INFO("[ROLE_TRANSITION] finish change ls mode", KR(ret), K(tenant_id_),
      K(target_access_mode), K(ref_ts));
  return ret;

}

int ObTenantRoleTransitionService::get_ls_access_mode_(ObIArray<LSAccessModeInfo> &ls_access_info)
{
  int ret = OB_SUCCESS;
  ls_access_info.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else {
    share::ObLSStatusInfoArray status_info_array;
    ObLSStatusOperator status_op;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else if (OB_FAIL(status_op.get_all_ls_status_by_order(
                 tenant_id_, status_info_array, *sql_proxy_))) {
      LOG_WARN("failed to update ls status", KR(ret), K(tenant_id_));
    } else {
      ObAddr leader;
      ObGetLSAccessModeProxy proxy(
          *rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_access_mode);
      obrpc::ObGetLSAccessModeInfoArg arg;
      int64_t rpc_count = 0;
      ObArray<int> return_code_array;
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
        return_code_array.reset();
        const ObLSStatusInfo &info = status_info_array.at(i);
        const int64_t timeout = ctx.get_timeout();
        if (info.ls_is_create_abort()) {
          LOG_INFO("LS is create abort, no need process", KR(ret), K(info));
        } else if (OB_FAIL(GCTX.location_service_->get_leader(
          GCONF.cluster_id, tenant_id_, info.ls_id_, false, leader))) {
          LOG_WARN("failed to get leader", KR(ret), K(tenant_id_), K(info));

        } else if (OB_FAIL(arg.init(tenant_id_, info.ls_id_))) {
          LOG_WARN("failed to init arg", KR(ret), K(tenant_id_), K(info));
        } else if (OB_FAIL(proxy.call(leader, timeout, GCONF.cluster_id, tenant_id_, arg))) {
          LOG_WARN("failed to send rpc", KR(ret), K(leader), K(timeout), K(tenant_id_), K(arg));
        } else {
          rpc_count++;
        }
        if (OB_FAIL(ret)) {
          const obrpc::ObGetLSAccessModeInfoArg &arg = proxy.get_args().at(i);
          if (OB_SUCCESS !=(tmp_ret = GCTX.location_service_->nonblock_renew(
                  GCONF.cluster_id, tenant_id_, info.ls_id_))) {
            LOG_WARN("failed to renew location", KR(ret), K(tenant_id_), K(info));
          }
        }
      }//end for
      //get result
      //need to wait all result whether success or fail
      if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_code_array))) {
        LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_FAIL(ret)) {
        //no need to process return code
      } else if (rpc_count != return_code_array.count() ||
                 rpc_count != proxy.get_args().count() ||
                 rpc_count != proxy.get_results().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rpc count not equal to result count", KR(ret),
                 K(rpc_count), K(return_code_array), "arg count",
                 proxy.get_args().count());
      } else {
        LSAccessModeInfo info;
        for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
          ret = return_code_array.at(i);
          const obrpc::ObGetLSAccessModeInfoArg &arg = proxy.get_args().at(i);
          if (OB_FAIL(ret)) {
            LOG_WARN("send rpc is failed", KR(ret), K(i));
          } else {
            const auto *result = proxy.get_results().at(i);
            const ObAddr &leader = proxy.get_dests().at(i);
            if (OB_ISNULL(result)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("result is null", KR(ret), K(i));
            } else if (OB_FAIL(info.init(tenant_id_, result->get_ls_id(),
            leader, result->get_mode_version(), result->get_access_mode()))) {
              LOG_WARN("failed to init info", KR(ret), KPC(result), K(leader));
            } else if (OB_FAIL(ls_access_info.push_back(info))) {
              LOG_WARN("failed to push back info", KR(ret), K(info));
            }
          }
          LOG_INFO("[ROLE_TRANSITION] get ls access mode", KR(ret), K(arg));
       
          if (OB_FAIL(ret)) {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS !=(tmp_ret = GCTX.location_service_->nonblock_renew(
                     GCONF.cluster_id, tenant_id_, arg.get_ls_id()))) {
              LOG_WARN("failed to renew location", KR(ret), K(tenant_id_),
                       K(arg));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::do_change_ls_access_mode_(const ObIArray<LSAccessModeInfo> &ls_access_info,
                                palf::AccessMode target_access_mode, int64_t ref_ts)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(0== ls_access_info.count()
                         || palf::AccessMode::INVALID_ACCESS_MODE == target_access_mode 
                         || OB_INVALID_VERSION == ref_ts)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_access_mode), K(ls_access_info), K(ref_ts));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx,
                                                          GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else {
    ObChangeLSAccessModeProxy proxy(*rpc_proxy_, &obrpc::ObSrvRpcProxy::change_ls_access_mode);
    obrpc::ObLSAccessModeInfo arg;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_access_info.count(); ++i) {
      const LSAccessModeInfo &info = ls_access_info.at(i);
      const int64_t timeout = ctx.get_timeout();
      if (OB_FAIL(arg.init(tenant_id_, info.ls_id_, info.mode_version_, target_access_mode, ref_ts))) {
        LOG_WARN("failed to init arg", KR(ret), K(info), K(target_access_mode), K(ref_ts));
      } else if (OB_FAIL(proxy.call(info.leader_addr_, timeout, GCONF.cluster_id, tenant_id_, arg))) {
        LOG_WARN("failed to send rpc", KR(ret), K(info), K(timeout), K(tenant_id_), K(arg));
      }
    }//end for
    //result
    ObArray<int> return_code_array;
    const int64_t rpc_count = ls_access_info.count();
    if (FAILEDx(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret));
    } else if (rpc_count != return_code_array.count() ||
               rpc_count != proxy.get_args().count() ||
               rpc_count != proxy.get_results().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc count not equal to result count", KR(ret), K(rpc_count),
               K(return_code_array), "arg count", proxy.get_args().count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        ret = return_code_array.at(i);
        const obrpc::ObLSAccessModeInfo &arg = proxy.get_args().at(i);
        const auto *result = proxy.get_results().at(i);
        if (OB_FAIL(ret)) {
          LOG_WARN("send rpc is failed", KR(ret), K(i));
        } else if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(i));
        } else if (OB_FAIL(result->get_result())) {
          LOG_WARN("failed to change ls mode", KR(ret), KPC(result));
        }

        LOG_INFO("[ROLE_TRANSITION] change ls access mode", KR(ret), K(arg), KPC(result),
            "leader", proxy.get_dests().at(i));
        if (OB_FAIL(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS !=
              (tmp_ret = GCTX.location_service_->nonblock_renew(
                   GCONF.cluster_id, tenant_id_, arg.get_ls_id()))) {
            LOG_WARN("failed to renew location", KR(ret), K(tenant_id_),
                     K(arg));
          }
        }
      }// end for
    }
  }
  return ret;
}

}
}
