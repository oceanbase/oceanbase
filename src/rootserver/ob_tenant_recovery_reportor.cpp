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

#include "rootserver/ob_tenant_recovery_reportor.h"
#include "storage/tx_storage/ob_ls_service.h" //ObLSService
#include "storage/tx_storage/ob_ls_map.h"//ObLSIterator
#include "storage/ls/ob_ls.h"//ObLSGetMod
#include "observer/ob_server_struct.h"//GCTX
#include "lib/profile/ob_trace_id.h"
#include "lib/thread/threads.h"//set_run_wrapper
#include "share/ls/ob_ls_recovery_stat_operator.h" //ObLSRecoveryStatOperator
#include "share/schema/ob_multi_version_schema_service.h"//is_tenant_full_schema
#include "logservice/ob_log_service.h"//get_palf_role
#include "storage/tx_storage/ob_ls_handle.h"  //ObLSHandle

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace storage;
namespace rootserver
{
int ObTenantRecoveryReportor::mtl_init(ObTenantRecoveryReportor *&ka)
{
  return ka->init();
}
int ObTenantRecoveryReportor::init()
{
  int ret = OB_SUCCESS;
  lib::ThreadPool::set_run_wrapper(MTL_CTX());
  const int64_t thread_cnt = 1;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    sql_proxy_ = GCTX.sql_proxy_;
    tenant_id_ = MTL_ID();
    if (!is_user_tenant(tenant_id_)) {
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret));
    } else if (OB_FAIL(create(thread_cnt, "TeRec"))) {
      LOG_WARN("failed to create tenant recovery stat thread",
          KR(ret), K(thread_cnt));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}
void ObTenantRecoveryReportor::destroy()
{
  LOG_INFO("tenant recovery service destory", KPC(this));
  stop();
  wait();
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_info_.reset();
  sql_proxy_ = NULL;
}

int ObTenantRecoveryReportor::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_user_tenant(tenant_id_)) {
    LOG_INFO("not user tenant no need reported", K(tenant_id_));
  } else if (OB_FAIL(logical_start())) {
    LOG_WARN("failed to start", KR(ret));
  } else {
    ObThreadCondGuard guard(get_cond());
    get_cond().broadcast();
    LOG_INFO("tenant recovery service start", KPC(this));
  }
  return ret;
}

void ObTenantRecoveryReportor::stop()
{
  logical_stop();
}
void ObTenantRecoveryReportor::wait()
{
  logical_wait();
}

void ObTenantRecoveryReportor::wakeup()
{
   if (OB_NOT_INIT) {
   } else {
     ObThreadCondGuard guard(get_cond());
     get_cond().broadcast();
   }
 }

void ObTenantRecoveryReportor::run2()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("tenant recovery service run", KPC(this));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObThreadCondGuard guard(get_cond());
    const int64_t idle_time = IDLE_TIME_US;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
    while (!stop_) {
      if (OB_ISNULL(GCTX.schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema service is empty", KR(ret));
      } else if (!GCTX.schema_service_->is_tenant_full_schema(meta_tenant_id)) {
        //need wait
        LOG_INFO("tenant schema not ready", KR(ret), K(meta_tenant_id));
      } else {
        if (OB_SUCCESS != (tmp_ret = update_ls_recovery_stat_())) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to update ls recovery stat", KR(ret), KR(tmp_ret));
        }

        if (OB_SUCCESS != (tmp_ret = load_tenant_info_())) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to update tenant info", KR(ret), KR(tmp_ret));
        }
        //更新受控回放位点到replayservice
        if (OB_SUCCESS != (tmp_ret = update_replayable_point_())) {
          LOG_WARN("failed to update_replayable_point", KR(tmp_ret));
        }
      }
      if (!stop_) {
        get_cond().wait_us(idle_time);
      }
    }//end while
  }
}

int ObTenantRecoveryReportor::update_ls_recovery_stat_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObLSIterator *iter = NULL;
    common::ObSharedGuard<ObLSIterator> guard;
    ObLSService *ls_svr = MTL(ObLSService *);
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mtl ObLSService should not be null", KR(ret));
    } else if (OB_FAIL(ls_svr->get_ls_iter(guard,
            storage::ObLSGetMod::RS_MOD))) {
      LOG_WARN("get log stream iter failed", KR(ret));
    } else if (OB_ISNULL(iter = guard.get_ptr())) {
      LOG_WARN("iter is NULL", KR(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      ObLS *ls = nullptr;
      while (OB_SUCC(iter->get_next(ls))) {// ignore failed of each ls
        if (OB_ISNULL(ls)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("ls is null", KR(ret), KP(ls));
        } else {
          do {
            SpinRLockGuard guard(lock_);
            if (tenant_info_.is_valid()) {
              //更新ls_meta中的受控回放位点
              if (OB_SUCCESS != (tmp_ret = ls->update_ls_replayable_point(tenant_info_.get_replayable_scn()))) {
                LOG_WARN("failed to update_ls_replayable_point", KR(tmp_ret), KPC(ls), K(tenant_info_));
              }
            }
          } while (0);
          if (ls->is_sys_ls()) {
            // nothing todo
            // sys ls of user tenant is in ls_recovery
          } else if (OB_FAIL(update_ls_recovery(ls, sql_proxy_))) {
            LOG_WARN("failed to update ls recovery", KR(ret), KPC(ls));
          }
        }
      }//end while
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to get next ls", KR(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("return code not expected", KR(ret));
      }
    }
  }
  return ret;
}

int ObTenantRecoveryReportor::update_ls_recovery(ObLS *ls, common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *ls_svr = MTL(logservice::ObLogService*);
  if (OB_ISNULL(ls) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls or sql proxy is null", KR(ret), KP(ls),  KP(sql_proxy));
  } else {
    int64_t sync_scn = 0;
    int64_t readable_scn = 0;
    int64_t first_proposal_id = 0;
    int64_t second_proposal_id = 0;
    common::ObRole role;
    ObLSRecoveryStat ls_recovery_stat;
    const ObLSID ls_id = ls->get_ls_id();
    const uint64_t tenant_id = MTL_ID();

    ObLSRecoveryStatOperator ls_recovery;
    if (OB_FAIL(ls_svr->get_palf_role(ls_id, role, first_proposal_id))) {
      LOG_WARN("failed to get first role", KR(ret), K(ls_id), KPC(ls));
    } else if (!is_strong_leader(role)) {
      // nothing todo
    } else if (OB_FAIL(get_sync_point_(ls_id, sync_scn, readable_scn))) {
      LOG_WARN("failed to get sync point", KR(ret), KPC(ls));
    } else {
      if (OB_FAIL(ls_recovery_stat.init_only_recovery_stat(
              tenant_id, ls_id,
              sync_scn, readable_scn))) {
        LOG_WARN("failed to init ls recovery stat", KR(ret), "ls_meta", ls->get_ls_meta(),
                 K(sync_scn), K(readable_scn));
      } else if (OB_FAIL(ls_svr->get_palf_role(ls_id, role, second_proposal_id))) {
        LOG_WARN("failed to get parl role again", KR(ret), K(role), K(ls_id));
      } else if (first_proposal_id != second_proposal_id ||
                 !is_strong_leader(role)) {
        // nothing
        ret = OB_EAGAIN;
        LOG_INFO("role change, try again", KR(ret), K(role),
                 K(first_proposal_id), K(second_proposal_id), KPC(ls));
      } else if (OB_FAIL(ls_recovery.update_ls_recovery_stat(ls_recovery_stat,
                                                             *sql_proxy))) {
        LOG_WARN("failed to update ls recovery stat", KR(ret),
                 K(ls_recovery_stat));
      }
    }
    const int64_t PRINT_INTERVAL = 10 * 1000 * 1000L;
    if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
      LOG_INFO("tenant update ls recovery stat", KR(ret), K(role),
               K(first_proposal_id), K(second_proposal_id),
               K(ls_recovery_stat));
    }

  }
  return ret;

}


int ObTenantRecoveryReportor::load_tenant_info_()
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_,
          sql_proxy_, false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
  } else {
    /**
    * Only need to refer to tenant role, no need to refer to switchover status.
    * tenant_role is primary only in <primary, normal switchoverstatus>.
    * When switch to standby starts, it will change to <standby, prepare switch to standby>.
    * During the master switch process, some LS may be in RO state.
    * This also ensures the consistency of tenant_role cache and the tenant role field in all_tenant_info
    */
    MTL_SET_TENANT_ROLE(tenant_info.get_tenant_role().value());
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(tenant_info_.assign(tenant_info))) {
      LOG_WARN("failed to assign tenant info", KR(ret), K(tenant_info));
    }
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_INFO("update tenant info", KR(ret), K(tenant_info_));
    }
  }
  return ret;
}

int ObTenantRecoveryReportor::get_tenant_info(share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (!tenant_info_.is_valid()) {
      ret = OB_NEED_WAIT;
      //before meta tenant create success or restart
      const int64_t PRINT_INTERVAL = 1 * 1000 * 1000L;
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
        LOG_WARN("tenant info is invalid, need wait", KR(ret));
      }
    } else if (OB_FAIL(tenant_info.assign(tenant_info_))) {
      LOG_WARN("failed to assign tenant info", KR(ret), K(tenant_info_));
    }
  }
  return ret;
}


int ObTenantRecoveryReportor::update_replayable_point_()
{
  int ret = OB_SUCCESS;
  const int64_t PRINT_INTERVAL = 10 * 1000 * 1000L;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(update_replayable_point_from_tenant_info_())) {
    //重启场景下从meta读取回放受控点,更新到replayservice
    if (OB_FAIL(update_replayable_point_from_meta_())) {
      LOG_WARN("update_replayable_point_from_meta_ failed", KR(ret));
    } else {
      LOG_INFO("update_replayable_point_from_meta_ success", KR(ret));
    }
  } else if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
    SpinRLockGuard guard(lock_); //for K(tenant_info_)
    LOG_INFO("update_replayable_point_from_tenant_info_ success", KR(ret), K(tenant_info_));
  }
  return ret;
}

int ObTenantRecoveryReportor::update_replayable_point_from_tenant_info_()
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
  SpinRLockGuard guard(lock_);
  if (!tenant_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_info invalid", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(log_service->update_replayable_point(tenant_info_.get_replayable_scn()))) {
    LOG_WARN("logservice update_replayable_point failed", KR(ret), K(tenant_info_));
  } else {
    // do nothing
  }
  return ret;
}

int ObTenantRecoveryReportor::update_replayable_point_from_meta_()
{
  int ret = OB_SUCCESS;
  int64_t replayable_point = OB_INVALID_TIMESTAMP;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService *);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObLSService should not be null", KR(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard,
          storage::ObLSGetMod::RS_MOD))) {
    LOG_WARN("get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    LOG_WARN("iter is NULL", KR(ret));
  } else {
    ObLS *ls = nullptr;
    int64_t max_replayable_point = OB_INVALID_TIMESTAMP;
    while (OB_SUCC(iter->get_next(ls))) {
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is null", KR(ret), KP(ls));
      } else if (OB_FAIL(ls->get_ls_replayable_point(replayable_point))) {
        LOG_WARN("failed to update_ls_replayable_point", KR(ret), KPC(ls), K(replayable_point));
      } else if (max_replayable_point < replayable_point) {
        max_replayable_point = replayable_point;
      }
    }
    if (OB_SUCC(ret)) {
      logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
      if (OB_FAIL(log_service->update_replayable_point(replayable_point))) {
        LOG_WARN("logservice update_replayable_point failed", KR(ret), K(replayable_point));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObTenantRecoveryReportor::get_sync_point_(const share::ObLSID &id,
    int64_t &sync_scn, int64_t &read_scn)
{
  int ret = OB_SUCCESS;
  palf::AccessMode access_mode;
  int64_t unused_mode_version;
  palf::PalfHandleGuard palf_handle_guard;
  if (OB_FAIL(MTL(logservice::ObLogService*)->open_palf(id, palf_handle_guard))) {
    LOG_WARN("failed to open palf", KR(ret), K(id));
  } else if (OB_FAIL(palf_handle_guard.get_end_ts_ns(sync_scn))) {
    LOG_WARN("failed to get end ts", KR(ret), K(id));
  } else if (OB_FAIL(palf_handle_guard.get_access_mode(unused_mode_version, access_mode))) {
    LOG_WARN("failed to get access_mode", KR(ret), K(id));
  } else if (palf::AccessMode::APPEND == access_mode) {
  } else {
    storage::ObLSHandle ls_handle;
    ObLSRestoreStatus restore_status;
    storage::ObLS *ls = NULL;
    logservice::ObLogRestoreHandler *restore_handler = NULL;
    if (OB_FAIL(MTL(storage::ObLSService*)->get_ls(id, ls_handle,
            storage::ObLSGetMod::LOG_MOD))) {
      LOG_WARN("failed to get ls", KR(ret), K(id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls is NULL", K(ret), K(id), K(ls));
    } else if (OB_FAIL(ls->get_restore_status(restore_status))) {
      LOG_WARN("failed to get restore status", KR(ret), K(id));
    } else if (! restore_status.is_in_restore()) {
    } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("restore_handler is NULL", K(ret), K(id), K(restore_handler));
    } else if (OB_FAIL(restore_handler->get_restore_sync_ts(id, sync_scn))) {
      LOG_WARN("get restore sync point failed", KR(ret), K(id));
    }
  }
  if (FAILEDx(get_readable_scn(id, read_scn))) {
    LOG_WARN("failed to get readable scn", KR(ret), K(id));
  }

  return ret;
}


int ObTenantRecoveryReportor::get_readable_scn(const share::ObLSID &id, int64_t &readable_scn)
{
  int ret = OB_SUCCESS;
  storage::ObLSHandle ls_handle;
  storage::ObLS *ls = NULL;
  ObLSVTInfo ls_info;
  if (OB_FAIL(MTL(storage::ObLSService*)->get_ls(id, ls_handle,
          storage::ObLSGetMod::LOG_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls is NULL", K(ret), K(id), K(ls));
  } else if (OB_FAIL(ls->get_ls_info(ls_info))) {
    LOG_WARN("failed to get ls info", KR(ret));
  } else {
    readable_scn = ls_info.weak_read_timestamp_ < OB_LS_MIN_SCN_VALUE ? 
                   OB_LS_MIN_SCN_VALUE : ls_info.weak_read_timestamp_; 
  }
  return ret;

}
}
}
