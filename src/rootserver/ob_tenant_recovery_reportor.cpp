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
#include "rootserver/ob_tenant_info_loader.h"
#include "rootserver/ob_tenant_role_transition_service.h"//ObTenantRoleTransitionConstants
#include "storage/tx_storage/ob_ls_service.h" //ObLSService
#include "storage/tx_storage/ob_ls_map.h"//ObLSIterator
#include "storage/ls/ob_ls.h"//ObLSGetMod
#include "observer/ob_server_struct.h"//GCTX
#include "lib/profile/ob_trace_id.h"
#include "lib/thread/threads.h"//set_run_wrapper
#include "share/ls/ob_ls_recovery_stat_operator.h" //ObLSRecoveryStatOperator
#include "share/ob_schema_status_proxy.h"//ObSchemaStatusProxy
#include "share/schema/ob_multi_version_schema_service.h"//is_tenant_full_schema
#include "logservice/ob_log_service.h"//get_palf_role
#include "share/scn.h"//SCN
#include "storage/tx_storage/ob_ls_handle.h"  //ObLSHandle

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace storage;
using namespace palf;
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
    const int64_t idle_time = ObTenantRoleTransitionConstants::TENANT_INFO_REFRESH_TIME_US;
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
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCCESS != (tmp_ret = submit_tenant_refresh_schema_task_())) {
        LOG_WARN("failed to submit_tenant_refresh_schema_task_", KR(tmp_ret));
      }

      //更新受控回放位点到replayservice
      if (OB_SUCCESS != (tmp_ret = update_replayable_point_())) {
        LOG_WARN("failed to update_replayable_point", KR(tmp_ret));
      }
      if (!stop_) {
        get_cond().wait_us(idle_time);
      }
    }//end while
  }
}

int ObTenantRecoveryReportor::submit_tenant_refresh_schema_task_()
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.ob_service_) || OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pointer is null", KR(ret), KP(GCTX.ob_service_), KP(GCTX.schema_service_), KP(sql_proxy_), KP(tenant_info_loader));
  } else if (OB_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
    LOG_WARN("fail to get tenant info", KR(ret), K_(tenant_id));
  } else if (tenant_info.is_standby() && tenant_info.is_normal_status()) {
    ObRefreshSchemaStatus schema_status;
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", KR(ret));
    } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(tenant_id_, schema_status))) {
      LOG_WARN("fail to get schema status", KR(ret), K(tenant_id_));
    } else if (common::OB_INVALID_TIMESTAMP == schema_status.snapshot_timestamp_) {
      int64_t version_in_inner_table = OB_INVALID_VERSION;
      int64_t local_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_refreshed_schema_version(
                        tenant_id_, local_schema_version))) {
        LOG_WARN("fail to get tenant refreshed schema version", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(GCTX.schema_service_->get_schema_version_in_inner_table(
                  *sql_proxy_, schema_status, version_in_inner_table))) {
        LOG_WARN("fail to get_schema_version_in_inner_table", KR(ret), K(schema_status));
      } else if (local_schema_version > version_in_inner_table) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("local_schema_version > version_in_inner_table", KR(ret), K_(tenant_id),
                  K(local_schema_version), K(version_in_inner_table));
      } else if (local_schema_version == version_in_inner_table) {
        // do nothing
      } else if (OB_FAIL(GCTX.ob_service_->submit_async_refresh_schema_task(tenant_id_, version_in_inner_table))) {
        LOG_WARN("failed to submit_async_refresh_schema_task", KR(ret), K_(tenant_id));
      }
    }
  }
  return ret;
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
    rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);

    if (OB_ISNULL(ls_svr) || OB_ISNULL(tenant_info_loader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mtl pointer is null", KR(ret), KP(ls_svr), KP(tenant_info_loader));
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
          ObAllTenantInfo tenant_info;
          if (OB_TMP_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
            LOG_WARN("failed to get_tenant_info", KR(ret), KPC(ls));
          } else if (OB_TMP_FAIL(ls->update_ls_replayable_point(tenant_info.get_replayable_scn()))) {
            LOG_WARN("failed to update_ls_replayable_point", KR(tmp_ret), KPC(ls), K(tenant_info));
          }

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
    SCN sync_scn;
    SCN readable_scn;
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
    LOG_TRACE("tenant update ls recovery stat", KR(ret), K(role),
              K(first_proposal_id), K(second_proposal_id),
              K(ls_recovery_stat));

  }
  return ret;

}

int ObTenantRecoveryReportor::get_tenant_readable_scn(SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  share::ObAllTenantInfo tenant_info;
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);

  if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl pointer is null", KR(ret), KP(tenant_info_loader));
  } else if (OB_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
    LOG_WARN("get_tenant_info failed", K(ret));
  } else if (OB_UNLIKELY(! tenant_info.is_valid())) {
    ret = OB_EAGAIN;
    LOG_WARN("tenant info not valid", K(ret), K(tenant_info));
  } else {
    readable_scn = tenant_info.get_standby_scn();
  }
  return ret;
}

int ObTenantRecoveryReportor::update_replayable_point_()
{
  int ret = OB_SUCCESS;
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
  }
  return ret;
}

int ObTenantRecoveryReportor::update_replayable_point_from_tenant_info_()
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
  const int64_t PRINT_INTERVAL = 10 * 1000 * 1000L;
  ObAllTenantInfo tenant_info;
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);

  if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl pointer is null", KR(ret), KP(tenant_info_loader));
  } else if (OB_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
    LOG_WARN("failed to get_tenant_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(log_service->update_replayable_point(tenant_info.get_replayable_scn()))) {
    LOG_WARN("logservice update_replayable_point failed", KR(ret), K(tenant_info));
  } else if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
    LOG_INFO("update_replayable_point_from_tenant_info_ success", KR(ret), K(tenant_info));
  }
  return ret;
}

int ObTenantRecoveryReportor::update_replayable_point_from_meta_()
{
  int ret = OB_SUCCESS;
  SCN replayable_point;
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
    SCN max_replayable_point;
    while (OB_SUCC(iter->get_next(ls))) {
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is null", KR(ret), KP(ls));
      } else if (OB_FAIL(ls->get_ls_replayable_point(replayable_point))) {
        LOG_WARN("failed to update_ls_replayable_point", KR(ret), KPC(ls), K(replayable_point));
      } else if (!max_replayable_point.is_valid() || max_replayable_point < replayable_point) {
        max_replayable_point = replayable_point;
      }
    }
    if (OB_ITER_END == ret) {
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
    SCN &sync_scn, SCN &read_scn)
{
  int ret = OB_SUCCESS;
  palf::AccessMode access_mode;
  int64_t unused_mode_version;
  palf::PalfHandleGuard palf_handle_guard;
  if (OB_FAIL(MTL(logservice::ObLogService*)->open_palf(id, palf_handle_guard))) {
    LOG_WARN("failed to open palf", KR(ret), K(id));
  } else if (OB_FAIL(palf_handle_guard.get_end_scn(sync_scn))) {
    LOG_WARN("failed to get end ts", KR(ret), K(id));
  } else if (OB_FAIL(get_readable_scn(id, read_scn))) {
    LOG_WARN("failed to get readable scn", KR(ret), K(id));
  }

  return ret;
}


int ObTenantRecoveryReportor::get_readable_scn(const share::ObLSID &id, SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  storage::ObLSHandle ls_handle;
  storage::ObLS *ls = NULL;
  ObLSVTInfo ls_info;
  readable_scn.set_min();
  if (OB_FAIL(MTL(storage::ObLSService*)->get_ls(id, ls_handle,
          storage::ObLSGetMod::LOG_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls is NULL", K(ret), K(id), K(ls_handle));
  } else if (OB_FAIL(ls->get_max_decided_scn(readable_scn))) {
    LOG_WARN("failed to get_max_decided_log_ts_ns", KR(ret), K(id), KPC(ls));
  } else {
    readable_scn = (readable_scn>= SCN::base_scn()) ? readable_scn : SCN::base_scn();
  }
  return ret;
}

}
}
