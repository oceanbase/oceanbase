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

#include "rootserver/ob_ls_recovery_reportor.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "rootserver/ob_tenant_role_transition_service.h"//ObTenantRoleTransitionConstants
#include "rootserver/ob_rs_async_rpc_proxy.h" //ObGetLSReplayedScnProxy
#include "rootserver/ob_ls_recovery_stat_handler.h" //ObLSRecoveryStatHandler
#include "rootserver/ob_ls_service_helper.h"//update_ls_stat_in_trans
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
int ObLSRecoveryReportor::mtl_init(ObLSRecoveryReportor *&ka)
{
  return ka->init();
}
int ObLSRecoveryReportor::init()
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
void ObLSRecoveryReportor::destroy()
{
  LOG_INFO("tenant recovery service destory", KPC(this));
  stop();
  wait();
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_proxy_ = NULL;
}

int ObLSRecoveryReportor::start()
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
    LOG_INFO("tenant recovery service start", KPC(this));
  }
  return ret;
}

void ObLSRecoveryReportor::stop()
{
  logical_stop();
}
void ObLSRecoveryReportor::wait()
{
  logical_wait();
}

void ObLSRecoveryReportor::wakeup()
{
   if (IS_NOT_INIT) {
     LOG_WARN_RET(OB_NOT_INIT, "not init no need wakeup");
   } else {
     ObThreadCondGuard guard(get_cond());
     get_cond().broadcast();
   }
 }

void ObLSRecoveryReportor::run2()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("tenant recovery service run", KPC(this));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObThreadCondGuard guard(get_cond());
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
    while (!stop_) {
      DEBUG_SYNC(STOP_LS_RECOVERY_THREAD);
      ObCurTraceId::init(GCONF.self_addr_);
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

      //更新受控回放位点到replayservice
      if (OB_SUCCESS != (tmp_ret = update_replayable_point_())) {
        LOG_WARN("failed to update_replayable_point", KR(tmp_ret));
      }

      if (!stop_) {
        (void) idle_some_time_();
      }
    }//end while
  }
}

void ObLSRecoveryReportor::idle_some_time_()
{
  int ret = OB_SUCCESS;
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
  const int64_t IDLE_TIME = ObTenantRoleTransitionConstants::STANDBY_UPDATE_LS_RECOVERY_STAT_TIME_US;
  int idle_count = 0;
  int idle_target_cnt = 10; // primary_normal: 10, others: 1
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pointer is null", KR(ret), KP(tenant_info_loader));
  }
  while (idle_count < idle_target_cnt && !stop_) {
    bool is_primary_normal_status = true;
    idle_count++;
    if (OB_FAIL(ret)) {
      idle_target_cnt = 1;
    } else if (OB_FAIL(tenant_info_loader->check_is_primary_normal_status(is_primary_normal_status))) {
      idle_target_cnt = 1;
      LOG_WARN("fail to get tenant status", KR(ret), K_(tenant_id));
    } else if (!is_primary_normal_status) {
      idle_target_cnt = 1;
    }
    get_cond().wait_us(IDLE_TIME);
  }
}

int ObLSRecoveryReportor::submit_tenant_refresh_schema_task_()
{
  int ret = OB_SUCCESS;
  bool is_standby_normal_status = false;
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.ob_service_) || OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(sql_proxy_) || OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pointer is null", KR(ret), KP(GCTX.ob_service_), KP(GCTX.schema_service_), KP(sql_proxy_), KP(tenant_info_loader));
  } else if (OB_FAIL(tenant_info_loader->check_is_standby_normal_status(is_standby_normal_status))) {
    LOG_WARN("fail to get tenant status", KR(ret), K_(tenant_id));
  } else if (is_standby_normal_status) {
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
int ObLSRecoveryReportor::update_ls_recovery_stat_()
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
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is NULL", KR(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      ObLS *ls = nullptr;
      while (OB_SUCC(iter->get_next(ls))) {// ignore failed of each ls
        if (OB_ISNULL(ls)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("ls is null", KR(ret), KP(ls));
        } else {
          share::SCN replayable_scn = SCN::base_scn();
          if (OB_TMP_FAIL(tenant_info_loader->get_local_replayable_scn(replayable_scn))) {
            LOG_WARN("failed to get replayable_scn", KR(ret), KPC(ls));
          } else if (OB_TMP_FAIL(ls->update_ls_replayable_point(replayable_scn))) {
            LOG_WARN("failed to update_ls_replayable_point", KR(tmp_ret), KPC(ls), K(replayable_scn));
          }
          if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) {
          } else if (OB_TMP_FAIL(ls->gather_replica_readable_scn())) {
            if (OB_NOT_MASTER == tmp_ret) {
              tmp_ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to gather replica readable scn", KR(tmp_ret));
            }
          }

          if (ls->is_sys_ls() && !MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
            // nothing todo
            // sys ls of user standby/restore tenant is in ls_recovery
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

int ObLSRecoveryReportor::update_ls_recovery(
    ObLS *ls,
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  ObLSRecoveryStat ls_recovery_stat;
  ObMySQLTransaction trans;
  const uint64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(tenant_id_);
  if (OB_ISNULL(ls) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls or sql proxy is null", KR(ret), KP(ls),  KP(sql_proxy));
  } else if (OB_FAIL(ls->get_ls_level_recovery_stat(ls_recovery_stat))) {
    if (OB_NOT_MASTER == ret) {
      LOG_TRACE("follower doesn't need to report ls recovery stat", KR(ret), KPC(ls));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get_ls_level_recovery_stat", KR(ret), KPC(ls));
    }
  } else if (OB_FAIL(trans.start(sql_proxy, exec_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id), K(ls_recovery_stat));
  } else if (ls->is_sys_ls()) {
    //only primary tenant
    if (OB_FAIL(update_sys_ls_recovery_stat_and_tenant_info(ls_recovery_stat, share::PRIMARY_TENANT_ROLE, false, trans))) {
      LOG_WARN("failed to update sys ls recovery stat", KR(ret), K(ls_recovery_stat));
    }
  } else if (OB_FAIL(ObLSServiceHelper::update_ls_recover_in_trans(ls_recovery_stat, false, trans))) {
    LOG_WARN("failed to update ls recovery stat in trans", KR(ret), K(ls_recovery_stat));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  if (ls_recovery_stat.is_valid()) {
    const int64_t PRINT_INTERVAL = 10 * 1000 * 1000L;
    if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
      LOG_INFO("tenant update ls recovery stat", KR(ret), K(ls_recovery_stat));
    }
    LOG_TRACE("tenant update ls recovery stat", KR(ret),
              K(ls_recovery_stat));
  }
  return ret;
}

int ObLSRecoveryReportor::update_sys_ls_recovery_stat_and_tenant_info(share::ObLSRecoveryStat &ls_recovery_stat,
      const share::ObTenantRole &tenant_role, const bool only_update_readable_scn, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_recovery_stat.is_valid() || !ls_recovery_stat.get_ls_id().is_sys_ls()
                  || !tenant_role.is_valid() || !trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_recovery_stat), K(tenant_role), "trans is start", trans.is_started());
  } else {
    ObAllTenantInfo tenant_info;
    const uint64_t tenant_id = ls_recovery_stat.get_tenant_id();
    if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, &trans, true, tenant_info))) {
      LOG_WARN("failed to load tenant info for update", KR(ret), K(tenant_id));
    } else if (tenant_info.get_tenant_role() != tenant_role
               || tenant_info.get_switchover_status().is_flashback_status()
               || (!only_update_readable_scn//If you only need to report readable_scn,
                   //there is no need to check the validity of sync_scn and recovery_until_scn
                   && ls_recovery_stat.get_sync_scn() > tenant_info.get_recovery_until_scn())) {
      //When reporting sync_scn, it is only guaranteed that if there are multi-source transactions,
      //it will wait for other LS to push through sync_scn,
      //and ensure that the sync_scn in tenant_info is equal to the sync_scn of SYS_LS.
      //In other cases, the sync_scn of SYS_LS may be greater than recovery_until_scn.
      ret = OB_NEED_RETRY;
      LOG_WARN("tenant status is not expected, do not update ls recovery", KR(ret),
          K(ls_recovery_stat), K(tenant_info), K(only_update_readable_scn));
    } else if (OB_FAIL(ObLSServiceHelper::update_ls_recover_in_trans(ls_recovery_stat, only_update_readable_scn, trans))) {
      LOG_WARN("failed to update ls recovery in trans", KR(ret), K(ls_recovery_stat), K(only_update_readable_scn));
    } else if (OB_FAIL(update_tenant_info_in_trans(tenant_info, trans))) {
      LOG_WARN("failed to update tenant info in trans", KR(ret), K(tenant_info));
    }
  }
  return ret;
}

int ObLSRecoveryReportor::update_tenant_info_in_trans(
    const share::ObAllTenantInfo &old_tenant_info,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!old_tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant info is invalid", KR(ret), K(old_tenant_info));
  } else {
    ObLSRecoveryStatOperator ls_recovery_op;
    ObAllTenantInfoProxy info_proxy;
    SCN sync_scn;
    SCN readable_scn;
    const uint64_t tenant_id = old_tenant_info.get_tenant_id();
    DEBUG_SYNC(BLOCK_TENANT_SYNC_SNAPSHOT_INC);
    if (OB_FAIL(ls_recovery_op.get_tenant_recovery_stat(
            tenant_id, trans, sync_scn, readable_scn))) {
      LOG_WARN("failed to get tenant recovery stat", KR(ret), K(tenant_id));
      //TODO replayable_scn is equal to sync_scn
    } else if (OB_FAIL(info_proxy.update_tenant_recovery_status_in_trans(
                   tenant_id, trans, old_tenant_info, sync_scn,
                   sync_scn, readable_scn))) {
      LOG_WARN("failed to update tenant recovery stat", KR(ret),
               K(tenant_id), K(sync_scn), K(readable_scn), K(old_tenant_info));
    }
  }
  return ret;
}

int ObLSRecoveryReportor::update_replayable_point_()
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

int ObLSRecoveryReportor::update_replayable_point_from_tenant_info_()
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
  const int64_t PRINT_INTERVAL = 10 * 1000 * 1000L;
  SCN replayable_scn = SCN::base_scn();
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);

  if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl pointer is null", KR(ret), KP(tenant_info_loader));
  } else if (OB_FAIL(tenant_info_loader->get_local_replayable_scn(replayable_scn))) {
    LOG_WARN("failed to get replayable_scn", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(log_service->update_replayable_point(replayable_scn))) {
    LOG_WARN("logservice update_replayable_point failed", KR(ret), K(replayable_scn));
  } else if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
    LOG_INFO("update_replayable_point_from_tenant_info_ success", KR(ret), K(replayable_scn));
  }
  return ret;
}

int ObLSRecoveryReportor::update_replayable_point_from_meta_()
{
  int ret = OB_SUCCESS;
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
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is NULL", KR(ret));
  } else {
    ObLS *ls = nullptr;
    SCN max_replayable_point;
    while (OB_SUCC(iter->get_next(ls))) {
      SCN replayable_point;
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
      if (OB_FAIL(log_service->update_replayable_point(max_replayable_point))) {
        LOG_WARN("logservice update_replayable_point failed", KR(ret), K(max_replayable_point));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

}
}
