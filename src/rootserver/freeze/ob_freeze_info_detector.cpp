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

#include "rootserver/freeze/ob_freeze_info_detector.h"

#include "rootserver/freeze/ob_freeze_info_manager.h"
#include "rootserver/ob_root_utils.h"
#include "lib/profile/ob_trace_id.h"
#include "share/config/ob_server_config.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_global_stat_proxy.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "rootserver/ob_thread_idling.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_zone_merge_info.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{
ObFreezeInfoDetector::ObFreezeInfoDetector()
  : ObFreezeReentrantThread(), is_inited_(false), is_primary_service_(true),
    is_global_merge_info_adjusted_(false), is_gc_scn_inited_(false),
    last_gc_timestamp_(0), freeze_info_mgr_(nullptr), major_scheduler_idling_(nullptr)
{}

int ObFreezeInfoDetector::init(
    const uint64_t tenant_id,
    const bool is_primary_service,
    ObMySQLProxy &sql_proxy,
    ObFreezeInfoManager &freeze_info_manager,
    ObThreadIdling &major_scheduler_idling)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_primary_service_ = is_primary_service;
    is_global_merge_info_adjusted_ = false;
    last_gc_timestamp_ = ObTimeUtility::current_time();
    sql_proxy_ = &sql_proxy;
    freeze_info_mgr_ = &freeze_info_manager;
    major_scheduler_idling_ = &major_scheduler_idling;
    is_inited_ = true;
    LOG_INFO("freeze info detector init succ", K_(tenant_id));
  }
  return ret;
}

int ObFreezeInfoDetector::start()
{
  int ret = OB_SUCCESS;
  lib::Threads::set_run_wrapper(MTL_CTX());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFreezeInfoDetector not init", K(ret));
  } else if (OB_FAIL(create(FREEZE_INFO_DETECTOR_THREAD_CNT, "FrzInfoDet"))) {
    LOG_WARN("fail to create thread", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObRsReentrantThread::start())) {
    LOG_WARN("fail to start thread", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("ObFreezeInfoDetector start succ", K_(tenant_id));
  }
  return ret;
}

void ObFreezeInfoDetector::run3()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    const int64_t start_time_us = ObTimeUtil::current_time();
    LOG_INFO("start freeze_info_detector", K_(tenant_id));
    ObThreadCondGuard guard(get_cond());
    while (!stop_) {
      update_last_run_timestamp();
      ObCurTraceId::init(GCONF.self_addr_);
      LOG_TRACE("run freeze info detector", K_(tenant_id));

      bool can_work = false;
      int64_t proposal_id = 0;
      ObRole role = ObRole::INVALID_ROLE;

      if (OB_FAIL(obtain_proposal_id_from_ls(is_primary_service_, proposal_id, role))) {
        LOG_WARN("fail to obtain proposal_id from ls", KR(ret));
      } else if (ObRole::LEADER != role) {
        LOG_INFO("follower should not run freeze_info_detector", K_(tenant_id), K(role),
                 K_(is_primary_service));
      } else if (OB_FAIL(can_start_work(can_work))) {
        LOG_WARN("fail to judge can start work", KR(ret), K_(tenant_id));
      } else if (can_work) {
        // In freeze_info_mgr, we use 'select snapshot_gc_scn for update' to execute sequentially,
        // avoiding multi-writing when switch-role.
        if (is_primary_service()) {  // only primary tenant need to renew_snapshot_gc_scn
          if (OB_FAIL(try_renew_snapshot_gc_scn())) {
            LOG_WARN("fail to renew gc snapshot", KR(ret), K_(tenant_id), K_(is_primary_service));
          }
        }

        // actively reload freeze_info in ObRestoreMajorFreezeService
        ret = OB_SUCCESS; // ignore ret
        if (OB_FAIL(try_reload_freeze_info(proposal_id))) {
          LOG_WARN("fail to try reload freeze info", KR(ret), K_(tenant_id), K_(is_primary_service),
                   K(proposal_id));
        }

        bool need_broadcast = false;
        ret = OB_SUCCESS; // ignore ret
        if (OB_FAIL(check_need_broadcast(need_broadcast, proposal_id))) {
          LOG_WARN("fail to check need broadcast", KR(ret), K_(tenant_id), K(proposal_id));
        }

        if (need_broadcast) {
          ret = OB_SUCCESS;
          if (OB_FAIL(try_minor_freeze())) {
            LOG_WARN("fail to try minor freeze", KR(ret), K_(tenant_id));
          }

          ret = OB_SUCCESS;
          if (OB_FAIL(try_broadcast_freeze_info(proposal_id))) {
            LOG_WARN("fail to broadcast freeze info", KR(ret), K_(tenant_id), K(proposal_id));
          }
        }

        ret = OB_SUCCESS;
        // only primary tenant need to check_snapshot_gc_scn.
        if (is_primary_service() && need_check_snapshot_gc_scn(start_time_us)) {
          if (OB_FAIL(freeze_info_mgr_->check_snapshot_gc_scn())) {
            LOG_WARN("fail to check_snapshot_gc_ts", KR(ret), K_(tenant_id));
          }
        }

        ret = OB_SUCCESS;
        if (OB_FAIL(try_update_zone_info(proposal_id))) {
          LOG_WARN("fail to try update zone info", KR(ret), K_(tenant_id), K(proposal_id));
        }
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(try_idle(get_schedule_interval(), ret))) {
        LOG_WARN("fail to try_idle", KR(ret), KR(tmp_ret));
      }
    }
  }
  LOG_INFO("stop freeze_info_detector", K_(tenant_id));
}

int ObFreezeInfoDetector::check_need_broadcast(bool &need_broadcast, const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(try_adjust_global_merge_info(expected_epoch))) {
    LOG_WARN("fail to try adjust global merge info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_->check_need_broadcast(need_broadcast))) {
    LOG_WARN("fail to check need broadcast", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObFreezeInfoDetector::try_broadcast_freeze_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_->broadcast_freeze_info(expected_epoch))) {
    LOG_WARN("fail to broadcast_frozen_info", KR(ret), K_(tenant_id), K(expected_epoch));
  } else {
    major_scheduler_idling_->wakeup();
  }
  return ret;
}

int ObFreezeInfoDetector::try_renew_snapshot_gc_scn()
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if ((now - last_gc_timestamp_) < MODIFY_GC_SNAPSHOT_INTERVAL) {
    // nothing
  } else if (OB_FAIL(freeze_info_mgr_->renew_snapshot_gc_scn())) {
    LOG_WARN("fail to renew snapshot gc scn", KR(ret), K_(tenant_id));
  } else {
    last_gc_timestamp_ = now;
  }
  return ret;
}

int ObFreezeInfoDetector::try_minor_freeze()
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  obrpc::ObRootMinorFreezeArg arg;
  if (OB_FAIL(arg.tenant_ids_.push_back(tenant_id_))) {
    LOG_WARN("fail to push back tenant_id", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid global context", KR(ret));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get rootservice address failed", K(ret));
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).timeout(GCONF.rpc_timeout)
                     .root_minor_freeze(arg))) {
    LOG_WARN("fail to execute root_minor_freeze rpc", KR(ret), K(arg));
  } else {
    LOG_INFO("succ to execute root_minor_freeze rpc", KR(ret), K(arg));
  }
  return ret;
}

int ObFreezeInfoDetector::try_update_zone_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_->try_update_zone_info(expected_epoch))) {
    LOG_WARN("fail to try update zone info", KR(ret), K_(tenant_id), K(expected_epoch));
  }
  return ret;
}

int ObFreezeInfoDetector::can_start_work(bool &can_work)
{
  int ret = OB_SUCCESS;
  can_work = true;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is nullptr", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get simple tenant schema", KR(ret));

  // 1. only normal state tenant schema need refresh freeze_info;
  // 2. common tenant(except sys tenant) init snapshot_gc_ts complete(in set_tenant_init_global_stat), 
  //    when tenant schema is noraml state, so can start work directly;
  } else if ((nullptr == tenant_schema) || !tenant_schema->is_normal()) {
    LOG_INFO("tenant is not normal status, no need detect now", K_(tenant_id), KPC(tenant_schema));
    can_work = false;
  } else if (is_sys_tenant(tenant_id_)) {
    // 3. sys tenant init global stat(snpshot_gc_ts) in ObBootstrap(ObBootstrap::init_global_stat()), 
    //    after tenant_state set to normal; 
    //    in order to avoid racing, detector will wait, until global_stat init complete;
    if (is_gc_scn_inited_) {
      // ...
    } else {
      SCN snapshot_gc_scn;
      ObGlobalStatProxy global_stat_proxy(*sql_proxy_, tenant_id_);
      if (OB_FAIL(global_stat_proxy.get_snapshot_gc_scn(snapshot_gc_scn))) {
        LOG_WARN("can not get snapshot gc ts", KR(ret), K_(tenant_id));
        ret = OB_SUCCESS;
        can_work = false;
      } else {
        LOG_INFO("snapshot_gc_scn init succ", K(snapshot_gc_scn), K_(tenant_id));
        is_gc_scn_inited_ = true;
      }
    }
  }
  return ret;
}

int64_t ObFreezeInfoDetector::get_schedule_interval() const
{
  return UPDATER_INTERVAL_US;
}

int ObFreezeInfoDetector::signal()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_cond().signal())) {
    LOG_WARN("fail to signal", KR(ret));
  }
  return ret;
}

int ObFreezeInfoDetector::check_tenant_is_restore(
    const uint64_t tenant_id,
    bool &is_restore)
{
  int ret = OB_SUCCESS;
  is_restore = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().check_tenant_is_restore(
                     NULL, tenant_id, is_restore))) {
    LOG_WARN("fail to check tenant restore", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObFreezeInfoDetector::try_reload_freeze_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  bool is_match = true;
  if (!is_primary_service()) {
    bool is_restore = false;
    if (OB_FAIL(check_tenant_is_restore(tenant_id_, is_restore))) {
      LOG_WARN("fail to check tenant is restore", KR(ret), K_(tenant_id), K_(is_primary_service));
    } else if (is_restore) {
      LOG_INFO("skip restoring tenant to reload freeze_info", K_(tenant_id), K(is_restore),
               K_(is_primary_service));
    } else if (OB_FAIL(ObServiceEpochProxy::check_service_epoch(*sql_proxy_, tenant_id_,
                ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, expected_epoch, is_match))) {
      LOG_WARN("fail to check freeze service epoch", KR(ret), K_(tenant_id), K_(is_primary_service));
    } else if (!is_match) {
      ret = OB_FREEZE_SERVICE_EPOCH_MISMATCH;
      LOG_WARN("cannot reload freeze_info now, cuz freeze_service_epoch mismatch", KR(ret),
               K_(tenant_id), K_(is_primary_service));
    } else if (OB_ISNULL(freeze_info_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to try reload freeze info, freeze info manager is null", KR(ret),
               K_(tenant_id), K_(is_primary_service));
    } else if (OB_FAIL(freeze_info_mgr_->reload())) {
      LOG_WARN("fail to reload freeze_info", KR(ret), K_(tenant_id), K_(is_primary_service));
    }
  }
  return ret;
}

int ObFreezeInfoDetector::try_adjust_global_merge_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  bool is_initial = false;
  // both primary and standby tenants should adjust global_merge_info to skip unnecessary major freeze
  // primary tenants:
  // standby tenants:
  if (!is_global_merge_info_adjusted_) {
    bool is_restore = false;
    if (OB_FAIL(check_tenant_is_restore(tenant_id_, is_restore))) {
      LOG_WARN("fail to check tenant is restore", KR(ret), K_(tenant_id), K_(is_primary_service));
    } else if (is_restore) {
      LOG_INFO("skip restoring tenant to adjust global merge info",
               K_(tenant_id), K(is_restore), K_(is_primary_service));
    } else if (OB_FAIL(check_global_merge_info(is_initial))) {
      LOG_WARN("fail to check global merge info", KR(ret), K_(tenant_id), K_(is_primary_service));
    } else if (!is_initial) {
      // avoid check again, e.g., when switch leader
      is_global_merge_info_adjusted_ = true;
    } else if (OB_ISNULL(freeze_info_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to try adjust global merge info, freeze info manager is null", KR(ret),
               K_(tenant_id), K_(is_primary_service));
    } else if (OB_FAIL(freeze_info_mgr_->adjust_global_merge_info(expected_epoch))) {
      LOG_WARN("fail to adjust global merge info", KR(ret), K_(tenant_id), K_(is_primary_service),
               K(expected_epoch));
    } else {
      is_global_merge_info_adjusted_ = true;
      LOG_INFO("succ to adjust global merge info", K_(tenant_id), K_(is_primary_service),
               K(expected_epoch));
    }
  }
  return ret;
}

int ObFreezeInfoDetector::check_global_merge_info(bool &is_initial) const
{
  int ret = OB_SUCCESS;
  is_initial = false;
  HEAP_VAR(ObGlobalMergeInfo, global_merge_info) {
    if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(*sql_proxy_,
                tenant_id_, global_merge_info))) {
      LOG_WARN("fail to get global merge info", KR(ret), K_(tenant_id), K_(is_primary_service));
    } else if ((global_merge_info.last_merged_scn_.get_scn().is_base_scn()) &&
               (global_merge_info.global_broadcast_scn_.get_scn().is_base_scn()) &&
               (global_merge_info.frozen_scn_.get_scn().is_base_scn())) {
      is_initial = true;
    }
  }
  return ret;
}

bool ObFreezeInfoDetector::need_check_snapshot_gc_scn(const int64_t start_time_us)
{
  const int64_t START_CHECK_INTERVAL_US = 10 * 60 * 1000 * 1000; // 10 min
  return (ObTimeUtility::current_time() - start_time_us) > START_CHECK_INTERVAL_US;
}

} //end rootserver
} //end oceanbase
