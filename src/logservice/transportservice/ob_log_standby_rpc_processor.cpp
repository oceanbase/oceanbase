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

#define USING_LOG_PREFIX CLOG

#include "ob_log_standby_rpc_processor.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_role.h"
#include "share/ob_define.h"
#include "share/ob_thread_mgr.h"
#include "logservice/ob_log_service.h"
#include "logservice/restoreservice/ob_log_restore_service.h"
#include "logservice/logrpc/ob_log_rpc_req.h"
#include "logservice/palf/palf_handle.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/palf/palf_options.h"
#include "logservice/ob_log_handler.h"
#include "logservice/palf/log_sync_mode_mgr.h"
#include "logservice/transportservice/ob_log_standby_ack_service.h"
#include "logservice/transportservice/ob_log_standby_transport_worker.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/rc/ob_tenant_base.h"
#include "rootserver/ob_tenant_info_loader.h" // ObTenantInfoLoader
#include "share/ob_tenant_info_proxy.h"       // ObAllTenantInfo
#include "share/ob_tenant_role.h"             // ObProtectionMode/ObProtectionLevel

namespace oceanbase
{
namespace logservice
{

// ERRSIM: force the level-check term to fail even when level allows early ACK,
// to exercise the flush-ACK fallback (simulates main/standby level view skew).
ERRSIM_POINT_DEF(EN_SEMI_SYNC_LEVEL_SKEW);
// ERRSIM: drop the early-ACK position (simulate early-ACK RPC loss); flush ACK must recover.
ERRSIM_POINT_DEF(EN_SEMI_SYNC_RPC_LOST);
// ERRSIM: delay only after the semi-sync early-ACK gate is hit. Used to verify
// that paths with semi-sync disabled do not enter the early-ACK branch.
ERRSIM_POINT_DEF(ERRSIM_DELAY_SEMI_SYNC_EARLY_ACK);

// Semi-sync early ACK gate: double-AND check.
//   cfg_on = TENANT_CONF(enable_standby_semi_sync), read per RPC
//   tenant_info mode ∈ {MPT, MA} && level = PRE_MPT means semi-sync
//   protection target is active on this server.
// Both must hold; any failure to read either term => no early ACK (flush ACK fallback).
static bool check_semi_sync_can_early_ack_(const share::ObLSID &ls_id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool can_early_ack = false;
  bool cfg_on = false;
  bool protection_stat_allows_early_ack = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  cfg_on = tenant_config.is_valid() && tenant_config->enable_standby_semi_sync;

  if (cfg_on) {
    rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
    share::ObAllTenantInfo tenant_info;
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(tenant_info_loader)) {
      CLOG_LOG(WARN, "ObTenantInfoLoader is null, skip early ACK", K(ls_id), K(tenant_id));
    } else if (OB_TMP_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
      CLOG_LOG(WARN, "get_tenant_info failed, skip early ACK", K(tmp_ret), K(ls_id), K(tenant_id));
    } else {
      const share::ObProtectionMode mode = tenant_info.get_protection_mode();
      const share::ObProtectionLevel level = tenant_info.get_protection_level();
      protection_stat_allows_early_ack =
          mode.is_valid() && mode.is_sync_mode()
          && level.is_valid() && level.is_pre_maximum_protection();
    }
  }
  if (OB_UNLIKELY(EN_SEMI_SYNC_LEVEL_SKEW)) {
    protection_stat_allows_early_ack = false;
    CLOG_LOG(INFO, "EN_SEMI_SYNC_LEVEL_SKEW: force flush ACK fallback", K(ls_id), K(tenant_id));
  }
  can_early_ack = cfg_on && protection_stat_allows_early_ack;
  return can_early_ack;
}

static void try_fill_semi_sync_early_ack_(
    const share::ObLSID &ls_id,
    const uint64_t tenant_id,
    ObLogStandbyTransportWorker *transport_worker,
    ObLogSyncStandbyInfo &resp)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(transport_worker)) {
    CLOG_LOG(WARN, "transport worker is null, skip early ACK", K(ls_id), K(tenant_id));
  } else if (check_semi_sync_can_early_ack_(ls_id, tenant_id)) {
    // Early ACK: immediately respond with queued_end_lsn_/scn_ from the task queue.
    // No wait for PALF flush — this is the semi-sync contract.
    palf::LSN queued_end_lsn;
    share::SCN queued_end_scn;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(transport_worker->get_queued_end_position(ls_id, queued_end_lsn, queued_end_scn))) {
      CLOG_LOG(WARN, "get_queued_end_position failed, skip early ACK", K(tmp_ret), K(ls_id), K(tenant_id));
    } else if (OB_UNLIKELY(ERRSIM_DELAY_SEMI_SYNC_EARLY_ACK > 0)) {
      const int64_t delay_s = abs(ERRSIM_DELAY_SEMI_SYNC_EARLY_ACK);
      CLOG_LOG(INFO, "ERRSIM_DELAY_SEMI_SYNC_EARLY_ACK enabled, sleep",
               K(delay_s), K(ls_id), K(tenant_id));
      ob_usleep(static_cast<uint32_t>(delay_s * 1000 * 1000));
    } else if (OB_UNLIKELY(EN_SEMI_SYNC_RPC_LOST)) {
      CLOG_LOG(INFO, "EN_SEMI_SYNC_RPC_LOST: drop early ACK position", K(ls_id), K(tenant_id));
    } else {
      resp.standby_committed_end_lsn_ = queued_end_lsn;
      resp.standby_committed_end_scn_ = queued_end_scn;
      CLOG_LOG(TRACE, "semi-sync early ACK sent", K(ls_id), K(tenant_id), K(resp),
               K(queued_end_lsn), K(queued_end_scn));
    }
  }
}

int ObLogStandbyTransportP::process()
{
  int ret = OB_SUCCESS;
  const ObLogTransportReq &req = arg_;
  const common::ObAddr server = req.src_;
  ObLogSyncStandbyInfo &resp = result_;

  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t tenant_id = MTL_ID();

  CLOG_LOG(TRACE, "ObLogStandbyTransportP process start", K(req),
      K(cluster_id), K(tenant_id), K(req.start_lsn_), K(req.end_lsn_), K(req.log_size_));

  if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid request", K(ret), K(req));
  } else if (req.standby_cluster_id_ != GCONF.cluster_id){
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "standby cluster id not match", K(req.standby_cluster_id_), K(cluster_id), K(ret));
  } else if (req.standby_tenant_id_ != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "standby tenant id not match", K(req.standby_tenant_id_), K(tenant_id), K(ret));
  } else if (OB_UNLIKELY(req.end_lsn_ != req.start_lsn_ + req.log_size_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "log size mismatch with lsn range", K(ret), K(req.start_lsn_), K(req.end_lsn_), K(req.log_size_));
  } else { } // success

  if (OB_FAIL(ret)) {
    resp.ret_code_ = ret;
    resp.standby_committed_end_lsn_.reset();
    resp.standby_committed_end_scn_.reset();
  } else {
    ObLogService *log_service = MTL(ObLogService*);
    ObLogRestoreService *restore_service = nullptr;
    ObLogStandbyTransportWorker *transport_worker = nullptr;

    if (OB_ISNULL(log_service)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "ObLogService is null", K(ret));
    } else if (OB_ISNULL(restore_service = log_service->get_log_restore_service())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "ObLogRestoreService is null", K(ret));
    } else if (OB_ISNULL(transport_worker = restore_service->get_log_standby_transport_worker())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "transport worker is null", K(ret), K(req.ls_id_));
    } else if (transport_worker->need_stop()) {
      ret = OB_IN_STOP_STATE;
      CLOG_LOG(WARN, "transport worker is stopping, reject new task", K(ret), K(req.ls_id_));
    } else {
      // 提交任务到worker队列，立即返回成功
      // 确认位点会通过 standbyAckService 异步返回给主库
      if (OB_FAIL(transport_worker->submit_transport_task(req))) {
        CLOG_LOG(WARN, "submit transport task failed", K(ret), K(req.ls_id_));
      } else {
        try_fill_semi_sync_early_ack_(req.ls_id_, tenant_id, transport_worker, resp);
        // Task submitted to queue; if not early ACK, flush ACK will be sent
        // asynchronously by ObStandbyFsCb → StandbyAckTask
        CLOG_LOG(TRACE, "ObLogStandbyTransportP submit task success, ack will be sent asynchronously",
                 K(ret), K(req), K(resp));
      }
    }

    // 提交任务流程结束，设置 ret_code_
    resp.ret_code_ = ret;

    // Semi-sync: if early ACK was sent, skip the palf flush ACK flow entirely.
    // Early ACK values were already set in the submit_transport_task success branch.
    const bool early_ack_sent = resp.standby_committed_end_lsn_.is_valid()
        && resp.standby_committed_end_scn_.is_valid();
    if (early_ack_sent) {
      // Early ACK already populated — skip palf flush ACK flow
    } else {
      // 最后获取 palf 的 committed_end_lsn 和 committed_end_scn，确保获取到最新的值
      palf::LSN palf_committed_end_lsn;
      share::SCN palf_committed_end_scn;
      palf::PalfHandleGuard palf_handle_guard;
      int64_t first_proposal_id = palf::INVALID_PROPOSAL_ID;
      int64_t second_proposal_id = palf::INVALID_PROPOSAL_ID;
      bool is_valid = false;
      bool skip_return_stale_ack = false;
      ret = OB_SUCCESS;

      // 第一次检查：确认是 leader 且 access mode 是 RAW_WRITE
      if (OB_ISNULL(log_service)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "ObLogService is null", K(ret));
      } else if (OB_FAIL(log_service->open_palf(req.ls_id_, palf_handle_guard))) {
        CLOG_LOG(WARN, "open_palf failed, skip returning committed ack", K(ret), K(req.ls_id_));
      } else if (OB_FAIL(ObLogStandbyAckService::check_leader_and_raw_write_mode(
          palf_handle_guard.get_palf_handle(), first_proposal_id, is_valid))) {
        CLOG_LOG(WARN, "first check leader and access mode failed", K(ret), K(req.ls_id_));
      } else if (!is_valid) {
        skip_return_stale_ack = true;
        CLOG_LOG(INFO, "first check failed, skip returning ack to primary",
                 K(req.ls_id_), K(first_proposal_id));
      } else if (OB_FAIL(ObLogStandbyAckService::check_restore_source_valid(req.ls_id_, is_valid))) {
        CLOG_LOG(WARN, "check restore source valid failed", K(ret), K(req.ls_id_));
      } else if (!is_valid) {
        skip_return_stale_ack = true;
        CLOG_LOG(INFO, "restore source not valid, skip returning ack to primary", K(req.ls_id_));
      } else {
        // 获取日志流的 end_lsn/end_scn
        if (OB_FAIL(palf_handle_guard.get_end_lsn(palf_committed_end_lsn))) {
          CLOG_LOG(WARN, "get_end_lsn failed", K(ret), K(req.ls_id_));
        } else if (OB_FAIL(palf_handle_guard.get_end_scn(palf_committed_end_scn))) {
          CLOG_LOG(WARN, "get_end_scn failed", K(ret), K(req.ls_id_));
        } else {
          // ERRSIM: 延迟返回，便于测试在延迟窗口内触发 failover
          ERRSIM_POINT_DEF(ERRSIM_DELAY_STANDBY_TRANSPORT_RESP);
          if (ERRSIM_DELAY_STANDBY_TRANSPORT_RESP > 0) {
            const int64_t delay_s = abs(ERRSIM_DELAY_STANDBY_TRANSPORT_RESP);
            CLOG_LOG(INFO, "ERRSIM_DELAY_STANDBY_TRANSPORT_RESP enabled, sleep", K(delay_s), K(req.ls_id_));
            ob_usleep(static_cast<uint32_t>(delay_s * 1000 * 1000));
          }

          // 第二次检查
          if (OB_FAIL(ObLogStandbyAckService::check_and_compare_leader_status(
              palf_handle_guard.get_palf_handle(), first_proposal_id, is_valid, second_proposal_id))) {
            CLOG_LOG(WARN, "second check leader and access mode failed", K(ret), K(req.ls_id_));
          } else if (!is_valid) {
            skip_return_stale_ack = true;
            CLOG_LOG(INFO, "second check failed, skip returning ack to primary",
                     K(req.ls_id_), K(first_proposal_id), K(second_proposal_id));
          }
        }
      }

      // 设置 committed 位点
      if (OB_SUCC(ret) && !skip_return_stale_ack && palf_committed_end_lsn.is_valid() && palf_committed_end_scn.is_valid()) {
        resp.standby_committed_end_lsn_ = palf_committed_end_lsn;
        resp.standby_committed_end_scn_ = palf_committed_end_scn;
      } else {
        resp.standby_committed_end_lsn_.reset();
        resp.standby_committed_end_scn_.reset();
      }
    }
  }

  // 设置 resp 基本字段
  resp.ls_id_ = req.ls_id_;
  resp.standby_cluster_id_ = cluster_id;
  resp.standby_tenant_id_ = tenant_id;
  return OB_SUCCESS;
}

} // namespace logservice
} // namespace oceanbase
