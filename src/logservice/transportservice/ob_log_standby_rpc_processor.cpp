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

namespace oceanbase
{
namespace logservice
{

int ObLogStandbyTransportP::process()
{
  int ret = OB_SUCCESS;
  const ObLogTransportReq &req = arg_;
  const common::ObAddr server = req.src_;
  ObLogSyncStandbyInfo &resp = result_;

  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t tenant_id = MTL_ID();

  CLOG_LOG(TRACE, "ObLogStandbyTransportP process start", K(req), K(cluster_id), K(tenant_id),
           K(req.start_lsn_), K(req.end_lsn_), K(req.log_size_));

  if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid request", K(ret), K(req));
  } else if (req.standby_cluster_id_ != GCONF.cluster_id){
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "standby cluster id not match", K(req.standby_cluster_id_), K(cluster_id), K(ret));
  } else if (req.standby_tenant_id_ != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "standby tenant id not match", K(req.standby_tenant_id_), K(tenant_id), K(ret));
  } else if (OB_UNLIKELY(nullptr != filter_ && true == (*filter_)(server))) {
    CLOG_LOG(INFO, "need filter this packet", K(req));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(req.end_lsn_ != req.start_lsn_ + req.log_size_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "log size mismatch with lsn range", K(ret), K(req.start_lsn_), K(req.end_lsn_), K(req.log_size_));
  } else { } // success

  if (OB_SUCCESS != ret) {
    resp.ret_code_ = ret;
    resp.refresh_info_ret_code_ = ret;
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
        // 任务已提交到队列，worker会异步处理
        // 写入完成后，PALF 会触发 ObStandbyFsCb::update_end_lsn 回调
        // 回调会提交 StandbyAckTask 到 ack_service_ 队列
        // ack_service_ 的独立线程会异步发送 ACK 给主库
        // 因此这里直接返回成功，不需要等待处理完成
        CLOG_LOG(INFO, "ObLogStandbyTransportP submit task success, ack will be sent asynchronously",
                 K(ret), K(req), K(resp));
      }
    }

    // 提交任务流程结束，设置 ret_code_
    resp.ret_code_ = ret;

    // 最后获取 palf 的 committed_end_lsn 和 committed_end_scn，确保获取到最新的值
    // 无论是否 ret != OB_SUCCESS，都更新 resp 中的 end_lsn/end_scn
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
      CLOG_LOG(WARN, "open_palf failed, use req value", K(ret), K(req.ls_id_));
    } else if (OB_FAIL(ObLogStandbyAckService::check_leader_and_raw_write_mode(
        palf_handle_guard.get_palf_handle(), first_proposal_id, is_valid))) {
      CLOG_LOG(WARN, "first check leader and access mode failed", K(ret), K(req.ls_id_));
    } else if (!is_valid) {
      // 不是 leader 或不是 RAW_WRITE 模式，跳过返回
      skip_return_stale_ack = true;
      CLOG_LOG(INFO, "first check failed, skip returning ack to primary",
               K(req.ls_id_), K(first_proposal_id));
    } else if (OB_FAIL(ObLogStandbyAckService::check_restore_source_valid(req.ls_id_, is_valid))) {
      CLOG_LOG(WARN, "check restore source valid failed", K(ret), K(req.ls_id_));
    } else if (!is_valid) {
      // log_restore_source 为空或无效，跳过返回
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

        // 第二次检查：确认仍然是 leader 且 access mode 是 RAW_WRITE，且 proposal_id 不变
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
    resp.refresh_info_ret_code_ = ret;
    if (!skip_return_stale_ack) {
      resp.standby_committed_end_lsn_ = palf_committed_end_lsn;
      resp.standby_committed_end_scn_ = palf_committed_end_scn;
    }
  }

  // 设置 resp 基本字段
  resp.ls_id_ = req.ls_id_;
  resp.standby_cluster_id_ = GCONF.cluster_id;
  resp.standby_tenant_id_ = MTL_ID();
  return OB_SUCCESS;
}

} // namespace logservice
} // namespace oceanbase