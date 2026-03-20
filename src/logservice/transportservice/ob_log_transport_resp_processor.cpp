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

#include "ob_log_transport_resp_processor.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_define.h"
#include "logservice/ob_log_service.h"
#include "logservice/transportservice/ob_log_transport_service.h"

namespace oceanbase
{
namespace logservice
{

ObLogTransportRespP::~ObLogTransportRespP()
{
}

int ObLogTransportRespP::process()
{
  int ret = OB_SUCCESS;
  const ObLogSyncStandbyInfo &ack = arg_;
  ObLogTransportReq &result = result_;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t tenant_id = MTL_ID();

  CLOG_LOG(INFO, "ObLogTransportRespP process start", K(ack), K(cluster_id), K(tenant_id));

  if (OB_UNLIKELY(!ack.ls_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid response, ls_id is invalid", K(ret), K(ack));
  } else {
    // 获取 transport service
    ObLogService *log_service = MTL(ObLogService*);
    ObLogTransportService *transport_service = nullptr;

    if (OB_ISNULL(log_service)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "ObLogService is null", K(ret));
    } else if (OB_ISNULL(transport_service = log_service->get_log_transport_service())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "ObLogTransportService is null", K(ret));
    } else {
      // 检查收到的 ack 是否是当前强同步备库回复的 ack
      ObSyncStandbyDestStruct sync_standby_dest;
      if (OB_FAIL(transport_service->get_sync_standby_dest(sync_standby_dest))) {
        CLOG_LOG(WARN, "failed to get sync_standby_dest", K(ret), K(ack));
      } else if (!sync_standby_dest.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "sync_standby_dest is invalid, cannot verify ack", K(ret), K(sync_standby_dest), K(ack));
      } else {
        uint64_t expected_standby_tenant_id = sync_standby_dest.restore_source_service_attr_.user_.tenant_id_;
        int64_t expected_standby_cluster_id = sync_standby_dest.restore_source_service_attr_.user_.cluster_id_;
        if (ack.standby_cluster_id_ != expected_standby_cluster_id) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "ack standby cluster id not match with sync standby dest",
                   K(ack.standby_cluster_id_), K(expected_standby_cluster_id), K(ret));
        } else if (ack.standby_tenant_id_ != expected_standby_tenant_id) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "ack standby tenant id not match with sync standby dest",
                   K(ack.standby_tenant_id_), K(expected_standby_tenant_id), K(ret));
        } else if (OB_SUCCESS != ack.ret_code_) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "ack ret code not success", K(ack), K(ret));
        } else if (!ack.standby_committed_end_lsn_.is_valid() || !ack.standby_committed_end_scn_.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "ack standby committed end lsn or scn is invalid", K(ret), K(ack));
        } else {
          // 获取 transport_status 并更新 standby_committed_end_lsn
          ObTpStatusGuard guard;
          LogTransportStatus *transport_status = nullptr;
          if (OB_FAIL(transport_service->get_transport_status(ack.ls_id_, guard))) {
            CLOG_LOG(WARN, "get_transport_status failed", K(ret), K(ack.ls_id_));
          } else if (OB_ISNULL(transport_status = guard.get_transport_status())) {
            ret = OB_ERR_UNEXPECTED;
            CLOG_LOG(WARN, "transport_status is null", K(ret), K(ack.ls_id_));
          } else if (ack.standby_committed_end_lsn_.is_valid() && ack.standby_committed_end_scn_.is_valid()) {
            // 更新备库的 committed_end_lsn 和 committed_end_scn
            int update_ret = transport_status->update_standby_committed_end_lsn(ack.standby_committed_end_lsn_,
                                                                                   ack.standby_committed_end_scn_);
            if (OB_FAIL(update_ret)) {
              if (OB_STATE_NOT_MATCH == update_ret) {
                CLOG_LOG(TRACE, "standby committed_end_lsn rollback, skip update and notify",
                         K(ack.ls_id_), K(ack.standby_committed_end_lsn_), K(ack.standby_committed_end_scn_));
                ret = OB_SUCCESS;
              } else if (OB_NO_NEED_UPDATE == update_ret) {
                CLOG_LOG(TRACE, "standby committed_end_lsn not changed, skip notify",
                         K(ack.ls_id_), K(ack.standby_committed_end_lsn_), K(ack.standby_committed_end_scn_));
                ret = OB_SUCCESS;
              } else {
                CLOG_LOG(WARN, "update_standby_committed_end_lsn failed", K(update_ret),
                         K(ack.ls_id_), K(ack.standby_committed_end_lsn_), K(ack.standby_committed_end_scn_));
                ret = update_ret;
              }
            } else {
              // 更新成功，update_standby_committed_end_lsn 内部已经会通知 apply_service
              CLOG_LOG(INFO, "ObLogTransportRespP updated standby_committed_end_lsn",
                       K(ack.ls_id_), K(ack.standby_committed_end_lsn_), K(ack.standby_committed_end_scn_));
              // RPC 处理成功
              ret = OB_SUCCESS;
            }
          } else {
            CLOG_LOG(TRACE, "standby_committed_end_lsn or scn is invalid, skip update",
                     K(ack.ls_id_), K(ack.standby_committed_end_lsn_), K(ack.standby_committed_end_scn_));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    result.ls_id_ = ack.ls_id_;
    CLOG_LOG(WARN, "ObLogTransportRespP process failed", K(ret), K(ack));
  } else {
    result.ls_id_ = ack.ls_id_;
    CLOG_LOG(INFO, "ObLogTransportRespP process success", K(ret), K(ack));
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase
