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

#include "ob_partition_log_packet_handler.h"
#include "storage/ob_partition_service.h"
#include "ob_clog_mgr.h"
#include "ob_i_log_engine.h"
#include "ob_log_req.h"
#include "ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
namespace clog {
template <typename T>
int deserialize(ObLogReqContext& ctx, T& t)
{
  int64_t pos = 0;
  return t.deserialize(ctx.data_, ctx.len_, pos);
}

int ObPartitionLogPacketHandler::init(storage::ObPartitionService* partition_service, ObICLogMgr* clog_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service) || OB_ISNULL(clog_mgr)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != partition_service_) {
    ret = OB_INIT_TWICE;
  } else {
    partition_service_ = partition_service;
    clog_mgr_ = clog_mgr;
  }
  return ret;
}

int ObPartitionLogPacketHandler::handle_request(ObLogReqContext& ctx)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_batch_submit_msg(ctx.type_)) {
    ret = handle_batch_request(ctx);
  } else {
    ret = handle_single_request(ctx);
  }
  return ret;
}

int ObPartitionLogPacketHandler::handle_single_request(ObLogReqContext& ctx)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  CLOG_LOG(DEBUG, "-->handle_request", K(ctx));
  if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "ctx is invalid", K(ctx));
  } else if (OB_FAIL(partition_service_->get_partition(ctx.pkey_, guard)) || NULL == guard.get_partition_group() ||
             NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!(guard.get_partition_group()->is_valid())) {
    ret = OB_INVALID_PARTITION;
  } else if (!clog_mgr_->is_scan_finished()) {
    ret = OB_SUCCESS;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "is_scan_finished false");
    }
  } else {
    const int64_t before_handle_ts = ObTimeUtility::current_time();
    switch (ctx.type_) {
      case OB_FETCH_LOG_RESP:
      case OB_PUSH_LOG: {
        ReceiveLogType type = (OB_FETCH_LOG_RESP == ctx.type_) ? FETCH_LOG : PUSH_LOG;
        ObILogEngine* log_engine = NULL;
        if (OB_ISNULL(clog_mgr_) || (OB_ISNULL(log_engine = clog_mgr_->get_log_engine()))) {
          ret = OB_INVALID_ARGUMENT;
          CLOG_LOG(WARN, "invalid arugment", KR(ret), KP(clog_mgr_), KP(log_engine), K(ctx));
        } else {
          ret = receive_log(log_service, log_engine, ctx, type);
        }
        break;
      }
      case OB_FAKE_PUSH_LOG:
        ret = fake_receive_log(log_service, ctx);
        break;
      case OB_FETCH_LOG_V2:
        ret = fetch_log(log_service, ctx);
        break;
      case OB_FAKE_ACK_LOG:
        ret = fake_ack_log(log_service, ctx);
        break;
      case OB_ACK_LOG_V2:
        ret = ack_log(log_service, ctx);
        break;
      case OB_STANDBY_ACK_LOG:
        ret = standby_ack_log(log_service, ctx);
        break;
      case OB_RENEW_MS_LOG_ACK:
        ret = ack_renew_ms_log(log_service, ctx);
        break;
      case OB_PREPARE_REQ:
        ret = prepare_req(log_service, ctx);
        break;
      case OB_PREPARE_RESP_V2:
        ret = prepare_resp(log_service, ctx);
        break;
      case OB_CONFIRMED_INFO_REQ:
        ret = receive_confirmed_info(log_service, ctx);
        break;
      case OB_KEEPALIVE_MSG:
        ret = process_keepalive_msg(log_service, ctx);
        break;
      case OB_RESTORE_LEADER_TAKEOVER_MSG:
        ret = process_restore_takeover_msg(log_service, ctx);
        break;
      case OB_NOTIFY_LOG_MISSING:
        ret = notify_log_missing(log_service, ctx);
        break;
      case OB_FETCH_REGISTER_SERVER_REQ_V2:
        ret = fetch_register_server_req(log_service, ctx);
        break;
      case OB_FETCH_REGISTER_SERVER_RESP:
        ret = OB_NOT_SUPPORTED;
        break;
      case OB_FETCH_REGISTER_SERVER_RESP_V2:
        ret = fetch_register_server_resp_v2(log_service, ctx);
        break;
      case OB_REJECT_MSG:
        ret = process_reject_msg(log_service, ctx);
        break;
      case OB_BROADCAST_INFO_REQ:
        ret = update_broadcast_info(log_service, ctx);
        break;
      case OB_REPLACE_SICK_CHILD_REQ:
        ret = replace_sick_child(log_service, ctx);
        break;
      case OB_LEADER_MAX_LOG_MSG:
        ret = process_leader_max_log_msg(log_service, ctx);
        break;
      case OB_REREGISTER_MSG:
        ret = process_reregister_msg(log_service, ctx);
        break;
      case OB_CHECK_REBUILD_REQ:
        ret = process_check_rebuild_req(log_service, ctx);
        break;
      case OB_SYNC_LOG_ARCHIVE_PROGRESS:
        ret = process_sync_log_archive_progress_msg(log_service, ctx);
        break;
      case OB_RESTORE_LOG_FINISH_MSG:
        ret = process_restore_log_finish_msg(log_service, ctx);
        break;
      case OB_RESTORE_ALIVE_MSG:
        ret = process_restore_alive_msg(log_service, ctx);
        break;
      case OB_RESTORE_ALIVE_REQ:
        ret = process_restore_alive_req(log_service, ctx);
        break;
      case OB_RESTORE_ALIVE_RESP:
        ret = process_restore_alive_resp(log_service, ctx);
        break;
      case OB_STANDBY_PREPARE_REQ:
        ret = handle_standby_prepare_req(log_service, ctx);
        break;
      case OB_STANDBY_PREPARE_RESP:
        ret = handle_standby_prepare_resp(log_service, ctx);
        break;
      case OB_STANDBY_QUERY_SYNC_START_ID:
        ret = handle_query_sync_start_id_req(log_service, ctx);
        break;
      case OB_STANDBY_SYNC_START_ID_RESP:
        ret = handle_sync_start_id_resp(log_service, ctx);
        break;
      case OB_PUSH_RENEW_MS_LOG:
        ret = receive_renew_ms_log(log_service, ctx, PUSH_LOG);
        break;
      case OB_RENEW_MS_CONFIRMED_INFO_REQ:
        ret = receive_renew_ms_log_confirmed_info(log_service, ctx);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
    const int64_t after_handle_ts = ObTimeUtility::current_time();
    if (after_handle_ts - before_handle_ts > 50 * 1000) {
      CLOG_LOG(WARN, "handle cost too much time", K(ctx), "time", after_handle_ts - before_handle_ts);
    }
  }
  if (OB_FAIL(ret)) {
    CLOG_LOG(DEBUG, "handle_packet fail", K(ret), K(ctx));
  }
  CLOG_LOG(DEBUG, "<--CLOG_RPC.handle_request", K(ctx), K(ret));
  return ret;
}

int ObPartitionLogPacketHandler::handle_batch_request(ObLogReqContext& ctx)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "ctx is invalid", K(ctx));
  } else if (!clog_mgr_->is_scan_finished()) {
    ret = OB_SUCCESS;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "is_scan_finished false");
    }
  } else {
    const int64_t before_handle_ts = common::ObClockGenerator::getClock();
    if (OB_BATCH_PUSH_LOG == ctx.type_) {
      ObBatchPushLogReq req;
      if (OB_FAIL(deserialize(ctx, req))) {
        CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
      } else {
        ret = clog_mgr_->batch_receive_log(
            req.trans_id_, req.partition_array_, req.log_info_array_, ctx.server_, ctx.cluster_id_);
        if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "batch_receive_log failed", K(ret), K(req), K(ctx));
        }
      }
    } else if (OB_BATCH_ACK_LOG == ctx.type_) {
      ObBatchAckLogReq req;
      if (OB_FAIL(deserialize(ctx, req))) {
        CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
      } else {
        ret = clog_mgr_->batch_ack_log(req.trans_id_, ctx.server_, req.batch_ack_array_);
        if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "batch_ack_log failed", K(ret), K(req), K(ctx));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    const int64_t after_handle_ts = common::ObClockGenerator::getClock();
    if (after_handle_ts - before_handle_ts > 50 * 1000) {
      CLOG_LOG(WARN, "handle cost too much time", K(ctx), "time", after_handle_ts - before_handle_ts);
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::receive_log(
    LogService* log_service, ObILogEngine* log_engine, Context& ctx, ReceiveLogType type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPushLogReq req;
  ObLogEntry entry;
  int64_t pos = 0;
  if (OB_ISNULL(log_service) || OB_ISNULL(log_engine) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(log_service), KP(log_engine), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize ObPushLogReq error", K(ret), K(ctx));
  } else {
    while (OB_SUCCESS == ret && pos < req.len_) {
      if (OB_FAIL(entry.deserialize(req.buf_, req.len_, pos))) {
        CLOG_LOG(WARN, "deserialize_entry", K(ret), K(pos), "len", req.len_);
      } else if (OB_SUCCESS !=
                 (tmp_ret = log_service->receive_log(
                      entry, ctx.server_, ctx.cluster_id_, req.proposal_id_, (ObPushLogMode)req.push_mode_, type))) {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN,
              "log_service.receive_log fail",
              "ret",
              tmp_ret,
              K(req.len_),
              K(pos),
              "ObLogEntryHeader",
              entry.get_header(),
              K(ctx.server_),
              K(req.proposal_id_));
        }
      } else {
        CLOG_LOG(DEBUG,
            "ObPartitionLogPacketHandler receive_log",
            K(req.len_),
            K(pos),
            "ObLogEntryHeader",
            entry.get_header(),
            K(ctx.server_),
            K(req.proposal_id_));
      }
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::receive_renew_ms_log(LogService* log_service, Context& ctx, ReceiveLogType type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPushLogReq req;
  ObLogEntry entry;
  int64_t pos = 0;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arugment", KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize ObPushLogReq error", K(ret), K(ctx));
  } else {
    while (OB_SUCCESS == ret && pos < req.len_) {
      if (OB_FAIL(entry.deserialize(req.buf_, req.len_, pos))) {
        CLOG_LOG(WARN, "deserialize_entry", K(ret), K(pos), "len", req.len_);
      } else if (OB_SUCCESS != (tmp_ret = log_service->receive_renew_ms_log(
                                    entry, ctx.server_, ctx.cluster_id_, req.proposal_id_, type))) {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN,
              "receive_renew_ms_log fail",
              "ret",
              tmp_ret,
              K(req.len_),
              K(pos),
              "ObLogEntryHeader",
              entry.get_header(),
              K(ctx.server_),
              K(req.proposal_id_));
        }
      } else {
        CLOG_LOG(DEBUG,
            "ObPartitionLogPacketHandler receive_renew_ms_log",
            K(req.len_),
            K(pos),
            "ObLogEntryHeader",
            entry.get_header(),
            K(ctx.server_),
            K(req.proposal_id_));
      }
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::fake_ack_log(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObFakeAckReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invlaid arugment", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else if (OB_FAIL(log_service->fake_ack_log(ctx.server_, req.log_id_, req.proposal_id_))) {
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionLogPacketHandler::fake_receive_log(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObFakePushLogReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invlaid arugment", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else if (OB_FAIL(log_service->fake_receive_log(ctx.server_, req.log_id_, req.proposal_id_))) {
    CLOG_LOG(WARN, "fake_receive_log failed", K(ret), K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionLogPacketHandler::ack_log(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObAckLogReqV2 req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(deserialize(ctx, req))) {
  } else if (OB_FAIL(log_service->ack_log(ctx.server_, req.log_id_, req.proposal_id_))) {
  } else {
  }
  return ret;
}

int ObPartitionLogPacketHandler::standby_ack_log(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObStandbyAckLogReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(deserialize(ctx, req))) {
  } else if (OB_FAIL(log_service->standby_ack_log(ctx.server_, ctx.cluster_id_, req.log_id_, req.proposal_id_))) {
    CLOG_LOG(WARN, "standby_ack_log failed", K(ret), K(ctx));
  } else {
  }
  return ret;
}

int ObPartitionLogPacketHandler::ack_renew_ms_log(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObRenewMsLogAckReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "ack_renew_ms_log deserialize failed", K(ret), K(ctx));
  } else {
    if (OB_FAIL(log_service->ack_renew_ms_log(ctx.server_, req.log_id_, req.submit_timestamp_, req.proposal_id_))) {
      CLOG_LOG(WARN, "ack_renew_ms_log failed", K(ret), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::fetch_log(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObFetchLogReqV2 req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(deserialize(ctx, req))) {
  } else if (OB_FAIL(log_service->get_log(ctx.server_,
                 req.start_id_,
                 req.end_limit_ - req.start_id_,
                 req.fetch_type_,
                 req.proposal_id_,
                 ctx.cluster_id_,
                 req.replica_type_,
                 req.network_limit_,
                 req.max_confirmed_log_id_))) {
  }
  return ret;
}

int ObPartitionLogPacketHandler::prepare_req(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObPrepareReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "prepare_req invalid_argument", K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(ERROR, "prepare_req", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(log_service->get_max_log_id(ctx.server_, ctx.cluster_id_, req.proposal_id_))) {
    CLOG_LOG(WARN, "prepare_req", K(ret), K(log_service), K(ctx));
  }
  CLOG_LOG(INFO, "handle prepare_req", K(req), K(ctx.len_));
  return ret;
}

int ObPartitionLogPacketHandler::handle_standby_prepare_req(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObStandbyPrepareReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "prepare_req invalid_argument", K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(ERROR, "prepare_req", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(log_service->handle_standby_prepare_req(ctx.server_, ctx.cluster_id_, req.ms_proposal_id_))) {
    CLOG_LOG(WARN, "handle_standby_prepare_req failed", K(ret), K(log_service), K(ctx));
  } else {
  }
  CLOG_LOG(INFO, "handle standby_prepare_req", K(req), K(ctx.len_));
  return ret;
}

int ObPartitionLogPacketHandler::prepare_resp(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObPrepareRespV2 req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "prepare_resp invalid argument", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(ERROR, "prepare_resp", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(
                 log_service->receive_max_log_id(ctx.server_, req.max_log_id_, req.proposal_id_, req.max_log_ts_))) {
    CLOG_LOG(DEBUG, "prepare_resp", K(ret), K(log_service), K(ctx));
  }
  CLOG_LOG(DEBUG, "handle prepare_resp", K(req), K(ctx.len_));
  return ret;
}

int ObPartitionLogPacketHandler::handle_standby_prepare_resp(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObStandbyPrepareResp req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "prepare_resp invalid argument", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(ERROR, "prepare_resp", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(log_service->handle_standby_prepare_resp(
                 ctx.server_, req.ms_proposal_id_, req.ms_log_id_, req.membership_version_, req.member_list_))) {
    CLOG_LOG(DEBUG, "handle_standby_preapre_resp failed", K(ret), K(log_service), K(ctx));
  } else {
  }
  CLOG_LOG(DEBUG, "handle standby_prepare_resp", K(req), K(ctx.len_));
  return ret;
}

int ObPartitionLogPacketHandler::handle_query_sync_start_id_req(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObStandbyQuerySyncStartIdReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "prepare_resp invalid argument", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(ERROR, "deserialize ObStandbyQuerySyncStartIdReq failed", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(log_service->handle_query_sync_start_id_req(ctx.server_, ctx.cluster_id_, req.send_ts_))) {
    CLOG_LOG(DEBUG, "handle_query_sync_start_id_req failed", K(ret), K(log_service), K(ctx));
  } else {
  }
  CLOG_LOG(DEBUG, "handle_query_sync_start_id_req", K(req), K(ctx.len_));
  return ret;
}

int ObPartitionLogPacketHandler::handle_sync_start_id_resp(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObStandbyQuerySyncStartIdResp req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "prepare_resp invalid argument", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(ERROR, "deserialize ObStandbyQuerySyncStartIdReq failed", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(log_service->handle_sync_start_id_resp(
                 ctx.server_, ctx.cluster_id_, req.original_send_ts_, req.sync_start_id_))) {
    CLOG_LOG(DEBUG, "handle_sync_start_id_resp failed", K(ret), K(log_service), K(ctx));
  } else {
  }
  CLOG_LOG(DEBUG, "handle_sync_start_id_resp", K(req), K(ctx.len_));
  return ret;
}

int ObPartitionLogPacketHandler::receive_confirmed_info(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObConfirmedInfoReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "receive_confirmed_info invalid argument", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
  } else if (OB_FAIL(log_service->receive_confirmed_info(
                 ctx.server_, ctx.cluster_id_, req.log_id_, req.confirmed_info_, req.batch_committed_))) {
  } else {
  }
  return ret;
}

int ObPartitionLogPacketHandler::receive_renew_ms_log_confirmed_info(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObRenewMsLogConfirmedInfoReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "receive_confirmed_info invalid argument", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
  } else {
    if (OB_FAIL(log_service->receive_renew_ms_log_confirmed_info(
            ctx.server_, req.log_id_, req.ms_proposal_id_, req.confirmed_info_))) {
      CLOG_LOG(WARN, "receive_renew_ms_log_confirmed_info failed", K(ret), K(log_service), K(ctx), K(req));
    }
  }
  CLOG_LOG(INFO, "receive_renew_ms_log_confirmed_info", K(ret), K(log_service), K(ctx), K(req));
  return ret;
}

int ObPartitionLogPacketHandler::process_keepalive_msg(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObLogKeepaliveMsg req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
  } else {
    log_service->process_keepalive_msg(
        ctx.server_, ctx.cluster_id_, req.next_log_id_, req.next_log_ts_lb_, req.deliver_cnt_);
  }
  return ret;
}

int ObPartitionLogPacketHandler::process_restore_takeover_msg(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObRestoreTakeoverMsg msg;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, msg))) {
  } else {
    log_service->process_restore_takeover_msg(msg.send_ts_);
  }
  return ret;
}

int ObPartitionLogPacketHandler::process_sync_log_archive_progress_msg(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObSyncLogArchiveProgressMsg req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "process_sync_log_archive_progress_msg failed", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
  } else {
    log_service->process_sync_log_archive_progress_msg(ctx.server_, ctx.cluster_id_, req.status_);
  }
  return ret;
}

int ObPartitionLogPacketHandler::notify_log_missing(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObNotifyLogMissingReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "notify_log_missing invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else if (OB_FAIL(log_service->notify_log_missing(
                 ctx.server_, req.start_log_id_, req.is_in_member_list_, req.msg_type_))) {
    CLOG_LOG(WARN,
        "notify_log_missing failed",
        K(ret),
        K(ctx),
        "start_log_id",
        req.start_log_id_,
        "is_in_member_list",
        req.is_in_member_list_);
  }
  return ret;
}

int ObPartitionLogPacketHandler::process_reject_msg(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObRejectMsgReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "process_replica_msg invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->process_reject_msg(ctx.server_, req.msg_type_, req.timestamp_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(WARN, "process_reject_msg failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::process_restore_alive_msg(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObRestoreAliveMsg req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "process_replica_msg invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->process_restore_alive_msg(ctx.server_, req.start_log_id_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(WARN, "process_restore_alive_msg failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::process_restore_alive_req(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObRestoreAliveReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "process_replica_msg invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->process_restore_alive_req(ctx.server_, ctx.cluster_id_, req.send_ts_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(WARN, "process_restore_alive_req failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::process_restore_alive_resp(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObRestoreAliveResp req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "process_replica_msg invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->process_restore_alive_resp(ctx.server_, req.send_ts_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(WARN, "process_restore_alive_req failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::process_restore_log_finish_msg(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObRestoreLogFinishMsg req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "process_replica_msg invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->process_restore_log_finish_msg(ctx.server_, req.log_id_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(WARN, "process_restore_log_finish_msg failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::process_reregister_msg(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObReregisterMsg req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "process_replica_msg invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->process_reregister_msg(ctx.server_, req.new_leader_, req.send_ts_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(WARN, "process_reregister_msg failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::fetch_register_server_req(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObFetchRegisterServerReqV2 req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->fetch_register_server(ctx.server_,
        req.replica_type_,
        req.next_replay_log_ts_,
        req.is_request_leader_,
        req.is_need_force_register_,
        req.region_,
        ctx.cluster_id_,
        req.idc_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(WARN, "process fetch_register_server failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::fetch_register_server_resp_v2(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObFetchRegisterServerRespV2 req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->fetch_register_server_resp_v2(
        ctx.server_, req.is_assign_parent_succeed_, req.candidate_list_, req.msg_type_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(WARN, "fetch_register_server_resp_v2 failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::update_broadcast_info(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObBroadcastInfoReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "update_broadcast_info invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->update_broadcast_info(ctx.server_, req.replica_type_, req.max_confirmed_log_id_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "update_broadcast_info failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::replace_sick_child(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObReplaceSickChildReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "ObReplaceSickChildReq invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->replace_sick_child(ctx.server_, ctx.cluster_id_, req.sick_child_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "replace_sick_child failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::process_leader_max_log_msg(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObLeaderMaxLogMsg msg;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, msg))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->process_leader_max_log_msg(
        ctx.server_, msg.switchover_epoch_, msg.leader_max_log_id_, msg.leader_next_log_ts_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "process_leader_max_log_msg failed", K(ret), K(msg), K(ctx));
    }
  }
  return ret;
}

int ObPartitionLogPacketHandler::process_check_rebuild_req(LogService* log_service, Context& ctx)
{
  int ret = OB_SUCCESS;
  ObCheckRebuildReq req;
  if (OB_ISNULL(log_service) || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(log_service), K(ctx));
  } else if (OB_FAIL(deserialize(ctx, req))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K(ctx));
  } else {
    ret = log_service->process_check_rebuild_req(ctx.server_, req.start_id_, ctx.cluster_id_);
    if (OB_SUCCESS != ret && REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "process_check_rebuild_req failed", K(ret), K(req), K(ctx));
    }
  }
  return ret;
}

};  // end namespace clog
};  // end namespace oceanbase
