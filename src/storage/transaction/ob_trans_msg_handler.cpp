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

#include "ob_trans_msg_handler.h"
#include "ob_trans_service.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_coord_ctx.h"
#include "ob_trans_msg_type2.h"
#include "ob_trans_msg2.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {

using namespace common;
using namespace obrpc;

namespace transaction {

int ObTransMsgHandler::init(ObTransService* txs, ObPartTransCtxMgr* part_trans_ctx_mgr,
    ObCoordTransCtxMgr* coord_trans_ctx_mgr, ObILocationAdapter* location_adapter, ObITransRpc* rpc)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(txs) || OB_ISNULL(part_trans_ctx_mgr) || OB_ISNULL(coord_trans_ctx_mgr) ||
      OB_ISNULL(location_adapter) || OB_ISNULL(rpc)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN,
        "invalid argument",
        K(ret),
        KP(txs),
        KP(part_trans_ctx_mgr),
        KP(coord_trans_ctx_mgr),
        KP(location_adapter),
        KP(rpc));
  } else {
    txs_ = txs;
    part_trans_ctx_mgr_ = part_trans_ctx_mgr;
    coord_trans_ctx_mgr_ = coord_trans_ctx_mgr;
    location_adapter_ = location_adapter;
    rpc_ = rpc;
  }

  return ret;
}

int ObTransMsgHandler::send_msg_(
    const uint64_t tenant_id, const ObTrxMsgBase& msg, const int64_t msg_type, const ObAddr& recv_addr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(rpc_->post_trans_msg(tenant_id, recv_addr, msg, msg_type))) {
    TRANS_LOG(WARN, "post transaction message error", K(ret), K(tenant_id), K(msg_type));
  }

  return ret;
}

int ObTransMsgHandler::handle_orphan_2pc_clear_request_(const ObTrx2PCClearRequest& msg)
{
  int ret = OB_SUCCESS;
  const int64_t msg_type = OB_TRX_2PC_CLEAR_RESPONSE;
  ObTrx2PCClearResponse ret_msg;
  // the local cached gts value is refresh, because of the transaction already
  // committed at this participant.
  int64_t clear_log_base_ts = 0;
  if (OB_FAIL(OB_TS_MGR.get_gts(msg.receiver_.get_tenant_id(), NULL, clear_log_base_ts))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "get gts error", KR(ret), K(msg));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (clear_log_base_ts <= 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected log timestamp", K(ret), K(clear_log_base_ts));
  } else if (OB_FAIL(ret_msg.init(msg.tenant_id_,
                 msg.trans_id_,
                 msg_type,
                 msg.trans_time_,
                 msg.receiver_,
                 msg.sender_,
                 msg.scheduler_,
                 msg.coordinator_,
                 msg.participants_,
                 msg.trans_param_,
                 txs_->get_server(),
                 msg.request_id_,
                 OB_SUCCESS,
                 clear_log_base_ts + 200000 /*200ms*/))) {
    TRANS_LOG(WARN, "ObTrx2PCClearResponse init failed", K(ret), K(msg));
  } else if (OB_FAIL(send_msg_(ret_msg.tenant_id_, ret_msg, msg_type, msg.sender_addr_))) {
    TRANS_LOG(WARN, "send msg error", K(ret), K(ret_msg));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransMsgHandler::handle_orphan_2pc_abort_request_(const ObTrx2PCAbortRequest& msg)
{
  int ret = OB_SUCCESS;

  const int64_t msg_type = OB_TRX_2PC_ABORT_RESPONSE;
  ObTrx2PCAbortResponse ret_msg;

  if (OB_FAIL(ret_msg.init(msg.tenant_id_,
          msg.trans_id_,
          msg_type,
          msg.trans_time_,
          msg.receiver_,
          msg.sender_,
          msg.scheduler_,
          msg.coordinator_,
          msg.participants_,
          msg.trans_param_,
          txs_->get_server(),
          msg.request_id_,
          OB_TRANS_STATE_UNKNOWN))) {
    TRANS_LOG(WARN, "ObTrx2PCAbortResponse init failed", K(ret), K(msg));
  } else if (OB_FAIL(send_msg_(ret_msg.tenant_id_, ret_msg, msg_type, msg.sender_addr_))) {
    TRANS_LOG(WARN, "send msg error", K(ret), K(ret_msg));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransMsgHandler::handle_orphan_2pc_commit_request_(const ObTrx2PCCommitRequest& msg)
{
  int ret = OB_SUCCESS;

  const int64_t msg_type = OB_TRX_2PC_COMMIT_RESPONSE;
  ObTrx2PCCommitResponse ret_msg;
  // transaction context not exist, this can happened at commit message duplicated,
  // the transaction already committed at this participant, the gts cache is uptodate.
  int64_t commit_log_ts = 0;
  if (OB_FAIL(OB_TS_MGR.get_gts(msg.receiver_.get_tenant_id(), NULL, commit_log_ts))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "get gts error", KR(ret), K(msg));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (commit_log_ts <= 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected log timestamp", K(ret), K(commit_log_ts));
  } else if (OB_FAIL(ret_msg.init(msg.tenant_id_,
                 msg.trans_id_,
                 msg_type,
                 msg.trans_time_,
                 msg.receiver_,
                 msg.sender_,
                 msg.scheduler_,
                 msg.coordinator_,
                 msg.participants_,
                 msg.trans_param_,
                 msg.trans_version_,
                 txs_->get_server(),
                 msg.request_id_,
                 msg.partition_log_info_arr_,
                 msg.status_,
                 commit_log_ts + 200000 /*200ms*/))) {
    TRANS_LOG(WARN, "ObTrx2PCAbortResponse init failed", K(ret), K(msg));
  } else if (OB_FAIL(send_msg_(ret_msg.tenant_id_, ret_msg, msg_type, msg.sender_addr_))) {
    TRANS_LOG(WARN, "send msg error", K(ret), K(ret_msg));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransMsgHandler::handle_orphan_2pc_prepare_request_(const ObTrx2PCPrepareRequest& msg)
{
  int ret = OB_SUCCESS;

  const int64_t msg_type = OB_TRX_2PC_PREPARE_RESPONSE;
  ObTrx2PCPrepareResponse ret_msg;
  int32_t msg_status = OB_SUCCESS;
  int64_t trans_version = ObTimeUtility::current_time();
  uint64_t prepare_log_id = trans_version;
  int64_t prepare_log_timestamp = 0;
  const PartitionLogInfoArray& arr = msg.partition_log_info_arr_;
  bool need_response = true;
  // for XA_PREPARE message, RM's transaction state should stay in INIT state.
  int64_t participant_state = msg.is_xa_prepare_ ? Ob2PCState::INIT : Ob2PCState::PREPARE;

  if (arr.count() != msg.participants_.count()) {
    if (msg.split_info_.is_valid()) {
      // split-dest partition hasn't copy src partition's transaction ctx, need retry
      need_response = false;
      if (EXECUTE_COUNT_PER_SEC(1)) {
        TRANS_LOG(INFO, "dest partition part ctx has not been copied", K(msg.participants_), K(msg.split_info_));
      }
    } else {
      msg_status = OB_TRANS_STATE_UNKNOWN;
      TRANS_LOG(INFO, "handle orphan 2pc prepare request", K(msg_status), K(msg));
    }
  } else {
    // this participant has been in batch-commit, check its logid and log_timestamp
    // correct in transaction context.
    uint64_t log_id = OB_INVALID_ID;
    int64_t ts = OB_INVALID_TIMESTAMP;
    for (int64_t i = 0; i < arr.count(); ++i) {
      if (msg.receiver_ == arr.at(i).get_partition()) {
        log_id = arr.at(i).get_log_id();
        ts = arr.at(i).get_log_timestamp();
        break;
      }
    }
    if (OB_INVALID_ID == log_id || OB_INVALID_TIMESTAMP == ts) {
      TRANS_LOG(ERROR, "unexpected log id or timestamp", K(msg));
      ret = OB_ERR_UNEXPECTED;
    } else {
      const ObPartitionKey& partition = msg.receiver_;
      storage::ObIPartitionGroupGuard guard;
      clog::ObIPartitionLogService* log_service = NULL;
      ObTransID trans_id;
      int64_t submit_timestamp = OB_INVALID_TIMESTAMP;

      if (OB_FAIL(txs_->get_partition_service()->get_partition(partition, guard))) {
        TRANS_LOG(WARN, "get partition failed", K(partition), K(ret));
      } else if (NULL == guard.get_partition_group()) {
        TRANS_LOG(WARN, "partition not exist", K(partition));
        ret = OB_PARTITION_NOT_EXIST;
      } else if (NULL == (log_service = guard.get_partition_group()->get_log_service())) {
        TRANS_LOG(WARN, "get partiton log service error", K(partition));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(log_service->query_confirmed_log(log_id, trans_id, submit_timestamp))) {
        if (OB_NOT_MASTER == ret || OB_EAGAIN == ret) {
          // let coordinator retry
        } else if (OB_ENTRY_NOT_EXIST == ret) {
          msg_status = OB_TRANS_KILLED;
          ret = OB_SUCCESS;
        } else {
          TRANS_LOG(ERROR, "unexpected ret code", K(ret), K(msg));
          ret = OB_ERR_UNEXPECTED;
        }
        TRANS_LOG(INFO, "handle query confirmed log", K(ret), K(msg_status), K(msg));
      } else {
        if (trans_id == msg.trans_id_) {
          if (ts == submit_timestamp) {
            prepare_log_id = log_id;
            prepare_log_timestamp = ts;
            trans_version = prepare_log_timestamp;
            msg_status = OB_SUCCESS;
            if (EXECUTE_COUNT_PER_SEC(64)) {
              TRANS_LOG(INFO, "query confirmed log success", K(prepare_log_id), K(prepare_log_timestamp), K(msg));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(
                ERROR, "submit_timestamp not match, unexpected", K(log_id), K(trans_id), K(submit_timestamp), K(ts));
          }
        } else {
          TRANS_LOG(WARN, "different trans_id", K(trans_id), K(msg));
          // transaction need to be rollbacked
          msg_status = OB_TRANS_KILLED;
          ret = OB_SUCCESS;
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_response) {
    if (OB_FAIL(ret_msg.init(msg.tenant_id_,
            msg.trans_id_,
            msg_type,
            msg.trans_time_,
            msg.receiver_,
            msg.sender_,
            msg.scheduler_,
            msg.coordinator_,
            msg.participants_,
            msg.trans_param_,
            prepare_log_id,
            prepare_log_timestamp,
            txs_->get_server(),
            msg_status,
            participant_state,
            trans_version,
            msg.request_id_,
            msg.partition_log_info_arr_,
            0,  // need_wait_interval_us
            msg.app_trace_info_,
            0,  // publish_version
            msg.xid_,
            msg.is_xa_prepare_))) {
      TRANS_LOG(WARN, "ObTrx2PCPrepareResponse init error", K(ret), K(msg));
    } else if (OB_FAIL(send_msg_(ret_msg.tenant_id_, ret_msg, msg_type, msg.sender_addr_))) {
      TRANS_LOG(WARN, "send msg error", K(ret), K(ret_msg));
    }
  }

  return OB_SUCCESS;
}

int ObTransMsgHandler::handle_orphan_listener_clear_request_(const ObTrxListenerClearRequest& req)
{
  int ret = OB_SUCCESS;
  UNUSED(req);
  /*
  TODO, replay with `clear_ok`.
  */
  return ret;
}

int ObTransMsgHandler::orphan_msg_handle(const ObTrxMsgBase& msg, const int64_t msg_type)
{
  return orphan_msg_helper_(msg, msg_type);
}

// handle orphan message (the participant not exist)
int ObTransMsgHandler::orphan_msg_helper_(const ObTrxMsgBase& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  ObTrxMsgBase* message = const_cast<ObTrxMsgBase*>(&msg);

  if (OB_TRX_2PC_CLEAR_REQUEST == msg_type) {
    if (OB_FAIL(handle_orphan_2pc_clear_request_(*static_cast<ObTrx2PCClearRequest*>(message)))) {
      TRANS_LOG(WARN, "handle orphan 2pc clear request failed", K(ret), K(*message));
    }
  } else if (OB_TRX_2PC_ABORT_REQUEST == msg_type) {
    if (OB_FAIL(handle_orphan_2pc_abort_request_(*static_cast<ObTrx2PCAbortRequest*>(message)))) {
      TRANS_LOG(WARN, "handle orphan 2pc abort request failed", K(ret), K(*message));
    }
  } else if (OB_TRX_2PC_COMMIT_REQUEST == msg_type) {
    if (OB_FAIL(handle_orphan_2pc_commit_request_(*static_cast<ObTrx2PCCommitRequest*>(message)))) {
      TRANS_LOG(WARN, "handle orphan 2pc commit request failed", K(ret), K(*message));
    }
  } else if (OB_TRX_2PC_PREPARE_REQUEST == msg_type) {
    if (OB_FAIL(handle_orphan_2pc_prepare_request_(*static_cast<ObTrx2PCPrepareRequest*>(message)))) {
      TRANS_LOG(WARN, "handle orphan 2pc prepare request failed", K(ret), K(*message));
    }
  } else if (OB_TRX_LISTENER_CLEAR_REQUEST == msg_type) {
    if (OB_FAIL(handle_orphan_listener_clear_request_(*static_cast<ObTrxListenerClearRequest*>(message)))) {
      TRANS_LOG(WARN, "handle orphan 2pc clear request failed", K(ret), K(*message));
    }
  } else {
    TRANS_LOG(WARN, "not need to handle orphan msg", K(*message));
  }

  return ret;
}

int ObTransMsgHandler::part_ctx_handle_request_helper_(
    ObPartTransCtx* part_ctx, const ObTrxMsgBase& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;

  if (!ObTransMsgType2Checker::is_listener_message(msg_type)) {
    if (OB_FAIL(part_ctx->handle_2pc_request(msg, msg_type))) {
      TRANS_LOG(WARN, "handle 2pc request failed", K(ret), K(*part_ctx), K(msg));
    }
  } else if (OB_FAIL(part_ctx->handle_listener_message(msg, msg_type))) {
    TRANS_LOG(WARN, "handle listener message failed", K(ret), K(*part_ctx), K(msg));
  }

  return ret;
}

int ObTransMsgHandler::part_ctx_handle_request(ObTrxMsgBase& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;

  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;
  const ObTransID& trans_id = msg.trans_id_;
  const bool for_replay = false;
  const bool is_readonly = false;
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;
  bool alloc = false;
  const ObPartitionArray& batch_partitions = msg.batch_same_leader_partitions_;

  if (OB_TRX_LISTENER_COMMIT_REQUEST == msg_type || OB_TRX_LISTENER_ABORT_REQUEST == msg_type) {
    alloc = true;
  }

  for (int i = 0; OB_SUCC(ret) && i <= batch_partitions.count(); i++) {
    if (batch_partitions.count() > 0) {
      if (i < batch_partitions.count()) {
        msg.receiver_ = batch_partitions.at(i);
      } else {
        break;
      }
    }
    if (OB_FAIL(txs_->check_partition_status(msg.receiver_))) {
      TRANS_LOG(WARN, "check partition status failed", K(ret), K(msg));
    } else if (OB_FAIL(part_trans_ctx_mgr_->get_trans_ctx(msg.receiver_,
                   trans_id,
                   for_replay,
                   is_readonly,
                   is_bounded_staleness_read,
                   need_completed_dirty_txn,
                   alloc,
                   ctx))) {
      TRANS_LOG(DEBUG, "get transaction context error", K(ret), K(msg));
      // rewrite ret
      ret = OB_SUCCESS;
      if (OB_FAIL(orphan_msg_helper_(msg, msg_type))) {
        TRANS_LOG(WARN, "orphan msg helper failed", K(ret), K(msg));
      }
    } else {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      if (alloc && OB_FAIL(part_ctx->construct_listener_context(msg))) {
        TRANS_LOG(WARN, "construct listener context failed", KR(ret));
      } else if (OB_FAIL(part_ctx_handle_request_helper_(part_ctx, msg, msg_type))) {
        TRANS_LOG(WARN, "handle 2pc request failed", K(ret), K(msg));
      }
      (void)part_trans_ctx_mgr_->revert_trans_ctx(ctx);
    }
  }

  return ret;
}

int ObTransMsgHandler::coord_ctx_handle_response(const ObTrxMsgBase& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(txs_->check_partition_status(msg.receiver_))) {
    TRANS_LOG(WARN, "check partition status failed", K(ret), K(msg));
  } else {
    ObTransCtx* ctx = NULL;
    ObCoordTransCtx* coord_ctx = NULL;
    const ObPartitionKey& partition = msg.receiver_;
    const uint64_t tenant_id = msg.tenant_id_;
    const ObTransID& trans_id = msg.trans_id_;
    const bool for_replay = false;
    const bool is_readonly = false;
    const bool is_bounded_staleness_read = false;
    bool alloc = true;
    if (OB_FAIL(coord_trans_ctx_mgr_->get_trans_ctx(partition, trans_id, false, is_readonly, alloc, ctx))) {
      TRANS_LOG(WARN, "get transaction context error", K(ret), K(partition), K(trans_id));
      ctx = NULL;
    } else if (OB_ISNULL(ctx)) {
      TRANS_LOG(WARN, "context is null", K(partition), K(trans_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      coord_ctx = static_cast<ObCoordTransCtx*>(ctx);
      if (OB_FAIL(coord_ctx_handle_response_helper_(coord_ctx, alloc, msg, msg_type))) {
        TRANS_LOG(WARN, "coord ctx handle response failed", K(ret), K(*coord_ctx), K(msg));
      }
      (void)coord_trans_ctx_mgr_->revert_trans_ctx(ctx);
    }
  }

  return ret;
}

int ObTransMsgHandler::coord_ctx_handle_response_helper_(
    ObCoordTransCtx* coord_ctx, const bool alloc, const ObTrxMsgBase& msg, const int64_t msg_type)
{
  int ret = OB_SUCCESS;
  const ObPartitionKey& partition = msg.receiver_;
  const uint64_t tenant_id = msg.tenant_id_;
  const ObTransID& trans_id = msg.trans_id_;
  // commit_times is 0, when coordinator was created by participant's 2pc message.
  int64_t commit_times = 0;

  if (alloc) {
    if (OB_FAIL(coord_ctx->init(tenant_id,
            trans_id,
            msg.trans_time_,
            partition,
            coord_trans_ctx_mgr_,
            msg.trans_param_,
            GET_MIN_CLUSTER_VERSION(),
            txs_,
            commit_times))) {
      TRANS_LOG(WARN, "transaction context init error", K(ret), K(partition), K(trans_id), K(msg), K(commit_times));
      coord_ctx = NULL;
    } else if (OB_FAIL(coord_ctx->construct_context(msg, msg_type))) {
      TRANS_LOG(WARN, "coordinator construct transaction context error", K(ret), K(trans_id), K(msg));
      coord_ctx = NULL;
    } else if (OB_FAIL(coord_ctx->handle_2pc_response(msg, msg_type))) {
      TRANS_LOG(WARN, "coordinator handle message error", K(ret), K(trans_id), K(msg));
    } else {
      // do nothing
    }
  } else if (OB_FAIL(coord_ctx->handle_2pc_response(msg, msg_type))) {
    TRANS_LOG(WARN, "coordinator handle message error", K(ret), K(trans_id), K(msg));
  } else {
    // do nothing
  }

  return ret;
}

}  // namespace transaction

}  // namespace oceanbase
