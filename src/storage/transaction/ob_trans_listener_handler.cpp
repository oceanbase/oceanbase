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

#include "ob_trans_listener_handler.h"
#include "ob_trans_part_ctx.h"

namespace oceanbase {
namespace transaction {

int ObTransListenerHandler::init_listener_mask_set_(ObPartTransCtx* part_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(part_ctx);
  /*
  TODO
  */
  return ret;
}

int ObTransListenerHandler::init(ObPartTransCtx* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret));
  } else {
    part_ctx_ = ctx;
    is_inited_ = true;
    in_clear_ = false;
    // if (OB_FAIL(init_listener_mask_set_(ctx))) {
    //   TRANS_LOG(WARN, "init listener mask set failed", KR(ret));
    // } else {
    //   is_inited_ = true;
    // }
  }
  return ret;
}

bool ObTransListenerHandler::is_listener() const
{
  return part_ctx_->is_listener();
}

int ObTransListenerHandler::notify_listener(const ListenerAction action)
{
  int ret = OB_SUCCESS;
  UNUSED(action);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "listener handler is not inited", KR(ret));
  } else {
    /*
    TODO
    */
  }
  return ret;
}

int ObTransListenerHandler::handle_listener_commit_request(const ObTransSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  UNUSED(split_info);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "listener handler is not inited", KR(ret));
  } else if (!is_commit_log_synced_) {
    if (part_ctx_->is_logging()) {
      // discard message
    } else {
      /*
      TODO, submit OB_LOG_LISTENER_COMMIT
      */
    }
  } else if (OB_FAIL(try_respond_participant_(OB_TRX_LISTENER_COMMIT_RESPONSE, LISTENER_COMMIT))) {
    TRANS_LOG(WARN, "try respond participant failed", KR(ret));
  }
  return ret;
}

int ObTransListenerHandler::handle_listener_abort_request(const ObTransSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  UNUSED(split_info);
  /*
  TODO
  */
  return ret;
}

int ObTransListenerHandler::handle_listener_commit_response(const ObTrxListenerCommitResponse& res)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "listener handler is not inited", KR(ret));
  } else if (!listener_mask_set_.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "listener mask set is not inited", KR(ret));
  } else if (OB_FAIL(listener_mask_set_.mask(res.sender_))) {
    TRANS_LOG(WARN, "mask listener failed", KR(ret));
  }
  return ret;
}

int ObTransListenerHandler::handle_listener_abort_response(const ObTrxListenerAbortResponse& res)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "listener handler is not inited", KR(ret));
  } else if (!listener_mask_set_.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "listener mask set is not inited", KR(ret));
  } else if (OB_FAIL(listener_mask_set_.mask(res.sender_))) {
    TRANS_LOG(WARN, "mask listener failed", KR(ret));
  }
  return ret;
}

int ObTransListenerHandler::handle_listener_clear_request(const ObTrxListenerClearRequest& req)
{
  int ret = OB_SUCCESS;
  UNUSED(req);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "listener handler is not inited", KR(ret));
  } else if (!listener_mask_set_.is_inited()) {
    /*
    TODO
    */
  } else {
    // have next Listener
    if (is_listener_ready()) {
      // TODO
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObTransListenerHandler::handle_listener_clear_response(const ObTrxListenerClearResponse& res)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "listener handler is not inited", KR(ret));
  } else if (OB_FAIL(listener_mask_set_.mask(res.sender_))) {
    TRANS_LOG(WARN, "mask listener failed", KR(ret));
  }
  if (!is_listener() && is_all_ready()) {
    /*
    TODO
    */
  }
  return ret;
}

int ObTransListenerHandler::on_listener_commit()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "listener handler is not inited", KR(ret));
  } else {
    set_commit_log_synced(true);
    if (OB_FAIL(try_respond_participant_(OB_TRX_LISTENER_COMMIT_RESPONSE, LISTENER_COMMIT))) {
      TRANS_LOG(WARN, "try respond participant failed", KR(ret));
    }
  }
  return ret;
}

int ObTransListenerHandler::on_listener_abort()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "listener handler is not inited", KR(ret));
  } else {
    set_commit_log_synced(true);
    if (OB_FAIL(try_respond_participant_(OB_TRX_LISTENER_ABORT_RESPONSE, LISTENER_ABORT))) {
      TRANS_LOG(WARN, "try respond participant failed", KR(ret));
    }
  }
  return ret;
}

int ObTransListenerHandler::try_respond_participant_(const int64_t msg_type, const ListenerAction action)
{
  int ret = OB_SUCCESS;
  bool is_split_partition = false;
  UNUSED(msg_type);
  if (listener_mask_set_.is_inited()) {
    if (listener_mask_set_.is_all_mask()) {
      /*
      TODO
      */
    } else if (OB_FAIL(notify_listener(action))) {
      TRANS_LOG(WARN, "notify listener failed", KR(ret));
    }
  } else if (OB_FAIL(part_ctx_->check_cur_partition_split_(is_split_partition))) {
    TRANS_LOG(WARN, "check cur partition split failed", KR(ret));
  } else if (!is_split_partition) {
    /*
    TODO
    */
  } else if (OB_FAIL(init_listener_mask_set_(part_ctx_))) {
    TRANS_LOG(WARN, "init listener mask set failed", KR(ret));
  } else if (OB_FAIL(notify_listener(action))) {
    TRANS_LOG(WARN, "notify listener failed", KR(ret));
  }
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase