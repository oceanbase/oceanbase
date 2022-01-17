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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_LISTENER_HANDLER_H
#define OCEANBASE_TRANSACTION_OB_TRANS_LISTENER_HANDLER_H

#include "ob_mask_set.h"
#include "ob_trans_msg2.h"

namespace oceanbase {
namespace transaction {

class ObPartTransCtx;
class ObTransSplitInfo;

enum ListenerAction { LISTENER_COMMIT = 0, LISTENER_ABORT, LISTENER_CLEAR };

class ObTransListenerHandler {
public:
  ObTransListenerHandler() : part_ctx_(NULL), is_commit_log_synced_(false), is_inited_(false), in_clear_(false)
  {}
  ~ObTransListenerHandler()
  {
    destroy();
  }
  void destroy()
  {
    reset();
  }
  int init(ObPartTransCtx* ctx);
  bool is_inited() const
  {
    return is_inited_;
  }
  void reset()
  {
    part_ctx_ = NULL;
    listener_mask_set_.reset();
    is_commit_log_synced_ = false;
    is_inited_ = false;
    in_clear_ = false;
  }
  // there is no downstream listener or the response of
  // all downstream listeners has been received
  bool is_listener_ready() const
  {
    return !listener_mask_set_.is_inited()      /* no downstream listener */
           || listener_mask_set_.is_all_mask(); /* all downstream listeners have responded */
  }
  // commit log has been written, and there is no downstream listener or
  // the response of all downstream listeners has been received
  bool is_all_ready() const
  {
    return is_commit_log_synced_ && is_listener_ready();
  }
  bool is_listener() const;
  int notify_listener(const ListenerAction action);
  int handle_listener_commit_request(const ObTransSplitInfo& split_info);
  int handle_listener_abort_request(const ObTransSplitInfo& split_info);
  int handle_listener_commit_response(const ObTrxListenerCommitResponse& res);
  int handle_listener_abort_response(const ObTrxListenerAbortResponse& res);
  int handle_listener_clear_request(const ObTrxListenerClearRequest& req);
  int handle_listener_clear_response(const ObTrxListenerClearResponse& res);
  int on_listener_commit();
  int on_listener_abort();
  void set_commit_log_synced(const bool synced)
  {
    is_commit_log_synced_ = synced;
  }
  bool is_commit_log_synced() const
  {
    return is_commit_log_synced_;
  }
  void clear_mask()
  {
    listener_mask_set_.clear_set();
  }
  void set_in_clear(const bool in_clear)
  {
    in_clear_ = in_clear;
  }
  bool is_in_clear() const
  {
    return in_clear_;
  }
  TO_STRING_KV(K_(is_commit_log_synced), K_(is_inited));

private:
  int init_listener_mask_set_(ObPartTransCtx* ctx);
  int try_respond_participant_(const int64_t msg_type, const ListenerAction action);
  DISALLOW_COPY_AND_ASSIGN(ObTransListenerHandler);

private:
  ObPartTransCtx* part_ctx_;
  common::ObMaskSet listener_mask_set_;
  bool is_commit_log_synced_;
  bool is_inited_;
  bool in_clear_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif