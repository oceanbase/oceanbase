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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_MSG_HANDLER_
#define OCEANBASE_TRANSACTION_OB_TRANS_MSG_HANDLER_

#include "ob_trans_define.h"
#include "ob_trans_rpc.h"
#include "ob_trans_msg2.h"

namespace oceanbase {
namespace transaction {
class ObTransService;
class ObPartTransCtxMgr;
class ObCoordTransCtxMgr;
class ObILocationAdapter;
class ObITransRpc;
class ObCoordTransCtx;

class ObTransMsgHandler {
public:
  ObTransMsgHandler()
  {
    reset();
  }
  ~ObTransMsgHandler()
  {
    reset();
  }
  void reset()
  {
    txs_ = NULL;
    part_trans_ctx_mgr_ = NULL;
    coord_trans_ctx_mgr_ = NULL;
    location_adapter_ = NULL;
    rpc_ = NULL;
  }
  int init(ObTransService* txs, ObPartTransCtxMgr* part_trans_ctx_mgr, ObCoordTransCtxMgr* coord_trans_ctx_mgr,
      ObILocationAdapter* location_adapter, ObITransRpc* rpc);
  int part_ctx_handle_request(ObTrxMsgBase& msg, const int64_t msg_type);
  int coord_ctx_handle_response(const ObTrxMsgBase& msg, const int64_t msg_type);
  int orphan_msg_handle(const ObTrxMsgBase& msg, const int64_t msg_type);

private:
  int handle_orphan_2pc_clear_request_(const ObTrx2PCClearRequest& msg);
  int handle_orphan_2pc_abort_request_(const ObTrx2PCAbortRequest& msg);
  int handle_orphan_2pc_commit_request_(const ObTrx2PCCommitRequest& msg);
  int handle_orphan_2pc_prepare_request_(const ObTrx2PCPrepareRequest& msg);
  int handle_orphan_listener_clear_request_(const ObTrxListenerClearRequest& req);
  int send_msg_(
      const uint64_t tenant_id, const ObTrxMsgBase& msg, const int64_t msg_type, const common::ObAddr& recv_addr);
  int orphan_msg_helper_(const ObTrxMsgBase& msg, const int64_t msg_type);
  int part_ctx_handle_request_helper_(ObPartTransCtx* part_ctx, const ObTrxMsgBase& msg, const int64_t msg_type);
  int coord_ctx_handle_response_helper_(
      ObCoordTransCtx* coord_ctx, const bool alloc, const ObTrxMsgBase& msg, const int64_t msg_type);

private:
  ObTransService* txs_;
  ObPartTransCtxMgr* part_trans_ctx_mgr_;
  ObCoordTransCtxMgr* coord_trans_ctx_mgr_;
  ObILocationAdapter* location_adapter_;
  ObITransRpc* rpc_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif
