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

#define USING_LOG_PREFIX TRANS
#include "ob_mock_lock_wait_mgr.h"

// #include "it/tx_node.h"

namespace oceanbase
{
namespace transaction
{
class ObTxNode;
}
namespace lockwaitmgr
{

ObMockLockWaitMgr::ObMockLockWaitMgr(const uint64_t tenant_id,
                                     MsgBus *msg_bus,
                                     const ObAddr &my_addr)
    : fake_rpc_(msg_bus, my_addr, this), inflight_cnt_(0), is_sleeping_(false)
{
  set_tenant_id(tenant_id_);
  set_rpc(&fake_rpc_);
  set_addr(my_addr);
}

int ObMockLockWaitMgr::init()
{
  int ret = OB_SUCCESS;
  ObLockWaitMgr::init(true);
  is_inited_ = true;
  total_wait_node_ = 0;
  stop_repost_node_ = false;
  store_repost_node_ = NULL;
  TRANS_LOG(TRACE, "mock lock wait mgr init", K(tenant_id_));
  last_check_session_idle_ts_ = ObClockGenerator::getClock();
  return ret;
}

int ObMockLockWaitMgr::repost(Node *node)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool has_stop = ATOMIC_LOAD(&stop_repost_node_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "lock wait mgr not inited", K(ret));
  } else if (has_stop) {
    store_repost_node_ = node;
    TRANS_LOG(TRACE, "store repost node", KPC(node));
  } else if (!has_stop) {
    ObTransID self_tx_id(node->tx_id_);
    if (node->get_node_type() == Node::REMOTE_EXEC_SIDE) {
      if (node->is_placeholder()) {
        // just discard and wakeup next waiter
        fetch_and_repost_head(node->hash());
      } else if (OB_FAIL(on_remote_lock_release_(*node))) {
        TRANS_LOG(WARN, "inform remote lock release fail", K(ret), KPC(node));
      }
      free_node_(node);
    } else if (repost_request_queue_ != NULL && cond_ != NULL) {
      repost_request_queue_->push(node);
      ATOMIC_INC(&inflight_cnt_);
      cond_->signal();
    }
  }

  return ret;
}

void ObMockLockWaitMgr::wakeup_key(uint64_t hash)
{
  TRANS_LOG_RET(DEBUG, OB_SUCCESS, "mock lock wait mgr wakeup key");
  fetch_and_repost_head(hash);
}

void ObMockLockWaitMgr::inc_seq(uint64_t hash)
{
  ATOMIC_INC(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT]);
  TRANS_LOG_RET(TRACE, OB_SUCCESS, "inc seq", K(hash), K(ATOMIC_LOAD(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT])));
}

} // lockwaitmgr
} // oceanbase