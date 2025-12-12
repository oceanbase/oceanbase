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
#ifndef OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_LOCK_WAIT_MGR
#define OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_LOCK_WAIT_MGR

#include "storage/lock_wait_mgr/ob_lock_wait_mgr.h"
#include "ob_mock_request_msg.h"
#include "../mock_utils/ob_mock_lock_wait_mgr_rpc.h"

namespace oceanbase
{

namespace lockwaitmgr
{

static const uint64_t TRANS_FLAG = 1L << 63L;       // 10
static const uint64_t TABLE_LOCK_FLAG = 1L << 62L;  // 01
static const uint64_t ROW_FLAG = 0L;                // 00
static const uint64_t HASH_MASK = ~(TRANS_FLAG | TABLE_LOCK_FLAG);

// inline
// uint64_t hash_rowkey(const ObTabletID &tablet_id, const ObLockWaitMgr::Key &key)
// {
//   uint64_t hash_id = tablet_id.hash();
//   uint64_t hash_key = key.hash();
//   uint64_t hash = murmurhash(&hash_key, sizeof(hash_key), hash_id);
//   return ((hash & HASH_MASK) | ROW_FLAG) | 1;
// }

inline
uint64_t hash_rowkey(const ObTabletID &tablet_id, const uint64_t key)
{
  uint64_t hash_id = tablet_id.hash();
  uint64_t hash_key = murmurhash(&key, sizeof(key), 0);
  uint64_t hash = murmurhash(&hash_key, sizeof(hash_key), hash_id);
  uint64_t hash_ret = ((hash & HASH_MASK) | ROW_FLAG) | 1;
  TRANS_LOG(DEBUG, "get hash row key", K(tablet_id), K(key), K(hash_ret));
  return hash_ret;
}

typedef rpc::ObLockWaitNode Node;
class ObMockLockWaitMgr : public ObLockWaitMgr
{
public:
  ObMockLockWaitMgr(const uint64_t tenant_id,
                    MsgBus *msg_bus,
                    const ObAddr &my_addr);
  ~ObMockLockWaitMgr() { ObLockWaitMgr::destroy(); }
  int init();
  void stop() {
    ObLockWaitMgr::stop();
    TRANS_LOG(INFO, "MockLockWaitMgr.stop");
  }
  void set_req_queue_cond(common::SimpleCond *cond) { cond_ = cond; }
  void set_req_queue(ObLinkQueue *req_queue) { repost_request_queue_ = req_queue; }
  void wakeup_repost_queue();
  void wakeup_key(uint64_t hash);
  void inc_seq(uint64_t hash);
  void stop_repost_node() {
    ATOMIC_SET(&stop_repost_node_, true);
  }
  void allow_repost_node() {
    ATOMIC_SET(&stop_repost_node_, false);
  }
  int wait_node_cnt() {
    return get_wait_node_cnt();
  }
  void handle_lock_conflict(ObTxDesc *tx)
  {
    bool unused = false;
    if (tx != NULL) {
      on_lock_conflict(*tx, unused);
    }
    // else {
    //   on_lock_conflict(cflict_info_array);
    // }
  }
  int repost(Node *node);
  void begin_row_lock_wait_event(const Node * const node) {}
  void end_row_lock_wait_event(const Node * const node) {}
  ObFakeLockWaitMgrRpc* get_fake_rpc() { return &fake_rpc_; }
  Node* get_store_repost_node() { return store_repost_node_; }
private:
  bool stop_repost_node_;
  Node* store_repost_node_;
  ObFakeLockWaitMgrRpc fake_rpc_;
  int inflight_cnt_;
  int is_sleeping_;
  common::SimpleCond *cond_;
  ObLinkQueue *repost_request_queue_;
};

} // memtable
} // oceanbase

#endif // OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_LOCK_WAIT_MGR