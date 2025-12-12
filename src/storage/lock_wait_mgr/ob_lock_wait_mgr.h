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

#ifndef OCEANBASE_LOCKWAITMGR_OB_LOCK_WAIT_MGR_H_
#define OCEANBASE_LOCKWAITMGR_OB_LOCK_WAIT_MGR_H_

#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_qsync.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/rowid/ob_urowid.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/utility.h"
#include "ob_lock_wait_mgr_rpc.h"
#include "observer/ob_server_struct.h"
#include "rpc/ob_request.h"
#include "share/deadlock/ob_deadlock_detector_common_define.h"
#include "share/ob_thread_pool.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"
#include "share/ob_delegate.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualLockWaitStat;
}
namespace lockwaitmgr
{
using namespace transaction;
using namespace memtable;
using namespace share::detector;
using namespace obrpc;

/*******************************************/

typedef rpc::NodeID NodeID;
class ObNodeSeqGenarator
{
public:
  ObNodeSeqGenarator() {
    max_seq_no_ = ObClockGenerator::getClock();
  };
  // generate machine level unique incremental id for LockWaitNode
  NodeID generate_node_seq() {
    return get_and_inc_max_seq_no();
  }
private:
  NodeID get_and_inc_max_seq_no() {
    return ATOMIC_FAA(&max_seq_no_, 1);
  };
  NodeID max_seq_no_ CACHE_ALIGNED;
};

class ObLockWaitMgr: public share::ObThreadPool
{
public:
  friend class ObDeadLockChecker;
  friend class observer::ObAllVirtualLockWaitStat;
  friend class ObMockLockWaitMgr;

public:
  enum { LOCK_BUCKET_COUNT = 16384};
  static const int64_t OB_SESSPAIR_COUNT = 16;
  // used for remote wake up, prevent missing wake up for successor node
  static const int64_t WAIT_TIMEOUT_TS = 1000 * 1000; // 1s
  static const int64_t CHECK_TIMEOUT_INTERVAL = 100 * 1000; // 100ms
  typedef ObMemtableKey Key;
  typedef rpc::ObLockWaitNode Node;
  typedef FixedHash2<Node> Hash;
  typedef rpc::ObLockWaitNode::NODE_TYPE NodeType;
  struct SessPair {
    uint32_t sess_id_;
    TO_STRING_KV(K(sess_id_));
  };
  typedef ObSEArray<SessPair, OB_SESSPAIR_COUNT> KilledSessionArray;

public:
  ObLockWaitMgr();
  virtual ~ObLockWaitMgr();

  static int mtl_init(ObLockWaitMgr *&lock_wait_mgr) { return lock_wait_mgr->init(); }
  static bool is_need_wait_remote_lock()
  {
    const int64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
    return min_cluster_version >= CLUSTER_VERSION_4_4_2_0;
  }
  int init(bool for_unit_test = false);
  bool is_inited() { return is_inited_; };
  int start();
  void stop();
  void stop_check_timeout() { ATOMIC_STORE(&stop_check_timeout_, true); }
  void start_check_timeout() { ATOMIC_STORE(&stop_check_timeout_, false); }
  void destroy();
  void run1(); // periodicly check requests which may be killed or timeout
  void wait() { share::ObThreadPool::wait(); }
  // Preprocessing before handling the request, which primarily sets up the
  // local variables, thread_node and last_wait_hash.
  void setup(Node &node, int64_t recv_ts)
  {
    if (node.hash_ != 0) {
      node.last_touched_thread_id_ = GETTID();
      TRANS_LOG(TRACE, "LockWaitMgr setup", K(node), KP(node.next_), K(get_wait_node_cnt()));
    }
    node.reset_need_wait();
    if (node.recv_ts_ != 0 && node.recv_ts_ != recv_ts) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "ObRequest recv ts should not change",
        K(node), KP(node.next_), K(get_wait_node_cnt()));
    }
    if (node.recv_ts_ == 0) {
      node.recv_ts_ = recv_ts;
    }
    get_thread_node() = &node;
    get_thread_last_wait_hash_() = node.last_wait_hash_;
    get_thread_last_wait_addr_() = node.get_exec_addr();
    node.last_wait_hash_ = 0;
  }
  // clear the local variable, thread_node. NB: we should wakeup the request
  // based on the last_wait_hash, because the wait hash for the request may be changed
  static void clear_thread_node() {
    get_thread_last_wait_hash_() = 0;
    get_thread_last_wait_addr_().reset();
    get_thread_node() = nullptr;
  }
  // get thread local lock wait node
  static Node*& get_thread_node()
  {
    RLOCAL_INLINE(Node*, node);
    return node;
  }
  void set_tenant_id(const int64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_addr(const ObAddr& addr) { addr_ = addr; }
  void set_rpc(ObILockWaitMgrRpc *rpc) { rpc_ = rpc; }
  Hash* get_fixed_hash() { return hash_; }
  void set_fixed_hash(Hash *hash)
  {
    WaitQuiescent(get_qs());
    hash_ = hash;
  }
  ObQSync& get_qs()
  {
    static ObQSync qsync;
    return qsync;
  }
  int64_t get_wait_node_cnt() { return ATOMIC_LOAD(&total_wait_node_); }

  // When the request ends, the thread worker will check whether the retry is
  // needed. And if so, it will push the request into the lock_wait_mgr based on
  // whether request encounters a conflict and needs to retry
  bool post_process(bool need_retry, bool& need_wait);
  // setup the retry parameter on the request
  int post_lock(const int tmp_ret,
                const share::ObLSID &ls_id,
                const ObTabletID &tablet_id,
                const ObStoreRowkey &row_key,
                const int64_t timeout,
                const bool is_remote_sql,
                const int64_t last_compact_cnt,
                const int64_t total_trans_node_cnt,
                const transaction::ObTransID &tx_id,
                const SessionIDPair sess_id_pair,
                const transaction::ObTransID &holder_tx_id,
                const transaction::ObTxSEQ &conflict_tx_hold_seq,
                ObRowConflictInfo &cflict_info,
                ObFunction<int(bool&, bool&)> &rechecker);
  // setup the table lock retry parameter on the request
  int post_lock(const int tmp_ret,
                const share::ObLSID &ls_id,
                const ObTabletID &tablet_id,
                const transaction::tablelock::ObLockID &lock_id,
                const int64_t timeout,
                const bool is_remote_sql,
                const int64_t last_compact_cnt,
                const int64_t total_trans_node_cnt,
                const transaction::ObTransID &tx_id,
                const transaction::ObTransID &holder_tx_id,
                const transaction::tablelock::ObTableLockMode &lock_mode,
                ObRowConflictInfo &cflict_info,
                ObFunction<int(bool &need_wait)> &check_need_wait);
  // when removing the callbacks of uncommitted transaction, we need transfer
  // the conflict dependency from rows to transactions
  int transform_row_lock_to_tx_lock(const ObTabletID &tablet_id,
                                    const Key &key,
                                    const transaction::ObTransID &tx_id,
                                    const ObAddr &tx_scheduler);
  // wakeup the request waiting on the row
  void wakeup(const ObTabletID &tablet_id, const Key& key);
  // wakeup the request waiting on the transaction
  void wakeup(const transaction::ObTransID &tx_id);
  // wakeup the request waiting on the tablelock.
  void wakeup(const transaction::tablelock::ObLockID &lock_id);

  // for deadlock
  RowHolderMapper &get_row_holder() { return row_holder_mapper_; }
  DELEGATE_WITH_RET(row_holder_mapper_, insert_hash_holder, void);
  DELEGATE_WITH_RET(row_holder_mapper_, get_hash_holder, int);
  DELEGATE_WITH_RET(row_holder_mapper_, erase_hash_holder_record, void);
  int notify_killed_session(const uint32_t sess_id);

  // for all_virtual_lock_wait_stat
  Node* next(Node*& iter, Node* target);

  // for remote wait
  // destination observer hanlde source observer inform to enqueue lock wait node request
  int handle_inform_dst_enqueue_req(const ObLockWaitMgrDstEnqueueMsg &msg,
                                    ObLockWaitMgrRpcResult &result);
  // source observer handle inform destination observer lock_wait_node enqueue response
  int handle_dst_enqueue_resp(const ObLockWaitMgrDstEnqueueRespMsg &msg,
                              ObLockWaitMgrRpcResult &result);
  // source observer handle lock release request(used for remote node wake up)
  int handle_lock_release_req(const ObLockWaitMgrLockReleaseMsg &msg,
                              ObLockWaitMgrRpcResult &result);
  // destination observer handle remote wakeup request(used for last_wait_hash wakeup)
  int handle_wake_up_req(const ObLockWaitMgrWakeUpRemoteMsg &msg,
                         ObLockWaitMgrRpcResult &result);
  // for node state detection rpc
  int handle_batch_req(int msg_type, const char *buf, int32_t size);
  // destination observer handle periodic probe node state request
  int handle_check_node_state_req(const ObLockWaitMgrCheckNodeStateMsg &msg);
  // source observer handle periodic probe node state response
  int handle_check_node_state_resp(const ObLockWaitMgrCheckNodeStateRespMsg &msg);
  // handle rpc callback
  int handle_msg_cb(int status,
                    int16_t msg_type,
                    uint64_t hash,
                    rpc::NodeID node_id,
                    const common::ObAddr &receiver_addr);
  // save execution conflict info to lock wait mgr
  // used for local and remote lock conflict which do end_stmt local
  void on_lock_conflict(ObTxDesc &tx,
                        bool &is_lock_wait_timeout);
  // save remote execution conflict info to lock wait mgr
  // tx desc is NULL for Normal ac=1 remote task, i.e., end_stmt at remote
  // and should not be NULL in other cases
  void on_lock_conflict(ObSArray<storage::ObRowConflictInfo> &cflict_infos,
                        ObTxDesc *tx,
                        uint32_t session_id,
                        bool &is_lock_wait_timeout);
  // no lock conflict
  void on_no_lock_conflict();
  // obtain the first request waiting on the row or transaction
  Node* fetch_wait_head(uint64_t hash);
  // obtain the specified node waiting on the row or transaction
  Node* fetch_wait_node(uint64_t hash,
                        NodeID node_id,
                        const ObAddr &addr,
                        NodeType node_type = NodeType::MAX);
  // obtain the first fake placeholder node waiting on the row or transaction
  bool remove_remote_exec_side_node_type_placeholder(uint64_t hash);
  // obtain the first request waiting on the row or transaction and retry
  Node* fetch_and_repost_head(uint64_t hash);
  // obtain the specified node waiting on the row or transaction and retry
  Node* fetch_and_repost_node(uint64_t hash,
                              NodeID node_id,
                              const ObAddr &addr);
  // get wait queue head without detaching it
  Node* get_wait_head(uint64_t hash);
  // get wait node without detaching it
  bool check_node_exsit(uint64_t hash,
                      NodeID node_id,
                      const ObAddr &addr);
  // LS switch to follower, wakeup all the requests waiting on the LS
  void ls_switch_to_follower(const share::ObLSID &ls_id);
  // remove placeholder node from waiting queue
  void remove_placeholder_node(Node *node);
  void handle_lock_conflict(const sql::ObExecContext &exec_ctx,
                            int exec_errcode,
                            sql::ObSQLSessionInfo &session,
                            bool &is_lock_wait_timeout);
protected:
  // obtain the request waiting on the row or transaction
  Node* get_waiter(uint64_t hash,
                   NodeID node_id = 0,
                   const ObAddr &ctrl_side_addr = ObAddr(),
                   bool detach_node = true,
                   NodeType node_type = NodeType::MAX);
  // check whether there exits requests already timeoutt or need be
  // retried(session is killed, deadlocked or son on), and wakeup and retry them
  ObLink* check_timeout();
  // reclaim the chained reuqests
  void retire_node(ObLink*& tail, Node* node);
  // wakeup the request and put into the thread worker queue
  virtual int repost(Node* node);
  bool is_hash_empty()
  {
    bool is_empty = false;
    {
      CriticalGuard(get_qs());
      is_empty = hash_->is_empty();
    }
    return is_empty;
  }
  // used for fake-node memory leak detect
  int64_t get_not_free_remote_exec_side_node_cnt() const {
    return ATOMIC_LOAD(&not_free_remote_exec_side_node_cnt_);
  }
private:
  // generate unique node id for fake node and remote node
  NodeID generate_node_seq_();
  // check wait request session stat
  void check_wait_node_session_stat_(Node *iter,
                                     Node *&node2del,
                                     int64_t curr_ts,
                                     int64_t wait_timeout_ts);
  // check if waiting for the lock was successful
  // success indicates that the lock is still being held by someone.
  bool wait_(Node* node, Node*& delete_node, bool &is_placeholder);
  // wait when deadlock is enabled
  bool wait_with_deadlock_enabled_(Node *node);
  // handle after wait node
  void after_wait_(Node* node,
                  Node *&delete_node,
                  bool enqueue_succ,
                  bool &wait_succ,
                  bool &is_placeholder);
  // normal local wakeup
  void wakeup_(uint64_t hash);
  // used for last_wait_hash wakeup
  // do remote wakeup when addr is valid
  void wakeup_(const uint64_t hash,
               const ObAddr &addr);
  // do remote wakeup
  void remote_wakeup_(const uint64_t hash,
                      const ObAddr &addr);
  // do after remote ctrl side node enter wait queue
  int on_remote_ctrl_side_node_enqueue_(const Node &node);
  // for periodic detection
  int check_remote_node_state_(const Node &node);
  // lock release and send rpc to do remote wakeup
  int on_remote_lock_release_(const Node &node);
  // rpc callback handle
  int handle_dst_enqueue_msg_cb_(int status,
                                 uint64_t hash,
                                 rpc::NodeID node_id,
                                 const common::ObAddr &receiver_addr);
  int handle_lock_release_msg_cb_(int status,
                                  uint64_t hash,
                                  rpc::NodeID node_id,
                                  const common::ObAddr &receiver_addr);
  // handle node in post_process
  int handle_local_node_(Node* node, Node*& delete_node, bool &wait_succ);
  // remote wait, control side node try to wait lock and enqueue
  bool wait_remote_ctrl_side_node_(Node* remote_node, Node*& delete_node);
  // handle remote wait control side node
  int handle_remote_ctrl_side_node_(Node* remote_node, Node*& delete_node, bool &wait_succ);
  // handle remote wait execution side node
  int handle_remote_exec_side_node_(Node* remote_exec_side_node,
                        const ObString &query_sql,
                        const int64_t query_timeout_us,
                        Node*& delete_node,
                        bool &wait_succ,
                        bool &is_placeholder);
  // data seq has changed
  bool on_seq_change_(Node *node,
                     Node *&delete_node,
                     bool &enqueue_succ,
                     uint64_t seq);
  // seq has changed after insert node
  bool on_seq_change_after_insert_(Node *node,
                                  Node *&delete_node,
                                  bool &enqueue_succ);
  // to avoid miss wakeup, remote wakeup should add timeout to subsequent node
  void add_timeout_to_head_node_(const uint64_t hash, int64_t timeout_us);
  // during retry find hash still being held, should set queue's head node wait timeout
  void reset_head_node_wait_timeout_if_need(uint64_t hash, int64_t lock_ts);
  // remove node from waiting queue
  int remove_node_(Node *node, Node *&node_ret);
  // insert node into waiting queue
  int insert_node_(Node *node);
  // mtl malloc ObLockWaitNode
  Node* alloc_node_() {
    Node *node = static_cast<Node*>(mtl_malloc(sizeof(Node), "LockWaitNode"));
    if (OB_NOT_NULL(node)) {
      ATOMIC_INC(&not_free_remote_exec_side_node_cnt_);
    }
    return node;
  }
  // mtl free ObLockWaitNodr
  void free_node_(Node *node) {
    mtl_free(node);
    ATOMIC_DEC(&not_free_remote_exec_side_node_cnt_);
  }
  // hash that this thread waiting for before retry
  static uint64_t& get_thread_last_wait_hash_()
  {
    RLOCAL_INLINE(uint64_t, last_wait_hash);
    return last_wait_hash;
  }
  // last execution addr, match with last_wait_hash
  static ObAddr& get_thread_last_wait_addr_()
  {
    RLOCAL_INLINE(ObAddr, last_exec_addr);
    return last_exec_addr;
  }
  // check seq whether changed
  bool check_wakeup_seq_(uint64_t hash, int64_t lock_seq, int64_t &cur_seq)
  {
    cur_seq = ATOMIC_LOAD(&sequence_[(hash >> 1)% LOCK_BUCKET_COUNT]);
    return cur_seq == lock_seq;
  }
  // get hash slot seq
  int64_t get_seq_(uint64_t hash)
  {
    return ATOMIC_LOAD(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT]);
  }
  void inc_wait_node_cnt_() { ATOMIC_INC(&total_wait_node_); }
  void dec_wait_node_cnt_() { ATOMIC_DEC(&total_wait_node_); }

// helper function for deadlock
private:
  // register to the deadlock detector
  // for normal local execution
  int register_local_node_to_deadlock_(const transaction::ObTransID &self_tx_id,
                                       const transaction::ObTransID &blocked_tx_id,
                                       const Node * const node);
  // for remote execution
  // register "wait remote self relation" to deadlock detector
  int register_remote_node_to_deadlock_(const transaction::ObTransID &self_tx_id,
                                        const Node * const node);
  // for remote execution
  // register "fake node wait tx/row relation" to deadlock detector
  int register_remote_exec_side_node_to_deadlock_(const transaction::ObTransID &self_tx_id,
                                      const transaction::ObTransID &blocked_tx_id,
                                      const Node * const node,
                                      const ObString &query_sql,
                                      const int64_t query_timeout_us);
  bool is_killed_session_(KilledSessionArray *sessions,
                              const uint32_t sess_id);
  void fetch_killed_sessions_(KilledSessionArray *&sessions);
  inline int64_t calc_holder_tx_lock_timestamp(const int64_t holder_tx_start_time, const int64_t holder_data_seq_num);
  virtual void begin_row_lock_wait_event(const Node * const node);
  virtual void end_row_lock_wait_event(const Node * const node);
  void set_ash_rowlock_diag_info(const ObRowConflictInfo &cflict_info);
private:
  bool is_inited_;
  Hash hash_ref_;
  Hash *hash_;
  int64_t sequence_[LOCK_BUCKET_COUNT];
  char hash_buf_[sizeof(SpHashNode) * LOCK_BUCKET_COUNT];
  ObByteLock locks_[LOCK_BUCKET_COUNT];
  int64_t last_check_session_idle_ts_;
  int64_t tenant_id_;
  ObAddr addr_;
  ObLockWaitMgrRpc rpc_def_;
  ObILockWaitMgrRpc *rpc_;
  ObNodeSeqGenarator node_id_generator_;
  bool stop_check_timeout_;
  int64_t not_free_remote_exec_side_node_cnt_; // to avoid memory leak
  int64_t total_wait_node_;

  // for deadlock
  ObSpinLock killed_sessions_lock_;
  int32_t killed_sessions_index_;
  KilledSessionArray killed_sessions_[2];
  RowHolderMapper row_holder_mapper_;
};

class LockHashHelper {
private:
  static const uint64_t TRANS_FLAG = 1L << 63L;       // 10
  static const uint64_t TABLE_LOCK_FLAG = 1L << 62L;  // 01
  static const uint64_t ROW_FLAG = 0L;                // 00
  static const uint64_t HASH_MASK = ~(TRANS_FLAG | TABLE_LOCK_FLAG);
public:
  static inline
  uint64_t hash_rowkey(const ObTabletID &tablet_id, const memtable::ObMemtableKey &key)
  {
    uint64_t hash_id = tablet_id.hash();
    uint64_t hash_key = key.hash();
    uint64_t hash = murmurhash(&hash_key, sizeof(hash_key), hash_id);
    return ((hash & HASH_MASK) | ROW_FLAG) | 1;
  }

  static inline
  uint64_t hash_trans(const ObTransID &tx_id)
  {
    return ((murmurhash(&tx_id, sizeof(tx_id), 0) & HASH_MASK) | TRANS_FLAG) | 1;
  }

  static inline
  uint64_t hash_lock_id(const ObLockID &lock_id)
  {
    return ((lock_id.hash() & HASH_MASK) | TABLE_LOCK_FLAG) | 1;
  }

  static inline
  bool is_rowkey_hash(const uint64_t hash) { return (hash & ~HASH_MASK) == ROW_FLAG; }

  static inline
  bool is_trans_hash(const uint64_t hash) { return (hash & ~HASH_MASK) == TRANS_FLAG; }

  static inline
  bool is_table_lock_hash(const uint64_t hash) { return (hash & ~HASH_MASK) == TABLE_LOCK_FLAG; }
};

inline void advance_tlocal_request_lock_wait_stat(rpc::RequestLockWaitStat::RequestStat new_stat) {
  rpc::ObLockWaitNode *node = lockwaitmgr::ObLockWaitMgr::get_thread_node();
  if (OB_NOT_NULL(node)) {
    node->advance_stat(new_stat);
  }
}

}; // end namespace lockwaitmgr
}; // end namespace oceanbase

#endif /* OCEANBASE_MEMTABLE_OB_LOCK_WAIT_MGR_H_ */
