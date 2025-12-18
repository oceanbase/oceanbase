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

#include "ob_lock_wait_mgr.h"

#include "common/storage/ob_sequence.h"
#include "observer/ob_server.h"
#include "observer/ob_srv_network_frame.h"
#include "share/ob_srv_rpc_proxy.h"
#include "ob_lock_wait_mgr_msg.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/random/ob_random.h"
#include "storage/memtable/deadlock_adapter/ob_local_execution_deadlock_callback.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"

#define USING_LOG_PREFIX TRANS
namespace oceanbase
{
namespace obmysql
{

// replace function in library
void request_finish_callback()
{
  bool unused = false;
  lockwaitmgr::ObLockWaitMgr *lock_wait_mgr = MTL(lockwaitmgr::ObLockWaitMgr*);
  if (OB_ISNULL(lock_wait_mgr)) {
    TRANS_LOG(TRACE, "MTL(lock wait mgr) is null", K(MTL_ID()));
  } else {
    lock_wait_mgr->post_process(false, unused);
  }
  lockwaitmgr::ObLockWaitMgr::clear_thread_node();
}

}
}
namespace oceanbase
{

#define LOG_LEVEL_FOR_LWM TRACE

using namespace storage;
using namespace common::hash;
using namespace common;
using namespace share;
using namespace transaction::tablelock;
using namespace detector;
using namespace obrpc;

namespace lockwaitmgr
{

ObLockWaitMgr::ObLockWaitMgr()
    : is_inited_(false),
      hash_ref_(hash_buf_, sizeof(hash_buf_)),
      tenant_id_(OB_INVALID_TENANT_ID),
      rpc_def_(),
      rpc_(&rpc_def_),
      stop_check_timeout_(false),
      not_free_remote_exec_side_node_cnt_(0),
      total_wait_node_(0),
      killed_sessions_lock_(common::ObLatchIds::DEADLOCK_DETECT_LOCK),
      killed_sessions_index_(0)
{
  memset(sequence_, 0, sizeof(sequence_));
  hash_ = &hash_ref_;
}

ObLockWaitMgr::~ObLockWaitMgr() {}

int ObLockWaitMgr::init(bool for_unit_test)
{
  int ret = OB_SUCCESS;
  const ObAddr &self = GCTX.self_addr();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "ob lock wait mgr already init", K(ret), K(is_inited_));
  } else if (OB_FAIL(row_holder_mapper_.init())) {
    TRANS_LOG(WARN, "fail to init row_holder_mapper_", K(ret), K(is_inited_));
  } else if (OB_FAIL(rpc_->init(this))) {
    TRANS_LOG(WARN, "init rpc error", KR(ret));
  } else {
    share::ObThreadPool::set_run_wrapper(MTL_CTX());
    last_check_session_idle_ts_ = ObClockGenerator::getClock();
    total_wait_node_ = 0;
    is_inited_ = true;
    tenant_id_ = MTL_ID();
    addr_ = self;
    TRANS_LOG(INFO, "LockWaitMgr.init", K(ret));
  }
  return ret;
}

int ObLockWaitMgr::start() {
  int ret = share::ObThreadPool::start();
  TRANS_LOG(INFO, "LockWaitMgr.start", K(ret));
  return ret;
}

void ObLockWaitMgr::stop() {
  WaitQuiescent(get_qs());
  share::ObThreadPool::stop();
  if (get_not_free_remote_exec_side_node_cnt() != 0) {
    TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "remote execution side lock wait node memory leak");
  }
  TRANS_LOG(INFO, "LockWaitMgr.stop");
}

void ObLockWaitMgr::destroy() {
  is_inited_ = false;
  total_wait_node_ = 0;
  killed_sessions_index_ = 0;
  row_holder_mapper_.~RowHolderMapper();
}

void ObLockWaitMgr::run1()
{
  lib::set_thread_name("LockWaitMgr");
  int64_t last_dump_ts = 0;
  int64_t now = 0;
  bool last_time_check_deadlock_enabled = false;
  bool this_time_check_deadlock_enabled = true;
  while(!has_set_stop() || !is_hash_empty()) {
    bool stop_check_timeout = ATOMIC_LOAD(&stop_check_timeout_);
    if (!stop_check_timeout) {
      ObLink* iter = check_timeout();
      while (NULL != iter) {
        Node* cur = CONTAINER_OF(iter, Node, retire_link_);
        iter = iter->next_;
        (void)repost(cur);
      }
      // dump debug info, and check deadlock enabdle, clear mapper if deadlock is disabled
      now = ObClockGenerator::getClock();
      if (now - last_dump_ts > 5_s) {
        last_dump_ts = now;
        row_holder_mapper_.periodic_tasks();
        this_time_check_deadlock_enabled = ObDeadLockDetectorMgr::is_deadlock_enabled();
      if (last_time_check_deadlock_enabled && !this_time_check_deadlock_enabled) {// switch to close
          row_holder_mapper_.clear();// clear all holder info
        }
        last_time_check_deadlock_enabled = this_time_check_deadlock_enabled;
      }
    } else {
      TRANS_LOG(INFO, "stop check_timeout");
    }
    ob_usleep(CHECK_TIMEOUT_INTERVAL, true/*is_idle_sleep*/);
  }
}

void ObLockWaitMgr::remove_placeholder_node(Node *node)
{
  int err = 0;
  bool is_placeholder = ATOMIC_LOAD(&node->is_placeholder_);
  Node *tmp_node = NULL;
  if (OB_UNLIKELY(is_placeholder && node->get_node_type() == Node::LOCAL)) {
    CriticalGuard(get_qs());
    err = remove_node_(node, tmp_node);
    if (err != 0) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "remove placeholder unexpected error",
        K(err), KPC(node), KP(tmp_node));
    }
    TRANS_LOG(INFO, "remove placeholder", K(err), KPC(node), KP(tmp_node));
  }
  if (NULL != tmp_node) {
    WaitQuiescent(get_qs());
    ATOMIC_STORE(&node->is_placeholder_, false);
  }
}

void ObLockWaitMgr::handle_lock_conflict(const ObExecContext &exec_ctx,
                                         int exec_errcode,
                                         sql::ObSQLSessionInfo &session,
                                         bool &is_lock_wait_timeout)
{
  if (OB_TRY_LOCK_ROW_CONFLICT == exec_errcode) {
    // save conflict infos to scheduler lock wait node
    if (exec_ctx.get_trans_state().is_start_stmt_executed()) {
      // have tx desc in session
      ObTxDesc *tx = NULL;
      if (OB_ISNULL(tx = session.get_tx_desc())) {
        TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "tx desc should not be NULL");
      } else {
        on_lock_conflict(*tx, is_lock_wait_timeout);
      }
    } else {
      transaction::ObTxExecResult &result = session.get_trans_result();
      on_lock_conflict(const_cast<ObSArray<storage::ObRowConflictInfo>&>(result.get_conflict_info_array()),
         NULL, session.get_server_sid(), is_lock_wait_timeout);
    }
    if (session.is_real_inner_session()) {
      rpc::ObLockWaitNode* node = NULL;
      if ((node = get_thread_node()) != NULL) {
        node->reset_need_wait();
      }
    }
  } else {
    TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "exec errcode should be OB_TRY_LOCK_ROW_CONFLICT");
  }
}

int ObLockWaitMgr::remove_node_(Node *node, Node *&node_ret) {
  int err = 0;
  int times = 0;
  int ret = OB_SUCCESS;
  while(-EAGAIN == (err = hash_->del(node, node_ret))) ;
  if (err == 0) {
    node->last_touched_thread_id_ = GETTID();
    dec_wait_node_cnt_();
  } else {
    TRANS_LOG(LOG_LEVEL_FOR_LWM, "LockWaitMgr remove node end", KPC(node), KP(node->next_), K(get_wait_node_cnt()), KP(node_ret), K(err));
    node_ret = NULL;
  }
  return err;
}

int ObLockWaitMgr::insert_node_(Node *node) {
  int err = 0;
  int times = 0;
  int ret = OB_SUCCESS;
  while(-EAGAIN == (err = hash_->insert(node))) ;
  if (err == 0) {
    node->last_touched_thread_id_ = GETTID();
    inc_wait_node_cnt_();
  } else {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "insert node fail unexpected error happened",
      K(err), KPC(node));
  }
  TRANS_LOG(LOG_LEVEL_FOR_LWM, "LockWaitMgr insert node end", KPC(node), KP(node->next_), K(get_wait_node_cnt()), K(err));
  return err;
}

// copy from src to dest, return copy length
int copy_str(const char *src,
             const int64_t src_len,
             char* dest,
             const int64_t dest_len)
{
  int64_t src_idx = 0;
  int64_t dest_idx = 0;
  if (dest_len > 0 && src && dest) {
    // remain 1 byte for '\0'
    while (src_idx < src_len && dest_idx < dest_len - 1) {
      if (src[src_idx] == '\0') {
        break;
      }
      dest[dest_idx++] = src[src_idx++];
    }
    dest[dest_idx] = '\0';
  }
  return dest_idx;
}

ERRSIM_POINT_DEF(ERRSIM_LWM_DST_ENQUEUE_RPC_LOSE);
int ObLockWaitMgr::on_remote_ctrl_side_node_enqueue_(const Node &node)
{
  static const int64_t QUERY_TIMEOUT_TS = 10 * 1000 * 1000; // just for test
  #define PRINT_WRAPPER K(ret), K(msg), K(node)
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // send rpc to dst to inform the source node enqueue result
  ObLockWaitMgrDstEnqueueMsg msg;
  int64_t query_timeout = QUERY_TIMEOUT_TS; // used for set deadlock detector timeout ts
  ObString query_str = "default query sql";
  SessionGuard sess_guard;
  if (OB_TMP_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(SessionIDPair(node.sessid_, node.assoc_sess_id_),
                                                                             sess_guard))) {
    TRANS_LOG(WARN, "get session info failed", PRINT_WRAPPER);
  } else {
    const ObString &cur_query_str = sess_guard->get_current_query_string();
    sess_guard->get_query_timeout(query_timeout);
    int copy_size = copy_str(cur_query_str.ptr(),
                             cur_query_str.length(),
                             msg.buffer_for_serialization_,
                             sizeof(msg.buffer_for_serialization_));
    query_str.assign(msg.buffer_for_serialization_, copy_size);
    TRANS_LOG(TRACE, "copy query sql", PRINT_WRAPPER);
  }
  if (OB_TMP_FAIL(tmp_ret)
   && OB_UNLIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled())) {
    // when deadlock is enabled and got session info fail
    // no need to wait for remote node
    // overwrite ret
    ret = tmp_ret;
  } else if (FALSE_IT(msg.init(tenant_id_, addr_, node.hash(), node.get_lock_seq(),
                               node.get_lock_ts(), node.get_node_id(), node.get_tx_id(),
                               node.get_holder_tx_id(), node.get_sess_id(), query_timeout,
                               node.get_recv_ts(), node.get_ls_id(), node.get_tablet_id(),
                               node.get_holder_tx_hold_seq_value(), node.get_tx_active_ts(),
                               node.get_lock_wait_expire_ts(), query_str))) {
  } else if (OB_FAIL(ERRSIM_LWM_DST_ENQUEUE_RPC_LOSE)) {
    TRANS_LOG(WARN, "LockWaitMgr errsim inform remote node enqueue rpc lose", PRINT_WRAPPER);
    ret = OB_SUCCESS;
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "remote node is invalid", PRINT_WRAPPER);
  } else if (OB_FAIL(rpc_->post_msg(node.get_exec_addr(), msg))) {
    TRANS_LOG(WARN, "post inform dst node enqueue msg fail", PRINT_WRAPPER);
  }
  TRANS_LOG(TRACE, "post remote node enqueue result finish", PRINT_WRAPPER);
  return ret;
  #undef PRINT_WRAPPER
}

ERRSIM_POINT_DEF(ERRSIM_LWM_NOT_CHECK_NODE_STATE);
int ObLockWaitMgr::check_remote_node_state_(const Node &node)
{
  #define PRINT_WRAPPER K(ret), K(msg), K(node)
  int ret = OB_SUCCESS;
  ObLockWaitMgrCheckNodeStateMsg msg;
  msg.init(tenant_id_, node.hash(), node.get_node_id(), addr_);
  if (OB_FAIL(ERRSIM_LWM_NOT_CHECK_NODE_STATE)) {
    TRANS_LOG(WARN, "LockWaitMgr errsim not check remote node state", PRINT_WRAPPER);
    ret = OB_SUCCESS;
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "remote node is invalid", PRINT_WRAPPER);
  } else if (OB_FAIL(rpc_->post_msg(node.get_exec_addr(), msg))) {
    TRANS_LOG(WARN, "post check remomote node state msg fail", PRINT_WRAPPER);
  }
  TRANS_LOG(TRACE, "post check remote node state finish", PRINT_WRAPPER);
  return ret;
  #undef PRINT_WRAPPER
}

int ObLockWaitMgr::handle_batch_req(int msg_type,
                                    const char *buf,
                                    int size)
{
  int ret = OB_SUCCESS;
  #define PRINT_WRAPPER K(ret), K(msg_type), K(size)
  #define CASE(msg_type, msg_class, msg_handler)                                      \
    case msg_type:                                                                    \
  {                                                                                   \
    int64_t pos = 0;                                                                  \
    msg_class msg;                                                                    \
    if (OB_FAIL(msg.deserialize(buf, size, pos))) {                                   \
      TRANS_LOG(WARN, "deserialize msg failed", PRINT_WRAPPER);                       \
    } else if (!msg.is_valid()) {                                                     \
      ret = OB_INVALID_ARGUMENT;                                                      \
      TRANS_LOG(ERROR, "msg is invalid", PRINT_WRAPPER);                              \
    } else if(msg.get_tenant_id() != MTL_ID()) {                                      \
      ret = OB_ERR_UNEXPECTED;                                                        \
      TRANS_LOG(ERROR, "tenant is not match", PRINT_WRAPPER);                         \
    } else if (msg_handler(msg)) {                                                    \
      TRANS_LOG(WARN, "handle lock wait mgr msg fail", PRINT_WRAPPER);                \
    }                                                                                 \
    break;                                                                            \
  }

  switch (msg_type) {
    CASE(LWM_CHECK_NODE_STATE, ObLockWaitMgrCheckNodeStateMsg, handle_check_node_state_req)
    CASE(LWM_CHECK_NODE_STATE_RESP, ObLockWaitMgrCheckNodeStateRespMsg, handle_check_node_state_resp)
    default: {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(WARN, "batch rpc not supported", PRINT_WRAPPER);
      break;
    }
  }
  TRANS_LOG(TRACE, "handle batch rpc finish", PRINT_WRAPPER);
  return ret;
  #undef CASE
  #undef PRINT_WRAPPER
}

int ObLockWaitMgr::handle_inform_dst_enqueue_req(const ObLockWaitMgrDstEnqueueMsg &msg,
                                                 ObLockWaitMgrRpcResult &result)
{
  #define PRINT_WRAPPER K(ret), K(remote_exec_side_node), K(msg), K(resp_msg), K(is_placeholder)
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  Node *delete_node = NULL;
  Node *remote_exec_side_node = NULL;
  Node *ptr = NULL;
  ObLockWaitMgrDstEnqueueRespMsg resp_msg;
  Node *last_node = fetch_wait_node(msg.get_hash(), msg.get_node_id(), msg.get_sender_addr(), rpc::ObLockWaitNode::REMOTE_EXEC_SIDE);
  bool is_placeholder = false;
  if (OB_UNLIKELY(last_node != NULL)) {
    ptr = last_node;
    ATOMIC_STORE(&last_node->is_placeholder_, false);
  } else {
    ptr = alloc_node_();
  }
  if (OB_ISNULL(ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "ob_malloc remote execution side node fail", K(ret), K(msg));
  } else {
    ObByteLockGuard lock_guard(locks_[(LockHashHelper::hash_trans(msg.get_tx_id()) >> 1) % LOCK_BUCKET_COUNT]);
    remote_exec_side_node = new(ptr) Node();
    remote_exec_side_node->node_type_ =  rpc::ObLockWaitNode::REMOTE_EXEC_SIDE;
    remote_exec_side_node->lock_ts_ =  msg.get_lock_ts();
    remote_exec_side_node->addr_ = ptr;
    remote_exec_side_node->node_id_ = msg.get_node_id();
    remote_exec_side_node->hash_ = msg.get_hash();
    remote_exec_side_node->lock_seq_ = msg.get_lock_seq();
    remote_exec_side_node->ctrl_addr_ = msg.get_sender_addr();
    remote_exec_side_node->tx_id_ = msg.get_tx_id();
    remote_exec_side_node->holder_tx_id_ = msg.get_holder_tx_id();
    remote_exec_side_node->recv_ts_ = msg.get_recv_ts();
    remote_exec_side_node->sessid_ = msg.get_sess_id();
    remote_exec_side_node->need_wait_ = true;
    remote_exec_side_node->ls_id_ = msg.get_ls_id();
    remote_exec_side_node->holder_tx_hold_seq_value_ = msg.get_holder_tx_hold_seq();
    remote_exec_side_node->tx_active_ts_ = msg.get_tx_active_ts();
    remote_exec_side_node->lock_wait_expire_ts_ = msg.get_abs_timeout_ts();

    resp_msg.init(tenant_id_, remote_exec_side_node->hash_, remote_exec_side_node->get_node_id(), addr_);
    if (OB_TMP_FAIL(handle_remote_exec_side_node_(remote_exec_side_node,
                                                  msg.get_query_sql(),
                                                  msg.get_query_timeout(),
                                                  delete_node,
                                                  resp_msg.get_enqueue_succ(),
                                                  is_placeholder))) {
      TRANS_LOG(WARN, "handle remote execution side node fail", PRINT_WRAPPER, KP(remote_exec_side_node));
    }
  }
  if (NULL != delete_node) {
    WaitQuiescent(get_qs());
  }
  if (OB_NOT_NULL(ptr) && !resp_msg.get_enqueue_succ()) {
    if (!is_placeholder) {
      TRANS_LOG(TRACE, "REMOTE_EXEC_SIDE node enqueue fail", PRINT_WRAPPER);
      mtl_free(remote_exec_side_node);
      ATOMIC_DEC(&not_free_remote_exec_side_node_cnt_);
    }
  }
  if (OB_NOT_NULL(ptr) && OB_FAIL(rpc_->post_msg(msg.get_sender_addr(), resp_msg))) {
    TRANS_LOG(WARN, "post inform dst node enqueue fail", PRINT_WRAPPER);
  }
  result.init(OB_SUCCESS);
  TRANS_LOG(TRACE, "LockWaitMgr hanlde inform dst enqueue request finish", PRINT_WRAPPER);
  return ret;
  #undef PRINT_WRAPPER
}


// obtain the first request waiting on the row or transaction
ObLockWaitMgr::Node* ObLockWaitMgr::fetch_wait_head(uint64_t hash)
{
  ObAddr invalid_addr;
  Node *node = get_waiter(hash, 0, invalid_addr, true);
  return node;
}

// obtain the specified node waiting on the row or transaction
ObLockWaitMgr::Node* ObLockWaitMgr::fetch_wait_node(uint64_t hash,
                                                    NodeID node_id,
                                                    const ObAddr &addr,
                                                    NodeType node_type)
{
  Node *node = get_waiter(hash, node_id, addr, true, node_type);
  return node;
}

// obtain the first remote execution side placeholder node waiting on the row or transaction
bool ObLockWaitMgr::remove_remote_exec_side_node_type_placeholder(uint64_t hash)
{
  bool remove_succ = false;
  Node *remove_ret = NULL;
  int err = 0;
  {
    CriticalGuard(get_qs());
    Node *node = get_wait_head(hash);
    if (node != NULL
     && (ATOMIC_LOAD(&node->is_placeholder_) == true)
     && node->get_node_type() == Node::REMOTE_EXEC_SIDE) {
     err = remove_node_(node, remove_ret);
    }
  }
  TRANS_LOG(TRACE, "LockWaitMgr remove fake type placeholder", K(hash), KP(remove_ret));
  if (0 == err && NULL != remove_ret) {
    WaitQuiescent(get_qs());
    remove_succ = true;
    mtl_free(remove_ret);
  }
  return remove_succ;
}

ObLockWaitMgr::Node* ObLockWaitMgr::fetch_and_repost_head(uint64_t hash)
{
  // remove remote execution side node type placeholder
  bool remove_succ = true;
  while (remove_succ) {
    remove_succ = remove_remote_exec_side_node_type_placeholder(hash);
  }

  ATOMIC_INC(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT]);
  if (nullptr != get_thread_node()) {
    if (((get_thread_node()->hash() >> 1) % LOCK_BUCKET_COUNT) == ((hash >> 1) % LOCK_BUCKET_COUNT)) {
      ++get_thread_node()->lock_seq_;
    }
  }
  Node* node = fetch_wait_head(hash);
  if (NULL != node) {
    EVENT_INC(MEMSTORE_WRITE_LOCK_WAKENUP_COUNT);
    EVENT_ADD(MEMSTORE_WAIT_WRITE_LOCK_TIME, ObTimeUtility::current_time() - node->lock_ts_);
    node->on_retry_lock(hash);
    (void)repost(node);
  }
  TRANS_LOG(TRACE, "LockWaitMgr fetch and repost head waiter finish", K(hash), KP(node));
  return node;
}

ObLockWaitMgr::Node* ObLockWaitMgr::fetch_and_repost_node(uint64_t hash,
                                                          NodeID node_id,
                                                          const ObAddr &addr)
{
  Node* node = fetch_wait_node(hash, node_id, addr);
  if (NULL != node) {
    EVENT_INC(MEMSTORE_WRITE_LOCK_WAKENUP_COUNT);
    EVENT_ADD(MEMSTORE_WAIT_WRITE_LOCK_TIME, ObTimeUtility::current_time() - node->lock_ts_);
    node->on_retry_lock(hash);
    (void)repost(node);
  }
  TRANS_LOG(TRACE, "LockWaitMgr fetch and repost node finish", K(hash), K(node_id), K(addr), KP(node));
  return node;
}

// get wait queue head without detaching it
ObLockWaitMgr::Node* ObLockWaitMgr::get_wait_head(uint64_t hash)
{
  Node* node = NULL;
  ObAddr invalid_addr;
  node = get_waiter(hash, 0, invalid_addr, false);
  return node;
}

// get wait node without detaching it
bool ObLockWaitMgr::check_node_exsit(uint64_t hash,
                                     NodeID node_id,
                                     const ObAddr &addr)
{
  return get_waiter(hash, node_id, addr, false) != NULL;
}

ERRSIM_POINT_DEF(ERRSIM_LWM_DST_ENQUEUE_RESP_RPC_LOSE);
int ObLockWaitMgr::handle_dst_enqueue_resp(const ObLockWaitMgrDstEnqueueRespMsg &msg,
                                           ObLockWaitMgrRpcResult &result)
{
  int ret = OB_SUCCESS;
  bool enqueue_succ = msg.get_enenqueue_succ();
  uint64_t hash = msg.get_hash();
  NodeID node_id = msg.get_node_id();
  ObAddr addr = msg.get_sender_addr();
  Node *node = NULL;
  if ((ERRSIM_LWM_DST_ENQUEUE_RESP_RPC_LOSE)) {
    TRANS_LOG(WARN, "LockWaitMgr errsim dst enqueue response rpc lose", K(ret), K(msg));
    ret = OB_SUCCESS;
  } else if (!enqueue_succ) {
    // destination enqueue fail, retry request
    node = fetch_and_repost_node(hash, node_id, addr);
  }
  result.init(OB_SUCCESS);
  TRANS_LOG(TRACE, "LockWaitMgr hanlde inform dst enqueue response finish", K(ret), K(msg), KP(node));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_LWM_LOCK_RELEASE_RPC_LOSE);
int ObLockWaitMgr::handle_lock_release_req(const ObLockWaitMgrLockReleaseMsg &msg,
                                           ObLockWaitMgrRpcResult &result)
{
  int ret = OB_SUCCESS;
  static int times = 0;
  uint64_t hash = msg.get_hash();
  NodeID node_id = msg.get_node_id();
  ObAddr addr = msg.get_sender_addr();
  if (OB_FAIL(ERRSIM_LWM_LOCK_RELEASE_RPC_LOSE)) {
    TRANS_LOG(WARN, "LockWaitMgr errsim lock release rpc lose", K(msg));
    ret = OB_SUCCESS;
  } else {
    (void)fetch_and_repost_node(hash, node_id, addr);
    result.init(OB_SUCCESS);
  }
  TRANS_LOG(TRACE, "LockWaitMgr hanlde lock release req finish", K(msg));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_LWM_WAKEUP_RPC_LOSE);
int ObLockWaitMgr::handle_wake_up_req(const ObLockWaitMgrWakeUpRemoteMsg &msg,
                                      ObLockWaitMgrRpcResult &result)
{
  int ret = OB_SUCCESS;
  uint64_t hash = msg.get_hash();
  if (OB_FAIL(ERRSIM_LWM_WAKEUP_RPC_LOSE)) {
    TRANS_LOG(WARN, "LockWaitMgr errsim wakeup rpc lose", K(msg));
    ret  = OB_SUCCESS;
  } else {
    (void)fetch_and_repost_head(hash);
    result.init(OB_SUCCESS);
  }
  TRANS_LOG(TRACE, "LockWaitMgr hanlde remote wakeup req finish", K(msg));
  return ret;
}

int ObLockWaitMgr::handle_check_node_state_req(const ObLockWaitMgrCheckNodeStateMsg &msg)
{
  int ret = OB_SUCCESS;
  uint64_t hash = msg.get_hash();
  NodeID node_id = msg.get_node_id();
  ObAddr sender_addr = msg.get_sender_addr();
  ObLockWaitMgrCheckNodeStateRespMsg resp_msg;
  resp_msg.set_is_exsit(check_node_exsit(hash, node_id, sender_addr));
  resp_msg.init(tenant_id_, hash, node_id, addr_);
  if (OB_FAIL(rpc_->post_msg(msg.get_sender_addr(), resp_msg))) {
    TRANS_LOG(WARN, "post check node state response msg fail", K(ret), K(msg), K(resp_msg));
  }
  TRANS_LOG(TRACE, "LockWaitMgr hanlde check node state finish", K(ret), K(msg), K(resp_msg));
  return ret;
}

int ObLockWaitMgr::handle_check_node_state_resp(const ObLockWaitMgrCheckNodeStateRespMsg &msg)
{
  int ret = OB_SUCCESS;
  uint64_t hash = msg.get_hash();
  NodeID node_id = msg.get_node_id();
  ObAddr addr = msg.get_sender_addr();
  bool is_exsit = msg.get_is_exsit();
  if (!is_exsit) {
    // node doesn't exsit in remote observer, retry request
    (void)fetch_and_repost_node(hash, node_id, addr);
  }
  TRANS_LOG(TRACE, "LockWaitMgr hanlde check node state response finish", K(ret), K(msg), K(addr));
  return ret;
}

NodeID ObLockWaitMgr::generate_node_seq_()
{
  int64_t node_id = node_id_generator_.generate_node_seq();
  return node_id;
}

bool ObLockWaitMgr::post_process(bool need_retry, bool& need_wait)
{
  need_wait = false;
  bool wait_succ = false; // whether wait lock in queue success
  Node* node = get_thread_node();
  int ret = OB_SUCCESS;
  if (node != nullptr) {
    remove_placeholder_node(node);
    uint64_t &last_wait_hash = get_thread_last_wait_hash_();
    need_wait = false;
    if (last_wait_hash != 0) {
      ObAddr &addr = get_thread_last_wait_addr_();
      wakeup_(last_wait_hash, addr);
    }
    if (need_retry && node->need_wait()) {
      need_wait = true;
      Node *delete_node = NULL;
      bool support_remote_wait = false;
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));

      if (OB_LIKELY(tenant_config.is_valid())) {
        if (tenant_config->_enable_wait_remote_lock) {
          if (!is_need_wait_remote_lock()) {
            if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
              TRANS_LOG(TRACE, "observer not upgrade to version that support wait remote lock");
            }
          } else {
            support_remote_wait = true;
          }
        } else {
          support_remote_wait = false;
        }
      }
      if (node->get_node_type() == Node::LOCAL) {
        // local lock conflict
        if (OB_FAIL(handle_local_node_(node, delete_node, wait_succ))) {
          TRANS_LOG(WARN, "hanlde local node fail", KP(node));
        }
      } else if (node->get_node_type() == Node::REMOTE_CTRL_SIDE) {
        // remote execution lock conflict
        if (!support_remote_wait) {
          wait_succ = false;
          need_wait = false;
        } else if (OB_FAIL(handle_remote_ctrl_side_node_(node, delete_node, wait_succ))) {
          TRANS_LOG(WARN, "hanlde remote node fail", KP(node));
        }
      } else {
        wait_succ = false;
        need_wait = false;
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "not support node type", KPC(node));
      }
      TRANS_LOG(LOG_LEVEL_FOR_LWM, "LockWaitMgr need retry request post process", KP(node), K(need_retry), K(wait_succ), K(get_wait_node_cnt()));
      if (NULL != delete_node) {
        WaitQuiescent(get_qs());
      }
    }
    // CAUTION: if need_retry is false, node may be not NULL, but can't be accessed.
    // case IO thread may already free it.
  }
  return wait_succ;
}

int ObLockWaitMgr::handle_msg_cb(int status,
                                 int16_t msg_type,
                                 uint64_t hash,
                                 rpc::NodeID node_id,
                                 const common::ObAddr &receiver_addr)
{
  #define PRINT_WRAPPER K(ret), K(status), K(msg_type), K(hash), K(node_id), K(receiver_addr)
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "lock wait mgr not inited", K(ret));
  } else if (!ObLWMMsgTypeChecker::is_valid_msg_type(msg_type)
         || !receiver_addr.is_valid()
         || hash <= 0
         || node_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", PRINT_WRAPPER);
  } else if (OB_SUCCESS != status) {
    TRANS_LOG(WARN, "there is an error in msg callback", PRINT_WRAPPER);
  }
  TRANS_LOG(TRACE, "handle msg callback finish", PRINT_WRAPPER);
  return ret;
  #undef PRINT_WRAPPER
}

// for local and remote execution, save the brought back conflict information to lock wait node
void ObLockWaitMgr::on_lock_conflict(ObTxDesc &tx,
                                    bool &is_lock_wait_timeout) {
  // add remote conflict info
  rpc::ObLockWaitNode* node = NULL;
  is_lock_wait_timeout = false;
  if (tx.has_conflict_infos()) {
    ObSArray<ObRowConflictInfo> &cflict_infos = tx.get_conflict_info_array();
    on_lock_conflict(cflict_infos, &tx, tx.get_assoc_session_id(), is_lock_wait_timeout);
    tx.reset_conflict_info_array();
  }
  TRANS_LOG(TRACE, "end stmt handle lock conflict end", K(tx));
}

// save the brought back conflict information to lock wait node
// direct call only for ac=1 remote execution, meanwhile runner_addr is valid
void ObLockWaitMgr::on_lock_conflict(ObSArray<ObRowConflictInfo> &cflict_infos,
                                     ObTxDesc *tx,
                                     uint32_t session_id,
                                     bool &is_lock_wait_timeout)
{
  // add remote conflict info
  rpc::ObLockWaitNode* node = NULL;
  is_lock_wait_timeout = false;
  if ((node = get_thread_node()) == NULL) {
    // do nothing
    TRANS_LOG_RET(TRACE, OB_SUCCESS, "thread node is NULL", K(cflict_infos));
  } else if (cflict_infos.count() == 0) {
    // do nothing
    TRANS_LOG_RET(TRACE, OB_SUCCESS, "cflict infos is empty", K(cflict_infos));
  } else {
    uint64_t &last_wait_hash = get_thread_last_wait_hash_();
    ObAddr &key_addr = get_thread_last_wait_addr_();
    int64_t curr_ts = common::ObTimeUtil::current_time();

    int64_t wait_cflict_idx = 0;
    bool last_wait_hash_still_cflict = false;
    ARRAY_FOREACH_NORET(cflict_infos, i) {
      if (cflict_infos.at(i).conflict_hash_ == node->hash()) {
        if (!key_addr.is_valid() && (cflict_infos.at(i).conflict_happened_addr_ == addr_)) {
          // still local conflict
          last_wait_hash = 0;
          key_addr.reset();
        } else if (key_addr.is_valid() && cflict_infos.at(i).conflict_happened_addr_ == key_addr) {
          // still conflict, otherwise should send rpc to wake up before retry request
          last_wait_hash = 0;
          key_addr.reset();
        }
        wait_cflict_idx = i;
        last_wait_hash_still_cflict = true;
      }
      if (curr_ts >= cflict_infos.at(i).abs_timeout_) {
        is_lock_wait_timeout = true;
      }
    }

    // choose local conflict or first conflict info to save
    ObRowConflictInfo &cflict_info = cflict_infos.at(wait_cflict_idx);
    Node::NODE_TYPE node_type = Node::NODE_TYPE::LOCAL;
    if (cflict_info.conflict_happened_addr_ != addr_) {
      // remote type node for remote conflict
      node_type = Node::NODE_TYPE::REMOTE_CTRL_SIDE;
    }
    // remove operation should precede the set conflict info operation
    remove_placeholder_node(node);

    if (false == last_wait_hash_still_cflict) {
      // last wait hash not conflict anymore, reset lock_wait_expire_ts
      node->set_lock_wait_expire_ts(cflict_info.abs_timeout_);
    }

    if (curr_ts >= node->get_lock_wait_expire_ts()) {
      // check lock wait timeout
      is_lock_wait_timeout = true;
    }

    if (is_lock_wait_timeout) {
      TRANS_LOG_RET(WARN, OB_ERR_EXCLUSIVE_LOCK_CONFLICT, "lock wait timeout",
        K(ret), K(cflict_infos), KPC(node), K(curr_ts));
    } else {
      ObCStringHelper helper;
      node->set((void*)node,
                cflict_info.conflict_hash_,
                cflict_info.lock_seq_,
                cflict_info.conflict_ls_.id(),
                cflict_info.conflict_tablet_.id(),
                cflict_info.last_compact_cnt_,
                cflict_info.total_update_cnt_,
                helper.convert(cflict_info.conflict_row_key_str_),
                cflict_info.conflict_row_key_str_.get_ob_string().length(),
                cflict_info.client_session_id_,
                cflict_info.holder_sess_id_,
                cflict_info.self_tx_id_,
                cflict_info.conflict_tx_id_,
                cflict_info.conflict_tx_hold_seq_.get_seq(),
                tx != NULL ? tx->get_active_ts() : 0,
                session_id,
                node_type,
                cflict_info.conflict_happened_addr_);
      set_ash_rowlock_diag_info(cflict_info);
      GET_DIAGNOSTIC_INFO->get_ash_stat().begin_retry_wait_event(
                ObWaitEventIds::ROW_LOCK_WAIT,
                cflict_info.conflict_tx_id_,
                cflict_info.conflict_tx_hold_seq_.get_seq(),
                calc_holder_tx_lock_timestamp(cflict_info.holder_tx_start_time_, cflict_info.conflict_tx_hold_seq_.get_seq()));
      TRANS_LOG_RET(INFO, OB_SUCCESS, "handle lock conflict end",
          K(ret), K(cflict_infos), KPC(node), K(is_lock_wait_timeout), K(curr_ts));
    }
  }
  if (OB_UNLIKELY(!ObDeadLockDetectorMgr::is_deadlock_enabled())) {
    cflict_infos.reset();
  }
}

void ObLockWaitMgr::on_no_lock_conflict()
{
  int ret = OB_SUCCESS;
  rpc::ObLockWaitNode* node = lockwaitmgr::ObLockWaitMgr::get_thread_node();
  if (OB_NOT_NULL(node)) {
    if (node->is_placeholder_) {
      // remove placeholder when execution result is not lock-conflict
      remove_placeholder_node(node);
    }
    // no lock conflict, reset need_wait and lock wait timeout ts
    node->reset_need_wait();
    node->reset_lock_wait_expire_ts();
    node->reset_hash();
  }
}

int ObLockWaitMgr::handle_local_node_(Node* node, Node*& delete_node, bool &wait_succ)
{
  // FIXME(xuwang.txw):create detector in check_timeout process
  // below code must keep current order to fix concurrency bug
  // more info see
  int ret = OB_SUCCESS;
  bool unused = false;
  bool deadlock_registered = false;
  ObTransID self_tx_id(node->tx_id_);
  begin_row_lock_wait_event(node);
  if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled())) {
    DETECT_LOG(TRACE, "register local node to deadlock detector success", K(ret), K(*node));
    ObTransID blocked_tx_id(node->holder_tx_id_);
    if (OB_UNLIKELY(OB_FAIL(register_local_node_to_deadlock_(self_tx_id,
                                                             blocked_tx_id,
                                                             node)))) {
      DETECT_LOG(WARN, "register to deadlock detector failed", K(ret), K(*node));
    } else {
      deadlock_registered = true;
      DETECT_LOG(TRACE, "register local node to deadlock detector success", K(ret), K(*node));
    }
  }
  wait_succ = wait_(node, delete_node, unused);
  if (OB_UNLIKELY(!wait_succ && deadlock_registered)) {
      (void) ObTransDeadlockDetectorAdapter::unregister_from_deadlock_detector(self_tx_id,
                                                                               ObTransDeadlockDetectorAdapter::
                                                                               UnregisterPath::
                                                                               LOCK_WAIT_MGR_WAIT_FAILED);
  }
  TRANS_LOG(TRACE, "handle local node finish", K(ret), K(wait_succ), KP(delete_node), KP(node));
  return ret;
}

bool ObLockWaitMgr::wait_remote_ctrl_side_node_(Node* remote_node, Node*& delete_node)
{
  bool wait_succ = false;
  int ret = OB_SUCCESS;
  CriticalGuard(get_qs());
  bool unused = false;
  wait_succ = wait_(remote_node, delete_node, unused);
  if (wait_succ) {
    if (OB_FAIL(on_remote_ctrl_side_node_enqueue_(*remote_node))) {
      // send rpc err, dequeue and retry request
      TRANS_LOG(WARN, "send enqueue msg to dst fail, retry remote request immediately",
        K(ret), KPC(remote_node));
      int err = 0;
      Node *remove_ret = NULL;
      err = remove_node_(remote_node, remove_ret);
      delete_node = delete_node == NULL ? remove_ret : delete_node;
      if (err == 0) {
        wait_succ = false;
      } else if (err == -ENOENT) { // maybe repost by checktimeout, do nothing
      } else {
        TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "insert node fail unexpected error happened",
          K(err), KPC(remote_node));
      }
    }
  }
  TRANS_LOG(TRACE, "handle remote node finish", K(ret), K(wait_succ), KP(delete_node), KP(remote_node));
  return wait_succ;
}

int ObLockWaitMgr::handle_remote_ctrl_side_node_(Node* remote_node, Node*& delete_node, bool &wait_succ)
{
  int ret = OB_SUCCESS;
  bool deadlock_registered = false;
  ObTransID self_tx_id(remote_node->tx_id_);
  if (!remote_node->is_node_id_valid()) {
    remote_node->set_node_id(generate_node_seq_());
  }
  begin_row_lock_wait_event(remote_node);
  if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled())) {
    if (OB_UNLIKELY(OB_FAIL(ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_remote_self_(*remote_node)))) {
      DETECT_LOG(WARN, "register to deadlock detector failed", K(ret), KPC(remote_node));
    } else {
      deadlock_registered = true;
      DETECT_LOG(TRACE, "register remote node to deadlock detector success", K(ret), KPC(remote_node));
    }
  }
  wait_succ = wait_remote_ctrl_side_node_(remote_node, delete_node);
  if (OB_UNLIKELY(!wait_succ && deadlock_registered)) {
    (void) ObTransDeadlockDetectorAdapter::unregister_from_deadlock_detector(self_tx_id,
                                                                              ObTransDeadlockDetectorAdapter::
                                                                              UnregisterPath::
                                                                              LOCK_WAIT_MGR_WAIT_FAILED);
  }
  TRANS_LOG(TRACE, "handle remote node finish", K(ret), K(wait_succ), KP(delete_node), KP(remote_node));
  return ret;
}

int ObLockWaitMgr::handle_remote_exec_side_node_(Node* remote_exec_side_node,
                                                 const ObString &query_sql,
                                                 const int64_t query_timeout_us,
                                                 Node*& delete_node,
                                                 bool &wait_succ,
                                                 bool &is_placeholder)
{
  // FIXME(xuwang.txw):create detector in check_timeout process
  // below code must keep current order to fix concurrency bug
  // more info see
  int ret = OB_SUCCESS;
  bool deadlock_registered = false;
  ObTransID self_tx_id(remote_exec_side_node->tx_id_);
  if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled())) {
    ObTransID blocked_tx_id(remote_exec_side_node->holder_tx_id_);
    if (OB_FAIL(register_remote_exec_side_node_to_deadlock_(self_tx_id,
                                                            blocked_tx_id,
                                                            remote_exec_side_node,
                                                            query_sql,
                                                            query_timeout_us))) {
      DETECT_LOG(WARN, "register remote execution side node to deadlock detector failed", K(ret), K(*remote_exec_side_node));
    } else {
      deadlock_registered = true;
      DETECT_LOG(TRACE, "register remote execution side node to deadlock detector success", K(ret), K(*remote_exec_side_node));
    }
  }
  wait_succ = wait_(remote_exec_side_node, delete_node, is_placeholder);
  if (OB_UNLIKELY(!wait_succ && deadlock_registered)) {
      (void) ObTransDeadlockDetectorAdapter::unregister_from_deadlock_detector(self_tx_id,
                                                                               ObTransDeadlockDetectorAdapter::
                                                                               UnregisterPath::
                                                                               LOCK_WAIT_MGR_WAIT_FAILED);
  }
  TRANS_LOG(TRACE, "handle remote execution side node finish", K(ret), K(wait_succ), KP(delete_node), KP(remote_exec_side_node), K(query_sql));
  return ret;
}

// for local execution
int ObLockWaitMgr::register_local_node_to_deadlock_(const transaction::ObTransID &self_tx_id,
                                                    const transaction::ObTransID &blocked_tx_id,
                                                    const Node * const node)
{
  int ret = OB_SUCCESS;
  CollectCallBack on_collect_callback((LocalDeadLockCollectCallBack(self_tx_id,
                                                                    node->key_,
                                                                    SessionIDPair(node->sessid_, node->assoc_sess_id_),
                                                                    node->get_ls_id(),
                                                                    node->tablet_id_,
                                                                    transaction::ObTxSEQ::cast_from_int(node->get_holder_tx_hold_seq_value()))));
  if (LockHashHelper::is_rowkey_hash(node->hash())) {// waiting for row
    DeadLockBlockCallBack deadlock_block_callback(row_holder_mapper_, node->hash());
    LocalExecutionWaitingForRowFillVirtualInfoOperation fill_virtual_info_callback(row_holder_mapper_,
                                                                                   ObLSID(node->ls_id_),
                                                                                   node->hash());
    if (OB_FAIL(ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_for_row(on_collect_callback,
                                                                                                   deadlock_block_callback,
                                                                                                   fill_virtual_info_callback,
                                                                                                   self_tx_id,
                                                                                                   SessionIDPair(node->sessid_,
                                                                                                                 node->assoc_sess_id_)))) {
      TRANS_LOG(WARN, "fail to regester to deadlock detector", K(*node), KR(ret));
    } else {
      TRANS_LOG(TRACE, "wait for row", K(*node));
    }
  } else {// waiting for other trans
    LocalExecutionWaitingForTransFillVirtualInfoOperation fill_virtual_info_callback(ObLSID(node->ls_id_),
                                                                                     blocked_tx_id,
                                                                                     ObTxSEQ::mk_v0(node->holder_tx_hold_seq_value_ == 0 ? 1 : node->holder_tx_hold_seq_value_));
    if (OB_FAIL(ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_for_trans(on_collect_callback,
                                                                                                     fill_virtual_info_callback,
                                                                                                     blocked_tx_id,
                                                                                                     self_tx_id,
                                                                                                     SessionIDPair(node->sessid_,
                                                                                                                   node->assoc_sess_id_)))) {
      TRANS_LOG(WARN, "fail to regester to deadlock detector", K(*node), KR(ret));
    } else {
      TRANS_LOG(TRACE, "wait for trans", K(*node));
    }
  }
  TRANS_LOG(TRACE, "register local node to deadlock detector", K(ret), K(self_tx_id), K(blocked_tx_id), KP(node));
  return ret;
}

void ObLockWaitMgr::ls_switch_to_follower(const share::ObLSID &ls_id)
{
  ObLink* retire_iter = NULL;
  {
    Node *iter = NULL;
    Node *node2del = NULL;
    CriticalGuard(get_qs());
    while(NULL != (iter = hash_->quick_next(iter))) {
      TRANS_LOG(TRACE, "switch to follower:", KPC(iter));
      if (iter->get_ls_id() == ls_id.id()) {
        node2del = iter;
        bool is_placeholder = ATOMIC_LOAD(&node2del->is_placeholder_);
        if (!is_placeholder || (is_placeholder && node2del->get_node_type() == Node::REMOTE_EXEC_SIDE)) {
          retire_node(retire_iter, node2del);
        }
        node2del = NULL;
      }
    }
  }
  if (NULL != retire_iter) {
    WaitQuiescent(get_qs());
  }
  while (NULL != retire_iter) {
    Node* cur = CONTAINER_OF(retire_iter, Node, retire_link_);
    retire_iter = retire_iter->next_;
    (void)repost(cur);
  }
}

int ObLockWaitMgr::register_remote_exec_side_node_to_deadlock_(const transaction::ObTransID &self_tx_id,
                                                   const transaction::ObTransID &blocked_tx_id,
                                                   const Node * const node,
                                                   const ObString &query_sql,
                                                   const int64_t query_timeout_us)
{
  int ret = OB_SUCCESS;
  CollectCallBack on_collect_callback((RemoteExecutionSideNodeDeadLockCollectCallBack(self_tx_id,
                                                                    "remote execution side node row key",
                                                                    query_sql,
                                                                    SessionIDPair(node->sessid_, node->assoc_sess_id_),
                                                                    node->get_ls_id(),
                                                                    node->tablet_id_,
                                                                    transaction::ObTxSEQ::cast_from_int(node->get_holder_tx_hold_seq_value()))));
  if (LockHashHelper::is_rowkey_hash(node->hash())) {// waiting for row
    LocalExecutionWaitingForRowFillVirtualInfoOperation fill_virtual_info_callback(row_holder_mapper_,
                                                                                   ObLSID(node->ls_id_),
                                                                                   node->hash());
    DeadLockBlockCallBack deadlock_block_call_back(row_holder_mapper_, node->hash());
    if (OB_FAIL(ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_for_row_without_session(on_collect_callback,
                                                                                                                   fill_virtual_info_callback,
                                                                                                                   deadlock_block_call_back,
                                                                                                                   self_tx_id,
                                                                                                                   node->get_sess_id(),
                                                                                                                   node->get_tx_active_ts(),
                                                                                                                   query_timeout_us))) {
      TRANS_LOG(WARN, "fail to regester to deadlock detector", K(ret));
    } else {
      TRANS_LOG(TRACE, "regester remote execution side node wait row to deadlock detector", K(ret), KPC(node));
    }
  } else {// waiting for other trans
    LocalExecutionWaitingForTransFillVirtualInfoOperation fill_virtual_info_callback(ObLSID(node->ls_id_),
                                                                                     blocked_tx_id,
                                                                                     ObTxSEQ::mk_v0(node->holder_tx_hold_seq_value_ == 0 ? 1 : node->holder_tx_hold_seq_value_));
    if (OB_FAIL(ObTransDeadlockDetectorAdapter::lock_wait_mgr_reconstruct_detector_waiting_for_trans_without_session(on_collect_callback,
                                                                                                                     fill_virtual_info_callback,
                                                                                                                     blocked_tx_id,
                                                                                                                     self_tx_id,
                                                                                                                     node->get_sess_id(),
                                                                                                                     node->get_tx_active_ts(),
                                                                                                                     query_timeout_us))) {
      TRANS_LOG(WARN, "fail to regester to deadlock detector", K(ret));
    } else {
      TRANS_LOG(TRACE, "regester remote execution side node wait trans to deadlock detector", K(ret), KPC(node));
    }
  }
  TRANS_LOG(TRACE, "register remote execution side node to deadlock detector", K(ret), K(self_tx_id), K(blocked_tx_id), KP(node));
  return ret;
}

bool ObLockWaitMgr::wait_(Node* node, Node*& delete_node, bool &is_placeholder)
{
  bool wait_succ = false;
  bool enqueue_succ = false;
  uint64_t hash = node->hash();
  int64_t last_lock_seq = node->lock_seq_;
  int64_t cur_seq = 0;
  is_placeholder = false;
  if (has_set_stop()) {
  } else {
    CriticalGuard(get_qs());
    if (node->get_node_type() == rpc::ObLockWaitNode::REMOTE_CTRL_SIDE) {
      node->try_lock_times_++;
      (void)insert_node_(node);
      wait_succ = true;
      enqueue_succ = true;
    } else if (check_wakeup_seq_(hash, last_lock_seq, cur_seq)) {
      // seq has not changed
      wait_succ = true;
      (void)insert_node_(node);
      enqueue_succ = true;
      if (!check_wakeup_seq_(hash, last_lock_seq, cur_seq)) {
        // double check, seq changed
        wait_succ = on_seq_change_after_insert_(node, delete_node, enqueue_succ);
        TRANS_LOG(TRACE, "LockWaitMgr.wait double check seq change", K(wait_succ), K(enqueue_succ), KPC(node), K(last_lock_seq), K(cur_seq));
      } else {
        TRANS_LOG(TRACE, "LockWaitMgr.wait", K(wait_succ), K(enqueue_succ), K(has_set_stop()), KP(node), K(last_lock_seq), K(cur_seq));
      }
    } else {
      // seq has changed
      wait_succ = on_seq_change_(node, delete_node, enqueue_succ, cur_seq);
      TRANS_LOG(TRACE, "LockWaitMgr.wait first check seq change", K(wait_succ), K(enqueue_succ), KPC(node), K(last_lock_seq), K(cur_seq));
    }
    // avoid miss wake up, reset hold key
    if (!wait_succ) {
      node->last_wait_hash_ = node->hash_;
    }
    after_wait_(node, delete_node, enqueue_succ, wait_succ, is_placeholder);
    TRANS_LOG(LOG_LEVEL_FOR_LWM, "LockWaitMgr.wait", K(wait_succ), K(enqueue_succ), K(has_set_stop()), KP(node), K(last_lock_seq), K(cur_seq));
  }
  return wait_succ;
}

void ObLockWaitMgr::after_wait_(Node* node,
                               Node *&delete_node,
                               bool enqueue_succ,
                               bool &wait_succ,
                               bool &is_placeholder)
{
  int err = 0;
  if (has_set_stop()) {
    // Exception operation
    enqueue_succ = false;
    err = remove_node_(node, delete_node);
    if (0 != err) {
      wait_succ = true; // maybe repost by check timeout
      node = NULL;
    } else {
      ATOMIC_STORE(&node->is_placeholder_, false);
      wait_succ = false;
    }
  }

  if (enqueue_succ) {
    node->try_lock_times_++;
  }
  is_placeholder = ATOMIC_LOAD(&node->is_placeholder_);
}

bool ObLockWaitMgr::on_seq_change_(Node *node,
                                  Node *&delete_node,
                                  bool &enqueue_succ,
                                  uint64_t seq)
{
  bool wait_succ = false;
  uint64_t hash = node->hash();
  int64_t cur_seq = 0;
  Node* wait_node = get_wait_head(hash);
  if (NULL != wait_node) {
    // has someone in queue, enqueue itself
    wait_succ = true;
    enqueue_succ = true;
    (void)insert_node_(node);
    if (!check_wakeup_seq_(hash, seq, cur_seq)) {
      wait_succ = on_seq_change_after_insert_(node, delete_node, enqueue_succ);
    }
  } else {
    // no one in queue, enqueue itself and retry
    wait_succ = false;
    enqueue_succ = true;
    ATOMIC_STORE(&node->is_placeholder_, true);
    if (node->get_node_type() == Node::REMOTE_EXEC_SIDE) {
      node->set_wait_timeout_ts(common::ObTimeUtil::current_time() + WAIT_TIMEOUT_TS);
    }
    (void)insert_node_(node);
  }
  TRANS_LOG(INFO, "LockWaitMgr.wait seq change after insert", K(wait_succ), K(enqueue_succ), KPC(node), KPC(wait_node));
  return wait_succ;
}

bool ObLockWaitMgr::on_seq_change_after_insert_(Node *node,
                                               Node *&delete_node,
                                               bool &enqueue_succ)
{
  // after insert seq changed, delete itself and retry
  bool wait_succ = true;
  int err = 0;
  uint64_t hash = node->hash();
  Node *wait_node = get_wait_head(hash);
  if (wait_node == node) {
    // node itself is the head node in queue, remove itself and retry
    wait_succ = false;
    enqueue_succ = false;
    err = remove_node_(node, delete_node);
    if (err == 0) {
    } else if (err == -ENOENT) {
      // wake up by others
      wait_succ = true;
    } else {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "remove node fail unexpected error happened",
        K(err), KPC(node));
    }
  }
  TRANS_LOG(TRACE, "LockWaitMgr.wait seq change after insert", K(wait_succ), K(enqueue_succ), KP(node), K(err));
  return wait_succ;
}
void ObLockWaitMgr::wakeup_(uint64_t hash)
{
  Node *node = NULL;
  ObAddr invalid_addr;
  do {
    node = fetch_and_repost_head(hash);
    // continue loop to wake up all requests waitting on the transaction.
    // or continue loop to wake up all requests waitting on the tablelock.
  } while (!LockHashHelper::is_rowkey_hash(hash) && node != NULL);
  TRANS_LOG(DEBUG, "LockWaitMgr.wakeup.done", K(hash));
}

void ObLockWaitMgr::wakeup_(const uint64_t hash,
                            const ObAddr &addr)
{
  if (hash == 0) {
    // do nothing
  } else if (!addr.is_valid()) {
    wakeup_(hash);
  } else {
    remote_wakeup_(hash, addr);
  }
  TRANS_LOG(TRACE, "LockWaitMgr wakeup last wait hash finish", K(hash), K(addr));
}
void ObLockWaitMgr::remote_wakeup_(const uint64_t hash,
                                   const ObAddr &addr)
{
  int ret = OB_SUCCESS;
  ObLockWaitMgrWakeUpRemoteMsg msg;
  msg.init(tenant_id_, hash, 0, addr_);
  if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "msg is invalid", K(ret), K(hash), K(addr), K(msg));
  } else if (OB_FAIL(rpc_->post_msg(addr, msg))) {
    TRANS_LOG(WARN, "post remote wake up msg fail", K(ret), K(hash), K(addr), K(msg));
  }
  TRANS_LOG(TRACE, "LockWaitMgr remote wakeup finish", K(ret), K(hash), K(addr), K(msg));
}

ObLockWaitMgr::Node* ObLockWaitMgr::next(Node*& iter, Node* target)
{
  CriticalGuard(get_qs());
  if (NULL != (iter = hash_->next(iter))) {
    *target = *iter;
    Node *node = hash_->get_next_internal(target->hash());
    while(NULL != node && node->hash() < target->hash()) {
      node = (Node*)link_next(node);
    }
    if (NULL != node && node->hash() == target->hash()) {
      target->set_block_sessid(sql::ObSQLSessionInfo::INVALID_SESSID ==
                                       node->client_sid_
                                   ? node->sessid_
                                   : node->client_sid_);
    }
  } else {
    target = NULL;
  }
  return target;
}

ObLockWaitMgr::Node* ObLockWaitMgr::get_waiter(uint64_t hash,
                                                 NodeID node_id,
                                                 const ObAddr &addr,
                                                 bool detach_node,
                                                 NodeType node_type)
{
  Node* remove_ret = NULL;
  Node* node = NULL;
  bool is_placeholder = false;
  bool find_with_node_id = node_id > 0;
  bool find_with_addr = addr.is_valid();
  bool find_with_node_type = node_type != NodeType::MAX;

  if (get_wait_node_cnt() > 0) {
    CriticalGuard(get_qs());
    node = hash_->get_next_internal(hash);
    // we do not need to wake up if the request is not running
    while(NULL != node && node->hash() <= hash) {
      int err = 0;
      if (find_with_node_id && node->get_node_id() != node_id) {
        // do nothing
      } else if (find_with_addr && node->get_ctrl_addr() != addr) {
        // do nothing
      } else if (find_with_node_type && node->get_node_type() != node_type) {
        // do nothing
      } else if (node->hash() == hash) {
        is_placeholder = ATOMIC_LOAD(&node->is_placeholder_);
        if (detach_node && is_placeholder && !find_with_node_id) {
        } else if (detach_node) {
          err = remove_node_(node, remove_ret);
          if (0 != err) {
            TRANS_LOG(TRACE, "remove node fail, maybe repost by others", KPC(node),
              K(err), K(hash), K(node_id), K(addr), K(detach_node));
            remove_ret = NULL;
          } else {
            break;
          }
        } else {
          remove_ret = node;
          break;
        }
      }
      node = (Node*)link_next(node);
    }
  }
  TRANS_LOG(TRACE, "LockWaitMgr fetch waiter finish",
    K(hash), K(node_id), K(addr), K(detach_node), K(ATOMIC_LOAD(&total_wait_node_)), KPC(remove_ret), K(ATOMIC_LOAD(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT])));
  if (NULL != remove_ret && detach_node) {
    WaitQuiescent(get_qs());
  }
  return remove_ret;
}

void ObLockWaitMgr::check_wait_node_session_stat_(Node *iter,
                                                  Node *&node2del,
                                                  int64_t curr_ts,
                                                  int64_t wait_timeout_ts)
{
  Node *remove_node = NULL;
  sql::ObSQLSessionInfo *session_info = NULL;
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (GCTX.session_mgr_ == NULL) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "session_mgr is NULL", K(ret),  "sessid", iter->get_sess_id(), K(*iter));
  } else {
    ObSessionGetterGuard guard(*GCTX.session_mgr_, iter->get_sess_id());
    if (OB_FAIL(guard.get_session(session_info))) {
      node2del = iter;
      TRANS_LOG(ERROR, "failed to get session_info ", K(ret),  "sessid", iter->get_sess_id(), K(*iter));
    } else if (OB_ISNULL(session_info)) {
      // when the request exist, the session should exist as well
      node2del = iter;
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "got session_info is NULL", K(ret),  "sessid", iter->get_sess_id(), K(*iter));
    } else if (OB_UNLIKELY(session_info->is_terminate(tmp_ret))) {
      // session is killed, just pop the request
      node2del = iter;
      // SET_NODE_TO_DELETE("session terminated");
      TRANS_LOG(INFO, "session is killed, pop the request",  "sessid", iter->get_sess_id(), K(*iter), K(tmp_ret));
    } else if (NULL == node2del && curr_ts - iter->lock_ts_ > wait_timeout_ts) {
      // in order to prevent missing to wakeup request, so we force to wakeup every 5s
      node2del = iter;
      // SET_NODE_TO_DELETE("wait time too long");
      iter->on_retry_lock(iter->hash());
      TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "LOCK_MGR: req wait lock cost too much time", K(*iter), K(wait_timeout_ts));
    } else {
      transaction::ObTxDesc *&tx_desc = session_info->get_tx_desc();
      bool ac = false, has_explicit_start_tx = session_info->has_explicit_start_trans();
      session_info->get_autocommit(ac);
      if (OB_ISNULL(tx_desc) && (!ac || has_explicit_start_tx)) {
        uint32_t session_id = session_info->get_server_sid();
        const common::ObCurTraceId::TraceId &trace_id = session_info->get_current_trace_id();
        TRANS_LOG(WARN, "LOG_MGR: found session ac = 0 or has_explicit_start_trans but txDesc was released!",
                  K(session_id), K(trace_id), K(ac), K(has_explicit_start_tx));
      }
    }
    if (OB_NOT_NULL(session_info)) {
      transaction::ObTxDesc *&tx_desc = session_info->get_tx_desc();
      TRANS_LOG(INFO, "check transaction state", KP(tx_desc));
    }
  }
}

ObLink* ObLockWaitMgr::check_timeout()
{
  #define SET_NODE_TO_DELETE(__reason__) \
  node2del = iter;\
  reason = __reason__;
  ObLink* tail = NULL;
  Node* iter = NULL;
  Node* node2del = NULL;
  const char *reason = NULL;
  bool need_check_session = false;
  const int64_t MAX_WAIT_TIME_US = 10 * 1000 * 1000;
  KilledSessionArray *killed_session = NULL;
  fetch_killed_sessions_(killed_session);
  // FIX:
  // lower down session idle check frequency to 10s
  int64_t curr_ts = ObClockGenerator::getClock();
  if (curr_ts - last_check_session_idle_ts_ > MAX_WAIT_TIME_US) {
    need_check_session = true;
    last_check_session_idle_ts_ = curr_ts;
  }
  {
    CriticalGuard(get_qs());
    while(NULL != (iter = hash_->quick_next(iter))) {
      TRANS_LOG(TRACE, "LOCK_MGR: check", K(*iter));
      // 0 - 5s rand wait time for wait timeout
      int64_t rand_wait_ts = ObRandom::rand(0, 50) * 100 * 1000;
      uint64_t hash = iter->hash();
      uint64_t last_lock_seq = iter->get_lock_seq();
      uint64_t curr_lock_seq = ATOMIC_LOAD(&sequence_[(hash >> 1)% LOCK_BUCKET_COUNT]);
      bool is_placeholder = ATOMIC_LOAD(&iter->is_placeholder_);
      int64_t wait_timeout_ts = MAX_WAIT_TIME_US/2 + rand_wait_ts;
      if (iter->get_node_type() == rpc::ObLockWaitNode::REMOTE_EXEC_SIDE) {
        if (need_check_session
        && (curr_ts - iter->lock_ts_ > wait_timeout_ts)) {
          // in order to prevent missing to wakeup request, so we force to wakeup every 5s
          SET_NODE_TO_DELETE("remote execution side node wait too much time");
          iter->on_retry_lock(hash);
          TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "LOCK_MGR: remote execution side node wait lock cost too much time", K(curr_lock_seq), K(last_lock_seq), K(*iter));
        } else if (iter->is_lock_wait_timeout()
                || iter->is_wait_timeout()
                || has_set_stop()) {
          TRANS_LOG_RET(INFO, OB_TIMEOUT, "LOCK_MGR: remote execution side node wait timeout", K(curr_lock_seq), K(last_lock_seq), K(*iter));
          SET_NODE_TO_DELETE("remote execution side node wait timeout or lwm stop");
          iter->on_retry_lock(hash);
        }
      } else if (is_placeholder) {
      } else if (iter->is_lock_wait_timeout() || has_set_stop()) {
        TRANS_LOG_RET(WARN, OB_TIMEOUT, "LOCK_MGR: req wait lock timeout", K(curr_lock_seq), K(last_lock_seq), K(*iter));
        need_check_session = true;
        SET_NODE_TO_DELETE("timeout or stopped");
        EVENT_INC(MEMSTORE_WRITE_LOCK_WAIT_TIMEOUT_COUNT);
        // it needs to be placed before the judgment of session_id to prevent the
        // abnormal case which session_id equals 0 from causing the problem of missing wakeup
      } else if (0 == iter->sessid_) {
        // do nothing, may be rpc plan, sessionid is not setted
      } else if (NULL != killed_session
                 && is_killed_session_(killed_session,
                                           iter->get_sess_id())) {
        SET_NODE_TO_DELETE("killed");
        TRANS_LOG(INFO, "session is killed, pop the request",
                  "sessid", iter->get_sess_id(), K(*iter));
      } else if (iter->is_wait_timeout()) {
        // wait timeout in queue, retry request
        TRANS_LOG_RET(TRACE, OB_TIMEOUT, "LOCK_MGR: wait timeout", K(curr_lock_seq), K(last_lock_seq), K(*iter));
        SET_NODE_TO_DELETE("wait timeout");
        // node2del = iter;
      } else if (iter->get_node_type() == Node::REMOTE_CTRL_SIDE) {
        // check remote node state
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(check_remote_node_state_(*iter))) {
          // fail to check remote node state, retry request
          TRANS_LOG_RET(WARN, tmp_ret,"failed to check remote node state", K(tmp_ret), K(*iter));
          // node2del = iter;
          SET_NODE_TO_DELETE("check remote node state fail");
        }
      } else {
        // do nothing
      }

      if (need_check_session
          && iter->get_sess_id() != 0
          && iter->get_node_type() != rpc::ObLockWaitNode::REMOTE_EXEC_SIDE
          && !is_placeholder) { // when lock wait in dag worker, session is not exist
        check_wait_node_session_stat_(iter, node2del, curr_ts, wait_timeout_ts);
      }
      if (NULL != node2del) {
        retire_node(tail, node2del);
        node2del = NULL;
      }
    }
  }
  if (NULL != tail) {
    WaitQuiescent(get_qs());
  }
  return tail;
  #undef SET_NODE_TO_DELETE
}

void ObLockWaitMgr::retire_node(ObLink*& tail, Node* node)
{
  int err = 0;
  Node* tmp_node = NULL;
  EVENT_INC(MEMSTORE_WRITE_LOCK_WAKENUP_COUNT);
  EVENT_ADD(MEMSTORE_WAIT_WRITE_LOCK_TIME, ObTimeUtility::current_time() - node->lock_ts_);
  err = remove_node_(node, tmp_node);
  if (0 == err) {
    node->retire_link_.next_ = tail;
    tail = &node->retire_link_;
  }
}

void ObLockWaitMgr::reset_head_node_wait_timeout_if_need(uint64_t hash, int64_t lock_ts)
{
  {
    // when retry request find row is still being held
    // should reset lock-wait-mgr's wait queue's head node's wait timeout
    // which satisfy: lock_ts > wait_timeout_set_time
    // e.g. wait_timeout_set_time = (wait_timeout_ts - WAIT_TIMEOUT_TS)
    CriticalGuard(get_qs());
    Node *node = get_wait_head(hash);
    if (NULL != node) {
      int64_t set_wait_timeout_ts = node->get_wait_timeout_ts() - WAIT_TIMEOUT_TS;
      if (set_wait_timeout_ts > 0 && lock_ts > set_wait_timeout_ts) {
        node->reset_wait_timeout_ts();
      }
    }
  }
}

int ObLockWaitMgr::post_lock(const int tmp_ret,
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
                             ObFunction<int(bool&, bool&)> &rechecker)
{
  int ret = OB_SUCCESS;
  uint64_t hash = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "lock wait mgr not inited", K(ret));
  } else if (OB_TRY_LOCK_ROW_CONFLICT == tmp_ret) {
    Key key(&row_key);
    uint64_t row_hash = LockHashHelper::hash_rowkey(tablet_id, key);
    uint64_t tx_hash = LockHashHelper::hash_trans(holder_tx_id);
    int64_t row_lock_seq = 0;
    int64_t tx_lock_seq = 0;
    bool locked = false, wait_on_row = true;
    int64_t lock_ts = common::ObTimeUtil::current_time();
    // double check to ensure atomic get seq, to avoid awaken missed
    do {
      row_lock_seq = get_seq_(row_hash);
      tx_lock_seq = get_seq_(tx_hash);
      if (OB_FAIL(rechecker(locked, wait_on_row))) {
        TRANS_LOG(WARN, "recheck lock fail", K(key), K(holder_tx_id));
      // if seq is changed, means some request is awaken，and locked/wait_on_row flag maybe wrong
      } else if (wait_on_row) {
        if (get_seq_(row_hash) == row_lock_seq) {
          break;
        }
      } else {
        if (get_seq_(tx_hash) == tx_lock_seq) {
          break;
        }
      }
    } while (OB_SUCC(ret) && locked);
    if (locked) {
      transaction::ObTransService *tx_service = nullptr;
      uint32_t holder_session_id = sql::ObSQLSessionInfo::INVALID_SESSID;
      uint32_t client_sid = sql::ObSQLSessionInfo::INVALID_SESSID;
      uint32_t holder_client_sid = sql::ObSQLSessionInfo::INVALID_SESSID;
      int64_t holder_tx_start_time = 0;
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(tx_service = MTL(transaction::ObTransService *))) {
        tmp_ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "ObTransService is null", K(tx_id), K(holder_tx_id), K(ls_id));
      } else if (OB_TMP_FAIL(sql::ObBasicSessionInfo::get_client_sid(sess_id_pair.get_valid_sess_id(), client_sid))) {
          TRANS_LOG(ERROR, "get client_sid failed", K(ret));
      } else if (OB_TMP_FAIL(tx_service->get_trans_start_session_id_and_ts(ls_id, holder_tx_id, holder_session_id, holder_tx_start_time))) {
        TRANS_LOG(WARN, "get transaction start session_id failed", K(tx_id), K(holder_tx_id), K(ls_id));
      } else if (OB_TMP_FAIL(sql::ObBasicSessionInfo::get_client_sid(holder_session_id, holder_client_sid))) {
        TRANS_LOG(ERROR, "get holder client_sid failed", K(ret));
      }
      uint64_t hash = wait_on_row ? row_hash : tx_hash;
      char buffer[rpc::ObLockWaitNode::KEY_BUFFER_SIZE] = {0};// if str length less than ObStringHolder::TINY_STR_SIZE, no heap memory llocated
      int64_t pos = 0;
      ObCStringHelper helper;
      common::databuff_printf(buffer, rpc::ObLockWaitNode::KEY_BUFFER_SIZE, pos, "%s", helper.convert(row_key.get_rowkey()));// it'ok if buffer not enough
      ObString temp_ob_string(pos, buffer);
      ObStringHolder temp_string_holder;
      temp_string_holder.assign(temp_ob_string);
      reset_head_node_wait_timeout_if_need(hash, lock_ts);
      cflict_info.init(addr_,
                      ls_id,
                      tablet_id,
                      meta::ObMover<ObStringHolder>(temp_string_holder),
                      sess_id_pair,
                      ObAddr(), // conflict_tx_scheduler
                      holder_tx_id,
                      conflict_tx_hold_seq,
                      hash,
                      wait_on_row ? row_lock_seq : tx_lock_seq,
                      timeout,
                      tx_id,
                      0, /*lock mode*/
                      last_compact_cnt,
                      total_trans_node_cnt,
                      sess_id_pair.assoc_sess_id_,
                      holder_session_id,
                      holder_tx_start_time,
                      holder_client_sid);
      if (OB_SUCCESS != ObTransDeadlockDetectorAdapter::
                  get_trans_info_on_participant(cflict_info.conflict_tx_id_,
                                                cflict_info.conflict_ls_,
                                                cflict_info.conflict_tx_scheduler_,
                                                cflict_info.conflict_sess_id_pair_)) {
        TRANS_LOG(WARN, "get transaction info fail", K(ret), K(cflict_info));
      }
    }
    TRANS_LOG(LOG_LEVEL_FOR_LWM, "post_lock", K(ret), K(tablet_id), K(row_key),
      K(row_hash), K(tx_hash), K(tx_id), K(holder_tx_id), K(cflict_info), K(ATOMIC_LOAD(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT])));
  }
  return ret;
}

int ObLockWaitMgr::post_lock(const int tmp_ret,
                             const share::ObLSID &ls_id,
                             const ObTabletID &tablet_id,
                             const ObLockID &lock_id,
                             const int64_t timeout,
                             const bool is_remote_sql,
                             const int64_t last_compact_cnt,
                             const int64_t total_trans_node_cnt,
                             const transaction::ObTransID &tx_id,
                             const transaction::ObTransID &holder_tx_id,
                             const ObTableLockMode &lock_mode,
                             ObRowConflictInfo &cflict_info,
                             ObFunction<int(bool&)> &check_need_wait)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "lock wait mgr not inited", K(ret));
  } else if (OB_TRY_LOCK_ROW_CONFLICT == tmp_ret) {
    uint64_t hash = LockHashHelper::hash_lock_id(lock_id);
    int64_t lock_ts = common::ObTimeUtil::current_time();
    int64_t lock_seq = get_seq_(hash);
    bool need_wait = false;
    if (OB_FAIL(check_need_wait(need_wait))) {
      TRANS_LOG(WARN, "check need wait failed", K(ret));
    } else if (need_wait) {
      transaction::ObTransService *tx_service = nullptr;
      uint32_t holder_session_id = sql::ObSQLSessionInfo::INVALID_SESSID;
      int64_t holder_tx_start_time = 0;
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(tx_service = MTL(transaction::ObTransService *))) {
        tmp_ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "ObTransService is null", K(tx_id), K(holder_tx_id), K(ls_id));
      } else if (OB_FAIL(tx_service->get_trans_start_session_id_and_ts(ls_id, holder_tx_id, holder_session_id, holder_tx_start_time))) {
        TRANS_LOG(WARN, "get transaction start session_id failed", K(tx_id), K(holder_tx_id), K(ls_id));
      }
      char lock_id_buf[common::MAX_LOCK_ID_BUF_LENGTH];
      int pos = lock_id.to_string(lock_id_buf, sizeof(lock_id_buf));
      lock_id_buf[common::MAX_LOCK_ID_BUF_LENGTH - 1] = '\0';
      ObString temp_ob_string(pos, lock_id_buf);
      ObStringHolder temp_string_holder;
      temp_string_holder.assign(temp_ob_string);
      reset_head_node_wait_timeout_if_need(hash, lock_ts);
      cflict_info.init(addr_,
                      ls_id,
                      tablet_id,
                      meta::ObMover<ObStringHolder>(temp_string_holder),
                      SessionIDPair(),
                      ObAddr(), // conflict_tx_scheduler
                      holder_tx_id,
                      ObTxSEQ(),
                      hash,
                      lock_seq,
                      timeout,
                      tx_id,
                      lock_mode,
                      last_compact_cnt,
                      total_trans_node_cnt,
                      0,
                      0,
                      holder_session_id,
                      holder_tx_start_time);
      if (OB_SUCCESS != ObTransDeadlockDetectorAdapter::
                  get_trans_info_on_participant(cflict_info.conflict_tx_id_,
                                                cflict_info.conflict_ls_,
                                                cflict_info.conflict_tx_scheduler_,
                                                cflict_info.conflict_sess_id_pair_)) {
        TRANS_LOG(WARN, "get transaction info fail", K(ret), K(cflict_info));
      }
      uint32_t holder_client_sid = sql::ObSQLSessionInfo::INVALID_SESSID;
      if (OB_TMP_FAIL(sql::ObBasicSessionInfo::get_client_sid(
                      cflict_info.conflict_sess_id_pair_.get_valid_sess_id(), holder_client_sid))) {
        TRANS_LOG(ERROR, "get client_sid failed", K(ret));
      } else {
        cflict_info.client_session_id_ = holder_client_sid;
      }
    }
    TRANS_LOG(LOG_LEVEL_FOR_LWM, "post_lock", K(ret), K(tablet_id),
      K(lock_id), K(tx_id), K(holder_tx_id), K(cflict_info));
  }
  return ret;
}

void ObLockWaitMgr::add_timeout_to_head_node_(const uint64_t hash, const int64_t timeout_us)
{
  ObAddr invalid_addr  = ObAddr();
  {
    CriticalGuard(get_qs());
    Node *node = get_wait_head(hash);
    if (NULL != node) {
      node->set_wait_timeout_ts(common::ObTimeUtil::current_time() + timeout_us);
    }
  }
  TRANS_LOG(DEBUG, "add timeout to head node when remote wakeup", K(hash), K(timeout_us));
}

int ObLockWaitMgr::on_remote_lock_release_(const Node &node)
{
  int ret = OB_SUCCESS;
  // used for remote wake up, prevent missing wake up for successor node
  add_timeout_to_head_node_(node.hash(), WAIT_TIMEOUT_TS);
  uint64_t wakeup_hash = node.get_wait_row_hash() == 0 ? node.hash() : node.get_wait_row_hash();

  ObLockWaitMgrLockReleaseMsg msg;
  msg.init(tenant_id_, wakeup_hash, node.get_node_id(), addr_);
  if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "remote node is invalid", K(ret), K(msg), K(node));
  } else if (OB_FAIL(rpc_->post_msg(node.get_ctrl_addr(), msg))) {
    TRANS_LOG(WARN, "post lock release msg fail", K(ret), K(msg), K(node));
  }
  TRANS_LOG(TRACE, "post lock release msg finish", K(ret), K(msg), K(node));
  return ret;
}

int ObLockWaitMgr::repost(Node* node)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  TRANS_LOG(LOG_LEVEL_FOR_LWM, "LockWaitMgr.repost", KPC(node), K(lbt()));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "lock wait mgr not inited", K(ret));
  } else {
    ObTransID self_tx_id(node->tx_id_);
    ObTransDeadlockDetectorAdapter::unregister_from_deadlock_detector(self_tx_id,
                                                                      ObTransDeadlockDetectorAdapter::
                                                                      UnregisterPath::
                                                                      LOCK_WAIT_MGR_REPOST);
    node->reset_wait_timeout_ts();
    if (node->get_node_type() == Node::REMOTE_EXEC_SIDE) {
      if (node->is_placeholder()) {
        // just discard and wakeup next waiter
        fetch_and_repost_head(node->hash());
      } else if (OB_FAIL(on_remote_lock_release_(*node))) {
        TRANS_LOG(WARN, "inform remote lock release fail", K(ret), KPC(node));
      }
      free_node_(node);
    } else if (FALSE_IT(end_row_lock_wait_event(node))) {
    } else if (OB_FAIL(OBSERVER.get_net_frame().get_deliver().repost((void*)node))) {
      TRANS_LOG(WARN, "report error", K(ret));
    }
  }
  return ret;
}

// wait lock on trans_id
int ObLockWaitMgr::transform_row_lock_to_tx_lock(const ObTabletID &tablet_id,
                                                 const Key &row_key,
                                                 const ObTransID &tx_id,
                                                 const ObAddr &tx_scheduler)
{
  TRANS_LOG(TRACE, "transform row lock to tx lock", K(tablet_id), K(row_key), K(tx_id), K(tx_scheduler));
  int ret = OB_SUCCESS;

  uint64_t hash_tx_id = LockHashHelper::hash_trans(tx_id);
  uint64_t hash_row_key = LockHashHelper::hash_rowkey(tablet_id, row_key);

  int64_t lock_seq = get_seq_(hash_tx_id);
  Node *node = NULL;
  Node *tmp_node = NULL;
  int wait_succ = false;
  bool is_placeholder = false;
  ATOMIC_INC(&sequence_[(hash_row_key >> 1) % LOCK_BUCKET_COUNT]);
  // fetch wait head will not fetch placeholder node
  while (NULL != (node = fetch_wait_head(hash_row_key))) {
    ATOMIC_INC(&sequence_[(hash_row_key >> 1) % LOCK_BUCKET_COUNT]);
    int tmp_ret = OB_SUCCESS;
    bool need_repost = true;
    tmp_node = NULL;
    ObTransID self_tx_id(node->tx_id_);
    if (tx_scheduler.is_valid()) {
      ObTransDeadlockDetectorAdapter::change_detector_waiting_obj_from_row_to_trans(self_tx_id, tx_id, tx_scheduler);
    } else {
      TRANS_LOG(WARN, "tx scheduler is invalid", K(tx_scheduler), K(tx_id), K(row_key));
    }
    node->change_hash(hash_tx_id, lock_seq);
    {
      CriticalGuard(get_qs());
      wait_succ = wait_(node, tmp_node, is_placeholder);
      if (!wait_succ
       && Node::REMOTE_EXEC_SIDE == node->get_node_type()
       && is_placeholder) {
        // should not repost and free remote-exec-node
        // which became placeholder after wait
        // because it's still in lock wait queue
        // just wake up ctrl side to retry
        need_repost = false;
        if (OB_FAIL(on_remote_lock_release_(*node))) {
          TRANS_LOG(WARN, "inform remote lock release fail", K(ret), KPC(node));
        }
      }
    }
    if (NULL != tmp_node) {
      WaitQuiescent(get_qs());
    }
    if (!wait_succ && need_repost) {
      repost(node);
    } else {
      // remove the repeated calculations
      node->try_lock_times_--;
    }
  }

  return ret;
}

void ObLockWaitMgr::wakeup(const ObTabletID &tablet_id, const Key& key)
{
  TRANS_LOG(TRACE, "LockWaitMgr.wakeup.byRowKey", K(tablet_id), K(key), K(lbt()));
  wakeup_(LockHashHelper::hash_rowkey(tablet_id, key));
}

void ObLockWaitMgr::wakeup(const ObTransID &tx_id)
{
  TRANS_LOG(TRACE, "LockWaitMgr.wakeup.byTransID", K(tx_id), K(lbt()));
  wakeup_(LockHashHelper::hash_trans(tx_id));
}

void ObLockWaitMgr::wakeup(const ObLockID &lock_id)
{
  TRANS_LOG(TRACE, "LockWaitMgr.wakeup.byLockID", K(lock_id), K(lbt()));
  wakeup_(LockHashHelper::hash_lock_id(lock_id));
}

int ObLockWaitMgr::notify_killed_session(const uint32_t sess_id)
{
  ObSpinLockGuard guard(killed_sessions_lock_);
  return killed_sessions_[killed_sessions_index_].push_back(SessPair{sess_id});
}

void ObLockWaitMgr::fetch_killed_sessions_(KilledSessionArray* &sessions)
{
  ObSpinLockGuard guard(killed_sessions_lock_);

  killed_sessions_index_ = 1 - killed_sessions_index_;
  if (0 != killed_sessions_index_
      && 1 != killed_sessions_index_) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected killed session index", K(killed_sessions_index_));
  }
  killed_sessions_[killed_sessions_index_].reset();
  sessions = killed_sessions_ + (1 - killed_sessions_index_);
}

bool ObLockWaitMgr::is_killed_session_(KilledSessionArray *sessions,
                                           const uint32_t sess_id)
{
  bool is_killed_session = false;

  for (int64_t i = 0; i < sessions->count(); i++) {
    if (sess_id == (*sessions)[i].sess_id_) {
      is_killed_session = true;
    }
  }

  return is_killed_session;
}

int64_t ObLockWaitMgr::calc_holder_tx_lock_timestamp(const int64_t holder_tx_start_time, const int64_t holder_data_seq_num)
{
  return holder_tx_start_time + ObTxSEQ::cast_from_int(holder_data_seq_num).get_seq();
}

void ObLockWaitMgr::begin_row_lock_wait_event(const Node * const node)
{
  rpc::ObRequest* req = CONTAINER_OF((const rpc::ObLockWaitNode *)node, rpc::ObRequest, lock_wait_node_);
  if (OB_NOT_NULL(req)) {
      ObDiagnosticInfo *di = req->get_type() == rpc::ObRequest::OB_MYSQL
        ? reinterpret_cast<observer::ObSMConnection *>(SQL_REQ_OP.get_sql_session(req))->get_diagnostic_info()
        : req->get_diagnostic_info();
      if (OB_NOT_NULL(di)) {
        ObActiveSessionStat &ash_stat = di->get_ash_stat();
        ash_stat.begin_row_lock_wait_event();
        ash_stat.block_sessid_ = node->holder_sessid_;
      }
    }
}

void ObLockWaitMgr::end_row_lock_wait_event(const Node * const node)
{
  if (oceanbase::lib::is_diagnose_info_enabled()) {
    rpc::ObRequest* req = CONTAINER_OF((const rpc::ObLockWaitNode *)node, rpc::ObRequest, lock_wait_node_);
    if (OB_NOT_NULL(req)) {
      ObDiagnosticInfo *di = req->get_type() == rpc::ObRequest::OB_MYSQL
        ? reinterpret_cast<observer::ObSMConnection *>(SQL_REQ_OP.get_sql_session(req))->get_diagnostic_info()
        : req->get_diagnostic_info();
      if (OB_NOT_NULL(di)) {
        ObActiveSessionStat &ash_stat = di->get_ash_stat();
        ash_stat.end_row_lock_wait_event();
        ash_stat.block_sessid_ = 0;
      }
    }
  }
}

void ObLockWaitMgr::set_ash_rowlock_diag_info(const ObRowConflictInfo &cflict_info)
{
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(holder_tx_id_, cflict_info.conflict_tx_id_);
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(holder_data_seq_num_, cflict_info.conflict_tx_hold_seq_.get_seq());
  int64_t holder_lock_timestamp = calc_holder_tx_lock_timestamp(cflict_info.holder_tx_start_time_, cflict_info.conflict_tx_hold_seq_.get_seq());
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(holder_lock_timestamp_, holder_lock_timestamp);
  GET_DIAGNOSTIC_INFO->get_ash_stat().block_sessid_ = cflict_info.holder_sess_id_;
}

}; // end namespace lockwaitmgr
}; // end namespace oceanbase
