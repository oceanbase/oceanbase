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

#ifndef OCEANBASE_MEMTABLE_OB_LOCK_WAIT_MGR_H_
#define OCEANBASE_MEMTABLE_OB_LOCK_WAIT_MGR_H_

#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_qsync.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/rowid/ob_urowid.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/utility.h"
#include "ob_memtable_key.h"
#include "observer/ob_server_struct.h"
#include "rpc/ob_lock_wait_node.h"
#include "rpc/ob_request.h"
#include "share/deadlock/ob_deadlock_detector_common_define.h"
#include "share/ob_thread_pool.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"
#include "share/ob_delegate.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualLockWaitStat;
}
namespace memtable
{
using namespace transaction;
using namespace share::detector;

/**********[for DeadLock detector]**********/
// for record row holder info
class RowHolderMapper {
public:
  RowHolderMapper() = default;
  ~RowHolderMapper() { map_.destroy(); }
  int init() { return map_.init("LockWaitMgr", MTL_ID()); }
  void set_hash_holder(const ObTabletID &tablet_id,
                       const memtable::ObMemtableKey &key,
                       const transaction::ObTransID &tx_id);
  void reset_hash_holder(const ObTabletID &tablet_id,
                         const ObMemtableKey &key,
                         const ObTransID &tx_id);
  int get_hash_holder(uint64_t hash, ObTransID &holder) { return map_.get(ObIntWarp(hash), holder); }
  int get_rowkey_holder(const ObTabletID &tablet_id,
                        const memtable::ObMemtableKey &key,
                        transaction::ObTransID &holder);
  void dump_mapper_info() const {
    int64_t count = map_.count();
    int64_t bkt_cnt = map_.get_bkt_cnt();
    TRANS_LOG(INFO, "report RowHolderMapper summary info", K(count), K(bkt_cnt));
  }
  void clear() { map_.clear(); }
private:
  ObLinearHashMap<ObIntWarp, ObTransID, UniqueMemMgrTag> map_;
};

class DeadLockBlockCallBack {
public:
  DeadLockBlockCallBack(RowHolderMapper &mapper, uint64_t hash) : mapper_(mapper), hash_(hash) {}
  int operator()(ObIArray<ObDependencyResource> &resource_array, bool &need_remove) {
    int ret = OB_SUCCESS;
    UserBinaryKey user_key;
    ObTransID trans_id;
    ObAddr trans_scheduler;
    ObDependencyResource resource;
    #define PRINT_WRAPPER KR(ret), K_(hash), K(trans_id), K(trans_scheduler)
    if (OB_FAIL(mapper_.get_hash_holder(hash_, trans_id))) {
      DETECT_LOG(WARN, "get hash holder failed", PRINT_WRAPPER);
    } else if (OB_FAIL(user_key.set_user_key(trans_id))) {
      DETECT_LOG(WARN, "set user key failed", PRINT_WRAPPER);
    } else if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_conflict_trans_scheduler(trans_id, trans_scheduler))) {
      DETECT_LOG(WARN, "get trans scheduler failed", PRINT_WRAPPER);
    } else if (OB_FAIL(resource.set_args(trans_scheduler, user_key))) {
      DETECT_LOG(WARN, "resource set args failed", PRINT_WRAPPER);
    } else if (OB_FAIL(resource_array.push_back(resource))) {
      DETECT_LOG(WARN, "fail to push resource to array", PRINT_WRAPPER);
    }
    #undef PRINT_WRAPPER
    need_remove = false;
    return ret;
  }
private:
  RowHolderMapper &mapper_;
  uint64_t hash_;
};

class LocalDeadLockCollectCallBack {
public:
  LocalDeadLockCollectCallBack(const ObTransID &self_trans_id,
                               const char *node_key_buffer,
                               const uint32_t sess_id) :
  self_trans_id_(self_trans_id),
  sess_id_(sess_id) {
    int64_t str_len = strlen(node_key_buffer);// not contain '\0'
    int64_t min_len = str_len > 127 ? 127 : str_len;
    memcpy(node_key_buffer_, node_key_buffer, min_len);
    node_key_buffer_[min_len] = '\0';
  }
  int operator()(ObDetectorUserReportInfo &info) {
    int ret = OB_SUCCESS;
    constexpr int64_t trans_id_str_len = 128;
    constexpr int64_t row_key_str_len = 128;
    constexpr int64_t current_sql_str_len = 256;
    char * buffer_trans_id = nullptr;
    char * buffer_row_key = nullptr;
    char * buffer_current_sql = nullptr;
    SessionGuard sess_guard;
    int step = 0;
    if (++step && OB_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(sess_id_, sess_guard))) {
    } else if (++step && !sess_guard.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_UNLIKELY(nullptr == (buffer_trans_id = (char*)ob_malloc(trans_id_str_len, "deadlockCB")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_UNLIKELY(nullptr == (buffer_row_key = (char*)ob_malloc(row_key_str_len, "deadlockCB")))) {
      ob_free(buffer_trans_id);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_UNLIKELY(nullptr == (buffer_current_sql = (char*)ob_malloc(current_sql_str_len, "deadlockCB")))) {
      ob_free(buffer_trans_id);
      ob_free(buffer_row_key);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObSharedGuard<char> temp_guard;
      (void) self_trans_id_.to_string(buffer_trans_id, trans_id_str_len);
      ObTransDeadlockDetectorAdapter::copy_str_and_translate_apostrophe(node_key_buffer_,
                                                                        strlen(node_key_buffer_),
                                                                        buffer_row_key,
                                                                        row_key_str_len);
      const ObString &cur_query_str = sess_guard->get_current_query_string();
      ObTransDeadlockDetectorAdapter::copy_str_and_translate_apostrophe(cur_query_str.ptr(),
                                                                        cur_query_str.length(),
                                                                        buffer_current_sql,
                                                                        current_sql_str_len);
      if (++step && OB_FAIL(temp_guard.assign((char*)"transaction", [](char*){}))) {
      } else if (++step && OB_FAIL(info.set_module_name(temp_guard))) {
      } else if (++step && OB_FAIL(temp_guard.assign(buffer_trans_id, [](char* buffer){ ob_free(buffer); }))) {
      } else if (FALSE_IT(buffer_trans_id = nullptr)) {
      } else if (++step && OB_FAIL(info.set_visitor(temp_guard))) {
      } else if (++step && OB_FAIL(temp_guard.assign(buffer_row_key, [](char* buffer){ ob_free(buffer);}))) {
      } else if (FALSE_IT(buffer_row_key = nullptr)) {
      } else if (++step && OB_FAIL(info.set_resource(temp_guard))) {
      } else if (++step && OB_FAIL(temp_guard.assign(buffer_current_sql, [](char* buffer){ ob_free(buffer);}))) {
      } else if (FALSE_IT(buffer_current_sql = nullptr)) {
      } else if (++step && OB_FAIL(info.set_extra_info("current sql", temp_guard))) {
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(buffer_trans_id)) {
        ob_free(buffer_trans_id);
      }
      if (OB_NOT_NULL(buffer_row_key)) {
        ob_free(buffer_row_key);
      }
      if (OB_NOT_NULL(buffer_current_sql)) {
        ob_free(buffer_current_sql);
      }
      DETECT_LOG(WARN, "generate and assign string failed in deadlock call back", KR(ret), K(step));
    }
    return ret;
  }
private:
  ObTransID self_trans_id_;
  char node_key_buffer_[128];
  const uint32_t sess_id_;
};
/*******************************************/

class ObLockWaitMgr: public share::ObThreadPool
{
public:
  friend class ObDeadLockChecker;
  friend class observer::ObAllVirtualLockWaitStat;

public:
  enum { LOCK_BUCKET_COUNT = 16384};
  static const int64_t OB_SESSPAIR_COUNT = 16;
  typedef ObMemtableKey Key;
  typedef rpc::ObLockWaitNode Node;
  typedef FixedHash2<Node> Hash;
  struct SessPair {
    uint32_t sess_id_;
    TO_STRING_KV(K(sess_id_));
  };
  typedef ObSEArray<SessPair, OB_SESSPAIR_COUNT> DeadlockedSessionArray;

public:
  ObLockWaitMgr();
  ~ObLockWaitMgr();

  static int mtl_init(ObLockWaitMgr *&lock_wait_mgr);
  int init();
  bool is_inited() { return is_inited_; };
  int start();
  void stop();
  void destroy();
  // periodicly check requests which may be killed or tmeout
  void run1();
  void wait() { share::ObThreadPool::wait(); }
  // Preprocessing before handling the request, which primarily sets up the
  // local variables, thread_node and hold_key.
  void setup(Node &node, int64_t recv_ts)
  {
    node.reset_need_wait();
    node.recv_ts_ = recv_ts;
    get_thread_node() = &node;
    get_thread_hold_key() = node.hold_key_;
    node.hold_key_ = 0;
  }
  // clear the local variable, thread_node. NB: we should wakeup the reqyest
  // based on the thread_key, because the key for the request may be changed
  static void clear_thread_node() { get_thread_node() = nullptr; }
  // When the request ends, the thread worker will check whether the retry is
  // needed. And if so, it will push the request into the lock_wait_mgr based on
  // whether request encounters a conflict and needs to retry
  bool post_process(bool need_retry, bool& need_wait);
  void delay_header_node_run_ts(const uint64_t hash);
  // setup the retry parameter on the request
  int post_lock(const int tmp_ret,
                const ObTabletID &tablet_id,
                const ObStoreRowkey &key,
                const int64_t timeout,
                const bool is_remote_sql,
                const int64_t last_compact_cnt,
                const int64_t total_trans_node_cnt,
                const uint32_t sess_id,
                const transaction::ObTransID &tx_id,
                const transaction::ObTransID &holder_tx_id,
                const ObLSID &ls_id,
                ObFunction<int(bool &, bool &)> &rechecker);
  int post_lock(const int tmp_ret,
                const ObTabletID &tablet_id,
                const transaction::tablelock::ObLockID &lock_id,
                const int64_t timeout,
                const bool is_remote_sql,
                const int64_t last_compact_cnt,
                const int64_t total_trans_node_cnt,
                const uint32_t sess_id,
                const transaction::ObTransID &tx_id,
                const transaction::ObTransID &holder_tx_id,
                const transaction::tablelock::ObTableLockMode &lock_mode,
                const ObLSID &ls_id,
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
  DELEGATE_WITH_RET(row_holder_mapper_, set_hash_holder, void);
  DELEGATE_WITH_RET(row_holder_mapper_, get_hash_holder, int);
  DELEGATE_WITH_RET(row_holder_mapper_, reset_hash_holder, void);
  DELEGATE_WITH_RET(row_holder_mapper_, get_rowkey_holder, int);

  Node* next(Node*& iter, Node* target);

  static Node*& get_thread_node()
  {
    RLOCAL_INLINE(Node*, node);
    return node;
  }

protected:
  // obtain the request waiting on the row or transaction
  Node* fetch_waiter(uint64_t hash);
  // check whether there exits requests already timeoutt or need be
  // retried(session is killed, deadlocked or son on), and wakeup and retry them
  ObLink* check_timeout();
  // reclaim the chained reuqests
  void retire_node(ObLink*& tail, Node* node);
  // wakeup the request and put into the thread worker queue
  virtual int repost(Node* node);

private:
  int64_t get_wait_lock_timeout(int64_t timeout);
  bool wait(Node* node);
  Node* get(uint64_t hash);
  void wakeup(uint64_t hash);
private:

  static uint64_t& get_thread_hold_key()
  {
    RLOCAL_INLINE(uint64_t, hold_key);
    return hold_key;
  }

  ObQSync& get_qs()
  {
    static ObQSync qsync;
    return qsync;
  }

  bool is_hash_empty()
  {
    bool is_empty = false;
    {
      CriticalGuard(get_qs());
      is_empty = hash_.is_empty();
    }
    return is_empty;
  }

  bool check_wakeup_seq(uint64_t hash, int64_t lock_seq, bool &standalone_task)
  {
    bool bool_ret = (ATOMIC_LOAD(&sequence_[(hash >> 1)% LOCK_BUCKET_COUNT]) == lock_seq);
    if (bool_ret) {
      standalone_task = false;
    } else {
      bool_ret = true;
      standalone_task = true;
    }
    return bool_ret;
  }
  int64_t get_seq(uint64_t hash)
  {
    return ATOMIC_LOAD(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT]);
  }

private:
  bool is_inited_;
  Hash hash_;
  int64_t sequence_[LOCK_BUCKET_COUNT];
  char hash_buf_[sizeof(SpHashNode) * LOCK_BUCKET_COUNT];
  int64_t last_check_session_idle_ts_;

public:
  int fullfill_row_key(uint64_t hash, char *row_key, int64_t length);
  int notify_deadlocked_session(const uint32_t sess_id);
private:
  // helper function for deadlock detector
  // register to the deadlock detector
  int register_to_deadlock_detector_(const transaction::ObTransID &self_tx_id,
                                     const transaction::ObTransID &blocked_tx_id,
                                     const Node * const node);
  bool is_deadlocked_session_(DeadlockedSessionArray *sessions,
                              const uint32_t sess_id);
  void fetch_deadlocked_sessions_(DeadlockedSessionArray *&sessions);
private:
  ObSpinLock deadlocked_sessions_lock_;
  int32_t deadlocked_sessions_index_;
  DeadlockedSessionArray deadlocked_sessions_[2];
private:
  RowHolderMapper row_holder_mapper_;
  int64_t total_wait_node_;
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
  rpc::ObLockWaitNode *node = memtable::ObLockWaitMgr::get_thread_node();
  if (OB_NOT_NULL(node)) {
    node->advance_stat(new_stat);
  }
}

}; // end namespace memtable
}; // end namespace oceanbase

#endif /* OCEANBASE_MEMTABLE_OB_LOCK_WAIT_MGR_H_ */
