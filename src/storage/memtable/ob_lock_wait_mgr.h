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
#include "lib/oblog/ob_log_module.h"
#include "lib/stat/ob_diagnose_info.h"
#include "mvcc/ob_row_lock.h"
#include "ob_memtable_key.h"
#include "observer/ob_server_struct.h"
#include "rpc/ob_request.h"
#include "share/ob_thread_pool.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
namespace observer {
class ObAllVirtualLockWaitStat;
}

namespace memtable {
class ObLockWaitMgr : public share::ObThreadPool {
public:
  friend class ObDeadLockChecker;
  friend class observer::ObAllVirtualLockWaitStat;

public:
  enum { LOCK_BUCKET_COUNT = 65536 };
  typedef ObMemtableKey Key;
  typedef rpc::ObLockWaitNode Node;
  typedef FixedHash2<Node> Hash;

public:
  ObLockWaitMgr();
  ~ObLockWaitMgr();

  int init();
  bool is_inited()
  {
    return is_inited_;
  };
  void destroy();
  // periodicly check requests which may be killed or tmeout
  void run1();
  void wait()
  {
    share::ObThreadPool::wait();
  }
  // Preprocessing before handling the request, which primarily sets up the
  // local variables, thread_node and hold_key.
  void setup(Node& node, int64_t recv_ts);
  // clear the local variable, thread_node. NB: we should wakeup the reqyest
  // based on the thread_key, because the key for the request may be changed
  void clear_thread_node()
  {
    get_thread_node() = nullptr;
  }
  // When the request ends, the thread worker will check whether the retry is
  // needed. And if so, it will push the request into the lock_wait_mgr based on
  // whether request encounters a conflict and needs to retry
  bool post_process(bool need_retry, bool& need_wait);
  void delay_header_node_run_ts(const Key& key);
  // setup the retry parameter on the request
  int post_lock(int tmp_ret, ObRowLock& lock, const Key& key, int64_t timeout, const bool is_remote_sql,
      const bool can_elr, const int64_t total_trans_node_cnt, uint32_t ctx_desc);
  int post_lock(int tmp_ret, const ObIMvccCtx& ctx, const uint64_t table_id, const ObStoreRowkey& key, int64_t timeout,
      const int64_t total_trans_node_cnt, uint32_t ctx_desc);
  // when removing the callbacks of uncommitted transaction, we need transfer
  // the conflict dependency from rows to transactions
  int delegate_waiting_querys_to_trx(const Key& key, const ObIMvccCtx& ctx);
  // wakeup the request waiting on the row
  void wakeup(const Key& key);
  // wakeup the request waiting on the transaction
  void wakeup(const uint32_t ctx_desc);

protected:
  // obtain the request waiting on the row or transaction
  Node* fetch_waiter(uint64_t hash);
  // check whether there exits requests already timeout or need be
  // retried(session is killed, deadlocked or son on), and wakeup and retry them
  ObLink* check_timeout();
  // reclaim the chained requests
  void retire_node(ObLink*& tail, Node* node);
  // wakeup the request and put into the thread worker queue
  virtual int repost(Node* node);

private:
  int64_t get_wait_lock_timeout(int64_t timeout);
  bool wait(Node* node);
  Node* next(Node*& iter, Node* target);
  Node* get(uint64_t hash);
  void wakeup(uint64_t hash);

private:
  Node*& get_thread_node()
  {
    static __thread Node* node = NULL;
    return node;
  }

  uint64_t& get_thread_hold_key()
  {
    static __thread uint64_t hold_key = 0;
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

  bool check_wakeup_seq(uint64_t hash, int64_t lock_seq, bool& standalone_task)
  {
    bool bool_ret = (ATOMIC_LOAD(&sequence_[(hash >> 1) % LOCK_BUCKET_COUNT]) == lock_seq);
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

public:
  int fullfill_row_key(uint64_t hash, char* row_key, int64_t length);

private:
  memtable::MemtableIDMap* mt_id_map_;
};

inline ObLockWaitMgr& get_global_lock_wait_mgr()
{
  static ObLockWaitMgr lock_mgr;
  return lock_mgr;
}

};  // end namespace memtable
};  // end namespace oceanbase

#endif /* OCEANBASE_MEMTABLE_OB_LOCK_WAIT_MGR_H_ */
