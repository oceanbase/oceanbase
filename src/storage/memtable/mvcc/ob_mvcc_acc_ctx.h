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

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_MVCC_READ_CTX_
#define OCEANBASE_MEMTABLE_MVCC_OB_MVCC_READ_CTX_

#include "share/ob_define.h"
#include "storage/tx/ob_trans_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/tx_table/ob_tx_table_guards.h"
#include "storage/memtable/ob_concurrent_control.h"
#include "storage/tx_table/ob_tx_table_interface.h"

namespace oceanbase
{
namespace transaction {
class ObPartTransCtx;
}

namespace storage {
class ObTxTable;
class ObTxTableGuard;
class ObTxTableGuards;
}

namespace memtable
{
class ObQueryAllocator;
class ObMemtableCtx;

class ObMvccAccessCtx
{
public:
  ObMvccAccessCtx()
    : type_(T::INVL),
      abs_lock_timeout_ts_(-1),
      tx_lock_timeout_us_(-1),
      snapshot_(),
      tx_table_guards_(),
      tx_id_(),
      tx_desc_(NULL),
      tx_ctx_(NULL),
      mem_ctx_(NULL),
      tx_scn_(),
      write_flag_(),
      handle_start_time_(OB_INVALID_TIMESTAMP),
      is_standby_read_(false),
      lock_wait_start_ts_(0)
  {}
  ~ObMvccAccessCtx() {
    type_ = T::INVL;
    abs_lock_timeout_ts_ = -1;
    tx_lock_timeout_us_ = -1;
    tx_table_guards_.reset();
    tx_id_.reset();
    tx_desc_ = NULL;
    tx_ctx_ = NULL;
    mem_ctx_ = NULL;
    tx_scn_.reset();
    write_flag_.reset();
    handle_start_time_ = OB_INVALID_TIMESTAMP;
    is_standby_read_ = false;
  }
  void reset() {
    if (is_write() && OB_UNLIKELY(tx_ctx_)) {
      warn_tx_ctx_leaky_();
    }
    type_ = T::INVL;
    abs_lock_timeout_ts_ = -1;
    tx_lock_timeout_us_ = -1;
    snapshot_.reset();
    tx_table_guards_.reset();
    tx_id_.reset();
    tx_desc_ = NULL;
    tx_ctx_ = NULL;
    mem_ctx_ = NULL;
    tx_scn_.reset();
    write_flag_.reset();
    handle_start_time_ = OB_INVALID_TIMESTAMP;
    is_standby_read_ = false;
  }
  bool is_valid() const {
    switch(type_) {
    case T::STRONG_READ: return is_read_valid__();
    case T::WEAK_READ: return is_read_valid__();
    case T::WRITE: return is_write_valid__();
    case T::REPLAY: return is_replay_valid__();
    default: return false;
    }
  }
  bool is_write_valid__() const {
    return abs_lock_timeout_ts_ >= 0
      && snapshot_.is_valid()
      && tx_ctx_
      && mem_ctx_
      && tx_scn_.is_valid()
      && tx_id_.is_valid()
      && tx_table_guards_.is_valid();
  }
  bool is_replay_valid__() const {
    return tx_ctx_
      && mem_ctx_
      && tx_id_.is_valid();
  }
  bool is_read_valid__() const {
    return abs_lock_timeout_ts_ >= 0
      && snapshot_.is_valid()
      && tx_table_guards_.is_valid()
      && (!tx_ctx_ || mem_ctx_);
  }
  void init_read(transaction::ObPartTransCtx *tx_ctx, /* nullable */
                 ObMemtableCtx *mem_ctx, /* nullable */
                 const storage::ObTxTableGuard &tx_table_guard,
                 const transaction::ObTxSnapshot &snapshot,
                 const int64_t abs_lock_timeout,
                 const int64_t tx_lock_timeout,
                 const bool is_weak_read)
  {
    reset();
    type_ = is_weak_read ? T::WEAK_READ : T::STRONG_READ;
    tx_ctx_ = tx_ctx;
    mem_ctx_ = mem_ctx;
    tx_table_guards_.tx_table_guard_ = tx_table_guard;
    snapshot_ = snapshot;
    abs_lock_timeout_ts_ = abs_lock_timeout;
    tx_lock_timeout_us_ = tx_lock_timeout;
  }
  // light read, used by storage background merge/compaction routine
  void init_read(const storage::ObTxTableGuard &tx_table_guard,
                 const share::SCN snapshot_version,
                 const int64_t timeout,
                 const int64_t tx_lock_timeout)
  {
    transaction::ObTxSnapshot snapshot;
    snapshot.version_ = snapshot_version;
    init_read(NULL, NULL, tx_table_guard, snapshot, timeout, tx_lock_timeout, false);
  }
  void init_write(transaction::ObPartTransCtx &tx_ctx,
                  ObMemtableCtx &mem_ctx,
                  const transaction::ObTransID &tx_id,
                  const transaction::ObTxSEQ tx_scn,
                  transaction::ObTxDesc &tx_desc,
                  const storage::ObTxTableGuard &tx_table_guard,
                  const transaction::ObTxSnapshot &snapshot,
                  const int64_t abs_lock_timeout,
                  const int64_t tx_lock_timeout,
                  const concurrent_control::ObWriteFlag write_flag)
  {
    reset();
    type_ = T::WRITE;
    tx_ctx_ = &tx_ctx;
    mem_ctx_ = &mem_ctx;
    tx_id_ = tx_id;
    tx_scn_ = tx_scn;
    tx_desc_ = &tx_desc;
    tx_table_guards_.tx_table_guard_ = tx_table_guard;
    snapshot_ = snapshot;
    abs_lock_timeout_ts_ = abs_lock_timeout;
    tx_lock_timeout_us_ = tx_lock_timeout;
    write_flag_ = write_flag;
  }

  void set_src_tx_table_guard(const storage::ObTxTableGuard &tx_table_guard,
                              storage::ObLSHandle &src_ls_handle)
  {
    tx_table_guards_.src_tx_table_guard_ = tx_table_guard;
    tx_table_guards_.src_ls_handle_ = src_ls_handle;
  }
  void set_write_flag(const concurrent_control::ObWriteFlag write_flag)
  {
    write_flag_ = write_flag;
  }
  void set_abs_lock_timeout_ts(const int64_t abs_lock_timeout)
  {
    abs_lock_timeout_ts_ = abs_lock_timeout;
  }
  void init_replay(transaction::ObPartTransCtx &tx_ctx,
                   ObMemtableCtx &mem_ctx,
                   const transaction::ObTransID &tx_id)
  {
    reset();
    type_ = T::REPLAY;
    tx_ctx_ = &tx_ctx;
    mem_ctx_ = &mem_ctx;
    tx_id_ = tx_id;
  }
  const transaction::ObTransID &get_tx_id() const {
    return tx_id_;
  }
  share::SCN get_snapshot_version() const {
    return snapshot_.version_;
  }

  storage::ObTxTableGuards &get_tx_table_guards() {
    return tx_table_guards_;
  }
  ObMemtableCtx *get_mem_ctx() const {
    return mem_ctx_;
  }
  transaction::ObTxDesc *get_tx_desc() const {
    return tx_desc_;
  }
  int64_t get_lock_wait_start_ts() const { return lock_wait_start_ts_; }
  void set_lock_wait_start_ts(const int64_t lock_wait_start_ts)
  { lock_wait_start_ts_ = lock_wait_start_ts; }
  bool is_read() const { return type_ == T::STRONG_READ || type_ == T::WEAK_READ; }
  bool is_weak_read() const { return type_ == T::WEAK_READ; }
  bool is_write() const { return type_ == T::WRITE; }
  bool is_replay() const { return type_ == T::REPLAY; }
  int64_t eval_lock_expire_ts(int64_t lock_wait_start_ts = 0) const {
    int64_t expire_ts = OB_INVALID_TIMESTAMP;
    if (tx_lock_timeout_us_ >= 0) {
      // Case 1: When tx_lock_timeout_us is not less than 0, we need to calculate the timeout timestamp for waiting for
      // the lock (by adding tx_lock_timeout_us to the timestamp of when we start waiting for the lock, i.e.
      // lock_wait_start_ts), and take the minimum value between this timeout timestamp and abs_lock_timeout_ts (which
      // is calcualted from select-for-update timeout and ob_query_timeout) as the value for absolute timeout timestamp.
      lock_wait_start_ts = lock_wait_start_ts > 0 ? lock_wait_start_ts : ObTimeUtility::current_time();
      expire_ts = MIN(lock_wait_start_ts + tx_lock_timeout_us_, abs_lock_timeout_ts_);
    } else {
      // Case 2: When tx_lock_timeout_us is less than 0, we use abs_lock_timeout_ts (which is calcualted from
      // select-for-update timeout and ob_query_timeout) as absolute timeout timestamp .
      expire_ts = abs_lock_timeout_ts_;
    }
    return expire_ts;
  }
  TO_STRING_KV(K_(type),
               K_(abs_lock_timeout_ts),
               K_(tx_lock_timeout_us),
               K_(snapshot),
               K_(tx_table_guards),
               K_(tx_id),
               KPC_(tx_desc),
               KP_(tx_ctx),
               KP_(mem_ctx),
               K_(tx_scn),
               K_(write_flag),
               K_(handle_start_time),
               K_(lock_wait_start_ts));
private:
  void warn_tx_ctx_leaky_();
public: // NOTE: those field should only be accessed by txn relative routine
  enum class T { INVL, STRONG_READ, WEAK_READ, WRITE, REPLAY } type_;
  // abs_lock_timeout_ts is the minimum value between the timeout timestamp of
  // the 'select for update' SQL statement and the timeout timestamp of the
  // dml_param / scan_param (which is calculated from ob_query_timeout).
  int64_t abs_lock_timeout_ts_;
  // tx_lock_timeout_us is defined as a system variable `ob_trx_lock_timeout`,
  // as the timeout of waiting on the WW conflict. it timeout reached
  // return OB_ERR_EXCLUSIVE_LOCK_CONFLICT error to SQL
  // SQL will stop retry, otherwise return OB_TRY_LOCK_ROW_CONFLICT, SQL will
  // retry until timeout
  // - When ob_trx_lock_timeout is smaller than 0, the timeout is equal to
  //   ob_query_timeout
  // - When ob_trx_lock_timeout is bigger than 0, the timeout is equal to the
  //   minimum between ob_query_timeout and ob_trx_lock_timeout
  // - When ob_trx_lock_timeout is equal to 0, it means never wait
  int64_t tx_lock_timeout_us_;
  transaction::ObTxSnapshot snapshot_;
  storage::ObTxTableGuards tx_table_guards_;  // for transfer query
  // specials for MvccWrite
  transaction::ObTransID tx_id_;
  transaction::ObTxDesc *tx_desc_;             // the txn descriptor
  transaction::ObPartTransCtx *tx_ctx_;        // the txn context
  ObMemtableCtx *mem_ctx_;                     // memtable-ctx
  transaction::ObTxSEQ tx_scn_;                             // the change's number of this modify
  concurrent_control::ObWriteFlag write_flag_; // the write flag of the write process

  // this was used for runtime metric
  int64_t handle_start_time_;

  bool is_standby_read_;
protected:
  int64_t lock_wait_start_ts_;
};
} // memtable
} // oceanbase

#endif //OCEANBASE_MEMTABLE_MVCC_OB_MVCC_READ_CTX_
