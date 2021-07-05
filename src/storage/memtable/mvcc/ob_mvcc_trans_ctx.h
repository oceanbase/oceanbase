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

#ifndef OCEANBASE_MVCC_OB_MVCC_TRANS_CTX_
#define OCEANBASE_MVCC_OB_MVCC_TRANS_CTX_
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "ob_row_data.h"
#include "ob_mvcc_row.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
};
namespace memtable {
enum TransCallbackType {
  TCB_INVALID = 0,
  TCB_TRANS_COMMIT = 1,
  TCB_TRANS_ABORT = 2,
  TCB_STMT_COMMIT = 3,
  TCB_STMT_ABORT = 4,
  TCB_UNLOCK = 5,
  TCB_PENDING = 6,
  TCB_LINK = 7,
  // check rowkey
  TCB_CRC = 8,
  // do not check rowkey
  TCB_CRC2 = 9,
  TCB_STMT_DATA_RELOCATE = 10,
  TCB_TRANS_DATA_RELOCATE = 11,
  TCB_DELETE = 12,
  TCB_RELOCATE_DELETE = 14,
  TCB_HALF_STMT_COMMIT = 15,
  TCB_ELR_TRANS_PREPARING = 16,
  TCB_REMOVE_CALLBACK = 17,
  TCB_SUB_STMT_ABORT = 18,
  TCB_CRC4 = 19,

  // TEST_ONLY, used for print callback
  TCB_PRINT_CALLBACK = 100
};

enum ObTransDataRelocateType {
  // executing statement
  TRANS_STMT = 1,
  // replaying redo log
  REPLAY_REDO = 2,
  // ending transaction in leader
  TRANS_END = 3,
  // replaying commit log
  TRANS_REPLAY_END = 4,
  // triggering data relocation when leader takeover
  LEADER_TAKEOVER = 5
};

struct RedoDataNode {
  void set(const ObMemtableKey* key, const ObRowData& old_row, const ObRowData& new_row,
      const storage::ObRowDml dml_type, const uint32_t modify_count, const uint32_t acc_checksum, const int64_t version,
      const int32_t sql_no, const int32_t flag)
  {
    key_.encode(*key);
    old_row_ = old_row;
    new_row_ = new_row;
    dml_type_ = dml_type;
    modify_count_ = modify_count;
    acc_checksum_ = acc_checksum;
    version_ = version;
    sql_no_ = sql_no;
    flag_ = flag;
  }
  ObMemtableKey key_;
  ObRowData old_row_;
  ObRowData new_row_;
  storage::ObRowDml dml_type_;
  uint32_t modify_count_;
  uint32_t acc_checksum_;
  int64_t version_;
  int32_t sql_no_;
  int32_t flag_;
};

class ObMemtableCtx;
class ObMemtable;
class ObITransCallback {
public:
  ObITransCallback() : prev_(NULL), next_(NULL)
  {}
  virtual ~ObITransCallback()
  {}
  virtual int callback(
      const int type, const bool for_replay, const bool need_lock_for_write, ObMemtable* memtable = NULL)
  {
    UNUSED(type);
    UNUSED(for_replay);
    UNUSED(need_lock_for_write);
    UNUSED(memtable);
    return common::OB_SUCCESS;
  }
  virtual int callback(transaction::ObMemtableKeyArray& memtable_key_arr, ObMemtable* memtable)
  {
    UNUSED(memtable_key_arr);
    UNUSED(memtable);
    return common::OB_SUCCESS;
  }
  virtual bool on_memtable(const ObMemtable* const memtable)
  {
    UNUSED(memtable);
    return common::OB_NOT_IMPLEMENT;
  }
  virtual ObMemtable* get_memtable() const
  {
    return NULL;
  }
  virtual int32_t get_sql_no() const
  {
    return 0;
  }
  ObITransCallback* get_next() const
  {
    return next_;
  }
  ObITransCallback* get_prev() const
  {
    return prev_;
  }
  void set_next(ObITransCallback* node)
  {
    next_ = node;
  }
  void set_prev(ObITransCallback* node)
  {
    prev_ = node;
  }
  int append(ObITransCallback* node)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == node) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      ret = insert(node, node->next_);
    }
    return ret;
  }
  virtual int del()
  {
    return common::OB_SUCCESS;
  }
  virtual bool is_savepoint() const
  {
    return false;
  }
  int remove(ObITransCallback* cur)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == cur) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      ObITransCallback* prev = cur->prev_;
      ObITransCallback* next = cur->next_;
      if (NULL == prev || NULL == next) {
        ret = common::OB_INVALID_ARGUMENT;
      } else {
        prev->next_ = next;
        next->prev_ = prev;
      }
    }
    return ret;
  }
  virtual bool log_synced() const
  {
    return false;
  }
  virtual bool mark_for_logging()
  {
    return false;
  }
  virtual bool need_mark_logging()
  {
    return false;
  }
  virtual int log_sync(const int64_t log_ts)
  {
    UNUSED(log_ts);
    return common::OB_SUCCESS;
  }
  virtual int calc_checksum_before_log_ts(const int64_t log_ts)
  {
    UNUSED(log_ts);
    return common::OB_SUCCESS;
  }
  virtual int fetch_rollback_data_size(int64_t& rollback_data_size)
  {
    UNUSED(rollback_data_size);
    return common::OB_SUCCESS;
  }
  virtual int print_callback()
  {
    return common::OB_SUCCESS;
  }
  virtual int64_t get_data_size()
  {
    return 0;
  }

  VIRTUAL_TO_STRING_KV(KP(this), KP_(prev), KP_(next));

private:
  int insert(ObITransCallback* prev, ObITransCallback* next)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == prev || NULL == next) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      this->prev_ = prev;
      this->next_ = next;
      prev->next_ = this;
      next->prev_ = this;
    }
    return ret;
  }

private:
  ObITransCallback* prev_;
  ObITransCallback* next_;
};

class ObTransCallbackMgr;
class ObTransCallbackList {
public:
  ObTransCallbackList(ObTransCallbackMgr& callback_mgr) : callback_mgr_(callback_mgr)
  {
    reset();
  }
  ~ObTransCallbackList()
  {}
  void reset()
  {
    head_.set_prev(&head_);
    head_.set_next(&head_);
    length_ = 0;
  }
  // release memory when transaction finished
  void release_all_callback()
  {
    truncate(&head_);
  }
  ObITransCallback* get_guard()
  {
    return &head_;
  }
  int append(ObITransCallback* callback)
  {
    length_++;
    return callback->append(get_tail());
  }
  void truncate(ObITransCallback* end, const bool verbose = false);
  uint64_t count() const
  {
    return length_;
  }
  // iterate callback when data relocation
  int data_relocate_fifo_callback(ObITransCallback* start, const ObITransCallback* end, const int type,
      const bool for_replay, const bool need_lock_for_write, ObMemtable* memtable, ObITransCallback*& sub_trans_begin);
  int remove_callback_fifo_callback(const ObITransCallback* start, const ObITransCallback* end,
      ObMemtable* release_memtable, const ObITransCallback* sub_trans_begin, bool& move_forward, int64_t& cnt);
  int fifo_callback(ObITransCallback* start, const ObITransCallback* end, const int type, const bool for_replay);
  int lifo_callback(ObITransCallback* start, ObITransCallback* end, const int type, const bool for_replay);
  ObITransCallback* search_callback(
      const int32_t sql_no, const bool unsynced, ObITransCallback* sub_trans_begin, bool& hit)
  {
    hit = false;
    ObITransCallback* iter = NULL;
    for (iter = get_guard()->get_prev(); NULL != iter && iter != get_guard(); iter = iter->get_prev()) {
      if ((iter->get_sql_no() <= sql_no && !iter->is_savepoint()) || (unsynced && iter->log_synced())) {
        break;
      } else if (sub_trans_begin == iter) {
        hit = true;
      } else {
        // do nothing
      }
    }
    return iter;
  }
  int iterate_memtablekey_fifo_callback(transaction::ObMemtableKeyArray& memtable_key_arr, ObMemtable* memtable)
  {
    int ret = common::OB_SUCCESS;
    ObITransCallback* end = get_guard();

    if (OB_ISNULL(memtable)) {
      ret = common::OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "memtable is null", K(ret));
    } else {
      const int64_t iter_ts = ObTimeUtility::current_time();
      for (ObITransCallback* iter = get_guard()->get_next(); common::OB_SUCCESS == ret && NULL != iter && iter != end;
           iter = iter->get_next()) {
        ret = iter->callback(memtable_key_arr, memtable);
        if (ObTimeUtility::current_time() - iter_ts >= 300000) {
          // avoid iterating callback cost too much time
          ret = common::OB_ITER_STOP;
        }
      }
    }

    return ret;
  }
  ObITransCallback* get_tail()
  {
    return head_.get_prev();
  }

  int mark_frozen_data(
      const ObMemtable* const frozen_memtable, const ObMemtable* const active_memtable, bool& marked, int64_t& cb_cnt);
  int calc_checksum_before_log_ts(const ObITransCallback* start, const ObITransCallback* end, const int64_t log_ts);
  int fetch_rollback_data_size(const ObITransCallback* start, const ObITransCallback* end, int64_t& rollback_size);

private:
  const ObMemtable* get_first_memtable();

private:
  ObITransCallback head_;
  uint64_t length_;
  ObTransCallbackMgr& callback_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObTransCallbackList);
};

class ObTransCallbackMgr {
public:
  ObTransCallbackMgr(ObIMvccCtx& host)
      : host_(host),
        callback_list_(*this),
        sub_trans_begin_(NULL),
        sub_stmt_begin_(NULL),
        for_replay_(false),
        leader_changed_(false),
        rowlocks_released_(false),
        marked_mt_(NULL),
        pending_log_size_(0),
        flushed_log_size_(0)
  {}
  ~ObTransCallbackMgr()
  {}
  void reset()
  {
    callback_list_.reset();
    sub_trans_begin_ = NULL;
    sub_stmt_begin_ = NULL;
    leader_changed_ = false;
    rowlocks_released_ = false;
    marked_mt_ = NULL;
    pending_log_size_ = 0;
    flushed_log_size_ = 0;
  }
  ObIMvccCtx& get_ctx()
  {
    return host_;
  }
  int append(ObITransCallback* node)
  {
    int64_t size = node->is_savepoint() ? 0 : node->get_data_size();
    inc_pending_log_size(size);
    if (for_replay_) {
      inc_flushed_log_size(size);
    }
    return callback_list_.append(node);
  }
  uint64_t count() const
  {
    return callback_list_.count();
  }
  void trans_pending()
  {
    // fill transaction version
    (void)lifo_callback(guard(), TCB_PENDING);
    // unlock row latch
    (void)lifo_callback(guard(), TCB_UNLOCK);
    leader_changed_ = true;
  }
  void trans_link()
  {
    (void)lifo_callback(guard(), TCB_UNLOCK);
    (void)fifo_callback(guard(), TCB_LINK);
    leader_changed_ = true;
  }
  void trans_start()
  {
    reset();
  }
  void calc_checksum()
  {
    (void)fifo_callback(guard(), TCB_CRC);
  }
  void calc_checksum2()
  {
    (void)fifo_callback(guard(), TCB_CRC2);
  }
  void calc_checksum4()
  {
    (void)fifo_callback(guard(), TCB_CRC4);
  }
  void elr_trans_preparing()
  {
    (void)fifo_callback(guard(), TCB_ELR_TRANS_PREPARING);
  }
  int trans_end(const bool commit)
  {
    int ret = common::OB_SUCCESS;
    sub_trans_end(commit);
    if (commit) {
      ret = fifo_callback(guard(), TCB_TRANS_COMMIT);
    } else {
      ret = lifo_callback(guard(), TCB_TRANS_ABORT);
    }
    if (OB_SUCC(ret)) {
      (void)lifo_callback(guard(), TCB_UNLOCK);
      release_rowlocks();
      // must release memory before reset
      callback_list_.release_all_callback();
      reset();
    }
    return ret;
  }
  void sub_trans_begin()
  {
    sub_trans_begin_ = guard()->get_prev();
  }
  void sub_trans_end(const bool commit)
  {
    if (NULL != sub_trans_begin_) {
      if (commit) {
        (void)fifo_callback(sub_trans_begin_, TCB_STMT_COMMIT);
      } else {
        (void)lifo_callback(sub_trans_begin_, TCB_STMT_ABORT);
        (void)lifo_callback(sub_trans_begin_, TCB_UNLOCK);
        callback_list_.truncate(sub_trans_begin_);
      }
      sub_trans_begin_ = NULL;
    }
  }

  void sub_stmt_begin()
  {
    sub_stmt_begin_ = guard()->get_prev();
  }

  void sub_stmt_end(const bool commit)
  {
    if (NULL != sub_stmt_begin_) {
      if (commit) {
        // do nothing
      } else {
        (void)lifo_callback(sub_stmt_begin_, TCB_SUB_STMT_ABORT);
        (void)lifo_callback(sub_stmt_begin_, TCB_UNLOCK);
        callback_list_.truncate(sub_stmt_begin_);
      }
      sub_stmt_begin_ = NULL;
    }
  }

  int rollback_to(const common::ObPartitionKey& pkey, const int32_t sql_no, const bool is_replay,
      const bool need_write_log, const int64_t max_durable_log_ts, bool& has_calc_checksum);

  int truncate_to(const int32_t sql_no);

  void half_stmt_commit()
  {
    if (NULL != sub_trans_begin_) {
      (void)fifo_callback(sub_trans_begin_, TCB_HALF_STMT_COMMIT);
    }
  }

  ObTransCallbackList& get_callback_list()
  {
    return callback_list_;
  }
  void set_for_replay(const bool for_replay)
  {
    for_replay_ = for_replay;
  }
  bool is_for_replay() const
  {
    return for_replay_;
  }
  int data_relocate(ObMemtable* dst_memtable, const int data_relocate_type);
  int remove_callback_for_uncommited_txn(memtable::ObMemtable* memtable, int64_t& cnt);
  int get_memtable_key_arr(transaction::ObMemtableKeyArray& memtable_key_arr, ObMemtable* memtable);
  int register_savepoint_callback_(const common::ObPartitionKey& pkey, const int32_t sql_no);

  // mark transaction dirty when it is uncommitted and in specified memtable
  int mark_frozen_data(
      const ObMemtable* const frozen_memtable, const ObMemtable* const active_memtable, bool& marked, int64_t& cb_cnt);
  int get_cur_max_sql_no(int64_t& sql_no);

private:
  void release_rowlocks();

public:
  bool is_rowlocks_released() const;
  int calc_checksum_before_log_ts(const int64_t log_ts);
  int fetch_rollback_data_size(const ObITransCallback* point, int64_t& rollback_size);
  void inc_pending_log_size(const int64_t size);
  void inc_flushed_log_size(const int64_t size)
  {
    flushed_log_size_ += size;
  }
  void clear_pending_log_size()
  {
    ATOMIC_STORE(&pending_log_size_, 0);
  }
  int64_t get_pending_log_size()
  {
    return ATOMIC_LOAD(&pending_log_size_);
  }
  int64_t get_flushed_log_size()
  {
    return flushed_log_size_;
  }

  int rollback_calc_checksum_(
      const bool is_replay, const bool need_write_log, const int64_t max_durable_log_ts, bool& has_calc_checksum);
  int rollback_pending_log_size_(const bool need_write_log, const ObITransCallback* const end);
  void truncate_to_(ObITransCallback* end, const bool change_sub_trans_end);

private:
  ObITransCallback* guard()
  {
    return callback_list_.get_guard();
  }
  int fifo_callback(ObITransCallback* start, const int type)
  {
    return callback_list_.fifo_callback(start, callback_list_.get_guard(), type, for_replay_);
  }
  int lifo_callback(ObITransCallback* end, const int type)
  {
    return callback_list_.lifo_callback(callback_list_.get_guard(), end, type, for_replay_);
  }

private:
  ObIMvccCtx& host_;
  ObTransCallbackList callback_list_;
  ObITransCallback* sub_trans_begin_;
  ObITransCallback* sub_stmt_begin_;
  bool for_replay_;
  bool leader_changed_;
  bool rowlocks_released_;
  const ObMemtable* marked_mt_;  // check pointer only.. do not access
  // current log size in leader participant
  int64_t pending_log_size_;
  // current flushed log size in leader participant
  int64_t flushed_log_size_;
};

// class ObIMvccCtx;
class ObMvccRowCallback : public ObITransCallback {
public:
  ObMvccRowCallback(ObIMvccCtx& ctx, ObMvccRow& value, ObMemtable* memtable)
      : ctx_(ctx),
        value_(value),
        tnode_(NULL),
        data_size_(-1),
        memtable_(memtable),
        sql_no_(0),
        log_ts_(INT64_MAX),
        is_link_(false),
        is_write_lock_(false),
        is_relocate_out_(false),
        need_fill_redo_(true),
        stmt_committed_(false),
        is_savepoint_(false),
        marked_for_logging_(false)
  {}
  ObMvccRowCallback(ObMvccRowCallback& cb, ObMemtable* memtable)
      : ctx_(cb.ctx_),
        value_(cb.value_),
        tnode_(cb.tnode_),
        data_size_(cb.data_size_),
        memtable_(memtable),
        sql_no_(cb.sql_no_),
        log_ts_(INT64_MAX),
        is_link_(cb.is_link_),
        is_write_lock_(cb.is_write_lock_),
        is_relocate_out_(cb.is_relocate_out_),
        need_fill_redo_(cb.need_fill_redo_),
        stmt_committed_(cb.stmt_committed_),
        is_savepoint_(cb.is_savepoint_),
        marked_for_logging_(cb.marked_for_logging_)
  {
    (void)key_.encode(cb.key_.get_table_id(), cb.key_.get_rowkey());
  }
  virtual ~ObMvccRowCallback()
  {}
  void set_write_locked()
  {
    is_write_lock_ = true;
  }
  bool is_write_locked() const
  {
    return is_write_lock_;
  }
  int link_trans_node();
  void unlink_trans_node();
  void set_is_link()
  {
    is_link_ = true;
  }
  void set(const ObMemtableKey* key, ObMvccTransNode* node, const int64_t data_size, const ObRowData* old_row,
      const bool is_replay, const bool need_fill_redo, const int32_t sql_no)
  {
    UNUSED(is_replay);

    if (NULL != key) {
      key_.encode(*key);
    }

    tnode_ = node;
    data_size_ = data_size;
    if (NULL != old_row) {
      old_row_ = *old_row;
      if (old_row_.size_ == 0 && old_row_.data_ != NULL) {
        ob_abort();
      }
    } else {
      old_row_.reset();
    }
    need_fill_redo_ = need_fill_redo;
    sql_no_ = sql_no;
    if (tnode_) {
      tnode_->set_sql_sequence(sql_no_);
    }
  }

  int callback(
      const int type, const bool for_replay, const bool need_lock_for_write, ObMemtable* memtable = NULL) override;
  int callback(transaction::ObMemtableKeyArray& memtable_key_arr, ObMemtable* memtable) override;
  bool on_memtable(const ObMemtable* const memtable) override
  {
    return memtable == memtable_;
  }
  ObMemtable* get_memtable() const override
  {
    return memtable_;
  };
  int get_redo(RedoDataNode& node);
  ObIMvccCtx& get_ctx() const
  {
    return ctx_;
  }
  const ObRowData& get_old_row() const
  {
    return old_row_;
  }
  const ObMvccRow& get_mvcc_row() const
  {
    return value_;
  }
  ObMvccTransNode* get_trans_node()
  {
    return tnode_;
  }
  const ObMvccTransNode* get_trans_node() const
  {
    return tnode_;
  }
  const ObMemtableKey* get_key()
  {
    return &key_;
  }
  int get_memtable_key(uint64_t& table_id, common::ObStoreRowkey& rowkey) const;
  uint32_t get_ctx_descriptor() const;
  void set_stmt_committed(const bool stmt_committed)
  {
    stmt_committed_ = stmt_committed;
  }
  bool is_stmt_committed() const
  {
    return stmt_committed_;
  }
  void set_savepoint()
  {
    is_savepoint_ = true;
  }
  bool is_savepoint() const override
  {
    return is_savepoint_;
  }
  bool need_fill_redo() const
  {
    return need_fill_redo_;
  }
  int32_t get_sql_no() const override
  {
    return sql_no_;
  }
  int get_trans_id(transaction::ObTransID& trans_id) const;
  transaction::ObTransCtx* get_trans_ctx() const;
  int64_t to_string(char* buf, const int64_t buf_len) const override;
  int64_t get_log_ts() const
  {
    return log_ts_;
  }
  void set_log_ts(const int64_t log_ts)
  {
    if (INT64_MAX == log_ts_) {
      log_ts_ = log_ts;
    }
  }
  bool log_synced() const override
  {
    return INT64_MAX != log_ts_;
  }
  int64_t get_data_size() override
  {
    return data_size_;
  }
  virtual int del() override;
  int check_sequential_relocate(const ObMemtable* memtable, bool& is_sequential_relocate) const;
  virtual bool mark_for_logging() override
  {
    if (marked_for_logging_) {
      return false;
    } else {
      marked_for_logging_ = true;
      return true;
    }
  }
  virtual bool need_mark_logging() override
  {
    return !marked_for_logging_ && need_fill_redo_;
  }
  virtual int log_sync(const int64_t log_ts) override;
  virtual int calc_checksum_before_log_ts(const int64_t log_ts) override;
  virtual int fetch_rollback_data_size(int64_t& rollback_size) override;
  virtual int print_callback() override;

private:
  int row_unlock();
  int row_unlock_v2();
  int trans_commit(const bool for_replay);
  int trans_abort();
  int stmt_commit(const bool half_commit);
  int stmt_abort();
  int sub_stmt_abort();
  int row_pending();
  int row_link();
  int row_crc();
  int row_crc2();
  int row_crc4();
  int row_remove_callback();
  int link_and_get_next_node(ObMvccTransNode*& next);
  int data_relocate(const bool for_replay, const bool need_lock_for_write, ObMemtable* memtable);
  int row_delete();
  int elr_trans_preparing();
  int merge_memtable_key(transaction::ObMemtableKeyArray& memtable_key_arr, ObMemtableKey& memtable_key);
  int32_t generate_redo_flag(const bool is_savepoint)
  {
    return (is_savepoint ? 1 : 0);
  }
  int dec_pending_cb_count();
  void mark_tnode_overflow(const int64_t log_ts);

private:
  ObIMvccCtx& ctx_;
  ObMemtableKey key_;
  ObMvccRow& value_;
  ObMvccRow undo_;
  ObMvccTransNode* tnode_;
  int64_t data_size_;
  ObRowData old_row_;
  ObMemtable* memtable_;
  int32_t sql_no_;
  // log_id in partition
  int64_t log_ts_;
  struct {
    bool is_link_ : 1;
    bool is_write_lock_ : 1;
    bool is_relocate_out_ : 1;
    bool need_fill_redo_ : 1;
    bool stmt_committed_ : 1;
    bool is_savepoint_ : 1;
    bool marked_for_logging_ : 1;
  };
};

};  // namespace memtable
};  // end namespace oceanbase

#endif /* OCEANBASE_MVCC_OB_MVCC_TRANS_CTX_ */
