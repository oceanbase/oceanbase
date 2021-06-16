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

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_MVCC_CTX_
#define OCEANBASE_MEMTABLE_MVCC_OB_MVCC_CTX_

#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"

namespace oceanbase {
namespace memtable {
class ObMemtableKey;
// Interfaces of Mvcc
////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMvccRow;
class ObIMvccCtx {
public:
  ObIMvccCtx()
      : alloc_type_(0),
        ctx_descriptor_(0),
        trans_start_time_(0),
        trans_mgr_(*this),
        // save min table version of current transaction
        min_table_version_(0),
        // save max table version of current transaction
        max_table_version_(0),
        is_safe_read_(false),
        has_read_relocated_row_(false),
        trans_version_(INT64_MAX),
        commit_version_(0),
        abs_expired_time_(0),
        abs_lock_wait_timeout_(0),
        stmt_start_time_(0),
        lock_start_time_(0),
        row_purge_version_(0),
        redo_log_timestamp_(0),
        trx_lock_timeout_(-1),
        lock_wait_start_ts_(0),
        multi_version_range_(),
        lob_start_log_ts_(0)
  {
    multi_version_range_.base_version_ = common::ObVersionRange::MIN_VERSION;  // TODO: remove it
  }
  virtual ~ObIMvccCtx()
  {}

public:  // for mvcc engine invoke
  // for write
  virtual int write_auth() = 0;
  virtual int write_done() = 0;
  virtual int wait_trans_version(const uint32_t descriptor, const int64_t version) = 0;
  virtual int wait_trans_version_v2(
      const uint32_t descriptor, const int64_t version, const ObMemtableKey* key, const ObMvccRow& row) = 0;
  virtual int try_elr_when_lock_for_write(
      const uint32_t descriptor, const int64_t version, const ObMemtableKey* key, const ObMvccRow& row) = 0;
  virtual void* callback_alloc(const int64_t size) = 0;
  virtual void callback_free(void* cb) = 0;
  virtual void* arena_alloc(const int64_t size) = 0;
  virtual common::ObIAllocator& get_query_allocator() = 0;
  virtual int add_crc(const void* key, ObMvccRow* value, ObMvccTransNode* tnode, int64_t data_size) = 0;
  virtual int add_crc2(ObMvccRow* value, ObMvccTransNode* tnode, int64_t data_size) = 0;
  virtual int add_crc4(ObMvccRow* value, ObMvccTransNode* tnode, int64_t data_size, const int64_t log_ts) = 0;
  virtual int row_compact(ObMvccRow* value, const int64_t snapshot_version)
  {
    UNUSED(value);
    UNUSED(snapshot_version);
    return common::OB_SUCCESS;
  }
  virtual const char* log_conflict_ctx(const uint32_t descriptor)
  {
    UNUSED(descriptor);
    return NULL;
  }
  virtual int read_lock_yield()
  {
    return common::OB_SUCCESS;
  }
  virtual int write_lock_yield()
  {
    return common::OB_SUCCESS;
  }
  virtual int after_link_trans_node(const void* key, ObMvccRow* value);
  virtual void inc_lock_for_read_retry_count() = 0;
  virtual void add_lock_for_read_elapse(const int64_t n) = 0;
  virtual int64_t get_lock_for_read_elapse() const = 0;
  virtual void on_tsc_retry(const ObMemtableKey& key) = 0;
  virtual void on_wlock_retry(const ObMemtableKey& key, const char* conflict_ctx) = 0;
  virtual int64_t get_relocate_cnt() = 0;
  virtual bool is_can_elr() const = 0;
  virtual bool is_elr_prepared() const = 0;
  virtual void inc_relocate_cnt() = 0;
  virtual void inc_truncate_cnt() = 0;
  virtual int insert_prev_trans(const uint32_t descriptor) = 0;
  virtual void add_trans_mem_total_size(const int64_t size) = 0;
  virtual bool has_redo_log() const = 0;
  virtual void set_contain_hotspot_row() = 0;
  virtual void update_max_durable_sql_no(const int64_t sql_no) = 0;

public:
  inline int get_alloc_type() const
  {
    return alloc_type_;
  }
  inline uint32_t get_ctx_descriptor() const
  {
    return ctx_descriptor_;
  }
  inline int64_t get_stmt_remainder_time() const
  {
    return abs_expired_time_ - ::oceanbase::common::ObTimeUtility::current_time();
  }
  inline int64_t get_abs_expired_time() const
  {
    return abs_expired_time_;
  }
  inline bool get_is_safe_read() const
  {
    return is_safe_read_;
  }
  inline int64_t get_read_snapshot() const
  {
    return multi_version_range_.snapshot_version_;
  }
  inline int64_t get_base_version() const
  {
    return multi_version_range_.base_version_;
  }
  inline int64_t get_trans_version() const
  {
    return ATOMIC_LOAD(&trans_version_);
  }
  inline int64_t get_commit_version() const
  {
    return ATOMIC_LOAD(&commit_version_);
  }
  inline int64_t get_abs_lock_wait_timeout() const
  {
    return abs_lock_wait_timeout_;
  }
  inline int64_t get_min_table_version() const
  {
    return min_table_version_;
  }
  inline int64_t get_max_table_version() const
  {
    return max_table_version_;
  }
  inline int64_t get_multi_version_start() const
  {
    return multi_version_range_.multi_version_start_;
  }
  inline void set_alloc_type(const int alloc_type)
  {
    alloc_type_ = alloc_type;
  }
  inline void set_is_safe_read(const bool is_safe_read)
  {
    is_safe_read_ = is_safe_read;
  }
  inline void set_read_snapshot(const int64_t snapshot)
  {
    multi_version_range_.snapshot_version_ = snapshot;
  }
  inline void set_multi_version_start(const int64_t multi_version_start)
  {
    multi_version_range_.multi_version_start_ = multi_version_start;
  }
  inline void set_base_version(const int64_t base_version)
  {
    multi_version_range_.base_version_ = base_version;
  }
  inline void set_ctx_descriptor(const uint32_t descriptor)
  {
    ctx_descriptor_ = descriptor;
  }
  inline void set_abs_expired_time(const int64_t abs_expired_time)
  {
    abs_expired_time_ = abs_expired_time;
  }
  void before_prepare();
  bool is_prepared() const;
  inline void set_prepare_version(const int64_t version)
  {
    set_trans_version(version);
  }
  inline void set_trans_version(const int64_t trans_version)
  {
    ATOMIC_STORE(&trans_version_, trans_version);
  }
  inline void set_commit_version(const int64_t trans_version)
  {
    ATOMIC_STORE(&commit_version_, trans_version);
  }
  inline void set_trans_start_time(const int64_t start_time)
  {
    trans_start_time_ = start_time;
  }
  inline int64_t get_trans_start_time() const
  {
    return trans_start_time_;
  }
  inline void set_abs_lock_wait_timeout(const int64_t timeout)
  {
    abs_lock_wait_timeout_ = timeout;
  }
  inline void set_table_version(const int64_t table_version)
  {
    if (INT64_MAX == min_table_version_) {
      // table version should not be INT64_MAX when update table version at first time
      if (INT64_MAX == table_version) {
        TRANS_LOG(WARN, "unexpected table version", K(table_version), K(*this));
      } else {
        min_table_version_ = table_version;
        max_table_version_ = table_version;
      }
    } else if (table_version < max_table_version_) {
      TRANS_LOG(DEBUG, "current table version lower the last one", K(table_version), K(*this));
      // table version should not be INT64_MAX when table version has already been updated
    } else if (INT64_MAX == table_version) {
      TRANS_LOG(ERROR, "unexpected table version", K(table_version), K(*this));
    } else {
      max_table_version_ = table_version;
    }
  }
  inline bool is_commit_version_valid() const
  {
    return commit_version_ != 0 && commit_version_ != INT64_MAX;
  }
  inline void set_stmt_start_time(const int64_t start_time)
  {
    stmt_start_time_ = start_time;
  }
  inline int64_t get_stmt_start_time() const
  {
    return stmt_start_time_;
  }
  inline int64_t get_stmt_timeout_us() const
  {
    return abs_expired_time_ - stmt_start_time_;
  }
  inline void set_lock_start_time(const int64_t start_time)
  {
    lock_start_time_ = start_time;
  }
  inline int64_t get_lock_start_time()
  {
    return lock_start_time_;
  }
  inline void set_for_replay(const bool for_replay)
  {
    trans_mgr_.set_for_replay(for_replay);
  }
  inline bool is_for_replay() const
  {
    return trans_mgr_.is_for_replay();
  }
  inline void set_redo_log_timestamp(const int64_t redo_log_timestamp)
  {
    redo_log_timestamp_ = redo_log_timestamp;
  }
  inline int64_t get_redo_log_timestamp() const
  {
    return redo_log_timestamp_;
  }
  inline int64_t get_callback_list_length() const
  {
    return trans_mgr_.count();
  }
  inline void set_trx_lock_timeout(const int64_t trx_lock_timeout)
  {
    trx_lock_timeout_ = trx_lock_timeout;
  }
  inline int64_t get_trx_lock_timeout() const
  {
    return trx_lock_timeout_;
  }
  inline void set_lock_wait_start_ts(const int64_t lock_wait_start_ts)
  {
    lock_wait_start_ts_ = lock_wait_start_ts;
  }
  inline int64_t get_lock_wait_start_ts() const
  {
    return lock_wait_start_ts_;
  }
  int64_t get_query_abs_lock_wait_timeout(const int64_t lock_wait_start_ts) const;
  bool is_rowlocks_released() const
  {
    return trans_mgr_.is_rowlocks_released();
  }
  inline void set_has_read_relocated_row()
  {
    has_read_relocated_row_ = true;
  }
  inline bool has_read_relocated_row() const
  {
    return has_read_relocated_row_;
  }
  inline void reset_has_read_relocated_row()
  {
    has_read_relocated_row_ = false;
  }

  int register_row_lock_release_cb(
      const ObMemtableKey* key, ObMvccRow* value, bool is_replay, ObMemtable* memtable, const int32_t sql_no);
  int register_row_commit_cb(const ObMemtableKey* key, ObMvccRow* value, ObMvccTransNode* node, const int64_t data_size,
      const ObRowData* old_row, const bool is_stmt_committed, const bool need_fill_redo, ObMemtable* memtable,
      const int32_t sql_no, const bool is_sequential_relocate);
  int register_row_replay_cb(const ObMemtableKey* key, ObMvccRow* value, ObMvccTransNode* node, const int64_t data_size,
      ObMemtable* memtable, const int32_t sql_no, const bool is_sequential_relocate, const int64_t log_ts);
  int register_savepoint_cb(const ObMemtableKey* key, ObMvccRow* value, ObMvccTransNode* node, const int64_t data_size,
      const ObRowData* old_row, const int32_t sql_no);
  int register_savepoint_cb(ObMvccRowCallback& cb, ObMemtable* memtable);
  bool need_retry()
  {
    return 0 != abs_lock_wait_timeout_;
  }

public:
  virtual void reset()
  {
    ctx_descriptor_ = 0;
    trans_start_time_ = 0;
    trans_mgr_.reset();
    min_table_version_ = INT64_MAX;
    max_table_version_ = 0;
    is_safe_read_ = false;
    has_read_relocated_row_ = false;
    trans_version_ = INT64_MAX;
    commit_version_ = 0;
    abs_expired_time_ = 0;
    abs_lock_wait_timeout_ = 0;
    stmt_start_time_ = 0;
    lock_start_time_ = 0;
    row_purge_version_ = 0;
    redo_log_timestamp_ = 0;
    multi_version_range_.reset();
    multi_version_range_.base_version_ = common::ObVersionRange::MIN_VERSION;  // TODO: remove it
    lock_wait_start_ts_ = 0;
    trx_lock_timeout_ = -1;
    lob_start_log_ts_ = 0;
  }
  virtual int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf,
        buf_len,
        pos,
        "alloc_type=%d "
        "ctx_descriptor=%u "
        "trans_start_time=%ld "
        "min_table_version=%ld "
        "max_table_version=%ld "
        "is_safe_read=%s "
        "has_read_relocated_row=%s "
        "read_snapshot=%ld "
        "start_version=%ld "
        "trans_version=%ld "
        "commit_version=%ld "
        "stmt_start_time=%ld "
        "abs_expired_time=%ld "
        "stmt_timeout=%ld "
        "abs_lock_wait_timeout=%ld "
        "row_purge_version=%ld "
        "lock_wait_start_ts=%ld "
        "trx_lock_timeout=%ld ",
        alloc_type_,
        ctx_descriptor_,
        trans_start_time_,
        min_table_version_,
        max_table_version_,
        STR_BOOL(is_safe_read_),
        STR_BOOL(has_read_relocated_row_),
        multi_version_range_.snapshot_version_,
        multi_version_range_.multi_version_start_,
        trans_version_,
        commit_version_,
        stmt_start_time_,
        abs_expired_time_,
        abs_expired_time_ - stmt_start_time_,
        abs_lock_wait_timeout_,
        row_purge_version_,
        lock_wait_start_ts_,
        trx_lock_timeout_);
    return pos;
  }

public:
  ObMvccRowCallback* alloc_row_callback(
      ObIMvccCtx& ctx, ObMvccRow& value, ObMemtable* memtable, const bool is_savepoint);
  ObMvccRowCallback* alloc_row_callback(ObMvccRowCallback& cb, ObMemtable* memtable, const bool is_savepoint);
  ObMemtableKey* alloc_memtable_key();
  ObMvccRow* alloc_mvcc_row();
  ObMvccTransNode* alloc_trans_node();
  int append_callback(ObITransCallback* cb);

protected:
  DISALLOW_COPY_AND_ASSIGN(ObIMvccCtx);
  int alloc_type_;
  uint32_t ctx_descriptor_;
  int64_t trans_start_time_;
  ObTransCallbackMgr trans_mgr_;
  int64_t min_table_version_;
  int64_t max_table_version_;
  bool is_safe_read_;
  bool has_read_relocated_row_;
  int64_t trans_version_;
  int64_t commit_version_;
  int64_t abs_expired_time_;
  int64_t abs_lock_wait_timeout_;
  int64_t stmt_start_time_;
  int64_t lock_start_time_;
  int64_t row_purge_version_;
  int64_t redo_log_timestamp_;
  int64_t trx_lock_timeout_;
  int64_t lock_wait_start_ts_;
  common::ObVersionRange multi_version_range_;
  int64_t lob_start_log_ts_;
};

class ObMvccWriteGuard {
public:
  ObMvccWriteGuard() : ctx_(NULL)
  {}
  ~ObMvccWriteGuard()
  {
    if (NULL != ctx_) {
      ctx_->write_done();
    }
  }
  void set_lock_fail()
  {
    ctx_ = NULL;
  }
  int write_auth(ObIMvccCtx& ctx)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ctx.write_auth())) {
      TRANS_LOG(WARN, "write_auth fail", K(ret));
    } else {
      ctx_ = &ctx;
    }
    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMvccWriteGuard);
  ObIMvccCtx* ctx_;
};
}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_MEMTABLE_MVCC_OB_MVCC_CTX_
