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

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ROW_
#define OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ROW_

#include "share/ob_define.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/queue/ob_link.h"
#include "lib/lock/ob_latch.h"
#include "storage/ob_i_store.h"
#include "ob_row_latch.h"
#include "ob_row_lock.h"

namespace oceanbase {
namespace memtable {

static const uint8_t NDT_NORMAL = 0x0;
static const uint8_t NDT_COMPACT = 0x1;
class ObIMvccCtx;
class ObMemtableKey;
class ObMvccRowCallback;

struct ObMvccTransNode {
public:
  ObMvccTransNode()
      : trans_version_(0),
        log_timestamp_(INT64_MAX),
        sql_sequence_(0),
        prev_(NULL),
        next_(NULL),
        modify_count_(0),
        acc_checksum_(0),
        version_(0),
        snapshot_version_barrier_(0),
        inc_num_(0),
        mvcc_row_cb_(NULL),
        ctx_descriptor_(0),
        type_(NDT_NORMAL),
        flag_(0)
  {}
  ~ObMvccTransNode()
  {}
  int64_t trans_version_;
  int64_t log_timestamp_;
  int64_t sql_sequence_;
  ObMvccTransNode* prev_;
  ObMvccTransNode* next_;
  uint32_t modify_count_;
  uint32_t acc_checksum_;
  int64_t version_;
  int64_t snapshot_version_barrier_;
  int64_t inc_num_;
  union {
    ObMvccRowCallback* mvcc_row_cb_;
    transaction::ObTransCtx* trans_ctx_;
  };
  uint32_t ctx_descriptor_;
  uint8_t type_;
  uint8_t flag_;
  char buf_[0];
  void checksum(common::ObBatchChecksum& bc) const;
  uint32_t m_cal_acc_checksum(const uint32_t last_acc_checksum) const;
  void cal_acc_checksum(const uint32_t last_acc_checksum);
  int verify_acc_checksum(const uint32_t last_acc_checksum) const;
  void set_safe_read_barrier(const bool is_weak_consistent_read);
  void clear_safe_read_barrier();
  bool is_safe_read_barrier() const
  {
    const uint8_t flag = flag_;
    return ((flag & 0x1 /*F_WEAK_CONSISTENT_READ_BARRIER*/) || (flag & 0x2 /*F_STRONG_CONSISTENT_READ_BARRIER*/));
  }
  void set_committed();
  void set_relocated();
  bool is_committed() const
  {
    return flag_ & 0x4 /*F_COMMITTED*/;
  }
  bool is_relocated() const
  {
    return (flag_ & 0x80 /*F_RELOCATED*/);
  }
  void set_elr();
  bool is_elr() const
  {
    return flag_ & 0x8 /*F_ELR*/;
  }
  void set_mvcc_row_cb(ObMvccRowCallback* mvcc_row_cb);
  const ObMvccRowCallback* get_mvcc_row_cb() const;
  int get_trans_ctx(transaction::ObTransCtx*& ctx);
  void set_aborted();
  void clear_aborted();
  bool is_aborted() const
  {
    return (flag_ & 0x10 /*F_ABORTED*/);
  }
  void set_overflow(const bool overflow = true);
  bool is_overflow() const
  {
    return flag_ & 0x20 /*F_OVERFLOW*/;
  }
  void set_delayed_cleanout(const bool delayed_cleanout);
  bool is_delayed_cleanout() const;
  void set_snapshot_version_barrier(const int64_t version)
  {
    snapshot_version_barrier_ = version;
  }
  void set_inc_num(const int64_t inc_num)
  {
    inc_num_ = inc_num;
  }
  storage::ObRowDml get_dml_type() const;
  int set_ctx_descriptor(const uint32_t ctx_descriptor);
  uint32_t get_ctx_descriptor() const;
  int fill_trans_version(const int64_t version);
  int get_trans_id_and_sql_no(transaction::ObTransID& trans_id, int64_t& sql_no);
  int get_sql_sequence() const
  {
    return sql_sequence_;
  }
  void set_sql_sequence(const int64_t sql_sequence)
  {
    sql_sequence_ = sql_sequence;
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;
  int is_lock_node(bool& is_lock) const;
  void trans_commit();
  void remove_callback();
  int try_cleanout(const ObMvccRow* value);
  int is_running(bool& is_running);

private:
  static const uint8_t F_INIT = 0x0;
  static const uint8_t F_WEAK_CONSISTENT_READ_BARRIER = 0x1;
  static const uint8_t F_STRONG_CONSISTENT_READ_BARRIER = 0x2;
  static const uint8_t F_COMMITTED = 0x4;
  static const uint8_t F_ELR = 0x8;
  static const uint8_t F_ABORTED = 0x10;
  // exceed freeze freeze log id of current memtable
  static const uint8_t F_OVERFLOW = 0x20;
  static const uint8_t F_DELAYED_CLEANOUT = 0x40;
  static const uint8_t F_RELOCATED = 0x80;

private:
  void lock();
  void unlock();

  class TransNodeMutexGuard {
  public:
    TransNodeMutexGuard(ObMvccTransNode& node) : node_(node)
    {
      node_.lock();
    }
    ~TransNodeMutexGuard()
    {
      node_.unlock();
    }
    DISABLE_COPY_ASSIGN(TransNodeMutexGuard);

  private:
    ObMvccTransNode& node_;
  };

  void set_trans_ctx_(transaction::ObTransCtx* trans_ctx);
  int get_trans_info_from_cb_(transaction::ObTransID& trans_id, int64_t& sql_no);
  int get_trans_info_from_ctx_(transaction::ObTransID& trans_id, int64_t& sql_no);
};

////////////////////////////////////////////////////////////////////////////////////////////////////

struct ObMvccRow {
  struct ObMvccRowIndex {
  public:
    ObMvccRowIndex() : is_empty_(true)
    {
      MEMSET(&replay_locations_, 0, sizeof(replay_locations_));
    }
    ~ObMvccRowIndex()
    {
      reset();
    }
    void reset();
    static bool is_valid_queue_index(const int64_t index);
    ObMvccTransNode* get_index_node(const int64_t index) const;
    void set_index_node(const int64_t index, ObMvccTransNode* node);

  public:
    bool is_empty_;
    ObMvccTransNode* replay_locations_[common::REPLAY_TASK_QUEUE_SIZE];
  };
  static const uint8_t F_INIT = 0x0;
  static const uint8_t F_HASH_INDEX = 0x1;
  static const uint8_t F_BTREE_INDEX = 0x2;
  static const uint8_t F_BTREE_TAG_DEL = 0x4;
  static const uint8_t F_LOWER_LOCK_SCANED = 0x8;
  static const uint8_t F_LOCK_DELAYED_CLEANOUT = 0x10;
  static const uint8_t F_RELOCATED = 0x20;

  static const int64_t NODE_SIZE_UNIT = 1024;
  static const int64_t WARN_WAIT_LOCK_TIME = 1 * 1000 * 1000;
  static const int64_t WARN_TIME_US = 10 * 1000 * 1000;
  static const int64_t LOG_INTERVAL = 1 * 1000 * 1000;

  // when the number of nodes visited before finding the right insert position exceeds INDEX_TRIGGER_LENGTH,
  // index will be constructed and used
  static const int64_t INDEX_TRIGGER_COUNT = 500;

  mutable ObRowLock row_lock_;    // 2 phase lock.
  ObRowLatch latch_;              // Spin lock that protects row data.
  int32_t update_since_compact_;  // Updates since last row compact.
  uint8_t flag_;
  int8_t first_dml_;
  int8_t last_dml_;
  ObMvccTransNode* list_head_;
  uint32_t modify_count_;
  uint32_t acc_checksum_;
  // max committed transaction version, updated by transaction committing
  int64_t max_trans_version_;
  // max ELR transaction version, updated by transaction unlocking
  int64_t max_elr_trans_version_;
  uint32_t ctx_descriptor_;
  // amount of ELR transaction
  int32_t elr_trans_count_;
  ObMvccTransNode* latest_compact_node_;
  ObMvccRowIndex* index_;  // using for inserting trans node when replaying
  int64_t total_trans_node_cnt_;
  int64_t latest_compact_ts_;
  ObMvccTransNode* first_overflow_node_;

  ObMvccRow() : row_lock_(*this)
  {
    reset();
  }
  ObMvccRow& operator=(const ObMvccRow& o);
  void reset();

  bool is_btree_indexed()
  {
    return flag_ & 0x2 /*F_BTREE_INDEX*/;
  }
  void set_btree_indexed()
  {
    flag_ |= 0x2 /*F_BTREE_INDEX*/;
  }
  void clear_btree_indexed()
  {
    flag_ &= static_cast<uint8_t>(~0x2 /*F_BTREE_INDEX*/);
  }
  bool is_btree_tag_del()
  {
    return flag_ & 0x4 /*F_BTREE_TAG_DEL*/;
  }
  void set_btree_tag_del()
  {
    flag_ |= 0x4 /*F_BTREE_TAG_DEL*/;
  }
  void clear_btree_tag_del()
  {
    flag_ &= static_cast<uint8_t>(~0x4 /*F_BTREE_TAG_DEL*/);
  }
  void set_hash_indexed()
  {
    flag_ |= 0x1 /*F_HASH_INDEX*/;
  }
  bool is_lower_lock_scaned() const
  {
    return flag_ & 0x8 /*F_LOWER_LOCK_SCANED*/;
  }
  void set_lower_lock_scaned()
  {
    flag_ |= 0x8 /*F_LOWER_LOCK_SCANED*/;
  }
  bool is_lock_delayed_cleanout()
  {
    return flag_ & 0x10 /*F_LOCK_DELAYED_CLEANOUT*/;
  }
  void set_lock_delayed_cleanout(bool delayed_cleanout);
  void set_first_dml(storage::ObRowDml dml_type)
  {
    first_dml_ = dml_type;
  }
  void set_last_dml(storage::ObRowDml dml_type)
  {
    last_dml_ = dml_type;
  }
  void set_relocated()
  {
    flag_ |= 0x20 /*F_RELOCATED*/;
  }
  bool is_has_relocated() const
  {
    return flag_ & 0x20 /*F_RELOCATED*/;
  }
  storage::ObRowDml get_first_dml() const
  {
    return static_cast<storage::ObRowDml>(first_dml_);
  }
  bool is_partial(const int64_t version) const;
  bool is_del(const int64_t version) const;
  bool need_compact(const bool for_read, const bool for_replay);
  int row_compact(const int64_t snapshow_version, common::ObIAllocator* node_alloc);
  int insert_trans_node(
      ObIMvccCtx& ctx, ObMvccTransNode& node, common::ObIAllocator& allocator, ObMvccTransNode*& next_node);
  int64_t get_del_version() const;
  bool is_empty() const
  {
    return (NULL == ATOMIC_LOAD(&list_head_));
  }
  ObMvccTransNode* get_list_head() const
  {
    return ATOMIC_LOAD(&list_head_);
  }
  void update_row_meta();
  int unlink_trans_node(ObIMvccCtx& ctx, const ObMvccTransNode& node, const ObMvccRow& undo_value);
  void undo(const ObMvccRow& undo_value);

  int lock_for_read(const ObMemtableKey* key, ObIMvccCtx& ctx) const;
  int lock_for_write(const ObMemtableKey* key, ObIMvccCtx& ctx);
  int unlock_for_write(const ObMemtableKey* key, ObIMvccCtx& ctx);
  int revert_lock_for_write(ObIMvccCtx& ctx);
  int check_row_locked(const ObMemtableKey* key, ObIMvccCtx& ctx, bool& is_locked, uint32_t& lock_descriptor,
      int64_t& max_trans_version);
  void lock_begin(ObIMvccCtx& ctx) const;
  void lock_for_write_end(ObIMvccCtx& ctx, int64_t ret) const;
  void lock_for_read_end(ObIMvccCtx& ctx, int64_t ret) const;

  int relocate_lock(ObIMvccCtx& ctx, const bool is_sequential_relocate);

  uint32_t get_modify_count() const
  {
    return modify_count_;
  }
  void inc_modify_count()
  {
    modify_count_++;
  }
  uint32_t get_acc_checksum() const
  {
    return acc_checksum_;
  }
  void set_row_header(const uint32_t modify_count, const uint32_t acc_checksum);
  int64_t get_max_trans_version() const;
  void update_max_trans_version(const int64_t max_trans_version);
  void set_head(ObMvccTransNode* node);
  int row_elr(const uint32_t descriptor, const int64_t elr_commit_version, const ObMemtableKey* key);
  int elr(const uint32_t descriptor, const int64_t elr_commit_version, const ObMemtableKey* key);
  bool is_transaction_set_violation(const int64_t snapshot_version);
  int set_ctx_descriptor(const uint32_t ctx_descriptor);
  int trans_commit(const int64_t commit_version, ObMvccTransNode& node);
  int32_t get_elr_trans_count() const
  {
    return ATOMIC_LOAD(&elr_trans_count_);
  }
  void inc_elr_trans_count()
  {
    ATOMIC_AAF(&elr_trans_count_, 1);
  }
  void dec_elr_trans_count()
  {
    ATOMIC_AAF(&elr_trans_count_, -1);
  }
  bool is_valid_replay_queue_index(const int64_t index) const;
  int remove_callback(ObMvccRowCallback& cb);
  void cleanout_rowlock();
  void set_overflow_node(ObMvccTransNode* node)
  {
    ATOMIC_BCAS(&first_overflow_node_, NULL, node);
  }
  void unset_overflow_node(ObMvccTransNode* node)
  {
    ATOMIC_BCAS(&first_overflow_node_, node, NULL);
  }

  int64_t to_string(char* buf, const int64_t buf_len) const;
  int64_t to_string(char* buf, const int64_t buf_len, const bool verbose) const;

private:
  int try_wait_row_lock_for_read(ObIMvccCtx& ctx, const ObMemtableKey* key) const;

  uint32_t get_writer_uid();
};

}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ROW_
