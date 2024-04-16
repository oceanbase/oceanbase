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

#ifndef OB_STORAGE_ROWS_INFO_H
#define OB_STORAGE_ROWS_INFO_H

#include "ob_table_access_context.h"
#include "share/allocator/ob_reserve_arena.h"

namespace oceanbase
{
namespace storage
{

struct ObRowState
{
  bool lock_is_decided_;
  share::SCN max_trans_version_;
  blocksstable::ObDmlFlag row_dml_flag_;
  ObRowState()
      : lock_is_decided_(false),
        max_trans_version_(share::SCN::min_scn()),
        row_dml_flag_(blocksstable::ObDmlFlag::DF_NOT_EXIST)
  {}
  inline bool is_not_exist() const { return blocksstable::ObDmlFlag::DF_NOT_EXIST == row_dml_flag_; }
  inline bool is_delete() const { return blocksstable::ObDmlFlag::DF_DELETE == row_dml_flag_; }
  inline bool is_lock_decided() const { return lock_is_decided_; }
  inline share::SCN get_max_trans_version() const { return max_trans_version_; }
  inline void set_lock_decided() { lock_is_decided_ = true; }
  TO_STRING_KV(K_(lock_is_decided), K_(max_trans_version), K_(row_dml_flag));
};

struct ObMarkedRowkeyAndLockState
{
  blocksstable::ObMarkedRowkey marked_rowkey_;
  ObStoreRowLockState *lock_state_;
  int64_t row_idx_;
  ObMarkedRowkeyAndLockState()
      : marked_rowkey_(),
        lock_state_(nullptr),
        row_idx_(0)
  {}
  TO_STRING_KV(K_(marked_rowkey), KP_(lock_state), K_(row_idx));
};

struct ObRowsInfo
{
public:
  typedef common::ObReserveArenaAllocator<1024> ObStorageReserveAllocator;
  static const int MAX_ROW_KEYS_ON_STACK = 16;
  using ObRowkeyAndLockStates = common::ObSEArray<ObMarkedRowkeyAndLockState, MAX_ROW_KEYS_ON_STACK>;
  using ObPermutation = common::ObSEArray<uint32_t, MAX_ROW_KEYS_ON_STACK>;
  ObRowsInfo();
  ~ObRowsInfo();
  void reset();
  OB_INLINE bool is_valid() const
  {
    return is_inited_ && exist_helper_.is_valid() && delete_count_ >= 0
        && rowkeys_.count() >= delete_count_ && OB_NOT_NULL(rows_);
  }
  OB_INLINE int64_t get_rowkey_cnt() const
  {
    return rowkeys_.count();
  }
  OB_INLINE int32_t get_datum_cnt() const
  {
    const blocksstable::ObMarkedRowkey &marked_rowkey = rowkeys_[0].marked_rowkey_;
    return marked_rowkey.get_rowkey().get_datum_cnt();
  }
  OB_INLINE const blocksstable::ObDatumRowkey &get_rowkey(const int64_t idx) const
  {
    const blocksstable::ObMarkedRowkey &marked_rowkey = rowkeys_[idx].marked_rowkey_;
    return marked_rowkey.get_rowkey();
  }
  OB_INLINE blocksstable::ObMarkedRowkey &get_marked_rowkey(const int64_t idx)
  {
    return rowkeys_[idx].marked_rowkey_;
  }
  OB_INLINE bool all_rows_found()
  {
    return delete_count_ == rowkeys_.count();
  }
  int init(
      const ObRelativeTable &table,
      ObStoreCtx &store_ctx,
      const ObITableReadInfo &rowkey_read_info);
  int check_duplicate(ObStoreRow *rows, const int64_t row_count, ObRelativeTable &table);
  blocksstable::ObDatumRowkey& get_duplicate_rowkey()
  {
    return min_key_;
  }
  blocksstable::ObDatumRowkey& get_conflict_rowkey()
  {
    const int64_t conflict_rowkey_idx = 1 == rowkeys_.count() ? 0 : conflict_rowkey_idx_;
    return rowkeys_[conflict_rowkey_idx].marked_rowkey_.get_rowkey();
  }
  int64_t get_conflict_idx() const
  {
    return conflict_rowkey_idx_;
  }
  ObStoreRowLockState &get_row_lock_state(const int64_t idx)
  {
    return *(rowkeys_[idx].lock_state_);
  }
  bool have_conflict()
  {
    return -1 != conflict_rowkey_idx_;
  }
  inline void set_conflict_rowkey(const int64_t idx)
  {
    conflict_rowkey_idx_ = idx;
  }
  inline void set_error_code(const int error_code)
  {
    error_code_ = error_code;
  }
  inline int get_error_code() const
  {
    return error_code_;
  }
  inline void set_row_lock_state(
      const int64_t idx,
      ObStoreRowLockState *lock_state)
  {
    lock_state->reset();
    rowkeys_[idx].lock_state_ = lock_state;
  }
  inline void set_row_lock_checked(
      const int64_t idx,
      const bool check_exist)
  {
    blocksstable::ObMarkedRowkey &marked_rowkey = rowkeys_[idx].marked_rowkey_;
    if (check_exist && !marked_rowkey.is_row_exist_checked()) {
      marked_rowkey.mark_row_lock_checked();
    } else if (!marked_rowkey.is_checked()) {
      marked_rowkey.mark_row_checked();
      ++delete_count_;
    }
  }
  inline void set_all_rows_lock_checked(const bool check_exist)
  {
    for (int64_t i = 0; i < rowkeys_.count(); ++i) {
      set_row_lock_checked(i, check_exist);
    }
  }
  inline void set_row_exist_checked(const int64_t idx)
  {
    rowkeys_[idx].marked_rowkey_.mark_row_exist_checked();
  }
  inline void set_row_bf_checked(const int64_t idx)
  {
    rowkeys_[idx].marked_rowkey_.mark_row_bf_checked();
  }
  inline void set_row_checked(const int64_t idx)
  {
    rowkeys_[idx].marked_rowkey_.mark_row_checked();
    ++delete_count_;
  }
  inline void set_row_non_existent(const int64_t idx)
  {
    rowkeys_[idx].marked_rowkey_.mark_row_non_existent();
  }
  inline bool is_row_checked(const int64_t idx) const
  {
    const blocksstable::ObMarkedRowkey &marked_rowkey = rowkeys_[idx].marked_rowkey_;
    return marked_rowkey.is_checked();
  }
  inline bool is_row_skipped(const int64_t idx) const
  {
    const blocksstable::ObMarkedRowkey &marked_rowkey = rowkeys_[idx].marked_rowkey_;
    return marked_rowkey.is_skipped();
  }
  inline bool is_row_exist_checked(const int64_t idx) const
  {
    const blocksstable::ObMarkedRowkey &marked_rowkey = rowkeys_[idx].marked_rowkey_;
    return marked_rowkey.is_row_exist_checked();
  }
  inline bool is_row_lock_checked(const int64_t idx) const
  {
    const blocksstable::ObMarkedRowkey &marked_rowkey = rowkeys_[idx].marked_rowkey_;
    return marked_rowkey.is_row_lock_checked();
  }
  inline bool is_row_bf_checked(const int64_t idx)
  {
    const blocksstable::ObMarkedRowkey &marked_rowkey = rowkeys_[idx].marked_rowkey_;
    return marked_rowkey.is_row_bf_checked();
  }
  inline uint32_t get_permutation_idx(const int64_t idx)
  {
    return permutation_[idx];
  }
  int check_min_rowkey_boundary(const blocksstable::ObDatumRowkey &max_rowkey, bool &may_exist);
  int refine_rowkeys();
  void return_exist_iter(ObStoreRowIterator *exist_iter);
  void reuse_scan_mem_allocator() { scan_mem_allocator_.reuse(); }
  ObTableAccessContext &get_access_context() { return exist_helper_.table_access_context_; }
  TO_STRING_KV(K_(rowkeys), K_(permutation), K_(min_key), K_(delete_count), K_(conflict_rowkey_idx), K_(error_code),
               K_(exist_helper));
public:
  struct ExistHelper final
  {
    ExistHelper();
    ~ExistHelper();
    void reset();
    int init(
        const ObRelativeTable &table,
        ObStoreCtx &store_ctx,
        const ObITableReadInfo &rowkey_read_info,
        ObStorageReserveAllocator &stmt_allocator,
        ObStorageReserveAllocator &allocator);
    OB_INLINE bool is_valid() const { return is_inited_; }
    TO_STRING_KV(K_(table_iter_param), K_(table_access_context));
    ObTableIterParam table_iter_param_;
    ObTableAccessContext table_access_context_;
    bool is_inited_;
    DISALLOW_COPY_AND_ASSIGN(ExistHelper);
  };
private:
  struct RowsCompare {
    RowsCompare(const blocksstable::ObStorageDatumUtils &datum_utils,
                blocksstable::ObDatumRowkey &dup_key,
                const bool check_dup,
                int &ret)
      : datum_utils_(datum_utils),
        dup_key_(dup_key),
        check_dup_(check_dup),
        ret_(ret)
    {}
    ~RowsCompare() = default;
    OB_INLINE bool operator() (const ObMarkedRowkeyAndLockState &left, const ObMarkedRowkeyAndLockState &right)
    {
      int cmp_ret = 0;
      int &ret = ret_;
      const blocksstable::ObDatumRowkey &left_rowkey = left.marked_rowkey_.get_rowkey();
      const blocksstable::ObDatumRowkey &right_rowkey = right.marked_rowkey_.get_rowkey();;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(left_rowkey.compare(right_rowkey, datum_utils_, cmp_ret))) {
        STORAGE_LOG(WARN, "Failed to compare datum rowkey", K(ret), K_(left.marked_rowkey), K_(right.marked_rowkey));
      } else if (OB_UNLIKELY(check_dup_ && 0 == cmp_ret)) {
        ret_ = common::OB_ERR_PRIMARY_KEY_DUPLICATE;
        dup_key_ = left_rowkey;
        STORAGE_LOG(WARN, "Rowkey already exists", K_(dup_key), K_(ret));
      }
      return cmp_ret < 0;
    }
    const blocksstable::ObStorageDatumUtils &datum_utils_;
    blocksstable::ObDatumRowkey &dup_key_;
    bool check_dup_;
    int &ret_;
  };
private:
  void reuse();
  ObStorageReserveAllocator scan_mem_allocator_; // scan/rescan level memory entity, only for query
  ObStorageReserveAllocator exist_allocator_;
  ObStorageReserveAllocator key_allocator_;
public:
  ObRowkeyAndLockStates rowkeys_;
  ObPermutation permutation_;
  ObStoreRow *rows_;
  ExistHelper exist_helper_;
  ObTabletID tablet_id_;
private:
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  blocksstable::ObDatumRowkey min_key_;
  int64_t conflict_rowkey_idx_;
  int error_code_;
  int64_t delete_count_;
  int16_t rowkey_column_num_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObRowsInfo);
};

struct ObRowKeysInfo
{
  OB_INLINE bool is_rowkey_not_exist(const int64_t idx) const
  {
    return row_states_->at(idx) == ObSSTableRowState::NOT_EXIST;
  }
  OB_INLINE void set_rowkey_not_exist(const int64_t idx)
  {
    row_states_->at(idx) = ObSSTableRowState::NOT_EXIST;
  }
  OB_INLINE const blocksstable::ObDatumRowkey &get_rowkey(const int64_t idx) const
  {
    return rowkeys_->at(idx);
  }
  const common::ObIArray<blocksstable::ObDatumRowkey> *rowkeys_;
  common::ObIArray<int8_t> *row_states_;
};

} // namespace storage
} // namespace oceanbase
#endif
