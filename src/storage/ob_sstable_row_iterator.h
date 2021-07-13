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

#ifndef OB_SSTABLE_ROW_ITERATOR_H_
#define OB_SSTABLE_ROW_ITERATOR_H_

#include "ob_i_store.h"
#include "ob_sstable.h"
#include "ob_micro_block_index_handle_mgr.h"
#include "ob_micro_block_handle_mgr.h"
#include "ob_table_store_stat_mgr.h"
#include "blocksstable/ob_storage_cache_suite.h"
#include "blocksstable/ob_micro_block_row_exister.h"
#include "blocksstable/ob_micro_block_row_getter.h"
#include "blocksstable/ob_micro_block_row_scanner.h"
#include "blocksstable/ob_micro_block_row_lock_checker.h"

namespace oceanbase {
namespace blocksstable {
class ObLobDataReader;
class ObIMicroBlockRowScanner;
}  // namespace blocksstable
namespace storage {

class ObSSTableRowIterator;
struct ObStoreRowLockState;

struct ObSSTableRowState {
  enum ObSSTableRowStateEnum { UNKNOWN_STATE = 0, NOT_EXIST, IN_ROW_CACHE, IN_BLOCK };
};

struct ObSSTableReadHandle {
public:
  ObSSTableReadHandle();
  virtual ~ObSSTableReadHandle();
  void reset();
  int get_index_handle(ObMicroBlockIndexHandle*& index_handle);
  OB_INLINE const blocksstable::MacroBlockId& get_macro_block_id() const
  {
    return macro_block_ctx_.get_macro_block_id();
  }
  TO_STRING_KV(K_(is_get), K_(state), K_(range_idx), K_(macro_block_ctx), K_(macro_idx), K_(micro_begin_idx),
      K_(micro_end_idx), K_(is_left_border), K_(is_right_border));

public:
  union {
    uint16 flag_;
    struct {
      uint16_t is_get_ : 1;
      uint16_t is_left_border_ : 1;
      uint16_t is_right_border_ : 1;
      uint16_t reserved_ : 13;
    };
  };
  int8_t state_;        // possible states: NOT_EXIST, IN_ROW_CACHE, IN_BLOCK
  bool is_bf_contain_;  // bloomfilter say false: not contain; true: contain
  int32_t range_idx_;
  blocksstable::ObMacroBlockCtx macro_block_ctx_;
  blocksstable::ObFullMacroBlockMeta full_meta_;
  int64_t macro_idx_;  // macro idx in range
private:
  ObMicroBlockIndexHandle index_handle_;

public:
  int64_t micro_begin_idx_;
  int64_t micro_end_idx_;
  union {
    const common::ObExtStoreRowkey* ext_rowkey_;
    const common::ObExtStoreRange* ext_range_;
  };
  blocksstable::ObRowValueHandle row_handle_;
  ObSSTableRowkeyHelper* rowkey_helper_;
};

class ObMicroInfoComparor {
public:
  ObMicroInfoComparor(common::ObIArray<blocksstable::ObMicroBlockInfo>& micro_infos) : micro_infos_(micro_infos)
  {}
  virtual ~ObMicroInfoComparor()
  {}
  OB_INLINE bool operator()(const int64_t i, const int64_t j)
  {
    return micro_infos_.at(i).offset_ < micro_infos_.at(j).offset_;
  }

private:
  common::ObIArray<blocksstable::ObMicroBlockInfo>& micro_infos_;
};

struct ObSSTableMicroBlockInfo {
  ObSSTableMicroBlockInfo() : macro_ctx_(), micro_info_(), micro_idx_(-1), is_skip_(false)
  {}
  TO_STRING_KV(K_(macro_ctx), K_(micro_info), K_(micro_idx), K_(is_skip));
  blocksstable::ObMacroBlockCtx macro_ctx_;
  blocksstable::ObMicroBlockInfo micro_info_;
  int64_t micro_idx_;
  bool is_skip_;
};

class ObSSTableMicroBlockInfoCmp {
public:
  ObSSTableMicroBlockInfoCmp()
  {}
  virtual ~ObSSTableMicroBlockInfoCmp()
  {}
  OB_INLINE bool operator()(const ObSSTableMicroBlockInfo& a, const ObSSTableMicroBlockInfo& b)
  {
    return a.macro_ctx_.get_macro_block_id() < b.macro_ctx_.get_macro_block_id() ||
           (a.macro_ctx_.get_macro_block_id() == b.macro_ctx_.get_macro_block_id() &&
               a.micro_info_.offset_ < b.micro_info_.offset_);
  }
};

class ObSSTableMicroBlockInfoIterator {
public:
  ObSSTableMicroBlockInfoIterator();
  virtual ~ObSSTableMicroBlockInfoIterator();
  void reset();
  void reuse();
  int open(ObSSTableRowIterator* iter, ObSSTableReadHandle& read_handle);
  int get_next_micro(ObSSTableMicroBlockInfo& sstable_micro);
  OB_INLINE void set_reverse(const bool is_reverse)
  {
    is_reverse_ = is_reverse;
    step_ = is_reverse_ ? -1 : 1;
  }

private:
  OB_INLINE int64_t get_micro_block_info_count()
  {
    return !is_get_ ? micro_block_infos_.count() : (micro_info_.is_valid() ? 1 : 0);
  }
  OB_INLINE blocksstable::ObMicroBlockInfo& get_micro_block_info(int64_t idx)
  {
    return !is_get_ ? micro_block_infos_.at(idx) : micro_info_;
  }

private:
  bool is_reverse_;
  int64_t total_micro_cnt_;
  int64_t cur_micro_idx_;
  int64_t step_;
  blocksstable::ObMacroBlockCtx macro_block_ctx_;
  common::ObSEArray<blocksstable::ObMicroBlockInfo, 16> micro_block_infos_;
  ObSSTableRowIterator* iter_;
  ObSSTableReadHandle* handle_;
  bool is_get_;
  blocksstable::ObMicroBlockInfo micro_info_;  // for get
};

class ObISSTableRowIterator : public ObStoreRowIterator {
public:
  ObISSTableRowIterator()
      : sstable_(NULL),
        lob_allocator_(NULL),
        lob_reader_(NULL),
        batch_rows_(NULL),
        batch_row_count_(0),
        batch_row_pos_(0)
  {}
  virtual ~ObISSTableRowIterator()
  {
    reset();
  }
  virtual int init(
      const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table, const void* query_range);
  virtual int get_next_row(const ObStoreRow*& store_row);
  virtual void reset();
  virtual void reuse();
  OB_INLINE bool has_lob_column() const
  {
    return OB_NOT_NULL(lob_reader_);
  }
  virtual int read_lob_columns(const ObStoreRow* store_row);

protected:
  virtual int inner_open(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table,
      const void* query_range) = 0;
  virtual int inner_get_next_row(const ObStoreRow*& store_row) = 0;
  virtual int add_lob_reader(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObSSTable& sstable);

  int batch_get_next_row(blocksstable::ObIMicroBlockRowScanner* scanner, const ObStoreRow*& store_row);

protected:
  ObSSTable* sstable_;
  ObIAllocator* lob_allocator_;
  blocksstable::ObLobDataReader* lob_reader_;
  const ObStoreRow* batch_rows_;
  int64_t batch_row_count_;
  int64_t batch_row_pos_;
};

struct ObSSTableSkipRangeCtx {
public:
  ObSSTableSkipRangeCtx()
      : range_idx_(-1),
        macro_idx_(-1),
        micro_idx_(-1),
        org_range_macro_cnt_(0),
        is_micro_reopen_(false),
        need_skip_(false)
  {}
  bool is_valid() const
  {
    return range_idx_ >= 0 && macro_idx_ >= 0 && micro_idx_ >= 0 && org_range_macro_cnt_ > 0;
  }
  TO_STRING_KV(
      K_(range_idx), K_(macro_idx), K_(micro_idx), K_(org_range_macro_cnt), K_(is_micro_reopen), K_(need_skip));
  void reset()
  {
    range_idx_ = -1;
    macro_idx_ = -1;
    micro_idx_ = -1;
    org_range_macro_cnt_ = 0;
    is_micro_reopen_ = false;
    need_skip_ = false;
  }

  int64_t range_idx_;  // the first range that need to be prefetched
  int64_t macro_idx_;  // macro idx in range
  int64_t micro_idx_;
  int64_t org_range_macro_cnt_;
  bool is_micro_reopen_;
  bool need_skip_;
};

template <typename T>
class ObSimpleArray {
public:
  ObSimpleArray() : array_(nullptr), capacity_(0)
  {}
  ~ObSimpleArray()
  {
    reset();
  }
  inline void reset()
  {
    for (int64_t i = 0; i < capacity_; ++i) {
      array_[i].~T();
    }
    array_ = nullptr;
    capacity_ = 0;
  }
  int reserve(common::ObArenaAllocator& allocator, const int64_t count)
  {
    int ret = common::OB_SUCCESS;
    if (capacity_ < count) {
      void* buf = nullptr;
      reset();
      if (OB_ISNULL(buf = allocator.alloc(sizeof(T) * count))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to allocate array", K(ret));
      } else {
        array_ = new (buf) T[count];
        capacity_ = count;
      }
    }
    return ret;
  }
  inline T& operator[](const int64_t i)
  {
    return array_[i];
  }

private:
  T* array_;
  int64_t capacity_;
};

struct ObFastSkipChecker {
  ObFastSkipChecker()
  {
    reset();
  }
  virtual ~ObFastSkipChecker() = default;
  OB_INLINE void reset()
  {
    clean();
  }
  OB_INLINE void clean()
  {
    iter_del_row_ = false;
    is_checked_ = false;
    effective_border_micro_idx_ = OB_INVALID_INDEX;
    check_border_micro_idx_ = OB_INVALID_INDEX;
    border_rowkey_.reset();
  }
  OB_INLINE bool is_checked() const
  {
    return is_checked_;
  }
  OB_INLINE bool is_effective(const int64_t idx) const
  {
    return is_checked_ && OB_INVALID_INDEX != effective_border_micro_idx_ && idx <= effective_border_micro_idx_;
  }
  TO_STRING_KV(
      K_(iter_del_row), K_(is_checked), K_(effective_border_micro_idx), K_(check_border_micro_idx), K_(border_rowkey));
  bool iter_del_row_;
  bool is_checked_;
  int64_t effective_border_micro_idx_;
  int64_t check_border_micro_idx_;
  common::ObStoreRowkey border_rowkey_;
};

class ObSSTableRowIterator : public ObISSTableRowIterator {
  typedef ObSimpleArray<ObSSTableReadHandle> ReadHandleArray;
  typedef ObSimpleArray<ObMicroBlockDataHandle> BlockDataHandleArray;
  typedef ObSimpleArray<ObSSTableMicroBlockInfo> MicroInfoArray;

public:
  ObSSTableRowIterator();
  virtual ~ObSSTableRowIterator();
  virtual void reset();
  virtual void reuse() override;
  virtual int get_skip_range_ctx(
      ObSSTableReadHandle& read_handle, const int64_t cur_micro_idx, ObSSTableSkipRangeCtx*& skip_ctx);
  int get_row_iter_flag_impl(uint8_t& flag);
  virtual int get_gap_end(int64_t& range_idx, const common::ObStoreRowkey*& gap_key, int64_t& gap_size) override;
  virtual int report_stat() override;
  virtual const common::ObIArray<ObRowkeyObjComparer*>* get_rowkey_cmp_funcs();
  int get_cur_micro_row_count(int64_t& row_count);
  int get_cur_read_handle(ObSSTableReadHandle*& read_handle);
  int get_cur_micro_idx_in_macro(int64_t& micro_idx);
  int check_row_locked(ObSSTableReadHandle& read_handle, ObStoreRowLockState& lock_state);
  virtual OB_INLINE bool is_base_sstable_iter() const override
  {
    return is_base_;
  }

protected:
  virtual int inner_open(
      const ObTableIterParam& access_param, ObTableAccessContext& access_ctx, ObITable* table, const void* query_range);
  virtual int inner_get_next_row(const ObStoreRow*& store_row);
  virtual int get_handle_cnt(const void* query_range, int64_t& read_handle_cnt, int64_t& micro_handle_cnt) = 0;
  virtual int prefetch_read_handle(ObSSTableReadHandle& read_handle) = 0;
  virtual int fetch_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row) = 0;
  virtual int get_range_count(const void* query_range, int64_t& range_count) const = 0;
  int get_read_handle(const common::ObExtStoreRowkey& ext_rowkey, ObSSTableReadHandle& read_handle);
  int get_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row);
  int exist_row(ObSSTableReadHandle& read_handle, ObStoreRow& store_row);
  int scan_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row);
  int get_cur_read_micro_handle(ObMicroBlockDataHandle*& micro_handle);
  inline int64_t get_cur_range_idx() const
  {
    return cur_range_idx_;
  }
  virtual void set_row_snapshot(ObStoreRow& row);

private:
  int prefetch();
  int prefetch_handle(const int64_t prefetch_handle_cnt);
  int prefetch_block(const int64_t sstable_micro_cnt);
  int submit_block_io(blocksstable::ObMultiBlockIOParam& io_param, MicroInfoArray& sstable_micro_infos,
      const int64_t start_sstable_micro_idx, const int64_t end_sstable_micro_idx);
  int alloc_micro_getter();
  int get_block_data(const int64_t micro_block_idx, blocksstable::ObMicroBlockData& block_data);
  int exist_block_row(ObSSTableReadHandle& read_handle, ObStoreRow& store_row);
  int get_block_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row);
  int open_cur_micro_block(ObSSTableReadHandle& read_handle, const bool is_new_skip_range = false);
  int init_handle_mgr(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, const void* query_range);
  int set_row_scn(const ObStoreRow*& store_row);
  int check_block_row_lock(ObSSTableReadHandle& read_handle, ObStoreRowLockState& lock_state);

protected:
  static const int64_t USE_HANDLE_CACHE_RANGE_COUNT_THRESHOLD = 300;
  static const int64_t LIMIT_PREFETCH_BLOCK_CACHE_THRESHOLD = 100;
  static const int64_t DEFAULT_PREFETCH_HANDLE_DEPTH = 4;
  static const int64_t DEFAULT_PREFETCH_MICRO_DEPTH = 4;
  const ObTableIterParam* iter_param_;
  ObTableAccessContext* access_ctx_;
  ObSSTable* sstable_;
  union {
    const common::ObExtStoreRowkey* rowkey_;                     // for single get/prefix_exist/single exist
    const common::ObIArray<common::ObExtStoreRowkey>* rowkeys_;  // for multi get/multi exist/single exist
    const common::ObExtStoreRange* range_;                       // for scan
    const common::ObIArray<common::ObExtStoreRange>* ranges_;    // for multi scan
    const void* query_range_;
  };
  int32_t scan_step_;
  int64_t read_handle_cnt_;
  int64_t micro_handle_cnt_;
  ObMacroBlockIterator macro_block_iter_;
  ObMicroBlockIndexHandleMgr block_index_handle_mgr_;
  ObMicroBlockHandleMgr block_handle_mgr_;
  ObTableStoreStat table_store_stat_;
  ObSSTableSkipRangeCtx skip_ctx_;
  blocksstable::ObStorageFile* storage_file_;

private:
  bool is_opened_;
  bool is_base_;
  blocksstable::ObIMicroBlockCache* block_cache_;
  ObITable::TableType table_type_;
  int64_t sstable_snapshot_version_;
  blocksstable::ObMacroBlockReader block_reader_;
  blocksstable::ObMicroBlockRowExister* micro_exister_;
  blocksstable::ObMicroBlockRowGetter* micro_getter_;
  blocksstable::ObMicroBlockRowLockChecker* micro_lock_checker_;
  blocksstable::ObIMicroBlockRowScanner* micro_scanner_;
  ReadHandleArray read_handles_;
  bool prefetch_handle_end_;
  bool prefetch_block_end_;
  int64_t cur_prefetch_handle_pos_;
  int64_t cur_fetch_handle_pos_;
  int64_t cur_read_handle_pos_;
  int64_t cur_prefetch_micro_pos_;
  int64_t cur_read_micro_pos_;
  int64_t cur_micro_idx_;
  int64_t cur_range_idx_;
  MicroInfoArray sstable_micro_infos_;
  MicroInfoArray sorted_sstable_micro_infos_;
  BlockDataHandleArray micro_handles_;
  common::ObSEArray<blocksstable::ObMicroBlockInfo, 16> io_micro_infos_;
  ObSSTableMicroBlockInfoIterator micro_info_iter_;
  int64_t prefetch_handle_depth_;
  int64_t prefetch_micro_depth_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_SSTABLE_ROW_ITERATOR_H_ */
