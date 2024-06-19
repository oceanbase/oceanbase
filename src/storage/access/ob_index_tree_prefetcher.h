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

#ifndef OB_STORAGE_OB_INDEX_TREE_PREFETCHER_H_
#define OB_STORAGE_OB_INDEX_TREE_PREFETCHER_H_

#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_param.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_row_scanner.h"
#include "storage/blocksstable/ob_row_cache.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/access/ob_micro_block_handle_mgr.h"
#include "storage/access/ob_rows_info.h"

namespace oceanbase {
using namespace blocksstable;
namespace storage {
class ObAggregatedStore;
class ObRowsInfo;

struct ObSSTableReadHandle
{
public:
  ObSSTableReadHandle() :
      is_get_(false),
      is_bf_contain_(false),
      is_sorted_multi_get_(false),
      row_state_(0),
      range_idx_(-1),
      micro_begin_idx_(-1),
      micro_end_idx_(-1),
      query_range_(nullptr),
      index_block_info_(),
      row_handle_(),
      micro_handle_(nullptr)
  {}
  ~ObSSTableReadHandle()
  {}
  void reuse()
  {
    is_get_ = false;
    is_bf_contain_ = false;
    is_sorted_multi_get_ = false;
    row_state_ = 0;
    range_idx_ = -1;
    micro_begin_idx_ = -1;
    micro_end_idx_ = -1;
    query_range_ = nullptr;
    index_block_info_.reset();
    row_handle_.reset();
  }
  void reset()
  {
    is_get_ = false;
    is_bf_contain_ = false;
    is_sorted_multi_get_ = false;
    row_state_ = 0;
    range_idx_ = -1;
    micro_begin_idx_ = -1;
    micro_end_idx_ = -1;
    query_range_ = nullptr;
    micro_handle_ = nullptr;
    index_block_info_.reset();
    row_handle_.reset();
  }
  OB_INLINE bool is_valid() const
  { return nullptr != query_range_; }
  OB_INLINE bool need_read_block() const
  { return ObSSTableRowState::IN_BLOCK == row_state_; }
  int get_block_data(ObMacroBlockReader &block_reader, blocksstable::ObMicroBlockData &block_data)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == micro_handle_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpect null micro_handle ", K(ret));
    } else if (OB_FAIL(micro_handle_->get_micro_block_data(&block_reader, block_data))) {
      STORAGE_LOG(WARN, "Fail to get block data ", K(ret));
    }
    return ret;
  }
  TO_STRING_KV(K_(is_get), K_(is_bf_contain), K_(is_sorted_multi_get), K_(row_state), K_(range_idx), K_(index_block_info),
               K_(micro_begin_idx), K_(micro_end_idx), KP_(query_range), KPC_(micro_handle));

public:
  bool is_get_;
  bool is_bf_contain_;
  bool is_sorted_multi_get_;
  int8_t row_state_;    // possible states: NOT_EXIST, IN_ROW_CACHE, IN_BLOCK
  int64_t range_idx_;
  int64_t micro_begin_idx_;
  int64_t micro_end_idx_;
  const blocksstable::ObDatumRowkey &get_rowkey()
  {
    return is_sorted_multi_get_ ? rowkeys_info_->get_rowkey(range_idx_) : *rowkey_;
  }
  union {
    const blocksstable::ObDatumRowkey *rowkey_;
    const blocksstable::ObDatumRange *range_;
    const ObRowsInfo *rows_info_;
    const ObRowKeysInfo *rowkeys_info_;
    const void *query_range_;
  };
  ObMicroIndexInfo index_block_info_;
  ObRowValueHandle row_handle_;
  ObMicroBlockDataHandle *micro_handle_;
};

class ObIndexTreePrefetcher
{
public:
  ObIndexTreePrefetcher() :
      is_inited_(false),
      iter_type_(0),
      cur_level_(0),
      index_tree_height_(0),
      max_rescan_height_(0),
      max_rescan_range_cnt_(0),
      data_version_(0),
      table_scan_cnt_(0),
      sstable_(nullptr),
      sstable_meta_handle_(),
      index_block_(),
      iter_param_(nullptr),
      access_ctx_(nullptr),
      datum_utils_(nullptr),
      index_scanner_(),
      last_micro_block_handle_(),
      long_life_allocator_(nullptr),
      micro_handles_()
  {}
  virtual ~ObIndexTreePrefetcher()
  {}
  virtual void reset();
  virtual void reuse();
  virtual void reclaim();
  virtual int init(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const void *query_range);
  virtual int switch_context(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const void *query_range);
  int single_prefetch(ObSSTableReadHandle &read_handle);
  int lookup_in_index_tree(ObSSTableReadHandle &read_handle, const bool force_prefetch);
  OB_INLINE bool is_valid() { return is_inited_; }
  ObMicroBlockDataHandle &get_last_data_handle() { return last_micro_block_handle_; }
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(iter_type), K_(cur_level), K_(index_tree_height),
      K_(max_rescan_height), K_(max_rescan_range_cnt), K_(data_version), K_(table_scan_cnt), K_(index_scanner),
      KPC_(sstable), KPC_(iter_param), K_(last_micro_block_handle));
protected:
  void inner_reset();
  int init_index_scanner(ObIndexBlockRowScanner &index_scanner);
  int check_bloom_filter(
      const ObMicroIndexInfo &index_info,
      const bool is_multi_check,
      ObSSTableReadHandle &read_handle);
  int prefetch_block_data(
      ObMicroIndexInfo &index_block_info,
      ObMicroBlockDataHandle &micro_handle,
      const bool is_data = true,
      const bool use_multi_block_prefetch = false,
      const bool need_submit_io = true);
  int lookup_in_cache(ObSSTableReadHandle &read_handle);
  int init_basic_info(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx);
  OB_INLINE bool is_first_scan() const { return nullptr == sstable_; }
  OB_INLINE bool is_rescan() const { return 1 < table_scan_cnt_; }
private:
  ObMicroBlockDataHandle &get_read_handle(const int64_t level)
  {
    return micro_handles_[level % DEFAULT_GET_MICRO_DATA_HANDLE_CNT];
  }
  bool last_handle_hit(const ObMicroIndexInfo &block_info, const bool is_data, ObMicroBlockDataHandle &micro_handle);
protected:
  static const int64_t MAX_RESCAN_HOLD_LIMIT = 64;
  static const int16_t MAX_INDEX_TREE_HEIGHT = 16;
  bool is_inited_;
  int16_t iter_type_;
  int16_t cur_level_;
  int16_t index_tree_height_;
  int16_t max_rescan_height_;
  int64_t max_rescan_range_cnt_;
  int64_t data_version_;
  int64_t table_scan_cnt_;
  ObSSTable *sstable_;
  ObSSTableMetaHandle sstable_meta_handle_;
  ObMicroBlockData index_block_;
  const ObTableIterParam *iter_param_;
  ObTableAccessContext *access_ctx_;
  const ObStorageDatumUtils *datum_utils_;
  static const int64_t DEFAULT_GET_MICRO_DATA_HANDLE_CNT = 2;
  ObIndexBlockRowScanner index_scanner_;
  ObMicroBlockDataHandle last_micro_block_handle_;
  ObIAllocator *long_life_allocator_;
  private:
  ObMicroBlockDataHandle micro_handles_[DEFAULT_GET_MICRO_DATA_HANDLE_CNT];
};

// pin the micro block which covers the following rowkeys: [rowkey_begin_idx_,rowkey_end_idx_)
// reset when the next rowkey exceeds rowkey_end_idx_
struct ObCachedLevelMicroDataHandle
{
  ObCachedLevelMicroDataHandle() :
      is_valid_(false),
      is_leaf_block_(false),
      rowkey_begin_idx_(-1),
      rowkey_end_idx_(-1),
      macro_id_(),
      handle_()
  {}
  ~ObCachedLevelMicroDataHandle()
  {}
  void reset()
  {
    is_valid_ = false;
    is_leaf_block_ = false;
    rowkey_begin_idx_ = -1;
    rowkey_end_idx_ = -1;
    macro_id_.reset();
    handle_.reset();
  }
  OB_INLINE bool is_covered(const int64_t rowkey_idx) const
  {
    return is_valid_ && rowkey_begin_idx_ <= rowkey_idx && rowkey_idx < rowkey_end_idx_;
  }
  OB_INLINE void set_handle(const bool leaf, const int64_t begin_idx, const int64_t end_idx, const MacroBlockId &macro_id, const ObMicroBlockDataHandle &handle)
  {
    is_valid_ = true;
    is_leaf_block_ = leaf;
    rowkey_begin_idx_ = begin_idx;
    rowkey_end_idx_ = end_idx;
    macro_id_ = macro_id;
    handle_.reset();
    handle_ = handle;
  }
  TO_STRING_KV(K_(is_valid), K_(is_leaf_block), K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(macro_id));
  bool is_valid_;
  bool is_leaf_block_;
  int64_t rowkey_begin_idx_;
  int64_t rowkey_end_idx_;
  MacroBlockId macro_id_;
  ObMicroBlockDataHandle handle_;
};

class ObIndexTreeMultiPrefetcher : public ObIndexTreePrefetcher
{
public:
  static const int32_t MAX_MULTIGET_MICRO_DATA_HANDLE_CNT = 32;
  struct ObSSTableReadHandleExt : public ObSSTableReadHandle {
    ObSSTableReadHandleExt() :
      ObSSTableReadHandle(),
      cur_level_(-1),
      cur_prefetch_end_(false),
      micro_handle_idx_(0)
    {}
    ~ObSSTableReadHandleExt()
    {}
    void reuse()
    {
      ObSSTableReadHandle::reuse();
      cur_level_ = -1;
      cur_prefetch_end_ = false;
      micro_handle_idx_ = 0;
    }
    void reset()
    {
      ObSSTableReadHandle::reset();
      cur_level_ = -1;
      cur_prefetch_end_ = false;
      micro_handle_idx_ = 0;
      for (int64_t i = 0; i < DEFAULT_MULTIGET_MICRO_DATA_HANDLE_CNT; ++i) {
        micro_handles_[i].reset();
      }
    }
    OB_INLINE ObMicroBlockDataHandle& get_read_handle()
    {
      return micro_handles_[micro_handle_idx_ % DEFAULT_MULTIGET_MICRO_DATA_HANDLE_CNT];
    }
    OB_INLINE void set_cur_micro_handle(ObMicroBlockDataHandle &handle)
    {
      micro_handle_ = &handle;
      micro_handle_idx_++;
    }
    INHERIT_TO_STRING_KV("ObSSTableReadHandle", ObSSTableReadHandle,
        K_(cur_level), K_(cur_prefetch_end), K_(micro_handle_idx), K_(micro_handles));
    // TODO(yht146439) change to 2
    static const int64_t DEFAULT_MULTIGET_MICRO_DATA_HANDLE_CNT = 3;
    int16_t cur_level_;
    bool cur_prefetch_end_;
    int64_t micro_handle_idx_;
    ObMicroBlockDataHandle micro_handles_[DEFAULT_MULTIGET_MICRO_DATA_HANDLE_CNT];
  };
  typedef ObReallocatedFixedArray<ObSSTableReadHandleExt> ReadHandleExtArray;
  ObIndexTreeMultiPrefetcher() :
      is_rowkey_sorted_(false),
      fetch_rowkey_idx_(0),
      prefetch_rowkey_idx_(0),
      prefetched_rowkey_cnt_(0),
      max_handle_prefetching_cnt_(0),
      rowkeys_(nullptr),
      ext_read_handles_()
  {}
  virtual ~ObIndexTreeMultiPrefetcher() { reset(); }
  virtual void reset() override;
  virtual void reuse() override;
  virtual void reclaim() override;
  virtual int init(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const void *query_range) override;
  virtual int switch_context(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const void *query_range) override;
  int multi_prefetch();
  OB_INLINE bool is_prefetch_end() { return prefetched_rowkey_cnt_ >= rowkeys_->count(); }
  OB_INLINE void mark_cur_rowkey_prefetched(ObSSTableReadHandleExt &read_handle)
  {
    read_handle.cur_prefetch_end_ = true;
    prefetched_rowkey_cnt_++;
  }
  OB_INLINE void mark_cur_rowkey_fetched(ObSSTableReadHandleExt &read_handle)
  {
    fetch_rowkey_idx_++;
  }
  OB_INLINE ObSSTableReadHandleExt &current_read_handle()
  { return ext_read_handles_[fetch_rowkey_idx_ % MAX_MULTIGET_MICRO_DATA_HANDLE_CNT]; }
  OB_INLINE ObMicroBlockDataHandle &current_micro_handle()
  { return *ext_read_handles_[fetch_rowkey_idx_ % MAX_MULTIGET_MICRO_DATA_HANDLE_CNT].micro_handle_; }
  INHERIT_TO_STRING_KV("ObIndexTreePrefetcher", ObIndexTreePrefetcher, K_(index_tree_height), K_(is_rowkey_sorted),
      K_(fetch_rowkey_idx), K_(prefetch_rowkey_idx), K_(prefetched_rowkey_cnt), K_(max_handle_prefetching_cnt));
  bool is_rowkey_sorted_;
  int64_t fetch_rowkey_idx_;
  int64_t prefetch_rowkey_idx_;
  int64_t prefetched_rowkey_cnt_;
  int32_t max_handle_prefetching_cnt_;
  const common::ObIArray<blocksstable::ObDatumRowkey> *rowkeys_;
  ReadHandleExtArray ext_read_handles_;
  ObReallocatedFixedArray<ObCachedLevelMicroDataHandle> level_handles_;
  ObRowKeysInfo rowkeys_info_;
  ObReallocatedFixedArray<int8_t> row_states_;
private:
  void inner_reset();
  int init_for_sorted_multi_get();
  int drill_down(
      const MacroBlockId &macro_id,
      ObSSTableReadHandleExt &read_handle,
      const bool cur_level_is_leaf,
      const bool force_prefetch);
};

template <int32_t DATA_PREFETCH_DEPTH = 32, int32_t INDEX_PREFETCH_DEPTH = 3>
class ObIndexTreeMultiPassPrefetcher : public ObIndexTreePrefetcher
{
public:
  ObIndexTreeMultiPassPrefetcher() :
      is_prefetch_end_(false),
      is_row_lock_checked_(false),
      cur_range_fetch_idx_(0),
      cur_range_prefetch_idx_(0),
      cur_micro_data_fetch_idx_(-1),
      micro_data_prefetch_idx_(0),
      row_lock_check_version_(transaction::ObTransVersion::INVALID_TRANS_VERSION),
      agg_row_store_(nullptr),
      can_blockscan_(false),
      need_check_prefetch_depth_(false),
      use_multi_block_prefetch_(false),
      need_submit_io_(true),
      tree_handle_cap_(0),
      prefetch_depth_(1),
      max_range_prefetching_cnt_(0),
      max_micro_handle_cnt_(0),
      total_micro_data_cnt_(0),
      query_range_(nullptr),
      border_rowkey_(),
      read_handles_(),
      tree_handles_(nullptr),
      multi_io_params_()
  {}
  virtual ~ObIndexTreeMultiPassPrefetcher();
  virtual void reset() override;
  virtual void reuse() override;
  virtual void reclaim() override;
  virtual int init(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const void *query_range) override final;
  virtual int switch_context(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const void *query_range) override final;
  void inc_cur_micro_data_fetch_idx();
  virtual int prefetch();
  OB_INLINE ObSSTableReadHandle &current_read_handle()
  { return read_handles_[cur_range_fetch_idx_ % max_range_prefetching_cnt_]; }
  OB_INLINE ObMicroBlockDataHandle &current_micro_handle()
  { return micro_data_handles_[(cur_micro_data_fetch_idx_ + max_micro_handle_cnt_) % max_micro_handle_cnt_]; }
  OB_INLINE ObMicroIndexInfo &current_micro_info()
  { return micro_data_infos_[cur_micro_data_fetch_idx_ % max_micro_handle_cnt_]; }
  OB_INLINE bool is_current_micro_data_blockscan() const
  { return micro_data_infos_[cur_micro_data_fetch_idx_ % max_micro_handle_cnt_].can_blockscan(iter_param_->has_lob_column_out()); }
  OB_INLINE int64_t prefetching_range_idx()
  {
    return 0 == cur_level_ ? cur_range_prefetch_idx_ - 1 :
        tree_handles_[cur_level_].current_block_read_handle().index_info_.range_idx();
  }
  OB_INLINE bool is_not_border(ObMicroIndexInfo &index_info)
  { return !index_info.is_left_border() && !index_info.is_right_border(); }
  OB_INLINE bool can_index_filter_skip(ObMicroIndexInfo &index_info, ObSampleFilterExecutor *sample_executor)
  {
    return (nullptr == sample_executor || is_not_border(index_info))
            && index_info.has_agg_data()
            && index_info.can_blockscan(iter_param_->has_lob_column_out())
            && index_info.is_filter_uncertain();
  }
  virtual bool read_wait()
  {
    return !is_prefetch_end_ &&
        cur_range_fetch_idx_ >= prefetching_range_idx() &&
        (-1 == current_read_handle().micro_begin_idx_ ||
         cur_micro_data_fetch_idx_ > current_read_handle().micro_end_idx_);

  }
  OB_INLINE bool is_multi_check()
  {
    return ObStoreRowIterator::IteratorType::IteratorMultiRowLockCheck == iter_type_;
  }
  int refresh_blockscan_checker(const int64_t start_micro_idx, const blocksstable::ObDatumRowkey &rowkey);
  int check_blockscan(bool &can_blockscan);
  int check_row_lock(
      const blocksstable::ObMicroIndexInfo &index_info,
      bool &is_prefetch_end);
  // For columnar store.
  OB_INLINE virtual bool switch_to_columnar_scan()
  {
    return false;
  };
  OB_INLINE const blocksstable::ObDatumRowkey& get_border_rowkey()
  {
    return border_rowkey_;
  }
  OB_INLINE int16_t get_index_tree_height() const
  {
    return index_tree_height_;
  }
  OB_INLINE int32_t get_index_prefetch_depth() const
  {
    return INDEX_TREE_PREFETCH_DEPTH;
  }
  OB_INLINE int32_t get_micro_data_pefetch_depth() const
  {
    return DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT;
  }

  static const int16_t MIN_DATA_READ_BATCH_COUNT = 4;
  static const int16_t MAX_INDEX_TREE_HEIGHT = 16;
  static const int32_t MAX_DATA_PREFETCH_DEPTH = 32;
  static const int32_t MAX_INDEX_PREFETCH_DEPTH = 3;

  INHERIT_TO_STRING_KV("ObIndexTreeMultiPassPrefetcher", ObIndexTreePrefetcher,
                       K_(is_prefetch_end), K_(cur_range_fetch_idx), K_(cur_range_prefetch_idx), K_(max_range_prefetching_cnt),
                       K_(cur_micro_data_fetch_idx), K_(micro_data_prefetch_idx), K_(max_micro_handle_cnt),
                       K_(iter_type), K_(cur_level), K_(index_tree_height), K_(max_rescan_height), KP_(long_life_allocator), K_(prefetch_depth),
                       K_(total_micro_data_cnt), KP_(query_range), K_(tree_handle_cap),
                       K_(can_blockscan), K_(need_check_prefetch_depth), K_(use_multi_block_prefetch), K_(need_submit_io),
                       K(ObArrayWrap<ObIndexTreeLevelHandle>(tree_handles_, index_tree_height_)), K_(multi_io_params));
protected:
  int init_basic_info(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const void *query_range);
  struct ObIndexTreeLevelHandle;
  virtual int prefetch_index_tree();
  virtual int prefetch_micro_data();
  int try_add_query_range(ObIndexTreeLevelHandle &tree_handle);
  int drill_down();
  int prepare_read_handle(
      ObIndexTreeLevelHandle &tree_handle,
      ObSSTableReadHandle &read_handle);
  int check_data_infos_border(
      const int64_t start_pos,
      const int64_t end_pos,
      const blocksstable::ObDatumRowkey &border_rowkey,
      bool is_reverse);
  OB_INLINE void clean_blockscan_check_info()
  {
    can_blockscan_ = false;
    for (int16_t level = 0; level < index_tree_height_; level++) {
      tree_handles_[level].can_blockscan_ = false;
    }
    border_rowkey_.reset();
  }
  void reset_tree_handles();
  void reclaim_tree_handles();
  void inner_reset();
  virtual int init_tree_handles(const int64_t count);
  int get_prefetch_depth(int64_t &depth);
  int prefetch_data_block(
      const int64_t prefetch_idx,
      ObMicroIndexInfo &index_block_info,
      ObMicroBlockDataHandle &micro_handle);
  int prefetch_multi_data_block(const int64_t max_prefetch_idx);

  static const int32_t DEFAULT_SCAN_RANGE_PREFETCH_CNT = 4;
  static const int32_t DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT = DATA_PREFETCH_DEPTH;
  static const int32_t INDEX_TREE_PREFETCH_DEPTH = INDEX_PREFETCH_DEPTH;
  static const int32_t SSTABLE_MICRO_AVG_COUNT = 100;
  struct ObIndexBlockReadHandle {
    ObIndexBlockReadHandle() :
        end_prefetched_row_idx_(-1),
        index_info_(),
        data_handle_()
    {}
    void reuse()
    {
      end_prefetched_row_idx_ = -1;
    }
    void reset()
    {
      end_prefetched_row_idx_ = -1;
      data_handle_.reset();
    }
    TO_STRING_KV(K_(end_prefetched_row_idx));
    // last row idx prefetched by index block
    int64_t end_prefetched_row_idx_;
    // micro index info of index block
    ObMicroIndexInfo index_info_;
    // prefetched micro data handle
    ObMicroBlockDataHandle data_handle_;
  };
  struct ObIndexTreeLevelHandle {
    ObIndexTreeLevelHandle() :
        is_prefetch_end_(false),
        is_row_lock_checked_(false),
        can_blockscan_(false),
        read_idx_(0),
        fetch_idx_(-1),
        prefetch_idx_(-1),
        index_scanner_(),
        index_block_()
    {}
    virtual ~ObIndexTreeLevelHandle()
    {}
    void reuse()
    {
      is_prefetch_end_ = false;
      is_row_lock_checked_ = false;
      can_blockscan_ = false;
      read_idx_ = 0;
      fetch_idx_ = -1;
      prefetch_idx_ = -1;
      index_scanner_.reuse();
      for (int64_t i = 0; i < INDEX_TREE_PREFETCH_DEPTH; i++) {
        index_block_read_handles_[i].reuse();
      }
    }
    void reset()
    {
      is_prefetch_end_ = false;
      is_row_lock_checked_ = false;
      can_blockscan_ = false;
      read_idx_ = 0;
      fetch_idx_ = -1;
      prefetch_idx_ = -1;
      index_scanner_.reset();
      for (int64_t i = 0; i < INDEX_TREE_PREFETCH_DEPTH; i++) {
        index_block_read_handles_[i].reset();
      }
    }
        OB_INLINE int get_next_data_row(
        const bool is_multi_check,
        ObMicroIndexInfo &block_info)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(index_scanner_.get_next(block_info, is_multi_check))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "Fail to get_next index row", K(ret), K_(index_scanner));
        }
      } else if (OB_UNLIKELY(!block_info.is_data_block())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected not data block", K(ret), K(block_info));
      } else {
        if (can_blockscan_) {
          block_info.set_blockscan();
        }
        if (OB_FAIL(block_info.copy_skipping_filter_results(current_block_read_handle().index_info_))) {
          STORAGE_LOG(WARN, "Failed to copy skipping filter results", K_(current_block_read_handle().index_info));
        }
      }
      return ret;
    }
    OB_INLINE int get_next_index_row(
        const bool has_lob_out,
        ObMicroIndexInfo &block_info,
        ObIndexTreeMultiPassPrefetcher &prefetcher)
    {
      int ret = OB_SUCCESS;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(index_scanner_.get_next(block_info, prefetcher.is_multi_check()))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            STORAGE_LOG(WARN, "Fail to get_next index row", K(ret), K_(index_scanner));
          } else if (fetch_idx_ < prefetch_idx_) {
            if (OB_FAIL(forward(prefetcher, has_lob_out))) {
              STORAGE_LOG(WARN, "Fail to forward index tree handle", K(ret));
            }
          }
        } else {
          if (can_blockscan_) {
            block_info.set_blockscan();
          }
          if (OB_FAIL(block_info.copy_skipping_filter_results(current_block_read_handle().index_info_))) {
            STORAGE_LOG(WARN, "Failed to copy skipping filter results", K_(current_block_read_handle().index_info));
          }
          break;
        }
      }
      return ret;
    }
    OB_INLINE bool is_prefetch_end() const { return is_prefetch_end_; }
    OB_INLINE void set_prefetch_end() { is_prefetch_end_ = true; }
    OB_INLINE bool reach_scanner_end() { return index_scanner_.end_of_block(); }
    OB_INLINE ObIndexBlockReadHandle &current_block_read_handle()
    {
      OB_ASSERT(0 <= fetch_idx_);
      return index_block_read_handles_[fetch_idx_ % INDEX_TREE_PREFETCH_DEPTH];
    }
    int prefetch(
        const int64_t level,
        ObIndexTreeMultiPassPrefetcher &prefetcher);
    virtual int forward(
        ObIndexTreeMultiPassPrefetcher &prefetcher,
        const bool has_lob_out);
    OB_INLINE int check_blockscan(const blocksstable::ObDatumRowkey &border_rowkey)
    {
      int ret = OB_SUCCESS;
      if (!can_blockscan_) {
      } else if (index_scanner_.end_of_block()) {
      } else if (OB_FAIL(index_scanner_.check_blockscan(border_rowkey, can_blockscan_))) {
        STORAGE_LOG(WARN, "Fail to update_blockscan", K(ret), K(index_scanner_), K(border_rowkey));
      }
      return ret;
    }
    OB_INLINE const ObMicroIndexInfo &last_prefetched_index() const
    {
      OB_ASSERT(0 <= prefetch_idx_);
      return index_block_read_handles_[prefetch_idx_ % INDEX_TREE_PREFETCH_DEPTH].index_info_;
    }
    TO_STRING_KV(K_(is_prefetch_end), K_(can_blockscan), K_(fetch_idx), K_(prefetch_idx),
                 K_(read_idx), K_(index_scanner),
                 K(ObArrayWrap<ObIndexBlockReadHandle>(index_block_read_handles_, INDEX_TREE_PREFETCH_DEPTH)));
  public:
    bool is_prefetch_end_;
    bool is_row_lock_checked_;
    bool can_blockscan_;
    int32_t read_idx_;
    int32_t fetch_idx_;
    int32_t prefetch_idx_;
    ObIndexBlockRowScanner index_scanner_;
    ObMicroBlockData index_block_;
    ObIndexBlockReadHandle index_block_read_handles_[INDEX_TREE_PREFETCH_DEPTH];
  };
  typedef ObReallocatedFixedArray<ObSSTableReadHandle> ReadHandleArray;
  typedef ObReallocatedFixedArray<ObIndexTreeLevelHandle> IndexTreeLevelHandleArray;

public:
  bool is_prefetch_end_;
  bool is_row_lock_checked_;
  int64_t cur_range_fetch_idx_;
  int64_t cur_range_prefetch_idx_;
  int64_t cur_micro_data_fetch_idx_;
  int64_t micro_data_prefetch_idx_;
  int64_t row_lock_check_version_;
  ObAggregatedStore *agg_row_store_;
protected:
  bool can_blockscan_;
  bool need_check_prefetch_depth_;
  bool use_multi_block_prefetch_;
  bool need_submit_io_;
  int16_t tree_handle_cap_;
  int16_t prefetch_depth_;
  int32_t max_range_prefetching_cnt_;
  int32_t max_micro_handle_cnt_;
  int64_t total_micro_data_cnt_;
  union {
    const common::ObIArray<blocksstable::ObDatumRowkey> *rowkeys_; // for multi get/multi exist/single exist
    const blocksstable::ObDatumRange *range_; // for scan
    const common::ObIArray<blocksstable::ObDatumRange> *ranges_; // for multi scan
    const ObRowsInfo *rows_info_; // for row lock multi check
    const void *query_range_;
  };
  blocksstable::ObDatumRowkey border_rowkey_;
  ReadHandleArray read_handles_;
  ObIndexTreeLevelHandle *tree_handles_;
  ObMicroIndexInfo micro_data_infos_[DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT];
  ObMicroBlockDataHandle micro_data_handles_[DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT];
  ObMultiBlockIOParam multi_io_params_;
};

}
}
#endif //OB_STORAGE_OB_INDEX_TREE_PREFETCHER_H_
