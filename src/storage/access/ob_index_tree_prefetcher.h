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
#include "storage/blocksstable/ob_index_block_row_struct.h"
#include "storage/blocksstable/ob_index_block_row_scanner.h"
#include "storage/blocksstable/ob_row_cache.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ob_micro_block_handle_mgr.h"
#include "storage/ob_table_store_stat_mgr.h"

namespace oceanbase {
using namespace blocksstable;
namespace storage {
class ObAggregatedStore;

struct ObSSTableRowState {
  enum ObSSTableRowStateEnum {
    UNKNOWN_STATE = 0,
    NOT_EXIST,
    IN_ROW_CACHE,
    IN_BLOCK
  };
};

struct ObSSTableReadHandle
{
public:
  ObSSTableReadHandle() :
      is_get_(false),
      is_bf_contain_(false),
      row_state_(0),
      range_idx_(-1),
      micro_begin_idx_(-1),
      micro_end_idx_(-1),
      query_range_(nullptr),
      row_handle_(),
      micro_handle_(nullptr)
  {}
  ~ObSSTableReadHandle()
  {}
  void reuse()
  {
    is_get_ = false;
    is_bf_contain_ = false;
    row_state_ = 0;
    range_idx_ = -1;
    micro_begin_idx_ = -1;
    micro_end_idx_ = -1;
    query_range_ = nullptr;
    row_handle_.reset();
  }
  void reset()
  {
    is_get_ = false;
    is_bf_contain_ = false;
    row_state_ = 0;
    range_idx_ = -1;
    micro_begin_idx_ = -1;
    micro_end_idx_ = -1;
    query_range_ = nullptr;
    micro_handle_ = nullptr;
    row_handle_.reset();
  }
  OB_INLINE bool is_valid() const
  { return nullptr != query_range_; }
  int get_block_data(ObMacroBlockReader &block_reader, blocksstable::ObMicroBlockData &block_data)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == micro_handle_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpect null micro_handle ", K(ret));
    } else if (OB_FAIL(micro_handle_->get_data_block_data(block_reader, block_data))) {
      STORAGE_LOG(WARN, "Fail to get block data ", K(ret));
    }
    return ret;
  }
  TO_STRING_KV(K_(is_get), K_(is_bf_contain), K_(row_state), K_(range_idx),
               K_(micro_begin_idx), K_(micro_end_idx), KP_(query_range), KPC_(micro_handle));

public:
  bool is_get_;
  bool is_bf_contain_;
  int8_t row_state_;    // possible states: NOT_EXIST, IN_ROW_CACHE, IN_BLOCK
  int64_t range_idx_;
  int64_t micro_begin_idx_;
  int64_t micro_end_idx_;
  union {
    const blocksstable::ObDatumRowkey *rowkey_;
    const blocksstable::ObDatumRange *range_;
    const void *query_range_;
  };
  ObRowValueHandle row_handle_;
  ObMicroBlockDataHandle *micro_handle_;
};

class ObIndexTreePrefetcher
{
public:
  ObIndexTreePrefetcher() :
      is_inited_(false),
      is_rescan_(false),
      rescan_cnt_(0),
      data_version_(0),
      sstable_(nullptr),
      sstable_meta_handle_(),
      data_block_cache_(nullptr),
      index_block_cache_(nullptr),
      micro_block_handle_mgr_(),
      index_block_(),
      iter_param_(nullptr),
      access_ctx_(nullptr),
	    datum_utils_(nullptr),
      index_scanner_(),
      micro_handles_(),
      macro_id_()
  {}
  virtual ~ObIndexTreePrefetcher()
  {}
  virtual void reset();
  virtual void reuse();
  virtual int init(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const void *query_range);
  virtual int switch_context(
      const int iter_type,
      ObSSTable &sstable,
      const ObStorageDatumUtils &datum_utils,
      ObTableAccessContext &access_ctx,
      const void *query_range);
  int single_prefetch(ObSSTableReadHandle &read_handle);
  OB_INLINE bool is_valid() { return is_inited_; }
  VIRTUAL_TO_STRING_KV(K_(data_version), K_(index_scanner));
protected:
  int init_index_scanner(ObIndexBlockRowScanner &index_scanner);
  int check_bloom_filter(const ObMicroIndexInfo &index_info, ObSSTableReadHandle &read_handle);
  int prefetch_block_data(
      ObMicroIndexInfo &index_block_info,
      ObMicroBlockDataHandle &micro_handle,
      const bool is_data = true);
  int lookup_in_cache(ObSSTableReadHandle &read_handle);
private:
  int lookup_in_index_tree(ObSSTableReadHandle &read_handle);
  ObMicroBlockDataHandle &get_read_handle(const int64_t level)
  {
    return micro_handles_[level % DEFAULT_GET_MICRO_DATA_HANDLE_CNT];
  }

protected:
  static const int64_t MAX_RESCAN_HOLD_LIMIT = 64;
  bool is_inited_;
  bool is_rescan_;
  int64_t rescan_cnt_;
  int64_t data_version_;
  ObSSTable *sstable_;
  ObSSTableMetaHandle sstable_meta_handle_;
  ObDataMicroBlockCache *data_block_cache_;
  ObIndexMicroBlockCache *index_block_cache_;
  ObMicroBlockHandleMgr micro_block_handle_mgr_;
  ObMicroBlockData index_block_;
  const ObTableIterParam *iter_param_;
  ObTableAccessContext *access_ctx_;
  const ObStorageDatumUtils *datum_utils_;
  common::ObFixedArray<int32_t, common::ObIAllocator> agg_projector_;
  common::ObFixedArray<share::schema::ObColumnSchemaV2, common::ObIAllocator> agg_column_schema_;
  static const int64_t DEFAULT_GET_MICRO_DATA_HANDLE_CNT = 2;
  ObIndexBlockRowScanner index_scanner_;
  private:
  ObMicroBlockDataHandle micro_handles_[DEFAULT_GET_MICRO_DATA_HANDLE_CNT];
  MacroBlockId macro_id_;
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
      index_block_info_(),
      micro_handle_idx_(0)
    {}
    ~ObSSTableReadHandleExt()
    {}
    void reuse()
    {
      ObSSTableReadHandle::reuse();
      cur_level_ = -1;
      cur_prefetch_end_ = false;
      index_block_info_.reset();
      micro_handle_idx_ = 0;
    }
    void reset()
    {
      ObSSTableReadHandle::reset();
      cur_level_ = -1;
      cur_prefetch_end_ = false;
      index_block_info_.reset();
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
    INHERIT_TO_STRING_KV("ObSSTableReadHandle", ObSSTableReadHandle, KPC_(rowkey),
        K_(cur_level), K_(cur_prefetch_end), K_(index_block_info), K_(micro_handle_idx), K_(micro_handles));
    // TODO(yht146439) change to 2
    static const int64_t DEFAULT_MULTIGET_MICRO_DATA_HANDLE_CNT = 3;
    int16_t cur_level_;
    bool cur_prefetch_end_;
    ObMicroIndexInfo index_block_info_;
    int64_t micro_handle_idx_;
    ObMicroBlockDataHandle micro_handles_[DEFAULT_MULTIGET_MICRO_DATA_HANDLE_CNT];
  };
  typedef ObReallocatedFixedArray<ObSSTableReadHandleExt> ReadHandleExtArray;
  ObIndexTreeMultiPrefetcher() :
      index_tree_height_(0),
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
  virtual int init(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const void *query_range) override;
  virtual int switch_context(
      const int iter_type,
      ObSSTable &sstable,
      const ObStorageDatumUtils &datum_utils,
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
  INHERIT_TO_STRING_KV("ObIndexTreePrefetcher", ObIndexTreePrefetcher, K_(index_tree_height),
      K_(fetch_rowkey_idx), K_(prefetch_rowkey_idx), K_(prefetched_rowkey_cnt), K_(max_handle_prefetching_cnt));
  int16_t index_tree_height_;
  int64_t fetch_rowkey_idx_;
  int64_t prefetch_rowkey_idx_;
  int64_t prefetched_rowkey_cnt_;
  int32_t max_handle_prefetching_cnt_;
  const common::ObIArray<blocksstable::ObDatumRowkey> *rowkeys_;
  ReadHandleExtArray ext_read_handles_;
private:
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
      iter_type_(0),
      cur_level_(0),
      index_tree_height_(0),
      prefetch_depth_(1),
      max_range_prefetching_cnt_(0),
      max_micro_handle_cnt_(0),
      total_micro_data_cnt_(0),
      query_range_(nullptr),
      border_rowkey_(),
      read_handles_(),
      tree_handles_()
  {}
  virtual ~ObIndexTreeMultiPassPrefetcher()
  {}
  virtual void reset() override final;
  virtual void reuse() override final;
  virtual int init(
      const int iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const void *query_range) override final;
  virtual int switch_context(
      const int iter_type,
      ObSSTable &sstable,
      const ObStorageDatumUtils &datum_utils,
      ObTableAccessContext &access_ctx,
      const void *query_range) override final;
  int prefetch();
  OB_INLINE ObSSTableReadHandle &current_read_handle()
  { return read_handles_[cur_range_fetch_idx_ % max_range_prefetching_cnt_]; }
  OB_INLINE ObMicroBlockDataHandle &current_micro_handle()
  { return micro_data_handles_[cur_micro_data_fetch_idx_ % max_micro_handle_cnt_]; }
  OB_INLINE ObMicroIndexInfo &current_micro_info()
  { return micro_data_infos_[cur_micro_data_fetch_idx_ % max_micro_handle_cnt_]; }
  OB_INLINE bool is_current_micro_data_blockscan() const
  { return micro_data_infos_[cur_micro_data_fetch_idx_ % max_micro_handle_cnt_].can_blockscan(iter_param_->has_lob_column_out()); }
  OB_INLINE int64_t prefetching_range_idx()
  {
    return 0 == cur_level_ ? cur_range_prefetch_idx_ - 1 :
        tree_handles_[cur_level_].current_block_read_handle().index_info_.range_idx();
  }
  OB_INLINE bool read_wait()
  {
    return !is_prefetch_end_ &&
        cur_range_fetch_idx_ >= prefetching_range_idx() &&
        (-1 == current_read_handle().micro_begin_idx_ ||
         cur_micro_data_fetch_idx_ > current_read_handle().micro_end_idx_);

  }
  int refresh_blockscan_checker(const int64_t start_micro_idx, const blocksstable::ObDatumRowkey &rowkey);
  int check_blockscan(bool &can_blockscan);
  int check_row_lock(
      const blocksstable::ObMicroIndexInfo &index_info,
      bool &is_prefetch_end);
  INHERIT_TO_STRING_KV("ObIndexTreeMultiPassPrefetcher", ObIndexTreePrefetcher,
                       K_(is_prefetch_end), K_(cur_range_fetch_idx), K_(cur_range_prefetch_idx), K_(max_range_prefetching_cnt),
                       K_(cur_micro_data_fetch_idx), K_(micro_data_prefetch_idx), K_(max_micro_handle_cnt),
                       K_(iter_type), K_(cur_level), K_(index_tree_height), K_(prefetch_depth),
                       K_(total_micro_data_cnt), KP_(query_range), K_(tree_handles), K_(border_rowkey),
                       K_(can_blockscan), K_(need_check_prefetch_depth));
private:
  int init_basic_info(
      const int iter_type,
      ObSSTable &sstable,
      ObTableAccessContext &access_ctx,
      const void *query_range,
      bool &is_multi_range);
  struct ObIndexTreeLevelHandle;
  int prefetch_index_tree();
  int prefetch_micro_data();
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
    for (int16_t level = 0; level < tree_handles_.count(); level++) {
      tree_handles_[level].can_blockscan_ = false;
    }
    border_rowkey_.reset();
  }

  static const int32_t DEFAULT_SCAN_RANGE_PREFETCH_CNT = 4;
  static const int32_t DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT = DATA_PREFETCH_DEPTH;
  static const int32_t INDEX_TREE_PREFETCH_DEPTH = INDEX_PREFETCH_DEPTH;
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
    OB_INLINE int get_next_data_row(ObMicroIndexInfo &block_info)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(index_scanner_.get_next(block_info))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "Fail to get_next index row", K(ret), K_(index_scanner));
        }
      } else if (OB_UNLIKELY(!block_info.is_data_block())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected not data block", K(ret), K(block_info));
      } else if (can_blockscan_) {
        block_info.set_blockscan();
      }
      return ret;
    }
    OB_INLINE int get_next_index_row(
        const blocksstable::ObDatumRowkey &border_rowkey,
        ObMicroIndexInfo &block_info,
        const bool has_lob_out)
    {
      int ret = OB_SUCCESS;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(index_scanner_.get_next(block_info))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            STORAGE_LOG(WARN, "Fail to get_next index row", K(ret), K_(index_scanner));
          } else if (fetch_idx_ < prefetch_idx_) {
            if (OB_FAIL(forward(border_rowkey, has_lob_out))) {
              STORAGE_LOG(WARN, "Fail to forward index tree handle", K(ret));
            }
          }
        } else {
          if (can_blockscan_) {
            block_info.set_blockscan();
          }
          break;
        }
      }
      return ret;
    }
    OB_INLINE bool is_prefetch_end() const
    { return is_prefetch_end_; }
    OB_INLINE void set_prefetch_end()
    { is_prefetch_end_ = true; }
    OB_INLINE bool reach_scanner_end()
    { return index_scanner_.end_of_block(); }
    OB_INLINE ObIndexBlockReadHandle &current_block_read_handle()
    {
      OB_ASSERT(0 <= fetch_idx_);
      return index_block_read_handles_[fetch_idx_ % INDEX_TREE_PREFETCH_DEPTH];
    }
    int prefetch(
        const blocksstable::ObDatumRowkey &border_rowkey,
        const int64_t level,
        ObIndexTreeMultiPassPrefetcher &prefetcher);
    int forward(
        const blocksstable::ObDatumRowkey &border_rowkey,
        const bool has_lob_out);
    OB_INLINE int check_blockscan(const blocksstable::ObDatumRowkey &border_rowkey)
    {
      int ret = OB_SUCCESS;
      if (!can_blockscan_) {
      } else if (OB_FAIL(index_scanner_.check_blockscan(border_rowkey, can_blockscan_))) {
        STORAGE_LOG(WARN, "Fail to update_blockscan", K(ret), K(index_scanner_), K(border_rowkey));
      }
      return ret;
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
private:
  bool can_blockscan_;
  bool need_check_prefetch_depth_;
  int16_t iter_type_;
  int16_t cur_level_;
  int16_t index_tree_height_;
  int16_t prefetch_depth_;
  int32_t max_range_prefetching_cnt_;
  int32_t max_micro_handle_cnt_;
  int64_t total_micro_data_cnt_;
  union {
    const common::ObIArray<blocksstable::ObDatumRowkey> *rowkeys_; // for multi get/multi exist/single exist
    const blocksstable::ObDatumRange *range_; // for scan
    const common::ObIArray<blocksstable::ObDatumRange> *ranges_; // for multi scan
    const void *query_range_;
  };
  blocksstable::ObDatumRowkey border_rowkey_;
  ReadHandleArray read_handles_;
  IndexTreeLevelHandleArray tree_handles_;
  ObMicroIndexInfo micro_data_infos_[DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT];
  ObMicroBlockDataHandle micro_data_handles_[DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT];
};

}
}
#endif //OB_STORAGE_OB_INDEX_TREE_PREFETCHER_H_
