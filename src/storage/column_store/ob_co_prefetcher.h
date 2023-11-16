// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_STORAGE_COLUMN_STORE_OB_CO_PREFETCHER_H_
#define OCEANBASE_STORAGE_COLUMN_STORE_OB_CO_PREFETCHER_H_
#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/column_store/ob_column_store_util.h"

namespace oceanbase {
namespace storage {

enum MicroDataBlockScanState
{
  // Open the next micro data block to determine border row id in current range.
  IN_END_OF_RANGE,
  // The border of current range is not determined, and the start rowid is determined.
  PENDING_BLOCK_SCAN,
  // The start and border of current range are both not determined.
  PENDING_SWITCH,
  // Row store scan.
  ROW_STORE_SCAN,
  MAX_SCAN_STATE
};

class ObCOPrefetcher : public ObIndexTreeMultiPassPrefetcher<>
{
public:
  static const int16_t INVALID_LEVEL = -1;
public:
  ObCOPrefetcher() :
      block_scan_state_(ROW_STORE_SCAN),
      block_scan_start_row_id_(OB_INVALID_CS_ROW_ID),
      block_scan_border_row_id_(OB_INVALID_CS_ROW_ID)
  {}
  virtual ~ObCOPrefetcher()
  {}
  virtual void reset() override final;
  virtual void reuse() override final;
  int refresh_blockscan_checker(
      const int64_t start_micro_idx,
      const blocksstable::ObDatumRowkey &rowkey);
  int refresh_blockscan_checker_for_column_store(
      const int64_t start_micro_idx,
      const blocksstable::ObDatumRowkey &rowkey);

  OB_INLINE void set_block_scan_state(MicroDataBlockScanState state)
  {
    block_scan_state_ = state;
  }
  OB_INLINE void set_start_row_id(ObCSRowId start_row_id)
  {
    block_scan_start_row_id_ = start_row_id;
  }
  OB_INLINE void set_border_row_id(ObCSRowId border_row_id)
  {
    block_scan_border_row_id_ = border_row_id;
  }
  OB_INLINE MicroDataBlockScanState get_block_scan_state() const
  {
    return block_scan_state_;
  }
  OB_INLINE bool is_in_row_store_scan_mode() const
  {
    return ROW_STORE_SCAN == block_scan_state_;
  }
  OB_INLINE bool is_in_switch_range_scan_mode()
  {
    return PENDING_SWITCH == block_scan_state_;
  }
  OB_INLINE bool has_valid_start_row_id()
  {
    return OB_INVALID_CS_ROW_ID != block_scan_start_row_id_;
  }
  OB_INLINE ObCSRowId cur_start_row_id()
  {
    blocksstable::ObMicroIndexInfo &micro_info = this->current_micro_info();
    const ObCSRange &cs_range = micro_info.get_row_range();
    return cs_range.begin();
  }
  OB_INLINE bool need_skip_prefetch()
  {
    return PENDING_BLOCK_SCAN == block_scan_state_;
  }
  OB_INLINE virtual bool switch_to_columnar_scan() const
  {
    return !is_in_row_store_scan_mode();
  }
  OB_INLINE void go_to_next_range()
  {
    ++cur_range_fetch_idx_;
  }
  OB_INLINE void adjust_start_row_id(
      const uint64_t row_count,
      const bool is_reverse_scan)
  {
    if (!is_reverse_scan) {
      block_scan_start_row_id_ -= row_count;
    } else {
      block_scan_start_row_id_ += row_count;
    }
  }
  OB_INLINE void adjust_border_row_id(
      const uint64_t row_count,
      const bool is_reverse_scan)
  {
    if (!is_reverse_scan) {
      block_scan_border_row_id_ += row_count;
    } else {
      block_scan_border_row_id_ -= row_count;
    }
  }
  OB_INLINE void set_range_scan_finish()
  {
    block_scan_start_row_id_ = OB_INVALID_CS_ROW_ID;
    block_scan_state_ = ROW_STORE_SCAN;
  }
  OB_INLINE ObCSRowId get_block_scan_start_row_id() { return block_scan_start_row_id_; }
  OB_INLINE ObCSRowId get_block_scan_border_row_id() const { return block_scan_border_row_id_; }

  OB_INLINE int is_last_range(bool &is_last)
  {
    int ret = OB_SUCCESS;
    switch (iter_type_) {
      case ObStoreRowIterator::IteratorScan:
      case ObStoreRowIterator::IteratorCOScan: {
        is_last = true;
        break;
      }
      case ObStoreRowIterator::IteratorMultiScan:
      case ObStoreRowIterator::IteratorCOMultiScan: {
        is_last = cur_range_fetch_idx_ >= (ranges_->count() - 1);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpceted iter_type",
                     K(ret), K_(iter_type));
      }
    }
    return ret;
  }

  INHERIT_TO_STRING_KV("ObCOPrefetcher", ObIndexTreeMultiPassPrefetcher, K_(block_scan_state),
                         K_(block_scan_start_row_id), K_(block_scan_border_row_id));

private:
  struct ObCOIndexTreeLevelHandle : public ObIndexTreeLevelHandle {
  public:
    virtual int forward(
        ObIndexTreeMultiPassPrefetcher &prefetcher,
        const bool has_lob_out) override final;
    int try_advancing_fetch_idx(
        ObCOPrefetcher &prefetcher,
        const ObDatumRowkey &border_rowkey,
        const int32_t range_idx,
        const bool is_reverse,
        const int32_t level,
        bool &is_advanced_to);
    int advance_to_border(
        const ObDatumRowkey &rowkey,
        const int32_t range_idx,
        ObCSRange &cs_range);
  };

private:
  int refresh_blockscan_checker_internal(
      const int64_t start_micro_idx,
      const blocksstable::ObDatumRowkey &rowkey);
  int refresh_each_levels(
      const ObDatumRowkey &border_rowkey,
      bool &is_advanced_to);
  int reset_each_levels(uint32_t start_level);
  void check_leaf_level_without_more_prefetch_data(const int64_t start_micro_idx);
  int refresh_blockscan_checker_in_leaf_level(
      const int64_t start_micro_idx,
      const blocksstable::ObDatumRowkey &border_rowkey);
  int advance_index_tree_level(
      ObCOIndexTreeLevelHandle &co_index_tree_level_handle);
  int skip_prefetch(
      ObCOIndexTreeLevelHandle &co_index_tree_level_handle);
  int try_jumping_to_next_skip_level(
      ObCOIndexTreeLevelHandle &co_index_tree_level_handle,
      bool is_out_of_range);
  OB_INLINE int16_t caculate_level(ObCOIndexTreeLevelHandle &co_index_tree_level_handle)
  {
    static_assert(sizeof(ObIndexTreeLevelHandle) == sizeof(ObCOIndexTreeLevelHandle),
                   "Size of ObCOIndexTreeLevelHandle should be equal with ObIndexTreeLevelHandle");
    return static_cast<ObIndexTreeLevelHandle*>(&co_index_tree_level_handle) - tree_handles_;
  }
  OB_INLINE bool is_blockscan_in_target_level(int32_t level)
  {
    OB_ASSERT(level >= 0 && level <= (index_tree_height_ - 1));
    return tree_handles_[level].can_blockscan_;
  }
  OB_INLINE void reset_blockscan_for_target_level(int32_t level)
  {
    OB_ASSERT(level >= 0 && level <= (index_tree_height_ - 1));
    tree_handles_[level].can_blockscan_ = false;
  }
  OB_INLINE void update_blockscan_for_target_level(
      int32_t old_level,
      int32_t new_level)
  {
    OB_ASSERT(new_level > 0 && new_level <= index_tree_height_);
    if (INVALID_LEVEL != old_level) {
      reset_blockscan_for_target_level(old_level);
    }
    if (index_tree_height_ == new_level) {
      block_scan_state_ = IN_END_OF_RANGE;
    } else {
      tree_handles_[new_level].can_blockscan_ = true;
    }
  }
  OB_INLINE int32_t get_cur_level_of_block_scan()
  {
    int32_t ret_level = INVALID_LEVEL;
    for (int32_t level = 0; level <= (index_tree_height_ - 1); ++level) {
      ObCOIndexTreeLevelHandle &tree_handle = static_cast<ObCOIndexTreeLevelHandle&>(tree_handles_[level]);
      if (tree_handle.can_blockscan_) {
        ret_level = level;
        break;
      }
    }
    return ret_level;
  }
  virtual int init_tree_handles(const int64_t count) override final;

public:
  MicroDataBlockScanState block_scan_state_;
  // When refresh_blockscan_checker is called,
  // block_scan_start_row_id_ will be set if we switch to scan column store.
  ObCSRowId block_scan_start_row_id_;
  // When refresh_blockscan_checker or prefetch is called,
  // block_scan_border_row_id_ will be updated if we are in column store mode.
  ObCSRowId block_scan_border_row_id_;
};

}
}
#endif // OCEANBASE_STORAGE_COLUMN_STORE_OB_CO_PREFETCHER_H_
