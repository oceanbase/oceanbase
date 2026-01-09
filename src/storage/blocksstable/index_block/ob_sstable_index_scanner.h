/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_INDEX_BLOCK_OB_SSTABLE_INDEX_SCANNER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_INDEX_BLOCK_OB_SSTABLE_INDEX_SCANNER_H_

#include "storage/access/ob_micro_block_handle_mgr.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_util.h"

namespace oceanbase
{
namespace blocksstable
{

class ObSSTableIndexScanParam
{
public:
  enum ScanLevel
  {
    ROOT = 0,
    LEAF,
    MAX_SCAN_LEVEL,
  };
public:
  ObSSTableIndexScanParam();
  virtual ~ObSSTableIndexScanParam() {};
  bool need_project_skip_index() const
  {
    return nullptr != skip_index_projector_ && skip_index_projector_->count() > 0;
  }
  const ObIArray<ObSkipIndexColMeta> &get_skip_index_projector() const
  {
    OB_ASSERT(nullptr != skip_index_projector_);
    return *skip_index_projector_;
  }
  const ObITableReadInfo *get_index_read_info() const { return index_read_info_; }
  const ObQueryFlag &get_query_flag() const { return query_flag_; }
  const ObTabletID &get_tablet_id() const { return tablet_id_; }
  const ScanLevel &get_scan_level() const { return scan_level_; }
  int init(
      const ObIArray<ObSkipIndexColMeta> &skip_index_projector,
      const ObITableReadInfo &index_read_info,
      const ScanLevel scan_level,
      ObQueryFlag &query_flag);
  bool is_valid() const { return nullptr != index_read_info_; }
  TO_STRING_KV(KPC_(skip_index_projector), K_(query_flag), K_(tablet_id), K_(scan_level));
private:
  const ObIArray<ObSkipIndexColMeta> *skip_index_projector_;
  const ObITableReadInfo *index_read_info_; // read_info for index block
  ObQueryFlag query_flag_;
  common::ObTabletID tablet_id_;
  ScanLevel scan_level_;
};

struct ObSSTableIndexRow
{
  ObSSTableIndexRow();
  ~ObSSTableIndexRow() {}
  void reset();
  TO_STRING_KV(KPC_(endkey), K_(skip_index_row));
  const ObDatumRowkey *endkey_;
  ObDatumRow skip_index_row_;
};



class ObSSTableIndexBlockLevelScanner final
{
public:
  static constexpr int64_t DEFAULT_PREFETCH_DEPTH = 3;
  ObSSTableIndexBlockLevelScanner();
  virtual ~ObSSTableIndexBlockLevelScanner() { reset(); }

  struct PrefetchItem
  {
    PrefetchItem()
      : macro_id_(), data_handle_(), is_left_border_(false), is_right_border_(false) {}
    ~PrefetchItem() {}
    void reset();
    MacroBlockId macro_id_;
    storage::ObMicroBlockDataHandle data_handle_;
    bool is_left_border_;
    bool is_right_border_;
  };
  int init(
      const ObStorageDatumUtils &datum_utils,
      const common::ObQueryFlag &query_flag,
      const ObTabletID &tablet_id,
      const ObDatumRange &query_range,
      const int64_t nested_offset,
      const int64_t prefetch_depth,
      ObIAllocator &scan_allocator,
      ObIAllocator &io_allocator);
  int get_next_row(ObMicroIndexInfo &index_row);
  int prefetch_root_block(ObSSTable &sstable);
  int prefetch_next_index_block(ObSSTableIndexBlockLevelScanner &parent_scanner);
  int advance_to(const ObDatumRowkey &rowkey, const bool inclusive);
  bool can_prefetch_next_block() const { return prefetch_idx_ - read_idx_ < prefetch_depth_ - 1; }
  bool is_iter_end() const { return iter_end_; }
  void reset();
  TO_STRING_KV(K_(query_range), K_(read_idx), K_(prefetch_idx), K_(prefetch_depth),
      K_(last_prefetch_key), K_(iter_end), K_(is_inited));
private:
  PrefetchItem &get_current_read_item() { return item_ring_buffer_[read_idx_ % MAX_PREFIX_DEPTH]; }
  PrefetchItem &get_current_prefetch_item() { return item_ring_buffer_[prefetch_idx_ % MAX_PREFIX_DEPTH]; }
  bool is_prefetch_queue_empty() { return prefetch_idx_ == read_idx_; }
  int open_current_read_index_block();
  int open_root_index_block(
      const ObMicroBlockData &root_block,
      const ObDatumRange &range,
      bool &contains_range);
  void release_current_read_item();
private:
  static constexpr int64_t MAX_PREFIX_DEPTH = 3;
  ObDatumRange query_range_;
  const ObStorageDatumUtils *datum_utils_;
  common::ObTabletID tablet_id_;
  ObIndexBlockRowScanner idx_row_scanner_;
  ObMicroBlockData idx_block_;
  PrefetchItem item_ring_buffer_[MAX_PREFIX_DEPTH];
  int64_t read_idx_;
  int64_t prefetch_idx_;
  int64_t prefetch_depth_;
  ObCommonDatumRowkey last_prefetch_key_;
  ObIAllocator *io_allocator_;
  bool use_block_cache_;
  bool block_opened_;
  bool is_root_block_;
  bool iter_end_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableIndexBlockLevelScanner);
};

// iterator to scan sstable index on specific key range, and read block-level meta data(such as skip-index)
class ObSSTableIndexScanner final
{
public:
  ObSSTableIndexScanner();
  virtual ~ObSSTableIndexScanner() { reset(); }

  int init(
      const ObDatumRange &scan_range,
      const ObSSTableIndexScanParam &scan_param,
      ObSSTable &sstable,
      ObIAllocator &scan_allocator);
  // advance to next block range that might include @rowkey
  int advance_to(const ObDatumRowkey &rowkey, const bool inclusive);
  int get_next(const ObSSTableIndexRow *&index_row);
  void reset();
  TO_STRING_KV(K_(is_inited), KPC_(scan_range), KPC_(scan_param));
private:
  int try_prefetch();
  int init_level_scanners(
      const ObDatumRange &scan_range,
      const ObSSTableIndexScanParam &scan_param,
      ObSSTable &sstable);
  int init_index_row(const ObSSTableIndexScanParam &scan_param);
  int inner_get_next_index_row(ObSSTableIndexRow &index_row);
  int process_endkey(const ObMicroIndexInfo &index_info, ObSSTableIndexRow &index_row);
  int project_skip_index_row(const ObMicroIndexInfo &index_info, ObSSTableIndexRow &index_row);
  ObSSTableIndexBlockLevelScanner *get_target_level_scanner() { return level_scanners_.at(level_scanners_.count() - 1); }

private:
  const ObSSTable *sstable_;
  const ObDatumRange *scan_range_;
  const ObSSTableIndexScanParam *scan_param_;
  ObFixedArray<ObSSTableIndexBlockLevelScanner *, ObIAllocator> level_scanners_;
  ObSSTableIndexRow index_row_;
  ObDatumRowkey endkey_;
  ObDatumRow rowkey_buf_;
  ObFIFOAllocator block_io_allocator_;
  ObIAllocator *scan_allocator_;
  bool is_inited_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_INDEX_BLOCK_OB_SSTABLE_INDEX_SCANNER_H_
