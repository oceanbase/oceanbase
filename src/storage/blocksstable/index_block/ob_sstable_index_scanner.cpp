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

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/index_block/ob_sstable_index_scanner.h"


namespace oceanbase
{
namespace blocksstable
{

ObSSTableIndexScanParam::ObSSTableIndexScanParam()
  : skip_index_projector_(nullptr),
    index_read_info_(nullptr),
    query_flag_(),
    tablet_id_(),
    scan_level_(ScanLevel::MAX_SCAN_LEVEL)
{
}

int ObSSTableIndexScanParam::init(
    const ObIArray<ObSkipIndexColMeta> &skip_index_projector,
    const ObITableReadInfo &index_read_info,
    const ScanLevel scan_level,
    ObQueryFlag &query_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!index_read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index read info", K(ret));
  } else {
    skip_index_projector_ = &skip_index_projector;
    index_read_info_ = &index_read_info;
    query_flag_ = query_flag;
    scan_level_ = scan_level;
  }
  return ret;
}

ObSSTableIndexRow::ObSSTableIndexRow()
  : endkey_(nullptr),
    skip_index_row_()
{
}

void ObSSTableIndexRow::reset()
{
  endkey_ = nullptr;
  skip_index_row_.reset();
}

void ObSSTableIndexBlockLevelScanner::PrefetchItem::reset()
{
  data_handle_.reset();
  is_left_border_ = false;
  is_right_border_ = false;
}

ObSSTableIndexBlockLevelScanner::ObSSTableIndexBlockLevelScanner()
  : query_range_(),
    datum_utils_(nullptr),
    tablet_id_(),
    idx_row_scanner_(),
    idx_block_(),
    item_ring_buffer_(),
    read_idx_(-1),
    prefetch_idx_(-1),
    prefetch_depth_(0),
    last_prefetch_key_(),
    io_allocator_(nullptr),
    use_block_cache_(false),
    block_opened_(false),
    is_root_block_(false),
    iter_end_(false),
    is_inited_(false) {}

void ObSSTableIndexBlockLevelScanner::reset()
{
  query_range_.reset();
  datum_utils_ = nullptr;
  tablet_id_.reset();
  idx_row_scanner_.reset();
  idx_block_.reset();
  for (int64_t i = 0; i < MAX_PREFIX_DEPTH; ++i) {
    item_ring_buffer_[i].reset();
  }
  read_idx_ = -1;
  prefetch_idx_ = -1;
  prefetch_depth_ = 0;
  last_prefetch_key_.reset();
  io_allocator_ = nullptr;
  use_block_cache_ = false;
  is_root_block_ = false;
  block_opened_ = false;
  iter_end_ = false;
  is_inited_ = false;
}

int ObSSTableIndexBlockLevelScanner::init(
    const ObStorageDatumUtils &datum_utils,
    const common::ObQueryFlag &query_flag,
    const ObTabletID &tablet_id,
    const ObDatumRange &query_range,
    const int64_t nested_offset,
    const int64_t prefetch_depth,
    ObIAllocator &scan_allocator,
    ObIAllocator &io_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(prefetch_depth > MAX_PREFIX_DEPTH || prefetch_depth < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid prefetch depth", K(ret), K(prefetch_depth));
  } else if (OB_FAIL(idx_row_scanner_.init(datum_utils, scan_allocator, query_flag, nested_offset))) {
    LOG_WARN("fail to init idx row scanner", K(ret));
  } else {
    query_range_ = query_range;
    datum_utils_ = &datum_utils;
    tablet_id_ = tablet_id;
    read_idx_ = -1;
    prefetch_idx_ = -1;
    prefetch_depth_ = prefetch_depth;
    use_block_cache_ = query_flag.is_use_block_cache();
    io_allocator_ = &io_allocator;
    block_opened_ = false;
    is_root_block_ = false;
    iter_end_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableIndexBlockLevelScanner::get_next_row(ObMicroIndexInfo &index_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (!block_opened_) {
    if (is_prefetch_queue_empty()) {
      iter_end_ = true;
      ret = OB_ITER_END;
    } else if (FALSE_IT(++read_idx_)) {
    } else if (OB_FAIL(open_current_read_index_block())) {
      LOG_WARN("fail to open current read index block", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(idx_row_scanner_.get_next(index_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next index row", K(ret));
    } else {
      if (!is_prefetch_queue_empty()) {
        release_current_read_item();
        ++read_idx_;
        if (OB_FAIL(open_current_read_index_block())) {
          LOG_WARN("fail to open current read index block", K(ret), K_(read_idx));
        } else if (OB_FAIL(idx_row_scanner_.get_next(index_row))) {
          LOG_WARN("fail to get next index row after open next block", K(ret));
        }
      } else {
        // ret = OB_ITER_END;
        iter_end_ = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (get_current_read_item().is_right_border_) {
    // Currently we need to solve the situation that the last block iterated might not contains data in scan range
    //  and break the exclusive advance_to(rowkey) semantic, because of the start key of micro block was not recorded.
    // e.g:
    //    advance_to(k1) -> next() -> advance_to(k1) -> next()
    //    k1 is the max key in scan range that is stored in sstable, exclusively advance_to(k1) will
    //    always point to the last range if we don't check key of every index row on the right border here.
    int cmp_ret = 0;
    if (OB_FAIL(index_row.endkey_.compare(query_range_.end_key_, *datum_utils_, cmp_ret, false))) {
      LOG_WARN("failed to compare index row endkey with scan range end key",
          K(ret), K(index_row.endkey_), K(query_range_.end_key_));
    } else if (cmp_ret > 0 || (cmp_ret == 0 && !query_range_.get_border_flag().inclusive_end())) {
      // found first endkey beyond scan range
      iter_end_ = true;
    }
  }
  return ret;
}

int ObSSTableIndexBlockLevelScanner::prefetch_root_block(ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  bool contains_range = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(sstable.is_ddl_sstable())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ddl sstable not supported for sstable index block scanner", K(ret));
  } else if (OB_FAIL(sstable.get_index_tree_root(idx_block_))) {
    LOG_WARN("fail to get index tree root block", K(ret));
  } else if (OB_FAIL(open_root_index_block(idx_block_, query_range_, contains_range))) {
   LOG_WARN("fail to open root index block", K(ret));
  } else {
    last_prefetch_key_.set_compact_rowkey((&query_range_.end_key_));
    block_opened_ = true;
    is_root_block_ = true;
  }
  return ret;
}

int ObSSTableIndexBlockLevelScanner::prefetch_next_index_block(ObSSTableIndexBlockLevelScanner &parent_scanner)
{
  int ret = OB_SUCCESS;
  ObMicroIndexInfo idx_block_row;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(parent_scanner.get_next_row(idx_block_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next index row", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_UNLIKELY(idx_block_row.is_data_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected data block in index block scanner", K(ret), K(idx_block_row));
  } else if (last_prefetch_key_.is_valid() && OB_FAIL(idx_block_row.endkey_.compare(last_prefetch_key_, *datum_utils_, cmp_ret))) {
    LOG_WARN("failed to compare prefetch key with last prefetch key", K(ret), K(idx_block_row), K(last_prefetch_key_));
  } else if (last_prefetch_key_.is_valid() && cmp_ret <= 0) {
    // when parent advance to a key that is already prefetched, do not repeatedly prefetch block
  } else {
    ++prefetch_idx_;
    ObMicroBlockCacheKey key(MTL_ID(), idx_block_row);
    PrefetchItem &prefetch_item = get_current_prefetch_item();
    storage::ObMicroBlockDataHandle &prefetch_handle = get_current_prefetch_item().data_handle_;
    ObIndexMicroBlockCache &index_block_cache = ObStorageCacheSuite::get_instance().get_index_block_cache();
    prefetch_handle.reset();
    prefetch_handle.allocator_ = io_allocator_;
    if (OB_FAIL(index_block_cache.get_cache_block(key, prefetch_handle.cache_handle_))) {
      // prefetch on cache miss
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get cache block", K(ret));
      } else if (OB_FAIL(index_block_cache.prefetch(
          MTL_ID(),
          idx_block_row.get_macro_id(),
          idx_block_row,
          use_block_cache_,
          tablet_id_,
          prefetch_handle.io_handle_,
          io_allocator_))) {
        LOG_WARN("failed to prefetch next index block", K(ret), K(idx_block_row));
      } else {
        prefetch_handle.tenant_id_ = MTL_ID();
        prefetch_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
        prefetch_handle.macro_block_id_ = idx_block_row.get_macro_id();
        prefetch_handle.micro_info_.set(
            idx_block_row.get_block_offset(),
            idx_block_row.get_block_size(),
            idx_block_row.get_logic_micro_id(),
            idx_block_row.get_data_checksum());

      }
    } else {
      // cache hit
      prefetch_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
    }

    if (OB_SUCC(ret)) {
      prefetch_item.macro_id_ = idx_block_row.get_macro_id();
      // TODO: check if border is set correctly here
      prefetch_item.is_left_border_ = idx_block_row.is_left_border();
      prefetch_item.is_right_border_ = idx_block_row.is_right_border();
      last_prefetch_key_ = idx_block_row.endkey_;
      is_root_block_ = false;
    }
  }
  return ret;
}

// Assumption here is advance_to rowkey should be always larger than current iterating rowkey,
// or there would be undefined behavior based on different state of scanner.
int ObSSTableIndexBlockLevelScanner::advance_to(const ObDatumRowkey &rowkey, const bool inclusive)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  }

#ifndef OB_BUILD_PACKAGE
  if (FAILEDx(rowkey.compare(query_range_.start_key_, *datum_utils_, cmp_ret))) {
    LOG_WARN("failed to compare advance key with current scan range start key", K(ret), K_(query_range), K(rowkey));
  } else if (OB_UNLIKELY(cmp_ret < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid advance to a smaller key than current scan range", K(ret), K(rowkey), K_(query_range));
  }
#endif
  bool prefetched = false;
  const bool has_valid_prefetched_key = last_prefetch_key_.is_valid();
  if (OB_FAIL(ret)) {
  } else if (has_valid_prefetched_key && OB_FAIL(last_prefetch_key_.compare(rowkey, *datum_utils_, cmp_ret))) {
    LOG_WARN("failed to compare advance key with last prefetch key", K(ret));
  } else if (has_valid_prefetched_key && (cmp_ret > 0 || (cmp_ret == 0 && inclusive))) {
    prefetched = true;
    // advance to key is in block already prefetched
    query_range_.start_key_ = rowkey;
    if (inclusive) {
      query_range_.set_left_closed();
    } else {
      query_range_.set_left_open();
    }
    bool found_advanced_key = false;

    if (is_root_block_) {
      if (OB_FAIL(open_root_index_block(idx_block_, query_range_, found_advanced_key))) {
        LOG_WARN("fail to open root index block", K(ret));
      }
    } else {
      while (OB_SUCC(ret) && !found_advanced_key) {
        if (!block_opened_) {
          ++read_idx_;
        }
        get_current_read_item().is_left_border_ = true;
        if (OB_FAIL(open_current_read_index_block())) {
          if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
            LOG_WARN("fail to open current read index block", K(ret));
          } else {
            ret = OB_SUCCESS;
            release_current_read_item();
            if (!is_prefetch_queue_empty()) {
              ++read_idx_;
            } else {
              break;
            }
          }
        } else {
          found_advanced_key = true;
        }
      }

      if (OB_SUCC(ret) && !found_advanced_key) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block contains advanced key already prefetched but not found",
            K(ret), K_(last_prefetch_key), K_(query_range), K_(is_root_block));
      }
    }
  } else {
    // advance to rowkey that is not prefetched yet
    // cancel all prefetch io
    while (!is_prefetch_queue_empty()) {
      release_current_read_item();
      ++read_idx_;
      idx_row_scanner_.reuse();
      block_opened_ = false;
    }
  }
  return ret;
}

int ObSSTableIndexBlockLevelScanner::open_current_read_index_block()
{
  int ret = OB_SUCCESS;
  PrefetchItem &read_item = get_current_read_item();
  storage::ObMicroBlockDataHandle &read_handle = read_item.data_handle_;
  if (OB_UNLIKELY(read_idx_ > prefetch_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read_idx_ is greater than prefetch_idx_", K(ret), K(read_idx_), K(prefetch_idx_));
  } else if (OB_FAIL(read_handle.get_micro_block_data(nullptr, idx_block_, false))) {
    LOG_WARN("failed to get micro block data", K(ret), K_(read_idx), K_(prefetch_idx));
  } else if (FALSE_IT(idx_row_scanner_.reuse())) {
  } else if (OB_FAIL(idx_row_scanner_.open(
      read_item.macro_id_,
      idx_block_,
      query_range_,
      0,
      read_item.is_left_border_,
      read_item.is_right_border_))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("fail to open index row scanner", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    block_opened_ = true;
  }
  return ret;
}

int ObSSTableIndexBlockLevelScanner::open_root_index_block(
    const ObMicroBlockData &root_block,
    const ObDatumRange &range,
    bool &contains_range)
{
  int ret = OB_SUCCESS;
  idx_row_scanner_.reuse();
  if (OB_FAIL(idx_row_scanner_.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
      root_block,
      range,
      0,
      true,
      true))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("fail to open index row scanner", K(ret));
    } else {
      ret = OB_SUCCESS;
      contains_range = false;
    }
  } else {
    contains_range = true;
  }
  return ret;
}

void ObSSTableIndexBlockLevelScanner::release_current_read_item()
{
  if (read_idx_ > 0) {
    get_current_read_item().reset();
  }
  return;
}

ObSSTableIndexScanner::ObSSTableIndexScanner()
  : sstable_(nullptr),
    scan_range_(nullptr),
    scan_param_(nullptr),
    level_scanners_(),
    index_row_(),
    endkey_(),
    rowkey_buf_(),
    block_io_allocator_(),
    scan_allocator_(nullptr),
    is_inited_(false) {}

void ObSSTableIndexScanner::reset()
{
  sstable_ = nullptr;
  scan_range_ = nullptr;
  scan_param_ = nullptr;
  // reset scanners
  FOREACH(scanner, level_scanners_) {
    if (OB_NOT_NULL(*scanner)) {
      (*scanner)->reset();
      (*scanner)->~ObSSTableIndexBlockLevelScanner();
      if (OB_NOT_NULL(scan_allocator_)) {
        scan_allocator_->free(*scanner);
      }
    }
  }
  level_scanners_.reset();
  index_row_.reset();
  endkey_.reset();
  rowkey_buf_.reset();
  block_io_allocator_.reset();
  scan_allocator_ = nullptr;
  is_inited_ = false;
}

int ObSSTableIndexScanner::init(
    const ObDatumRange &scan_range,
    const ObSSTableIndexScanParam &scan_param,
    ObSSTable &sstable,
    ObIAllocator &scan_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTableIndexScanner init twice", K(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid() || !scan_range.is_valid() || !scan_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sstable), K(scan_range), K(scan_param));
  } else if (sstable.is_empty()) {
  } else if (OB_FAIL(block_io_allocator_.init(nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE, lib::ObMemAttr(MTL_ID(), "SSTIdxScanIO")))) {
    LOG_WARN("failed to init block io allocator", K(ret));
  } else if (FALSE_IT(scan_allocator_ = &scan_allocator)) {
  } else if (OB_FAIL(init_level_scanners(scan_range, scan_param, sstable))) {
    LOG_WARN("failed to init level scanners", K(ret));
  } else if (OB_FAIL(init_index_row(scan_param))) {
    LOG_WARN("failed to init index row", K(ret));
  }

  if (OB_SUCC(ret)) {
    sstable_ = &sstable;
    scan_range_ = &scan_range;
    scan_param_ = &scan_param;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableIndexScanner::get_next(const ObSSTableIndexRow *&index_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (sstable_->is_empty()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(try_prefetch())) {
    LOG_WARN("failed to try prefetch index block", K(ret));
  } else if (OB_FAIL(inner_get_next_index_row(index_row_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next index row", K(ret));
    }
  } else {
    index_row = &index_row_;
  }
  return ret;
}

int ObSSTableIndexScanner::advance_to(const ObDatumRowkey &rowkey, const bool inclusive)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (sstable_->is_empty()) {
    // skip
  } else {
    for (int64_t level = 0; OB_SUCC(ret) && level < level_scanners_.count(); ++level) {
      ObSSTableIndexBlockLevelScanner *curr_scanner = level_scanners_.at(level);
      if (OB_ISNULL(curr_scanner)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null level scanner", K(ret), K(level));
      } else if (curr_scanner->is_iter_end()) {
        // skip advance level scanner already finished iteration
      } else if (OB_FAIL(curr_scanner->advance_to(rowkey, inclusive))) {
        LOG_WARN("failed to advance current level scanner to rowkey", K(ret), K(rowkey));
      } else if (level > 0) {
        // try prefetch for non-root level scanner
        ObSSTableIndexBlockLevelScanner *parent_scanner = level_scanners_.at(level - 1);
        if (OB_ISNULL(parent_scanner)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null parent level scanner", K(ret), K(level));
        } else if (!curr_scanner->can_prefetch_next_block() || parent_scanner->is_iter_end()) {
          // skip prefetch
        } else if (OB_FAIL(curr_scanner->prefetch_next_index_block(*parent_scanner))) {
          LOG_WARN("failed to prefetch next index block", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObSSTableIndexScanner::try_prefetch()
{
  int ret = OB_SUCCESS;
  for (int64_t level = 1; OB_SUCC(ret) && level < level_scanners_.count(); ++level) {
    ObSSTableIndexBlockLevelScanner *curr_scanner = level_scanners_.at(level);
    ObSSTableIndexBlockLevelScanner *parent_scanner = level_scanners_.at(level - 1);
    if (OB_ISNULL(curr_scanner) || OB_ISNULL(parent_scanner)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to scanner", K(ret), K(level), KP(curr_scanner), KP(parent_scanner));
    }

    while (OB_SUCC(ret) && curr_scanner->can_prefetch_next_block() && !parent_scanner->is_iter_end()) {
      if (OB_FAIL(curr_scanner->prefetch_next_index_block(*parent_scanner))) {
        LOG_WARN("failed to get next row", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableIndexScanner::init_level_scanners(
    const ObDatumRange &scan_range,
    const ObSSTableIndexScanParam &scan_param,
    ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  int64_t scan_level_cnt = 0;
  ObSSTableMetaHandle sst_meta_handle;
  level_scanners_.set_allocator(scan_allocator_);

  switch (scan_param.get_scan_level()) {
  case ObSSTableIndexScanParam::ScanLevel::ROOT:
    scan_level_cnt = 1;
    break;
  case ObSSTableIndexScanParam::ScanLevel::LEAF:
    if (OB_FAIL(sstable.get_meta(sst_meta_handle))) {
      LOG_WARN("failed to get sstable meta handle", K(ret));
    } else {
      scan_level_cnt = sst_meta_handle.get_sstable_meta().get_data_index_tree_height();
    }
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan level", K(ret), K(scan_param));
    break;
  }


  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(level_scanners_.init(scan_level_cnt))) {
    LOG_WARN("failed to init level scanners", K(ret));
  } else if (OB_FAIL(level_scanners_.prepare_allocate(scan_level_cnt))) {
    LOG_WARN("failed to prepare allocate level scanners", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < scan_level_cnt; ++i) {
    ObSSTableIndexBlockLevelScanner *level_scanner = OB_NEWx(ObSSTableIndexBlockLevelScanner, scan_allocator_);
    if (OB_ISNULL(level_scanner)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for level scanner", K(ret));
    } else if (OB_FAIL(level_scanner->init(
        scan_param.get_index_read_info()->get_datum_utils(),
        scan_param.get_query_flag(),
        scan_param.get_tablet_id(),
        scan_range,
        sstable.get_macro_offset(),
        ObSSTableIndexBlockLevelScanner::DEFAULT_PREFETCH_DEPTH,
        *scan_allocator_,
        block_io_allocator_))) {
      LOG_WARN("failed to init sstable index block level scanner", K(ret));
    } else if (FALSE_IT(level_scanners_.at(i) = level_scanner)) {
    } else if (0 == i) {
      if (OB_FAIL(level_scanner->prefetch_root_block(sstable))) {
        LOG_WARN("failed to prefetch root block", K(ret));
      }
    } else if (OB_FAIL(level_scanner->prefetch_next_index_block(*level_scanners_.at(i - 1)))) {
      LOG_WARN("failed to prefetch next index block", K(ret), K(i));
    }
  }

  return ret;
}

int ObSSTableIndexScanner::init_index_row(
    const ObSSTableIndexScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_column_count = scan_param.get_index_read_info()->get_rowkey_count();
  const int64_t index_proj_column_count = scan_param.get_skip_index_projector().count();
  if (scan_param.need_project_skip_index()
      && OB_FAIL(index_row_.skip_index_row_.init(*scan_allocator_, index_proj_column_count))) {
    LOG_WARN("failed to init skip index row", K(ret));
  } else if (OB_FAIL(rowkey_buf_.init(*scan_allocator_, rowkey_column_count))) {
    LOG_WARN("failed to init datum row for rowkey datum buffer", K(ret));
  } else if (OB_FAIL(endkey_.assign(rowkey_buf_.storage_datums_, rowkey_column_count))) {
    LOG_WARN("failed to assign datum to endkey", K(ret));
  }
  return ret;
}

int ObSSTableIndexScanner::inner_get_next_index_row(ObSSTableIndexRow &index_row)
{
  int ret = OB_SUCCESS;
  ObMicroIndexInfo index_info;
  ObSSTableIndexBlockLevelScanner *level_scanner = get_target_level_scanner();
  if (OB_ISNULL(level_scanner)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null target level scanner", K(ret), K(level_scanners_.count()));
  } else if (OB_FAIL(level_scanner->get_next_row(index_info))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next micro index info", K(ret));
    }
  } else if (OB_FAIL(process_endkey(index_info, index_row))) {
    LOG_WARN("failed to process endkey", K(ret));
  } else if (OB_FAIL(project_skip_index_row(index_info, index_row))) {
    LOG_WARN("failed to project skip index row", K(ret));
  }
  return ret;
}

int ObSSTableIndexScanner::process_endkey(const ObMicroIndexInfo &index_info, ObSSTableIndexRow &index_row)
{
  int ret = OB_SUCCESS;
  if (index_info.endkey_.is_compact_rowkey()) {
    index_row.endkey_ = index_info.endkey_.get_compact_rowkey();
  } else if (index_info.endkey_.is_discrete_rowkey()) {
    const ObDiscreteDatumRowkey *discrete_rowkey = index_info.endkey_.get_discrete_rowkey();
    if (OB_FAIL(discrete_rowkey->rowkey_vector_->get_rowkey(discrete_rowkey->row_idx_, endkey_))) {
      STORAGE_LOG(WARN, "failed to get rowkey from discrete rowkey vector", K(ret));
    } else {
      index_row.endkey_ = &endkey_;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected endkey type", K(ret), K(index_info.endkey_.type_));
  }
  return ret;
}

int ObSSTableIndexScanner::project_skip_index_row(const ObMicroIndexInfo &index_info, ObSSTableIndexRow &index_row)
{
  int ret = OB_SUCCESS;
  if (!scan_param_->need_project_skip_index()) {
    // skip
  } else if (!index_info.is_pre_aggregated()) {
    // set result index row to null
    for (int64_t i = 0; i < index_row.skip_index_row_.get_column_count(); ++i) {
      index_row.skip_index_row_.storage_datums_[i].set_null();
    }
  } else {
    ObAggRowReader agg_reader;
    if (OB_FAIL(agg_reader.init(index_info.agg_row_buf_, index_info.agg_buf_size_))) {
      LOG_WARN("failed to init agg row reader", K(ret), K(index_info));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_param_->get_skip_index_projector().count(); ++i) {
      const ObSkipIndexColMeta &col_meta = scan_param_->get_skip_index_projector().at(i);
      ObDatum &read_datum = index_row.skip_index_row_.storage_datums_[i];
      if (OB_FAIL(agg_reader.read(col_meta, read_datum))) {
        LOG_WARN("failde to read one skip index agg datum", K(ret), K(col_meta));
      }
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
