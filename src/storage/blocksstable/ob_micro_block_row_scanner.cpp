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

#define USING_LOG_PREFIX STORAGE

#include "ob_micro_block_row_scanner.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/transaction/ob_trans_part_ctx.h"

using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace share::schema;
using namespace blocksstable;

/*****************           ObIMicroBlockRowScanner        ********************/
int ObIMicroBlockRowScanner::init(
    const ObTableIterParam& param, ObTableAccessContext& context, const ObSSTable* sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid()) || OB_UNLIKELY(!context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(param), K(context));
  } else {
    bool has_lob_column = false;
    if (OB_FAIL(param.has_lob_column_out(context.use_fuse_row_cache_, has_lob_column))) {
      STORAGE_LOG(WARN, "fail to check has lob column out", K(ret));
    } else if (has_lob_column && OB_NOT_NULL(sstable) && sstable->has_lob_macro_blocks()) {
      if (OB_FAIL(add_lob_reader(param, context, *sstable))) {
        STORAGE_LOG(WARN, "Failed to add lob reader", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    param_ = &param;
    context_ = &context;
    sstable_ = sstable;
    range_ = nullptr;
    if (NULL != reader_) {
      reader_->reset();
    }
    STORAGE_LOG(DEBUG, "init ObIMicroBlockRowScanner", "pkey", context.pkey_, K(param));
  }
  return ret;
}

int ObIMicroBlockRowScanner::set_range(const common::ObStoreRange& range)
{
  int ret = OB_SUCCESS;
  if (!range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "range is not valid", K(ret), K(range));
  } else {
    range_ = &range;
  }
  return ret;
}

int ObIMicroBlockRowScanner::open(const MacroBlockId& macro_id, const ObFullMacroBlockMeta& macro_meta,
    const ObMicroBlockData& block_data, const bool is_left_border, const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || nullptr == range_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret), KP(range_));
  } else if (OB_UNLIKELY(!macro_id.is_valid()) || OB_UNLIKELY(!block_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(macro_id), K(block_data));
  } else {
    reverse_scan_ = context_->query_flag_.is_reverse_scan();
    is_left_border_ = is_left_border;
    is_right_border_ = is_right_border;
    macro_id_ = macro_id;
    if (OB_FAIL(column_map_.rebuild(macro_meta, param_->need_build_column_map(column_map_.get_schema_version())))) {
      STORAGE_LOG(WARN, "Fail to rebuild col map, ", K(ret));
    } else if (OB_FAIL(set_reader(static_cast<ObRowStoreType>(macro_meta.meta_->row_store_type_)))) {
      STORAGE_LOG(WARN, "failed to set reader", K(ret), K(macro_meta.meta_->row_store_type_));
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::get_next_row(const ObStoreRow*& store_row)
{
  int ret = OB_SUCCESS;
  lob_reader_.reuse();
  if (OB_FAIL(inner_get_next_row(store_row))) {
  } else if (has_lob_column() && OB_FAIL(read_lob_columns(store_row))) {
    STORAGE_LOG(WARN, "Failed to read lob columns from store row", K(ret));
  }
  return ret;
}

int ObIMicroBlockRowScanner::get_next_rows(const storage::ObStoreRow*& rows, int64_t& count)
{
  int ret = OB_SUCCESS;
  if (has_lob_column()) {
    // To avoid reading lob rows in batches taking up too much memory,
    // temporarily degenerate into single row mode
    lob_reader_.reuse();
    if (OB_FAIL(inner_get_next_row(rows))) {
    } else if (OB_FAIL(read_lob_columns(rows))) {
      STORAGE_LOG(WARN, "Failed to read lob columns from store row", K(ret));
    } else {
      count = 1;
    }
  } else {
    if (OB_FAIL(inner_get_next_rows(rows, count))) {
      // do nothing, print log in upper layer if necessary
    }
  }
  if (OB_FAIL(ret)) {
    rows = NULL;
    count = 0;
  }
  return ret;
}

void ObIMicroBlockRowScanner::reset()
{
  lob_reader_.reset();
  has_lob_column_ = false;
  param_ = NULL;
  context_ = NULL;
  range_ = NULL;
  sstable_ = NULL;
  column_map_.reset();
  if (OB_NOT_NULL(reader_)) {
    reader_->reset();
  }
  current_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  start_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  last_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  reverse_scan_ = false;
  is_left_border_ = false;
  is_right_border_ = false;
  step_ = 1;
  macro_id_.reset();
  is_inited_ = false;
}

void ObIMicroBlockRowScanner::rescan()
{
  range_ = NULL;
  current_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  start_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  last_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  reverse_scan_ = false;
  is_left_border_ = false;
  is_right_border_ = false;
  macro_id_.reset();
}

int ObIMicroBlockRowScanner::set_reader(const ObRowStoreType store_type)
{
  int ret = OB_SUCCESS;
  switch (store_type) {
    case FLAT_ROW_STORE: {
      reader_ = &flat_reader_;
      break;
    }
    case SPARSE_ROW_STORE: {
      reader_ = &sparse_reader_;
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "not supported row store type", K(ret), K(store_type));
  }
  if (OB_SUCC(ret) && reader_->is_inited()) {
    reader_->reset();
  }
  return ret;
}

int ObIMicroBlockRowScanner::set_base_scan_param(const bool is_left_bound_block, const bool is_right_bound_block)
{
  int ret = OB_SUCCESS;
  int64_t start;
  int64_t last;

  if (OB_FAIL(locate_range_pos(is_left_bound_block, is_right_bound_block, start, last))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      STORAGE_LOG(WARN, "failed to locate start pos.", K(ret));
    }
  } else {
    if (reverse_scan_) {
      current_ = last;
      start_ = last;
      last_ = start;
      step_ = -1;
    } else {
      current_ = start;
      start_ = start;
      last_ = last;
      step_ = 1;
    }
  }

  if (OB_BEYOND_THE_RANGE == ret) {
    current_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
    start_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
    last_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
    ret = OB_SUCCESS;
  }
  STORAGE_LOG(DEBUG, "set_base_scan_param", K(current_), K(start_), K(last_));

  return ret;
}

int ObIMicroBlockRowScanner::add_lob_reader(
    const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, const ObSSTable& sstable)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(has_lob_column_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObISSTableRowIterator add lob reader twice", K(ret));
  } else {
    if (OB_FAIL(lob_reader_.init(access_ctx.query_flag_.is_daily_merge(), sstable))) {
      STORAGE_LOG(WARN,
          "Failed to init lob reader for ObISSTableRowIterator",
          K(sstable),
          K(access_ctx.query_flag_),
          K(iter_param),
          K(ret));
    } else {
      has_lob_column_ = true;
      STORAGE_LOG(DEBUG,
          "[LOB] success to add lob reader for ObISSTableRowIterator",
          K(sstable),
          K(access_ctx.query_flag_),
          K(iter_param));
    }
  }

  return ret;
}

int ObIMicroBlockRowScanner::read_lob_columns(const ObStoreRow* store_row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!has_lob_column_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobDataReader not init for ObISSTableRowIterator", K(ret));
  } else if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid row to read lob columns", K(ret));
  } else {
    ObStoreRow* row = const_cast<ObStoreRow*>(store_row);
    for (int64_t i = 0; OB_SUCC(ret) && i < row->row_val_.count_; ++i) {
      ObObj& obj = row->row_val_.cells_[i];
      if (ob_is_text_tc(obj.get_type())) {
        if (obj.is_lob_outrow()) {
          if (OB_FAIL(lob_reader_.read_lob_data(obj, obj))) {
            STORAGE_LOG(WARN,
                "Failed to read lob obj",
                K(obj.get_scale()),
                K(obj.get_meta()),
                K(obj.val_len_),
                K(macro_id_),
                K(ret));
          } else {
            STORAGE_LOG(
                DEBUG, "[LOB] Succ to load lob obj", K(obj.get_scale()), K(obj.get_meta()), K(obj.val_len_), K(ret));
          }
        } else if (obj.is_lob_inrow()) {
        } else {
          STORAGE_LOG(ERROR,
              "[LOB] Unexpected lob obj scale, to compatible, change to inrow mode",
              K(obj),
              K(obj.get_scale()),
              K(ret));
          obj.set_lob_inrow();
        }
      }
    }
  }

  return ret;
}

int ObIMicroBlockRowScanner::get_cur_micro_row_count(int64_t& row_count) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader_ is null", K(ret));
  } else if (OB_UNLIKELY(!is_inited_ || !reader_->is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Scanner not init", K(ret), K(is_inited_), K(reader_->is_inited()));
  } else if (OB_FAIL(reader_->get_row_count(row_count))) {
    STORAGE_LOG(WARN, "get micro row count failed, ", K(ret), K(row_count));
  }
  return ret;
}

int ObIMicroBlockRowScanner::locate_range_pos(
    const bool is_left_bound_block, const bool is_right_bound_block, int64_t& begin, int64_t& end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(range_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "range_ is null", K(ret), KP(range_));
  } else if (OB_FAIL(reader_->locate_range(*range_, is_left_bound_block, is_right_bound_block, begin, end))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      STORAGE_LOG(WARN, "fail to locate range", K(ret));
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::alloc_row(ObIAllocator& allocator, const int64_t cell_cnt, ObStoreRow& row)
{
  int ret = common::OB_SUCCESS;
  void* buf = NULL;
  int malloc_column_cnt = 0;
  if (context_->query_flag_.is_multi_version_minor_merge() && SPARSE_ROW_STORE == context_->read_out_type_) {
    malloc_column_cnt = OB_ROW_MAX_COLUMNS_COUNT;
  } else {
    malloc_column_cnt = cell_cnt;
  }
  if (OB_UNLIKELY(malloc_column_cnt <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(cell_cnt), K(malloc_column_cnt));
  } else if (row.row_val_.count_ >= malloc_column_cnt) {  // do nothing
  } else {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(common::ObObj) * malloc_column_cnt))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc obobjs failed", K(ret), K(malloc_column_cnt));
    } else {
      row.row_val_.cells_ = (common::ObObj*)buf;
    }
    if (OB_SUCC(ret) && SPARSE_ROW_STORE == context_->read_out_type_) {  // need to allocate column_ids
      if (OB_ISNULL(buf = allocator.alloc(sizeof(uint16_t) * malloc_column_cnt))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "alloc column ids failed", K(ret), K(malloc_column_cnt));
      } else {
        row.column_ids_ = (uint16_t*)buf;
      }
    }
  }

  if (OB_SUCC(ret)) {
    row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
    row.from_base_ = false;
    row.capacity_ = malloc_column_cnt;
    row.row_val_.count_ = cell_cnt;
    row.row_val_.projector_ = NULL;
    row.row_val_.projector_size_ = 0;
  }
  return ret;
}

int ObIMicroBlockRowScanner::fuse_row(const storage::ObStoreRow& former, storage::ObStoreRow& result,
    storage::ObNopPos& nop_pos, bool& final_result, storage::ObObjDeepCopy* obj_copy)
{
  int ret = common::OB_SUCCESS;
  if (is_row_empty(result)) {
    if (context_->query_flag_.is_multi_version_minor_merge()) {
      result.set_dml(former.get_dml());
      result.set_first_dml(former.get_first_dml());
    }
    result.snapshot_version_ = former.snapshot_version_;
  }
  if (OB_FAIL(storage::ObRowFuse::fuse_row(former, result, nop_pos, final_result, obj_copy))) {
    STORAGE_LOG(WARN, "failed to fuse row", K(ret), K(former), K(result));
  } else if (0 == result.row_val_.count_ &&
             common::ObActionFlag::OP_DEL_ROW == result.flag_) {  // copy rowkey, for minor merge
    result.row_val_.count_ = former.row_val_.count_;
    for (int64_t j = 0; OB_SUCC(ret) && j < result.row_val_.count_; ++j) {
      if (nullptr == obj_copy) {  // shallow copy
        result.row_val_.cells_[j] = former.row_val_.cells_[j];
      } else if (OB_FAIL((*obj_copy)(former.row_val_.cells_[j], result.row_val_.cells_[j]))) {
        STORAGE_LOG(WARN, "Failed to deep copy obj", K(ret));
      }
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::fuse_sparse_row(const storage::ObStoreRow& former, storage::ObStoreRow& result,
    ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>& bit_set, bool& final_result, storage::ObObjDeepCopy* obj_copy)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(result.column_ids_) || OB_ISNULL(former.column_ids_)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "column id is null", K(ret), K(result.column_ids_), K(former.column_ids_));
  } else {
    if (context_->query_flag_.is_multi_version_minor_merge() && is_row_empty(result)) {
      result.set_dml(former.get_dml());
      result.set_first_dml(former.get_first_dml());
    }
    if (OB_FAIL(storage::ObRowFuse::fuse_sparse_row(former, result, bit_set, final_result, obj_copy))) {
      STORAGE_LOG(WARN, "failed to fuse row", K(ret), K(former), K(result));
    } else if (0 == result.row_val_.count_ &&
               common::ObActionFlag::OP_DEL_ROW == result.flag_) {  // copy rowkey, for minor merge
      result.row_val_.count_ = former.row_val_.count_;
      for (int64_t j = 0; OB_SUCC(ret) && j < result.row_val_.count_; ++j) {
        if (OB_ISNULL(obj_copy)) {  // shallow copy
          result.row_val_.cells_[j] = former.row_val_.cells_[j];
        } else if (OB_FAIL((*obj_copy)(former.row_val_.cells_[j], result.row_val_.cells_[j]))) {
          STORAGE_LOG(WARN, "Failed to deep copy obj", K(ret));
        }
        if (OB_SUCC(ret)) {
          result.column_ids_[j] = former.column_ids_[j];
        }
      }  // end for
    }
  }
  return ret;
}

/*****************           ObMicroBlockRowScanner        ********************/
int ObMicroBlockRowScanner::init(const ObTableIterParam& param, ObTableAccessContext& context, const ObSSTable* sstable)
{
  int ret = OB_SUCCESS;
  const ObColDescIArray* out_cols = nullptr;
  const ObIArray<int32_t>* projector = nullptr;
  const share::schema::ColumnMap* column_id_map = nullptr;
  if (OB_FAIL(ObIMicroBlockRowScanner::init(param, context, sstable))) {
    STORAGE_LOG(WARN, "base init failed", K(ret));
  } else if (OB_FAIL(param_->get_out_cols(false /*is get*/, out_cols))) {
    STORAGE_LOG(WARN, "fail to get out cols", K(ret));
  } else if (OB_FAIL(param_->get_projector(false /*is get*/, projector))) {
    STORAGE_LOG(WARN, "fail to get projector", K(ret));
  } else if (OB_FAIL(param_->get_column_map(false /*is get*/, column_id_map))) {
    STORAGE_LOG(WARN, "fail to get column id map", K(ret));
  } else if (OB_FAIL(column_map_.init(*context_->allocator_,
                 param_->schema_version_,
                 param_->rowkey_cnt_,
                 0, /*store count*/
                 *out_cols,
                 column_id_map,
                 projector,
                 storage::ObMultiVersionRowkeyHelpper::MVRC_NONE /*multi version col type*/))) {
    STORAGE_LOG(WARN, "Fail to init col map, ", K(ret));
  } else {
    for (int64_t i = 0; i < ObIMicroBlockReader::OB_MAX_BATCH_ROW_COUNT; ++i) {
      ObStoreRow& row = rows_[i];
      row.row_val_.cells_ = reinterpret_cast<ObObj*>(obj_buf_) + i * OB_ROW_MAX_COLUMNS_COUNT;
      row.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
      row.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
    }
    is_inited_ = true;
  }
  return ret;
}

int ObMicroBlockRowScanner::open(const MacroBlockId& macro_id, const ObFullMacroBlockMeta& macro_meta,
    const ObMicroBlockData& block_data, const bool is_left_border, const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::open(macro_id, macro_meta, block_data, is_left_border, is_right_border))) {
    STORAGE_LOG(WARN, "base open failed", K(ret));
  } else if (OB_FAIL(reader_->init(block_data, &column_map_))) {
    STORAGE_LOG(WARN, "failed to init micro block reader", K(ret), K(macro_id));
  } else if (OB_FAIL(set_base_scan_param(is_left_border, is_right_border))) {
    STORAGE_LOG(WARN, "failed to set base scan param", K(ret), K(is_left_border), K(is_right_border), K(macro_id));
  }
  return ret;
}

OB_INLINE static void compat_old_dump_sstable_row(ObStoreRow* rows, const int64_t count)
{
  for (int64_t i = 0; i < count; ++i) {
    ObStoreRow* tmp_row = rows + i;
    tmp_row->row_type_flag_.set_last_multi_version_row(true);
    tmp_row->row_type_flag_.set_first_multi_version_row(true);
    tmp_row->row_type_flag_.set_compacted_multi_version_row(true);
  }
}

int ObMicroBlockRowScanner::inner_get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  row = NULL;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "fail to judge end of block or not, ", K(ret));
    }
  } else {
    ObStoreRow& dest_row = rows_[0];
    dest_row.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
    if (OB_FAIL(reader_->get_row(current_, dest_row))) {
      STORAGE_LOG(WARN, "micro block reader fail to get row.", K(ret));
    } else {
      row = &dest_row;
      if (context_->query_flag_.is_multi_version_minor_merge()) {
        compat_old_dump_sstable_row(const_cast<ObStoreRow*>(row), 1);
      }
      current_ += step_;
    }
  }
  return ret;
}

int ObMicroBlockRowScanner::inner_get_next_rows(const storage::ObStoreRow*& rows, int64_t& count)
{
  int ret = OB_SUCCESS;
  rows = nullptr;
  count = 0;
  while (OB_SUCC(ret) && count == 0) {
    if (OB_FAIL(end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "fail to judge end of block or not, ", K(ret));
      }
    } else if (OB_FAIL(reader_->get_rows(
                   current_, last_ + step_, ObIMicroBlockReader::OB_MAX_BATCH_ROW_COUNT, rows_, count))) {
      STORAGE_LOG(WARN, "fail to get rows", K(ret), K(current_), K(start_), K(last_), K(macro_id_), K(*sstable_));
    } else if (0 == count) {
      current_ += step_ * ObIMicroBlockReader::OB_MAX_BATCH_ROW_COUNT;
    } else {
      rows = rows_;
    }
  }
  STORAGE_LOG(DEBUG,
      "inner get next rows",
      K(ret),
      "is_iter_end",
      end_of_block(),
      K(current_),
      K(last_),
      K(step_),
      KP(rows),
      K(count));
  if (OB_SUCC(ret)) {
    if (context_->query_flag_.is_multi_version_minor_merge()) {
      compat_old_dump_sstable_row(const_cast<ObStoreRow*>(rows), count);
    }
    current_ += step_ * count;
  }
  return ret;
}

void ObMicroBlockRowScanner::reset()
{
  ObIMicroBlockRowScanner::reset();
}

int ObMultiVersionMicroBlockRowScanner::init(
    const ObTableIterParam& param, ObTableAccessContext& context, const ObSSTable* sstable)
{
  int ret = OB_SUCCESS;
  reset();
  const ObColDescIArray* out_cols = nullptr;
  const ObIArray<int32_t>* projector = nullptr;
  const share::schema::ColumnMap* column_id_map = nullptr;
  if (OB_FAIL(ObIMicroBlockRowScanner::init(param, context, sstable))) {
    STORAGE_LOG(WARN, "base init failed", K(ret));
  } else if (OB_FAIL(param_->get_out_cols(context.use_fuse_row_cache_, out_cols))) {
    STORAGE_LOG(WARN, "fail to get out cols", K(ret));
  } else if (OB_FAIL(param_->get_projector(context.use_fuse_row_cache_, projector))) {
    STORAGE_LOG(WARN, "fail to get projector", K(ret));
  } else if (OB_FAIL(param_->get_column_map(context.use_fuse_row_cache_, column_id_map))) {
    STORAGE_LOG(WARN, "fail to get column id map", K(ret));
  } else if (OB_FAIL(column_map_.init(*context_->allocator_,
                 param_->schema_version_,
                 param_->rowkey_cnt_,
                 0, /*store count*/
                 *out_cols,
                 column_id_map,
                 projector,
                 sstable->get_multi_version_rowkey_type()))) {
    STORAGE_LOG(WARN, "Fail to init col map, ", K(ret));
  } else if (!is_inited_) {
    cell_cnt_ = out_cols->count();
    if (OB_FAIL(alloc_row(*context.allocator_, cell_cnt_, cur_micro_row_))) {
      STORAGE_LOG(WARN, "failed to alloc row", K(ret), K(cell_cnt_));
    } else if (OB_FAIL(alloc_row(*context.allocator_, cell_cnt_, prev_micro_row_))) {
      STORAGE_LOG(WARN, "failed to alloc row", K(ret), K(cell_cnt_));
    } else if (OB_FAIL(nop_pos_.init(*context.allocator_, cell_cnt_))) {
      STORAGE_LOG(WARN, "failed to init nop_pos", K(ret), K(cell_cnt_));
    } else {
      const int64_t extra_multi_version_col_cnt =
          ObMultiVersionRowkeyHelpper::get_multi_version_rowkey_cnt(sstable->get_multi_version_rowkey_type());
      trans_version_col_idx_ = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
          param_->rowkey_cnt_, extra_multi_version_col_cnt);
      sql_sequence_col_idx_ = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
          param_->rowkey_cnt_, extra_multi_version_col_cnt);
    }
    if (OB_SUCC(ret)) {
      version_range_ = context.trans_version_range_;
      version_range_.base_version_ = std::max(version_range_.base_version_, sstable->get_base_version());
      is_inited_ = true;
    }
  }
  return ret;
}

/*****************           ObMultiVersionMicroBlockRowScanner        ********************/
int ObMultiVersionMicroBlockRowScanner::open(const MacroBlockId& macro_id, const ObFullMacroBlockMeta& macro_meta,
    const ObMicroBlockData& block_data, const bool is_left_border, const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::open(macro_id, macro_meta, block_data, is_left_border, is_right_border))) {
    STORAGE_LOG(WARN, "base open failed", K(ret));
  } else if (OB_FAIL(reader_->init(block_data, &column_map_))) {
    STORAGE_LOG(WARN, "failed to init micro block reader", K(ret), K(macro_id));
  } else if (OB_FAIL(set_base_scan_param(is_left_border, is_right_border))) {
    STORAGE_LOG(WARN, "failed to set base scan param", K(ret), K(is_left_border), K(is_right_border), K(macro_id));
  } else if (reverse_scan_) {
    reserved_pos_ = current_;
  }
  if (OB_SUCC(ret)) {
    read_row_direct_flag_ = false;
    if (OB_NOT_NULL(macro_meta.meta_) && OB_NOT_NULL(sstable_) && !macro_meta.meta_->contain_uncommitted_row_ &&
        macro_meta.meta_->max_merged_trans_version_ <= context_->trans_version_range_.snapshot_version_ &&
        0 == context_->trans_version_range_.base_version_) {
      // read_row_direct_flag_ can only be effective when read base version is zero
      // since we have no idea about the accurate base version of the macro block
      read_row_direct_flag_ = true;
      STORAGE_LOG(DEBUG,
          "use direct read",
          K(ret),
          KPC(macro_meta.meta_),
          K(sstable_->get_base_version()),
          K(context_->trans_version_range_));
    }
  }
  return ret;
}

void ObMultiVersionMicroBlockRowScanner::inner_reset()
{
  prev_micro_row_.reset();
  cur_micro_row_.reset();
  nop_pos_.reset();
  cell_allocator_.reset();
  reserved_pos_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  finish_scanning_cur_rowkey_ = true;
  is_last_multi_version_row_ = true;
  trans_version_col_idx_ = -1;
  sql_sequence_col_idx_ = -1;
  cell_cnt_ = 0;
  read_row_direct_flag_ = false;
}

void ObMultiVersionMicroBlockRowScanner::reset()
{
  inner_reset();
  ObIMicroBlockRowScanner::reset();
}

void ObMultiVersionMicroBlockRowScanner::rescan()
{
  nop_pos_.reset();
  cell_allocator_.reuse();
  reserved_pos_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  finish_scanning_cur_rowkey_ = true;
  is_last_multi_version_row_ = true;
  read_row_direct_flag_ = false;
  ObIMicroBlockRowScanner::rescan();
}

int ObMultiVersionMicroBlockRowScanner::inner_get_next_row_impl(const ObStoreRow*& ret_row)
{
  int ret = OB_SUCCESS;
  // TRUE:For the multi-version row of the current rowkey, when there is no row to be read in this micro_block
  bool final_result = false;
  // TRUE:For reverse scanning, if this micro_block has the last row of the previous rowkey
  bool found_first_row = false;
  const ObStoreRow* multi_version_row = NULL;
  ret_row = NULL;

  while (OB_SUCC(ret)) {
    final_result = false;
    found_first_row = false;
    if (OB_FAIL(locate_cursor_to_read(found_first_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "failed to locate cursor to read", K(ret), K_(macro_id));
      }
    }
    STORAGE_LOG(DEBUG,
        "locate cursor to read",
        K(ret),
        K(finish_scanning_cur_rowkey_),
        K(found_first_row),
        K(current_),
        K(reserved_pos_),
        K(last_),
        K_(macro_id));

    while (OB_SUCC(ret)) {
      multi_version_row = NULL;
      bool version_fit = false;
      bool iter_end = false;
      if (read_row_direct_flag_) {
        if (OB_FAIL(inner_get_next_row_directly(multi_version_row, version_fit, final_result))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            STORAGE_LOG(WARN, "failed to inner get next row directly", K(ret), K_(macro_id));
          }
        }
      } else if (OB_FAIL(inner_inner_get_next_row(multi_version_row, version_fit, final_result))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "failed to inner get next row", K(ret), K_(macro_id));
        }
      }
      if (OB_SUCC(ret)) {
        if (!version_fit) {
          // do nothing
        } else if (OB_FAIL(do_compact(multi_version_row, cur_micro_row_, final_result))) {
          STORAGE_LOG(WARN, "failed to do compact", K(ret));
        }
      }
      STORAGE_LOG(DEBUG,
          "do compact",
          K(ret),
          K(current_),
          K(version_fit),
          K(final_result),
          K(finish_scanning_cur_rowkey_),
          K_(macro_id),
          "cur_row",
          is_row_empty(cur_micro_row_) ? "empty" : to_cstring(cur_micro_row_),
          "multi_version_row",
          to_cstring(multi_version_row));

      if ((OB_SUCC(ret) && final_result) || OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(cache_cur_micro_row(found_first_row, final_result))) {
          STORAGE_LOG(WARN, "failed to cache cur micro row", K(ret), K_(macro_id));
        }
        STORAGE_LOG(DEBUG,
            "cache cur micro row",
            K(ret),
            K(finish_scanning_cur_rowkey_),
            K_(macro_id),
            "cur_row",
            is_row_empty(cur_micro_row_) ? "empty" : to_cstring(cur_micro_row_),
            "prev_row",
            is_row_empty(prev_micro_row_) ? "empty" : to_cstring(prev_micro_row_));
        break;
      }
    }

    if (OB_SUCC(ret) && finish_scanning_cur_rowkey_) {
      if (!is_row_empty(prev_micro_row_)) {
        ret_row = &prev_micro_row_;
      } else if (!is_row_empty(cur_micro_row_)) {
        ret_row = &cur_micro_row_;
      }
      // If row is NULL, means no multi_version row of current rowkey in [base_version, snapshot_version) range
      if (NULL != ret_row) {
        ret_row->row_type_flag_.set_uncommitted_row(false);
        break;
      }
    }
  }
  if (OB_NOT_NULL(ret_row) && !ret_row->is_valid()) {
    STORAGE_LOG(ERROR, "row is invalid", K(*ret_row));
  } else {
    STORAGE_LOG(DEBUG, "row is valid", K(*ret_row));
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::inner_get_next_row(const ObStoreRow*& row)
{
  reuse_cur_micro_row();
  return inner_get_next_row_impl(row);
}

int ObMultiVersionMicroBlockRowScanner::inner_get_next_rows(const storage::ObStoreRow*& rows, int64_t& count)
{
  int ret = OB_SUCCESS;
  rows = NULL;
  count = 0;
  if (OB_FAIL(inner_get_next_row(rows))) {
    if (ret != OB_ITER_END) {
      STORAGE_LOG(WARN, "fail to get next row", K(ret), K_(macro_id));
    }
  } else {
    count = 1;
  }
  return ret;
}

inline void ObMultiVersionMicroBlockRowScanner::reuse_cur_micro_row()
{
  cur_micro_row_.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  cur_micro_row_.from_base_ = false;
  nop_pos_.reset();
}

void ObMultiVersionMicroBlockRowScanner::reuse_prev_micro_row()
{
  for (int64_t i = 0; i < cell_cnt_; ++i) {
    prev_micro_row_.row_val_.cells_[i].set_nop_value();
  }
  prev_micro_row_.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  prev_micro_row_.from_base_ = false;
  cell_allocator_.reuse();

  reserved_pos_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
}

int ObMultiVersionMicroBlockRowScanner::locate_cursor_to_read(bool& found_first_row)
{
  int ret = OB_SUCCESS;
  bool need_locate_next_rowkey = false;
  found_first_row = false;

  STORAGE_LOG(DEBUG, "locate_cursor_to_read", K(finish_scanning_cur_rowkey_), K(is_last_multi_version_row_));
  if (!reverse_scan_) {
    if (OB_FAIL(end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "failed to judge end of block or not, ", K(ret), K_(macro_id));
      }
    } else if (finish_scanning_cur_rowkey_) {
      if (!is_last_multi_version_row_) {
        need_locate_next_rowkey = true;
      } else {
        finish_scanning_cur_rowkey_ = false;
      }
      reuse_prev_micro_row();
    }
  } else {
    if (ObIMicroBlockReader::INVALID_ROW_INDEX == reserved_pos_) {
      ret = OB_ITER_END;
    } else if (finish_scanning_cur_rowkey_) {
      current_ = reserved_pos_ - 1;  // skip current L
      reuse_prev_micro_row();
    }
    need_locate_next_rowkey = true;
  }

  // 1)For forward scanning: only after reading the current rowkey,
  // it will be locate the next rowkey by finding the last row in the current rowkey multi-version row;
  // 2)For reverse scan: If you have read a rowkey, first locate the last row of rowkey
  // that needs to be read through reserved_pos_, and then move forward.
  // If found the last row of the previous rowkey when positioning forward, then start reading,
  // otherwise, reading from the first row of this micro_block
  if (OB_SUCC(ret) && need_locate_next_rowkey) {
    const ObRowHeader* row_header = nullptr;
    ObMultiVersionRowFlag row_flag;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(end_of_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "failed to judge end of block or not, ", K(ret), K_(macro_id));
        } else {
          if (reverse_scan_) {
            // read from the first row
            finish_scanning_cur_rowkey_ = false;
            reserved_pos_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
            current_ = last_;
            found_first_row = is_left_border_;
            ret = OB_SUCCESS;
          } else {
            // return OB_ITER_END
          }
          break;
        }
      } else if (OB_FAIL(reader_->get_row_header(current_, row_header))) {
        STORAGE_LOG(WARN, "failed to get row header", K(ret), K(current_), K_(macro_id));
      } else {
        row_flag.flag_ = row_header->get_row_type_flag();
        if (row_flag.is_last_multi_version_row()) {
          finish_scanning_cur_rowkey_ = false;
          found_first_row = true;
          reserved_pos_ = current_;
          current_ = current_ + 1;
          break;
        } else {
          current_ = reverse_scan_ ? current_ - 1 : current_ + 1;
        }
      }
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::inner_get_next_row_directly(
    const storage::ObStoreRow*& ret_row, bool& version_fit, bool& final_result)
{
  int ret = OB_SUCCESS;
  ret_row = nullptr;
  version_fit = true;
  bool is_magic_row_flag = false;
  ObStoreRow* row = nullptr;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "failed to judge end of block or not, ", K(ret), K_(macro_id));
    }
  } else {
    if (is_row_empty(cur_micro_row_)) {
      row = &cur_micro_row_;
    } else {
      tmp_row_.row_val_.cells_ = reinterpret_cast<ObObj*>(tmp_row_obj_buf_);
      tmp_row_.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
      row = &tmp_row_;
    }
    if (OB_FAIL(reader_->get_row(current_, *row))) {
      STORAGE_LOG(WARN, "micro block reader fail to get block_row", K(ret), K(current_), K_(macro_id));
    } else if (OB_FAIL(ObMagicRowManager::is_magic_row(row->row_type_flag_, is_magic_row_flag))) {
      STORAGE_LOG(WARN, "fail to check magic row", K(ret), K_(current), KPC(row));
    } else if (OB_UNLIKELY(is_magic_row_flag)) {
      version_fit = false;
      row->flag_ = common::ObActionFlag::OP_ROW_DOES_NOT_EXIST;
      STORAGE_LOG(DEBUG, "is magic row", K(ret), K(current_), K(is_magic_row_flag));
    } else {
      row->snapshot_version_ = 0;
      ret_row = row;
    }
  }
  if (OB_SUCC(ret)) {
    is_last_multi_version_row_ = row->row_type_flag_.is_last_multi_version_row();
    final_result = is_last_multi_version_row_;
    // multi-version must be forward reading, and reverse positioning is processed in locate_cursor_to_read
    current_++;
    STORAGE_LOG(DEBUG, "inner get next row", KPC(ret_row), K_(is_last_multi_version_row), K_(macro_id));
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::inner_inner_get_next_row(
    const ObStoreRow*& ret_row, bool& version_fit, bool& final_result)
{
  int ret = OB_SUCCESS;
  ret_row = nullptr;
  version_fit = false;
  final_result = false;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "failed to judge end of block or not, ", K(ret), K_(macro_id));
    }
  } else {
    ObMultiVersionRowFlag flag;
    int64_t trans_version = 0;
    transaction::ObTransID trans_id;
    int64_t sql_sequence = 0;
    bool can_read = true;
    bool is_determined_state = false;
    bool read_uncommitted_row = false;
    bool is_magic_row_flag = false;
    const int64_t snapshot_version = context_->trans_version_range_.snapshot_version_;
    if (OB_UNLIKELY(context_->query_flag_.is_ignore_trans_stat())) {
      version_fit = true;
    } else if (OB_FAIL(reader_->get_multi_version_info(current_,
                   trans_version_col_idx_,
                   sql_sequence_col_idx_,
                   flag,
                   trans_id,
                   trans_version,
                   sql_sequence))) {
      STORAGE_LOG(WARN,
          "fail to get multi version info",
          K(ret),
          K(current_),
          K(trans_version_col_idx_),
          K(sql_sequence_col_idx_),
          K_(macro_id));
    } else if (flag.is_uncommitted_row()) {
      transaction::ObTransSnapInfo snapshot_info;
      if (OB_FAIL(context_->store_ctx_->get_snapshot_info(snapshot_info))) {
        TRANS_LOG(WARN, "get snapshot info failed", K(ret));
      } else {
        memtable::ObMemtableCtx& mem_ctx = (memtable::ObMemtableCtx&)(*(context_->store_ctx_->mem_ctx_));
        transaction::ObLockForReadArg lock_for_read_arg(mem_ctx,
            snapshot_version,
            context_->store_ctx_->trans_id_,
            trans_id,
            snapshot_info.get_read_sql_no(),
            sql_sequence,
            context_->query_flag_.read_latest_);

        if (OB_FAIL(lock_for_read(lock_for_read_arg, can_read, trans_version, is_determined_state))) {
          STORAGE_LOG(WARN, "fail to check transaction status", K(ret), K(trans_id));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObMagicRowManager::is_magic_row(flag, is_magic_row_flag))) {
      STORAGE_LOG(WARN, "fail to check magic row", K(ret), K_(current), K(trans_id), K(trans_version), K(sql_sequence));
    } else if (OB_UNLIKELY(is_magic_row_flag)) {
      can_read = false;
      is_determined_state = true;
      STORAGE_LOG(DEBUG, "is magic row", K(ret), K(current_), K(flag));
    }

    if (OB_SUCC(ret)) {
      is_last_multi_version_row_ = flag.is_last_multi_version_row();
      final_result = is_last_multi_version_row_;
      if (OB_UNLIKELY(context_->query_flag_.is_ignore_trans_stat())) {
        // do nothing
      } else if (!can_read) {
        if (!is_determined_state && context_->query_flag_.iter_uncommitted_row()) {  // for mark deletion
          version_fit = true;
          read_uncommitted_row = true;
        } else {
          version_fit = false;
        }
      } else if (!flag.is_uncommitted_row() || is_determined_state) {  // committed
        if (trans_version <= version_range_.base_version_) {
          version_fit = false;
          // filter multi version row whose trans version is smaller than base_version
          final_result = true;
        } else if (trans_version >
                   snapshot_version) {  // filter multi version row whose trans version is larger than snapshot_version
          version_fit = false;
        } else {
          version_fit = true;
        }
      } else {
        // read rows in current transaction
        version_fit = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (version_fit) {
        ObStoreRow* row = nullptr;
        if (is_row_empty(cur_micro_row_)) {
          row = &cur_micro_row_;
        } else {
          tmp_row_.row_val_.cells_ = reinterpret_cast<ObObj*>(tmp_row_obj_buf_);
          tmp_row_.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
          tmp_row_.trans_id_ptr_ = flag.is_uncommitted_row() ? &trans_id_ : NULL;
          row = &tmp_row_;
        }
        if (OB_FAIL(reader_->get_row(current_, *row))) {
          STORAGE_LOG(WARN, "micro block reader fail to get block_row", K(ret), K(current_), K_(macro_id));
        } else {
          if (read_uncommitted_row) {
            // Need to iterate out uncommitted rows when building a bloomfilter or marking for deletion,
            // The uncommitted rows are not yet determined,
            // so even if they are deleted, they must be treated as existing rows.
            row->flag_ = ObActionFlag::OP_ROW_EXIST;
          }
          if (!row->row_type_flag_.is_uncommitted_row() || is_determined_state) {
            row->snapshot_version_ = 0;
          } else {  // uncommitted row
            row->snapshot_version_ = INT64_MAX;
          }
          ret_row = row;
        }
      }
    }
    if (OB_SUCC(ret)) {
      // Multi-version must be forward reading, and reverse positioning is processed in locate_cursor_to_read
      current_++;
      STORAGE_LOG(DEBUG,
          "inner get next block_row",
          KPC(ret_row),
          K_(is_last_multi_version_row),
          K(trans_version),
          K_(macro_id));
    }
  }
  return ret;
}

/**
 * The current multi-version rowkey has many rows, and when a micro block or macro block cannot be stored,
 * it is necessary to temporarily store the results that have been read to prev_row
 */
int ObMultiVersionMicroBlockRowScanner::cache_cur_micro_row(const bool found_first_row, const bool final_result)
{
  int ret = OB_SUCCESS;
  ObObj* src_cell = NULL;
  ObObj* dest_cell = NULL;

  if (!reverse_scan_) {  // forward scan
    bool has_nop = true;
    if (final_result && is_row_empty(prev_micro_row_)) {
      // already get final result and no row cached, just return this row
      // do not need deep copy
    } else if (!is_row_empty(cur_micro_row_)) {
      if (is_row_empty(prev_micro_row_)) {  // Save static meta information when first cached
        prev_micro_row_.flag_ = cur_micro_row_.flag_;
        prev_micro_row_.from_base_ = cur_micro_row_.from_base_;
        prev_micro_row_.row_val_.count_ = cur_micro_row_.row_val_.count_;
      }
      // The positive scan scans from new to old (trans_version is negative), so cur_row is older than prev_row
      // So just add the nop column (column for which the value has not been decided)
      has_nop = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_micro_row_.row_val_.count_; ++i) {
        src_cell = &cur_micro_row_.row_val_.cells_[i];
        dest_cell = &prev_micro_row_.row_val_.cells_[i];
        if (dest_cell->is_nop_value()) {
          if (OB_FAIL(deep_copy_obj(cell_allocator_, *src_cell, *dest_cell))) {
            STORAGE_LOG(WARN, "failed to deep copy obj", K(ret));
          } else if (dest_cell->is_nop_value()) {
            has_nop = true;
          }
        }
      }
    }
    // Forward scan, as long as the fuse result of cur_row is final_result or the fuse result of prev_row has no nop,
    // it means that the current rowkey has been read
    if (final_result || !has_nop) {
      finish_scanning_cur_rowkey_ = true;
    }
  } else {  // Reverse scan
    // apply, since cur_row is newer than prev_row
    if (is_row_empty(prev_micro_row_)) {
      if (found_first_row) {
        // do not need deep copy
      } else if (!is_row_empty(cur_micro_row_)) {
        prev_micro_row_.flag_ = cur_micro_row_.flag_;
        prev_micro_row_.from_base_ = cur_micro_row_.from_base_;
        prev_micro_row_.row_val_.count_ = cur_micro_row_.row_val_.count_;
        const int64_t end_cell_pos =
            (ObActionFlag::OP_DEL_ROW == cur_micro_row_.flag_) ? param_->rowkey_cnt_ : cur_micro_row_.row_val_.count_;
        for (int64_t i = 0; OB_SUCC(ret) && i < end_cell_pos; ++i) {
          src_cell = &cur_micro_row_.row_val_.cells_[i];
          dest_cell = &prev_micro_row_.row_val_.cells_[i];
          if (OB_FAIL(deep_copy_obj(cell_allocator_, *src_cell, *dest_cell))) {
            STORAGE_LOG(WARN, "failed to deep copy obj", K(ret));
          }
        }
      }
    } else if (!is_row_empty(cur_micro_row_)) {
      prev_micro_row_.flag_ = cur_micro_row_.flag_;
      prev_micro_row_.from_base_ = cur_micro_row_.from_base_;
      prev_micro_row_.row_val_.count_ = cur_micro_row_.row_val_.count_;
      if (ObActionFlag::OP_DEL_ROW == cur_micro_row_.flag_) {
        for (int64_t i = param_->rowkey_cnt_; OB_SUCC(ret) && i < prev_micro_row_.row_val_.count_; ++i) {
          prev_micro_row_.row_val_.cells_[i].set_nop_value();
        }
      } else if (final_result) {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_micro_row_.row_val_.count_; ++i) {
          src_cell = &cur_micro_row_.row_val_.cells_[i];
          dest_cell = &prev_micro_row_.row_val_.cells_[i];
          if (OB_FAIL(deep_copy_obj(cell_allocator_, *src_cell, *dest_cell))) {
            STORAGE_LOG(WARN, "failed to deep copy obj", K(ret));
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_micro_row_.row_val_.count_; ++i) {
          src_cell = &cur_micro_row_.row_val_.cells_[i];
          dest_cell = &prev_micro_row_.row_val_.cells_[i];
          if (!src_cell->is_nop_value() && OB_FAIL(deep_copy_obj(cell_allocator_, *src_cell, *dest_cell))) {
            STORAGE_LOG(WARN, "failed to deep copy obj", K(ret));
          }
        }
      }
    }
    // Reverse scanning, only when the latest row is read,
    // that is, the last row of the previous rowkey is found,
    // can it be said that the current rowkey has been read.
    finish_scanning_cur_rowkey_ = found_first_row;
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::do_compact(const ObStoreRow* src_row, ObStoreRow& dest_row, bool& final_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_row)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "multi version row is NULL", K(ret));
  } else if (&dest_row == src_row) {
    if (dest_row.row_type_flag_.is_compacted_multi_version_row()) {
      final_result = true;
    } else {
      nop_pos_.reset();
      int64_t nop_count = 0;
      for (int64_t i = 0; i < dest_row.row_val_.count_; ++i) {
        if (dest_row.row_val_.cells_[i].is_nop_value()) {
          nop_pos_.nops_[nop_count++] = static_cast<int16_t>(i);
        }
      }
      nop_pos_.count_ = nop_count;
      if (0 == nop_count) {
        final_result = true;
      }
    }
  } else if (OB_FAIL(fuse_row(*src_row, dest_row, nop_pos_, final_result))) {
    STORAGE_LOG(WARN, "failed to fuse row", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (final_result || src_row->row_type_flag_.is_last_multi_version_row() ||
        src_row->row_type_flag_.is_compacted_multi_version_row()) {
      final_result = true;
    }
  }
  return ret;
}

// The transaction has been submitted or read the data in this transaction and sql_sequence has not been rollback
// can_read=true The situation of reading this sentence is more complicated, if read_latest=false, it is not readable if
// read_latest=true, it can be read If it has been submitted, assign the submitted transaction version number to version
// If it is not submitted but read_snapshot_version > prepare_timestamp needs to wait [timeout judgment]
int ObMultiVersionMicroBlockRowScanner::lock_for_read(const transaction::ObLockForReadArg& lock_for_read_arg,
    bool& can_read, int64_t& trans_version, bool& is_determined_state)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(context_->store_ctx_->trans_table_guard_->get_trans_state_table().lock_for_read(
          lock_for_read_arg, can_read, trans_version, is_determined_state))) {
    STORAGE_LOG(WARN, "failed to check transaction status", K(ret));
  }

  return ret;
}

/************     ObMultiVersionMicroBlockMinorMergeRowScanner    *************/

static inline int push_multi_version_col_desc(
    ObIArray<share::schema::ObColDesc>& out_cols, const int multi_version_rowkey_cnt)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < multi_version_rowkey_cnt; ++i) {
    share::schema::ObColDesc col_desc;
    col_desc.col_id_ = OB_MULTI_VERSION_EXTRA_ROWKEY[i].column_index_;
    col_desc.col_type_.set_int();
    if (OB_FAIL(out_cols.push_back(col_desc))) {
      STORAGE_LOG(WARN, "fail to push back column desc", K(ret));
    }
  }
  return ret;
}

static inline int build_minor_merge_out_cols(const storage::ObTableIterParam& param,
    ObIArray<share::schema::ObColDesc>& out_cols, const int multi_version_rowkey_cnt)
{
  int ret = OB_SUCCESS;
  out_cols.reset();
  const ObColDescIArray* param_out_cols = nullptr;
  if (OB_FAIL(param.get_out_cols(false /*is get*/, param_out_cols))) {
    STORAGE_LOG(WARN, "fail to get out cols", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_out_cols->count(); ++i) {
      if (OB_SUCC(ret) && OB_FAIL(out_cols.push_back(param_out_cols->at(i)))) {
        STORAGE_LOG(WARN, "fail to push back column desc", K(ret));
      }
      if (OB_SUCC(ret) && param.rowkey_cnt_ - 1 == i &&
          OB_FAIL(push_multi_version_col_desc(out_cols, multi_version_rowkey_cnt))) {
        STORAGE_LOG(WARN, "fail to push back multi version column desc", K(ret));
      }
    }
  }
  return ret;
}

ObMultiVersionMicroBlockMinorMergeRowScanner::ObMultiVersionMicroBlockMinorMergeRowScanner()
    : trans_version_col_idx_(ObIMicroBlockReader::INVALID_ROW_INDEX),
      sql_sequence_col_store_idx_(ObIMicroBlockReader::INVALID_ROW_INDEX),
      sql_sequence_col_idx_(ObIMicroBlockReader::INVALID_ROW_INDEX),
      row_allocator_(common::ObModIds::OB_SSTABLE_READER),
      allocator_(common::ObModIds::OB_SSTABLE_READER),
      row_queue_(),
      obj_copy_(row_allocator_),
      row_(),
      is_last_multi_version_row_(false),
      is_row_queue_ready_(false),
      add_sql_sequence_col_flag_(false),
      scan_state_(SCAN_START),
      committed_trans_version_(INT64_MAX),
      not_first_trans_flag_(false),
      read_trans_id_(),
      last_trans_id_(),
      first_rowkey_flag_(true),
      have_output_row_flag_(false),
      is_first_row_filtered_(false)
{
  for (int i = 0; i < RNPI_MAX; ++i) {
    nop_pos_[i] = NULL;
    bit_set_[i] = NULL;
  }
}

ObMultiVersionMicroBlockMinorMergeRowScanner::~ObMultiVersionMicroBlockMinorMergeRowScanner()
{}

int ObMultiVersionMicroBlockMinorMergeRowScanner::init_row_queue(const int64_t row_col_cnt)
{
  int ret = OB_SUCCESS;
  if (FLAT_ROW_STORE == context_->read_out_type_) {  // read flat row
    void* ptr = NULL;
    for (int i = 0; OB_SUCC(ret) && i < RNPI_MAX; ++i) {  // init nop pos
      if (NULL == (ptr = context_->allocator_->alloc(sizeof(storage::ObNopPos)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "fail to alloc nop pos");
      } else {
        nop_pos_[i] = new (ptr) storage::ObNopPos();
        if (OB_FAIL(nop_pos_[i]->init(*context_->allocator_, OB_ROW_MAX_COLUMNS_COUNT))) {
          STORAGE_LOG(WARN, "failed to init first row nop pos", K(ret));
        }
      }
    }
  } else if (SPARSE_ROW_STORE == context_->read_out_type_) {  // read sparse row
    void* ptr = NULL;
    for (int i = 0; OB_SUCC(ret) && i < RNPI_MAX; ++i) {  // init nop pos
      if (NULL == (ptr = context_->allocator_->alloc(sizeof(ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "fail to alloc nop pos", K(ret));
      } else {
        bit_set_[i] = new (ptr) ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>();
        bit_set_[i]->reset();
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid read out row type", K(ret), K(context_->read_out_type_));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_queue_.init(context_->read_out_type_, row_col_cnt))) {
      STORAGE_LOG(WARN, "failed to init row_queue", K(ret), K(row_col_cnt), K_(macro_id));
    }
  }
  return ret;
}

// init multi_version_rowkey(trans_version/sql_sequence) related infomation
void ObMultiVersionMicroBlockMinorMergeRowScanner::init_multi_version_rowkey_info(
    const int multi_version_rowkey_type, int64_t& expect_multi_version_col_cnt)
{
  const int64_t extra_multi_version_col_store_cnt =
      ObMultiVersionRowkeyHelpper::get_multi_version_rowkey_cnt(multi_version_rowkey_type);
  expect_multi_version_col_cnt =
      ObMultiVersionRowkeyHelpper::get_multi_version_rowkey_cnt(ObMultiVersionRowkeyHelpper::MVRC_VERSION_AFTER_3_0);
  trans_version_col_idx_ = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
      param_->rowkey_cnt_, extra_multi_version_col_store_cnt);
  sql_sequence_col_store_idx_ = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
      param_->rowkey_cnt_, extra_multi_version_col_store_cnt);
  sql_sequence_col_idx_ =
      ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(param_->rowkey_cnt_, expect_multi_version_col_cnt);
  // old version row need add sql_squence column
  add_sql_sequence_col_flag_ = (multi_version_rowkey_type == ObMultiVersionRowkeyHelpper::MVRC_OLD_VERSION);
}

// The scanner of the same sstable is shared, the previous state needs to be kept, so clear_status cannot be called
int ObMultiVersionMicroBlockMinorMergeRowScanner::init(
    const ObTableIterParam& param, ObTableAccessContext& context, const ObSSTable* sstable)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::init(param, context, sstable))) {
    STORAGE_LOG(WARN, "base init failed", K(ret));
  } else {
    const int multi_version_rowkey_type = sstable->get_multi_version_rowkey_type();
    int64_t expect_multi_version_col_cnt = 0;
    init_multi_version_rowkey_info(multi_version_rowkey_type, expect_multi_version_col_cnt);
    common::ObSEArray<share::schema::ObColDesc, OB_DEFAULT_SE_ARRAY_COUNT> out_cols;
    // minor merge should contain 2
    if (OB_FAIL(build_minor_merge_out_cols(*param_, out_cols, expect_multi_version_col_cnt))) {
      STORAGE_LOG(WARN, "fail to build minor merge out columns", K(ret));
    } else if (OB_FAIL(column_map_.init(*context_->allocator_,
                   param_->schema_version_,
                   param_->rowkey_cnt_,
                   0, /*store count*/
                   out_cols,
                   nullptr,
                   nullptr,
                   multi_version_rowkey_type))) {
      STORAGE_LOG(WARN, "Fail to init col map", K(ret), KPC_(param), K(multi_version_rowkey_type));
    } else if (OB_FAIL(init_row_queue(out_cols.count()))) {
      STORAGE_LOG(WARN, "Fail to init row queue", K(ret), K(out_cols.count()));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::open(const MacroBlockId& macro_id,
    const ObFullMacroBlockMeta& macro_meta, const ObMicroBlockData& block_data, const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::open(macro_id, macro_meta, block_data, is_left_border, is_right_border))) {
    STORAGE_LOG(WARN, "base open failed", K(ret));
  } else if (OB_UNLIKELY(reverse_scan_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "minor merge row scanner cannot do reverse scan", K(ret), K_(macro_id));
  } else {
    ObColumnMap* column_map_ptr = nullptr;
    if (SPARSE_ROW_STORE == macro_meta.meta_->row_store_type_ && SPARSE_ROW_STORE == context_->read_out_type_) {
      column_map_ptr = nullptr;  // read full sparse row
    } else {
      column_map_ptr = &column_map_;
    }
    if (OB_FAIL(reader_->init(block_data, column_map_ptr, context_->read_out_type_))) {
      STORAGE_LOG(WARN, "failed to init micro block reader", K(ret), K_(macro_id));
    } else if (OB_FAIL(set_base_scan_param(is_left_border, is_right_border))) {
      STORAGE_LOG(WARN, "failed to set base scan param", K(ret), K(is_left_border), K(is_right_border), K(macro_id));
    } else {
      row_.row_val_.cells_ = obj_buf_;
      row_.column_ids_ = col_id_buf_;
      row_.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
      row_.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
      STORAGE_LOG(DEBUG, "set read out type", K(ret), K(context_->read_out_type_));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::inner_get_next_rows(const storage::ObStoreRow*& rows, int64_t& count)
{
  // not support for now, simply wrap the original single row interface
  int ret = OB_SUCCESS;
  rows = NULL;
  count = 0;
  if (OB_FAIL(inner_get_next_row(rows))) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      STORAGE_LOG(WARN, "fail to get next row", K(ret), K_(macro_id));
    }
  } else {
    count = 1;
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::locate_last_committed_row()
{
  int ret = OB_SUCCESS;

  if (!is_last_multi_version_row_) {  // havn't meet Last row
    const ObRowHeader* row_header = nullptr;
    ObMultiVersionRowFlag row_flag;
    while (OB_SUCC(ret)) {  // move forward to find last row of this rowkey
      if (OB_FAIL(end_of_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "failed to check end of block", K(ret));
        } else if (is_right_border_) {
          ret = OB_SUCCESS;
          STORAGE_LOG(DEBUG, "find last micro of this macro block during locate last row", KPC(this));
        }
        break;
      } else if (OB_FAIL(reader_->get_row_header(current_, row_header))) {
        STORAGE_LOG(WARN, "failed to get row header", K(ret), K(current_), K_(macro_id));
      } else {
        ++current_;
        row_flag.flag_ = row_header->get_row_type_flag();
        if (row_flag.is_last_multi_version_row()) {
          is_last_multi_version_row_ = true;
          break;
        }
      }
    }
  }

  if (OB_SUCC(ret) && row_queue_.has_next()) {
    if (is_last_multi_version_row_) {
      complete_row_queue();
    }
    scan_state_ = GET_ROW_FROM_ROW_QUEUE;
  }
  STORAGE_LOG(DEBUG, "locate last multi version row", K(ret), KPC(this));
  return ret;
}

void ObMultiVersionMicroBlockMinorMergeRowScanner::complete_row_queue()
{
  if (!row_queue_.is_empty()) {
    ObStoreRow* last_row = row_queue_.get_last();
    const int64_t last_row_trans_version = -last_row->row_val_.cells_[trans_version_col_idx_].get_int();
    last_row->row_type_flag_.set_last_multi_version_row(true);
    if (last_row_trans_version <= context_->trans_version_range_.multi_version_start_) {
      last_row->row_type_flag_.set_compacted_multi_version_row(true);  //  For 2.1 version compatibility
    }
    row_queue_.get_first()->row_type_flag_.set_compacted_multi_version_row(true);  // set first row compacted
    STORAGE_LOG(DEBUG, "set last row", KPC(this), KPC(last_row));
  }
}

void ObMultiVersionMicroBlockMinorMergeRowScanner::reset()
{
  trans_version_col_idx_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  sql_sequence_col_idx_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  row_allocator_.reset();  // clear row memory
  allocator_.reset();
  row_queue_.reset();
  if (FLAT_ROW_STORE == context_->read_out_type_) {
    for (int i = 0; i < RNPI_MAX; ++i) {
      if (OB_NOT_NULL(nop_pos_[i])) {
        nop_pos_[i]->reset();
        nop_pos_[i] = NULL;
      }
    }
  } else {
    for (int i = 0; i < RNPI_MAX; ++i) {
      if (OB_NOT_NULL(bit_set_[i])) {
        bit_set_[i]->reset();
        bit_set_[i] = NULL;
      }
    }
  }
  is_row_queue_ready_ = false;
  first_rowkey_flag_ = true;
  row_.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  clear_scan_status();
  ObIMicroBlockRowScanner::reset();
}

void ObMultiVersionMicroBlockMinorMergeRowScanner::clear_scan_status()
{
  scan_state_ = SCAN_START;
  committed_trans_version_ = INT64_MAX;
  is_last_multi_version_row_ = false;
  not_first_trans_flag_ = false;
  have_output_row_flag_ = false;
  is_first_row_filtered_ = false;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::inner_get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  row = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (!row_queue_.has_next()) {  // empty row queue
    if (is_last_multi_version_row_) {
      clear_scan_status();  // clear transaction related infomation
      first_rowkey_flag_ = false;
    }
    if (is_row_queue_ready_) {
      clear_row_queue_status();  // clear row queue
    } else {
      STORAGE_LOG(DEBUG,
          "don't clear_row_queue_status ",
          K(ret),
          K(scan_state_),
          K(is_row_queue_ready_),
          K(is_last_multi_version_row_),
          K(row_queue_.count()));
    }
  }

  while (OB_SUCC(ret) && OB_ISNULL(row)) {
    switch (scan_state_) {
      case SCAN_START: {
        // meeet a new trans OR rowkey
        ret = find_uncommitted_row();
        break;
      }
      case PREPARE_COMMITTED_ROW_QUEUE: {
        // put all committed row into row_queue
        ret = prepare_committed_row_queue(row);
        break;
      }
      case GET_ROW_FROM_ROW_QUEUE: {
        // row_queue is prepared, get row to output
        ret = get_row_from_row_queue(row);
        break;
      }
      case FILTER_ABORT_TRANS_ROW: {
        // meet abort trans, filter all rows
        ret = filter_abort_trans_row(row);
        break;
      }
      case GET_RUNNING_TRANS_ROW: {
        // meet running trans, get row and output
        ret = get_running_trans_row(row);
        break;
      }
      case COMPACT_COMMIT_TRANS_ROW: {
        // meet commit trans, compact all rows into last row of row_queue
        ret = compact_commit_trans_row(row);
        break;
      }
      case LOCATE_LAST_COMMITTED_ROW: {
        // row queue is prepared(no row needed), just loop to meet Last row
        ret = locate_last_committed_row();
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "not support scan status", K(ret));
        break;
      }
    }  // end of switch
  }    // end of while

  if (OB_FAIL(ret)) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "switch status to call func error", K(ret), K(scan_state_));
    }
  } else if (OB_UNLIKELY(OB_ISNULL(row))) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "row must not null", K(ret));
  } else if (OB_UNLIKELY(!row->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "row is invalid", KPC(row), K(scan_state_));
  } else if (!row->row_type_flag_.is_uncommitted_row() && row->row_type_flag_.is_first_multi_version_row() &&
             !row->row_type_flag_.is_compacted_multi_version_row()) {
    // This section is defensive code to prevent illegal first row that is not compacted
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "invalid row", K(ret), KPC(this), KPC(row));
  } else if (row->is_delete() && !row->row_type_flag_.is_uncommitted_row()) {
    // set delete committed row compacted
    row->row_type_flag_.set_compacted_multi_version_row(true);
  }
  if (OB_SUCC(ret)) {
    have_output_row_flag_ = true;
    STORAGE_LOG(DEBUG, "output one row", K(ret), KPC(row));
  }

  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::prepare_committed_row_queue(const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  bool add_row_queue_flag = false;
  row_.trans_id_ptr_ = NULL;  // commit row does not have TransID
  while (OB_SUCC(ret) && PREPARE_COMMITTED_ROW_QUEUE == scan_state_ && !is_last_multi_version_row_) {
    if (OB_FAIL(read_committed_row(add_row_queue_flag, row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "failed to read committed row", K(ret), K(row_));
      } else if (is_right_border_) {
        ret = OB_SUCCESS;
      }
      break;
    } else if (add_row_queue_flag && OB_FAIL(add_row_into_row_queue(row))) {
      STORAGE_LOG(WARN, "failed to add row into row queue", K(ret), KPC(this));
    }
  }

  //  get one row from row_queue to return
  if (OB_SUCC(ret) && OB_ISNULL(row)) {
    if (is_last_multi_version_row_) {
      complete_row_queue();
    }
    if (PREPARE_COMMITTED_ROW_QUEUE == scan_state_) {
      scan_state_ = GET_ROW_FROM_ROW_QUEUE;
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::get_row_from_row_queue(const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (!row_queue_.has_next()) {
    ret = OB_ITER_END;
    STORAGE_LOG(DEBUG, "row queue is empty", K(ret), KPC(this));
  } else if (OB_FAIL(row_queue_.get_next_row(row))) {
    STORAGE_LOG(WARN, "failed to get next row from prepared row queue", K(ret), KPC(this));
  } else {
    STORAGE_LOG(DEBUG, "get row from row queue", K(ret), KPC(this), K(*row));
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::add_sql_sequence_col(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (GCONF._enable_sparse_row) {  // sparse row
    if (!row.is_sparse_row_ || OB_ISNULL(row.column_ids_) || row.row_val_.count_ >= row.capacity_) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), K(row));
    } else if (ObIMicroBlockReader::INVALID_ROW_INDEX == sql_sequence_col_idx_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "sql sequence col index is invalid", K(ret), K(sql_sequence_col_idx_));
    } else {
      for (int i = row.row_val_.count_; i > sql_sequence_col_idx_; --i) {  // move forward
        row.row_val_.cells_[i] = row.row_val_.cells_[i - 1];
        row.column_ids_[i] = row.column_ids_[i - 1];
      }
      row.row_val_.cells_[sql_sequence_col_idx_].set_int(0);
      row.column_ids_[sql_sequence_col_idx_] = OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID;
      row.row_val_.count_++;  // flus one
      STORAGE_LOG(DEBUG, "add_sql_sequence_col", K(ret), K(row), K(sql_sequence_col_idx_));
    }
  } else {  // flat row
    if (!row_.row_val_.cells_[sql_sequence_col_idx_].is_nop_value()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "sql sequence col should be NOP", K(ret), K(sql_sequence_col_idx_), K(row_));
    } else {
      row_.row_val_.cells_[sql_sequence_col_idx_].set_int(0);
    }
  }
  return ret;
}

// For the data with only one row for the same rowkey,
// it is passed directly from row as the outgoing parameter. Won't enter here.
// For data with more than one row of the same rowkey, it needs to be stored in row_queue,
// and then spit out in the order of newest version to oldest version
// There is a special case here. A rowkey may span multiple macro blocks.
// For incremental merge:
// Need to deal with the situation of reusing macro_blocks,
// so when the last micro_block in the macro_block is set is_right_border_,
// this class needs to be able to start spitting rows.
// When opening a new macro block after reusing the macro block, the state of this class will be cleared.
// The first row is not necessarily with First flag (the first row may be in the previous macro_block to be reused).
// At this time, if the scan snapshot version is smaller than the snapshot_version of the sstable, an error may be
// reported. For full scan (only logical migration will come): The data of the entire sstable uses the same scanner, and
// the status is inherited. At this time, scenarios where the scan snapshot version is smaller than the snapshot version
// of the sstable are allowed.

// The first row flag will only be used for the following scenarios:
// 1. This row is first row
// 2. The first row of the rowkey has been encountered during scan.
// The compact row flag will only be used for the following scenarios:
// 1. This row is compact row
// 2. The first row or last row no longer contains the nop column
// 3. First row or last row has been fuse with compact row.
// 4. First row or last row has been fuse with last row. (Last row itself is compact, but there may not necessarily be a
// compact flag)
// 5. Last row was scanned, and last row needs to be compact (set for compatibility with 2.1, in fact, for last row,
// setting compact is meaningless)
// 6. After scanning the last row, you need to set the compact flag for the first row
// The last row flag will only be set for the following scenarios:
// 1. This row is last row
// 2. The last row of the same rowkey has been scanned.

void ObMultiVersionMicroBlockMinorMergeRowScanner::clear_row_queue_status()
{
  STORAGE_LOG(DEBUG, "clear status about row queue");
  row_queue_.reuse();
  row_allocator_.reuse();  // clear row memory
  if (FLAT_ROW_STORE == context_->read_out_type_) {
    for (int i = 0; i < RNPI_MAX; ++i) {
      if (OB_NOT_NULL(nop_pos_[i])) {
        nop_pos_[i]->reset();
      }
    }
  } else {
    for (int i = 0; i < RNPI_MAX; ++i) {
      if (OB_NOT_NULL(bit_set_[i])) {
        bit_set_[i]->reset();
      }
    }
  }
  is_row_queue_ready_ = false;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::read_committed_row(
    bool& add_row_queue_flag, const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  add_row_queue_flag = true;
  bool is_magic_row_flag = false;
  bool is_filtered = false;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "failed to check end of block", K(ret));
    }
  } else if (OB_FAIL(reader_->get_row(current_, row_))) {
    STORAGE_LOG(WARN, "micro block reader fail to get row.", K(ret));
  } else if (OB_FAIL(ObMagicRowManager::is_magic_row(row_.row_type_flag_, is_magic_row_flag))) {
    STORAGE_LOG(WARN, "failed to check magic row", K(ret), K_(row));
  } else if (OB_UNLIKELY(is_magic_row_flag)) {
    add_row_queue_flag = false;
    ++current_;
  } else if (add_sql_sequence_col_flag_ && OB_FAIL(add_sql_sequence_col(row_))) {
    STORAGE_LOG(WARN, "add sql sequence col failed", K(ret), K(row_));
  } else {
    ++current_;
    if (context_->query_flag_.is_sstable_cut() && OB_FAIL(filter_unneeded_row(add_row_queue_flag, is_magic_row_flag))) {
      STORAGE_LOG(WARN, "failed to filter unneeded row", K(ret), K_(row));
    }
  }
  if (OB_SUCC(ret) && row_.row_type_flag_.is_last_multi_version_row()) {  // meet last row
    is_last_multi_version_row_ = true;

    if (row_queue_.is_empty()) {
      if (!is_magic_row_flag || first_rowkey_flag_ || have_output_row_flag_) {
        // three conditions need to output this row
        // 1. is not magic row
        // 2. this magic row is first rowkey in this macro block
        // 3. have output row of this rowkey before this magic row
        row = &row_;
      } else {
        // meet not-output magic row
        clear_scan_status();
      }
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::meet_uncommitted_last_row(
    const bool can_read, const storage::ObStoreRow*& row)
{
  is_last_multi_version_row_ = true;
  int ret = OB_SUCCESS;
  if (row_queue_.is_empty()) {  // need output a uncommitted row with Last Flag
    if (!can_read) {
      if (OB_FAIL(ObMagicRowManager::make_magic_row(sql_sequence_col_idx_, context_->query_flag_, row_))) {
        STORAGE_LOG(WARN, "failed to make magic row", K(ret), K(row_));
      }
    } else if (COMPACT_COMMIT_TRANS_ROW == scan_state_) {
      row_.row_type_flag_.set_uncommitted_row(false);
      row_.trans_id_ptr_ = nullptr;
    }
    if (OB_SUCC(ret)) {
      STORAGE_LOG(DEBUG, "meet_uncommitted_last_row", K(ret), K(row_));
      row = &row_;  // return this row
    }
  } else if (FILTER_ABORT_TRANS_ROW == scan_state_ || !can_read) {  // only set Last row flag
    // 1)there are rows in row queue
    // 2)current row can't be read(transaction abort OR sql_seq rollback)
    row_queue_.get_last()->row_type_flag_.set_last_multi_version_row(true);
    row_queue_.get_first()->row_type_flag_.set_compacted_multi_version_row(true);
    scan_state_ = GET_ROW_FROM_ROW_QUEUE;
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::read_uncommitted_row(bool& can_read, const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  ObMultiVersionRowFlag flag;
  int64_t trans_version = 0;
  int64_t sql_sequence = 0;
  row = nullptr;
  can_read = false;
  if (OB_FAIL(reader_->get_multi_version_info(current_,
          trans_version_col_idx_,
          sql_sequence_col_store_idx_,
          flag,
          read_trans_id_,
          trans_version,
          sql_sequence))) {
    STORAGE_LOG(WARN,
        "fail to get multi version info",
        K(ret),
        K(current_),
        K(trans_version_col_idx_),
        K(sql_sequence_col_store_idx_),
        K_(macro_id));
  } else if (flag.is_uncommitted_row()) {  // uncommitted row
    bool read_row_flag = false;
    if (check_meet_another_trans(read_trans_id_)) {  // another uncommitted trans
      // do nothing, jump to SCAN_START
    } else if (FILTER_ABORT_TRANS_ROW != scan_state_ &&
               OB_FAIL(check_curr_row_can_read(read_trans_id_, sql_sequence, can_read))) {  // same uncommitted trans
      STORAGE_LOG(WARN, "micro block reader fail to get row.", K(ret));
    } else if (!can_read && !flag.is_last_multi_version_row()) {  // can't read && not last row
      ++current_;                                                 // move forward
      read_row_flag = false;
    } else {
      read_row_flag = true;
    }
    if (OB_SUCC(ret) && read_row_flag) {
      if (OB_FAIL(reader_->get_row(current_, row_))) {  // read row
        STORAGE_LOG(WARN, "micro block reader fail to get row.", K(ret));
      } else if (OB_FAIL(set_trans_version_for_uncommitted_row(row_))) {
        STORAGE_LOG(WARN, "failed to set trans_version for uncommitted row", K(ret), K_(row));
      } else {
        ++current_;
        row_.trans_id_ptr_ = &read_trans_id_;
      }
      if (OB_SUCC(ret) && SCAN_START != scan_state_ && flag.is_last_multi_version_row()) {
        meet_uncommitted_last_row(can_read, row);
        STORAGE_LOG(DEBUG, "meet last row", K(ret), K_(row), K(scan_state_), K(can_read));
      }
    }
  } else {  // committed row
    scan_state_ = PREPARE_COMMITTED_ROW_QUEUE;
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::add_row_into_row_queue(const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  const common::ObVersionRange& version_range = context_->trans_version_range_;
  const int64_t trans_version = -row_.row_val_.cells_[trans_version_col_idx_].get_int();

  STORAGE_LOG(DEBUG, "prepare row queue, get row", K(ret), KPC(this), K(row_), K(trans_version), K(version_range));
  if (trans_version <= version_range.base_version_) {  // too old data
    STORAGE_LOG(WARN, "find too old row", "count", row_queue_.count(), K_(row));
  }

  if (is_last_multi_version_row_ && row_queue_.is_empty()) {  // return this row
    row_.row_type_flag_.set_compacted_multi_version_row(true);
    row = &row_;
  } else {
    is_row_queue_ready_ = true;  // set flag to call clear_row_queue_status
    // Be careful to use compact_first_row and compact_last_row
    // first_crow_nop_pos_ and last_crow_nop_pos_ inherit the state of the previous row
    // if a row have compacted to first row, it can't compact to last row
    const bool first_row_flag = row_queue_.is_empty() ? true : false;
    if (OB_FAIL(compact_first_row())) {
      STORAGE_LOG(WARN, "failed to compact first row", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "succeed to compact first row", KPC(row_queue_.get_first()));
    }

    if (OB_SUCC(ret) && !first_row_flag) {  // not first row
      if (trans_version > version_range.multi_version_start_) {
        row_.row_type_flag_.set_first_multi_version_row(false);   // clear flag
        if (OB_FAIL(row_queue_.add_row(row_, row_allocator_))) {  // no need to compact
          STORAGE_LOG(WARN, "failed to add row queue", K(ret), K(row_), K(row_queue_));
        } else {
          STORAGE_LOG(DEBUG, "succeed to add row", K(row_));
        }
      } else {  // trans_version <= version_range.multi_version_start_
        const int64_t last_row_trans_version =
            -row_queue_.get_last()->row_val_.cells_[trans_version_col_idx_].get_int();
        if (last_row_trans_version > version_range.multi_version_start_) {
          if (OB_FAIL(row_queue_.add_empty_row(row_allocator_))) {
            STORAGE_LOG(
                WARN, "failed to add row queue for first need compact last row", K(ret), K(row_), K(row_queue_));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(compact_last_row())) {
            STORAGE_LOG(WARN, "failed to compact last row", K(ret));
          } else if (row_queue_.get_last()->row_type_flag_.is_compacted_multi_version_row()) {
            // last row is compacted, could just filter the rest rows of current rowkey
            scan_state_ = LOCATE_LAST_COMMITTED_ROW;
            STORAGE_LOG(DEBUG, "success to compact last row", K(ret), K(row_));
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_row(
    const ObStoreRow& former, const int64_t row_compact_info_index, ObStoreRow& result)
{
  int ret = OB_SUCCESS;
  bool final_result = false;
  if (OB_UNLIKELY(row_compact_info_index < 0 || row_compact_info_index >= RNPI_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row compact info index", K(ret), K(row_compact_info_index));
  } else if (former.is_sparse_row_) {  // sparse row
    if (OB_UNLIKELY(OB_ISNULL(bit_set_[row_compact_info_index]))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "bit set is null", K(ret), K(row_compact_info_index));
    } else if (OB_FAIL(fuse_sparse_row(former, result, *bit_set_[row_compact_info_index], final_result, &obj_copy_))) {
      STORAGE_LOG(WARN, "failed to fuse sparse row", K(ret), K(row_compact_info_index));
    }
  } else {  // flat row
    if (OB_UNLIKELY(OB_ISNULL(nop_pos_[row_compact_info_index]))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "nop pos is null", K(ret), K(row_compact_info_index));
    } else if (OB_FAIL(fuse_row(former, result, *nop_pos_[row_compact_info_index], final_result, &obj_copy_))) {
      STORAGE_LOG(WARN, "failed to fuse row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (final_result || former.row_type_flag_.is_last_multi_version_row() ||
        former.row_type_flag_.is_compacted_multi_version_row()) {
      result.row_type_flag_.set_compacted_multi_version_row(true);
      STORAGE_LOG(DEBUG, "set compact row", K(ret), K(final_result), K(former));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_trans_row_to_one()
{
  int ret = OB_SUCCESS;
  if (row_queue_.is_empty() ||
      -committed_trans_version_ != row_queue_.get_last()->row_val_.cells_[trans_version_col_idx_].get_int()) {
    // It belongs to a different transaction from the last row in the row queue,
    // and a new row belonging to this transaction needs to be added
    if (OB_FAIL(row_queue_.add_empty_row(row_allocator_))) {  // add one empty row
      STORAGE_LOG(WARN, "failed to add row", K(ret));
    } else {
      STORAGE_LOG(DEBUG,
          "add one row for another trans",
          K(row_queue_.count()),
          K(committed_trans_version_),
          K(*row_queue_.get_last()));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(row_queue_.get_last())) {
    ObStoreRow* result_row = row_queue_.get_last();
    const int nop_pos_index = (1 == row_queue_.count()) ? RNPI_FIRST_ROW : RNPI_TRANS_COMPACT_ROW;
    // In fuse_row, if it is the first time fuse will clear nop_pos/bit_set
    if (OB_FAIL(compact_row(row_, nop_pos_index, *result_row))) {
      STORAGE_LOG(WARN, "failed to compact second row", K(ret), KPC(result_row), K(row_));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_trans_row_into_row_queue()
{
  int ret = OB_SUCCESS;
  if (not_first_trans_flag_) {
    if (OB_FAIL(compact_trans_row_to_one())) {  // not first trans
      STORAGE_LOG(WARN, "failed to compact trans row", K(ret));
    } else if (OB_FAIL(compact_first_row())) {
      STORAGE_LOG(WARN, "failed to call compact to first row", K(ret), K(row_));
    }
  } else if (OB_FAIL(compact_first_row())) {
    // All rows of this rowkey need to be compacted into first row
    STORAGE_LOG(WARN, "failed to call compact to first row", K(ret), K(row_));
  } else {
    STORAGE_LOG(DEBUG,
        "success to compact trans row",
        K(ret),
        KPC(row_queue_.get_first()),
        KPC(row_queue_.get_last()),
        K(row_));
  }
  if (OB_SUCC(ret) && row_.row_type_flag_.is_last_multi_version_row()) {
    row_queue_.get_last()->row_type_flag_.set_last_multi_version_row(true);
    row_queue_.get_last()->row_type_flag_.set_compacted_multi_version_row(true);
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_first_row()
{
  int ret = OB_SUCCESS;
  if (row_queue_.is_empty() && OB_FAIL(row_queue_.add_empty_row(row_allocator_))) {
    // push an empty row for compact first row
    STORAGE_LOG(WARN, "failed to add row", K(ret));
  } else {
    ObStoreRow* first_row = row_queue_.get_first();
    if (!first_row->row_type_flag_.is_compacted_multi_version_row() &&
        OB_FAIL(compact_row(row_, RNPI_FIRST_ROW, *first_row))) {
      STORAGE_LOG(WARN, "failed to compact first row", K(ret), KPC(first_row), K(row_));
    } else {
      STORAGE_LOG(DEBUG, "success to compact first row", K(ret), K(row_), KPC(first_row));
    }
    if (OB_SUCC(ret) && (row_.row_type_flag_.is_first_multi_version_row() || is_first_row_filtered_)) {
      first_row->row_type_flag_.set_first_multi_version_row(true);
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_last_row()
{
  int ret = OB_SUCCESS;
  if (row_queue_.count() > 1) {  // When there is only one row, compact_first_row has been fuse
    ObStoreRow* last_row = row_queue_.get_last();
    if (OB_FAIL(compact_row(row_, RNPI_LAST_ROW, *last_row))) {
      STORAGE_LOG(WARN, "failed to compact last row", K(ret), KPC(last_row), K(row_));
    } else {
      STORAGE_LOG(DEBUG, "success to compact last row", K(ret), KPC(last_row), K(row_));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::find_uncommitted_row()
{
  int ret = OB_SUCCESS;
  last_trans_id_.reset();
  if (OB_UNLIKELY(OB_ISNULL(reader_) || SCAN_START != scan_state_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "reader is null OR scan state is wrong", K(ret), K(reader_), K(scan_state_));
  } else {
    if (OB_FAIL(end_of_block())) {  // do nothing
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        STORAGE_LOG(DEBUG, "find uncommitted row failed", K(ret));
      }
    } else {
      ObMultiVersionRowFlag flag;
      int64_t trans_version = 0;
      int64_t sql_sequence = 0;
      if (OB_FAIL(reader_->get_multi_version_info(current_,
              trans_version_col_idx_,
              sql_sequence_col_store_idx_,
              flag,
              last_trans_id_,  // record the trans_id
              trans_version,
              sql_sequence))) {
        STORAGE_LOG(WARN,
            "fail to get multi version info",
            K(ret),
            K(current_),
            K(trans_version_col_idx_),
            K(sql_sequence_col_store_idx_),
            K_(macro_id));
      } else if (flag.is_uncommitted_row()) {  // uncommitted
        // get trans status & committed_trans_version_
        transaction::ObTransTableStatusType status;
        int64_t commit_trans_version = INT64_MAX;
        if (OB_FAIL(get_trans_status(last_trans_id_, status, commit_trans_version))) {
          STORAGE_LOG(WARN, "get transaction status failed", K(ret), K(last_trans_id_), K(status));
        } else if (OB_FAIL(judge_trans_status(status, commit_trans_version))) {
          STORAGE_LOG(WARN, "failed to judge transaction status", K(ret), K(last_trans_id_), K(status));
        }
      } else {  // no uncommitted rows
        scan_state_ = PREPARE_COMMITTED_ROW_QUEUE;
      }
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::get_trans_status(
    const transaction::ObTransID& trans_id, transaction::ObTransTableStatusType& status, int64_t& commit_trans_version)
{
  int ret = OB_SUCCESS;
  // get trans status & committed_trans_version_
  commit_trans_version = INT64_MAX;
  if (OB_FAIL(context_->store_ctx_->trans_table_guard_->get_trans_state_table().get_transaction_status_with_log_ts(
          trans_id, context_->merge_log_ts_, status, commit_trans_version))) {
    STORAGE_LOG(WARN, "get transaction status failed", K(ret), K(trans_id), K(status));
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::judge_trans_status(
    const transaction::ObTransTableStatusType status, const int64_t commit_trans_version)
{
  int ret = OB_SUCCESS;
  const common::ObVersionRange& version_range = context_->trans_version_range_;

  switch (status) {
    case transaction::ObTransTableStatusType::COMMIT: {
      if (context_->query_flag_.is_sstable_cut() && commit_trans_version > version_range.snapshot_version_) {
        scan_state_ = FILTER_ABORT_TRANS_ROW;
      } else {
        scan_state_ = COMPACT_COMMIT_TRANS_ROW;
        committed_trans_version_ = commit_trans_version;
      }
      break;
    }
    case transaction::ObTransTableStatusType::RUNNING: {
      scan_state_ = GET_RUNNING_TRANS_ROW;
      committed_trans_version_ = commit_trans_version;
      break;
    }
    case transaction::ObTransTableStatusType::ABORT: {
      scan_state_ = FILTER_ABORT_TRANS_ROW;
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "wrong transaction status", K(ret), K(commit_trans_version));
  }
  return ret;
}

bool ObMultiVersionMicroBlockMinorMergeRowScanner::check_meet_another_trans(const transaction::ObTransID& trans_id)
{
  bool bret = false;
  if (last_trans_id_ != trans_id) {  // another uncommitted trans
    not_first_trans_flag_ = true;
    scan_state_ = SCAN_START;
    bret = true;
    STORAGE_LOG(DEBUG, "meet different trans ", K(trans_id), K(last_trans_id_), K(scan_state_));
  }
  return bret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::filter_abort_trans_row(const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  bool can_read = false;
  while (OB_SUCC(ret) && FILTER_ABORT_TRANS_ROW == scan_state_ && !is_last_multi_version_row_) {
    if (OB_FAIL(end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "failed to reach end of block", K(ret));
      }
    } else if (OB_FAIL(read_uncommitted_row(can_read, row))) {
      STORAGE_LOG(WARN, "failed to read uncommitted row", K(ret));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_commit_trans_row(const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  bool can_read = false;
  while (OB_SUCC(ret) && COMPACT_COMMIT_TRANS_ROW == scan_state_ && !is_last_multi_version_row_) {
    if (OB_FAIL(end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        STORAGE_LOG(WARN, "failed to reach end of block", K(ret));
      }
    } else if (OB_FAIL(read_uncommitted_row(can_read, row))) {
      STORAGE_LOG(WARN, "failed to read uncommitted row", K(ret));
    } else if (COMPACT_COMMIT_TRANS_ROW == scan_state_ && OB_ISNULL(row) && can_read) {
      if (OB_FAIL(compact_trans_row_into_row_queue())) {
        STORAGE_LOG(WARN, "Failed to compact trans row", K(ret));
      } else {
        is_row_queue_ready_ = true;
      }
    }
  }  // end of while

  if ((OB_SUCC(ret) || (OB_ITER_END == ret && is_right_border_)) && COMPACT_COMMIT_TRANS_ROW == scan_state_ &&
      row_queue_.has_next()) {
    if (!is_last_multi_version_row_) {
      // transaction crosses macro block, first row of this transaction can't have Compacted flag
      // make all rows in same transaction compacted into one in ObMinorMergeMacroRowIterator
      row_queue_.get_first()->row_type_flag_.set_compacted_multi_version_row(false);
    }
    scan_state_ = GET_ROW_FROM_ROW_QUEUE;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::get_running_trans_row(const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  bool can_read = false;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "failed to check end of block", K(ret));
    }
  } else if (OB_FAIL(read_uncommitted_row(can_read, row))) {
    STORAGE_LOG(WARN, "failed to read uncommitted row", K(ret));
  } else if (GET_RUNNING_TRANS_ROW == scan_state_ && can_read && OB_ISNULL(row)) {
    row = &row_;
    STORAGE_LOG(DEBUG, "get uncommitted row.", K(ret), KPC(row));
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::set_trans_version_for_uncommitted_row(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  const int64_t row_trans_version = -row.row_val_.cells_[trans_version_col_idx_].get_int();
  if (INT64_MAX == committed_trans_version_ && INT64_MAX != row_trans_version &&
      FILTER_ABORT_TRANS_ROW != scan_state_) {
    // If the transaction is aborted, the current trans_version is MAX,
    // but the transaction may have been filled with prepare_version
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "failed to set trans_version for uncommitted row", K(ret), K(row_), K_(committed_trans_version));
  } else if (row_trans_version > committed_trans_version_) {
    row.row_val_.cells_[trans_version_col_idx_].set_int(-committed_trans_version_);
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::check_curr_row_can_read(
    const transaction::ObTransID& trans_id, const int64_t sql_seq, bool& can_read)
{
  int ret = OB_SUCCESS;
  can_read = false;
  if (OB_FAIL(context_->store_ctx_->trans_table_guard_->get_trans_state_table().check_sql_sequence_can_read(
          trans_id, sql_seq, can_read))) {
    STORAGE_LOG(WARN, "check sql sequence can read failed", K(ret), K(can_read), K(trans_id), K(sql_seq));
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::filter_unneeded_row(bool& add_row_queue_flag, bool& is_magic_row_flag)
{
  int ret = OB_SUCCESS;
  add_row_queue_flag = true;
  is_magic_row_flag = false;
  const common::ObVersionRange& version_range = context_->trans_version_range_;
  const int64_t trans_version = -row_.row_val_.cells_[trans_version_col_idx_].get_int();
  const bool is_first_row = row_.row_type_flag_.is_first_multi_version_row();
  if (trans_version > version_range.snapshot_version_) {
    add_row_queue_flag = false;
    if (is_first_row) {
      is_first_row_filtered_ = true;
    }
    STORAGE_LOG(DEBUG, "filter unneeded row", K(row_), K(version_range), K(context_->pkey_));

    if (OB_SUCC(ret) && row_.row_type_flag_.is_last_multi_version_row()) {  // meet last row
      if (row_queue_.is_empty()) {
        // if row has been filtered and has output row or first rowkey flag is true, need output magic row
        // else need clean scan status
        if (have_output_row_flag_ || first_rowkey_flag_) {
          if (OB_FAIL(ObMagicRowManager::make_magic_row(sql_sequence_col_idx_, context_->query_flag_, row_))) {
            STORAGE_LOG(WARN, "failed to make magic row", K(ret), K(row_));
          }
        } else {
          // no need output this row, make clear scan status condition
          is_magic_row_flag = true;
        }
      }
    }
  }
  return ret;
}
