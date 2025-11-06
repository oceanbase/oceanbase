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
#include "storage/access/ob_aggregate_base.h"
#include "storage/access/ob_block_batched_row_store.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/memtable/ob_memtable_block_reader.h"
#include "storage/compaction/ob_compaction_trans_cache.h"
#include "storage/truncate_info/ob_truncate_partition_filter.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace blocksstable
{
ObIMicroBlockRowScanner::ObIMicroBlockRowScanner(common::ObIAllocator &allocator)
    : is_inited_(false),
    reverse_scan_(false),
    is_left_border_(false),
    is_right_border_(false),
    can_ignore_multi_version_(false),
    can_blockscan_(false),
    is_filter_applied_(false),
    use_private_bitmap_(false),
    current_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    start_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    last_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    reserved_pos_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    step_(1),
    row_(),
    macro_id_(),
    read_info_(nullptr),
    range_(nullptr),
    sstable_(nullptr),
    reader_(nullptr),
    memtable_reader_(nullptr),
    param_(nullptr),
    context_(nullptr),
    allocator_(allocator),
    block_row_store_(nullptr),
    di_bitmap_(nullptr),
    filter_bitmap_(nullptr)
{}

ObIMicroBlockRowScanner::~ObIMicroBlockRowScanner()
{
  if (nullptr != memtable_reader_) {
    memtable_reader_->~ObMemtableBlockReader();
    allocator_.free(memtable_reader_);
  }
  if (nullptr != di_bitmap_) {
    di_bitmap_->~ObCGBitmap();
    allocator_.free(di_bitmap_);
  }
  if (nullptr != filter_bitmap_) {
    filter_bitmap_->~ObCGBitmap();
    allocator_.free(filter_bitmap_);
  }
  reader_helper_.reset();
}

void ObIMicroBlockRowScanner::reuse()
{
  range_ = nullptr;
  reverse_scan_ = false;
  is_left_border_ = false;
  is_right_border_ = false;
  can_ignore_multi_version_ = false;
  can_blockscan_ = false;
  is_filter_applied_ = false;
  use_private_bitmap_ = false;
  current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  start_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  last_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  reserved_pos_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
}

int ObIMicroBlockRowScanner::init(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() ||
                         !context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), K(context));
  } else if (FALSE_IT(read_info_ = param.get_read_info(context.use_fuse_row_cache_))) {
  } else if (OB_UNLIKELY(nullptr == read_info_ || !read_info_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null columns info", K(ret), K(param), K(context.use_fuse_row_cache_), KPC_(read_info));
  } else if (OB_FAIL(row_.init(allocator_, param.get_buffered_out_col_cnt()))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  }
  if (OB_SUCC(ret)) {
    param_ = &param;
    context_ = &context;
    sstable_ = sstable;
    range_ = nullptr;
    block_row_store_ = context.block_row_store_;
    if (NULL != reader_) {
      reader_->reset();
    }
    LOG_DEBUG("init ObIMicroBlockRowScanner", K(context), KPC_(read_info), K(param));
  }
  return ret;
}

int ObIMicroBlockRowScanner::set_range(const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("range is not valid", K(ret), K(range));
  } else {
    range_ = &range;
  }
  return ret;
}

int ObIMicroBlockRowScanner::switch_context(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sstable ||
                  !param.is_valid() ||
                  !context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sstable), K(param), K(context));
  } else if (FALSE_IT(read_info_ = param.get_read_info(context.use_fuse_row_cache_))) {
  } else if (OB_UNLIKELY(nullptr == read_info_ || !read_info_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null columns info", K(ret), K(param), K(context.use_fuse_row_cache_), KPC_(read_info));
  } else if (OB_FAIL(row_.reserve(read_info_->get_request_count()))) {
    LOG_WARN("Fail to reserve datum row", K(ret));
  } else {
    param_ = &param;
    context_ = &context;
    sstable_ = sstable;
    range_ = nullptr;
    block_row_store_ = context.block_row_store_;
  }

  return ret;
}

int ObIMicroBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &block_data,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP_(range), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !block_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_id), K(block_data));
  } else if (OB_FAIL(set_reader(block_data.get_store_type()))) {
    const ObMicroBlockHeader *micro_header = reinterpret_cast<const ObMicroBlockHeader *>(block_data.buf_);
    LOG_WARN("failed to set reader", K(ret), K(block_data), KPC(micro_header));
  } else if (OB_FAIL(reader_->init(block_data, *read_info_))) {
    LOG_WARN("failed to init micro block reader", K(ret), KPC(read_info_));
  } else {
    reset_blockscan();
    use_private_bitmap_ = false;
    reverse_scan_ = context_->query_flag_.is_reverse_scan();
    is_left_border_ = is_left_border;
    is_right_border_ = is_right_border;
    macro_id_ = macro_id;
  }
  return ret;
}

int ObIMicroBlockRowScanner::open_column_block(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &block_data,
    const ObCSRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP_(range), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !block_data.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_id), K(block_data), K(range));
  } else if (OB_FAIL(set_reader(block_data.get_store_type()))) {
    const ObMicroBlockHeader *micro_header = reinterpret_cast<const ObMicroBlockHeader *>(block_data.buf_);
    LOG_WARN("failed to set reader", K(ret), K(block_data), KPC(micro_header));
  } else if (OB_FAIL(reader_->init(block_data, *read_info_))) {
    LOG_WARN("failed to init micro block reader", K(ret), KPC(read_info_));
  } else {
    reset_blockscan();
    reverse_scan_ = context_->query_flag_.is_reverse_scan();
    macro_id_ = macro_id;

    int64_t begin;
    int64_t end;
    if (0 == reader_->row_count()) {
      begin = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
      end = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    } else {
      begin = range.start_row_id_;
      end = MIN(range.end_row_id_, reader_->row_count() - 1);
    }
    if (reverse_scan_) {
      current_ = end;
      start_ = end;
      last_ = begin;
      step_ = -1;
    } else {
      current_ = begin;
      start_ = begin;
      last_ = end;
      step_ = 1;
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::get_next_row(const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(row_.reserve(read_info_->get_request_count()))) {
    LOG_WARN("Fail to reserve datum row", K(ret), K_(row));
  } else if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader is NULL", K(ret));
  } else if (OB_FAIL(inner_get_next_row(store_row))) {
  }
  return ret;
}

int ObIMicroBlockRowScanner::get_next_rows()
{
  int ret = OB_SUCCESS;
  ObFilterResult res;
  if (OB_UNLIKELY(nullptr == block_row_store_ || !block_row_store_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid pushdown scanner", K(ret), KPC_(block_row_store));
  } else if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to judge end of block or not", K(ret));
    }
  } else if (OB_UNLIKELY(!is_filter_applied_ || nullptr == reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpect micro scanner", K(ret), KPC(this));
  } else {
    int64_t prev_current = current_;
    ObBlockBatchedRowStore *batch_store = reinterpret_cast<ObBlockBatchedRowStore *>(block_row_store_);
    if (OB_FAIL(get_filter_result(res))) {
      LOG_WARN("Failed to get pushdown filter result bitmap", K(ret));
    } else if (OB_FAIL(batch_store->fill_rows(
                range_->get_group_idx(),
                *this,
                current_,
                last_ + step_,
                res))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to apply rows", K(ret), K(current_), K(last_), K(step_),
                 K(res), KPC(sstable_));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t scan_cnt = std::abs(current_ - prev_current);
      if (OB_NOT_NULL(context_) && 0 < scan_cnt) {
        if (sstable_->is_minor_sstable()) {
          context_->table_store_stat_.minor_sstable_read_row_cnt_ += scan_cnt;
        } else if (sstable_->is_major_sstable()) {
          context_->table_store_stat_.major_sstable_read_row_cnt_ += scan_cnt;
        }
      }
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to judge end of block or not", K(ret));
    }
  } else if (can_blockscan_) {
    if (OB_FAIL(inner_get_next_row_blockscan(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("[PUSHDOWN] failed to get next row pushdown", K(ret), K_(macro_id));
      }
    }
  } else {
    if (OB_FAIL(reader_->get_row(current_, row_))) {
      LOG_WARN("micro block reader fail to get row.", K(ret), K_(macro_id));
    } else {
      row = &row_;
      current_ += step_;
    }
  }
  LOG_DEBUG("get next row", K(ret), KPC(row), K_(macro_id));
  return ret;
}

int ObIMicroBlockRowScanner::inner_get_next_row_blockscan(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = NULL;
  ObFilterResult res;
  if (OB_FAIL(get_filter_result(res))) {
    LOG_WARN("Failed to get pushdown filter result bitmap", K(ret));
  } else {
    bool readed = false;
    while (OB_SUCC(ret) && !readed) {
      if (OB_FAIL(end_of_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to judge end of block or not", K(ret));
        }
      } else if (res.test(current_)) {
        if (OB_FAIL(reader_->get_row(current_, row_))) {
          LOG_WARN("micro block reader fail to get row.", K(ret), K_(macro_id));
        } else {
          readed = true;
          row_.fast_filter_skipped_ = is_filter_applied_;
          row = &row_;
        }
      }
      current_ += step_;
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::apply_filter(const bool can_blockscan)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == block_row_store_ || !block_row_store_->is_valid() || nullptr == reader_ ||
                  (!param_->is_delete_insert_ && !can_blockscan))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected block row store", K(ret), K(can_blockscan), KPC_(param), KPC_(block_row_store), KP_(reader));
  } else if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to judge end of block or not", K(ret), K_(macro_id), K_(start), K_(last), K_(current));
    } else {
      // can_blockscan_ = true;
      // is_filter_applied_ = block_row_store_->filter_pushdown() && (param_->is_delete_insert_ || can_ignore_multi_version_);
    }
  } else if (!block_row_store_->filter_pushdown() || (!param_->is_delete_insert_ && !can_ignore_multi_version_)) {
    can_blockscan_ = true;
    is_filter_applied_ = false;
  } else if (OB_FAIL(block_row_store_->reorder_filter())) {
    LOG_WARN("Fail to reorder filter", K(ret));
  } else {
    ACTIVE_SESSION_FLAG_SETTER_GUARD(in_filter_rows);
    if (param_->has_lob_column_out()) {
      context_->reuse_lob_locator_helper();
    }
    if (OB_FAIL(filter_micro_block_in_blockscan(block_row_store_->get_pd_filter_info()))) {
      LOG_WARN("Failed to apply pushdown filter in block reader", K(ret), KPC_(block_row_store));
    } else {
      can_blockscan_ = can_blockscan;
      is_filter_applied_ = true;
      if (param_->has_lob_column_out()) {
        context_->reuse_lob_locator_helper();
      }
      ++context_->table_store_stat_.pushdown_micro_access_cnt_;
      context_->table_store_stat_.blockscan_row_cnt_ += get_access_cnt();
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("query interrupt", K(ret));
  }
  LOG_DEBUG("micro scanner apply filter", K(ret), K(can_blockscan), K(get_access_cnt()), K_(param_->is_delete_insert),
            K_(can_ignore_multi_version), K_(can_blockscan), K_(is_filter_applied), KPC_(block_row_store));
  return ret;
}

int ObIMicroBlockRowScanner::set_reader(const ObRowStoreType store_type)
{
  int ret = OB_SUCCESS;

  if (ObStoreFormat::is_row_store_type_with_encoding(store_type)) {
    if (OB_UNLIKELY(nullptr != sstable_ && sstable_->is_multi_version_minor_sstable())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported multi version encode store type", K(ret), K(store_type));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!reader_helper_.is_inited() && OB_FAIL(reader_helper_.init(allocator_))) {
    LOG_WARN("Fail to init reader helper", KR(ret), K(store_type));
  } else if (OB_FAIL(reader_helper_.get_reader(store_type, reader_))) {
    LOG_WARN("Fail to get reader", KR(ret), K(store_type));
  }

  return ret;
}

int ObIMicroBlockRowScanner::set_base_scan_param(
    const bool is_left_bound_block,
    const bool is_right_bound_block)
{
  int ret = OB_SUCCESS;
  int64_t start;
  int64_t last;

  if (OB_FAIL(locate_range_pos(is_left_bound_block, is_right_bound_block, start, last))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("failed to locate start pos.", K(ret));
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
    current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    start_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    last_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    ret = OB_SUCCESS;
  }
  LOG_DEBUG("set_base_scan_param", K(current_), K(start_), K(last_));

  return ret;
}

int ObIMicroBlockRowScanner::end_of_block() const
{
  int ret = common::OB_SUCCESS;
  if (ObIMicroBlockReaderInfo::INVALID_ROW_INDEX == current_) {
    ret = common::OB_ITER_END;
  } else {
    if (!reverse_scan_) {
      if (current_ > last_) {
        ret = common::OB_ITER_END; // forward scan to border
      }
    } else { // reverse_scan_
      if (current_ < last_ || current_ > start_) {
        ret = common::OB_ITER_END;  // reverse scan to border
      }
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::fuse_row(
    const ObDatumRow &former,
    ObDatumRow &result,
    storage::ObNopPos &nop_pos,
    bool &final_result,
    common::ObIAllocator *allocator)
{
  int ret = common::OB_SUCCESS;
  if (is_row_empty(result)) {
    result.snapshot_version_ = former.snapshot_version_;
  }
  if (OB_FAIL(storage::ObRowFuse::fuse_row(former,
                                           result,
                                           nop_pos,
                                           final_result,
                                           allocator))) {
    LOG_WARN("failed to fuse row", K(ret), K(former), K(result));
  } else if (0 == result.count_ && result.row_flag_.is_delete()) { // copy rowkey, for minor merge
    result.count_ = former.count_;
    for (int64_t j = 0; OB_SUCC(ret) && j < result.count_; ++j) {
      if (nullptr == allocator) { // shallow copy
        result.storage_datums_[j] = former.storage_datums_[j];
      } else if(OB_FAIL(result.storage_datums_[j].deep_copy(former.storage_datums_[j], *allocator))) {
        LOG_WARN("Failed to deep copy datum", K(ret));
      }
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::locate_range_pos(
    const bool is_left_bound_block,
    const bool is_right_bound_block,
    int64_t &begin,
    int64_t &end)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(range_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("range_ is null", K(ret), KP(range_));
  } else if (OB_FAIL(reader_->locate_range(*range_, is_left_bound_block, is_right_bound_block, begin, end))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      LOG_WARN("fail to locate range", K(ret), KPC_(range), K(begin), K(end));
    }
  }
  return ret;
}
int ObIMicroBlockRowScanner::apply_filter_batch(
    sql::ObPushdownFilterExecutor *parent,
    sql::ObPhysicalFilterExecutor &filter,
    sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t cur_row_index = pd_filter_info.start_;
  int64_t end_row_index = pd_filter_info.start_ + pd_filter_info.count_;
  int32_t *row_ids = pd_filter_info.row_ids_;
  int64_t row_cap = 0;
  int64_t bitmap_offset = 0;
  const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets(pd_filter_info.is_pd_to_cg_);
  ObSEArray<ObSqlDatumInfo, 16> datum_infos;
  bool filter_applied_directly = false;
  if ((ObIMicroBlockReader::Decoder == reader_->get_type() ||
      ObIMicroBlockReader::CSDecoder == reader_->get_type()) &&
      filter.can_pushdown_decoder()) {
    if (!filter.is_filter_black_node()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected filter node type", K(ret), K(filter));
    } else {
      ObIMicroBlockDecoder *decoder = static_cast<ObIMicroBlockDecoder *>(reader_);
      sql::ObBlackFilterExecutor *black_filter = static_cast<sql::ObBlackFilterExecutor *>(&filter);
      if (decoder->can_apply_black(col_offsets) &&
          OB_FAIL(decoder->filter_black_filter_batch(
                  parent,
                  *black_filter,
                  pd_filter_info,
                  result_bitmap,
                  filter_applied_directly))) {
        LOG_WARN("Failed to execute black pushdown filter in decoder", K(ret));
      }
    }
  }

  if (OB_FAIL(ret) || filter_applied_directly) {
  } else if (OB_FAIL(filter.get_datums_from_column(datum_infos))) {
    LOG_WARN("Failed to get filter column datum_infos", K(ret));
  } else {
    while (OB_SUCC(ret) && cur_row_index < end_row_index) {
      row_cap = MIN(pd_filter_info.batch_size_, end_row_index - cur_row_index);
      for (int64_t i = 0; i < row_cap; i++) {
        row_ids[i] = cur_row_index + i;
      }
      if (0 == filter.get_col_count()) {
      } else if (param_->use_new_format()) {
        if (OB_FAIL(get_rows_for_rich_format(col_offsets,
                                             filter.get_col_params(),
                                             row_ids,
                                             row_cap,
                                             0,
                                             pd_filter_info.cell_data_ptrs_,
                                             pd_filter_info.len_array_,
                                             *const_cast<sql::ObExprPtrIArray*>(filter.get_cg_col_exprs()),
                                             &filter.get_default_datums(),
                                             filter.is_padding_mode()))) {
          LOG_WARN("Failed to get rows for rich format", K(ret), K(cur_row_index));
        }
      } else if (OB_FAIL(get_rows_for_old_format(col_offsets,
                                                 filter.get_col_params(),
                                                 row_ids,
                                                 row_cap,
                                                 0,
                                                 pd_filter_info.cell_data_ptrs_,
                                                 *const_cast<sql::ObExprPtrIArray*>(filter.get_cg_col_exprs()),
                                                 datum_infos,
                                                 &filter.get_default_datums(),
                                                 filter.is_padding_mode()))) {
        LOG_WARN("Failed to get rows for old format", K(ret), K(cur_row_index));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(filter.filter_batch(parent, bitmap_offset, bitmap_offset + row_cap, result_bitmap))) {
        LOG_WARN("failed to filter batch", K(ret), K(cur_row_index), K(row_cap));
      } else {
        cur_row_index += row_cap;
        bitmap_offset += row_cap;
      }
    }
  }
  LOG_TRACE("[PUSHDOWN] apply filter batch", K(ret), K(reader_->get_type()), K(filter_applied_directly),
            K(pd_filter_info.start_), K(pd_filter_info.count_),
            K(result_bitmap.popcnt()), K(result_bitmap), K(filter));
  return ret;
}

int ObIMicroBlockRowScanner::get_rows_for_old_format(
    const common::ObIArray<int32_t> &col_offsets,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
    const int32_t *row_ids,
    const int64_t row_cap,
    const int64_t vector_offset,
    const char **cell_datas,
    sql::ObExprPtrIArray &exprs,
    common::ObIArray<ObSqlDatumInfo> &datum_infos,
    const common::ObIArray<blocksstable::ObStorageDatum> *default_datums,
    const bool is_padding_mode)
{
  int ret = OB_SUCCESS;
  sql::ObEvalCtx &eval_ctx = param_->op_->get_eval_ctx();

  switch (reader_->get_type())
  {
    case ObIMicroBlockReader::Reader:
    case ObIMicroBlockReader::NewFlatReader: {
      if (OB_FAIL(reader_->get_rows(col_offsets,
                                    col_params,
                                    default_datums,
                                    is_padding_mode,
                                    row_ids,
                                    row_cap,
                                    row_,
                                    datum_infos,
                                    vector_offset,
                                    exprs,
                                    eval_ctx))) {
        LOG_WARN("Failed to copy rows",
                 K(ret),
                 K(row_cap),
                 "row_ids",
                 common::ObArrayWrap<const int32_t>(row_ids, row_cap));
      }
      break;
    }

    case ObIMicroBlockReader::Decoder:
    case ObIMicroBlockReader::CSDecoder: {
      ObIMicroBlockDecoder *decoder = static_cast<ObIMicroBlockDecoder *>(reader_);
      if (OB_FAIL(decoder->get_rows(col_offsets,
                                    col_params,
                                    is_padding_mode,
                                    row_ids,
                                    cell_datas,
                                    row_cap,
                                    datum_infos,
                                    vector_offset))) {
        LOG_WARN("Failed to get rows", K(ret), K(row_cap), K(vector_offset));
      }
      break;
    };

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected micro block reader", K(ret), K(reader_->get_type()));
      break;
    }
  }

  if (OB_SUCC(ret) && param_->has_lob_column_out() && reader_->has_lob_out_row()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_offsets.count(); i++) {
      if (nullptr != col_params.at(i) && col_params.at(i)->get_meta_type().is_lob_storage()) {
        if (OB_FAIL(fill_datums_lob_locator(*param_,
                                            *context_,
                                            *col_params.at(i),
                                            row_cap,
                                            datum_infos.at(i).datum_ptr_ + vector_offset,
                                            false))) {
          LOG_WARN("Fail to fill lob locator", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::get_rows_for_rich_format(
    const common::ObIArray<int32_t> &col_offsets,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
    const int32_t *row_ids,
    const int64_t row_cap,
    const int64_t vector_offset,
    const char **cell_datas,
    uint32_t *len_array,
    sql::ObExprPtrIArray &exprs,
    const common::ObIArray<blocksstable::ObStorageDatum> *default_datums,
    const bool is_padding_mode,
    const bool need_init_vector)
{
  int ret = OB_SUCCESS;
  sql::ObEvalCtx &eval_ctx = param_->op_->get_eval_ctx();

  switch (reader_->get_type())
  {
    case ObIMicroBlockReader::Reader:
    case ObIMicroBlockReader::NewFlatReader: {
      if (OB_FAIL(reader_->get_rows(col_offsets,
                                    col_params,
                                    default_datums,
                                    is_padding_mode,
                                    row_ids,
                                    vector_offset,
                                    row_cap,
                                    row_,
                                    exprs,
                                    eval_ctx,
                                    need_init_vector))) {
        LOG_WARN("Failed to copy rows", K(ret), K(row_cap),
                 "row_ids", common::ObArrayWrap<const int32_t>(row_ids, row_cap));
      }
      break;
    }

    case ObIMicroBlockReader::Decoder:
    case ObIMicroBlockReader::CSDecoder: {
      if (OB_FAIL(reader_->get_rows(col_offsets,
                                    col_params,
                                    default_datums,
                                    is_padding_mode,
                                    row_ids,
                                    row_cap,
                                    cell_datas,
                                    vector_offset,
                                    len_array,
                                    eval_ctx,
                                    exprs,
                                    need_init_vector))) {
        LOG_WARN("Failed to get rows", K(ret), K(row_cap));
      }
      break;
    };

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected micro block reader", K(ret), K(reader_->get_type()));
      break;
    }
  }

  if (OB_SUCC(ret) && param_->has_lob_column_out() && reader_->has_lob_out_row()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_offsets.count(); i++) {
      if (nullptr != col_params.at(i) && col_params.at(i)->get_meta_type().is_lob_storage() &&
          OB_FAIL(fill_exprs_lob_locator(*param_,
                                         *context_,
                                         *col_params.at(i),
                                         *(exprs.at(i)),
                                         eval_ctx,
                                         vector_offset,
                                         row_cap))) {
        LOG_WARN("Fail to fill lob locator", K(ret));
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_FILTER_PUSHDOWN_DISABLE_FLAT_VECTORIZE);
int ObIMicroBlockRowScanner::filter_pushdown_filter(
    sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor *filter,
    sql::PushdownFilterInfo &pd_filter_info,
    const bool can_use_vectorize,
    common::ObBitmap &bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == reader_ || nullptr == filter || !filter->is_filter_node())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(reader_), KPC(filter));
  } else if (filter->is_truncate_node()) {
    if (OB_FAIL(reader_->filter_pushdown_truncate_filter(parent, *filter, pd_filter_info, bitmap))) {
      LOG_WARN("Failed to pushdown truncate scn filter", K(ret));
    }
  } else if (filter->is_sample_node()) {
    if (ObIMicroBlockReader::MemtableReader == reader_->get_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("memtable does not support sample pushdown", KR(ret), KP(this), KP(reader_));
    } else if (OB_FAIL(static_cast<ObSampleFilterExecutor *>(filter)->apply_sample_filter(
                pd_filter_info,
                sstable_->is_major_sstable(),
                bitmap))) {
      LOG_WARN("Failed to execute sample pushdown filter", K(ret));
    }
  // use uniform base currently, support new format later
  // TODO(hanling): If the new vectorization format does not start counting from the 0th row, it can be computed in batches.
  } else if (can_use_vectorize &&
             filter->get_op().enable_rich_format_ &&
             OB_FAIL(init_exprs_uniform_header(filter->get_cg_col_exprs(),
                                               filter->get_op().get_eval_ctx(),
                                               filter->get_op().get_eval_ctx().max_batch_size_))) {
    LOG_WARN("Failed to init exprs vector header", K(ret));
  } else {
    switch (reader_->get_type())
    {
      case ObIMicroBlockReader::Decoder:
      case ObIMicroBlockReader::CSDecoder: {
        ObIMicroBlockDecoder *decoder = static_cast<ObIMicroBlockDecoder *>(reader_);
        // change to black filter in below situation
        // 1. if need project lob column and micro block has outrow lob
        // 2. if it is semistruct_filter_node, but not cs decoder (beacuase semistrcut white filter only support cs decoder)
        if ((param_->has_lob_column_out() && reader_->has_lob_out_row())
            || (filter->is_semistruct_filter_node() && ObIMicroBlockReader::CSDecoder != reader_->get_type())) {
          sql::ObPhysicalFilterExecutor *physical_filter = static_cast<sql::ObPhysicalFilterExecutor *>(filter);
          if (OB_FAIL(decoder->filter_pushdown_filter(parent,
                                                      *physical_filter,
                                                      pd_filter_info,
                                                      bitmap))) {
            LOG_WARN("Failed to execute pushdown filter", K(ret));
          }
        } else if (filter->is_filter_black_node()) {
          sql::ObBlackFilterExecutor *black_filter = static_cast<sql::ObBlackFilterExecutor *>(filter);
          if (can_use_vectorize && black_filter->can_vectorized()) {
            if (OB_FAIL(apply_filter_batch(
                        parent,
                        *black_filter,
                        pd_filter_info,
                        bitmap))) {
              LOG_WARN("Failed to execute black pushdown filter in batch", K(ret));
            }
          } else if (OB_FAIL(decoder->filter_pushdown_filter(
                      parent,
                      *black_filter,
                      pd_filter_info,
                      bitmap))) {
            LOG_WARN("Failed to execute black pushdown filter", K(ret));
          }
        } else {
          if (OB_FAIL(decoder->filter_pushdown_filter(
                      parent,
                      *static_cast<sql::ObWhiteFilterExecutor *>(filter),
                      pd_filter_info,
                      bitmap))) {
            LOG_WARN("Failed to execute white pushdown filter", K(ret));
          }
        }
        break;
      }

      case ObIMicroBlockReader::MemtableReader: {
        if (OB_FAIL(
                memtable_reader_->filter_pushdown_filter(parent, *filter, pd_filter_info, bitmap))) {
          LOG_WARN("failed to execute memtable pushdown filter");
        }
        break;
      }

      case ObIMicroBlockReader::Reader:
      case ObIMicroBlockReader::NewFlatReader: {
        if (can_use_vectorize && filter->can_vectorized() && EN_FILTER_PUSHDOWN_DISABLE_FLAT_VECTORIZE == OB_SUCCESS && !filter->is_filter_dynamic_node()) {
          sql::ObPhysicalFilterExecutor *physical_filter = static_cast<sql::ObPhysicalFilterExecutor *>(filter);
          if (OB_FAIL(apply_filter_batch(
                      parent,
                      *physical_filter,
                      pd_filter_info,
                      bitmap))) {
            LOG_WARN("Failed to execute pushdown filter in batch", K(ret));
          }
        } else if (OB_FAIL(reader_->filter_pushdown_filter(parent, *filter, pd_filter_info, bitmap))) {
          LOG_WARN("Failed to execute pushdown filter", K(ret));
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unknown type of reader", KR(ret), K(reader_->get_type()));
        break;
      }
    }
  }

  return ret;
}

////////////////////////////////// ObMicroBlockRowDirectScanner ////////////////////////////////////////////
int ObMicroBlockRowDirectScanner::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::init(param, context, sstable))) {
    LOG_WARN("base init failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObIMicroBlockRowScanner::filter_micro_block_in_blockscan(sql::PushdownFilterInfo &pd_filter_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == reader_ || nullptr == block_row_store_ || !pd_filter_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP_(reader), KP_(block_row_store), K(pd_filter_info));
  } else if (nullptr == pd_filter_info.filter_) {
    // nothing to do
  } else {
    pd_filter_info.start_ = reverse_scan_ ? last_ : current_;
    pd_filter_info.count_ = get_access_cnt();
    pd_filter_info.is_pd_to_cg_ = false;
    pd_filter_info.param_ = param_;
    pd_filter_info.context_ = context_;
    pd_filter_info.disable_bypass_ = block_row_store_->disable_bypass();
    if (use_private_bitmap_) {
      if (OB_UNLIKELY(pd_filter_info.start_ != filter_bitmap_->get_start_id()
                      || (is_di_bitmap_valid() && pd_filter_info.start_ != di_bitmap_->get_start_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected start row id", K(ret), K(pd_filter_info.start_), KPC_(filter_bitmap), KPC_(di_bitmap));
      } else if (pd_filter_info.filter_->is_filter_always_true()) {
      } else if (pd_filter_info.filter_->is_filter_always_false()) {
        // TODO: opt always false when open micro block row scanner
        filter_bitmap_->reuse(pd_filter_info.start_, false);
      } else {
        if (is_di_bitmap_valid() && !di_bitmap_->is_all_true()) {
          pd_filter_info.set_delete_insert_bitmap(di_bitmap_->get_inner_bitmap());
        }
        if (OB_FAIL(pd_filter_info.filter_->execute(nullptr, pd_filter_info, this, param_->vectorized_enabled_ && block_row_store_->is_empty()))) {
          LOG_WARN("Fail to filter", K(ret), KPC(pd_filter_info.filter_));
        } else if (OB_FAIL(filter_bitmap_->bit_and(*pd_filter_info.filter_->get_result()))) {
          LOG_WARN("Fail to combine delete insert bitmap", K(ret), KPC_(filter_bitmap));
        }
        pd_filter_info.reset_delete_insert_bitmap();
      }
    } else {
      if (pd_filter_info.filter_->is_filter_constant()) {
        common::ObBitmap *result = nullptr;
        if (OB_FAIL(pd_filter_info.filter_->init_bitmap(pd_filter_info.count_, result))) {
          LOG_WARN("Failed to init bitmap in filter", K(ret), K_(pd_filter_info.count), KP(result));
        } else {
          result->reuse(pd_filter_info.filter_->is_filter_always_true());
        }
      } else if (OB_FAIL(pd_filter_info.filter_->execute(nullptr, pd_filter_info, this, param_->vectorized_enabled_ && block_row_store_->is_empty()))) {
        LOG_WARN("Fail to filter", K(ret), KPC(pd_filter_info.filter_));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t bitmap_cnt = use_private_bitmap_ ? filter_bitmap_->size() : pd_filter_info.filter_->get_result()->size();
      const int64_t select_cnt = use_private_bitmap_ ? filter_bitmap_->popcnt() : pd_filter_info.filter_->get_result()->popcnt();
      context_->table_store_stat_.storage_filtered_row_cnt_ += (bitmap_cnt - select_cnt);
    }
  }
  return ret;
}

int ObMicroBlockRowDirectScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &block_data,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::open(macro_id, block_data, is_left_border, is_right_border))) {
    LOG_WARN("base open failed", K(ret));
  } else if (OB_FAIL(set_base_scan_param(is_left_border, is_right_border))) {
    LOG_WARN("failed to set base scan param", K(ret), K_(macro_id), K(is_left_border), K(is_right_border));
  }
  return ret;
}

int ObIMicroBlockRowScanner::filter_micro_block_in_cg(
    sql::ObPushdownFilterExecutor *parent,
    sql::PushdownFilterInfo &pd_filter_info,
    const ObCGBitmap *parent_bitmap,
    const ObCSRowId micro_start_id,
    int64_t &access_count)
{
  int ret = OB_SUCCESS;
  access_count = 0;
  if (OB_UNLIKELY(nullptr == reader_ || !pd_filter_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP_(reader), K(pd_filter_info));
  } else if (OB_FAIL(end_of_block())) {
    LOG_WARN("end of micro scanner", K(ret), K_(current), K_(start), K_(last));
  } else {
    int64_t begin = reverse_scan_ ? last_ : current_;
    int64_t row_count = get_access_cnt();
    pd_filter_info.start_ = begin;
    pd_filter_info.count_ = row_count;
    pd_filter_info.is_pd_to_cg_ = true;
    pd_filter_info.param_ = param_;
    pd_filter_info.context_ = context_;
    if (nullptr != parent) {
      common::ObBitmap *parent_result = nullptr;
      if (OB_FAIL(parent->init_bitmap(row_count, parent_result))) {
        LOG_WARN("Failed to get parent bitmap", K(ret), K(row_count));
      } else if (OB_FAIL(parent_bitmap->set_bitmap(begin + micro_start_id, row_count, false, *parent_result))) {
        LOG_WARN("Fail go get bitmap", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pd_filter_info.filter_->execute(parent, pd_filter_info, this, param_->vectorized_enabled_))) {
      LOG_WARN("Fail to filter", K(ret), KPC(pd_filter_info.filter_), K(pd_filter_info.start_), K(pd_filter_info.count_));
    } else {
      access_count = row_count;
      current_ = reverse_scan_ ? current_ - row_count : current_ + row_count;
      is_filter_applied_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("[COLUMNSTORE] filter micro block in cg", "begin", pd_filter_info.start_, "count", pd_filter_info.count_,
              "bitmap_size", pd_filter_info.filter_->get_result()->size(), "popcnt", pd_filter_info.filter_->get_result()->popcnt());

  }
  return ret;
}

// ATTENTION only called in cg scanner
// TODO remove useless code
int ObIMicroBlockRowScanner::get_next_rows(
    const common::ObIArray<int32_t> &cols_projector,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObIArray<ObSqlDatumInfo> &datum_infos,
    const int64_t datum_offset,
    uint32_t *len_array,
    const bool is_padding_mode,
    const bool need_init_vector,
    const ObIArray<ObStorageDatum>* default_datums)
{
  int ret = OB_SUCCESS;
  sql::ObExprPtrIArray &exprs = *(const_cast<sql::ObExprPtrIArray *>(param_->output_exprs_));
  if (OB_UNLIKELY(nullptr == row_ids || nullptr == cell_datas || 0 == row_cap)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(row_ids), KP(cell_datas),
             K(row_cap), K(cols_projector.count()), K(datum_infos.count()));
  } else if (param_->use_new_format()) {
    if (OB_FAIL(get_rows_for_rich_format(cols_projector,
                                         col_params,
                                         row_ids,
                                         row_cap,
                                         datum_offset,
                                         cell_datas,
                                         len_array,
                                         exprs,
                                         default_datums,
                                         is_padding_mode,
                                         need_init_vector))) {
      LOG_WARN("Failed to get rows for rich format", K(ret));
    }
  // cg scanner use major sstable only, no need default row
  } else if (OB_FAIL(get_rows_for_old_format(cols_projector,
                                             col_params,
                                             row_ids,
                                             row_cap,
                                             datum_offset,
                                             cell_datas,
                                             exprs,
                                             datum_infos,
                                             default_datums,
                                             is_padding_mode))) {
    LOG_WARN("Failed to get rows for old format", K(ret));
  }
  return ret;
}

// start_offset is inclusive, end_offset is exclusive.
int ObIMicroBlockRowScanner::advance_to_border(
    const ObDatumRowkey &rowkey,
    int64_t &start_offset,
    int64_t &end_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument rowkey", K(ret), K(rowkey));
  } else if (OB_ITER_END != end_of_block()) {
    int64_t row_idx = 0;
    bool equal = false;
    int64_t start = !reverse_scan_ ? start_ : last_;
    int64_t last = !reverse_scan_ ? (last_ + 1) : (start_ + 1);
    if (OB_FAIL(reader_->find_bound(rowkey, !reverse_scan_, start, last, row_idx, equal))) {
      LOG_WARN("Failed to find bound", K(ret), K(rowkey));
    } else if (!reverse_scan_) {
      start_offset = current_;
      current_ = row_idx;
      end_offset = row_idx;
    } else {
      end_offset = current_ + 1;
      current_ = row_idx - 1;
      start_offset = row_idx;
    }
  } else if (!reverse_scan_) {
    start_offset = last_ + 1;
    end_offset = last_ + 1;
  } else {
    start_offset = last_ - 1;
    end_offset = last_ - 1;
  }
  LOG_DEBUG("ObIMicroBlockRowScanner::advance_to_border",
             K(ret), K(current_), K(start_), K(last_), K(start_offset), K(end_offset));
  return ret;
}

int ObIMicroBlockRowScanner::check_can_group_by(
    const int32_t group_by_col,
    int64_t &row_cnt,
    int64_t &read_cnt,
    int64_t &distinct_cnt,
    bool &can_group_by) const
{
  int ret = OB_SUCCESS;
  row_cnt = reader_->row_count();
  read_cnt = last_ - current_ + 1;
  if (OB_FAIL(reader_->get_distinct_count(group_by_col, distinct_cnt))) {
    if (OB_UNLIKELY(OB_NOT_SUPPORTED != ret)) {
      LOG_WARN("Failed to get distinct cnt", K(ret));
    } else {
      can_group_by = false;
      ret = OB_SUCCESS;
    }
  } else {
    can_group_by = true;
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(can_group_by), K(group_by_col), K(distinct_cnt),
      K(row_cnt), K(read_cnt), K(last_), K(current_), K(reader_->get_type()));
  return ret;
}

int ObIMicroBlockRowScanner::read_distinct(
    const int32_t group_by_col,
    const char **cell_datas,
    const bool is_padding_mode,
    storage::ObGroupByCellBase &group_by_cell) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(blocksstable::ObIMicroBlockReader::Decoder != reader_->get_type() &&
      blocksstable::ObIMicroBlockReader::CSDecoder != reader_->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected reader type", K(ret), K(reader_->get_type()));
  } else if (OB_FAIL((static_cast<blocksstable::ObIMicroBlockDecoder*>(reader_))->read_distinct(
      group_by_col, cell_datas, is_padding_mode, group_by_cell))) {
    LOG_WARN("Failed to read distinct", K(ret));
  }
  return ret;
}

int ObIMicroBlockRowScanner::read_reference(
    const int32_t group_by_col,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObGroupByCellBase &group_by_cell) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(blocksstable::ObIMicroBlockReader::Decoder != reader_->get_type() &&
      blocksstable::ObIMicroBlockReader::CSDecoder != reader_->get_type())) {
    LOG_WARN("Unexpected reader type", K(ret), K(reader_->get_type()));
  } else if (OB_FAIL((static_cast<blocksstable::ObIMicroBlockDecoder*>(reader_))->read_reference(
      group_by_col, row_ids, row_cap, group_by_cell))) {
    LOG_WARN("Failed to read reference", K(ret));
  }
  return ret;
}

int ObIMicroBlockRowScanner::get_next_border_rows(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to judge end of block or not", K(ret));
    }
  } else if (OB_UNLIKELY(nullptr == block_row_store_ ||
                  !block_row_store_->is_empty() ||
                  !is_filter_applied_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid block row store status", K(ret), K_(is_filter_applied), KPC_(block_row_store));
  } else {
    ObFilterResult res;
    bool is_equal = false;
    bool contained_border_key = false;
    int64_t prev_current = current_;
    int64_t start_idx = reverse_scan_ ? last_ : current_;
    int64_t end_idx = reverse_scan_ ? (current_ + 1) : (last_ + 1); // exclusive border key locate end idx
    int64_t border_row_idx = -1;
    int64_t scan_end_idx = -1; // exclusive scan end idx
    ObBlockBatchedRowStore *batch_store = reinterpret_cast<ObBlockBatchedRowStore *>(block_row_store_);
    if (OB_FAIL(reader_->locate_border_row_id(rowkey, start_idx, end_idx, border_row_idx, is_equal))) {
      LOG_WARN("Fail to locate border row id", K(ret));
    } else if (OB_UNLIKELY(border_row_idx < start_idx || border_row_idx > end_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected border row idx", K(ret), K(start_idx), K(end_idx), K(border_row_idx), K(rowkey));
    } else if (reverse_scan_) {
      scan_end_idx = is_equal ? border_row_idx : border_row_idx - 1;
      if (is_equal || border_row_idx > last_) {
        contained_border_key = true;
      }
    } else {
      scan_end_idx = border_row_idx;
      if (border_row_idx <= last_) {
        contained_border_key = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (current_ == scan_end_idx) {
    } else if (OB_FAIL(get_filter_result(res))) {
      LOG_WARN("Failed to get pushdown filter result bitmap", K(ret));
    } else if (OB_FAIL(batch_store->fill_rows(range_->get_group_idx(),
                                              *this,
                                              current_,
                                              scan_end_idx,
                                              res))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to fill rows", K(ret), K_(current), K_(last), K_(step), K(res));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t scan_cnt = std::abs(current_ - prev_current);
      if (OB_NOT_NULL(context_) && 0 < scan_cnt) {
        context_->table_store_stat_.major_sstable_read_row_cnt_ += scan_cnt;
      }
      if (current_ == scan_end_idx) {
        if (is_equal) {
          current_ += step_;
          ret = OB_PUSHDOWN_STATUS_CHANGED;
        } else if (contained_border_key) {
          ret = OB_PUSHDOWN_STATUS_CHANGED;
        }
      }
    }
    FLOG_INFO("Get next border rows", K(ret), K(rowkey), K_(reverse_scan), K_(current), K_(last),
        K(is_equal), K(scan_end_idx), KPC(batch_store));
  }
  return ret;
}

int ObIMicroBlockRowScanner::check_and_revert_non_border_rowkey(
    const ObDatumRowkey &border_rowkey,
    const ObDatumRow &deleted_row,
    ObCSRowId &co_current)
{
  int ret = OB_SUCCESS;
  const int64_t schema_rowkey_cnt = read_info_->get_schema_rowkey_count();
  if (OB_UNLIKELY(deleted_row.get_column_count() < schema_rowkey_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column count of border rowkey or deleted row", K(ret), K(schema_rowkey_cnt), K(deleted_row));
  } else {
    int cmp_result = 0;
    ObDatumRowkey deleted_rowkey;
    deleted_rowkey.assign(deleted_row.storage_datums_, schema_rowkey_cnt);
    if (OB_FAIL(deleted_rowkey.compare(border_rowkey, read_info_->get_datum_utils(), cmp_result, false/*compare_datum_cnt*/))) {
      LOG_WARN("fail to compare deleted rowkey with border rowkey", K(ret), K(deleted_rowkey), K(border_rowkey));
    } else if (0 != cmp_result) {
      // the rowkey of deleted row is not equal to border rowkey, need revert the index of micro scanner
      current_ -= step_;
      LOG_TRACE("[COLUMNSTORE] get non border rowkey and revert current index", K(ret), K_(current), K(co_current));
    } else {
      // inner_get_next_row_with_row_id return the index of border rowkey, will increase here
      co_current += step_;
      LOG_TRACE("[COLUMNSTORE] skip border rowkey and increase co current index", K(ret), K_(current), K(co_current));
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::get_filter_result(ObFilterResult &res)
{
  int ret = OB_SUCCESS;
  sql::PushdownFilterInfo &pd_filter_info = block_row_store_->get_pd_filter_info();
  res.bitmap_ = nullptr;
  res.filter_start_ = pd_filter_info.start_;
  if (use_private_bitmap_) {
    if (OB_ISNULL(filter_bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected filter bitmap", K(ret));
    } else {
      res.filter_start_ = filter_bitmap_->get_start_id();
      if (!filter_bitmap_->is_all_true()) {
        res.bitmap_ = filter_bitmap_->get_inner_bitmap();
      }
    }
  } else if (!is_filter_applied_ || nullptr == pd_filter_info.filter_) {
  } else if (OB_ISNULL(res.bitmap_ = pd_filter_info.filter_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null filter bitmap", K(ret));
  } else if (res.bitmap_->is_all_true()) {
    res.bitmap_ = nullptr;
  }
  return ret;
}

bool ObIMicroBlockRowScanner::is_di_bitmap_valid() const
{
  return param_->is_delete_insert_ &&
         !can_ignore_multi_version_ &&
         reader_ != memtable_reader_ &&
         nullptr != di_bitmap_;
}

int ObIMicroBlockRowScanner::init_bitmap(ObCGBitmap *&bitmap, bool is_all_true)
{
  int ret = OB_SUCCESS;
  int64_t row_count = get_access_cnt();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_NOT_NULL(bitmap)) {
    if (OB_FAIL(bitmap->switch_context(row_count, false/*is_reverse*/))) {
      LOG_WARN("fail to expand size of bitmap", K(ret), K(row_count));
    } else {
      bitmap->reuse(start_, is_all_true);
    }
  } else {
    if (OB_ISNULL(bitmap = OB_NEWx(ObCGBitmap, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate bitmap failed", KR(ret));
    } else if (OB_FAIL(bitmap->init(row_count, false/*is_reverse*/))) {
      LOG_WARN("fail to init bitmap", K(ret), K(row_count));
    } else {
      bitmap->reuse(start_, is_all_true);
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::get_next_skip_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to judge end of block or not", K(ret));
    }
  } else if (OB_FAIL(reader_->get_row(current_, row_))) {
    LOG_WARN("micro block reader fail to get row.", K(ret), K_(macro_id));
  } else {
    row = &row_;
  }
  return ret;
}

int ObIMicroBlockRowScanner::skip_to_range(
    const int64_t begin,
    const int64_t end,
    const ObDatumRange &range,
    const bool is_left_border,
    const bool is_right_border,
    int64_t &skip_row_idx,
    bool &has_data,
    bool &range_finished)
{
  int ret = OB_SUCCESS;
  bool equal = false;
  int64_t begin_row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  int64_t end_row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  const int64_t last = end + 1;
  has_data = true;
  range_finished = false;
  if (OB_UNLIKELY(begin < 0 || begin > end || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range", K(ret), K(begin), K(end), K(range));
  } else if (range.get_start_key().is_min_rowkey()) {
    begin_row_idx = begin;
  } else if (OB_FAIL(reader_->find_bound(range.get_start_key(), true, begin, last, begin_row_idx, equal))) {
    LOG_WARN("fail to find bound", K(ret), K(range));
  } else if (begin_row_idx == last) {
    has_data = false;
  } else if (!range.get_border_flag().inclusive_start()) {
    if (equal) {
      ++begin_row_idx;
      if (begin_row_idx == last) {
        has_data = false;
      }
    }
  }
  LOG_DEBUG("[INDEX SKIP SCAN] micro skip to range, locate start key", K(ret), K(begin), K(end), K(range),
            K(has_data), K(equal), K(begin_row_idx));
  if (OB_SUCC(ret) && has_data) {
    if (range.get_end_key().is_max_rowkey()) {
      end_row_idx = end;
    } else if (OB_FAIL(reader_->find_bound(range.get_end_key(), !range.get_border_flag().inclusive_end(), begin_row_idx, last, end_row_idx, equal))) {
      LOG_WARN("fail to find bound", K(ret), K(range));
    } else if (end_row_idx == last) {
      --end_row_idx;
    } else if (end_row_idx == begin_row_idx) {
      has_data = false;
    } else {
      --end_row_idx;
    }
  }
  if (OB_SUCC(ret)) {
    skip_row_idx = reverse_scan_ ? end_row_idx : begin_row_idx;
    range_finished = reverse_scan_ ? begin_row_idx > begin : (begin_row_idx < last && end_row_idx < end);
    is_left_border_ = begin_row_idx > 0 || (0 == begin_row_idx && is_left_border);
    is_right_border_ = end_row_idx < reader_->row_count() || (end_row_idx == reader_->row_count() && is_right_border);
    if (!has_data) {
      current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
      start_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
      last_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
      reserved_pos_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    } else if (reverse_scan_) {
      current_ = start_ = end_row_idx;
      last_ = begin_row_idx;
      reserved_pos_ = current_;
    } else {
      current_ = start_ = begin_row_idx;
      last_ = end_row_idx;
    }
  }
  LOG_DEBUG("[INDEX SKIP SCAN] micro skip to range, locate end key", K(ret), K(begin), K(end), K(range),
             K(has_data), K(equal), K(begin_row_idx), K(end_row_idx), K_(current), K_(start), K_(last),
             K_(reserved_pos), K(range_finished), K(skip_row_idx));
  return ret;
}

int ObIMicroBlockRowScanner::compare_rowkey(const ObDatumRowkey &rowkey, const bool is_cmp_end, int32_t &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey), K(is_cmp_end), KPC(this));
  } else if (OB_FAIL(reader_->compare_rowkey(rowkey, is_cmp_end ? reader_->row_count() - 1 : 0, cmp_ret))) {
    LOG_WARN("failed to compare rowkey", K(ret), K(rowkey), K(is_cmp_end), KPC(this));
  } else {
    LOG_DEBUG("compare rowkey", K(ret), K(rowkey), K(is_cmp_end), K(cmp_ret), KPC(this));
  }
  return ret;
}

////////////////////////////////// ObMicroBlockRowScanner ////////////////////////////////////////////
int ObMicroBlockRowScanner::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::init(param, context, sstable))) {
    LOG_WARN("base init failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMicroBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &block_data,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  can_ignore_multi_version_ = true;
  if (OB_FAIL(ObIMicroBlockRowScanner::open(macro_id, block_data, is_left_border, is_right_border))) {
    LOG_WARN("base open failed", K(ret));
  } else if (OB_FAIL(set_base_scan_param(is_left_border, is_right_border))) {
    LOG_WARN("failed to set base scan param", K(ret), K_(macro_id), K(is_left_border), K(is_right_border));
  } else if (param_->is_delete_insert_ && nullptr != param_->pushdown_filter_) {
    if (OB_FAIL(init_bitmap(filter_bitmap_, true/*is_all_true*/))) {
      LOG_WARN("fail to init filter bitmap", K(ret));
    } else {
      use_private_bitmap_ = true;
    }
  }
  return ret;
}

int ObMicroBlockRowScanner::estimate_row_count(
    const ObITableReadInfo &read_info,
    const ObMicroBlockData &block_data,
    const ObDatumRange &range,
    bool consider_multi_version,
    ObPartitionEst &est)
{
  int ret = OB_SUCCESS;
  range_ = &range;
  int64_t max_col_count = read_info.get_request_count();
  if (OB_UNLIKELY(!range_->is_valid() || !block_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC_(range), K(block_data));
  } else if (OB_FAIL(set_reader(block_data.get_store_type()))) {
    LOG_WARN("failed to set reader", K(ret), K(block_data));
  } else if (OB_FAIL(reader_->init(block_data, &read_info.get_datum_utils()))) {
    LOG_WARN("failed to init micro block reader", K(ret), K(block_data), K(read_info));
  } else {
    is_inited_ = true;
    if (OB_FAIL(set_base_scan_param(true, true))) {
      LOG_WARN("failed to set base scan param", K(ret), K(range));
    }
  }

  if (OB_SUCC(ret)) {
    if (ObIMicroBlockReaderInfo::INVALID_ROW_INDEX != last_
        && ObIMicroBlockReaderInfo::INVALID_ROW_INDEX != start_) {
      est.physical_row_count_ = last_ - start_ + 1;
      if (est.physical_row_count_ < 0) {
        est.physical_row_count_ = 0;
      }
      est.logical_row_count_ = est.physical_row_count_;

      ObIMicroBlockFlatReaderBase *reader = static_cast<ObIMicroBlockFlatReaderBase *>(reader_);
      if (est.physical_row_count_ > 0 && consider_multi_version
          && !reader->committed_single_version_rows()) {
        est.logical_row_count_ = 0;
        if (OB_FAIL(reader->get_logical_row_cnt(last_, current_, est.logical_row_count_))) {
          LOG_WARN("failed to get logical row count", K(ret));
        }
      }
    }
  }
  return ret;
}

///////////////////////////// ObMultiVersionMicroBlockRowScannerV2 ///////////////////////////////////
void ObMultiVersionMicroBlockRowScanner::reuse()
{
  ObIMicroBlockRowScanner::reuse();
  nop_pos_.reset();
  cell_allocator_.reuse();
  finish_scanning_cur_rowkey_ = true;
  is_last_multi_version_row_ = true;
  read_row_direct_flag_ = false;
}

void ObMultiVersionMicroBlockRowScanner::inner_reset()
{
  reuse();
  prev_micro_row_.reset();
  row_.reset();
  tmp_row_.reset();
  trans_version_col_idx_ = -1;
  sql_sequence_col_idx_ = -1;
  cell_cnt_ = 0;
  context_ = nullptr;
  read_info_ = nullptr;
  if (OB_NOT_NULL(reader_)) {
    reader_->reset();
  }
  step_ = 1;
  is_inited_ = false;
}

int ObMultiVersionMicroBlockRowScanner::switch_context(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::switch_context(param, context, sstable))) {
    STORAGE_LOG(WARN, "Failed to switch context", K(ret));
  } else if (OB_FAIL(prev_micro_row_.reserve(read_info_->get_request_count()))) {
    LOG_WARN("Fail to reserve datum row", K(ret));
  } else if (OB_FAIL(tmp_row_.reserve(read_info_->get_request_count()))) {
    LOG_WARN("Fail to reserve datum row", K(ret));
  } else if (OB_FAIL(nop_pos_.init(allocator_, read_info_->get_request_count()))) {
    LOG_WARN("failed to init nop_pos", K(ret), K(cell_cnt_));
  } else {
    cell_cnt_ = read_info_->get_request_count();
    trans_version_col_idx_ = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
        read_info_->get_schema_rowkey_count(), true);
    sql_sequence_col_idx_ = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
        read_info_->get_schema_rowkey_count(), true);
    version_range_ = context.trans_version_range_;
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  reuse();
  if (OB_FAIL(ObIMicroBlockRowScanner::init(param, context, sstable))) {
    LOG_WARN("base init failed", K(ret));
  } else if (!is_inited_) {
    cell_cnt_ = read_info_->get_request_count();
    const int64_t max_cell_cnt = param.get_buffered_request_cnt(read_info_);
    if (OB_FAIL(prev_micro_row_.init(allocator_, max_cell_cnt))) {
      STORAGE_LOG(WARN, "Failed to init cur_micro_row", K(ret), K_(cell_cnt));
    } else if (OB_FAIL(nop_pos_.init(allocator_, max_cell_cnt))) {
      LOG_WARN("failed to init nop_pos", K(ret), K(cell_cnt_));
    } else if (OB_FAIL(tmp_row_.init(allocator_, max_cell_cnt))) {
      LOG_WARN("fail to init tmp datum row", K(ret), K_(cell_cnt));
    } else {
      trans_version_col_idx_ = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
          read_info_->get_schema_rowkey_count(), true);
      sql_sequence_col_idx_ = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
          read_info_->get_schema_rowkey_count(), true);
    }
    if (OB_SUCC(ret)) {
      version_range_ = context.trans_version_range_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &block_data,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::open(macro_id, block_data, is_left_border, is_right_border))) {
    LOG_WARN("base open failed", K(ret));
  } else if (OB_FAIL(set_base_scan_param(is_left_border, is_right_border))) {
    LOG_WARN("failed to set base scan param", K(ret), K(is_left_border), K(is_right_border));
  } else {
    int64_t column_number = block_data.get_micro_header()->column_count_;
    int64_t max_col_count = MAX(read_info_->get_request_count(), column_number);
    if (OB_FAIL(tmp_row_.reserve(max_col_count))) {
      LOG_WARN("fail to reserve memory for tmp datumrow", K(ret), K(max_col_count));
    }
  }

  if (OB_SUCC(ret)) {
    if (reverse_scan_) {
      reserved_pos_ = current_;
    }
    read_row_direct_flag_ = false;
    can_ignore_multi_version_ = false;
    if (OB_UNLIKELY(!block_data.get_micro_header()->has_min_merged_trans_version_)) {
      LOG_INFO("micro block header not has_min_merged_trans_version_", K(ret), K(block_data));
    } else if (OB_NOT_NULL(sstable_)
        && !block_data.get_micro_header()->contain_uncommitted_rows()
        && block_data.get_micro_header()->max_merged_trans_version_ <= context_->trans_version_range_.snapshot_version_
        && block_data.get_micro_header()->min_merged_trans_version_ > context_->trans_version_range_.base_version_) {
      read_row_direct_flag_ = true;
      can_ignore_multi_version_
          = static_cast<ObIMicroBlockFlatReaderBase *>(reader_)->single_version_rows()
            && is_last_multi_version_row_
            && !reverse_scan_
            && !(0 == version_range_.base_version_ && IF_NEED_CHECK_BASE_VERSION_FILTER(context_));
      LOG_DEBUG("use direct read", K(ret), K(can_ignore_multi_version_),
                KPC(block_data.get_micro_header()), K(context_->trans_version_range_));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (can_ignore_multi_version_) { // TODO(yuanzhe) refactor blockscan opt of multi version sstable
    if (OB_FAIL(ObIMicroBlockRowScanner::inner_get_next_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to inner get next row", K(ret), K_(start), K_(last), K_(current));
      }
    }
  } else {
    reuse_cur_micro_row();
    if (OB_FAIL(inner_get_next_row_impl(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to inner get next row", K(ret), K_(start), K_(last), K_(current));
      }
    } else if (OB_NOT_NULL(row)) {
      const_cast<ObDatumRow *>(row)->fast_filter_skipped_ = is_filter_applied_;
    }
  }
  return ret;
}

inline void ObMultiVersionMicroBlockRowScanner::reuse_cur_micro_row()
{
  row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  nop_pos_.reset();
}

void ObMultiVersionMicroBlockRowScanner::reuse_prev_micro_row()
{
  for (int64_t i = 0; i < cell_cnt_; ++i) {
    prev_micro_row_.storage_datums_[i].set_nop();
  }
  prev_micro_row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  cell_allocator_.reuse();
  reserved_pos_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
}

int ObMultiVersionMicroBlockRowScanner::inner_get_next_row_impl(const ObDatumRow *&ret_row)
{
  int ret = OB_SUCCESS;
  // TRUE:For the multi-version row of the current rowkey, when there is no row to be read in this micro_block
  bool final_result = false;
  // TRUE:For reverse scanning, if this micro_block has the last row of the previous rowkey
  bool found_first_row = false;
  bool have_uncommited_row = false;
  const ObDatumRow *multi_version_row = NULL;
  ret_row = NULL;

  while (OB_SUCC(ret)) {
    final_result = false;
    found_first_row = false;
    if (OB_FAIL(locate_cursor_to_read(found_first_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to locate cursor to read", K(ret), K_(macro_id));
      }
    }
    LOG_DEBUG("locate cursor to read", K(ret), K(finish_scanning_cur_rowkey_),
              K(found_first_row), K(current_), K(reserved_pos_), K(last_), K_(macro_id));

    while (OB_SUCC(ret)) {
      multi_version_row = NULL;
      bool version_fit = false;
      if (read_row_direct_flag_) {
        if (OB_FAIL(inner_get_next_row_directly(multi_version_row, version_fit, final_result))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to inner get next row directly", K(ret), K_(macro_id));
          }
        }
      } else if (OB_FAIL(inner_inner_get_next_row(multi_version_row, version_fit, final_result, have_uncommited_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to inner get next row", K(ret), K_(macro_id));
        }
      }

      if (OB_SUCC(ret)) {
        if (!version_fit) {
          // do nothing
        } else if (OB_FAIL(do_compact(multi_version_row, row_, final_result))) {
          LOG_WARN("failed to do compact", K(ret));
        } else {
          if (have_uncommited_row) {
            row_.set_have_uncommited_row();
          }
        }
      }
      ObCStringHelper helper;
      LOG_DEBUG("do compact", K(ret), K(current_), K(version_fit), K(final_result), K(finish_scanning_cur_rowkey_),
                "cur_row", is_row_empty(row_) ? "empty" : helper.convert(row_),
                "multi_version_row", helper.convert(multi_version_row), K_(macro_id));

      if ((OB_SUCC(ret) && final_result) || OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(cache_cur_micro_row(found_first_row, final_result))) {
          LOG_WARN("failed to cache cur micro row", K(ret), K_(macro_id));
        }
        helper.reset();
        LOG_DEBUG("cache cur micro row", K(ret), K(finish_scanning_cur_rowkey_),
                  "cur_row", is_row_empty(row_) ? "empty" : helper.convert(row_),
                  "prev_row", is_row_empty(prev_micro_row_) ? "empty" : helper.convert(prev_micro_row_),
                  K_(macro_id));
        break;
      }
    }

    if (OB_SUCC(ret) && finish_scanning_cur_rowkey_) {
      if (!is_row_empty(prev_micro_row_)) {
        ret_row = &prev_micro_row_;
      } else if (!is_row_empty(row_)) {
        ret_row = &row_;
      }
      // If row is NULL, means no multi_version row of current rowkey in [base_version, snapshot_version) range
      if (NULL != ret_row) {
        (const_cast<ObDatumRow *>(ret_row))->mvcc_row_flag_.set_uncommitted_row(false);
        const_cast<ObDatumRow *>(ret_row)->trans_id_.reset();
        break;
      }
    }
  }
  if (OB_NOT_NULL(ret_row)) {
    if (!ret_row->is_valid()) {
      LOG_ERROR("row is invalid", KPC(ret_row));
    } else {
      LOG_DEBUG("row is valid", KPC(ret_row));
    }
  } else if (OB_UNLIKELY(OB_SUCCESS == ret || OB_ITER_END == ret)) {
    if (!reverse_scan_ && (last_ < reader_->row_count() - 1) &&
        !is_row_empty(prev_micro_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected locate range", K(ret), K_(start), K_(last), K_(reverse_scan), KPC_(range),
               K_(macro_id), K_(prev_micro_row_.row_flag), K_(prev_micro_row_.mvcc_row_flag),
               K_(prev_micro_row_.count), K_(prev_micro_row_.have_uncommited_row));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::locate_cursor_to_read(bool &found_first_row)
{
  int ret = OB_SUCCESS;
  bool need_locate_next_rowkey = false;
  found_first_row = false;

  LOG_DEBUG("locate_cursor_to_read", K(finish_scanning_cur_rowkey_),
            K(is_last_multi_version_row_), K(reserved_pos_),
            K(is_left_border_), K(is_right_border_));
  if (!reverse_scan_) {
    if (OB_FAIL(end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to judge end of block or not", K(ret), K_(macro_id));
      } else if (finish_scanning_cur_rowkey_) {
        reuse_prev_micro_row();
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
    if (ObIMicroBlockReaderInfo::INVALID_ROW_INDEX == reserved_pos_) {
      ret = OB_ITER_END;
    } else if (finish_scanning_cur_rowkey_) {
      current_ = reserved_pos_ - 1; // skip current L
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
    const ObRowHeader *row_header = nullptr;
    ObMultiVersionRowFlag row_flag;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(end_of_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to judge end of block or not", K(ret), K_(macro_id));
        } else {
          if (reverse_scan_) {
            // read from the first row
            finish_scanning_cur_rowkey_ = false;
            reserved_pos_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
            current_ = last_;
            found_first_row = is_left_border_;
            ret = OB_SUCCESS;
          } else {
            // return OB_ITER_END
          }
          break;
        }
      } else if (OB_FAIL(reader_->get_row_header(current_, row_header))) {
        LOG_WARN("failed to get row header", K(ret), K(current_), K_(macro_id));
      } else {
        row_flag.flag_ = row_header->get_mvcc_row_flag();
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
    const ObDatumRow *&ret_row,
    bool &version_fit,
    bool &final_result)
{
  int ret = OB_SUCCESS;
  ret_row = nullptr;
  version_fit = true;
  bool is_ghost_row_flag = false;
  ObDatumRow *row = nullptr;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to judge end of block or not", K(ret));
    }
  } else {
    if (is_row_empty(row_)) {
      row = &row_;
    } else {
      tmp_row_.count_ = tmp_row_.get_capacity();
      tmp_row_.trans_id_.reset();
      row = &tmp_row_;
    }
    if (OB_FAIL(reader_->get_row(current_, *row))) {
      LOG_WARN("micro block reader fail to get block_row", K(ret), K(current_), K_(macro_id));
    } else if (OB_UNLIKELY(row->mvcc_row_flag_.is_uncommitted_row())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("meet uncommitted row in macro_block with contain_uncommitted_row=false",
                K(ret), K(current_), K_(macro_id));
    } else if (OB_FAIL(ObGhostRowUtil::is_ghost_row(row->mvcc_row_flag_, is_ghost_row_flag))) {
      LOG_WARN("fail to check ghost row", K(ret), K_(current), KPC(row));
    } else if (OB_UNLIKELY(is_ghost_row_flag)) {
      version_fit = false;
      row->row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
      LOG_DEBUG("is ghost row", K(ret), K(current_), K(is_ghost_row_flag), K_(macro_id));
    } else if (OB_UNLIKELY(0 == version_range_.base_version_ &&
                           IF_NEED_CHECK_BASE_VERSION_FILTER(context_))) {
      if (OB_FAIL(context_->check_filtered_by_base_version(*row))) {
        LOG_DEBUG("check base version filter fail", K(ret));
      } else if (row->row_flag_.is_not_exist()) {
        version_fit = false;
      } else {
        row->snapshot_version_ = 0;
        ret_row = row;
      }
    } else {
      row->snapshot_version_ = 0;
      ret_row = row;
    }
  }
  if (OB_SUCC(ret)) {
    is_last_multi_version_row_ = row->mvcc_row_flag_.is_last_multi_version_row();
    final_result = is_last_multi_version_row_;
    // multi-version must be forward reading, and reverse positioning is processed in locate_cursor_to_read
    current_++;
    LOG_DEBUG("inner get next row", KPC(ret_row), K_(is_last_multi_version_row));
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::inner_inner_get_next_row(
    const ObDatumRow *&ret_row, bool &version_fit, bool &final_result, bool &have_uncommitted_row)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ret_row = nullptr;
  version_fit = false;
  final_result = false;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to judge end of block or not", K(ret), K_(macro_id));
    }
  } else {
    const ObRowHeader *row_header = nullptr;
    int64_t trans_version = 0;
    int64_t sql_sequence = 0;
    if (OB_FAIL(check_trans_version(final_result, version_fit, have_uncommitted_row, trans_version, sql_sequence, current_, row_header))) {
      LOG_WARN("fail to check trans version", K(ret), K_(current), K(version_fit), K(trans_version), K(sql_sequence));
    } else if (OB_FAIL(check_foreign_key(trans_version, sql_sequence, row_header))) {
      LOG_WARN("fail to check foreign key", K(ret), K_(current), K(trans_version), K(sql_sequence));
    } else {
      is_last_multi_version_row_ = row_header->get_row_multi_version_flag().is_last_multi_version_row();
    }
    if (OB_SUCC(ret)) {
      if (version_fit) {
        ObDatumRow *row = nullptr;
        if (is_row_empty(row_)) {
          row = &row_;
        } else {
          tmp_row_.count_ = tmp_row_.get_capacity();
          tmp_row_.trans_id_.reset();
          row = &tmp_row_;
        }
        if (OB_FAIL(reader_->get_row(current_, *row))) {
          LOG_WARN("micro block reader fail to get block_row", K(ret), K(current_));
        } else if (row->row_flag_.is_lock()) {
          if (OB_FAIL(ObLockRowChecker::check_lock_row_valid(*row, *read_info_))) {
            LOG_WARN("micro block reader fail to get block_row", K(ret), K(current_), KPC(row), KPC_(read_info));
          } else if (row->is_uncommitted_row()) {
            version_fit = false;
            row->row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
          }
        }
        if (OB_SUCC(ret) && version_fit) {
          if (OB_INVALID_INDEX != read_info_->get_trans_col_index()
              && transaction::is_effective_trans_version(trans_version)) {
            // only uncommitted row need to be set, committed row set in row reader
            int64_t trans_idx = read_info_->get_trans_col_index();
            if (OB_UNLIKELY(trans_idx >= row->count_ || 0 >= trans_version)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unexpected trans info", K(ret), K(trans_idx), K(trans_version), KPC(row), KPC_(read_info));
            } else {
              LOG_DEBUG("success to set trans_version on uncommitted row", K(ret), K(trans_version));
              row->storage_datums_[trans_idx].reuse();
              row->storage_datums_[trans_idx].set_int(-trans_version);
            }
            if (OB_UNLIKELY(0 == version_range_.base_version_ &&
                            IF_NEED_CHECK_BASE_VERSION_FILTER(context_))) {
              if (OB_FAIL(context_->check_filtered_by_base_version(*row))) {
                LOG_DEBUG("check base version filter fail", K(ret));
              } else if (row->row_flag_.is_not_exist()) {
                version_fit = false;
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (!row->mvcc_row_flag_.is_uncommitted_row()
                || transaction::is_effective_trans_version(trans_version)) {
              row->snapshot_version_ = 0;
              row->trans_id_.reset();
            } else { // uncommitted row
              row->snapshot_version_ = INT64_MAX;
            }
            ret_row = row;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      // Multi-version must be forward reading, and reverse positioning is processed in locate_cursor_to_read
      current_++;
      LOG_DEBUG("inner get next block_row", KPC(ret_row), K_(is_last_multi_version_row), K(trans_version), K_(macro_id));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::check_trans_version(
    bool &final_result,
    bool &version_fit,
    bool &have_uncommited_row,
    int64_t &trans_version,
    int64_t &sql_sequence,
    const int64_t index,
    const ObRowHeader *&row_header)
{
  int ret = OB_SUCCESS;
  ObMultiVersionRowFlag flag;
  bool is_ghost_row_flag = false;
  const int64_t snapshot_version = context_->trans_version_range_.snapshot_version_;
  memtable::ObMvccAccessCtx &acc_ctx = context_->store_ctx_->mvcc_acc_ctx_;

  if (OB_FAIL(reader_->get_multi_version_info(
              index,
              read_info_->get_schema_rowkey_count(),
              row_header,
              trans_version,
              sql_sequence))) {
    LOG_WARN("fail to get multi version info", K(ret), K(index), KPC_(read_info),
              K(sql_sequence_col_idx_), K_(macro_id));
  } else if (OB_ISNULL(row_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("row header is null", K(ret));
  } else if (FALSE_IT(flag = row_header->get_row_multi_version_flag())) {
  } else if (OB_FAIL(ObGhostRowUtil::is_ghost_row(flag, is_ghost_row_flag))) {
    LOG_WARN("fail to check ghost row", K(ret), K_(current), KPC(row_header),
              K(trans_version), K(sql_sequence), K_(macro_id));
  } else {
    final_result = flag.is_last_multi_version_row();

    if (OB_UNLIKELY(is_ghost_row_flag)) {
      // Case1: Data is ghost row, and it means no valid value for the row, so
      //        we can skip it
      version_fit = false;
      LOG_DEBUG("is ghost row", K(ret), K(index), K(flag));
    } else if (flag.is_uncommitted_row()) {
      have_uncommited_row = true;  // TODO @lvling check transaction status instead
      transaction::ObTxSEQ tx_sequence = transaction::ObTxSEQ::cast_from_int(sql_sequence);
      // Case2: Data is uncommitted, so we use the txn state cache or txn
      //        table to decide whether uncommitted txns are readable
      compaction::ObMergeCachedTransState trans_state;
      if (OB_NOT_NULL(context_->get_mds_collector())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("meet uncommitted row in mds query", KR(ret), K(flag), KPC(context_->get_mds_collector()));
      } else if (OB_NOT_NULL(context_->trans_state_mgr_) &&
                 OB_SUCCESS == context_->trans_state_mgr_->get_trans_state(
                     transaction::ObTransID(row_header->get_trans_id()), tx_sequence, trans_state)) {
        version_fit = trans_state.can_read_;
        trans_version = trans_state.trans_version_;

        if (transaction::is_effective_trans_version(trans_version)
            && trans_version <= version_range_.base_version_) {
          version_fit = false;
          // filter multi version row whose trans version is smaller than base_version
          final_result = true;
        }
      } else {
        transaction::ObLockForReadArg lock_for_read_arg(
          acc_ctx,
          transaction::ObTransID(row_header->get_trans_id()),
          tx_sequence,
          context_->query_flag_.read_latest_,
          context_->query_flag_.iter_uncommitted_row(),
          // TODO(handora.qc): remove it in the future
          sstable_->get_end_scn());

        if (OB_FAIL(lock_for_read(lock_for_read_arg,
                                  version_fit,
                                  trans_version))) {
          STORAGE_LOG(WARN, "fail to check transaction status",
                      K(ret), KPC(row_header), K_(macro_id));
        } else if (transaction::is_effective_trans_version(trans_version)
                    && trans_version <= version_range_.base_version_) {
          version_fit = false;
          // filter multi version row whose trans version is smaller than base_version
          final_result = true;
        }
      }
    } else {
      // Case3: Data is committed, so we use the version on the data to decide
      //        whether uncommitted txns are readable
      if (context_->query_flag_.iter_uncommitted_row()) {
        version_fit = true;
      } else if (trans_version <= version_range_.base_version_) {
        // filter multi version row whose trans version is smaller than base_version
        version_fit = false;
        final_result = true;
      } else if (trans_version > snapshot_version) {
        // filter multi version row whose trans version is larger than snapshot_version
        version_fit = false;
        if (OB_NOT_NULL(context_->get_mds_collector())) {
          context_->get_mds_collector()->exist_new_committed_node_ = true;
          LOG_TRACE("exist new committed node", KR(ret), K(trans_version), K(snapshot_version), KPC(context_->get_mds_collector()));
        }
      } else {
        version_fit = true;
      }
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::check_foreign_key(
  const int64_t trans_version,
  const int64_t sql_sequence,
  const ObRowHeader *row_header)
{
  int ret = OB_SUCCESS;
  ObStoreRowLockState lock_state;
  const int64_t snapshot_version = context_->trans_version_range_.snapshot_version_;
  memtable::ObMvccAccessCtx &acc_ctx = context_->store_ctx_->mvcc_acc_ctx_;
  bool is_plain_insert_gts_opt = context_->query_flag_.is_plain_insert_gts_opt();
  bool is_for_fk_check = context_->query_flag_.is_for_foreign_key_check();
  if ((is_plain_insert_gts_opt || is_for_fk_check) &&
      OB_FAIL(ObRowConflictHandler::check_foreign_key_constraint_for_sstable(
              acc_ctx.get_tx_table_guards(),
              acc_ctx.get_tx_id(),
              transaction::ObTransID(row_header->get_trans_id()),
              transaction::ObTxSEQ::cast_from_int(sql_sequence),
              trans_version,
              snapshot_version,
              sstable_->get_end_scn(),
              lock_state))) {
    if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
      int tmp_ret = OB_SUCCESS;
      ObStoreRowkey store_rowkey;
      ObDatumRowkeyHelper rowkey_helper;
      if (OB_TMP_FAIL(get_store_rowkey(store_rowkey, rowkey_helper))) {
        LOG_WARN("get store rowkey fail", K(tmp_ret));
      } else {
        ObRowConflictHandler::post_row_read_conflict(
                  acc_ctx,
                  store_rowkey,
                  lock_state,
                  context_->tablet_id_,
                  context_->ls_id_,
                  0, 0 /* these two params get from mvcc_row, and for statistics, so we ignore them */,
                  sstable_->get_end_scn());
      }
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::cache_cur_micro_row(const bool found_first_row, const bool final_result)
{
  int ret = OB_SUCCESS;

  if (!reverse_scan_) { // forward scan
    bool has_nop = true;
    if (final_result && is_row_empty(prev_micro_row_)) {
      // already get final result and no row cached, just return this row
      // do not need deep copy
    } else if (!is_row_empty(row_)) {
      if (is_row_empty(prev_micro_row_)) { // Save static meta information when first cached
        prev_micro_row_.row_flag_ = row_.row_flag_;
        prev_micro_row_.count_ = row_.count_;
        prev_micro_row_.have_uncommited_row_ = row_.have_uncommited_row_;
      }
      // The positive scan scans from new to old (trans_version is negative), so cur_row is older than prev_row
      // So just add the nop column (column for which the value has not been decided)
      has_nop = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_.count_; ++i) {
        ObStorageDatum &src_datum = row_.storage_datums_[i];
        ObStorageDatum &dest_datum = prev_micro_row_.storage_datums_[i];
        if (dest_datum.is_nop()) {
          if (OB_FAIL(dest_datum.deep_copy(src_datum, cell_allocator_))) {
            LOG_WARN("failed to deep copy datum", K(ret));
          } else if (!has_nop) {
            has_nop = dest_datum.is_nop();
          }
        }
      }
    }
    // Forward scan, as long as the fuse result of cur_row is final_result or the fuse result of prev_row has no nop,
    // it means that the current rowkey has been read
    if (final_result || !has_nop) {
      finish_scanning_cur_rowkey_ = true;
    }
  } else { // Reverse scan
    // apply, since cur_row is newer than prev_row
    if (is_row_empty(prev_micro_row_)) {
      if (found_first_row) {
        // do not need deep copy
      } else if (!is_row_empty(row_)) {
        prev_micro_row_.row_flag_ = row_.row_flag_;
        prev_micro_row_.count_ = row_.count_;
        prev_micro_row_.have_uncommited_row_ = row_.have_uncommited_row_;
        int64_t end_cell_pos = row_.count_;
        if (row_.row_flag_.is_delete()) {
          end_cell_pos = OB_INVALID_INDEX != read_info_->get_trans_col_index() ? read_info_->get_schema_rowkey_count() + 1 :
            read_info_->get_schema_rowkey_count();
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < end_cell_pos; ++i) {
          ObStorageDatum &src_datum = row_.storage_datums_[i];
          ObStorageDatum &dest_datum = prev_micro_row_.storage_datums_[i];
          if (OB_FAIL(dest_datum.deep_copy(src_datum, cell_allocator_))) {
            LOG_WARN("failed to deep copy datum", K(ret), K(src_datum));
          }
        }
      }
    } else if (!is_row_empty(row_)) {
      prev_micro_row_.row_flag_ = row_.row_flag_;
      prev_micro_row_.count_ = row_.count_;
      prev_micro_row_.have_uncommited_row_ = row_.have_uncommited_row_;
      if (row_.row_flag_.is_delete()) {
        for (int64_t i = read_info_->get_schema_rowkey_count(); i < prev_micro_row_.count_; ++i) {
          prev_micro_row_.storage_datums_[i].set_nop();
        }
        // in reverse scan if one rowkey covers multiple blocks, the scan order is: old version block -> new version block
        // so prev_micro_row_ is older and row_ is newer, need set the newer trans version
        const int64_t trans_idx = read_info_->get_trans_col_index();
        if (OB_SUCC(ret) && OB_INVALID_INDEX != trans_idx) {
          if (OB_UNLIKELY(trans_idx < 0 || trans_idx >= row_.count_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected trans info", K(ret), K(trans_idx), K_(row), KPC_(read_info));
          } else {
            const ObStorageDatum &src_datum = row_.storage_datums_[trans_idx];
            ObStorageDatum &dest_datum = prev_micro_row_.storage_datums_[trans_idx];
            dest_datum.reuse();
            dest_datum.set_int(src_datum.get_int());
          }
        }
      } else if (final_result) {
        for (int64_t i = 0; OB_SUCC(ret) && i < row_.count_; ++i) {
          ObStorageDatum &src_datum = row_.storage_datums_[i];
          ObStorageDatum &dest_datum = prev_micro_row_.storage_datums_[i];
          if (OB_FAIL(dest_datum.deep_copy(src_datum, cell_allocator_))) {
            LOG_WARN("failed to deep copy datum", K(ret), K(src_datum));
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < row_.count_; ++i) {
          ObStorageDatum &src_datum = row_.storage_datums_[i];
          ObStorageDatum &dest_datum = prev_micro_row_.storage_datums_[i];
          if (!src_datum.is_nop() && OB_FAIL(dest_datum.deep_copy(src_datum, cell_allocator_))) {
            LOG_WARN("failed to deep copy datum", K(ret), K(src_datum));
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

int ObMultiVersionMicroBlockRowScanner::do_compact(
    const ObDatumRow *src_row,
    ObDatumRow &dest_row,
    bool &final_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi version row is NULL", K(ret));
  } else if (&dest_row == src_row) {
    if (dest_row.mvcc_row_flag_.is_compacted_multi_version_row()) {
      final_result = true;
    } else {
      nop_pos_.reset();
      int64_t nop_count = 0;
      for (int64_t i = 0; i < dest_row.count_; ++i) {
        if (dest_row.storage_datums_[i].is_nop()) {
          nop_pos_.nops_[nop_count++] = static_cast<int16_t>(i);
        }
      }
      nop_pos_.count_ = nop_count;
      if (0 == nop_count) {
        final_result = true;
      }
    }
  } else if (OB_FAIL(fuse_row(*src_row, dest_row, nop_pos_, final_result))) {
    LOG_WARN("failed to fuse row", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (final_result || src_row->is_shadow_row() || src_row->is_last_multi_version_row()) {
      final_result = true;
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::lock_for_read(
    const transaction::ObLockForReadArg &lock_for_read_arg,
    bool &can_read,
    int64_t &trans_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SCN scn_trans_version = SCN::invalid_scn();
  auto &tx_table_guards = context_->store_ctx_->mvcc_acc_ctx_.get_tx_table_guards();
  int64_t cost_time = common::ObClockGenerator::getClock();

  if (OB_FAIL(tx_table_guards.lock_for_read(lock_for_read_arg,
                                            can_read,
                                            scn_trans_version))) {
    LOG_WARN("failed to check transaction status", K(ret), K(*context_->store_ctx_));
  } else {
    trans_version = scn_trans_version.get_val_for_tx();
    if (OB_NOT_NULL(context_->trans_state_mgr_) &&
      OB_TMP_FAIL(context_->trans_state_mgr_->add_trans_state(
        lock_for_read_arg.data_trans_id_, lock_for_read_arg.data_sql_sequence_,
        trans_version, ObTxData::MAX_STATE_CNT, can_read))) {
      LOG_WARN("failed to add trans state to cache", K(tmp_ret),
        "trans_id", lock_for_read_arg.data_trans_id_,
        "sql_seq", lock_for_read_arg.data_sql_sequence_);
    }
  }
  if (REACH_THREAD_TIME_INTERVAL(30 * 1000 * 1000 /*30s*/)) {
    cost_time = common::ObClockGenerator::getClock() - cost_time;
    if (cost_time > 10 * 1000 /*10ms*/) {
      LOG_INFO("multi-ver row scanner lock for read", K(ret), K(cost_time));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::get_store_rowkey(ObStoreRowkey &store_rowkey,
                                                         ObDatumRowkeyHelper &rowkey_helper)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey datum_rowkey;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(row_.reserve(read_info_->get_request_count()))) {
    LOG_WARN("Fail to reserve datum row", K(ret), K_(row));
  } else if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to judge end of block or not", K(ret));
    }
  } else if (OB_FAIL(reader_->get_row(current_, row_))) {
    LOG_WARN("micro block reader fail to get block_row", K(ret), K(current_));
  } else if (OB_FAIL(datum_rowkey.assign(row_.storage_datums_, read_info_->get_schema_rowkey_count()))) {
    LOG_WARN("assign datum_rowkey fail", K(ret), K(row_), KPC(read_info_));
  } else if (OB_FAIL(rowkey_helper.convert_store_rowkey(datum_rowkey, read_info_->get_columns_desc(), store_rowkey))) {
    LOG_WARN("convert datumn_rowkey to store_rowkey fail", K(ret), KPC(read_info_), K(datum_rowkey));
  }

  return ret;
}

////////////////////////////// ObMultiVersionDIMicroBlockRowScanner //////////////////////////////
void ObMultiVersionDIMicroBlockRowScanner::reuse()
{
  ObMultiVersionMicroBlockRowScanner::reuse();
  is_prev_micro_row_valid_ = false;
  found_first_di_row_ = false;
}

int ObMultiVersionDIMicroBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &block_data,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (ObQueryFlag::ScanOrder::NoOrder != context_->query_flag_.scan_order_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only use di multi version micro scanner in no-order scan mode", K(ret), K_(macro_id), K(context_->query_flag_.scan_order_));
  } else if (OB_FAIL(ObMultiVersionMicroBlockRowScanner::open(macro_id, block_data, is_left_border, is_right_border))) {
    LOG_WARN("base open failed", K(ret));
  } else if (!can_ignore_multi_version_) {
    if (OB_FAIL(init_bitmap(di_bitmap_, false/*is_all_true*/))) {
      LOG_WARN("fail to init di bitmap", K(ret), K_(macro_id));
    } else if (OB_FAIL(preprocess_di_rows())) {
      LOG_WARN("fail to process delete insert rows in micro scanner", K(ret), K_(macro_id));
    }
  }
  if (OB_SUCC(ret) && nullptr != param_->pushdown_filter_) {
    if (OB_FAIL(init_bitmap(filter_bitmap_, true/*is_all_true*/))) {
      LOG_WARN("fail to init filter bitmap", K(ret), K_(macro_id));
    } else {
      use_private_bitmap_ = true;
    }
  }
  return ret;
}

int ObMultiVersionDIMicroBlockRowScanner::inner_get_next_compact_di_row(const ObDatumRow *&ret_row)
{
  int ret = OB_SUCCESS;
  ret_row = nullptr;
  int64_t insert_idx = -1;
  int64_t delete_idx = -1;
  bool meet_next_rowkey = false;
  int64_t filtered_idx = -1;
  const ObRowHeader *row_header = nullptr;
  bool need_check_next_rowkey = is_prev_micro_row_valid_;
  while (OB_SUCC(ret) && !meet_next_rowkey) {
    if (OB_FAIL(end_of_block())) {
      // meet iter end
    } else if (OB_FAIL(di_bitmap_->get_next_valid_idx_directly(current_, filtered_idx))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next valid idx", K(ret), K_(macro_id), K_(current), K_(last), K(filtered_idx));
      } else if (OB_FAIL(check_meet_next_rowkey(current_, last_, meet_next_rowkey))) {
        LOG_WARN("fail to check meet next rowkey", K(ret), K_(macro_id), K_(current), K(filtered_idx), K(meet_next_rowkey));
      } else {
        ret = OB_ITER_END;
      }
    } else if (need_check_next_rowkey && OB_FAIL(check_meet_next_rowkey(current_, filtered_idx - 1, meet_next_rowkey))) {
      LOG_WARN("fail to check meet next rowkey", K(ret), K_(macro_id), K_(current), K(filtered_idx), K(meet_next_rowkey));
    } else if (!meet_next_rowkey) {
      if (OB_FAIL(reader_->get_row_header(filtered_idx, row_header))) {
        LOG_WARN("fail to get next first row header", K(ret), K_(macro_id), K(filtered_idx));
      } else {
        if (row_header->get_row_flag().is_insert()) {
          if (OB_UNLIKELY(-1 != insert_idx)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected insert idx of same rowkey while delete idx exists", K(ret), K_(macro_id), K(delete_idx), K_(current));
          } else {
            insert_idx = filtered_idx;
          }
        } else if (row_header->get_row_flag().is_delete()) {
          delete_idx = filtered_idx;
        }
        if (OB_SUCC(ret)) {
          meet_next_rowkey = row_header->get_row_multi_version_flag().is_last_multi_version_row();
          need_check_next_rowkey = !meet_next_rowkey;
          current_ = filtered_idx + 1;
        }
      }
    }
  }

  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    if (meet_next_rowkey) {
      if (OB_FAIL(compact_rows_of_same_rowkey(insert_idx, delete_idx, ret_row))) {
        LOG_WARN("fail to get rows of the same rowkey", K(ret), K_(macro_id), K(insert_idx), K(delete_idx), K(meet_next_rowkey));
      } else if (OB_ISNULL(ret_row)) {
        ret = OB_ITER_END;
      } else {
        is_prev_micro_row_valid_ = false;
      }
    } else {
      if (OB_FAIL(try_cache_unfinished_row(insert_idx, delete_idx))) {
        LOG_WARN("fail to cache the last insert row", K(ret), K_(macro_id), K(insert_idx), K(delete_idx));
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObMultiVersionDIMicroBlockRowScanner::inner_get_next_header_info(
    int64_t &index,
    bool &version_fit,
    bool &final_result,
    ObDmlRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  version_fit = true;
  final_result = false;
  const ObRowHeader *row_header = nullptr;
  if (ObIMicroBlockReaderInfo::INVALID_ROW_INDEX == index || index > last_) {
    ret = OB_ITER_END;
  } else if (read_row_direct_flag_) {
    bool is_ghost_row_flag = false;
    if (OB_FAIL(reader_->get_row_header(index, row_header))) {
      LOG_WARN("micro block reader fail to get row header", K(ret), K_(macro_id), K(index));
    } else {
      const ObMultiVersionRowFlag &mvcc_row_flag = row_header->get_row_multi_version_flag();
      row_flag = row_header->get_row_flag();
      is_last_multi_version_row_ = mvcc_row_flag.is_last_multi_version_row();
      if (OB_FAIL(ObGhostRowUtil::is_ghost_row(mvcc_row_flag, is_ghost_row_flag))) {
        LOG_WARN("fail to check ghost row", K(ret), K_(macro_id), K(index), K(mvcc_row_flag));
      } else if (OB_UNLIKELY(is_ghost_row_flag)) {
        version_fit = false;
        row_flag.set_flag(ObDmlFlag::DF_NOT_EXIST);
      }
    }
  } else {
    // check whether version fit
    bool unused_have_uncommited_row = false;
    int64_t trans_version = 0;
    int64_t sql_sequence = 0;
    if (OB_FAIL(check_trans_version(final_result, version_fit, unused_have_uncommited_row, trans_version, sql_sequence, index, row_header))) {
      LOG_WARN("fail to check trans version", K(ret), K_(macro_id), K(index), K(version_fit), K(trans_version), K(sql_sequence));
    } else if (OB_FAIL(check_foreign_key(trans_version, sql_sequence, row_header))) {
      LOG_WARN("fail to check foreign key", K(ret), K_(macro_id), K(index), K(trans_version), K(sql_sequence));
    } else {
      row_flag = row_header->get_row_flag();
      is_last_multi_version_row_ = row_header->get_row_multi_version_flag().is_last_multi_version_row();
    }
  }
  if (OB_SUCC(ret)) {
    index = index + 1;
    LOG_DEBUG("[MULTIVERSION MOW] inner get next header info", K(ret), K_(macro_id), K(index),
        K(version_fit), K(row_flag), K_(read_row_direct_flag), K_(is_last_multi_version_row));
  }
  return ret;
}

int ObMultiVersionDIMicroBlockRowScanner::locate_next_rowkey(
    int64_t &index)
{
  int ret = OB_SUCCESS;
  if (ObIMicroBlockReaderInfo::INVALID_ROW_INDEX == index || index > last_) {
    ret = OB_ITER_END;
  } else if (finish_scanning_cur_rowkey_) {
    if (!is_last_multi_version_row_) {
      const ObRowHeader *row_header = nullptr;
      while (OB_SUCC(ret)) {
        if (index > last_) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(reader_->get_row_header(index, row_header))) {
          LOG_WARN("failed to get row header", K(ret), K(index), K_(macro_id));
        } else {
          index = index + 1;
          if (row_header->get_row_multi_version_flag().is_last_multi_version_row()) {
            found_first_di_row_ = false;
            finish_scanning_cur_rowkey_ = false;
            break;
          }
        }
      }
    } else {
      found_first_di_row_ = false;
      finish_scanning_cur_rowkey_ = false;
    }
  }
  return ret;
}

int ObMultiVersionDIMicroBlockRowScanner::preprocess_di_rows()
{
  int ret = OB_SUCCESS;
  int64_t index = start_;
  bool need_check_cached_rowkey = !finish_scanning_cur_rowkey_ && is_prev_micro_row_valid_;
  for ( ; OB_SUCC(ret) && ObIMicroBlockReaderInfo::INVALID_ROW_INDEX != index && index <= last_; ) {
    int64_t insert_row_idx = -1;
    int64_t delete_row_idx = -1;
    int64_t last_row_idx = -1;
    if (OB_FAIL(locate_next_rowkey(index))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to locate cursor to read", K(ret), K_(macro_id), K_(start), K_(last), K(index));
      }
    }
    while (OB_SUCC(ret) && !finish_scanning_cur_rowkey_) {
      ObDmlRowFlag row_flag;
      bool version_fit = false;
      bool final_result = false;
      if (OB_FAIL(inner_get_next_header_info(index, version_fit, final_result, row_flag))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row header info", K(ret), K_(macro_id), K(index), K(row_flag));
        }
      } else if (!version_fit || row_flag.is_lock()) {
        if (is_last_multi_version_row_ || final_result) {
          finish_scanning_cur_rowkey_ = true;
        }
      } else {
        // get version fit rows of current rowkey
        if (row_flag.is_insert()) {
          if (!found_first_di_row_) {
            insert_row_idx = index - 1;
          }
          // check whether cached row is valid
          if (need_check_cached_rowkey) {
            if (prev_micro_row_.row_flag_.is_insert()) {
              prev_micro_row_.delete_version_ = 0;
              prev_micro_row_.is_delete_filtered_ = false;
            }
          }
        } else {
          delete_row_idx = index - 1;
        }
        found_first_di_row_ = true;
        last_row_idx = index - 1;
        if (is_last_multi_version_row_) {
          finish_scanning_cur_rowkey_ = true;
        }
      }
    }
    // set di bitmap and set cached di rows indexes
    if (OB_SUCC(ret) || OB_LIKELY(OB_ITER_END == ret)) {
      if (-1 != insert_row_idx && OB_FAIL(di_bitmap_->set(insert_row_idx, true))) {
        LOG_WARN("fail to set bitmap for the last insert row", K(ret), K_(macro_id), K(index), K(insert_row_idx));
      } else if (-1 != delete_row_idx) {
        if ((-1 != insert_row_idx) || (need_check_cached_rowkey && prev_micro_row_.row_flag_.is_insert())) {
          // the latest insert row exists, check whether the delete row is earliest row
          if (last_row_idx == delete_row_idx) {
            if (OB_FAIL(di_bitmap_->set(delete_row_idx, true))) {
              LOG_WARN("fail to set bitmap for the first delete row", K(ret), K_(macro_id), K(index), K(delete_row_idx));
            }
          }
        } else if (OB_FAIL(di_bitmap_->set(delete_row_idx, true))) {
          LOG_WARN("fail to set bitmap for the delete row", K(ret), K_(macro_id), K(index), K(delete_row_idx));
        }
      }
    }
    // only one unfinished rowkey, only need to check once
    need_check_cached_rowkey = false;
  }
  if (OB_LIKELY(OB_ITER_END == ret)) {
    ret = OB_SUCCESS;
  }
  LOG_TRACE("[MULTIVERSION MOW] check processed delete insert bitmap", K(ret), K_(macro_id), K_(finish_scanning_cur_rowkey), KPC_(di_bitmap), K_(is_last_multi_version_row));
  return ret;
}

int ObMultiVersionDIMicroBlockRowScanner::compact_rows_of_same_rowkey(
  const int64_t insert_idx,
  const int64_t delete_idx,
  const ObDatumRow *&ret_row)
{
  int ret = OB_SUCCESS;
  ret_row = nullptr;
  ObDatumRow *insert_row = nullptr;
  ObDatumRow *delete_row = nullptr;
  int64_t delete_version = 0;
  if (-1 != insert_idx) {
    int64_t insert_version = 0;
    if (OB_UNLIKELY(is_prev_micro_row_valid_ && prev_micro_row_.row_flag_.is_insert())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected insert row while cached row exists", K(ret), K_(macro_id), K(insert_idx), K_(prev_micro_row));
    } else if (OB_FAIL(get_trans_version(insert_version, insert_idx))) {
      LOG_WARN("fail to get insert version", K(ret), K_(macro_id), K(insert_idx));
    } else if (OB_FAIL(reader_->get_row(insert_idx, row_))) {
      LOG_WARN("micro block reader fail to get insert row", K(ret), K_(macro_id), K(insert_idx));
    } else if (OB_FAIL(set_row_trans_col(insert_version, row_))) {
      LOG_WARN("fail to set ora_rowscn", K(ret), K_(macro_id), K(insert_idx), K_(row));
    } else {
      row_.is_insert_filtered_ = use_private_bitmap_ && !filter_bitmap_->test(insert_idx);
      row_.insert_version_ = insert_version;
      insert_row = &row_;
    }
  }

  if (OB_SUCC(ret) && is_prev_micro_row_valid_ && prev_micro_row_.row_flag_.is_insert()) {
    insert_row = &prev_micro_row_;
  }

  if (OB_SUCC(ret) && -1 != delete_idx) {
    if (OB_FAIL(get_trans_version(delete_version, delete_idx))) {
      LOG_WARN("fail to get delete version", K(ret), K_(macro_id), K(delete_idx));
    } else if (nullptr == insert_row) {
      if (OB_FAIL(reader_->get_row(delete_idx, row_))) {
        LOG_WARN("micro block reader fail to get delete row", K(ret), K_(macro_id), K(delete_idx));
      } else {
        row_.is_delete_filtered_ = use_private_bitmap_ && !filter_bitmap_->test(delete_idx);
        row_.delete_version_ = delete_version;
        delete_row = &row_;
      }
    } else {
      insert_row->is_delete_filtered_ = use_private_bitmap_ && !filter_bitmap_->test(delete_idx);
      insert_row->delete_version_ = delete_version;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (nullptr != insert_row) {
    ret_row = insert_row;
  } else if (nullptr != delete_row) {
    ret_row = delete_row;
  } else if (is_prev_micro_row_valid_) {
    // return cached delete row if insert_row and delete_row are null
    ret_row = &prev_micro_row_;
  }
  LOG_DEBUG("[MULTIVERSION MOW] get compacted di row", K(ret), K_(macro_id), KPC(ret_row), K(insert_idx), K(delete_idx), K_(prev_micro_row));
  return ret;
}

// start inclusive, end inclusive, start <= end
int ObMultiVersionDIMicroBlockRowScanner::check_meet_next_rowkey(
    const int64_t start,
    const int64_t end,
    bool &meet_next_rowkey) const
{
  int ret = OB_SUCCESS;
  meet_next_rowkey = false;
  const ObRowHeader *row_header = nullptr;
  for (int64_t idx = start; OB_SUCC(ret) && idx <= end; ++idx) {
    if (OB_FAIL(reader_->get_row_header(idx, row_header))) {
      LOG_WARN("failed to get row header", K(ret), K(idx), K_(macro_id));
    } else if (row_header->get_row_multi_version_flag().is_last_multi_version_row()) {
      meet_next_rowkey = true;
      break;
    }
  }
  LOG_DEBUG("[MULTIVERSION MOW] check meet next rowkey", K(start), K(end), K_(start), K(meet_next_rowkey));
  return ret;
}

int ObMultiVersionDIMicroBlockRowScanner::try_cache_unfinished_row(
    const int64_t insert_idx,
    const int64_t delete_idx)
{
  int ret = OB_SUCCESS;
  if (-1 != insert_idx) {
    int64_t insert_version = 0;
    // cache the last insert row, the cached row will output in the next micro block
    if (OB_UNLIKELY(is_prev_micro_row_valid_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected insert idx while cached row exists", K(ret), K_(macro_id), K(insert_idx), K_(prev_micro_row));
    } else if (OB_FAIL(reader_->get_row(insert_idx, tmp_row_))) {
      LOG_WARN("fail to get the last insert row", K(ret), K_(macro_id), K(insert_idx));
    } else if (OB_FAIL(prev_micro_row_.deep_copy(tmp_row_, allocator_))) {
      LOG_WARN("fail to deep copy prev insert row", K(ret), K_(tmp_row));
    } else if (OB_FAIL(get_trans_version(insert_version, insert_idx))) {
      LOG_WARN("fail to get insert version", K(ret), K_(macro_id), K(insert_idx));
    } else if (OB_FAIL(set_row_trans_col(insert_version, prev_micro_row_))) {
      LOG_WARN("fail to set ora_rowscn", K(ret), K_(macro_id), K(insert_idx), K_(prev_micro_row));
    } else {
      is_prev_micro_row_valid_ = true;
      prev_micro_row_.insert_version_ = insert_version;
      prev_micro_row_.is_insert_filtered_ = use_private_bitmap_ && !filter_bitmap_->test(insert_idx);
    }
  }
  if (OB_SUCC(ret) && -1 != delete_idx) {
    // cache the delete version or delete row, the cached row/delete verion may not output in the next micro block
    int64_t delete_version = 0;
    if (OB_FAIL(get_trans_version(delete_version, delete_idx))) {
      LOG_WARN("fail to get delete version", K(ret), K(delete_idx));
    } else if (!is_prev_micro_row_valid_ || prev_micro_row_.row_flag_.is_delete()) {
      if (OB_FAIL(reader_->get_row(delete_idx, tmp_row_))) {
        LOG_WARN("fail to get the first delete row", K(ret), K_(macro_id), K(delete_idx));
      } else if (OB_FAIL(prev_micro_row_.deep_copy(tmp_row_, allocator_))) {
        LOG_WARN("fail to deep copy prev delete row", K(ret), K_(tmp_row));
      } else {
        is_prev_micro_row_valid_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      prev_micro_row_.delete_version_ = delete_version;
      prev_micro_row_.is_delete_filtered_ = use_private_bitmap_ && !filter_bitmap_->test(delete_idx);
    }
  }
  LOG_DEBUG("[MULTIVERSION MOW] try cache unfinished row", K(ret), K_(macro_id), K(insert_idx), K(delete_idx), K_(is_prev_micro_row_valid), K_(prev_micro_row));
  return ret;
}

int ObMultiVersionDIMicroBlockRowScanner::get_trans_version(
    int64_t &trans_version,
    const int64_t index)
{
  int ret = OB_SUCCESS;
  bool unused_final_result = false;
  bool version_fit = false;
  bool have_uncommited_row = false;
  int64_t unused_sql_sequence = 0;
  const ObRowHeader *unused_row_header = nullptr;
  if (OB_FAIL(check_trans_version(unused_final_result, version_fit, have_uncommited_row, trans_version, unused_sql_sequence, index, unused_row_header))) {
    LOG_WARN("fail to check trans version", K(ret), K_(macro_id), K(index), K(trans_version));
  } else if (version_fit && have_uncommited_row && 0 == trans_version) {
    // running transaction and can be read
    trans_version = INT64_MAX;
  }
  return ret;
}

int ObMultiVersionDIMicroBlockRowScanner::set_row_trans_col(
    const int64_t trans_version,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX != read_info_->get_trans_col_index()) {
    int64_t trans_idx = read_info_->get_trans_col_index();
    if (OB_UNLIKELY(trans_idx >= row.count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected trans info", K(ret), K(trans_idx), K(row), KPC_(read_info));
    } else if (transaction::is_effective_trans_version(trans_version)) {
      if (OB_UNLIKELY(0 >= trans_version)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected trans info", K(ret), K(trans_version), K(row), KPC_(read_info));
      } else {
        LOG_DEBUG("success to set trans_version", K(ret), K(trans_version));
        row.storage_datums_[trans_idx].reuse();
        row.storage_datums_[trans_idx].set_int(-trans_version);
      }
    }
  }
  return ret;
}

int ObMultiVersionDIMicroBlockRowScanner::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (can_ignore_multi_version_) { // TODO(yuanzhe) refactor blockscan opt of multi version sstable
    if (OB_FAIL(inner_get_next_di_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to inner get next row", K(ret), K_(start), K_(last), K_(current));
      }
    }
  } else {
    reuse_cur_micro_row();
    if (OB_FAIL(inner_get_next_compact_di_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to inner get next row", K(ret), K_(start), K_(last), K_(current));
      }
    } else if (OB_NOT_NULL(row)) {
      const_cast<ObDatumRow *>(row)->fast_filter_skipped_ = is_filter_applied_;
    }
  }
  return ret;
}

int ObMultiVersionDIMicroBlockRowScanner::inner_get_next_di_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to judge end of block or not", K(ret));
    }
  } else if (can_blockscan_) {
    if (OB_FAIL(inner_get_next_row_blockscan(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("[PUSHDOWN] failed to get next row pushdown", K(ret), K_(macro_id));
      }
    }
  } else {
    if (OB_FAIL(reader_->get_row(current_, row_))) {
      LOG_WARN("micro block reader fail to get row.", K(ret), K_(macro_id));
    } else {
      int64_t trans_version = 0;
      int64_t sql_sequence = 0;
      const ObRowHeader *row_header = nullptr;
      if (OB_FAIL(reader_->get_multi_version_info(
                  current_,
                  read_info_->get_schema_rowkey_count(),
                  row_header,
                  trans_version,
                  sql_sequence))) {
        LOG_WARN("fail to get multi version info", K(ret), K_(current), KPC_(read_info),
                 K_(macro_id));
      } else if (row_.row_flag_.is_delete()) {
        row_.delete_version_ = trans_version;
        row_.is_delete_filtered_ = use_private_bitmap_ && !filter_bitmap_->test(current_);
      } else {
        row_.insert_version_ = trans_version;
        row_.is_insert_filtered_ = use_private_bitmap_ && !filter_bitmap_->test(current_);
      }
    }

    if (OB_SUCC(ret)) {
      row_.fast_filter_skipped_ = is_filter_applied_;
      row = &row_;
      current_ += step_;
    }
  }
  LOG_DEBUG("get next row", K(ret), KPC(row), K_(macro_id));
  return ret;
}

////////////////////////////// ObMultiVersionMicroBlockMinorMergeRowScanner //////////////////////////////
void ObMultiVersionMicroBlockMinorMergeRowScanner::reuse()
{
  row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  ObIMicroBlockRowScanner::reuse();
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::init(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::init(param, context, sstable))) {
    LOG_WARN("base init failed", K(ret));
  } else {
    trans_version_col_idx_ = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
        read_info_->get_schema_rowkey_count(), true);
    sql_sequence_col_idx_ = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
        read_info_->get_schema_rowkey_count(), true);
    is_inited_ = true;
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &block_data,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::open(
              macro_id, block_data, is_left_border, is_right_border))) {
    LOG_WARN("base open failed", K(ret));
  } else if (OB_UNLIKELY(reverse_scan_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("minor merge row scanner cannot do reverse scan", K(ret));
  } else {
    const ObMicroBlockHeader *micro_header = block_data.get_micro_header();
    int64_t col_count = MAX(read_info_->get_request_count(), micro_header->column_count_);
    if (OB_FAIL(set_base_scan_param(is_left_border, is_right_border))) {
      LOG_WARN("failed to set base scan param", K(ret), K(is_left_border), K(is_right_border), K_(macro_id));
    } else if (OB_FAIL(row_.reserve(col_count))) {
      STORAGE_LOG(WARN, "Failed to reserve datum row", K(ret), K(col_count));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(CHECK_UNCOMMIT_TX_CORRECT, "used to check uncommit tx correct");
int ObMultiVersionMicroBlockMinorMergeRowScanner::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }

  while (OB_SUCC(ret)) {
    bool skip_curr_row = false;
    bool need_ghost = false;
    if (OB_FAIL(end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to check end of block", K(ret));
      }
    } else if (OB_FAIL(reader_->get_row(current_, row_))) {
      LOG_WARN("micro block reader fail to get row.", K(ret), K_(macro_id));
    } else if (OB_FAIL(check_row_trans_state(skip_curr_row))) {
      LOG_WARN("fail to check_row_trans_state", K(ret));
    } else if (FALSE_IT(++current_)) {
    } else if (skip_curr_row) {
      if (row_.is_last_multi_version_row()) {
        if (OB_FAIL(ObGhostRowUtil::make_ghost_row(sql_sequence_col_idx_, row_))) {
          LOG_WARN("failed to make ghost row", K(ret), K(row_));
        } else {
          break;
        }
      }
    } else {
      break;
    }
  } // end of while

  if (OB_FAIL(ret)) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("switch status to call func error", K(ret));
    }
  } else if (OB_UNLIKELY(!row_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("row is invalid", KPC(row));
  } else if (FALSE_IT(row = &row_)) {
  } else if (row_.row_flag_.is_delete() && !row_.mvcc_row_flag_.is_uncommitted_row()) {
    // set delete committed row compacted
    row_.set_compacted_multi_version_row();
  } else {
#ifdef ERRSIM
    // here row committed or during execution will be output, only used for minor merge.
    bool is_check_uncommit_tx_info_correct = CHECK_UNCOMMIT_TX_CORRECT ? true : false;
    if (is_check_uncommit_tx_info_correct && row_.mvcc_row_flag_.is_uncommitted_row()) {
      const int64_t tx_id = row_.trans_id_.get_id();
      const int64_t sql_seq = row_.storage_datums_[sql_sequence_col_idx_].get_int();
      if (OB_FAIL(check_uncommit_tx_info_correct(tx_id, sql_seq))) {
        LOG_WARN("Failed to check uncommit tx info valid", K(ret));
      }
    }
#endif
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::check_row_trans_state(bool &skip_curr_row)
{
  int ret = OB_SUCCESS;
  skip_curr_row = false;
  if (row_.mvcc_row_flag_.is_uncommitted_row()) {
    bool can_read = false;
    int64_t state = ObTxData::MAX_STATE_CNT;
    const transaction::ObTransID &read_trans_id = row_.trans_id_;
    if (OB_FAIL(get_trans_state(read_trans_id, state, can_read))) { // will get committed_trans_version_ & last_trans_state_
      LOG_WARN("get transaction status failed", K(ret), "trans_id", row_.get_trans_id(), K(state));
    } else if (!can_read) {
      skip_curr_row = true;
    } else {
      switch (state) {
      case ObTxData::COMMIT: {
        if (row_.row_flag_.is_lock()) {
          skip_curr_row = true;
        } else {
          row_.mvcc_row_flag_.set_uncommitted_row(false);
          row_.trans_id_.reset();
          row_.storage_datums_[sql_sequence_col_idx_].reuse();
          row_.storage_datums_[sql_sequence_col_idx_].set_int(0);
          row_.storage_datums_[trans_version_col_idx_].reuse();
          row_.storage_datums_[trans_version_col_idx_].set_int(-committed_trans_version_);
        }
        break;
      }
      case ObTxData::RUNNING: {
        break;
      }
      case ObTxData::ABORT: {
        skip_curr_row = true;
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("wrong transaction status", K(ret), K(committed_trans_version_));
      }
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::get_trans_state(
  const transaction::ObTransID &read_trans_id,
  int64_t &state,
  bool &can_read)
{
  int ret = OB_SUCCESS;
  can_read = false;
  const int64_t sql_sequence = -row_.storage_datums_[sql_sequence_col_idx_].get_int();
  const transaction::ObTxSEQ tx_sequence = transaction::ObTxSEQ::cast_from_int(sql_sequence);
  state = get_trans_state_from_cache(read_trans_id, tx_sequence, can_read);
  if (ObTxCommitData::MAX_STATE_CNT == state
      && OB_FAIL(get_trans_state_from_tx_table(read_trans_id, tx_sequence, state, can_read))) {
    LOG_WARN("failed to get trans state from tx table", KR(ret), "trans_id", row_.get_trans_id(), K(tx_sequence));
  }
  return ret;
}

int64_t ObMultiVersionMicroBlockMinorMergeRowScanner::get_trans_state_from_cache(
  const transaction::ObTransID &read_trans_id,
  const transaction::ObTxSEQ &tx_sequence,
  bool &can_read)
{
  int64_t state = ObTxCommitData::MAX_STATE_CNT;
  compaction::ObMergeCachedTransState trans_state;
  if (OB_NOT_NULL(context_->trans_state_mgr_) &&
      OB_SUCCESS == context_->trans_state_mgr_->get_trans_state(
                        read_trans_id, tx_sequence, trans_state)) {
    state = trans_state.trans_state_;
    last_trans_state_ = trans_state.trans_state_;
    committed_trans_version_ = trans_state.trans_version_;
    can_read = (ObTxData::ABORT == state ? false :trans_state.can_read_);
  }
  return state;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::get_trans_state_from_tx_table(
   const transaction::ObTransID &read_trans_id,
  const transaction::ObTxSEQ &sql_seq,
  int64_t &state,
  bool &can_read)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  can_read = false;
  SCN scn_commit_trans_version = SCN::max_scn();
  storage::ObTxTableGuards &tx_table_guards = context_->store_ctx_->mvcc_acc_ctx_.get_tx_table_guards();
  if (OB_FAIL(tx_table_guards.get_tx_state_with_scn(
      read_trans_id, context_->merge_scn_, state, scn_commit_trans_version))) {
    LOG_WARN("get transaction status failed", K(ret), K(read_trans_id), K(state));
  } else {
    committed_trans_version_ = scn_commit_trans_version.get_val_for_tx();
    last_trans_state_ = state;
    if (ObTxData::ABORT != state) { // check sql seq can read for RUNNING/COMMIT trans
      int64_t cost_time = common::ObClockGenerator::getClock();
      if (OB_FAIL(tx_table_guards.check_sql_sequence_can_read(
              read_trans_id,
              sql_seq,
              sstable_->get_end_scn(),
              can_read))) {
        LOG_WARN("check sql sequence can read failed", K(ret), K(can_read), K(read_trans_id), K(sql_seq));
      } else if (OB_NOT_NULL(context_->trans_state_mgr_) &&
        OB_TMP_FAIL(context_->trans_state_mgr_->add_trans_state(read_trans_id, sql_seq,
          committed_trans_version_, last_trans_state_, can_read))) {
        LOG_WARN("failed to add minor trans state", K(tmp_ret), K(read_trans_id), K(sql_seq), K(can_read));
      }
      if (REACH_THREAD_TIME_INTERVAL(30 * 1000 * 1000 /*30s*/)) {
        cost_time = common::ObClockGenerator::getClock() - cost_time;
        if (cost_time > 10 * 1000 /*10ms*/) {
          LOG_INFO("multi-ver minor row scanner check seq", K(ret), K(cost_time));
        }
      }
    }
  }
  LOG_DEBUG("cxf debug check sql sequence can read", K(ret), K(can_read), K(read_trans_id), K(sql_seq));
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::get_first_row_mvcc_info(
    bool &is_first_row,
    bool &is_shadow_row) const
{
  int ret = OB_SUCCESS;
  is_first_row = false;
  is_shadow_row = false;
  const ObRowHeader *row_header = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ITER_END == end_of_block()) {
  } else if (OB_FAIL(reader_->get_row_header(current_, row_header))) {
    LOG_WARN("failed to get row header", K(ret), KPC(this), K_(macro_id));
  } else {
    is_first_row = row_header->get_row_multi_version_flag().is_first_multi_version_row();
    is_shadow_row = row_header->get_row_multi_version_flag().is_shadow_row();
  }
  return ret;
}

#ifdef ERRSIM
int ObMultiVersionMicroBlockMinorMergeRowScanner::check_uncommit_tx_info_correct(const int64_t tx_id, const int64_t sql_seq)
{
  int ret = OB_SUCCESS;
  blocksstable::ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(sstable_->get_meta(meta_handle))) {
    LOG_WARN("failed to get meta", K(ret), K_(sstable), K(meta_handle));
  } else {
    const compaction::ObMetaUncommitTxInfo &tmp_uncommit_tx_info = meta_handle.get_sstable_meta().get_uncommit_tx_info();
    if (tmp_uncommit_tx_info.is_info_overflow()) { // if info overflow, skip
    } else {
      // this path will be executed only when have uncommitted row,
      // if info_status not INFO_OVERFLOW but count == 0 , it means uncommit tx info collected is not correct
      bool has_found = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_uncommit_tx_info.uncommit_tx_desc_count_; ++i) {
        if (tx_id == tmp_uncommit_tx_info.tx_infos_[i].tx_id_) {
          if (!tmp_uncommit_tx_info.tx_infos_[i].is_sql_seq() || sql_seq == tmp_uncommit_tx_info.tx_infos_[i].sql_seq_) {
            has_found = true;
            break;
          }
        }
      }
      if (!has_found) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("uncommit tx info collected is not correct", K(ret), K(tx_id), K(sql_seq), K(tmp_uncommit_tx_info));
      }
    }
  }
  return ret;
}
#endif

}
}
