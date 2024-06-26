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
#include "storage/ob_row_fuse.h"
#include "storage/access/ob_block_row_store.h"
#include "storage/access/ob_aggregated_store.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/blocksstable/index_block/ob_index_block_row_scanner.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tx/ob_tx_data_functor.h"
#include "storage/compaction/ob_compaction_trans_cache.h"

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
    current_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    start_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    last_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    step_(1),
    row_(),
    macro_id_(),
    read_info_(nullptr),
    range_(nullptr),
    sstable_(nullptr),
    reader_(nullptr),
    flat_reader_(nullptr),
    decoder_(nullptr),
    pax_decoder_(nullptr),
    cs_decoder_(nullptr),
    param_(nullptr),
    context_(nullptr),
    allocator_(allocator),
    can_ignore_multi_version_(false),
    block_row_store_(nullptr)
{}

ObIMicroBlockRowScanner::~ObIMicroBlockRowScanner()
{
  if (nullptr != flat_reader_) {
    flat_reader_->~ObMicroBlockReader();
    allocator_.free(flat_reader_);
  }
  if (nullptr != pax_decoder_) {
    pax_decoder_->~ObMicroBlockDecoder();
    allocator_.free(pax_decoder_);
  }
  if (nullptr != cs_decoder_) {
    cs_decoder_->~ObMicroBlockCSDecoder();
    allocator_.free(cs_decoder_);
  }
}

void ObIMicroBlockRowScanner::reuse()
{
  range_ = nullptr;
  reverse_scan_ = false;
  is_left_border_ = false;
  is_right_border_ = false;
  current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  start_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  last_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  can_ignore_multi_version_ = false;
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
  } else {
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
    LOG_WARN("Invalid pushdown scanner", K(ret), K(block_row_store_));
  } else if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to judge end of block or not", K(ret));
    }
  } else if (OB_UNLIKELY(!block_row_store_->filter_applied())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpect pushdown filter", K(ret), KPC(block_row_store_));
  } else {
    int64_t prev_current = current_;
    ObBlockBatchedRowStore *batch_store = reinterpret_cast<ObBlockBatchedRowStore *>(block_row_store_);
    if (OB_ISNULL(reader_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected micro block reader to pushdown", K(ret), KP(flat_reader_), KP(decoder_));
    } else if (OB_FAIL(block_row_store_->get_filter_result(res))) {
      LOG_WARN("Failed to get pushdown filter result bitmap", K(ret));
    } else if (OB_FAIL(batch_store->fill_rows(
                range_->get_group_idx(),
                this,
                current_,
                last_ + step_,
                res))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to apply rows", K(ret), K(current_), K(last_), K(step_),
                 K(res), KPC(sstable_));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(context_)) {
      context_->table_store_stat_.logical_read_cnt_ += (current_ - prev_current);
      context_->table_store_stat_.physical_read_cnt_ += (current_ - prev_current);
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
  } else if (OB_UNLIKELY(nullptr != block_row_store_ && block_row_store_->can_blockscan())) {
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
  if (OB_SUCC(ret) && OB_NOT_NULL(context_)) {
    ++context_->table_store_stat_.physical_read_cnt_;
  }
  LOG_DEBUG("get next row", K(ret), KPC(row), K_(macro_id));
  return ret;
}

int ObIMicroBlockRowScanner::inner_get_row_header(const ObRowHeader *&row_header)
{
  int ret = OB_SUCCESS;
  row_header = nullptr;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to judge end of block or not", K(ret));
    }
  } else {
    if (OB_FAIL(reader_->get_row_header(current_, row_header))) {
      LOG_WARN("micro block reader fail to get row.", K(ret), K_(macro_id));
    } else {
      current_ += step_;
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::inner_get_next_row_blockscan(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = NULL;
  ObFilterResult res;
  if (OB_FAIL(block_row_store_->get_filter_result(res))) {
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
          row_.fast_filter_skipped_ = block_row_store_->filter_applied();
          row = &row_;
        }
      }
      current_ += step_;
    }
  }
  return ret;
}

int ObIMicroBlockRowScanner::apply_blockscan(
    storage::ObBlockRowStore *block_row_store,
    storage::ObTableScanStoreStat &table_store_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == block_row_store || !block_row_store->is_valid() || nullptr == reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected block row store", K(ret), KPC(block_row_store_), KP_(reader));
  } else if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to judge end of block or not", K(ret), K_(macro_id), K_(start), K_(last), K_(current));
    }
  } else if (reader_->get_column_count() <= read_info_->get_max_col_index()) {
  } else if (OB_FAIL(block_row_store->apply_blockscan(
              *this,
              can_ignore_multi_version_,
              table_store_stat))) {
    LOG_WARN("Failed to filter and aggregate micro block", K(ret), K_(macro_id));
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("query interrupt", K(ret));
  }
  return ret;
}

int ObIMicroBlockRowScanner::set_reader(const ObRowStoreType store_type)
{
#define INIT_MICRO_READER(ptr, type)                                        \
  do {                                                                      \
    if (ptr == nullptr) {                                                   \
      if (OB_ISNULL(ptr = OB_NEWx(type, (&allocator_)))) {                  \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                    \
        LOG_WARN("Failed to alloc memory for reader", K(ret));              \
      }                                                                     \
    }                                                                       \
    if (OB_SUCC(ret)) {                                                     \
      reader_ = ptr;                                                        \
    }                                                                       \
  } while(0)

  int ret = OB_SUCCESS;
  switch (store_type) {
    case FLAT_ROW_STORE: {
      INIT_MICRO_READER(flat_reader_, ObMicroBlockReader);
      break;
    }

    case ENCODING_ROW_STORE:
    case SELECTIVE_ENCODING_ROW_STORE:
    case CS_ENCODING_ROW_STORE: {
      if (OB_UNLIKELY(nullptr != sstable_ && sstable_->is_multi_version_minor_sstable())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported multi version encode store type", K(ret), K(store_type));
      } else {
        if (CS_ENCODING_ROW_STORE == store_type) {
          INIT_MICRO_READER(cs_decoder_, ObMicroBlockCSDecoder);
          decoder_ = cs_decoder_;
        } else {
          INIT_MICRO_READER(pax_decoder_, ObMicroBlockDecoder);
          decoder_ = pax_decoder_;
        }
      }
      break;
    }

    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported row store type", K(ret), K(store_type));
  }
#undef INIT_MICRO_READER

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
int ObIMicroBlockRowScanner::apply_black_filter_batch(
    sql::ObPushdownFilterExecutor *parent,
    sql::ObBlackFilterExecutor &filter,
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
  if (ObIMicroBlockReader::Decoder == reader_->get_type() ||
      ObIMicroBlockReader::CSDecoder == reader_->get_type()) {
    if (decoder_->can_apply_black(col_offsets) &&
        OB_FAIL(decoder_->filter_black_filter_batch(
                parent,
                filter,
                pd_filter_info,
                result_bitmap,
                filter_applied_directly))) {
      LOG_WARN("Failed to execute black pushdown filter in decoder", K(ret));
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
                                             filter.get_filter_node().column_exprs_,
                                             nullptr))) {
          LOG_WARN("Failed to get rows for rich format", K(ret), K(cur_row_index));
        }
      } else if (OB_FAIL(get_rows_for_old_format(col_offsets,
                                                 filter.get_col_params(),
                                                 row_ids,
                                                 row_cap,
                                                 0,
                                                 pd_filter_info.cell_data_ptrs_,
                                                 filter.get_filter_node().column_exprs_,
                                                 datum_infos,
                                                 nullptr))) {
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
  LOG_TRACE("[PUSHDOWN] apply black filter batch", K(ret), K(filter_applied_directly),
            K(pd_filter_info.start_), K(pd_filter_info.count_),
            K(result_bitmap.popcnt()), K(result_bitmap.size()), K(filter));
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
    blocksstable::ObDatumRow *default_row)
{
  int ret = OB_SUCCESS;
  sql::ObEvalCtx &eval_ctx = param_->op_->get_eval_ctx();
  if (0 == vector_offset &&
      param_->use_uniform_format() &&
      OB_FAIL(init_exprs_uniform_header(&exprs, eval_ctx, eval_ctx.max_batch_size_))) {
    LOG_WARN("Fail to init uniform header", K(ret));
  } else if (ObIMicroBlockReader::Reader == reader_->get_type()) {
    ObMicroBlockReader *flat_reader = static_cast<ObMicroBlockReader *>(reader_);
    if (OB_FAIL(flat_reader->get_rows(col_offsets,
                                      col_params,
                                      default_row,
                                      row_ids,
                                      row_cap,
                                      row_,
                                      datum_infos,
                                      vector_offset,
                                      exprs,
                                      eval_ctx))) {
      LOG_WARN("Failed to copy rows", K(ret), K(row_cap),
               "row_ids", common::ObArrayWrap<const int32_t>(row_ids, row_cap));
    }
  } else if (ObIMicroBlockReader::Decoder == reader_->get_type() ||
             ObIMicroBlockReader::CSDecoder == reader_->get_type()) {
    ObIMicroBlockDecoder *decoder = static_cast<ObIMicroBlockDecoder *>(reader_);
    if (OB_FAIL(decoder->get_rows(col_offsets,
                                  col_params,
                                  row_ids,
                                  cell_datas,
                                  row_cap,
                                  datum_infos,
                                  vector_offset))) {
      LOG_WARN("Failed to get rows", K(ret), K(row_cap), K(vector_offset));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected micro block reader", K(ret), K(reader_->get_type()));
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
    blocksstable::ObDatumRow *default_row)
{
  int ret = OB_SUCCESS;
  sql::ObEvalCtx &eval_ctx = param_->op_->get_eval_ctx();
  if (ObIMicroBlockReader::Reader == reader_->get_type()) {
    ObMicroBlockReader *flat_reader = static_cast<ObMicroBlockReader *>(reader_);
    if (OB_FAIL(flat_reader->get_rows(col_offsets,
                                      col_params,
                                      default_row,
                                      row_ids,
                                      vector_offset,
                                      row_cap,
                                      row_,
                                      exprs,
                                      eval_ctx))) {
      LOG_WARN("Failed to copy rows", K(ret), K(row_cap),
               "row_ids", common::ObArrayWrap<const int32_t>(row_ids, row_cap));
    }
  } else if (ObIMicroBlockReader::Decoder == reader_->get_type() ||
             ObIMicroBlockReader::CSDecoder == reader_->get_type()) {
    ObIMicroBlockDecoder *decoder = static_cast<ObIMicroBlockDecoder *>(reader_);
    if (OB_FAIL(decoder->get_rows(col_offsets,
                                  col_params,
                                  row_ids,
                                  row_cap,
                                  cell_datas,
                                  vector_offset,
                                  len_array,
                                  eval_ctx,
                                  exprs))) {
      LOG_WARN("Failed to get rows", K(ret), K(row_cap));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected micro block reader", K(ret), K(reader_->get_type()));
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
  } else if (filter->is_sample_node()) {
    if (OB_FAIL(static_cast<ObSampleFilterExecutor *>(filter)->apply_sample_filter(
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
  } else if (ObIMicroBlockReader::Decoder == reader_->get_type() ||
             ObIMicroBlockReader::CSDecoder == reader_->get_type()) {
    if (param_->has_lob_column_out() && reader_->has_lob_out_row()) {
      sql::ObPhysicalFilterExecutor *physical_filter = static_cast<sql::ObPhysicalFilterExecutor *>(filter);
      if (OB_FAIL(decoder_->filter_pushdown_filter(parent,
                                                   *physical_filter,
                                                   pd_filter_info,
                                                   bitmap))) {
        LOG_WARN("Failed to execute pushdown filter", K(ret));
      }
    } else if (filter->is_filter_black_node()) {
      sql::ObBlackFilterExecutor *black_filter = static_cast<sql::ObBlackFilterExecutor *>(filter);
      if (can_use_vectorize && black_filter->can_vectorized()) {
        if (OB_FAIL(apply_black_filter_batch(
                    parent,
                    *black_filter,
                    pd_filter_info,
                    bitmap))) {
          LOG_WARN("Failed to execute black pushdown filter in batch", K(ret));
        }
      } else if (OB_FAIL(decoder_->filter_pushdown_filter(
                  parent,
                  *black_filter,
                  pd_filter_info,
                  bitmap))) {
        LOG_WARN("Failed to execute black pushdown filter", K(ret));
      }
    } else {
      if (OB_FAIL(decoder_->filter_pushdown_filter(
                  parent,
                  *static_cast<sql::ObWhiteFilterExecutor *>(filter),
                  pd_filter_info,
                  bitmap))) {
        LOG_WARN("Failed to execute white pushdown filter", K(ret));
      }
    }
  } else {
    if (OB_FAIL(flat_reader_->filter_pushdown_filter(
                parent,
                *filter,
                pd_filter_info,
                bitmap))) {
      LOG_WARN("Failed to execute black pushdown filter", K(ret));
    }
  }

  return ret;
}

int ObIMicroBlockRowScanner::filter_micro_block_in_blockscan(sql::PushdownFilterInfo &pd_filter_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == reader_ || nullptr == block_row_store_ || !pd_filter_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP_(reader), KP_(block_row_store), K(pd_filter_info));
  } else if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to judge end of micro scanner or not", K(ret), K_(current), K_(start), K_(last));
    }
  } else {
    if (!reverse_scan_) {
      pd_filter_info.start_ = current_;
    } else {
      pd_filter_info.start_ = last_;
    }
    pd_filter_info.count_ = get_access_cnt();
    pd_filter_info.is_pd_to_cg_ = false;
    pd_filter_info.param_ = param_;
    pd_filter_info.context_ = context_;
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
    int64_t end = reverse_scan_ ? current_ : last_;
    int64_t row_count = end - begin + 1;
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
    uint32_t *len_array)
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
                                         nullptr))) {
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
                                             nullptr))) {
    LOG_WARN("Failed to get rows for old format", K(ret));
  }
  return ret;
}

int ObIMicroBlockRowScanner::get_aggregate_result(
    const int32_t col_idx,
    const int32_t *row_ids,
    const int64_t row_cap,
    ObCGAggCells &cg_agg_cells)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP_(range), K_(is_inited));
  } else if (OB_FAIL(cg_agg_cells.process(*param_, *context_, col_idx, reader_, row_ids, row_cap))) {
    LOG_WARN("Fail to process agg cells", K(ret));
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
  if (param_->has_lob_column_out() && reader_->has_lob_out_row()) {
    can_group_by = false;
  } else if (OB_FAIL(reader_->get_distinct_count(group_by_col, distinct_cnt))) {
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
    storage::ObGroupByCell &group_by_cell) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(blocksstable::ObIMicroBlockReader::Decoder != reader_->get_type() &&
      blocksstable::ObIMicroBlockReader::CSDecoder != reader_->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected reader type", K(ret), K(reader_->get_type()));
  } else if (OB_FAIL((static_cast<blocksstable::ObIMicroBlockDecoder*>(reader_))->read_distinct(
      group_by_col, cell_datas, group_by_cell))) {
    LOG_WARN("Failed to read distinct", K(ret));
  }
  return ret;
}

int ObIMicroBlockRowScanner::read_reference(
    const int32_t group_by_col,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObGroupByCell &group_by_cell) const
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

////////////////////////////////// ObMicroBlockRowScannerV2 ////////////////////////////////////////////
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
  } else if (OB_NOT_NULL(block_row_store_) && block_row_store_->is_valid()) {
    // Storage pushdown filter is valid and reuse
    block_row_store_->reset_blockscan();
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

      if (est.physical_row_count_ > 0 && consider_multi_version) {
        const ObRowHeader *row_header;
        est.logical_row_count_ = 0;
        while (OB_SUCC(inner_get_row_header(row_header))) {
          if (row_header->get_row_multi_version_flag().is_first_multi_version_row()) {
            est.logical_row_count_ += row_header->get_row_flag().get_delta();
          }
        }
        if (OB_ITER_END == ret || OB_BEYOND_THE_RANGE == ret) {
          ret = OB_SUCCESS;
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
  reserved_pos_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
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
      // read_row_direct_flag_ can only be effective when read base version is zero
      // since we have no idea about the accurate base version of the macro block
      read_row_direct_flag_ = true;
      can_ignore_multi_version_ = flat_reader_->single_version_rows()
                                  && is_last_multi_version_row_
                                  && !reverse_scan_;
      LOG_DEBUG("use direct read", K(ret), K(can_ignore_multi_version_),
                KPC(block_data.get_micro_header()), K(context_->trans_version_range_));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(block_row_store_) && block_row_store_->is_valid()) {
      block_row_store_->reset_blockscan();
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockRowScanner::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  // TODO(yuanzhe) refactor blockscan opt of multi version sstable
  if (can_ignore_multi_version_) {
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
          if (OB_NOT_NULL(context_)) {
            ++context_->table_store_stat_.physical_read_cnt_;
          }
          if (have_uncommited_row) {
            row_.set_have_uncommited_row();
          }
        }
      }
      LOG_DEBUG("do compact", K(ret), K(current_), K(version_fit), K(final_result), K(finish_scanning_cur_rowkey_),
                "cur_row", is_row_empty(row_) ? "empty" : to_cstring(row_),
                "multi_version_row", to_cstring(multi_version_row), K_(macro_id));

      if ((OB_SUCC(ret) && final_result) || OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(cache_cur_micro_row(found_first_row, final_result))) {
          LOG_WARN("failed to cache cur micro row", K(ret), K_(macro_id));
        }
        LOG_DEBUG("cache cur micro row", K(ret), K(finish_scanning_cur_rowkey_),
                  "cur_row", is_row_empty(row_) ? "empty" : to_cstring(row_),
                  "prev_row", is_row_empty(prev_micro_row_) ? "empty" : to_cstring(prev_micro_row_),
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
            K(is_last_multi_version_row_));
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
    const ObDatumRow *&ret_row, bool &version_fit, bool &final_result, bool &have_uncommited_row)
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
    ObMultiVersionRowFlag flag;
    const ObRowHeader *row_header = nullptr;
    int64_t trans_version = 0;
    int64_t sql_sequence = 0;
    bool is_ghost_row_flag = false;
    const int64_t snapshot_version = context_->trans_version_range_.snapshot_version_;
    memtable::ObMvccAccessCtx &acc_ctx = context_->store_ctx_->mvcc_acc_ctx_;

    if (OB_FAIL(reader_->get_multi_version_info(
                current_,
                read_info_->get_schema_rowkey_count(),
                row_header,
                trans_version,
                sql_sequence))) {
      LOG_WARN("fail to get multi version info", K(ret), K(current_), KPC_(read_info),
               K(sql_sequence_col_idx_), K_(macro_id));
    } else if (OB_ISNULL(row_header)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("row header is null", K(ret));
    } else if (FALSE_IT(flag = row_header->get_row_multi_version_flag())) {
    } else if (OB_FAIL(ObGhostRowUtil::is_ghost_row(flag, is_ghost_row_flag))) {
      LOG_WARN("fail to check ghost row", K(ret), K_(current), KPC(row_header),
               K(trans_version), K(sql_sequence), K_(macro_id));
    } else {
      is_last_multi_version_row_ = flag.is_last_multi_version_row();
      final_result = is_last_multi_version_row_;

      if (OB_UNLIKELY(is_ghost_row_flag)) {
        // Case1: Data is ghost row, and it means no valid value for the row, so
        //        we can skip it
        version_fit = false;
        LOG_DEBUG("is ghost row", K(ret), K(current_), K(flag));
      } else if (flag.is_uncommitted_row()) {
        have_uncommited_row = true;  // TODO @lvling check transaction status instead
        transaction::ObTxSEQ tx_sequence = transaction::ObTxSEQ::cast_from_int(sql_sequence);
        // Case2: Data is uncommitted, so we use the txn state cache or txn
        //        table to decide whether uncommitted txns are readable
        compaction::ObMergeCachedTransState trans_state;
        if (OB_NOT_NULL(context_->trans_state_mgr_) &&
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
        } else {
          version_fit = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObStoreRowLockState lock_state;
      if (context_->query_flag_.is_for_foreign_key_check() &&
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
          int64_t rowkey_read_cnt = MIN(read_info_->get_seq_read_column_count(), read_info_->get_rowkey_count());
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
              row->storage_datums_[trans_idx].set_int(-trans_version);
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
        const int64_t end_cell_pos =
            row_.row_flag_.is_delete() ? read_info_->get_schema_rowkey_count() : row_.count_;
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
    LOG_WARN("failed to check transaction status", K(ret));
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
  if (REACH_TENANT_TIME_INTERVAL(30 * 1000 * 1000 /*30s*/)) {
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
      if (REACH_TENANT_TIME_INTERVAL(30 * 1000 * 1000 /*30s*/)) {
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

}
}
