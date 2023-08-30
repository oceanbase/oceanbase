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
#include "storage/access/ob_block_batched_row_store.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/blocksstable/ob_index_block_row_scanner.h"
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
    use_fuse_row_cache_(false),
    reverse_scan_(false),
    is_left_border_(false),
    is_right_border_(false),
    current_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    start_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    last_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    step_(1),
    macro_id_(),
    read_info_(nullptr),
    range_(nullptr),
    sstable_(nullptr),
    reader_(nullptr),
    flat_reader_(nullptr),
    decoder_(nullptr),
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
  if (nullptr != decoder_) {
    decoder_->~ObMicroBlockDecoder();
    allocator_.free(decoder_);
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
  tx_table_guard_.reuse();
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
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_.init(allocator_, param.get_max_out_col_cnt()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
    } else {
      param_ = &param;
      context_ = &context;
      sstable_ = sstable;
      range_ = nullptr;
      use_fuse_row_cache_ = context.use_fuse_row_cache_;
      if (NULL != reader_) {
        reader_->reset();
      }
      tx_table_guard_ = context.store_ctx_->mvcc_acc_ctx_.get_tx_table_guards();
      LOG_DEBUG("init ObIMicroBlockRowScanner", K(context), KPC_(read_info), K(param));
    }
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
  if (OB_UNLIKELY(!param.is_valid() ||
                  !context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), K(context));
  } else if (FALSE_IT(read_info_ = param.get_read_info(context.use_fuse_row_cache_))) {
  } else if (OB_UNLIKELY(nullptr == read_info_ || !read_info_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null columns info", K(ret), K(param), K(context.use_fuse_row_cache_), KPC_(read_info));
  } else if (use_fuse_row_cache_ == context.use_fuse_row_cache_) {
    // already set lob reader or using the same fuse cache mode
  } else if (OB_UNLIKELY(nullptr == sstable)) {
    ret = OB_ERR_SYS;
    LOG_WARN("Unexpected null table access param or sstable", K(ret), KP_(param), KP(sstable));
  }
  if (OB_SUCC(ret)) {
    param_ = &param;
    context_ = &context;
    sstable_ = sstable;
    use_fuse_row_cache_ = context.use_fuse_row_cache_;
    tx_table_guard_ = context.store_ctx_->mvcc_acc_ctx_.get_tx_table_guards();
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
  if (OB_UNLIKELY(!is_inited_ || nullptr == range_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP_(range), K_(is_inited));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !block_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_id), K(block_data));
  } else if (OB_FAIL(set_reader(block_data.get_store_type()))) {
    const ObMicroBlockHeader *micro_header = reinterpret_cast<const ObMicroBlockHeader *>(block_data.buf_);
    LOG_WARN("failed to set reader", K(ret), K(block_data), KPC(micro_header));
  } else if (OB_FAIL(reader_->init(block_data, *read_info_))) {
    LOG_WARN("failed to init micro block reader", K(ret), KPC(read_info_), K_(use_fuse_row_cache));
  } else {
    reverse_scan_ = context_->query_flag_.is_reverse_scan();
    is_left_border_ = is_left_border;
    is_right_border_ = is_right_border;
    macro_id_ = macro_id;
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
  const common::ObBitmap *bitmap = nullptr;
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
    } else if (OB_FAIL(block_row_store_->get_result_bitmap(bitmap))) {
      LOG_WARN("Failed to get pushdown filter result bitmap", K(ret));
    } else if (OB_FAIL(batch_store->fill_rows(
                range_->get_group_idx(),
                reader_,
                current_,
                last_ + step_,
                bitmap))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to apply rows", K(ret), K(current_), K(last_), K(step_),
                 KP(bitmap), KPC(sstable_));
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
    ++context_->table_store_stat_.logical_read_cnt_;
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
  const common::ObBitmap *result_bitmap = nullptr;
  if (OB_FAIL(block_row_store_->get_result_bitmap(result_bitmap))) {
    LOG_WARN("Failed to get pushdown filter result bitmap", K(ret));
  } else {
    bool readed = false;
    while (OB_SUCC(ret) && !readed) {
      if (OB_FAIL(end_of_block())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to judge end of block or not", K(ret));
        }
      } else if (nullptr == result_bitmap || result_bitmap->test(current_)) {
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
    storage::ObTableStoreStat &table_store_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == block_row_store || !block_row_store->is_valid() || nullptr == reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected block row store", K(ret), KPC(block_row_store_), KP_(reader));
  } else if (FALSE_IT(block_row_store_ = block_row_store)) {
  } else if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to judge end of block or not", K(ret), K_(macro_id), K_(start), K_(last), K_(current));
    }
  } else if (reader_->get_column_count() <= read_info_->get_max_col_index()) {
  } else if (OB_FAIL(block_row_store->apply_blockscan(
              *this,
              reader_->row_count(),
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
    case SELECTIVE_ENCODING_ROW_STORE: {
      if (OB_UNLIKELY(nullptr != sstable_ && sstable_->is_multi_version_minor_sstable())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported multi version encode store type", K(ret), K(store_type));
      } else {
        INIT_MICRO_READER(decoder_, ObMicroBlockDecoder);
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

int ObIMicroBlockRowScanner::filter_pushdown_filter(
    sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor *filter,
    storage::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &bitmap)
{
  int ret = OB_SUCCESS;
  if (!reverse_scan_) {
    pd_filter_info.start_ = current_;
    pd_filter_info.end_ = last_ + 1;
  } else {
    pd_filter_info.start_ = last_;
    pd_filter_info.end_ = current_ + 1;
  }
  if (OB_UNLIKELY(nullptr == reader_ || nullptr == block_row_store_ ||
                  nullptr == filter || !filter->is_filter_node())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(reader_), KP_(block_row_store), KPC(filter));
  } else if (ObIMicroBlockReader::Decoder == reader_->get_type()) {
    blocksstable::ObMicroBlockDecoder *decoder = static_cast<blocksstable::ObMicroBlockDecoder *>(reader_);
    if (filter->is_filter_black_node()) {
      sql::ObBlackFilterExecutor *black_filter = static_cast<sql::ObBlackFilterExecutor *>(filter);
      if (param_->vectorized_enabled_ && black_filter->can_vectorized() && block_row_store_->is_empty()) {
        if (OB_FAIL(block_row_store_->filter_micro_block_batch(
                    *decoder,
                    parent,
                    *black_filter,
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
        LOG_WARN("Failed to execute black pushdown filter", K(ret));
      }
    }
  } else {
    blocksstable::ObMicroBlockReader *flat_reader = static_cast<blocksstable::ObMicroBlockReader *>(reader_);
    if (OB_FAIL(flat_reader->filter_pushdown_filter(
                parent,
                *filter,
                pd_filter_info,
                bitmap))) {
      LOG_WARN("Failed to execute black pushdown filter", K(ret));
    }
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
  if (OB_LIKELY(use_fuse_row_cache_ == context.use_fuse_row_cache_)) {
    if (OB_FAIL(ObIMicroBlockRowScanner::switch_context(param, context, sstable))) {
      STORAGE_LOG(WARN, "Failed to switch context", K(ret));
    } else {
      version_range_ = context.trans_version_range_;
    }
  } else if (OB_UNLIKELY(nullptr == param_ || nullptr == sstable)) {
    ret = OB_ERR_SYS;
    LOG_WARN("Unexpected null table access param or sstable", K(ret), KP_(param), KP(sstable));
  } else {
    inner_reset();
    if (OB_FAIL(init(*param_, context, sstable))) {
      LOG_WARN("Failed to swith new table context", K(ret));
    }
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
    if (OB_FAIL(prev_micro_row_.init(*context.stmt_allocator_, cell_cnt_))) {
      STORAGE_LOG(WARN, "Failed to init cur_micro_row", K(ret), K_(cell_cnt));
    } else if (OB_FAIL(nop_pos_.init(*context.stmt_allocator_, cell_cnt_))) {
      LOG_WARN("failed to init nop_pos", K(ret), K(cell_cnt_));
    } else if (OB_FAIL(tmp_row_.init(*context.stmt_allocator_, cell_cnt_))) {
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
    if (OB_NOT_NULL(sstable_)
        && !block_data.get_micro_header()->contain_uncommitted_rows()
        && block_data.get_micro_header()->max_merged_trans_version_ <= context_->trans_version_range_.snapshot_version_
        && 0 == context_->trans_version_range_.base_version_) {
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
      if (OB_NOT_NULL(context_)) {
        ++context_->table_store_stat_.logical_read_cnt_;
      }
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
    int64_t trans_version = 0;
    const ObRowHeader *row_header = nullptr;
    int64_t sql_sequence = 0;
    bool can_read = true;
    bool is_determined_state = false;
    bool read_uncommitted_row = false;
    bool is_ghost_row_flag = false;
    const int64_t snapshot_version = context_->trans_version_range_.snapshot_version_;
    memtable::ObMvccAccessCtx &acc_ctx = context_->store_ctx_->mvcc_acc_ctx_;

    if (OB_UNLIKELY(context_->query_flag_.is_ignore_trans_stat())) {
      version_fit = true;
    } else if (OB_FAIL(reader_->get_multi_version_info(
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
    } else if (flag.is_uncommitted_row()) {
      have_uncommited_row = true;  // TODO @lvling check transaction status instead
      compaction::ObMergeCachedTransState trans_state;
      transaction::ObTxSEQ tx_sequence = transaction::ObTxSEQ::cast_from_int(sql_sequence);
      if (OB_NOT_NULL(context_->trans_state_mgr_) &&
        OB_SUCCESS == context_->trans_state_mgr_->get_trans_state(
          transaction::ObTransID(row_header->get_trans_id()), tx_sequence, trans_state)) {
        can_read = trans_state.can_read_;
        trans_version = trans_state.trans_version_;
        is_determined_state = trans_state.is_determined_state_;
      } else {
        transaction::ObLockForReadArg lock_for_read_arg(acc_ctx,
                                                        transaction::ObTransID(row_header->get_trans_id()),
                                                        tx_sequence,
                                                        context_->query_flag_.read_latest_,
                                                        sstable_->get_end_scn());

        if (OB_FAIL(lock_for_read(lock_for_read_arg,
                                  can_read,
                                  trans_version,
                                  is_determined_state))) {
          STORAGE_LOG(WARN, "fail to check transaction status", K(ret), KPC(row_header), K_(macro_id));
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

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObGhostRowUtil::is_ghost_row(flag, is_ghost_row_flag))) {
      LOG_WARN("fail to check ghost row", K(ret), K_(current), KPC(row_header),
               K(trans_version), K(sql_sequence), K_(macro_id));
    } else if (OB_UNLIKELY(is_ghost_row_flag)) {
      can_read = false;
      is_determined_state = true;
      LOG_DEBUG("is ghost row", K(ret), K(current_), K(flag));
    }

    if (OB_SUCC(ret)) {
      is_last_multi_version_row_ = flag.is_last_multi_version_row();
      final_result = is_last_multi_version_row_;
      if (OB_UNLIKELY(context_->query_flag_.is_ignore_trans_stat())) {
        // do nothing
      } else if (!can_read) {
        if (!is_determined_state && context_->query_flag_.iter_uncommitted_row()) { // for mark deletion
          version_fit = true;
          read_uncommitted_row = true;
        } else {
          version_fit = false;
        }
      } else if (!flag.is_uncommitted_row() || is_determined_state) { // committed
        if (trans_version <= version_range_.base_version_) {
          version_fit = false;
          // filter multi version row whose trans version is smaller than base_version
          final_result = true;
        } else if (trans_version > snapshot_version) { // filter multi version row whose trans version is larger than snapshot_version
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
          if (OB_FAIL(ObLockRowChecker::check_lock_row_valid(
                      *row,
                      *read_info_))) {
            LOG_WARN("micro block reader fail to get block_row", K(ret), K(current_), KPC(row), KPC_(read_info));
          } else if (row->is_uncommitted_row()) {
            version_fit = false;
            row->row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
          }
        }
        if (OB_SUCC(ret) && version_fit) {
          if (OB_INVALID_INDEX != read_info_->get_trans_col_index() && is_determined_state) {
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
            if (!row->mvcc_row_flag_.is_uncommitted_row() || is_determined_state) {
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
    int64_t &trans_version,
    bool &is_determined_state)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SCN scn_trans_version = SCN::invalid_scn();
  auto &tx_table_guards = context_->store_ctx_->mvcc_acc_ctx_.get_tx_table_guards();

  if (OB_FAIL(tx_table_guards.lock_for_read(lock_for_read_arg, can_read, scn_trans_version,
      is_determined_state))) {
    LOG_WARN("failed to check transaction status", K(ret));
  } else {
    trans_version = scn_trans_version.get_val_for_tx();
    if (OB_NOT_NULL(context_->trans_state_mgr_) &&
      OB_TMP_FAIL(context_->trans_state_mgr_->add_trans_state(
        lock_for_read_arg.data_trans_id_, lock_for_read_arg.data_sql_sequence_,
        trans_version, ObTxData::MAX_STATE_CNT, can_read, is_determined_state))) {
      LOG_WARN("failed to add trans state to cache", K(tmp_ret),
        "trans_id", lock_for_read_arg.data_trans_id_,
        "sql_seq", lock_for_read_arg.data_sql_sequence_);
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

////////////////////////////// ObMultiVersionMicroBlockMinorMergeRowScannerV2 //////////////////////////////
void ObMultiVersionMicroBlockMinorMergeRowScanner::reuse()
{
  row_allocator_.reuse(); // clear row memory
  row_queue_.reuse();
  is_row_queue_ready_ = false;
  first_rowkey_flag_ = true;
  row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  clear_scan_status();
  ObIMicroBlockRowScanner::reuse();
}

void ObMultiVersionMicroBlockMinorMergeRowScanner::clear_scan_status()
{
  scan_state_ = SCAN_START;
  committed_trans_version_ = INT64_MAX;
  is_last_multi_version_row_ = false;
  have_output_row_flag_ = false;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::init_row_queue(const int64_t row_col_cnt)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  for (int i = 0; OB_SUCC(ret) && i < COMPACT_MAX_ROW; ++i) { // init nop pos
    if (nullptr == (ptr = context_->stmt_allocator_->alloc(sizeof(storage::ObNopPos)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc nop pos");
    } else {
      nop_pos_[i] = new (ptr) storage::ObNopPos();
      if (OB_FAIL(nop_pos_[i]->init(*context_->stmt_allocator_, OB_ROW_MAX_COLUMNS_COUNT))) {
        LOG_WARN("failed to init first row nop pos", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_queue_.init(row_col_cnt))) {
      LOG_WARN("failed to init row_queue", K(ret), K(row_col_cnt), K_(macro_id));
    }
  }
  return ret;
}

// The scanner of the same sstable is shared, the previous state needs to be kept, so clear_status cannot be called
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
    if (OB_FAIL(init_row_queue(read_info_->get_request_count()))) {
      LOG_WARN("Fail to init row queue", K(ret), K(read_info_->get_request_count()));
    } else {
      is_inited_ = true;
    }
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
    if (OB_FAIL(reader_->init(block_data, *read_info_))) {
      LOG_WARN("failed to init micro block reader", K(ret));
    } else if (OB_FAIL(set_base_scan_param(is_left_border, is_right_border))) {
      LOG_WARN("failed to set base scan param", K(ret), K(is_left_border), K(is_right_border), K_(macro_id));
    } else if (OB_FAIL(row_.reserve(col_count))) {
      STORAGE_LOG(WARN, "Failed to reserve datum row", K(ret), K(col_count));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::locate_last_committed_row()
{
  int ret = OB_SUCCESS;
  const ObRowHeader *row_header = nullptr;
  while (OB_SUCC(ret) && !is_last_multi_version_row_) { // move forward to find last row of this rowkey
    if (OB_FAIL(end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to check end of block", K(ret));
      } else if (is_right_border_) {
        ret = OB_SUCCESS;
        LOG_DEBUG("find last micro of this macro block during locate last row", KPC(this));
      }
      break;
    } else if (OB_FAIL(reader_->get_row_header(current_, row_header))) {
      LOG_WARN("failed to get row header", K(ret), K(current_), K_(macro_id));
    } else {
      ++current_;
      is_last_multi_version_row_ = row_header->get_row_multi_version_flag().is_last_multi_version_row();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!row_queue_.has_next())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected", K(ret), KPC(this));
    } else {
      if(is_last_multi_version_row_) {
        row_queue_.get_last()->set_last_multi_version_row();
      }
      scan_state_ = GET_ROW_FROM_ROW_QUEUE;
    }
  }
  LOG_DEBUG("locate last multi version row", K(ret), KPC(this));
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!row_queue_.has_next()) { // empty row queue
    if (is_last_multi_version_row_) {
      clear_scan_status(); // clear transaction related infomation
      first_rowkey_flag_ = false;
    }
    clear_row_queue_status(); // clear row queue
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
      LOG_WARN("not support scan status", K(ret));
      break;
    }
    } // end of switch
  } // end of while

  if (OB_FAIL(ret)) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("switch status to call func error", K(ret), K(scan_state_));
    }
  } else if (OB_UNLIKELY(OB_ISNULL(row))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("row must not null", K(ret));
  } else if (OB_UNLIKELY(!row->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("row is invalid", KPC(row), K(scan_state_));
  } else if (row->row_flag_.is_delete() && !row->mvcc_row_flag_.is_uncommitted_row()) {
    // set delete/insert committed row compacted
    (const_cast<ObDatumRow *>(row))->set_compacted_multi_version_row();
  }
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(row) && row->is_shadow_row() && row->is_last_multi_version_row()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected last shadow row", K(ret), KPC(row), KPC(this));
    } else {
      have_output_row_flag_ = true;
    }
    LOG_DEBUG("output one row", K(ret), KPC(row));
  }

  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::prepare_committed_row_queue(
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  bool add_row_queue_flag = false;
  while (OB_SUCC(ret) && PREPARE_COMMITTED_ROW_QUEUE == scan_state_ && !is_last_multi_version_row_) {
    if (OB_FAIL(read_committed_row(add_row_queue_flag, row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to read committed row", K(ret), K(row_));
      } else if (is_right_border_) {
        ret = OB_SUCCESS;
      }
      break;
    } else if (add_row_queue_flag && OB_FAIL(add_row_into_row_queue(row))) {
      LOG_WARN("failed to add row into row queue", K(ret), KPC(this));
    }
  }

  //  get one row from row_queue to return
  if (OB_SUCC(ret) && OB_ISNULL(row)) {
    if (is_last_multi_version_row_) {
      if (!row_queue_.is_empty()) {
        row_queue_.get_last()->set_last_multi_version_row();
      }
    }
    if (PREPARE_COMMITTED_ROW_QUEUE == scan_state_) {
      scan_state_ = GET_ROW_FROM_ROW_QUEUE;
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::get_row_from_row_queue(
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (!row_queue_.has_next()) {
    ret = OB_ITER_END;
    scan_state_ = SCAN_START;
    LOG_DEBUG("row queue is empty", K(ret), KPC(this));
  } else if (OB_FAIL(row_queue_.get_next_row(row))) {
    LOG_WARN("failed to get next row from prepared row queue", K(ret), KPC(this));
  } else {
    LOG_DEBUG("get row from row queue", K(ret), KPC(this), KPC(row));
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
// At this time, if the scan snapshot version is smaller than the snapshot_version of the sstable, an error may be reported.
// For full scan (only logical migration will come):
// The data of the entire sstable uses the same scanner, and the status is inherited.
// At this time, scenarios where the scan snapshot version is smaller than the snapshot version of the sstable are allowed.

// The first row flag will only be used for the following scenarios:
// 1. This row is first row
// 2. The first row of the rowkey has been encountered during scan.
// The compact row flag will only be used for the following scenarios:
// 1. This row is compact row
// 2. The first row or last row no longer contains the nop column
// 3. First row or last row has been fuse with compact row.
// 4. First row or last row has been fuse with last row. (Last row itself is compact, but there may not necessarily be a compact flag)
// 5. Last row was scanned, and last row needs to be compact (set for compatibility with 2.1, in fact, for last row, setting compact is meaningless)
// 6. After scanning the last row, you need to set the compact flag for the first row
// The last row flag will only be set for the following scenarios:
// 1. This row is last row
// 2. The last row of the same rowkey has been scanned.

void ObMultiVersionMicroBlockMinorMergeRowScanner::clear_row_queue_status()
{
  LOG_DEBUG("clear status about row queue");
  if (is_row_queue_ready_) {
    row_queue_.reuse();
    row_allocator_.reuse(); // clear row memory
    for (int i = 0; i < COMPACT_MAX_ROW; ++i) {
      if (OB_NOT_NULL(nop_pos_[i])) {
        nop_pos_[i]->reset();
      }
    }
  } else {
    LOG_DEBUG("don't clear_row_queue_status", K_(scan_state), K_(is_row_queue_ready),
              K_(is_last_multi_version_row), K(row_queue_.count()));
  }
  is_row_queue_ready_ = false;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::read_committed_row(
    bool &add_row_queue_flag,
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  add_row_queue_flag = true;
  bool is_ghost_row_flag = false;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to check end of block", K(ret));
    }
  } else if (OB_FAIL(reader_->get_row(current_, row_))) {
    LOG_WARN("micro block reader fail to get row.", K(ret), K_(macro_id));
  } else if (OB_FAIL(ObGhostRowUtil::is_ghost_row(row_.mvcc_row_flag_, is_ghost_row_flag))) {
    LOG_WARN("failed to check ghost row", K(ret), K_(row));
  } else if (OB_UNLIKELY(is_ghost_row_flag)) {
    add_row_queue_flag = false;
    ++current_;
  } else {
    ++current_;
    if (context_->query_flag_.is_sstable_cut() && OB_FAIL(filter_unneeded_row(
        add_row_queue_flag, is_ghost_row_flag))) {
      LOG_WARN("failed to filter unneeded row", K(ret), K_(row), K_(macro_id));
    }
  }
  if (OB_SUCC(ret) && row_.mvcc_row_flag_.is_last_multi_version_row()) { // meet last row
    is_last_multi_version_row_ = true;

    if (row_queue_.is_empty()) {
      if (!is_ghost_row_flag || first_rowkey_flag_ || have_output_row_flag_) {
        // three conditions need to output this row
        // 1. is not ghost row
        // 2. this ghost row is first rowkey in this macro block
        // 3. have output row of this rowkey before this ghost row
        row = &row_;
      } else {
        // meet not-output ghost row
        clear_scan_status();
      }
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::meet_uncommitted_last_row(
    const bool can_read,
    const ObDatumRow *&row)
{
  is_last_multi_version_row_ = true;
  int ret = OB_SUCCESS;
  if (row_queue_.is_empty()) { // need output a uncommitted row with Last Flag
    if (!can_read) {
      if (OB_FAIL(ObGhostRowUtil::make_ghost_row(sql_sequence_col_idx_, context_->query_flag_, row_))) {
        LOG_WARN("failed to make ghost row", K(ret), K(row_));
      }
    } else if (COMPACT_COMMIT_TRANS_ROW == scan_state_) {
      row_.mvcc_row_flag_.set_uncommitted_row(false);
      row_.trans_id_.reset();
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("meet_uncommitted_last_row", K(ret), K(row_));
      row = &row_; // return this row
    }
  } else if (FILTER_ABORT_TRANS_ROW == scan_state_ || !can_read) { // only set Last row flag
    // 1)there are rows in row queue
    // 2)current row can't be read(transaction abort OR sql_seq rollback)
    // if is abort trans, rows in row_queue in the same transaction
    row_queue_.get_last()->set_last_multi_version_row();
    scan_state_ = GET_ROW_FROM_ROW_QUEUE;
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::read_uncommitted_row(
    bool &can_read, const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRowHeader *row_header = nullptr;
  ObMultiVersionRowFlag flag;
  int64_t trans_version = 0;
  int64_t sql_sequence = 0;
  row = nullptr;
  can_read = false;
  if (OB_FAIL(reader_->get_multi_version_info(
              current_,
              read_info_->get_schema_rowkey_count(),
              row_header,
              trans_version,
              sql_sequence))) {
    LOG_WARN("fail to get multi version info", K(ret), K(current_),
             KPC_(read_info), K_(macro_id));
  } else if (OB_ISNULL(row_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("row header is null", K(ret));
  } else if (FALSE_IT(flag = row_header->get_row_multi_version_flag())) {
  } else if (FALSE_IT(read_trans_id_ = row_header->get_trans_id())) {
  } else if (flag.is_uncommitted_row()) { // uncommitted row
    bool read_row_flag = false;
    transaction::ObTxSEQ tx_sequence = transaction::ObTxSEQ::cast_from_int(sql_sequence);
    if (OB_UNLIKELY(read_trans_id_ != last_trans_id_)) { // another uncommitted trans
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected meet multi uncommitted trans in sstable", K(ret),
               K_(read_trans_id), K_(last_trans_id), KPC(this));
    } else if (FILTER_ABORT_TRANS_ROW != scan_state_
               && OB_FAIL(check_curr_row_can_read(read_trans_id_, tx_sequence, can_read))) { // same uncommitted trans
      LOG_WARN("micro block reader fail to get row.", K(ret), K_(macro_id));
    } else if (!can_read && !flag.is_last_multi_version_row()) { // can't read && not last row
      ++current_; // move forward
      read_row_flag = false;
    } else {
      read_row_flag = true;
    }
    if (OB_SUCC(ret) && read_row_flag) {
      if (OB_FAIL(reader_->get_row(current_, row_))) { // read row
        LOG_WARN("micro block reader fail to get row.", K(ret));
      } else if (row_.row_flag_.is_lock()) { // is a lock row
        if (OB_FAIL(ObLockRowChecker::check_lock_row_valid(
            row_,
            sql_sequence_col_idx_ + 1,
            false/*is_memtable_iter_row_check*/))) {
          LOG_WARN("failed to check lock row valid", K(ret), K_(row), K_(macro_id));
        } else {
          if (GET_RUNNING_TRANS_ROW != scan_state_) {
            can_read = false;
          }
          ++current_;
        }
      } else if (OB_FAIL(set_trans_version_for_uncommitted_row(row_))) {
        LOG_WARN("failed to set trans_version for uncommitted row", K(ret), K_(row));
      } else {
        ++current_;
      }
      if (OB_SUCC(ret) && SCAN_START != scan_state_ && flag.is_last_multi_version_row()) {
        meet_uncommitted_last_row(can_read, row);
        LOG_DEBUG("meet last row", K(ret), K_(row), K(scan_state_), K(can_read));
      }
    }
    LOG_DEBUG("cxf debug read_uncommitted_row ", K(ret), K(sql_sequence), K_(row), K(scan_state_), K(can_read));
  } else { // committed row
    scan_state_ = PREPARE_COMMITTED_ROW_QUEUE;
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::add_row_into_row_queue(
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObVersionRange &version_range = context_->trans_version_range_;
  const int64_t trans_version = -row_.storage_datums_[trans_version_col_idx_].get_int();

  LOG_DEBUG("prepare row queue, get row", K(ret), KPC(this), K(row_), K(trans_version), K(version_range));
  if (trans_version <= version_range.base_version_) {  // too old data
    LOG_DEBUG("find too old row", "count", row_queue_.count(), K_(row));
  }

  if (is_last_multi_version_row_ && row_queue_.is_empty()) { // return this row
    row = &row_;
  } else {
    is_row_queue_ready_ = true; // set flag to call clear_row_queue_status
    // Be careful to use compact_first_row and compact_last_row
    // first_crow_nop_pos_ and last_crow_nop_pos_ inherit the state of the previous row
    // if a row have compacted to first row, it can't compact to last row
    const bool first_row_flag = row_queue_.is_empty() ? true : false;
    // TODO: @dengzhi.ldz
    // 1. No need to compact to first if trans_version is less than recycle version;
    // 2. Skip the old shadow row and regenerated the shadow row every time in case that row is recycled.
    if (row_.is_shadow_row() && trans_version <= version_range.multi_version_start_) {
      if (OB_FAIL(compact_shadow_row_to_last())) {
        LOG_WARN("Fail to compact shadow row to last", K(ret), K_(scan_state), K_(row));
      }
    } else if (OB_FAIL(compact_first_row())) {
      LOG_WARN("failed to compact first row", K(ret));
    } else if (first_row_flag || row_.is_shadow_row()) {
      // skip shadow row if uncommitted committed
    } else {
      if (trans_version > version_range.multi_version_start_) {
        if (OB_FAIL(row_queue_.add_row(row_, row_allocator_))) { // no need to compact
          LOG_WARN("failed to add row queue", K(ret), K(row_), K(row_queue_));
        } else {
          LOG_DEBUG("succeed to add row", K(row_));
        }
      } else { // trans_version <= version_range.multi_version_start_
        const int64_t last_row_trans_version =
            -row_queue_.get_last()->storage_datums_[trans_version_col_idx_].get_int();
        if (last_row_trans_version > version_range.multi_version_start_) {
          if (OB_FAIL(row_queue_.add_empty_row(row_allocator_))) {
            LOG_WARN("failed to add row queue for first need compact last row", K(ret), K(row_), K(row_queue_));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(compact_last_row())) {
            LOG_WARN("failed to compact last row", K(ret));
          } else if (row_queue_.get_last()->is_compacted_multi_version_row()) {
            // last row is compacted, could just filter the rest rows of current rowkey
            scan_state_ = LOCATE_LAST_COMMITTED_ROW;
            LOG_DEBUG("success to compact last row", K(ret), K(row_));
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_shadow_row_to_last()
{
  int ret = OB_SUCCESS;
  const int64_t row_trans_version = -row_.storage_datums_[trans_version_col_idx_].get_int();
  const int64_t multi_version_start = context_->trans_version_range_.multi_version_start_;
  if (OB_UNLIKELY(!(row_.is_shadow_row() && row_trans_version <= multi_version_start))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K_(row), K(multi_version_start));
  } else {
    row_.mvcc_row_flag_.set_shadow_row(false);
    row_.storage_datums_[sql_sequence_col_idx_].reuse();
    row_.storage_datums_[sql_sequence_col_idx_].set_int(0);

    // 1. compact to first row
    if (OB_FAIL(compact_first_row())) {
      LOG_WARN("Fail to compact to first row", K(ret), K_(scan_state), K_(row));
    }
    // 2. compact to last if last row greater than multi_version_start
    if (OB_SUCC(ret) &&
        -row_queue_.get_last()->storage_datums_[trans_version_col_idx_].get_int() > multi_version_start &&
        !(OB_ITER_END == end_of_block() && is_right_border_)) {
      // TODO: @dengzhi.ldz issue #44375086, skip all the rows in the following macro block when shadow row recycled
      if (OB_FAIL(row_queue_.add_empty_row(row_allocator_))) {
        LOG_WARN("failed to add row queue for first need compact last row", K(ret), K(row_), K(row_queue_));
      } else if (OB_FAIL(compact_last_row())) {
        LOG_WARN("failed to compact last row", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    scan_state_ = LOCATE_LAST_COMMITTED_ROW;
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_row(
    const ObDatumRow &former,
    const int64_t row_compact_info_index,
    ObDatumRow &result)
{
  int ret = OB_SUCCESS;
  bool final_result = false;
  if (OB_UNLIKELY(row_compact_info_index < 0 || row_compact_info_index >= COMPACT_MAX_ROW)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row compact info index", K(ret), K(row_compact_info_index), K(former));
  } else if (OB_UNLIKELY(OB_ISNULL(nop_pos_[row_compact_info_index]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nop pos is null", K(ret), K(row_compact_info_index));
  } else if (OB_FAIL(fuse_row(
              former, result, *nop_pos_[row_compact_info_index], final_result, &row_allocator_))) {
    LOG_WARN("failed to fuse row", K(ret));
  } else {
    if (final_result) {
      result.set_compacted_multi_version_row();
    }
    if (former.is_shadow_row()) {
      result.set_shadow_row();
    }
    LOG_DEBUG("set compact row", K(ret), K(final_result), K(former), K(result));
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_trans_row_into_row_queue()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_queue_.count() > 1 ||
                  (row_queue_.count() == 1 &&
                   -committed_trans_version_ != row_queue_.get_last()->storage_datums_[trans_version_col_idx_].get_int()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected commit row", K(ret), K_(committed_trans_version), KPC(row_queue_.get_last()));
  } else if (row_queue_.is_empty() && OB_FAIL(row_queue_.add_empty_row(row_allocator_))) {
    LOG_WARN("failed to add empty row", K(ret));
  } else {
    ObDatumRow *first_row = row_queue_.get_first();
    if (!first_row->is_compacted_multi_version_row()
        && OB_FAIL(compact_row(row_, COMPACT_FIRST_ROW, *first_row))) {
      LOG_WARN("failed to compact first row", K(ret), KPC(first_row), K(row_));
    } else {
      first_row->row_flag_.fuse_flag(row_.row_flag_);
      if (row_.is_last_multi_version_row()) {
        row_queue_.get_last()->set_last_multi_version_row();
      }
    }
  }
  LOG_DEBUG("compact trans row", K(ret), K(row_queue_.count()), KPC(row_queue_.get_first()), K_(row));
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_first_row()
{
  int ret = OB_SUCCESS;
  if (row_queue_.is_empty()) {
    // push an empty row for compact first row
    if (OB_FAIL(row_queue_.add_empty_row(row_allocator_))) {
      LOG_WARN("failed to add row", K(ret));
    }
  } else {
    ObDatumRow *first_row = row_queue_.get_first();
    if (!first_row->is_shadow_row() &&
        -first_row->storage_datums_[trans_version_col_idx_].get_int() > context_->trans_version_range_.multi_version_start_) {
      if (OB_UNLIKELY(1 != row_queue_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected row queue", K(ret), KPC(this));
      } else {
        // 1. copy first row
        if (OB_FAIL(row_queue_.add_row(*first_row, row_allocator_))) {
          LOG_WARN("failed to add row queue", K(ret), KPC(first_row), K(row_queue_));
          // 2. make the first row as shadow row
        } else {
          first_row->storage_datums_[sql_sequence_col_idx_].reuse();
          first_row->storage_datums_[sql_sequence_col_idx_].set_int(-INT64_MAX);
          first_row->set_shadow_row();
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObDatumRow *first_row = row_queue_.get_first();
    if (!first_row->is_compacted_multi_version_row()
        && OB_FAIL(compact_row(row_, COMPACT_FIRST_ROW, *first_row))) {
      LOG_WARN("failed to compact first row", K(ret), KPC(first_row), K(row_));
    } else {
      first_row->row_flag_.fuse_flag(row_.row_flag_);
      LOG_DEBUG("success to compact first row", K(ret), K(row_), KPC(first_row));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_last_row()
{
  int ret = OB_SUCCESS;
  if (row_queue_.count() > 1) { // When there is only one row, compact_first_row has been fuse
    ObDatumRow *last_row = row_queue_.get_last();
    if (!last_row->is_compacted_multi_version_row() &&
        OB_FAIL(compact_row(row_, COMPACT_LAST_ROW, *last_row))) {
      LOG_WARN("failed to compact last row", K(ret), KPC(last_row), K(row_));
    } else {
      LOG_DEBUG("success to compact last row", K(ret), KPC(last_row), K(row_));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::find_uncommitted_row()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  last_trans_id_.reset();
  last_trans_state_ = INT64_MAX;
  if (OB_UNLIKELY(OB_ISNULL(reader_) || SCAN_START != scan_state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader is null OR scan state is wrong", K(ret), K(reader_), K(scan_state_));
  } else {
    if (OB_FAIL(end_of_block())) { // do nothing
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_DEBUG("find uncommitted row failed", K(ret));
      }
    } else {
      const ObRowHeader *row_header = nullptr;
      ObMultiVersionRowFlag flag;
      int64_t trans_version = 0;
      int64_t sql_sequence = 0;
      if (OB_FAIL(reader_->get_multi_version_info(
                  current_,
                  read_info_->get_schema_rowkey_count(),
                  row_header,
                  trans_version,
                  sql_sequence))) {
        LOG_WARN("fail to get multi version info", K(ret), K(current_),
                 KPC_(read_info), K_(macro_id));
      } else if (OB_ISNULL(row_header)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("row header is null", K(ret));
      } else if (FALSE_IT(flag = row_header->get_row_multi_version_flag())) {
      } else if (FALSE_IT(last_trans_id_ = row_header->get_trans_id())) {
      } else if (flag.is_uncommitted_row()) { // uncommitted
        //get trans status & committed_trans_version_
        int64_t state;
        int64_t commit_trans_version = INT64_MAX;
        compaction::ObMergeCachedTransState trans_state;
        transaction::ObTxSEQ tx_sequence = transaction::ObTxSEQ::cast_from_int(sql_sequence);
        if (OB_NOT_NULL(context_->trans_state_mgr_) &&
          OB_SUCCESS == context_->trans_state_mgr_->get_trans_state(last_trans_id_, tx_sequence, trans_state)) {
          state = trans_state.trans_state_;
          last_trans_state_ = trans_state.trans_state_;
          commit_trans_version = trans_state.trans_version_;
        } else if (OB_FAIL(get_trans_state(last_trans_id_, state, commit_trans_version))) {
          LOG_WARN("get transaction status failed", K(ret), K(last_trans_id_), K(state));
        }
        if (OB_SUCC(ret) && OB_FAIL(judge_trans_state(state, commit_trans_version))) {
          LOG_WARN("failed to judge transaction status", K(ret), K(last_trans_id_),
                   K(state));
        }
      } else { // no uncommitted rows
        scan_state_ = PREPARE_COMMITTED_ROW_QUEUE;
      }
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::get_trans_state(const transaction::ObTransID &trans_id,
                                                                  int64_t &state,
                                                                  int64_t &commit_trans_version)
{
  int ret = OB_SUCCESS;
  // get trans status & committed_trans_version_
  SCN scn_commit_trans_version = SCN::max_scn();
  auto &tx_table_guards = context_->store_ctx_->mvcc_acc_ctx_.get_tx_table_guards();
  if (OB_FAIL(tx_table_guards.get_tx_state_with_scn(
      trans_id, context_->merge_scn_, state, scn_commit_trans_version))) {
    LOG_WARN("get transaction status failed", K(ret), K(trans_id), K(state));
  } else {
    commit_trans_version = scn_commit_trans_version.get_val_for_tx();
    last_trans_state_ = state;
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::judge_trans_state(
    const int64_t status,
    const int64_t commit_trans_version)
{
  int ret = OB_SUCCESS;
  const common::ObVersionRange &version_range = context_->trans_version_range_;

  switch (status) {
  case ObTxData::COMMIT: {
    if (context_->query_flag_.is_sstable_cut()
        && commit_trans_version > version_range.snapshot_version_) {
      scan_state_ = FILTER_ABORT_TRANS_ROW;
    } else {
      scan_state_ = COMPACT_COMMIT_TRANS_ROW;
      committed_trans_version_ = commit_trans_version;
    }
    break;
  }
  case ObTxData::RUNNING: {
    scan_state_ = GET_RUNNING_TRANS_ROW;
    committed_trans_version_ = commit_trans_version;
    break;
  }
  case ObTxData::ABORT: {
    scan_state_ = FILTER_ABORT_TRANS_ROW;
    break;
  }
  default:
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("wrong transaction status", K(ret), K(commit_trans_version));
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::filter_abort_trans_row(
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  bool can_read = false;
  while (OB_SUCC(ret) && FILTER_ABORT_TRANS_ROW == scan_state_ && !is_last_multi_version_row_) {
    if (OB_FAIL(end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to reach end of block", K(ret));
      }
    } else if (OB_FAIL(read_uncommitted_row(can_read, row))) {
      LOG_WARN("failed to read uncommitted row", K(ret));
    }
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::compact_commit_trans_row(
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  bool can_read = false;
  while (OB_SUCC(ret) && COMPACT_COMMIT_TRANS_ROW == scan_state_ && !is_last_multi_version_row_) {
    if (OB_FAIL(end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to reach end of block", K(ret));
      }
    } else if (OB_FAIL(read_uncommitted_row(can_read, row))) {
      LOG_WARN("failed to read uncommitted row", K(ret));
    } else if (COMPACT_COMMIT_TRANS_ROW == scan_state_
        && OB_ISNULL(row)
        && can_read) {
      if (OB_FAIL(compact_trans_row_into_row_queue())) {
        LOG_WARN("Failed to compact trans row", K(ret));
      } else {
        is_row_queue_ready_ = true;
      }
    }
  } // end of while

  if ((OB_SUCC(ret) || (OB_ITER_END == ret && is_right_border_))
      && COMPACT_COMMIT_TRANS_ROW == scan_state_ && row_queue_.has_next()) {
    scan_state_ = GET_ROW_FROM_ROW_QUEUE;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::get_running_trans_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  bool can_read = false;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to check end of block", K(ret));
    }
  } else if (OB_FAIL(read_uncommitted_row(can_read, row))) {
    LOG_WARN("failed to read uncommitted row", K(ret));
  } else if (GET_RUNNING_TRANS_ROW == scan_state_ && can_read && OB_ISNULL(row)) {
    row = &row_;
    LOG_DEBUG("get uncommitted row.", K(ret), KPC(row));
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::set_trans_version_for_uncommitted_row(
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t row_trans_version = -row.storage_datums_[trans_version_col_idx_].get_int();
  if (INT64_MAX == committed_trans_version_
      && INT64_MAX != row_trans_version
      && FILTER_ABORT_TRANS_ROW != scan_state_) {
    // If the transaction is aborted, the current trans_version is MAX,
    // but the transaction may have been filled with prepare_version
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to set trans_version for uncommitted row", K(ret), K(row_),
        K_(committed_trans_version));
  } else if (row_trans_version > committed_trans_version_) {
    row.storage_datums_[trans_version_col_idx_].reuse();
    row.storage_datums_[trans_version_col_idx_].set_int(-committed_trans_version_);
  }
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::check_curr_row_can_read(
    const transaction::ObTransID &trans_id,
    const transaction::ObTxSEQ &sql_seq,
    bool &can_read)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_cached = false;
  can_read = false;
  compaction::ObMergeCachedTransState trans_state;
  if (OB_NOT_NULL(context_->trans_state_mgr_) &&
    OB_SUCCESS == context_->trans_state_mgr_->get_trans_state(trans_id, sql_seq, trans_state)) {
    can_read = trans_state.can_read_;
  } else {
    storage::ObTxTableGuards tx_table_guards = context_->store_ctx_->mvcc_acc_ctx_.get_tx_table_guards();
    if (OB_FAIL(tx_table_guards.check_sql_sequence_can_read(
            trans_id,
            sql_seq,
            sstable_->get_end_scn(),
            can_read))) {
      LOG_WARN("check sql sequence can read failed", K(ret), K(can_read), K(trans_id), K(sql_seq));
    } else if (OB_NOT_NULL(context_->trans_state_mgr_) &&
      OB_TMP_FAIL(context_->trans_state_mgr_->add_trans_state(trans_id, sql_seq,
        committed_trans_version_, last_trans_state_, can_read, 0))) {
      LOG_WARN("failed to add minor trans state", K(tmp_ret), K(trans_id), K(sql_seq), K(can_read));
    }
  }
  LOG_DEBUG("cxf debug check sql sequence can read", K(ret), K(can_read), K(trans_id), K(sql_seq));
  return ret;
}

int ObMultiVersionMicroBlockMinorMergeRowScanner::filter_unneeded_row(
    bool &add_row_queue_flag,
    bool &is_ghost_row_flag)
{
  int ret = OB_SUCCESS;
  add_row_queue_flag = true;
  is_ghost_row_flag = false;
  const common::ObVersionRange &version_range = context_->trans_version_range_;
  const int64_t trans_version = -row_.storage_datums_[trans_version_col_idx_].get_int();
  if (trans_version > version_range.snapshot_version_) {
    add_row_queue_flag = false;
    LOG_DEBUG("filter unneeded row", K(row_), K(version_range), KPC(context_));

    if (OB_SUCC(ret) && row_.mvcc_row_flag_.is_last_multi_version_row()) { // meet last row
      if (row_queue_.is_empty()) {
        //if row has been filtered and has output row or first rowkey flag is true, need output ghost row
        //else need clean scan status
        if (have_output_row_flag_ || first_rowkey_flag_) {
          if (OB_FAIL(ObGhostRowUtil::make_ghost_row(sql_sequence_col_idx_, context_->query_flag_, row_))) {
            LOG_WARN("failed to make ghost row", K(ret), K(row_));
          }
        } else {
          //no need output this row, make clear scan status condition
          is_ghost_row_flag = true;
        }
      }
    }
  }
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
