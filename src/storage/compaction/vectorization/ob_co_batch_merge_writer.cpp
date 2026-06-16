/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_co_batch_merge_writer.h"
#include "storage/column_store/ob_co_merge_ctx.h"

namespace oceanbase
{
namespace compaction
{
// -------------------------------- ObCOBatchMergeRowWriter -------------------------------- //

ObCOBatchMergeRowWriter::~ObCOBatchMergeRowWriter()
{
  stores_.reset();
}

int ObCOBatchMergeRowWriter::inner_init(ObBasicTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObMergeVectorStoreLayoutParam *layout_param = nullptr;
  ObCOTabletMergeCtx *co_ctx = nullptr;
  if (OB_ISNULL(co_ctx = static_cast<ObCOTabletMergeCtx *>(&ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null co merge ctx", K(ret), KP(co_ctx));
  } else if (OB_FAIL(co_ctx->get_cg_layout_param(cg_idx_, layout_param))) {
    LOG_WARN("failed to get cg layout param", K(ret), K(cg_idx_));
  } else if (OB_ISNULL(layout_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null layout param", K(ret), K(cg_idx_));
  } else if (OB_FAIL(stores_.init(*layout_param,
                                  write_helper_.get_data_store_desc().get_col_desc_array(),
                                  &default_row_))) {
    LOG_WARN("failed to init stores with cg layout param", K(ret), K(cg_idx_), KPC(layout_param));
  } else if (OB_FAIL(filter_handle_.init(ctx.get_compaction_filter(), ctx.get_filter_col_idxs()))) {
    LOG_WARN("failed to init filter handle", K(ret));
  }
  return ret;
}

int ObCOBatchMergeRowWriter::end_write(ObCOTabletMergeCtx &co_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCOMergeRowWriter::end_write(co_ctx))) {
    LOG_WARN("failed to end write", K(ret));
  } else if (cg_idx_ == co_ctx.base_rowkey_cg_idx_) {
    co_ctx.collect_filter_statistics(filter_handle_.filter_statistics_); // only a cg do this
  }
  return ret;
}

int ObCOBatchMergeRowWriter::flush_pending_buffered_rows()
{
  int ret = OB_SUCCESS;
  ObMergeVectorStore &write_store = stores_.write_store();
  if (!write_store.is_empty() && OB_FAIL(process_batch_rows())) {
    LOG_WARN("failed to process batch rows", K(ret));
  }
  return ret;
}

int ObCOBatchMergeRowWriter::replay_batch_mergelog(
    const ObMergeLog &mergelog,
    const ObMergeVectorStore &vector_store,
    const bool need_check_project,
    const bool need_check_filter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOBatchMergeRowWriter not init", K(ret));
  } else if (OB_UNLIKELY(!mergelog.is_valid() || ObMergeLog::INSERT != mergelog.op_ || 0 >= vector_store.get_row_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge log or vector store", K(ret), K(mergelog), K(vector_store));
  } else {
    ObMergeVectorStore &write_store = stores_.write_store();
    common::ObArrayWrap<uint16_t> cols(write_helper_.get_projector(), write_helper_.get_projector_count());
    int64_t batch_idx = 0;
    while (OB_SUCC(ret) && batch_idx < vector_store.get_row_count()) {
      int tmp_ret = write_store.append_batch(vector_store, batch_idx,
          need_check_project && write_helper_.need_project() ? &cols : nullptr,
          need_check_filter ? &filter_handle_ : nullptr);
      if (OB_SUCCESS == tmp_ret) {
      } else if (OB_BUF_NOT_ENOUGH != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to append batch to write store", K(ret), K(batch_idx), K(vector_store), K(write_store));
      } else if (OB_FAIL(process_batch_rows())) {
        LOG_WARN("failed to process batch rows", K(ret), K(write_store));
      }
    }
  }
  return ret;
}

int ObCOBatchMergeRowWriter::replay_range_mergelog(const ObMergeLog &mergelog)
{
  int ret = OB_SUCCESS;
  ObCOMajorMergeIter *merge_iter = nullptr;
  ObMergeIter *iter = nullptr;
  if (OB_FAIL(get_curr_major_iter(mergelog, merge_iter))) {
    LOG_WARN("failed to get curr major iter", K(ret), K(mergelog));
  } else if (FALSE_IT(iter = nullptr != merge_iter ? merge_iter->iter_ : nullptr)) {
  } else if (nullptr == iter || iter->is_iter_end()) { // do nothing
  } else if (!iter->can_batch_scan_by_rowid() || ObMergeLog::DELETE_RANGE == mergelog.op_) {
    // For DELETE_RANGE the iterator just needs to be advanced past the filtered rows;
    // no data needs to be read into a vector store, so fall back to the row-by-row path.
    if (OB_FAIL(ObCOMergeWriter::replay_range_mergelog(mergelog))) {
      LOG_WARN("failed to replay merge log", K(ret), K(mergelog));
    }
  } else {
    bool reach_border = false;
    bool reuse_curr_range = false;
    bool need_move_next = true;
    ObMergeVectorStore &read_store = stores_.read_store();
    ObMergeVectorStore &write_store = stores_.write_store();
    common::ObArrayWrap<uint16_t> cols(write_helper_.get_projector(), write_helper_.get_projector_count());
    while (OB_SUCC(ret) && !reach_border) {
      read_store.reuse();
      if (OB_FAIL(iter->get_next_batch_rows(
              ObMergeBatchBorder::make_row_id(mergelog.row_id_),
              read_store,
              reach_border,
              reuse_curr_range,
              need_move_next,
              merge_iter->need_project_ ? &cols : nullptr))) {
        LOG_WARN("failed to get next batch rows", K(ret), K(mergelog), K(read_store));
      } else {
        if (0 < read_store.get_row_count()) {
          ObMergeLog batch_log(ObMergeLog::INSERT, -1/*major_idx*/, INT64_MAX/*row_id*/);
          // batch row from major is projected
          if (OB_FAIL(replay_batch_mergelog(batch_log, read_store,
                read_store.has_single_row() && merge_iter->need_project_, false/*need_check_filter*/))) {
            LOG_WARN("failed to replay merge log", K(ret), K(batch_log), K(read_store));
          }
        }
        if (OB_SUCC(ret) && reuse_curr_range) {
          // to loop again because when curr_range_end_rowid == border_row_id, the range maybe opened by reorg/rewrite
          reach_border = false;
          // TODO: The rows read when opening a macroblock by rewrite/reorg will not be grouped together with the previous rows now.
          if (OB_FAIL(append_iter_curr_row_or_range(merge_iter, mergelog.op_))) {
            LOG_WARN("failed to append curr range", K(ret), KPC(iter));
          }
        }
        if (OB_SUCC(ret) && need_move_next && OB_FAIL(merge_iter->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            reach_border = true;
          } else {
            LOG_WARN("failed to move next", K(ret), KPC(iter));
          }
        }
      }
    }
  }
  return ret;
}

int ObCOBatchMergeRowWriter::process_batch_rows()
{
  int ret = OB_SUCCESS;
  blocksstable::ObBatchDatumRows batch_rows;
  ObMergeVectorStore &write_store = stores_.write_store();
  if (OB_FAIL(write_store.get_batch_datum_rows(batch_rows))) {
    LOG_WARN("failed to get batch datum rows from write store", K(ret), K(write_store));
  } else if (OB_UNLIKELY(batch_rows.row_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty batch rows", K(ret), K(batch_rows));
  } else if (OB_FAIL(write_helper_.append_batch(batch_rows))) {
    LOG_WARN("failed to append batch", K(ret), K(batch_rows), K(write_helper_));
  } else {
    write_store.reuse();
  }
  return ret;
}

int ObCOBatchMergeRowWriter::process(
    ObCOMajorMergeIter *iter,
    const ObMacroBlockDesc &macro_desc,
    const ObMicroBlockData *micro_block_data)
{
  int ret = OB_SUCCESS;
  ObMergeVectorStore &write_store = stores_.write_store();
  if (!write_store.is_empty() && OB_FAIL(process_batch_rows())) {
    LOG_WARN("failed to process batch rows", K(ret), K(write_store));
  } else if (OB_FAIL(ObCOMergeRowWriter::process(iter, macro_desc, micro_block_data))) {
    LOG_WARN("failed to process", K(ret), K(macro_desc), K(micro_block_data));
  }
  return ret;
}

int ObCOBatchMergeRowWriter::process(
    const blocksstable::ObMicroBlock &micro_block,
    const int64_t sstable_idx,
    ObMergeVectorStore *read_vector_store)
{
  int ret = OB_SUCCESS;
  ObMergeVectorStore &write_store = stores_.write_store();
  if (!write_store.is_empty() && OB_FAIL(process_batch_rows())) {
    LOG_WARN("failed to process batch rows", K(ret), K(write_store));
  } else if (OB_FAIL(ObCOMergeRowWriter::process(micro_block, sstable_idx, read_vector_store))) {
    LOG_WARN("failed to process", K(ret), K(micro_block), K(sstable_idx), K(read_vector_store));
  }
  return ret;
}

int ObCOBatchMergeRowWriter::process(const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row.is_valid() || row.row_flag_.is_delete() || row.row_flag_.is_not_exist())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row", K(ret), K(row));
  } else {
    ObMergeVectorStore &write_store = stores_.write_store();
    int tmp_ret = OB_SUCCESS;
    while (OB_SUCC(ret) && OB_SUCCESS != (tmp_ret = write_store.append_row(row))) {
      if (OB_BUF_NOT_ENOUGH == tmp_ret) {
        if (OB_FAIL(process_batch_rows())) {
          LOG_WARN("failed to process batch rows", K(ret), K(write_store));
        }
      } else {
        ret = tmp_ret;
        LOG_WARN("failed to append row", K(ret), K(row), K(write_store));
      }
    }
  }
  return ret;
}

// -------------------------------- ObCOBatchMergeBaseRowWriter -------------------------------- //
int ObCOBatchMergeBaseRowWriter::replay_single_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row)
{
  return replay_row_directly(mergelog, row, merge_iter_);
}

// TODO: batch scan base cg sstable.
int ObCOBatchMergeBaseRowWriter::replay_range_mergelog(const ObMergeLog &mergelog)
{
  return replay_iter_directly(mergelog, merge_iter_);
}
}
}