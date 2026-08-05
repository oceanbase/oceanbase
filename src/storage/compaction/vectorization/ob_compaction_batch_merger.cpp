/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_compaction_batch_merger.h"
#include "storage/column_store/ob_co_merge_ctx.h"

namespace oceanbase
{
namespace compaction
{
// -------------------------------- ObPartitionMajorBatchMerger -------------------------------- //
ObPartitionMajorBatchMerger::ObPartitionMajorBatchMerger(
  compaction::ObLocalArena &allocator,
  const ObStaticMergeParam &static_param)
  : ObPartitionMajorMerger(allocator, static_param),
    stores_()
{}

ObPartitionMajorBatchMerger::~ObPartitionMajorBatchMerger()
{
  reset();
}

void ObPartitionMajorBatchMerger::reset()
{
  stores_.reset();
  ObPartitionMajorMerger::reset();
}

int ObPartitionMajorBatchMerger::inner_init()
{
  int ret = OB_SUCCESS;
  const ObMergeVectorStoreLayoutParam *layout_param = nullptr;
  if (OB_FAIL(ObPartitionMajorMerger::inner_init())) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_ISNULL(partition_fuser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null partition fuser", K(ret));
  } else if (OB_ISNULL(layout_param = merge_ctx_->row_store_layout_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null layout param", K(ret), K(data_store_desc_));
  } else if (OB_FAIL(stores_.init(*layout_param,
                                  data_store_desc_.get_col_desc_array(),
                                  partition_fuser_->get_default_row_ptr()))) {
    LOG_WARN("failed to init batch vector stores", K(ret));
  }
  return ret;
}

int ObPartitionMajorBatchMerger::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_write_store())) {
    LOG_WARN("failed to flush write store on close", K(ret));
  } else if (OB_FAIL(ObPartitionMajorMerger::inner_close())) {
    LOG_WARN("failed to inner close partition merger", K(ret));
  }
  return ret;
}

int ObPartitionMajorBatchMerger::flush_write_store()
{
  int ret = OB_SUCCESS;
  ObMergeVectorStore &write_store = stores_.write_store();
  if (!write_store.is_empty()) {
    blocksstable::ObBatchDatumRows batch_rows;
    if (OB_FAIL(write_store.get_batch_datum_rows(batch_rows))) {
      LOG_WARN("failed to get batch datum rows from write store", K(ret), K(write_store));
    } else if (OB_UNLIKELY(batch_rows.row_count_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty batch rows", K(ret), K(batch_rows));
    } else if (OB_FAIL(macro_writer_->append_batch(batch_rows))) {
      LOG_WARN("failed to append batch to macro writer", K(ret), K(batch_rows));
    } else {
      macro_writer_->add_incremental_row_count(write_store.get_incremental_row_count());
      write_store.reuse();
    }
  }
  return ret;
}

int ObPartitionMajorBatchMerger::flush_read_store(const bool is_incremental_row)
{
  int ret = OB_SUCCESS;
  ObMergeVectorStore &read_store = stores_.read_store();
  ObMergeVectorStore &write_store = stores_.write_store();
  int64_t batch_idx = 0;
  while (OB_SUCC(ret) && batch_idx < read_store.get_row_count()) {
    int tmp_ret = write_store.append_batch(read_store, batch_idx, nullptr, &filter_handle_, is_incremental_row);
    if (OB_SUCCESS == tmp_ret) {
    } else if (OB_BUF_NOT_ENOUGH != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to append batch to write store", K(ret), K(batch_idx), K(read_store), K(write_store));
    } else if (OB_FAIL(flush_write_store())) {
      LOG_WARN("failed to process batch rows", K(ret), K(write_store));
    }
  }
  if (OB_SUCC(ret)) {
    read_store.reuse();
  }
  return ret;
}

int ObPartitionMajorBatchMerger::process(const blocksstable::ObMicroBlock &micro_block, const int64_t sstable_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_write_store())) {
    LOG_WARN("failed to flush write store", K(ret));
  } else if (OB_FAIL(process_micro_block(micro_block, sstable_idx, &stores_.read_store()))) {
    LOG_WARN("failed to process micro block", K(ret));
  }
  return ret;
}

int ObPartitionMajorBatchMerger::process(
    const blocksstable::ObMacroBlockDesc &macro_meta,
    const ObMicroBlockData *micro_block_data,
    const int64_t sstable_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_write_store())) {
    LOG_WARN("failed to flush write store", K(ret));
  } else if (OB_FAIL(ObPartitionMajorMerger::process(macro_meta, micro_block_data, sstable_idx))) {
    LOG_WARN("failed to process macro block", K(ret));
  }
  return ret;
}

int ObPartitionMajorBatchMerger::inner_process(const blocksstable::ObDatumRow &row, const bool is_incremental_row)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  if (data_store_desc_.get_row_column_count() > data_store_desc_.get_rowkey_column_count()) {
    write_wrong_row(data_store_desc_.get_tablet_id(), row);
  }
#endif
  ObMergeVectorStore &write_store = stores_.write_store();
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_TMP_FAIL(write_store.append_row(row, true/*need_check_order*/, nullptr/*cols*/, is_incremental_row))) {
    if (OB_BUF_NOT_ENOUGH == tmp_ret) {
      if (OB_FAIL(flush_write_store())) {
        LOG_WARN("failed to flush read store", K(ret));
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("failed to append row", K(ret), K(row), K(write_store));
    }
  }
  return ret;
}

int ObPartitionMajorBatchMerger::merge_batch_rows(MERGE_ITER_ARRAY &merge_iters)
{
  int ret = OB_SUCCESS;
  ObPartitionMergeIter *iter = nullptr;
  const ObDatumRow *border_row = nullptr;
  blocksstable::ObDatumRowkey border_rowkey;
  if (OB_UNLIKELY(1 != merge_iters.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected merge iters count", K(ret), K(merge_iters));
  } else if (OB_ISNULL(iter = merge_iters.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null iter", K(ret));
  } else if (OB_FAIL(merge_helper_->get_next_row(border_row))) {
    LOG_WARN("failed to get next row", K(ret));
  } else if (nullptr == border_row) {
    border_rowkey.set_max_rowkey();
  } else if (OB_FAIL(border_rowkey.assign(border_row->storage_datums_, merge_ctx_->read_info_.get_schema_rowkey_count()))) {
    LOG_WARN("failed to assign border rowkey", K(ret));
  }

  if (OB_SUCC(ret)) {
    bool reach_border = false;
    bool need_move_next = true;
    bool reuse_curr_range = false;
    const bool is_incremental_row = !iter->is_major_sstable_iter();
    ObMergeVectorStore &read_store = stores_.read_store();
    // consume first row without compare
    const ObDatumRow *row = nullptr;
    if (OB_ISNULL(row = iter->get_curr_row())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get curr row", K(ret));
    } else if (OB_FAIL(ObPartitionMerger::process(*row, is_incremental_row))) {
      LOG_WARN("failed to process single row", K(ret), K(*row));
    } else {
      iter->set_curr_row_returned_in_batch();
    }
    // consume next rows
    while (OB_SUCC(ret) && !reach_border) {
      read_store.reuse();
      if (OB_FAIL(iter->get_next_batch_rows(ObMergeBatchBorder::make_rowkey(border_rowkey),
                                            read_store,
                                            reach_border,
                                            reuse_curr_range,
                                            need_move_next))) {
        LOG_WARN("failed to get next batch rows", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (0 < read_store.get_row_count()) {
          if (OB_FAIL(flush_read_store(is_incremental_row))) {
            LOG_WARN("failed to flush read store", K(ret), K(read_store));
          }
        }
        if (OB_SUCC(ret) && reuse_curr_range) {
          if (OB_FAIL(try_reuse_range(*iter))) {
            LOG_WARN("failed to try reuse range", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && need_move_next && OB_FAIL(iter->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          reach_border = true;
        } else {
          LOG_WARN("failed to move next", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("failed to merge batch rows", K(ret), K(border_rowkey));
    }
  }
  return ret;
}
// -------------------------------- ObCOBatchMergeLogBuilder -------------------------------- //
void ObCOBatchMergeLogBuilder::reset()
{
  set_need_move_flag(MoveNextOp::DO_NOTHING, MoveNextOp::DO_NOTHING);
  batch_scan_iter_ = nullptr;
  border_rowkey_.reset();
  read_store_.reset();
  ObCOMergeLogBuilder::reset();
  LOG_INFO("ObCOBatchMergeLogBuilder reset", K_(batch_row_count), K_(time_guard));
  batch_row_count_ = 0;
}

int ObCOBatchMergeLogBuilder::init(ObBasicTabletMergeCtx &ctx, const int64_t idx, const int64_t cg_idx)
{
  int ret = OB_SUCCESS;
  const ObMergeVectorStoreLayoutParam *layout_param = nullptr;
  ObSEArray<share::schema::ObColDesc, 16> full_row_col_descs;
  full_row_col_descs.set_attr(ObMemAttr(MTL_ID(), "FullColDes"));
  if (OB_FAIL(ObCOMergeLogBuilder::init(ctx, idx, cg_idx))) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(ctx.get_schema()->get_multi_version_column_descs(full_row_col_descs))) {
    LOG_WARN("failed to get full row column descs", K(ret));
  } else if (OB_ISNULL(layout_param = ctx.row_store_layout_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null layout param", K(ret));
  } else if (OB_FAIL(read_store_.init(*layout_param,
                                      full_row_col_descs,
                                      false/*not continuous*/,
                                      get_full_default_row()))) {
    LOG_WARN("failed to init vector store", K(ret));
  }
  return ret;
}

int ObCOBatchMergeLogBuilder::get_next_batch_log(
    ObMergeLog &mergelog,
    const ObMergeVectorStore *&vector_store)
{
  int ret = OB_SUCCESS;
  bool reach_border = false;
  bool need_move_next = false;
  bool reuse_curr_range = false;
  read_store_.reuse();
  time_guard_.set_last_click_ts(common::ObTimeUtility::current_time());
  // do not move next after create merge log, cause single_row is from iter curr_row_
  if (MoveNextOp::NEED_MOVE_NEXT == need_move_minor_iter_ && OB_FAIL(batch_scan_iter_->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to move next", K(ret));
    } else {
      reach_border = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(batch_scan_iter_->get_next_batch_rows(ObMergeBatchBorder::make_rowkey(border_rowkey_),
                                                           read_store_,
                                                           reach_border,
                                                           reuse_curr_range,
                                                           need_move_next))) {
    LOG_WARN("failed to get next batch rows", K(ret));
  }
  time_guard_.click(ObCOMergeTimeGuard::BATCH_MOVE_NEXT);
  if (OB_SUCC(ret)) {
    if (0 < read_store_.get_row_count()) {
      vector_store = &read_store_;
      mergelog.set_value(ObMergeLog::INSERT, -1/*major_idx*/, INT64_MAX/*row_id*/);
      batch_row_count_ += read_store_.get_row_count();
    }
    const blocksstable::ObDatumRow *row = nullptr;
    if (OB_FAIL(replay_base_cg(mergelog, vector_store, row, nullptr/*iter*/))) {
      LOG_WARN("failed to replay merge log", K(ret), K(mergelog), K(vector_store), K(row));
    } else if (FALSE_IT(time_guard_.click(ObCOMergeTimeGuard::REPLAY_BASE_CG))) {
    } else if (reach_border) {
      border_rowkey_.reset();
      batch_scan_iter_ = nullptr;
      need_move_minor_iter_ = need_move_next ? MoveNextOp::NEED_MOVE_NEXT : MoveNextOp::ONLY_REBUILD;
    } else {
      need_move_minor_iter_ = need_move_next ? MoveNextOp::NEED_MOVE_NEXT : MoveNextOp::DO_NOTHING;
    }
  }
  return ret;
}

int ObCOBatchMergeLogBuilder::calculate_border_rowkey(
    ObPartitionMergeIter *batch_scan_minor_iter,
    ObPartitionMergeHelper *minor_merge_helper,
    ObPartitionMergeIter *major_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == batch_scan_minor_iter || nullptr == minor_merge_helper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null batch scan minor iter or minor merge helper",
        K(ret), KP(batch_scan_minor_iter), KP(minor_merge_helper));
  } else {
    const ObDatumRow *next_competing_minor_row = nullptr;
    const ObDatumRow *border_row = nullptr;

    // The batch-scan minor iterator has already been removed from the minor helper's loser tree.
    // This row is the minimum current row among the remaining minor iterators; it does
    // not belong to batch_scan_minor_iter, and observing it does not advance that iterator.
    if (OB_FAIL(minor_merge_helper->get_next_row(next_competing_minor_row))) {
      LOG_WARN("failed to get next competing minor row", K(ret));
    } else if (nullptr == next_competing_minor_row && nullptr == major_iter) {
      // border_rowkey is max rowkey
    } else if (nullptr == major_iter) {
      // No major iterator, use the next competing minor row if available.
      border_row = next_competing_minor_row;
    } else {
      // Both a competing minor row and a major iterator may constrain the batch border.
      const ObDatumRow *major_row = nullptr;
      int64_t compare_ret = 0;
      if (OB_ISNULL(major_row = major_iter->get_curr_row())) {
        if (OB_FAIL(major_iter->get_curr_range_first_row(major_row))) {
          LOG_WARN("failed to get current major row", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (nullptr == next_competing_minor_row) {
          // No other minor iterator is competing, use the major current row.
          border_row = major_row;
        } else {
          if (OB_FAIL(cmp_->compare_rowkey(*next_competing_minor_row, *major_row, compare_ret))) {
            LOG_WARN("failed to compare rowkey", K(ret), K(*next_competing_minor_row), K(*major_row));
          } else {
            border_row = (compare_ret < 0) ? next_competing_minor_row : major_row;
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      // Do nothing if error occurred previously
    } else if (nullptr == border_row) {
      border_rowkey_.set_max_rowkey(); // we trust that iter has been clamped by merge_range
    } else if (OB_FAIL(border_rowkey_.assign(border_row->storage_datums_, merge_ctx_->read_info_.get_schema_rowkey_count()))) {
      LOG_WARN("failed to assign border key", K(ret));
    }

    if (OB_SUCC(ret)) {
      batch_scan_iter_ = batch_scan_minor_iter;
    }
  }
  return ret;
}

int ObCOBatchMergeLogBuilder::inner_get_next_log(ObMergeLog &mergelog, const ObMergeVectorStore *&vector_store, const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  bool unused_is_single_iter_end = false;
  mergelog.reset();
  vector_store = nullptr;
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogBuilder not init", K(ret));
  } else if (can_batch_scan()) {
    if (OB_FAIL(get_next_batch_log(mergelog, vector_store))) {
      LOG_WARN("failed to get next batch log", K(ret));
    }
  } else {
    time_guard_.set_last_click_ts(common::ObTimeUtility::current_time());
    int64_t cmp_ret = 0;
    ObMergeLog::OpType op;
    ObCOMinorBatchMergeIter *incre_iter = static_cast<ObCOMinorBatchMergeIter*>(merge_helper_);
    ObPartitionMergeIter *major_iter = nullptr;
    // move iter next first to prevent inc row changes,
    // cause fuser will not deep copy inc row
    if (OB_FAIL(move_iters_next(unused_is_single_iter_end))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to move incre iter next", K(ret));
      }
    }
    time_guard_.click(ObCOMergeTimeGuard::MOVE_NEXT);
    // decide merge log
    if (OB_SUCC(ret)) {
      bool need_get_incre_row = true;
      bool need_get_major_iter = true;
      if (incre_iter->is_iter_end()) {
        cmp_ret = 1;
        need_get_incre_row = false;
      } else if (majors_merge_iter_->is_iter_end()) {
        cmp_ret = -1;
        need_get_major_iter = false;
      }
      if (need_get_incre_row && OB_FAIL(incre_iter->get_curr_row(row))) {
        LOG_WARN("failed to get curr row", K(ret), K(row));
      } else if (need_get_major_iter && OB_FAIL(majors_merge_iter_->get_current_major_iter(major_iter))) {
        LOG_WARN("failed to get current major iter", K(ret));
      } else if (need_get_incre_row && need_get_major_iter && OB_FAIL(compare(*row, *major_iter, cmp_ret))) {
        LOG_WARN("failed to compare iter", K(ret), K(*row), KPC(major_iter));
      }
    }
    time_guard_.click(ObCOMergeTimeGuard::COMPARE);
    if (OB_FAIL(ret)) {
    } else if (cmp_ret == 0) {
      op = row->row_flag_.is_delete() ? ObMergeLog::DELETE : ObMergeLog::UPDATE;
      set_need_move_flag(MoveNextOp::NEED_MOVE_NEXT/*need_move_minor_iter*/, MoveNextOp::NEED_MOVE_NEXT/*need_move_major_iter*/);
    } else if (cmp_ret < 0) { // inc row < major row/range
      op = ObMergeLog::INSERT;
      if (incre_iter->can_batch_scan()) {
        if (OB_FAIL(calculate_border_rowkey(incre_iter->get_top_iter(), incre_iter, major_iter))) {
          LOG_WARN("failed to try do batch scan", K(ret));
        } else {
          set_need_move_flag(MoveNextOp::DO_NOTHING/*need_move_minor_iter*/, MoveNextOp::DO_NOTHING/*need_move_major_iter*/);
        }
      } else {
        set_need_move_flag(MoveNextOp::NEED_MOVE_NEXT/*need_move_minor_iter*/, MoveNextOp::DO_NOTHING/*need_move_major_iter*/);
      }
    } else { // major row/range < inc row
      op = ObMergeLog::REPLAY;
      set_need_move_flag(MoveNextOp::DO_NOTHING/*need_move_minor_iter*/, MoveNextOp::NEED_MOVE_NEXT/*need_move_major_iter*/);
    }
    // build merge log
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_merge_log(op, major_iter, row, mergelog, false/*replay_to_end*/))) {
      LOG_WARN("failed to build merge log", K(ret), K(op));
    } else if (can_batch_scan()) {
      batch_scan_iter_->set_curr_row_returned_in_batch();
    }
  }
  return ret;
}

int ObCOBatchMergeLogBuilder::alloc_base_writer(ObIArray<ObITable*> &tables)
{
  return inner_alloc_base_writer<ObCOBatchMergeBaseRowWriter>(tables);
}
// -------------------------------- ObCOBatchMergeLogReplayer -------------------------------- //
int ObCOBatchMergeLogReplayer::alloc_writers(
    const blocksstable::ObDatumRow &default_row,
    ObTabletMergeInfo **merge_infos,
    ObIArray<ObITable*> &tables)
{
  return alloc_row_writers<ObCOBatchMergeRowWriter>(default_row, merge_infos, tables);
}
}
}
