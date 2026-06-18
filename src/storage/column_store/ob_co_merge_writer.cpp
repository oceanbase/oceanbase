/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "ob_co_merge_writer.h"
#include "ob_co_merge_ctx.h"

namespace oceanbase
{
namespace compaction
{
/**
 * ---------------------------------------------------------ObWriteHelper--------------------------------------------------------------
 */
int ObWriteHelper::init_data_store_desc(
    ObBasicTabletMergeCtx &ctx,
    const ObMergeParameter &merge_param,
    ObTabletMergeInfo &merge_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDataDescHelper::build(merge_param, merge_info, data_store_desc_))) {
    STORAGE_LOG(WARN, "fail to build data desc", K(ret));
  } else if (OB_FAIL(ObSSTableMergeHistory::init_sstable_merge_block_info_array(
        ctx.get_major_sstable_count(), sstable_merge_block_info_array_))) {
    STORAGE_LOG(WARN, "failed to init sstable merge block info array", K(ret));
  }
  return ret;
}

int ObWriteHelper::open_macro_writer(
    ObBasicTabletMergeCtx &ctx,
    const int64_t parallel_idx,
    const ObStorageColumnGroupSchema &cg_schema,
    ObIAllocator &allocator,
    const ObITableReadInfo *merge_micro_block_read_info)
{
  int ret = OB_SUCCESS;
  int64_t macro_start_seq = 0;
  ObMacroSeqParam macro_seq_param;
  macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  ObISSTableObjectCleaner *object_cleaner = nullptr;
  if (OB_FAIL(ctx.generate_macro_seq_info(parallel_idx, macro_start_seq))) {
    LOG_WARN("failed to generate macro seq info for cur merge task", K(ret), K(parallel_idx), K(ctx));
  } else if (FALSE_IT(macro_seq_param.start_ = macro_start_seq)) {
  } else if (OB_FAIL(ObISSTableObjectCleaner::get_cleaner_from_data_store_desc(
                                                        data_store_desc_, object_cleaner))) {
    LOG_WARN("failed to get cleaner from data store desc", K(ret), K(data_store_desc_), KP(object_cleaner));
  } else if (OB_FAIL(macro_writer_.open(
                 data_store_desc_, parallel_idx, macro_seq_param,
                 ctx.get_pre_warm_param(),
                 *object_cleaner,
                 nullptr /*callback*/,
                 nullptr /*validator*/, // TODO: remove ss related parameters
                 nullptr /*device_handle*/,
                 merge_micro_block_read_info))) {
    STORAGE_LOG(WARN, "Failed to open macro writer",
                K(ret), K(parallel_idx), K(data_store_desc_), KPC(object_cleaner), KP(merge_micro_block_read_info));
  } else if (cg_schema.is_all_column_group()) {
    skip_project_ = true;
  } else if (OB_FAIL(projector_.init(cg_schema, allocator))) {
    STORAGE_LOG(WARN, "fail to init project", K(ret), K(cg_schema));
  } else {
    LOG_INFO("success to open macro writer with pre warmer", KR(ret), K(data_store_desc_),
      K(macro_writer_), K(ctx.get_pre_warm_param()), K(cg_schema), KP(merge_micro_block_read_info));
  }
  return ret;
}

int ObWriteHelper::init(
    ObBasicTabletMergeCtx &ctx,
    const ObMergeParameter &merge_param,
    const int64_t parallel_idx,
    const ObStorageColumnGroupSchema &cg_schema,
    ObTabletMergeInfo &merge_info,
    ObIAllocator &allocator,
    const ObITableReadInfo *merge_micro_block_read_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_data_store_desc(ctx, merge_param, merge_info))) {
    STORAGE_LOG(WARN, "fail to init data store desc", K(ret));
  } else if (OB_FAIL(open_macro_writer(ctx, parallel_idx, cg_schema, allocator, merge_micro_block_read_info))) {
    STORAGE_LOG(WARN, "fail to open macro writer", K(ret), K(parallel_idx), K(cg_schema), KP(merge_micro_block_read_info));
  }
  return ret;
}

int ObWriteHelper::append(const blocksstable::ObDatumRow &row, const bool direct_append)
{
  int ret = OB_SUCCESS;

  if (skip_project_ || direct_append) {
    if (OB_FAIL(macro_writer_.append_row(row))) {
      STORAGE_LOG(WARN, "fail to append row", K(ret), K(row), K(macro_writer_));
    }
  } else if (OB_FAIL(projector_.project(row))) {
    STORAGE_LOG(WARN, "failed to project row", K(ret), K(row), K(projector_));
  } else if (OB_FAIL(macro_writer_.append_row(projector_.get_project_row()))) {
    STORAGE_LOG(WARN, "fail to append row", K(ret), K(row), K(macro_writer_));
  }

  return ret;
}

int ObWriteHelper::append_batch(const blocksstable::ObBatchDatumRows &batch_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_writer_.append_batch(batch_rows))) {
    STORAGE_LOG(WARN, "failed to append batch", K(ret), K(batch_rows), K(macro_writer_));
  }
  return ret;
}

int ObWriteHelper::append_micro_block(
    const blocksstable::ObMicroBlock &micro_block,
    const int64_t sstable_idx,
    ObMergeVectorStore *read_vector_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_writer_.append_micro_block(micro_block, nullptr, read_vector_store))) {
    STORAGE_LOG(WARN, "failed to append micro block", K(ret), K(micro_block), K(macro_writer_));
  } else if (sstable_idx >= 0 && sstable_idx < sstable_merge_block_info_array_.count()) {
    sstable_merge_block_info_array_.at(sstable_idx).inc_multiplexed_micro_count_in_new_macro();
  }
  return ret;
}

int ObWriteHelper::append_macro_block(const ObMacroBlockDesc &macro_desc, const ObMicroBlockData *micro_block_data, const int64_t sstable_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_writer_.append_macro_block(macro_desc, micro_block_data))) {
    STORAGE_LOG(WARN, "failed to append macro block", K(ret), K(macro_desc), K(micro_block_data), K(macro_writer_));
  } else if (sstable_idx >= 0 && sstable_idx < sstable_merge_block_info_array_.count()) {
    sstable_merge_block_info_array_.at(sstable_idx).inc_multiplexed_macro_block_count();
  }
  return ret;
}

int ObWriteHelper::project(const blocksstable::ObDatumRow &row, const blocksstable::ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(projector_.project(row))) {
    LOG_WARN("failed to project major row", K(ret), K(projector_));
  } else {
    result_row = &projector_.get_project_row();
  }
  return ret;
}

int ObWriteHelper::end_write(ObTabletMergeInfo &merge_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(macro_writer_.close())) {
    STORAGE_LOG(WARN, "failed to close macro writer", K(ret), K(macro_writer_));
  } else {
    ObSSTableMergeHistory &merge_history = merge_info.get_merge_history();
    merge_history.update_block_info_with_sstable_block_info(
        macro_writer_.get_merge_block_info(),
        false/*without_row_cnt*/,
        sstable_merge_block_info_array_);
  }

  return ret;
}

int ObCOMajorMergeIter::next()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr iter", K(ret));
  } else if (OB_FAIL(iter_->next())) {
    LOG_WARN("failed to move next", K(ret));
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObCOMergeWriter--------------------------------------------------------------
 */
ObCOMergeWriter::~ObCOMergeWriter()
{
  ObCOMajorMergeIter *iter = nullptr;
  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (OB_NOT_NULL(iter = iters_.at(i))) {
      iter->~ObCOMajorMergeIter();
      iter = nullptr;
    }
  }
  iters_.reset();
  default_row_.reset();
  allocator_.reset();
}

int ObCOMergeWriter::move_iters_next()
{
  int ret = OB_SUCCESS;
  ObCOMajorMergeIter *iter = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
    if (OB_ISNULL(iter = iters_.at(i))) {
      // do thing // empty major
    } else if (OB_ISNULL(iter->iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null iter", K(ret), K(i), K(iters_));
    } else if (OB_FAIL(iter->iter_->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Failed to next merge iter", K(i), K(ret), KPC(iter));
      }
    }
  }
  return ret;
}

int ObCOMergeWriter::get_curr_major_iter(const ObMergeLog &mergelog, ObCOMajorMergeIter *&merge_iter)
{
  int ret = OB_SUCCESS;
  merge_iter = nullptr;
  if (mergelog.major_idx_ == -1 && ObMergeLog::INSERT == mergelog.op_) {
    // do nothing // replay row directly
  } else if (0 > mergelog.major_idx_ || iters_.count() <= mergelog.major_idx_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mergelog", K(ret), K(mergelog), K(iters_.count()));
  } else if (OB_ISNULL(merge_iter = iters_.at(mergelog.major_idx_))) { // nullptr iter
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr iter", K(ret));
  } else if (OB_ISNULL(merge_iter->iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr iter", K(ret));
  }
  return ret;
}

bool ObCOMergeWriter::check_is_all_nop(const blocksstable::ObDatumRow &row)
{
  bool is_all_nop = true;
  for (int64_t i = 0; i < row.count_ && is_all_nop; i++) {
    if (!row.storage_datums_[i].is_nop()) {
      is_all_nop = false;
    }
  }
  return is_all_nop;
}

int ObCOMergeWriter::init_merge_iter(
    const ObMergeParameter &merge_param,
    const ObITableReadInfo *read_info,
    const ObStorageColumnGroupSchema *cg_schema,
    ObITable *table,
    const int64_t sstable_idx,
    const bool add_column,
    const bool major_need_project,
    const bool need_full_merge)
{
  // TODO Consider complex scenarios:
  // 1. cloumn major |  column inc major | row inc major | column inc major, row inc major can't reuse macro
  // ...
  int ret = OB_SUCCESS;
  ObSSTable* sstable = nullptr;
  ObMergeIter *iter = nullptr;
  ObCOMajorMergeIter *merge_iter = nullptr;

  if (OB_ISNULL(table)) {
    LOG_INFO("empty sstable, need not create iter", K(ret));
  } else if (!table->is_major_type_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("UNEXPECTED table type", K(ret), KPC(table));
  } else {
    sstable = static_cast<ObSSTable *>(table);
    ObMergeLevel merge_level = MERGE_LEVEL_MAX;
    if (OB_FAIL(merge_param.static_param_.get_sstable_merge_level(sstable_idx, merge_level))) {
      LOG_WARN("failed to get merge level", K(ret), K(sstable_idx), K(merge_param));
    } else if (OB_UNLIKELY(MERGE_LEVEL_MAX == merge_level)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected merge level", K(ret), K(sstable_idx), K(merge_param));
    } else if (!merge_param.is_empty_table(*table)) {
      const uint64_t compat_version = merge_param.static_param_.data_version_;
      const bool is_rowkey_cg_writer = nullptr != cg_schema && cg_schema->is_rowkey_column_group();
      bool need_co_scan_from_rowkey_base = need_co_scan_
                                        && table->is_co_sstable()
                                        && static_cast<ObCOSSTableV2 *>(table)->is_rowkey_cg_base()
                                        && !is_rowkey_cg_writer;
      if (add_column) {
        iter = OB_NEWx(ObDefaultRowIter, (&allocator_), default_row_);
      } else if (merge_param.is_full_merge() || need_full_merge) {
        iter = OB_NEWx(ObPartitionRowMergeIter, (&allocator_), allocator_, need_co_scan_from_rowkey_base);
      } else if (sstable->is_small_sstable()) {
        if (compat_version >= DATA_VERSION_4_3_5_1 && MICRO_BLOCK_MERGE_LEVEL == merge_level) {
          iter = OB_NEWx(ObPartitionMicroMergeIter, (&allocator_), allocator_);
        } else {
          iter = OB_NEWx(ObPartitionRowMergeIter, (&allocator_), allocator_, need_co_scan_from_rowkey_base);
        }
      } else if (MICRO_BLOCK_MERGE_LEVEL == merge_level) {
        iter = OB_NEWx(ObPartitionMicroMergeIter, (&allocator_), allocator_);
      } else {
        iter = OB_NEWx(ObPartitionMacroMergeIter, (&allocator_), allocator_);
      }

      if (OB_ISNULL(iter)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for partition iter", K(ret));
      } else if (OB_FAIL(iter->init(merge_param, sstable_idx, table, read_info))) {
        LOG_WARN("failed to init iter", K(ret), K(merge_param), KPC(table));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
        iter->~ObMergeIter();
        allocator_.free(iter);
        iter = nullptr;
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(merge_iter = alloc_helper<ObCOMajorMergeIter>(allocator_, iter, major_need_project, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for merge iter", K(ret));
        } else if (OB_FAIL(iters_.push_back(merge_iter))) {
          LOG_WARN("Failed to push back merge iter", K(ret));
        } else {
          LOG_INFO("Succ to init iter", K(ret), KPC(iter));
          merge_iter = nullptr;
        }
        if (nullptr != merge_iter) {
          merge_iter->~ObCOMajorMergeIter();
          allocator_.free(merge_iter);
          merge_iter = nullptr;
        }
      }
    } else {
      LOG_INFO("empty sstable, need not create iter", K(ret));
    }
  }

  return ret;
}

// init iters first before
// default_row is projected row
int ObCOMergeWriter::basic_init(const blocksstable::ObDatumRow &default_row,
                                const ObMergeParameter &merge_param,
                                const int64_t column_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!default_row.is_valid() || OB_ISNULL(merge_param.error_location_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(default_row), K(merge_param.error_location_));
  } else if (FALSE_IT(error_location_ = merge_param.error_location_)) {
  } else if (OB_FAIL(default_row_.init(allocator_, column_cnt))) {
    LOG_WARN("Failed to init default row", K(ret), K(column_cnt));
  } else if (OB_FAIL(default_row_.deep_copy(default_row, allocator_))) {
    LOG_WARN("failed to deep copy default row", K(ret));
  } else if (OB_FAIL(fuser_.init(column_cnt))) {
    LOG_WARN("failed to init fuser", K(ret), K(column_cnt));
  } else if (OB_FAIL(move_iters_next())) {
    LOG_WARN("failed to move iters next", K(ret));
  } else {
    compat_version_ = merge_param.static_param_.data_version_;
    is_inited_ = true;
  }
  return ret;
}

int ObCOMergeWriter::replay_last_skip_major(const int64_t current_major_idx, const int64_t current_row_id)
{
  int ret = OB_SUCCESS;
  if (last_skip_major_idx_ >= 0) {
    if (last_skip_major_idx_ == current_major_idx && last_skip_major_row_id_ <= current_row_id) {
      // larger row id for same major, do nothing
      last_skip_major_idx_ = -1;
      last_skip_major_row_id_ = -1;
    } else if (last_skip_major_idx_ != current_major_idx) {
      ObMergeLog merge_log(ObMergeLog::REPLAY, last_skip_major_idx_, last_skip_major_row_id_);
      if (OB_FAIL(replay_range_mergelog(merge_log))) {
        LOG_WARN("failed to replay merge log");
      } else {
        last_skip_major_idx_ = -1;
        last_skip_major_row_id_ = -1;
        LOG_INFO("success to replay last skip major", K(ret), K(merge_log), K(cg_idx_));
      }
    }
  }
  return ret;
}

// The main entry for replay mergelog
int ObCOMergeWriter::replay_mergelog(
    const ObMergeLog &mergelog,
    const ObMergeVectorStore *&vector_store,
    const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObCOMergeWriter not init", K(ret));
  } else if (OB_UNLIKELY(!mergelog.is_valid() || (nullptr != vector_store && nullptr != row))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge log", K(ret), K(mergelog), K(vector_store), K(row));
  } else if (OB_FAIL(replay_last_skip_major(mergelog.major_idx_, mergelog.row_id_))) {
    LOG_WARN("failed to replay last skip major", K(ret));
  } else if (mergelog.is_range_mergelog()) {
    if (OB_FAIL(replay_range_mergelog(mergelog))) {
      LOG_WARN("failed to replay merge log", K(ret), K(mergelog));
    }
  } else if (OB_UNLIKELY(nullptr == vector_store && nullptr == row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row", K(ret), K(mergelog), K(vector_store), K(row));
  } else if (nullptr != row) {
    if (ObMergeLog::INSERT == mergelog.op_ && row->row_flag_.is_delete()) {
    } else if (OB_FAIL(replay_single_mergelog(mergelog, *row))) {
      LOG_WARN("failed to replay merge log", K(ret), K(mergelog), KPC(row));
    }
  } else { // nullptr != vector_store
    if (OB_FAIL(replay_batch_mergelog(mergelog, *vector_store, true/*need_check_project*/, true/*need_check_filter*/))) {
      LOG_WARN("failed to replay merge log", K(ret), K(mergelog), K(vector_store));
    }
  }
  LOG_TRACE("success to replay merge log", K(ret), K(mergelog), KPC(vector_store), KPC(row));
  return ret;
}

// replay single row which is seen as inc row
int ObCOMergeWriter::replay_single_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObCOMajorMergeIter *merge_iter = nullptr;
  bool finish = false;
  if (OB_FAIL(get_curr_major_iter(mergelog, merge_iter))) {
    LOG_WARN("failed to get curr major iter", K(ret), K(mergelog));
  }
  while (OB_SUCC(ret) && !finish) {
    int64_t cmp_ret = 0;
    bool skip_curr_row = false;
    if (OB_FAIL(compare(merge_iter, mergelog, cmp_ret, row, skip_curr_row))) {
      LOG_WARN("failed to compare", K(ret), K(mergelog), K(row));
    } else if (skip_curr_row) {
      last_skip_major_idx_ = mergelog.major_idx_;
      last_skip_major_row_id_ = mergelog.row_id_;
      break;
    } else if (cmp_ret > 0) {
      if (OB_UNLIKELY(mergelog.op_ != ObMergeLog::INSERT)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected cmp ret", K(ret), K(cmp_ret), K(mergelog), K(row), K(*this));
      } else if (OB_FAIL(process_mergelog_row(merge_iter, mergelog, row))) {
        STORAGE_LOG(WARN, "failed to process_mergelog_row", K(ret), K(mergelog), K(row));
      } else {
        finish = true;
      }
    } else if (cmp_ret < 0) {
      // we can't replay to row_id - 1 directly, cause merge log maybe update which can be skipped,
      // in which situation, the range can be reused but replay to row_id - 1 will cause range be opened.
      if (OB_FAIL(append_iter_curr_row_or_range(merge_iter, mergelog.op_))) {
        STORAGE_LOG(WARN, "failed to append iter curr row or range", K(ret));
      }
    } else if (OB_FAIL(process_mergelog_row(merge_iter, mergelog, row))) {
      STORAGE_LOG(WARN, "failed to process_mergelog_row", K(ret), K(mergelog), K(row));
    } else {
      finish = true;
    }
    if (OB_FAIL(ret)) {
    } else if (cmp_ret <= 0 && OB_FAIL(merge_iter->next())) {
      if (OB_LIKELY(ret == OB_ITER_END)) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "fail to move next", K(ret), KPC(merge_iter));
      }
    }
  }
  return ret;
}

int ObCOMergeWriter::replay_range_mergelog(const ObMergeLog &mergelog)
{
  int ret = OB_SUCCESS;
  bool finish = false;
  int64_t cmp_ret = 0;
  bool skip_curr_row = false; // no used
  blocksstable::ObDatumRow row; // no used
  ObCOMajorMergeIter *merge_iter = nullptr;
  if (ObMergeLog::DELETE_RANGE == mergelog.op_) {
    if (OB_FAIL(replay_last_skip_major(-1))) {
      LOG_WARN("failed to replay last skip major", K(ret));
    }
  }
  if (FAILEDx(get_curr_major_iter(mergelog, merge_iter))) {
    LOG_WARN("failed to get curr major iter", K(ret), K(mergelog));
  }
  while (OB_SUCC(ret) && !finish) {
    if (OB_FAIL(compare(merge_iter, mergelog, cmp_ret, row, skip_curr_row))) {
      LOG_WARN("failed to compare", K(ret), K(mergelog));
    } else if (cmp_ret > 0) {
      finish = true;
    } else if (OB_FAIL(append_iter_curr_row_or_range(merge_iter, mergelog.op_))) {
      LOG_WARN("failed to append iter curr row or range", K(ret));
    } else if (OB_FAIL(merge_iter->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to move next", K(ret), K(mergelog));
      }
    }
  }
  return ret;
}

int ObCOMergeWriter::get_curr_major_row(ObCOMajorMergeIter &iter, const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (iter.need_project_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected major", K(iter));
  } else {
    row = iter.iter_->get_curr_row();
  }
  return ret;
}

int ObCOMergeWriter::append_iter_curr_row_or_range(
  ObCOMajorMergeIter *merge_iter,
  const ObMergeLog::OpType op_type)
{
  int ret = OB_SUCCESS;
  ObMergeIter *iter = nullptr != merge_iter ? merge_iter->iter_ : nullptr;
  if (OB_UNLIKELY(nullptr == iter || iter->is_iter_end())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected iter state", K(ret), KPC(iter));
  } else if (ObMergeLog::DELETE_RANGE == op_type) {
    STORAGE_LOG(TRACE, "delete range, no need to append iter curr row or range", K_(cg_idx), "op_type", ObMergeLog::get_op_type_str(op_type));
  } else if (iter->is_macro_block_opened()) {
    const blocksstable::ObDatumRow *major_row = nullptr;
    if (OB_ISNULL(iter->get_curr_row())) { // micro not opened
      const blocksstable::ObMicroBlock *micro_block = nullptr;
      if (OB_FAIL(iter->get_curr_micro_block(micro_block))) {
        STORAGE_LOG(WARN, "failed to get_curr_micro_block", K(ret), KPC(iter));
      } else if (OB_ISNULL(micro_block) || !micro_block->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected micro block", K(ret), KPC(iter), KPC(micro_block));
      } else if (OB_FAIL(process(*micro_block, static_cast<ObPartitionMergeIter *>(iter)->get_sstable_idx(), get_read_vector_store()))) {
        STORAGE_LOG(WARN, "failed to process micro block", K(ret));
      }
    } else if (OB_FAIL(get_curr_major_row(*merge_iter, major_row))) {
      STORAGE_LOG(WARN, "failed to get curr major row", K(ret), KPC(iter));
    } else if (OB_FAIL(fuser_.fuse_rows(*major_row, default_row_))) {
      STORAGE_LOG(WARN, "failed to fuse row", K(ret), KPC(iter), K(default_row_));
    } else if (OB_FAIL(process(fuser_.get_result_row()))) {
      STORAGE_LOG(WARN, "failed to process iter curr row", K(ret), KPC(iter), K(default_row_));
    }
  } else if (iter->is_small_sstable_iter()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPartitionMicroMergeIter for small sstable is unexpected not opened", K(ret));
  } else {
    const ObMacroBlockDesc *macro_desc = nullptr;
    const ObMicroBlockData *micro_block_data = nullptr;
    bool need_rewrite = false;

    if (OB_FAIL(iter->get_curr_macro_block(macro_desc, micro_block_data))) {
      STORAGE_LOG(WARN, "Failed to get current micro block", K(ret), KPC(iter));
    } else if (OB_ISNULL(macro_desc) || OB_UNLIKELY(!macro_desc->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null macro block", K(ret), KP(macro_desc), KPC(iter));
    } else if (OB_FAIL(process(merge_iter, *macro_desc, micro_block_data))) {
      STORAGE_LOG(WARN, "Failed to append macro block", K(ret), KPC(macro_desc));
    }
  }

  return ret;
}

int ObCOMergeWriter::compare(
    ObCOMajorMergeIter *merge_iter,
    const ObMergeLog &mergelog,
    int64_t &cmp_ret,
    const blocksstable::ObDatumRow &row,
    bool &skip_curr_row) const
{
  int ret = OB_SUCCESS;
  const int64_t log_row_id = mergelog.row_id_;
  int64_t iter_row_id = 0;
  skip_curr_row = false;
  ObMergeIter *iter = nullptr != merge_iter ? merge_iter->iter_ : nullptr;
  if (nullptr == iter || iter->is_iter_end()) {
    cmp_ret = 1;
  } else {
    int64_t curr_range_end_rowid;
    bool check_iter_range = true;
    bool need_open = true;
    while (OB_SUCC(ret) && OB_ISNULL(iter->get_curr_row()) && check_iter_range) {
      check_iter_range = false;
      if (log_row_id <= iter->get_last_row_id()) {
        cmp_ret = 1;
      } else if (OB_FAIL(iter->get_curr_range_end_rowid(curr_range_end_rowid))) {
        STORAGE_LOG(WARN, "Failed to get current row count", K(ret), KPC(iter));
      } else if (log_row_id > curr_range_end_rowid) {
        cmp_ret = -1;
      } else if ((mergelog.op_ == ObMergeLog::INSERT || mergelog.is_range_mergelog())
          && log_row_id == curr_range_end_rowid) {
        cmp_ret = 0;
      } else if (mergelog.op_ == ObMergeLog::UPDATE && is_cg()
          && OB_FAIL(iter->need_open_curr_range(row, need_open, mergelog.row_id_))) {
        STORAGE_LOG(WARN, "fail to check row changed ", K(ret), K(mergelog), KPC(iter));
      } else if (!need_open) {
        skip_curr_row = true;
        break;
      } else if (FALSE_IT(check_iter_range = true)) {
      } else if (OB_FAIL(iter->open_curr_range(false /* rewrite */, false/*for_compare*/))) {
        STORAGE_LOG(WARN, "failed to open curr range", K(ret), KPC(iter));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(iter->get_curr_row())) {
      if (OB_FAIL(iter->get_curr_row_id(iter_row_id))) {
        STORAGE_LOG(WARN, "failed to get iter row id", K(ret), KPC(iter));
      } else if (iter_row_id == log_row_id) {
        cmp_ret = 0;
      } else {
        cmp_ret = iter_row_id > log_row_id ? 1 : -1;
      }
    }
  }

  return ret;
}

int ObCOMergeWriter::process_mergelog_row(ObCOMajorMergeIter *merge_iter, const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (!mergelog.is_delete_mergelog()) {
    ObMergeIter *iter = nullptr != merge_iter ? merge_iter->iter_ : nullptr;
    if (mergelog.op_ == ObMergeLog::INSERT) {
      if (OB_FAIL(fuser_.fuse_rows(row, default_row_))) {
        STORAGE_LOG(WARN, "failed to fuse row", K(ret), K(row));
      }
    } else if (mergelog.op_ == ObMergeLog::UPDATE) {
      const blocksstable::ObDatumRow *major_row = nullptr;
      if (OB_UNLIKELY(nullptr == iter || nullptr == iter->get_curr_row())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null iter or null curr row", K(ret), KPC(iter));
      } else if (OB_FAIL(get_curr_major_row(*merge_iter, major_row))) {
        STORAGE_LOG(WARN, "failed to get curr major row", K(ret), KPC(this));
      } else if(OB_FAIL(fuser_.fuse_rows(row, *major_row, default_row_))) {
        STORAGE_LOG(WARN, "failed to fuse row", K(ret), K(row), KPC(this));
      }
    }

    if (FAILEDx(process(fuser_.get_result_row()))){
      STORAGE_LOG(WARN, "failed to process iter curr row", K(ret), K(fuser_.get_result_row()));
    }
  }
  return ret;
}

void ObCOMergeWriter::dump_info() const
{
  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (OB_NOT_NULL(iters_.at(i)) && OB_NOT_NULL(iters_.at(i)->iter_)) {
      ObMergeIter *iter = iters_.at(i)->iter_;
      FLOG_INFO("co merge iter idx", KPC(iter->get_table()), "row_count", iter->get_last_row_id(),
          "is_iter_end", iter->is_iter_end());
    }
  }
}

int ObCOMergeWriter::init_default_row(
    ObIAllocator &allocator,
    const ObMergeParameter &merge_param,
    const int64_t major_working_cluster_version,
    blocksstable::ObDatumRow &default_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(default_row.init(allocator, merge_param.static_param_.multi_version_column_descs_.count()))) {
    LOG_WARN("Failed to init datum row", K(ret));
  } else if (OB_FAIL(merge_param.get_schema()->get_orig_default_row(merge_param.static_param_.multi_version_column_descs_,
      major_working_cluster_version >= DATA_VERSION_4_3_1_0, default_row))) {
    LOG_WARN("Failed to get default row from table schema", K(ret), K(merge_param.static_param_.multi_version_column_descs_));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator, merge_param.static_param_.multi_version_column_descs_, default_row))) {
    LOG_WARN("fail to fill lob header for default row", K(ret));
  } else if (FALSE_IT(default_row.row_flag_.set_flag(ObDmlFlag::DF_UPDATE))) {
  }
  return ret;
}
/**
 * ---------------------------------------------------------ObCOMergeRowWriter--------------------------------------------------------------
 */
ObCOMergeRowWriter::~ObCOMergeRowWriter()
{
  if (OB_NOT_NULL(progressive_merge_helper_)) {
    progressive_merge_helper_->~ObProgressiveMergeHelper();
    progressive_merge_helper_ = nullptr;
  }
  cg_wrappers_.reset();
}

int ObCOMergeRowWriter::choose_read_info_for_old_major(
   const ObMergeParameter &merge_param,
   const ObITableReadInfo &full_read_info,
   const ObStorageColumnGroupSchema &cg_schema,
   const ObITableReadInfo *&read_info)
{
  int ret = OB_SUCCESS;
  read_info = NULL;

  if (cg_schema.is_single_column_group()) {
    single_read_info_.reset();
    if (OB_FAIL(ObTenantCGReadInfoMgr::construct_cg_read_info(allocator_,
                                                              full_read_info.is_oracle_mode(),
                                                              write_helper_.get_col_desc_array().at(0),
                                                              nullptr,
                                                              single_read_info_))) {
      LOG_WARN("Fail to init cg read info", K(ret));
    } else {
      read_info = &single_read_info_;
    }
  } else {
    read_info = cg_schema.is_rowkey_column_group() ? merge_param.cg_rowkey_read_info_ : &full_read_info;
  }
  return ret;
}

int ObCOMergeRowWriter::get_writer_param(
    const ObMergeParameter &merge_param,
    const ObStorageColumnGroupSchema *cg_schema,
    const int64_t cg_idx,
    ObSSTable *sstable,
    ObITable *&table,
    bool &add_column)
{
  int ret = OB_SUCCESS;
  ObCOSSTableV2 *co_sstable = nullptr;
  ObSSTable *cg_sstable = nullptr;
  ObSSTableWrapper cg_wrapper;
  table = nullptr;
  add_column = false;
  if (OB_ISNULL(sstable) || OB_ISNULL(merge_param.static_param_.schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr sstable or schema", K(ret), KP(sstable), KP(merge_param.static_param_.schema_));
  } else if (merge_param.is_empty_table(*sstable)) {
    table = nullptr;
  } else if (!sstable->is_column_store_sstable()) {// TODO + check inc major sstable
    table = sstable;
  } else if (OB_ISNULL(co_sstable = static_cast<ObCOSSTableV2*>(sstable))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr co_sstable", K(ret), KPC(sstable));
  } else if (co_sstable->is_row_store_only_co_table()) {
    table = sstable;
  } else {
    const int64_t normal_cg_cnt = co_sstable->get_column_group_count(false/*include hidden cg*/);
    int64_t fetch_cg_idx = cg_idx;
    bool skip_fetch_cg = false;
    if (merge_param.static_param_.schema_->is_row_store()) {
      // column store switch to row store: fetch the base CG of this specific CO SSTable
      // For ALL_CG_BASE: key_.column_group_idx_ points to ALL CG which contains all columns
      // For ROWKEY_CG_BASE: key_.column_group_idx_ points to ROWKEY CG, cross-CG scan reads all columns
      fetch_cg_idx = co_sstable->get_key().column_group_idx_;
      LOG_INFO("[ROW_COL_SWITCH] col to row store, redirect to base cg", K(cg_idx), K(fetch_cg_idx), KPC(co_sstable));
    } else if (co_sstable->is_all_cg_base() && !merge_param.static_param_.schema_->has_all_column_group()) {
      // all cg switch to each cg: if cur major has hidden rowkey cg, redirect to hidden rowkey cg, otherwise redirect to base cg
      if (cg_schema->is_rowkey_column_group()) {
        if (co_sstable->has_hidden_rowkey_cg()) {
          fetch_cg_idx = HIDDEN_ROWKEY_COLUMN_GROUP_IDX;
          LOG_INFO("[ROW_COL_SWITCH] redirect rowkey cg to hidden rowkey cg", K(cg_idx), K(fetch_cg_idx), KPC(co_sstable));
        } else {
          fetch_cg_idx = co_sstable->get_key().column_group_idx_;
          LOG_INFO("[ROW_COL_SWITCH] no hidden rowkey cg, redirect rowkey cg to all cg base", K(cg_idx), K(fetch_cg_idx), KPC(co_sstable));
        }
      }
    } else if (co_sstable->is_rowkey_cg_base()
            && merge_param.static_param_.schema_->has_all_column_group()
            && nullptr == merge_param.static_param_.orig_schema_) {
      // each cg switch to all cg: when fetch hidden rowkey cg
      if (HIDDEN_ROWKEY_COLUMN_GROUP_IDX == cg_idx) {
        fetch_cg_idx = co_sstable->get_key().column_group_idx_;
        LOG_INFO("[ROW_COL_SWITCH] redirect hidden rowkey cg to old rowkey cg", K(cg_idx), K(fetch_cg_idx), KPC(co_sstable));
      } else if (cg_schema->is_all_column_group()) {
        skip_fetch_cg = true;
        LOG_INFO("[ROW_COL_SWITCH] old co has no all cg, skip fetch", K(cg_idx), KPC(co_sstable));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (skip_fetch_cg) {
      // there is no all cg in old co sstable, set table to nullptr
      table = nullptr;
    } else if (fetch_cg_idx < normal_cg_cnt || HIDDEN_ROWKEY_COLUMN_GROUP_IDX == fetch_cg_idx) {
      if (OB_FAIL(co_sstable->fetch_cg_sstable(fetch_cg_idx, cg_wrapper))) {
        LOG_WARN("failed to get cg sstable", K(ret), K(fetch_cg_idx), K(cg_idx), K(sstable));
      } else if (OB_FAIL(cg_wrapper.get_loaded_column_store_sstable(cg_sstable))) {
        LOG_WARN("failed to get sstable from wrapper", K(ret), K(cg_wrapper));
      } else if (OB_FAIL(cg_wrappers_.push_back(cg_wrapper))) {
        LOG_WARN("failed to push cg wrapper", K(ret), K(cg_wrappers_));
      } else {
        table = cg_sstable;
      }
    } else if (!cg_schema->is_single_column_group()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cg schema", K(ret), K(cg_idx), K(co_sstable->get_cs_meta().column_group_cnt_), KPC(cg_schema), K(sstable));
    } else {
      table = sstable;
      add_column = true; // for add column, will use ObDefaultRowIter
      LOG_INFO("add column for cg", K(ret), K(cg_idx), K(co_sstable->get_cs_meta().column_group_cnt_), KPC(cg_schema), K(sstable));
    }
  }
  return ret;
}

int ObCOMergeRowWriter::init(
    ObBasicTabletMergeCtx &ctx,
    const blocksstable::ObDatumRow &default_row,
    const ObMergeParameter &merge_param,
    const int64_t parallel_idx,
    const ObITableReadInfo *full_read_info,
    const int64_t cg_idx,
    ObTabletMergeInfo &merge_info,
    ObIArray<ObITable*> &tables)
{
  int ret = OB_SUCCESS;
  bool is_all_nop = false;
  const ObITableReadInfo *read_info = nullptr;
  ObCOTabletMergeCtx *co_ctx = static_cast<ObCOTabletMergeCtx *>(&ctx);
  const ObStorageColumnGroupSchema *cg_schema = nullptr;
  int64_t cg_column_cnt = 0;
  bool add_column = false;
  bool major_need_project = false;
  bool need_full_merge = false;
  bool table_need_full_merge = false;
  ObITable *table = nullptr;
  ObSSTable *sstable = nullptr;
  is_using_column_tmp_file_ = co_ctx->is_using_column_tmp_file() && !is_base_cg_writer();
  ObProgressiveMergeMgr &progressive_merge_mgr = co_ctx->progressive_merge_mgr_;
  ObSEArray<ObITable*, 16> cg_tables;
  if (OB_FAIL(co_ctx->get_cg_schema_for_merge(cg_idx, cg_schema))) {
    LOG_WARN("fail to get cg schema for merge", K(ret), K(cg_idx));
  } else if (FALSE_IT(cg_column_cnt = cg_schema->column_cnt_)) {
  } else if (OB_FAIL(write_helper_.init_data_store_desc(ctx, merge_param, merge_info))) {
    LOG_WARN("fail to init data store desc", K(ret), K(parallel_idx), K(cg_schema));
  } else if (OB_FAIL(choose_read_info_for_old_major(merge_param, *full_read_info, *cg_schema, read_info))) {
    LOG_WARN("fail to choose read info for old major", K(ret), K(cg_schema), KP(read_info));
  } else if (OB_ISNULL(read_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read info is unexpected null", KR(ret), KP(read_info));
  } else if (OB_FAIL(write_helper_.open_macro_writer(ctx, parallel_idx, *cg_schema, allocator_, read_info))) {
    LOG_WARN("fail to open macro writer", K(ret), K(parallel_idx), K(cg_schema), KPC(read_info));
  } else if (OB_FAIL(row_.init(cg_column_cnt))) {
    LOG_WARN("fail to init row", K(ret), K(cg_schema));
  } else {
    // inner_init() uses cg_idx_; set before the major loop because batch merge may skip every
    // major (empty cg_tables path) so the loop body never assigns cg_idx_.
    cg_idx_ = cg_idx;
    // Note: The order of 'tables' here is the reverse of 'merge_sstable_status_array_'
    for (int64_t i = 0, sstable_idx = tables.count() - 1; OB_SUCC(ret) && i < tables.count(); i++, sstable_idx--) {
      sstable = static_cast<ObSSTable *>(tables.at(i));
      table_need_full_merge = false;
      if (OB_FAIL(merge_param.static_param_.get_sstable_need_full_merge(sstable_idx, table_need_full_merge))) {
        LOG_WARN("failed to get sstable need_full_merge", K(ret), K(sstable_idx), KPC(table));
      } else if (is_batch_merge_writer() && table_need_full_merge) {
        LOG_INFO("skip major which is treated as minor in batch merge", K(sstable_idx), KPC(table));
      } else if (OB_FAIL(get_writer_param(merge_param, cg_schema, cg_idx, sstable, table, add_column))) {
        LOG_WARN("failed to get writer param", K(ret));
      } else if (nullptr != table && OB_FAIL(cg_tables.push_back(table))) {
        LOG_WARN("failed to push back table", K(ret), KPC(table));
      } else {
        major_need_project = !add_column && table != nullptr &&
          (!table->is_column_store_sstable() ||
          (table->is_co_sstable() && static_cast<ObCOSSTableV2*>(table)->is_row_store_only_co_table())) &&
          !cg_schema->is_all_column_group();
        need_full_merge = major_need_project ||
                          table_need_full_merge ||
                          (cg_schema == &co_ctx->mocked_row_store_cg_) ||
                          (table != nullptr && !table->is_column_store_sstable()); // TODO: allow reuse row-store sstable
        if (OB_FAIL(ret)) {
        } else if (is_base_cg_writer()) { // no need major iter
        } else if (OB_FAIL(init_merge_iter(
            merge_param, major_need_project ? full_read_info : read_info, cg_schema, table, sstable_idx, add_column, major_need_project, need_full_merge))) {
          LOG_WARN("failed to init merge iter", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && !merge_param.is_full_merge() && !cg_tables.empty()) {
      progressive_merge_helper_ = OB_NEWx(ObProgressiveMergeHelper, (&allocator_), cg_idx);
      if (OB_ISNULL(progressive_merge_helper_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for progressive_merge_helper_", K(ret));
      } else if (OB_FAIL(progressive_merge_helper_->init(cg_tables, merge_param, &progressive_merge_mgr))) {
        LOG_WARN("failed to init progressive_merge_helper", K(ret), KPC(table));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (write_helper_.need_project()) {
    if (OB_FAIL(write_helper_.project(default_row, row_, is_all_nop))) {
      LOG_WARN("fail to project", K(ret), K(default_row), K(row_));
    } else if (OB_FAIL(basic_init(row_ /*default_row*/, merge_param, cg_column_cnt))) {
      LOG_WARN("Failed to init default row", K(ret), K(cg_column_cnt));
    }
  } else if (OB_FAIL(basic_init(default_row, merge_param, cg_column_cnt))) {
    LOG_WARN("Failed to init default row", K(ret), K(cg_column_cnt));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(inner_init(ctx))) {
    LOG_WARN("failed to inner init", K(ret));
  }

  return ret;
}

int ObCOMergeRowWriter::get_curr_major_row(ObCOMajorMergeIter &iter, const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (iter.need_project_) {
    if (OB_FAIL(write_helper_.project(*iter.iter_->get_curr_row(), row))) {
      LOG_WARN("failed to project major row", K(ret), K(iter), K(write_helper_));
    }
  } else {
    row = iter.iter_->get_curr_row();
  }
  return ret;
}

int ObCOMergeRowWriter::inner_process_macro_rewrite(ObCOMajorMergeIter &merge_iter)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObDatumRow *major_row = nullptr;
  ObMergeIter *iter = merge_iter.iter_;
  if (OB_UNLIKELY(iter->is_macro_block_opened())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected macro block opened", K(ret), KPC(iter));
  } else if (OB_FAIL(iter->open_curr_range(true /* rewrite */, false/*for_compare*/))) {
    STORAGE_LOG(WARN, "failed to open iter range", K(ret), KPC(iter));
  } else if (OB_ISNULL(iter->get_curr_row())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null row", K(ret), KPC(iter));
  } else if (OB_FAIL(get_curr_major_row(merge_iter, major_row))) {
    STORAGE_LOG(WARN, "failed to get curr major row", K(ret), KPC(this));
  } else if (OB_FAIL(fuser_.fuse_rows(*major_row, default_row_))) {
    STORAGE_LOG(WARN, "failed to fuse row", K(ret), KPC(iter), K(default_row_));
  } else if (OB_FAIL(process(fuser_.get_result_row()))) {
    STORAGE_LOG(WARN, "failed to process iter curr row", K(ret), KPC(iter));
  }

  return ret;
}

int ObCOMergeRowWriter::inner_process_macro_open(ObCOMajorMergeIter &merge_iter)
{
  int ret = OB_SUCCESS;
  ObMergeIter *iter = merge_iter.iter_;
  if (OB_FAIL(iter->open_curr_range(false /* rewrite */, false/*for_compare*/))) {
    STORAGE_LOG(WARN, "Failed to open_curr_range", K(ret));
  } else if (OB_FAIL(append_iter_curr_row_or_range(&merge_iter, ObMergeLog::REPLAY))) {
    STORAGE_LOG(WARN, "failed to append iter curr row or range", K(ret), KPC(iter));
  }
  return ret;
}

int ObCOMergeRowWriter::process(
    ObCOMajorMergeIter *merge_iter,
    const ObMacroBlockDesc &macro_desc,
    const ObMicroBlockData *micro_block_data)
{
  int ret = OB_SUCCESS;
  ObMergeIter *iter = nullptr != merge_iter ? merge_iter->iter_ : nullptr;
  ObBlockOp block_op;
  if (OB_UNLIKELY(iter == nullptr || iter->is_iter_end())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null iter", K(ret), KP(iter));
  } else {
    ObPartitionMergeIter *partition_iter = static_cast<ObPartitionMergeIter *>(iter);
    if (OB_ISNULL(progressive_merge_helper_) || !progressive_merge_helper_->is_valid()) {
      // do nothing
    } else if (OB_FAIL(progressive_merge_helper_->check_macro_block_op(macro_desc, block_op))) {
      STORAGE_LOG(WARN, "failed to check macro operation", K(ret), K(macro_desc));
    }

    if (OB_FAIL(ret)) {
    } else if (block_op.is_rewrite()) {
      if (OB_FAIL(inner_process_macro_rewrite(*merge_iter))) {
        STORAGE_LOG(WARN, "failed to process_macro_rewrite", K(ret));
      }
    } else if (block_op.is_open()) {
      if (OB_FAIL(inner_process_macro_open(*merge_iter))) {
        STORAGE_LOG(WARN, "failed to process_macro_open", K(ret));
      }
    } else if (OB_FAIL(write_helper_.append_macro_block(macro_desc, micro_block_data, partition_iter->get_sstable_idx()))) {
      STORAGE_LOG(WARN, "failed to append macro block", K(ret), K(macro_desc));
    }
  }
  STORAGE_LOG(INFO, "process micro data", K(ret), K(macro_desc), K(block_op), K(micro_block_data), KPC(this));
  return ret;
}

int ObCOMergeRowWriter::process(
    const blocksstable::ObMicroBlock &micro_block,
    const int64_t sstable_idx,
    ObMergeVectorStore *read_vector_store)
{
  return write_helper_.append_micro_block(micro_block, sstable_idx, read_vector_store);
}

int ObCOMergeRowWriter::process(const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!row.is_valid() || row.row_flag_.is_delete() || row.row_flag_.is_not_exist())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row", K(ret), K(row));
  } else if (OB_FAIL(write_helper_.append(row, true))) {
    STORAGE_LOG(WARN, "failed to append row", K(ret), K(row), K(write_helper_));
    SET_DIAGNOSE_LOCATION(error_location_);
  }

  return ret;
}

// prepare projected row for replay
int ObCOMergeRowWriter::prepare_replay_row(
    const ObMergeLog &mergelog,
    const blocksstable::ObDatumRow &row,
    const blocksstable::ObDatumRow *&res_row,
    bool &skip_replay)
{
  int ret = OB_SUCCESS;
  bool is_all_nop = false;
  if (!is_using_column_tmp_file_ && write_helper_.need_project()) {
    if (OB_FAIL(write_helper_.project(row, row_, is_all_nop))) {
      STORAGE_LOG(WARN, "fail to project", K(ret), K(write_helper_), K(row));
    } else {
      res_row = &row_;
    }
  } else {
    is_all_nop = check_is_all_nop(row);
    res_row = &row;
  }
  if (OB_FAIL(ret)) {
  } else if (mergelog.op_ == ObMergeLog::UPDATE && is_all_nop) { // only single major can skip nop
    last_skip_major_idx_ = mergelog.major_idx_;
    last_skip_major_row_id_ = mergelog.row_id_;
    skip_replay = true;
  }
  return ret;
}

int ObCOMergeRowWriter::replay_row_directly(
    const ObMergeLog &mergelog,
    const blocksstable::ObDatumRow &row,
    ObCOMajorMergeIter &merge_iter)
{
  int ret = OB_SUCCESS;
  bool is_all_nop = false;
  if (write_helper_.need_project()) {
    if (OB_FAIL(write_helper_.project(row, row_, is_all_nop))) {
      STORAGE_LOG(WARN, "fail to project", K(ret), K(write_helper_), K(row));
    } else if (OB_FAIL(process_mergelog_row(&merge_iter, mergelog, row_))) {
      LOG_WARN("failed to process_mergelog_row", K(ret), K(mergelog), K(row), K(merge_iter));
    }
  } else if (OB_FAIL(process_mergelog_row(&merge_iter, mergelog, row))) {
    LOG_WARN("failed to process_mergelog_row", K(ret), K(mergelog), K(row), K(merge_iter));
  }
  return ret;
}

int ObCOMergeRowWriter::replay_iter_directly(const ObMergeLog &mergelog, ObCOMajorMergeIter &merge_iter)
{
  int ret = OB_SUCCESS;
  ObMergeIter *iter = merge_iter.iter_;
  int64_t curr_end_row_id = 0;
  int64_t expected_end_row_id = 0;
#define GET_CURR_END_ROW_ID(iter, end_row_id) \
  if (nullptr == iter->get_curr_row()) { \
    if (OB_FAIL(iter->get_curr_range_end_rowid(end_row_id))) { \
      LOG_WARN("failed to get curr range end row id", K(ret), K(iter)); \
    } \
  } else if (OB_FAIL(iter->get_curr_row_id(end_row_id))) { \
    LOG_WARN("failed to get curr row id", K(ret), K(iter)); \
  }
  if (OB_NOT_NULL(iter)) {
    GET_CURR_END_ROW_ID(iter, expected_end_row_id);
  }
  if (OB_SUCC(ret) && mergelog.row_id_ != INT64_MAX && mergelog.row_id_ != expected_end_row_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected curr end row id", K(ret), K(mergelog), K(expected_end_row_id));
  }
  while (OB_SUCC(ret) && OB_NOT_NULL(iter) && !iter->is_iter_end()) {
    // if current row is not opened, range maybe reused, so flush pending buffered rows first
    if (nullptr == iter->get_curr_row() && OB_FAIL(flush_pending_buffered_rows())) {
      LOG_WARN("failed to flush pending buffered rows", K(ret), K(iter));
    } else if (OB_FAIL(append_iter_curr_row_or_range(&merge_iter, mergelog.op_))) {
      LOG_WARN("failed to append iter curr row or range", K(ret), K(merge_iter));
    } else {
      GET_CURR_END_ROW_ID(iter, curr_end_row_id);
    }
    if (OB_FAIL(ret)) {
    } else if (compat_version_ < DATA_VERSION_4_5_1_0 && mergelog.row_id_ != INT64_MAX) {
      break;
    } else if (mergelog.row_id_ == curr_end_row_id) {
      break;
    } else if (mergelog.row_id_ < curr_end_row_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected curr end row id", K(ret), K(mergelog), K(curr_end_row_id));
    } else if (OB_FAIL(iter->next())) {
      if (OB_LIKELY(ret == OB_ITER_END)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to move next", K(ret), K(iter));
      }
    }
  }
#undef GET_CURR_END_ROW_ID
  return ret;
}

int ObCOMergeRowWriter::replay_single_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool skip_replay = false;
  const blocksstable::ObDatumRow *tmp_row = nullptr;
  if (OB_FAIL(prepare_replay_row(mergelog, row, tmp_row, skip_replay))) {
    LOG_WARN("failed to prepare replay row", K(ret), K(mergelog), K(row));
  } else if (skip_replay) {
  } else if (OB_FAIL(ObCOMergeWriter::replay_single_mergelog(mergelog, *tmp_row))) {
    STORAGE_LOG(WARN, "failed to replay mergelog", K(ret), K(mergelog), K(*tmp_row));
  }

  return ret;
}

int ObCOMergeRowWriter::end_write(ObCOTabletMergeCtx &co_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replay_last_skip_major(-1))) {
    LOG_WARN("failed to replay last skip major", K(ret));
  } else if (OB_FAIL(flush_pending_buffered_rows())) {
    LOG_WARN("failed to flush last data", K(ret));
  } else if (OB_NOT_NULL(progressive_merge_helper_)) {
    progressive_merge_helper_->end();
  }
  if (OB_FAIL(ret)) {
  } else {
    ObTabletMergeInfo **merge_infos = co_ctx.cg_merge_info_array_;
    int64_t iter_idx = -1;
    if (OB_UNLIKELY(nullptr == merge_infos)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid count or unexpected null merge info array", K(ret), K(merge_infos));
    } else if (OB_FAIL(co_ctx.get_schema()->convert_column_group_idx_to_iter_idx(cg_idx_, iter_idx))) {
      STORAGE_LOG(WARN, "failed to convert column group idx to iter idx", K(ret), K(cg_idx_));
    } else if (nullptr == merge_infos[iter_idx]) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null merge info", K(ret), K(cg_idx_), K(iter_idx));
    } else if (OB_FAIL(write_helper_.end_write(*merge_infos[iter_idx]))) {
      LOG_WARN("failed to end write", K(ret));
    }
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObCOMergeBaseRowWriter--------------------------------------------------------------
 */
int ObCOMergeBaseRowWriter::replay_single_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row)
{
  return replay_row_directly(mergelog, row, merge_iter_);
}

int ObCOMergeBaseRowWriter::replay_range_mergelog(const ObMergeLog &mergelog)
{
  return replay_iter_directly(mergelog, merge_iter_);
}

/**
 * ---------------------------------------------------------ObCOMergeSingleWriter--------------------------------------------------------------
 */
ObCOMergeSingleWriter::~ObCOMergeSingleWriter()
{
  for (int64_t i = 0; i < write_helpers_.count(); i++) {
    ObWriteHelper *write_helper = write_helpers_.at(i);
    if (OB_NOT_NULL(write_helper)) {
      write_helper->~ObWriteHelper();
      allocator_.free(write_helper);
    }
  }
}

int ObCOMergeSingleWriter::init(
    ObBasicTabletMergeCtx &ctx,
    const blocksstable::ObDatumRow &default_row,
    const ObMergeParameter &merge_param,
    const ObITableReadInfo *full_read_info,
    const int64_t parallel_idx,
    ObTabletMergeInfo **merge_infos,
    ObIArray<ObITable*> &tables)
{
  int ret = OB_SUCCESS;
  int64_t full_column_cnt = 0;
  ObCOTabletMergeCtx &co_ctx = static_cast<ObCOTabletMergeCtx &>(ctx);
  if (OB_UNLIKELY(end_cg_idx_ - start_cg_idx_ < 0 || merge_infos == nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected arguments", K(ret), K(end_cg_idx_), K(start_cg_idx_), K(merge_infos));
  } else if (OB_FAIL(merge_param.get_schema()->get_store_column_count(full_column_cnt, true))) {
    STORAGE_LOG(WARN, "fail to get store column cnt", K(ret), KPC(merge_param.get_schema()));
  } else {
    base_cg_idx_ = co_ctx.base_rowkey_cg_idx_;
    ignore_base_cg_ = co_ctx.need_replay_base_directly_
        && base_cg_idx_ >= start_cg_idx_
        && base_cg_idx_ < end_cg_idx_;

    ObStorageCGSchemaIterator cg_iterator(*ctx.get_schema());
    for (uint32_t idx = start_cg_idx_; OB_SUCC(ret) && idx < end_cg_idx_; idx++) {
      const ObStorageColumnGroupSchema *cg_schema_ptr = nullptr;
      ObWriteHelper *write_helper = nullptr;
      if (ignore_base_cg_ && idx == base_cg_idx_) {
        // only nullptr writer_helper to placeholder
      } else if (OB_FAIL(co_ctx.get_schema()->get_cg_schema_with_iter_idx(idx, cg_schema_ptr))) {
        LOG_WARN("fail to get cg schema", K(ret), K(idx));
      } else if (OB_ISNULL(write_helper = alloc_helper<ObWriteHelper>(allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc write helper", K(ret));
      } else if (OB_ISNULL(merge_infos[idx])) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "merge info should not be null", K(ret), K(idx));
      } else if (OB_FAIL(write_helper->init(ctx, merge_param, parallel_idx, *cg_schema_ptr, *merge_infos[idx], allocator_))) {
        STORAGE_LOG(WARN, "fail to init write helper", K(ret));
      }
      if (FAILEDx(write_helpers_.push_back(write_helper))) {
        STORAGE_LOG(WARN, "fail to push back", K(ret), K(write_helpers_));
      } else {
        write_helper = nullptr;
      }

      if (OB_FAIL(ret) && write_helper != nullptr) {
        write_helper->~ObWriteHelper();
        allocator_.free(write_helper);
        write_helper = nullptr;
      }
    }
  }

  for (int64_t i = 0, sstable_idx = tables.count() - 1; OB_SUCC(ret) && i < tables.count(); ++i, --sstable_idx) {
    if (OB_FAIL(init_merge_iter(
        merge_param, full_read_info, nullptr/*cg_schema*/, tables.at(i), sstable_idx,
        false/*add_column*/, false/*need_project*/, true/*need_full_merge*/))) {
      LOG_WARN("failed to init merge iter", K(ret), K(tables.at(i)));
    }
  }

  if (FAILEDx(basic_init(
                default_row, merge_param,
                full_column_cnt + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
    LOG_WARN("fail to basic init", K(ret), K(merge_param), K(full_column_cnt), K(tables));
  } else {
    FLOG_INFO("succ to init ObCOMergeSingleWriter", K(ret), K(parallel_idx), K(start_cg_idx_), K(end_cg_idx_), K(tables));
  }
  return ret;
}

int ObCOMergeSingleWriter::process(const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!row.is_valid() || row.row_flag_.is_delete() || row.row_flag_.is_not_exist())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row", K(ret), K(row));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < write_helpers_.count(); i++) {
      ObWriteHelper *write_helper = write_helpers_.at(i);
      if (ignore_base_cg_ && i + start_cg_idx_ == base_cg_idx_ && nullptr == write_helper) {
      } else if (OB_UNLIKELY(write_helper == nullptr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null write helper", K(ret), K(write_helper));
      } else if (OB_FAIL(write_helper->append(row))) {
        STORAGE_LOG(WARN, "failed to project row", K(ret), K(i), K(row), K(write_helper));
      }
    }
  }

  return ret;
}

int ObCOMergeSingleWriter::end_write(ObCOTabletMergeCtx &co_ctx)
{
  int ret = OB_SUCCESS;
  ObTabletMergeInfo **merge_infos = co_ctx.cg_merge_info_array_;
  if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObCOMergeWriter not init", K(ret));
  } else if (OB_UNLIKELY((end_cg_idx_ - start_cg_idx_) != write_helpers_.count() || nullptr == merge_infos)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid count or unexpected null merge info array", K(ret),
      K(write_helpers_.count()), K(start_cg_idx_), K(end_cg_idx_), K(merge_infos));
  } else if (OB_FAIL(replay_last_skip_major(-1))) {
    LOG_WARN("failed to replay last skip major", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < write_helpers_.count(); i++) {
      ObWriteHelper *write_helper = write_helpers_.at(i);
      if (ignore_base_cg_ && i + start_cg_idx_ == base_cg_idx_ && nullptr == write_helper) {
      } else if (OB_UNLIKELY(write_helper == nullptr || merge_infos[i + start_cg_idx_] == nullptr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null write helper", K(ret), K(write_helper), K(merge_infos[i + start_cg_idx_]));
      } else if (OB_FAIL(write_helper->end_write(*merge_infos[i + start_cg_idx_]))) {
        STORAGE_LOG(WARN, "fail to close", K(ret), K(i), K(write_helper));
      }
    }
  }

  if (OB_SUCC(ret)) {
    dump_info();
  }
  return ret;
}
} //compaction
} //oceanbase
