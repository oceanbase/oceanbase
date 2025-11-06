/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "ob_column_oriented_merger.h"

namespace oceanbase
{
namespace compaction
{
/**
 * ---------------------------------------------------------ObCOMergeLogBuilder--------------------------------------------------------------
 */
int ObCOMergeLogBuilder::init(ObBasicTabletMergeCtx &ctx, const int64_t idx, const int64_t cg_idx)
{
  UNUSED(cg_idx);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(basic_prepare(ctx, idx))) {
    LOG_WARN("failed to do basic prepare", K(ret));
  } else if (OB_FAIL(inner_init())) {
    LOG_WARN("failed to inner init", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

#define ALLOC_AND_INIT_MERGE_HELPER(merge_helper, T, ...) \
do { \
  merge_helper = OB_NEWx(T, (&merger_arena_), __VA_ARGS__); \
  if (OB_ISNULL(merge_helper)) { \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    LOG_WARN("Failed to allocate memory for merge helper", K(ret)); \
  } else if (OB_FAIL(merge_helper->init(merge_param_))) { \
    LOG_WARN("Failed to init merge helper", K(ret)); \
  } \
} while(0)

int ObCOMergeLogBuilder::inner_init()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable*, 8> tables;
  if (OB_UNLIKELY(!merge_param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_param not valid", K(ret), K(merge_param_));
  } else if (OB_FAIL(get_all_majors(tables))) {
    LOG_WARN("failed to get all majors", K(ret));
  } else if (OB_FAIL(init_majors_merge_helper())) {
    LOG_WARN("failed to init majors merge helper", K(ret));
  } else if (OB_FAIL(alloc_base_writer(tables))) {
    LOG_WARN("failed to alloc base writer", K(ret));
  } else {
    ALLOC_AND_INIT_MERGE_HELPER(merge_helper_, ObCOMinorSSTableMergeIter, merge_ctx_->read_info_, merger_arena_, *partition_fuser_);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(cmp_ = OB_NEWx(ObPartitionMergeLoserTreeCmp, (&merger_arena_),
                merge_ctx_->read_info_.get_datum_utils(), merge_ctx_->read_info_.get_schema_rowkey_count()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    }
  }
  return ret;
}

int ObCOMergeLogBuilder::init_majors_merge_helper()
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx *ctx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_);
  const ObStorageColumnGroupSchema *cg_schema_ptr = nullptr;
  if (OB_FAIL(ctx->get_cg_schema_for_merge(ctx->base_rowkey_cg_idx_, cg_schema_ptr))) {
    LOG_WARN("fail to get cg schema for merge", K(ret), K(ctx->base_rowkey_cg_idx_));
  } else {
    bool replay_all_cg_directly = need_replay_base_cg_ && (cg_schema_ptr->is_all_column_group() || ctx->is_build_row_store());
    const ObITableReadInfo *read_info = merge_param_.cg_rowkey_read_info_;
    if (replay_all_cg_directly) {
      read_info = ctx->get_full_read_info();
    }
    ALLOC_AND_INIT_MERGE_HELPER(majors_merge_iter_, ObMultiMajorMergeIter, *read_info, merger_arena_, need_replay_base_cg_);
  }
  return ret;
}

int ObCOMergeLogBuilder::alloc_base_writer(ObIArray<ObITable*> &tables)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow default_row;
  base_writer_ = nullptr;
  ObCOTabletMergeCtx *ctx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_);
  ObTabletMergeInfo **merge_infos = ctx->cg_merge_info_array_;
  int64_t base_cg_idx = ctx->base_rowkey_cg_idx_;
  if (!need_replay_base_cg_) {
  } else if (OB_ISNULL(merge_infos) || OB_ISNULL(merge_infos[base_cg_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null merge info", K(ret), K(base_cg_idx), K(merge_infos));
  } else if (OB_FAIL(ObCOMergeWriter::init_default_row(merger_arena_, merge_param_, *merge_infos[base_cg_idx], default_row))) {
    LOG_WARN("Failed to init default row", K(ret));
  } else if (FALSE_IT(merge_param_.error_location_ = &ctx->info_collector_.error_location_)) {
  } else if (OB_FAIL(alloc_row_writer(base_cg_idx, default_row, tables, base_writer_))) {
    LOG_WARN("Failed to alloc row writer", K(ret));
  }
  return ret;
}

// replay base cg directly without replay task
int ObCOMergeLogBuilder::replay_base_cg(
    const ObMergeLog &merge_log,
    const blocksstable::ObDatumRow *&row,
    ObMergeIter *iter)
{
  int ret = OB_SUCCESS;
  if (nullptr != base_writer_ && merge_log.is_valid()) {
    base_writer_->set_merge_iter(iter);
    if (OB_FAIL(base_writer_->ObCOMergeWriter::replay_mergelog(merge_log, row))) {
      LOG_WARN("failed to replay merge log", K(ret), K(merge_log), K(row));
    }
  }
  return ret;
}

void ObCOMergeLogBuilder::reset()
{
  is_inited_ = false;
  if (OB_NOT_NULL(cmp_)) {
    cmp_->~ObPartitionMergeLoserTreeCmp();
    merger_arena_.free(cmp_);
    cmp_ = nullptr;
  }
  if (OB_NOT_NULL(majors_merge_iter_)) {
    majors_merge_iter_->~ObMultiMajorMergeIter();
    merger_arena_.free(majors_merge_iter_);
    majors_merge_iter_ = nullptr;
  }
  if (OB_NOT_NULL(base_writer_)) {
    base_writer_->~ObCOMergeBaseRowWriter();
    merger_arena_.free(base_writer_);
    base_writer_ = nullptr;
  }
  FLOG_INFO("ObCOMergeLogBuilder reset", K_(time_guard));
  ObMerger::reset();
}

int ObCOMergeLogBuilder::move_iters_next(bool &is_single_iter_end)
{
  int ret = OB_SUCCESS;
  is_single_iter_end = false;
#define MOVE_ITER_NEXT(flag, iter) \
do { \
  if (flag) { \
    if (OB_FAIL(iter->next())) { \
      if (OB_ITER_END == ret) { \
        ret = OB_SUCCESS; \
      } else { \
        LOG_WARN("failed to move next", K(ret), KPC(iter)); \
      } \
    } \
    flag = false; \
  } \
} while (0)
  ObCOMinorSSTableMergeIter *inc_iter = static_cast<ObCOMinorSSTableMergeIter*>(merge_helper_);
  if (OB_SUCC(ret)) {
    MOVE_ITER_NEXT(need_move_minor_iter_, inc_iter);
  }
  if (OB_SUCC(ret)) {
    MOVE_ITER_NEXT(need_move_major_iter_, majors_merge_iter_);
  }
  if (OB_SUCC(ret)) {
    if (inc_iter->is_iter_end() && majors_merge_iter_->is_iter_end()) {
      ret = OB_ITER_END;
    } else if (inc_iter->is_iter_end() || majors_merge_iter_->is_iter_end()) {
      is_single_iter_end = true;
    }
  }
  return ret;
}

int ObCOMergeLogBuilder::get_next_log(ObMergeLog &mergelog, const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_single_iter_end = false;
  mergelog.reset();
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogBuilder not init", K(ret));
  } else {
    time_guard_.set_last_click_ts(common::ObTimeUtility::current_time());
    int64_t cmp_ret = 0;
    ObMergeLog::OpType op;
    ObCOMinorSSTableMergeIter *incre_iter = static_cast<ObCOMinorSSTableMergeIter*>(merge_helper_);
    ObPartitionMergeIter *row_store_iter = nullptr;
    // move iter next first to prevent inc row changes,
    // cause fuser will not deep copy inc row
    if (OB_FAIL(move_iters_next(is_single_iter_end))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to move incre iter next", K(ret));
      }
    }
    time_guard_.click(ObCOMergeTimeGuard::MOVE_NEXT);
    // decide merge log
    if (OB_FAIL(ret)) {
    } else if (is_single_iter_end && OB_FAIL(handle_single_iter_end(mergelog, row))) {
      LOG_WARN("failed to handle single iter end", K(ret));
    } else if (!is_single_iter_end) {
      if (OB_FAIL(incre_iter->get_curr_row(row))) {
        LOG_WARN("failed to get curr row", K(ret), K(row));
      } else if (OB_FAIL(majors_merge_iter_->get_current_major_iter(row_store_iter))) {
        LOG_WARN("failed to get current major iter", K(ret));
      } else if (OB_FAIL(compare(*row, *row_store_iter, cmp_ret))) {
        LOG_WARN("failed to compare iter", K(ret), K(*row), KPC(row_store_iter));
      } else if (FALSE_IT(time_guard_.click(ObCOMergeTimeGuard::COMPARE))) {
      } else if (cmp_ret == 0) { // inc row == major row // fuse
        op = row->row_flag_.is_delete() ? ObMergeLog::DELETE : ObMergeLog::UPDATE;
        set_need_move_flag(true/*need_move_minor_iter*/, true/*need_move_major_iter*/);
      } else if (cmp_ret < 0) { // inc row < major row/range, insert inc row
        op = ObMergeLog::INSERT;
        set_need_move_flag(true/*need_move_minor_iter*/, false/*need_move_major_iter*/);
      } else if (cmp_ret > 0) { // major row/range > inc row, replay major
        op = ObMergeLog::REPLAY;
        set_need_move_flag(false/*need_move_minor_iter*/, true/*need_move_major_iter*/);
      }
      // build merge log
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(build_merge_log(op, row_store_iter, row, mergelog))) {
        LOG_WARN("failed to build merge log", K(ret), K(op));
      }
    }
  }
  return ret;
}

int ObCOMergeLogBuilder::build_merge_log(
    ObMergeLog::OpType op_type,
    ObPartitionMergeIter *row_store_iter,
    const blocksstable::ObDatumRow *row,
    ObMergeLog &mergelog,
    const bool replay_to_end)
{
  int ret = OB_SUCCESS;
  int64_t row_id = 0;
  if (mergelog.is_valid()) {
  } else {
    if (ObMergeLog::REPLAY == op_type) {
      if (replay_to_end) { // no inc and only one major, replay last major to the end
        mergelog.set_value(ObMergeLog::REPLAY, row_store_iter->get_major_idx(), INT64_MAX);
        majors_merge_iter_->move_to_end();
        const ObITable *table = nullptr;
        if (OB_NOT_NULL(table = row_store_iter->get_table())) {
          FLOG_INFO("major iter move to the end promptly",
              "row_count", row_store_iter->get_iter_row_count(),
              "ghost_row_count", row_store_iter->get_ghost_row_count(),
              "table_key", table->get_key());
        }
      } else if (nullptr != row_store_iter->get_curr_row() &&
          OB_FAIL(row_store_iter->get_curr_row_id(row_id))) {
        STORAGE_LOG(WARN, "failed to get_curr_row_id", K(ret), KPC(row_store_iter));
      } else if (nullptr == row_store_iter->get_curr_row() &&
          OB_FAIL(row_store_iter->get_curr_range_end_rowid(row_id))) {
        STORAGE_LOG(WARN, "failed to get_curr_range_end_rowid", K(ret), KPC(row_store_iter));
      } else {
        mergelog.set_value(op_type, row_store_iter->get_major_idx(), row_id);
      }
    } else if (ObMergeLog::DELETE == op_type || ObMergeLog::UPDATE == op_type) {
      if (OB_FAIL(row_store_iter->get_curr_row_id(row_id))) {
        STORAGE_LOG(WARN, "failed to get_curr_row_id", K(ret), KPC(row_store_iter));
      } else {
        mergelog.set_value(op_type, row_store_iter->get_major_idx(), row_id);
      }
    } else if (ObMergeLog::INSERT == op_type) {
      mergelog.set_value(ObMergeLog::INSERT, -1/*major_idx*/, INT64_MAX/*row_id*/);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected op_type", K(ret), K(op_type));
    }
    time_guard_.click(ObCOMergeTimeGuard::BUILD_LOG);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(replay_base_cg(mergelog, row, row_store_iter))) {
      LOG_WARN("failed to replay base cg directly", K(ret), K(mergelog), K(row));
    } else if (FALSE_IT(time_guard_.click(ObCOMergeTimeGuard::REPLAY_BASE_CG))) {
    } else {
      LOG_TRACE("success to build merge log", K(mergelog), KPC(row));
    }
  }
  return ret;
}

int ObCOMergeLogBuilder::handle_single_iter_end(
    ObMergeLog &mergelog,
    const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  ObCOMinorSSTableMergeIter *incre_iter = static_cast<ObCOMinorSSTableMergeIter*>(merge_helper_);
  ObPartitionMergeIter *row_store_iter = nullptr;
  bool replay_to_end = false;
  ObMergeLog::OpType op = ObMergeLog::INVALID;
  if (majors_merge_iter_->is_iter_end()) {
    if (OB_FAIL(incre_iter->get_curr_row(row))) {
      LOG_WARN("failed to get curr row", K(ret), K(row));
    } else {
      op = ObMergeLog::INSERT;
      set_need_move_flag(true/*need_move_minor_iter*/, false/*need_move_major_iter*/);
    }
  } else if (incre_iter->is_iter_end()) {
    if (OB_FAIL(majors_merge_iter_->get_current_major_iter(row_store_iter))) {
      LOG_WARN("failed to get current major iter", K(ret));
    } else {
      op = ObMergeLog::REPLAY;
      replay_to_end = majors_merge_iter_->check_could_move_to_end();
      set_need_move_flag(false/*need_move_minor_iter*/, true/*need_move_major_iter*/);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no iter end", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_merge_log(op, row_store_iter, row, mergelog, replay_to_end))) {
    LOG_WARN("failed to build merge log", K(ret), K(op));
  }
  return ret;
}

int ObCOMergeLogBuilder::compare(
    const blocksstable::ObDatumRow &left,
    ObPartitionMergeIter &row_store_iter,
    int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObDatumRow *right_row = nullptr;

  if (OB_UNLIKELY(nullptr == cmp_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null cmp", K(ret));
  } else if (OB_UNLIKELY(row_store_iter.is_iter_end())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected row store iter has ended", K(ret));
  } else {
    blocksstable::ObDatumRange range;

    while (OB_SUCC(ret) && OB_ISNULL(right_row = row_store_iter.get_curr_row())) {
      if (OB_FAIL(row_store_iter.get_curr_range(range))) {
        STORAGE_LOG(WARN, "Failed to get curr range", K(ret), K(row_store_iter));
      } else if (OB_FAIL(cmp_->compare_hybrid(left, range, cmp_ret))) {
        STORAGE_LOG(WARN, "Failed to compare hybrid", K(ret), K(left), K(range));
      } else if (!cmp_->check_cmp_finish(cmp_ret)) {
        if (OB_UNLIKELY(!cmp_->need_open_right_range(cmp_ret))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected cmp ret", K(ret), K(cmp_ret));
        } else if (OB_FAIL(row_store_iter.open_curr_range(false))) {
          STORAGE_LOG(WARN, "Failed to open iter curr range", K(ret));
        }
      } else {
        break; //cmp finish, break while
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(right_row)) {
    if (OB_FAIL(cmp_->compare_rowkey(left, *right_row, cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(left), KPC(right_row));
    }
  }

  return ret;
}

int ObCOMergeLogBuilder::close()
{
  int ret = OB_SUCCESS;
  if (nullptr != base_writer_) {
    ObTabletMergeInfo **merge_infos = static_cast<ObCOTabletMergeCtx *>(merge_ctx_)->cg_merge_info_array_;
    const int64_t base_cg_idx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_)->base_rowkey_cg_idx_;
    if (OB_UNLIKELY(nullptr == merge_infos || nullptr == merge_infos[base_cg_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null merge info array", K(ret), K(merge_infos));
    } else if (OB_FAIL(base_writer_->end_write(*merge_infos[base_cg_idx]))) {
      LOG_WARN("failed to close writer", K(ret), KPC(base_writer_));
    } else {
      base_writer_->~ObCOMergeBaseRowWriter();
      merger_arena_.free(base_writer_);
      base_writer_ = nullptr;
    }
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObCOMinorSSTableMergeIter--------------------------------------------------------------
 */
int ObCOMinorSSTableMergeIter::next()
{
  int ret = OB_SUCCESS;
  if (is_iter_end()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(move_iters_next(minimum_iters_))) {
    LOG_WARN("failed to move iters next", K(ret), K(minimum_iters_));
  } else if (OB_FAIL(rebuild_rows_merger())) {
    LOG_WARN("failed to rebuild rows merger", K(ret));
  } else {
    curr_row_ = nullptr;
    minimum_iters_.reset();
  }
  return ret;
}

int ObCOMinorSSTableMergeIter::get_curr_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (is_iter_end()) {
  } else {
    if (nullptr == curr_row_) {
      if (OB_FAIL(find_rowkey_minimum_iters(minimum_iters_))) {
        LOG_WARN("failed to find_rowkey_minimum_iters", K(ret));
      } else if (OB_UNLIKELY(minimum_iters_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty minimum iters", K(ret));
      } else if (1 == minimum_iters_.count()) {
        curr_row_ = minimum_iters_.at(0)->get_curr_row();
      } else if (OB_FAIL(partition_fuser_.fuse_row(minimum_iters_))) {
        LOG_WARN("failed to fuse row", K(ret), K(minimum_iters_));
      } else {
        curr_row_ = &(partition_fuser_.get_result_row());
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      row = curr_row_;
    }
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObCOMergeLogReplayer--------------------------------------------------------------
 */
ObCOMergeLogReplayer::ObCOMergeLogReplayer(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param,
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    const bool use_row_to_build_column)
  : ObMergerBasic(allocator, static_param),
    is_inited_(false),
    mergelog_iter_(nullptr),
    merge_writers_(OB_MALLOC_NORMAL_BLOCK_SIZE, merger_arena_),
    start_cg_idx_(start_cg_idx),
    end_cg_idx_(end_cg_idx),
    use_row_to_build_column_(use_row_to_build_column),
    need_replay_base_cg_(false),
    trans_state_mgr_(merger_arena_),
    time_guard_()
{}

void ObCOMergeLogReplayer::reset()
{
  ObMergerBasic::reset();
  if (OB_NOT_NULL(mergelog_iter_)) {
    mergelog_iter_->~ObCOMergeLogIterator();
    merger_arena_.free(mergelog_iter_);
    mergelog_iter_ = nullptr;
  }
  for (int64_t i = 0; i < merge_writers_.count(); i++) {
    if (OB_NOT_NULL(merge_writers_.at(i))) {
      ObCOMergeWriter *&writer = merge_writers_.at(i);
      writer->~ObCOMergeWriter();
      writer = nullptr;
    }
  }
  FLOG_INFO("ObCOMergeLogReplayer reset", K_(time_guard));
  merge_writers_.reset();
  is_inited_ = false;
}

int ObCOMergeLogReplayer::init(ObBasicTabletMergeCtx &ctx, const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx &co_ctx = static_cast<ObCOTabletMergeCtx &>(ctx);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    // for with tmp file, need_replay_base_cg means base cg has been replayed directly in persist task
    // for without tmp file, need_replay_base_cg means base cg will be replayed directly in mergelog build process
    need_replay_base_cg_ = co_ctx.need_replay_base_directly_
                           && co_ctx.base_rowkey_cg_idx_ >= start_cg_idx_
                           && co_ctx.base_rowkey_cg_idx_ < end_cg_idx_;
    SET_MEM_CTX(ctx.mem_ctx_);
    if (OB_FAIL(basic_prepare(ctx, idx))) {
      LOG_WARN("failed to basic prepare", K(ret));
    } else if (OB_FAIL(init_mergelog_iter(ctx))) {
      LOG_WARN("failed to init merge log iter", K(ret));
    } else if (OB_FAIL(inner_init())) {
      LOG_WARN("failed to inner init", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObCOMergeLogReplayer::inner_init()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable*, 8> tables;
  if (OB_UNLIKELY(!merge_param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_param not valid", K(ret), K(merge_param_));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans_state_mgr_.init(ObMergerBasic::CACHED_TRANS_STATE_MAX_CNT))) {
      STORAGE_LOG(WARN, "failed to init merge trans state mgr", K(tmp_ret));
    } else {
      merge_param_.trans_state_mgr_ = &trans_state_mgr_;
    }
    if (FAILEDx(get_all_majors(tables))) {
      LOG_WARN("failed to get all majors", K(ret));
    } else if (OB_FAIL(init_cg_writers(tables))) {
      LOG_WARN("failed to init cg writers", K(ret), K(tables));
    }
  }
  return ret;
}

int ObCOMergeLogReplayer::init_mergelog_iter(ObBasicTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx &co_ctx = static_cast<ObCOTabletMergeCtx &>(ctx);
  if (start_cg_idx_ >= end_cg_idx_
     || co_ctx.array_count_ < end_cg_idx_
     || (co_ctx.is_using_column_tmp_file() && start_cg_idx_ + 1 != end_cg_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect cg idx", K(ret), K(start_cg_idx_), K(end_cg_idx_), K(co_ctx.array_count_), K(co_ctx.merge_log_storage_));
  } else if (need_replay_base_cg_ && co_ctx.is_using_column_tmp_file()) { // no base cg row file // no merge log iter
  } else {
    if (co_ctx.is_using_tmp_file()) {
      mergelog_iter_ = OB_NEWx(ObCOMergeLogFileReader, (&merger_arena_), merger_arena_);
    } else {
      mergelog_iter_ = OB_NEWx(ObCOMergeLogBuilder, (&merger_arena_), merger_arena_, merge_param_.static_param_, need_replay_base_cg_);
    }
    if (OB_ISNULL(mergelog_iter_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new ObCOMergeLogIterator", K(ret));
    } else if (OB_FAIL(mergelog_iter_->init(ctx, task_idx_, co_ctx.is_using_column_tmp_file() ? start_cg_idx_ : 0/*cg_idx*/))) {
      LOG_WARN("failed to init ObCOMergeLogIterator", K(ret));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(mergelog_iter_)) {
    mergelog_iter_->~ObCOMergeLogIterator();
    merger_arena_.free(mergelog_iter_);
    mergelog_iter_ = nullptr;
  }
  return ret;
}

int ObCOMergeLogReplayer::replay_merge_log()
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogReplayer not init", K(ret));
  } else if (OB_ISNULL(ctx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctx", K(ret));
  } else if (nullptr == mergelog_iter_) { // base cg with column tmp file, do nothing
  } else {
    const int64_t repalyed_idx = need_replay_base_cg_ ? ctx->base_rowkey_cg_idx_ - start_cg_idx_ : -1;
    ObCOMergeLogReplayerCallback callback(merge_writers_, repalyed_idx, time_guard_);
    if (OB_FAIL(ObCOMergeLogConsumer<ObCOMergeLogReplayerCallback>::consume_all_merge_log(*mergelog_iter_, callback))) {
      LOG_WARN("failed to consume all merge log", K(ret));
    } else if (OB_FAIL(close())) {
      LOG_WARN("Failed to close ObCOMergeLogReplayer", K(ret));
    }
  }
  return ret;
}

int ObCOMergeLogReplayer::ObCOMergeLogReplayerCallback::consume(
    const ObMergeLog &log,
    const blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  time_guard_.set_last_click_ts(common::ObTimeUtility::current_time());
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_writers_.count(); ++i) {
    if (OB_ISNULL(merge_writers_.at(i))) {
      if (replayed_idx_ == i) { // replayed base cg, just do nothing
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("merge writer is null", K(ret), K(i));
      }
    } else if (OB_FAIL(merge_writers_.at(i)->replay_mergelog(log, row))) {
      LOG_WARN("fail to replay merge log", K(ret), K(i));
    }
  }
  time_guard_.click(ObCOMergeTimeGuard::REPLAY_LOG);
  return ret;
}

int ObCOMergeLogReplayer::close()
{
  int ret = OB_SUCCESS;
  compaction::ObCOMergeWriter *writer = nullptr;
  ObCOTabletMergeCtx *ctx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_);
  ObTabletMergeInfo **merge_infos = ctx->cg_merge_info_array_;
  const int64_t base_cg_idx = ctx->base_rowkey_cg_idx_;
  if (OB_UNLIKELY(nullptr == merge_infos)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid count or unexpected null merge info array", K(ret), K(merge_infos));
  } else if (use_row_to_build_column_) {
    if (OB_ISNULL(writer = merge_writers_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null writer", K(ret));
    } else if (OB_FAIL(writer->end_write(merge_infos))) {
      STORAGE_LOG(WARN, "fail to end writer", K(ret), KPC(writer));
    }
  } else if (OB_UNLIKELY((end_cg_idx_ - start_cg_idx_) != merge_writers_.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid merge_writers_ count ", K(ret), K(merge_writers_), K(start_cg_idx_), K(end_cg_idx_));
  } else {
    for (int64_t i = start_cg_idx_; OB_SUCC(ret) && i < end_cg_idx_; i++) {
      writer = merge_writers_.at(i - start_cg_idx_);
      if (nullptr == writer && i == base_cg_idx && need_replay_base_cg_) {
      } else if (OB_UNLIKELY(nullptr == writer || nullptr == merge_infos[i])) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null writer", K(ret), K(i), K(merge_writers_));
      } else if (OB_FAIL(writer->end_write(*merge_infos[i]))) {
        STORAGE_LOG(WARN, "failed to close writer", K(ret), KPC(writer));
      }
    }
  }
  return ret;
}

int ObCOMergeLogReplayer::init_cg_writers(ObIArray<ObITable*> &tables)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow default_row;
  ObCOTabletMergeCtx *ctx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_);
  ObTabletMergeInfo **merge_infos = ctx->cg_merge_info_array_;
  const common::ObIArray<ObStorageColumnGroupSchema> &cg_array = ctx->get_schema()->get_column_groups();

  if (OB_UNLIKELY(tables.empty() || nullptr == merge_infos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sstable", K(ret), KP(merge_infos), K(tables));
  } else if (OB_UNLIKELY(end_cg_idx_ <= start_cg_idx_
      || ctx->array_count_ < end_cg_idx_
      || cg_array.count() != ctx->array_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid merge batch count", K(ret), K(ctx->array_count_), K(start_cg_idx_), K(cg_array.count()));
  } else if (OB_ISNULL(merge_infos[start_cg_idx_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null merge info", K(ret), K(start_cg_idx_));
  } else if (OB_FAIL(ObCOMergeWriter::init_default_row(
      merger_arena_, merge_param_, *merge_infos[start_cg_idx_], default_row))) {
    LOG_WARN("failed to init_default_row", K(ret));
  } else if (FALSE_IT(merge_param_.error_location_ = &ctx->info_collector_.error_location_)) {
  } else if (OB_FAIL(alloc_writers(default_row, cg_array, merge_infos, tables))) {
    LOG_WARN("fail to alloc writers", K(ret), K(cg_array), K(merge_infos), K(tables), K(default_row));
  }
  return ret;
}

int ObCOMergeLogReplayer::alloc_writers(
    const blocksstable::ObDatumRow &default_row,
    const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
    ObTabletMergeInfo **merge_infos,
    ObIArray<ObITable*> &tables)
{
  int ret = OB_SUCCESS;
  if (!use_row_to_build_column_ && OB_FAIL(alloc_row_writers(default_row, merge_infos, tables))) {
    LOG_WARN("Failed to allocate ObCOMergeRowWriter", K(ret));
  } else if (use_row_to_build_column_ && OB_FAIL(alloc_single_writer(default_row, cg_array, merge_infos, tables))) {
    LOG_WARN("Failed to allocate ObCOMergeSingleWriter", K(ret));
  }
  return ret;
}

int ObCOMergeLogReplayer::alloc_single_writer(
    const blocksstable::ObDatumRow &default_row,
    const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
    ObTabletMergeInfo **merge_infos,
    ObIArray<ObITable*> &tables)
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx *ctx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_);
  ObCOMergeWriter *writer = nullptr;
  const bool need_co_scan = ctx->contain_rowkey_base_co_sstable();
  if (OB_ISNULL(writer = OB_NEWx(ObCOMergeSingleWriter, &merger_arena_, start_cg_idx_, end_cg_idx_, need_co_scan))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for ObCOMergeSingleWriter", K(ret));
  } else if (OB_FAIL(writer->init(*merge_ctx_, default_row, merge_param_, ctx->get_full_read_info(), task_idx_,
      cg_array, merge_infos, tables))) {
    LOG_WARN("fail to init writer", K(ret));
  } else if (OB_FAIL(merge_writers_.push_back(writer))) {
    LOG_WARN("failed to push writer", K(ret), K(merge_writers_));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(writer)) {
    writer->~ObCOMergeWriter();
    merger_arena_.free(writer);
    writer = nullptr;
  }
  return ret;
}

int ObCOMergeLogReplayer::alloc_row_writers(
    const blocksstable::ObDatumRow &default_row,
    ObTabletMergeInfo **merge_infos,
    ObIArray<ObITable*> &tables)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_major = static_cast<ObSSTable *>(tables.at(0));
  ObCOSSTableV2 &co_sstable = static_cast<ObCOSSTableV2 &>(*base_major);
  ObCOTabletMergeCtx *ctx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_);
  const int64_t base_cg_idx = ctx->base_rowkey_cg_idx_;
  if (ctx->should_mock_row_store_cg_schema()) {
    if (OB_UNLIKELY((co_sstable.is_rowkey_cg_base() && !ctx->is_build_row_store_from_rowkey_cg())
                  || (co_sstable.is_all_cg_base() && !ctx->is_build_row_store()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid combination for co base type and merge type", K(ret), K(co_sstable), K(ctx->static_param_));
    } else if (OB_UNLIKELY((start_cg_idx_+1) != end_cg_idx_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid cg idx for mock row store cg schema", K(ret), K(start_cg_idx_), K(end_cg_idx_));
    } else {
      LOG_DEBUG("[RowColSwitch] mock row store cg", K(ctx->mocked_row_store_cg_));
    }
  }
  for (uint32_t idx = start_cg_idx_; OB_SUCC(ret) && idx < end_cg_idx_; idx++) {
    ObCOMergeRowWriter *writer = nullptr;
    if (need_replay_base_cg_ && idx == base_cg_idx) { // replayed base cg, only nullptr writer to placeholder
    } else if (OB_FAIL(alloc_row_writer(idx, default_row, tables, writer))) {
      LOG_WARN("failed to alloc row writer", K(ret), K(idx));
    }
    if (FAILEDx(merge_writers_.push_back(writer))) {
      LOG_WARN("failed to push back writer", K(ret));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(writer)) {
      writer->~ObCOMergeRowWriter();
      merger_arena_.free(writer);
      writer = nullptr;
    }
  } // for

  return ret;
}
/**
 * ---------------------------------------------------------ObCOMergeLogPersister--------------------------------------------------------------
 */
void ObCOMergeLogPersister::reset()
{
  is_inited_ = false;
  if (OB_NOT_NULL(mergelog_iter_)) {
    mergelog_iter_->~ObCOMergeLogIterator();
    merger_arena_.free(mergelog_iter_);
    mergelog_iter_ = nullptr;
  }
  FLOG_INFO("ObCOMergeLogPersister reset", K_(time_guard));
  mergelog_writer_.reset();
}

int ObCOMergeLogPersister::init(ObBasicTabletMergeCtx &ctx, const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx &co_ctx = static_cast<ObCOTabletMergeCtx &>(ctx);
  ObCOMergeLogFileMgr *mgr = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCOMergeLogPersister init twice", K(ret));
  } else if (OB_FAIL(co_ctx.init_merge_log_mgr(idx))) {
    LOG_WARN("fail to init merge log mgr", K(ret));
  } else if (OB_FAIL(init_mergelog_iter(ctx, idx))) {
    LOG_WARN("fail to init merge log iter", K(ret));
  } else if (OB_FAIL(mergelog_writer_.init(merger_arena_, ctx, idx))) {
    LOG_WARN("fail to init merge log writer", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    (void)co_ctx.destroy_merge_log_mgr(idx);
    reset();
  }
  return ret;
}

int ObCOMergeLogPersister::init_mergelog_iter(ObBasicTabletMergeCtx &ctx, const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx &co_ctx = static_cast<ObCOTabletMergeCtx &>(ctx);
  if (OB_ISNULL(mergelog_iter_ = OB_NEWx(ObCOMergeLogBuilder, (&merger_arena_),
      merger_arena_, ctx.static_param_, co_ctx.need_replay_base_directly_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObCOMergeLogBuilder", K(ret));
  } else if (OB_FAIL(mergelog_iter_->init(ctx, idx/*idx*/, 0/*cg_idx*/))) {
    LOG_WARN("fail to init merge log iter", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(mergelog_iter_)) {
    mergelog_iter_->~ObCOMergeLogIterator();
    merger_arena_.free(mergelog_iter_);
    mergelog_iter_ = nullptr;
  }
  return ret;
}

int ObCOMergeLogPersister::ObCOMergeLogPersisterCallback::consume(
    const ObMergeLog &log,
    const blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  time_guard_.set_last_click_ts(common::ObTimeUtility::current_time());
  if (OB_FAIL(mergelog_writer_.write_merge_log(log, row))) {
    LOG_WARN("fail to write log", K(ret));
  }
  time_guard_.click(ObCOMergeTimeGuard::PERSIST_LOG);
  return ret;
}

int ObCOMergeLogPersister::persist_merge_log()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMergeLogPersister is not inited", K(ret));
  } else {
    ObCOMergeLogPersisterCallback callback(mergelog_writer_, time_guard_);
    if (OB_FAIL(ObCOMergeLogConsumer<ObCOMergeLogPersisterCallback>::consume_all_merge_log(*mergelog_iter_, callback))) {
      LOG_WARN("failed to consume all merge log", K(ret));
    } else if (OB_FAIL(mergelog_writer_.close())) {
      LOG_WARN("fail to close merge log writer", K(ret));
    }
  }
  return ret;
}

} //compaction
} //oceanbase
