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
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/blocksstable/ob_sstable.h"
#include "ob_column_oriented_sstable.h"

namespace oceanbase
{
namespace compaction
{
/**
 * ---------------------------------------------------------ObCOMerger--------------------------------------------------------------
 */
ObCOMerger::ObCOMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param,
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    const bool only_use_row_table)
  : ObMerger(allocator, static_param),
    row_store_iter_(nullptr),
    merge_progress_(nullptr),
    merge_writers_(OB_MALLOC_NORMAL_BLOCK_SIZE, merger_arena_),
    cg_wrappers_(OB_MALLOC_NORMAL_BLOCK_SIZE, merger_arena_),
    cmp_(nullptr),
    start_cg_idx_(start_cg_idx),
    end_cg_idx_(end_cg_idx),
    only_use_row_table_(only_use_row_table)
{
}

void ObCOMerger::reset()
{
  if (OB_NOT_NULL(cmp_)) {
    cmp_ = nullptr;
    cmp_->~ObPartitionMergeLoserTreeCmp();
  }

  if (OB_NOT_NULL(row_store_iter_)) {
    row_store_iter_->~ObPartitionMergeIter();
    row_store_iter_ = nullptr;
  }

  for (int64_t i = 0; i < merge_writers_.count(); i++) {
    if (OB_NOT_NULL(merge_writers_.at(i))) {
      ObCOMergeWriter *&writer = merge_writers_.at(i);
      writer->~ObCOMergeWriter();
      writer = nullptr;
    }
  }
  merge_writers_.reset();
  cg_wrappers_.reset();
  ObMerger::reset();
}

int ObCOMerger::inner_prepare_merge(ObBasicTabletMergeCtx &ctx, const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  ObSSTable *sstable = nullptr;

  if (OB_UNLIKELY(!merge_param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "merge_param not valid", K(ret), K(merge_param_));
  } else if (OB_ISNULL(table = merge_param_.get_tables_handle().get_table(0))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null table", K(ret));
  } else if (!table->is_co_sstable()) {
    if (!table->is_major_sstable() || table->is_column_store_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "UNEXPECTED table type", K(ret), KPC(table));
    } else {
      only_use_row_table_ = true;
      STORAGE_LOG(INFO, "use major table for column store sstaable merge", K(ret), KPC(table));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null table", K(ret));
  } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
  } else if (OB_FAIL(init_merge_iters(sstable))) {
    STORAGE_LOG(WARN, "failed to init_merge_iters", K(ret));
  } else if (OB_FAIL(init_writers(sstable))) {
    STORAGE_LOG(WARN, "failed to init writers", K(ret));
  } else {
    merge_helper_ = OB_NEWx(ObCOMinorSSTableMergeHelper,
                            (&merger_arena_),
                            merge_ctx_->read_info_,
                            sstable->get_max_merged_trans_version(),
                            merger_arena_);

    if (OB_ISNULL(merge_helper_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory for partition helper", K(ret));
    } else if (OB_FAIL(merge_helper_->init(merge_param_))) {
      STORAGE_LOG(WARN, "Failed to init merge helper", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(cmp_ = OB_NEWx(ObPartitionMergeLoserTreeCmp, (&merger_arena_),
                merge_ctx_->read_info_.get_datum_utils(), merge_ctx_->read_info_.get_schema_rowkey_count()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", K(ret));
    }
  }
  return ret;
}

int ObCOMerger::init_merge_iters(ObSSTable *sstable)
{
  int ret = OB_SUCCESS;

  //prepare row_store_iter_
  if (OB_UNLIKELY(sstable == nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null sstable", K(ret));
  } else if (sstable->get_data_macro_block_count() <= 0) {
  } else {
    if (merge_param_.is_full_merge() || sstable->is_small_sstable()) {
      row_store_iter_ = OB_NEWx(ObPartitionRowMergeIter, (&merger_arena_), merger_arena_);
    } else if (MICRO_BLOCK_MERGE_LEVEL == merge_param_.static_param_.merge_level_) {
      row_store_iter_ = OB_NEWx(ObPartitionMicroMergeIter, (&merger_arena_), merger_arena_);
    } else {
      row_store_iter_ = OB_NEWx(ObPartitionMacroMergeIter, (&merger_arena_), merger_arena_);
    }

    if (OB_ISNULL(row_store_iter_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory for partition iter", K(ret));
    } else if (OB_FAIL(row_store_iter_->init(merge_param_, sstable, merge_param_.cg_rowkey_read_info_))) {
      STORAGE_LOG(WARN, "failed to init iter", K(ret), K(merge_param_), KPC(sstable));
    } else if (OB_FAIL(move_iter_next(*row_store_iter_))) {
      STORAGE_LOG(WARN, "failed to move row_store_iter next", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(row_store_iter_)) {
      row_store_iter_->~ObPartitionMergeIter();
      merger_arena_.free(row_store_iter_);
      row_store_iter_ = nullptr;
    }
  }

  return ret;
}

int ObCOMerger::init_writers(ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow default_row;
  ObCOTabletMergeCtx *ctx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_);
  ObTabletMergeInfo **merge_infos = ctx->cg_merge_info_array_;
  const common::ObIArray<ObStorageColumnGroupSchema> &cg_array = ctx->static_param_.schema_->get_column_groups();

  if (OB_UNLIKELY(sstable == nullptr || nullptr == merge_infos)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null sstable", K(ret), KP(merge_infos), KPC(sstable));
  } else if (OB_UNLIKELY(end_cg_idx_ <= start_cg_idx_
      || ctx->array_count_ < end_cg_idx_
      || cg_array.count() != ctx->array_count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid merge batch count", K(ret), K(ctx->array_count_), K(end_cg_idx_),
        K(start_cg_idx_), K(cg_array.count()));
  } else if (OB_FAIL(default_row.init(merger_arena_, merge_param_.static_param_.multi_version_column_descs_.count()))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else if (OB_FAIL(merge_param_.static_param_.schema_->get_orig_default_row(merge_param_.static_param_.multi_version_column_descs_,
      merge_infos[start_cg_idx_]->get_sstable_build_desc().get_static_desc().major_working_cluster_version_ >= DATA_VERSION_4_3_1_0, default_row))) {
    STORAGE_LOG(WARN, "Failed to get default row from table schema", K(ret), K(merge_param_.static_param_.multi_version_column_descs_));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(merger_arena_, merge_param_.static_param_.multi_version_column_descs_, default_row))) {
    STORAGE_LOG(WARN, "fail to fill lob header for default row", K(ret));
  } else if (FALSE_IT(default_row.row_flag_.set_flag(ObDmlFlag::DF_UPDATE))) {
  } else if (FALSE_IT(merge_param_.error_location_ = &ctx->info_collector_.error_location_)) {
  } else if (OB_FAIL(alloc_writers(default_row, cg_array, merge_infos, *sstable))) {
    STORAGE_LOG(WARN, "fail to alloc writers", K(ret), K(cg_array), K(merge_infos), KPC(sstable), K(default_row));
  }
  return ret;
}

int ObCOMerger::alloc_writers(
    const blocksstable::ObDatumRow &default_row,
    const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
    ObTabletMergeInfo **merge_infos,
    ObSSTable &sstable)
{
  int ret = OB_SUCCESS;

  if (only_use_row_table_) {
    if (OB_FAIL(alloc_single_writer(default_row, cg_array, merge_infos, sstable))) {
      STORAGE_LOG(WARN, "Failed to allocate ObCOMergeSingleWriter", K(ret));
    }
  } else if (OB_UNLIKELY(!sstable.is_co_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected sstable", K(ret), K(sstable));
  } else if (OB_FAIL(alloc_row_writers(default_row, cg_array, merge_infos, sstable))) {
    STORAGE_LOG(WARN, "Failed to allocate ObCOMergeRowWriter", K(ret));
  }

  return ret;
}

int ObCOMerger::alloc_single_writer(
    const blocksstable::ObDatumRow &default_row,
    const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
    ObTabletMergeInfo **merge_infos,
    ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObCOMergeWriter *writer = nullptr;
  if (OB_ISNULL(writer = OB_NEWx(ObCOMergeSingleWriter, &merger_arena_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to allocate memory for ObCOMergeSingleWriter", K(ret));
  } else if (OB_FAIL(writer->init(default_row, merge_param_, &merge_ctx_->read_info_, task_idx_,
      cg_array, start_cg_idx_, end_cg_idx_, merge_infos, is_empty_table(sstable) ? nullptr : &sstable))) {
    STORAGE_LOG(WARN, "fail to init writer", K(ret));
  } else if (OB_FAIL(merge_writers_.push_back(writer))) {
    STORAGE_LOG(WARN, "failed to push writer", K(ret), K(merge_writers_));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(writer)) {
    writer->~ObCOMergeWriter();
    merger_arena_.free(writer);
    writer = nullptr;
  }
  return ret;
}

int ObCOMerger:: alloc_row_writers(
    const blocksstable::ObDatumRow &default_row,
    const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
    ObTabletMergeInfo **merge_infos,
    ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObCOMergeWriter *writer = nullptr;
  const ObStorageColumnGroupSchema *cg_schema_ptr = nullptr;
  ObCOSSTableV2 &co_sstable = static_cast<ObCOSSTableV2 &>(sstable);
  ObCOTabletMergeCtx *ctx = static_cast<ObCOTabletMergeCtx *>(merge_ctx_);
  const bool empty_table = is_empty_table(sstable);

  if (ctx->should_mock_row_store_cg_schema()) {
    if ((OB_UNLIKELY((co_sstable.is_rowkey_cg_base() && !ctx->is_build_row_store_from_rowkey_cg())
                  || (co_sstable.is_all_cg_base() && !ctx->is_build_row_store())))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid combination for co base type and merge type", K(ret), K(co_sstable), K(ctx->static_param_));
    } else if (OB_UNLIKELY((start_cg_idx_+1) != end_cg_idx_)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid cg idx for mock row store cg schema", K(ret), K(start_cg_idx_), K(end_cg_idx_));
    } else {
      STORAGE_LOG(DEBUG, "[RowColSwitch] mock row store cg", K(ctx->mocked_row_store_cg_));
    }
  }

  for (uint32_t idx = start_cg_idx_; OB_SUCC(ret) && idx < end_cg_idx_; idx++) {
    ObSSTableWrapper cg_wrapper;
    ObITable *table = nullptr;
    ObSSTable *cg_sstable = nullptr;
    ObITableReadInfo *read_info = nullptr;
    bool add_column = false;

    if (OB_FAIL(ctx->get_cg_schema_for_merge(idx, cg_schema_ptr))) {
      LOG_WARN("fail to get cg schema for merge", K(ret), K(idx));
    } else if (OB_ISNULL(writer = OB_NEWx(ObCOMergeRowWriter, &merger_arena_, ctx->is_build_row_store_from_rowkey_cg()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory for ObCOMergeWriter", K(ret));
    } else if (OB_ISNULL(merge_infos[idx])) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "merge info should not be null", K(ret), K(idx));
    } else if (empty_table) {
      table = nullptr;
    } else if (co_sstable.get_cs_meta().column_group_cnt_ <= idx) {
      if (!cg_schema_ptr->is_single_column_group()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected cg schema", K(ret), K(idx), K(co_sstable.get_cs_meta().column_group_cnt_), KPC(cg_schema_ptr), K(sstable));
      } else {
        table = &sstable;
        add_column = true;
        STORAGE_LOG(INFO, "add column for cg", K(ret), K(idx), K(co_sstable.get_cs_meta().column_group_cnt_), KPC(cg_schema_ptr), K(sstable));
      }
    } else if (OB_FAIL(co_sstable.fetch_cg_sstable(idx, cg_wrapper))) {
      STORAGE_LOG(WARN, "failed to get cg sstable", K(ret), K(sstable));
    } else if (OB_FAIL(cg_wrapper.get_loaded_column_store_sstable(cg_sstable))) {
      STORAGE_LOG(WARN, "failed to get sstable from wrapper", K(ret), K(cg_wrapper));
    } else if (OB_FAIL(cg_wrappers_.push_back(cg_wrapper))) {
      STORAGE_LOG(WARN, "failed to push cg wrapper", K(ret), K(cg_wrappers_));
    } else {
      table = cg_sstable;
    }

    if (OB_FAIL(ret)) {
    } else if (ctx->is_build_row_store_from_rowkey_cg()) {
      read_info = &ctx->table_read_info_;
    } else {
      read_info = cg_schema_ptr->is_rowkey_column_group() ?  merge_param_.cg_rowkey_read_info_ : &merge_ctx_->read_info_;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(writer->init(default_row, merge_param_, task_idx_, read_info, *cg_schema_ptr, idx, *merge_infos[idx], table, add_column))) {
      STORAGE_LOG(WARN, "failed to init writer", K(ret), K(default_row), K(merge_param_), KPC(table));
    } else if (OB_FAIL(merge_writers_.push_back(writer))) {
      STORAGE_LOG(WARN, "failed to push writer", K(ret), K(merge_writers_));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(writer)) {
      writer->~ObCOMergeWriter();
      merger_arena_.free(writer);
      writer = nullptr;
    }
  }

  return ret;
}

bool ObCOMerger::is_empty_table(const ObSSTable &sstable) const
{
  bool is_empty_table = false;
  const ObDatumRange &merge_rowid_range = merge_param_.merge_rowid_range_;
  if (sstable.get_data_macro_block_count() <= 0) {
    is_empty_table = true;
  } else if (merge_rowid_range.start_key_.is_min_rowkey() || merge_rowid_range.end_key_.is_max_rowkey()) {
    is_empty_table = false;
  } else {
    const int64_t start_rowid = merge_rowid_range.start_key_.datums_[0].get_int(), end_rowid = merge_rowid_range.end_key_.datums_[0].get_int();
    is_empty_table = end_rowid < start_rowid;
  }

  return is_empty_table;
}

int ObCOMerger::close()
{
  int ret = OB_SUCCESS;
  compaction::ObCOMergeWriter *writer = nullptr;

  ObTabletMergeInfo **merge_infos = static_cast<ObCOTabletMergeCtx *>(merge_ctx_)->cg_merge_info_array_;
  if (OB_UNLIKELY(nullptr == merge_infos)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid count or unexpected null merge info array", K(ret), K(merge_infos));
  } else if (only_use_row_table_) {
    if (OB_ISNULL(writer = merge_writers_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null writer", K(ret));
    } else if (OB_FAIL(writer->end_write(start_cg_idx_, end_cg_idx_, merge_infos))) {
      STORAGE_LOG(WARN, "fail to end writer", K(ret), KPC(writer));
    }
  } else if (OB_UNLIKELY((end_cg_idx_ - start_cg_idx_) != merge_writers_.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid merge_writers_ count ", K(ret), K(merge_writers_), K(start_cg_idx_), K(end_cg_idx_));
  } else {
    for (int64_t i = start_cg_idx_; OB_SUCC(ret) && i < end_cg_idx_; i++) {
      writer = merge_writers_.at(i - start_cg_idx_);
      if (OB_UNLIKELY(nullptr == writer || nullptr == merge_infos[i])) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null writer", K(ret), K(i), K(merge_writers_));
      } else if (OB_FAIL(writer->end_write(task_idx_, *merge_infos[i]))) {
        STORAGE_LOG(WARN, "failed to close writer", K(ret), KPC(writer));
      }
    }
  }

  return ret;
}

int ObCOMerger::build_mergelog(const blocksstable::ObDatumRow &row, ObMergeLog &merge_log, bool &need_replay, bool &row_store_iter_need_move)
{
  int ret = OB_SUCCESS;
  int64_t cmp_ret = 0;
  need_replay = true;
  row_store_iter_need_move = false;

  if (OB_ISNULL(row_store_iter_)) {
    merge_log.op_ = ObMergeLog::INSERT;
    merge_log.row_id_ = INT64_MAX;
  } else if (row_store_iter_->is_iter_end()) {
    merge_log.op_ = ObMergeLog::INSERT;
    merge_log.row_id_ = row_store_iter_->get_last_row_id();
  } else if (OB_FAIL(compare(row, *row_store_iter_, cmp_ret))) {
    STORAGE_LOG(WARN, "failed to compare iter", K(ret), K(row), KPC(row_store_iter_));
  } else if (cmp_ret == 0) {
    int64_t row_id = 0;
    row_store_iter_need_move = true;
    if (OB_FAIL(row_store_iter_->get_curr_row_id(row_id))) {
      STORAGE_LOG(WARN, "failed to get_curr_row_id", K(ret), KPC(row_store_iter_));
    } else {
      merge_log.op_ = row.row_flag_.is_delete() ? ObMergeLog::DELETE : ObMergeLog::UPDATE;
      merge_log.row_id_ = row_id;
    }
  } else if (cmp_ret < 0) {
    merge_log.row_id_ = row_store_iter_->get_last_row_id();
    merge_log.op_ = ObMergeLog::INSERT;
  } else if (cmp_ret > 0) {
    need_replay = false;
    row_store_iter_need_move = true;
  }

  return ret;
}

int ObCOMerger::replay_merglog(const ObMergeLog &merge_log, const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  if (merge_log.op_ == ObMergeLog::INSERT && row.row_flag_.is_delete()) {
  } else {
    //replay mergelog for all cg_iters
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_writers_.count(); i++) {
      ObCOMergeWriter *merge_writer = nullptr;
      if (OB_ISNULL(merge_writer = merge_writers_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "UNEXPECTED null writer", K(ret));
      } else if (OB_FAIL(merge_writer->replay_mergelog(merge_log, row))) {
        STORAGE_LOG(WARN, "failed to replay_mergelog", K(ret), K(i), KPC(merge_writer));
      }
    }
  }

  return ret;
}

int ObCOMerger::write_residual_data()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < merge_writers_.count(); i++) {
    ObCOMergeWriter *merge_writer = nullptr;
    if (OB_FAIL(share::dag_yield())) {
      STORAGE_LOG(WARN, "fail to yield co merge dag", KR(ret));
    } else if (OB_ISNULL(merge_writer = merge_writers_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "UNEXPECTED null writer", K(ret));
    } else if (OB_FAIL(merge_writer->append_residual_data())) {
      STORAGE_LOG(WARN, "failed to replay_mergelog", K(ret), K(i));
    }
  }

  return ret;
}

int ObCOMerger::merge_partition(ObBasicTabletMergeCtx &ctx, const int64_t idx)
{
  int ret = OB_SUCCESS;
  SET_MEM_CTX(ctx.mem_ctx_);
  ObMergeLog merge_log;

  if (OB_FAIL(prepare_merge(ctx, idx))) {
    STORAGE_LOG(WARN, "Failed to prepare merge partition", K(ret), K(ctx), K(idx));
  } else {
    MERGE_ITER_ARRAY minimum_iters;
    const blocksstable::ObDatumRow *result_row = nullptr;
    bool need_replay_mergelog = true;
    bool need_move_row_iter = false;
    while (OB_SUCC(ret) && !merge_helper_->is_iter_end()) {
      if (OB_FAIL(share::dag_yield())) {
        STORAGE_LOG(WARN, "fail to yield co merge dag", KR(ret));
      }
#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_PARTITION_LONG_TIME) ret;
        if (OB_FAIL(ret)) {
          if (REACH_TENANT_TIME_INTERVAL(ObPartitionMergeProgress::UPDATE_INTERVAL)) {
            LOG_INFO("ERRSIM EN_COMPACTION_CO_MERGE_PARTITION_LONG_TIME", K(ret));
          }
          ret = OB_SUCCESS;
          continue;
        }
      }
#endif
      if (OB_FAIL(ret)) {
      } else if (need_replay_mergelog == false) {
        //reuse result_row
      } else if (OB_FAIL(merge_helper_->find_rowkey_minimum_iters(minimum_iters))) {
        STORAGE_LOG(WARN, "failed to find_rowkey_minimum_iters", K(ret), KPC(merge_helper_));
      } else if (OB_UNLIKELY(minimum_iters.empty())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected empty minimum iters", K(ret), KPC(merge_helper_));
      } else if (OB_FAIL(partition_fuser_->fuse_row(minimum_iters))) {
        STORAGE_LOG(WARN, "failed to fuse row", K(ret), K(minimum_iters));
      } else {
        result_row = &(partition_fuser_->get_result_row());
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(result_row) || OB_UNLIKELY(!result_row->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "UNEXPECTED result_row", K(ret), KPC(partition_fuser_));
      } else if (OB_FAIL(build_mergelog(*result_row, merge_log, need_replay_mergelog, need_move_row_iter))) {
        STORAGE_LOG(WARN, "failed to build mergelog", K(ret));
      } else if (need_move_row_iter) {
        if (OB_UNLIKELY(row_store_iter_ == nullptr)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected null row_store_iter_", K(ret));
        } else if (OB_FAIL(move_iter_next(*row_store_iter_))) {
          STORAGE_LOG(WARN, "failed to move_iter_next", K(ret));
        }
      }
      if (OB_SUCC(ret) && need_replay_mergelog) {
        if (OB_FAIL(replay_merglog(merge_log, *result_row))) {
          STORAGE_LOG(WARN, "failed to replay merge log", K(ret), K(merge_log), KPC(result_row));
        } else if (OB_FAIL(merge_helper_->move_iters_next(minimum_iters))) {
          STORAGE_LOG(WARN, "failed to move iters next", K(ret), K(minimum_iters));
        } else if (OB_FAIL(merge_helper_->rebuild_rows_merger())) {
          STORAGE_LOG(WARN, "failed to rebuild rows merger", K(ret), KPC(merge_helper_));
        } else {
          minimum_iters.reset();
        }
      }
      // updating merge progress should not have effect on normal merge process
      if (REACH_TENANT_TIME_INTERVAL(ObPartitionMergeProgress::UPDATE_INTERVAL)) {
        if (OB_NOT_NULL(merge_progress_) && (OB_SUCC(ret) || ret == OB_ITER_END)) {
          int tmp_ret = OB_SUCCESS;
          int64_t scanned_row_cnt = merge_helper_->get_iters_row_count();
          if (OB_NOT_NULL(row_store_iter_)) {
            scanned_row_cnt += row_store_iter_->get_last_row_id();
          }
          if (OB_SUCCESS != (tmp_ret = merge_progress_->update_merge_progress(idx,
              scanned_row_cnt))) {
            STORAGE_LOG(WARN, "failed to update merge progress", K(tmp_ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(write_residual_data())) {
    STORAGE_LOG(WARN, "failed to write residual data", K(ret), KPC(this));
    CTX_SET_DIAGNOSE_LOCATION(*merge_ctx_);
  } else if (OB_FAIL(close())) {
    STORAGE_LOG(WARN, "failed to close merger", K(ret));
  }

  reset();
  return ret;
}

int ObCOMerger::compare(const blocksstable::ObDatumRow &left,
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
        if (OB_UNLIKELY(!cmp_->need_open_right_macro(cmp_ret))) {
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

int ObCOMerger::move_iter_next(ObPartitionMergeIter &iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iter.next())) {
    if (OB_LIKELY(ret == OB_ITER_END)) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "fail to move next", K(ret), K(iter));
    }
  }
  return ret;
}
} //compaction
} //oceanbase
