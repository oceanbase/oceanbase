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

#include "ob_co_merge_writer.h"

namespace oceanbase
{
namespace compaction
{
/**
 * ---------------------------------------------------------ObCOMergeProjector--------------------------------------------------------------
 */
int ObCOMergeProjector::init(const ObStorageColumnGroupSchema &cg_schema)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(!cg_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid cg schema", K(ret), K(cg_schema));
  } else {
    projector_.reuse();
    project_row_.reset();
    const uint16_t column_cnt = cg_schema.column_cnt_;
    for (uint16_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
      const uint16_t project_idx = cg_schema.is_all_column_group() ? i : cg_schema.column_idxs_[i];
      if (OB_FAIL(projector_.push_back(project_idx))) {
        STORAGE_LOG(WARN, "failed to push back project idx", K(ret), K(i), K(cg_schema), K(projector_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(project_row_.init(column_cnt))) {
      STORAGE_LOG(WARN, "failed to init project row", K(ret), K(column_cnt));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

bool ObCOMergeProjector::is_all_nop(const blocksstable::ObDatumRow &row) const
{
  bool all_nop = true;

  for (int64_t i = 0; i < projector_.count(); i++) {
    const uint16_t idx = projector_.at(i);
    if (idx < row.count_ && !row.storage_datums_[idx].is_nop()) {
      all_nop = false;
    }
  }

  return all_nop;
}

int ObCOMergeProjector::project(const blocksstable::ObDatumRow &row)
{
  bool is_all_nop = false;
  clean_project_row();
  return project(row, project_row_, is_all_nop);
}

int ObCOMergeProjector::project(const blocksstable::ObDatumRow &row, blocksstable::ObDatumRow &result_row, bool &is_all_nop) const
{
  int ret = OB_SUCCESS;
  is_all_nop = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObProjector is not init", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid() || result_row.count_ != projector_.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(row));
  } else {
    result_row.row_flag_ = row.row_flag_;

    for (int64_t i = 0; OB_SUCC(ret) && i < projector_.count(); i++) {
      const uint16_t idx = projector_.at(i);
      if (idx < 0 || idx >= row.count_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected idx", K(ret), K(i), K(idx), K(row.count_));
      } else {
        result_row.storage_datums_[i] = row.storage_datums_[idx];
        if (!row.storage_datums_[idx].is_nop()) {
          is_all_nop = false;
        }
      }
    }
  }

  return ret;
}

void ObCOMergeProjector::clean_project_row()
{
  project_row_.row_flag_.reset();
  project_row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
}

/**
 * ---------------------------------------------------------ObWriteHelper--------------------------------------------------------------
 */
int ObWriteHelper::init(
    const ObMergeParameter &merge_param,
    const int64_t parallel_idx,
    const int64_t cg_idx,
    const ObStorageColumnGroupSchema &cg_schema,
    ObTabletMergeInfo &merge_info)
{
  int ret = OB_SUCCESS;
  ObMacroDataSeq macro_start_seq(0);

  if (OB_FAIL(ObDataDescHelper::build(merge_param, merge_info, data_store_desc_, merge_info_))) {
    STORAGE_LOG(WARN, "fail to build data desc", K(ret));
  } else if (OB_FAIL(macro_start_seq.set_parallel_degree(parallel_idx))) {
    STORAGE_LOG(WARN, "Failed to set parallel degree to macro start seq", K(ret), K(parallel_idx));
  } else if (OB_FAIL(macro_writer_.open(data_store_desc_, macro_start_seq))) {
    STORAGE_LOG(WARN, "Failed to open macro writer", K(ret));
  } else if (cg_schema.is_all_column_group()) {
    skip_project_ = true;
  } else if (OB_FAIL(projector_.init(cg_schema))) {
    STORAGE_LOG(WARN, "fail to init project", K(ret), K(cg_schema));
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

int ObWriteHelper::end_write(ObTabletMergeInfo &merge_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(macro_writer_.close())) {
    STORAGE_LOG(WARN, "failed to close macro writer", K(ret), K(macro_writer_));
  } else if (FALSE_IT(merge_info_.merge_finish_time_ = common::ObTimeUtility::fast_current_time())) {
  } else if (OB_FAIL(merge_info.add_macro_blocks(merge_info_))) {
    STORAGE_LOG(WARN, "Failed to add macro blocks", K(ret));
  } else {
    merge_info_.dump_info("macro block builder close");
  }

  return ret;
}

/**
 * ---------------------------------------------------------ObCOMergeWriter--------------------------------------------------------------
 */
ObCOMergeWriter::~ObCOMergeWriter()
{
  if (OB_NOT_NULL(iter_)) {
    iter_->~ObMergeIter();
    iter_ = nullptr;
  }

  default_row_.reset();
  allocator_.reset();
}

int ObCOMergeWriter::basic_init(const blocksstable::ObDatumRow &default_row,
                                const ObMergeParameter &merge_param,
                                const ObITableReadInfo *read_info,
                                const int64_t column_cnt,
                                ObITable *table,
                                const bool add_column,
                                const bool only_use_row_table)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(!default_row.is_valid() || OB_ISNULL(merge_param.error_location_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(default_row), K(merge_param.error_location_));
  } else if (FALSE_IT(error_location_ = merge_param.error_location_)) {
  } else if (OB_FAIL(default_row_.init(allocator_, column_cnt))) {
    STORAGE_LOG(WARN, "Failed to init default row", K(ret), K(column_cnt));
  } else if (OB_FAIL(default_row_.deep_copy(default_row, allocator_))) {
    STORAGE_LOG(WARN, "failed to deep copy default row", K(ret));
  } else if (OB_FAIL(fuser_.init(column_cnt))) {
    STORAGE_LOG(WARN, "failed to init fuser", K(ret), K(column_cnt));
  } else if (OB_ISNULL(table)) {
    iter_ = nullptr;
  } else if (!table->is_major_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "UNEXPECTED table type", K(ret), KPC(table));
  } else {
    const ObSSTable *sstable = static_cast<ObSSTable *>(table);
    if (add_column) {
      iter_ = OB_NEWx(ObDefaultRowIter, (&allocator_), default_row_);
    } else if (merge_param.is_full_merge() || sstable->is_small_sstable() || only_use_row_table) {
      iter_ = OB_NEWx(ObPartitionRowMergeIter, (&allocator_), allocator_, iter_co_build_row_store_);
    } else if (MICRO_BLOCK_MERGE_LEVEL == merge_param.static_param_.merge_level_) {
      iter_ = OB_NEWx(ObPartitionMicroMergeIter, (&allocator_), allocator_);
    } else {
      iter_ = OB_NEWx(ObPartitionMacroMergeIter, (&allocator_), allocator_);
    }

    if (OB_ISNULL(iter_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory for partition iter", K(ret));
    } else if (OB_FAIL(iter_->init(merge_param, table, read_info))) {
      STORAGE_LOG(WARN, "failed to init iter", K(ret), K(merge_param), KPC(table));
    } else if (OB_FAIL(iter_->next())) {
      STORAGE_LOG(WARN, "fail to move next", K(ret), KPC(iter_));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
    STORAGE_LOG(INFO, "Succ to init merge writer", K(ret), KPC(iter_), KPC(table));
  }
  return ret;
}

int ObCOMergeWriter::replay_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool is_all_nop = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObCOMergeWriter not init", K(ret));
  } else {
    bool finish = false;
    while (OB_SUCC(ret) && !finish) {
      int64_t cmp_ret = 0;
      bool skip_curr_row = false;
      if (OB_FAIL(compare(mergelog, cmp_ret, row, skip_curr_row))) {
        STORAGE_LOG(WARN, "failed to compare", K(ret), K(mergelog), KPC(iter_));
      } else if (skip_curr_row) {
        break;//skip
      } else if (cmp_ret < 0) {
        if (OB_FAIL(append_iter_curr_row_or_range())) {
          STORAGE_LOG(WARN, "failed to append iter curr row or range", K(ret), KPC(iter_));
        }
      } else if (cmp_ret == 0) {
        if (mergelog.op_ == ObMergeLog::INSERT) {
          if (OB_FAIL(append_iter_curr_row_or_range())) {
            STORAGE_LOG(WARN, "failed to append iter curr row or range", K(ret), KPC(iter_));
          }
        } else if (OB_FAIL(process_mergelog_row(mergelog, row))) {
          STORAGE_LOG(WARN, "failed to process_mergelog_row", K(ret), K(mergelog), K(row), K(iter_));
        } else {
          finish = true;
        }
      } else if (cmp_ret > 0){
        if (OB_UNLIKELY(mergelog.op_ != ObMergeLog::INSERT)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected cmp ret", K(ret), K(cmp_ret), K(mergelog), K(*this));
        } else if (OB_FAIL(process_mergelog_row(mergelog, row))) {
          STORAGE_LOG(WARN, "failed to process_mergelog_row", K(ret), K(mergelog), K(row), K(iter_));
        } else {
          finish = true;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (cmp_ret <= 0 && OB_FAIL(iter_->next())) {
        if (OB_LIKELY(ret == OB_ITER_END)) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "fail to move next", K(ret), KPC(iter_));
        }
      }
    }
  }

  return ret;
}

int ObCOMergeWriter::append_residual_data()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObCOMergeWriter not init", K(ret));
  }

  while (OB_SUCC(ret) && OB_NOT_NULL(iter_) && !iter_->is_iter_end()) {
    if (OB_FAIL(append_iter_curr_row_or_range())) {
      STORAGE_LOG(WARN, "failed to append iter curr row or range", K(ret), K(iter_));
    } else {
      if (OB_FAIL(iter_->next())) {
        if (OB_LIKELY(ret == OB_ITER_END)) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "fail to move next", K(ret), KPC(iter_));
        }
      }
    }
  }

  return ret;
}

int ObCOMergeWriter::append_iter_curr_row_or_range()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == iter_ || iter_->is_iter_end())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected iter_ state", K(ret), KPC(iter_));
  } else if (iter_->is_macro_block_opened()) {
    if (OB_ISNULL(iter_->get_curr_row())) {
      const blocksstable::ObMicroBlock *micro_block;

      if (OB_FAIL(iter_->get_curr_micro_block(micro_block))) {
        STORAGE_LOG(WARN, "failed to get_curr_micro_block", K(ret), KPC(iter_));
      } else if (OB_ISNULL(micro_block) || !micro_block->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected micro block", K(ret), KPC(iter_), KPC(micro_block));
      } else if (OB_FAIL(process(*micro_block))) {
        STORAGE_LOG(WARN, "failed to process micro block", K(ret));
      }
    } else if (OB_FAIL(fuser_.fuse_rows(*iter_->get_curr_row(), default_row_))) {
      STORAGE_LOG(WARN, "failed to fuse row", K(ret), KPC(iter_), K(default_row_));
    } else if (OB_FAIL(process(fuser_.get_result_row()))) {
      STORAGE_LOG(WARN, "failed to process iter curr row", K(ret), KPC(iter_), K(default_row_));
    }
  } else {
    const ObMacroBlockDesc *macro_desc = nullptr;

    if (OB_FAIL(iter_->get_curr_macro_block(macro_desc))) {
      STORAGE_LOG(WARN, "Failed to get current micro block", K(ret), KPC(iter_));
    } else if (OB_ISNULL(macro_desc) || OB_UNLIKELY(!macro_desc->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null macro block", K(ret), KP(macro_desc), KPC(iter_));
    } else if (OB_FAIL(process(*macro_desc))) {
      STORAGE_LOG(WARN, "Failed to append macro block", K(ret), KPC(macro_desc));
    }
  }

  return ret;
}

int ObCOMergeWriter::compare(const ObMergeLog &mergelog, int64_t &cmp_ret, const blocksstable::ObDatumRow &row, bool &skip_curr_row) const
{
  int ret = OB_SUCCESS;
  const int64_t log_row_id = mergelog.row_id_;
  int64_t iter_row_id = 0;
  skip_curr_row = false;

  if (nullptr == iter_ || iter_->is_iter_end()) {
    cmp_ret = 1;
  } else {
    int64_t curr_range_end_rowid;
    bool check_iter_range = true;
    bool need_open = true;
    while (OB_SUCC(ret) && OB_ISNULL(iter_->get_curr_row()) && check_iter_range) {
      check_iter_range = false;
      if (log_row_id <= iter_->get_last_row_id()) {
        cmp_ret = 1;
      } else if (OB_FAIL(iter_->get_curr_range_end_rowid(curr_range_end_rowid))) {
        STORAGE_LOG(WARN, "Failed to get current row count", K(ret), KPC(iter_));
      } else if (log_row_id > curr_range_end_rowid) {
        cmp_ret = -1;
      } else if (mergelog.op_ == ObMergeLog::INSERT
          && log_row_id == curr_range_end_rowid) {
        cmp_ret = 0;
      } else if (mergelog.op_ == ObMergeLog::UPDATE && is_cg()
          && OB_FAIL(iter_->need_open_curr_range(row, need_open, mergelog.row_id_))) {
        STORAGE_LOG(WARN, "fail to check row changed ", K(ret), K(mergelog), KPC(iter_));
      } else if (!need_open) {
        skip_curr_row = true;
        break;
      } else if (FALSE_IT(check_iter_range = true)) {
      } else if (OB_FAIL(iter_->open_curr_range(false /* rewrite */))) {
        STORAGE_LOG(WARN, "failed to open curr range", K(ret), KPC(iter_));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(iter_->get_curr_row())) {
      if (OB_FAIL(iter_->get_curr_row_id(iter_row_id))) {
        STORAGE_LOG(WARN, "failed to get iter row id", K(ret), KPC(iter_));
      } else if (iter_row_id == log_row_id) {
        cmp_ret = 0;
      } else {
        cmp_ret = iter_row_id > log_row_id ? 1 : -1;
      }
    }
  }

  return ret;
}

int ObCOMergeWriter::process_mergelog_row(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  if (mergelog.op_ == ObMergeLog::INSERT) {
    if (OB_FAIL(fuser_.fuse_rows(row, default_row_))) {
      STORAGE_LOG(WARN, "failed to fuse row", K(ret), K(row));
    }
  } else if (mergelog.op_ == ObMergeLog::UPDATE) {
    if (OB_UNLIKELY(nullptr == iter_ || nullptr == iter_->get_curr_row())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null iter or null curr row", K(ret), KPC(iter_));
    } else if(OB_FAIL(fuser_.fuse_rows(row, *iter_->get_curr_row(), default_row_))) {
      STORAGE_LOG(WARN, "failed to fuse row", K(ret), K(row), KPC(this));
    }
  }

  if (OB_FAIL(ret) || mergelog.op_ == ObMergeLog::DELETE) {
  } else if (OB_FAIL(process(fuser_.get_result_row()))){
    STORAGE_LOG(WARN, "failed to process iter curr row", K(ret), K(fuser_.get_result_row()));
  }
  return ret;
}

int ObCOMergeWriter::process_macro_rewrite()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(iter_->is_macro_block_opened())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected macro block opened", K(ret), KPC(iter_));
  } else if (OB_FAIL(iter_->open_curr_range(true /* rewrite */))) {
    STORAGE_LOG(WARN, "failed to open iter range", K(ret), KPC(iter_));
  } else if (OB_ISNULL(iter_->get_curr_row())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null row", K(ret), KPC(iter_));
  } else if (OB_FAIL(fuser_.fuse_rows(*iter_->get_curr_row(), default_row_))) {
    STORAGE_LOG(WARN, "failed to fuse row", K(ret), KPC(iter_), K(default_row_));
  } else if (OB_FAIL(process(fuser_.get_result_row()))) {
    STORAGE_LOG(WARN, "failed to process iter curr row", K(ret), KPC(iter_));
  }

  return ret;
}

void ObCOMergeWriter::dump_info() const
{
  if (OB_NOT_NULL(iter_)) {
    FLOG_INFO("co merge iter idx", KPC(iter_->get_table()), "row_count", iter_->get_last_row_id(),
          "is_iter_end", iter_->is_iter_end());
  }
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
}

int ObCOMergeRowWriter::init(const blocksstable::ObDatumRow &default_row,
                          const ObMergeParameter &merge_param,
                          const int64_t parallel_idx,
                          const ObITableReadInfo *full_read_info,
                          const ObStorageColumnGroupSchema &cg_schema,
                          const int64_t cg_idx,
                          ObTabletMergeInfo &merge_info,
                          ObITable *table,
                          const bool add_column)
{
  int ret = OB_SUCCESS;
  bool is_all_nop = false;
  const ObITableReadInfo *read_info = nullptr;

  if (OB_FAIL(write_helper_.init(merge_param, parallel_idx, cg_idx, cg_schema, merge_info))) {
    STORAGE_LOG(WARN, "fail to init write helper", K(ret), K(parallel_idx), K(cg_idx), K(cg_schema));
  } else if (OB_FAIL(row_.init(cg_schema.column_cnt_))) {
    STORAGE_LOG(WARN, "fail to init row", K(ret), K(cg_schema));
  } else if (add_column) {//skip init read info and progressive_merge_helper_
  } else if (cg_schema.is_single_column_group()) {
    single_read_info_.reset();
    if (OB_FAIL(ObTenantCGReadInfoMgr::construct_cg_read_info(allocator_,
                                                              full_read_info->is_oracle_mode(),
                                                              write_helper_.get_col_desc_array().at(0),
                                                              nullptr,
                                                              single_read_info_))) {
      LOG_WARN("Fail to init cg read info", K(ret));
    } else {
      read_info = &single_read_info_;
    }
  } else {
    read_info = full_read_info;
  }

  if (OB_SUCC(ret) && !merge_param.is_full_merge() && table != nullptr) {
    const ObSSTable *sstable = static_cast<ObSSTable *>(table);
    progressive_merge_helper_ = OB_NEWx(ObProgressiveMergeHelper, (&allocator_), sstable->get_key().column_group_idx_);
    if (OB_ISNULL(progressive_merge_helper_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory for progressive_merge_helper_", K(ret));
    } else if (OB_FAIL(progressive_merge_helper_->init(*sstable, merge_param, allocator_))) {
      STORAGE_LOG(WARN, "failed to init progressive_merge_helper", K(ret), KPC(table));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (write_helper_.need_project()) {
    if (write_helper_.project(default_row, row_, is_all_nop)) {
      STORAGE_LOG(WARN, "fail to project", K(ret), K(default_row), K(row_));
    } else if (OB_FAIL(basic_init(row_, merge_param, read_info, cg_schema.column_cnt_, table, add_column))) {
      STORAGE_LOG(WARN, "Failed to init default row", K(ret), K(cg_schema.column_cnt_));
    }
  } else if (OB_FAIL(basic_init(default_row, merge_param, read_info, cg_schema.column_cnt_, table, add_column))) {
    STORAGE_LOG(WARN, "Failed to init default row", K(ret), K(cg_schema.column_cnt_));
  }

  return ret;
}

int ObCOMergeRowWriter::process(const ObMacroBlockDesc &macro_desc)
{
  int ret = OB_SUCCESS;
  ObMacroBlockOp block_op;
  if (OB_NOT_NULL(progressive_merge_helper_) && progressive_merge_helper_->is_valid()) {
    if (OB_FAIL(progressive_merge_helper_->check_macro_block_op(macro_desc, block_op))) {
      STORAGE_LOG(WARN, "failed to check macro operation", K(ret), K(macro_desc));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (block_op.is_rewrite()) {
    progressive_merge_helper_->inc_rewrite_block_cnt();
    if (OB_FAIL(process_macro_rewrite())) {
      STORAGE_LOG(WARN, "failed to process_macro_rewrite", K(ret));
    }
  } else if (block_op.is_reorg()) {
    if (OB_FAIL(iter_->open_curr_range(false /* rewrite */))) {
      STORAGE_LOG(WARN, "Failed to open_curr_range", K(ret));
    } else if (OB_FAIL(append_iter_curr_row_or_range())) {
      STORAGE_LOG(WARN, "failed to append iter curr row or range", K(ret), KPC(iter_));
    }
  } else if (OB_FAIL(write_helper_.append_macro_block(macro_desc))) {
    STORAGE_LOG(WARN, "failed to append macro block", K(ret), K(macro_desc));
  }

  return ret;
}

int ObCOMergeRowWriter::process(const blocksstable::ObMicroBlock &micro_block)
{
  return write_helper_.append_micro_block(micro_block);
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

int ObCOMergeRowWriter::replay_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool is_all_nop = false;

  if (!is_init()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObCOMergeRowWriter not init", K(ret));
  } else if (!write_helper_.need_project()) {
    if (OB_FAIL(ObCOMergeWriter::replay_mergelog(mergelog, row))) {
      STORAGE_LOG(WARN, "fariled to replay_mergelog", K(ret), K(mergelog), K(row));
    }
  } else if (OB_FAIL(write_helper_.project(row, row_, is_all_nop))) {
    STORAGE_LOG(WARN, "fail to project", K(ret), K(write_helper_), K(row));
  } else if (mergelog.op_ == ObMergeLog::UPDATE && is_all_nop) { //skip replay
  } else if (OB_FAIL(ObCOMergeWriter::replay_mergelog(mergelog, row_))) {
    STORAGE_LOG(WARN, "fariled to replay_mergelog", K(ret), K(mergelog), K(row));
  }

  return ret;
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
    }
  }
}

int ObCOMergeSingleWriter::init(
    const blocksstable::ObDatumRow &default_row,
    const ObMergeParameter &merge_param,
    const ObITableReadInfo *full_read_info,
    const int64_t parallel_idx,
    const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
    const int64_t start_cg_idx,
    const int64_t end_cg_idx,
    ObTabletMergeInfo **merge_infos,
    ObSSTable *row_table)
{
  int ret = OB_SUCCESS;
  int64_t full_column_cnt = 0;
  const int64_t cg_cnt = end_cg_idx - start_cg_idx;

  if (OB_UNLIKELY(cg_cnt < 0 || merge_infos == nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected arguments", K(ret), K(end_cg_idx), K(start_cg_idx), K(merge_infos));
  } else if (OB_FAIL(merge_param.get_schema()->get_store_column_count(full_column_cnt, true))) {
    STORAGE_LOG(WARN, "fail to get store column cnt", K(ret), KPC(merge_param.get_schema()));
  } else {
    for (uint32_t idx = start_cg_idx; OB_SUCC(ret) && idx < end_cg_idx; idx++) {
      const ObStorageColumnGroupSchema &cg_schema = cg_array.at(idx);
      ObWriteHelper *write_helper = nullptr;

      if (OB_ISNULL(write_helper = alloc_helper<ObWriteHelper>(allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc write helper", K(ret));
      } else if (OB_ISNULL(merge_infos[idx])) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "merge info should not be null", K(ret), K(idx));
      } else if (OB_FAIL(write_helper->init(merge_param, parallel_idx, idx, cg_schema, *merge_infos[idx]))) {
        STORAGE_LOG(WARN, "fail to init write helper", K(ret));
      } else if (OB_FAIL(write_helpers_.push_back(write_helper))) {
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

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(basic_init(default_row, merge_param, full_read_info,
      full_column_cnt + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(), row_table, false, true))) {
    STORAGE_LOG(WARN, "fail to basic init", K(ret), K(merge_param), K(full_column_cnt), KPC(row_table));
  } else {
    FLOG_INFO("succ to init ObCOMergeSingleWriter", K(ret), K(parallel_idx), K(start_cg_idx), K(end_cg_idx), KPC(row_table));
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
      if (OB_UNLIKELY(write_helper == nullptr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null write helper", K(ret), K(write_helper));
      } else if (OB_FAIL(write_helper->append(row))) {
        STORAGE_LOG(WARN, "failed to project row", K(ret), K(i), K(row), KPC(write_helper));
      }
    }
  }

  return ret;
}

int ObCOMergeSingleWriter::end_write(
    const int64_t start,
    const int64_t end,
    ObTabletMergeInfo **merge_infos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObCOMergeWriter not init", K(ret));
  } else if (OB_UNLIKELY((end - start) != write_helpers_.count() || nullptr == merge_infos)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid count or unexpected null merge info array", K(ret),
      K(write_helpers_.count()), K(start), K(end), K(merge_infos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < write_helpers_.count(); i++) {
      ObWriteHelper *write_helper = write_helpers_.at(i);
      if (OB_UNLIKELY(write_helper == nullptr || merge_infos[i + start] == nullptr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null write helper", K(ret), K(write_helper), K(merge_infos[i + start]));
      } else if (OB_FAIL(write_helper->end_write(*merge_infos[i + start]))) {
        STORAGE_LOG(WARN, "fail to close", K(ret), KPC(write_helper));
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
