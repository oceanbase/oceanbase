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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_partition_merge_fuser.h"

#include "ob_tablet_merge_ctx.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace blocksstable;

namespace compaction
{
/*
 *ObIPartitionMergeFuser
 */
ObIPartitionMergeFuser::~ObIPartitionMergeFuser()
{
  reset();
}

void ObIPartitionMergeFuser::reset()
{
  result_row_.reset();
  schema_rowkey_column_cnt_ = 0;
  column_cnt_ = 0;
  nop_pos_.reset();
  multi_version_column_ids_.reset();
  allocator_.reset();
  is_inited_ = false;
}

bool ObIPartitionMergeFuser::is_valid() const
{
  return (is_inited_ && schema_rowkey_column_cnt_ > 0 && column_cnt_ > 0);
}

int ObIPartitionMergeFuser::calc_column_checksum(const bool rewrite)
{
  int ret = OB_SUCCESS;
  UNUSED(rewrite);
  return ret;
}

int ObIPartitionMergeFuser::init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIPartitionMergeFuser init twice", K(ret));
  } else if (OB_FAIL(check_merge_param(merge_param))) {
    STORAGE_LOG(WARN, "Invalid argument to init ObIPartitionMergeFuser", K(merge_param), K(ret));
  } else if (OB_FAIL(inner_init(merge_param))) {
    STORAGE_LOG(WARN, "Failed to inner init", K(ret), K(*this));
  } else if (OB_FAIL(base_init(merge_param))) {
    STORAGE_LOG(WARN, "Failed to base init", K(ret), K(*this));
  } else {
    is_inited_ = true;
    STORAGE_LOG(DEBUG, "Succ to init partition fuser", K(ret), K(*this),
        "merge_range", merge_param.merge_range_);
  }
  return ret;
}

void ObIPartitionMergeFuser::reset_store_row(ObDatumRow &store_row)
{
  store_row.row_flag_.reset();
  store_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  store_row.trans_id_.reset();
  store_row.mvcc_row_flag_.reset();
  for (int64_t i = 0; i < store_row.count_; i++) {
    store_row.storage_datums_[i].set_nop();
  }
}

int ObIPartitionMergeFuser::check_merge_param(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObIPartitionMergeFuser", K(merge_param), K(ret));
  } else if (OB_FAIL(inner_check_merge_param(merge_param))) {
    STORAGE_LOG(WARN, "Unexcepted merge param to init merge fuser", K(merge_param), K(ret));
  }

  return ret;
}

int ObIPartitionMergeFuser::base_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(column_cnt_ <= 0 || column_cnt_ > OB_MAX_COLUMN_NUMBER)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected column cnt", K_(column_cnt), K(ret));
  } else if (OB_FAIL(result_row_.init(allocator_, column_cnt_))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else if (OB_FAIL(nop_pos_.init(allocator_, column_cnt_))) {
    STORAGE_LOG(WARN, "Failed to init nop pos", K_(column_cnt), K(ret));
  } else {
    schema_rowkey_column_cnt_ = merge_param.merge_schema_->get_rowkey_column_num();
  }

  return ret;
}

int ObIPartitionMergeFuser::set_multi_version_flag(const ObMultiVersionRowFlag &row_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIPartitionMergeFuser is not inited", K(ret));
  } else {
    result_row_.set_multi_version_flag(row_flag);
  }

  return ret;
}

/*
 *ObMajorPartitionMergeFuser
 */
ObMajorPartitionMergeFuser::~ObMajorPartitionMergeFuser()
{
  reset();
}

void ObMajorPartitionMergeFuser::reset()
{
  default_row_.reset();
  generated_cols_.reset();
  ObIPartitionMergeFuser::reset();
}

bool ObMajorPartitionMergeFuser::is_valid() const
{
  return ObIPartitionMergeFuser::is_valid() && multi_version_column_ids_.count() > 0;
}

int ObMajorPartitionMergeFuser::inner_check_merge_param(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_major_merge_type(merge_param.merge_type_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected merge param with major fuser", K(merge_param), K(ret));
  } else {
    ObITable *first_table = merge_param.tables_handle_->get_table(0);
    if (NULL == first_table) {
      ret =  OB_ERR_SYS;
      LOG_ERROR("first table must not null", K(ret), K(merge_param));
    } else if (!first_table->is_major_sstable() && !first_table->is_meta_major_sstable()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid first table type", K(ret), K(*first_table));
    }
  }

  return ret;
}

int ObMajorPartitionMergeFuser::inner_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIPartitionMergeFuser init twice", K(ret));
  } else if (OB_FAIL(merge_param.merge_schema_->get_multi_version_column_descs(multi_version_column_ids_))) {
    STORAGE_LOG(WARN, "Failed to get column ids", K(ret));
  } else if (OB_FAIL(default_row_.init(allocator_, multi_version_column_ids_.count()))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else if (OB_FAIL(merge_param.merge_schema_->get_orig_default_row(multi_version_column_ids_, default_row_))) {
    STORAGE_LOG(WARN, "Failed to get default row from table schema", K(ret));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator_, multi_version_column_ids_, default_row_))) {
    STORAGE_LOG(WARN, "fail to fill lob header for default row", K(ret));
  } else {
    default_row_.row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
    column_cnt_ = multi_version_column_ids_.count();
  }
  if (FAILEDx(generated_cols_.init(column_cnt_))) {
    LOG_WARN("Fail to init generated_cols", K(ret), K(column_cnt_));
  }
  if (OB_SUCC(ret) && !merge_param.merge_schema_->is_materialized_view()) {
    const ObColumnSchemaV2 *column_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_version_column_ids_.count(); ++i) {
      if (OB_HIDDEN_TRANS_VERSION_COLUMN_ID == multi_version_column_ids_.at(i).col_id_ ||
          OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == multi_version_column_ids_.at(i).col_id_) {
        // continue;
      } else {
        const ObStorageSchema *storage_schema = static_cast<const ObStorageSchema *>(merge_param.merge_schema_);
        const ObStorageColumnSchema *column_schema = NULL;
        if (OB_ISNULL(column_schema = storage_schema->get_column_schema(
                  multi_version_column_ids_.at(i).col_id_))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "The column schema is NULL", K(ret), K(i), K(multi_version_column_ids_.at(i)));
        } else if (column_schema->is_generated_column()
                   && !merge_param.merge_schema_->is_storage_index_table()) {
          // the generated columns in index are always filled before insert
          if (OB_FAIL(generated_cols_.push_back(i))) {
            LOG_WARN("Fail to push_back generated_cols", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObMajorPartitionMergeFuser::fuse_row(MERGE_ITER_ARRAY &macro_row_iters)
{
  int ret = OB_SUCCESS;
  bool final_result = false;
  int64_t macro_row_iters_cnt = macro_row_iters.count();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMajorPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(macro_row_iters.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro row iters to fuse row", K(macro_row_iters), K(ret));
  } else {
    nop_pos_.reset();
    reset_store_row(result_row_);
    result_row_.mvcc_row_flag_.flag_ = 0;

    for (int64_t i = 0; OB_SUCC(ret) && !final_result && i < macro_row_iters_cnt; ++i) {
      if (OB_FAIL(storage::ObRowFuse::fuse_row(*macro_row_iters.at(i)->get_curr_row(),
                                                      result_row_, nop_pos_, final_result))) {
        STORAGE_LOG(WARN, "Failed to fuse row", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "success to fuse row", K(ret), K(i),
            "curr_row", *macro_row_iters.at(i)->get_curr_row(), K(result_row_));
      }
    }
  }

  if (OB_SUCC(ret) && nop_pos_.count() > 0 && result_row_.row_flag_.is_exist_without_delete()) {
    if (generated_cols_.count() > 0) {
      // add defense for generated exprs
      int64_t idx = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < nop_pos_.count(); i++) {
        if (OB_FAIL(nop_pos_.get_nop_pos(i, idx))) {
          LOG_WARN("Failed to get nop pos", K(i), K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < generated_cols_.count(); j++) {
            if (idx == generated_cols_.at(j)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Found nop generated columns in major fuser", K(ret), K(i), K(idx),
                       K_(nop_pos), K_(generated_cols), K_(result_row));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(storage::ObRowFuse::fuse_row(default_row_, result_row_, nop_pos_, final_result))) {
        STORAGE_LOG(WARN, "Failed to fuse default row", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && result_row_.row_flag_.is_exist_without_delete()) {
    result_row_.row_flag_.reset();
    result_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  }
  return ret;
}

int ObMajorPartitionMergeFuser::fuse_delete_row(
    ObPartitionMergeIter *row_iter,
    ObDatumRow &row,
    const int64_t rowkey_column_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid null argument", KP(row_iter), K(ret));
  } else if (OB_UNLIKELY(!row_iter->get_curr_row()->row_flag_.is_delete())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected row flag", K(ret));
  } else {
    const ObDatumRow *del_row = row_iter->get_curr_row();
    for (int64_t i = 0; i < rowkey_column_cnt; ++i) {
      row.storage_datums_[i] = del_row->storage_datums_[i];
    }
    for (int64_t i = rowkey_column_cnt; i < row.count_; ++i) {
      row.storage_datums_[i].set_nop();
    }
    if (OB_SUCC(ret)) {
      row.row_flag_.set_flag(ObDmlFlag::DF_DELETE);
      row.mvcc_row_flag_ = del_row->mvcc_row_flag_;
      STORAGE_LOG(DEBUG, "fuse delete row", K(ret), K(*del_row), K(row));
    }
  }

  return ret;
}

/*
 *ObMetaPartitionMergeFuser
 */
int ObMetaPartitionMergeFuser::inner_check_merge_param(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  if (META_MAJOR_MERGE != merge_param.merge_type_
      || merge_param.version_range_.multi_version_start_ != merge_param.version_range_.snapshot_version_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "Unexpected merge param with meta fuser", K(ret), K(merge_param));
  }

  return ret;
}

/*
 *ObMinorPartitionMergeFuser
 */
ObMinorPartitionMergeFuser::~ObMinorPartitionMergeFuser()
{
  reset();
}

void ObMinorPartitionMergeFuser::reset()
{
  multi_version_rowkey_column_cnt_ = 0;
  ObIPartitionMergeFuser::reset();
}

bool ObMinorPartitionMergeFuser::is_valid() const
{
  return ObIPartitionMergeFuser::is_valid() && multi_version_rowkey_column_cnt_ > 0 && multi_version_column_ids_.count() == multi_version_rowkey_column_cnt_;
}

int ObMinorPartitionMergeFuser::inner_check_merge_param(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_major_merge_type(merge_param.merge_type_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected merge param with major fuser", K(merge_param), K(ret));
  } else {
    ObITable *first_table = merge_param.tables_handle_->get_table(0);
    if (OB_UNLIKELY(NULL == first_table || !first_table->is_multi_version_table())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid first table", K(ret), KPC(first_table));
    }
  }
  return ret;
}

int ObMinorPartitionMergeFuser::inner_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIPartitionMergeFuser init twice", K(ret));
  } else if (OB_FAIL(merge_param.merge_schema_->get_mulit_version_rowkey_column_ids(multi_version_column_ids_))) {
    STORAGE_LOG(WARN, "Failed to get column ids", K(ret));
  } else if (OB_FAIL(merge_param.merge_schema_->get_store_column_count(column_cnt, true/*full_col*/))) {
    STORAGE_LOG(WARN, "failed to get store column count", K(ret), K(merge_param.merge_schema_));
  } else {
    column_cnt_ = column_cnt + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    multi_version_rowkey_column_cnt_ = multi_version_column_ids_.count();
  }

  return ret;
}

int ObFlatMinorPartitionMergeFuser::fuse_row(MERGE_ITER_ARRAY &macro_row_iters)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFlatMinorPartitionMergeFuser is not inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(macro_row_iters.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro row iters to fuse row", K(macro_row_iters), K(ret));
  } else {
    int64_t macro_row_iters_cnt = macro_row_iters.count();
    bool final_result = false;

    nop_pos_.reset();
    reset_store_row(result_row_);
    for (int64_t i = 0; OB_SUCC(ret) && !final_result && i < macro_row_iters_cnt; ++i) {
      const ObDatumRow *cur_row = macro_row_iters.at(i)->get_curr_row();
      if (OB_ISNULL(cur_row)) {
        ret = OB_INNER_STAT_ERROR;
        STORAGE_LOG(WARN, "Unexpected null curr row", K(ret), K(i), KPC(macro_row_iters.at(i)));
      } else if (0 == i && (cur_row->trans_id_.is_valid())) {
        if (!cur_row->mvcc_row_flag_.is_uncommitted_row()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("uncommitted row is not valid", K(ret), K(macro_row_iters),
              "row", *cur_row, K(cur_row->trans_id_));
        } else {
          result_row_.trans_id_ = cur_row->trans_id_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (0 == i && macro_row_iters.at(i)->get_curr_row()->row_flag_.is_delete()) {
        if (OB_FAIL(fuse_delete_row(macro_row_iters.at(0), result_row_, multi_version_rowkey_column_cnt_))) {
          STORAGE_LOG(WARN, "Failed to fuse delete row", K(ret), K_(multi_version_rowkey_column_cnt));
        } else {
          STORAGE_LOG(DEBUG, "success to fuse delete row", K(ret), K(*macro_row_iters.at(i)->get_curr_row()),
              K(result_row_), K(macro_row_iters_cnt));
          final_result = true;
        }
      } else {
        if (OB_FAIL(storage::ObRowFuse::fuse_row(
                  *macro_row_iters.at(i)->get_curr_row(),
                  result_row_,
                  nop_pos_,
                  final_result))) {
          STORAGE_LOG(WARN, "Fail to fuse row", K(ret));
        } else if (result_row_.count_ != column_cnt_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "row count is not valid", K(ret), K(i), KPC(cur_row), K(result_row_),
              KPC(macro_row_iters.at(i)), KPC(macro_row_iters.at(i)->get_table()),
              KPC(this));
        } else {
          result_row_.row_flag_.fuse_flag(macro_row_iters.at(i)->get_curr_row()->row_flag_);
          LOG_DEBUG("Success to fuse row", K(ret),
              K(*macro_row_iters.at(i)),
              K(*macro_row_iters.at(i)->get_curr_row()),
              K(result_row_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (final_result) {
        result_row_.set_compacted_multi_version_row();
      }
      STORAGE_LOG(DEBUG, "fuse row", K(ret), K(result_row_), K(macro_row_iters_cnt));
    }
  }
  return ret;
}

// fuse delete flat row
int ObFlatMinorPartitionMergeFuser::fuse_delete_row(
    ObPartitionMergeIter *row_iter,
    blocksstable::ObDatumRow &row,
    const int64_t rowkey_column_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", KP(row_iter), K(ret));
  } else if (!row_iter->get_curr_row()->row_flag_.is_delete()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected row flag", K(ret));
  } else {
    int64_t copy_cnt = rowkey_column_cnt;
    if (row_iter->get_curr_row()->is_uncommitted_row()){
      copy_cnt = row.count_;
    }
    const ObDatumRow *del_row = row_iter->get_curr_row();
    for (int64_t i = 0; i < copy_cnt; ++i) {
      row.storage_datums_[i] = del_row->storage_datums_[i];
    }
    for (int64_t i = copy_cnt; i < row.count_; ++i) {
      row.storage_datums_[i].set_nop();
    }
    if (OB_SUCC(ret)) {
      row.row_flag_.set_flag(ObDmlFlag::DF_DELETE);
      row.mvcc_row_flag_ = del_row->mvcc_row_flag_;
      STORAGE_LOG(DEBUG, "fuse delete row", K(ret), K(*del_row), K(row));
    }
  }

  return ret;
}

} //compaction
} //oceanbase
