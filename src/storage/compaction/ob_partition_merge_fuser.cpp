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
#include "storage/column_store/ob_column_oriented_merge_fuser.h"

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
 *ObMergeFuser
 */

int ObMergeFuser::base_init(const bool is_fuse_row_flag)
{
  int ret = OB_SUCCESS;
  is_fuse_row_flag_ = is_fuse_row_flag;

  if (OB_FAIL(result_row_.init(allocator_, column_cnt_))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else if (OB_FAIL(nop_pos_.init(allocator_, column_cnt_))) {
    STORAGE_LOG(WARN, "Failed to init nop pos", K(ret), K(column_cnt_));
  }
  return ret;
}

void ObMergeFuser::clean_nop_pos_and_result_row()
{
  nop_pos_.reset();
  result_row_.row_flag_.reset();
  result_row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  result_row_.trans_id_.reset();
  result_row_.mvcc_row_flag_.reset();
  for (int64_t i = 0; i < result_row_.count_; i++) {
    result_row_.storage_datums_[i].set_nop();
  }
}

int ObMergeFuser::add_fuse_row(const blocksstable::ObDatumRow &row, bool &final_result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMergeFuser is not inited", K(ret), K(is_inited_));
  } else if (OB_FAIL(storage::ObRowFuse::fuse_row(row, result_row_, nop_pos_, final_result))) {
    STORAGE_LOG(WARN, "Failed to fuse row", K(ret));
  } else if (result_row_.count_ != column_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row count is not valid", K(ret), K(row), K(result_row_), K(column_cnt_));
  } else if (is_fuse_row_flag_) {
    result_row_.row_flag_.fuse_flag(row.row_flag_);
  }
  return ret;
}

int ObMergeFuser::preprocess_fuse_row(const blocksstable::ObDatumRow &row, bool &is_need_fuse)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  clean_nop_pos_and_result_row();
  is_need_fuse = true;
  return ret;
}
int ObMergeFuser::end_fuse_row(const storage::ObNopPos &nop_pos, blocksstable::ObDatumRow &result_row)
{
  if (result_row.row_flag_.is_exist_without_delete()) {
    result_row.row_flag_.reset();
    result_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  }
  return OB_SUCCESS;
}

int ObMergeFuser::set_multi_version_flag(const ObMultiVersionRowFlag &row_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMergeFuser is not inited", K(ret));
  } else {
    result_row_.set_multi_version_flag(row_flag);
  }

  return ret;
}

int ObMergeFuser::fuse_row(MERGE_ITER_ARRAY &macro_row_iters)
{
  int ret = OB_SUCCESS;
  bool is_need_fuse = false;
  const int64_t macro_row_iters_cnt = macro_row_iters.count();

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMergeFuser not init", K(ret));
  } else if (OB_UNLIKELY(macro_row_iters_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro row iters to fuse row", K(ret), K(macro_row_iters));
  } else if (OB_ISNULL(macro_row_iters.at(0)) || OB_ISNULL(macro_row_iters.at(0)->get_curr_row())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid macro row iters to fuse row", K(ret), K(macro_row_iters));
  } else if (OB_FAIL(preprocess_fuse_row(*macro_row_iters.at(0)->get_curr_row(), is_need_fuse))) {
    STORAGE_LOG(WARN, "failed to preprocess_fuse_row", K(ret));
  } else if (!is_need_fuse && macro_row_iters.at(0)->get_curr_row()->row_flag_.is_delete()) {
    result_row_.row_flag_.reset();
    result_row_.row_flag_ = macro_row_iters.at(0)->get_curr_row()->row_flag_;
    for (int64_t i = 1; i < macro_row_iters_cnt; ++i) {
      result_row_.row_flag_.fuse_flag(macro_row_iters.at(i)->get_curr_row()->row_flag_);
    }
  } else {
    bool final_result = false;
    for (int64_t i = 0; OB_SUCC(ret) && !final_result && i < macro_row_iters_cnt; ++i) {
      if (OB_FAIL(add_fuse_row(*macro_row_iters.at(i)->get_curr_row(), final_result))) {
        STORAGE_LOG(WARN, "Failed to fuse row", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(end_fuse_row(nop_pos_, result_row_))) {
      STORAGE_LOG(WARN, "failed to end fuse row", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObMergeFuser::make_result_row_shadow(const int64_t sql_sequence_col_idx)
{
  return ObShadowRowUtil::make_shadow_row(sql_sequence_col_idx, result_row_);
}

// fuse delete row
int ObMergeFuser::fuse_delete_row(
    const blocksstable::ObDatumRow &del_row,
    const int64_t rowkey_column_cnt)
{
  int ret = OB_SUCCESS;

  if (!del_row.row_flag_.is_delete()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected row flag", K(ret));
  } else {
    int64_t copy_cnt = rowkey_column_cnt;
    if (del_row.is_uncommitted_row()){
      copy_cnt = result_row_.count_;
    }
    for (int64_t i = 0; i < copy_cnt; ++i) {
      result_row_.storage_datums_[i] = del_row.storage_datums_[i];
    }
    for (int64_t i = copy_cnt; i < result_row_.count_; ++i) {
      result_row_.storage_datums_[i].set_nop();
    }
    if (OB_SUCC(ret)) {
      result_row_.row_flag_.set_flag(ObDmlFlag::DF_DELETE);
      result_row_.mvcc_row_flag_ = del_row.mvcc_row_flag_;
      result_row_.set_compacted_multi_version_row();
      STORAGE_LOG(DEBUG, "fuse delete row", K(ret), K(del_row), K(result_row_));
    }
  }

  return ret;
}

bool ObMergeFuser::is_valid() const
{
  return (is_inited_ && column_cnt_ > 0);
}

/*
 *ObIPartitionMergeFuser
 */
bool ObIPartitionMergeFuser::is_valid() const
{
  return ObMergeFuser::is_valid();
}

int ObIPartitionMergeFuser::init(const ObMergeParameter &merge_param, const bool is_fuse_row_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIPartitionMergeFuser init twice", K(ret));
  } else if (OB_UNLIKELY(!merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObIPartitionMergeFuser", K(merge_param), K(ret));
  } else if (OB_FAIL(inner_init(merge_param))) {
    STORAGE_LOG(WARN, "Failed to inner init", K(ret), K(*this));
  } else if (OB_FAIL(base_init(is_fuse_row_flag))){
    STORAGE_LOG(WARN, "failed to init ObMergeFuser", K(ret), K(merge_param));
  } else {
    is_inited_ = true;
    STORAGE_LOG(INFO, "Succ to init partition fuser", K(ret), K(*this));
  }
  return ret;
}

/*
 *ObDefaultMergeFuser
 */
int ObDefaultMergeFuser::init(const int64_t column_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMergeFuser init twice", K(ret));
  } else if (FALSE_IT(column_cnt_ = column_count)) {
  } else if (OB_FAIL(base_init())) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

/*
 *ObMajorPartitionMergeFuser
 */
ObMajorPartitionMergeFuser::~ObMajorPartitionMergeFuser()
{
  default_row_.reset();
  generated_cols_.reset();
}

int ObMajorPartitionMergeFuser::inner_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::schema::ObColDesc> &multi_version_column_ids = merge_param.static_param_.multi_version_column_descs_;
  const ObStorageSchema *schema = merge_param.get_schema();
  column_cnt_ = multi_version_column_ids.count();
  const bool need_trim_default_row = cluster_version_ >= DATA_VERSION_4_3_1_0 || cluster_version_ < DATA_VERSION_4_3_0_0;

  if (OB_FAIL(default_row_.init(allocator_, column_cnt_))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K_(column_cnt));
  } else if (OB_FAIL(schema->get_orig_default_row(multi_version_column_ids, need_trim_default_row, default_row_))) {
    STORAGE_LOG(WARN, "Failed to get default row from table schema", K(ret), K(multi_version_column_ids));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator_, multi_version_column_ids, default_row_))) {
    STORAGE_LOG(WARN, "fail to fill lob header for default row", K(ret), K(multi_version_column_ids));
  } else if (FALSE_IT(default_row_.row_flag_.set_flag(ObDmlFlag::DF_UPDATE))) {
  } else if (OB_FAIL(generated_cols_.init(column_cnt_))) {
    LOG_WARN("Fail to init generated_cols", K(ret), K_(column_cnt));
  } else {
    const ObColumnSchemaV2 *column_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt_; ++i) {
      if (OB_HIDDEN_TRANS_VERSION_COLUMN_ID == multi_version_column_ids.at(i).col_id_ ||
          OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == multi_version_column_ids.at(i).col_id_) {
        // continue;
      } else{
        const ObStorageColumnSchema *column_schema = NULL;
        if (OB_ISNULL(column_schema = schema->get_column_schema(
                multi_version_column_ids.at(i).col_id_))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "The column schema is NULL", K(ret), K(i), K(multi_version_column_ids.at(i)));
        } else if (column_schema->is_generated_column()
            && !schema->is_storage_index_table()) {
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

int ObMajorPartitionMergeFuser::end_fuse_row(const storage::ObNopPos &nop_pos, blocksstable::ObDatumRow &result_row)
{
  int ret = OB_SUCCESS;
  if (nop_pos.count() > 0 && result_row.row_flag_.is_exist_without_delete()) {
    if (generated_cols_.count() > 0) {
      // add defense for generated exprs
      int64_t idx = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < nop_pos.count(); i++) {
        if (OB_FAIL(nop_pos.get_nop_pos(i, idx))) {
          LOG_WARN("Failed to get nop pos", K(i), K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < generated_cols_.count(); j++) {
            if (idx == generated_cols_.at(j)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Found nop generated columns in major fuser", K(ret), K(i), K(idx),
                       K(nop_pos), K_(generated_cols), K(result_row));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool final_result = false;
      if (OB_FAIL(add_fuse_row(default_row_, final_result))) {
        STORAGE_LOG(WARN, "Failed to fuse default row", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && result_row.row_flag_.is_exist_without_delete()) {
    result_row.row_flag_.reset();
    result_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  }
  return ret;
}

/*
 *ObMinorPartitionMergeFuser
 */

bool ObMinorPartitionMergeFuser::is_valid() const
{
  return ObIPartitionMergeFuser::is_valid() && multi_version_rowkey_column_cnt_ > 0;
}

int ObMinorPartitionMergeFuser::inner_init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  int64_t column_cnt = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIPartitionMergeFuser init twice", K(ret));
  } else if (OB_FAIL(merge_param.get_schema()->get_store_column_count(column_cnt, true/*full_col*/))) {
    STORAGE_LOG(WARN, "failed to get store column count", K(ret), K(merge_param.get_schema()));
  } else {
    column_cnt_ = column_cnt + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    multi_version_rowkey_column_cnt_ = merge_param.static_param_.multi_version_column_descs_.count();
  }

  return ret;
}

int ObMinorPartitionMergeFuser::end_fuse_row(const storage::ObNopPos &nop_pos, blocksstable::ObDatumRow &result_row)
{
  int ret = OB_SUCCESS;
  if (result_row.row_flag_.is_delete() || nop_pos.count_ == 0) {
    result_row.set_compacted_multi_version_row();
  }
  return ret;
}

int ObMinorPartitionMergeFuser::preprocess_fuse_row(const blocksstable::ObDatumRow &row, bool &is_need_fuse)
{
  int ret = OB_SUCCESS;
  is_need_fuse = true;
  clean_nop_pos_and_result_row();
  if (row.trans_id_.is_valid()) {
    if (!row.mvcc_row_flag_.is_uncommitted_row()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("uncommitted row is not valid",K(ret), K(row), K(row.trans_id_));
    } else {
      set_trans_id(row.trans_id_);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (row.row_flag_.is_delete()) {
    if (OB_FAIL(fuse_delete_row(row, multi_version_rowkey_column_cnt_))) {
      STORAGE_LOG(WARN, "failed to fuse_delete_row", K(ret), K(row), K(multi_version_rowkey_column_cnt_));
    } else {
      is_need_fuse = false;
    }
  }
  return ret;
}


int ObMergeFuserBuilder::build(const ObMergeParameter &merge_param,
                               const int64_t cluster_version,
                               ObIAllocator &allocator,
                               ObIPartitionMergeFuser *&partition_fuser)
{
  int ret = OB_SUCCESS;
  bool is_fuse_row_flag = true;
  partition_fuser = nullptr;
  if (OB_UNLIKELY(!merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build MergeFuser", K(merge_param), K(ret));
  } else {
    const ObMergeType merge_type = merge_param.static_param_.get_merge_type();
    if (is_major_or_meta_merge_type(merge_type) && !merge_param.get_schema()->is_row_store()) {
      partition_fuser = alloc_helper<ObCOMinorSSTableFuser>(allocator, allocator);
    } else if (is_major_or_meta_merge_type(merge_type)) {
      is_fuse_row_flag = false;
      partition_fuser = alloc_helper<ObMajorPartitionMergeFuser>(allocator, allocator, cluster_version);
    } else {
      partition_fuser = alloc_helper<ObMinorPartitionMergeFuser>(allocator, allocator);
    }

    if (OB_ISNULL(partition_fuser)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory for partition fuser", K(ret), K(merge_param));
    } else if (OB_FAIL(partition_fuser->init(merge_param, is_fuse_row_flag))) {
      STORAGE_LOG(WARN, "Failed to init partition fuser", K(ret));
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(partition_fuser)) {
        partition_fuser->~ObIPartitionMergeFuser();
        allocator.free(partition_fuser);
        partition_fuser = nullptr;
      }
    }
  }
  return ret;
}

} //compaction
} //oceanbase
