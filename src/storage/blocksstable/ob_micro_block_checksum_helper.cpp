/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/blocksstable/ob_micro_block_checksum_helper.h"
namespace oceanbase
{
namespace blocksstable
{
int ObMicroBlockChecksumHelper::init(
    const common::ObIArray<share::schema::ObColDesc> *col_descs,
    const bool need_opt_row_chksum)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(col_descs)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(col_descs));
  } else {
    if (need_opt_row_chksum) {
      for (int64_t i = 0; i < col_descs->count(); ++i) {
        if (col_descs->at(i).col_type_.is_integer_type()) {
          ++integer_col_cnt_;
        }
      }
    }
    if (NEED_INTEGER_BUF_CNT < integer_col_cnt_) {
      if (LOCAL_INTEGER_COL_CNT >= integer_col_cnt_) {
        integer_col_buf_ = local_integer_col_buf_;
        integer_col_idx_ = local_integer_col_idx_;
      } else if (OB_ISNULL(integer_col_buf_ =
          static_cast<int64_t*>(allocator_.alloc(sizeof(int64_t) * integer_col_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc integer_col_buf", K(ret), K_(integer_col_cnt));
      } else if (OB_ISNULL(integer_col_idx_ =
          static_cast<int16_t*>(allocator_.alloc(sizeof(int16_t) * integer_col_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc integer_col_idx", K(ret), K_(integer_col_cnt));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // traverse once again to fill idx
    if (OB_NOT_NULL(integer_col_idx_)) {
      for (int64_t i = 0, idx = 0; i < col_descs->count(); ++i) {
        if (idx < integer_col_cnt_ && col_descs->at(i).col_type_.is_integer_type()) {
          integer_col_idx_[idx] = i;
          ++idx;
        }
      }
    }
    micro_block_row_checksum_ = 0;
    col_descs_ = col_descs;
  } else {
    reset();
  }
  return ret;
}

void ObMicroBlockChecksumHelper::reset()
{
  col_descs_ = nullptr;
  integer_col_cnt_ = 0;
  micro_block_row_checksum_ = 0;
  if (OB_NOT_NULL(integer_col_buf_)) {
    if (!is_local_buf()) {
      allocator_.free(integer_col_buf_);
    }
    integer_col_buf_ = nullptr;
  }
  if (OB_NOT_NULL(integer_col_idx_)) {
    if (!is_local_idx()) {
      allocator_.free(integer_col_idx_);
    }
    integer_col_idx_ = nullptr;
  }
  allocator_.reset();
}

int ObMicroBlockChecksumHelper::cal_rows_checksum(
    const common::ObArray<ObColDatums *> &all_col_datums,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = all_col_datums.count();
  if (OB_ISNULL(integer_col_buf_) || OB_ISNULL(integer_col_idx_)) {
    for (int64_t row_idx = 0; row_idx < row_count; row_idx++) {
      for (int64_t i = 0; i < col_cnt; ++i) {
        micro_block_row_checksum_ = all_col_datums[i]->at(row_idx).checksum(micro_block_row_checksum_);
      }
    }
  } else if (OB_ISNULL(col_descs_) || col_cnt != col_descs_->count()) { // defense
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpect error", K(ret), KPC(col_descs_), K_(integer_col_cnt), K(col_cnt));
  } else {
    for (int64_t row_idx = 0; row_idx < row_count; row_idx++) {
      for (int64_t i = 0, idx = 0; i < col_cnt; ++i) {
        if (idx < integer_col_cnt_ && integer_col_idx_[idx] == i) {
          if (all_col_datums[i]->at(row_idx).is_nop()) {
            integer_col_buf_[idx] = MAGIC_NOP_NUMBER;
          } else if (all_col_datums[i]->at(row_idx).is_null()) {
            integer_col_buf_[idx] = MAGIC_NULL_NUMBER;
          } else {
            integer_col_buf_[idx] = all_col_datums[i]->at(row_idx).get_int();
          }
          ++idx;
        } else {
          micro_block_row_checksum_ = all_col_datums[i]->at(row_idx).checksum(micro_block_row_checksum_);
        }
      }
      micro_block_row_checksum_ = ob_crc64_sse42(micro_block_row_checksum_,
          static_cast<void*>(integer_col_buf_), sizeof(int64_t) * integer_col_cnt_);
    }
  }
  return ret;
}

int ObMicroBlockChecksumHelper::cal_column_checksum(
    const common::ObArray<ObColDatums *> &all_col_datums,
    const int64_t row_count,
    int64_t *curr_micro_column_checksum)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = all_col_datums.count();

  if (OB_UNLIKELY(nullptr == curr_micro_column_checksum)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(curr_micro_column_checksum));
  } else if (OB_ISNULL(col_descs_) || col_cnt != col_descs_->count()) { // defense
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpect error", K(ret), KPC(col_descs_), K_(integer_col_cnt), K(col_cnt));
  } else {
    for (int64_t col_idx = 0; col_idx < col_cnt; col_idx++) {
      for (int64_t row_idx = 0; row_idx < row_count; row_idx++) {
        curr_micro_column_checksum[col_idx] += all_col_datums[col_idx]->at(row_idx).checksum(0);
      }
    }
  }
  return ret;
}

}//end namespace blocksstable
}//end namespace oceanbase
