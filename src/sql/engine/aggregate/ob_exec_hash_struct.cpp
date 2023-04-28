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

#include "ob_exec_hash_struct.h"
#include "common/row/ob_row_store.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

//When there's stored_row_ and reserved_cells_, use store_row's reserved_cells_ for calc hash.
//Other, use row_ for calc hash
uint64_t ObHashCols::inner_hash() const
{
  uint64_t result = 99194853094755497L;
  if (hash_col_idx_ != NULL) {
    int64_t group_col_count = hash_col_idx_->count();
    if (stored_row_ != NULL
        && stored_row_->reserved_cells_count_ > 0) {
      const ObObj *cells = stored_row_->reserved_cells_;
      for (int32_t i = 0; i < group_col_count; ++i) {
        if (hash_col_idx_->at(i).index_ < stored_row_->reserved_cells_count_) {
          const ObObj &cell = cells[hash_col_idx_->at(i).index_];
          if (cell.is_string_type()) {
            result = cell.varchar_murmur_hash(hash_col_idx_->at(i).cs_type_, result);
          } else {
            cell.hash(result, result);
          }
        }
      }
    } else if (row_ != NULL && row_->is_valid()) {
      const ObObj *cells = row_->cells_;
      const int32_t *projector = row_->projector_;
      for (int64_t i = 0; i < group_col_count; ++i) {
        int64_t real_index = row_->projector_size_ > 0 ?
            projector[hash_col_idx_->at(i).index_] : hash_col_idx_->at(i).index_;
        const ObObj &cell = cells[real_index];
        if (cell.is_string_type()) {
          result = cell.varchar_murmur_hash(hash_col_idx_->at(i).cs_type_, result);
        } else {
          cell.hash(result, result);
        }
      }
    }
  }
  return result;
}

//When there is stored_row_ reserved_cells, use stored_row_'s reserved_cells_ for calc equal.
//Other use row_.
bool ObHashCols::operator ==(const ObHashCols &other) const
{
  bool result = true;
  const ObObj *lcell = NULL;
  const ObObj *rcell = NULL;

  if (OB_ISNULL(hash_col_idx_)) {
    result = false;
  } else {
    int64_t group_col_count = hash_col_idx_->count();
    //当group_col_count<=0是hash group by const的情况，应该认为是一个分组，所以result要初始化为true
    for (int32_t i = 0; i < group_col_count && result; ++i) {
      int64_t group_idx = hash_col_idx_->at(i).index_;
      if (stored_row_ != NULL) {
        if (group_idx < stored_row_->reserved_cells_count_) {
          lcell = &stored_row_->reserved_cells_[group_idx];
        }
      } else if (row_ != NULL) {
        if (row_->is_valid()) {
          int64_t real_idx = row_->projector_size_ > 0 ? row_->projector_[group_idx] : group_idx;
          lcell = &row_->cells_[real_idx];
        }
      }

      if (other.stored_row_ != NULL) {
        if (group_idx < other.stored_row_->reserved_cells_count_) {
          rcell = &other.stored_row_->reserved_cells_[group_idx];
        }
      } else if (other.row_ != NULL) {
        if (other.row_->is_valid()) {
          int64_t real_idx = other.row_->projector_size_ > 0 ? other.row_->projector_[group_idx] : group_idx;
          rcell = &other.row_->cells_[real_idx];
        }
      }
      if (NULL == lcell || NULL == rcell) {
        result = false;
      } else {
        result = lcell->is_equal(*rcell, hash_col_idx_->at(i).cs_type_);
      }
    }
  }
  return result;
}

void ObHashCols::set_stored_row(const ObRowStore::StoredRow *stored_row)
{
  stored_row_ = stored_row;
  row_ = NULL;
}
}//ns sql
}//ns oceanbase

