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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_hash_partitioning_infrastructure.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

bool HashPartCols::equal(const HashPartCols& other, const common::ObIArray<common::ObColumnInfo>* hash_col_idxs) const
{
  bool result = true;
  const ObObj* lcell = NULL;
  const ObObj* rcell = NULL;
  if (OB_ISNULL(hash_col_idxs)) {
    result = false;
  } else if (nullptr == store_row_ || nullptr == other.store_row_) {
    result = false;
  } else {
    int64_t group_col_count = hash_col_idxs->count();
    for (int64_t i = 0; i < group_col_count && result; ++i) {
      int64_t group_idx = hash_col_idxs->at(i).index_;
      if (group_idx < store_row_->cnt_) {
        lcell = &store_row_->cells()[group_idx];
      }
      if (group_idx < other.store_row_->cnt_) {
        rcell = &other.store_row_->cells()[group_idx];
      }
      if (NULL == lcell || NULL == rcell) {
        result = false;
      } else {
        result = lcell->is_equal(*rcell, hash_col_idxs->at(i).cs_type_);
      }
    }
  }
  return result;
}

bool HashPartCols::equal_temp(
    const TempHashPartCols& other, const common::ObIArray<common::ObColumnInfo>* hash_col_idxs) const
{
  bool result = true;
  const ObObj* lcell = NULL;
  const ObObj* rcell = NULL;
  if (OB_ISNULL(hash_col_idxs)) {
    result = false;
  } else if (nullptr == store_row_ || nullptr == other.row_) {
    result = false;
  } else {
    int64_t group_col_count = hash_col_idxs->count();
    for (int64_t i = 0; i < group_col_count && result; ++i) {
      int64_t group_idx = hash_col_idxs->at(i).index_;
      if (group_idx < store_row_->cnt_) {
        lcell = &store_row_->cells()[group_idx];
      }
      if (group_idx < other.row_->get_count()) {
        rcell = &other.row_->get_cell(group_idx);
      }
      if (NULL == lcell || NULL == rcell) {
        result = false;
      } else {
        result = lcell->is_equal(*rcell, hash_col_idxs->at(i).cs_type_);
      }
    }
  }
  return result;
}
