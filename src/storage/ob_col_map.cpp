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

#include "storage/ob_col_map.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
void ObColMap::destroy()
{
  reset();
}

int ObColMap::init(const int64_t col_count)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  const uint64_t count = static_cast<uint64_t>(col_count);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObColMap init twice", K(ret));
  } else if (count <= FINAL_LEVEL_MAX_COL_NUM) {
    if (OB_UNLIKELY(count > FIRST_LEVEL_MAX_COL_NUM)) {
      if (NULL == (ptr = ob_malloc(sizeof(ColMapFinal), ObModIds::OB_COL_MAP))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "fail to allocate memory for column_ids map", K(ret));
      } else {
        col_map_final_ = new (ptr) ColMapFinal();
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid column count, not support", K(ret), K(col_count));
  }
  if (OB_SUCCESS != ret && !is_inited_) {
    destroy();
  }
  return ret;
}

int ObColMap::set_refactored(const uint64_t col_id, const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ColMap is not initialized", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == col_id) || OB_UNLIKELY(col_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(col_id), K(col_idx));
  } else if (OB_UNLIKELY(NULL != col_map_final_)) {
    ret = col_map_final_->set_refactored(col_id, col_idx);
  } else {
    ret = col_map_first_.set_refactored(col_id, col_idx);
  }
  return ret;
}

int ObColMap::get_refactored(const uint64_t col_id, int64_t &col_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ColMap is not initialized", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == col_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(col_id));
  } else if (OB_UNLIKELY(NULL != col_map_final_)) {
    ret = col_map_final_->get_refactored(col_id, col_idx);
  } else {
    ret = col_map_first_.get_refactored(col_id, col_idx);
  }
  return ret;
}

const int64_t *ObColMap::get(const uint64_t col_id) const
{
  int ret = OB_SUCCESS;
  const int64_t *col_idx = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ColMap is not initialized", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == col_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(col_id));
  } else if (OB_UNLIKELY(NULL != col_map_final_)) {
    col_idx = col_map_final_->get(col_id);
  } else {
    col_idx = col_map_first_.get(col_id);
  }
  return col_idx;
}

int64_t *ObColMap::get(const uint64_t col_id)
{
  int ret = OB_SUCCESS;
  int64_t *col_idx = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ColMap is not initialized", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == col_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(col_id));
  } else if (OB_UNLIKELY(NULL != col_map_final_)) {
    col_idx = col_map_final_->get(col_id);
  } else {
    col_idx = col_map_first_.get(col_id);
  }
  return col_idx;
}

void ObColMap::reset()
{
  col_map_first_.reset();
  if (OB_UNLIKELY(NULL != col_map_final_)) {
    col_map_final_->~ColMapFinal();
    ob_free(col_map_final_);
    col_map_final_ = NULL;
  }
  is_inited_ = false;
}

int64_t ObColMap::count() const
{
  int64_t count = 0;
  if (OB_LIKELY(is_inited_)) {
    if (OB_UNLIKELY(NULL != col_map_final_)) {
      count = col_map_final_->count();
    } else {
      count = col_map_first_.count();
    }
  }
  return count;
}

} // namespace storage
} // namespace oceanbase
