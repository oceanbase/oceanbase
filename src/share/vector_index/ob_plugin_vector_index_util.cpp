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
 * This file is for define of plugin vector index util
 */

#define USING_LOG_PREFIX SHARE

#include "ob_plugin_vector_index_util.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/das/iter/ob_das_iter.h"

using namespace oceanbase::sql;

namespace oceanbase
{
namespace share
{
int ObVectorQueryRowkeyIterator::init(int64_t total, ObIArray<common::ObRowkey> *rowkeys)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(rowkeys) || total != rowkeys->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get rowkeys", K(ret), K(rowkeys), K(total));
  } else {
    rowkeys_ = rowkeys;
    total_ = total;
    cur_pos_ = 0;
    is_init_ = true;
  }

  return ret;
}

int ObVectorQueryRowkeyIterator::init(sql::ObDASIter *rowkey_scan_iter)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(rowkey_scan_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null ", K(ret), K(rowkey_scan_iter));
  } else if (!rowkey_scan_iter->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("res expr idx is invalid", K(ret), K(rowkey_scan_iter));
  } else {
    scan_iter_ = rowkey_scan_iter;
    is_init_ = true;
  }

  return ret;
}

int ObVectorQueryRowkeyIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret));
  } else if (OB_ISNULL(scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey scan iter is null", K(ret), K(scan_iter_));
  } else if (!scan_iter_->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("rowkey scan iter is not inited", K(ret), K(scan_iter_));
  } else if (OB_FALSE_IT(scan_iter_->clear_evaluated_flag())) {
  } else if (OB_FAIL(scan_iter_->get_next_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row", K(ret));
    }
  }

  return ret;
}

int ObVectorQueryRowkeyIterator::get_next_rows(int64_t &count)
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret));
  } else if (batch_size_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch_size_ is not init", K(ret), K_(batch_size));
  } else if (OB_ISNULL(scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey scan iter is null", K(ret), K(scan_iter_));
  } else if (!scan_iter_->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("rowkey scan iter is not inited", K(ret), K(scan_iter_));
  } else if (OB_FALSE_IT(scan_iter_->clear_evaluated_flag())) {
  } else if (OB_FAIL(scan_iter_->get_next_rows(count, batch_size_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next rows", K(ret), K(count), K(batch_size_));
    }
  }
  return ret;
}

int ObVectorQueryRowkeyIterator::get_next_row(ObRowkey &rowkey)
{
  INIT_SUCC(ret);
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret));
  } else if (cur_pos_ < total_) {
    rowkey = rowkeys_->at(cur_pos_++);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObVectorQueryRowkeyIterator::get_next_rows(ObIArray<common::ObRowkey>& rowkeys, int64_t &row_count)
{
  INIT_SUCC(ret);
  row_count = 0;
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret));
  } else if (cur_pos_ < total_) {
    ObObj *obj = nullptr;
    if (batch_size_ > 0) {
      if (OB_FAIL(rowkeys.reserve(batch_size_))) {
        LOG_WARN("failed reserve rowkeys", K(ret));
      } else {
        int64_t index = 0;
        for (; index < batch_size_ && cur_pos_ < total_; ++index) {
          rowkeys.push_back(rowkeys_->at(cur_pos_++));
        }
        row_count = index;
      }
    }
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}

void ObVectorQueryRowkeyIterator::reset()
{
  is_init_ = false;
  total_ = 0;
  cur_pos_ = 0;
  rowkeys_ = nullptr;
  scan_iter_ = nullptr;
}

int ObVectorQueryVidIterator::init()
{
  INIT_SUCC(ret);
  if (extra_column_count_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extra_column_count_ is invalid", K(ret), K(extra_column_count_));
  } else if (OB_ISNULL(row_ = static_cast<ObNewRow *>(allocator_->alloc(sizeof(ObNewRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else if (OB_ISNULL(obj_ = static_cast<ObObj *>(allocator_->alloc(sizeof(ObObj) * (2 + extra_column_count_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else {
    is_init_ = true;
  }
  return ret;
}

int ObVectorQueryVidIterator::init(int64_t total, int64_t *vids, float *distances, ObIAllocator *allocator)
{
  INIT_SUCC(ret);
  if (extra_column_count_ != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extra_column_count_ is invalid", K(ret), K(extra_column_count_));
  } else if ((OB_ISNULL(vids) && total != 0) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get vids or allocator", K(ret), K(vids), K(allocator));
  } else if (OB_ISNULL(row_ =  OB_NEWx(ObNewRow, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else if (OB_ISNULL(obj_ = static_cast<ObObj*>(allocator->alloc(sizeof(ObObj) * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else {
    obj_[0].reset();
    obj_[1].reset();
    is_init_ = true;
    total_ = total;
    alloc_size_ = total_;
    cur_pos_ = 0;
    vids_ = vids;
    allocator_ = allocator;
    distance_ = distances;
  }
  return ret;
}

int ObVectorQueryVidIterator::init(int64_t need_count, ObIAllocator *allocator)
{
  INIT_SUCC(ret);
  if (need_count == 0 || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get vids or allocator", K(ret), K(allocator), K(need_count));
  } else if (OB_ISNULL(row_ = static_cast<ObNewRow *>(allocator->alloc(sizeof(ObNewRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else if (OB_ISNULL(obj_ = static_cast<ObObj *>(allocator->alloc(sizeof(ObObj) * (2 + extra_column_count_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else if (OB_ISNULL(vids_ = static_cast<int64_t *>(allocator->alloc(sizeof(int64_t) * need_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator vids.", K(ret), K(need_count));
  } else if (OB_ISNULL(distance_ = static_cast<float *>(allocator->alloc(sizeof(float) * need_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator vids.", K(ret), K(need_count));
  } else {
    if (extra_column_count_ > 0 && OB_FAIL(extra_info_ptr_.init(allocator, extra_info_actual_size_, need_count))) {
      LOG_WARN("failed to init extra info array", K(ret), K(extra_info_ptr_), K(need_count));
    } else {
      for (int i = 0; i < 2 + extra_column_count_; i++) {
        obj_[i].reset();
      }
      total_ = 0;
      cur_pos_ = 0;
      alloc_size_ = need_count;
      allocator_ = allocator;
      is_init_ = true;
    }
  }
  return ret;
}

int ObVectorQueryVidIterator::add_results(int64_t add_cnt, int64_t *add_vids, float *add_distance, const ObVecExtraInfoPtr &extra_infos)
{
  INIT_SUCC(ret);
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret));
  } else if (!extra_infos.is_null() && extra_infos.count_ != add_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra info array count is not equal to add cnt", K(ret), K(extra_infos), K(add_cnt));
  } else if (!extra_infos.is_null() && extra_info_ptr_.is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra info array not empty extra_info_ptr_ is null", K(ret), K(extra_infos), K(add_cnt));
  } else {
    int start_pos = total_;
    for (int64_t i = 0; i < add_cnt && (start_pos + i < alloc_size_) && OB_SUCC(ret); ++i) {
      vids_[start_pos + i] = add_vids[i];
      distance_[start_pos + i] = add_distance[i];
      if (!extra_infos.is_null()) {
        if (OB_FAIL(extra_info_ptr_.set_no_copy(start_pos + i, extra_infos[i]))) {
          LOG_WARN("failed to set extra info", K(ret), K(extra_info_ptr_), K(extra_infos));
        }
      }
      ++total_;
    }
  }
  return ret;
}

int ObVectorQueryVidIterator::add_result(int64_t add_vid, float add_distance, const char *extra_info)
{
  INIT_SUCC(ret);
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret));
  } else if ((extra_info_ptr_.is_null() && OB_NOT_NULL(extra_info)) ||
             (OB_ISNULL(extra_info) && !extra_info_ptr_.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra info is null", K(ret), KP(extra_info), K(extra_info_ptr_), K(extra_column_count_));
  } else if (total_ < alloc_size_) {
    vids_[total_] = add_vid;
    distance_[total_] = add_distance;
    if (OB_NOT_NULL(extra_info)) {
      if (OB_FAIL(extra_info_ptr_.set_no_copy(total_, extra_info))){
        LOG_WARN("failed to set extra info", K(ret), K(extra_info_ptr_), K(extra_info));
      }
    }
    ++total_;
  }
  return ret;
}

int ObVectorQueryVidIterator::init(int64_t total, int64_t *vids, float *distances, const ObVecExtraInfoPtr& extra_info_ptr, ObIAllocator *allocator) {
  INIT_SUCC(ret);
  if (extra_column_count_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extra_column_count_ is invalid", K(ret), K(extra_column_count_));
  } else if (total != 0 && ((extra_column_count_ > 0 && extra_info_ptr.is_null()) || (extra_column_count_ == 0 && !extra_info_ptr.is_null()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extra_info_ptr is null", K(ret), K(extra_column_count_));
  } else if ((OB_ISNULL(vids) && total != 0) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get vids or allocator", K(ret), K(vids), K(allocator));
  } else if (OB_ISNULL(row_ =  OB_NEWx(ObNewRow, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else if (OB_ISNULL(obj_ = static_cast<ObObj*>(allocator->alloc(sizeof(ObObj) * (2 + extra_column_count_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else {
    for (int i = 0; i < 2 + extra_column_count_; i++) {
      obj_[i].reset();
    }
    is_init_ = true;
    total_ = total;
    cur_pos_ = 0;
    vids_ = vids;
    allocator_ = allocator;
    distance_ = distances;
    extra_info_ptr_ = extra_info_ptr;
  }
  return ret;
}

int ObVectorQueryVidIterator::get_next_row(ObNewRow *&row, const sql::ExprFixedArray& res_exprs)
{
  INIT_SUCC(ret);
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret));
  } else if (cur_pos_ < total_) {
    obj_[0].reset();
    obj_[1].reset();
    for (int i = 0; i < extra_column_count_; i++) {
      obj_[i + 2].reset();
    }
    row_->reset();

    obj_[0].set_float(distance_[cur_pos_]);
    obj_[1].set_int(vids_[cur_pos_]);
    if (!extra_info_ptr_.is_null()) {
      if (res_exprs.count() != extra_column_count_ + 1) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("res_exprs is not vid, extra_column", K(ret), K(res_exprs.count()), K(extra_column_count_));
      }
      if (OB_SUCC(ret)) {
        for (int i = 0; i < extra_column_count_; i++) {
          obj_[2 + i].set_meta_type(res_exprs.at(i + 1)->obj_meta_);
        }
        if (OB_FAIL(ObVecExtraInfo::extra_buf_to_obj(extra_info_ptr_[cur_pos_], extra_info_actual_size_,
                                                     extra_column_count_, obj_ + 2))) {
          LOG_WARN("failed to get extra info", K(ret), K(extra_info_ptr_[cur_pos_]), K(extra_info_actual_size_),
                   K(extra_column_count_));
        }
      }
    }

    cur_pos_++;
    row_->cells_ = obj_;
    row_->count_ = 2 + extra_column_count_;
    row_->projector_ = NULL;
    row_->projector_size_ = 0;

    row = row_;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObVectorQueryVidIterator::get_next_rows(ObNewRow *&row, int64_t &size, const sql::ExprFixedArray& res_exprs)
{
  INIT_SUCC(ret);
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret), K(is_init_), K(alloc_size_), K(total_));
  } else if (cur_pos_ < total_) {
    size = 0;
    row = nullptr;
    ObObj *obj = nullptr;
    if (batch_size_ > 0) {
      // row.cells: [dis\vid\extras][dis\vid\extras]...
      // Indicates how many obj are in one row, 2 represents vid and dis
      int64_t one_size_obj_count = 2 + extra_column_count_;
      if (OB_ISNULL(row = static_cast<ObNewRow *>(allocator_->alloc(sizeof(ObNewRow))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator NewRow.", K(ret));
      } else if (OB_ISNULL(obj = static_cast<ObObj *>(allocator_->alloc(sizeof(ObObj) * one_size_obj_count * batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator NewRow.", K(ret));
      } else {
        int64_t index = 0;
        for (; index < batch_size_ && cur_pos_ < total_; ++index) {
          obj[index * one_size_obj_count].set_float(distance_[cur_pos_]);
          obj[index * one_size_obj_count + 1].set_int(vids_[cur_pos_]);
          if (!extra_info_ptr_.is_null()) {
            if (res_exprs.count() != extra_column_count_ + 1) {
              ret = OB_ERR_UNDEFINED;
              LOG_WARN("res_exprs is not vid, extra_column", K(ret), K(res_exprs.count()), K(extra_column_count_));
            }
            if (OB_SUCC(ret)) {
              for (int i = 0; i < extra_column_count_; i++) {
                obj[index * one_size_obj_count + 2 + i].set_meta_type(res_exprs.at(i + 1)->obj_meta_);
              }
              if (OB_FAIL(ObVecExtraInfo::extra_buf_to_obj(extra_info_ptr_[cur_pos_], extra_info_actual_size_,
                                                           extra_column_count_,
                                                           obj + index * one_size_obj_count + 2))) {
                LOG_WARN("failed to get extra info", K(ret), K(extra_info_ptr_[cur_pos_]), K(extra_info_actual_size_),
                         K(extra_column_count_));
              }
            }
          }
          cur_pos_++;
        }
        row->cells_ = obj;
        row->count_ = index * one_size_obj_count;
        row->projector_ = NULL;
        row->projector_size_ = 0;
        size = index;
      }
    }
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}

void ObVectorQueryVidIterator::reset()
{
  is_init_ = false;
  total_ = 0;
  cur_pos_ = 0;
  extra_info_ptr_.reset();
}

int ObPluginVectorIndexHelper::driect_merge_delta_and_snap_vids(const ObVsagQueryResult &first,
                                                                       const ObVsagQueryResult &second,
                                                                       int64_t &actual_cnt,
                                                                       int64_t *&vids_result,
                                                                       float *&float_result,
                                                                       ObVecExtraInfoPtr &extra_info_result)
{
  INIT_SUCC(ret);
  actual_cnt = 0;
  int64_t res_num = 0;
  if (first.total_ == 0) {
    while (res_num < second.total_ && OB_SUCC(ret)) {
      if (!extra_info_result.is_null()) {
        if (OB_FAIL(extra_info_result.set_with_copy(res_num, second.extra_info_ptr_[res_num], second.extra_info_ptr_.extra_info_actual_size_))) {
          LOG_WARN("set extra info failed", K(ret), K(second.extra_info_ptr_), K(res_num));
        }
      }
      vids_result[res_num] = second.vids_[res_num];
      float_result[res_num] = second.distances_[res_num];
      res_num++;
    }
    actual_cnt = res_num;
  } else {
    const int64_t hashset_size = first.total_;
    common::hash::ObHashSet<int64_t> vid_hash_set;
    if (OB_FAIL(vid_hash_set.create(hashset_size))){
      LOG_WARN("fail to create vid hashset id set failed", KR(ret), K(hashset_size));
    } else {
      while (res_num < first.total_ && OB_SUCC(ret)) {
        if (OB_FAIL(vid_hash_set.set_refactored(first.vids_[res_num]))) {
          LOG_WARN("fail to set vid to hashset", K(first.vids_[res_num]));
        } else {
          if (!extra_info_result.is_null()) {
            if (OB_FAIL(extra_info_result.set_with_copy(res_num, first.extra_info_ptr_[res_num], first.extra_info_ptr_.extra_info_actual_size_))) {
              LOG_WARN("set extra info failed", K(ret), K(first.extra_info_ptr_), K(res_num));
            }
          }
          vids_result[res_num] = first.vids_[res_num];
          float_result[res_num] = first.distances_[res_num];
          res_num++;
        }
      }
      int64_t i = res_num;
      while (i < first.total_ + second.total_ && OB_SUCC(ret)) {
        ret = vid_hash_set.exist_refactored(second.vids_[res_num - first.total_]);
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
          i++; // skip
        } else if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          if (!extra_info_result.is_null()) {
            if (OB_FAIL(extra_info_result.set_with_copy(res_num, second.extra_info_ptr_[res_num - first.total_], second.extra_info_ptr_.extra_info_actual_size_))) {
              LOG_WARN("set extra info failed", K(ret), K(second.extra_info_ptr_), K(res_num));
            }
          }
          vids_result[res_num] = second.vids_[res_num - first.total_];
          float_result[res_num] = second.distances_[res_num -first.total_];
          res_num++;
          i++;
        } else {
          LOG_WARN("fail to check exist refactored", K(ret));
        }
      }
      actual_cnt = res_num;
    }
  }

  return ret;
}

int ObPluginVectorIndexHelper::sort_merge_delta_and_snap_vids(const ObVsagQueryResult &first,
                                                         const ObVsagQueryResult &second,
                                                         const int64_t total,
                                                         int64_t &actual_cnt,
                                                         int64_t *&vids_result,
                                                         float *&float_result,
                                                         ObVecExtraInfoPtr &extra_info_result)
{
  INIT_SUCC(ret);
  actual_cnt = 0;
  int64_t res_num = 0;
  if (first.total_ == 0) {
    while (res_num < total && res_num < second.total_ && OB_SUCC(ret)) {
      if (!extra_info_result.is_null()) {
        if (OB_FAIL(extra_info_result.set_with_copy(res_num, second.extra_info_ptr_[res_num], second.extra_info_ptr_.extra_info_actual_size_))) {
          LOG_WARN("set extra info failed", K(ret), K(second.extra_info_ptr_), K(res_num));
        }
      }
      vids_result[res_num] = second.vids_[res_num];
      float_result[res_num] = second.distances_[res_num];
      res_num++;
    }
    actual_cnt = res_num;
  } else if (second.total_ == 0) {
    while (res_num < total && res_num < first.total_ && OB_SUCC(ret)) {
      if (!extra_info_result.is_null()) {
        if (OB_FAIL(extra_info_result.set_with_copy(res_num, first.extra_info_ptr_[res_num], first.extra_info_ptr_.extra_info_actual_size_))) {
          LOG_WARN("set extra info failed", K(ret), K(first.extra_info_ptr_), K(res_num));
        }
      }
      vids_result[res_num] = first.vids_[res_num];
      float_result[res_num] = first.distances_[res_num];
      res_num++;
    }
    actual_cnt = res_num;
  } else if (OB_ISNULL(first.vids_) || OB_ISNULL(second.vids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get vids invalid.", K(ret), K(first.vids_), K(second.vids_));
  } else {
    const int64_t hashset_size = total;
    common::hash::ObHashSet<int64_t> vid_hash_set;
    if (OB_FAIL(vid_hash_set.create(hashset_size))){
      LOG_WARN("fail to create vid hashset id set failed", KR(ret), K(hashset_size));
    } else {
      int64_t i = 0, j = 0;
      while (OB_SUCC(ret) && res_num < total && i < first.total_ && j < second.total_) {
        if (first.distances_[i] <= second.distances_[j]) {
          int tmp_ret = vid_hash_set.exist_refactored(first.vids_[i]);
          if (OB_HASH_EXIST == tmp_ret) {
            i++; // skip
          } else if (OB_HASH_NOT_EXIST == tmp_ret) {
            if (OB_FAIL(vid_hash_set.set_refactored(first.vids_[i]))) {
              LOG_WARN("fail to set vid to hashset", K(first.vids_[i]));
            } else {
              if (!extra_info_result.is_null()) {
                if (OB_FAIL(extra_info_result.set_with_copy(res_num, first.extra_info_ptr_[i], first.extra_info_ptr_.extra_info_actual_size_))) {
                  LOG_WARN("set extra info failed", K(ret), K(first.extra_info_ptr_), K(i), K(res_num));
                }
              }
              float_result[res_num] = first.distances_[i];
              vids_result[res_num++] = first.vids_[i++];
            }
          } else {
            LOG_WARN("fail to check exist refactored", K(ret));
          }
        } else {
          int tmp_ret = vid_hash_set.exist_refactored(second.vids_[j]);
          if (OB_HASH_EXIST == tmp_ret) {
            j++; // skip
          } else if (OB_HASH_NOT_EXIST == tmp_ret) {
            if (OB_FAIL(vid_hash_set.set_refactored(second.vids_[j]))) {
              LOG_WARN("fail to set vid to hashset", K(second.vids_[j]));
            } else {
              if (!extra_info_result.is_null()) {
                if (OB_FAIL(extra_info_result.set_with_copy(res_num, second.extra_info_ptr_[j], second.extra_info_ptr_.extra_info_actual_size_))) {
                  LOG_WARN("set extra info failed", K(ret), K(first.extra_info_ptr_), K(j), K(res_num));
                }
              }
              float_result[res_num] = second.distances_[j];
              vids_result[res_num++] = second.vids_[j++];
            }
          } else {
            LOG_WARN("fail to check exist refactored", K(ret));
          }
        }
      }

      while (OB_SUCC(ret) && res_num < total && i < first.total_) {
        int tmp_ret = vid_hash_set.exist_refactored(first.vids_[i]);
        if (OB_HASH_EXIST == tmp_ret) {
          i++; // skip
        } else if (OB_HASH_NOT_EXIST == tmp_ret) {
          if (OB_FAIL(vid_hash_set.set_refactored(first.vids_[i]))) {
            LOG_WARN("fail to set vid to hashset", K(first.vids_[i]));
          } else {
            if (!extra_info_result.is_null()) {
              if (OB_FAIL(extra_info_result.set_with_copy(res_num, first.extra_info_ptr_[i], first.extra_info_ptr_.extra_info_actual_size_))) {
                LOG_WARN("set extra info failed", K(ret), K(first.extra_info_ptr_), K(i), K(res_num));
              }
            }
            float_result[res_num] = first.distances_[i];
            vids_result[res_num++] = first.vids_[i++];
          }
        } else {
          LOG_WARN("fail to check exist refactored", K(ret));
        }
      }

      while (OB_SUCC(ret) && res_num < total && j < second.total_) {
        int tmp_ret = vid_hash_set.exist_refactored(second.vids_[j]);
        if (OB_HASH_EXIST == tmp_ret) {
          j++; // skip
        } else if (OB_HASH_NOT_EXIST == tmp_ret) {
          if (OB_FAIL(vid_hash_set.set_refactored(second.vids_[j]))) {
            LOG_WARN("fail to set vid to hashset", K(second.vids_[j]));
          } else {
            if (!extra_info_result.is_null()) {
              if (OB_FAIL(extra_info_result.set_with_copy(res_num, second.extra_info_ptr_[j], second.extra_info_ptr_.extra_info_actual_size_))) {
                LOG_WARN("set extra info failed", K(ret), K(first.extra_info_ptr_), K(j), K(res_num));
              }
            }
            float_result[res_num] = second.distances_[j];
            vids_result[res_num++] = second.vids_[j++];
          }
        } else {
          LOG_WARN("fail to check exist refactored", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        actual_cnt = res_num;
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexHelper::get_vector_memory_limit_size(const uint64_t tenant_id, int64_t& memory_limit)
{
  bool ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    memory_limit = 0;
    LOG_WARN("get invalid tenant config", K(tenant_id));
  } else {
    int64_t total_memory = lib::get_tenant_memory_limit(tenant_id);
    int64_t vector_limit = ObTenantVectorAllocator::get_vector_mem_limit_percentage(tenant_config);
    memory_limit = total_memory * vector_limit / 100;
    LOG_TRACE("vector index memory limit debug", K(tenant_id), K(total_memory), K(vector_limit), K(memory_limit));
  }
  return ret;
}

};
};
