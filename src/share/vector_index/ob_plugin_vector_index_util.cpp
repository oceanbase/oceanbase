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
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
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

int ObVectorQueryVidIterator::init(int64_t need_count, ObIAllocator *allocator)
{
  INIT_SUCC(ret);
  if (need_count == 0 || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get vids or allocator", K(ret), K(allocator), K(need_count));
  } else if (OB_ISNULL(row_ = static_cast<ObNewRow *>(allocator->alloc(sizeof(ObNewRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else if (OB_ISNULL(obj_ = static_cast<ObObj *>(allocator->alloc(sizeof(ObObj) * (2 + extra_column_count_ + rel_count_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else if (OB_ISNULL(vids_ = static_cast<int64_t *>(allocator->alloc(sizeof(int64_t) * need_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator vids.", K(ret), K(need_count));
  } else if (OB_ISNULL(distance_ = static_cast<float *>(allocator->alloc(sizeof(float) * need_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator vids.", K(ret), K(need_count));
  } else {
    is_init_ = true; // obj is inited
    if (extra_column_count_ > 0 && OB_FAIL(extra_info_ptr_.init(allocator, extra_info_actual_size_, need_count))) {
      LOG_WARN("failed to init extra info array", K(ret), K(extra_info_ptr_), K(need_count));
    } else if (OB_FAIL(reset_obj())) {
      is_init_ = false;
      LOG_WARN("failed to reset obj", K(ret));
    } else {
      total_ = 0;
      cur_pos_ = 0;
      alloc_size_ = need_count;
      allocator_ = allocator;
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
  } else if (OB_ISNULL(obj_ = static_cast<ObObj*>(allocator->alloc(sizeof(ObObj) * (2 + extra_column_count_ + rel_count_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator NewRow.", K(ret));
  } else {
    for (int i = 0; i < 2 + extra_column_count_ + rel_count_; i++) {
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

int ObVectorQueryVidIterator::reset_obj()
{
  INIT_SUCC(ret);
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter is not initialized.", K(ret));
  } else {
    for (int i = 0; i < 2 + extra_column_count_ + rel_count_; i++) {
      obj_[i].reset();
    }
  }
  return ret;
}

int ObVectorQueryVidIterator::get_next_row(ObNewRow *&row, const sql::ExprFixedArray& res_exprs, const bool no_rel)
{
  INIT_SUCC(ret);
  if (OB_FAIL(reset_obj())) {
    LOG_WARN("fail to reset obj.", K(ret));
  } else if (cur_pos_ < total_) {
    row_->reset();
    obj_[0].set_float(distance_[cur_pos_]);
    obj_[1].set_int(vids_[cur_pos_]);
    // set extra info if need
    if (!extra_info_ptr_.is_null()) {
      if (res_exprs.count() != rel_count_ + extra_column_count_ + 1) {
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
    // set relevance if need
    if (OB_SUCC(ret) && (rel_count_ > 0 && !no_rel) && OB_NOT_NULL(rel_map_ptr_)) {
      double* rel_ptr = nullptr;
      if (OB_FAIL(rel_map_ptr_->get_refactored(vids_[cur_pos_], rel_ptr))) {
        LOG_WARN("failed to find rel_ptr", K(ret), K(vids_[cur_pos_]));
      } else if (OB_ISNULL(rel_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rel_ptr is null", K(ret), K(vids_[cur_pos_]));
      }
      for (int i = 0; i < rel_count_ && OB_SUCC(ret); i++) {
        obj_[2 + extra_column_count_ + i].set_meta_type(res_exprs.at(i + extra_column_count_ + 1)->obj_meta_);
        obj_[2 + extra_column_count_ + i].set_double_value(rel_ptr[i]);
      }
    }

    cur_pos_++;
    row_->cells_ = obj_;
    row_->count_ = 2 + extra_column_count_ + rel_count_;
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
      int64_t one_size_obj_count = 2 + extra_column_count_ + rel_count_;
      if (OB_ISNULL(row = static_cast<ObNewRow *>(allocator_->alloc(sizeof(ObNewRow))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator NewRow.", K(ret));
      } else if (OB_ISNULL(obj = static_cast<ObObj *>(allocator_->alloc(sizeof(ObObj) * one_size_obj_count * batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator NewRow.", K(ret));
      } else {
        int64_t index = 0;
        for (; index < batch_size_ && cur_pos_ < total_ && OB_SUCC(ret); ++index) {
          obj[index * one_size_obj_count].set_float(distance_[cur_pos_]);
          obj[index * one_size_obj_count + 1].set_int(vids_[cur_pos_]);
          // set extra info if need
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
          // set relevance if need
          if (OB_SUCC(ret) && rel_count_ > 0 && OB_NOT_NULL(rel_map_ptr_)) {
            double* rel_ptr = nullptr;
            if (OB_FAIL(rel_map_ptr_->get_refactored(vids_[cur_pos_], rel_ptr))) {
              LOG_WARN("failed to find rel_ptr", K(ret), K(vids_[cur_pos_]));
            } else if (OB_ISNULL(rel_ptr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("rel_ptr is null", K(ret), K(vids_[cur_pos_]));
            }
            for (int i = 0; i < rel_count_ && OB_SUCC(ret); i++) {
              obj[index * one_size_obj_count + 2 + extra_column_count_ + i].set_meta_type(res_exprs.at(i + extra_column_count_ + 1)->obj_meta_);
              obj[index * one_size_obj_count + 2 + extra_column_count_ + i].set_double_value(rel_ptr[i]);
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
  rel_count_ = 0;
  rel_map_ptr_ = nullptr;
}

int ObPluginVectorIndexHelper::driect_merge_delta_and_snap_vids(
                                                                       const ObVsagQueryResultArray &results,
                                                                       int64_t &actual_cnt,
                                                                       int64_t *&vids_result,
                                                                       float *&float_result,
                                                                       ObVecExtraInfoPtr &extra_info_result)
{
  INIT_SUCC(ret);
  actual_cnt = 0;
  int64_t res_num = 0;
  if (results.count() <= 0) {
    LOG_INFO("emptry result array", K(actual_cnt), K(results.count()));
  } else if (results.count() == 1) {
    const ObVsagQueryResult &first = *results.at(0);
    while (res_num < first.total_ && OB_SUCC(ret)) {
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
  } else {
    const int64_t hashset_size = 32;
    common::hash::ObHashSet<int64_t, common::hash::NoPthreadDefendMode> vid_hash_set;
    if (OB_FAIL(vid_hash_set.create(hashset_size))){
      LOG_WARN("fail to create vid hashset id set failed", KR(ret), K(hashset_size));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
        const ObVsagQueryResult &result = *results.at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < result.total_; ++j) {
          const int64_t vid = result.vids_[j];
          if (OB_FAIL(vid_hash_set.set_refactored(vid, 0/* flag=0, not overwrite*/))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
              LOG_TRACE("vid exist", K(vid), K(i), K(j));
            } else {
              LOG_WARN("fail to set vid to hashset", K(vid));
            }
          } else {
            if (!extra_info_result.is_null()) {
              if (OB_FAIL(extra_info_result.set_with_copy(res_num, result.extra_info_ptr_[j], result.extra_info_ptr_.extra_info_actual_size_))) {
                LOG_WARN("set extra info failed", K(ret), K(vid), K(i), K(j), K(res_num));
              }
            }
            if (OB_SUCC(ret)) {
              float_result[res_num] = result.distances_[j];
              vids_result[res_num++] = vid;
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      actual_cnt = res_num;
    }
  }

  return ret;
}

int ObPluginVectorIndexHelper::sort_merge_delta_and_snap_vids(
                                                         ObVsagQueryResultArray& results,
                                                         const int64_t total,
                                                         int64_t &actual_cnt,
                                                         int64_t *&vids_result,
                                                         float *&float_result,
                                                         ObVecExtraInfoPtr &extra_info_result)
{
  INIT_SUCC(ret);
  actual_cnt = 0;
  int64_t res_num = 0;
  if (results.count() <= 0) {
    LOG_INFO("emptry result array", K(total), K(results.count()));
  } else if (results.count() == 1) {
    const ObVsagQueryResult &first = *results.at(0);
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
  } else {
    const int64_t hashset_size = 32;
    common::hash::ObHashSet<int64_t, common::hash::NoPthreadDefendMode> vid_hash_set;
    if (OB_FAIL(vid_hash_set.create(hashset_size))) {
      LOG_WARN("fail to create vid hashset id set failed", KR(ret), K(hashset_size));
    } else {
      int64_t min_dist_idx = -1;
      do {
        min_dist_idx = -1;
        float min_dist = 0.0f;
        for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
          ObVsagQueryResult &result = *results.at(i);
          if (result.idx_ < result.total_) {
            if (min_dist_idx == -1 || min_dist > result.distances_[result.idx_]) {
              min_dist_idx = i;
              min_dist = result.distances_[result.idx_];
            }
          }
        }

        if (OB_SUCC(ret) && min_dist_idx >= 0) {
          ObVsagQueryResult &result = *results.at(min_dist_idx);
          const int64_t vid = result.vids_[result.idx_];
          if (OB_FAIL(vid_hash_set.set_refactored(vid, 0/* flag=0, not overwrite*/))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
              LOG_TRACE("vid exist", K(vid), K(min_dist_idx), K(min_dist), K(result.idx_));
              result.idx_++;
            } else {
              LOG_WARN("fail to set vid to hashset", K(vid));
            }
          } else {
            if (!extra_info_result.is_null()) {
              if (OB_FAIL(extra_info_result.set_with_copy(res_num, result.extra_info_ptr_[result.idx_], result.extra_info_ptr_.extra_info_actual_size_))) {
                LOG_WARN("set extra info failed", K(ret), K(min_dist_idx), K(min_dist), K(result.idx_), K(res_num));
              }
            }
            if (OB_SUCC(ret)) {
              float_result[res_num] = min_dist;
              vids_result[res_num++] = vid;
              result.idx_++;
            }
          }
        }
      } while (OB_SUCC(ret) && min_dist_idx >= 0 && res_num < total);
    }

    if (OB_SUCC(ret)) {
      actual_cnt = res_num;
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
    int64_t vector_limit = ObTenantVectorAllocator::get_vector_mem_limit_percentage(tenant_config, tenant_id);
    memory_limit = total_memory * vector_limit / 100;
    LOG_TRACE("vector index memory limit debug", K(tenant_id), K(total_memory), K(vector_limit), K(memory_limit));
  }
  return ret;
}

int ObPluginVectorIndexHelper::get_active_segment_max_size(uint64_t tenant_id, int64_t &size)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  size = 0;
  if (tenant_config.is_valid()) {
    size = tenant_config->ob_vector_index_active_segment_max_size;
  }
  if (0 == size) {
    int64_t memory_limit = 0;
    if (OB_FAIL(ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id, memory_limit))) {
      LOG_WARN("failed to get vector memory limit size", K(ret), K(tenant_id));
    } else if (memory_limit <= 4 * 1024 * 1024 * 1024L /*4GB*/) {
      size = 32 * 1024 * 1024L/*32MB*/;
    } else if (memory_limit <= 16 * 1024 * 1024 * 1024L /*4GB*/) {
      size = 64 * 1024 * 1024L/*64MB*/;
    } else {
      size = 128 * 1024 * 1024L/*128MB*/;
    }
  }
  return ret;
}

int ObPluginVectorIndexHelper::get_merge_base_percentage(const uint64_t tenant_id, int64_t &percentage)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  percentage = OB_VECTOR_INDEX_MERGE_BASE_PERCENTAGE;
  if (tenant_config.is_valid()) {
    percentage = tenant_config->ob_vector_index_merge_trigger_percentage;
    if (0 == percentage) {
      // currently treat 0 as default
      percentage = OB_VECTOR_INDEX_MERGE_BASE_PERCENTAGE;
    }
  }
  return ret;
}

bool ObPluginVectorIndexHelper::enable_persist_vector_index_incremental(const uint64_t tenant_id)
{
  bool enable = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    enable = tenant_config->_persist_vector_index_incremental;
  }
  return enable;
}

};
};
