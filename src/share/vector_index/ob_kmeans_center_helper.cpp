/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#include "lib/random/ob_mysql_random.h"
#define USING_LOG_PREFIX COMMON
#include "share/vector_index/ob_kmeans_center_helper.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace share {
/*
 * ObKMeansCenterHelper Impl
 */
int ObKMeansCenterHelper::init(const int64_t tenant_id,
                                    const int64_t lists,
                                    const ObVectorDistanceType distance_type,
                                    const int64_t max_iterate_times,
                                    const bool use_elkan_kmeans) {
  int ret = OB_SUCCESS;
  if (is_init_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (0 >= lists_ ||
      ObVectorDistanceType::INVALID_DISTANCE_TYPE == distance_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lists), K(distance_type));
  } else if (OB_FAIL(allocator_.init(
                 lib::ObMallocAllocator::get_instance(), PAGE_SIZE,
                 lib::ObMemAttr(tenant_id, allocator_label_)))) { // TODO(@jingshui) limit
                                                                  // the max memory size
    LOG_WARN("failed to init fifo fallocator");
  } else if (FALSE_IT(arena_allocator_.set_attr(
                 ObMemAttr(tenant_id, arena_label_)))) {
  } else if (OB_FAIL(center_vectors_[0].reserve(lists))) {
    LOG_WARN("failed to reserve cur_center_vectors", K(ret), K(lists));
  } else if (OB_FAIL(center_vectors_[1].reserve(lists))) {
    LOG_WARN("failed to reserve new_center_vectors", K(ret), K(lists));
  } else {
    max_iterate_times_ = max_iterate_times;
    use_elkan_kmeans_ = use_elkan_kmeans;
    distance_type_ = distance_type;
    tenant_id_ = tenant_id;
    lists_ = lists;
    init_lists_ = lists;
    is_init_ = true;
  }
  return ret;
}

void ObKMeansCenterHelper::reuse() {
  cur_idx_ = 0;
  total_cnt_ = 0;
  failed_times_ = 0;
  iterate_times_ = 0;

  status_ = PREPARE_CENTERS;
  ObTypeVector::reuse_array(allocator_, center_vectors_[0]);
  ObTypeVector::reuse_array(allocator_, center_vectors_[1]);
  if (OB_NOT_NULL(nearest_centers_)) {
    allocator_.free(nearest_centers_);
    nearest_centers_ = nullptr;
  }
  if (OB_NOT_NULL(lower_bounds_)) {
    allocator_.free(lower_bounds_);
    lower_bounds_ = nullptr;
  }
  if (OB_NOT_NULL(upper_bounds_)) {
    allocator_.free(upper_bounds_);
    upper_bounds_ = nullptr;
  }
  if (OB_NOT_NULL(weight_)) {
    arena_allocator_.free(weight_);
    weight_ = nullptr;
  }

  // allocator_.reset();
  arena_allocator_.reset();
}

void ObKMeansCenterHelper::destroy() {
  for (int64_t i = 0; i < 2; ++i) {
    ObTypeVector::reuse_array(allocator_, center_vectors_[i]);
    center_vectors_[i].reset();
  }
  if (OB_NOT_NULL(nearest_centers_)) {
    allocator_.free(nearest_centers_);
    nearest_centers_ = nullptr;
  }
  if (OB_NOT_NULL(lower_bounds_)) {
    allocator_.free(lower_bounds_);
    lower_bounds_ = nullptr;
  }
  if (OB_NOT_NULL(upper_bounds_)) {
    allocator_.free(upper_bounds_);
    upper_bounds_ = nullptr;
  }
  if (OB_NOT_NULL(weight_)) {
    arena_allocator_.free(weight_);
    weight_ = nullptr;
  }
  allocator_.reset();
  arena_allocator_.reset();
  // LOG_TRACE("ObKMeansCenterHelper destroy finish");
}

int64_t ObKMeansCenterHelper::to_string(char *buf,
                                             const int64_t buf_len) const {
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(lists), K_(total_cnt), "center_count",
         center_vectors_[cur_idx_].count(), K_(iterate_times),
         K_(max_iterate_times), K_(status), K_(use_elkan_kmeans));
    for (int64_t i = 0; i < center_vectors_[cur_idx_].count(); ++i) {
      J_COMMA();
      BUF_PRINTO(center_vectors_[cur_idx_].at(i));
    }
    J_OBJ_END();
  }
  return pos;
}

// TODO(@jingshui) : need to opt! select * from data_table too many times now.
int ObKMeansCenterHelper::build() {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null cache", K(ret));
  } else if (OB_FAIL(cache_->read())) {
    LOG_WARN("failed to read from cache", K(ret));
  } else {
    switch (status_) {
    case ObIvfKMeansStatus::PREPARE_CENTERS: {
      lists_ = init_lists_; // reinit
      if (use_elkan_kmeans_ && OB_FAIL(init_first_center_elkan())) {
        LOG_WARN("failed to init first center", K(ret));
      } else if (!use_elkan_kmeans_ && OB_FAIL(init_first_center())) {
        LOG_WARN("failed to init first center", K(ret));
      }
      break;
    }
    case ObIvfKMeansStatus::INIT_CENTERS: {
      if (use_elkan_kmeans_ && OB_FAIL(init_centers_elkan())) {
        LOG_WARN("failed to init centers", K(ret));
      } else if (!use_elkan_kmeans_ && OB_FAIL(init_centers())) {
        LOG_WARN("failed to init centers", K(ret));
      }
      break;
    }
    case ObIvfKMeansStatus::RUNNING_KMEANS: {
      if (use_elkan_kmeans_ && OB_FAIL(elkan_kmeans())) {
        LOG_WARN("failed to do kmeans", K(ret));
      } else if (!use_elkan_kmeans_ && OB_FAIL(kmeans())) {
        LOG_WARN("failed to do kmeans", K(ret));
      }
      break;
    }
    case ObIvfKMeansStatus::FINISH: {
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not expected status", K(ret), K_(status));
      break;
    }
    }
    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      ++failed_times_;
      if (MAX_RETRY_CNT >= failed_times_) {
        ret = OB_NEED_RETRY; // change the error code to retry
      }
    }
    // tmp code // reset
    if (OB_ALLOCATE_MEMORY_FAILED == ret && use_elkan_kmeans_) {
      destroy();
      use_elkan_kmeans_ = false;
      status_ = PREPARE_CENTERS;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObKMeansCenterHelper::init_first_center() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  double max_distance = 0;
  double min_distance = DBL_MAX;
  if (ObIvfKMeansStatus::PREPARE_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (0 == total_cnt_) {
    status_ = FINISH; // do nothing
  } else {
    LOG_TRACE("start to init_first_center", K(ret), KPC(this));
    // scan cnt 1
    if (OB_FAIL(cache_->get_random_vector(vector))) {
      LOG_WARN("failed to get random vector", K(ret));
    } else {
      ObTypeVector *first_center = nullptr;
      if (OB_FAIL(ObTypeVector::alloc_and_copy_vector(allocator_, vector, first_center))) {
        LOG_WARN("failed to alloc and copy vector", K(ret));
      } else if (OB_FAIL(center_vectors_[cur_idx_].push_back(first_center))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      ObTypeVector::reuse_array(allocator_, center_vectors_[cur_idx_]);
    } else {
      if (lists_ < center_vectors_[cur_idx_].count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected center counts is larger than lists", K(ret),
                 "center_count", center_vectors_[cur_idx_].count(), K_(lists));
      } else if (lists_ == center_vectors_[cur_idx_].count()) {
        status_ = RUNNING_KMEANS; // skip INIT_CENTERS
      } else {
        status_ = INIT_CENTERS;
      }
      LOG_TRACE("success to init_first_center", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObKMeansCenterHelper::quick_init_centers() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  ObTypeVector *center = nullptr;
  int64_t idx = 0;
  while (OB_SUCC(ret) && lists_ > center_vectors_[cur_idx_].count()) {
    if (OB_FAIL(cache_->get_next_vector(vector))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", K(ret));
      }
      break;
    } else {
      for (idx = 0; idx < center_vectors_[cur_idx_].count(); ++idx) {
        if (0 == center_vectors_[cur_idx_].at(idx)->vector_cmp(vector)) {
          break;
        }
      }
      if (idx == center_vectors_[cur_idx_]
                     .count()) { // vector is not exist in center array
        center = nullptr;
        if (OB_FAIL(ObTypeVector::alloc_and_copy_vector(allocator_, vector, center))) {
          LOG_WARN("failed to alloc and copy vector", K(ret));
        } else if (OB_FAIL(center_vectors_[cur_idx_].push_back(center))) {
          LOG_WARN("failed to push back into array", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    ObTypeVector::reuse_array(allocator_, center_vectors_[cur_idx_]);
    ObTypeVector::destory_vector(allocator_, center);
    status_ = PREPARE_CENTERS; // retry from PREPARE_CENTERS // TODO(@jingshui)
  } else {
    status_ = FINISH; // don't need to kmeans
    LOG_TRACE("success to quick_init_centers", K(ret), KPC(this));
  }
  return ret;
}

// TODO(@jingshui): sample counts may be less than center counts, we can select
// count(*) first to opt
int ObKMeansCenterHelper::init_centers() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  double max_distance = 0;
  double min_distance = DBL_MAX;
  ObTypeVector *next_center = nullptr;
  int64_t nearest_center_idx = -1;
  if (ObIvfKMeansStatus::INIT_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (total_cnt_ <= lists_) {
    lists_ = total_cnt_;
    if (OB_FAIL(quick_init_centers())) {
      LOG_WARN("failed to quick init centers", K(ret));
    }
  } else {
    LOG_TRACE("start to init_centers", K(ret), KPC(this));
    // scan cnt lists-2
    while (OB_SUCC(ret)) {
      if (OB_FAIL(cache_->get_next_vector(vector))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next result failed", K(ret));
        }
        break;
      } else if (OB_FAIL(get_nearest_center(vector, nearest_center_idx,
                                            min_distance))) {
        LOG_WARN("failed to get nearest center", K(ret), K(vector));
      } else if (min_distance > max_distance) {
        max_distance = min_distance;
        if (OB_NOT_NULL(next_center)) {
          if (OB_FAIL(next_center->deep_copy(vector))) {
            LOG_WARN("failed to deep copy", K(ret), K(vector));
          }
        } else if (OB_FAIL(ObTypeVector::alloc_and_copy_vector(allocator_, vector, next_center))) {
          LOG_WARN("failed to alloc and copy vector", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(next_center)) {
      if (OB_FAIL(center_vectors_[cur_idx_].push_back(next_center))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      ObTypeVector::destory_vector(allocator_, next_center);
      // stay in INIT_CENTERS
    }
  }
  if (OB_SUCC(ret)) {
    if (lists_ < center_vectors_[cur_idx_].count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected center counts is larger than lists", K(ret),
               KPC(this));
    } else if (lists_ == center_vectors_[cur_idx_].count()) {
      status_ = RUNNING_KMEANS;
      LOG_TRACE("success to init_centers", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObKMeansCenterHelper::kmeans() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  int64_t nearest_center_idx = -1;
  double min_distance = DBL_MAX;
  double distance = DBL_MAX;
  const int64_t next_idx = get_next_idx();
  if (ObIvfKMeansStatus::RUNNING_KMEANS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (lists_ != center_vectors_[cur_idx_].count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array count", K(ret), K_(lists), "count",
             center_vectors_[cur_idx_].count());
  } else if (max_iterate_times_ == iterate_times_) {
    status_ = FINISH;
  } else {
    ++iterate_times_;
    // 1. init next_idx array
    ObTypeVector *new_vector = nullptr;
    const int64_t vector_length = center_vectors_[cur_idx_].at(0)->dims();
    while (OB_SUCC(ret) && lists_ > center_vectors_[next_idx].count()) {
      if (OB_FAIL(ObTypeVector::alloc_vector(allocator_, new_vector, vector_length))) {
        LOG_WARN("failed to alloc vector", K(ret));
      } else if (OB_FAIL(center_vectors_[next_idx].push_back(new_vector))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      ObTypeVector::destory_vector(allocator_, new_vector);
      // stay in RUNNING_KMEANS
    } else {
      // 2. update centers
      bool is_finish = true;
      common::ObArray<int64_t> data_cnt_array;
      data_cnt_array.set_attr(ObMemAttr(tenant_id_, "DataCnts"));
      if (FAILEDx(data_cnt_array.reserve(lists_))) {
        LOG_WARN("failed to reserve array", K(ret), K_(lists));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
          if (OB_FAIL(data_cnt_array.push_back(0))) { // init
            LOG_WARN("failed to push back into array", K(ret));
          }
        }
        while (OB_SUCC(ret)) {
          if (OB_FAIL(cache_->get_next_vector(vector))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("get next result failed", K(ret));
            }
            break;
          } else if (OB_FAIL(get_nearest_center(vector, nearest_center_idx,
                                                min_distance))) {
            LOG_WARN("failed to get nearest center", K(ret), K(vector));
          } else if (OB_FAIL(center_vectors_[next_idx]
                                 .at(nearest_center_idx)
                                 ->add(vector))) {
            LOG_WARN("failed to add vector", K(ret));
          } else if (FALSE_IT(++data_cnt_array.at(nearest_center_idx))) {
          }
        }
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
            if (data_cnt_array.at(i) > 0) {
              if (OB_FAIL(center_vectors_[next_idx].at(i)->divide(
                      data_cnt_array.at(i)))) {
                LOG_WARN("failed to divide vector", K(ret), "data_count",
                         data_cnt_array.at(i));
              } else if (L2 == distance_type_ &&
                         OB_FAIL(center_vectors_[cur_idx_].at(i)->cal_l2_square(
                             *center_vectors_[next_idx].at(i), distance))) {
                LOG_WARN("failed to cal distance", K(ret), K_(distance_type), K(*center_vectors_[cur_idx_].at(i)),
                         K(*center_vectors_[next_idx].at(i)));
              } else if (L2 != distance_type_ &&
                         OB_FAIL(center_vectors_[cur_idx_].at(i)->cal_distance(
                             distance_type_, *center_vectors_[next_idx].at(i),
                             distance))) {
                LOG_WARN("failed to cal distance", K(ret), K_(distance_type), K(*center_vectors_[cur_idx_].at(i)),
                         K(*center_vectors_[next_idx].at(i)));
              } else if (L2 == distance_type_) {
                if (L2_SQUARE_ERROR_THRESHOLD < distance) {
                  is_finish = false;
                }
              } else if (L2_DISTANCE_ERROR_THRESHOLD < distance) {
                is_finish = false;
              }
            } else if (OB_FAIL(center_vectors_[next_idx].at(i)->deep_copy(
                           *center_vectors_[cur_idx_].at(i)))) {
              LOG_WARN("fail to copy vector", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        switch_cur_idx();
        status_ = is_finish ? FINISH : status_;
      }
      // clear swap array
      for (int64_t i = 0; i < lists_; ++i) {
        center_vectors_[get_next_idx()].at(i)->clear_vals();
      }
    }
    LOG_TRACE("success to kmeans", K(ret), KPC(this));
  }
  return ret;
}

//// elkan kmeans
int ObKMeansCenterHelper::get_vector_by_sql(const int64_t offset,
                                                 ObTypeVector *&next_vector) {
  int ret = OB_SUCCESS;
  ObSqlString select_sql;
  if (OB_FAIL(select_sql.assign(select_sql_str_))) {
    LOG_WARN("fail to assign sql string", K(ret), K_(select_sql_str));
  } else if (OB_FAIL(select_sql.append_fmt(" limit %ld,1", offset))) {
    LOG_WARN("fail to append sql string", K(ret), K_(select_sql_str),
             K(offset));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      sqlclient::ObMySQLResult *res = nullptr;
      if (OB_FAIL(GCTX.sql_proxy_->read(
              result, tenant_id_, select_sql.ptr()))) { // open another connect
        LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K(select_sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(select_sql));
      } else {
        ObMysqlResultIterator iter(*res);
        ObTypeVector vector;
        if (OB_FAIL(iter.get_next_vector(vector))) {
          LOG_WARN("get next result failed", K(ret));
        } else if (OB_FAIL(ObTypeVector::alloc_and_copy_vector(allocator_, vector, next_vector))) {
          LOG_WARN("failed to alloc and copy vector", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObKMeansCenterHelper::init_first_center_elkan() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  if (ObIvfKMeansStatus::PREPARE_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (0 == total_cnt_) {
    status_ = FINISH; // do nothing
  } else {
    LOG_TRACE("start to init_first_center", K(ret), KPC(this));
    // scan cnt 1
    if (OB_FAIL(cache_->get_random_vector(vector))) {
      LOG_WARN("failed to get random vector", K(ret));
    } else {
      ObTypeVector *first_center = nullptr;
      if (OB_FAIL(ObTypeVector::alloc_and_copy_vector(allocator_, vector, first_center))) {
        LOG_WARN("failed to alloc and copy vector", K(ret));
      } else if (OB_FAIL(center_vectors_[cur_idx_].push_back(first_center))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      ObTypeVector::reuse_array(allocator_, center_vectors_[cur_idx_]);
    } else if (lists_ < center_vectors_[cur_idx_].count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected center counts is larger than lists", K(ret),
               "center_count", center_vectors_[cur_idx_].count(), K_(lists));
    } else if (lists_ == center_vectors_[cur_idx_].count()) {
      status_ = FINISH; // skip INIT_CENTERS and RUNNING_KMEANS
    } else if (lists_ >= total_cnt_) {
      status_ = INIT_CENTERS; // quick_init_centers
    } else {
      if (OB_ISNULL(nearest_centers_ = static_cast<int32_t *>(
                        allocator_.alloc(sizeof(int32_t) * total_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KPC(this));
      } else if (OB_ISNULL(lower_bounds_ =
                               static_cast<float *>(allocator_.alloc(
                                   sizeof(float) * total_cnt_ * lists_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KPC(this));
      } else if (OB_ISNULL(upper_bounds_ = static_cast<float *>(
                               allocator_.alloc(sizeof(float) * total_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KPC(this));
      } else if (OB_ISNULL(
                     weight_ = static_cast<float *>(
                         arena_allocator_.alloc(sizeof(float) * total_cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KPC(this));
      } else {
        status_ = INIT_CENTERS;
        MEMSET(nearest_centers_, 0, sizeof(int32_t) * total_cnt_);
        MEMSET(lower_bounds_, 0, sizeof(float) * total_cnt_ * lists_);
        MEMSET(upper_bounds_, 0, sizeof(float) * total_cnt_);
        for (int64_t i = 0; i < total_cnt_; ++i) {
          weight_[i] = FLT_MAX;
        }
      }
    }

    if (OB_FAIL(ret)) {
      ObTypeVector::reuse_array(allocator_, center_vectors_[cur_idx_]);
    } else if (OB_SUCC(ret)) {
      LOG_TRACE("success to init_first_center", K(ret), KPC(this));
    }
  }
  return ret;
}

// kmeans++
int ObKMeansCenterHelper::init_centers_elkan() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  double distance = 0;
  ObTypeVector *next_center = nullptr;
  bool is_finish = false;
  if (ObIvfKMeansStatus::INIT_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (total_cnt_ <= lists_) {
    lists_ = total_cnt_;
    if (OB_FAIL(quick_init_centers())) {
      LOG_WARN("failed to quick init centers", K(ret));
    }
  } else {
    LOG_TRACE("start to init_centers", K(ret), KPC(this));
    int64_t center_idx = center_vectors_[cur_idx_].count() - 1;
    is_finish = lists_ == center_vectors_[cur_idx_].count();
    int64_t idx = 0;
    double sum = 0;
    double random_weight = 0;
    ObTypeVector *cur_center = center_vectors_[cur_idx_].at(center_idx);

    // cal every sample weight
    while (OB_SUCC(ret)) {
      if (OB_FAIL(cache_->get_next_vector(vector))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next result failed", K(ret));
        }
        break;
      } else {
        if (OB_FAIL(vector.cal_kmeans_distance(distance_type_, *cur_center,
                                               distance))) {
          LOG_WARN("failed to cal distance", K(ret), K_(distance_type), K(*cur_center), K(vector));
        } else {
          lower_bounds_[idx * lists_ + center_idx] = distance;
          if (!is_finish) { // is_finish means no need to add new center, only
                            // update lower_bounds_
            distance *= distance;
            if (distance < weight_[idx]) {
              weight_[idx] = distance;
            }
            sum += weight_[idx];
          }
        }
      }
      ++idx;
    }
    // get the next center randomly
    if (OB_SUCC(ret) && !is_finish) {
      if (idx != total_cnt_) {
        // ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data count has changed during building index", K(idx),
                 K_(total_cnt));
      }
      random_weight = (double)ObRandom::rand(1, 100) / 100.0 * sum;
      for (idx = 0; idx < total_cnt_; ++idx) {
        if ((random_weight -= weight_[idx]) <= 0.0) {
          break;
        }
      }

      if (idx >= total_cnt_) {
        idx = total_cnt_ - 1 < 0 ? 0 : total_cnt_ - 1;
      }
      if (select_sql_str_.empty() && OB_FAIL(cache_->get_vector(idx, next_center))) {
        LOG_WARN("fail to get vector from cache", K(ret), K(idx), K_(total_cnt));
      } else if (!select_sql_str_.empty() && OB_FAIL(get_vector_by_sql(idx, next_center))) {
        LOG_WARN("fail to get vector by sql", K(ret), K(idx), K_(total_cnt));
      } else if (OB_SUCC(ret) && OB_FAIL(center_vectors_[cur_idx_].push_back(next_center))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      ObTypeVector::destory_vector(allocator_, next_center);
      // stay in INIT_CENTERS
    }
  }
  if (OB_SUCC(ret)) {
    if (lists_ < center_vectors_[cur_idx_].count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected center counts is larger than lists", K(ret),
               KPC(this));
    } else if (is_finish) {
      for (int64_t i = 0; i < total_cnt_; ++i) {
        double min_distance = DBL_MAX;
        int64_t nearest_center_idx = 0;
        for (int64_t j = 0; j < lists_; ++j) {
          double distance = lower_bounds_[i * lists_ + j];
          if (distance < min_distance) {
            min_distance = distance;
            nearest_center_idx = j;
          }
        }
        upper_bounds_[i] = min_distance; // 离初始最近中心的距离为初始上界
        nearest_centers_[i] = nearest_center_idx;
      }

      if (OB_NOT_NULL(weight_)) {
        arena_allocator_.free(weight_);
        weight_ = nullptr;
      }
      arena_allocator_.reset();

      status_ = RUNNING_KMEANS;
      LOG_TRACE("finish init_centers", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObKMeansCenterHelper::elkan_kmeans() {
  int ret = OB_SUCCESS;
  ObTypeVector vector;
  int64_t nearest_center_idx = -1;
  double min_distance = DBL_MAX;
  double distance = DBL_MAX;
  const int64_t next_idx = get_next_idx();
  if (ObIvfKMeansStatus::RUNNING_KMEANS != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K_(status));
  } else if (lists_ != center_vectors_[cur_idx_].count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array count", K(ret), K_(lists), "count",
             center_vectors_[cur_idx_].count());
  } else if (max_iterate_times_ == iterate_times_) {
    status_ = FINISH;
  } else {
    // init tmp structs
    float *half_centers_distance =
        nullptr; // 每两个中心之间距离的一半, 因为2D(x, c1) <= D(c1, c2) 等价于
                 // D(x,c1) <= 0.5 * D(c1,c2)
    float *half_center_min_distance = nullptr; // 每个中心离其他中心的最短距离
    int32_t *data_cnt_in_center = nullptr; // 每个中心包含数据点的数量
    float *center_distance_diff = nullptr; // 更新center前后两个center的距离
    if (OB_ISNULL(
            half_centers_distance = static_cast<float *>(
                arena_allocator_.alloc(sizeof(float) * lists_ * lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (OB_ISNULL(half_center_min_distance = static_cast<float *>(
                             arena_allocator_.alloc(sizeof(float) * lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (OB_ISNULL(
                   data_cnt_in_center = static_cast<int32_t *>(
                       arena_allocator_.alloc(sizeof(int32_t) * lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else {
      MEMSET(half_centers_distance, 0, sizeof(float) * lists_ * lists_);
      MEMSET(half_center_min_distance, 0, sizeof(float) * lists_);
      MEMSET(data_cnt_in_center, 0, sizeof(int32_t) * lists_);
    }

    // 1. cal distance between centers
    for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < lists_; ++j) {
        if (OB_FAIL(center_vectors_[cur_idx_].at(i)->cal_kmeans_distance(
                distance_type_, *center_vectors_[cur_idx_].at(j), distance))) {
          LOG_WARN("failed to cal distance", K(ret), K(i), K(j),
                   K(*center_vectors_[cur_idx_].at(i)), K(*center_vectors_[cur_idx_].at(j)));
        } else {
          distance = 0.5 * distance;
          half_centers_distance[i * lists_ + j] = distance;
          half_centers_distance[j * lists_ + i] = distance;
        }
      }
    }
    // 2. cal the nearest distance between the center and other centers
    for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
      half_center_min_distance[i] = DBL_MAX;
      for (int64_t j = 0; OB_SUCC(ret) && j < lists_; ++j) {
        if (i == j) {
          // do nothing
        } else {
          distance = half_centers_distance[i * lists_ + j];
          if (distance < half_center_min_distance[i]) {
            half_center_min_distance[i] = distance;
          }
        }
      }
    }
    // 3. init next_idx array // only once
    ObTypeVector *new_vector = nullptr;
    const int64_t vector_length = center_vectors_[cur_idx_].at(0)->dims();
    while (OB_SUCC(ret) && lists_ > center_vectors_[next_idx].count()) {
      if (OB_FAIL(ObTypeVector::alloc_vector(allocator_, new_vector, vector_length))) {
        LOG_WARN("failed to alloc vector", K(ret));
      } else if (OB_FAIL(center_vectors_[next_idx].push_back(new_vector))) {
        LOG_WARN("failed to push back into array", K(ret));
      }
    }
    // 4. cal to update centers
    if (OB_SUCC(ret)) {
      bool is_finish = true;
      int64_t idx = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(cache_->get_next_vector(vector))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get next result failed", K(ret));
          }
          break;
        } else if (upper_bounds_[idx] <=
                   half_center_min_distance[nearest_centers_[idx]]) {
          // do nothing
          LOG_TRACE("upper_bound <= half center mini distance", K(idx),
                    K(upper_bounds_[idx]),
                    K(half_center_min_distance[nearest_centers_[idx]]));
          // D(x,c1) <= 0.5 * D(c1,c2) , c2是离c1最近的中心, 此时D(x, c1) <=
          // D(x, c2), 上界已经最小了, 不会有距离更近的中心
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
            if (i == nearest_centers_[idx]) {
              // do nothing
            } else if (upper_bounds_[idx] <= lower_bounds_[idx * lists_ + i]) {
              // do nothing
              // ub <= lb, skip the center
              LOG_TRACE("upper_bound <= lower_bound", K(idx),
                        K(upper_bounds_[idx]),
                        K(lower_bounds_[idx * lists_ + i]));
            } else if (upper_bounds_[idx] <=
                       half_centers_distance[nearest_centers_[idx] * lists_ +
                                             i]) {
              // do nothing
              LOG_TRACE(
                  "upper_bound <= half centers distance", K(idx),
                  K(upper_bounds_[idx]),
                  K(half_centers_distance[nearest_centers_[idx] * lists_ + i]));
            } else {
              if (0 == iterate_times_) {
                min_distance = upper_bounds_[idx];
              } else if (OB_FAIL(vector.cal_kmeans_distance(
                             distance_type_,
                             *center_vectors_[cur_idx_].at(
                                 nearest_centers_[idx]),
                             min_distance))) {
                LOG_WARN("failed to cal distance", K(ret), K(i),
                         K(*center_vectors_[cur_idx_].at(nearest_centers_[idx])),
                         K(vector));
              }
              if (OB_SUCC(ret)) {
                LOG_TRACE(
                    "try to update min distance", K(idx), K(min_distance),
                    K(lower_bounds_[idx * lists_ + i]),
                    K(half_centers_distance[nearest_centers_[idx] * lists_ +
                                            i]));
                if (min_distance > lower_bounds_[idx * lists_ + i] ||
                    min_distance >
                        half_centers_distance[nearest_centers_[idx] * lists_ +
                                              i]) {
                  if (OB_FAIL(vector.cal_kmeans_distance(
                          distance_type_, *center_vectors_[cur_idx_].at(i),
                          distance))) {
                    LOG_WARN("failed to cal distance", K(ret), K(i),
                             K(*center_vectors_[cur_idx_].at(i)), K(vector));
                  } else {
                    lower_bounds_[idx * lists_ + i] = distance;
                    if (distance < min_distance) {
                      nearest_centers_[idx] = i;
                      upper_bounds_[idx] = distance;
                      is_finish = false;
                    }
                  }
                }
              }
            }
          }
        }
        // sum vector store in center_vectors_[next_idx]
        if (OB_SUCC(ret)) {
          if (OB_FAIL(center_vectors_[next_idx]
                          .at(nearest_centers_[idx])
                          ->add(vector))) {
            LOG_WARN("fail to add vector", K(ret));
          } else {
            ++data_cnt_in_center[nearest_centers_[idx]];
          }
        }
        ++idx;
      }
      // 5. cal the new centers
      if (OB_ISNULL(center_distance_diff = static_cast<float *>(
                        arena_allocator_.alloc(sizeof(float) * lists_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else {
        MEMSET(center_distance_diff, 0, sizeof(float) * lists_);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < lists_; ++i) {
        if (data_cnt_in_center[i] > 0) {
          if (OB_FAIL(center_vectors_[next_idx].at(i)->divide(
                  data_cnt_in_center[i]))) {
            LOG_WARN("failed to divide vector", K(ret), "data_count",
                     data_cnt_in_center[i]);
          } else if (OB_FAIL(
                         center_vectors_[next_idx].at(i)->cal_kmeans_distance(
                             distance_type_, *center_vectors_[cur_idx_].at(i),
                             distance))) {
            LOG_WARN("failed to cal distance", K(ret), K_(distance_type),
                     K(*center_vectors_[next_idx].at(i)), K(*center_vectors_[cur_idx_].at(i)));
          } else {
            center_distance_diff[i] = distance;
          }
        } else if (OB_FAIL(center_vectors_[next_idx].at(i)->deep_copy(
                       *center_vectors_[cur_idx_].at(
                           i)))) { // TODO(@jingshui): use random vector?
          LOG_WARN("fail to copy vector", K(ret));
        }
      }
      // 6. adjust ub & lb
      for (int64_t i = 0; i < total_cnt_; ++i) {
        upper_bounds_[i] += center_distance_diff[nearest_centers_[i]];
        for (int64_t j = 0; j < lists_; ++j) {
          distance = lower_bounds_[i * lists_ + j] - center_distance_diff[j];
          distance = distance < 0 ? 0 : distance;
          lower_bounds_[i * lists_ + j] = distance;
        }
      }
      // 7. switch center array
      if (OB_SUCC(ret)) {
        is_finish = is_finish && iterate_times_ != 0;
        ++iterate_times_;
        switch_cur_idx();
        status_ = is_finish ? FINISH : status_;
        LOG_TRACE("success to kmeans", K(ret), KPC(this));
      }
      // 8. clear tmp swap array
      for (int64_t i = 0; i < lists_; ++i) {
        center_vectors_[get_next_idx()].at(i)->clear_vals();
      }
    }
    // free memory
    if (OB_NOT_NULL(half_centers_distance)) {
      arena_allocator_.free(half_centers_distance);
      half_centers_distance = nullptr;
    }
    if (OB_NOT_NULL(half_center_min_distance)) {
      arena_allocator_.free(half_center_min_distance);
      half_center_min_distance = nullptr;
    }
    if (OB_NOT_NULL(data_cnt_in_center)) {
      arena_allocator_.free(data_cnt_in_center);
      data_cnt_in_center = nullptr;
    }
    if (OB_NOT_NULL(center_distance_diff)) {
      arena_allocator_.free(center_distance_diff);
      center_distance_diff = nullptr;
    }
    arena_allocator_.reset();
    if (OB_FAIL(ret)) {
      ObTypeVector::destory_vector(allocator_, new_vector);
      // stay in RUNNING_KMEANS
    }
  }
  return ret;
}

int ObKMeansCenterHelper::get_nearest_center(const ObTypeVector &vector,
                                                  int64_t &nearest_center_idx,
                                                  double &min_distance) {
  int ret = OB_SUCCESS;
  nearest_center_idx = -1;
  int64_t center_count = center_vectors_[cur_idx_].count();
  double distance = 0;
  min_distance = DBL_MAX;
  ObVectorDistanceType distance_type =
      distance_type_ == INNER_PRODUCT ? COSINE : distance_type_;
  for (int64_t i = 0; OB_SUCC(ret) && i < center_count; ++i) {
    ObTypeVector *cur_center = center_vectors_[cur_idx_].at(i);
    if (L2 == distance_type &&
        OB_FAIL(cur_center->cal_l2_square(vector, distance))) {
      LOG_WARN("failed to cal distance", K(ret), K(distance_type),
               K(*cur_center), K(vector));
    } else if (L2 != distance_type && OB_FAIL(cur_center->cal_distance(
                                          distance_type, vector, distance))) {
      LOG_WARN("failed to cal distance", K(ret), K(distance_type),
               K(*cur_center), K(vector));
    } else if (distance < min_distance) {
      min_distance = distance;
      nearest_center_idx = i;
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
