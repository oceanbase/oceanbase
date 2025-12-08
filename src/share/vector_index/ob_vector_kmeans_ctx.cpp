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

#define USING_LOG_PREFIX SHARE
#include "ob_vector_kmeans_ctx.h"
#include "lib/container/ob_array_array.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "lib/ob_define.h"
#include "share/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
using namespace common;
namespace share {
// ------------------ ObKmeansCtx implement ------------------
void ObKmeansCtx::destroy()
{
  for (int i = 0; i < sample_vectors_.count(); ++i) {
    ivf_build_mem_ctx_.Deallocate(sample_vectors_[i]);
  }
  sample_vectors_.reset();
}

int ObKmeansCtx::init(
    const int64_t tenant_id,
    const int64_t lists,
    const int64_t samples_per_nlist,
    const int64_t dim,
    ObVectorIndexDistAlgorithm dist_algo,
    ObVectorNormalizeInfo *norm_info,
    int64_t pq_m)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || 0 >= lists || 0 >= samples_per_nlist || 0 >= dim || VIDA_MAX <= dist_algo
    || dim % pq_m != 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(lists), K(tenant_id), K(samples_per_nlist), K(dim), K(dist_algo));
  } else if (INT64_MAX / samples_per_nlist < lists) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ivf vector index param nlist_value * sample_per_nlist_value should less than int64_max", K(ret),
             K(lists), K(samples_per_nlist));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                   "ivf vector index param nlist_value * sample_per_nlist_value should less than int64_max");
  } else {
    sample_vectors_.set_attr(ObMemAttr(tenant_id, "KmeansSample"));
    tenant_id_ = tenant_id;
    sample_dim_ = dim;
    dim_ = sample_dim_ / pq_m;
    lists_ = lists;
    max_sample_count_ = lists_ * samples_per_nlist;
    dist_algo_ = dist_algo;
    norm_info_ = norm_info;
    is_inited_ = true;
  }
  return ret;
}

int ObKmeansCtx::try_normalize(int64_t dim, float *data, float *norm_vector) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(norm_info_)) { // cos&ip need norm center vec
    if (OB_FAIL(norm_info_->normalize_func_(dim, data, norm_vector, nullptr))) {
      LOG_WARN("failed to normalize vector", K(ret));
    }
  }
  return ret;
}

int ObKmeansCtx::try_normalize_samples() const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(norm_info_) && VIDA_COS == dist_algo_) {  // cos need norm before kmeans
    for (int i = 0; OB_SUCC(ret) && i < sample_vectors_.count(); ++i) {
      if (OB_FAIL(norm_info_->normalize_func_(sample_dim_, sample_vectors_[i], sample_vectors_[i], nullptr))) {
        LOG_WARN("failed to normalize vector", K(ret), K(i), K(sample_dim_));
      }
    }
  }
  return ret;
}

int ObKmeansCtx::append_sample_vector(float* vector)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "kmeans ctx is not inited", K(ret));
  } else if (OB_ISNULL(vector)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid null vector", K(ret));
  } else {
    // reservoir sampling
    lib::ObMutexGuard guard(lock_);
    if (max_sample_count_ > sample_vectors_.count()) {
      float* save_vector = nullptr;
      if (OB_ISNULL(save_vector = static_cast<float*>(ivf_build_mem_ctx_.Allocate(sizeof(float) * sample_dim_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "failed to alloc vector", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
      } else {
        MEMCPY(save_vector, vector, sizeof(float) * sample_dim_);
        if (OB_FAIL(sample_vectors_.push_back(save_vector))) {
          SHARE_LOG(WARN, "failed to push back array", K(ret));
        }
      }
    } else {
      int64_t random = 0;
      random = ObRandom::rand(0, total_scan_count_ - 1);
      if (random < sample_vectors_.count()) {
        float* switch_vector = sample_vectors_.at(random);
        MEMCPY(switch_vector, vector, sizeof(float) * sample_dim_);
      }
    }
  }
  return ret;
}

// ------------------ ObKmeansAlgo implement ------------------
void ObKmeansAlgo::destroy()
{
  if (OB_NOT_NULL(weight_)) {
    ivf_build_mem_ctx_.Deallocate(weight_);
    weight_ = nullptr;
  }
  if (OB_NOT_NULL(distance_tasks_)) {
    ivf_build_mem_ctx_.Deallocate(distance_tasks_);
    distance_tasks_ = nullptr;
  }
  if (OB_NOT_NULL(assign_tasks_)) {
    ivf_build_mem_ctx_.Deallocate(assign_tasks_);
    assign_tasks_ = nullptr;
  }
  task_handler_ = nullptr;
  kmeans_monitor_ = nullptr;
}

bool ObKmeansAlgo::check_stop()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::dag_yield())) {
    LOG_WARN("dag yield failed", K(ret)); // exit for dag task as soon as possible after canceled.
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check status failed", K(ret));
  } else if (ATOMIC_LOAD(&force_stop_) == true) {
    ret = OB_CANCELED;
    LOG_WARN("kmeans ctx is fore stop", K(ret), K(*this));
  }
  return OB_SUCCESS != ret;
}

int ObKmeansAlgo::init(ObKmeansCtx &kmeans_ctx, bool enable_parallel /* = false */)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = kmeans_ctx.tenant_id_;
  int64_t lists = kmeans_ctx.lists_;
  int64_t dim = kmeans_ctx.dim_;
  if (OB_INVALID_ID == tenant_id || 0 >= lists || 0 >= dim) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(lists), K(tenant_id), K(dim));
  } else {
    kmeans_ctx_ = &kmeans_ctx;
    enable_parallel_ = enable_parallel;
    is_inited_ = true;
  }
  return ret;
}

int ObKmeansAlgo::build(const ObIArray<float*> &input_vectors)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "kmeans ctx is not inited", K(ret));
  } else {
    ObKMeansStatus last_status = status_;
    int64_t status_start_time = ObTimeUtility::current_time_ms();
    while (OB_SUCC(ret) && !is_finish()) {
      if (OB_FAIL(inner_build(input_vectors))) {
        SHARE_LOG(WARN, "failed to do kmeans", K(ret));
      } else if (check_stop()) {
        ret = OB_CANCELED;
        SHARE_LOG(INFO, "kmeans ctx is fore stop", K(ret), K(*this));
      } else if (last_status != status_) {
        SHARE_LOG(INFO, "status change", K(last_status), K(status_), K(ObTimeUtility::current_time_ms() - status_start_time));
        last_status = status_;
        status_start_time = ObTimeUtility::current_time_ms();
      }
    }
  }
  return ret;
}

int ObKmeansAlgo::inner_build(const ObIArray<float*> &input_vectors)
{
  int ret = OB_SUCCESS;
  switch (status_) {
    case PREPARE_CENTERS: {
      if (kmeans_ctx_->lists_ >= input_vectors.count()) {
        if (OB_FAIL(quick_centers(input_vectors))) {
          SHARE_LOG(WARN, "failed to quick centers", K(ret));
        }
      } else if (OB_FAIL(init_first_center(input_vectors))) {
        SHARE_LOG(WARN, "failed to init first center", K(ret));
      }
      break;
    }
    case INIT_CENTERS: {
      if (OB_FAIL(init_centers(input_vectors))) {
        SHARE_LOG(WARN, "failed to init centers", K(ret));
      }
      break;
    }
    case RUNNING_KMEANS: {
      if (OB_FAIL(do_kmeans(input_vectors))) {
        SHARE_LOG(WARN, "failed to do kmeans", K(ret));
      }
      break;
    }
    case FINISH: {
      LOG_INFO("finish kmeans build", K(ret));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "not expected status", K(ret), K(status_));
      break;
    }
  }
  return ret;
}

// only use sample vectors as centers
int ObKmeansAlgo::quick_centers(const ObIArray<float*> &input_vectors)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "kmeans ctx is not inited", K(ret));
  } else if (PREPARE_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    SHARE_LOG(WARN, "status not match", K(ret), K(status_));
  } else if (OB_FAIL(centers_[cur_idx_].init(kmeans_ctx_->dim_, input_vectors.count(), ivf_build_mem_ctx_))) {
    SHARE_LOG(WARN, "failed to init center buffer", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < input_vectors.count(); ++i) {
      if (OB_FAIL(centers_[cur_idx_].push_back(kmeans_ctx_->dim_, input_vectors.at(i)))) {
        SHARE_LOG(WARN, "failed to push back center", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      status_ = FINISH;
      const int64_t center_count = centers_[cur_idx_].count();
      const int64_t sample_count = input_vectors.count();
      SHARE_LOG(INFO, "success to quick centers", K(ret), K(center_count), K(kmeans_ctx_->lists_), K(sample_count));
    }
  }
  return ret;
}

int ObKmeansAlgo::init_first_center(const ObIArray<float *> &input_vectors)
{
  int ret = OB_SUCCESS;
  if (PREPARE_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    SHARE_LOG(WARN, "status not match", K(ret), K(status_));
  } else if (OB_FAIL(centers_[0].init(kmeans_ctx_->dim_, kmeans_ctx_->lists_, ivf_build_mem_ctx_))) {
    SHARE_LOG(WARN, "failed to init center buffer", K(ret));
  } else if (OB_FAIL(centers_[1].init(kmeans_ctx_->dim_, kmeans_ctx_->lists_, ivf_build_mem_ctx_))) {
    SHARE_LOG(WARN, "failed to init center buffer", K(ret));
  } else if (enable_parallel_) {
    // Get task handler, only get once
    ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
    if (OB_ISNULL(service)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to get plugin vector index service", K(ret));
    } else {
      task_handler_ = &service->get_kmeans_build_handler();
      int64_t max_thread_cnt = 0;

      // Initialize task handler
      if (OB_ISNULL(task_handler_)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "task handler is null, should be initialized in init_first_center", K(ret));
      } else if (OB_FAIL(init_build_handle(*task_handler_))) {
        SHARE_LOG(WARN, "failed to init build handle", K(ret));
      } else if (OB_FAIL(task_handler_->get_max_thread_count(max_thread_cnt, true /*with_refresh*/))) {
        SHARE_LOG(WARN, "failed to get max thread count", K(ret));
      } else {
        // Allocate task array memory, determine task count based on thread count and vector count
        const int64_t sample_cnt = input_vectors.count();
        // Ensure each task processes at least 1 vector, task count not exceeding the smaller of thread count and vector count
        max_distance_tasks_ = std::min(max_thread_cnt, sample_cnt);
        max_assign_tasks_ = max_distance_tasks_; // Assignment task count same as distance calculation tasks
        
        // Allocate distance calculation task array
        void *tmp_buf = ivf_build_mem_ctx_.Allocate(sizeof(ObKmeansDistanceCalcTask) * max_distance_tasks_);
        if (OB_ISNULL(tmp_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SHARE_LOG(WARN, "failed to alloc distance tasks memory", K(ret));
        } else {
          distance_tasks_ = static_cast<ObKmeansDistanceCalcTask*>(tmp_buf);
          // Initialize distance calculation task objects
          for (int64_t i = 0; i < max_distance_tasks_; ++i) {
            new (&distance_tasks_[i]) ObKmeansDistanceCalcTask();
          }
        }
        
        // Allocate vector assignment task array
        if (OB_SUCC(ret)) {
          void *assign_tmp_buf = ivf_build_mem_ctx_.Allocate(sizeof(ObKmeansAssignTask) * max_assign_tasks_);
          if (OB_ISNULL(assign_tmp_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SHARE_LOG(WARN, "failed to alloc assign tasks memory", K(ret));
          } else {
            assign_tasks_ = static_cast<ObKmeansAssignTask*>(assign_tmp_buf);
            // Initialize vector assignment task objects
            for (int64_t i = 0; i < max_assign_tasks_; ++i) {
              new (&assign_tasks_[i]) ObKmeansAssignTask();
            }
          }
        }
      }
    }
  }
  
  if (OB_SUCC(ret)) {
    const int64_t sample_cnt = input_vectors.count();
    int64_t random = 0;
    random = ObRandom::rand(0, sample_cnt - 1);
    // use random sample vector as the first center
    if (OB_FAIL(centers_[cur_idx_].push_back(kmeans_ctx_->dim_, input_vectors.at(random)))) {
      SHARE_LOG(WARN, "failed to push back center", K(ret));
    } else if (OB_ISNULL(weight_ = static_cast<float *>(ivf_build_mem_ctx_.Allocate(sizeof(float) * sample_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
    } else {
      for (int64_t i = 0; i < sample_cnt; ++i) {
        weight_[i] = FLT_MAX;
      }
      status_ = INIT_CENTERS;
      SHARE_LOG(TRACE, "success to init first center", K(ret));
    }
  }
  return ret;
}

// Kmeans++
int ObKmeansAlgo::init_centers(const ObIArray<float*> &input_vectors)
{
  int ret = OB_SUCCESS;
  
  if (INIT_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    SHARE_LOG(WARN, "status not match", K(ret), K(status_));
  } else {
    int64_t center_idx = centers_[cur_idx_].count() - 1;
    float *current_center = centers_[cur_idx_].at(center_idx);

    float distance = 0;
    bool is_finish = kmeans_ctx_->lists_ == centers_[cur_idx_].count();
    float sum = 0;

    if (is_finish) {
      status_ = RUNNING_KMEANS;
      const int64_t center_count = centers_[cur_idx_].count();
      const int64_t sample_count = input_vectors.count();
      SHARE_LOG(INFO, "success to init all centers", K(ret), K(center_count), K(kmeans_ctx_->lists_), K(sample_count));
    } else if (OB_FAIL(calc_distances_parallel(input_vectors, current_center, sum))) {  // Use block parallel distance calculation
      SHARE_LOG(WARN, "failed to calc distances parallel", K(ret));
    } else {
      const int64_t sample_cnt = input_vectors.count();
      // get the next center randomly
      float random_weight = (float)ObRandom::rand(1, 100) / 100.0 * sum;
      int64_t i = 0;
      for (i = 0; i < sample_cnt; ++i) {
        if ((random_weight -= weight_[i]) <= 0.0) {
          break;
        }
      }
      if (i >= sample_cnt) {
        i = sample_cnt - 1 < 0 ? 0 : sample_cnt - 1;
      }
      if (OB_FAIL(centers_[cur_idx_].push_back(kmeans_ctx_->dim_, input_vectors.at(i)))) {
        SHARE_LOG(WARN, "failed to push back center", K(ret));
      } else {
        const int64_t center_count = centers_[cur_idx_].count();
        if (OB_NOT_NULL(kmeans_monitor_)) {
          const int64_t progress_percent = (center_count * 100) / kmeans_ctx_->lists_;
          kmeans_monitor_->set_kmeams_monitor(progress_percent, 0, 0, 0);
        }
        SHARE_LOG(TRACE, "success to init center", K(ret), K(center_count));
      }
    }
  }
  return ret;
}

int ObKmeansAlgo::calc_kmeans_distance(const float* a, const float* b, const int64_t len, float &distance)
{
  int ret = OB_SUCCESS;
  // only use l2_distance
  distance = ObVectorL2Distance<float>::l2_square_flt_func(a, b, len);
  return ret;
}

void ObKmeansAlgo::set_centers_distance(float* centers_distance, int64_t i, int64_t j, float distance)
{
  if (i != j) {
    (i > j) ? centers_distance[i * (i - 1) / 2 + j] = distance : centers_distance[j * (j - 1) / 2 + i] = distance;
  }
}

float ObKmeansAlgo::get_centers_distance(float* centers_distance, int64_t i, int64_t j)
{
  if (i != j) {
    return (i > j) ? centers_distance[i * (i - 1) / 2 + j] : centers_distance[j * (j - 1) / 2 + i];
  } else {
    return 0.0;
  }
}

double ObKmeansAlgo::calc_imbalance_factor(const ObIArray<float*> &input_vectors, int32_t *data_cnt_in_cluster)
{
  double imbalance_factor = 0.0;
  if (OB_ISNULL(data_cnt_in_cluster) || kmeans_ctx_->lists_ <= 0) {
    return imbalance_factor;
  }
  
  int64_t total_vectors = 0;
  double sum_squares = 0.0;
  
  for (int64_t i = 0; i < kmeans_ctx_->lists_; ++i) {
    total_vectors += data_cnt_in_cluster[i];
    sum_squares += static_cast<double>(data_cnt_in_cluster[i]) * data_cnt_in_cluster[i];
  }
  
  if (total_vectors > 0) {
    imbalance_factor = sum_squares * kmeans_ctx_->lists_ / (static_cast<double>(total_vectors) * total_vectors);
  }
  
  return imbalance_factor;
}

int ObKmeansAlgo::init_build_handle(ObKmeansBuildTaskHandler &handle)
{
  int ret = OB_SUCCESS;

  common::ObSpinLockGuard init_guard(handle.lock_);                     // lock thread pool init to avoid init twice
  if (handle.get_tg_id() != ObKmeansBuildTaskHandler::INVALID_TG_ID) {  // no need to init twice, skip
  } else if (OB_FAIL(handle.init())) {
    LOG_WARN("fail to init vector kmeans build task handle", K(ret));
  } else if (OB_FAIL(handle.start())) {
    LOG_WARN("fail to start vector kmeans build thread pool", K(ret));
  }

  return ret;
}

// Block parallel distance calculation
int ObKmeansAlgo::calc_distances_parallel(const ObIArray<float*> &input_vectors, 
                                         float *current_center, float &sum)
{
  int ret = OB_SUCCESS;
  const int64_t sample_cnt = input_vectors.count();
  
  // Use cached task handler
  if (OB_ISNULL(task_handler_)) {
    // If task handler is null, fallback to serial calculation
    if (OB_FAIL(calc_distances_range(input_vectors, 0, sample_cnt, current_center, weight_, kmeans_ctx_->dim_, sum))) {
      SHARE_LOG(WARN, "failed to calc distances range", K(ret));
    }
  } else if (OB_UNLIKELY(OB_ISNULL(distance_tasks_) || max_distance_tasks_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "unexpected nullptr", K(ret));
  } else {
    // NOTE: max_distance_tasks_ + 1 is used to handle the final task
    const int64_t block_size = std::max(1L, sample_cnt / (max_distance_tasks_ + 1));
    int64_t end_idx = 0;

    // Create and submit tasks
    for (int64_t i = 0; OB_SUCC(ret) && i < max_distance_tasks_; ++i) {
      // max_distance_tasks_ is min(sample_cnt, max_thread_cnt), so end_idx <= sample_cnt
      int64_t start_idx = i * block_size;
      end_idx = start_idx + block_size;

      ObKmeansDistanceCalcTask &task = distance_tasks_[i];
      // Reset task state to ensure proper reuse
      task.reset();
      if (OB_FAIL(task.init(start_idx, end_idx, &input_vectors, current_center, weight_, kmeans_ctx_->dim_))) {
        SHARE_LOG(WARN, "failed to init distance calc task", K(ret), K(i));
      } else if (OB_FAIL(task_handler_->push_task(task))) {
        if (OB_EAGAIN != ret) {
          SHARE_LOG(WARN, "failed to push distance calc task", K(ret), K(i));
        } else if (check_stop()) {  // check_stop in each loop will lead to a decrease in performance; the decision is
                                    // only made in exceptional cases.
          ret = OB_CANCELED;
          SHARE_LOG(WARN, "check stop", K(ret));
        } else if (OB_FAIL(task.do_work())) {
          SHARE_LOG(WARN, "failed to do distance calc task", K(ret));
        }
      }
    }

    // Handle the final task
    if (OB_SUCC(ret)) {
      float tmp_sum = 0.0f;
      if (OB_FAIL(calc_distances_range(input_vectors, end_idx, sample_cnt, current_center, weight_, kmeans_ctx_->dim_,
                                       tmp_sum))) {
        SHARE_LOG(WARN, "failed to calc distances range", K(ret));
      } else {
        sum += tmp_sum;
      }
    } else {
      for (int64_t i = 0; i < max_distance_tasks_; ++i) {
        ObKmeansDistanceCalcTask &task = distance_tasks_[i];
        task.set_task_stop();
      }
    }
    // Note. Anywhere above, all tasks need to be stopped otherwise it may cause a core dump because sample vectors
    // will be released.
    wait_parallel_task_finish(distance_tasks_, max_distance_tasks_, *task_handler_);

    // Wait for all tasks to complete and collect results
    for (int64_t i = 0; OB_SUCC(ret) && i < max_distance_tasks_; ++i) {
      ObKmeansDistanceCalcTask &task = distance_tasks_[i];
      if (OB_FAIL(task.get_ret())) {
        SHARE_LOG(WARN, "distance calc task failed", K(ret), K(i));
      } else {
        sum += task.get_sum();
      }
    }
  }

  return ret;
}

// ------------------ ObKmeansExecutor implement ------------------

bool ObKmeansExecutor::check_stop()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::dag_yield())) {
    LOG_WARN("dag yield failed", K(ret)); // exit for dag task as soon as possible after canceled.
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check status failed", K(ret));
  }
  return OB_SUCCESS != ret;
}

int ObKmeansExecutor::append_sample_vector(float* vector)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "kmeans ctx is not inited", K(ret));
  } else if (OB_ISNULL(vector)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid null vector", K(ret));
  } else if (OB_FAIL(ctx_.append_sample_vector(vector))) {
    LOG_WARN("failed to append sample vector", K(ret), K(ctx_));
  } else {
    ++ctx_.total_scan_count_;
  }
  return ret;
}

// ------------------ ObSingleKmeansExecutor implement ------------------
int ObSingleKmeansExecutor::init(
    ObKmeansAlgoType algo_type,
    const int64_t tenant_id,
    const int64_t lists,
    const int64_t samples_per_nlist,
    const int64_t dim,
    ObVectorIndexDistAlgorithm dist_algo,
    ObVectorNormalizeInfo *norm_info/* = nullptr*/,
    const int64_t pq_m_size/* = 1*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx_.init(tenant_id, lists, samples_per_nlist, dim, dist_algo, norm_info, 1 /*pq_m*/))) {
    LOG_WARN("fail to init kmeans ctx", K(ret), K(tenant_id), K(lists), K(samples_per_nlist), K(dim), K(dist_algo));
  } else {
    if (algo_type == ObKmeansAlgoType::KAT_ELKAN) {
      void *tmp_buf = nullptr;
      if (OB_ISNULL(tmp_buf = ivf_build_mem_ctx_.Allocate(sizeof(ObElkanKmeansAlgo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc tmp_buf", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
      } else {
        algo_ = new (tmp_buf) ObElkanKmeansAlgo(ivf_build_mem_ctx_);
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid kmeans algorithm type", K(ret), K(algo_type));
    }
  }
  
  if (FAILEDx(algo_->init(ctx_, true /* enable_parallel */))) {
    LOG_WARN("fail to init kmeans algo", K(ret), K(ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSingleKmeansExecutor::build(ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtil::current_time_ms();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "kmeans ctx is not inited", K(ret));
  } else if (OB_ISNULL(insert_monitor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(insert_monitor));
  } else if (OB_FALSE_IT(algo_->set_kmeans_monitor(insert_monitor->kmeans_monitor_))) {
  } else if (OB_FAIL(ctx_.try_normalize_samples())) {
    LOG_WARN("fail to try normalize all samples", K(ret), K(ctx_));
  } else if (OB_FAIL(algo_->build(ctx_.sample_vectors_))) {
    LOG_WARN("fail to build kmeans algo", K(ret), K(algo_));
  } else {
    insert_monitor->kmeans_monitor_.add_finish_tablet_cnt();
  }
  if (OB_NOT_NULL(algo_)) {
    algo_->destroy();
  }
  LOG_INFO("SingleKmeans build cost", K(ret), K(ObTimeUtil::current_time_ms() - start_time));
  return ret;
}

int ObSingleKmeansExecutor::get_kmeans_algo(ObKmeansAlgo *&algo)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "kmeans ctx is not inited", K(ret));
  } else if (OB_ISNULL(algo_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null algo", K(ret));
  } else {
    algo = algo_;
  }
  return ret;
}

int64_t ObSingleKmeansExecutor::get_centers_count() const
{
  int64_t i_ret = 0;
  if (OB_NOT_NULL(algo_)) {
    i_ret = algo_->get_cur_centers().count();
  }
  return i_ret;
}

int64_t ObSingleKmeansExecutor::get_centers_dim() const
{
  int64_t i_ret = 0;
  if (OB_NOT_NULL(algo_)) {
    i_ret = algo_->get_cur_centers().dim_;
  }
  return i_ret;
}

int ObSingleKmeansExecutor::get_center(const int64_t pos, float *&center_vector)
{
  int ret = OB_SUCCESS;
  if (pos < 0 || pos >= get_centers_count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", K(ret), K(pos), K(get_centers_count()));
  } else {
    center_vector = algo_->get_cur_centers().at(pos);
  }
  return ret;
}

// ------------------ ObMultiKmeansExecutor implement ------------------
ObMultiKmeansExecutor::~ObMultiKmeansExecutor()
{
  for (int i = 0; i < algos_.count(); ++i) {
    if (OB_NOT_NULL(algos_[i])) {
      algos_[i]->~ObKmeansAlgo();
      ivf_build_mem_ctx_.Deallocate(algos_[i]);
      algos_[i] = nullptr;
    }
  }
  algos_.reset();
}

int ObMultiKmeansExecutor::init(
    ObKmeansAlgoType algo_type,
    const int64_t tenant_id,
    const int64_t lists,
    const int64_t samples_per_nlist,
    const int64_t dim,
    ObVectorIndexDistAlgorithm dist_algo,
    ObVectorNormalizeInfo *norm_info/* = nullptr*/,
    const int64_t pq_m_size/* = 1*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx_.init(tenant_id, lists, samples_per_nlist, dim, dist_algo, norm_info, pq_m_size))) {
    LOG_WARN("fail to init kmeans ctx", K(ret), K(tenant_id), K(lists), K(samples_per_nlist), K(dim), K(dist_algo));
  } else {
    pq_m_size_ = pq_m_size;
    if (OB_FAIL(algos_.prepare_allocate(pq_m_size))) {
      LOG_WARN("fail to reserve space", K(ret), K(pq_m_size));
    }

    for (int i = 0; OB_SUCC(ret) && i < algos_.count(); ++i) {
      if (algo_type == ObKmeansAlgoType::KAT_ELKAN) {
        void *tmp_buf = nullptr;
        if (OB_ISNULL(tmp_buf = ivf_build_mem_ctx_.Allocate(sizeof(ObElkanKmeansAlgo)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc tmp_buf", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
        } else {
          algos_[i] = new (tmp_buf) ObElkanKmeansAlgo(ivf_build_mem_ctx_);
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid kmeans algorithm type", K(ret), K(algo_type));
      }
      if (FAILEDx(algos_[i]->init(ctx_))) {
        LOG_WARN("fail to init kmeans algo", K(ret), K(ctx_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObMultiKmeansExecutor::build(ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "kmeans ctx is not inited", K(ret));
  } else {
    int64_t start_time = ObTimeUtil::current_time_ms();
    ObArenaAllocator tmp_alloc("MulKmeans", OB_MALLOC_NORMAL_BLOCK_SIZE, ctx_.tenant_id_);
    // init spilited_arrs, size: m * sample_vectors_.count()
    ObArrayArray<float*> splited_arrs(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(tmp_alloc, "MulKmeans"));
    if (OB_FAIL(prepare_splited_arrs(splited_arrs))) {
      LOG_WARN("fail to prepare splited_arrs", K(ret));
    } else if (OB_NOT_NULL(insert_monitor)) {
      if (OB_NOT_NULL(insert_monitor->kmeans_monitor_.vec_index_task_total_cnt_)) {
        (void)ATOMIC_AAF(insert_monitor->kmeans_monitor_.vec_index_task_total_cnt_, pq_m_size_);
      }
      if (OB_NOT_NULL(insert_monitor->kmeans_monitor_.vec_index_task_thread_pool_cnt_)) {
        (void)ATOMIC_SET(insert_monitor->kmeans_monitor_.vec_index_task_thread_pool_cnt_, 1);
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < pq_m_size_; ++i) {
      if (i >= algos_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("size of algos_ should be pq_m_size_", K(ret), K(i), K(pq_m_size_), K(algos_.count()));
      } else if (OB_FAIL(algos_[i]->build(splited_arrs.at(i)))) {
        LOG_WARN("fail to build kmeans algo", K(ret), K(i), K(algos_[i]));
      } else if (OB_NOT_NULL(insert_monitor) && OB_NOT_NULL(insert_monitor->kmeans_monitor_.vec_index_task_finish_cnt_)) {
        (void)ATOMIC_AAF(insert_monitor->kmeans_monitor_.vec_index_task_finish_cnt_, 1);
      }
      if (OB_NOT_NULL(algos_[i])) {
        algos_[i]->destroy();
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(insert_monitor)) {
      insert_monitor->kmeans_monitor_.add_finish_tablet_cnt();
    }
    LOG_INFO("MultiKmeans build cost", K(ret), K(ObTimeUtil::current_time_ms() - start_time));
  }

  return ret;
}

int ObMultiKmeansExecutor::prepare_splited_arrs(ObArrayArray<float *> &splited_arrs)
{
  int ret = OB_SUCCESS;
  const ObIArray<float *> &sample_arr = ctx_.sample_vectors_;
  if (OB_FAIL(splited_arrs.reserve(pq_m_size_))) {
    LOG_WARN("fail to prepare alloc space", K(ret), K(pq_m_size_));
  }
  for (int i = 0; OB_SUCC(ret) && i < pq_m_size_; ++i) {
    if (OB_FAIL(splited_arrs.push_back(ObArray<float *>()))) {
      LOG_WARN("fail to push back empty array", K(ret), K(i), K(pq_m_size_), K(splited_arrs.count()));
    } else if (OB_FAIL(splited_arrs.at(i).reserve(sample_arr.count()))) {
      LOG_WARN("fail to reserve space", K(ret), K(i), K(pq_m_size_), K(splited_arrs.count()));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < sample_arr.count(); ++i) {
    if (OB_FAIL(split_vector(sample_arr.at(i), splited_arrs))) {
      LOG_WARN("fail to split vector", K(ret));
    }
  }

  return ret;
}

int ObMultiKmeansExecutor::init_build_handle(ObKmeansBuildTaskHandler &handle)
{
  int ret = OB_SUCCESS;

  common::ObSpinLockGuard init_guard(handle.lock_);               // lock thread pool init to avoid init twice
  if (handle.get_tg_id() != ObKmeansBuildTaskHandler::INVALID_TG_ID) {  // no need to init twice, skip
  } else if (OB_FAIL(handle.init())) {
    LOG_WARN("fail to init vector kmeans build task handle", K(ret));
  } else if (OB_FAIL(handle.start())) {
    LOG_WARN("fail to start vector kmeans build thread pool", K(ret));
  }

  return ret;
}

void ObMultiKmeansExecutor::wait_kmeans_task_finish(ObKmeansBuildTask *build_tasks, ObKmeansBuildTaskHandler &handle)
{
  int ret = OB_SUCCESS;

  bool is_all_finish = false;
  if (OB_NOT_NULL(build_tasks)) {
    while (!is_all_finish && handle.get_task_ref() > 0) {
      if (check_stop()) {
        for (int i = 0; i < pq_m_size_; i++) {
          build_tasks[i].set_task_stop();
        }
        LOG_INFO("kmeans executor is fore stop", K(*this));
        // do not break, wait for all task finish
      }
      int64_t max_thread_cnt = 0;
      if (OB_FAIL(handle.get_max_thread_count(max_thread_cnt, true /*with_refresh*/))) {
        LOG_WARN("fail to get max thread count", K(ret));
      }
      ob_usleep(ObKmeansBuildTaskHandler::WAIT_RETRY_PUSH_TASK_TIME);
      is_all_finish = true;
      for (int i = 0; i < pq_m_size_; i++) {
        if (!build_tasks[i].is_finish()) {
          is_all_finish = false;
          break;
        }
      }
    }
  }
}

int ObMultiKmeansExecutor::do_build_task_local(const common::ObTableID &table_id, const common::ObTabletID &tablet_id,
                                               ObKmeansBuildTaskHandler &handle,
                                               const ObArrayArray<float *> &splited_arrs,
                                               ObKmeansBuildTask *build_tasks, int task_idx,
                                               ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(build_tasks))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(build_tasks), K(task_idx));
  }
  for (; task_idx < pq_m_size_ && OB_SUCC(ret); task_idx++) {
    if (check_stop()) {
      ret = OB_CANCELED;
      LOG_INFO("kmeans executor is fore stop", K(*this));
      break;
    }
    if (OB_NOT_NULL(insert_monitor) && OB_NOT_NULL(insert_monitor->kmeans_monitor_.vec_index_task_thread_pool_cnt_)) {
      int64_t thread_cnt = TG_GET_THREAD_CNT(handle.get_tg_id());
      (void)ATOMIC_SET(insert_monitor->kmeans_monitor_.vec_index_task_thread_pool_cnt_, thread_cnt + 1);
    }
    ObKmeansBuildTask &build_task = build_tasks[task_idx];
    int64_t max_thread_cnt = 0;
    if (OB_FAIL(build_task.init(table_id, tablet_id, task_idx, algos_[task_idx],
                                &splited_arrs.at(task_idx), insert_monitor))) {  // 1. make task
      LOG_WARN("fail to init opt async task", KR(ret));
    } else if (OB_FAIL(handle.get_max_thread_count(max_thread_cnt, true /*with_refresh*/))) {
      LOG_WARN("fail to get max thread count", K(ret));
    } else if (handle.get_task_ref() < max_thread_cnt) {
      if (OB_FAIL(handle.push_task(build_task))) {
        LOG_WARN("fail to push build task", KR(ret), K(max_thread_cnt));
      }
    } else if (OB_FAIL(build_task.do_work())) {
      LOG_WARN("fail to do build task", KR(ret));
    }
  }
  return ret;
}

int ObMultiKmeansExecutor::build_parallel(const common::ObTableID &table_id, const common::ObTabletID &tablet_id, ObInsertMonitor* insert_monitor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("kmeans ctx is not inited", K(ret));
  } else {
    int64_t start_time = ObTimeUtil::current_time_ms();
    LOG_INFO("start build_parallel", K(table_id), K(tablet_id), K(ctx_));
    ObArenaAllocator tmp_alloc("MulKmeans", OB_MALLOC_NORMAL_BLOCK_SIZE, ctx_.tenant_id_);
    // init spilited_arrs, size: m * sample_vectors_.count()
    ObArrayArray<float *> splited_arrs(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(tmp_alloc, "MulKmeans"));
    if (OB_FAIL(prepare_splited_arrs(splited_arrs))) {
      LOG_WARN("fail to prepare splited_arrs", K(ret));
    } else {
      // build_parallel
      ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
      if (OB_ISNULL(service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else {
        ObKmeansBuildTaskHandler &handle = service->get_kmeans_build_handler();
        void *buf = nullptr;
        ObKmeansBuildTask *build_tasks = nullptr;
        int64_t max_thread_cnt = 0;

        if (OB_FAIL(init_build_handle(handle))) {
          LOG_WARN("fail to init build handle", K(ret));
        } else if (OB_FAIL(handle.get_max_thread_count(max_thread_cnt, true /*with_refresh*/))) {
          LOG_WARN("fail to get max thread count", K(ret));
        } else if (OB_ISNULL(buf = tmp_alloc.alloc(sizeof(ObKmeansBuildTask) * pq_m_size_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory of ObKmeansBuildTask", K(ret));
        } else if (FALSE_IT(build_tasks = new (buf) ObKmeansBuildTask[pq_m_size_])) {
        } else if (pq_m_size_ != algos_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("size of algos_ should be pq_m_size_", K(ret), K(pq_m_size_), K(algos_.count()));
        } else if (pq_m_size_ != splited_arrs.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("size of splited_arrs should be pq_m_size_", K(ret), K(pq_m_size_), K(splited_arrs.count()));
        } else if (OB_NOT_NULL(insert_monitor) && OB_NOT_NULL(insert_monitor->kmeans_monitor_.vec_index_task_total_cnt_)) {
          (void)ATOMIC_AAF(insert_monitor->kmeans_monitor_.vec_index_task_total_cnt_, pq_m_size_);
        } else if (OB_NOT_NULL(insert_monitor) && OB_NOT_NULL(insert_monitor->kmeans_monitor_.vec_index_task_thread_pool_cnt_)) {
          int64_t thread_cnt = TG_GET_THREAD_CNT(handle.get_tg_id());
          (void)ATOMIC_SET(insert_monitor->kmeans_monitor_.vec_index_task_thread_pool_cnt_, thread_cnt);
        }

        int64_t a_thread_task_cnt = pq_m_size_ / (max_thread_cnt + 1);
        int task_idx = 0;
        for (; task_idx < pq_m_size_ - a_thread_task_cnt && OB_SUCC(ret); task_idx++) {
          if (check_stop()) {
            ret = OB_CANCELED;
            LOG_INFO("kmeans executor is fore stop", K(*this));
            break;
          }
          ObKmeansBuildTask &build_task = build_tasks[task_idx];
          if (OB_FAIL(build_task.init(table_id, tablet_id, task_idx, algos_[task_idx], &splited_arrs.at(task_idx), insert_monitor))) {  // 1. make task
            LOG_WARN("fail to init opt async task", KR(ret));
          } else if (OB_FAIL(handle.push_task(build_task))) {  // 2. push task
            if (OB_EAGAIN != ret) {
              SHARE_LOG(WARN, "failed to push build task", K(ret), K(task_idx));
            } else if (OB_FAIL(build_task.do_work())) {  // Degraded to single-threaded processing
              LOG_WARN("fail to do build task", KR(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          // The remaining tasks are executed by the current thread.
          if (OB_FAIL(do_build_task_local(table_id, tablet_id, handle, splited_arrs, build_tasks, task_idx, insert_monitor))) {
            LOG_WARN("fail to do build task local", KR(ret));
          }
        } else {
          // Note. If the previous process fails, all tasks need to be stopped otherwise it may cause a core dump
          for (int i = 0; i < pq_m_size_; i++) {
            ObKmeansBuildTask &build_task = build_tasks[i];
            build_task.set_task_stop();
          }
        }

        // 3. wait for all task finish
        wait_kmeans_task_finish(build_tasks, handle);

        // 4. check task result
        for (int i = 0; i < pq_m_size_ && OB_SUCC(ret); i++) {
          if (OB_UNLIKELY(OB_ISNULL(build_tasks))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(ret));
          } else if (OB_FAIL(build_tasks[i].get_ret())) {
            LOG_WARN("fail to build kmeans algo", K(ret), K(i), K(build_tasks[i]));
          }
        }  // end for
        if (OB_SUCC(ret) && OB_NOT_NULL(insert_monitor)) {
          insert_monitor->kmeans_monitor_.add_finish_tablet_cnt();
        }
      }
    }
    LOG_INFO("build_parallel cost", K(ret), K(ObTimeUtil::current_time_ms() - start_time));
  }

  return ret;
}

int ObMultiKmeansExecutor::split_vector(float *vector, ObArrayArray<float *> &splited_arrs)
{
  int ret = OB_SUCCESS;
  int64_t start_idx = 0;
  for (int i = 0; OB_SUCC(ret) && i < pq_m_size_; ++i) {
    if (OB_FAIL(splited_arrs.push_back(i, vector + start_idx))) {
      SHARE_LOG(WARN, "failed to push back array", K(ret), K(i));
    } else {
      start_idx += ctx_.dim_;
    }
  }
  return ret;
}

int64_t ObMultiKmeansExecutor::get_total_centers_count() const
{
  int64_t i_ret = 0;
  if (!algos_.empty() && OB_NOT_NULL(algos_[0])) {
    i_ret = algos_[0]->get_cur_centers().count() * algos_.count();
  }
  return i_ret;
}

int64_t ObMultiKmeansExecutor::get_centers_count_per_kmeans() const
{
  int64_t i_ret = 0;
  if (!algos_.empty() && OB_NOT_NULL(algos_[0])) {
    i_ret = algos_[0]->get_cur_centers().count();
  }
  return i_ret;
}

int64_t ObMultiKmeansExecutor::get_centers_dim() const
{
  int64_t i_ret = 0;
  if (!algos_.empty() && OB_NOT_NULL(algos_[0])) {
    i_ret = algos_[0]->get_cur_centers().dim_;
  }
  return i_ret;
}

int ObMultiKmeansExecutor::get_center(const int64_t pos, float *&center_vector)
{
  int ret = OB_SUCCESS;
  int64_t centers_count = get_centers_count_per_kmeans();
  if (pos < 0 || pos >= get_total_centers_count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", K(ret), K(pos), K(get_total_centers_count()));
  } else if (centers_count <= 0 || pos / centers_count >= algos_.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", K(ret), K(centers_count), K(pos), K(algos_.count()));
  } else {
    ObKmeansAlgo *algo = algos_[pos / centers_count];
    if (OB_ISNULL(algo)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null algo", K(ret), K(pos / centers_count));
    } else if (pos % centers_count >= algo->get_cur_centers().count()) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("index out of range", K(ret), K(centers_count), K(pos), K(algo->get_cur_centers().count()));
    } else {
      center_vector = algo->get_cur_centers().at(pos % centers_count);
    }
  }
  return ret;
}

// ------------------ ObElkanKmeansAlgo implement ------------------
void ObElkanKmeansAlgo::destroy()
{
  ObKmeansAlgo::destroy();
}

int ObElkanKmeansAlgo::assign_vectors_parallel(const ObIArray<float *> &input_vectors, float *centers_distance,
                                               int32_t *data_cnt_in_cluster, float &dis_obj)
{
  int ret = OB_SUCCESS;
  const int64_t sample_cnt = input_vectors.count();
  
  if (OB_ISNULL(task_handler_)) {
    // If task handler is unavailable, fallback to serial processing
    if (OB_FAIL(assign_vectors_range(input_vectors, 0, sample_cnt, centers_distance, data_cnt_in_cluster, dis_obj, false))) {
      SHARE_LOG(WARN, "failed to assign vectors range", K(ret));
    }
  } else if (OB_UNLIKELY(OB_ISNULL(assign_tasks_) || max_assign_tasks_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "unexpected nullptr", K(ret));
  } else {
    dis_obj = 0.0f;
    // NOTE: max_assign_tasks_ + 1 is used to handle the final task
    const int64_t block_size = std::max(1L, sample_cnt / (max_assign_tasks_ + 1));
    int64_t end_idx = 0;

    // Create and submit tasks
    for (int64_t i = 0; OB_SUCC(ret) && i < max_assign_tasks_; ++i) {
      // max_assign_tasks_ is min(sample_cnt, max_thread_cnt), so end_idx <= sample_cnt
      int64_t start_idx = i * block_size;
      end_idx = start_idx + block_size;

      ObKmeansAssignTask &task = assign_tasks_[i];
      // Reset task state to ensure proper reuse
      task.reset();
      if (check_stop()) {
        ret = OB_CANCELED;
        SHARE_LOG(WARN, "check stop", K(ret));
      } else if (OB_FAIL(task.init(start_idx, end_idx, this, &input_vectors, centers_distance, data_cnt_in_cluster))) {
        SHARE_LOG(WARN, "failed to init assign task", K(ret), K(i));
      } else if (OB_FAIL(task_handler_->push_task(task))) {
        if (OB_EAGAIN != ret) {
          SHARE_LOG(WARN, "failed to push assign task", K(ret), K(i));
        } else if (OB_FAIL(task.do_work())) {
          SHARE_LOG(WARN, "failed to do assign task", K(ret));
        }
      }
    }

    // Handle the final task
    if (OB_SUCC(ret)) {
      float tmp_dis_obj = 0.0f;
      if (OB_FAIL(assign_vectors_range(input_vectors, end_idx, sample_cnt, centers_distance, data_cnt_in_cluster,
                                       tmp_dis_obj, true))) {
        SHARE_LOG(WARN, "failed to assign vectors range", K(ret), K(end_idx), K(sample_cnt));
      } else {
        dis_obj += tmp_dis_obj;
      }
    } else {
      for (int64_t i = 0; i < max_assign_tasks_; ++i) {
        ObKmeansAssignTask &task = assign_tasks_[i];
        task.set_task_stop();
      }
    }

    // Note. Anywhere above, all tasks need to be stopped otherwise it may cause a core dump because some data
    // will be released.
    wait_parallel_task_finish(assign_tasks_, max_assign_tasks_, *task_handler_);

    for (int64_t i = 0; OB_SUCC(ret) && i < max_assign_tasks_; ++i) {
      ObKmeansAssignTask &task = assign_tasks_[i];
      if (OB_FAIL(task.get_ret())) {
        SHARE_LOG(WARN, "assign task failed", K(ret), K(i));
      } else {
        dis_obj += task.get_dis_obj();
      }
    }
  }

  return ret;
}

int ObElkanKmeansAlgo::search_nearest_center(const ObIArray<float*> &input_vectors, float* centers_distance, int32_t *data_cnt_in_cluster, float &dis_obj)
{
  int ret = OB_SUCCESS;  
  if (OB_UNLIKELY(kmeans_ctx_->lists_ != centers_[cur_idx_].count() || OB_ISNULL(centers_distance) || OB_ISNULL(data_cnt_in_cluster))) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "param error", K(ret), K(kmeans_ctx_->lists_), K(centers_[cur_idx_].count()), KP(centers_distance), KP(data_cnt_in_cluster));
  } else {
    const int64_t sample_cnt = input_vectors.count();
    const int64_t center_count = kmeans_ctx_->lists_;
    const int64_t dim = kmeans_ctx_->dim_;

    float distance = 0.0;
    // 1. calc distance between each two centers
    for (int64_t i = 0; OB_SUCC(ret) && i < center_count; ++i) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < center_count; ++j) {
        if (OB_FAIL(calc_kmeans_distance(centers_[cur_idx_].at(i), centers_[cur_idx_].at(j), dim, distance))) {
          SHARE_LOG(WARN, "failed to calc kmeans distance between centers", K(ret));
        } else {
          set_centers_distance(centers_distance, i, j, distance);
        }
      }
    }
    if (OB_SUCC(ret)) {
      // Use block parallel processing for vector assignment
      if (OB_FAIL(assign_vectors_parallel(input_vectors, centers_distance, data_cnt_in_cluster, dis_obj))) {
        SHARE_LOG(WARN, "failed to assign vectors parallel", K(ret));
      } else {
        dis_obj = dis_obj / sample_cnt;
      }
    }
  }
  return ret;
}

int ObElkanKmeansAlgo::do_kmeans(const ObIArray<float*> &input_vectors)
{
  int ret = OB_SUCCESS;
  
  if (RUNNING_KMEANS != status_) {
    ret = OB_STATE_NOT_MATCH;
    SHARE_LOG(WARN, "status not match", K(ret), K(status_));
  } else {
    // Upper triangular matrix
    float* centers_distance = nullptr; // half the distance between each two centers
    int32_t *data_cnt_in_cluster = nullptr; // the number of vectors contained in each center (cluster)
    // init tmp variables
    float *tmp = nullptr;
    int64_t center_dis_size = max(1, kmeans_ctx_->lists_ * (kmeans_ctx_->lists_ - 1) / 2);
    if (OB_ISNULL(tmp = static_cast<float *>(ivf_build_mem_ctx_.Allocate(sizeof(float) * center_dis_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
    } else {
      MEMSET(tmp, 0, sizeof(float) * center_dis_size);
      centers_distance = tmp;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(data_cnt_in_cluster =
        static_cast<int32_t*>(ivf_build_mem_ctx_.Allocate(sizeof(int32_t) * kmeans_ctx_->lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
    } else {
      MEMSET(data_cnt_in_cluster, 0, sizeof(int32_t) * kmeans_ctx_->lists_);
    }

    const int64_t dim = kmeans_ctx_->dim_;
    float prev_dis_obj = 0;

    for (int64_t iter = 0; OB_SUCC(ret) && iter < N_ITER; ++iter) {
      if (check_stop()) {
        ret = OB_CANCELED;
        SHARE_LOG(INFO, "kmeans ctx is fore stop", K(ret), K(*this));
        break;
      }
      int64_t iter_start_time = ObTimeUtility::current_time_ms();
      float dis_obj = 0.0;
      MEMSET(data_cnt_in_cluster, 0, sizeof(int32_t) * kmeans_ctx_->lists_);
      centers_[next_idx()].clear();
      // 1. search nearest center
      if (OB_FAIL(search_nearest_center(input_vectors, centers_distance, data_cnt_in_cluster, dis_obj))) {
        SHARE_LOG(WARN, "failed to search nearest center", K(ret));
      }

      // 2. calc the new centers
      for (int64_t i = 0; OB_SUCC(ret) && i < kmeans_ctx_->lists_; ++i) {
        if (data_cnt_in_cluster[i] > 0) {
          if (OB_FAIL(centers_[next_idx()].divide(i, data_cnt_in_cluster[i]))) {
            SHARE_LOG(WARN, "failed to divide vector", K(ret));
          }
        } else {
          // use random sample vector as the center
          int64_t random = 0;
          const int64_t sample_cnt = input_vectors.count();
          random = ObRandom::rand(0, sample_cnt - 1);
          if (OB_FAIL(centers_[next_idx()].add(i, kmeans_ctx_->dim_, input_vectors.at(random)))) {
            SHARE_LOG(WARN, "failed to add vector", K(ret));
          }
        }
        // 3. normalize the new center, if need
        if (OB_SUCC(ret)) {
          if (OB_FAIL(kmeans_ctx_->try_normalize(
              kmeans_ctx_->dim_,
              centers_[next_idx()].at(i), 
              centers_[next_idx()].at(i)))) {
            LOG_WARN("failed to normalize vector", K(ret));
          }
        }
      } // end for
      
      // 4. check finish && switch center buffer
      if (OB_SUCC(ret)) {
        double imbalance_factor = this->calc_imbalance_factor(input_vectors, data_cnt_in_cluster);
        float diff = (iter == 0) ? 1.0 : fabs(prev_dis_obj - dis_obj) / prev_dis_obj;
        prev_dis_obj = dis_obj;
        if (OB_NOT_NULL(kmeans_monitor_)) {
          kmeans_monitor_->set_kmeams_monitor(iter, EARLY_FINISH_THRESHOLD, diff, imbalance_factor);
        }
        if (iter > 0 && diff <= EARLY_FINISH_THRESHOLD) {
          LOG_INFO("finish do kmeans before all iters", K(ret), K(iter), K(dis_obj), K(diff), K(imbalance_factor));
          break;  // finish
        } else {
          cur_idx_ = next_idx();
          LOG_INFO("finish one iters", K(ret), K(iter), K(dis_obj), K(diff), K(ObTimeUtility::current_time_ms() - iter_start_time));
          if (iter + 1 >= N_ITER) {
            LOG_INFO("finish do kmeans iters", K(ret), K(iter), K(dis_obj), K(diff), K(imbalance_factor));
          }
        }
      }
    }  // iter end for
    // free tmp memory
    int64_t mem_used = ivf_build_mem_ctx_.get_all_vsag_use_mem_byte() >> 20;
    LOG_INFO("elkan kmeans memused", K(ret), K(mem_used));
    if (OB_NOT_NULL(centers_distance)) {
      ivf_build_mem_ctx_.Deallocate(centers_distance);
      centers_distance = nullptr;
    }
    if (OB_NOT_NULL(data_cnt_in_cluster)) {
      ivf_build_mem_ctx_.Deallocate(data_cnt_in_cluster);
      data_cnt_in_cluster = nullptr;
    }
    if (OB_SUCC(ret)) {
      status_ = FINISH;
    }
  }
  return ret;
}

int ObElkanKmeansAlgo::add_vector_to_center_safe(int64_t center_idx, int64_t dim, float* vector, int32_t* data_cnt_in_cluster)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(assign_lock_);
  if (OB_FAIL(get_centers(next_idx()).add(center_idx, dim, vector))) {
    SHARE_LOG(WARN, "failed to add vector to center buffer", K(ret));
  } else {
    ++data_cnt_in_cluster[center_idx];
  }
  return ret;
}

int ObElkanKmeansAlgo::assign_vectors_range(const ObIArray<float *> &input_vectors, int64_t start_idx, int64_t end_idx,
                                            float *centers_distance, int32_t *data_cnt_in_cluster, float &dis_obj,
                                            bool use_safe_add)
{
  int ret = OB_SUCCESS;
  const int64_t dim = kmeans_ctx_->dim_;
  const int64_t center_count = kmeans_ctx_->lists_;
  
  for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
    if (check_stop()) {
      ret = OB_CANCELED;
      SHARE_LOG(WARN, "check stop", K(ret));
      break;
    }
    float* sample_vector = input_vectors.at(i);
    int64_t nearest_center_idx = 0;
    float min_distance = FLT_MAX;
    float gate_distance = FLT_MAX;
    
    if (OB_FAIL(calc_kmeans_distance(sample_vector, centers_[cur_idx_].at(0), dim, min_distance))) {
      SHARE_LOG(WARN, "failed to calc kmeans distance", K(ret));
    } else {
      nearest_center_idx = 0;
      gate_distance = min_distance * GATE_DISTANCE_FACTOR;
    }
    
    for (int64_t j = 1; OB_SUCC(ret) && j < center_count; ++j) {
      float dis_near_cur = get_centers_distance(centers_distance, nearest_center_idx, j);
      if (dis_near_cur < gate_distance) {
        float dis_half_dim = 0.0f;
        if (OB_FAIL(calc_kmeans_distance(sample_vector, centers_[cur_idx_].at(j), dim / 2, dis_half_dim))) {
          SHARE_LOG(WARN, "failed to calc kmeans distance", K(ret)); 
        } else if (dis_half_dim < min_distance) {
          float full_distance = 0.0f;
          if (OB_FAIL(calc_kmeans_distance(sample_vector + dim / 2, centers_[cur_idx_].at(j) + dim / 2, dim - dim / 2, full_distance))) {
            SHARE_LOG(WARN, "failed to calc kmeans distance", K(ret));
          } else if (OB_FALSE_IT(full_distance += dis_half_dim)) {
          } else if (full_distance < min_distance) {
            min_distance = full_distance;
            gate_distance = min_distance * GATE_DISTANCE_FACTOR;
            nearest_center_idx = j;
          }
        }
      }
    }
    
    if (OB_SUCC(ret)) {
      // Update the distance of the target function
      dis_obj += min_distance;
      
      if (use_safe_add) {
        if (OB_FAIL(add_vector_to_center_safe(nearest_center_idx, dim, sample_vector, data_cnt_in_cluster))) {
          SHARE_LOG(WARN, "failed to add vector to center buffer safely", K(ret));
        }
      } else {
        // Use normal method (for serial processing)
        if (OB_FAIL(centers_[next_idx()].add(nearest_center_idx, dim, sample_vector))) {
          SHARE_LOG(WARN, "failed to add vector to center buffer", K(ret));
        } else {
          ++data_cnt_in_cluster[nearest_center_idx];
        }
      }
    }
  }
  
  return ret;
}

int ObKmeansAlgo::calc_distances_range(const ObIArray<float*> &input_vectors, int64_t start_idx, int64_t end_idx,
                                      float* current_center, float* weight, const int64_t dim, float &sum)
{
  int ret = OB_SUCCESS;
  
  for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
    float distance = 0.0f;
    if (OB_FAIL(ObKmeansAlgo::calc_kmeans_distance(input_vectors.at(i), current_center, dim, distance))) {
      SHARE_LOG(WARN, "failed to calc kmeans distance", K(ret));
    } else {
      distance *= distance;
      if (distance < weight[i]) {
        weight[i] = distance;
      }
      sum += weight[i];
    }
  }
  
  return ret;
}

// ------------------ ObIvfBuildHelper implement ------------------
void ObIvfBuildHelper::reset()
{
  ObIAllocator *allocator = get_allocator();
  if (OB_NOT_NULL(allocator) && OB_NOT_NULL(ivf_build_mem_ctx_)) {
    ivf_build_mem_ctx_->~ObIvfMemContext();
    allocator->free(ivf_build_mem_ctx_);
    ivf_build_mem_ctx_ = nullptr;
  }
}

int ObIvfBuildHelper::init(ObString &init_str, lib::MemoryContext &parent_mem_ctx, uint64_t *all_vsag_use_mem)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(init_str, ObVectorIndexType::VIT_IVF_INDEX, param_))) {
    LOG_WARN("failed to parse params.", K(ret));
  } else if (OB_ISNULL(ivf_build_mem_ctx_ = OB_NEWx(ObIvfMemContext, get_allocator(), all_vsag_use_mem))) { 
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create ivf_build_mem_ctx", K(ret)); 
  } else if (OB_FAIL(ivf_build_mem_ctx_->init(parent_mem_ctx, all_vsag_use_mem, tenant_id_, ObIvfMemContext::IVF_BUILD_LABEL))) {
    LOG_WARN("failed to init memory context", K(ret));
    get_allocator()->free(ivf_build_mem_ctx_);
    ivf_build_mem_ctx_ = nullptr;
  } else {
    int64_t mem_used = ivf_build_mem_ctx_->get_all_vsag_use_mem_byte() >> 20;
    SHARE_LOG(INFO, "init ivf_build_mem_ctx", K(ret), K(mem_used));
  }
  return ret;
}

int ObIvfBuildHelper::init_ctx(int64_t dim)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  if (is_inited_) {
    ret = OB_SUCCESS;
    SHARE_LOG(INFO, "init ctx already inited", K(ret), K(dim), K(param_));
  } else if (first_ret_code_ != OB_SUCCESS) {
    ret = first_ret_code_;
    SHARE_LOG(WARN, "init falied before", K(ret), K(dim), K(param_));
  } else if (OB_FAIL(init_kmeans_ctx(dim))) {
    SHARE_LOG(WARN, "failed to init kmeans ctx", K(ret), K(dim), K(param_));
  } else {
    is_inited_ = true;
  }
  first_ret_code_ = ret;

  return ret;
}

void ObIvfBuildHelper::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
  OB_LOG(DEBUG, "inc ref count", K(ref_cnt_), KP(this), KPC(this), K(lbt())); // remove later
}

bool ObIvfBuildHelper::dec_ref_and_check_release()
{
  int64_t ref_count = ATOMIC_SAF(&ref_cnt_, 1);
  OB_LOG(DEBUG,"dec ref count", K(ref_count), KP(this), KPC(this), K(lbt())); // remove later
  return (ref_count == 0);
}

int64_t ObIvfBuildHelper::get_free_vector_mem_size()
{
  int ret = OB_SUCCESS;
  int64_t free_vector_mem_size = 0;
  int64_t tenant_mem_size = 0;
  int64_t curr_used = 0;
  if (OB_ISNULL(ivf_build_mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem ctx is null", K(ret));
  } else if (OB_FALSE_IT(curr_used = ATOMIC_LOAD(ivf_build_mem_ctx_->get_all_vsag_use_mem()))) {
  } else if (OB_FAIL(ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id_, tenant_mem_size))) {
    LOG_WARN("failed to get vector mem limit size.", K(ret), K(tenant_id_));
  } else if (tenant_mem_size > curr_used) {
    free_vector_mem_size = tenant_mem_size - curr_used;
  }
  LOG_INFO("free vector mem limit size.", K(ret), K(free_vector_mem_size), K(tenant_mem_size), K(curr_used));
  return free_vector_mem_size;
}

// ------------------ ObIvfFlatBuildHelper implement ------------------
ObIvfFlatBuildHelper::~ObIvfFlatBuildHelper()
{
  if (OB_NOT_NULL(executor_) && OB_NOT_NULL(ivf_build_mem_ctx_)) {
    executor_->~ObSingleKmeansExecutor();
    ivf_build_mem_ctx_->Deallocate(executor_);
    executor_ = nullptr;
  }
}

int ObIvfFlatBuildHelper::init_kmeans_ctx(const int64_t dim)
{
  int ret = OB_SUCCESS;
  ObKmeansAlgoType algo_type = ObKmeansAlgoType::KAT_ELKAN;
  void *buf = nullptr;
  ObVectorNormalizeInfo *norm_info = nullptr;
  if (OB_NOT_NULL(executor_)) {
    // do nothing
  } else if (0 >= param_.nlist_ || 0 >= param_.sample_per_nlist_ || 0 >= dim || VIDA_MAX <= param_.dist_algorithm_) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), K(param_));
  } else if ((VIDA_IP == param_.dist_algorithm_ || VIDA_COS == param_.dist_algorithm_) && 
              FALSE_IT(norm_info = &norm_info_)) { // IP and COS algorithms need normalization
  } else if (OB_ISNULL(ivf_build_mem_ctx_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ivf_build_mem_ctx_ is null", K(ret));
  } else {
    void *tmp_buf = nullptr;
    if (OB_ISNULL(tmp_buf = ivf_build_mem_ctx_->Allocate(sizeof(ObSingleKmeansExecutor)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc tmp_buf", K(ret), K(ivf_build_mem_ctx_->get_all_vsag_use_mem_byte()));
    } else if (OB_FALSE_IT(executor_ = new (tmp_buf) ObSingleKmeansExecutor(*ivf_build_mem_ctx_))) {
    } else if (OB_FAIL(executor_->init(algo_type, tenant_id_, param_.nlist_,
                                       param_.sample_per_nlist_, dim, param_.dist_algorithm_, norm_info))) {
      LOG_WARN("failed to init kmeans ctx", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  first_ret_code_ = ret;
  return ret;
}

// ------------------ ObIvfSq8BuildHelper implement ------------------
ObIvfSq8BuildHelper::~ObIvfSq8BuildHelper()
{
  if (OB_NOT_NULL(ivf_build_mem_ctx_)) {
    if (OB_NOT_NULL(min_vector_)) {
      ivf_build_mem_ctx_->Deallocate(min_vector_);
      min_vector_ = nullptr;
    }
    if (OB_NOT_NULL(max_vector_)) {
      ivf_build_mem_ctx_->Deallocate(max_vector_);
      max_vector_ = nullptr;
    }
    if (OB_NOT_NULL(step_vector_)) {
      ivf_build_mem_ctx_->Deallocate(step_vector_);
      step_vector_ = nullptr;
    }
  }
}

int ObIvfSq8BuildHelper::update(const float *vector, int64_t dim)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIvfSq8BuildHelper is not inited", K(ret));
  } else if (OB_ISNULL(vector) || dim != dim_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector or dim", K(ret), KP(vector), K(dim), K(dim_));
  }
  float cur = 0;
  for (int i = 0; i < dim_ && OB_SUCC(ret); ++i) {
    cur = vector[i];
    if (cur < min_vector_[i]) {
      min_vector_[i] = cur;
    }
    if (cur > max_vector_[i]) {
      max_vector_[i] = cur;
    }
  }
  return ret;
}

int ObIvfSq8BuildHelper::build()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIvfSq8BuildHelper is not inited", K(ret));
  } else {
    for (int i = 0; i < dim_; ++i) {
      step_vector_[i] = (max_vector_[i] - min_vector_[i]) / ObIvfConstant::SQ8_META_STEP_SIZE;
    }
  }
  return ret;
}

int ObIvfSq8BuildHelper::get_result(int row_pos, float *&vector)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIvfSq8BuildHelper is not inited", K(ret));
  } else if (row_pos < 0 || row_pos >= ObIvfConstant::SQ8_META_ROW_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row_pos out of range", K(ret), K(row_pos));
  } else if (row_pos == ObIvfConstant::SQ8_META_MIN_IDX) {
    vector = min_vector_;
  } else if (row_pos == ObIvfConstant::SQ8_META_MAX_IDX) {
    vector = max_vector_;
  } else if (row_pos == ObIvfConstant::SQ8_META_STEP_IDX) {
    vector = step_vector_;
  } else {
    // should not be here
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected row pos", K(ret), K(row_pos));
  }
  return ret;
}

int ObIvfSq8BuildHelper::init_kmeans_ctx(const int64_t vec_dim)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ivf_build_mem_ctx_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ivf_build_mem_ctx_ is null", K(ret));
  } else if (OB_ISNULL(min_vector_ = static_cast<float*>(ivf_build_mem_ctx_->Allocate(vec_dim * sizeof(float))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(vec_dim), K(ivf_build_mem_ctx_->get_all_vsag_use_mem_byte()));
  } else if (OB_ISNULL(max_vector_ = static_cast<float*>(ivf_build_mem_ctx_->Allocate(vec_dim * sizeof(float))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(vec_dim), K(ivf_build_mem_ctx_->get_all_vsag_use_mem_byte()));
  } else if (OB_ISNULL(step_vector_ = static_cast<float*>(ivf_build_mem_ctx_->Allocate(vec_dim * sizeof(float))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(vec_dim), K(ivf_build_mem_ctx_->get_all_vsag_use_mem_byte()));
  } else {
    MEMSET(step_vector_, 0, sizeof(float) * vec_dim);
    for (int i = 0; i < vec_dim; ++i) {
      min_vector_[i] = FLT_MAX;
      max_vector_[i] = FLT_MIN;
    }
    dim_ = vec_dim;
    is_inited_ = true;
  }
  first_ret_code_ = ret;
  return ret;
}

// ------------------ ObIvfPqBuildHelper implement ------------------
ObIvfPqBuildHelper::~ObIvfPqBuildHelper()
{
  if (OB_NOT_NULL(executor_) && OB_NOT_NULL(ivf_build_mem_ctx_)) {
    executor_->~ObMultiKmeansExecutor();
    ivf_build_mem_ctx_->Deallocate(executor_);
    executor_ = nullptr;
  }
}

int ObIvfPqBuildHelper::init_kmeans_ctx(const int64_t dim)
{
  int ret = OB_SUCCESS;
  ObKmeansAlgoType algo_type = ObKmeansAlgoType::KAT_ELKAN;
  
  void *buf = nullptr;
  int64_t pqnlist = 0;
  int64_t sample_per_nlist = 0;
  if (OB_NOT_NULL(executor_)) {
    // do nothing
  } else if (0 >= param_.nbits_ || 24 < param_.nbits_ || 0 >= param_.sample_per_nlist_ || 0 >= dim || VIDA_MAX <= param_.dist_algorithm_
            || param_.m_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), K(param_));
  } else if (FALSE_IT(pqnlist = 1L << param_.nbits_)) {
  } else if (OB_ISNULL(ivf_build_mem_ctx_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ivf_build_mem_ctx_ is null", K(ret));
  } else {
    int64_t sample_count = MAX(pqnlist * param_.sample_per_nlist_, param_.nlist_ * param_.sample_per_nlist_);
    sample_per_nlist = sample_count / pqnlist;
    void *tmp_buf = nullptr;
    if (OB_ISNULL(tmp_buf = ivf_build_mem_ctx_->Allocate(sizeof(ObMultiKmeansExecutor)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc tmp_buf", K(ret), K(ivf_build_mem_ctx_->get_all_vsag_use_mem_byte()));
    } else if (OB_FALSE_IT(executor_ = new (tmp_buf) ObMultiKmeansExecutor(*ivf_build_mem_ctx_))) {
    } else if (OB_FAIL(executor_->init(algo_type, 
                                  tenant_id_, 
                                  pqnlist, 
                                  sample_per_nlist,
                                  dim, 
                                  param_.dist_algorithm_, 
                                  nullptr, // pq center kmeans no need normlize, Reference faiss
                                  param_.m_))) {
      LOG_WARN("failed to init kmeans ctx", K(ret), K(param_), K(pqnlist));
    } else {
      is_inited_ = true;
    }
  }
  first_ret_code_ = ret;
  return ret;
}

int ObIvfPqBuildHelper::build(const common::ObTableID &table_id, const common::ObTabletID &tablet_id, ObInsertMonitor* insert_monitor)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctx", K(ret));
  } else if (can_use_parallel()) {
    if (OB_FAIL(executor_->build_parallel(table_id, tablet_id, insert_monitor))) {
      LOG_WARN("failed to build clusters", K(ret));
    }
  } else if (OB_FAIL(executor_->build(insert_monitor))) {
    LOG_WARN("failed to build clusters", K(ret));
  }
  return ret;
}

bool ObIvfPqBuildHelper::can_use_parallel()
{
  int ret = OB_SUCCESS;
  bool res = false;
  int64_t max_thread_cnt = MTL_CPU_COUNT() * ObKmeansBuildTaskHandler::THREAD_FACTOR;
  max_thread_cnt = OB_MAX(max_thread_cnt, ObKmeansBuildTaskHandler::MIN_THREAD_COUNT);
  uint64_t parallel_need_max_mem = 0;
  int64_t vector_free_mem = 0;
  if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor_ is null", KR(ret));
  } else if (OB_FAIL(ObVectorIndexUtil::estimate_ivf_pq_kmeans_memory(executor_->get_max_sample_count(), param_, max_thread_cnt, parallel_need_max_mem))) {
    LOG_WARN("estimate ivf memory failed", KR(ret), K(executor_->get_max_sample_count()), K(param_));
  } else {
    vector_free_mem = get_free_vector_mem_size();
    if (vector_free_mem > parallel_need_max_mem) {
      res = true;
    }
  }
  LOG_INFO("can use parallel", K(res), K(max_thread_cnt), K(parallel_need_max_mem), K(vector_free_mem));
  return res;
}

/**************************** ObKmeansBuildTaskHandler ******************************/
int ObKmeansBuildTaskHandler::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    LOG_INFO("init before", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::VectorTaskPool, tg_id_))) {
    LOG_WARN("TG_CREATE_TENANT failed", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("init vector kmeans build task handler", K(ret), K_(tg_id));
  }
  return ret;
}

int ObKmeansBuildTaskHandler::start()
{
  int ret = OB_SUCCESS;
  int64_t max_thread_cnt = MTL_CPU_COUNT() * THREAD_FACTOR;
  max_thread_cnt = OB_MAX(max_thread_cnt, MIN_THREAD_COUNT);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_THREAD(tg_id_, MIN_THREAD_COUNT,
                                            max_thread_cnt))) {  // must be call TG_SET_ADAPTIVE_THREAD
    LOG_WARN("TG_SET_ADAPTIVE_THREAD failed", KR(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("TG_SET_HANDLER_AND_START failed", KR(ret), K_(tg_id));
  } else {
    max_thread_cnt_ = max_thread_cnt;
    LOG_INFO("succ to start vector kmeans build task handler", K_(tg_id), K(max_thread_cnt));
  }
  return ret;
}

void ObKmeansBuildTaskHandler::stop()
{
  LOG_INFO("vector kmeans build task start to stop", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_STOP(tg_id_);
  }
}

void ObKmeansBuildTaskHandler::wait()
{
  LOG_INFO("vector kmeans build task handler start to wait", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
}

void ObKmeansBuildTaskHandler::destroy()
{
  LOG_INFO("vector kmeans build task handler start to destroy", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  is_inited_ = false;
}

int ObKmeansBuildTaskHandler::get_max_thread_count(int64_t& max_thread_cnt, bool with_refresh /* = false */)
{
  int ret = OB_SUCCESS;
  if (with_refresh) {
    common::ObSpinLockGuard guard(lock_);
    int64_t tmp_max_thread_cnt = MTL_CPU_COUNT() * THREAD_FACTOR;
    tmp_max_thread_cnt = OB_MAX(tmp_max_thread_cnt, MIN_THREAD_COUNT);
    if (tmp_max_thread_cnt == max_thread_cnt_) {
    } else if (OB_FAIL(TG_SET_ADAPTIVE_THREAD(tg_id_, MIN_THREAD_COUNT,
      tmp_max_thread_cnt))) {
      LOG_WARN("TG_SET_ADAPTIVE_THREAD failed", KR(ret), K_(tg_id));
    } else {
      LOG_INFO("succ to set max thread count", KR(ret), K_(tg_id), K(max_thread_cnt_), K(tmp_max_thread_cnt));
      max_thread_cnt_ = tmp_max_thread_cnt;
    }
  }
  max_thread_cnt = max_thread_cnt_;
  return ret;
}

int ObKmeansBuildTaskHandler::push_task(ObKmeansBaseTask &task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  }

  // !!!! inc task ref cnt;
  inc_task_ref();

  bool is_push_succ = false;
  int64_t has_retry_cnt = 0;
  while (OB_SUCC(ret) && !is_push_succ && has_retry_cnt++ <= MAX_RETRY_PUSH_TASK_CNT) {
    if (OB_FAIL(TG_PUSH_TASK(tg_id_, &task))) {
      if (ret != OB_EAGAIN) {
        LOG_WARN("fail to TG_PUSH_TASK", KR(ret), K(task));
      } else {
        // sleep 1s and retry
        LOG_INFO("fail to TG_PUSH_TASK, queue is full will retry", KR(ret), K(task));
        ob_usleep(WAIT_RETRY_PUSH_TASK_TIME);
        ret = OB_SUCCESS;
      }
    } else {
      is_push_succ = true;
    }
  }

  if (!is_push_succ) {
    if (OB_SUCC(ret)) {
      ret = OB_EAGAIN;
    }
    // !!!!! desc task ref cnt
    dec_task_ref();
    LOG_WARN("fail to push task", KR(ret), K(task), K(is_push_succ));
  }
  return ret;
}

void ObKmeansBuildTaskHandler::handle(void *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    ObKmeansBaseTask *base_task = static_cast<ObKmeansBaseTask *>(task);
    if (!base_task->is_finish()) {
      if (OB_FAIL(base_task->do_work())) {
        LOG_WARN("fail to do task", KR(ret));
      }
    }
  }
  // !!!!! desc task ref cnt
  dec_task_ref();
}

void ObKmeansBuildTaskHandler::handle_drop(void *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    // thread has set stop.
    // Use base class pointer to handle task
    ObKmeansBaseTask *base_task = static_cast<ObKmeansBaseTask *>(task);
    if (!base_task->is_finish()) {
      base_task->set_finish(OB_CANCELED);
    }
    // !!!!! desc task ref cnt
    dec_task_ref();
  }
}

/******************************* ObKmeansDistanceCalcTask **********************************/
int ObKmeansDistanceCalcTask::init(int64_t start_idx, int64_t end_idx, 
                                   const ObIArray<float *> *vectors,
                                   float *current_center, float *weight, const int64_t dim)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (start_idx < 0 || end_idx < start_idx || 
             OB_ISNULL(vectors) || OB_ISNULL(current_center) ||
             OB_ISNULL(weight) || dim <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_idx), K(end_idx), 
             KP(vectors), KP(current_center), KP(weight), K(dim));
  } else {
    task_ctx_.vectors_ = vectors;
    task_ctx_.current_center_ = current_center;
    task_ctx_.weight_ = weight;
    task_ctx_.start_idx_ = start_idx;
    task_ctx_.end_idx_ = end_idx;
    task_ctx_.sum_ = 0.0f;
    task_ctx_.dim_ = dim;
    base_ctx_.init();
    is_inited_ = true;
  }
  return ret;
}

void ObKmeansDistanceCalcTask::reset()
{
  ObKmeansBaseTask::reset();
  // update ctx
  task_ctx_.reset();
}

int ObKmeansDistanceCalcTask::do_work()
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || is_stop()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init or stop", KR(ret), K_(is_stop));
  } else if (OB_FALSE_IT(base_ctx_.gmt_modified_ = ObTimeUtility::current_time())) {
  } else if (OB_FAIL(ObKmeansAlgo::calc_distances_range(*task_ctx_.vectors_, task_ctx_.start_idx_, task_ctx_.end_idx_,
                                                        task_ctx_.current_center_, task_ctx_.weight_, task_ctx_.dim_,
                                                        task_ctx_.sum_))) {
    LOG_WARN("failed to calc distances range", K(ret));
  }

  // update ctx
  set_finish(ret);
  return ret;
}

/******************************* ObKmeansBuildTask **********************************/
int ObKmeansBuildTask::init(const common::ObTableID &table_id, const common::ObTabletID &tablet_id, int m_idx,
                            ObKmeansAlgo *algo, const ObIArray<float *> *vectors, ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (m_idx < 0 || OB_ISNULL(algo) || OB_ISNULL(vectors)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(m_idx), KP(algo), KP(vectors));
  } else {

    algo_ = algo;
    task_ctx_.vectors_ = vectors;
    task_ctx_.table_id_ = table_id;
    task_ctx_.tablet_id_ = tablet_id;
    task_ctx_.m_idx_ = m_idx;
    task_ctx_.insert_monitor_ = insert_monitor;
    base_ctx_.init();
    is_inited_ = true;
  }

  return ret;
}

int ObKmeansBuildTask::do_work()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || is_stop()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init or stop", KR(ret), K_(is_stop));
  } else if (OB_FALSE_IT(base_ctx_.gmt_modified_ = ObTimeUtility::current_time())) {
  } else if (OB_FAIL(algo_->build(*task_ctx_.vectors_))) {
    LOG_WARN("fail to build", KR(ret), K_(task_ctx), KP_(algo));
  }
  if (OB_NOT_NULL(algo_)) {
      algo_->destroy();
  }
  // update ctx
  base_ctx_.finish(ret);
  if (OB_NOT_NULL(task_ctx_.insert_monitor_) && OB_NOT_NULL(task_ctx_.insert_monitor_->kmeans_monitor_.vec_index_task_finish_cnt_)) {
    (void)ATOMIC_AAF(task_ctx_.insert_monitor_->kmeans_monitor_.vec_index_task_finish_cnt_, 1);
  }
  return ret;
}

/******************************* ObKmeansAssignTask **********************************/
int ObKmeansAssignTask::init(int64_t start_idx, int64_t end_idx, ObElkanKmeansAlgo *algo,
                             const ObIArray<float *> *input_vectors, float *centers_distance,
                             int32_t *data_cnt_in_cluster)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(algo) || OB_ISNULL(input_vectors) || OB_ISNULL(centers_distance) || OB_ISNULL(data_cnt_in_cluster)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(algo), KP(input_vectors), KP(centers_distance),
              KP(data_cnt_in_cluster));
  } else {
    task_ctx_.start_idx_ = start_idx;
    task_ctx_.end_idx_ = end_idx;
    task_ctx_.input_vectors_ = input_vectors;
    task_ctx_.centers_distance_ = centers_distance;
    task_ctx_.data_cnt_in_cluster_ = data_cnt_in_cluster;
    task_ctx_.dis_obj_ = 0.0f;
    base_ctx_.init();
    algo_ = algo;
    is_inited_ = true;
  }
  return ret;
}

void ObKmeansAssignTask::reset()
{
  ObKmeansBaseTask::reset();
  algo_ = nullptr;
  task_ctx_.reset();
}

int ObKmeansAssignTask::do_work()
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || is_stop()) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init or stop", KR(ret), K_(is_stop));
  } else {
    if (OB_FAIL(algo_->assign_vectors_range(*task_ctx_.input_vectors_, task_ctx_.start_idx_, task_ctx_.end_idx_,
                                            task_ctx_.centers_distance_, task_ctx_.data_cnt_in_cluster_,
                                            task_ctx_.dis_obj_, true))) {
      SHARE_LOG(WARN, "failed to assign vectors range", K(ret));
    }
  }

  // update ctx
  set_finish(ret);
  return ret;
}
// ------------------ ObKmeansBaseTaskCtx implement ------------------
void ObKmeansBaseTaskCtx::init()
{
  gmt_create_ = ObTimeUtility::current_time();
  gmt_modified_ = ObTimeUtility::current_time();
  is_finish_ = false;
  ret_code_ = OB_SUCCESS;
}

void ObKmeansBaseTaskCtx::finish(int ret_code)
{
  ATOMIC_STORE(&is_finish_, true);
  ret_code_ = ret_code;
  gmt_modified_ = ObTimeUtility::current_time();
}

} // end namespace share
} // end namespace oceanbase
