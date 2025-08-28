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
  tmp_allocator_.reset();
}

int ObKmeansAlgo::init(ObKmeansCtx &kmeans_ctx)
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
    tmp_allocator_.set_attr(ObMemAttr(tenant_id, "KmeansCtxTmp"));
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
    while (OB_SUCC(ret) && !is_finish()) {
      if (OB_FAIL(inner_build(input_vectors))) {
        SHARE_LOG(WARN, "failed to do kmeans", K(ret));
      } else if (check_stop()) {
        ret = OB_CANCELED;
        SHARE_LOG(INFO, "kmeans ctx is fore stop", K(ret), K(*this));
      }
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

// ------------------ ObKmeansExecutor implement ------------------
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
  if (OB_FAIL(ctx_.init(tenant_id, lists, samples_per_nlist, dim, dist_algo, norm_info, 1/*pq_m*/))) {
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
  
  if (FAILEDx(algo_->init(ctx_))) {
    LOG_WARN("fail to init kmeans algo", K(ret), K(ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSingleKmeansExecutor::build()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "kmeans ctx is not inited", K(ret));
  } else if (OB_FAIL(ctx_.try_normalize_samples())) {
    LOG_WARN("fail to try normalize all samples", K(ret), K(ctx_));
  } else if (OB_FAIL(algo_->build(ctx_.sample_vectors_))) {
    LOG_WARN("fail to build kmeans algo", K(ret), K(algo_));
  }
  if (OB_NOT_NULL(algo_)) {
    algo_->destroy();
  }
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
    ObArenaAllocator tmp_alloc;
    // init spilited_arrs, size: m * sample_vectors_.count()
    ObArrayArray<float*> splited_arrs(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(tmp_alloc, "MulKmeans"));
    if (OB_FAIL(prepare_splited_arrs(splited_arrs))) {
      LOG_WARN("fail to prepare splited_arrs", K(ret));
    } else if (OB_NOT_NULL(insert_monitor)) {
      if (OB_NOT_NULL(insert_monitor->vec_index_task_total_cnt_)) {
        (void)ATOMIC_AAF(insert_monitor->vec_index_task_total_cnt_, pq_m_size_);
      }
      if (OB_NOT_NULL(insert_monitor->vec_index_task_thread_pool_cnt_)) {
        (void)ATOMIC_SET(insert_monitor->vec_index_task_thread_pool_cnt_, 1);
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < pq_m_size_; ++i) {
      if (i >= algos_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("size of algos_ should be pq_m_size_", K(ret), K(i), K(pq_m_size_), K(algos_.count()));
      } else if (OB_FAIL(algos_[i]->build(splited_arrs.at(i)))) {
        LOG_WARN("fail to build kmeans algo", K(ret), K(i), K(algos_[i]));
      } else if (OB_NOT_NULL(insert_monitor) && OB_NOT_NULL(insert_monitor->vec_index_task_finish_cnt_)) {
        (void)ATOMIC_AAF(insert_monitor->vec_index_task_finish_cnt_, 1);
      }
      if (OB_NOT_NULL(algos_[i])) {
        algos_[i]->destroy();
      }
    }
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

  common::ObSpinLockGuard init_guard(handle.lock_);                     // lock thread pool init to avoid init twice
  if (handle.get_tg_id() != ObKmeansBuildTaskHandler::INVALID_TG_ID) {  // no need to init twice, skip
  } else if (OB_FAIL(handle.init())) {
    LOG_WARN("fail to init vector kmeans build task handle", K(ret));
  } else if (OB_FAIL(handle.start())) {
    LOG_WARN("fail to start vector kmeans build thread pool", K(ret));
  }

  return ret;
}

void ObMultiKmeansExecutor::wait_kmeans_task_finish(ObKmeansBuildTask *build_tasks, ObKmeansBuildTaskHandler &handle,
                                                    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  int64_t last_finish_count = 0;
  bool is_all_finish = false;
  if (OB_NOT_NULL(build_tasks)) {
    if (OB_NOT_NULL(insert_monitor) && OB_NOT_NULL(insert_monitor->vec_index_task_total_cnt_)) {
      (void)ATOMIC_AAF(insert_monitor->vec_index_task_total_cnt_, pq_m_size_);
    }
    while (!is_all_finish && handle.get_task_ref() > 0) {
      ob_usleep(ObKmeansBuildTaskHandler::WAIT_RETRY_PUSH_TASK_TIME);
      is_all_finish = true;
      int64_t now_finish_count = 0;
      for (int i = 0; i < pq_m_size_; i++) {
        if (build_tasks[i].is_finish()) {
          now_finish_count++;
        }
      }
      is_all_finish = pq_m_size_ == now_finish_count;
      if (last_finish_count != now_finish_count && OB_NOT_NULL(insert_monitor) &&
          OB_NOT_NULL(insert_monitor->vec_index_task_finish_cnt_)) {
        (void)ATOMIC_AAF(insert_monitor->vec_index_task_finish_cnt_, now_finish_count - last_finish_count);
      }
      if (OB_NOT_NULL(insert_monitor) && OB_NOT_NULL(insert_monitor->vec_index_task_thread_pool_cnt_)) {
        (void)ATOMIC_SET(insert_monitor->vec_index_task_thread_pool_cnt_, handle.get_thread_cnt());
      }
      last_finish_count = now_finish_count;
    }
  }
}

int ObMultiKmeansExecutor::build_parallel(const common::ObTableID &table_id, const common::ObTabletID &tablet_id, ObInsertMonitor* insert_monitor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("kmeans ctx is not inited", K(ret));
  } else {
    LOG_INFO("start build_parallel", K(table_id), K(tablet_id), K(ctx_));
    ObArenaAllocator tmp_alloc;
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

        if (OB_FAIL(init_build_handle(handle))) {
          LOG_WARN("fail to init build handle", K(ret));
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
        }
        for (int i = 0; i < pq_m_size_ && OB_SUCC(ret); i++) {
          ObKmeansBuildTask &build_task = build_tasks[i];
          if (OB_FAIL(build_task.init(table_id, tablet_id, i, algos_[i], &splited_arrs.at(i)))) {  // 1. make task
            LOG_WARN("fail to init opt async task", KR(ret));
          } else if (OB_FAIL(handle.push_task(build_task))) {  // 2. push task
            LOG_WARN("fail to push task", K(ret));
          }
        }
        // Note. If the previous process fails, all tasks need to be stopped otherwise it may cause a core dump because
        // splited_arrs will be released.
        if (OB_FAIL(ret) && OB_NOT_NULL(build_tasks)) {
          for (int i = 0; i < pq_m_size_; i++) {
            ObKmeansBuildTask &build_task = build_tasks[i];
            build_task.set_task_stop();
          }
        }
        // 3. wait for all task finish
        wait_kmeans_task_finish(build_tasks, handle, insert_monitor);

        // 4. check task result
        for (int i = 0; i < pq_m_size_ && OB_SUCC(ret); i++) {
          if (OB_UNLIKELY(OB_ISNULL(build_tasks))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(ret));
          } else if (OB_FAIL(build_tasks[i].get_ret())) {
            LOG_WARN("fail to build kmeans algo", K(ret), K(i), K(build_tasks[i]));
          }
        }  // end for
      }
    }
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
  if (lower_bounds_.count() > 0) {
    ivf_build_mem_ctx_.Deallocate(lower_bounds_.at(0));
  }
  lower_bounds_.reset();
  if (OB_NOT_NULL(upper_bounds_)) {
    ivf_build_mem_ctx_.Deallocate(upper_bounds_);
    upper_bounds_ = nullptr;
  }
  if (OB_NOT_NULL(nearest_centers_)) {
    ivf_build_mem_ctx_.Deallocate(nearest_centers_);
    nearest_centers_ = nullptr;
  }
  ObKmeansAlgo::destroy();
}

int ObElkanKmeansAlgo::calc_kmeans_distance(const float* a, const float* b, const int64_t len, float &distance)
{
  int ret = OB_SUCCESS;
  // only use l2_distance
  distance = ObVectorL2Distance<float>::l2_square_flt_func(a, b, len);
  return ret;
}

int ObElkanKmeansAlgo::inner_build(const ObIArray<float*> &input_vectors)
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

int ObElkanKmeansAlgo::init_first_center(const ObIArray<float*> &input_vectors)
{
  int ret = OB_SUCCESS;
  if (PREPARE_CENTERS != status_) {
    ret = OB_STATE_NOT_MATCH;
    SHARE_LOG(WARN, "status not match", K(ret), K(status_));
  } else if (OB_FAIL(centers_[0].init(kmeans_ctx_->dim_, kmeans_ctx_->lists_, ivf_build_mem_ctx_))) {
    SHARE_LOG(WARN, "failed to init center buffer", K(ret));
  } else if (OB_FAIL(centers_[1].init(kmeans_ctx_->dim_, kmeans_ctx_->lists_, ivf_build_mem_ctx_))) {
    SHARE_LOG(WARN, "failed to init center buffer", K(ret));
  } else {
    const int64_t sample_cnt = input_vectors.count();
    int64_t random = 0;
    random = ObRandom::rand(0, sample_cnt - 1);
    // use random sample vector as the first center
    if (OB_FAIL(centers_[cur_idx_].push_back(kmeans_ctx_->dim_, input_vectors.at(random)))) {
      SHARE_LOG(WARN, "failed to push back center", K(ret));
    } else if (OB_ISNULL(nearest_centers_ = static_cast<int32_t *>(
        ivf_build_mem_ctx_.Allocate(sizeof(int32_t) * sample_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
    } else if (OB_ISNULL(upper_bounds_ = static_cast<float *>(
        ivf_build_mem_ctx_.Allocate(sizeof(float) * sample_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
    } else if (OB_ISNULL(weight_ = static_cast<float *>(
        ivf_build_mem_ctx_.Allocate(sizeof(float) * sample_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
    } else if (OB_FAIL(lower_bounds_.allocate_array(tmp_allocator_, sample_cnt))) {
      LOG_WARN("failed to allocate array", K(ret));
    } else {
      MEMSET(lower_bounds_.get_data(), 0, sizeof(float *) * sample_cnt);
      float *bounds = nullptr;
      if (OB_ISNULL(bounds = static_cast<float *>(ivf_build_mem_ctx_.Allocate(sizeof(float) * kmeans_ctx_->lists_ * sample_cnt)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
      } else {
        MEMSET(bounds, 0, sizeof(float) * kmeans_ctx_->lists_ * sample_cnt);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < sample_cnt; ++i) {
        lower_bounds_.at(i) = bounds + i * kmeans_ctx_->lists_;
      }
      if (OB_SUCC(ret)) {
        MEMSET(nearest_centers_, 0, sizeof(int32_t) * sample_cnt);
        MEMSET(upper_bounds_, 0, sizeof(float) * sample_cnt);
        for (int64_t i = 0; i < sample_cnt; ++i) {
          weight_[i] = FLT_MAX;
        }
        status_ = INIT_CENTERS;
        SHARE_LOG(TRACE, "success to init first center", K(ret));
      }
    }
  }
  return ret;
}

// Kmeans++
int ObElkanKmeansAlgo::init_centers(const ObIArray<float*> &input_vectors)
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
    float random_weight = 0;

    const int64_t sample_cnt = input_vectors.count();
    int64_t i = 0;
    for (i = 0; OB_SUCC(ret) && i < sample_cnt; ++i) {
      float* sample_vector = input_vectors.at(i);
      if (OB_FAIL(calc_kmeans_distance(sample_vector, current_center, kmeans_ctx_->dim_, distance))) {
        SHARE_LOG(WARN, "failed to calc kmeans distance", K(ret));
      } else {
        lower_bounds_.at(i)[center_idx] = distance;
        // is_finish means no need to add new center, only update lower_bounds_
        if (!is_finish) { 
          distance *= distance;
          if (distance < weight_[i]) {
            weight_[i] = distance;
          }
          sum += weight_[i];
        }
      }
    }
    
    if (OB_SUCC(ret)) {
      if (is_finish) {
        float min_distance;
        int64_t nearest_center_idx;
        for (i = 0; i < sample_cnt; ++i) {
          min_distance = FLT_MAX;
          nearest_center_idx = 0;
          for (int64_t j = 0; j < kmeans_ctx_->lists_; ++j) {
            distance = lower_bounds_.at(i)[j];
            if (distance < min_distance) {
              min_distance = distance;
              nearest_center_idx = j;
            }
          }
          upper_bounds_[i] = min_distance;
          nearest_centers_[i] = nearest_center_idx;
        }
        status_ = RUNNING_KMEANS;

        const int64_t center_count = centers_[cur_idx_].count();
        const int64_t sample_count = input_vectors.count();
        SHARE_LOG(INFO, "success to init all centers", K(ret), K(center_count), K(kmeans_ctx_->lists_), K(sample_count));
      } else {
        // get the next center randomly
        random_weight = (float)ObRandom::rand(1, 100) / 100.0 * sum;
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
          SHARE_LOG(TRACE, "success to init center", K(ret), K(center_count));
        }
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
    // tmp variables
    common::ObArrayWrap<float*> half_centers_distance; // half the distance between each two centers
    float *half_center_min_distance = nullptr; // the min distance from each center to other centers
    float *center_distance_diff = nullptr; // the distance before and after each center update
    int32_t *data_cnt_in_cluster = nullptr; // the number of vectors contained in each center (cluster)
    // init tmp variables
    if (OB_FAIL(half_centers_distance.allocate_array(tmp_allocator_, kmeans_ctx_->lists_))) {
      LOG_WARN("failed to allocate array", K(ret));
    } else {
      MEMSET(half_centers_distance.get_data(), 0, sizeof(float *) * kmeans_ctx_->lists_);
      float *tmp = nullptr;
      if (OB_ISNULL(tmp = static_cast<float*>(ivf_build_mem_ctx_.Allocate(sizeof(float) * kmeans_ctx_->lists_ * kmeans_ctx_->lists_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
      } else {
        MEMSET(tmp, 0, sizeof(float) * kmeans_ctx_->lists_ * kmeans_ctx_->lists_);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < kmeans_ctx_->lists_; ++i) {
        half_centers_distance.at(i) = tmp + i * kmeans_ctx_->lists_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(half_center_min_distance =
        static_cast<float*>(ivf_build_mem_ctx_.Allocate(sizeof(float) * kmeans_ctx_->lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
    } else if (OB_ISNULL(center_distance_diff =
        static_cast<float*>(ivf_build_mem_ctx_.Allocate(sizeof(float) * kmeans_ctx_->lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
    } else if (OB_ISNULL(data_cnt_in_cluster =
        static_cast<int32_t*>(ivf_build_mem_ctx_.Allocate(sizeof(int32_t) * kmeans_ctx_->lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(ivf_build_mem_ctx_.get_all_vsag_use_mem_byte()));
    } else {
      MEMSET(half_center_min_distance, 0, sizeof(float) * kmeans_ctx_->lists_);
      MEMSET(center_distance_diff, 0, sizeof(float) * kmeans_ctx_->lists_);
      MEMSET(data_cnt_in_cluster, 0, sizeof(int32_t) * kmeans_ctx_->lists_);
    }

    float distance = FLT_MAX;
    const int64_t sample_cnt = input_vectors.count();
    // 500 iterations
    for (int64_t iter = 0; OB_SUCC(ret) && iter < 500; ++iter) {
      int32_t changes = 0;
      // 1. calc distance between each two centers
      for (int64_t i = 0; OB_SUCC(ret) && i < kmeans_ctx_->lists_; ++i) {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < kmeans_ctx_->lists_; ++j) {
          if (OB_FAIL(calc_kmeans_distance(centers_[cur_idx_].at(i), 
              centers_[cur_idx_].at(j), kmeans_ctx_->dim_, distance))) {
            SHARE_LOG(WARN, "failed to calc kmeans distance between centers", K(ret));
          } else {
            distance = distance / 2.0;
            half_centers_distance.at(i)[j] = distance;
            half_centers_distance.at(j)[i] = distance;
          }
        }
      }
      // 2. calc the nearest distance between the center and other centers
      for (int64_t i = 0; OB_SUCC(ret) && i < kmeans_ctx_->lists_; ++i) {
        half_center_min_distance[i] = FLT_MAX;
        for (int64_t j = 0; OB_SUCC(ret) && j < kmeans_ctx_->lists_; ++j) {
          if (i != j) {
            distance = half_centers_distance.at(i)[j];
            if (distance < half_center_min_distance[i]) {
              half_center_min_distance[i] = distance;
            }
          }
        }
      }
      // 3. calc new clusters
      bool need_recalc = iter != 0;
      float min_distance = FLT_MAX;
      centers_[next_idx()].clear();
      MEMSET(data_cnt_in_cluster, 0, sizeof(int32_t) * kmeans_ctx_->lists_);
      for (int64_t i = 0; OB_SUCC(ret) && i < sample_cnt; ++i) {
        bool recalc = need_recalc;
        float* sample_vector = input_vectors.at(i);
        // 3.1. if D(x, c1) <= 0.5 * D(c1, ci), then D(x, c1) < D(x, ci)
        // c1 is the nearest_center, ci is the closest center to c1
        // so all other centers satisfy this condition
        if (upper_bounds_[i] <= half_center_min_distance[nearest_centers_[i]]) {
          // do nothing
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < kmeans_ctx_->lists_; ++j) {
            if (j == nearest_centers_[i]) {
              // 3.2. do nothing
            } else if (upper_bounds_[i] <= lower_bounds_.at(i)[j]) {
              // 3.3. if D(x, c1) <= min D(x, c2)
              // do nothing
            } else if (upper_bounds_[i] <= half_centers_distance.at(nearest_centers_[i])[j]) {
              // 3.4. if D(x, c1) <= 0.5 * D(c1, c2), then D(x, c1) < D(x, c2)
              // c1 is closer, do nothing
            } else {
              
              // all variables describe the relationship between data and clusters,
              // but the actual cluster center is updated in each iteration,
              // we need to calc the real distance
              if (recalc) {
                if (OB_FAIL(calc_kmeans_distance(sample_vector,
                    centers_[cur_idx_].at(nearest_centers_[i]), kmeans_ctx_->dim_, min_distance))) {
                  SHARE_LOG(WARN, "failed to calc kmeans distance", K(ret));
                } else {
                  lower_bounds_.at(i)[nearest_centers_[i]] = min_distance;
                  upper_bounds_[i] = min_distance;
                  recalc = false;
                }
              } else {
                min_distance = upper_bounds_[i];
              }
              if (OB_SUCC(ret)) {
                // 3.5 calc the distance between sample_vector and center
                if (min_distance > lower_bounds_.at(i)[j]
                    || min_distance > half_centers_distance.at(nearest_centers_[i])[j]) {
                  if (OB_FAIL(calc_kmeans_distance(sample_vector,
                      centers_[cur_idx_].at(j), kmeans_ctx_->dim_, distance))) {
                    SHARE_LOG(WARN, "failed to calc kmeans distance", K(ret));
                  } else {
                    lower_bounds_.at(i)[j] = distance;
                    if (distance < min_distance) {
                      nearest_centers_[i] = j;
                      upper_bounds_[i] = distance;
                      ++changes;
                    }
                  }
                }
              }
            }
          } // end centers for
        }
        // 3.6. add each sample vector to its cluster for sum
        if (OB_SUCC(ret)) {
          if (OB_FAIL(centers_[next_idx()].add(nearest_centers_[i], kmeans_ctx_->dim_, sample_vector))) {
            SHARE_LOG(WARN, "failed to add vector to center buffer", K(ret));
          } else {
            ++data_cnt_in_cluster[nearest_centers_[i]];
          }
        }
      } // end sample for
      // 4. calc the new centers
      MEMSET(center_distance_diff, 0, sizeof(float) * kmeans_ctx_->lists_);
      for (int64_t i = 0; OB_SUCC(ret) && i < kmeans_ctx_->lists_; ++i) {
        if (data_cnt_in_cluster[i] > 0) {
          if (OB_FAIL(centers_[next_idx()].divide(i, data_cnt_in_cluster[i]))) {
            SHARE_LOG(WARN, "failed to divide vector", K(ret));
          }
        } else {
          // use random sample vector as the center
          int64_t random = 0;
          random = ObRandom::rand(0, sample_cnt - 1);
          if (OB_FAIL(centers_[next_idx()].add(i, kmeans_ctx_->dim_, input_vectors.at(random)))) {
            SHARE_LOG(WARN, "failed to add vector", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(kmeans_ctx_->try_normalize(
              kmeans_ctx_->dim_,
              centers_[next_idx()].at(i), 
              centers_[next_idx()].at(i)))) {
            LOG_WARN("failed to normalize vector", K(ret));
          } else if (OB_FAIL(calc_kmeans_distance(centers_[next_idx()].at(i), centers_[cur_idx_].at(i),
              kmeans_ctx_->dim_, center_distance_diff[i]))) {
            SHARE_LOG(WARN, "failed to calc kmeans distance", K(ret));
          }
        }
      } // end for
      // 5. adjust ub & lb by center_distance_diff
      for (int64_t i = 0; OB_SUCC(ret) && i < sample_cnt; ++i) {
        upper_bounds_[i] += center_distance_diff[nearest_centers_[i]];
        for (int64_t j = 0; OB_SUCC(ret) && j < kmeans_ctx_->lists_; ++j) {
          distance = lower_bounds_.at(i)[j] - center_distance_diff[j];
          distance = distance < 0 ? 0 : distance;
          lower_bounds_.at(i)[j] = distance;
        }
      }
      // 6. check finish && switch center buffer
      if (OB_SUCC(ret)) {
        if (changes == 0 && iter != 0) {
          LOG_INFO("finish do kmeans before all iters", K(ret), K(iter));
          break; // finish
        } else {
          cur_idx_ = next_idx();
        }
      }
    } // iter end for
    // free tmp memory
    if (half_centers_distance.count() > 0) {
      ivf_build_mem_ctx_.Deallocate(half_centers_distance.at(0));
      half_centers_distance.reset();
    }
    if (OB_NOT_NULL(half_center_min_distance)) {
      ivf_build_mem_ctx_.Deallocate(half_center_min_distance);
      half_center_min_distance = nullptr;
    }
    if (OB_NOT_NULL(center_distance_diff)) {
      ivf_build_mem_ctx_.Deallocate(center_distance_diff);
      center_distance_diff = nullptr;
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
  }
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
  lib::ObMutexGuard guard(lock_);
  void *buf = nullptr;
  ObVectorNormalizeInfo *norm_info = nullptr;
  if (first_ret_code_ != OB_SUCCESS) {
    ret = first_ret_code_;
    SHARE_LOG(WARN, "init falied before", K(ret), K(dim), K(param_));
  } else if (OB_NOT_NULL(executor_)) {
    // do nothing
  } else if (0 >= param_.nlist_ || 0 >= param_.sample_per_nlist_ || 0 >= dim || VIDA_MAX <= param_.dist_algorithm_) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), K(param_));
  } else if ((VIDA_IP == param_.dist_algorithm_ || VIDA_COS == param_.dist_algorithm_) && 
              FALSE_IT(norm_info = &norm_info_)) { // IP和COS算法需要归一化
  } else if (OB_ISNULL(ivf_build_mem_ctx_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "ivf_build_mem_ctx_ is null", K(ret));
  } else {
    void *tmp_buf = nullptr;
    if (OB_ISNULL(tmp_buf = ivf_build_mem_ctx_->Allocate(sizeof(ObSingleKmeansExecutor)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc tmp_buf", K(ret), K(ivf_build_mem_ctx_->get_all_vsag_use_mem_byte()));
    } else if (OB_FALSE_IT(executor_ = new (tmp_buf) ObSingleKmeansExecutor(*ivf_build_mem_ctx_))) {
    } else if (OB_FAIL(executor_->init(ObKmeansAlgoType::KAT_ELKAN, tenant_id_, param_.nlist_,
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

int ObIvfSq8BuildHelper::init_result_vectors(int64_t vec_dim)
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

int ObIvfPqBuildHelper::init_ctx(const int64_t dim)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  void *buf = nullptr;

  int64_t pqnlist = 0;
  if (first_ret_code_ != OB_SUCCESS) {
    ret = first_ret_code_;
    SHARE_LOG(WARN, "init falied before", K(ret), K(dim), K(param_));
  } else if (OB_NOT_NULL(executor_)) {
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
    void *tmp_buf = nullptr;
    if (OB_ISNULL(tmp_buf = ivf_build_mem_ctx_->Allocate(sizeof(ObMultiKmeansExecutor)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc tmp_buf", K(ret), K(ivf_build_mem_ctx_->get_all_vsag_use_mem_byte()));
    } else if (OB_FALSE_IT(executor_ = new (tmp_buf) ObMultiKmeansExecutor(*ivf_build_mem_ctx_))) {
    } else if (OB_FAIL(executor_->init(ObKmeansAlgoType::KAT_ELKAN, 
                                  tenant_id_, 
                                  pqnlist, 
                                  param_.sample_per_nlist_, 
                                  dim, 
                                  param_.dist_algorithm_, 
                                  nullptr, // pq center kmenas no need normlize, Reference faiss
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
  uint64_t construct_mem = 0;
  uint64_t buff_mem = 0;
  int64_t parallel_need_max_mem = 0;
  int64_t vector_free_mem = 0;
  if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor_ is null", KR(ret));
  } else if (OB_FAIL(ObVectorIndexUtil::estimate_ivf_memory(executor_->get_max_sample_count(), param_, construct_mem, buff_mem))) {
    LOG_WARN("estimate ivf memory failed", KR(ret), K(executor_->get_max_sample_count()), K(param_));
  } else {
    parallel_need_max_mem = max_thread_cnt * construct_mem;
    vector_free_mem = get_free_vector_mem_size();
    if (vector_free_mem > parallel_need_max_mem) {
      res = true;
    }
  }
  LOG_INFO("can use parallel", K(res), K(max_thread_cnt), K(construct_mem), K(parallel_need_max_mem), K(vector_free_mem));
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

int ObKmeansBuildTaskHandler::push_task(ObKmeansBuildTask &build_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  }

  bool is_push_succ = false;
  int64_t has_retry_cnt = 0;
  while (OB_SUCC(ret) && !is_push_succ && has_retry_cnt++ <= MAX_RETRY_PUSH_TASK_CNT) {
    if (OB_FAIL(TG_PUSH_TASK(tg_id_, &build_task))) {
      if (ret != OB_EAGAIN) {
        LOG_WARN("fail to TG_PUSH_TASK", KR(ret), K(build_task));
      } else {
        // sleep 1s and retry
        LOG_DEBUG("fail to TG_PUSH_TASK, queue is full will retry", KR(ret), K(build_task));
        ob_usleep(WAIT_RETRY_PUSH_TASK_TIME);
        ret = OB_SUCCESS;
      }
    } else {
      is_push_succ = true;
    }
  }

  if (OB_FAIL(ret) || !is_push_succ) {
    LOG_WARN("fail to push task", KR(ret), K(build_task), K(is_push_succ));
  } else {
    // !!!! inc task ref cnt;
    inc_task_ref();
  }
  return ret;
}

void ObKmeansBuildTaskHandler::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObKmeansBuildTask *build_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    build_task = static_cast<ObKmeansBuildTask *>(task);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_task->do_work())) {
      LOG_WARN("fail to do task", KR(ret), KPC(build_task));
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
    ObKmeansBuildTask *build_task = nullptr;
    build_task = static_cast<ObKmeansBuildTask *>(task);
    build_task->~ObKmeansBuildTask();
    build_task = nullptr;
    // !!!!! desc task ref cnt
    dec_task_ref();
  }
}

/******************************* ObKmeansBuildTask **********************************/
int ObKmeansBuildTask::init(const common::ObTableID &table_id, const common::ObTabletID &tablet_id, int m_idx,
                            ObKmeansAlgo *algo, const ObIArray<float *> *vectors)
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
    vectors_ = vectors;
    task_ctx_.table_id_ = table_id;
    task_ctx_.tablet_id_ = tablet_id;
    task_ctx_.gmt_create_ = ObTimeUtility::current_time();
    task_ctx_.is_finish_ = false;
    task_ctx_.m_idx_ = m_idx;
    task_ctx_.ret_code_ = OB_SUCCESS;
    is_inited_ = true;
  }

  return ret;
}

void ObKmeansBuildTask::reset()
{

  is_inited_ = false;
  algo_ = nullptr;
  vectors_ = nullptr;
  // update ctx
  task_ctx_.reset();
}

int ObKmeansBuildTask::do_work()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FALSE_IT(task_ctx_.gmt_modified_ = ObTimeUtility::current_time())) {
  } else if (OB_FAIL(algo_->build(*vectors_))) {
    LOG_WARN("fail to build", KR(ret), K_(task_ctx), KP_(algo), KP_(vectors));
  }
  if (OB_NOT_NULL(algo_)) {
      algo_->destroy();
  }
  // update ctx
  task_ctx_.is_finish_ = true;
  task_ctx_.ret_code_ = ret;
  task_ctx_.gmt_modified_ = ObTimeUtility::current_time();
  return ret;
}
}
}
