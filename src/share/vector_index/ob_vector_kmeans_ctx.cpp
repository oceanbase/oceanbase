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

namespace oceanbase {
using namespace common;
namespace share {
// ------------------ ObKmeansCtx implement ------------------
void ObKmeansCtx::destroy()
{
  allocator_.reset();
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
  if (OB_NOT_NULL(norm_info_)) {
    if (OB_FAIL(norm_info_->normalize_func_(dim, data, norm_vector))) {
      LOG_WARN("failed to normalize vector", K(ret));
    }
  }
  return ret;
}

int ObKmeansCtx::try_normalize_samples() const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(norm_info_)) {
    for (int i = 0; OB_SUCC(ret) && i < sample_vectors_.count(); ++i) {
      if (OB_FAIL(norm_info_->normalize_func_(sample_dim_, sample_vectors_[i], sample_vectors_[i]))) {
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
      if (OB_ISNULL(save_vector = static_cast<float*>(allocator_.alloc(sizeof(float) * sample_dim_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "failed to alloc vector", K(ret));
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
    allocator_ = &kmeans_ctx.allocator_;
    if (OB_FAIL(centers_[0].init(dim, lists, *allocator_))) {
      SHARE_LOG(WARN, "failed to init center buffer", K(ret));
    } else if (OB_FAIL(centers_[1].init(dim, lists, *allocator_))) {
      SHARE_LOG(WARN, "failed to init center buffer", K(ret));
    } else {
      kmeans_ctx_ = &kmeans_ctx;
      tmp_allocator_.set_attr(ObMemAttr(tenant_id, "KmeansCtxTmp"));
      is_inited_ = true;
    }
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
      if (OB_ISNULL(algo_ = OB_NEWx(ObElkanKmeansAlgo, &ctx_.allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fial to alloc memory for ObElkanKmeansAlgo", K(ret));
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

float * ObSingleKmeansExecutor::get_center(const int64_t pos)
{
  float *p_ret = nullptr;
  if (pos < get_centers_count() && pos >= 0) {
    p_ret = algo_->get_cur_centers().at(pos);
  }
  return p_ret;
}

// ------------------ ObMultiKmeansExecutor implement ------------------
ObMultiKmeansExecutor::~ObMultiKmeansExecutor()
{
  for (int i = 0; i < algos_.count(); ++i) {
    if (OB_NOT_NULL(algos_[i])) {
      algos_[i]->~ObKmeansAlgo();
      algos_[i] = nullptr;
    }
  }
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
        if (OB_ISNULL(algos_[i] = OB_NEWx(ObElkanKmeansAlgo, &ctx_.allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fial to alloc memory for ObElkanKmeansAlgo", K(ret));
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

int ObMultiKmeansExecutor::build()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "kmeans ctx is not inited", K(ret));
  } else {
    ObArenaAllocator tmp_alloc;
    // init spilited_arrs, size: m * sample_vectors_.count()
    ObArrayArray<float*> splited_arrs(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(tmp_alloc, "MulKmeans"));
    const ObIArray<float*> &sample_arr = ctx_.sample_vectors_;
    if (OB_FAIL(splited_arrs.reserve(pq_m_size_))) {
      LOG_WARN("fail to prepare alloc space", K(ret), K(pq_m_size_));
    }
    for (int i = 0; OB_SUCC(ret) && i < pq_m_size_; ++i) {
      if (OB_FAIL(splited_arrs.push_back(ObArray<float*>()))) {
        LOG_WARN("fail to push back empty array", K(ret), K(i), K(pq_m_size_), K(splited_arrs.count()));
      } else if (OB_FAIL(splited_arrs.at(i).reserve(sample_arr.count()))) {
        LOG_WARN("fail to reserve space", K(ret), K(i), K(pq_m_size_), K(splited_arrs.count()));
      }
    }

    for (int i = 0; OB_SUCC(ret) && i < sample_arr.count(); ++i) {
      if (OB_FAIL(split_vector(tmp_alloc, sample_arr.at(i), splited_arrs))) {
        LOG_WARN("fail to split vector", K(ret));
      }
    }

    for (int i = 0; OB_SUCC(ret) && i < pq_m_size_; ++i) {
      if (i >= algos_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("size of algos_ should be pq_m_size_", K(ret), K(i), K(pq_m_size_), K(algos_.count()));
      } else if (OB_FAIL(algos_[i]->build(splited_arrs.at(i)))) {
        LOG_WARN("fail to build kmeans algo", K(ret), K(i), K(algos_[i]));
      }
    }
  }


  return ret;
}

int ObMultiKmeansExecutor::split_vector(ObIAllocator &alloc, float* vector, ObArrayArray<float*> &splited_arrs)
{
  int ret = OB_SUCCESS;
  int64_t start_idx = 0;
  for (int i = 0; OB_SUCC(ret) && i < pq_m_size_; ++i) {
    float *splited_vec = nullptr;
    if (OB_ISNULL(splited_vec = static_cast<float*>(alloc.alloc(sizeof(float) * ctx_.dim_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc vector", K(ret));
    } else {
      MEMCPY(splited_vec, vector + start_idx, sizeof(float) * ctx_.dim_);
      if (OB_FAIL(splited_arrs.push_back(i, splited_vec))) {
        SHARE_LOG(WARN, "failed to push back array", K(ret), K(i));
      } else {
        start_idx += ctx_.dim_;
      }
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

float * ObMultiKmeansExecutor::get_center(const int64_t pos)
{
  float *p_ret = nullptr;
  if (pos < get_total_centers_count() && pos >= 0) {
    ObKmeansAlgo *algo = algos_[pos % algos_.count()];
    if (OB_NOT_NULL(algo)) {
      p_ret = algo->get_cur_centers().at(pos / algos_.count());
    }
  }
  return p_ret;
}

// ------------------ ObElkanKmeansAlgo implement ------------------

int ObElkanKmeansAlgo::calc_kmeans_distance(const float* a, const float* b, const int64_t len, double &distance)
{
  int ret = OB_SUCCESS;
  // only use l2_distance
  if (OB_FAIL(ObVectorL2Distance::l2_distance_func(a, b, len, distance))) {
    SHARE_LOG(WARN, "faild to calc l2 distance", K(ret));
  }
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
  } else {
    const int64_t sample_cnt = input_vectors.count();
    int64_t random = 0;
    random = ObRandom::rand(0, sample_cnt - 1);
    // use random sample vector as the first center
    if (OB_FAIL(centers_[cur_idx_].push_back(kmeans_ctx_->dim_, input_vectors.at(random)))) {
      SHARE_LOG(WARN, "failed to push back center", K(ret));
    } else if (OB_ISNULL(nearest_centers_ = static_cast<int32_t *>(
        tmp_allocator_.alloc(sizeof(int32_t) * sample_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret));
    } else if (OB_ISNULL(upper_bounds_ = static_cast<double *>(
        tmp_allocator_.alloc(sizeof(double) * sample_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret));
    } else if (OB_ISNULL(weight_ = static_cast<double *>(
        tmp_allocator_.alloc(sizeof(double) * sample_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret));
    } else if (OB_FAIL(lower_bounds_.allocate_array(tmp_allocator_, sample_cnt))) {
      LOG_WARN("failed to allocate array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < sample_cnt; ++i) {
        double *bound = nullptr;
        if (OB_ISNULL(bound = static_cast<double *>(tmp_allocator_.alloc(sizeof(double) * kmeans_ctx_->lists_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SHARE_LOG(WARN, "failed to alloc memory", K(ret));
        } else {
          MEMSET(bound, 0, sizeof(double) * kmeans_ctx_->lists_);
          lower_bounds_.at(i) = bound;
        }
      }
      if (OB_SUCC(ret)) {
        MEMSET(nearest_centers_, 0, sizeof(int32_t) * sample_cnt);
        MEMSET(upper_bounds_, 0, sizeof(float) * sample_cnt);
        for (int64_t i = 0; i < sample_cnt; ++i) {
          weight_[i] = DBL_MAX;
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

    double distance = 0;
    bool is_finish = kmeans_ctx_->lists_ == centers_[cur_idx_].count();
    double sum = 0;
    double random_weight = 0;

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
        double min_distance;
        int64_t nearest_center_idx;
        for (i = 0; i < sample_cnt; ++i) {
          min_distance = DBL_MAX;
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
        random_weight = (double)ObRandom::rand(1, 100) / 100.0 * sum;
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
    common::ObArrayWrap<double*> half_centers_distance; // half the distance between each two centers
    double *half_center_min_distance = nullptr; // the min distance from each center to other centers
    double *center_distance_diff = nullptr; // the distance before and after each center update
    int32_t *data_cnt_in_cluster = nullptr; // the number of vectors contained in each center (cluster)
    // init tmp variables
    if (OB_FAIL(half_centers_distance.allocate_array(tmp_allocator_, kmeans_ctx_->lists_))) {
      LOG_WARN("failed to allocate array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < kmeans_ctx_->lists_; ++i) {
        double *tmp = nullptr;
        if (OB_ISNULL(tmp = static_cast<double*>(tmp_allocator_.alloc(sizeof(double) * kmeans_ctx_->lists_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SHARE_LOG(WARN, "failed to alloc memory", K(ret));
        } else {
          MEMSET(tmp, 0, sizeof(double) * kmeans_ctx_->lists_);
          half_centers_distance.at(i) = tmp;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(half_center_min_distance =
        static_cast<double*>(tmp_allocator_.alloc(sizeof(double) * kmeans_ctx_->lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret));
    } else if (OB_ISNULL(center_distance_diff =
        static_cast<double*>(tmp_allocator_.alloc(sizeof(double) * kmeans_ctx_->lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret));
    } else if (OB_ISNULL(data_cnt_in_cluster =
        static_cast<int32_t*>(tmp_allocator_.alloc(sizeof(int32_t) * kmeans_ctx_->lists_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc memory", K(ret));
    } else {
      MEMSET(half_center_min_distance, 0, sizeof(double) * kmeans_ctx_->lists_);
      MEMSET(center_distance_diff, 0, sizeof(double) * kmeans_ctx_->lists_);
      MEMSET(data_cnt_in_cluster, 0, sizeof(int32_t) * kmeans_ctx_->lists_);
    }

    double distance = DBL_MAX;
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
        half_center_min_distance[i] = DBL_MAX;
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
      double min_distance = DBL_MAX;
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
      MEMSET(center_distance_diff, 0, sizeof(double) * kmeans_ctx_->lists_);
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
    tmp_allocator_.reset();
    if (OB_SUCC(ret)) {
      status_ = FINISH;
    }
  }
  return ret;
}

// ------------------ ObIvfBuildHelper implement ------------------
int ObIvfBuildHelper::init(ObString &init_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor allocator invalid.", K(ret));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(init_str, ObVectorIndexType::VIT_IVF_INDEX, param_))) {
    LOG_WARN("failed to parse params.", K(ret));
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

// ------------------ ObIvfFlatBuildHelper implement ------------------
ObIvfFlatBuildHelper::~ObIvfFlatBuildHelper()
{
  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(executor_)) {
      executor_->~ObSingleKmeansExecutor();
      allocator_->free(executor_);
      executor_ = nullptr;
    }
  }
}

int ObIvfFlatBuildHelper::init_kmeans_ctx(const int64_t dim)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  void *buf = nullptr;
  ObVectorNormalizeInfo *norm_info = nullptr;
  if (OB_NOT_NULL(executor_)) {
    // do nothing
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr allocator", K(ret));
  } else if (0 >= param_.nlist_ || 0 >= param_.sample_per_nlist_ || 0 >= dim || VIDA_MAX <= param_.dist_algorithm_) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), K(param_));
  } else if ((VIDA_IP == param_.dist_algorithm_ || VIDA_COS == param_.dist_algorithm_) &&
              FALSE_IT(norm_info = &norm_info_)) { // IP和COS算法需要归一化
  } else if (OB_ISNULL(executor_ = OB_NEWx(ObSingleKmeansExecutor, allocator_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected new ObElkanKmeansAlgo", K(ret));
  } else if (OB_FAIL(executor_->init(ObKmeansAlgoType::KAT_ELKAN,
                                tenant_id_,
                                param_.nlist_,
                                param_.sample_per_nlist_,
                                dim,
                                param_.dist_algorithm_,
                                norm_info))) {
    LOG_WARN("failed to init kmeans ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

// ------------------ ObIvfSq8BuildHelper implement ------------------
ObIvfSq8BuildHelper::~ObIvfSq8BuildHelper()
{
  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(min_vector_)) {
      allocator_->free(min_vector_);
      min_vector_ = nullptr;
    }
    if (OB_NOT_NULL(max_vector_)) {
      allocator_->free(max_vector_);
      max_vector_ = nullptr;
    }
    if (OB_NOT_NULL(step_vector_)) {
      allocator_->free(step_vector_);
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
  if (OB_ISNULL(min_vector_ = static_cast<float*>(allocator_->alloc(vec_dim * sizeof(float))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(vec_dim));
  } else if (OB_ISNULL(max_vector_ = static_cast<float*>(allocator_->alloc(vec_dim * sizeof(float))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(vec_dim));
  } else if (OB_ISNULL(step_vector_ = static_cast<float*>(allocator_->alloc(vec_dim * sizeof(float))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(vec_dim));
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
  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(executor_)) {
      executor_->~ObMultiKmeansExecutor();
      allocator_->free(executor_);
      executor_ = nullptr;
    }
  }
}

int ObIvfPqBuildHelper::init_ctx(const int64_t dim)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  void *buf = nullptr;
  ObVectorNormalizeInfo *norm_info = nullptr;
  if (OB_NOT_NULL(executor_)) {
    // do nothing
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr allocator", K(ret));
  } else if (0 >= param_.nlist_ || 0 >= param_.sample_per_nlist_ || 0 >= dim || VIDA_MAX <= param_.dist_algorithm_
            || param_.m_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), K(param_));
  } else if ((VIDA_IP == param_.dist_algorithm_ || VIDA_COS == param_.dist_algorithm_) &&
              FALSE_IT(norm_info = &norm_info_)) { // The IP and COS algorithms need to be normalized
  } else if (OB_ISNULL(executor_ = OB_NEWx(ObMultiKmeansExecutor, allocator_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected new ObElkanKmeansAlgo", K(ret));
  } else if (OB_FAIL(executor_->init(ObKmeansAlgoType::KAT_ELKAN,
                                tenant_id_,
                                param_.nlist_,
                                param_.sample_per_nlist_,
                                dim,
                                param_.dist_algorithm_,
                                norm_info,
                                param_.m_))) {
    LOG_WARN("failed to init kmeans ctx", K(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

}
}