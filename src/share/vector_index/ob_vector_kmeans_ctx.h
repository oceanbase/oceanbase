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

#ifndef SRC_SHARE_VECTOR_INDEX_OB_VECTOR_KMEANS_CTX_H
#define SRC_SHARE_VECTOR_INDEX_OB_VECTOR_KMEANS_CTX_H

#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "ob_vector_index_util.h"
#include "share/vector_type/ob_vector_l2_distance.h"
#include "share/vector_type/ob_vector_common_util.h"

namespace oceanbase {
namespace share {
enum ObKMeansStatus
{
  PREPARE_CENTERS, // init first center && init temp variables
  INIT_CENTERS, // init all centers
  RUNNING_KMEANS,
  FINISH
};

class ObKmeansCtx {
public:
  ObKmeansCtx()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      dim_(0),
      lists_(0),
      max_sample_count_(0),
      total_scan_count_(0),
      dist_algo_(VIDA_MAX),
      allocator_(ObMemAttr(MTL_ID(), "KMeansCtx")),
      norm_info_(nullptr),
      sample_vectors_()
  {}

  ~ObKmeansCtx() { destroy(); }
  void destroy();
  int init(
    const int64_t tenant_id,
    const int64_t lists,
    const int64_t samples_per_nlist,
    const int64_t dim,
    ObVectorIndexDistAlgorithm dist_algo,
    ObVectorNormalizeInfo *norm_info,
    const int64_t pq_m);
  int try_normalize(int64_t dim, float *data, float *norm_vector) const;
  int try_normalize_samples() const;
  int append_sample_vector(float* vector);
  bool is_empty() { return sample_vectors_.empty(); }

  TO_STRING_KV(K(is_inited_),
               K(dim_),
               K(lists_),
               K(tenant_id_),
               K(max_sample_count_),
               K(total_scan_count_),
               K(dist_algo_));

public:
  bool is_inited_;
  int64_t tenant_id_;
  // for FLAT/SQ sample dim == dim, for PQ dim = sample_dim / m
  int64_t sample_dim_;
  int64_t dim_;
  int64_t lists_;
  int64_t max_sample_count_;
  int64_t total_scan_count_; // the number of rows scanned // for reservoir sampling
  ObVectorIndexDistAlgorithm dist_algo_; // TODO(@jingshui): use ObVecDisType ?
  ObArenaAllocator allocator_;
  ObVectorNormalizeInfo *norm_info_;
  lib::ObMutex lock_; // for sample_vectors_
  ObSEArray<float*, 64> sample_vectors_;
};

// normal kmeans
// quantization and normalization are not of concern here
class ObKmeansAlgo {
public:
  ObKmeansAlgo()
    : kmeans_ctx_(nullptr),
      allocator_(nullptr),
      cur_idx_(0),
      weight_(nullptr),
      status_(PREPARE_CENTERS),
      tmp_allocator_()
  {}
  virtual ~ObKmeansAlgo() {
    tmp_allocator_.reset();
  }
  int init(ObKmeansCtx &kmeans_ctx);
  int build(const ObIArray<float*> &input_vectors);
  bool is_finish() const { return FINISH == status_; }
  int64_t next_idx() { return 1L - cur_idx_; }
  ObCentersBuffer<float> &get_cur_centers() { return centers_[cur_idx_]; }

  VIRTUAL_TO_STRING_KV(K(is_inited_),
               KPC(kmeans_ctx_),
               K(cur_idx_),
               KP(weight_),
               K(status_));

  virtual int inner_build(const ObIArray<float*> &input_vectors) = 0;
  virtual int calc_kmeans_distance(const float* a, const float* b, const int64_t len, double &distance) = 0;
  int quick_centers(const ObIArray<float*> &input_vectors); // use samples as finally centers

protected:
  bool is_inited_;
  const ObKmeansCtx *kmeans_ctx_;
  ObIAllocator *allocator_; // from kmeans_ctx
  int64_t cur_idx_; // switch center buffer
  ObCentersBuffer<float> centers_[2]; // TODO(@jingshui): vector<FLOAT> may need DOUBLE to avoids overflow
  double *weight_; // only for kmeans++ // each vector has a weight
  ObKMeansStatus status_;
  ObArenaAllocator tmp_allocator_;
};

// elkan kmeans
// distance must satisfy triangle inequality // l2 or angular distance
// cuz D(x, c1) + D(x, c2) > D(c1, c2), so D(x, c2) > D(c1, c2) - D(x, c1)
// if 2D(x, c1) <= D(c1, c2), then D(x, c2) > D(c1, c2) - D(x, c1) >= D(x, c1) -> D(x, c2) > D(x, c1)
class ObElkanKmeansAlgo : public ObKmeansAlgo
{
public:
  ObElkanKmeansAlgo()
    : ObKmeansAlgo(),
      lower_bounds_(),
      upper_bounds_(nullptr),
      nearest_centers_(nullptr)
  {}

  TO_STRING_KV(KP(upper_bounds_),
               KP(nearest_centers_));

protected:
  virtual int inner_build(const ObIArray<float*> &input_vectors) override;
  virtual int calc_kmeans_distance(const float* a, const float* b, const int64_t len, double &distance) override;

private:
  int init_first_center(const ObIArray<float*> &input_vectors);
  int init_centers(const ObIArray<float*> &input_vectors);
  int do_kmeans(const ObIArray<float*> &input_vectors);

protected:
  common::ObArrayWrap<double*> lower_bounds_; // the minimum possible distance from a vector to the every cluster center
  double *upper_bounds_; // the distance from a vector to its nearest cluster center
  int32_t *nearest_centers_; // idx of each vector's nearest cluster center
};

class ObKmeansExecutor
{
public:
  ObKmeansExecutor() : is_inited_(false) {}
  virtual ~ObKmeansExecutor() {
    is_inited_ = false;
  }
  virtual int init(ObKmeansAlgoType algo_type,
           const int64_t tenant_id,
           const int64_t lists,
           const int64_t samples_per_nlist,
           const int64_t dim,
           ObVectorIndexDistAlgorithm dist_algo,
           ObVectorNormalizeInfo *norm_info = nullptr,
           const int64_t pq_m_size = 1) = 0;
  virtual int append_sample_vector(float* vector);
  bool is_empty() { return ctx_.is_empty(); }

  TO_STRING_KV(K(is_inited_),
               K(ctx_));

protected:
  bool is_inited_;
  ObKmeansCtx ctx_;
};

class ObSingleKmeansExecutor : public ObKmeansExecutor
{
public:
  ObSingleKmeansExecutor() : ObKmeansExecutor(), algo_(nullptr) {}
  virtual ~ObSingleKmeansExecutor() {
    is_inited_ = false;
    if (OB_NOT_NULL(algo_)) {
      algo_->~ObKmeansAlgo();
      algo_ = nullptr;
    }
  }
  virtual int init(ObKmeansAlgoType algo_type,
           const int64_t tenant_id,
           const int64_t lists,
           const int64_t samples_per_nlist,
           const int64_t dim,
           ObVectorIndexDistAlgorithm dist_algo,
           ObVectorNormalizeInfo *norm_info = nullptr,
           const int64_t pq_m_size = 1) override;
  virtual int build();
  int get_kmeans_algo(ObKmeansAlgo *&algo);
  int64_t get_centers_count() const;
  int64_t get_centers_dim() const;
  float *get_center(const int64_t pos);

  TO_STRING_KV(K(is_inited_),
               K(ctx_),
               K(algo_));

private:
  ObKmeansAlgo *algo_;
};

class ObMultiKmeansExecutor : public ObKmeansExecutor
{
public:
  ObMultiKmeansExecutor() : ObKmeansExecutor(), pq_m_size_(0) {
    algos_.set_attr(ObMemAttr(MTL_ID(), "MKmeansExu"));
  }
  virtual ~ObMultiKmeansExecutor();
  virtual int init(
          ObKmeansAlgoType algo_type,
          const int64_t tenant_id,
          const int64_t lists,
          const int64_t samples_per_nlist,
          const int64_t dim,
          ObVectorIndexDistAlgorithm dist_algo,
          ObVectorNormalizeInfo *norm_info = nullptr,
          const int64_t pq_m_size = 1) override;
  virtual int build();
  int64_t get_total_centers_count() const;
  int64_t get_centers_count_per_kmeans() const;
  int64_t get_centers_dim() const;
  float *get_center(const int64_t pos);

  TO_STRING_KV(K(is_inited_),
               K(ctx_));

private:
  int split_vector(ObIAllocator &alloc, float* vector, ObArrayArray<float*> &splited_arrs);

  int pq_m_size_;
  ObSEArray<ObKmeansAlgo *, 4> algos_;
};

class ObIvfBuildHelper
{
public:
  explicit ObIvfBuildHelper(common::ObIAllocator *allocator, uint64_t tenant_id)
  : is_inited_(false),
    tenant_id_(tenant_id),
    ref_cnt_(0),
    allocator_(allocator),
    param_()
  {}
  virtual ~ObIvfBuildHelper() = default;
  virtual int init(ObString &init_str);
  ObIAllocator *get_allocator() { return allocator_; }

  void inc_ref();
  bool dec_ref_and_check_release();
  OB_INLINE const ObVectorIndexParam &get_param() const { return param_; }
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(ref_cnt), KP_(allocator), K_(param));

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t ref_cnt_;
  ObIAllocator *allocator_; // allocator for alloc helper self
  lib::ObMutex lock_;
  ObVectorIndexParam param_;
};


class ObIvfFlatBuildHelper : public ObIvfBuildHelper
{
public:
  ObIvfFlatBuildHelper(common::ObIAllocator *allocator, uint64_t tenant_id)
  : ObIvfBuildHelper(allocator, tenant_id),
    executor_(nullptr),
    norm_info_()
  {}
  virtual ~ObIvfFlatBuildHelper();
  virtual int init_kmeans_ctx(const int64_t dim);
  ObSingleKmeansExecutor *get_kmeans_ctx() { return executor_; }

  TO_STRING_KV(K_(tenant_id), K_(ref_cnt), KP_(allocator), KP_(executor), K_(param));
private:
  ObSingleKmeansExecutor *executor_; // for build centers
  ObVectorNormalizeInfo norm_info_;
};

class ObIvfSq8BuildHelper : public ObIvfBuildHelper
{
public:
  ObIvfSq8BuildHelper(common::ObIAllocator *allocator, uint64_t tenant_id)
  : ObIvfBuildHelper(allocator, tenant_id),
    min_vector_(nullptr),
    max_vector_(nullptr),
    step_vector_(nullptr),
    dim_(0)
  {}
  virtual ~ObIvfSq8BuildHelper();
  int init_result_vectors(int64_t vec_dim);
  int update(const float *vector, int64_t dim);
  int build();
  // shallow copy
  int get_result(int row_pos, float *&vector);

  TO_STRING_KV(KP_(min_vector), KP_(max_vector), KP_(step_vector));
private:
  float* min_vector_;
  float* max_vector_;
  float* step_vector_;
  int64_t dim_;
};

class ObIvfPqBuildHelper : public ObIvfBuildHelper
{
public:
  ObIvfPqBuildHelper(common::ObIAllocator *allocator, uint64_t tenant_id)
  : ObIvfBuildHelper(allocator, tenant_id),
    executor_(nullptr),
    norm_info_()
  {}
  virtual ~ObIvfPqBuildHelper();
  virtual int init_ctx(const int64_t dim);
  ObMultiKmeansExecutor *get_kmeans_ctx() { return executor_; }

  TO_STRING_KV(K_(tenant_id), K_(ref_cnt), KP_(allocator), KP_(executor), K_(param));
private:
  ObMultiKmeansExecutor *executor_; // for build centers
  ObVectorNormalizeInfo norm_info_;
};

class ObIvfBuildHelperGuard
{
public:
  ObIvfBuildHelperGuard(ObIvfBuildHelper *helper = nullptr)
    : helper_(helper)
  {}
  ~ObIvfBuildHelperGuard()
  {
    if (is_valid()) {
      if (helper_->dec_ref_and_check_release()) {
        ObIAllocator *allocator = helper_->get_allocator();
        if (OB_ISNULL(allocator)) {
          const int ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "null allocator", KPC(helper_));
        } else {
          OB_LOG(INFO, "build helper released", KPC(helper_), K(lbt()));
          helper_->~ObIvfBuildHelper(); // TODO: different type
          allocator->free(helper_);
        }
      }
      helper_ = nullptr;
    }
  }

  bool is_valid() { return helper_ != nullptr; }
  ObIvfBuildHelper* get_helper() { return helper_; }
  int set_helper(ObIvfBuildHelper *helper)
  {
    int ret = OB_SUCCESS;
    if (is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "vector index build helper guard can only set once", KPC(helper_), KPC(helper));
    } else {
      helper_ = helper;
      (void)helper_->inc_ref();
    }
    return ret;
  }
  TO_STRING_KV(KPC_(helper));

private:
  ObIvfBuildHelper *helper_;
};

}
}

#endif