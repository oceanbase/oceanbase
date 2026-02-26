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
#include "share/allocator/ob_tenant_vector_allocator.h"

namespace oceanbase {
namespace storage
{
struct ObInsertMonitor;
}

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
  explicit ObKmeansCtx(ObIvfMemContext &ivf_build_mem_ctx)
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      dim_(0),
      lists_(0),
      max_sample_count_(0),
      total_scan_count_(0),
      dist_algo_(VIDA_MAX),
      ivf_build_mem_ctx_(ivf_build_mem_ctx),
      norm_info_(nullptr),
      sample_vectors_()
  {}

  virtual ~ObKmeansCtx()
  {
    destroy();
  }
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
               K(dist_algo_),
               K(sample_dim_),
               KP(norm_info_),
               K(sample_vectors_.count()));

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
  ObIvfMemContext &ivf_build_mem_ctx_; // from ObIvfBuildHelper, used for alloc memory for kmeans build process
  ObVectorNormalizeInfo *norm_info_;
  lib::ObMutex lock_; // for sample_vectors_
  ObSEArray<float*, 64> sample_vectors_;
};

// normal kmeans
// quantization and normalization are not of concern here
class ObKmeansBaseTask;
class ObKmeansBuildTaskHandler;
class ObKmeansDistanceCalcTask;
class ObKmeansAssignTask;
class ObKmeansAlgo {
public:
  explicit ObKmeansAlgo(ObIvfMemContext &ivf_build_mem_ctx)
    : kmeans_ctx_(nullptr),
      cur_idx_(0),
      weight_(nullptr),
      status_(PREPARE_CENTERS),
      force_stop_(false),
      ivf_build_mem_ctx_(ivf_build_mem_ctx),
      task_handler_(nullptr),
      distance_tasks_(nullptr),
      assign_tasks_(nullptr),
      max_distance_tasks_(0),
      max_assign_tasks_(0),
      enable_parallel_(false),
      kmeans_monitor_(nullptr)
  {}
  virtual ~ObKmeansAlgo() {
    ObKmeansAlgo::destroy();
  }
  virtual void destroy();
  // ivfpq enable_parallel is false, because ivf_dim is small, there is no need for parallel kmeans.
  int init(ObKmeansCtx &kmeans_ctx, bool enable_parallel = false);
  int build(const ObIArray<float*> &input_vectors);
  bool is_finish() const { return FINISH == status_; }
  int64_t next_idx() { return 1L - cur_idx_; }
  ObCentersBuffer<float> &get_cur_centers() { return centers_[cur_idx_]; }
  ObCentersBuffer<float> &get_centers(int64_t idx) { return centers_[idx]; }
  int64_t get_dim() const { return kmeans_ctx_ ? kmeans_ctx_->dim_ : 0; }
  ObKmeansBuildTaskHandler* get_task_handler() { return task_handler_; }
  int init_build_handle(ObKmeansBuildTaskHandler &handle);
  float get_centers_distance_public(float* centers_distance, int64_t i, int64_t j) { return get_centers_distance(centers_distance, i, j); }
  static int calc_distances_range(const ObIArray<float*> &input_vectors, int64_t start_idx, int64_t end_idx,
                                 float* current_center, float* weight, const int64_t dim, float &sum);

  template<typename TaskType>
  void wait_parallel_task_finish(TaskType *tasks, int64_t tasks_cnt, ObKmeansBuildTaskHandler &handle);
  VIRTUAL_TO_STRING_KV(K(is_inited_),
               KP(kmeans_ctx_),
               KPC(kmeans_ctx_),
               K(cur_idx_),
               KP(weight_),
               K(status_));
  // virtual functions
  virtual int do_kmeans(const ObIArray<float*> &input_vectors) = 0;
  static int calc_kmeans_distance(const float* a, const float* b, const int64_t len, float &distance);
  OB_INLINE void set_kmeans_monitor(ObKmeansMonitor &kmeans_monitor) { kmeans_monitor_ = &kmeans_monitor; }

  void set_stop() { ATOMIC_STORE(&force_stop_, true); }
  bool check_stop();

protected:
  int inner_build(const ObIArray<float*> &input_vectors);
  int quick_centers(const ObIArray<float*> &input_vectors); // use samples as finally centers
  virtual int init_first_center(const ObIArray<float*> &input_vectors);
  // use kmeans++ to init centers
  virtual int init_centers(const ObIArray<float*> &input_vectors);
  int calc_distances_parallel(const ObIArray<float*> &input_vectors,
                             float *current_center, float &sum);
  double calc_imbalance_factor(const ObIArray<float*> &input_vectors, int32_t *data_cnt_in_cluster);
  void set_centers_distance(float* centers_distance, int64_t i, int64_t j, float distance);
  float get_centers_distance(float* centers_distance, int64_t i, int64_t j);

protected:
  bool is_inited_;
  const ObKmeansCtx *kmeans_ctx_;
  int64_t cur_idx_; // switch center buffer
  ObCentersBuffer<float> centers_[2]; // TODO(@jingshui): vector<FLOAT> may need float to avoids overflow
  float *weight_; // only for kmeans++ // each vector has a weight
  ObKMeansStatus status_;
  // When executing in parallel, tasks may be forcibly stopped.
  volatile bool force_stop_;
  ObIvfMemContext &ivf_build_mem_ctx_; // from ObIvfBuildHelper, used for alloc memory for kmeans build process
  ObKmeansBuildTaskHandler *task_handler_;
  ObKmeansDistanceCalcTask *distance_tasks_; // Dynamically allocated task array to avoid stack memory limitations
  ObKmeansAssignTask *assign_tasks_; // Dynamically allocated assignment task array
  int64_t max_distance_tasks_; // Maximum number of distance calculation tasks
  int64_t max_assign_tasks_; // Maximum number of assignment tasks
  bool enable_parallel_; // Whether to enable parallel computation
  ObKmeansMonitor *kmeans_monitor_;
};

class ObElkanKmeansAlgo : public ObKmeansAlgo
{
public:
  ObElkanKmeansAlgo(ObIvfMemContext &ivf_build_mem_ctx)
    : ObKmeansAlgo(ivf_build_mem_ctx)
  {}
  virtual ~ObElkanKmeansAlgo() {
    destroy();
  }
  virtual void destroy() override;
  int assign_vectors_range(const ObIArray<float *> &input_vectors, int64_t start_idx, int64_t end_idx,
                           float *centers_distance, int32_t *data_cnt_in_cluster, float &dis_obj,
                           bool use_safe_add = false);

protected:
  virtual int do_kmeans(const ObIArray<float*> &input_vectors) override;

private:
  int search_nearest_center(const ObIArray<float *> &input_vectors, float *centers_distance,
                            int32_t *data_cnt_in_cluster, float &dis_obj);
  int assign_vectors_parallel(const ObIArray<float *> &input_vectors, float *centers_distance,
                              int32_t *data_cnt_in_cluster, float &dis_obj);

protected:
  static constexpr float GATE_DISTANCE_FACTOR = 4.0; // for gate distance
  static constexpr float EARLY_FINISH_THRESHOLD = 1e-3F; // 0.1% for early finish threshold
  static const int64_t N_ITER = 25; // for max iterations
  common::ObSpinLock assign_lock_; // Lock to protect vector assignment operations

public:
  int add_vector_to_center_safe(int64_t center_idx, int64_t dim, float* vector, int32_t* data_cnt_in_cluster);
};

class ObKmeansExecutor
{
public:
  explicit ObKmeansExecutor(ObIvfMemContext &ivf_build_mem_ctx) : is_inited_(false), ctx_(ivf_build_mem_ctx), ivf_build_mem_ctx_(ivf_build_mem_ctx) {}
  virtual ~ObKmeansExecutor() {
    is_inited_ = false;
  }
  virtual void cancel() = 0;
  virtual int init(ObKmeansAlgoType algo_type,
           const int64_t tenant_id,
           const int64_t lists,
           const int64_t samples_per_nlist,
           const int64_t dim,
           ObVectorIndexDistAlgorithm dist_algo,
           ObVectorNormalizeInfo *norm_info = nullptr,
           const int64_t pq_m_size = 1) = 0;
  virtual int get_center(const int64_t pos, float *&center_vector) = 0;
  virtual int append_sample_vector(float* vector);
  OB_INLINE int64_t get_max_sample_count() { return ctx_.max_sample_count_; }
  bool is_empty() { return ctx_.is_empty(); }
  bool check_stop();

  VIRTUAL_TO_STRING_KV(K(is_inited_),
               K(ctx_));

protected:
  bool is_inited_;
  ObKmeansCtx ctx_;
  ObIvfMemContext &ivf_build_mem_ctx_; // from ObIvfBuildHelper, used for alloc memory for kmeans build process
};

class ObSingleKmeansExecutor : public ObKmeansExecutor
{
public:
  ObSingleKmeansExecutor(ObIvfMemContext &ivf_build_mem_ctx) : ObKmeansExecutor(ivf_build_mem_ctx), algo_(nullptr) {}
  virtual ~ObSingleKmeansExecutor() {
    is_inited_ = false;
    if (OB_NOT_NULL(algo_)) {
      algo_->~ObKmeansAlgo();
      ivf_build_mem_ctx_.Deallocate(algo_);
      algo_ = nullptr;
    }
  }
  virtual void cancel() override {
    if (OB_NOT_NULL(algo_)) {
      algo_->set_stop();
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
  virtual int build(ObInsertMonitor *insert_monitor);
  int get_kmeans_algo(ObKmeansAlgo *&algo);
  int64_t get_centers_count() const;
  int64_t get_centers_dim() const;
  int get_center(const int64_t pos, float *&center_vector) override;

  TO_STRING_KV(KP(algo_), KPC(algo_));

private:
  ObKmeansAlgo *algo_;
};

class ObKmeansBuildTaskHandler;
class ObKmeansBuildTask;
class ObKmeansDistanceCalcTask;
class ObMultiKmeansExecutor : public ObKmeansExecutor
{
public:
  ObMultiKmeansExecutor(ObIvfMemContext &ivf_build_mem_ctx) : ObKmeansExecutor(ivf_build_mem_ctx), pq_m_size_(0) {
    algos_.set_attr(ObMemAttr(MTL_ID(), "MKmeansExu"));
  }
  virtual ~ObMultiKmeansExecutor();
  virtual void cancel() override {
    for (int i = 0; i < algos_.count(); ++i) {
      if (OB_NOT_NULL(algos_[i])) {
        algos_[i]->set_stop();
      }
    }
  }
  virtual int init(
          ObKmeansAlgoType algo_type,
          const int64_t tenant_id,
          const int64_t lists,
          const int64_t samples_per_nlist,
          const int64_t dim,
          ObVectorIndexDistAlgorithm dist_algo,
          ObVectorNormalizeInfo *norm_info = nullptr,
          const int64_t pq_m_size = 1) override;
  virtual int build(ObInsertMonitor *insert_monitor);
  int init_build_handle(ObKmeansBuildTaskHandler &handle);
  int build_parallel(const common::ObTableID &table_id, const common::ObTabletID &tablet_id,
                     ObInsertMonitor *insert_monitor);
  int64_t get_total_centers_count() const;
  int64_t get_centers_count_per_kmeans() const;
  int64_t get_centers_dim() const;
  int get_center(const int64_t pos, float *&center_vector) override;

  TO_STRING_KV(K(is_inited_),
               K(ctx_));

private:
  int split_vector(float* vector, ObArrayArray<float*> &splited_arrs);
  int prepare_splited_arrs(ObArrayArray<float *> &splited_arrs);
  void wait_kmeans_task_finish(ObKmeansBuildTask *build_tasks, ObKmeansBuildTaskHandler &handle);
  int do_build_task_local(const common::ObTableID &table_id, const common::ObTabletID &tablet_id,
                          ObKmeansBuildTaskHandler &handle, const ObArrayArray<float *> &splited_arrs,
                          ObKmeansBuildTask *build_tasks, int task_idx, ObInsertMonitor *insert_monitor);

private:
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
    param_(),
    first_ret_code_(OB_SUCCESS),
    ivf_build_mem_ctx_(nullptr)
  {}
  virtual ~ObIvfBuildHelper() {
    reset();
  }
  void reset();
  virtual void cancel() = 0;
  virtual int init(ObString &init_str, lib::MemoryContext &parent_mem_ctx, uint64_t* all_vsag_use_mem);
  int init_ctx(int64_t dim);
  virtual int init_kmeans_ctx(int64_t dim) = 0;
  ObIAllocator *get_allocator() { return allocator_; }

  void inc_ref();
  bool dec_ref_and_check_release();
  int64_t get_free_vector_mem_size();
  OB_INLINE const ObVectorIndexParam &get_param() const { return param_; }
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(ref_cnt), KP_(allocator), K_(param), K_(first_ret_code));

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t ref_cnt_;
  ObIAllocator *allocator_; // allocator for alloc helper self
  lib::ObMutex lock_;
  ObVectorIndexParam param_;
  int first_ret_code_;
  ObIvfMemContext *ivf_build_mem_ctx_; // for mem alloc in ivf build process
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
  virtual int init_kmeans_ctx(const int64_t dim) override;
  ObSingleKmeansExecutor *get_kmeans_ctx() { return executor_; }
  void cancel() override
  {
    if (OB_NOT_NULL(executor_)) {
      executor_->cancel();
    }
  }

  TO_STRING_KV(K_(tenant_id), K_(ref_cnt), KP_(allocator), KP_(executor), K_(param), KPC_(executor));
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
  void cancel() override {}
  virtual int init_kmeans_ctx(const int64_t dim) override;
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
  void cancel() override {
    if (OB_NOT_NULL(executor_)) {
      executor_->cancel();
    }
  }
  virtual int init_kmeans_ctx(const int64_t dim) override;
  bool can_use_parallel();
  ObMultiKmeansExecutor *get_kmeans_ctx() { return executor_; }
  int build(const common::ObTableID &table_id, const common::ObTabletID &tablet_id, ObInsertMonitor* insert_monitor);

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

  void cancel()
  {
    if (is_valid()) {
      helper_->cancel();
    }
  }
  TO_STRING_KV(KP_(helper), KPC_(helper));

private:
  ObIvfBuildHelper *helper_;
};

// Common base class for task contexts
struct ObKmeansBaseTaskCtx {
  ObKmeansBaseTaskCtx()
      : gmt_create_(0),
        gmt_modified_(0),
        is_finish_(false),
        ret_code_(OB_ERR_UNEXPECTED)
  {}
  virtual ~ObKmeansBaseTaskCtx() = default;

  void init();
  void finish(int ret_code);

  void reset()
  {
    gmt_create_ = 0;
    gmt_modified_ = 0;
    is_finish_ = false;
    ret_code_ = OB_ERR_UNEXPECTED;
  }
  TO_STRING_KV(K_(gmt_create), K_(gmt_modified), K_(is_finish), K_(ret_code));

  int64_t gmt_create_;
  int64_t gmt_modified_;
  bool is_finish_;
  int ret_code_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObKmeansBaseTaskCtx);
};

struct ObKmeansBuildTaskCtx {
  ObKmeansBuildTaskCtx()
      : table_id_(OB_INVALID_ID),
        tablet_id_(OB_INVALID_ID),
        m_idx_(-1),
        vectors_(nullptr),
        insert_monitor_(nullptr)
  {}
  TO_STRING_KV(K_(table_id), K_(tablet_id), K_(m_idx), K_(vectors));
  void reset()
  {
    m_idx_ = -1;
    vectors_ = nullptr;
    insert_monitor_ = nullptr;
  }
  common::ObTableID table_id_;
  common::ObTabletID tablet_id_;
  int m_idx_;
  const ObIArray<float *> *vectors_;
  ObInsertMonitor *insert_monitor_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObKmeansBuildTaskCtx);
};

// Task class for chunked parallel distance calculation
struct ObKmeansDistanceCalcTaskCtx {
  ObKmeansDistanceCalcTaskCtx()
      : start_idx_(0),
        end_idx_(0),
        sum_(0.0f),
        vectors_(nullptr),
        current_center_(nullptr),
        weight_(nullptr),
        dim_(0)
  {}
  TO_STRING_KV(K_(start_idx), K_(end_idx), K_(sum), K_(dim));
  void reset()
  {
    start_idx_ = 0;
    end_idx_ = 0;
    sum_ = 0.0f;
    vectors_ = nullptr;
    current_center_ = nullptr;
    weight_ = nullptr;
    dim_ = 0;
  }
  int64_t start_idx_;
  int64_t end_idx_;
  float sum_;
  const ObIArray<float *> *vectors_;
  float *current_center_;
  float *weight_;
  int64_t dim_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObKmeansDistanceCalcTaskCtx);
};

struct ObKmeansAssignTaskCtx {
  ObKmeansAssignTaskCtx()
      : start_idx_(0),
        end_idx_(0),
        input_vectors_(nullptr),
        centers_distance_(nullptr),
        data_cnt_in_cluster_(nullptr),
        dis_obj_(0.0f)
  {}
  TO_STRING_KV(K_(start_idx), K_(end_idx), K_(dis_obj));
  void reset()
  {
    start_idx_ = 0;
    end_idx_ = 0;
    input_vectors_ = nullptr;
    centers_distance_ = nullptr;
    data_cnt_in_cluster_ = nullptr;
    dis_obj_ = 0.0f;
  }
  int64_t start_idx_;
  int64_t end_idx_;
  const ObIArray<float*> *input_vectors_;
  float* centers_distance_;
  int32_t* data_cnt_in_cluster_;
  float dis_obj_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObKmeansAssignTaskCtx);
};

// Forward declarations
class ObKmeansBuildTaskHandler;
class ObKmeansDistanceCalcTask;

// Base task class
class ObKmeansBaseTask
{
public:
  ObKmeansBaseTask() : is_inited_(false), is_stop_(false) {}
  virtual ~ObKmeansBaseTask() = default;
  virtual void reset() {
    is_inited_ = false;
    is_stop_ = false;
    base_ctx_.reset();
  }
  virtual void set_task_stop() { ATOMIC_STORE(&is_stop_, true); }
  OB_INLINE bool is_stop() const { return ATOMIC_LOAD(&is_stop_); }
  virtual int do_work() = 0;
  OB_INLINE bool is_finish() const { return !is_inited_ || ATOMIC_LOAD(&base_ctx_.is_finish_); }
  OB_INLINE void set_finish(int ret_code) { base_ctx_.finish(ret_code); }
  OB_INLINE int get_ret() const { return base_ctx_.ret_code_; }
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(is_stop), K_(base_ctx));

protected:
  bool is_inited_;
  bool is_stop_;
  ObKmeansBaseTaskCtx base_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObKmeansBaseTask);
};

class ObKmeansDistanceCalcTask : public ObKmeansBaseTask
{
public:
  ObKmeansDistanceCalcTask() : ObKmeansBaseTask() {}
  virtual ~ObKmeansDistanceCalcTask() { reset(); }
  int init(int64_t start_idx, int64_t end_idx,
           const ObIArray<float *> *vectors,
           float *current_center, float *weight, const int64_t dim);
  void reset() override;
  int do_work() override;
  OB_INLINE float get_sum() const { return task_ctx_.sum_; }

  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(task_ctx));

private:
  // task ctx
  ObKmeansDistanceCalcTaskCtx task_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObKmeansDistanceCalcTask);
};


class ObKmeansAssignTask : public ObKmeansBaseTask {
public:
  ObKmeansAssignTask() : ObKmeansBaseTask(), algo_(nullptr) {}
  virtual ~ObKmeansAssignTask() = default;

  int init(int64_t start_idx, int64_t end_idx, ObElkanKmeansAlgo *algo, const ObIArray<float *> *input_vectors,
           float *centers_distance, int32_t *data_cnt_in_cluster);
  virtual void reset() override;
  virtual void set_task_stop() override {
    if (OB_NOT_NULL(algo_)) {
      algo_->set_stop();
    }
    ObKmeansBaseTask::set_task_stop();
  }
  virtual int do_work() override;
  OB_INLINE float get_dis_obj() const { return task_ctx_.dis_obj_; }

private:
  ObElkanKmeansAlgo *algo_;
  ObKmeansAssignTaskCtx task_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObKmeansAssignTask);
};

class ObKmeansBuildTask : public ObKmeansBaseTask
{
public:

  ObKmeansBuildTask() : ObKmeansBaseTask(), algo_(nullptr) {}
  virtual ~ObKmeansBuildTask() { reset(); }
  int init(const common::ObTableID &table_id, const common::ObTabletID &tablet_id, int m_idx, ObKmeansAlgo *algo,
           const ObIArray<float *> *vectors, ObInsertMonitor *insert_monitor);
  void reset() override {
    ObKmeansBaseTask::reset();
    algo_ = nullptr;
    task_ctx_.reset();
  }
  int do_work() override;
  virtual void set_task_stop() override
  {
    if (OB_NOT_NULL(algo_)) {
      algo_->set_stop();
    }
    ObKmeansBaseTask::set_task_stop();
  }

  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(task_ctx));

private:
  ObKmeansAlgo *algo_; // Algorithm instance
  // task ctx
  ObKmeansBuildTaskCtx task_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObKmeansBuildTask);
};

// QUEUE_THREAD
class ObKmeansBuildTaskHandler : public lib::TGTaskHandler
{
public:
  ObKmeansBuildTaskHandler() : is_inited_(false), tg_id_(INVALID_TG_ID), task_ref_cnt_(0), max_thread_cnt_(0), lock_() {};
  virtual ~ObKmeansBuildTaskHandler() = default;
  int init(int tg_id);
  int start();
  void stop();
  void wait();
  void destroy();
  int push_task(ObKmeansBaseTask &task);
  int get_tg_id() { return tg_id_; }
  int get_max_thread_count(int64_t& max_thread_cnt, bool with_refresh = false);

  void inc_task_ref() { ATOMIC_INC(&task_ref_cnt_); }
  void dec_task_ref() { ATOMIC_DEC(&task_ref_cnt_); }
  int64_t get_task_ref() const { return ATOMIC_LOAD(&task_ref_cnt_); }

  virtual void handle(void *task) override;
  virtual void handle_drop(void *task) override;

public:
  // dynamic thread cnt, max cnt is THREAD_FACTOR * tenent_cpu_cnt
  constexpr static const float THREAD_FACTOR = 0.6;
  // 1s
  const static int64_t WAIT_RETRY_PUSH_TASK_TIME = 1 * 1000 * 1000; // us
  // push task max wait time: 1s * 5 * 60 = 5 min
  const static int64_t MAX_RETRY_PUSH_TASK_CNT = 5 * 60;
  static const int64_t INVALID_TG_ID = -1;
  static const int64_t MIN_THREAD_COUNT = 1;

private:
  bool is_inited_;
  int tg_id_;
  volatile int64_t task_ref_cnt_;
  int64_t max_thread_cnt_;

public:
  common::ObSpinLock lock_; // lock for init
private:
  DISALLOW_COPY_AND_ASSIGN(ObKmeansBuildTaskHandler);
};

template<typename TaskType>
void ObKmeansAlgo::wait_parallel_task_finish(TaskType *tasks, int64_t tasks_cnt, ObKmeansBuildTaskHandler &handle)
{
  int ret = OB_SUCCESS;
  bool is_all_finish = false;
  if (OB_ISNULL(tasks)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(tasks), K(tasks_cnt));
  }
  while (OB_SUCC(ret) && !is_all_finish && handle.get_task_ref() > 0) {
    is_all_finish = true;
    for (int64_t i = 0; i < tasks_cnt; ++i) {
      if (!tasks[i].is_finish()) {
        is_all_finish = false;        break;
      }
    }
    if (!is_all_finish) {
      ob_usleep(1000);  // 1ms
      if (check_stop()) {
        for (int64_t i = 0; i < tasks_cnt; ++i) {
          tasks[i].set_task_stop();
        }
        SHARE_LOG(WARN, "check stop");
        // do not break, wait for all task finish
      }
    }
  }
  if (is_all_finish && handle.get_task_ref() != 0) {
    SHARE_LOG(INFO, "wait parallel task finish but task ref is not 0", K(ret), K(is_all_finish), K(handle.get_task_ref()), K(tasks_cnt));
  }
}

}  // namespace share
}  // namespace oceanbase

#endif
