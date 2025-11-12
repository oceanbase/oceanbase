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

#ifndef SRC_SHARE_VECTOR_TYPE_OB_VECTOR_COMMON_UTIL_H
#define SRC_SHARE_VECTOR_TYPE_OB_VECTOR_COMMON_UTIL_H

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "ob_vector_norm.h"
#include "ob_vector_l2_distance.h"
#include "ob_vector_add.h"
#include "ob_vector_div.h"
#include "lib/container/ob_heap.h"
#include "common/object/ob_object.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/engine/expr/ob_expr_vector.h"
#include "sql/engine/expr/ob_expr_vector_similarity.h"
#include "share/allocator/ob_tenant_vector_allocator.h"

namespace oceanbase {
namespace share {

int get_distance_threshold(const oceanbase::sql::ObExprVectorDistance::ObVecDisType& dis_type, const float& similarity_threshold, float& distance_threshold);


static bool is_satify_distance_threshold(const oceanbase::sql::ObExprVectorDistance::ObVecDisType& dis_type, const float& distance_threshold, const double& distance)
{
  bool is_satify = true;
  if (dis_type == oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT) {
    is_satify = distance >= distance_threshold;
  } else {
    is_satify = distance <= distance_threshold;
  } 
  return is_satify;
}

struct ObDocidScoreItem
{
  ObDocidScoreItem() = default;
  ObDocidScoreItem(ObString docid, double score): docid_(docid), score_(score) {}
  ~ObDocidScoreItem() = default;
  ObString docid_;
  double score_;
  TO_STRING_KV(K_(docid), K_(score));
};
struct ObDocidScoreItemCmp
{
public:
  bool operator() (const ObDocidScoreItem &a, const ObDocidScoreItem &b)
  {
    return a.score_ < b.score_;
  }

  int get_error_code() { return common::OB_SUCCESS; }
};

struct ObRowkeyScoreItem
{
  ObRowkeyScoreItem() = default;
  ObRowkeyScoreItem(ObRowkey *rowkey, double score): rowkey_(rowkey), score_(score) {}
  ~ObRowkeyScoreItem() = default;
  ObRowkey *rowkey_;
  double score_;
  TO_STRING_KV(KP_(rowkey), K_(score));
};

struct ObRowkeyScoreItemCmp
{
public:
  bool operator() (const ObRowkeyScoreItem &a, const ObRowkeyScoreItem &b)
  {
    return a.score_ < b.score_;
  }

  int get_error_code() { return common::OB_SUCCESS; }
};
template <typename T,
          typename CompareFunctor>
class ObSPIVFixedSizeHeap {
public:
  ObSPIVFixedSizeHeap(int64_t limit_size, ObIAllocator &allocator, CompareFunctor cmp):
                                                                          limit_size_(limit_size),
                                                                          allocator_(allocator),
                                                                          cmp_(cmp),
                                                                          heap_(cmp, &allocator_)
                                                                          {}
  
  ~ObSPIVFixedSizeHeap() = default;
  int push(T &item)
  {
    int ret = OB_SUCCESS;

    if (heap_.count() < limit_size_) {
      if (OB_FAIL(heap_.push(item))) {
        SHARE_LOG(WARN, "failed to push heap", K(ret));
      }
    } else if (cmp_(item, heap_.top())) {
      if (OB_FAIL(heap_.pop())) {
        SHARE_LOG(WARN, "failed to pop heap", K(ret));
      } else if (OB_FAIL(heap_.push(item))) {
        SHARE_LOG(WARN, "failed to push heap", K(ret));
      }
    }

    return ret;
  }
  int pop() { return heap_.pop(); }
  int64_t count() { return heap_.count(); }
  T& top() { return heap_.top(); }
  bool empty() { return heap_.empty(); }
private:
  int64_t limit_size_;
  ObArenaAllocator allocator_;
  CompareFunctor cmp_;
  ObBinaryHeap<T, CompareFunctor, 16> heap_;
};
class ObVectorNormalize final
{
public:
  static int L2_normalize_vector(const int64_t dim, float *data, float *norm_vector, bool *do_normalize = nullptr);
  static int L2_normalize_vectors(const int64_t dim, const int64_t count, float *datas, float *norm_vectors);
  // others if needed
};

enum ObVectorNormalizeType
{
  L2,
  NORM_MAX
};

struct ObVectorNormalizeInfo
{
  ObVectorNormalizeInfo()
    : type_(L2),
      normalize_func_(ObVectorNormalize::L2_normalize_vector)
  {}

  int normalize_vectors(const int64_t dim, const int64_t count, float *datas, float *norm_vectors);

  using FuncPtrType = int (*)(const int64_t dim, float *data, float *norm_vector, bool *do_normalize);

  ObVectorNormalizeType type_;
  FuncPtrType normalize_func_;
};

struct ObIvfRowkeyDistEntry
{
  ObIvfRowkeyDistEntry(): rowkey_(), distance_(0.0f) {}
  ObIvfRowkeyDistEntry(const ObRowkey &rowkey, float distance):
    rowkey_(rowkey), distance_(distance)
  {}

  ObRowkey rowkey_;
  float distance_;

  TO_STRING_KV(K_(rowkey), K_(distance));
};

typedef common::hash::ObHashMap<ObRowkey, ObIvfRowkeyDistEntry, common::hash::NoPthreadDefendMode> ObIVFRowkeyDistMap;
typedef common::hash::ObHashMap<ObRowkey, ObIvfRowkeyDistEntry, common::hash::NoPthreadDefendMode>::iterator ObIVFRowkeyDistMapIterator;

template <typename T>
struct ObCentersBuffer
{
  explicit ObCentersBuffer()
    : dim_(0),
      total_cnt_(0),
      capacity_(0),
      ivf_build_mem_ctx_(nullptr),
      vectors_(nullptr)
  {}
  ~ObCentersBuffer() { destroy(); }
  int init(const int64_t dim, const int64_t capacity, ObIvfMemContext &ivf_build_mem_ctx);
  void destroy();

  T* at(const int64_t idx)
  {
    return vectors_ + idx * dim_;
  }
  int push_back(const int64_t dim, T *vector);
  int add(const int64_t idx, const int64_t dim, T *vector);
  int divide(const int64_t idx, const int64_t count);
  int64_t count() { return total_cnt_; }
  void clear(); // clear as all zero vectors
  
  // for unittest
  int get_nearest_center(const int64_t dim, T *vector, int64_t &center_idx);

  int64_t dim_;
  int32_t total_cnt_;
  int64_t capacity_;
  ObIvfMemContext *ivf_build_mem_ctx_; // from ObIvfBuildHelper, used for alloc memory for kmeans build process
  T *vectors_; // Continuous memory stores all vectors: capacity_ * dim_
};

class ObVectorClusterHelper
{
public:
  enum IvfParseCentIdFlag: uint8_t  {
    IVF_PARSE_TABLET_ID = 0x1,
    IVF_PARSE_CENTER_ID = 0x2,
    IVF_PARSE_M_ID      = 0x4, // only for pq
    IVF_PARSE_CENTER    = IVF_PARSE_TABLET_ID | IVF_PARSE_CENTER_ID,
    IVF_PARSE_PQ_CENTER = IVF_PARSE_TABLET_ID | IVF_PARSE_CENTER_ID | IVF_PARSE_M_ID,
  };

  ObVectorClusterHelper()
  : max_heap_(max_compare_)
  {}
  // get nearest nprobe center
  int get_nearest_probe_centers(
      float *vector,
      const int64_t dim,
      ObIArray<float*> &centers,
      const int64_t nprobe,
      ObIAllocator &allocator,
      share::ObVectorNormalizeInfo *norm_info = nullptr,
      int l_idx = 0,
      int r_idx = -1);
  int get_center_idx(const int64_t idx, int64_t &center_id);
  int get_pq_center_idx(const int64_t idx, const int64_t pq_center_num, int64_t &center_id);
  int get_center_vector(const int64_t idx, const ObIArray<float *> &centers, float*& center_vector);
  void reset();

  static int get_center_id_from_string(ObCenterId &center_id, const ObString &str, uint8_t flag = IVF_PARSE_CENTER);
  static int set_center_id_to_string(const ObCenterId &center_id, ObString &str, ObIAllocator *allocator = nullptr);
  static int get_pq_center_id_from_string(ObPqCenterId &pq_center_id, const ObString &str, uint8_t flag = IVF_PARSE_PQ_CENTER);
  static int set_pq_center_id_to_string(const ObPqCenterId &pq_center_id, ObString &str, ObIAllocator *alloc = nullptr);

  static int create_inner_session(
      const bool is_oracle_mode,
      sql::ObFreeSessionCtx &free_session_ctx,
      sql::ObSQLSessionInfo *&session);
  static void release_inner_session(sql::ObFreeSessionCtx &free_session_ctx, sql::ObSQLSessionInfo *&session);
  static int create_inner_connection(sql::ObSQLSessionInfo *session, common::sqlclient::ObISQLConnection *&connection);
  static void release_inner_connection(common::sqlclient::ObISQLConnection *&connection);

protected:
  struct HeapItem
  {
    HeapItem() : distance_(DBL_MAX) {}
    HeapItem(const double distance) : distance_(distance) {}
    double distance_;
    TO_STRING_KV(K_(distance));
  };
  struct MaxHeapCompare
  {
    bool operator()(const HeapItem &lhs, const HeapItem &rhs)
    {
      return lhs.distance_ < rhs.distance_ ? true : false;
    }
    int get_error_code() const { return OB_SUCCESS; }
  };
  struct MinHeapCompare
  {
    bool operator()(const HeapItem &lhs, const HeapItem &rhs)
    {
      return lhs.distance_ > rhs.distance_ ? true : false;
    }
    int get_error_code() const { return OB_SUCCESS; }
  };
  struct HeapCenterItem : public HeapItem
  {
    HeapCenterItem() : HeapItem(), center_idx_(-1) {}
    HeapCenterItem(const double distance, const int64_t center_idx) : HeapItem(distance), center_idx_(center_idx) {}
    int64_t center_idx_;
    TO_STRING_KV(K_(distance), K_(center_idx));
  };
public:
  typedef common::ObBinaryHeap<HeapCenterItem, MaxHeapCompare, 64> CenterMaxHeap;
  typedef common::ObBinaryHeap<HeapCenterItem, MinHeapCompare, 64> CenterMinHeap;

private:
  MaxHeapCompare max_compare_;
  CenterMaxHeap max_heap_;
};

template <typename VEC_T, typename CENTER_T>
struct ObCentroidQueryInfo
{
  ObCentroidQueryInfo():
    id_(), vec_(nullptr), distance_(0) 
  {}

  ObCentroidQueryInfo(const CENTER_T &id, const VEC_T *vec, const double &distance):
    id_(id), vec_(vec), distance_(distance) 
  {}

  TO_STRING_KV(K_(id), KP_(vec), K_(distance));

  CENTER_T id_;
  const VEC_T *vec_;
  double distance_;
};

template <typename VEC_T, typename CENTER_T>
struct ObCentroidQueryInfoCompare {
  explicit ObCentroidQueryInfoCompare(const oceanbase::sql::ObExprVectorDistance::ObVecDisType dis_type)
  {
    is_reverse_ = (oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT == dis_type
        || oceanbase::sql::ObExprVectorDistance::ObVecDisType::COSINE == dis_type);
  }

  bool operator()(const ObCentroidQueryInfo<VEC_T, CENTER_T> &lhs, const ObCentroidQueryInfo<VEC_T, CENTER_T> &rhs)
  {
    return is_reverse_ ? (lhs.distance_ > rhs.distance_) : (lhs.distance_ < rhs.distance_);
  }
  bool is_reverse_;
};

template<typename T>
class ObCenterWithBuf
{
public:
  ObCenterWithBuf() : alloc_(nullptr), buf_(nullptr), buf_size_(0)
  {}
  explicit ObCenterWithBuf(ObIAllocator *alloc) : alloc_(alloc), buf_(nullptr), buf_size_(0)
  {}
  TO_STRING_KV(K_(buf_size), KP_(buf));

  void reset()
  {
    memset(buf_, 0, buf_size_);
    center_.reset();
  }

  void free_buf()
  {
    if (OB_NOT_NULL(alloc_) && OB_NOT_NULL(buf_)) {
      alloc_->free(buf_);
    }
    buf_ = nullptr;
    buf_size_ = 0;
    center_.reset();
  }
  inline int64_t get_buf_size()
  {
    return buf_size_;
  }

  int new_from_src(const T &src_center);

  const inline T &get_center() const
  {
    return center_;
  }
private:
  ObIAllocator *alloc_;
  char *buf_;
  int64_t buf_size_;
  T center_;
};

template <typename T>
int ObCenterWithBuf<T>::new_from_src(const T &src_center) {
  int ret = common::OB_NOT_SUPPORTED;
  SHARE_LOG(WARN, "not define", K(ret), K(src_center));
  return ret;
}
template <>
int ObCenterWithBuf<ObRowkey>::new_from_src(const ObRowkey &src_rowkey);
template <>
int ObCenterWithBuf<ObString>::new_from_src(const ObString &src_cid);
template <>
int ObCenterWithBuf<ObCenterId>::new_from_src(const ObCenterId &src_cid);

template<typename T>
struct ObVecWithDim
{
  int new_from_src(ObIAllocator& allocator,T *src_vec, int64_t dim) {
    int ret = OB_SUCCESS;
    vec_ = nullptr;
    if (OB_ISNULL(src_vec) || dim < 0) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "invalid argument", K(ret), KP(src_vec), K(dim));
    } else {
      vec_ = (T *)allocator.alloc(dim * sizeof(T));
      if (OB_ISNULL(vec_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(dim));
      } else {
        MEMCPY(vec_, src_vec, dim * sizeof(T));
        dim_ = dim;
      }
    }
    return ret;
  }

  int reuse_from_src(T *src_vec, int64_t dim) {
    int ret = OB_SUCCESS;

    if (OB_ISNULL(src_vec) || dim < 0) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "invalid argument", K(ret), KP(src_vec), K(dim));
    } else if (OB_ISNULL(vec_) || dim != dim_) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "unexpected error", K(ret), K(dim), K(dim_), KP(vec_));
    } else {
      MEMCPY(vec_, src_vec, dim * sizeof(T));
    }
    return ret;
  }

  TO_STRING_KV(K_(dim), KP_(vec));

  int64_t dim_;
  T *vec_;
};

enum CenterSaveMode
{
  NOT_SAVE_CENTER_VEC = 0,
  DEEP_COPY_CENTER_VEC = 1,
  SHALLOW_COPY_CENTER_VEC = 2,
};
template <typename VEC_T, typename CENTER_T>
class ObVectorCenterClusterHelper
{

public:
  ObVectorCenterClusterHelper(ObIAllocator &allocator, const VEC_T *const_vec,
      oceanbase::sql::ObExprVectorDistance::ObVecDisType dis_type,
      int64_t dim, int64_t nprobe, float similarity_threshold, const bool is_save_all_center=false, ObIAllocator *center_alloc_=nullptr)
      : alloc_(allocator), const_vec_(const_vec), dis_type_(dis_type), dim_(dim),
        nprobe_(nprobe), compare_(dis_type), heap_(compare_), similarity_threshold_(similarity_threshold),
        is_save_all_center_(is_save_all_center), center_alloc_(center_alloc_)
  {
    int ret = OB_SUCCESS;
    distance_threshold_ = FLT_MAX; // default value
    if (OB_FAIL(get_distance_threshold(dis_type, similarity_threshold, distance_threshold_))) {
      SHARE_LOG(WARN, "failed to get distance threshold", K(ret), K(dis_type), K(similarity_threshold), K(distance_threshold_));
    }
  }

  int push_center(const CENTER_T &center, VEC_T *center_vec, const int64_t dim, CenterSaveMode center_save_mode = NOT_SAVE_CENTER_VEC);
  int push_center(const CENTER_T &center, double distance, CenterSaveMode center_save_mode = NOT_SAVE_CENTER_VEC, VEC_T *center_vec = nullptr);
  bool should_push_center(double distance) const;
  int get_nearest_probe_center_ids(ObIArray<CENTER_T> &center_ids);
  int get_nearest_probe_center_ids_dist(ObArrayWrap<bool> &nearest_cid_vecs);
  int get_nearest_probe_centers_ptrs(ObArrayWrap<VEC_T *> &nearest_cid_vecs);
  int get_nearest_probe_centers(ObIArray<std::pair<CENTER_T, VEC_T *>> &center_ids);

  int get_nearest_probe_centers_dist_map(ObIVFRowkeyDistMap &map)
  {
    int ret = OB_SUCCESS;
    if (is_save_all_center_) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "save all center mode not support this", K(ret), K(lbt()));
    } else if (heap_.count() > nprobe_) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "max heap count is not equal to nprobe", K(ret), K(heap_.count()), K(nprobe_));
    }
    while(OB_SUCC(ret) && !heap_.empty()) {
      const HeapCenterItemTemp &cur_top = heap_.top();
      if (OB_ISNULL(cur_top.center_with_buf_)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "center_with_buf is null", K(ret), K(cur_top));
      } else if (OB_FAIL(map.set_refactored(cur_top.center_with_buf_->get_center(), ObIvfRowkeyDistEntry(cur_top.center_with_buf_->get_center(), cur_top.distance_)))) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "failed to push center id", K(ret), K(cur_top));
      } else if (OB_FAIL(heap_.pop())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "failed to pop max heap", K(ret));
      }
    }
    return ret;
  }
  int get_nearest_probe_centers_vec_dist(ObIArray<std::pair<CENTER_T, VEC_T *>> &center_ids,
                                         ObIArray<float> &distances);
  int64_t get_center_count() const {
    return is_save_all_center_ ? all_centroids_.count() : heap_.count();
  }

  bool is_save_all_center() const { return is_save_all_center_; }

  int record_center(const CENTER_T &center, const double distance, CenterSaveMode center_save_mode, VEC_T *center_vec)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(center_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "center_alloc_ is nullptr", K(ret), K(is_save_all_center_));
    } else {
      VEC_T *vec = center_vec;
      if (DEEP_COPY_CENTER_VEC == center_save_mode) {
        vec = nullptr;
        int64_t len = dim_ * sizeof(VEC_T);
        if (OB_ISNULL(center_vec) || dim_ < 0) {
          ret = OB_INVALID_ARGUMENT;
          SHARE_LOG(WARN, "invalid argument", K(ret), KP(center_vec), K(dim_));
        } else if (OB_ISNULL(vec = (VEC_T *)center_alloc_->alloc(len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(dim_), K(len), "elem_size", (sizeof(VEC_T)));
        } else {
          MEMCPY(vec, center_vec, len);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(all_centroids_.push_back(ObCentroidQueryInfo<VEC_T, CENTER_T>(center, vec, distance)))) {
        SHARE_LOG(WARN, "failed to push back centroids", K(ret), K(center), K(distance), KP(center_vec), KP(vec));
      } else {
        SHARE_LOG(TRACE, "center info", K(center), K(distance));
      }
    }
    return ret;
  }

  int get_all_centroids(ObIArray<ObCentroidQueryInfo<VEC_T, CENTER_T>> &centroids)
  {
    int ret = OB_SUCCESS;
    lib::ob_sort(all_centroids_.begin(), all_centroids_.end(), ObCentroidQueryInfoCompare<VEC_T, CENTER_T>(dis_type_));
    for (int64_t i=0; OB_SUCC(ret) && i < all_centroids_.count(); ++i) {
      if (OB_FAIL(centroids.push_back(all_centroids_.at(i)))) {
        SHARE_LOG(WARN, "push back fail", K(ret), K(i));
      } else {
        SHARE_LOG(TRACE, "copy center info", K(i), K(all_centroids_.at(i)));
      }
    }
    return ret;
  }

  int64_t count() const { return heap_.count(); }

 public:
  struct HeapCenterItemTemp
  {
    HeapCenterItemTemp() : distance_(DBL_MAX), center_with_buf_(nullptr) {}
    HeapCenterItemTemp(const double distance, ObCenterWithBuf<CENTER_T>* center_with_buf) : 
      distance_(distance), center_with_buf_(center_with_buf) {}
    void set_vec_dim(const ObVecWithDim<VEC_T>& vec_dim) {
      vec_dim_ = vec_dim;
    }
    double distance_;
    ObCenterWithBuf<CENTER_T>* center_with_buf_;
    // for pq, need center idx to get center vector
    ObVecWithDim<VEC_T> vec_dim_;
    TO_STRING_KV(K_(distance), KP_(center_with_buf), K_(vec_dim));
  };
  struct HeapCompare {
    HeapCompare() : is_max_heap_(true) {}
    explicit HeapCompare(oceanbase::sql::ObExprVectorDistance::ObVecDisType dis_type) {
      if (dis_type == oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT) {
        is_max_heap_ = false;
      } else {
        is_max_heap_ = true;
      }
    }
    bool operator()(const HeapCenterItemTemp &lhs, const HeapCenterItemTemp &rhs) const
    {
      if (is_max_heap_) {
        return lhs.distance_ < rhs.distance_ ? true : false;
      } else {
        return lhs.distance_ > rhs.distance_ ? true : false;
      }
    }
    int get_error_code() const
    {
      return OB_SUCCESS;
    }
    bool is_max_heap_ = true;
  };

public:
  typedef common::ObBinaryHeap<HeapCenterItemTemp, HeapCompare, 64> CenterHeap;

private:
  ObIAllocator &alloc_;
  const VEC_T *const_vec_;
  oceanbase::sql::ObExprVectorDistance::ObVecDisType dis_type_;
  int64_t dim_;
  int64_t nprobe_;
  HeapCompare compare_;
  CenterHeap heap_;
  float similarity_threshold_;
  float distance_threshold_;
  bool is_save_all_center_;
  ObIAllocator *center_alloc_;
  ObArray<ObCentroidQueryInfo<VEC_T, CENTER_T>> all_centroids_;
};

// Union structure for packing kmeans iteration info into a single int64_t
struct ObKmeansIterInfo {
  union {
    int64_t packed_value;
    struct {
      int64_t now_iter_ : 10;       // 10 bits, can represent 0-1023
      int64_t finish_diff_int_ : 17; // 17 bits, can represent 0-131071
      int64_t now_diff_int_ : 17;    // 17 bits, can represent 0-131071
      int64_t reserved : 20;         // reserved bits, total 64 bits
    } fields;
  };
  
  ObKmeansIterInfo() : packed_value(0) {}
  
  void set_kmeans_iter_info(int64_t now_iter, int64_t finish_diff, int64_t now_diff) {
    fields.now_iter_ = now_iter & 0x3FF;             // 10 bits mask
    fields.finish_diff_int_ = finish_diff & 0x1FFFF; // 17 bits mask
    fields.now_diff_int_ = now_diff & 0x1FFFF;       // 17 bits mask
  }
  
  static void get_kmeans_iter_info(const int64_t &packed_value, int64_t &now_iter, int64_t &finish_diff, int64_t &now_diff) {
    ObKmeansIterInfo info;
    info.packed_value = packed_value;
    now_iter = info.fields.now_iter_;
    finish_diff = info.fields.finish_diff_int_;
    now_diff = info.fields.now_diff_int_;
  }
};


struct ObKmeansMonitor {
  ObKmeansMonitor() {
    memset(this, 0, sizeof(*this));
  }
  static int64_t diff_float_to_int(float diff) { return static_cast<int64_t>(diff * DIFF_FIXED_POINT_FACTOR); }
  static float diff_int_to_float(int64_t diff_int) { return static_cast<float>(diff_int / DIFF_FIXED_POINT_FACTOR); }
  static int64_t imbalance_float_to_int(float imbalance_factor)
  {
    return static_cast<int64_t>(imbalance_factor * IMPALANCE_FIXED_POINT_FACTOR);
  }
  static float imbalance_int_to_float(int64_t imbalance_int) { return static_cast<float>(imbalance_int / IMPALANCE_FIXED_POINT_FACTOR); }
  void set_kmeams_monitor(int64_t now_iter, float finish_diff, float now_diff, float imbalance_factor) {
    if (OB_UNLIKELY(OB_NOT_NULL(kmeans_iter_info_value_))) {
      kmeans_iter_info_->set_kmeans_iter_info(now_iter, diff_float_to_int(finish_diff), diff_float_to_int(now_diff));
      (void)ATOMIC_SET(kmeans_iter_info_value_, kmeans_iter_info_->packed_value);
    }
    if (OB_UNLIKELY(OB_NOT_NULL(imbalance_factor_int_))) {
      (void)ATOMIC_SET(imbalance_factor_int_, imbalance_float_to_int(imbalance_factor));
    }
  }
  void add_finish_tablet_cnt() {
    if (OB_UNLIKELY(OB_NOT_NULL(finish_tablet_cnt_))) {
      (void)ATOMIC_AAF(finish_tablet_cnt_, 1);
    }
  }
  int64_t *finish_tablet_cnt_;
  union {
    ObKmeansIterInfo *kmeans_iter_info_; // for now_iter_,finish_diff_int_,now_diff_int_
    int64_t *kmeans_iter_info_value_;
    int64_t *vec_index_task_thread_pool_cnt_;
  };
  union {
    int64_t *imbalance_factor_int_;
    int64_t *vec_index_task_total_cnt_;
  };

  union {
    int64_t *vec_index_task_finish_cnt_;
  };

  static constexpr float DIFF_FIXED_POINT_FACTOR = 1e5F;
  static constexpr float IMPALANCE_FIXED_POINT_FACTOR = 1e2F;
};

// ------------------ ObCentersBuffer implement ------------------
template <typename T>
int ObCentersBuffer<T>::init(const int64_t dim, const int64_t capacity, ObIvfMemContext &ivf_build_mem_ctx)
{
  int ret = OB_SUCCESS;
  if (0 >= dim || 0 >= capacity) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), K(capacity));
  } else if (OB_FALSE_IT(ivf_build_mem_ctx_ = &ivf_build_mem_ctx)) {
  } else {
    int64_t total_size = capacity * dim * sizeof(T);
    if (OB_ISNULL(vectors_ = static_cast<T*>(ivf_build_mem_ctx_->Allocate(total_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc vectors memory", K(ret), K(total_size), K(ivf_build_mem_ctx_->get_all_vsag_use_mem_byte()));
    } else {
      MEMSET(vectors_, 0, total_size);
      dim_ = dim;
      capacity_ = capacity;
      total_cnt_ = 0;
    }
  }
  return ret;
}

template <typename T>
void ObCentersBuffer<T>::destroy()
{
  if (OB_NOT_NULL(ivf_build_mem_ctx_) && OB_NOT_NULL(vectors_)) {
    ivf_build_mem_ctx_->Deallocate(vectors_);
    vectors_ = nullptr;
  }

  dim_ = 0;
  total_cnt_ = 0;
  capacity_ = 0;
  ivf_build_mem_ctx_ = nullptr;
}

template <typename T>
void ObCentersBuffer<T>::clear()
{
  if (OB_NOT_NULL(vectors_)) {
    MEMSET(vectors_, 0, capacity_ * dim_ * sizeof(T));
  }
  total_cnt_ = capacity_;
}

template <typename T>
int ObCentersBuffer<T>::push_back(const int64_t dim, T *vector)
{
  int ret = OB_SUCCESS;
  if (total_cnt_ >= capacity_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "buffer full", K(ret), K(capacity_));
  } else if (dim != dim_ || nullptr == vector) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), KP(vector));
  } else {
    T *raw_vector = vectors_ + total_cnt_ * dim_;
    MEMCPY(raw_vector, vector, dim * sizeof(T));
    ++total_cnt_;
  }
  return ret;
}

template <typename T>
int ObCentersBuffer<T>::add(const int64_t idx, const int64_t dim, T *vector)
{
  int ret = OB_SUCCESS;
  if (dim != dim_ || nullptr == vector || idx < 0 || idx >= total_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), KP(vector), K(idx), K_(total_cnt));
  } else {
    T *raw_vector = vectors_ + idx * dim_;
    for (int64_t i = 0; i < dim_; ++i) {
      raw_vector[i] += vector[i];
    }
  }
  return ret;
}

template <>
int ObCentersBuffer<float>::add(const int64_t idx, const int64_t dim, float *vector);

template <typename T>
int ObCentersBuffer<T>::divide(const int64_t idx, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (0 >= count || idx < 0 || idx >= total_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(idx), K(count), K_(total_cnt));
  } else {
    T *raw_vector = vectors_ + idx * dim_;
    for (int64_t i = 0; i < dim_; ++i) {
      raw_vector[i] /= count;
    }
  }
  return ret;
}

template <>
int ObCentersBuffer<float>::divide(const int64_t idx, const int64_t count);

template <typename T>
int ObCentersBuffer<T>::get_nearest_center(const int64_t dim, T *vector, int64_t &center_idx)
{
  return OB_NOT_SUPPORTED;
}

template <>
int ObCentersBuffer<float>::get_nearest_center(const int64_t dim, float *vector, int64_t &center_idx);

// ------------------------------- ObVectorCenterClusterHelper implement --------------------------------
template <typename VEC_T, typename CENTER_T>
int ObVectorCenterClusterHelper<VEC_T, CENTER_T>::push_center(
    const CENTER_T &center, 
    VEC_T *center_vec, 
    const int64_t dim,
    CenterSaveMode center_save_mode /*= NOT_SAVE_CENTER_VEC*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(center_vec) || dim_ != dim) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(center), KP(center_vec), K(dim));
  } else {
    double distance = DBL_MAX;
    if (OB_FAIL(oceanbase::sql::ObExprVectorDistance::DisFunc<VEC_T>::distance_funcs[dis_type_](const_vec_, center_vec, dim_, distance))) {
      SHARE_LOG(WARN, "failed to get distance type", K(ret));
    } else if (OB_FAIL(push_center(center, distance, center_save_mode, center_vec))) {
      SHARE_LOG(WARN, "fail to push back center with distance", K(ret), K(distance));
    }
  }

  return ret;
}

template <typename VEC_T, typename CENTER_T>
int ObVectorCenterClusterHelper<VEC_T, CENTER_T>::push_center(
  const CENTER_T &center,
  double distance,
  CenterSaveMode center_save_mode /*= NOT_SAVE_CENTER_VEC*/,
  VEC_T *center_vec /*= nullptr*/)
{
  int ret = OB_SUCCESS;
  if (distance_threshold_ != FLT_MAX && !is_satify_distance_threshold(dis_type_, distance_threshold_, distance)) {
    // if not satify distance threshold, do not push center
  } else if (is_save_all_center_) {
    if (OB_FAIL(record_center(center, distance, center_save_mode, center_vec))) {
      SHARE_LOG(WARN, "failed to push back centroids", K(ret), K(center), K(distance));
    }
  } else if (heap_.count() < nprobe_) {
    void *ptr = alloc_.alloc(sizeof(ObCenterWithBuf<CENTER_T>));
    if (NULL == ptr) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "no memory for table entity", K(ret));
    } else {
      ObCenterWithBuf<CENTER_T> *center_with_buf = new (ptr) ObCenterWithBuf<CENTER_T>(&alloc_);
      if (OB_ISNULL(center_with_buf)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "center_entity is null", K(ret));
      } else if (OB_FAIL(center_with_buf->new_from_src(center))) {
        SHARE_LOG(WARN, "center_entity fail init", K(ret));
      } else {
        HeapCenterItemTemp item(distance, center_with_buf);
        if (center_save_mode == DEEP_COPY_CENTER_VEC && OB_FAIL(item.vec_dim_.new_from_src(alloc_, center_vec, dim_))) {
          SHARE_LOG(WARN, "failed to new from src", K(ret), K(center_vec));
        } else if (center_save_mode == SHALLOW_COPY_CENTER_VEC && OB_FALSE_IT(item.vec_dim_.vec_ = center_vec)) {
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(heap_.push(item))) {
          SHARE_LOG(WARN, "failed to push center heap", K(ret), K(center), K(distance));
        }
      }
    }
  } else {
    const HeapCenterItemTemp &top = heap_.top();
    ObCenterWithBuf<CENTER_T> tmp_center_with_buf;
    HeapCenterItemTemp tmp(distance, &tmp_center_with_buf);
    if (compare_(tmp, top)) {
      ObCenterWithBuf<CENTER_T> *old_center_with_buf = top.center_with_buf_;
      if (OB_ISNULL(old_center_with_buf)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "center_with_buf is null", K(ret));
      } else if (OB_FAIL(old_center_with_buf->new_from_src(center))) {
        SHARE_LOG(WARN, "failed to new from src", K(ret), K(center));
      } else {
        HeapCenterItemTemp new_top(distance, old_center_with_buf);
        if (center_save_mode == DEEP_COPY_CENTER_VEC) {
          new_top.set_vec_dim(top.vec_dim_);
          if (OB_FAIL(new_top.vec_dim_.reuse_from_src(center_vec, dim_))) {
            SHARE_LOG(WARN, "failed to new from src", K(ret), K(center_vec));
          }
        } else if (center_save_mode == SHALLOW_COPY_CENTER_VEC) {
          new_top.set_vec_dim(top.vec_dim_);
          new_top.vec_dim_.vec_ = center_vec;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(heap_.replace_top(new_top))) {
          SHARE_LOG(WARN, "failed to replace top", K(ret), K(new_top));
        }
      }
    }
  }
  return ret;
}

template <typename VEC_T, typename CENTER_T>
int ObVectorCenterClusterHelper<VEC_T, CENTER_T>::get_nearest_probe_center_ids(ObIArray<CENTER_T> &center_ids)
{
  int ret = OB_SUCCESS;
  if (is_save_all_center_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "save all center mode not support this", K(ret), K(lbt()));
  } else if (heap_.count() > nprobe_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "max heap count is not equal to nprobe", K(ret), K(heap_.count()), K(nprobe_));
  }
  
  while(OB_SUCC(ret) && !heap_.empty()) {
    const HeapCenterItemTemp &cur_top = heap_.top();
    if (OB_ISNULL(cur_top.center_with_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "center_with_buf is null", K(ret));
    } else if (OB_FAIL(center_ids.push_back(cur_top.center_with_buf_->get_center()))) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to push center id", K(ret), K(cur_top.center_with_buf_->get_center()));
    } else if (OB_FAIL(heap_.pop())) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to pop max heap", K(ret));
    }
  }
  // reverse center_ids
  if (OB_SUCC(ret)) {
    std::reverse(center_ids.get_data(), center_ids.get_data() + center_ids.count());
  }
  return ret;
}

template <typename VEC_T, typename CENTER_T>
int ObVectorCenterClusterHelper<VEC_T, CENTER_T>::get_nearest_probe_center_ids_dist(ObArrayWrap<bool> &nearest_cid_dist)
{
  int ret = OB_NOT_SUPPORTED;
  SHARE_LOG(WARN, "not define", K(ret));
  return ret;
}

template <>
int ObVectorCenterClusterHelper<float, ObCenterId>::get_nearest_probe_center_ids_dist(ObArrayWrap<bool> &nearest_cid_dist);

template <typename VEC_T, typename CENTER_T>
int ObVectorCenterClusterHelper<VEC_T, CENTER_T>::get_nearest_probe_centers_ptrs(ObArrayWrap<VEC_T *> &nearest_cid_dist)
{
  int ret = OB_NOT_SUPPORTED;
  SHARE_LOG(WARN, "not define", K(ret));
  return ret;
}

template <>
int ObVectorCenterClusterHelper<float, ObCenterId>::get_nearest_probe_centers_ptrs(ObArrayWrap<float *> &nearest_cid_dist);

template <typename VEC_T, typename CENTER_T>
int ObVectorCenterClusterHelper<VEC_T, CENTER_T>::get_nearest_probe_centers(ObIArray<std::pair<CENTER_T, VEC_T *>> &center_ids)
{
  int ret = OB_SUCCESS;
  if (is_save_all_center_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "save all center mode not support this", K(ret), K(lbt()));
  } else if (heap_.count() > nprobe_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "max heap count is not equal to nprobe", K(ret), K(heap_.count()), K(nprobe_));
  } else if (similarity_threshold_ > 0.0 ){
    ret = OB_NOT_SUPPORTED;
    SHARE_LOG(WARN, "similarity threshold", K(ret), K(similarity_threshold_));
  }
  while(OB_SUCC(ret) && !heap_.empty()) {
    const HeapCenterItemTemp &cur_top = heap_.top();
    if (OB_ISNULL(cur_top.center_with_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "center_with_buf is null", K(ret), K(cur_top));
    } else if (OB_FAIL(center_ids.push_back(std::make_pair(cur_top.center_with_buf_->get_center(), cur_top.vec_dim_.vec_)))) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to push center id", K(ret), K(cur_top));
    } else if (OB_FAIL(heap_.pop())) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to pop max heap", K(ret));
    }
  }
  return ret;
}

template <typename VEC_T, typename CENTER_T>
int ObVectorCenterClusterHelper<VEC_T, CENTER_T>::get_nearest_probe_centers_vec_dist(
  ObIArray<std::pair<CENTER_T, VEC_T *>> &center_ids, ObIArray<float>& distances)
{
  int ret = OB_SUCCESS;
  if (is_save_all_center_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "save all center mode not support this", K(ret), K(lbt()));
  } else if (heap_.count() > nprobe_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "max heap count is not equal to nprobe", K(ret), K(heap_.count()), K(nprobe_));
  } else if (similarity_threshold_ > 0.0 ){
    ret = OB_NOT_SUPPORTED;
    SHARE_LOG(WARN, "similarity threshold", K(ret), K(similarity_threshold_));
  }
  while(OB_SUCC(ret) && !heap_.empty()) {
    const HeapCenterItemTemp &cur_top = heap_.top();
    if (OB_ISNULL(cur_top.center_with_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "center_with_buf is null", K(ret), K(cur_top));
    } else if (OB_FAIL(center_ids.push_back(std::make_pair(cur_top.center_with_buf_->get_center(), cur_top.vec_dim_.vec_)))) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to push center id", K(ret), K(cur_top));
    } else if (OB_FAIL(distances.push_back(cur_top.distance_))) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to push distance", K(ret), K(cur_top));
    } else if (OB_FAIL(heap_.pop())) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to pop max heap", K(ret));
    }
  }
  return ret;
}

// 预检查方法：判断是否应该将center加入堆中，避免不必要的内存分配
template <typename VEC_T, typename CENTER_T>
bool ObVectorCenterClusterHelper<VEC_T, CENTER_T>::should_push_center(double distance) const
{
  if (is_save_all_center_) {
    return true;  // 保存所有center模式，总是返回true
  }
  
  if (heap_.count() < nprobe_) {
    return true;  // 堆未满，可以加入
  }
  
  // 堆已满，检查是否比堆顶更好
  if (!heap_.empty()) {
    const HeapCenterItemTemp &top = heap_.top();
    ObCenterWithBuf<CENTER_T> tmp_center_with_buf;
    HeapCenterItemTemp tmp(distance, &tmp_center_with_buf);
    return compare_(tmp, top);  // 如果新元素比堆顶更好，则应该替换
  }
  
  return false;
}
}
}

#endif
