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

namespace oceanbase {
namespace share {
class ObVectorNormalize final
{
public:
  static int L2_normalize_vector(const int64_t dim, float *data, float *norm_vector);
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

  using FuncPtrType = int (*)(const int64_t dim, float *data, float *norm_vector);

  ObVectorNormalizeType type_;
  FuncPtrType normalize_func_;
};

template <typename T>
struct ObCentersBuffer
{
  ObCentersBuffer()
    : dim_(0),
      total_cnt_(0),
      vectors_()
  {}
  ~ObCentersBuffer() { destroy(); }
  int init(const int64_t dim, const int64_t capacity, ObIAllocator &allocator);
  void destroy();

  T* at(const int64_t idx)
  {
    return vectors_.at(idx);
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
  ObIAllocator *allocator_;
  // T *vectors_; // dim_ * capacity_
  common::ObArrayWrap<T*> vectors_;
};

class ObVectorClusterHelper
{
public:
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
  int get_center_vector(const int64_t idx, const ObIArray<float *> &centers, float*& center_vector);
  void reset();

  static int get_center_id_from_string(ObCenterId &center_id, const ObString &str);
  static int set_center_id_to_string(const ObCenterId &center_id, ObString &str);
  static int get_pq_center_id_from_string(ObPqCenterId &pq_center_id, const ObString &str);
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
  typedef common::ObBinaryHeap<HeapCenterItem, MaxHeapCompare, 64> CenterMinHeap;

private:
  MaxHeapCompare max_compare_;
  CenterMaxHeap max_heap_;
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

template <typename VEC_T, typename CENTER_T>
class ObVectorCentorClusterHelper
{
public:
  ObVectorCentorClusterHelper(ObIAllocator &allocator, const VEC_T *const_vec, oceanbase::sql::ObExprVectorDistance::ObVecDisType dis_type, int64_t dim, int64_t nprobe)
      : alloc_(allocator), const_vec_(const_vec), dis_type_(dis_type), dim_(dim), nprobe_(nprobe), max_heap_(max_compare_)
  {
    // fixme: use euclidean dis type instead dot
    if (dis_type == oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT) {
      dis_type_ = oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN;
    }
  }

  int push_center(const CENTER_T &center, VEC_T *center_vec, const int64_t dim, bool save_center_vec = false);
  int push_center(const CENTER_T &center, double distance, bool save_center_vec = false, VEC_T *center_vec = nullptr);
  int get_nearest_probe_center_ids(ObIArray<CENTER_T> &center_ids);
  int get_nearest_probe_centers(ObIArray<std::pair<CENTER_T, VEC_T *>> &center_ids);

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
  struct MaxHeapCompare {
    bool operator()(const HeapCenterItemTemp &lhs, const HeapCenterItemTemp &rhs)
    {
      return lhs.distance_ < rhs.distance_ ? true : false;
    }
    int get_error_code() const
    {
      return OB_SUCCESS;
    }
  };

public:
  typedef common::ObBinaryHeap<HeapCenterItemTemp, MaxHeapCompare, 64> CenterMaxHeap;

private:
  ObIAllocator &alloc_;
  const VEC_T *const_vec_;
  oceanbase::sql::ObExprVectorDistance::ObVecDisType dis_type_;
  int64_t dim_;
  int64_t nprobe_;
  MaxHeapCompare max_compare_;
  CenterMaxHeap max_heap_;
};

// ------------------ ObCentersBuffer implement ------------------
template <typename T>
int ObCentersBuffer<T>::init(const int64_t dim, const int64_t capacity, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (0 >= dim || 0 >= capacity) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), K(capacity));
  } else if (OB_FAIL(vectors_.allocate_array(allocator, capacity))) {
    SHARE_LOG(WARN, "failed to allocate array", K(ret), K(dim), K(capacity));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < capacity; ++i) {
      T *vector = nullptr;
      if (OB_ISNULL(vector = static_cast<T*>(allocator.alloc(dim * sizeof(T))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "failed to alloc memory", K(ret), K(dim));
      } else {
        MEMSET(vector, 0, sizeof(T) * dim);
        vectors_.at(i) = vector;
      }
    }
    if (OB_SUCC(ret)) {
      dim_ = dim;
      total_cnt_ = 0;
      allocator_ = &allocator;
    }
  }
  return ret;
}

template <typename T>
void ObCentersBuffer<T>::destroy()
{
  for (int64_t i = 0; i < vectors_.count(); ++i) {
    T *vector = vectors_.at(i);
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(vector);
    }
  }
  dim_ = 0;
  total_cnt_ = 0;
}

template <typename T>
void ObCentersBuffer<T>::clear()
{
  for (int64_t i = 0; i < vectors_.count(); ++i) {
    T *vector = vectors_.at(i);
    MEMSET(vector, 0, sizeof(T) * dim_);
  }
  total_cnt_ = vectors_.count();
}

template <typename T>
int ObCentersBuffer<T>::push_back(const int64_t dim, T *vector)
{
  int ret = OB_SUCCESS;
  const int64_t capacity = vectors_.count();
  if (total_cnt_ >= capacity) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "buffer full", K(ret), K(capacity));
  } else if (dim != dim_ || nullptr == vector) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), KP(vector));
  } else {
    T *raw_vector = vectors_.at(total_cnt_);
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
    T *raw_vector = vectors_.at(idx);
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
    T *raw_vector = vectors_.at(idx);
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

// ------------------------------- ObVectorCentorClusterHelper implement --------------------------------
template <typename VEC_T, typename CENTER_T>
int ObVectorCentorClusterHelper<VEC_T, CENTER_T>::push_center(
    const CENTER_T &center,
    VEC_T *center_vec,
    const int64_t dim,
    bool save_center_vec /*= false*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(center_vec) || dim_ != dim) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(center), KP(center_vec), K(dim));
  } else {
    double distance = DBL_MAX;
    if (OB_FAIL(oceanbase::sql::ObExprVectorDistance::DisFunc<VEC_T>::distance_funcs[dis_type_](const_vec_, center_vec, dim_, distance))) {
      SHARE_LOG(WARN, "failed to get distance type", K(ret));
    } else if (OB_FAIL(push_center(center, distance, save_center_vec, center_vec))) {
      SHARE_LOG(WARN, "fail to push back center with distance", K(ret), K(distance));
    }
  }

  return ret;
}

template <typename VEC_T, typename CENTER_T>
int ObVectorCentorClusterHelper<VEC_T, CENTER_T>::push_center(
  const CENTER_T &center,
  double distance,
  bool save_center_vec /*= false*/,
  VEC_T *center_vec /*= nullptr*/)
{
  int ret = OB_SUCCESS;
  if (max_heap_.count() < nprobe_) {
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
        if (save_center_vec && OB_FAIL(item.vec_dim_.new_from_src(alloc_, center_vec, dim_))) {
          SHARE_LOG(WARN, "failed to new from src", K(ret), K(center_vec));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(max_heap_.push(item))) {
          SHARE_LOG(WARN, "failed to push center heap", K(ret), K(center), K(distance));
        }
      }
    }
  } else {
    const HeapCenterItemTemp &top = max_heap_.top();
    ObCenterWithBuf<CENTER_T> tmp_center_with_buf;
    HeapCenterItemTemp tmp(distance, &tmp_center_with_buf);
    if (max_compare_(tmp, top)) {
      ObCenterWithBuf<CENTER_T> *old_center_with_buf = top.center_with_buf_;
      if (OB_ISNULL(old_center_with_buf)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "center_with_buf is null", K(ret));
      } else if (OB_FAIL(old_center_with_buf->new_from_src(center))) {
        SHARE_LOG(WARN, "failed to new from src", K(ret), K(center));
      } else {
        HeapCenterItemTemp new_top(distance, old_center_with_buf);
        if (save_center_vec) {
          new_top.set_vec_dim(top.vec_dim_);
          if (OB_FAIL(new_top.vec_dim_.reuse_from_src(center_vec, dim_))) {
            SHARE_LOG(WARN, "failed to new from src", K(ret), K(center_vec));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(max_heap_.replace_top(new_top))) {
          SHARE_LOG(WARN, "failed to replace top", K(ret), K(new_top));
        }
      }
    }
  }
  return ret;
}

template <typename VEC_T, typename CENTER_T>
int ObVectorCentorClusterHelper<VEC_T, CENTER_T>::get_nearest_probe_center_ids(ObIArray<CENTER_T> &center_ids)
{
  int ret = OB_SUCCESS;
  if (max_heap_.count() > nprobe_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "max heap count is not equal to nprobe", K(ret), K(max_heap_.count()), K(nprobe_));
  }
  while(OB_SUCC(ret) && !max_heap_.empty()) {
    const HeapCenterItemTemp &cur_top = max_heap_.top();
    if (OB_ISNULL(cur_top.center_with_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "center_with_buf is null", K(ret));
    } else if (OB_FAIL(center_ids.push_back(cur_top.center_with_buf_->get_center()))) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to push center id", K(ret), K(cur_top.center_with_buf_->get_center()));
    } else if (OB_FAIL(max_heap_.pop())) {
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
int ObVectorCentorClusterHelper<VEC_T, CENTER_T>::get_nearest_probe_centers(ObIArray<std::pair<CENTER_T, VEC_T *>> &center_ids)
{
  int ret = OB_SUCCESS;
  if (max_heap_.count() > nprobe_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "max heap count is not equal to nprobe", K(ret), K(max_heap_.count()), K(nprobe_));
  }
  while(OB_SUCC(ret) && !max_heap_.empty()) {
    const HeapCenterItemTemp &cur_top = max_heap_.top();
    if (OB_ISNULL(cur_top.center_with_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "center_with_buf is null", K(ret), K(cur_top));
    } else if (OB_FAIL(center_ids.push_back(std::make_pair(cur_top.center_with_buf_->get_center(), cur_top.vec_dim_.vec_)))) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to push center id", K(ret), K(cur_top));
    } else if (OB_FAIL(max_heap_.pop())) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to pop max heap", K(ret));
    }
  }
  return ret;
}
}
}

#endif
