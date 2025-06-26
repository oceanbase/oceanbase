/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_continuous_vector.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector/ob_discrete_base.h"
#include "share/vector/ob_uniform_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadContinuousVector::ObDirectLoadContinuousVector(ObContinuousBase *continuous_vector)
  : continuous_vector_(continuous_vector),
    vec_offsets_(continuous_vector->get_offsets()),
    offsets_(vec_offsets_),
    data_(nullptr),
    buf_(nullptr),
    capacity_(0),
    size_(0)
{
}

ObDirectLoadContinuousVector::~ObDirectLoadContinuousVector()
{
  if (nullptr != buf_) {
    ob_free_align(buf_);
    buf_ = nullptr;
  }
}

void ObDirectLoadContinuousVector::reuse(const int64_t batch_size)
{
  // shallow_copy可能会修改offsets_和data_
  if (offsets_ != vec_offsets_ || data_ != buf_) {
    set_vector(vec_offsets_, buf_);
  } else {
    MEMSET(offsets_, 0, sizeof(uint32_t) * (batch_size + 1));
    if (nullptr != buf_) {
      ob_free_align(buf_);
      buf_ = nullptr;
    }
    capacity_ = 0;
    size_ = 0;
    set_data(buf_);
  }
}

int ObDirectLoadContinuousVector::expand(const int64_t need_size)
{
  int ret = OB_SUCCESS;
  const int64_t need_capacity = size_ + need_size;
  if (OB_UNLIKELY(data_ != buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected data", KR(ret), KP(data_), KP(buf_));
  } else if (need_capacity < capacity_) {
  } else {
    int64_t new_capacity = capacity_ > 0 ? capacity_ * 2 : PAGE_SIZE;
    if (need_capacity > new_capacity) {
      new_capacity = ALIGN_UP(need_capacity, PAGE_SIZE);
    }
    ObMemAttr mem_attr(MTL_ID(), "TLD_Continuous");
    char *new_data = static_cast<char *>(ob_malloc_align(16, new_capacity, mem_attr));
    if (OB_ISNULL(new_data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate mem", KR(ret), K(capacity_), K(size_), K(need_size),
               K(new_capacity));
    } else {
      if (size_ > 0) {
        MEMCPY(new_data, buf_, size_);
      }
      ob_free_align(buf_);
      buf_ = new_data;
      capacity_ = new_capacity;
      set_data(buf_);
    }
  }
  return ret;
}

template <>
inline int ObDirectLoadContinuousVector::_append_batch(const int64_t batch_idx,
                                                       ObContinuousBase *src_vec,
                                                       const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  const uint32_t *offsets = src_vec->get_offsets();
  // 更新offsets
  MEMCPY(offsets_ + batch_idx + 1, offsets + offset + 1, sizeof(uint32_t) * size);
  const int64_t delta = offsets_[batch_idx] - offsets[offset];
  if (delta != 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      offsets_[dest_idx + 1] += delta;
    }
  }
  // 拷贝数据
  const int64_t total_size = offsets[offset + size] - offsets[offset];
  if (total_size <= 0) {
  } else if (OB_FAIL(expand(total_size))) {
    LOG_WARN("fail to expand", KR(ret));
  } else {
    MEMCPY(data_ + size_, src_vec->get_data() + offsets[offset], total_size);
    size_ += total_size;
  }
  return ret;
}

template <>
inline int ObDirectLoadContinuousVector::_append_batch(const int64_t batch_idx,
                                                       ObDiscreteBase *src_vec,
                                                       const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  const ObLength *lens = src_vec->get_lens();
  char **ptrs = src_vec->get_ptrs();
  // 统计total_size
  int64_t total_size = 0;
  for (int64_t src_idx = offset; src_idx < offset + size; ++src_idx) {
    total_size += lens[src_idx];
  }
  // 拷贝数据, 更新offsets
  if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      offsets_[dest_idx + 1] = size_;
    }
  } else if (OB_FAIL(expand(total_size))) {
    LOG_WARN("fail to expand", KR(ret));
  } else {
    for (int64_t src_idx = offset, dest_idx = batch_idx; src_idx < offset + size;
         ++src_idx, ++dest_idx) {
      MEMCPY(data_ + size_, ptrs[src_idx], lens[src_idx]);
      size_ += lens[src_idx];
      offsets_[dest_idx + 1] = size_;
    }
  }
  return ret;
}

template <>
inline int ObDirectLoadContinuousVector::_append_batch<false>(const int64_t batch_idx,
                                                              const ObDatum *datums,
                                                              const int64_t offset,
                                                              const int64_t size)
{
  int ret = OB_SUCCESS;
  // 统计total_size
  int64_t total_size = 0;
  for (int64_t src_idx = offset; src_idx < offset + size; ++src_idx) {
    const ObDatum &datum = datums[src_idx];
    total_size += datum.len_;
  }
  // 拷贝数据
  if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      offsets_[dest_idx + 1] = size_;
    }
  } else if (OB_FAIL(expand(total_size))) {
    LOG_WARN("fail to expand", KR(ret));
  } else {
    for (int64_t src_idx = offset, dest_idx = batch_idx; src_idx < offset + size;
         ++src_idx, ++dest_idx) {
      const ObDatum &datum = datums[src_idx];
      MEMCPY(data_ + size_, datum.ptr_, datum.len_);
      size_ += datum.len_;
      offsets_[dest_idx + 1] = size_;
    }
  }
  return ret;
}

template <>
inline int ObDirectLoadContinuousVector::_append_batch<true>(const int64_t batch_idx,
                                                             const ObDatum *datums,
                                                             const int64_t offset,
                                                             const int64_t size)
{
  int ret = OB_SUCCESS;
  const ObDatum &datum = datums[0];
  int64_t total_size = datum.len_ * size;
  if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      offsets_[dest_idx + 1] = size_;
    }
  } else if (OB_FAIL(expand(total_size))) {
    LOG_WARN("fail to expand", KR(ret));
  } else {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      MEMCPY(data_ + size_, datum.ptr_, datum.len_);
      size_ += datum.len_;
      offsets_[dest_idx + 1] = size_;
    }
  }
  return ret;
}

template <>
inline int ObDirectLoadContinuousVector::_append_selective(const int64_t batch_idx,
                                                           ObContinuousBase *src_vec,
                                                           const uint16_t *selector,
                                                           const int64_t size)
{
  int ret = OB_SUCCESS;
  const uint32_t *offsets = src_vec->get_offsets();
  const char *data = src_vec->get_data();
  // 统计total_size
  int64_t total_size = 0;
  for (int64_t i = 0; i < size; ++i) {
    const int64_t src_idx = selector[i];
    total_size += (offsets[src_idx + 1] - offsets[src_idx]);
  }
  // 拷贝数据, 更新offsets
  if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      offsets_[dest_idx + 1] = size_;
    }
  } else if (OB_FAIL(expand(total_size))) {
    LOG_WARN("fail to expand", KR(ret));
  } else {
    for (int64_t i = 0, dest_idx = batch_idx; i < size; ++i, ++dest_idx) {
      const int64_t src_idx = selector[i];
      const uint32_t len = offsets[src_idx + 1] - offsets[src_idx];
      MEMCPY(data_ + size_, data + offsets[src_idx], len);
      size_ += len;
      offsets_[dest_idx + 1] = size_;
    }
  }
  return ret;
}

template <>
inline int ObDirectLoadContinuousVector::_append_selective(const int64_t batch_idx,
                                                           ObDiscreteBase *src_vec,
                                                           const uint16_t *selector,
                                                           const int64_t size)
{
  int ret = OB_SUCCESS;
  const ObLength *lens = src_vec->get_lens();
  char **ptrs = src_vec->get_ptrs();
  // 统计total_size
  int64_t total_size = 0;
  for (int64_t i = 0; i < size; ++i) {
    const int64_t src_idx = selector[i];
    total_size += lens[src_idx];
  }
  // 拷贝数据, 更新offsets
  if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      offsets_[dest_idx + 1] = size_;
    }
  } else if (OB_FAIL(expand(total_size))) {
    LOG_WARN("fail to expand", KR(ret));
  } else {
    for (int64_t i = 0, dest_idx = batch_idx; i < size; ++i, ++dest_idx) {
      const int64_t src_idx = selector[i];
      MEMCPY(data_ + size_, ptrs[src_idx], lens[src_idx]);
      size_ += lens[src_idx];
      offsets_[dest_idx + 1] = size_;
    }
  }
  return ret;
}

template <>
inline int ObDirectLoadContinuousVector::_append_selective<false>(const int64_t batch_idx,
                                                                  const ObDatum *datums,
                                                                  const uint16_t *selector,
                                                                  const int64_t size)
{
  int ret = OB_SUCCESS;
  // 统计total_size
  int64_t total_size = 0;
  for (int64_t i = 0; i < size; ++i) {
    const int64_t src_idx = selector[i];
    total_size += datums[src_idx].len_;
  }
  // 拷贝数据, 更新offsets
  if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      offsets_[dest_idx + 1] = size_;
    }
  } else if (OB_FAIL(expand(total_size))) {
    LOG_WARN("fail to expand", KR(ret));
  } else {
    for (int64_t i = 0, dest_idx = batch_idx; i < size; ++i, ++dest_idx) {
      const int64_t src_idx = selector[i];
      const ObDatum &datum = datums[src_idx];
      MEMCPY(data_ + size_, datum.ptr_, datum.len_);
      size_ += datum.len_;
      offsets_[dest_idx + 1] = size_;
    }
  }
  return ret;
}

template <>
inline int ObDirectLoadContinuousVector::_append_selective<true>(const int64_t batch_idx,
                                                                 const ObDatum *datums,
                                                                 const uint16_t *selector,
                                                                 const int64_t size)
{
  UNUSED(selector);
  return _append_batch<true>(batch_idx, datums, 0 /*offset*/, size);
}

template <>
inline int ObDirectLoadContinuousVector::_shallow_copy(ObContinuousBase *src_vec,
                                                       const int64_t batch_size)
{
  set_vector(src_vec->get_offsets(), src_vec->get_data());
  return OB_SUCCESS;
}

template <>
inline int ObDirectLoadContinuousVector::_shallow_copy(ObDiscreteBase *src_vec,
                                                       const int64_t batch_size)
{
  return _append_batch(0 /*batch_idx*/, src_vec, 0 /*offset*/, batch_size);
}

template <>
inline int ObDirectLoadContinuousVector::_shallow_copy<false>(const ObDatum *datums,
                                                              const int64_t batch_size)
{
  return _append_batch<false>(0 /*batch_idx*/, datums, 0 /*offset*/, batch_size);
}

template <>
inline int ObDirectLoadContinuousVector::_shallow_copy<true>(const ObDatum *datums,
                                                             const int64_t batch_size)
{
  return _append_batch<true>(0 /*batch_idx*/, datums, 0 /*offset*/, batch_size);
}

int ObDirectLoadContinuousVector::append_datum(const int64_t batch_idx, const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  uint32_t *offsets = continuous_vector_->get_offsets();
  if (datum.len_ <= 0) {
  } else if (OB_FAIL(expand(datum.len_))) {
    LOG_WARN("fail to expand", KR(ret));
  } else {
    MEMCPY(data_ + offsets[batch_idx], datum.ptr_, datum.len_);
    size_ += datum.len_;
  }
  offsets[batch_idx + 1] = size_;
  return ret;
}

int ObDirectLoadContinuousVector::append_batch(const int64_t batch_idx,
                                               const ObDirectLoadVector &src, const int64_t offset,
                                               const int64_t size)
{
  return _append_batch(batch_idx,
                       static_cast<const ObDirectLoadContinuousVector &>(src).continuous_vector_,
                       offset, size);
}

int ObDirectLoadContinuousVector::append_batch(const int64_t batch_idx, ObIVector *src,
                                               const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  const VectorFormat format = src->get_format();
  switch (format) {
    case VEC_CONTINUOUS:
      ret = _append_batch(batch_idx, static_cast<ObContinuousBase *>(src), offset, size);
      break;
    case VEC_DISCRETE:
      ret = _append_batch(batch_idx, static_cast<ObDiscreteBase *>(src), offset, size);
      break;

    case VEC_UNIFORM:
      ret = _append_batch<false>(batch_idx, static_cast<ObUniformBase *>(src)->get_datums(), offset,
                                 size);
      break;
    case VEC_UNIFORM_CONST:
      ret = _append_batch<true>(batch_idx, static_cast<ObUniformBase *>(src)->get_datums(), offset,
                                size);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", KR(ret), KPC(this), KPC(src), K(format));
      break;
  }
  return ret;
}

int ObDirectLoadContinuousVector::append_batch(const int64_t batch_idx,
                                               const ObDatumVector &datum_vec, const int64_t offset,
                                               const int64_t size)
{
  if (datum_vec.is_batch()) {
    return _append_batch<false>(batch_idx, datum_vec.datums_, offset, size);
  } else {
    return _append_batch<true>(batch_idx, datum_vec.datums_, offset, size);
  }
}

int ObDirectLoadContinuousVector::append_selective(const int64_t batch_idx,
                                                   const ObDirectLoadVector &src,
                                                   const uint16_t *selector, const int64_t size)
{
  return _append_selective(
    batch_idx, static_cast<const ObDirectLoadContinuousVector &>(src).continuous_vector_, selector,
    size);
}

int ObDirectLoadContinuousVector::append_selective(const int64_t batch_idx, ObIVector *src,
                                                   const uint16_t *selector, const int64_t size)
{
  int ret = OB_SUCCESS;
  const VectorFormat format = src->get_format();
  switch (format) {
    case VEC_CONTINUOUS:
      ret = _append_selective(batch_idx, static_cast<ObContinuousBase *>(src), selector, size);
      break;
    case VEC_DISCRETE:
      ret = _append_selective(batch_idx, static_cast<ObDiscreteBase *>(src), selector, size);
      break;
    case VEC_UNIFORM:
      ret = _append_selective<false>(batch_idx, static_cast<ObUniformBase *>(src)->get_datums(),
                                     selector, size);
      break;
    case VEC_UNIFORM_CONST:
      ret = _append_selective<true>(batch_idx, static_cast<ObUniformBase *>(src)->get_datums(),
                                    selector, size);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", KR(ret), KPC(this), KPC(src), K(format));
      break;
  }
  return ret;
}

int ObDirectLoadContinuousVector::append_selective(const int64_t batch_idx,
                                                   const ObDatumVector &datum_vec,
                                                   const uint16_t *selector, const int64_t size)
{
  if (datum_vec.is_batch()) {
    return _append_selective<false>(batch_idx, datum_vec.datums_, selector, size);
  } else {
    return _append_selective<true>(batch_idx, datum_vec.datums_, selector, size);
  }
}

int ObDirectLoadContinuousVector::shallow_copy(ObIVector *src, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const VectorFormat format = src->get_format();
  switch (format) {
    case VEC_CONTINUOUS:
      ret = _shallow_copy(static_cast<ObContinuousBase *>(src), batch_size);
      break;
    case VEC_DISCRETE:
      ret = _shallow_copy(static_cast<ObDiscreteBase *>(src), batch_size);
      break;
    case VEC_UNIFORM:
      ret = _shallow_copy<false>(static_cast<ObUniformBase *>(src)->get_datums(), batch_size);
      break;
    case VEC_UNIFORM_CONST:
      ret = _shallow_copy<true>(static_cast<ObUniformBase *>(src)->get_datums(), batch_size);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", KR(ret), KPC(this), KPC(src), K(format));
      break;
  }
  return ret;
}

int ObDirectLoadContinuousVector::shallow_copy(const ObDatumVector &datum_vec,
                                               const int64_t batch_size)
{
  if (datum_vec.is_batch()) {
    return _shallow_copy<false>(datum_vec.datums_, batch_size);
  } else {
    return _shallow_copy<true>(datum_vec.datums_, batch_size);
  }
}

} // namespace storage
} // namespace oceanbase
