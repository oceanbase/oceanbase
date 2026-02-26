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

#include "storage/direct_load/ob_direct_load_discrete_vector.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector/ob_continuous_base.h"
#include "share/vector/ob_uniform_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadDiscreteVector::ObDirectLoadDiscreteVector(ObDiscreteBase *discrete_vector)
  : discrete_vector_(discrete_vector),
    vec_lens_(discrete_vector->get_lens()),
    vec_ptrs_(discrete_vector->get_ptrs()),
    lens_(vec_lens_),
    ptrs_(vec_ptrs_),
    allocator_("TLD_Discrete")
{
  allocator_.set_tenant_id(MTL_ID());
}

void ObDirectLoadDiscreteVector::reuse(const int64_t batch_size)
{
  // shallow_copy可能会修改lens_和ptrs_
  if (lens_ != vec_lens_ || ptrs_ != vec_ptrs_) {
    set_vector(vec_lens_, vec_ptrs_);
  } else {
    MEMSET(lens_, 0, sizeof(ObLength) * batch_size);
    MEMSET(ptrs_, 0, sizeof(char *) * batch_size);
  }
  allocator_.reset_remain_one_page();
}

template <>
int ObDirectLoadDiscreteVector::_append_batch(const int64_t batch_idx, ObContinuousBase *src_vec,
                                              const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  const uint32_t *offsets = src_vec->get_offsets();
  const char *data = src_vec->get_data();
  // 计算total_size
  const int64_t total_size = offsets[offset + size] - offsets[offset];
  char *buf = nullptr;
  if (OB_ISNULL(buf = alloc_buf(total_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc buf", KR(ret), K(total_size));
  } else if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      lens_[dest_idx] = 0;
      ptrs_[dest_idx] = buf;
    }
  } else {
    MEMCPY(buf, data + offsets[offset], total_size);
    for (int64_t src_idx = offset, dest_idx = batch_idx; src_idx < offset + size;
         ++src_idx, ++dest_idx) {
      lens_[dest_idx] = offsets[src_idx + 1] - offsets[src_idx];
      ptrs_[dest_idx] = buf + (offsets[src_idx] - offsets[offset]);
    }
  }
  return ret;
}

template <>
int ObDirectLoadDiscreteVector::_append_batch(const int64_t batch_idx, ObDiscreteBase *src_vec,
                                              const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObDiscreteBase *dest_vec = discrete_vector_;
  ObLength *lens = src_vec->get_lens();
  char **ptrs = src_vec->get_ptrs();
  // 统计total_size
  int64_t total_size = 0;
  for (int64_t src_idx = offset; src_idx < offset + size; ++src_idx) {
    total_size += lens[src_idx];
  }
  // 更新lens
  MEMCPY(lens_ + batch_idx, lens + offset, sizeof(ObLength) * size);
  // 拷贝数据, 更新ptrs
  char *buf = nullptr;
  if (OB_ISNULL(buf = alloc_buf(total_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc buf", KR(ret), K(total_size));
  } else if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      ptrs_[dest_idx] = buf;
    }
  } else {
    for (int64_t src_idx = offset, dest_idx = batch_idx; src_idx < offset + size;
         ++src_idx, ++dest_idx) {
      ptrs_[dest_idx] = buf;
      MEMCPY(buf, ptrs[src_idx], lens[src_idx]);
      buf += lens[src_idx];
    }
  }
  return ret;
}

template <>
int ObDirectLoadDiscreteVector::_append_batch<false>(const int64_t batch_idx, const ObDatum *datums,
                                                     const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  // 统计total_size
  int64_t total_size = 0;
  for (int64_t src_idx = offset; src_idx < offset + size; ++src_idx) {
    total_size += datums[src_idx].len_;
  }
  // 拷贝数据, 更新ptrs, lens
  char *buf = nullptr;
  if (OB_ISNULL(buf = alloc_buf(total_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(INFO, "fail to alloc buf", KR(ret), K(total_size));
  } else {
    for (int64_t src_idx = offset, dest_idx = batch_idx; src_idx < offset + size;
         ++src_idx, ++dest_idx) {
      const ObDatum &datum = datums[src_idx];
      lens_[dest_idx] = datum.len_;
      ptrs_[dest_idx] = buf;
      MEMCPY(buf, datum.ptr_, datum.len_);
      buf += datum.len_;
    }
  }
  return ret;
}

template <>
int ObDirectLoadDiscreteVector::_append_batch<true>(const int64_t batch_idx, const ObDatum *datums,
                                                    const int64_t offset, const int64_t size)
{
  UNUSED(offset);
  int ret = OB_SUCCESS;
  const ObDatum &datum = datums[0];
  char *buf = nullptr;
  if (OB_ISNULL(buf = alloc_buf(datum.len_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(INFO, "fail to alloc buf", KR(ret), K(datum.len_));
  } else {
    MEMCPY(buf, datum.ptr_, datum.len_);
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      lens_[dest_idx] = datum.len_;
      ptrs_[dest_idx] = buf;
    }
  }
  return ret;
}

template <>
inline int ObDirectLoadDiscreteVector::_append_selective(const int64_t batch_idx,
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
    total_size += offsets[src_idx + 1] - offsets[src_idx];
  }
  // 拷贝数据, 更新lens, ptrs
  char *buf = nullptr;
  if (OB_ISNULL(buf = alloc_buf(total_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc buf", KR(ret), K(total_size));
  } else if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      lens_[dest_idx] = 0;
      ptrs_[dest_idx] = buf;
    }
  } else {
    for (int64_t i = 0, dest_idx = batch_idx; i < size; ++i, ++dest_idx) {
      const int64_t src_idx = selector[i];
      const ObLength len = offsets[src_idx + 1] - offsets[src_idx];
      lens_[dest_idx] = len;
      ptrs_[dest_idx] = buf;
      MEMCPY(buf, data + offsets[src_idx], len);
      buf += len;
    }
  }
  return ret;
}
template <>
inline int ObDirectLoadDiscreteVector::_append_selective(const int64_t batch_idx,
                                                         ObDiscreteBase *src_vec,
                                                         const uint16_t *selector,
                                                         const int64_t size)
{
  int ret = OB_SUCCESS;
  ObLength *lens = src_vec->get_lens();
  char **ptrs = src_vec->get_ptrs();
  // 统计total_size
  int64_t total_size = 0;
  for (int64_t i = 0; i < size; ++i) {
    const int64_t src_idx = selector[i];
    total_size += lens[src_idx];
  }
  // 拷贝数据, 更新lens, ptrs
  char *buf = nullptr;
  if (OB_ISNULL(buf = alloc_buf(total_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc buf", KR(ret), K(total_size));
  } else if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      lens_[dest_idx] = 0;
      ptrs_[dest_idx] = buf;
    }
  } else {
    for (int64_t i = 0, dest_idx = batch_idx; i < size; ++i, ++dest_idx) {
      const int64_t src_idx = selector[i];
      lens_[dest_idx] = lens[src_idx];
      ptrs_[dest_idx] = buf;
      MEMCPY(buf, ptrs[src_idx], lens[src_idx]);
      buf += lens[src_idx];
    }
  }
  return ret;
}

template <>
inline int ObDirectLoadDiscreteVector::_append_selective<false>(const int64_t batch_idx,
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
  // 拷贝数据, 更新ptrs, lens
  char *buf = nullptr;
  if (OB_ISNULL(buf = alloc_buf(total_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc buf", KR(ret), K(total_size));
  } else if (total_size == 0) {
    for (int64_t dest_idx = batch_idx; dest_idx < batch_idx + size; ++dest_idx) {
      lens_[dest_idx] = 0;
      ptrs_[dest_idx] = buf;
    }
  } else {
    for (int64_t i = 0, dest_idx = batch_idx; i < size; ++i, ++dest_idx) {
      const int64_t src_idx = selector[i];
      const ObDatum &datum = datums[src_idx];
      lens_[dest_idx] = datum.len_;
      ptrs_[dest_idx] = buf;
      MEMCPY(buf, datum.ptr_, datum.len_);
      buf += datum.len_;
    }
  }
  return ret;
}
template <>
inline int ObDirectLoadDiscreteVector::_append_selective<true>(const int64_t batch_idx,
                                                               const ObDatum *datums,
                                                               const uint16_t *selector,
                                                               const int64_t size)
{
  UNUSED(selector);
  return _append_batch<true>(batch_idx, datums, 0 /*offset*/, size);
}

template <>
inline int ObDirectLoadDiscreteVector::_shallow_copy(ObContinuousBase *vec,
                                                     const int64_t batch_size)
{
  uint32_t *offsets = vec->get_offsets();
  char *data = vec->get_data();
  for (int64_t i = 0; i < batch_size; ++i) {
    lens_[i] = offsets[i + 1] - offsets[i];
    ptrs_[i] = data + offsets[i];
  }
  return OB_SUCCESS;
}
template <>
inline int ObDirectLoadDiscreteVector::_shallow_copy(ObDiscreteBase *vec, const int64_t batch_size)
{
  set_vector(vec->get_lens(), vec->get_ptrs());
  return OB_SUCCESS;
}

template <>
inline int ObDirectLoadDiscreteVector::_shallow_copy<false>(const ObDatum *datums,
                                                            const int64_t batch_size)
{
  for (int64_t i = 0; i < batch_size; ++i) {
    const ObDatum &datum = datums[i];
    lens_[i] = datum.len_;
    ptrs_[i] = const_cast<char *>(datum.ptr_);
  }
  return OB_SUCCESS;
}

template <>
inline int ObDirectLoadDiscreteVector::_shallow_copy<true>(const ObDatum *datums,
                                                           const int64_t batch_size)
{
  const ObDatum &datum = datums[0];
  for (int64_t i = 0; i < batch_size; ++i) {
    lens_[i] = datum.len_;
    ptrs_[i] = const_cast<char *>(datum.ptr_);
  }
  return OB_SUCCESS;
}

int ObDirectLoadDiscreteVector::append_datum(const int64_t batch_idx, const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (OB_ISNULL(buf = alloc_buf(datum.len_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(datum.len_));
  } else {
    MEMCPY(buf, datum.ptr_, datum.len_);
    lens_[batch_idx] = datum.len_;
    ptrs_[batch_idx] = buf;
  }
  return ret;
}

int ObDirectLoadDiscreteVector::append_batch(const int64_t batch_idx, const ObDirectLoadVector &src,
                                             const int64_t offset, const int64_t size)
{
  return _append_batch(
    batch_idx, static_cast<const ObDirectLoadDiscreteVector &>(src).discrete_vector_, offset, size);
}

int ObDirectLoadDiscreteVector::append_batch(const int64_t batch_idx, ObIVector *src,
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

int ObDirectLoadDiscreteVector::append_batch(const int64_t batch_idx,
                                             const ObDatumVector &datum_vec, const int64_t offset,
                                             const int64_t size)
{
  if (datum_vec.is_batch()) {
    return _append_batch<false>(batch_idx, datum_vec.datums_, offset, size);
  } else {
    return _append_batch<true>(batch_idx, datum_vec.datums_, offset, size);
  }
}

int ObDirectLoadDiscreteVector::append_selective(const int64_t batch_idx,
                                                 const ObDirectLoadVector &src,
                                                 const uint16_t *selector, const int64_t size)
{
  return _append_selective(batch_idx,
                           static_cast<const ObDirectLoadDiscreteVector &>(src).discrete_vector_,
                           selector, size);
}

int ObDirectLoadDiscreteVector::append_selective(const int64_t batch_idx, ObIVector *src,
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

int ObDirectLoadDiscreteVector::append_selective(const int64_t batch_idx,
                                                 const ObDatumVector &datum_vec,
                                                 const uint16_t *selector, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (datum_vec.is_batch()) {
    ret = _append_selective<false>(batch_idx, datum_vec.datums_, selector, size);
  } else {
    ret = _append_selective<true>(batch_idx, datum_vec.datums_, selector, size);
  }
  return ret;
}

int ObDirectLoadDiscreteVector::shallow_copy(ObIVector *src, const int64_t batch_size)
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

int ObDirectLoadDiscreteVector::shallow_copy(const ObDatumVector &datum_vec,
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
