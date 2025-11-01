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

#pragma once

#include "share/vector/ob_fixed_length_base.h"
#include "share/vector/ob_uniform_base.h"
#include "storage/direct_load/ob_direct_load_vector.h"

namespace oceanbase
{
namespace storage
{
template <typename T>
class ObDirectLoadFixedLengthBase : public ObDirectLoadVector
{
public:
  ObDirectLoadFixedLengthBase(ObFixedLengthBase *fixed_vector)
    : fixed_vector_(fixed_vector),
      vec_data_(reinterpret_cast<T *>(fixed_vector->get_data())),
      data_(vec_data_)
  {
  }
  ~ObDirectLoadFixedLengthBase() override = default;

  ObIVector *get_vector() const override { return fixed_vector_; }
  int64_t memory_usage() const override { return 0; }
  int64_t bytes_usage(const int64_t batch_size) const override { return sizeof(T) * batch_size; }
  void sum_bytes_usage(int64_t *sum_bytes, const int64_t batch_size) const override
  {
    for (int64_t i = 0; i < batch_size; ++i) {
      sum_bytes[i] += sizeof(T);
    }
  }

  int sum_lob_length(int64_t *sum_bytes, const int64_t batch_size) const override
  {
    return OB_ERR_UNEXPECTED;
  }

  void reuse(const int64_t batch_size) override
  {
    // shallow_copy可能会修改data_
    if (data_ != vec_data_) {
      set_vector(vec_data_);
    } else {
      MEMSET(data_, 0, sizeof(T) * batch_size);
    }
  }

  // --------- append interface --------- //
  int append_default(const int64_t batch_idx) override { return OB_SUCCESS; }
  int append_default(const int64_t batch_idx, const int64_t size) override { return OB_SUCCESS; }
  int append_datum(const int64_t batch_idx, const ObDatum &datum)
  {
    data_[batch_idx] = *reinterpret_cast<const T *>(datum.ptr_);
    return OB_SUCCESS;
  }
  int append_batch(const int64_t batch_idx, const ObDirectLoadVector &src, const int64_t offset,
                   const int64_t size) override
  {
    return _append_batch(
      batch_idx, static_cast<const ObDirectLoadFixedLengthBase &>(src).fixed_vector_, offset, size);
  }
  int append_batch(const int64_t batch_idx, ObIVector *src, const int64_t offset,
                   const int64_t size) override
  {
    int ret = OB_SUCCESS;
    const VectorFormat format = src->get_format();
    switch (format) {
      case VEC_FIXED:
        ret = _append_batch(batch_idx, static_cast<ObFixedLengthBase *>(src), offset, size);
        break;
      case VEC_UNIFORM:
        ret = _append_batch<false>(batch_idx, static_cast<ObUniformBase *>(src)->get_datums(),
                                   offset, size);
        break;
      case VEC_UNIFORM_CONST:
        ret = _append_batch<true>(batch_idx, static_cast<ObUniformBase *>(src)->get_datums(),
                                  offset, size);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected vector format", KR(ret), KPC(this), KPC(src), K(format));
        break;
    }
    return ret;
  }
  int append_batch(const int64_t batch_idx, const ObDatumVector &datum_vec, const int64_t offset,
                   const int64_t size) override
  {
    if (datum_vec.is_batch()) {
      return _append_batch<false>(batch_idx, datum_vec.datums_, offset, size);
    } else {
      return _append_batch<true>(batch_idx, datum_vec.datums_, offset, size);
    }
  }
  int append_selective(const int64_t batch_idx, const ObDirectLoadVector &src,
                       const uint16_t *selector, const int64_t size) override
  {
    return _append_selective(batch_idx,
                             static_cast<const ObDirectLoadFixedLengthBase &>(src).fixed_vector_,
                             selector, size);
  }
  int append_selective(const int64_t batch_idx, ObIVector *src, const uint16_t *selector,
                       const int64_t size) override
  {
    int ret = OB_SUCCESS;
    const VectorFormat format = src->get_format();
    switch (format) {
      case VEC_FIXED:
        ret = _append_selective(batch_idx, static_cast<ObFixedLengthBase *>(src), selector, size);
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
        STORAGE_LOG(WARN, "unexpected vector format", KR(ret), KPC(this), KPC(src), K(format));
        break;
    }
    return ret;
  }
  int append_selective(const int64_t batch_idx, const ObDatumVector &datum_vec,
                       const uint16_t *selector, const int64_t size) override
  {
    {
      int ret = OB_SUCCESS;
      if (datum_vec.is_batch()) {
        ret = _append_selective<false>(batch_idx, datum_vec.datums_, selector, size);
      } else {
        ret = _append_selective<true>(batch_idx, datum_vec.datums_, selector, size);
      }
      return ret;
    }
  }

  // --------- set interface --------- //
  int set_default(const int64_t batch_idx) override
  {
    data_[batch_idx] = T();
    return OB_SUCCESS;
  }
  int set_datum(const int64_t batch_idx, const ObDatum &datum) override
  {
    data_[batch_idx] = *reinterpret_cast<const T *>(datum.ptr_);
    return OB_SUCCESS;
  }

  // --------- shallow copy interface --------- //
  int shallow_copy(ObIVector *src, const int64_t batch_size) override
  {
    if (VEC_FIXED == src->get_format()) {
      set_vector(reinterpret_cast<T *>(static_cast<ObFixedLengthBase *>(src)->get_data()));
      return OB_SUCCESS;
    } else {
      return append_batch(0 /*batch_idx*/, src, 0 /*offset*/, batch_size);
    }
  }
  int shallow_copy(const ObDatumVector &datum_vec, const int64_t batch_size) override
  {
    if (datum_vec.is_batch()) {
      return _append_batch<false>(0 /*batch_idx*/, datum_vec.datums_, 0 /*offset*/, batch_size);
    } else {
      return _append_batch<true>(0 /*batch_idx*/, datum_vec.datums_, 0 /*offset*/, batch_size);
    }
  }

  // --------- get interface --------- //
  int get_datum(const int64_t batch_idx, common::ObDatum &datum) override
  {
    datum.set_none();
    datum.ptr_ = reinterpret_cast<char *>(data_ + batch_idx);
    datum.len_ = sizeof(T);
    return OB_SUCCESS;
  }

  VIRTUAL_TO_STRING_KV(KPC_(fixed_vector), KP_(data));

protected:
  template <typename VEC>
  inline int _append_batch(const int64_t batch_idx, VEC *vec, const int64_t offset,
                           const int64_t size)
  {
    return OB_ERR_UNEXPECTED;
  }
  template <>
  inline int _append_batch(const int64_t batch_idx, ObFixedLengthBase *vec, const int64_t offset,
                           const int64_t size)
  {
    T *data = reinterpret_cast<T *>(vec->get_data());
    MEMCPY(data_ + batch_idx, data + offset, sizeof(T) * size);
    return OB_SUCCESS;
  }

  template <bool IS_CONST>
  inline int _append_batch(const int64_t batch_idx, const ObDatum *datums, const int64_t offset,
                           const int64_t size);
  template <>
  inline int _append_batch<false>(const int64_t batch_idx, const ObDatum *datums,
                                  const int64_t offset, const int64_t size)
  {
    for (int64_t src_idx = offset, dest_idx = batch_idx; src_idx < offset + size;
         ++src_idx, ++dest_idx) {
      const ObDatum &datum = datums[src_idx];
      if (!datum.is_null()) {
        data_[dest_idx] = *reinterpret_cast<const T *>(datum.ptr_);
      }
    }
    return OB_SUCCESS;
  }
  template <>
  inline int _append_batch<true>(const int64_t batch_idx, const ObDatum *datums,
                                 const int64_t offset, const int64_t size)
  {
    if (!datums[0].is_null()) {
      const T value = *reinterpret_cast<const T *>(datums[0].ptr_);
      for (int64_t i = 0; i < size; ++i) {
        data_[batch_idx + i] = value;
      }
    }
    return OB_SUCCESS;
  }

  template <typename VEC>
  inline int _append_selective(const int64_t batch_idx, VEC *vec, const uint16_t *selector,
                               const int64_t size)
  {
    return OB_ERR_UNEXPECTED;
  }
  template <>
  inline int _append_selective(const int64_t batch_idx, ObFixedLengthBase *vec,
                               const uint16_t *selector, const int64_t size)
  {
    T *data = reinterpret_cast<T *>(vec->get_data());
    for (int64_t i = 0; i < size; ++i) {
      data_[batch_idx + i] = data[selector[i]];
    }
    return OB_SUCCESS;
  }

  template <bool IS_CONST>
  inline int _append_selective(const int64_t batch_idx, const ObDatum *datums,
                               const uint16_t *selector, const int64_t size);
  template <>
  inline int _append_selective<false>(const int64_t batch_idx, const ObDatum *datums,
                                      const uint16_t *selector, const int64_t size)
  {
    for (int64_t i = 0; i < size; ++i) {
      const int64_t src_idx = selector[i];
      if (!datums[src_idx].is_null()) {
        data_[batch_idx + i] = *reinterpret_cast<const T *>(datums[src_idx].ptr_);
      }
    }
    return OB_SUCCESS;
  }
  template <>
  inline int _append_selective<true>(const int64_t batch_idx, const ObDatum *datums,
                                     const uint16_t *selector, const int64_t size)
  {
    UNUSED(selector);
    return _append_batch<true>(batch_idx, datums, 0 /*offset*/, size);
  }

protected:
  inline void set_vector(T *data)
  {
    data_ = data;
    fixed_vector_->set_data(reinterpret_cast<char *>(data));
  }

protected:
  ObFixedLengthBase *const fixed_vector_;
  T *vec_data_;
  T *data_;
};

template <typename T>
class ObDirectLoadFixedLengthVector final : public ObDirectLoadFixedLengthBase<T>
{
  using SuperClass = ObDirectLoadFixedLengthBase<T>;

public:
  ObDirectLoadFixedLengthVector(ObFixedLengthBase *fixed_vector) : SuperClass(fixed_vector) {}
  ~ObDirectLoadFixedLengthVector() override = default;
};

} // namespace storage
} // namespace oceanbase
