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

#include "share/vector/ob_bitmap_null_vector_base.h"
#include "share/vector/ob_discrete_base.h"
#include "share/vector/ob_uniform_base.h"
#include "storage/direct_load/ob_direct_load_vector.h"

namespace oceanbase
{
namespace storage
{
template <typename T, typename V>
class ObDirectLoadNullableVector final : public ObDirectLoadVector
{
public:
  ObDirectLoadNullableVector(V *vector)
    : data_vector_(vector),
      base_(static_cast<ObBitmapNullVectorBase *>(vector)),
      vec_nulls_(base_->get_nulls()),
      nulls_(vec_nulls_),
      is_all_null_(true)
  {
  }
  ~ObDirectLoadNullableVector() override = default;

  ObIVector *get_vector() const override { return base_; }
  int64_t memory_usage() const override { return data_vector_.memory_usage(); }
  int64_t bytes_usage(const int64_t batch_size) const override
  {
    return data_vector_.bytes_usage(batch_size);
  }
  void sum_bytes_usage(int64_t *sum_bytes, const int64_t batch_size) const override
  {
    data_vector_.sum_bytes_usage(sum_bytes, batch_size);
  }

  int sum_lob_length(int64_t *sum_bytes, const int64_t batch_size) const override
  {
    return data_vector_.sum_lob_length(sum_bytes, batch_size);
  }

  void reuse(const int64_t batch_size) override
  {
    data_vector_.reuse(batch_size);
    // shallow_copy可能会修改nulls_
    if (nulls_ != vec_nulls_) {
      set_vector(vec_nulls_, 0 /*flag*/);
    } else {
      nulls_->reset(batch_size);
    }
    base_->reset_flag();
    is_all_null_ = true;
  }

  // --------- append interface --------- //
  int append_default(const int64_t batch_idx) override
  {
    base_->set_null(batch_idx);
    return data_vector_.append_default(batch_idx);
  }
  int append_default(const int64_t batch_idx, const int64_t size) override
  {
    base_->get_nulls()->set_all(batch_idx, batch_idx + size);
    base_->set_has_null();
    return data_vector_.append_default(batch_idx, size);
  }
  int append_datum(const int64_t batch_idx, const ObDatum &datum) override
  {
    if (datum.is_null()) {
      return append_default(batch_idx);
    } else {
      is_all_null_ = false;
      return data_vector_.append_datum(batch_idx, datum);
    }
  }
  int append_batch(const int64_t batch_idx, const ObDirectLoadVector &src, const int64_t offset,
                   const int64_t size) override
  {
    const ObDirectLoadNullableVector &nullable_vector =
      static_cast<const ObDirectLoadNullableVector &>(src);
    if (nullable_vector.is_all_null_) {
      return append_default(batch_idx, size);
    } else {
      _append_batch(batch_idx, nullable_vector.base_, offset, size);
      return data_vector_.append_batch(batch_idx, nullable_vector.data_vector_, offset, size);
    }
  }
  int append_batch(const int64_t batch_idx, ObIVector *src, const int64_t offset,
                   const int64_t size) override
  {
    int ret = OB_SUCCESS;
    const VectorFormat format = src->get_format();
    switch (format) {
      case VEC_FIXED:
      case VEC_CONTINUOUS:
        _append_batch(batch_idx, static_cast<ObBitmapNullVectorBase *>(src), offset, size);
        break;
      case VEC_DISCRETE:
        _adapt_vector_batch(static_cast<ObDiscreteBase *>(src), offset, size);
        _append_batch(batch_idx, static_cast<ObBitmapNullVectorBase *>(src), offset, size);
        break;
      case VEC_UNIFORM:
        _append_batch<false>(batch_idx, static_cast<ObUniformBase *>(src)->get_datums(), offset,
                             size);
        break;
      case VEC_UNIFORM_CONST:
        _append_batch<true>(batch_idx, static_cast<ObUniformBase *>(src)->get_datums(), offset,
                            size);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected vector format", KR(ret), KPC(this), KPC(src), K(format));
        break;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(data_vector_.append_batch(batch_idx, src, offset, size))) {
        STORAGE_LOG(WARN, "fail to append batch", KR(ret));
      }
    }
    return ret;
  }
  int append_batch(const int64_t batch_idx, const ObDatumVector &datum_vec, const int64_t offset,
                   const int64_t size) override
  {
    if (datum_vec.is_batch()) {
      _append_batch<false>(batch_idx, datum_vec.datums_, offset, size);
    } else {
      _append_batch<true>(batch_idx, datum_vec.datums_, offset, size);
    }
    return data_vector_.append_batch(batch_idx, datum_vec, offset, size);
  }
  int append_selective(const int64_t batch_idx, const ObDirectLoadVector &src,
                       const uint16_t *selector, const int64_t size) override
  {
    const ObDirectLoadNullableVector &nullable_vector =
      static_cast<const ObDirectLoadNullableVector &>(src);
    if (nullable_vector.is_all_null_) {
      return append_default(batch_idx, size);
    } else {
      _append_selective(batch_idx, nullable_vector.base_, selector, size);
      return data_vector_.append_selective(batch_idx, nullable_vector.data_vector_, selector, size);
    }
  }
  int append_selective(const int64_t batch_idx, ObIVector *src, const uint16_t *selector,
                       const int64_t size) override
  {
    int ret = OB_SUCCESS;
    const VectorFormat format = src->get_format();
    switch (format) {
      case VEC_FIXED:
      case VEC_CONTINUOUS:
        _append_selective(batch_idx, static_cast<ObBitmapNullVectorBase *>(src), selector, size);
        break;
      case VEC_DISCRETE:
        _adapt_vector_selective(static_cast<ObDiscreteBase *>(src), selector, size);
        _append_selective(batch_idx, static_cast<ObBitmapNullVectorBase *>(src), selector, size);
        break;
      case VEC_UNIFORM:
        _append_selective<false>(batch_idx, static_cast<ObUniformBase *>(src)->get_datums(),
                                 selector, size);
        break;
      case VEC_UNIFORM_CONST:
        _append_selective<true>(batch_idx, static_cast<ObUniformBase *>(src)->get_datums(),
                                selector, size);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected vector format", KR(ret), KPC(this), KPC(src), K(format));
        break;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(data_vector_.append_selective(batch_idx, src, selector, size))) {
        STORAGE_LOG(WARN, "fail to append selective", KR(ret));
      }
    }
    return ret;
  }
  int append_selective(const int64_t batch_idx, const ObDatumVector &datum_vec,
                       const uint16_t *selector, const int64_t size) override
  {
    if (datum_vec.is_batch()) {
      _append_selective<false>(batch_idx, datum_vec.datums_, selector, size);
    } else {
      _append_selective<true>(batch_idx, datum_vec.datums_, selector, size);
    }
    return data_vector_.append_selective(batch_idx, datum_vec, selector, size);
  }

  // --------- set interface --------- //
  int set_all_null(const int64_t batch_size) override
  {
    base_->set_has_null();
    base_->get_nulls()->set_all(batch_size);
    is_all_null_ = true;
    return OB_SUCCESS;
  }
  int set_default(const int64_t batch_idx) override
  {
    base_->set_null(batch_idx);
    return data_vector_.set_default(batch_idx);
  }
  int set_datum(const int64_t batch_idx, const ObDatum &datum) override
  {
    if (datum.is_null()) {
      return set_default(batch_idx);
    } else {
      is_all_null_ = false;
      return data_vector_.set_datum(batch_idx, datum);
    }
  }

  // --------- shallow copy interface --------- //
  int shallow_copy(ObIVector *src, const int64_t batch_size) override
  {
    int ret = OB_SUCCESS;
    const VectorFormat format = src->get_format();
    switch (format) {
      case VEC_FIXED:
      case VEC_CONTINUOUS:
        _shallow_copy(static_cast<ObBitmapNullVectorBase *>(src), batch_size);
        break;
      case VEC_DISCRETE:
        _adapt_vector_batch(static_cast<ObDiscreteBase *>(src), 0 /*offset*/, batch_size);
        _shallow_copy(static_cast<ObBitmapNullVectorBase *>(src), batch_size);
        break;
      case VEC_UNIFORM:
        _shallow_copy<false>(static_cast<ObUniformBase *>(src)->get_datums(), batch_size);
        break;
      case VEC_UNIFORM_CONST:
        _shallow_copy<true>(static_cast<ObUniformBase *>(src)->get_datums(), batch_size);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected vector format", KR(ret), KPC(this), KPC(src), K(format));
        break;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(data_vector_.shallow_copy(src, batch_size))) {
        STORAGE_LOG(WARN, "fail to shallow copy", KR(ret));
      }
    }
    return ret;
  }
  int shallow_copy(const ObDatumVector &datum_vec, const int64_t batch_size) override
  {
    if (datum_vec.is_batch()) {
      _shallow_copy<false>(datum_vec.datums_, batch_size);
    } else {
      _shallow_copy<true>(datum_vec.datums_, batch_size);
    }
    return data_vector_.shallow_copy(datum_vec, batch_size);
  }

  // --------- get interface --------- //
  int get_datum(const int64_t batch_idx, ObDatum &datum) override
  {
    int ret = OB_SUCCESS;
    if (base_->is_null(batch_idx)) {
      datum.set_null();
    } else {
      ret = data_vector_.get_datum(batch_idx, datum);
    }
    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(data_vector), KP_(base), K_(is_all_null));

private:
  template <typename VEC>
  inline void _append_batch(const int64_t batch_idx, VEC *vec, const int64_t offset,
                            const int64_t size);
  template <>
  inline void _append_batch(const int64_t batch_idx, ObBitmapNullVectorBase *base,
                            const int64_t offset, const int64_t size)
  {
    const int64_t null_cnt =
      base->has_null() ? base->get_nulls()->accumulate_bit_cnt(offset, offset + size) : 0;
    if (0 == null_cnt) {
      // 全是notnull
      is_all_null_ = false;
    } else if (size == null_cnt) {
      // 全是null
      base_->get_nulls()->set_all(batch_idx, batch_idx + size);
      base_->set_has_null(true);
    } else {
      // 部分null
      sql::ObBitVector *dest_nulls = base_->get_nulls();
      sql::ObBitVector *src_nulls = base->get_nulls();
      for (int64_t src_idx = offset, dest_idx = batch_idx; src_idx < offset + size;
           ++src_idx, ++dest_idx) {
        dest_nulls->bit_or_assign(dest_idx, src_nulls->at(src_idx));
      }
      base_->set_has_null(true);
      is_all_null_ = false;
    }
  }

  template <bool IS_CONST>
  inline void _append_batch(const int64_t batch_idx, const ObDatum *datums, const int64_t offset,
                            const int64_t size);
  template <>
  inline void _append_batch<false>(const int64_t batch_idx, const ObDatum *datums,
                                   const int64_t offset, const int64_t size)
  {
    sql::ObBitVector *nulls = base_->get_nulls();
    for (int64_t src_idx = offset, dest_idx = batch_idx; src_idx < offset + size;
         ++src_idx, ++dest_idx) {
      nulls->bit_or_assign(dest_idx, datums[src_idx].is_null());
    }
    const int64_t null_cnt = nulls->accumulate_bit_cnt(batch_idx, batch_idx + size);
    base_->set_has_null(base_->has_null() || null_cnt > 0);
    is_all_null_ = is_all_null_ && size == null_cnt;
  }
  template <>
  inline void _append_batch<true>(const int64_t batch_idx, const ObDatum *datums,
                                  const int64_t offset, const int64_t size)
  {
    UNUSED(offset);
    const ObDatum &datum = datums[0];
    if (datum.is_null()) {
      base_->get_nulls()->set_all(batch_idx, batch_idx + size);
      base_->set_has_null(true);
    } else {
      is_all_null_ = false;
    }
  }

  template <typename VEC>
  inline void _append_selective(const int64_t batch_idx, VEC *vec, const uint16_t *selector,
                                const int64_t size);
  template <>
  inline void _append_selective(const int64_t batch_idx, ObBitmapNullVectorBase *base,
                                const uint16_t *selector, const int64_t size)
  {
    if (base->has_null()) {
      sql::ObBitVector *dest_nulls = base_->get_nulls();
      sql::ObBitVector *src_nulls = base->get_nulls();
      for (int64_t i = 0, dest_idx = batch_idx; i < size; ++i, ++dest_idx) {
        const int64_t src_idx = selector[i];
        dest_nulls->bit_or_assign(dest_idx, src_nulls->at(src_idx));
      }
      const int64_t null_cnt = dest_nulls->accumulate_bit_cnt(batch_idx, batch_idx + size);
      base_->set_has_null(base_->has_null() || null_cnt > 0);
      is_all_null_ = is_all_null_ && size == null_cnt;
    } else {
      // 全是notnull
      is_all_null_ = false;
    }
  }

  template <bool IS_CONST>
  inline void _append_selective(const int64_t batch_idx, const ObDatum *datums,
                                const uint16_t *selector, const int64_t size);
  template <>
  inline void _append_selective<false>(const int64_t batch_idx, const ObDatum *datums,
                                       const uint16_t *selector, const int64_t size)
  {
    sql::ObBitVector *nulls = base_->get_nulls();
    for (int64_t i = 0, dest_idx = batch_idx; i < size; ++i, ++dest_idx) {
      const int64_t src_idx = selector[i];
      nulls->bit_or_assign(dest_idx, datums[src_idx].is_null());
    }
    const int64_t null_cnt = nulls->accumulate_bit_cnt(batch_idx, batch_idx + size);
    base_->set_has_null(base_->has_null() || null_cnt > 0);
    is_all_null_ = is_all_null_ && size == null_cnt;
  }
  template <>
  inline void _append_selective<true>(const int64_t batch_idx, const ObDatum *datums,
                                      const uint16_t *selector, const int64_t size)
  {
    UNUSED(selector);
    _append_batch<true>(batch_idx, datums, 0 /*offset*/, size);
  }

  template <typename VEC>
  inline void _shallow_copy(VEC *vec, const int64_t batch_size);
  template <>
  inline void _shallow_copy(ObBitmapNullVectorBase *base, const int64_t batch_size)
  {
    set_vector(base->get_nulls(), base->get_flag());
    is_all_null_ = base_->has_null() ? nulls_->is_all_true(batch_size) : false;
  }

  template <bool IS_CONST>
  inline void _shallow_copy(const ObDatum *datums, const int64_t batch_size);
  template <>
  inline void _shallow_copy<false>(const ObDatum *datums, const int64_t batch_size)
  {
    _append_batch<false>(0 /*batch_idx*/, datums, 0 /*offset*/, batch_size);
  }
  template <>
  inline void _shallow_copy<true>(const ObDatum *datums, const int64_t batch_size)
  {
    _append_batch<true>(0 /*batch_idx*/, datums, 0 /*offset*/, batch_size);
  }

  template <typename VEC>
  inline void _adapt_vector_batch(VEC *vec, const int64_t offset, const int64_t size);
  template <typename VEC>
  inline void _adapt_vector_selective(VEC *vec, const uint16_t *selector, const int64_t size);
  // ObDiscreteBase: 将null对应的len设置为0
  template <>
  inline void _adapt_vector_batch(ObDiscreteBase *vec, const int64_t offset, const int64_t size)
  {
    if (vec->has_null()) {
      sql::ObBitVector *nulls = vec->get_nulls();
      ObLength *lens = vec->get_lens();
      for (int64_t i = offset; i < offset + size; ++i) {
        lens[i] *= !nulls->at(i);
      }
    }
  }
  template <>
  inline void _adapt_vector_selective(ObDiscreteBase *vec, const uint16_t *selector,
                                      const int64_t size)
  {
    if (vec->has_null()) {
      sql::ObBitVector *nulls = vec->get_nulls();
      ObLength *lens = vec->get_lens();
      for (int64_t i = 0; i < size; ++i) {
        const int64_t idx = selector[i];
        lens[idx] *= !nulls->at(idx);
      }
    }
  }

private:
  inline void set_vector(sql::ObBitVector *nulls, const uint16_t flag)
  {
    nulls_ = nulls;
    base_->from(nulls, flag);
  }

private:
  T data_vector_;
  ObBitmapNullVectorBase *base_;
  sql::ObBitVector *vec_nulls_;
  sql::ObBitVector *nulls_;
  bool is_all_null_;
};

} // namespace storage
} // namespace oceanbase
