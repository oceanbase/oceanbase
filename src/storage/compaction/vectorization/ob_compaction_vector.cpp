/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include <cstdint>
#include "ob_compaction_vector.h"
#include "ob_compaction_vector_utils.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/ob_storage_datum.h"

namespace oceanbase
{
namespace compaction
{
int ObCompactionGrowableBuffer::ensure(
    const int64_t need_size,
    int64_t &expand_delta,
    char **ptrs,
    const int64_t ptr_cnt)
{
  int ret = OB_SUCCESS;
  expand_delta = 0;
  const int64_t need_capacity = size_ + need_size;
  if (ptr_cnt < 0 || (ptrs == nullptr && ptr_cnt > 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ptr_cnt), KP(ptrs));
  } else if (need_size <= 0 || need_capacity <= capacity_) {
    // no-op
  } else {
    const int64_t old_capacity = capacity_;
    int64_t new_capacity = capacity_ > 0 ? capacity_ * 2 : page_size_;
    if (need_capacity > new_capacity) {
      new_capacity = ALIGN_UP(need_capacity, page_size_);
    }
    char *new_data = static_cast<char *>(allocator_.alloc(new_capacity));
    if (OB_ISNULL(new_data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate compaction growable buffer",
               KR(ret), K(old_capacity), K(size_), K(need_size), K(new_capacity));
    } else {
      if (size_ > 0) {
        MEMCPY(new_data, data_, size_);
      }
      if (nullptr != ptrs && ptr_cnt > 0 && nullptr != data_ && size_ > 0) {
        const uintptr_t old_base_addr = reinterpret_cast<uintptr_t>(data_);
        const uintptr_t old_end_addr = old_base_addr + size_;
        const uintptr_t new_base_addr = reinterpret_cast<uintptr_t>(new_data);
        for (int64_t i = 0; i < ptr_cnt; ++i) {
          const uintptr_t ptr_addr = reinterpret_cast<uintptr_t>(ptrs[i]);
          if (ptr_addr >= old_base_addr && ptr_addr < old_end_addr) {
            ptrs[i] = reinterpret_cast<char *>(new_base_addr + (ptr_addr - old_base_addr));
          }
        }
      }
      allocator_.free(data_);
      data_ = new_data;
      capacity_ = new_capacity;
      expand_delta = capacity_ - old_capacity;
    }
  }
  return ret;
}

// --------- ObCompactionVector --------- //
ObCompactionVector::ObCompactionVector(const VectorFormat format)
  : vector_header_()
{
  vector_header_.set_format(format);
}

void ObCompactionVector::reuse(const int64_t batch_size)
{
  ObBitmapNullVectorBase *base = reinterpret_cast<ObBitmapNullVectorBase *>(get_vector());
  sql::ObBitVector *nulls = base->get_nulls();
  nulls->reset(batch_size);
  base->reset_flag();
}

int ObCompactionVector::create_vector(
    VectorFormat format,
    VecValueTypeClass value_tc,
    const int64_t max_batch_size,
    ObIAllocator &allocator,
    ObCompactionVector *&vector)
{
  int ret = OB_SUCCESS;
  vector = nullptr;
  switch (format) {
    case VEC_FIXED: {
      switch (value_tc) {
#define FIXED_VECTOR_INIT_SWITCH(value_tc)                                          \
  case value_tc: {                                                                  \
    using VecValueType = RTCType<value_tc>;                                         \
    using FixedLengthVecType = ObCompactionFixedLengthBase<VecValueType>;           \
    vector = OB_NEWx(FixedLengthVecType, &allocator);                               \
    break;                                                                          \
  }
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTEGER);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_UINTEGER);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_FLOAT);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DOUBLE);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_FIXED_DOUBLE);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DATETIME);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DATE);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIME);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_YEAR);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_UNKNOWN);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_BIT);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TZ);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TINY);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_YM);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_DS);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT32);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT64);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT128);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT256);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT512);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_MYSQL_DATETIME);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_MYSQL_DATE);
#undef FIXED_VECTOR_INIT_SWITCH
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected fixed vector value type class", KR(ret), K(value_tc));
          break;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(vector)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc vector", KR(ret));
      }
      break;
    }
    case VEC_DISCRETE: {
      if (OB_ISNULL(vector = OB_NEWx(ObCompactionDiscreteVector, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc vector", KR(ret));
      }
      break;
    }
    case VEC_CONTINUOUS: {
      if (OB_ISNULL(vector = OB_NEWx(ObCompactionContinuousVector, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc vector", KR(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", KR(ret), K(format));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCompactionVectorUtils::new_vector(vector->get_vector_header(), value_tc))) {
      LOG_WARN("fail to new vector", KR(ret), K(format), K(value_tc));
    } else if (OB_FAIL(ObCompactionVectorUtils::prepare_vector(vector->get_vector(), max_batch_size, allocator))) {
      LOG_WARN("fail to prepare vector", KR(ret));
    } else {
      vector->init();
    }
  }
  return ret;
}

int ObCompactionVector::create_vector(
    const bool is_continuous,
    const share::schema::ObColDesc &col_desc,
    const int64_t max_batch_size,
    ObIAllocator &allocator,
    ObCompactionVector *&vector)
{
  int ret = OB_SUCCESS;
  const common::ObObjMeta &col_type = col_desc.col_type_;
  const int16_t precision = col_type.is_decimal_int()
                              ? col_type.get_stored_precision()
                              : PRECISION_UNKNOWN_YET;
  VecValueTypeClass value_tc =
    get_vec_value_tc(col_type.get_type(), col_type.get_scale(), precision);
  const bool is_fixed = is_fixed_length_vec(value_tc);
  VectorFormat format = is_fixed ? VEC_FIXED : (is_continuous ? VEC_CONTINUOUS : VEC_DISCRETE);
  if (OB_FAIL(create_vector(format, value_tc, max_batch_size, allocator, vector))) {
    LOG_WARN("fail to create vector", KR(ret), K(col_type), K(value_tc), K(format));
  } else {
    LOG_DEBUG("success to create vector", KR(ret), K(col_type), K(value_tc), K(format));
  }
  return ret;
}

// --------- ObCompactionFixedLengthBase --------- //
template <typename T>
void ObCompactionFixedLengthBase<T>::init()
{
  ObFixedLengthBase *data_vector = reinterpret_cast<ObFixedLengthBase *>(get_vector());
  data_ = reinterpret_cast<T *>(data_vector->get_data());
}

template <typename T>
int ObCompactionFixedLengthBase<T>::append_datum(
    const int64_t batch_idx,
    const blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    append_null(batch_idx);
  } else {
    data_[batch_idx] = *reinterpret_cast<const T *>(datum.ptr_);
  }
  return ret;
}

template <typename T>
void ObCompactionFixedLengthBase<T>::reuse(const int64_t batch_size)
{
  ObCompactionVector::reuse(batch_size);
  MEMSET(data_, 0, sizeof(T) * batch_size);
}

template <typename T>
int ObCompactionFixedLengthBase<T>::get_datum(const int64_t batch_idx, ObDatum &datum)
{
  if (is_null(batch_idx)) {
    datum.set_null();
  } else {
    datum.set_none();
    datum.ptr_ = reinterpret_cast<char *>(data_ + batch_idx);
    datum.len_ = sizeof(T);
  }
  return OB_SUCCESS;
}

// Explicit template instantiation
// VEC_TC_INTEGER, VEC_TC_DATETIME, VEC_TC_TIME, VEC_TC_UNKNOWN, VEC_TC_INTERVAL_YM, VEC_TC_DEC_INT64, VEC_TC_MYSQL_DATETIME
template class ObCompactionFixedLengthBase<int64_t>;
// VEC_TC_UINTEGER, VEC_TC_BIT, VEC_TC_ENUM_SET
template class ObCompactionFixedLengthBase<uint64_t>;
// VEC_TC_FLOAT
template class ObCompactionFixedLengthBase<float>;
// VEC_TC_DOUBLE, VEC_TC_FIXED_DOUBLE
template class ObCompactionFixedLengthBase<double>;
// VEC_TC_DATE, VEC_TC_DEC_INT32, VEC_TC_MYSQL_DATE
template class ObCompactionFixedLengthBase<int32_t>;
// VEC_TC_YEAR
template class ObCompactionFixedLengthBase<uint8_t>;
// VEC_TC_TIMESTAMP_TZ
template class ObCompactionFixedLengthBase<common::ObOTimestampData>;
// VEC_TC_TIMESTAMP_TINY
template class ObCompactionFixedLengthBase<common::ObOTimestampTinyData>;
// VEC_TC_INTERVAL_DS
template class ObCompactionFixedLengthBase<common::ObIntervalDSValue>;
// VEC_TC_DEC_INT128
template class ObCompactionFixedLengthBase<int128_t>;
// VEC_TC_DEC_INT256
template class ObCompactionFixedLengthBase<int256_t>;
// VEC_TC_DEC_INT512
template class ObCompactionFixedLengthBase<int512_t>;

// --------- ObCompactionDiscreteVector --------- //
void ObCompactionDiscreteVector::init()
{
  ObDiscreteBase *discrete_vec = reinterpret_cast<ObDiscreteBase *>(get_vector());
  lens_ = discrete_vec->get_lens();
  ptrs_ = discrete_vec->get_ptrs();
}

void ObCompactionDiscreteVector::reuse(const int64_t batch_size)
{
  ObCompactionVector::reuse(batch_size);
  MEMSET(lens_, 0, sizeof(ObLength) * batch_size);
  MEMSET(ptrs_, 0, sizeof(char *) * batch_size);
  buffer_.reuse();
}

int ObCompactionDiscreteVector::append_datum(
    const int64_t batch_idx,
    const blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    append_null(batch_idx);
  } else {
    lens_[batch_idx] = datum.len_;
    if (datum.is_local_buf()) {
      char *dst = nullptr;
      if (OB_FAIL(buffer_.append_copy(datum.ptr_, datum.len_, dst, ptrs_, batch_idx))) {
        LOG_WARN("fail to deep copy datum into discrete vector buffer", KR(ret), K(datum.len_));
      } else {
        ptrs_[batch_idx] = dst;
      }
    } else {
      ptrs_[batch_idx] = const_cast<char *>(datum.ptr_);
    }
  }
  return ret;
}

int ObCompactionDiscreteVector::get_datum(const int64_t batch_idx, ObDatum &datum)
{
  if (is_null(batch_idx)) {
    datum.set_null();
  } else {
    datum.set_none();
    datum.ptr_ = ptrs_[batch_idx];
    datum.len_ = lens_[batch_idx];
  }
  return OB_SUCCESS;
}

// --------- ObCompactionContinuousVector --------- //
ObCompactionContinuousVector::ObCompactionContinuousVector(const VectorFormat format)
  : ObCompactionVector(format),
    offsets_(nullptr),
    buffer_(MTL_ID(), "CompCont", PAGE_SIZE)
{
}

void ObCompactionContinuousVector::init()
{
  ObContinuousBase *continuous_vec = reinterpret_cast<ObContinuousBase *>(get_vector());
  offsets_ = continuous_vec->get_offsets();
}

void ObCompactionContinuousVector::reuse(const int64_t batch_size)
{
  ObCompactionVector::reuse(batch_size);
  MEMSET(offsets_, 0, sizeof(uint32_t) * (batch_size + 1));
  buffer_.reuse();
}

int ObCompactionContinuousVector::append_datum(
    const int64_t batch_idx,
    const blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  int64_t expand_delta = 0;
  if (datum.is_null()) {
    append_null(batch_idx);
  } else {
    if (datum.len_ <= 0) {
    } else if (OB_FAIL(buffer_.ensure(datum.len_, expand_delta))) {
      LOG_WARN("fail to expand", KR(ret), K(datum.len_));
    } else {
      if (expand_delta > 0) { // buffer is expanded
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(get_vector());
        continuous_vec->set_data(buffer_.data());
      }
      MEMCPY(buffer_.data() + offsets_[batch_idx], datum.ptr_, datum.len_);
      buffer_.advance(datum.len_);
    }
  }
  if (OB_SUCC(ret)) {
    offsets_[batch_idx + 1] = buffer_.size();
  }
  return ret;
}

int ObCompactionContinuousVector::get_datum(const int64_t batch_idx, ObDatum &datum)
{
  if (is_null(batch_idx)) {
    datum.set_null();
  } else {
    datum.set_none();
    datum.ptr_ = buffer_.data() + offsets_[batch_idx];
    datum.len_ = offsets_[batch_idx + 1] - offsets_[batch_idx];
  }
  return OB_SUCCESS;
}
} // namespace compaction
} // namespace oceanbase