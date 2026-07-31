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
char ObCompactionGrowableBuffer::empty_data_ = '\0';

int ObCompactionGrowableBuffer::ensure(
    const int64_t need_size,
    int64_t &expand_delta,
    char **ptrs,
    const int64_t ptr_cnt)
{
  int ret = OB_SUCCESS;
  expand_delta = 0;
  int64_t need_capacity = 0;
  if (ptr_cnt < 0 || (ptrs == nullptr && ptr_cnt > 0) || need_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(need_size), K(ptr_cnt), KP(ptrs));
  } else if (size_ < 0 || capacity_ < 0 || size_ > capacity_ || page_size_ <= 0
             || (capacity_ > 0 && OB_ISNULL(data_))
             || (0 == capacity_ && nullptr != data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid growable buffer state",
             KR(ret), K(size_), K(capacity_), KP(data_), K(page_size_));
  } else if (0 == need_size) {
    // no-op
  } else if (need_size > INT64_MAX - size_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("growable buffer size overflow", KR(ret), K(size_), K(need_size));
  } else if ((need_capacity = size_ + need_size) <= capacity_) {
    // no-op
  } else {
    const int64_t old_capacity = capacity_;
    int64_t new_capacity = capacity_ > INT64_MAX / 2
                             ? INT64_MAX
                             : (capacity_ > 0 ? capacity_ * 2 : page_size_);
    if (need_capacity > new_capacity
        && need_capacity > INT64_MAX - (page_size_ - 1)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("growable buffer aligned capacity overflow",
               KR(ret), K(need_capacity), K(page_size_));
    } else if (need_capacity > new_capacity) {
      new_capacity = ALIGN_UP(need_capacity, page_size_);
    }
    if (OB_SUCC(ret)) {
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
  }
  return ret;
}

// --------- ObCompactionVector --------- //
ObCompactionVector::ObCompactionVector(const VectorFormat format)
  : vector_header_(),
    max_batch_size_(0)
{
  vector_header_.set_format(format);
}

void ObCompactionVector::reuse(const int64_t batch_size)
{
  if (OB_UNLIKELY(!is_valid_batch_size(batch_size))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT,
                 "invalid reuse batch size", K(batch_size), K_(max_batch_size));
  } else {
    ObBitmapNullVectorBase *base = reinterpret_cast<ObBitmapNullVectorBase *>(get_vector());
    sql::ObBitVector *nulls = base->get_nulls();
    nulls->reset(batch_size);
    base->reset_flag();
  }
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
      vector->set_max_batch_size(max_batch_size);
      vector->init();
    }
  }
  if (OB_FAIL(ret) && nullptr != vector) {
    vector->~ObCompactionVector();
    allocator.free(vector);
    vector = nullptr;
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
OB_INLINE uint32_t get_fixed_datum_length()
{
  return static_cast<uint32_t>(sizeof(T));
}

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
  if (OB_FAIL(check_batch_idx(batch_idx))) {
    LOG_WARN("invalid batch index", KR(ret), K(batch_idx), K_(max_batch_size));
  } else if (datum.is_null()) {
    if (OB_FAIL(append_null(batch_idx))) {
      LOG_WARN("fail to append null datum", KR(ret), K(batch_idx));
    }
  } else if (datum.is_ext()
             || OB_ISNULL(datum.ptr_)
             || datum.len_ != get_fixed_datum_length<T>()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fixed-length datum",
             KR(ret), K(batch_idx), KP(datum.ptr_), K(datum.len_),
             "expected_len", get_fixed_datum_length<T>(), "is_ext", datum.is_ext());
  } else {
    MEMCPY(data_ + batch_idx, datum.ptr_, sizeof(T));
    reinterpret_cast<ObBitmapNullVectorBase *>(get_vector())->unset_null(batch_idx);
  }
  return ret;
}

template <typename T>
void ObCompactionFixedLengthBase<T>::reuse(const int64_t batch_size)
{
  if (OB_UNLIKELY(!is_valid_batch_size(batch_size))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT,
                 "invalid reuse batch size", K(batch_size), K_(max_batch_size));
  } else {
    ObCompactionVector::reuse(batch_size);
    MEMSET(data_, 0, sizeof(T) * batch_size);
  }
}

template <typename T>
int ObCompactionFixedLengthBase<T>::get_datum(const int64_t batch_idx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_batch_idx(batch_idx))) {
    LOG_WARN("invalid batch index", KR(ret), K(batch_idx), K_(max_batch_size));
  } else if (is_null(batch_idx)) {
    datum.set_null();
  } else {
    datum.set_none();
    datum.ptr_ = reinterpret_cast<char *>(data_ + batch_idx);
    datum.len_ = sizeof(T);
  }
  return ret;
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
  if (OB_UNLIKELY(!is_valid_batch_size(batch_size))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT,
                 "invalid reuse batch size", K(batch_size), K_(max_batch_size));
  } else {
    ObCompactionVector::reuse(batch_size);
    MEMSET(lens_, 0, sizeof(ObLength) * batch_size);
    MEMSET(ptrs_, 0, sizeof(char *) * batch_size);
    buffer_.reuse();
  }
}

int ObCompactionDiscreteVector::append_datum(
    const int64_t batch_idx,
    const blocksstable::ObStorageDatum &datum)
{
  return inner_append_datum(batch_idx, datum, false /*force_deep_copy*/);
}

int ObCompactionDiscreteVector::append_datum_deep_copy(
    const int64_t batch_idx,
    const blocksstable::ObStorageDatum &datum)
{
  return inner_append_datum(batch_idx, datum, true /*force_deep_copy*/);
}

int ObCompactionDiscreteVector::inner_append_datum(
    const int64_t batch_idx,
    const blocksstable::ObStorageDatum &datum,
    const bool force_deep_copy)
{
  int ret = OB_SUCCESS;
  char *datum_ptr = nullptr;
  if (OB_FAIL(check_batch_idx(batch_idx))) {
    LOG_WARN("invalid batch index", KR(ret), K(batch_idx), K_(max_batch_size));
  } else if (datum.is_null()) {
    if (OB_FAIL(append_null(batch_idx))) {
      LOG_WARN("fail to append null datum", KR(ret), K(batch_idx));
    }
  } else if (datum.is_ext()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extended datum cannot be appended to discrete vector",
             KR(ret), K(batch_idx), K(datum));
  } else if (datum.len_ > 0 && OB_ISNULL(datum.ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum", KR(ret), K(batch_idx), KP(datum.ptr_), K(datum.len_));
  } else if (0 == datum.len_) {
    datum_ptr = buffer_.data_or_empty();
  } else if (force_deep_copy || datum.is_local_buf()) {
    if (OB_FAIL(buffer_.append_copy(
        datum.ptr_, datum.len_, datum_ptr, ptrs_, max_batch_size_))) {
      LOG_WARN("fail to deep copy datum into discrete vector buffer", KR(ret), K(datum.len_));
    }
  } else {
    datum_ptr = const_cast<char *>(datum.ptr_);
  }
  if (OB_SUCC(ret) && !datum.is_null()) {
    lens_[batch_idx] = datum.len_;
    ptrs_[batch_idx] = datum_ptr;
    reinterpret_cast<ObBitmapNullVectorBase *>(get_vector())->unset_null(batch_idx);
  }
  return ret;
}

int ObCompactionDiscreteVector::get_datum(const int64_t batch_idx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_batch_idx(batch_idx))) {
    LOG_WARN("invalid batch index", KR(ret), K(batch_idx), K_(max_batch_size));
  } else if (is_null(batch_idx)) {
    datum.set_null();
  } else {
    datum.set_none();
    datum.ptr_ = ptrs_[batch_idx];
    datum.len_ = lens_[batch_idx];
  }
  return ret;
}

// --------- ObCompactionContinuousVector --------- //
ObCompactionContinuousVector::ObCompactionContinuousVector(const VectorFormat format)
  : ObCompactionVector(format),
    offsets_(nullptr),
    buffer_(MTL_ID(), "CompCont", PAGE_SIZE),
    next_append_idx_(0)
{
}

void ObCompactionContinuousVector::init()
{
  ObContinuousBase *continuous_vec = reinterpret_cast<ObContinuousBase *>(get_vector());
  offsets_ = continuous_vec->get_offsets();
  continuous_vec->set_data(buffer_.data_or_empty());
}

void ObCompactionContinuousVector::reuse(const int64_t batch_size)
{
  if (OB_UNLIKELY(!is_valid_batch_size(batch_size))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT,
                 "invalid reuse batch size", K(batch_size), K_(max_batch_size));
  } else {
    ObCompactionVector::reuse(batch_size);
    MEMSET(offsets_, 0, sizeof(uint32_t) * (batch_size + 1));
    buffer_.reuse();
    next_append_idx_ = 0;
  }
}

int ObCompactionContinuousVector::append_datum(
    const int64_t batch_idx,
    const blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  int64_t expand_delta = 0;
  if (OB_FAIL(check_batch_idx(batch_idx))) {
    LOG_WARN("invalid batch index", KR(ret), K(batch_idx), K_(max_batch_size));
  } else if (OB_UNLIKELY(batch_idx != next_append_idx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("continuous vector datum must be appended in order",
             KR(ret), K(batch_idx), K_(next_append_idx));
  } else if (OB_UNLIKELY(static_cast<int64_t>(offsets_[batch_idx]) != buffer_.size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("continuous vector offset is inconsistent with buffer size",
             KR(ret), K(batch_idx), K(offsets_[batch_idx]), "buffer_size", buffer_.size());
  } else if (datum.is_ext()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extended datum cannot be appended to continuous vector",
             KR(ret), K(batch_idx), K(datum));
  } else if (!datum.is_null() && datum.len_ > 0 && OB_ISNULL(datum.ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum", KR(ret), K(batch_idx), KP(datum.ptr_), K(datum.len_));
  } else if (buffer_.size() < 0
             || static_cast<uint64_t>(buffer_.size()) + datum.len_ > UINT32_MAX) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("continuous vector offset overflow",
             KR(ret), K(batch_idx), "buffer_size", buffer_.size(), K(datum.len_));
  } else if (datum.is_null()) {
    if (OB_FAIL(append_null(batch_idx))) {
      LOG_WARN("fail to append null datum", KR(ret), K(batch_idx));
    }
  } else if (datum.len_ > 0) {
    if (OB_FAIL(buffer_.ensure(datum.len_, expand_delta))) {
      LOG_WARN("fail to expand", KR(ret), K(datum.len_));
    } else {
      if (expand_delta > 0) { // buffer is expanded
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(get_vector());
        continuous_vec->set_data(buffer_.data());
      }
      MEMCPY(buffer_.data() + buffer_.size(), datum.ptr_, datum.len_);
      buffer_.advance(datum.len_);
    }
  }
  if (OB_SUCC(ret)) {
    if (!datum.is_null()) {
      reinterpret_cast<ObBitmapNullVectorBase *>(get_vector())->unset_null(batch_idx);
    }
    offsets_[batch_idx + 1] = static_cast<uint32_t>(buffer_.size());
    ++next_append_idx_;
  }
  return ret;
}

int ObCompactionContinuousVector::get_datum(const int64_t batch_idx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_batch_idx(batch_idx))) {
    LOG_WARN("invalid batch index", KR(ret), K(batch_idx), K_(max_batch_size));
  } else if (is_null(batch_idx)) {
    datum.set_null();
  } else if (OB_UNLIKELY(offsets_[batch_idx + 1] < offsets_[batch_idx]
                         || offsets_[batch_idx + 1] > buffer_.size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid continuous vector offsets",
             KR(ret), K(batch_idx), K(offsets_[batch_idx]), K(offsets_[batch_idx + 1]),
             "buffer_size", buffer_.size());
  } else {
    datum.set_none();
    ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(get_vector());
    datum.ptr_ = continuous_vec->get_data() + offsets_[batch_idx];
    datum.len_ = offsets_[batch_idx + 1] - offsets_[batch_idx];
  }
  return ret;
}
} // namespace compaction
} // namespace oceanbase
