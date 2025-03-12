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
#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_batch_row_buffer.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;
using namespace share::schema;
using namespace sql;

ObDirectLoadBatchRowBuffer::ObDirectLoadBatchRowBuffer()
  : allocator_("TLD_BatchBuf"),
    vector_allocators_(),
    vectors_(),
    max_batch_size_(0),
    row_count_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  vector_allocators_.set_block_allocator(ModulePageAllocator(allocator_));
  vectors_.set_block_allocator(ModulePageAllocator(allocator_));
}

ObDirectLoadBatchRowBuffer::~ObDirectLoadBatchRowBuffer() { reset(); }

void ObDirectLoadBatchRowBuffer::reset()
{
  is_inited_ = false;
  for (int64_t i = 0; i < vector_allocators_.count(); ++i) {
    ObArenaAllocator *allocator = vector_allocators_.at(i);
    if (nullptr != allocator) {
      allocator->~ObArenaAllocator();
      allocator_.free(allocator);
    }
  }
  vector_allocators_.reset();
  vectors_.reset();
  max_batch_size_ = 0;
  row_count_ = 0;
  allocator_.reset();
}

void ObDirectLoadBatchRowBuffer::reuse()
{
  row_count_ = 0;
  for (int64_t i = 0; i < vector_allocators_.count(); ++i) {
    ObArenaAllocator *allocator = vector_allocators_.at(i);
    if (nullptr != allocator) {
      allocator->reset_remain_one_page();
    }
  }
  for (int64_t i = 0; i < vectors_.count(); ++i) {
    ObBitmapNullVectorBase *vector = static_cast<ObBitmapNullVectorBase *>(vectors_.at(i));
    vector->reset_flag();
    vector->get_nulls()->reset(max_batch_size_);
  }
}

int ObDirectLoadBatchRowBuffer::init(const ObIArray<ObColDesc> &col_descs,
                                     const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadBatchRowBuffer init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(col_descs.empty() || max_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(col_descs), K(max_batch_size));
  } else {
    if (OB_FAIL(init_vectors(col_descs, max_batch_size))) {
      LOG_WARN("fail to init vectors", KR(ret));
    } else {
      max_batch_size_ = max_batch_size;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadBatchRowBuffer::init_vectors(const ObIArray<ObColDesc> &col_descs,
                                             int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vector_allocators_.prepare_allocate(col_descs.count()))) {
    LOG_WARN("fail to prepare allocate", KR(ret), K(col_descs.count()));
  } else if (OB_FAIL(vectors_.prepare_allocate(col_descs.count()))) {
    LOG_WARN("fail to prepare allocate", KR(ret), K(col_descs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
    const ObColDesc &col_desc = col_descs.at(i);
    const int16_t precision = col_desc.col_type_.is_decimal_int()
                                ? col_desc.col_type_.get_stored_precision()
                                : PRECISION_UNKNOWN_YET;
    VecValueTypeClass value_tc = get_vec_value_tc(col_desc.col_type_.get_type(),
                                                  col_desc.col_type_.get_scale(),
                                                  precision);
    const bool is_fixed = is_fixed_length_vec(value_tc);
    ObIVector *vector = nullptr;
    ObArenaAllocator *allocator = nullptr;
    if (is_fixed) { // fixed format
      if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(VEC_FIXED, value_tc, allocator_, vector))) {
        LOG_WARN("fail to new fixed vector", KR(ret), K(value_tc));
      } else if (OB_FAIL(
                   ObDirectLoadVectorUtils::prepare_vector(vector, max_batch_size, allocator_))) {
        LOG_WARN("fail to prepare fixed vector", KR(ret), K(value_tc));
      }
    } else { // discrete format
      if (OB_FAIL(
            ObDirectLoadVectorUtils::new_vector(VEC_DISCRETE, value_tc, allocator_, vector))) {
        LOG_WARN("fail to new discrete vector", KR(ret), K(value_tc));
      } else if (OB_FAIL(
                   ObDirectLoadVectorUtils::prepare_vector(vector, max_batch_size, allocator_))) {
        LOG_WARN("fail to prepare discrete vector", KR(ret), K(value_tc));
      } else if (OB_ISNULL(allocator = OB_NEWx(ObArenaAllocator, &allocator_, "TLD_VecAlloc"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObArenaAllocator", KR(ret));
      } else {
        allocator->set_tenant_id(MTL_ID());
      }
    }
    if (OB_SUCC(ret)) {
      vector_allocators_.at(i) = allocator;
      vectors_.at(i) = vector;
      int64_t fixed_len = 0;
      if (is_fixed) {
        fixed_len = vector->get_length(0);
      }
    }
  }
  return ret;
}

int ObDirectLoadBatchRowBuffer::to_vector(const ObDatum &datum, ObIVector *vector,
                                          const int64_t batch_idx, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  const VectorFormat format = vector->get_format();
  switch (format) {
    case VEC_FIXED: {
      ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
      const ObLength len = fixed_vec->get_length();
      if (datum.is_null()) {
        fixed_vec->set_null(batch_idx);
      } else if (OB_UNLIKELY(datum.len_ != len)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid datum", KR(ret), K(datum), K(len), K(batch_idx));
      } else {
        MEMCPY(static_cast<char *>(fixed_vec->get_data()) + len * batch_idx, datum.ptr_,
               datum.len_);
      }
      break;
    }
    case VEC_DISCRETE: {
      ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
      if (datum.is_null()) {
        discrete_vec->set_null(batch_idx);
      } else {
        char *buf = nullptr;
        if (OB_ISNULL(allocator)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected allocator is null", KR(ret));
        } else if (OB_ISNULL(buf = static_cast<char *>(
                               allocator->alloc(datum.len_ > 0 ? datum.len_ : 1)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(datum.len_));
        } else {
          if (datum.len_ > 0) {
            MEMCPY(buf, datum.ptr_, datum.len_);
          }
          discrete_vec->get_lens()[batch_idx] = datum.len_;
          discrete_vec->get_ptrs()[batch_idx] = buf;
        }
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", KR(ret), K(format));
      break;
  }
  return ret;
}

int ObDirectLoadBatchRowBuffer::to_vector(ObIVector *src_vector, const int64_t src_batch_idx,
                                          ObIVector *vector, const int64_t batch_idx,
                                          ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  bool is_null = false;
  const char *payload = nullptr;
  ObLength len = 0;
  if (OB_FAIL(
        ObDirectLoadVectorUtils::get_payload(src_vector, src_batch_idx, is_null, payload, len))) {
    LOG_WARN("fail to get payload", KR(ret));
  } else {
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_FIXED: {
        ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
        const ObLength fixed_len = fixed_vec->get_length();
        if (is_null) {
          fixed_vec->set_null(batch_idx);
        } else {
          const ObLength fixed_len = fixed_vec->get_length();
          if (OB_UNLIKELY(len != fixed_len)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid value", KR(ret), K(src_batch_idx), K(len), K(fixed_len));
          } else {
            MEMCPY(static_cast<char *>(fixed_vec->get_data()) + len * batch_idx, payload, len);
          }
        }
        break;
      }
      case VEC_DISCRETE: {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
        if (is_null) {
          discrete_vec->set_null(batch_idx);
        } else {
          char *buf = nullptr;
          if (OB_ISNULL(allocator)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected allocator is null", KR(ret));
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(len > 0 ? len : 1)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc mem", KR(ret), K(len));
          } else {
            if (len > 0) {
              MEMCPY(buf, payload, len);
            }
            discrete_vec->get_lens()[batch_idx] = len;
            discrete_vec->get_ptrs()[batch_idx] = buf;
          }
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", KR(ret), K(format));
        break;
    }
  }
  return ret;
}

int ObDirectLoadBatchRowBuffer::append_row(const ObDirectLoadDatumRow &datum_row,
                                           const ObDirectLoadRowFlag &row_flag,
                                           bool &is_full)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRowBuffer not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(row_count_ >= max_batch_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(row_count_));
  } else if (OB_UNLIKELY(row_flag.get_column_count(datum_row.get_column_count()) !=
                         vectors_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(vectors_.count()), K(datum_row), K(row_flag));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_row.count_; ++i) {
      const ObDatum &datum = datum_row.storage_datums_[i];
      ObIVector *vec = vectors_.at(row_flag.uncontain_hidden_pk_ ? i + 1 : i);
      ObIAllocator *allocator = vector_allocators_.at(row_flag.uncontain_hidden_pk_ ? i + 1 : i);
      if (OB_UNLIKELY(datum.is_ext())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid datum", KR(ret), K(i), K(datum));
      } else if (OB_FAIL(to_vector(datum, vec, row_count_, allocator))) {
        LOG_WARN("fail to set vector", KR(ret), K(i), K(datum), K(row_flag));
      }
    }
    if (OB_SUCC(ret)) {
      ++row_count_;
      is_full = (row_count_ == max_batch_size_);
    }
  }
  return ret;
}

int ObDirectLoadBatchRowBuffer::append_row(const IVectorPtrs &vectors,
                                           const int64_t row_idx,
                                           const ObDirectLoadRowFlag &row_flag,
                                           bool &is_full)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRowBuffer not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(row_count_ >= max_batch_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(row_count_));
  } else if (OB_UNLIKELY(row_flag.get_column_count(vectors.count()) != vectors_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(vectors_.count()), K(vectors.count()), K(row_flag));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
      ObIVector *src_vec = vectors.at(i);
      ObIVector *dest_vec = vectors_.at(row_flag.uncontain_hidden_pk_ ? i + 1 : i);
      ObIAllocator *allocator = vector_allocators_.at(row_flag.uncontain_hidden_pk_ ? i + 1 : i);
      if (OB_FAIL(to_vector(src_vec, row_idx, dest_vec, row_count_, allocator))) {
        LOG_WARN("fail to set vector", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ++row_count_;
      is_full = (row_count_ == max_batch_size_);
    }
  }
  return ret;
}

int ObDirectLoadBatchRowBuffer::get_batch(const IVectorPtrs *&vectors, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadBatchRowBuffer not init", KR(ret), KP(this));
  } else {
    vectors = &vectors_;
    row_count = row_count_;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
