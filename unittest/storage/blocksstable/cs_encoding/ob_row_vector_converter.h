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

#ifndef OCEANBASE_CS_ENCODING_OB_ROW_VECTOR_CONVERTER_H_
#define OCEANBASE_CS_ENCODING_OB_ROW_VECTOR_CONVERTER_H_
#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_vector_utils.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase
{
namespace  blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;
class ObRowVectorConverter
{
public:
  ObRowVectorConverter();
  ~ObRowVectorConverter();
  void reset();
  void reuse();
  int init(const common::ObIArray<share::schema::ObColDesc> &col_descs,
           const ObIArray<VectorFormat> &vec_formats,
           const int64_t max_batch_size);
  int append_row(const blocksstable::ObDatumRow &datum_row, bool &is_full);
  int append_row(const blocksstable::ObDatumRow &datum_row, bool &is_full, ObIAllocator &allocator);
  int get_batch_datums(ObBatchDatumRows &vec_batch, ObIAllocator *allocator = nullptr);
  bool empty() const { return 0 == row_count_; }
  int64_t get_max_batch_size() const { return max_batch_size_; }

  TO_STRING_KV(K_(vectors), K_(max_batch_size), K_(row_count), K_(is_inited));

private:
  int init_vectors(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                   const ObIArray<VectorFormat> &vec_formats,
                   int64_t max_batch_size);
  int to_vector(const ObDatum &datum, ObIVector *vector, const int64_t batch_idx);
  int deep_copy_vector(ObIVector *src_vec, ObIVector *dst_vec, ObIAllocator &allocator);

private:
  common::ObArenaAllocator allocator_; // 常驻内存分配器
  ObArray<ObIVector *> vectors_;
  ObArray<VectorFormat> vec_formats_; // 保存 vector 格式信息，用于深度拷贝
  ObArray<VecValueTypeClass> vec_value_tcs_; // 保存 vector 类型信息，用于深度拷贝
  int64_t max_batch_size_;
  int64_t row_count_;
  bool is_inited_;
};

ObRowVectorConverter::ObRowVectorConverter()
  : allocator_(),
    vectors_(),
    vec_formats_(),
    vec_value_tcs_(),
    max_batch_size_(0),
    row_count_(0),
    is_inited_(false)
{
}

ObRowVectorConverter::~ObRowVectorConverter() { reset(); }

void ObRowVectorConverter::reset()
{
  is_inited_ = false;
  vectors_.reset();
  vec_formats_.reset();
  vec_value_tcs_.reset();
  max_batch_size_ = 0;
  row_count_ = 0;
  allocator_.reset();
}

int ObRowVectorConverter::init(const ObIArray<ObColDesc> &col_descs,
                               const ObIArray<VectorFormat> &vec_formats,
                               const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRowVectorConverter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(col_descs.empty() || max_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(col_descs), K(max_batch_size));
  } else {
    if (OB_FAIL(init_vectors(col_descs, vec_formats, max_batch_size))) {
      LOG_WARN("fail to init vectors", KR(ret));
    } else {
      max_batch_size_ = max_batch_size;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObRowVectorConverter::init_vectors(const ObIArray<ObColDesc> &col_descs,
                                       const ObIArray<VectorFormat> &vec_formats,
                                       int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vectors_.prepare_allocate(col_descs.count()))) {
    LOG_WARN("fail to prepare allocate", KR(ret), K(col_descs.count()));
  } else if (OB_FAIL(vec_formats_.prepare_allocate(col_descs.count()))) {
    LOG_WARN("fail to prepare allocate vec_formats", KR(ret), K(col_descs.count()));
  } else if (OB_FAIL(vec_value_tcs_.prepare_allocate(col_descs.count()))) {
    LOG_WARN("fail to prepare allocate vec_value_tcs", KR(ret), K(col_descs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
    const ObColDesc &col_desc = col_descs.at(i);
    const int16_t precision = col_desc.col_type_.is_decimal_int()
                                ? col_desc.col_type_.get_stored_precision()
                                : PRECISION_UNKNOWN_YET;
    VecValueTypeClass value_tc = get_vec_value_tc(col_desc.col_type_.get_type(),
                                                  col_desc.col_type_.get_scale(),
                                                  precision);
    const VectorFormat format = vec_formats.at(i);
    const bool is_fixed = is_fixed_length_vec(value_tc);
    ObIVector *vector = nullptr;
    ObArenaAllocator *allocator = nullptr;
    if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(format, value_tc, allocator_, vector))) {
      LOG_WARN("fail to new fixed vector", KR(ret), K(value_tc));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(vector, max_batch_size, allocator_))) {
      LOG_WARN("fail to prepare vector", KR(ret), K(value_tc));
    } else {
      vectors_.at(i) = vector;
      vec_formats_.at(i) = format;
      vec_value_tcs_.at(i) = value_tc;
      if (VEC_CONTINUOUS == format) {
        char *conti_buf = nullptr;
        if (OB_ISNULL(conti_buf = (char *)allocator_.alloc(2 << 20))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc conti buf", K(ret));
        } else {
          ObContinuousBase *conti_vec = static_cast<ObContinuousBase *>(vector);
          conti_vec->set_data(conti_buf);
        }
      }
    }
  }
  return ret;
}

int ObRowVectorConverter::to_vector(const ObDatum &datum,
                                    ObIVector *vector,
                                    const int64_t batch_idx)
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
        if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(datum.len_ > 0 ? datum.len_ : 1)))) {
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
    case VEC_CONTINUOUS: {
      ObContinuousBase *conti_vec = static_cast<ObContinuousBase *>(vector);
      char *data_buf = conti_vec->get_data();
      uint32_t *offsets = conti_vec->get_offsets();
      if (0 == batch_idx) {
        offsets[0] = 0;
      }
      if (datum.is_null()) {
        conti_vec->set_null(batch_idx);
        offsets[batch_idx + 1] = offsets[batch_idx];
      } else {
        if (datum.len_ > 0) {
          MEMCPY(data_buf + offsets[batch_idx], datum.ptr_, datum.len_);
        }
        offsets[batch_idx + 1] = offsets[batch_idx] + datum.len_;
      }
      break;
    }

    case VEC_UNIFORM: {
      ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
      ObDatum *dst_datums = uniform_vec->get_datums();
      dst_datums[batch_idx] = datum;
      break;
    }
    case VEC_UNIFORM_CONST: {
      ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
      ObDatum *dst_datums = uniform_vec->get_datums();
      if (0 == batch_idx) {
        dst_datums[batch_idx] = datum;
      } else if (false == ObDatum::binary_equal(dst_datums[0], datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not const datum", K(ret), K(dst_datums[0]), K(datum));
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

int ObRowVectorConverter::append_row(const ObDatumRow &datum_row, bool &is_full)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRowVectorConverter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(row_count_ >= max_batch_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", KR(ret), K(max_batch_size_), K(row_count_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_row.count_; ++i) {
      const ObDatum &datum = datum_row.storage_datums_[i];
      ObIVector *vec = vectors_.at(i);
      if (OB_UNLIKELY(datum.is_ext())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid datum", KR(ret), K(i), K(datum));
      } else if (OB_FAIL(to_vector(datum, vec, row_count_))) {
        LOG_WARN("fail to set vector", KR(ret), K(i), K(datum));
      }
    }
    if (OB_SUCC(ret)) {
      ++row_count_;
      is_full = (row_count_ == max_batch_size_);
    }
  }
  return ret;
}
int ObRowVectorConverter::get_batch_datums(ObBatchDatumRows &vec_batch, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  vec_batch.row_count_ = row_count_;
  if (nullptr == allocator) {
    // 浅拷贝：直接保存指针
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors_.count(); i++) {
      if (OB_FAIL(vec_batch.vectors_.push_back(vectors_.at(i)))) {
        LOG_WARN("fail to push back", K(ret), K(i));
      }
    }
  } else {
    // 深度拷贝：创建新的 vector 并拷贝数据
    if (OB_UNLIKELY(vec_formats_.count() != vectors_.count() ||
                    vec_value_tcs_.count() != vectors_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vec_formats or vec_value_tcs count mismatch", KR(ret),
               K(vec_formats_.count()), K(vec_value_tcs_.count()), K(vectors_.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < vectors_.count(); i++) {
        ObIVector *src_vec = vectors_.at(i);
        ObIVector *dst_vec = nullptr;
        const VectorFormat format = vec_formats_.at(i);
        const VecValueTypeClass value_tc = vec_value_tcs_.at(i);

        // 创建新的 vector
        if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(format, value_tc, *allocator, dst_vec))) {
          LOG_WARN("fail to new vector for deep copy", KR(ret), K(i), K(format), K(value_tc));
        } else if (OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(dst_vec, max_batch_size_, *allocator))) {
          LOG_WARN("fail to prepare vector for deep copy", KR(ret), K(i));
        } else if (VEC_CONTINUOUS == format) {
          // 为 CONTINUOUS 格式分配数据缓冲区
          // 计算源 vector 的实际数据大小
          int64_t total_data_size = 0;
          if (row_count_ > 0) {
            ObContinuousBase *src_conti_vec = static_cast<ObContinuousBase *>(src_vec);
            const uint32_t *src_offsets = src_conti_vec->get_offsets();
            total_data_size = src_offsets[row_count_]; // 最后一个 offset 就是总数据大小
          }
          // 至少分配 2MB，或者根据实际数据大小分配（加上一些余量）
          const int64_t min_buf_size = 2 << 20; // 2MB
          const int64_t buf_size = (min_buf_size > total_data_size + (1 << 16))
                                   ? min_buf_size
                                   : (total_data_size + (1 << 16)); // 加上 64KB 余量
          char *conti_buf = nullptr;
          if (OB_ISNULL(conti_buf = static_cast<char *>(allocator->alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc conti buf for deep copy", KR(ret), K(buf_size));
          } else {
            ObContinuousBase *conti_vec = static_cast<ObContinuousBase *>(dst_vec);
            conti_vec->set_data(conti_buf);
          }
        }

        // 深度拷贝数据
        if (OB_SUCC(ret)) {
          if (OB_FAIL(deep_copy_vector(src_vec, dst_vec, *allocator))) {
            LOG_WARN("fail to deep copy vector", KR(ret), K(i));
          } else if (OB_FAIL(vec_batch.vectors_.push_back(dst_vec))) {
            LOG_WARN("fail to push back deep copied vector", KR(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObRowVectorConverter::deep_copy_vector(ObIVector *src_vec, ObIVector *dst_vec, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == src_vec || nullptr == dst_vec)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(src_vec), KP(dst_vec));
  } else {
    const VectorFormat format = src_vec->get_format();
    // 逐行拷贝数据
    for (int64_t idx = 0; OB_SUCC(ret) && idx < row_count_; ++idx) {
      bool is_null = false;
      const char *payload = nullptr;
      ObLength length = 0;

      // 获取源 vector 的数据
      src_vec->get_payload(idx, is_null, payload, length);

      // 设置到目标 vector（set_payload 会进行深度拷贝）
      if (is_null) {
        dst_vec->set_null(idx);
      } else {
        dst_vec->set_payload(idx, payload, length);
      }
    }

    // 拷贝 null 标志位状态
    if (OB_SUCC(ret) && src_vec->has_null()) {
      dst_vec->set_has_null();
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
#endif
