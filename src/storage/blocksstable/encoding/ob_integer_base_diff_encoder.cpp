/**
 * Copyright (c) 2021 OceanBase
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

#include "ob_integer_base_diff_encoder.h"

#include <limits>
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_bit_stream.h"
#include "ob_encoding_hash_util.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

template <typename T>
ObIntegerBaseDiffEncoder::ObIntegerData<T>::ObIntegerData(ObIntegerBaseDiffEncoder &encoder)
  : min_(std::numeric_limits<T>::max()), max_(std::numeric_limits<T>::min()), encoder_(encoder)
{
}

template <>
OB_INLINE uint64_t ObIntegerBaseDiffEncoder::ObIntegerData<int64_t>::cast_to_uint64(
    const uint64_t v) const
{
  int64_t nv = v;
  if (0 != encoder_.reverse_mask_ && (v & (encoder_.reverse_mask_ >> 1))) {
    nv |= encoder_.reverse_mask_;
  }
  return nv;
}

template <typename T>
OB_INLINE void ObIntegerBaseDiffEncoder::ObIntegerData<T>::traverse_cell(const ObDatum &datum)
{
  uint64_t v = cast_to_uint64(datum.get_uint64() & encoder_.mask_);
  T t = *reinterpret_cast<T *>(&v);
  if (t < min_) {
    min_ = t;
  }
  if (t > max_) {
    max_ = t;
  }
}

template <typename T>
uint64_t ObIntegerBaseDiffEncoder::ObIntegerData<T>::max_delta() const
{
  uint64_t delta = 0;
  if (min_ < max_) {
    delta = max_ - min_;
  }
  return delta;
}

template <typename T>
uint64_t ObIntegerBaseDiffEncoder::ObIntegerData<T>::max_unsign_value() const
{
  uint64_t v = max_;
  if (min_ < 0) {
    v = std::numeric_limits<uint64_t>::max();
  }
  return v;
}

template <typename T>
uint64_t ObIntegerBaseDiffEncoder::ObIntegerData<T>::delta(const common::ObDatum &datum) const
{
  uint64_t v = cast_to_uint64(datum.get_uint64() & encoder_.mask_);
  return *reinterpret_cast<T *>(&v) - min_;
}

template <typename T>
void ObIntegerBaseDiffEncoder::ObIntegerData<T>::reuse()
{
  min_ = std::numeric_limits<T>::max();
  max_ = std::numeric_limits<T>::min();
}

const ObColumnHeader::Type ObIntegerBaseDiffEncoder::type_;
ObIntegerBaseDiffEncoder::ObIntegerBaseDiffEncoder()
  : store_class_(ObExtendSC), type_store_size_(0), mask_(0), reverse_mask_(0),
    integer_data_(&int64_data_),
    int64_data_(*this), uint64_data_(*this), header_(NULL)
{
}

int ObIntegerBaseDiffEncoder::init(
    const ObColumnEncodingCtx &ctx,
    const int64_t column_index,
    const ObConstDatumRowArray &rows)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObIColumnEncoder::init(ctx, column_index, rows))) {
    LOG_WARN("init base column encoder failed",
        K(ret), K(ctx), K(column_index), "row count", rows.count());
  } else {
    const ObObjTypeStoreClass sc = get_store_class_map()[
        ob_obj_type_class(column_type_.get_type())];
    type_store_size_ = get_type_size_map()[column_type_.get_type()];
    if ((ObIntSC != sc && ObUIntSC != sc) || type_store_size_ < 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported type for integer base diff",
          K(ret), K(sc), K_(type_store_size), K_(column_index));
    } else {
      mask_ = INTEGER_MASK_TABLE[type_store_size_];
      if (ObIntSC == sc) {
        integer_data_ = &int64_data_;
        reverse_mask_ = ~mask_;
      } else {
        integer_data_ = &uint64_data_;
      }
      column_header_.type_ = type_;
    }
  }
  return ret;
}

void ObIntegerBaseDiffEncoder::reuse()
{
  ObIColumnEncoder::reuse();
  store_class_ = ObExtendSC;
  type_store_size_ = 0;
  mask_ = 0;
  reverse_mask_ = 0;
  integer_data_ = &int64_data_;
  int64_data_.reuse();
  uint64_data_.reuse();
  header_ = NULL;
  is_inited_ = false;
}

template <typename T>
int ObIntegerBaseDiffEncoder::traverse_cells(T &integer_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; i < ctx_->col_datums_->count(); ++i) {
      const ObDatum &datum = ctx_->col_datums_->at(i);
      if (!datum.is_null() && !datum.is_nop()) {
        integer_data.traverse_cell(datum);
      }
    }
  }
  return ret;
}

int ObIntegerBaseDiffEncoder::traverse(bool &suitable)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t null_cnt = ctx_->null_cnt_;
    int64_t nope_cnt = ctx_->nope_cnt_;
    suitable = false;
    if (integer_data_ == &int64_data_) {
      if (OB_FAIL(traverse_cells(int64_data_))) {
        LOG_WARN("traverse signed integers failed", K(ret));
      }
    } else {
      if (OB_FAIL(traverse_cells(uint64_data_))) {
        LOG_WARN("traverse unsigned integers failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const uint64_t delta = integer_data_->max_delta();
      const uint64_t max_value = integer_data_->max_unsign_value();
      if (0 == delta) {
        // not suitable for integer base diff
      } else {
        bool bit_packing = false;
        int64_t orig_size = get_packing_size(bit_packing, max_value);
        if (!bit_packing) {
          orig_size *= CHAR_BIT;
        }
        bit_packing = false;
        int64_t delta_size = get_packing_size(bit_packing, delta);
        if (!bit_packing) {
          delta_size *= CHAR_BIT;
        }
        LOG_DEBUG("integer base diff size", K_(column_index), K(delta_size), K(orig_size));
        if ((orig_size - delta_size) * rows_->count()
            > (sizeof(*header_) + type_store_size_) * CHAR_BIT) {
          suitable = true;
          if (bit_packing) {
            desc_.bit_packing_length_ = delta_size;
          } else {
            desc_.fix_data_length_ = delta_size / CHAR_BIT;
          }
          desc_.need_data_store_ = true;
          desc_.has_null_ = null_cnt > 0;
          desc_.has_nope_ = nope_cnt > 0;
          desc_.need_extend_value_bit_store_ = desc_.has_null_ || desc_.has_nope_;
          if (desc_.need_extend_value_bit_store_) {
            column_header_.set_has_extend_value_attr();
          }
          if (desc_.bit_packing_length_ > 0) {
            column_header_.set_bit_packing_attr();
          }
          column_header_.set_fix_lenght_attr();
        }
      }
    }
  }
  return ret;
}

int ObIntegerBaseDiffEncoder::store_meta(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    char *data = buf_writer.current();
    header_ = reinterpret_cast<ObIntegerBaseDiffHeader *>(data);
    data += sizeof(*header_);
    if (OB_FAIL(buf_writer.advance_zero(sizeof(*header_) + type_store_size_))) {
      LOG_WARN("advance meta store size failed", K(ret), K_(type_store_size));
    } else {
      const uint64_t base = integer_data_->base();
      LOG_DEBUG("integer base", K(base));
      MEMCPY(data, &base, type_store_size_);
    }
  }
  return ret;
}

int64_t ObIntegerBaseDiffEncoder::calc_size() const
{
  int64_t size = INT64_MAX;
  if (is_inited_) {
    if (desc_.bit_packing_length_ > 0) {
      size = (rows_->count() * desc_.bit_packing_length_ + CHAR_BIT - 1) / CHAR_BIT;
    } else {
      size = rows_->count() * desc_.fix_data_length_;
    }
  }
  return size + sizeof(*header_) + type_store_size_;
}

int ObIntegerBaseDiffEncoder::store_fix_data(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_fix_encoder())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(desc));
  } else {
    DeltaGetter getter(integer_data_);
    FixDataSetter setter(integer_data_);
    header_->length_ = static_cast<uint8_t>(desc_.bit_packing_length_ > 0
        ? desc_.bit_packing_length_
        : desc_.fix_data_length_);
    if (OB_FAIL(fill_column_store(buf_writer, *ctx_->col_datums_, getter, setter))) {
      LOG_WARN("fill column store failed", K(ret));
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
