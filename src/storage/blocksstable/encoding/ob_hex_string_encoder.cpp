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

#include "ob_hex_string_encoder.h"

#include "lib/container/ob_array_iterator.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

ObHexStringMap::ObHexStringMap()
{
  MEMSET(this, 0, sizeof(*this));
}

void ObHexStringMap::build_index(unsigned char *char_array)
{
  uint8_t index = 0;
  unsigned char *p = char_array;
  static_assert(ARRAYSIZEOF(map_) == 256, "The size of map isn't 256");
  for (uint8_t i = 0; index < size_; ++i) {
    if (0 != map_[i]) {
      map_[i] = index;
      index++;
      *p = i;
      p++;
    }
  }
}
const ObColumnHeader::Type ObHexStringEncoder::type_;

ObHexStringEncoder::ObHexStringEncoder() : min_string_size_(INT64_MAX),
    max_string_size_(-1), sum_size_(0), null_cnt_(0), nope_cnt_(0),
    header_(NULL)
{
}

int ObHexStringEncoder::init(
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
    column_header_.type_ = type_;
    min_string_size_ = INT64_MAX;
    max_string_size_ = ctx.max_string_size_;
    const ObObjTypeStoreClass sc = get_store_class_map()[
        ob_obj_type_class(column_type_.get_type())];
    if (OB_UNLIKELY(!is_string_encoding_valid(sc))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported type for string diff", K(ret), K(sc), K_(column_index));
    }
  }
  return ret;
}

int ObHexStringEncoder::traverse(bool &suitable)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    suitable = true;
    FOREACH_X(r, *rows_, OB_SUCC(ret) && suitable) {
      const ObDatum &datum = r->get_datum(column_index_);
      if (datum.is_null()) {
        null_cnt_++;
      } else if (datum.is_nop()) {
        nope_cnt_++;
      } else if (datum.is_ext()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported extend object type",
            K(ret), K(datum), K_(column_type), K_(column_index));
      } else {
        if (datum.len_ < min_string_size_) {
          min_string_size_ = datum.len_;
        }
        sum_size_ += (datum.len_ + 1) / 2;
        for (int64_t i = 0; i < datum.len_; ++i) {
          hex_string_map_.mark(static_cast<unsigned char>(datum.ptr_[i]));
        }
        if (!hex_string_map_.can_packing()) {
          suitable = false;
        }
      }
    }
    if (OB_SUCC(ret) && suitable) {
      if (rows_->count() - null_cnt_ - nope_cnt_ <= 1) {
        suitable = false;
      } else {
        bool fix_store = (min_string_size_ == max_string_size_);
        if (fix_store) {
          const int64_t row_size = (max_string_size_ + 1) / 2;
          const int64_t ext_obj_waste_size = (null_cnt_ + nope_cnt_) * row_size;
          const int64_t var_idx_waste_size =
              (DEF_VAR_INDEX_BYTE + sizeof(ObVarHexCellHeader)) * rows_->count();
          if (ext_obj_waste_size > var_idx_waste_size) {
            fix_store = false;
          }
        }
        if (fix_store) {
          desc_.fix_data_length_ = (max_string_size_ + 1) / 2;
          column_header_.set_fix_lenght_attr();
        } else {
          desc_.is_var_data_ = true;
        }
        desc_.need_data_store_ = true;
        desc_.has_null_ = null_cnt_ > 0;
        desc_.has_nope_ = nope_cnt_ > 0;
        desc_.need_extend_value_bit_store_ = desc_.has_null_ || desc_.has_nope_;
        if (desc_.need_extend_value_bit_store_) {
          column_header_.set_has_extend_value_attr();
        }
      }
    }
  }
  return ret;
}

int ObHexStringEncoder::get_encoding_store_meta_need_space(int64_t &need_size) const
{
  int ret = OB_SUCCESS;
  need_size = sizeof(*header_) + hex_string_map_.size_;
  return  ret;
}

int ObHexStringEncoder::store_meta(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    header_ = reinterpret_cast<ObHexStringHeader *>(buf_writer.current());
    header_->reset();
    const int64_t size = sizeof(*header_) + hex_string_map_.size_;
    if (OB_FAIL(buf_writer.advance_zero(size))) {
      LOG_WARN("advance meta store size failed", K(ret), K(size));
    } else {
      header_->max_string_size_ = static_cast<uint32_t>(max_string_size_);
      hex_string_map_.build_index(reinterpret_cast<unsigned char *>(header_->hex_char_array_));
    }
  }
  return ret;
}

int ObHexStringEncoder::store_data(
    const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(row_id < 0 || row_id >= rows_->count() || len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_id));
  } else {
    const ObDatum &datum = rows_->at(row_id).get_datum(column_index_);
    const ObStoredExtValue ext_val = get_stored_ext_value(datum);
    if (STORED_NOT_EXT != ext_val) {
      if (OB_FAIL(bs.set(column_header_.extend_value_index_,
          extend_value_bit_, static_cast<int64_t>(ext_val)))) {
        LOG_WARN("store extend value bit failed",
            K(ret), K_(column_header), K_(extend_value_bit), K(ext_val));
      }
    } else {
      int64_t offset = 0;
      if (!column_header_.is_fix_length()) {
        ObVarHexCellHeader *cell_header = reinterpret_cast<ObVarHexCellHeader *>(buf);
        offset += sizeof(*cell_header);
        cell_header->odd_ = static_cast<uint8_t>(datum.len_ % 2);
      }
      ObHexStringPacker packer(hex_string_map_,
          reinterpret_cast<unsigned char *>(buf + offset));
      packer.pack(reinterpret_cast<const unsigned char *>(datum.ptr_), datum.len_);
    }
  }
  return ret;
}

int ObHexStringEncoder::set_data_pos(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(header_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("call set data pos before store meta", K(ret));
  } else if (offset < 0 || length < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data position",
        K(ret), K(offset), K(length), K(desc_), K_(column_header));
  } else {
    header_->offset_ = static_cast<uint32_t>(offset);
    header_->length_ = static_cast<uint32_t>(length);
  }
  return ret;
}

int ObHexStringEncoder::get_var_length(const int64_t row_id, int64_t &length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(row_id < 0 || row_id >= rows_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_id));
  } else {
    const ObDatum &datum = rows_->at(row_id).get_datum(column_index_);
    if (datum.is_null() || datum.is_nop()) {
      length = 0;
    } else {
      // FIXME baihua: do not packing string less than 4 for var store ?
      length = sizeof(ObVarHexCellHeader) + (datum.len_ + 1) / 2;
    }
  }
  return ret;
}

int64_t ObHexStringEncoder::calc_size() const
{
  int64_t size = INT64_MAX;
  if (is_inited_) {
    size = sizeof(*header_) + hex_string_map_.size_;
    if (desc_.fix_data_length_ > 0) {
      size += rows_->count() * desc_.fix_data_length_;
    } else {
      size += DEF_VAR_INDEX_BYTE * rows_->count() + sum_size_
          + sizeof(ObVarHexCellHeader) * (rows_->count() - null_cnt_ - nope_cnt_);
    }
    // FIXME baihua: add extend value bit?
  }
  return size;
}

void ObHexStringEncoder::reuse()
{
  ObIColumnEncoder::reuse();
  min_string_size_ = 0;
  max_string_size_ = 0;
  sum_size_ = 0;
  null_cnt_ = 0;
  nope_cnt_ = 0;
  header_ = NULL;
  MEMSET(&hex_string_map_, 0, sizeof(ObHexStringMap));
}

struct ObHexStringEncoder::ColumnStoreFiller
{
  explicit ColumnStoreFiller(ObHexStringEncoder &encoder) : enc_(encoder) {}

  // fill fix store value
  inline int operator()(
      const int64_t row_id,
      const common::ObDatum &datum,
      char *buf,
      const int64_t len) const
  {
    UNUSEDx(row_id, len);
    // performance critical, do not check parameters
    ObHexStringPacker packer(enc_.hex_string_map_, reinterpret_cast<unsigned char *>(buf));
    packer.pack(reinterpret_cast<const unsigned char *>(datum.ptr_), datum.len_);
    return OB_SUCCESS;
  }

  ObHexStringEncoder &enc_;
};

int ObHexStringEncoder::store_fix_data(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_fix_encoder())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(desc));
  } else if (OB_UNLIKELY(0 >= desc_.fix_data_length_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fix_data_length should be larger than 0", K(ret), K_(desc));
  } else {
    if (desc_.fix_data_length_ > 0) {
      header_->length_ = static_cast<uint32_t>(desc_.fix_data_length_);
    }
    EmptyGetter getter;
    ColumnStoreFiller filler(*this);
    if (OB_FAIL(fill_column_store(buf_writer, *ctx_->col_datums_, getter, filler))) {
      LOG_WARN("fill column store failed", K(ret));
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
