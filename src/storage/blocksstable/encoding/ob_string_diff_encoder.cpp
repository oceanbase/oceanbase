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

#include "ob_string_diff_encoder.h"

#include <limits>

#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

const ObColumnHeader::Type ObStringDiffEncoder::type_;

ObStringDiffEncoder::ObStringDiffEncoder()
    : string_size_(0), common_size_(0), row_store_size_(0),
    null_cnt_(0), nope_cnt_(0), first_string_(NULL),
    header_(NULL), last_change_diff_row_id_(0),
    allocator_(blocksstable::OB_ENCODING_LABEL_STRING_DIFF, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
  diff_descs_.set_attr(ObMemAttr(MTL_ID(), "StrDiffEnc"));
}

int ObStringDiffEncoder::init(
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
    if (OB_UNLIKELY(!is_string_encoding_valid(sc))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported type for string diff", K(ret), K(sc), K_(column_index));
    }
    column_header_.type_ = type_;
  }
  return ret;
}

void ObStringDiffEncoder::reuse()
{
  ObIColumnEncoder::reuse();
  string_size_ = 0;
  common_size_ = 0;
  row_store_size_ = 0;
  null_cnt_ = 0;
  nope_cnt_ = 0;
  first_string_ = NULL;
  header_ = NULL;
  diff_descs_.reuse();
  MEMSET(&hex_string_map_, 0, sizeof(ObHexStringMap));
  last_change_diff_row_id_ = 0;
  allocator_.reuse();
}

int ObStringDiffEncoder::traverse(bool &suitable)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    suitable = true;
    char *data = NULL;
    bool *diff = NULL;
    for (int64_t i = 0; i < rows_->count() && OB_SUCC(ret) && suitable; i++) {
      const ObDatum &datum = rows_->at(i).get_datum(column_index_);
      if (datum.is_null()) {
        null_cnt_++;
      } else if (datum.is_nop()) {
        nope_cnt_++;
      } else if (datum.is_ext()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported extend object type",
            K(ret), K(datum), K_(column_type), K_(column_index));
      } else if (OB_FAIL(traverse_cell(data, diff, suitable, datum, i))) {
        LOG_WARN("traverse cell failed", K(ret), K(datum));
      }
    }
    if (OB_SUCC(ret) && suitable && hex_string_map_.can_packing()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < last_change_diff_row_id_
          && hex_string_map_.can_packing(); ++i) {
        const ObDatum &datum = rows_->at(i).get_datum(column_index_);
        if (!datum.is_null() && !datum.is_nop()) {
          if (OB_FAIL(traverse_cell(data, diff, suitable, datum, i))) {
            LOG_WARN("traverse cell failed", K(ret), K(datum));
          }
        }
      }
    }
    const int64_t min_diff_rows = 2;
    ObStringDiffHeader::DiffDesc desc;
    if (OB_SUCC(ret) && suitable && (null_cnt_ + nope_cnt_ + min_diff_rows) < rows_->count()) {
      common_size_ = 0;
      int64_t begin = 0;
      desc.diff_ = diff[0] ? 1 : 0;
      for (int64_t i = 0; OB_SUCC(ret) && i <= string_size_; ++i) {
        if (i == string_size_ || desc.diff_ != diff[i]
            || i - begin == (std::numeric_limits<uint8_t>::max() & 0x7F)) {
          desc.count_ = (i - begin) & 0x7f;
          if (!desc.diff_) {
            common_size_ += desc.count_;
          }
          if (OB_FAIL(diff_descs_.push_back(desc))) {
            LOG_WARN("add diff description failed", K(ret), K(desc));
          } else {
            begin = i;
            if (i < string_size_) {
              desc.diff_ = diff[i] ? 1 : 0;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        row_store_size_ = string_size_ - common_size_;
        bool hex_packing = false;
        if (row_store_size_ > 1 && hex_string_map_.can_packing()) {
          hex_packing = true;
          row_store_size_ = (row_store_size_ + 1) / 2;
        }
        if (row_store_size_ * (null_cnt_ + nope_cnt_)
            > DEF_VAR_INDEX_BYTE * rows_->count()) {
          desc_.is_var_data_ = true;
        }
        if (string_size_ == common_size_) {
          suitable = false;
        } else {
          ObString tmp(string_size_, data);
          LOG_DEBUG("may use string diff",
              K_(column_index), "string", tmp,
              K_(string_size), K_(common_size), K(hex_packing));
          desc_.need_data_store_ = true;
          desc_.has_null_ = null_cnt_ > 0;
          desc_.has_nope_ = nope_cnt_ > 0;
          desc_.need_extend_value_bit_store_ = desc_.has_null_ || desc_.has_nope_;
          if (!desc_.is_var_data_) {
            column_header_.set_fix_lenght_attr();
            desc_.fix_data_length_ = row_store_size_;
          }
          if (desc_.need_extend_value_bit_store_) {
            column_header_.set_has_extend_value_attr();
          }
        }
      }

      if (diff_descs_.count()
          > std::numeric_limits<__typeof__(ObStringDiffHeader::diff_desc_cnt_)>::max()) {
        suitable = false;
      }
    } else {
      suitable = false;
    }
  }
  return ret;
}

int ObStringDiffEncoder::traverse_cell(
    char *&data,
    bool *&diff,
    bool &suitable,
    const ObDatum &datum,
    const int64_t row_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (string_size_ <= 0) {
      if (datum.len_ <= 0 || datum.len_ >= UINT16_MAX) {
        suitable = false;
      } else if (OB_ISNULL(data = static_cast<char *>(allocator_.alloc(datum.len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), "size", datum.len_);
      } else if (OB_ISNULL(diff = static_cast<bool *>(
          allocator_.alloc(sizeof(bool) * datum.len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), "size", sizeof(bool) * datum.len_);
      } else {
        string_size_ = datum.len_;
        first_string_ = datum.ptr_;
        MEMCPY(data, datum.ptr_, datum.len_);
        MEMSET(diff, 0, sizeof(bool) * string_size_);
      }
    } else if (datum.len_ != string_size_) {
      suitable = false;
    } else {
      int64_t common_cnt = 0;
      for (int64_t i = 0; i < string_size_; ++i) {
        if (diff[i]) {
          hex_string_map_.mark(datum.ptr_[i]);
        } else {
          if (data[i] == datum.ptr_[i]) {
            common_cnt++;
          } else {
            diff[i] = true;
            data[i] = '*'; // FIXME remove?
            last_change_diff_row_id_ = row_id;
            hex_string_map_.mark(datum.ptr_[i]);
          }
        }
      }
      if (common_cnt == 0) {
        suitable = false;
      }
    }
  }
  return ret;
}

int ObStringDiffEncoder::get_encoding_store_meta_need_space(int64_t &need_size) const
{
  int ret = OB_SUCCESS;
  const bool hex_packing = string_size_ - common_size_ != row_store_size_;
  need_size = sizeof(*header_)
        + sizeof(ObStringDiffHeader::DiffDesc) * diff_descs_.count()
        + (hex_packing ? hex_string_map_.size_ : 0) + common_size_;
  return ret;
}

int ObStringDiffEncoder::store_meta(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const bool hex_packing = string_size_ - common_size_ != row_store_size_;
    char *data = buf_writer.current();
    header_ = reinterpret_cast<ObStringDiffHeader *>(data);
    header_->reset();
    header_->version_ = ObStringDiffHeader::OB_STRING_DIFF_HEADER_V1;
    int64_t size = sizeof(*header_)
        + sizeof(ObStringDiffHeader::DiffDesc) * diff_descs_.count()
        + (hex_packing ? hex_string_map_.size_ : 0) + common_size_;
    if (OB_FAIL(buf_writer.advance_zero(size))) {
      LOG_WARN("advance meta store size failed", K(ret), K(size));
    } else {
      header_->string_size_ = static_cast<uint16_t>(string_size_);
      header_->diff_desc_cnt_ = static_cast<uint8_t>(diff_descs_.count());
      for (int64_t i = 0; i < diff_descs_.count(); ++i) {
        header_->diff_descs_[i] = diff_descs_.at(i);
      }
      if (hex_packing) {
        header_->hex_char_array_size_ = static_cast<uint8_t>(hex_string_map_.size_);
        hex_string_map_.build_index(header_->hex_char_array());
      } else {
        header_->hex_char_array_size_ = 0;
      }
      header_->copy_string(ObStringDiffHeader::RightToLeft(), std::logical_not<uint8_t>(),
          header_->common_data(), first_string_);
    }
  }
  return ret;
}

struct ObStringDiffEncoder::ColumnStoreFiller
{
  explicit ColumnStoreFiller(ObStringDiffEncoder &encoder) : enc_(encoder) {}

  // fill fix store value
  OB_INLINE int operator()(
      const int64_t row_id,
      const common::ObDatum &datum,
      char *buf,
      const int64_t len) const
  {
    UNUSEDx(row_id, len);
    // performance critical, do not check parameters
    if (enc_.header_->is_hex_packing()) {
      ObHexStringPacker packer(enc_.hex_string_map_, reinterpret_cast<unsigned char *>(buf));
      enc_.header_->copy_hex_string(packer, static_cast<void (ObHexStringPacker::*)(unsigned char)>(
          &ObHexStringPacker::pack), datum.ptr_);
    } else {
      enc_.header_->copy_string(ObStringDiffHeader::RightToLeft(),
          ObStringDiffHeader::LogicTrue<uint8_t>(),
          buf, datum.ptr_);
    }
    return OB_SUCCESS;
  }

  ObStringDiffEncoder &enc_;
};

int ObStringDiffEncoder::store_data(
    const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row_id < 0 || row_id >= rows_->count() || len < 0) {
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
      ColumnStoreFiller filler(*this);
      if (OB_FAIL(filler(row_id, datum, buf, len))) {
        LOG_WARN("fill data failed", K(ret), K(row_id), K(datum), KP(buf), K(len));
      }
    }
  }
  return ret;
}

int ObStringDiffEncoder::set_data_pos(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(header_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("call set data pos before store meta", K(ret));
  } else if (OB_UNLIKELY(offset < 0 || length < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data position",
        K(ret), K(offset), K(length), K(desc_), K_(column_header));
  } else {
    header_->offset_ = static_cast<uint32_t>(offset);
    header_->length_ = static_cast<uint32_t>(length);
  }
  return ret;
}

int ObStringDiffEncoder::get_var_length(const int64_t row_id, int64_t &length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(row_id < 0 || row_id >= rows_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_id));
  } else {
    length = row_store_size_;
  }
  return ret;
}

int64_t ObStringDiffEncoder::calc_size() const
{
  int64_t size = INT64_MAX;
  if (is_inited_) {
    bool hex_packing = string_size_ - common_size_ != row_store_size_;
    size = sizeof(*header_) + sizeof(*header_->diff_descs_) * diff_descs_.count()
        + common_size_ + (hex_packing ? hex_string_map_.size_ : 0);
    if (!desc_.is_var_data_) {
      size += rows_->count() * row_store_size_;
    } else {
      size += DEF_VAR_INDEX_BYTE * rows_->count()
          + row_store_size_ * (rows_->count() - null_cnt_ - nope_cnt_);
    }
    LOG_DEBUG("string diff encoded size", K(size), K_(column_index));
  }
  return size;
}

int ObStringDiffEncoder::store_fix_data(ObBufferWriter &buf_writer)
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
