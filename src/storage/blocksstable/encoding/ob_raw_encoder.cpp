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

#include "ob_raw_encoder.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

struct ObRawEncoder::DatumDataSetter
{
  int operator()(const int64_t, const common::ObDatum &datum, char *buf, const int64_t len) const
  {
    MEMCPY(buf, datum.ptr_, len);
    return OB_SUCCESS;
  }
};

const ObColumnHeader::Type ObRawEncoder::type_;
ObRawEncoder::ObRawEncoder() : store_class_(ObExtendSC), type_store_size_(0),
    null_cnt_(0), nope_cnt_(0), fix_data_size_(0), max_integer_(0), var_data_size_(0)
{
}

ObRawEncoder::~ObRawEncoder()
{
}


int ObRawEncoder::init(const ObColumnEncodingCtx &ctx,
    const int64_t column_index, const ObConstDatumRowArray &rows)
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
    store_class_ = get_store_class_map()[ob_obj_type_class(column_type_.get_type())];
    type_store_size_ = get_type_size_map()[column_type_.get_type()];
    if (type_store_size_ > 0) {
      if(type_store_size_ > sizeof(int64_t)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("fix length type's store size should less than or equal to 8", K(ret));
      }
    }
    null_cnt_ = ctx.null_cnt_;
    nope_cnt_ = ctx.nope_cnt_;
    fix_data_size_ = ctx.fix_data_size_;
    max_integer_ = ctx.max_integer_;
    var_data_size_ = ctx.var_data_size_;
  }
  return ret;
}

void ObRawEncoder::reuse()
{
  ObIColumnEncoder::reuse();
  store_class_ = ObExtendSC;
  type_store_size_ = 0;
  null_cnt_ = 0;
  nope_cnt_ = 0;
  fix_data_size_ = 0;
  max_integer_ = 0;
  var_data_size_ = 0;
}

int ObRawEncoder::traverse(bool &suitable) {
  const bool force_var_store = false;
  return traverse(force_var_store, suitable);
}

int ObRawEncoder::traverse(const bool force_var_store, bool &suitable)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    suitable = true;
    // traverse column data
    switch (store_class_) {
      case ObIntSC:
      case ObUIntSC: {
          bool bit_packing = false;
          const bool enable_bit_packing = ctx_->encoding_ctx_->encoder_opt_.enable_bit_packing_;
          int64_t size = get_packing_size(bit_packing, max_integer_, enable_bit_packing);
          if (bit_packing) {
            if (force_var_store || size * (nope_cnt_ + null_cnt_) > rows_->count() * DEF_VAR_INDEX_BYTE * CHAR_BIT) {
              desc_.is_var_data_ = true;
              fix_data_size_ = size / CHAR_BIT + 1;
            } else {
              desc_.bit_packing_length_ = size;
            }
          } else if (!force_var_store) {
            desc_.fix_data_length_ = size;
            fix_data_size_ = size;
          } else {
            desc_.is_var_data_ = true;
            fix_data_size_ = size;
          }
          if (desc_.is_var_data_) {
            var_data_size_ = fix_data_size_ * (rows_->count() - nope_cnt_ - null_cnt_);
          }
        break;
      }
      case ObNumberSC:
      case ObStringSC:
      case ObTextSC:
      case ObJsonSC:
      case ObGeometrySC:
      case ObOTimestampSC:
      case ObIntervalSC: {
          if (force_var_store || fix_data_size_ < 0) {
            desc_.is_var_data_ = true;
          } else {
            desc_.fix_data_length_ = fix_data_size_;
          }
        break;
      }
      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("not supported store class", K(ret), K_(store_class), K_(column_type));
    }
    if (OB_SUCC(ret)) {
      // turn to var data store if has too many extend values
      if (desc_.fix_data_length_ > 0 && 0 == desc_.bit_packing_length_) {
        if (desc_.fix_data_length_ * (nope_cnt_ + null_cnt_)
            > rows_->count() * DEF_VAR_INDEX_BYTE) {
          desc_.is_var_data_ = true;
          desc_.fix_data_length_ = 0;
        }
      }
      desc_.need_data_store_ = true;
      desc_.has_null_ = null_cnt_ > 0;
      desc_.has_nope_ = nope_cnt_ > 0;
      desc_.need_extend_value_bit_store_ = desc_.has_null_ || desc_.has_nope_;

      if (desc_.need_extend_value_bit_store_) {
        column_header_.set_has_extend_value_attr();
      }
      if (!desc_.is_var_data_) {
        column_header_.set_fix_lenght_attr();
        if (desc_.fix_data_length_ > 0) {
          column_header_.length_ = static_cast<uint32_t>(desc_.fix_data_length_);
        } else if (desc_.bit_packing_length_ > 0) {
          column_header_.set_bit_packing_attr();
          column_header_.length_ = static_cast<uint32_t>(desc_.bit_packing_length_);
        }
      }
    }
  }
  LOG_DEBUG("data desc", K_(desc), K(*this));

  return ret;
}

int ObRawEncoder::set_data_pos(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(offset < 0 || length < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data position", K(ret), K(offset), K(length));
  } else {
    column_header_.offset_ = static_cast<uint32_t>(offset);
    column_header_.length_ = static_cast<uint32_t>(length);
  }
  return ret;
}

int ObRawEncoder::get_var_length(const int64_t row_id, int64_t &length)
{
  // FIXME baihua: called for every cells, no checking?
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
      switch (store_class_) {
        case ObIntSC:
        case ObUIntSC: {
          length = fix_data_size_;
          break;
        }
        case ObNumberSC:
        case ObStringSC:
        case ObTextSC:
        case ObJsonSC:
        case ObGeometrySC:
        case ObOTimestampSC:
        case ObIntervalSC: {
          length = datum.len_;
          break;
        }
        default:
          ret = OB_INNER_STAT_ERROR;
          LOG_WARN("not supported store class", K(ret), K_(store_class), K_(column_type), K(datum));
      }
    }
  }
  return ret;
}

int ObRawEncoder::store_meta(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    UNUSED(buf_writer);
    // do nothing for raw encoding
  }
  return ret;
}

int64_t ObRawEncoder::calc_size() const
{
  int64_t size = INT64_MAX;
  if (is_inited_) {
    if (desc_.bit_packing_length_ > 0) {
      size = desc_.bit_packing_length_ * rows_->count() / CHAR_BIT + 1;
    } else if (!desc_.is_var_data_) {
      size = desc_.fix_data_length_ * rows_->count();
    } else {
      size = var_data_size_ + rows_->count() * DEF_VAR_INDEX_BYTE;
    }
    if (nope_cnt_ > 0 || null_cnt_ > 0) {
      size += (rows_->count() * ctx_->extend_value_bit_ + 1) / CHAR_BIT;
    }
  }
  return size;
}

int ObRawEncoder::store_fix_data(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_fix_encoder())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(desc));
  } else {
    ValueGetter getter(desc_.bit_packing_length_);
    column_header_.length_ = static_cast<uint32_t>(desc_.bit_packing_length_ > 0
        ? desc_.bit_packing_length_
        : desc_.fix_data_length_);
    if (OB_FAIL(fill_column_store(buf_writer, *ctx_->col_datums_, getter, DatumDataSetter()))) {
      LOG_WARN("fill datum column store failed", K(ret));
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
