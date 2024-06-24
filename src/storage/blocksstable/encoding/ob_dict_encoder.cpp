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

#include "ob_dict_encoder.h"
#include "lib/container/ob_array_iterator.h"
#include "ob_bit_stream.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_integer_array.h"
#include "ob_encoding_hash_util.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
const ObColumnHeader::Type ObDictEncoder::type_;

ObDictEncoder::ObDictEncoder() : store_class_(ObExtendSC), type_store_size_(0),
                                 dict_fix_data_size_(0), var_data_size_(0), dict_index_byte_(0),
                                 max_integer_(0), dict_meta_header_(NULL),
                                 count_(0), need_sort_(false), ht_(NULL)
{
}

ObDictEncoder::~ObDictEncoder()
{
}

int ObDictEncoder::init(const ObColumnEncodingCtx &ctx,
                        const int64_t column_index,
                        const ObConstDatumRowArray &rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObIColumnEncoder::init(ctx, column_index, rows))) {
    LOG_WARN("init base column encoder failed", K(ret), K(ctx), K(column_index), "row_count", rows.count());
  } else {
    column_header_.type_ = type_;
    store_class_ = get_store_class_map()[ob_obj_type_class(column_type_.get_type())];
    type_store_size_ = get_type_size_map()[column_type_.get_type()];

    dict_fix_data_size_ = ctx.fix_data_size_;
    max_integer_ = ctx.max_integer_;
    var_data_size_ = ctx.dict_var_data_size_;
    need_sort_ = ctx.need_sort_;
    ht_ = ctx.ht_;

    if (type_store_size_ > 0 && type_store_size_ > sizeof(int64_t)) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("fix length type's store size should less than or equal to 8",
          K(ret), K_(type_store_size), "column type", column_type_.get_type());
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObDictEncoder::reuse()
{
  ObIColumnEncoder::reuse();
  store_class_ = ObExtendSC;
  type_store_size_ = 0;
  dict_fix_data_size_ = 0;
  var_data_size_ = 0;
  dict_index_byte_ = 0;
  max_integer_ = 0;
  dict_meta_header_ = NULL;
  count_ = 0;
  need_sort_ = false;
  ht_ = NULL;
}

int ObDictEncoder::traverse(bool &suitable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    suitable = true;
    count_ = ht_->size();
    const bool enable_bit_packing = ctx_->encoding_ctx_->encoder_opt_.enable_bit_packing_;

    if (store_var_dict()) {
      dict_index_byte_ = var_data_size_ <= UINT8_MAX ? 1 : 2;
      if (OB_UNLIKELY(var_data_size_ > UINT16_MAX)) {
        dict_index_byte_ = 4;
      }
    } else if (ObIntSC == store_class_ || ObUIntSC == store_class_) {
      if (enable_bit_packing) {
        dict_fix_data_size_ = get_int_size(max_integer_);
      } else {
        // pack integer value to 1, 2, 4, 8 bytes when using byte packing
        dict_fix_data_size_ = get_byte_packed_int_size(max_integer_);
      }
    }

    // only refs are stored in row data
    // so only need to consider bit packing and fix length
    int64_t max_ref = count_ - 1;
    desc_.need_data_store_ = true;
    desc_.is_var_data_ = false;
    if (0 < ht_->get_null_list().size_) {
      desc_.has_null_ = true;
      max_ref = count_;
    }
    if (0 < ht_->get_nope_list().size_) {
      desc_.has_nope_ = true;
      max_ref = count_ + 1;
    }
    // Dict stores the null and nope at the last two pos of dictionary
    desc_.need_extend_value_bit_store_ = false;

    bool bit_packing  = false;
    int64_t size = get_packing_size(bit_packing, max_ref, enable_bit_packing);
    if (bit_packing) {
      desc_.bit_packing_length_ = size;
    } else {
      desc_.fix_data_length_ = size;
    }
  }
  return ret;
}

int ObDictEncoder::build_dict()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (need_sort_) {
      ObPrecision precision = PRECISION_UNKNOWN_YET;
      if (column_type_.is_decimal_int()) {
        precision = column_type_.get_stored_precision();
        OB_ASSERT(precision != PRECISION_UNKNOWN_YET);
      }
      bool has_lob_header = is_lob_storage(column_type_.get_type());
      sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
          column_type_.get_type(), column_type_.get_collation_type(), column_type_.get_scale(),
          lib::is_oracle_mode(), has_lob_header, precision);
      ObCmpFunc cmp_func;
      cmp_func.cmp_func_ = lib::is_oracle_mode()
          ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
      lib::ob_sort(ht_->begin(), ht_->end(), DictCmp(ret, cmp_func));
      // calc new dict_ref if dict is sorted
      int64_t i = 0;
      FOREACH(l, *ht_) {
        FOREACH(n, *l) {
          n->dict_ref_ = i;
        }
        ++i;
      }
    }
    FOREACH(l, *ht_) {
      LOG_DEBUG("dict", K_(column_index), K(*l),
          K(*l->header_->datum_), K(*l->header_));
    }
  }
  return ret;
}

int ObDictEncoder::get_encoding_store_meta_need_space(int64_t &need_size) const
{
  int ret = OB_SUCCESS;
  need_size = 0;
  if (store_var_dict()) {
    need_size = sizeof(ObDictMetaHeader) +
        (count_ - 1) * dict_index_byte_ + var_data_size_;
  } else {
    need_size = sizeof(ObDictMetaHeader) +
        count_ * dict_fix_data_size_;
  }
  return ret;
}

int ObDictEncoder::store_meta(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(build_dict())) {
    LOG_WARN("failed to build dict", K(ret));
  } else {
    int64_t len = 0;
    int64_t offset = 0;

    char *buf = buf_writer.current();
    dict_meta_header_ = reinterpret_cast<ObDictMetaHeader *>(buf);
    buf += sizeof(ObDictMetaHeader);

    if (store_var_dict()) { // fill var dict data
      // var: the first var do not need index
      const int64_t meta_size = sizeof(ObDictMetaHeader) +
        (count_ - 1) * dict_index_byte_ + var_data_size_;
      int i = 0;
      ObIntegerArrayGenerator gen;
      if (OB_FAIL(buf_writer.advance_zero(meta_size))) {
        LOG_WARN("failed to advance buf_writer", K(ret));
      } else if (OB_FAIL(gen.init(buf, dict_index_byte_))) {
        LOG_WARN("init integer array generator failed", K(ret), K(dict_index_byte_));
      } else {
        dict_meta_header_->reset();
        // Set sorted attribute for ObNumber under "condensed" row-format for better performance
        if (need_sort_
            && column_type_.get_type_class() == ObNumberTC
            && ctx_->encoding_ctx_->encoder_opt_.store_sorted_var_len_numbers_dict_) {
          dict_meta_header_->set_sorted_attr();
        }
        dict_meta_header_->index_byte_ = static_cast<uint16_t>(dict_index_byte_);
        buf += (count_ - 1) * dict_index_byte_;
        FOREACH_X(l, *ht_, OB_SUCC(ret)) {
          if (OB_UNLIKELY(l->size_ <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row id array is empty", K(ret), "size", l->size_);
          } else if (OB_FAIL(store_dict(*l->header_->datum_, buf + offset, len))) {
            LOG_WARN("failed to store dict", K(ret));
          } else {
            if (i > 0) {
              gen.get_array().set(i - 1, offset);
            }
            offset += len;
            ++i;
          }
        }
      }
    } else { // fill fixed dict data
      const int64_t meta_size = sizeof(ObDictMetaHeader) +
        count_ * dict_fix_data_size_;
      if (OB_FAIL(buf_writer.advance_zero(meta_size))) {
        LOG_WARN("failed to advance buf_writer", K(ret));
      } else {
        dict_meta_header_->reset();
        dict_meta_header_->set_fix_length_attr();
        dict_meta_header_->data_size_ = static_cast<uint16_t>(dict_fix_data_size_);
        if (need_sort_) {
          dict_meta_header_->set_sorted_attr();
        }
        FOREACH_X(l, *ht_, OB_SUCC(ret)) {
          if (OB_UNLIKELY(l->size_ <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row id array is empty", K(ret), "size", l->size_);
          } else if (OB_FAIL(store_dict(*l->header_->datum_, buf + offset, len))) {
            LOG_WARN("failed to store dict", K(ret));
          } else {
            OB_ASSERT(len == dict_fix_data_size_);
            offset += dict_fix_data_size_;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      dict_meta_header_->count_ = static_cast<uint32_t>(count_);
      if (desc_.bit_packing_length_ > 0) {
        column_header_.set_bit_packing_attr();
      }
      // actually, DICT always uses fix length
      if (!desc_.is_var_data_) {
        column_header_.set_fix_lenght_attr();
      }
    }
  }
  return ret;
}

int ObDictEncoder::store_dict(const ObDatum &datum, char *buf, int64_t &len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    switch (store_class_) {
      case ObIntSC:
      case ObUIntSC:
        MEMCPY(buf, datum.ptr_, dict_fix_data_size_);
        len = dict_fix_data_size_;
        break;
      case ObNumberSC:
      case ObDecimalIntSC:
        // wide int type is not packed yet, store as fixed-length buf for now
      case ObStringSC:
      case ObTextSC:
      case ObJsonSC:
      case ObOTimestampSC:
      case ObIntervalSC:
      case ObGeometrySC:
      case ObRoaringBitmapSC:
        MEMCPY(buf, datum.ptr_, datum.len_);
        len = datum.len_;
        break;
      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("not supported store class",
            K(ret), K_(store_class), K_(column_type), K(datum));
    }
  }
  return ret;
}

int ObDictEncoder::get_row_checksum(int64_t &checksum) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    checksum = 0;
    FOREACH(l, *ht_) {
      checksum += l->header_->datum_->checksum(0) * l->size_;
    }
  }
  return ret;
}

bool ObDictEncoder::DictCmp::operator()(
    const ObEncodingHashNodeList &lhs,
    const ObEncodingHashNodeList &rhs)
{
  bool res = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
  } else if (OB_UNLIKELY(nullptr == lhs.header_ || nullptr == rhs.header_)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(ret_, "invalid argument", K_(ret), KP(lhs.header_), KP(rhs.header_));
  } else {
    int cmp_ret = 0;
    ret_ = cmp_func_.cmp_func_(*lhs.header_->datum_, *rhs.header_->datum_, cmp_ret);
    if (ret_ != OB_SUCCESS) {
      LOG_WARN_RET(ret_, "failed to compare", K_(ret), KP(lhs.header_), KP(rhs.header_));
    } else {
      res = cmp_ret < 0;
    }
  }
  return res;
}

struct ObDictEncoder::ColumnStoreFiller
{
  struct {} INCLUDE_EXT_CELL[0]; // call filler with extend cells
  STATIC_ASSERT(HAS_MEMBER(ColumnStoreFiller, INCLUDE_EXT_CELL),
      "dict column data filler should be called for extend value cell");

  ColumnStoreFiller(ObDictEncoder &encoder) : enc_(encoder) {}

  // get bit packing value
  inline int operator()(const int64_t row_id, const common::ObDatum &, uint64_t &v)
  {
    // performance critical, do not check parameters
    v = enc_.ht_->get_node_list()[row_id].dict_ref_;
    return OB_SUCCESS;
  }

  // fill fix store value
  inline int operator()(
      const int64_t row_id,
      const common::ObDatum &,
      char *buf,
      const int64_t len) const
  {
    // performance critical, do not check parameters
    MEMCPY(buf, &(enc_.ht_->get_node_list()[row_id].dict_ref_), len);
    return OB_SUCCESS;
  }

  ObDictEncoder &enc_;
};

int ObDictEncoder::store_fix_data(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_fix_encoder())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(desc));
  } else {
    dict_meta_header_->row_ref_size_ = static_cast<uint8_t>(desc_.bit_packing_length_ > 0
        ? desc_.bit_packing_length_
        : desc_.fix_data_length_);

    ColumnStoreFiller filler(*this);
    if (OB_FAIL(fill_column_store(buf_writer, *ctx_->col_datums_, filler, filler))) {
      LOG_WARN("fill dict column store failed", K(ret));
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
