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

#include "ob_const_encoder.h"
#include "ob_bit_stream.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_integer_array.h"
#include "ob_encoding_hash_util.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

const ObColumnHeader::Type ObConstEncoder::type_;

ObConstEncoder::ObConstEncoder() : sc_(ObExtendSC), count_(0), row_id_byte_(0),
                                   const_meta_header_(NULL), const_list_header_(NULL),
                                   ht_(NULL), dict_encoder_()
{
}

ObConstEncoder::~ObConstEncoder()
{
}

int ObConstEncoder::init(
    const ObColumnEncodingCtx &ctx,
    const int64_t column_index,
    const ObConstDatumRowArray &rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObIColumnEncoder::init(ctx, column_index, rows))) {
    LOG_WARN("init base column encoder failed", K(ret), K(ctx),
        K(column_index), "row count", rows.count());
  } else if (OB_FAIL(dict_encoder_.init(ctx, column_index, rows))) {
    LOG_WARN("failed to init dict encoder", K(ret), K(ctx), K(column_index));
  } else {
    column_header_.type_ = type_;
    sc_ = get_store_class_map()[ob_obj_type_class(column_type_.get_type())];
    ht_ = ctx.ht_;
  }
  return ret;
}

int ObConstEncoder::traverse(bool &suitable)
{
  STATIC_ASSERT(MAX_EXCEPTION_SIZE
      <= std::numeric_limits<__typeof__(const_meta_header_->count_)>::max(),
      "MAX_EXCEPTION_SIZE is too large");

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ht_->get_nope_list().size_ - 1 > MAX_EXCEPTION_SIZE + 1) {
    suitable = false;
  } else if (OB_FAIL(dict_encoder_.traverse(suitable))) {
    LOG_WARN("failed to traverse dict", K(ret));
  } else {
    // FIXME: maybe we can decide whether CONST is sutiable by ?
    suitable = true;
    int64_t max_row_id = 0;
    int64_t max_ref = ht_->size() - 1;
    int64_t max_cnt = 0;

    // choose the const value
    FOREACH(l, *ht_) {
      if (l->size_ > max_cnt) {
        max_cnt = l->size_;
        const_list_header_ = l->header_;
      }
    }
    if (0 < ht_->get_null_list().size_) {
      desc_.has_null_ = true;
      max_ref = ht_->size();
      if (ht_->get_null_list().size_ > max_cnt) {
        max_cnt = ht_->get_null_list().size_;
        const_list_header_ = ht_->get_null_list().header_;
      }
    }
    if (0 < ht_->get_nope_list().size_) {
      desc_.has_nope_ = true;
      max_ref = ht_->size() + 1;
      if (ht_->get_nope_list().size_ > max_cnt) {
        max_cnt = ht_->get_nope_list().size_;
        const_list_header_ = ht_->get_nope_list().header_;
      }
    }

    count_ = rows_->count() - max_cnt;
    desc_.need_data_store_ = false;
    desc_.need_extend_value_bit_store_ = false;

    if (OB_UNLIKELY(0 > count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count", K(ret), K_(count));
    } else if (count_ > MAX_EXCEPTION_SIZE
        || count_ > MAX(rows_->count() * MAX_EXCEPTION_PCT / 100, 1L)) {
      suitable = false;
    } else if (0 < count_) {
      const ObEncodingHashNode *node_list = ht_->get_node_list();
      for (int64_t i = ht_->get_node_cnt() - 1; i >= 0; --i) {
        if (const_list_header_->dict_ref_ != node_list[i].dict_ref_) {
          max_row_id = i;
          break;
        }
      }
      if (OB_UNLIKELY(max_row_id > UINT32_MAX || max_ref > UINT8_MAX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row_id_byte and ref_byte should be less than or equal to 2",
            K(ret), K(max_row_id), K(max_ref));
      } else {
        row_id_byte_ = get_byte_packed_int_size(max_row_id);
      }
    }
  }
  return ret;
}

int64_t ObConstEncoder::calc_size() const
{
  int64_t size = 0;
  int tmp_ret = OB_SUCCESS;
  const int64_t ref_byte = 1;
  if (0 == count_) {
    if (!desc_.has_null_ && !desc_.has_nope_) {
      tmp_ret = get_cell_len(*const_list_header_->datum_, size);
    }
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      LOG_WARN_RET(tmp_ret, "failed to get cell len", K(tmp_ret));
    } else {
      size += sizeof(ObConstMetaHeader);
    }
  } else {
    size = sizeof(ObConstMetaHeader) + dict_encoder_.calc_meta_size()
        + count_ * (row_id_byte_ + ref_byte);
  }
  return size;
}

int ObConstEncoder::store_meta_without_dict(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    char *buf = buf_writer.current();
    const_meta_header_ = reinterpret_cast<ObConstMetaHeader *>(buf);
    int64_t size = sizeof(ObConstMetaHeader);
    if (desc_.has_null_ || desc_.has_nope_) { // null or nope
      if (OB_FAIL(buf_writer.advance_zero(size))) {
        LOG_WARN("failed to advance buf writer", K(ret), K(size));
      } else {
        const_meta_header_->reset();
        const_meta_header_->count_ = 0;
        const_meta_header_->const_ref_ = desc_.has_null_ ? 1 : 2;
        const_meta_header_->offset_ = static_cast<uint16_t>(sizeof(ObConstMetaHeader));
      }
    } else {
      const ObDatum &datum = *const_list_header_->datum_;
      int64_t cell_len = 0;
      if (OB_FAIL(get_cell_len(datum, cell_len))) {
        LOG_WARN("failed to get cell len", K(ret), K(datum));
      } else {
        size += cell_len;
        buf += sizeof(ObConstMetaHeader);
        if (OB_FAIL(buf_writer.advance_zero(size))) {
          LOG_WARN("failed to advance buf writer", K(ret), K(size));
        } else if (OB_FAIL(store_value(datum, buf))) {
          LOG_WARN("failed to store value", K(ret), K(datum));
        } else {
          const_meta_header_->reset();
          const_meta_header_->count_ = 0;
          const_meta_header_->const_ref_ = 0;
          const_meta_header_->offset_ = static_cast<uint16_t>(sizeof(ObConstMetaHeader));
        }
      }
    }
    LOG_DEBUG("const meta header", K_(column_index), K_(*const_meta_header));
  }
  return ret;
}

int ObConstEncoder::get_cell_len(const ObDatum &datum, int64_t &length) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    switch (sc_) {
      case ObIntSC:
      case ObUIntSC: {
        length = get_type_size_map()[column_type_.get_type()];
        break;
      }
      case ObNumberSC:
      case ObDecimalIntSC:
      case ObStringSC:
      case ObTextSC:
      case ObJsonSC:
      case ObOTimestampSC:
      case ObIntervalSC:
      case ObGeometrySC:
      case ObRoaringBitmapSC: {
        length = datum.len_;
        break;
      }
      default: {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("not supported store class",
            K(ret), K_(sc), K_(column_type), K(datum));
      }
    }
  }
  return ret;
}

int ObConstEncoder::store_value(const ObDatum &datum, char *buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    switch (sc_) {
      case ObIntSC:
      case ObUIntSC: {
        const int64_t len = get_type_size_map()[column_type_.get_type()];
        MEMCPY(buf, datum.ptr_, len);
        break;
      }
      case ObDecimalIntSC:
      case ObNumberSC:
      case ObStringSC:
      case ObTextSC:
      case ObJsonSC:
      case ObOTimestampSC:
      case ObIntervalSC:
      case ObGeometrySC:
      case ObRoaringBitmapSC: {
        MEMCPY(buf, datum.ptr_, datum.len_);
        break;
      }
      default: {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("not supported store class",
            K(ret), K_(sc), K_(column_type), K(datum));
      }
    }
  }
  return ret;
}

int ObConstEncoder::get_encoding_store_meta_need_space(int64_t &need_size) const
{
  int ret = OB_SUCCESS;
  need_size = 0;

  if (count_ == 0) {
    const ObDatum &datum = *const_list_header_->datum_;
    int64_t cell_len = 0;
    if (OB_FAIL(get_cell_len(datum, cell_len))) {
      LOG_WARN("failed to get cell len", K(ret), K(datum));
    } else {
      need_size = cell_len + 2 * sizeof(ObConstMetaHeader);
    }
  } else {
    need_size = sizeof(ObConstMetaHeader)
        + count_ * (row_id_byte_ + 1);
    int64_t dict_encoder_need_size = 0;
    if (OB_FAIL(dict_encoder_.get_encoding_store_meta_need_space(dict_encoder_need_size))) {
      LOG_WARN("failed to get_encoding_store_meta_need_space", K(ret));
    } else {
      need_size += dict_encoder_need_size;
    }
  }

  return ret;
}

int ObConstEncoder::store_meta(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == count_) {
    if (OB_FAIL(store_meta_without_dict(buf_writer))) {
      LOG_WARN("failed to store meta without dict", K(ret));
    }
  } else {
    const int64_t ref_byte = 1;
    char *buf = buf_writer.current();
    const_meta_header_ = reinterpret_cast<ObConstMetaHeader *>(buf);
    int64_t size = sizeof(ObConstMetaHeader)
        + count_ * (row_id_byte_ + ref_byte);
    if (OB_FAIL(buf_writer.advance_zero(size))) {
      LOG_WARN("failed to advance buf_writer", K(ret), K(size));
    } else {
      const_meta_header_->reset();
      const_meta_header_->offset_ = static_cast<uint16_t>(size);
      const_meta_header_->count_ = static_cast<uint8_t>(count_);
      const_meta_header_->row_id_byte_ = row_id_byte_ & 0x7;
      if (OB_FAIL(dict_encoder_.store_meta(buf_writer))) {
        LOG_WARN("failed to store dict meta", K(ret));
      } else {
        // store_meta might recalculate dict_ref for all nodes in ht,
        // if dict needs to be sorted
        const int64_t const_ref = const_list_header_->dict_ref_;
        const_meta_header_->const_ref_ = static_cast<uint8_t>(const_ref);

        buf += sizeof(ObConstMetaHeader);
        ObIntegerArrayGenerator row_id_gen;
        ObIntegerArrayGenerator dict_ref_gen;
        if (OB_FAIL(dict_ref_gen.init(buf, ref_byte))) {
          LOG_WARN("failed to init dict_ref_gen", K(ret));
        } else if (OB_FAIL(row_id_gen.init(buf + count_ * ref_byte, row_id_byte_))) {
          LOG_WARN("failed to init row_id_gen", K(ret));
        } else {
          const ObEncodingHashNode *node_list = ht_->get_node_list();
          int64_t idx = 0;
          for (int64_t row_id = 0; OB_SUCC(ret) && row_id < ht_->get_node_cnt(); ++row_id) {
            const ObEncodingHashNode &node = node_list[row_id];
            if (const_ref != node.dict_ref_) {
              if (OB_UNLIKELY(idx >= count_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected idx", K(ret), K(idx), K_(count));
                int *p = 0;
                *p = 0;
              } else {
                row_id_gen.get_array().set(idx, row_id);
                dict_ref_gen.get_array().set(idx, node.dict_ref_);
                ++idx;
              }
            }
          }
        }
      }
    }
    LOG_DEBUG("test const meta header", K_(column_index), K_(*const_meta_header));
  }
  return ret;
}

int ObConstEncoder::store_data(const int64_t row_id, ObBitStream &bs,
    char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    UNUSED(row_id);
    UNUSED(bs);
    UNUSED(buf);
    UNUSED(len);
  }
  return ret;
}

int ObConstEncoder::get_row_checksum(int64_t &checksum) const
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

void ObConstEncoder::reuse()
{
  ObIColumnEncoder::reuse();
  count_ = 0;
  row_id_byte_ = 0;
  const_meta_header_ = NULL;
  const_list_header_ = NULL;
  ht_ = NULL;
  dict_encoder_.reuse();
}

int ObConstEncoder::store_fix_data(ObBufferWriter &buf_writer)
{
  UNUSED(buf_writer);
  return OB_NOT_SUPPORTED;
}

} // end namespace blocksstable
} // end namespace oceanbase
