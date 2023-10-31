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

#include "ob_rle_encoder.h"
#include "ob_bit_stream.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_integer_array.h"
#include "ob_encoding_hash_util.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

const ObColumnHeader::Type ObRLEEncoder::type_;

ObRLEEncoder::ObRLEEncoder() : count_(0), row_id_byte_(0), ref_byte_(0),
                               rle_meta_header_(NULL), ht_(NULL), dict_encoder_()
{
}

ObRLEEncoder::~ObRLEEncoder()
{
}

int ObRLEEncoder::init(
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
    ht_ = ctx.ht_;
  }
  return ret;
}

void ObRLEEncoder::reuse()
{
  ObIColumnEncoder::reuse();
  count_ = 0;
  row_id_byte_ = 0;
  ref_byte_ = 0;
  rle_meta_header_ = NULL;
  ht_ = NULL;
  dict_encoder_.reuse();
}

int ObRLEEncoder::traverse(bool &suitable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(dict_encoder_.traverse(suitable))) {
    LOG_WARN("failed to traverse dict", K(ret));
  } else {
    // FIXME: maybe we can decide whether RLE is
    // sutiable by max_ref and max_rle_row_id
    suitable = true;
    int64_t pre = 0;
    int64_t max_rle_row_id = 0;
    count_ = 1;
    int64_t max_ref = ht_->size() - 1;

    const ObEncodingHashNode *node_list = ht_->get_node_list();
    for (int64_t i = 1; i < ht_->get_node_cnt(); ++i) {
      if (node_list[i].dict_ref_ != node_list[pre].dict_ref_) {
        max_rle_row_id = i;
        pre = i;
        ++count_;
      }
    }
    if (0 < ht_->get_null_list().size_) {
      desc_.has_null_ = true;
      max_ref = ht_->size();
    }
    if (0 < ht_->get_nope_list().size_) {
      desc_.has_nope_ = true;
      max_ref = ht_->size() + 1;
    }

    desc_.need_data_store_ = false;
    desc_.need_extend_value_bit_store_ = false;
    if (OB_UNLIKELY(count_ >= MAX_DICT_COUNT)) {
      suitable = false;
    } else if (OB_UNLIKELY(max_rle_row_id > UINT32_MAX || max_ref > UINT32_MAX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row_id_byte and ref_byte should be less than or equal to 2",
          K(ret), K(max_rle_row_id), K(max_ref));
    } else {
      row_id_byte_ = get_byte_packed_int_size(max_rle_row_id);
      ref_byte_ = get_byte_packed_int_size(max_ref);
    }

  }
  return ret;
}

int64_t ObRLEEncoder::calc_size() const
{
  return sizeof(ObRLEMetaHeader) + dict_encoder_.calc_meta_size() +
    count_ * (row_id_byte_ + ref_byte_);
}

int ObRLEEncoder::get_encoding_store_meta_need_space(int64_t &need_size) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dict_encoder_.get_encoding_store_meta_need_space(need_size))) {
    STORAGE_LOG(WARN, "fail to get_encoding_store_meta_need_space", K(ret), K(dict_encoder_));
  } else {
    need_size = need_size + sizeof(ObRLEMetaHeader) + count_ * (row_id_byte_ + ref_byte_);
  }
  return ret;
}

int ObRLEEncoder::store_meta(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    char *buf = buf_writer.current();
    rle_meta_header_ = reinterpret_cast<ObRLEMetaHeader *>(buf);
    int64_t size = sizeof(ObRLEMetaHeader) + count_ * (row_id_byte_ + ref_byte_);
    if (OB_FAIL(buf_writer.advance_zero(size))) {
      LOG_WARN("failed to advance buf_writer", K(ret));
    } else {
      rle_meta_header_->reset();
      rle_meta_header_->count_ = static_cast<uint32_t>(count_);
      rle_meta_header_->offset_ = static_cast<uint32_t>(size);
      rle_meta_header_->row_id_byte_ = row_id_byte_ & 0x7;
      rle_meta_header_->ref_byte_ = ref_byte_ & 0x7;
      if (OB_FAIL(dict_encoder_.store_meta(buf_writer))) {
        LOG_WARN("failed to store dict meta", K(ret));
      } else {
        // store_meta might recalculates dict_ref for all nodes in ht,
        // if dict needs to be sorted
        buf += sizeof(ObRLEMetaHeader);
        ObIntegerArrayGenerator row_id_gen;
        ObIntegerArrayGenerator dict_ref_gen;
        if (OB_FAIL(row_id_gen.init(buf, row_id_byte_))) {
          LOG_WARN("failed to init row_id_gen", K(ret));
        } else if (OB_FAIL(dict_ref_gen.init(buf + count_ * row_id_byte_, ref_byte_))) {
          LOG_WARN("failed to init dict_ref_gen", K(ret));
        } else {
          const ObEncodingHashNode *pre = NULL;
          const ObEncodingHashNode *node_list = ht_->get_node_list();
          int64_t idx = 0;
          for (int64_t i = 0; i < ht_->get_node_cnt(); ++i) {
            const ObEncodingHashNode &node = node_list[i];
            if (NULL == pre || node.dict_ref_ != pre->dict_ref_) {
              row_id_gen.get_array().set(idx, i);
              dict_ref_gen.get_array().set(idx, node.dict_ref_);
              pre = &node;
              ++idx;
            }
          }
        }
      }
    }
    LOG_DEBUG("test rle meta header", K_(column_index), K_(*rle_meta_header));
  }
  return ret;
}

int ObRLEEncoder::store_data(const int64_t row_id, ObBitStream &bs,
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

int ObRLEEncoder::get_row_checksum(int64_t &checksum) const
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

int ObRLEEncoder::store_fix_data(ObBufferWriter &buf_writer)
{
  UNUSED(buf_writer);
  return OB_NOT_SUPPORTED;
}

} // end namespace blocksstable
} // end namespace oceanbase
