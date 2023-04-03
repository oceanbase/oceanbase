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

#include "ob_inter_column_substring_decoder.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
const ObColumnHeader::Type ObInterColSubStrDecoder::type_;
ObInterColSubStrDecoder::ObInterColSubStrDecoder()
    : meta_header_(NULL)
{
}

ObInterColSubStrDecoder::~ObInterColSubStrDecoder()
{
}

int ObInterColSubStrDecoder::decode(ObColumnDecoderCtx &ctx, ObObj &cell, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(data) || OB_UNLIKELY(len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(len));
  } else {
    int64_t ref = 0;
    if (cell.get_meta() != ctx.obj_meta_) {
      cell.set_meta_type(ctx.obj_meta_);
    }

    if (!has_exc(ctx)) {
      ref = -1;
    } else {
      if (OB_FAIL(ObBitMapMetaReader<ObStringSC>::read(
          meta_header_->payload_,
          ctx.micro_block_header_->row_count_,
          ctx.is_bit_packing(), row_id,
          ctx.col_header_->length_ - sizeof(ObInterColSubStrMetaHeader),
          ref, cell, ctx.col_header_->get_store_obj_type()))) {
        LOG_WARN("meta_reader_ read failed", K(ret), K(row_id));
      }
    }

    // not an exception data
    if (OB_SUCC(ret) && -1 == ref) {
      ObObj ref_cell;
      if (OB_FAIL(ctx.ref_decoder_->decode(*ctx.ref_ctx_, ref_cell, row_id, bs, data, len))) {
        LOG_WARN("ref_decoder decode failed", K(ret),
            K(row_id), KP(data), K(len));
      } else if (ref_cell.is_null()) {
        cell.set_null();
      } else if (ObActionFlag::OP_NOP == ref_cell.get_ext()) {
        cell.set_ext(ObActionFlag::OP_NOP);
      } else {
        const char *cell_data =
            reinterpret_cast<const char *>(meta_header_) + ctx.col_header_->length_
            + row_id * (meta_header_->start_pos_byte_ + meta_header_->val_len_byte_);
        int64_t start_pos = 0;
        if (!meta_header_->is_same_start_pos()) {
          MEMCPY(&start_pos, cell_data, meta_header_->start_pos_byte_);
        } else {
          start_pos = meta_header_->start_pos_;
        }
        int64_t val_len = 0;
        if (!meta_header_->is_fix_length()) {
          MEMCPY(&val_len, cell_data + meta_header_->start_pos_byte_, meta_header_->val_len_byte_);
        } else {
          val_len = meta_header_->length_;
        }

        cell.v_.string_ = ref_cell.v_.string_ + start_pos;
        cell.val_len_ = static_cast<int32_t>(val_len);
      }
    }
  }
  return ret;
}

int ObInterColSubStrDecoder::update_pointer(const char *old_block, const char *cur_block)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(old_block) || OB_ISNULL(cur_block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(old_block), KP(cur_block));
  } else {
    ObIColumnDecoder::update_pointer(meta_header_, old_block, cur_block);
    //ObIColumnDecoder::update_pointer(meta_data_, old_block, cur_block);
  }
  return ret;
}

int ObInterColSubStrDecoder::get_ref_col_idx(int64_t &ref_col_idx) const
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ref_col_idx = meta_header_->ref_col_idx_;
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
