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

int ObInterColSubStrDecoder::decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
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
    if (!has_exc(ctx)) {
      ref = -1;
    } else {
      if (OB_FAIL(ObBitMapMetaReader<ObStringSC>::read(
          meta_header_->payload_,
          ctx.micro_block_header_->row_count_,
          ctx.is_bit_packing(), row_id,
          ctx.col_header_->length_ - sizeof(ObInterColSubStrMetaHeader),
          ref, datum, ctx.col_header_->get_store_obj_type()))) {
        LOG_WARN("meta_reader_ read failed", K(ret), K(row_id));
      }
    }

    // not an exception data
    if (OB_SUCC(ret) && -1 == ref) {
      ObDatum ref_datum;
      if (OB_FAIL(ctx.ref_decoder_->decode(*ctx.ref_ctx_, ref_datum, row_id, bs, data, len))) {
        LOG_WARN("ref_decoder decode failed", K(ret),
            K(row_id), KP(data), K(len));
      } else if (ref_datum.is_null()) {
        datum.set_null();
      } else if (ref_datum.is_nop()) {
        datum.set_ext();
        datum.no_cv(datum.extend_obj_)->set_ext(common::ObActionFlag::OP_NOP);
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

        datum.pack_ =  static_cast<int32_t>(val_len);
        datum.ptr_ = ref_datum.ptr_ + start_pos;
      }
    }
  }
  return ret;
}

int ObInterColSubStrDecoder::decode_vector(
    const ObColumnDecoderCtx &decoder_ctx,
    const ObIRowIndex *row_index,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (has_exc(decoder_ctx)) {
      const char *except_bitset_buf = meta_header_->payload_ + sizeof(ObBitMapMetaHeader);
      for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
        const int64_t row_id = vector_ctx.row_ids_[i];
        const int64_t ref = BitSet::get_ref(reinterpret_cast<const uint64_t *>(except_bitset_buf), row_id);
        reinterpret_cast<int32_t *>(vector_ctx.len_arr_)[i] = static_cast<int32_t>(ref);
      }
    }

    switch (vector_ctx.get_format()) {
    case VEC_DISCRETE: {
      ret = inner_decode_vector<ObDiscreteFormat>(decoder_ctx, row_index, vector_ctx);
      break;
    }
    case VEC_CONTINUOUS: {
      ret = inner_decode_vector<ObContinuousFormat>(decoder_ctx, row_index, vector_ctx);
      break;
    }
    case VEC_UNIFORM: {
      ret = inner_decode_vector<ObUniformFormat<false>>(decoder_ctx, row_index, vector_ctx);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected vector format for inter-column-substr encoding", K(ret), K(vector_ctx));
    }
    }
  }
  return ret;
}

template<typename VectorType>
int ObInterColSubStrDecoder::inner_decode_vector(
    const ObColumnDecoderCtx &decoder_ctx,
    const ObIRowIndex *row_index,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t col_ref_start_idx = 0;
  int64_t col_ref_end_idx = 0;

  if (has_exc(decoder_ctx)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < vector_ctx.row_cap_; ++i) {
      const int64_t ref = reinterpret_cast<int32_t *>(vector_ctx.len_arr_)[i];
      if (-1 == ref) {
        // not exception
        col_ref_end_idx = i;
      } else {
        // found exception
        if (col_ref_end_idx >= col_ref_start_idx) {
          // decode previous referenced range
          const int64_t row_cap = col_ref_end_idx - col_ref_start_idx + 1;
          const int32_t *row_id_arr = vector_ctx.row_ids_ + col_ref_start_idx;
          const char **ptr_arr = vector_ctx.ptr_arr_ + col_ref_start_idx;
          uint32_t *len_arr = vector_ctx.len_arr_ + col_ref_start_idx;
          const int64_t vec_offset = vector_ctx.vec_offset_ + col_ref_start_idx;
          ObVectorDecodeCtx ref_range_vector_ctx(
              ptr_arr, len_arr, row_id_arr, row_cap, vec_offset, vector_ctx.vec_header_);
          if (OB_FAIL(decoder_ctx.ref_decoder_->decode_vector(
              *decoder_ctx.ref_ctx_, row_index, ref_range_vector_ctx))) {
            LOG_WARN("Failed to decode vector from referenced column", K(ret),
                K(decoder_ctx), K(col_ref_start_idx), K(col_ref_end_idx));
          } else if (OB_FAIL(rearrange_sub_str_column_len<VectorType>(decoder_ctx, ref_range_vector_ctx))) {
            LOG_WARN("Failed to rearrange substr column len", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          col_ref_start_idx = i + 1;
          const char *exc_buf = meta_header_->payload_;
          const int64_t exc_buf_len = decoder_ctx.col_header_->length_ - sizeof(ObInterColSubStrMetaHeader);
          if (OB_FAIL(decode_exception_vector(
              decoder_ctx, ref, exc_buf, exc_buf_len, vector_ctx.vec_offset_ + i, vector_ctx.vec_header_))) {
            LOG_WARN("Failed to decode exception to vector", K(ret), K(i), K(decoder_ctx), K(vector_ctx));
          }
        }
      }
    }
  } else {
    col_ref_start_idx = 0;
    col_ref_end_idx = vector_ctx.row_cap_ - 1;
  }

  if (OB_SUCC(ret) && col_ref_end_idx >= col_ref_start_idx) {
    // decode last referenced range
    const int64_t row_cap = col_ref_end_idx - col_ref_start_idx + 1;
    const int32_t *row_id_arr = vector_ctx.row_ids_ + col_ref_start_idx;
    const char **ptr_arr = vector_ctx.ptr_arr_ + col_ref_start_idx;
    uint32_t *len_arr = vector_ctx.len_arr_ + col_ref_start_idx;
    const int64_t vec_offset = vector_ctx.vec_offset_ + col_ref_start_idx;
    ObVectorDecodeCtx ref_range_vector_ctx(
        ptr_arr, len_arr, row_id_arr, row_cap, vec_offset, vector_ctx.vec_header_);
    if (OB_FAIL(decoder_ctx.ref_decoder_->decode_vector(
        *decoder_ctx.ref_ctx_, row_index, ref_range_vector_ctx))) {
      LOG_WARN("Failed to decode vector from referenced column", K(ret),
          K(decoder_ctx), K(col_ref_start_idx), K(col_ref_end_idx));
    } else if (OB_FAIL(rearrange_sub_str_column_len<VectorType>(decoder_ctx, ref_range_vector_ctx))) {
      LOG_WARN("Failed to rearrange substr column len", K(ret));
    }
  }
  return ret;
}

template<typename VectorType>
int ObInterColSubStrDecoder::rearrange_sub_str_column_len(
    const ObColumnDecoderCtx &decoder_ctx,
    ObVectorDecodeCtx &vec_ctx) const
{
  int ret = OB_SUCCESS;
  VectorType *vector = static_cast<VectorType *>(vec_ctx.get_vector());
  for (int64_t i = 0; i < vec_ctx.row_cap_; ++i) {
    const int64_t row_id = vec_ctx.row_ids_[i];
    const int64_t curr_vec_offset = vec_ctx.vec_offset_ + i;
    if (!vector->is_null(curr_vec_offset)) {
      const int64_t cell_offset = decoder_ctx.col_header_->length_
          + row_id * (meta_header_->start_pos_byte_ + meta_header_->val_len_byte_);
      const char *cell_data = reinterpret_cast<const char *>(meta_header_) + cell_offset;
      int64_t start_pos = 0;
      uint32_t val_len = 0;
      if (meta_header_->is_same_start_pos()) {
        start_pos = meta_header_->start_pos_;
      } else {
        MEMCPY(&start_pos, cell_data, meta_header_->start_pos_byte_);
      }
      if (meta_header_->is_fix_length()) {
        val_len = meta_header_->length_;
      } else {
        MEMCPY(&val_len, cell_data + meta_header_->start_pos_byte_, meta_header_->val_len_byte_);
      }
      const char *full_str = vector->get_payload(curr_vec_offset);
      vector->set_payload_shallow(curr_vec_offset, full_str + start_pos, val_len);
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
