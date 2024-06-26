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

#include "ob_string_diff_decoder.h"

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_bit_stream.h"
#include "ob_raw_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
const ObColumnHeader::Type ObStringDiffDecoder::type_;

ObStringDiffDecoder::ObStringDiffDecoder() : header_(NULL)
{
}

ObStringDiffDecoder::~ObStringDiffDecoder()
{
}

int ObStringDiffDecoder::decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len) const
{
  int ret = OB_SUCCESS;
  uint64_t val = STORED_NOT_EXT;
  ObBitStream fix_bs;
  const char *col_data = reinterpret_cast<const char *>(header_) + ctx.col_header_->length_;
  int64_t data_offset = 0;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == data || len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(len));
  } else {

    // read extend value bit
    if (ctx.has_extend_value()) {
      if (ctx.is_fix_length()) {
        data_offset = ctx.micro_block_header_->row_count_ * ctx.micro_block_header_->extend_value_bit_;
        data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
        if (OB_FAIL(ObBitStream::get(reinterpret_cast<const unsigned char *>(col_data),
            row_id * ctx.micro_block_header_->extend_value_bit_,
            ctx.micro_block_header_->extend_value_bit_, val))) {
          LOG_WARN("get extend value failed",
              K(ret), K(fix_bs), K_(header), K(ctx));
        }
      } else {
        if (OB_FAIL(bs.get(ctx.col_header_->extend_value_index_,
                ctx.micro_block_header_->extend_value_bit_, val))) {
          LOG_WARN("get extend value failed",
              K(ret), K(bs), K_(header), K(ctx));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (STORED_NOT_EXT != val) {
    set_stored_ext_value(datum, static_cast<ObStoredExtValue>(val));
  } else {
    const char *cell_data = NULL;
    int64_t cell_len = 0;
    // get cell data offset and length
    if (ctx.is_fix_length()) {
      cell_data = col_data + data_offset + row_id * header_->length_;
    } else {
      if (OB_FAIL(ObRawDecoder::locate_cell_data(cell_data, cell_len, data, len,
              *ctx.micro_block_header_, *ctx.col_header_, *header_))) {
        LOG_WARN("locate cell data failed", K(ret), K(len),
            K(ctx), "header", *header_);
      }
    }

    char *buf = NULL;
    if (OB_SUCC(ret)) {
      const static uint16_t min_buf_size = 128;
      const int64_t buf_size = std::max(header_->string_size_, min_buf_size);
      if (OB_ISNULL(buf = static_cast<char *>(ctx.allocator_->alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(buf_size));
      } else {
        header_->copy_string(ObStringDiffHeader::LeftToRight(), std::logical_not<uint8_t>(),
            header_->common_data(), buf);
      }
    }

    if (OB_SUCC(ret)) {
      //int64_t cell_len = header_->length_;
      if (header_->is_hex_packing()) {
        ObHexStringUnpacker unpacker(header_->hex_char_array(),
            reinterpret_cast<const unsigned char *>(cell_data));
        header_->copy_hex_string(unpacker,
            static_cast<void (ObHexStringUnpacker::*)(unsigned char &)>(&ObHexStringUnpacker::unpack),
            reinterpret_cast<unsigned char *>(buf));
      } else {
        header_->copy_string(ObStringDiffHeader::LeftToRight(),
            ObStringDiffHeader::LogicTrue<uint8_t>(),
            cell_data, buf);
      }
      datum.pack_ = header_->string_size_;
      datum.ptr_ = buf;
    }
  }

  return ret;
}

int ObStringDiffDecoder::update_pointer(const char *old_block, const char *cur_block)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(old_block) || OB_ISNULL(cur_block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(old_block), KP(cur_block));
  } else {
    ObIColumnDecoder::update_pointer(header_, old_block, cur_block);
  }
  return ret;
}

// Internal call, not check parameters for performance
int ObStringDiffDecoder::batch_decode(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  UNUSED(cell_datas);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else {
    const unsigned char *col_data = reinterpret_cast<const unsigned char *>(header_)
        + ctx.col_header_->length_;
    int64_t data_offset = 0;
    if (ctx.has_extend_value()) {
      if (ctx.is_fix_length()) {
        data_offset = ctx.micro_block_header_->row_count_ * ctx.micro_block_header_->extend_value_bit_;
        data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
        if (OB_FAIL(set_null_datums_from_fixed_column(
            ctx, row_ids, row_cap, col_data, datums))) {
          LOG_WARN("Failed to set null datums from fixed data", K(ret), K(ctx));
        }
      } else if (OB_FAIL(set_null_datums_from_var_column(
          ctx, row_index, row_ids, row_cap, datums))) {
        LOG_WARN("Failed to set null datums from var data", K(ret), K(ctx));
      }
    }

    // Fill string data
    const uint16_t string_size = header_->string_size_;
    const static uint16_t min_buf_size = 128;
    const int64_t buf_size = std::max(string_size, min_buf_size);
    char *buf = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = static_cast<char *>(ctx.allocator_->alloc(buf_size * row_cap)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory", K(ret), K(buf_size), K(row_cap));
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        header_->copy_string(ObStringDiffHeader::LeftToRight(), std::logical_not<uint8_t>(),
            header_->common_data(), buf + i * buf_size);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (ctx.is_fix_length()) {
      int64_t row_id = 0;
      const unsigned char *cell_data = nullptr;
      char *str_ptr = nullptr;
      for (int64_t i = 0; i < row_cap; ++i) {
        if (ctx.has_extend_value() && datums[i].is_null()) {
          // Skip
        } else {
          row_id = row_ids[i];
          cell_data = col_data + data_offset + row_id * header_->length_;
          str_ptr = buf + i * buf_size;
          if (header_->is_hex_packing()) {
            ObHexStringUnpacker unpacker(header_->hex_char_array(),
                reinterpret_cast<const unsigned char *>(cell_data));
            header_->copy_hex_string(unpacker,
                static_cast<void (ObHexStringUnpacker::*)(unsigned char &)>(&ObHexStringUnpacker::unpack),
                reinterpret_cast<unsigned char *>(str_ptr));
          } else {
            header_->copy_string(
              ObStringDiffHeader::LeftToRight(),
              ObStringDiffHeader::LogicTrue<uint8_t>(),
              cell_data, str_ptr);
          }
          datums[i].pack_ = string_size;
          datums[i].ptr_ = str_ptr;
        }
      }
    } else {
      int64_t row_id = 0;
      const char *row_data = nullptr;
      int64_t row_len = 0;
      const char *cell_data = nullptr;
      int64_t cell_len = 0;
      char *str_ptr = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
        row_id = row_ids[i];
        str_ptr = buf + i * buf_size;
        if (ctx.has_extend_value() && datums[i].is_null()) {
          // Do nothing
        } else if (OB_FAIL(locate_row_data(ctx, row_index, row_id, row_data, row_len))) {
          LOG_WARN("Failed to read data offset from row index", K(ret), KP(row_index));
        } else if (OB_FAIL(ObRawDecoder::locate_cell_data(cell_data, cell_len, row_data, row_len,
            *ctx.micro_block_header_, *ctx.col_header_, *header_))) {
          LOG_WARN("Failed to locate cell data", K(ret), K(ctx), K(cell_len));
        } else {
          if (header_->is_hex_packing()) {
            ObHexStringUnpacker unpacker(header_->hex_char_array(),
                reinterpret_cast<const unsigned char *>(cell_data));
            header_->copy_hex_string(unpacker,
                static_cast<void (ObHexStringUnpacker::*)(unsigned char &)>(&ObHexStringUnpacker::unpack),
                reinterpret_cast<unsigned char *>(str_ptr));
          } else {
            header_->copy_string(
              ObStringDiffHeader::LeftToRight(),
              ObStringDiffHeader::LogicTrue<uint8_t>(),
              cell_data, str_ptr);
          }
          datums[i].pack_ = string_size;
          datums[i].ptr_ = str_ptr;
        }
      }
    }
  }
  return ret;
}

int ObStringDiffDecoder::decode_vector(
    const ObColumnDecoderCtx &decoder_ctx,
    const ObIRowIndex *row_index,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else {
    #define FILL_VECTOR_FUNC_BY_HEX(func_name, vector_type, has_null, hex_packed) \
      if (hex_packed) { \
        ret = func_name<vector_type, has_null, true>(decoder_ctx, row_index, vector_ctx); \
      } else { \
        ret = func_name<vector_type, has_null, false>(decoder_ctx, row_index, vector_ctx); \
      }

    #define FILL_VECTOR_FUNC(func_name, vector_type, has_null, hex_packed) \
      if (has_null) { \
        FILL_VECTOR_FUNC_BY_HEX(func_name, vector_type, true, hex_packed); \
      } else { \
        FILL_VECTOR_FUNC_BY_HEX(func_name, vector_type, false, hex_packed); \
      }

    bool has_null = false;
    if (decoder_ctx.is_fix_length()) {
      has_null = decoder_ctx.has_extend_value();
      switch (vector_ctx.get_format()) {
      case VEC_DISCRETE: {
        FILL_VECTOR_FUNC(decode_vector_from_fixed_data, ObDiscreteFormat, has_null, header_->is_hex_packing());
        break;
      }
      case VEC_CONTINUOUS: {
        FILL_VECTOR_FUNC(decode_vector_from_fixed_data, ObContinuousFormat, has_null, header_->is_hex_packing());
        break;
      }
      case VEC_UNIFORM: {
        FILL_VECTOR_FUNC(decode_vector_from_fixed_data, ObUniformFormat<false>, has_null, header_->is_hex_packing());
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", K(ret), K(vector_ctx));
      }
      }
    } else if (OB_FAIL(batch_locate_var_len_row(decoder_ctx, row_index, vector_ctx, has_null))) {
      LOG_WARN("Failed to batch locate variable length row", K(ret), K(decoder_ctx), K(vector_ctx));
    } else {
      if (has_null) {
        ret = ObIColumnDecoder::batch_locate_cell_data<ObStringDiffHeader, true>(decoder_ctx, *header_,
          vector_ctx.ptr_arr_, vector_ctx.len_arr_, vector_ctx.row_ids_, vector_ctx.row_cap_);
      } else {
        ret = ObIColumnDecoder::batch_locate_cell_data<ObStringDiffHeader, false>(decoder_ctx, *header_,
          vector_ctx.ptr_arr_, vector_ctx.len_arr_, vector_ctx.row_ids_, vector_ctx.row_cap_);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to locate cell datas", K(ret));
      } else {
        switch (vector_ctx.get_format()) {
        case VEC_DISCRETE: {
          FILL_VECTOR_FUNC(decode_vector_from_var_len_data, ObDiscreteFormat, has_null, header_->is_hex_packing());
          break;
        }
        case VEC_CONTINUOUS: {
          FILL_VECTOR_FUNC(decode_vector_from_var_len_data, ObContinuousFormat, has_null, header_->is_hex_packing());
          break;
        }
        case VEC_UNIFORM: {
          FILL_VECTOR_FUNC(decode_vector_from_var_len_data, ObUniformFormat<false>, has_null, header_->is_hex_packing());
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vector format", K(ret), K(vector_ctx));
        }
        }
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("failed to decode string diff data to vector", K(ret), K(decoder_ctx), K(vector_ctx));
    }
    #undef FILL_VECTOR_FUNC_BY_HEX
    #undef FILL_VECTOR_FUNC
    LOG_DEBUG("[Vector decode] decode string diff data to vector", K(ret), K(decoder_ctx.is_fix_length()),
        K(has_null), KPC_(header), K(vector_ctx));
  }
  return ret;
}



template <typename VectorType, bool HAS_NULL, bool HEX_PACKED>
int ObStringDiffDecoder::decode_vector_from_fixed_data(
    const ObColumnDecoderCtx &decoder_ctx,
    const ObIRowIndex *row_index,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  VectorType *vector = static_cast<VectorType *>(vector_ctx.get_vector());
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(header_)
      + decoder_ctx.col_header_->length_;
  const uint32_t string_size = header_->string_size_;
  unsigned char *string_buf = nullptr;
  if (OB_ISNULL(string_buf = static_cast<unsigned char *>(
      decoder_ctx.allocator_->alloc(string_size * vector_ctx.row_cap_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory", K(ret), K(string_size), K(vector_ctx));
  } else {
    const sql::ObBitVector *null_bitmap = nullptr;
    int64_t data_offset = 0;
    if (decoder_ctx.has_extend_value()) {
      null_bitmap = sql::to_bit_vector(col_data);
      data_offset = decoder_ctx.micro_block_header_->row_count_ * decoder_ctx.micro_block_header_->extend_value_bit_;
      data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
    }

    for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
      const int64_t row_id = vector_ctx.row_ids_[i];
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      if (HAS_NULL && null_bitmap->contain(row_id)) {
        vector->set_null(curr_vec_offset);
      } else {
        const unsigned char *cell_data = col_data + data_offset + row_id * header_->length_;
        unsigned char *str_ptr = string_buf + i * string_size;
        header_->copy_string(ObStringDiffHeader::LeftToRight(), std::logical_not<uint8_t>(),
            header_->common_data(), str_ptr);
        if (HEX_PACKED) {
          ObHexStringUnpacker unpacker(header_->hex_char_array(), cell_data);
          header_->copy_hex_string(unpacker,
              static_cast<void (ObHexStringUnpacker::*)(unsigned char &)>(&ObHexStringUnpacker::unpack),
              str_ptr);
        } else {
          header_->copy_string(
              ObStringDiffHeader::LeftToRight(),
              ObStringDiffHeader::LogicTrue<uint8_t>(),
              cell_data, str_ptr);
        }
        vector->set_payload_shallow(curr_vec_offset, str_ptr, string_size);
      }
    }
  }
  return ret;
}

template <typename VectorType, bool HAS_NULL, bool HEX_PACKED>
int ObStringDiffDecoder::decode_vector_from_var_len_data(
    const ObColumnDecoderCtx &decoder_ctx,
    const ObIRowIndex *row_index,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  VectorType *vector = static_cast<VectorType *>(vector_ctx.get_vector());
  const uint32_t string_size = header_->string_size_;
  unsigned char *string_buf = nullptr;
  if (OB_ISNULL(string_buf = static_cast<unsigned char *>(
      decoder_ctx.allocator_->alloc(string_size * vector_ctx.row_cap_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory", K(ret), K(string_size), K(vector_ctx));
  } else {
    for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
      const int64_t row_id = vector_ctx.row_ids_[i];
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      const unsigned char *cell_data = reinterpret_cast<const unsigned char *>(vector_ctx.ptr_arr_[i]);
      unsigned char *str_ptr = string_buf + i * string_size;
      if (HAS_NULL && nullptr == cell_data) {
        vector->set_null(curr_vec_offset);
      } else {
        header_->copy_string(ObStringDiffHeader::LeftToRight(), std::logical_not<uint8_t>(),
            header_->common_data(), str_ptr);
        if (HEX_PACKED) {
          ObHexStringUnpacker unpacker(header_->hex_char_array(), cell_data);
          header_->copy_hex_string(unpacker,
              static_cast<void (ObHexStringUnpacker::*)(unsigned char &)>(&ObHexStringUnpacker::unpack),
              str_ptr);
        } else {
          header_->copy_string(
              ObStringDiffHeader::LeftToRight(),
              ObStringDiffHeader::LogicTrue<uint8_t>(),
              cell_data, str_ptr);
        }
        vector->set_payload_shallow(curr_vec_offset, str_ptr, string_size);
      }
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
