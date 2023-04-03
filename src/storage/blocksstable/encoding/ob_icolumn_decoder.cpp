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

#include "ob_icolumn_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObIColumnDecoder::get_is_null_bitmap_from_fixed_column(
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    ObBitmap &result_bitmap) const
{
  // 定长列从column meta中直接读 bit packing 的 is_null_bitmap
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_ctx.micro_block_header_->row_count_ != result_bitmap.size())
          || OB_ISNULL(col_data)
          || OB_UNLIKELY(!col_ctx.is_fix_length()
              && !col_ctx.is_bit_packing())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for getting isnull bitmap from fixed column", K(ret));
  } else {
    if (col_ctx.has_extend_value()) {
      int64_t row_count = col_ctx.micro_block_header_->row_count_;
      int64_t bm_block_count = (row_count % ObBitmap::BITS_PER_BLOCK == 0)
                                ? row_count / ObBitmap::BITS_PER_BLOCK
                                : row_count / ObBitmap::BITS_PER_BLOCK + 1;
      int64_t bm_block_bits = ObBitmap::BITS_PER_BLOCK;
      uint64_t read_buf[bm_block_count];
      MEMSET(read_buf, 0, sizeof(uint64_t) * bm_block_count);
      for (int64_t i = 0; OB_SUCC(ret) && i < bm_block_count - 1; ++i) {
        if (OB_FAIL(ObBitStream::get(col_data, i * bm_block_bits, bm_block_bits, read_buf[i]))) {
          LOG_WARN("Get extended value from column meta failed", K(ret), K(col_ctx));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObBitStream::get(
                    col_data,
                    (bm_block_count - 1) * bm_block_bits,
                    bm_block_bits,
                    read_buf[bm_block_count - 1]))) {
        LOG_WARN("Get extended value from column meta failed", K(ret), K(col_ctx));
      } else {
        if (OB_FAIL(result_bitmap.load_blocks_from_array(read_buf, row_count))) {
          LOG_WARN("Failed to load result_bitmap from fix_size column null bitmap", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObIColumnDecoder::get_is_null_bitmap_from_var_column(
    const ObColumnDecoderCtx &col_ctx,
    const ObIRowIndex* row_index,
    ObBitmap &result_bitmap) const
{
  // 变长列需要遍历对应row区并更新result bitmap
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_ctx.micro_block_header_->row_count_ != result_bitmap.size())
          || OB_ISNULL(row_index)
          || OB_UNLIKELY(col_ctx.is_fix_length()
              || col_ctx.is_bit_packing())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for getting isnull bitmap from var column", K(ret));
  } else {
    if (col_ctx.has_extend_value()) {
      uint64_t value;
      const char* row_data = nullptr;
      int64_t row_len = 0;
      for (int64_t row_id = 0;
          OB_SUCC(ret) && row_id < col_ctx.micro_block_header_->row_count_;
          ++row_id) {
        if (OB_FAIL(locate_row_data(col_ctx, row_index, row_id, row_data, row_len))) {
          LOG_WARN("Failed to get row_data offset from row index", K(ret));
        } else if (OB_FAIL(ObBitStream::get(
                              reinterpret_cast<const unsigned char*>(row_data),
                              col_ctx.col_header_->extend_value_index_,
                              col_ctx.micro_block_header_->extend_value_bit_,
                              value))) {
          LOG_WARN("Get extended value from row data failed", K(ret), K(col_ctx));
        } else if (value & static_cast<uint64_t>(1)) {
          if (OB_FAIL(result_bitmap.set(row_id))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(row_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObIColumnDecoder::set_null_datums_from_fixed_column(
    const ObColumnDecoderCtx &ctx,
    const int64_t *row_ids,
    const int64_t row_cap,
    const unsigned char *col_data,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  int64_t row_id = 0;
  uint64_t val = STORED_NOT_EXT;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_id = row_ids[i];
    if (OB_FAIL(ObBitStream::get(col_data, row_id * ctx.micro_block_header_->extend_value_bit_,
        ctx.micro_block_header_->extend_value_bit_, val))) {
      LOG_WARN("Get extend value failed", K(ret), K(ctx));
    } else {
      datums[i].null_ = STORED_NOT_EXT != val;
      datums[i].len_ = 0;
    }
  }
  return ret;
}

int ObIColumnDecoder::set_null_datums_from_var_column(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  int64_t row_id = 0;
  uint64_t val = STORED_NOT_EXT;
  const char *row_data = nullptr;
  int64_t row_len = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_id = row_ids[i];
    if (OB_FAIL(locate_row_data(ctx, row_index, row_id, row_data, row_len))) {
      LOG_WARN("Failed to get row_data offset from row index", K(ret), K(row_id));
    } else if (OB_FAIL(ObBitStream::get(
        reinterpret_cast<const unsigned char *>(row_data),
        ctx.col_header_->extend_value_index_,
        ctx.micro_block_header_->extend_value_bit_,
        val))) {
      LOG_WARN("Failed to get extend value from row data", K(ret), K(ctx));
    } else {
      datums[i].null_ = STORED_NOT_EXT != val;
      datums[i].len_ = 0;
    }
  }
  return ret;
}

int ObIColumnDecoder::get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  // Default implementation, decode the column data and check whether is null
  int ret = OB_SUCCESS;
  null_count = 0;
  common::ObObj cell;
  const char *row_data = NULL;
  int64_t row_len = 0;
  int64_t row_id = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_id = row_ids[i];
    if (OB_FAIL(row_index->get(row_id, row_data, row_len))) {
      LOG_WARN("get row data failed", K(ret), K(row_id));
    }
    ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(decode(const_cast<ObColumnDecoderCtx &>(ctx), cell, row_id, bs, row_data, row_len))) {
      LOG_WARN("failed to decode cell", K(ret));
    } else if (cell.is_null()) {
      null_count++;
    }
  }
  return ret;
}

int ObIColumnDecoder::get_null_count_from_fixed_column(
    const ObColumnDecoderCtx &ctx,
    const int64_t *row_ids,
    const int64_t row_cap,
    const unsigned char *col_data,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  int64_t row_id = 0;
  uint64_t val = STORED_NOT_EXT;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_id = row_ids[i];
    if (OB_FAIL(ObBitStream::get(col_data, row_id * ctx.micro_block_header_->extend_value_bit_,
        ctx.micro_block_header_->extend_value_bit_, val))) {
      LOG_WARN("Get extend value failed", K(ret), K(ctx));
    } else if (STORED_NOT_EXT != val) {
      null_count++;
    }
  }
  return ret;
}

int ObIColumnDecoder::get_null_count_from_var_column(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  int64_t row_id = 0;
  uint64_t val = STORED_NOT_EXT;
  const char *row_data = nullptr;
  int64_t row_len = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_id = row_ids[i];
    if (OB_FAIL(locate_row_data(ctx, row_index, row_id, row_data, row_len))) {
      LOG_WARN("Failed to get row_data offset from row index", K(ret), K(row_id));
    } else if (OB_FAIL(ObBitStream::get(
        reinterpret_cast<const unsigned char *>(row_data),
        ctx.col_header_->extend_value_index_,
        ctx.micro_block_header_->extend_value_bit_,
        val))) {
      LOG_WARN("Failed to get extend value from row data", K(ret), K(ctx));
    } else if (STORED_NOT_EXT != val) {
      null_count++;
    }
  }
  return ret;
}

int ObIColumnDecoder::get_null_count_from_extend_value(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    const char *meta_data_,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  null_count = 0;
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(meta_data_);
  if (ctx.has_extend_value()) {
    if (ctx.is_fix_length() || ctx.is_bit_packing()) {
      if (OB_FAIL(get_null_count_from_fixed_column(
          ctx, row_ids, row_cap, col_data, null_count))) {
        LOG_WARN("Failed to get null count from fixed data", K(ret), K(ctx));
      }
    } else if (OB_FAIL(get_null_count_from_var_column(
        ctx, row_index, row_ids, row_cap, null_count))) {
      LOG_WARN("Failed to get null count from var data", K(ret), K(ctx));
    }
  } else {
    null_count = 0;
  }
  return ret;
}

} // end of namespace oceanbase
} // end of namespace oceanbase
