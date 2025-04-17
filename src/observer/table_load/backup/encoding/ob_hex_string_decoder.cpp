/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/backup/ob_table_load_backup_block_sstable_struct.h"
#include "ob_hex_string_decoder.h"
#include "ob_bit_stream.h"
#include "ob_integer_array.h"
#include "ob_raw_decoder.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
const ObColumnHeader::Type ObHexStringDecoder::type_;

ObHexStringDecoder::ObHexStringDecoder() :  header_(NULL)
{
}

ObHexStringDecoder::~ObHexStringDecoder()
{
}

int ObHexStringDecoder::decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len) const
{
  UNUSED(row_id);
  int ret = OB_SUCCESS;
  uint64_t val = STORED_NOT_EXT;
  const char *col_data = reinterpret_cast<const char *>(header_) + ctx.col_header_->length_;
  int64_t data_offset = 0;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == data || len < 0) {
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
          LOG_WARN("get extend value failed", K(ret), K(ctx));
        }
      } else {
        if (OB_FAIL(bs.get(ctx.col_header_->extend_value_index_,
                           ctx.micro_block_header_->extend_value_bit_,
                           val))) {
          LOG_WARN("get extend value failed",
              K(ret), K(bs), K(ctx));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (STORED_NOT_EXT != val) {
    set_stored_ext_value(cell, static_cast<ObStoredExtValue>(val));
  } else {
    if (cell.get_meta() != ctx.obj_meta_) {
      cell.set_meta_type(ctx.obj_meta_);
    }
    const char *cell_data = NULL;
    int64_t cell_len = 0;
    if (ctx.is_fix_length()) {
      cell_data = col_data + data_offset + row_id * header_->length_;
      cell_len = header_->length_;
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
      const int64_t buf_size = std::max(header_->max_string_size_, min_buf_size);
      if (OB_ISNULL(buf = static_cast<char *>(ctx.allocator_->alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(buf_size));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t str_len = header_->max_string_size_;
      if (!ctx.is_fix_length()) {
        const ObVarHexCellHeader *cell_header
            = reinterpret_cast<const ObVarHexCellHeader *>(cell_data);
        cell_data += sizeof(*cell_header);
        cell_len -= sizeof(*cell_header);
        str_len = cell_len * 2 - cell_header->odd_;
      }
      ObHexStringUnpacker unpacker(header_->hex_char_array_,
          reinterpret_cast<const unsigned char *>(cell_data));
      unpacker.unpack(reinterpret_cast<unsigned char *>(buf), str_len);
      cell.val_len_ = static_cast<int32_t>(str_len);
      cell.v_.string_ = buf;
    }
  }
  return ret;
}

int ObHexStringDecoder::update_pointer(const char *old_block, const char *cur_block)
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

/**
 * Internal call, not check parameters for performance
 *
 * Possible future optimizations:
 *  1. Reuse HexString Unpacker
 *  2. SIMD loop parallelization
 *  3. Loop fission maybe?
 */
int ObHexStringDecoder::batch_decode(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int64_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  UNUSED(cell_datas);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    const static uint16_t min_buf_size = 128;
    const int64_t buf_size = std::max(header_->max_string_size_, min_buf_size);
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(ctx.allocator_->alloc(buf_size * row_cap)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory", K(ret), K(buf_size), K(row_cap));
    } else {
      const unsigned char *col_data = reinterpret_cast<const unsigned char *>(header_)
          + ctx.col_header_->length_;
      const unsigned char *hex_char_map = header_->hex_char_array_;
      const int64_t max_string_size = header_->max_string_size_;
      int64_t fix_data_offset = 0;
      // Set null values
      if (OB_SUCC(ret) && ctx.has_extend_value()) {
        if (ctx.is_fix_length()) {
          fix_data_offset = ctx.micro_block_header_->row_count_
                                * ctx.micro_block_header_->extend_value_bit_;
          fix_data_offset = (fix_data_offset + CHAR_BIT - 1) / CHAR_BIT;
          if (OB_FAIL(set_null_datums_from_fixed_column(
              ctx, row_ids, row_cap, col_data, datums))) {
            LOG_WARN("Failed to set null datums from fixed data", K(ret), K(ctx));
          }
        } else if (OB_FAIL(set_null_datums_from_var_column(
            ctx, row_index, row_ids, row_cap, datums))) {
          LOG_WARN("Failed to set null datums from var data", K(ret), K(ctx));
        }
      }

      int64_t row_id = 0;
      if (OB_FAIL(ret)) {
      } else if (ctx.is_fix_length()) {
        for (int64_t i = 0; i < row_cap; ++i) {
          if (ctx.has_extend_value() && datums[i].is_null()) {
            // Skip
          } else {
            row_id = row_ids[i];
            const unsigned char *cell_data = reinterpret_cast<const unsigned char *>(
                  col_data + fix_data_offset + row_id * header_->length_);
            ObHexStringUnpacker unpacker(hex_char_map, cell_data);
            int64_t buf_offset = i * buf_size;
            unpacker.unpack(reinterpret_cast<unsigned char *>(buf + buf_offset), max_string_size);
            datums[i].pack_ = static_cast<int32_t>(max_string_size);
            datums[i].ptr_ = buf + buf_offset;
          }
        }
      } else {
        const ObVarHexCellHeader *cell_header = nullptr;
        uint32_t var_cell_len = 0;
        int64_t buf_offset = 0;
        int64_t unpack_size = 0;
        const char *cell_data = nullptr;
        const char *row_data = nullptr;
        int64_t row_len = 0;
        int64_t cell_len = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
          if (ctx.has_extend_value() && datums[i].is_null()) {
            // Skip
          } else {
            row_id = row_ids[i];
            if (OB_FAIL(locate_row_data(ctx, row_index, row_id, row_data, row_len))) {
              LOG_WARN("Failed to read row data from row index", K(ret), KP(row_index), K(row_id));
            } else if (OB_FAIL(ObRawDecoder::locate_cell_data(cell_data, cell_len,
                row_data, row_len, *ctx.micro_block_header_, *ctx.col_header_, *header_))) {
              LOG_WARN("Failed to locate cell data",
                  K(ret), K(row_len), KP(row_data), K(i), K(ctx));
            } else {
              cell_header = reinterpret_cast<const ObVarHexCellHeader *>(cell_data);
              cell_data += sizeof(*cell_header);
              var_cell_len = static_cast<uint32_t>(cell_len) - sizeof(*cell_header);
              datums[i].pack_ = var_cell_len * 2 - cell_header->odd_;

              ObHexStringUnpacker unpacker(hex_char_map,
                  reinterpret_cast<const unsigned char *>(cell_data));
              buf_offset = i * buf_size;
              unpack_size = datums[i].len_;
              unpacker.unpack(reinterpret_cast<unsigned char *>(buf + buf_offset), unpack_size);
              datums[i].ptr_ = buf + buf_offset;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHexStringDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const char* meta_data,
    const ObIRowIndex* row_index,
    ObBitmap &result_bitmap) const
{
  // No enough meta data to improve comparison operation, so only implement is null, not null operator here
  // Other pushdown operators will retrograde to row-wise decode and compare
  UNUSEDx(parent, meta_data);
  int ret = OB_SUCCESS;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  const char *col_data = reinterpret_cast<const char *>(header_) + col_ctx.col_header_->length_;
  const unsigned char *col_u_data = reinterpret_cast<const unsigned char*>(col_data);
  if (OB_UNLIKELY(op_type >= sql::WHITE_OP_MAX)
      || OB_ISNULL(row_index)
      || OB_ISNULL(col_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid op type for pushed dow white filter", K(ret), K(op_type));
  } else if (col_ctx.is_fix_length() || col_ctx.is_bit_packing()) {
    if (OB_FAIL(get_is_null_bitmap_from_fixed_column(col_ctx, col_u_data, result_bitmap))) {
      LOG_WARN("Failed to get isnull bitmap from fixed column", K(ret));
    }
  } else {
    if (OB_FAIL(get_is_null_bitmap_from_var_column(col_ctx, row_index, result_bitmap))) {
      LOG_WARN("Failed to get isnull bitmap from variable column", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    switch (op_type) {
      case sql::WHITE_OP_NU: {
        break;
      }
      case sql::WHITE_OP_NN: {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to flip bits for result bitmap", K(ret), K(result_bitmap.size()));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
      }
    }
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
