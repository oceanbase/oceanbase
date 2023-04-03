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

#include "ob_string_prefix_decoder.h"

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_bit_stream.h"
#include "ob_integer_array.h"
#include "ob_raw_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
const ObColumnHeader::Type ObStringPrefixDecoder::type_;

ObStringPrefixDecoder::~ObStringPrefixDecoder()
{
}

int ObStringPrefixDecoder::decode(ObColumnDecoderCtx &ctx, common::ObObj &cell,
    const int64_t row_id, const ObBitStream &bs, const char *data, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == data || len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(len));
  } else {
    UNUSED(row_id);
    uint64_t val = STORED_NOT_EXT;
    // read extend value bit
    if (ctx.has_extend_value()) {
      if (OB_FAIL(bs.get(ctx.col_header_->extend_value_index_,
          ctx.micro_block_header_->extend_value_bit_, val))) {
        LOG_WARN("get extend value failed",
            K(ret), K(bs), K(ctx));
      }
    }
    if (OB_SUCC(ret)) {
      if (STORED_NOT_EXT != val) {
        set_stored_ext_value(cell, static_cast<ObStoredExtValue>(val));
      } else {
        if (cell.get_meta() != ctx.obj_meta_) {
          cell.set_meta_type(ctx.obj_meta_);
        }
        const char *cell_data = NULL;
        int64_t cell_len = 0;
        if (OB_FAIL(ObRawDecoder::locate_cell_data(cell_data, cell_len,
                data, len, *ctx.micro_block_header_, *ctx.col_header_, *meta_header_))) {
          LOG_WARN("locate cell data failed", K(ret), K(len),
              K(ctx), "header", *meta_header_);
        } else {
          // get prefix
          const ObStringPrefixCellHeader *cell_header =
              reinterpret_cast<const ObStringPrefixCellHeader *>(cell_data);
          ObIntegerArrayGenerator meta_gen;
          const char *var_data = meta_data_
            + (meta_header_->count_ - 1) * meta_header_->prefix_index_byte_;
          const char *prefix_str = NULL;
          if (OB_FAIL(meta_gen.init(meta_data_, meta_header_->prefix_index_byte_))) {
            LOG_WARN("failed to init integer array generator", K(ret),
                KP_(meta_data), "prefix index byte", meta_header_->prefix_index_byte_);
          } else {
            int64_t offset = 0;
            if (0 != cell_header->get_ref()) {
              offset = meta_gen.get_array().at(cell_header->get_ref() - 1);
            }
            prefix_str = var_data + offset;
          }

          char *buf = NULL;
          if (OB_SUCC(ret)) {
            const static uint32_t min_buf_size = 128;
            const int64_t buf_size = std::max(meta_header_->max_string_size_, min_buf_size);
            if (OB_ISNULL(buf = static_cast<char *>(ctx.allocator_->alloc(buf_size)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory", K(ret), K(buf_size));
            }
          }

          // fill data
          if (OB_SUCC(ret)) {
            char *string = buf;
            cell_data += sizeof(ObStringPrefixCellHeader);
            cell_len -= sizeof(ObStringPrefixCellHeader);
            MEMCPY(string, prefix_str, cell_header->len_);
            if (meta_header_->is_hex_packing()) {
              int64_t str_len = cell_len * 2 - cell_header->get_odd();
              ObHexStringUnpacker unpacker(meta_header_->hex_char_array_,
                  reinterpret_cast<const unsigned char *>(cell_data));
              for (int64_t i = cell_header->len_; i < str_len + cell_header->len_; ++i) {
                string[i] = static_cast<char>(unpacker.unpack());
              }
              cell.val_len_ = static_cast<int32_t>(cell_header->len_ + str_len);
              cell.v_.string_ = string;
              //LOG_DEBUG("debug: fill hex data", K(cell_header->len_), K(cell_len), K(str_len));
            } else {
              MEMCPY(string + cell_header->len_, cell_data, cell_len);
              cell.val_len_ = static_cast<int32_t>(cell_header->len_ + cell_len);
              cell.v_.string_ = string;
              //LOG_DEBUG("debug: fill data", K(cell_header->len_), K(cell_len));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObStringPrefixDecoder::update_pointer(const char *old_block, const char *cur_block)
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
    ObIColumnDecoder::update_pointer(meta_data_, old_block, cur_block);
  }
  return ret;
}

int ObStringPrefixDecoder::batch_decode(
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
    LOG_WARN("Not inited", K(ret));
  } else {
    const char *row_data = nullptr;
    int64_t row_len = 0;
    if (ctx.has_extend_value()) {
      if (OB_FAIL(set_null_datums_from_var_column(
          ctx, row_index, row_ids, row_cap, datums))) {
        LOG_WARN("Failed to set null datums from var data", K(ret), K(ctx));
      }
    }

    if (OB_SUCC(ret)) {
      ObIntegerArrayGenerator meta_gen;
      char *buf = nullptr;
      const static uint32_t min_buf_size = 128;
      const int64_t buf_size = std::max(meta_header_->max_string_size_, min_buf_size);
      if (OB_ISNULL(buf = static_cast<char *>(ctx.allocator_->alloc(buf_size * row_cap)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory", K(ret), K(buf_size));
      } else if (OB_FAIL(meta_gen.init(meta_data_, meta_header_->prefix_index_byte_))) {
        LOG_WARN("Failed to init integer array generator", K(ret), KP_(meta_data),
            "Prefix index byte", meta_header_->prefix_index_byte_);
      } else {
        const ObStringPrefixCellHeader *cell_header = nullptr;
        const char *var_data = meta_data_
            + (meta_header_->count_ - 1) * meta_header_->prefix_index_byte_;
        const char *prefix_str = nullptr;
        char *string = nullptr;
        int64_t row_id = 0;
        const char *cell_data = nullptr;
        int64_t cell_len = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
          row_id = row_ids[i];
          string = buf + i * buf_size;
          if (ctx.has_extend_value() && datums[i].is_null()) {
            // Do nothing
          } else if (OB_FAIL(locate_row_data(ctx, row_index, row_id, row_data, row_len))) {
            LOG_WARN("Failed to locate row data", K(ret), K(row_id));
          } else if (OB_FAIL(ObRawDecoder::locate_cell_data(cell_data, cell_len, row_data, row_len,
              *ctx.micro_block_header_, *ctx.col_header_, *meta_header_))) {
            LOG_WARN("Failed to locate cell data", K(ret), K(row_id), K(ctx));
          } else {
            cell_header = reinterpret_cast<const ObStringPrefixCellHeader *>(cell_data);
            int64_t offset = 0;
            if (0 != cell_header->get_ref()) {
              offset = meta_gen.get_array().at(cell_header->get_ref() - 1);
            }
            prefix_str = var_data + offset;
            cell_data += sizeof(ObStringPrefixCellHeader);
            cell_len -= sizeof(ObStringPrefixCellHeader);
            MEMCPY(string, prefix_str, cell_header->len_);
            if (meta_header_->is_hex_packing()) {
              int64_t str_len = cell_len * 2 - cell_header->get_odd();
              ObHexStringUnpacker unpacker(meta_header_->hex_char_array_,
                  reinterpret_cast<const unsigned char *>(cell_data));
              for (int64_t j = cell_header->len_; j < str_len + cell_header->len_; ++j) {
                string[j] = static_cast<char>(unpacker.unpack());
              }
              datums[i].pack_ = static_cast<int32_t>(cell_header->len_ + str_len);
              datums[i].ptr_ = string;
            } else {
              MEMCPY(string + cell_header->len_, cell_data, cell_len);
              datums[i].pack_ = static_cast<uint32_t>(cell_header->len_ + cell_len);
              datums[i].ptr_ = string;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObStringPrefixDecoder::get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("StringPrefix decoder is not inited", K(ret));
  } else if OB_FAIL(ObIColumnDecoder::get_null_count_from_extend_value(
      ctx,
      row_index,
      row_ids,
      row_cap,
      meta_data_,
      null_count)) {
    LOG_WARN("Failed to get null count", K(ctx), K(ret));
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
