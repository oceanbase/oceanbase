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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_chunk_datum_store_compact_header.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
namespace sql
{
static const uint8_t COMPACT_ROW_LEGACY_IMAGE = 1U << 0;
static const uint8_t COMPACT_ROW_FLAGS_MASK = COMPACT_ROW_LEGACY_IMAGE;

static inline uint32_t compact_fixed_len(const uint32_t *fixed_lens, const int64_t fixed_len_cnt, const int64_t col_idx)
{
  return OB_NOT_NULL(fixed_lens) && col_idx < fixed_len_cnt ? fixed_lens[col_idx] : 0;
}

static int64_t compact_row_size(const ObChunkDatumStore::StoredRow *sr, const uint32_t *fixed_lens,
                                const int64_t fixed_len_cnt, const int64_t col_cnt, const int64_t null_bitmap_bytes,
                                bool &legacy_image)
{
  const common::ObDatum *cells = sr->cells();
  legacy_image = sr->cnt_ != col_cnt;
  for (uint32_t i = 0; !legacy_image && i < sr->cnt_; ++i) {
    legacy_image = common::ObDatumDesc::NONE != cells[i].flag_;
  }
  int64_t size = 0;
  if (legacy_image) {
    size = 1 + sizeof(uint32_t) + sr->row_size_;
  } else {
    size = 1 + null_bitmap_bytes;
    for (uint32_t i = 0; i < sr->cnt_; ++i) {
      const uint32_t fixed_len = compact_fixed_len(fixed_lens, fixed_len_cnt, i);
      if (cells[i].null_) {
      } else if (0 != fixed_len) {
        if (cells[i].len_ != fixed_len) {
          legacy_image = true;
          size = 1 + sizeof(uint32_t) + sr->row_size_;
          break;
        }
        size += fixed_len;
      } else {
        size += sizeof(uint32_t) + cells[i].len_;
      }
    }
  }
  return size;
}

template <bool replay>
static int read_compact_i32(const char *buf, const int64_t data_len, int64_t &pos, int32_t &value)
{
  // Replay reads the same immutable input validated by the first parse.
  int64_t field_pos = 0;
  const int ret = serialization::decode_i32(buf + pos, replay ? sizeof(value) : data_len - pos, field_pos, &value);
  pos += field_pos;
  return ret;
}

template <bool rebuild>
static int parse_compact_row(const char *buf, const int64_t data_len, int64_t &pos, const int64_t col_cnt,
                             const int64_t null_bitmap_bytes, const uint32_t *fixed_lens, const int64_t fixed_len_cnt,
                             const uint32_t row_extend_size, char *row_dst, int64_t &rebuilt_size)
{
  int ret = OB_SUCCESS;
  rebuilt_size = 0;
  uint8_t row_flags = 0;
  if (!rebuild && (pos < 0 || pos >= data_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("compact row flags out of bounds", K(ret), K(pos), K(data_len));
  } else if (FALSE_IT(row_flags = static_cast<uint8_t>(buf[pos++]))) {
  } else if (!rebuild && OB_UNLIKELY(0 != (row_flags & ~COMPACT_ROW_FLAGS_MASK))) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid compact row flags", K(ret), K(row_flags));
  } else if (0 != (row_flags & COMPACT_ROW_LEGACY_IMAGE)) {
    int32_t row_size = 0;
    if (OB_FAIL(read_compact_i32<rebuild>(buf, data_len, pos, row_size))) {
      LOG_WARN("decode legacy-image row size failed", K(ret));
    } else if (!rebuild
               && (row_size < static_cast<int32_t>(sizeof(ObChunkDatumStore::StoredRow))
                   || data_len - pos < row_size)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("invalid legacy-image row size", K(ret), K(row_size), K(pos), K(data_len));
    } else {
      if constexpr (rebuild) {
        MEMCPY(row_dst, buf + pos, row_size);
        reinterpret_cast<ObChunkDatumStore::StoredRow *>(row_dst)->swizzling();
      } else {
        ObChunkDatumStore::StoredRow row_header;
        MEMCPY(&row_header, buf + pos, sizeof(row_header));
      }
      pos += row_size;
      rebuilt_size = row_size;
    }
  } else if (!rebuild && null_bitmap_bytes > data_len - pos) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("compact row null bitmap out of bounds", K(ret), K(pos), K(data_len), K(null_bitmap_bytes));
  } else {
    const ObTinyBitVector *null_bitmap = reinterpret_cast<const ObTinyBitVector *>(buf + pos);
    if (OB_SUCC(ret)) { pos += null_bitmap_bytes; }
    ObChunkDatumStore::StoredRow *sr = rebuild ? reinterpret_cast<ObChunkDatumStore::StoredRow *>(row_dst) : nullptr;
    common::ObDatum *cells = rebuild ? sr->cells() : nullptr;
    if (rebuild) { sr->cnt_ = static_cast<uint32_t>(col_cnt); }
    uint64_t data_off = sizeof(ObChunkDatumStore::StoredRow) + sizeof(common::ObDatum) * col_cnt + row_extend_size;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      if (null_bitmap->at(i)) {
        if (rebuild) {
          cells[i].set_null();
          cells[i].ptr_ = nullptr;
        }
      } else {
        uint32_t pack = 0;
        const uint32_t fixed_len = compact_fixed_len(fixed_lens, fixed_len_cnt, i);
        if (0 != fixed_len) {
          pack = fixed_len;
        } else {
          int32_t encoded_pack = 0;
          if (OB_FAIL(read_compact_i32<rebuild>(buf, data_len, pos, encoded_pack))) {
            LOG_WARN("decode compact row cell descriptor failed", K(ret), K(i));
          } else {
            pack = static_cast<uint32_t>(encoded_pack);
          }
        }
        common::ObDatumDesc desc;
        desc.pack_ = pack;
        if (OB_FAIL(ret)) {
        } else if (!rebuild && desc.len_ > data_len - pos) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("compact row cell out of bounds", K(ret), K(i), K(pos), K(data_len), K(desc.len_));
        } else if (!rebuild && desc.len_ > UINT32_MAX - data_off) {
          ret = OB_DESERIALIZE_ERROR;
          LOG_WARN("compact rebuilt row is too large", K(ret), K(i), K(data_off), K(desc.len_));
        } else {
          if (rebuild) {
            MEMCPY(row_dst + data_off, buf + pos, desc.len_);
            cells[i].pack_ = pack;
            cells[i].ptr_ = row_dst + data_off;
          }
          pos += desc.len_;
          data_off += desc.len_;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (rebuild) { sr->row_size_ = static_cast<uint32_t>(data_off); }
      rebuilt_size = data_off;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObChunkDatumStoreCompactHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compact datum store is not bound for encode", K(ret));
  } else if (!store_->inited_ || store_->enable_dump_ || 0 != store_->row_extend_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compact datum store requires an inited non-dump zero-extension store", K(ret), K(store_->inited_),
             K(store_->enable_dump_), K(store_->row_extend_size_));
  } else {
    const int64_t col_cnt = store_->col_count_;
    const int64_t null_bitmap_bytes = ObTinyBitVector::memory_size(col_cnt);
    if (OB_UNLIKELY(OB_ISNULL(buf) || pos < 0 || buf_len < pos)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("invalid compact datum-store buffer", K(ret), K(pos), K(buf_len));
    } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, static_cast<int32_t>(col_cnt)))) {
      LOG_WARN("encode compact datum-store column count failed", K(ret), K(col_cnt));
    } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, store_->blocks_.get_size()))) {
      LOG_WARN("encode compact datum-store block count failed", K(ret), "block_count", store_->blocks_.get_size());
    }
    ObChunkDatumStore::Block *block = store_->blocks_.get_first();
    while (OB_SUCC(ret) && OB_NOT_NULL(block)) {
      if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, static_cast<int32_t>(block->rows_)))) {
        LOG_WARN("encode compact datum-store row count failed", K(ret), K(block->rows_));
      }
      int64_t cur = 0;
      for (uint32_t r = 0; OB_SUCC(ret) && r < block->rows_; ++r) {
        const ObChunkDatumStore::StoredRow *sr =
          reinterpret_cast<const ObChunkDatumStore::StoredRow *>(block->payload_ + cur);
        cur += sr->row_size_;
        bool legacy_image = false;
        const int64_t row_size =
          compact_row_size(sr, fixed_lens_, fixed_len_cnt_, col_cnt, null_bitmap_bytes, legacy_image);
        if (OB_UNLIKELY(pos > buf_len || row_size > buf_len - pos)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("compact row does not fit encode buffer", K(ret), K(pos), K(buf_len), K(row_size));
          break;
        }
        buf[pos++] = legacy_image ? static_cast<char>(COMPACT_ROW_LEGACY_IMAGE) : 0;
        if (legacy_image) {
          if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, static_cast<int32_t>(sr->row_size_)))) {
            LOG_WARN("encode legacy-image row size failed", K(ret), K(sr->row_size_));
          } else {
            MEMCPY(buf + pos, sr, sr->row_size_);
            ObChunkDatumStore::StoredRow *image = reinterpret_cast<ObChunkDatumStore::StoredRow *>(buf + pos);
            image->unswizzling(const_cast<char *>(reinterpret_cast<const char *>(sr)));
            pos += sr->row_size_;
          }
        } else {
          ObTinyBitVector *null_bitmap = reinterpret_cast<ObTinyBitVector *>(buf + pos);
          null_bitmap->init(col_cnt);
          pos += null_bitmap_bytes;
          const common::ObDatum *cells = sr->cells();
          for (uint32_t i = 0; OB_SUCC(ret) && i < sr->cnt_; ++i) {
            const uint32_t fixed_len = compact_fixed_len(fixed_lens_, fixed_len_cnt_, i);
            if (cells[i].null_) {
              null_bitmap->set(i);
            } else if (0 != fixed_len) {
              MEMCPY(buf + pos, cells[i].ptr_, fixed_len);
              pos += fixed_len;
            } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, static_cast<int32_t>(cells[i].pack_)))) {
              LOG_WARN("encode compact variable cell descriptor failed", K(ret), K(i));
            } else {
              MEMCPY(buf + pos, cells[i].ptr_, cells[i].len_);
              pos += cells[i].len_;
            }
          }
        }
      }
      block = block->get_next();
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObChunkDatumStoreCompactHeader)
{
  int ret = OB_SUCCESS;
  int64_t block_cnt = 0;
  block_descs_.reset();
  compact_buf_ = nullptr;
  compact_buf_len_ = 0;
  if (OB_ISNULL(store_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compact datum store is not bound for decode", K(ret));
  } else if (!store_->is_inited() || store_->enable_dump_ || 0 != store_->row_extend_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compact datum-store decode target is invalid", K(ret), K(store_->inited_), K(store_->enable_dump_),
             K(store_->row_extend_size_));
  } else if (OB_ISNULL(buf) || pos < 0 || data_len < pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compact datum-store decode buffer", K(ret), KP(buf), K(pos), K(data_len));
  } else {
    if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &decoded_column_cnt_))) {
      LOG_WARN("decode compact datum-store column count failed", K(ret));
    } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &block_cnt))) {
      LOG_WARN("decode compact datum-store block count failed", K(ret));
    }
    if (OB_SUCC(ret)
        && OB_UNLIKELY(block_cnt < 0 || block_cnt > data_len - pos
                       || (block_cnt > 0
                           && (decoded_column_cnt_ < 0
                               || decoded_column_cnt_ > (UINT32_MAX - sizeof(ObChunkDatumStore::StoredRow))
                                                          / sizeof(common::ObDatum))))) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("invalid compact datum-store header", K(ret), K(decoded_column_cnt_), K(block_cnt), K(pos), K(data_len));
    }
    const int64_t null_bitmap_bytes = decoded_column_cnt_ > 0 ? ObTinyBitVector::memory_size(decoded_column_cnt_) : 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_cnt; ++i) {
      BlockDesc desc;
      if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &desc.row_cnt_))) {
        LOG_WARN("decode compact datum-store row count failed", K(ret), K(i));
      } else if (OB_UNLIKELY(desc.row_cnt_ < 0 || desc.row_cnt_ > data_len - pos)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("invalid compact datum-store row count", K(ret), K(i), K(desc.row_cnt_), K(pos), K(data_len));
      }
      desc.compact_block_start_ = pos;
      for (int32_t r = 0; OB_SUCC(ret) && r < desc.row_cnt_; ++r) {
        int64_t row_rebuilt_size = 0;
        if (OB_FAIL(parse_compact_row<false>(buf, data_len, pos, decoded_column_cnt_, null_bitmap_bytes, fixed_lens_,
                                             fixed_len_cnt_, store_->row_extend_size_, nullptr, row_rebuilt_size))) {
          LOG_WARN("parse compact datum-store row failed", K(ret), K(i), K(r));
        }
        // Each row fits uint32_t and row_cnt_ fits positive int32_t, so the sum fits int64_t.
        if (OB_SUCC(ret)) { desc.rebuilt_size_ += row_rebuilt_size; }
      }
      desc.compact_block_end_ = pos;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(block_descs_.push_back(desc))) {
        LOG_WARN("save compact datum-store block descriptor failed", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(pos != data_len)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("compact datum-store body length mismatch", K(ret), K(pos), K(data_len));
    }
    if (OB_SUCC(ret)) {
      compact_buf_ = buf;
      compact_buf_len_ = data_len;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObChunkDatumStoreCompactHeader)
{
  int64_t len = 0;
  if (OB_NOT_NULL(store_)) {
    const int64_t col_cnt = store_->col_count_;
    const int64_t null_bitmap_bytes = ObTinyBitVector::memory_size(col_cnt);
    len += serialization::encoded_length_vi32(static_cast<int32_t>(col_cnt));
    len += serialization::encoded_length_vi64(store_->blocks_.get_size());
    ObChunkDatumStore::Block *block = store_->blocks_.get_first();
    while (OB_NOT_NULL(block)) {
      len += serialization::encoded_length_vi32(static_cast<int32_t>(block->rows_));
      int64_t cur = 0;
      for (uint32_t r = 0; r < block->rows_; ++r) {
        const ObChunkDatumStore::StoredRow *sr =
          reinterpret_cast<const ObChunkDatumStore::StoredRow *>(block->payload_ + cur);
        bool legacy_image = false;
        len += compact_row_size(sr, fixed_lens_, fixed_len_cnt_, col_cnt, null_bitmap_bytes, legacy_image);
        cur += sr->row_size_;
      }
      block = block->get_next();
    }
  }
  return len;
}

int ObChunkDatumStoreCompactHeader::replay_to(ObChunkDatumStore &dst, ObChunkDatumStoreReplayCtx &) const
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObChunkDatumStore::Block *, 4> staged_blocks;
  const int64_t old_max_blk_size = dst.max_blk_size_;
  const int64_t old_min_blk_size = dst.min_blk_size_;
  if (&dst != store_ || OB_ISNULL(compact_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compact datum-store replay binding mismatch", K(ret), KP(store_), KP(&dst));
  } else if (!dst.blocks_.is_empty() || 0 != dst.row_cnt_ || 0 != dst.n_blocks_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("compact datum-store replay target is not empty", K(ret), K(dst.row_cnt_), K(dst.n_blocks_));
  } else if (-1 != dst.col_count_ && dst.col_count_ != decoded_column_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compact datum-store replay column count mismatch", K(ret), K(dst.col_count_), K(decoded_column_cnt_));
  }
  const int64_t null_bitmap_bytes = decoded_column_cnt_ > 0 ? ObTinyBitVector::memory_size(decoded_column_cnt_) : 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < block_descs_.count(); ++i) {
    const BlockDesc &desc = block_descs_.at(i);
    ObChunkDatumStore::Block *block = nullptr;
    if (OB_FAIL(dst.alloc_block_buffer(block, ObChunkDatumStore::Block::min_buf_size(desc.rebuilt_size_),
                                       false /*for_iterator*/))) {
      LOG_WARN("allocate compact datum-store replay block failed", K(ret), K(i));
    }
    if (OB_SUCC(ret)) {
      char *row_dst = block->payload_;
      ObChunkDatumStore::StoredRow *last_row = nullptr;
      int64_t compact_pos = desc.compact_block_start_;
      for (int32_t r = 0; OB_SUCC(ret) && r < desc.row_cnt_; ++r) {
        int64_t row_rebuilt_size = 0;
        last_row = reinterpret_cast<ObChunkDatumStore::StoredRow *>(row_dst);
        if (OB_FAIL(parse_compact_row<true>(compact_buf_, desc.compact_block_end_, compact_pos, decoded_column_cnt_,
                                            null_bitmap_bytes, fixed_lens_, fixed_len_cnt_, dst.row_extend_size_,
                                            row_dst, row_rebuilt_size))) {
          LOG_WARN("rebuild compact datum-store row failed", K(ret), K(i), K(r));
        }
        row_dst += row_rebuilt_size;
      }
      block->rows_ = desc.row_cnt_;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(block->get_buffer()->advance(desc.rebuilt_size_))) {
        LOG_WARN("advance compact datum-store replay block failed", K(ret), K(i));
      } else if (OB_FAIL(staged_blocks.push_back(block))) {
        LOG_WARN("stage compact datum-store replay block failed", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(block)) { dst.free_blk_mem(block, block->get_buffer()->mem_size()); }
  }
  if (OB_SUCC(ret)) {
    dst.col_count_ = decoded_column_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < staged_blocks.count(); ++i) {
      if (OB_FAIL(dst.add_block(staged_blocks.at(i), false /*need_swizzling*/))) {
        LOG_WARN("add compact datum-store replay block failed", K(ret), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < staged_blocks.count(); ++i) {
      ObChunkDatumStore::Block *block = staged_blocks.at(i);
      dst.free_blk_mem(block, block->get_buffer()->mem_size());
    }
    dst.max_blk_size_ = old_max_blk_size;
    dst.min_blk_size_ = old_min_blk_size;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
