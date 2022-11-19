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

#ifndef OCEANBASE_ENCODING_OB_BIT_STREAM_H_
#define OCEANBASE_ENCODING_OB_BIT_STREAM_H_

#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include <limits.h>

namespace oceanbase
{
namespace blocksstable
{

// Store and get integer which less than 64-bit.
// NOTE: To simplify implement, we assume data been set to zero and only set once.
// TODO bin.lb: optimize signal bit access && inline?
class ObBitStream
{
public:
  typedef unsigned char BS_WORD;
  const static int64_t BS_WORD_BIT = CHAR_BIT;

  ObBitStream() : data_(NULL), bit_len_(0) {}
  ObBitStream(unsigned char *data, const int64_t len) : data_(data), bit_len_(len * BS_WORD_BIT)
  {
  }

  OB_INLINE int init(unsigned char *data, const int64_t length);
  OB_INLINE int set(const int64_t offset, const int64_t cnt, const int64_t value);
  OB_INLINE int set(const int64_t offset, const int64_t cnt, const uint64_t value)
  {
    return set(offset, cnt, static_cast<int64_t>(value));
  }
  OB_INLINE int get(const int64_t offset, const int64_t cnt, int64_t &value) const;
  OB_INLINE int get(const int64_t offset, const int64_t cnt, uint64_t &value) const
  {
    return get(offset, cnt, *reinterpret_cast<int64_t *>(&value));
  }

  enum ObBitStreamUnpackType
  {
    PACKED_LEN_LESS_THAN_10 = 0,
    PACKED_LEN_LESS_THAN_26,
    DEFAULT,
  };

  template <ObBitStreamUnpackType type>
  OB_INLINE static int get(
      const unsigned char *buf,
      const int64_t offset,
      const int64_t cnt,
      const int64_t bs_len,
      int64_t &value);

  OB_INLINE static int get(const unsigned char *buf, const int64_t offset, const int64_t cnt, int64_t &value);

  OB_INLINE static int get(const unsigned char *buf, const int64_t offset, const int64_t cnt, uint64_t &value)
  {
    return get(buf, offset, cnt, *reinterpret_cast<int64_t *>(&value));
  }

  bool is_init() const { return NULL != data_; }

  const static BS_WORD bit_mask_table_[BS_WORD_BIT + 1];

  // performance critical, do not check parameters.
  // %word_off must LESS than CHAR_BIT
  // high bits of %v must been cleared which no need to store.
  OB_INLINE static void memory_safe_set(unsigned char *buf, const int64_t word_off,
      const bool overflow, const uint64_t v)
  {
    *reinterpret_cast<uint64_t *>(buf) |= (v << word_off);
    if (overflow) {
      *(buf + sizeof(v)) |= static_cast<BS_WORD>(
          bit_mask_table_[word_off] & (v >> (sizeof(v) * BS_WORD_BIT - word_off)));
    }
  }

  // performance critical, do not check parameters.
  OB_INLINE static void memory_safe_set(unsigned char *buf,
      const int64_t pos, const int64_t len, const uint64_t v)
  {
    const int64_t off = pos / CHAR_BIT;
    const int64_t word_off = pos % CHAR_BIT;
    const bool overflow = (word_off + len) > 64;
    memory_safe_set(buf + off, word_off, overflow, v);
  }

  // performance critical, do not check parameters.
  // caller MUST be sure there will no overflow
  OB_INLINE static void memory_safe_set(unsigned char *buf,
      const int64_t pos, const uint64_t v)
  {
    const int64_t off = pos / CHAR_BIT;
    const int64_t word_off = pos % CHAR_BIT;
    memory_safe_set(buf + off, word_off, false, v);
  }

  OB_INLINE static uint64_t get_mask(const int64_t len);

  TO_STRING_KV(KP_(data), K_(bit_len));

  void update_pointer(int64_t offset)
  {
    if (NULL != data_) {
      data_ += offset;
    }
  }
private:
  BS_WORD *data_;
  int64_t bit_len_;

  DISALLOW_COPY_AND_ASSIGN(ObBitStream);
};

OB_INLINE int ObBitStream::init(unsigned char *data, const int64_t length)
{
  int ret = common::OB_SUCCESS;
  // can init twice and length may equal to zero
  if (NULL == data || length < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(data), K(length));
  } else {
    data_ = data;
    bit_len_ = length * BS_WORD_BIT;
  }

  return ret;
}

OB_INLINE int ObBitStream::set(const int64_t offset, const int64_t cnt, const int64_t value)
{
  int ret = common::OB_SUCCESS;
  if (!is_init()) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (offset < 0 || cnt <= 0 || cnt >= sizeof(value) * BS_WORD_BIT
      || offset + cnt > bit_len_) {
    ret = common::OB_INDEX_OUT_OF_RANGE;
    STORAGE_LOG(WARN, "index out of range", K(ret), K(offset), K(cnt), K_(bit_len));
  } else {
    uint64_t v = static_cast<uint64_t>(value);
    int64_t index = offset;
    int64_t done = 0;
    while (done < cnt) {
      const int64_t word_off = index % BS_WORD_BIT;
      const int64_t  bit = std::min(cnt - done, BS_WORD_BIT - word_off);
      const BS_WORD wv = static_cast<BS_WORD>((bit_mask_table_[bit] & (v >> done)) << word_off);
      data_[index / BS_WORD_BIT] |= wv;
      done += bit;
      index += bit;
    }
  }
  return ret;
}

// performance critical, do not check parameters.
OB_INLINE int ObBitStream::get(const unsigned char *buf,
    const int64_t offset, const int64_t cnt, int64_t &value)
{
  int ret = common::OB_SUCCESS;
  int64_t index = offset;
  int64_t done = 0;
  uint64_t v = 0;
  while (done < cnt) {
    const int64_t word_off = index % BS_WORD_BIT;
    const int64_t  bit = std::min(cnt - done, BS_WORD_BIT - word_off);
    uint64_t wv = static_cast<BS_WORD>(
        bit_mask_table_[bit] & (buf[index / BS_WORD_BIT] >> word_off));
    v |= (wv << done);
    done += bit;
    index += bit;
  }
  value = static_cast<int64_t>(v);

  return ret;
}

OB_INLINE int ObBitStream::get(const int64_t offset, const int64_t cnt, int64_t &value) const
{
  int ret = common::OB_SUCCESS;
  if (!is_init()) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (offset < 0 || cnt <= 0 || cnt >= sizeof(value) * BS_WORD_BIT
      || offset + cnt > bit_len_) {
    ret = common::OB_INDEX_OUT_OF_RANGE;
    STORAGE_LOG(WARN, "index out of range", K(ret), K(offset), K(cnt), K_(bit_len));
  } else {
    ret = get(data_, offset, cnt, value);
  }

  return ret;
}

template <>
OB_INLINE int ObBitStream::get<ObBitStream::PACKED_LEN_LESS_THAN_10>(
    const unsigned char *buf,
    const int64_t offset,
    const int64_t cnt,
    const int64_t bs_len,
    int64_t &value)
{
  int ret = common::OB_SUCCESS;
  const int64_t byte_offset = offset >> 3; // offset / (sizeof(uint8_t) * CHAR_BIT)
  const int64_t bit_offset = offset & 7; // offset % (sizeof(uint8_t) * CHAR_BIT);
  if (OB_UNLIKELY(offset >= bs_len - 8 && bit_offset + cnt <= 8)) {
    // ret = get(buf, offset, cnt, value);
    const uint8_t mask = (1 << cnt) - 1;
    uint8_t v = *reinterpret_cast<const uint8_t *>(buf + byte_offset);
    v = v >> bit_offset;
    value = static_cast<int64_t >(v & mask);
  } else {
    const uint16_t mask = (1 << cnt) - 1;
    uint16_t v = *reinterpret_cast<const uint16_t *>(buf + byte_offset);
    v = v >> bit_offset;
    value = static_cast<int64_t>(v & mask);
  }
  return ret;
}

template <>
OB_INLINE int ObBitStream::get<ObBitStream::PACKED_LEN_LESS_THAN_26>(
    const unsigned char *buf,
    const int64_t offset,
    const int64_t cnt,
    const int64_t bs_len,
    int64_t &value)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(offset >= bs_len - 24)) {
    ret = get(buf, offset, cnt, value);
  } else {
    const int64_t byte_offset = offset >> 3; // offset / (sizeof(uint8_t) * CHAR_BIT)
    const int64_t bit_offset = offset & 7; // offset % (sizeof(uint8_t) * CHAR_BIT);
    const uint32_t mask = (1 << cnt) - 1;
    uint32_t v = *reinterpret_cast<const uint32_t *>(buf + byte_offset);
    v = v >> bit_offset;
    value = static_cast<int64_t>(v & mask);
  }
  return ret;
}

template <>
OB_INLINE int ObBitStream::get<ObBitStream::DEFAULT>(
    const unsigned char *buf,
    const int64_t offset,
    const int64_t cnt,
    const int64_t bs_len,
    int64_t &value)
{
  UNUSED(bs_len);
  return get(buf, offset, cnt, value);
}

OB_INLINE uint64_t ObBitStream::get_mask(const int64_t len)
{
  constexpr int64_t TABLE_SIZE = 65;
  using MaskTable = uint64_t[TABLE_SIZE];
  RLOCAL_INLINE(MaskTable, mask_table_64bit);
  uint64_t mask = 0;
  if (OB_UNLIKELY(0 == mask_table_64bit[1])) {
    for (int64_t i = 0; i < TABLE_SIZE; ++i) {
      for (int64_t j = 1; j <= i; ++j) {
        mask_table_64bit[i] |= (1UL << (j - 1));
      }
    }
  }
  if (OB_LIKELY(len > 0) && OB_LIKELY(len <= 64)) {
    mask = mask_table_64bit[len];
  }
  return mask;
}

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_BIT_STREAM_H_
