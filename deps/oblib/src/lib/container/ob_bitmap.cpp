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

#include "lib/container/ob_bitmap.h"
#include "common/ob_target_specific.h"

#if OB_USE_MULTITARGET_CODE
#include <emmintrin.h>
#include <immintrin.h>
#endif

namespace oceanbase
{
namespace common
{

OB_DECLARE_AVX512_SPECIFIC_CODE(
// Transform 64-byte mask to 64-bit mask
inline static uint64_t bytes64mask_to_bits64mask(
    const uint8_t *bytes64,
    const bool need_flip = false)
{
  const __m512i vbytes = _mm512_loadu_si512(reinterpret_cast<const void *>(bytes64));
  uint64_t res = _mm512_testn_epi8_mask(vbytes, vbytes);
  if (!need_flip) {
    res = ~res;
  }
  return res;
}

inline static void bitmap_get_condensed_index(
    const uint8_t *data,
    const int32_t size,
    int32_t *row_ids,
    int32_t &row_count)
{
  int32_t offset = 0;
  const uint8_t *pos = data;
  const uint8_t *end_pos = data + size;
  const uint8_t *end_pos64 = pos + size / 64 * 64;
  row_count = 0;
  for (; pos < end_pos64; pos += 64) {
    uint64_t mask64 = bytes64mask_to_bits64mask(pos);
    __m512i start_index = _mm512_set1_epi32(offset);
    __m512i base_index = _mm512_setr_epi32(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);
    base_index = _mm512_add_epi32(base_index, start_index);
    uint16_t mask16 = mask64 & 0xFFFF;
    _mm512_mask_compressstoreu_epi32(row_ids + row_count, mask16, base_index);
    row_count += popcount64(mask16);
    const __m512i constant16 = _mm512_set1_epi32(16);
    base_index = _mm512_add_epi32(base_index, constant16);
    mask16 = (mask64 >> 16) & 0xFFFF;
    _mm512_mask_compressstoreu_epi32(row_ids + row_count, mask16, base_index);
    row_count += popcount64(mask16);
    base_index = _mm512_add_epi32(base_index, constant16);
    mask16 = (mask64 >> 32) & 0xFFFF;
    _mm512_mask_compressstoreu_epi32(row_ids + row_count, mask16, base_index);
    row_count += popcount64(mask16);
    base_index = _mm512_add_epi32(base_index, constant16);
    mask16 = mask64 >> 48;
    _mm512_mask_compressstoreu_epi32(row_ids + row_count, mask16, base_index);
    row_count += popcount64(mask16);

    offset += 64;
  }
  while (pos < end_pos) {
    if (*pos) {
      row_ids[row_count++] = pos - data;
    }
    ++pos;
  }
}

inline static void uint64_mask_to_bits_mask(
    const uint64_t *data,
    const int64_t size,
    uint8_t *skip)
{
  const uint64_t *pos = data;
  const uint64_t *end_pos = data + size;
  const uint64_t *end_pos64 = data + size / 8 * 8;
  uint64_t i = 0;
  const __m512i zero64 = _mm512_setzero_si512();

  while (pos < end_pos64) {
    __m512i v = _mm512_loadu_si512(pos);
    skip[i++] |= _mm512_cmp_epi64_mask(v, zero64, 0);
    pos += 8;
  }

  uint64_t *skip64 = reinterpret_cast<uint64_t *>(skip);

  while (pos < end_pos) {
    if (*pos == 0) {
      i = pos - data;
      skip64[i / 64] |= 1LU << (i % 64);
    }
    ++pos;
  }
}
)

OB_DECLARE_AVX2_SPECIFIC_CODE(
inline static uint64_t bytes64mask_to_bits64mask(
    const uint8_t *bytes64,
    const bool need_flip = false)
{
  const __m256i zero32 = _mm256_setzero_si256();
  uint64_t res =
      (static_cast<uint64_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(
                      _mm256_loadu_si256(reinterpret_cast<const __m256i *>(bytes64)), zero32))) & 0xffffffff)
      | (static_cast<uint64_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(
                      _mm256_loadu_si256(reinterpret_cast<const __m256i *>(bytes64 + 32)), zero32))) << 32);
  if (!need_flip) {
    res = ~res;
  }
  return res;
}
)

OB_DECLARE_SSE42_SPECIFIC_CODE(
inline static uint64_t bytes64mask_to_bits64mask(
    const uint8_t *bytes64,
    const bool need_flip = false)
{
  const __m128i zero16 = _mm_setzero_si128();
  uint64_t res =
      (static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                      _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64)), zero16))) & 0xffff)
      | ((static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                          _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 16)), zero16))) << 16) & 0xffff0000)
      | ((static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                          _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 32)), zero16))) << 32) & 0xffff00000000)
      | ((static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                          _mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 48)), zero16))) << 48) & 0xffff000000000000);

  if (!need_flip) {
    res = ~res;
  }
  return res;
}
)

OB_DECLARE_DEFAULT_CODE(
inline static uint64_t bytes64mask_to_bits64mask(
    const uint8_t *bytes64,
    const bool need_flip = false)
{
  uint64_t res = 0;
  for (int64_t i = 0; i < 64; ++i) {
    res |= static_cast<uint64_t>(0 == bytes64[i]) << i;
  }
  if (!need_flip) {
    res = ~res;
  }
  return res;
}
)

OB_INLINE static uint8_t is_bit_set(
    const uint8_t *src_byte,
    const uint32_t offset)
{
  const uint32_t index = offset / CHAR_BIT;
  const uint32_t bit_offset = offset % CHAR_BIT;
  return (src_byte[index] & static_cast<uint8_t>(1 << bit_offset)) != 0 ? 1 : 0;
}

OB_DECLARE_DEFAULT_AND_AVX2_CODE(
inline static uint64_t bitmap_popcnt(
    const uint8_t *data,
    const int64_t valid_bytes)
{
  uint64_t count = 0;
  const uint8_t *pos = data;
  const uint8_t *end_pos = pos + valid_bytes;
  const uint8_t *end_pos64 = pos + valid_bytes / 64 * 64;
  for (; pos < end_pos64; pos += 64) {
    uint64_t mask = bytes64mask_to_bits64mask(pos);
    count += popcount64(mask);
  }
  for (; pos < end_pos; ++pos) {
    count += (*pos != 0);
  }
  return count;
}

inline static bool bitmap_is_all_true(
    const int64_t start,
    const int64_t end,
    const uint8_t *data)
{
  // Based on the assumption that 'is_all_true' in pieces of
  // consecutive values is not a small probability event.
  // Therefore, we will check once every 64 bytes.
  bool is_all_true = true;
  const int64_t end_offset = end + 1;
  const int64_t length = end_offset - start;
  const uint8_t *pos = data + start;
  const uint8_t *end_pos = data + end_offset;
  const uint8_t *end_pos64 = pos + length / 64 * 64;
  for (; is_all_true && pos < end_pos64; pos += 64) {
    uint64_t mask = bytes64mask_to_bits64mask(pos);
    if (0xFFFFFFFFFFFFFFFFUL != mask) {
      is_all_true = false;
    }
  }
  for (; is_all_true && pos < end_pos; ++pos) {
    is_all_true = *pos;
  }
  return is_all_true;
}

inline static bool bitmap_is_all_false(
    const int64_t start,
    const int64_t end,
    const uint8_t *data)
{
  // Based on the assumption that 'is_all_false' in pieces of
  // consecutive values is not a small probability event.
  // Therefore, we will check once every 64 bytes.
  bool is_all_false = true;
  const int64_t end_offset = end + 1;
  const int64_t length = end_offset - start;
  const uint8_t *pos = data + start;
  const uint8_t *end_pos = data + end_offset;
  const uint8_t *end_pos64 = pos + length / 64 * 64;
  for (; is_all_false && pos < end_pos64; pos += 64) {
    uint64_t mask = bytes64mask_to_bits64mask(pos);
    if (0 != mask) {
      is_all_false = false;
    }
  }
  for (; is_all_false && pos < end_pos; ++pos) {
    is_all_false = (*pos == 0);
  }
  return is_all_false;
}


inline static void bitmap_next_valid_idx(
    const int64_t start,
    const int64_t count,
    const bool is_reverse,
    const uint8_t *data,
    int64_t &offset)
{
  if (!is_reverse) {
    const uint8_t *pos = data + start;
    const uint8_t *end_pos = pos + count;
    const uint8_t *end_pos64 = pos + count / 64 * 64;
    for (; pos < end_pos64; pos += 64) {
      uint64_t mask = bytes64mask_to_bits64mask(pos);
      if (0 != mask) {
        uint64_t index = countr_zero64(mask);
        offset = pos - data + index;
        break;
      }
    }
    if (-1 == offset) {
      for (; pos < end_pos; ++pos) {
        if (*pos != 0) {
          offset = pos - data;
          break;
        }
      }
    }
  } else {
    const uint8_t *pos = data + start + count;
    const uint8_t *end_pos = data + start - 1;
    const uint8_t *end_pos64 = pos - count / 64 * 64;
    for (; pos > end_pos64; pos -= 64) {
      uint64_t mask = bytes64mask_to_bits64mask(pos - 64);
      if (0 != mask) {
        uint64_t index = countl_zero64(mask);
        offset = pos - data - index - 1;
        break;
      }
    }
    --pos;
    if (-1 == offset) {
      for (; pos > end_pos; --pos) {
        if (*pos != 0) {
          offset = pos - data;
          break;
        }
      }
    }
  }
}

inline static void bitmap_get_row_ids(
    int32_t *row_ids,
    int64_t &row_count,
    int64_t &from,
    const int64_t to,
    const int64_t limit,
    const int64_t id_offset,
    const uint8_t *data)
{
  const uint8_t *pos = data + from;
  const uint8_t *end_pos = data + to;
  const uint8_t *end_pos64 = pos + (to - from) / 64 * 64;
  row_count = 0;
  for (; pos < end_pos64 && row_count < limit; pos += 64) {
    uint64_t mask = bytes64mask_to_bits64mask(pos);
    while (mask && row_count < limit) {
      uint64_t index = countr_zero64(mask);
      mask = blsr64(mask);
      row_ids[row_count++] = pos - data + index - id_offset;
    }
  }
  while (row_count < limit && pos < end_pos) {
    if (*pos) {
      row_ids[row_count++] = pos - data - id_offset;
    }
    ++pos;
  }
  if (row_count >= limit) {
    from = row_ids[limit - 1] + id_offset + 1;
    row_count = limit;
  } else {
    from = to;
  }
}

inline static void bitmap_get_condensed_index(
    const uint8_t *data,
    const int32_t size,
    int32_t *row_ids,
    int32_t &row_count)
{
  const uint8_t *pos = data;
  const uint8_t *end_pos = data + size;
  const uint8_t *end_pos64 = pos + size / 64 * 64;
  row_count = 0;
  for (; pos < end_pos64; pos += 64) {
    uint64_t mask = bytes64mask_to_bits64mask(pos);
    while (mask) {
      uint64_t index = countr_zero64(mask);
      mask = blsr64(mask);
      row_ids[row_count++] = pos - data + index;
    }
  }
  while (pos < end_pos) {
    if (*pos) {
      row_ids[row_count++] = pos - data;
    }
    ++pos;
  }
}

inline static void bitmap_to_bits_mask(
    const int64_t from,
    const int64_t to,
    const bool need_flip,
    const uint8_t *data,
    uint8_t* bits)
{
  const uint8_t *pos = data + from;
  const uint8_t *end_pos = data + to;
  const uint8_t *end_pos64 = pos + (to - from) / 64 * 64;
  for (; pos < end_pos64; pos += 64) {
    uint64_t *mask = reinterpret_cast<uint64_t *>(bits);
    *mask = bytes64mask_to_bits64mask(pos, need_flip);
    bits += sizeof(uint64_t);
  }
  if (pos < end_pos) {
    const uint8_t *tmp_pos = pos;
    uint64_t *bits64 = reinterpret_cast<uint64_t *>(bits);
    bits64[0] = 0;
    if (need_flip) {
      while (pos < end_pos) {
        const uint64_t idx = pos - tmp_pos;
        bits64[0] |= (static_cast<uint64_t>(0 == *(pos++)) << (idx % 64));
      }
    } else {
      while (pos < end_pos) {
        const uint64_t idx = pos - tmp_pos;
        bits64[0] |= (static_cast<uint64_t>(0 != *(pos++)) << (idx % 64));
      }
    }
  }
}

inline static void uint64_mask_to_bits_mask(
    const uint64_t *data,
    const int64_t size,
    uint8_t *skip)
{
  const uint64_t *pos = data;
  const uint64_t *end_pos = data + size;
  uint64_t i = 0;
  uint64_t *skip64 = reinterpret_cast<uint64_t *>(skip);

  while (pos < end_pos) {
    if (*pos == 0) {
      i = pos - data;
      skip64[i / 64] |= 1LU << (i % 64);
    }
    ++pos;
  }
}
)

class SelectAndOp {
public:
  OB_INLINE static uint8_t apply(uint8_t a, uint8_t b) { return a & b; }
};

class SelectOrOp {
public:
  OB_INLINE static uint8_t apply(uint8_t a, uint8_t b) { return a | b; }
};

class SelectNotOp {
public:
  OB_INLINE static uint8_t apply(uint8_t a) { return a == 0; }
};

template <typename Op>
struct SelectOpImpl
{
  OB_MULTITARGET_FUNCTION_AVX2_SSE42(
  OB_MULTITARGET_FUNCTION_HEADER(static void), apply_op, OB_MULTITARGET_FUNCTION_BODY((
      uint8_t *a,
      const uint8_t *b,
      const uint64_t size)
  {
    // GCC vectorizes a loop only if it is written in this form.
    // In this case, if we loop through the array index (the code will look simpler),
    // the loop will not be vectorized.
    uint8_t* __restrict a_pos = a;
    const uint8_t* __restrict b_pos = b;
    const uint8_t* __restrict b_end = b_pos + size;
    while (b_pos < b_end) {
      *a_pos = Op::apply(*a_pos, *b_pos);
      ++a_pos;
      ++b_pos;
    }
  }))

  OB_MULTITARGET_FUNCTION_AVX2_SSE42(
  OB_MULTITARGET_FUNCTION_HEADER(static void), apply_not_op, OB_MULTITARGET_FUNCTION_BODY((
      uint8_t *a,
      const uint64_t size)
  {
    uint8_t* __restrict a_pos = a;
    const uint8_t* __restrict a_end = a_pos + size;
    while (a_pos < a_end) {
      *a_pos = Op::apply(*a_pos);
      ++a_pos;
    }
  }))
};

ObBitmap::ObBitmap(ObIAllocator &allocator)
  : is_inited_(false), valid_bytes_(0), capacity_(0), data_(nullptr),
    condensed_cnt_(-1), condensed_idx_(nullptr), allocator_(allocator)
{}

ObBitmap::~ObBitmap()
{
  destroy();
}

uint64_t ObBitmap::popcnt() const
{
  uint64_t ret = 0;
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX2)) {
    ret = common::specific::avx2::bitmap_popcnt(data_, valid_bytes_);
  } else {
    ret = common::specific::normal::bitmap_popcnt(data_, valid_bytes_);
  }
#else
  ret = common::specific::normal::bitmap_popcnt(data_, valid_bytes_);
#endif
  return ret;
}

bool ObBitmap::is_all_true(const int64_t start, const int64_t end) const
{
  bool ret = false;
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX2)) {
    ret = common::specific::avx2::bitmap_is_all_true(start, end, data_);
  } else {
    ret = common::specific::normal::bitmap_is_all_true(start, end, data_);
  }
#else
  ret = common::specific::normal::bitmap_is_all_true(start, end, data_);
#endif
  return ret;
}

bool ObBitmap::is_all_false(const int64_t start, const int64_t end) const
{
  bool ret = false;
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX2)) {
    ret = common::specific::avx2::bitmap_is_all_false(start, end, data_);
  } else {
    ret = common::specific::normal::bitmap_is_all_false(start, end, data_);
  }
#else
  ret = common::specific::normal::bitmap_is_all_false(start, end, data_);
#endif
  return ret;
}

int64_t ObBitmap::next_valid_idx(const int64_t start,
                                 const int64_t count,
                                 const bool is_reverse,
                                 int64_t &offset) const
{
  int ret = OB_SUCCESS;
  offset = -1;
  if (OB_UNLIKELY(start + count > valid_bytes_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument", K(ret), K(start), K(count), K_(valid_bytes));
#if OB_USE_MULTITARGET_CODE
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    common::specific::avx2::bitmap_next_valid_idx(start, count, is_reverse, data_, offset);
#endif
  } else {
    common::specific::normal::bitmap_next_valid_idx(start, count, is_reverse, data_, offset);
  }
  return ret;
}

int ObBitmap::get_row_ids(
    int32_t *row_ids,
    int64_t &row_count,
    int64_t &from,
    const int64_t to,
    const int64_t limit,
    const int64_t id_offset) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(from < 0 || to > valid_bytes_ || to < from || limit <= 0 || from < id_offset)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid from or to when get row ids", K(ret), K(from), K(to), K_(valid_bytes),
            K(limit), K(id_offset));
#if OB_USE_MULTITARGET_CODE
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    common::specific::avx2::bitmap_get_row_ids(row_ids, row_count, from, to, limit, id_offset, data_);
#endif
  } else {
    common::specific::normal::bitmap_get_row_ids(row_ids, row_count, from, to, limit, id_offset, data_);
  }
  return ret;
}

int ObBitmap::bit_and(const ObBitmap &right)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(right.size() != valid_bytes_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Bitmaps for bytewise AND have different valid bytes length", K(ret), K(right.size()), K_(valid_bytes));
#if OB_USE_MULTITARGET_CODE
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    SelectOpImpl<SelectAndOp>::apply_op_avx2(data_, right.data_, valid_bytes_);
  } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
    SelectOpImpl<SelectAndOp>::apply_op_sse42(data_, right.data_, valid_bytes_);
#endif
  } else {
    SelectOpImpl<SelectAndOp>::apply_op(data_, right.data_, valid_bytes_);
  }
  return ret;
}

int ObBitmap::bit_or(const ObBitmap &right)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ObBitmap is not inited", K(ret));
  } else if (OB_UNLIKELY(right.size() != valid_bytes_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Bitmaps for bytewise OR have different valid bytes length", K(ret), K(right.size()), K_(valid_bytes));
#if OB_USE_MULTITARGET_CODE
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    SelectOpImpl<SelectOrOp>::apply_op_avx2(data_, right.data_, valid_bytes_);
  } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
    SelectOpImpl<SelectOrOp>::apply_op_sse42(data_, right.data_, valid_bytes_);
#endif
  } else {
    SelectOpImpl<SelectOrOp>::apply_op(data_, right.data_, valid_bytes_);
  }
  return ret;
}

int ObBitmap::bit_not()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ObBitmap is not inited", K(ret));
#if OB_USE_MULTITARGET_CODE
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    SelectOpImpl<SelectNotOp>::apply_not_op_avx2(data_, valid_bytes_);
  } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
    SelectOpImpl<SelectNotOp>::apply_not_op_sse42(data_, valid_bytes_);
#endif
  } else {
    SelectOpImpl<SelectNotOp>::apply_not_op(data_, valid_bytes_);
  }
  return ret;
}

int ObBitmap::load_blocks_from_array(size_type *block_data, size_type num_bytes)
{
  // It is better not to use 'load_blocks_from_array' in this implementation of bitmap.
  int ret = OB_SUCCESS;
  if (OB_ISNULL(block_data)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Trying to load data to Bitmap from null array", K(ret), K(block_data), K(num_bytes));
  } else if (OB_FAIL(reserve(num_bytes))) {
    LIB_LOG(WARN, "Failed to reserve bitmap with num_bytes", K(ret), K(num_bytes));
  } else {
    const uint64_t *src = reinterpret_cast<uint64_t*>(block_data);
    uint8_t *pos = data_;
    const uint8_t *end_pos = pos + num_bytes;
    const uint8_t *end_pos64 = pos + num_bytes / 64 * 64;
    for (; pos < end_pos64; pos += 64, ++src) {
      uint64_t mask = *src;
      if (0xFFFFFFFFFFFFFFFFUL == mask) {
        MEMSET(static_cast<void*>(pos), 1, 64);
      } else if (0 == mask) {
        MEMSET(static_cast<void*>(pos), 0, 64);
      } else {
        while (mask) {
          size_type index = countr_zero64(mask);
          pos[index] = 1;
          mask = blsr64(mask);
        }
      }
    }
    const uint8_t *src_byte = reinterpret_cast<const uint8_t *>(src);
    uint32_t remained = end_pos - pos;
    while (remained > 0) {
      uint32_t offset = end_pos - pos - remained;
      pos[offset] = is_bit_set(src_byte, offset);
      --remained;
    }
    valid_bytes_ = num_bytes;
  }
  return ret;
}

int ObBitmap::append_bitmap(
    const ObBitmap &bitmap,
    const uint32_t offset,
    const bool is_reverse)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(valid_bytes_ < (bitmap.size() + offset))) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "Current bitmap is not big enough", K(ret), K(offset),
             K_(valid_bytes), K(bitmap.size()));
  } else if (is_reverse) {
    uint32_t cur_offset = size() - offset - bitmap.size();
    for (int64_t i = bitmap.size() - 1; OB_SUCC(ret) && i >= 0; i--) {
      set_(cur_offset++, bitmap.test(i));
    }
  } else {
    uint8_t *dest = data_ + offset;
    const uint8_t *src = bitmap.data_;
    MEMCPY(static_cast<void*>(dest), static_cast<const void*>(src), bitmap.size());
  }
  return ret;
}

int ObBitmap::reserve(size_type capacity)
{
  int ret = OB_SUCCESS;
  if (capacity > capacity_) {
    const int64_t block_size = DEFAULT_BLOCK_SIZE;
    int64_t new_size = (capacity - 1) / block_size * block_size + block_size;
    uint8_t *new_data = static_cast<uint8_t *>(allocator_.alloc(new_size));
    if (OB_ISNULL(new_data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "Failed to alloc memory for bitmap", K(ret), K(new_size));
    } else {
      destroy();
      capacity_ = new_size;
      data_ = new_data;
    }
  }
  if (OB_SUCC(ret)) {
    valid_bytes_ = capacity;
    condensed_cnt_ = -1;
  }
  return ret;
}

int ObBitmap::init(const size_type valid_bytes, const bool is_all_true)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "ObBitmap init twice", K(ret));
  } else if (OB_FAIL(reserve(valid_bytes))) {
    LIB_LOG(WARN, "Failed to reserver", K(ret), K(valid_bytes));
  } else {
    if (is_all_true) {
      MEMSET(static_cast<void*>(data_), 1, valid_bytes);
    } else {
      MEMSET(static_cast<void*>(data_), 0, valid_bytes);
    }
    valid_bytes_ = valid_bytes;
    is_inited_ = true;
  }
  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

int ObBitmap::copy_from(const ObBitmap &bitmap, const int64_t start, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(valid_bytes_ < count || start + count > bitmap.size())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "Unexpected copy info", K(ret), K_(valid_bytes), K(start), K(count), K(bitmap.size()));
  } else {
    MEMCPY(static_cast<void*>(data_), static_cast<const void*>(bitmap.get_data() + start), count);
  }
  return ret;
}

void ObBitmap::reuse(const bool is_all_true)
{
  if (is_all_true) {
    MEMSET(static_cast<void*>(data_), 1, valid_bytes_);
  } else {
    MEMSET(static_cast<void*>(data_), 0, valid_bytes_);
  }
  condensed_cnt_ = -1;
}

int ObBitmap::set_bitmap_batch(const int64_t offset, const int64_t count, const bool value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count < 0 || valid_bytes_ < (offset + count))) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument", K(ret), K_(valid_bytes), K(offset), K(count));
  } else if (value) {
    MEMSET(static_cast<void*>(data_ + offset), 1, count);
  } else {
    MEMSET(static_cast<void*>(data_ + offset), 0, count);
  }
  return ret;
}

OB_DECLARE_AVX2_SPECIFIC_CODE(
inline static void inner_from_bits_mask(
    const int64_t from,
    const int64_t to,
    uint8_t* bits,
    uint8_t* data)
{
  const uint64_t size = to - from;
  const uint8_t *pos = bits;
  const uint8_t *end_pos32 = pos + size / 32 * 4;
  uint8_t *out = data + from;
  for (; pos < end_pos32; pos += 4) {
    // we only use the low 32bits of each lane, but this is fine with AVX2
    __m256i xbcast = _mm256_set1_epi32(*(reinterpret_cast<const int32_t *>(pos)));
    // Each byte gets the source byte containing the corresponding bit
    __m256i shufmask = _mm256_set_epi64x(
        0x0303030303030303, 0x0202020202020202,
        0x0101010101010101, 0x0000000000000000);
    __m256i shuf  = _mm256_shuffle_epi8(xbcast, shufmask);
    __m256i andmask  = _mm256_set1_epi64x(0x8040201008040201);  // every 8 bits -> 8 bytes, pattern repeats.
    __m256i isolated_inverted = _mm256_andnot_si256(shuf, andmask);
    // this is the extra step: compare each byte == 0 to produce 0 or -1
    __m256i z = _mm256_cmpeq_epi8(isolated_inverted, _mm256_setzero_si256());
    // alternative: compare against the AND mask to get 0 or -1,
    // avoiding the need for a vector zero constant.
    _mm256_storeu_si256((__m256i*)out,z);

    out += 32;
  }
  const int64_t remain_size = (to - from) % 32;
  for (int64_t idx = 0; idx < remain_size; ++idx) {
    *(out++) = is_bit_set(pos, idx);
  }
}
)

OB_DECLARE_DEFAULT_CODE(
inline static void inner_from_bits_mask(
    const int64_t from,
    const int64_t to,
    uint8_t* bits,
    uint8_t* data)
{
  const uint64_t size = to - from;
  uint8_t *out = data + from;
  uint64_t *bits64 = reinterpret_cast<uint64_t *>(bits);
  for (uint64_t i = 0; i < size; ++i) {
    if (bits64[i / 64] & (1LU << (i % 64))) {
      *out = 1;
    }
    ++out;
  }
}
)

int ObBitmap::from_bits_mask(
    const int64_t from,
    const int64_t to,
    uint8_t* bits)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(valid_bytes_ < to || nullptr == bits)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument", K_(valid_bytes), K(to), KP(bits));
#if OB_USE_MULTITARGET_CODE
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    common::specific::avx2::inner_from_bits_mask(from, to, bits, data_);
#endif
  } else {
    common::specific::normal::inner_from_bits_mask(from, to, bits, data_);
  }

  return ret;
}

int ObBitmap::to_bits_mask(
    const int64_t from,
    const int64_t to,
    const bool need_flip,
    uint8_t* bits) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(valid_bytes_ < to || nullptr == bits)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument", K_(valid_bytes), K(to), KP(bits));
#if OB_USE_MULTITARGET_CODE
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    common::specific::avx2::bitmap_to_bits_mask(from, to, need_flip, data_, bits);
#endif
  } else {
    common::specific::normal::bitmap_to_bits_mask(from, to, need_flip, data_, bits);
  }
  return ret;
}

void ObBitmap::filter(
    const bool has_null,
    const uint8_t *nulls,
    const uint64_t *data,
    const int64_t size,
    uint8_t *skip)
{
  if (!has_null) {
#if OB_USE_MULTITARGET_CODE
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    SelectOpImpl<SelectOrOp>::apply_op_avx2(skip, nulls, (size + 7) / 8);
  } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
    SelectOpImpl<SelectOrOp>::apply_op_sse42(skip, nulls, (size + 7) / 8);
#endif
  } else {
    SelectOpImpl<SelectOrOp>::apply_op(skip, nulls, (size + 7) / 8);
  }

#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX512)) {
    common::specific::avx512::uint64_mask_to_bits_mask(data, size, skip);
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    common::specific::avx2::uint64_mask_to_bits_mask(data, size, skip);
  } else {
#endif
    common::specific::normal::uint64_mask_to_bits_mask(data, size, skip);
#if OB_USE_MULTITARGET_CODE
  }
#endif
}

int ObBitmap::generate_condensed_index()
{
  int ret = OB_SUCCESS;
  if (nullptr == condensed_idx_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(int32_t) * capacity()))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "fail to alloc row_ids", K(ret), K(capacity()));
    } else {
      condensed_idx_ = reinterpret_cast<int32_t *>(buf);
    }
  }

  if (OB_FAIL(ret)) {
#if OB_USE_MULTITARGET_CODE
  // enable when avx512 is more efficient
  //} else if (common::is_arch_supported(ObTargetArch::AVX512)) {
  //  common::specific::avx512::bitmap_get_condensed_index(data_, valid_bytes_, condensed_idx_, condensed_cnt_);
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    common::specific::avx2::bitmap_get_condensed_index(data_, valid_bytes_, condensed_idx_, condensed_cnt_);
#endif
  } else {
    common::specific::normal::bitmap_get_condensed_index(data_, valid_bytes_, condensed_idx_, condensed_cnt_);
  }
  return ret;
}

} //end namespace oceanbase

} //end namespace common
