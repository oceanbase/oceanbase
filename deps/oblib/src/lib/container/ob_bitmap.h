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

#ifndef OCEANBASE_COMMON_OB_BITMAP_H_
#define OCEANBASE_COMMON_OB_BITMAP_H_

#include "lib/allocator/page_arena.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{

OB_INLINE int64_t popcount64(uint64_t v)
{
  int64_t cnt = 0;
#if __POPCNT__
  cnt = __builtin_popcountl(v);
#else
  if (0 != v) {
    v = v - ((v >> 1) & 0x5555555555555555UL);
    v = (v & 0x3333333333333333UL) + ((v >> 2) & 0x3333333333333333UL);
    cnt = (((v + (v >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >> 56;
  }
#endif
  return cnt;
}

OB_INLINE uint64_t blsr64(uint64_t mask)
{
#ifdef __BMI__
    return _blsr_u64(mask);
#else
    return mask & (mask-1);
#endif
}

// Returns the index of the least significant 1-bit of mask.
// mask cannot be zero.
OB_INLINE uint64_t countr_zero64(uint64_t mask)
{
  return __builtin_ffsll(mask) - 1;
}

// Count Leading Zeros from highest bit, mask cannot be zero.
OB_INLINE uint64_t countl_zero64(uint64_t mask)
{
  return __builtin_clzll(mask);
}

//TODO: use template to avoid branch prediction in simd
class ObBitmap
{
public:
  typedef uint64_t size_type;
  static const size_type DEFAULT_BLOCK_SIZE = 1024;
public:
  ObBitmap(ObIAllocator &allocator);
  virtual ~ObBitmap();
  DISALLOW_COPY_AND_ASSIGN(ObBitmap);

  OB_INLINE bool empty() const;
  OB_INLINE void destroy();
  OB_INLINE int set(const size_type pos, const bool value = true);
  OB_INLINE int wipe(const size_type pos);
  OB_INLINE int flip(const size_type pos);
  OB_INLINE bool test(const size_type pos) const;
  OB_INLINE bool operator[](const size_type pos) const;
  OB_INLINE bool is_all_false() const;
  OB_INLINE bool is_all_true() const;
  int bit_and(const ObBitmap &right);
  int bit_or(const ObBitmap &right);
  int bit_not();
  int load_blocks_from_array(size_type *block_data, size_type num_bytes);
  int append_bitmap(
      const ObBitmap &bitmap,
      const uint32_t offset,
      const bool is_reverse);
  uint64_t popcnt() const;
  // [start, end]
  bool is_all_false(const int64_t start, const int64_t end) const;
  bool is_all_true(const int64_t start, const int64_t end) const;
  // Next true offset in the bitmap, return -1 if not found.
  int64_t next_valid_idx(const int64_t start, const int64_t count, const bool is_reverse, int64_t &offset) const;

  int init(const size_type valid_bytes, const bool is_all_true = false);
  int reserve(size_type capacity);
  int copy_from(const ObBitmap &bitmap, const int64_t start, const int64_t count);
  void reuse(const bool is_all_true = false);
  int get_row_ids(
      int32_t *row_ids,
      int64_t &row_count,
      int64_t &from,
      const int64_t to,
      const int64_t limit,
      const int64_t id_offset = 0) const;
  int generate_condensed_index();
  OB_INLINE bool is_index_generated() { return -1 != condensed_cnt_; }
  OB_INLINE const int32_t *get_condensed_idx() { return condensed_idx_; }
  OB_INLINE int32_t get_condensed_cnt() { return condensed_cnt_; }

  OB_INLINE size_type capacity() const
  {
    return capacity_;
  }
  OB_INLINE size_type size() const
  {
    return valid_bytes_;
  }
  OB_INLINE bool is_inited() const
  {
    return is_inited_;
  }
  OB_INLINE uint8_t *get_data() const
  {
    return data_;
  }
  int set_bitmap_batch(const int64_t offset, const int64_t count, const bool value);
  int from_bits_mask(
      const int64_t from,
      const int64_t to,
      uint8_t* bits);
  int to_bits_mask(
      const int64_t from,
      const int64_t to,
      const bool need_flip,
      uint8_t* bits) const;
  static void filter(
      const bool has_null,
      const uint8_t *nulls,
      const uint64_t *data,
      const int64_t size,
      uint8_t *skip);

  TO_STRING_KV(K_(is_inited), K_(valid_bytes), K_(capacity), KP_(data));

private:
  OB_INLINE void set_(const size_type pos, const bool value = true);
  OB_INLINE int is_valid(const size_type pos)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(pos > valid_bytes_)) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LIB_LOG(WARN, "Pos out of valid bytes", K(ret), K(pos), K_(valid_bytes));
    }
    return ret;
  }

private:
  bool is_inited_;
  int32_t valid_bytes_;
  int32_t capacity_;
  // Make sure that data_[i] can only be equal to 0x00 or 0x01 when i is from 0 to (valid_bytes_ - 1).
  uint8_t *data_;
  int32_t condensed_cnt_;
  int32_t *condensed_idx_;
  ObIAllocator &allocator_;
};

OB_INLINE bool ObBitmap::is_all_true() const
{
  return is_all_true(0, valid_bytes_ - 1);
}

OB_INLINE bool ObBitmap::is_all_false() const
{
  return is_all_false(0, valid_bytes_ - 1);
}

OB_INLINE void ObBitmap::destroy()
{
  if (nullptr != data_) {
    allocator_.free(data_);
    data_ = nullptr;
  }
  if (nullptr != condensed_idx_) {
    allocator_.free(condensed_idx_);
    condensed_idx_ = nullptr;
  }
  valid_bytes_ = 0;
  capacity_ = 0;
  condensed_cnt_ = -1;
}

OB_INLINE bool ObBitmap::empty() const
{
  return 0 == valid_bytes_;
}

OB_INLINE int ObBitmap::set(const size_type pos, const bool value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(is_valid(pos))) {
    LIB_LOG(WARN, "Faild to check valid", K(ret), K(pos), K_(valid_bytes));
  } else {
    data_[pos] = value;
  }
  return ret;
}

OB_INLINE void ObBitmap::set_(const size_type pos, const bool value)
{
  data_[pos] = value;
}

OB_INLINE int ObBitmap::wipe(const size_type pos)
{
  return set(pos, false);
}

OB_INLINE int ObBitmap::flip(const size_type pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(is_valid(pos))) {
    LIB_LOG(WARN, "Faild to check valid", K(ret), K(pos), K_(valid_bytes));
  } else {
    data_[pos] = (data_[pos] == 0);
  }
  return ret;
}

OB_INLINE bool ObBitmap::test(const size_type pos) const
{
  return data_[pos];
}

OB_INLINE bool ObBitmap::operator[](const size_type pos) const
{
  return test(pos);
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_BITMAP_H_
