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

class ObBitmap
{
public:
  typedef uint64_t size_type;

  static const size_type BYTES_PER_BLOCK = sizeof(size_type);
  static const size_type BITS_PER_BLOCK = BYTES_PER_BLOCK * 8;
  static const size_type BLOCKS_PER_MEM_BLOCK = 64;
  static const size_type MEM_BLOCK_BYTES = BLOCKS_PER_MEM_BLOCK * BYTES_PER_BLOCK;
  static const size_type MEM_BLOCK_BITS = BLOCKS_PER_MEM_BLOCK * BITS_PER_BLOCK;
  static const size_type BLOCK_MOD_MASK = BITS_PER_BLOCK - 1;
  static const size_type BLOCK_MOD_BITS = 6;

public:
  ObBitmap(ObIAllocator &allocator);
  virtual ~ObBitmap();

  /**
   * check if bitmap is empty; not contains any data.
   */
  OB_INLINE bool empty() const;

  /**
   * free all hold memory in bitmap.
   * empty will return true;
   */
  OB_INLINE void destroy();

  /**
   * set bit at %pos if value true
   * or wipe out bit at %pos if value false, same as clear(pos)
   */
  OB_INLINE int set(const size_type pos, const bool value = true);

  /**
   * same as set(pos, false);
   */
  OB_INLINE int wipe(const size_type pos);

  /**
   * reset all bits in bitmap;
   */
  OB_INLINE void reuse(const bool is_all_true = false);

  /**
   * xor with origin bit.
   */
  OB_INLINE int flip(const size_type pos);

  /**
   * check bit at %pos if set
   */
  OB_INLINE bool test(const size_type pos) const;

  /**
   * same as test(pos);
   */
  OB_INLINE bool operator[](const size_type pos) const
  {
    return test(pos);
  }

  /**
   * Expand @valid_bits_ to @valid_bits,
   * reserve more memblocks if needed.
   */
  OB_INLINE int expand_size(const size_type valid_bits);

  OB_INLINE size_type get_block(const size_type pos) const;

  /**
   * Reserve more memory if @size is larger than the capacity now
   */
  OB_INLINE int reserve(const size_type capacity);

  /**
   * Bitwise AND operation with another bitmap with equal @valid_bits_
   * Current bitmap is the left value, where the result of operation is stored.
   */
  int bit_and(const ObBitmap &right);

  /**
   * Bitwise OR operation with another bitmap with equal @valid_bits_
   * Current bitmap is the left value, where the result of operation is stored.
   */
  int bit_or(const ObBitmap &right);

  /**
   * Bitwise NOT operation on the bitmap. Only the bits in range @valid_bits_ will be flipped
   */
  int bit_not();

  /**
   * Update blocks of Bitmap from array @*block_data
   */
  int load_blocks_from_array(size_type *block_data, size_type num_bits);

  OB_INLINE bool get_bit_set(const int64_t start, const int64_t size, void *&ptr) const;

public:
  OB_INLINE size_type capacity() const
  {
    return num_bits_;
  }
  OB_INLINE size_type size() const
  {
    return valid_bits_;
  }

  OB_INLINE uint64_t popcnt() const;

  OB_INLINE bool is_all_false() const;

  OB_INLINE bool is_all_true() const;

  OB_INLINE bool is_inited() const
  {
    return is_inited_;
  }
  OB_INLINE int init(const size_type valid_bits, const bool is_all_true = false);

private:
  struct MemBlock
  {
    MemBlock *next_;
    size_type bits_[BLOCKS_PER_MEM_BLOCK];
    OB_INLINE void clear_block_bits(const bool is_all_true)
    {
      if (is_all_true) {
        MEMSET(static_cast<void *>(bits_), 255, MEM_BLOCK_BYTES);
      } else {
        MEMSET(static_cast<void *>(bits_), 0, MEM_BLOCK_BYTES);
      }
    }
    OB_INLINE uint64_t popcnt(const int64_t valid_bit) const
    {
      uint64_t bit_cnt = 0;
      const int64_t cnt = valid_bit / BITS_PER_BLOCK;
      const int64_t remain = valid_bit % BITS_PER_BLOCK;
      for (int64_t i = 0; i < cnt; i++) {
        uint64_t v = bits_[i];
        bit_cnt += popcount64(v);
      }
      if (remain > 0) {
        bit_cnt += popcount64(bits_[cnt] & ((1LU << remain) - 1));
      }
      return bit_cnt;
    }
    OB_INLINE bool is_all_true(const int64_t valid_bits) const
    {
      bool is_all_true = true;
      const int64_t cnt = valid_bits / BITS_PER_BLOCK;
      const int64_t remain = valid_bits % BITS_PER_BLOCK;
      for (int64_t i = 0; is_all_true && i < cnt; i++) {
        uint64_t v = bits_[i];
        is_all_true = (uint64_t(-1) == v);
      }
      if (is_all_true && remain > 0) {
        uint64_t mask = ((1LU << remain) - 1);
        is_all_true = ((bits_[cnt] & mask) == mask);
      }
      return is_all_true;
    }
    OB_INLINE bool is_all_false(const int64_t valid_bits) const
    {
      bool is_all_false = true;
      const int64_t cnt = valid_bits / BITS_PER_BLOCK;
      const int64_t remain = valid_bits % BITS_PER_BLOCK;
      for (int64_t i = 0; is_all_false && i < cnt; i++) {
        uint64_t v = bits_[i];
        is_all_false = (0 == v);
      }
      if (is_all_false && remain > 0) {
        uint64_t mask = ((1LU << remain) - 1);
        is_all_false = ((bits_[cnt] & mask) == 0);
      }
      return is_all_false;
    }
  };

private:
  OB_INLINE static size_type block_index(size_type pos)
  {
    return pos >> BLOCK_MOD_BITS;
  }
  OB_INLINE static size_type bit_index(size_type pos)
  {
    return pos & BLOCK_MOD_MASK;
  }
  OB_INLINE static size_type bit_mask(size_type pos)
  {
    return 1ULL << bit_index(pos);
  }
  OB_INLINE static size_type round_up(size_type n, size_type align)
  {
    return (n + align - 1) & ~(align - 1);
  }

  int allocate_blocks(const size_type num_blocks, MemBlock* &head, MemBlock* &tail, const bool value = false);
  OB_INLINE MemBlock *find_block(const size_type pos, size_type &inner_pos) const;
  int expand_block(const size_type pos, size_type &inner_pos, MemBlock* &mem_block);

  DISALLOW_COPY_AND_ASSIGN(ObBitmap);

private:
  size_type valid_bits_;
  size_type num_bits_;
  MemBlock *header_;
  MemBlock *tailer_;
  ObIAllocator &allocator_;
  bool is_inited_;
};


OB_INLINE uint64_t ObBitmap::popcnt() const
{
  uint64_t popcnt = 0;
  int64_t inner_bits = valid_bits_;
  MemBlock *cur_mem_block = header_;
  while (inner_bits > 0) {
    int64_t cur_bits = min(inner_bits, static_cast<int64_t>(MEM_BLOCK_BITS));
    popcnt += cur_mem_block->popcnt(cur_bits);
    inner_bits -= MEM_BLOCK_BITS;
    cur_mem_block = cur_mem_block->next_;
  }
  return popcnt;
}

OB_INLINE bool ObBitmap::is_all_true() const
{
  bool is_all_true = true;
  int64_t inner_bits = valid_bits_;
  MemBlock *cur_mem_block = header_;
  while (inner_bits > 0 && is_all_true) {
    int64_t cur_bits = min(inner_bits, static_cast<int64_t>(MEM_BLOCK_BITS));
    is_all_true = cur_mem_block->is_all_true(cur_bits);
    inner_bits -= MEM_BLOCK_BITS;
    cur_mem_block = cur_mem_block->next_;
  }
  return is_all_true;
}

OB_INLINE bool ObBitmap::is_all_false() const
{
  bool is_all_false = true;
  int64_t inner_bits = valid_bits_;
  MemBlock *cur_mem_block = header_;
  while (inner_bits > 0 && is_all_false) {
    int64_t cur_bits = min(inner_bits, static_cast<int64_t>(MEM_BLOCK_BITS));
    is_all_false = cur_mem_block->is_all_false(cur_bits);
    inner_bits -= MEM_BLOCK_BITS;
    cur_mem_block = cur_mem_block->next_;
  }
  return is_all_false;
}

OB_INLINE int ObBitmap::reserve(const size_type capacity)
{
  int ret = OB_SUCCESS;
  size_type inner_pos = 0;
  if (capacity > num_bits_) {
    MemBlock *new_block = NULL;
    if (OB_FAIL(expand_block(capacity - 1, inner_pos, new_block))) {
      LIB_LOG(WARN, "Failed to expand blocks", K(ret));
    } else if (OB_ISNULL(new_block)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "Failed to expand MemBlock when reserve space for ObBitmap", K(capacity));
    }
  }
  return ret;
}

OB_INLINE int ObBitmap::init(const size_type valid_bits, const bool is_all_true)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "ObBitmap init twice", K(ret));
  } else {
    num_bits_ = valid_bits;
    if (num_bits_ < MEM_BLOCK_BITS) {
      num_bits_ = MEM_BLOCK_BITS;
    }
    num_bits_ = round_up(num_bits_, BITS_PER_BLOCK);
    size_type num_blocks = num_bits_ >> BLOCK_MOD_BITS;
    if (OB_FAIL(allocate_blocks(num_blocks, header_, tailer_, is_all_true))) {
      LIB_LOG(WARN, "Allocate blocks failed", K(ret), K(num_blocks));
    } else if (OB_ISNULL(header_)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "Init ObBitmap failed: no mem block allocated", K(valid_bits));
    } else {
      valid_bits_ = valid_bits;
      is_inited_ = true;
    }
  }
  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

OB_INLINE void ObBitmap::destroy()
{
  if (OB_NOT_NULL(header_)) {
    MemBlock *walk = header_;
    MemBlock *next;
    while (NULL != walk) {
      next = walk->next_;
      // deallocate block
      walk->next_ = NULL;
      allocator_.free(walk);
      walk = next;
    }
  }
}

OB_INLINE bool ObBitmap::empty() const
{
  return (NULL == header_ || 0 == num_bits_);
}

OB_INLINE int ObBitmap::set(const size_type pos, const bool value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos >= valid_bits_)) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LIB_LOG(WARN, "Pos out of valid bits", K(ret), K(pos), K(valid_bits_));
  } else if (OB_LIKELY(MEM_BLOCK_BITS > pos)) {
    if (value) {
      header_->bits_[pos >> BLOCK_MOD_BITS] |= (1ULL << (pos & BLOCK_MOD_MASK));
    } else {
      header_->bits_[pos >> BLOCK_MOD_BITS] &= (~(1ULL << (pos & BLOCK_MOD_MASK)));
    }
  } else {
    size_type inner_pos = pos;
    MemBlock *cur_mem_block = header_;
    while (MEM_BLOCK_BITS <= inner_pos) {
      cur_mem_block = cur_mem_block->next_;
      inner_pos -= MEM_BLOCK_BITS;
    }
    if (value) {
      cur_mem_block->bits_[inner_pos >> BLOCK_MOD_BITS] |= (1ULL << (inner_pos & BLOCK_MOD_MASK));
    } else {
      cur_mem_block->bits_[inner_pos >> BLOCK_MOD_BITS] &= (~(1ULL << (inner_pos & BLOCK_MOD_MASK)));
    }
  }
  return ret;
}

OB_INLINE int ObBitmap::wipe(const size_type pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "Not inited", K(ret));
  } else {
    size_type inner_pos = pos;
    MemBlock *mem_block = find_block(pos, inner_pos);
    if (OB_NOT_NULL(mem_block)) {
      mem_block->bits_[block_index(inner_pos)] &= ~bit_mask(inner_pos);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "Failed to find Bitmap's memblock", K(ret), K(pos));
    }
  }
  return ret;
}

OB_INLINE void ObBitmap::reuse(const bool is_all_true)
{
  if (OB_NOT_NULL(header_)) {
    MemBlock *walk = header_;
    while(NULL != walk) {
      walk->clear_block_bits(is_all_true);
      walk = walk->next_;
    }
  }
}

OB_INLINE int ObBitmap::expand_size(const size_type valid_bits)
{
  int ret = OB_SUCCESS;
  valid_bits_ = valid_bits;
  if (OB_FAIL(reserve(valid_bits))) {
    LIB_LOG(WARN, "Failed to reserve bitmap with num_bits", K(ret), K(valid_bits));
  }
  return ret;
}

OB_INLINE int ObBitmap::flip(const size_type pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "Not inited", K(ret));
  } else {
    size_type inner_pos = pos;
    MemBlock *mem_block = find_block(pos, inner_pos);
    if (OB_NOT_NULL(mem_block)) {
      mem_block->bits_[block_index(inner_pos)] ^= bit_mask(inner_pos);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "Failed to find Bitmap's memblock", K(ret), K(pos));
    }
  }
  return ret;
}

OB_INLINE bool ObBitmap::test(const size_type pos) const
{
  bool result = false;
  if (OB_LIKELY(MEM_BLOCK_BITS > pos)) {
    result = (header_->bits_[pos >> BLOCK_MOD_BITS] & (1ULL << (pos & BLOCK_MOD_MASK)));
  } else {
    size_type inner_pos = pos;
    MemBlock *cur_mem_block = header_;
    while (MEM_BLOCK_BITS <= inner_pos) {
      cur_mem_block = cur_mem_block->next_;
      inner_pos -= MEM_BLOCK_BITS;
    }
    result = (cur_mem_block->bits_[inner_pos >> BLOCK_MOD_BITS] & (1ULL << (inner_pos & BLOCK_MOD_MASK)));
  }
  return result;
}

OB_INLINE typename ObBitmap::size_type
ObBitmap::get_block(const size_type pos) const
{
  ObBitmap::size_type result = 0;
  if (OB_UNLIKELY(valid_bits_ < pos)) {
    LIB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "Index out of range when getting block from bitmap", K_(valid_bits), K(pos));
  } else {
    size_type inner_pos = pos;
    MemBlock *mem_block = find_block(pos, inner_pos);
    if (OB_ISNULL(mem_block)) {
      LIB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "Null pointer error when finding memblock", K(pos));
    } else {
      result = mem_block->bits_[block_index(inner_pos)];
    }
  }
  return result;
}

OB_INLINE typename ObBitmap::MemBlock *
ObBitmap::find_block(const size_type pos, size_type &inner_pos) const
{
  inner_pos = pos;
  MemBlock *cur_mem_block = header_;
  if (OB_LIKELY(MEM_BLOCK_BITS > inner_pos)) {
  } else {
    while (MEM_BLOCK_BITS <= inner_pos) {
      if (OB_ISNULL(cur_mem_block)) {
        cur_mem_block = NULL;
        break;
      }
      cur_mem_block = cur_mem_block->next_;
      inner_pos -= MEM_BLOCK_BITS;
    }
  }

  return cur_mem_block;
}

OB_INLINE bool ObBitmap::get_bit_set(const int64_t start, const int64_t size, void *&ptr) const
{
  ptr = nullptr;
  bool success = false;
  if (OB_UNLIKELY(nullptr == header_ ||
                  0 != start % BITS_PER_BLOCK ||
                  start + size > valid_bits_ ||
                  start + size > MEM_BLOCK_BITS)) {
  } else {
    success = true;
    ptr = static_cast<void *>(header_->bits_ + (start / BITS_PER_BLOCK));
  }
  return success;
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_BITMAP_H_
