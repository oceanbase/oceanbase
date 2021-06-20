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

namespace oceanbase {
namespace common {
class ObBitmap {
public:
  typedef uint64_t size_type;
  typedef uint64_t block_type;

  static const size_type BYTES_PER_BLOCK = sizeof(block_type);
  static const size_type BITS_PER_BLOCK = BYTES_PER_BLOCK * 8;
  static const size_type BLOCKS_PER_MEM_BLOCK = 64;
  static const size_type MEM_BLOCK_BYTES = BLOCKS_PER_MEM_BLOCK * BYTES_PER_BLOCK;
  static const size_type MEM_BLOCK_BITS = BLOCKS_PER_MEM_BLOCK * BITS_PER_BLOCK;
  static const size_type BLOCK_MOD_MASK = BITS_PER_BLOCK - 1;
  static const size_type BLOCK_MOD_BITS = 6;

public:
  ObBitmap(ObIAllocator& allocator);
  virtual ~ObBitmap();

  /**
   * check if bitmap is empty; not contains any data.
   */
  inline bool empty() const;

  /**
   * free all hold memory in bitmap.
   * empty will return true;
   */
  inline void destroy();

  /**
   * set bit at %pos if value true
   * or wipe out bit at %pos if value false, same as clear(pos)
   */
  inline int set(const size_type pos, const bool value = true);

  /**
   * same as set(pos, false);
   */
  inline int wipe(const size_type pos);

  /**
   * reset all bits in bitmap;
   */
  inline void reuse(const bool is_all_true = false);

  /**
   * xor with origin bit.
   */
  inline int flip(const size_type pos);

  /**
   * check bit at %pos if set
   */
  inline int test(const size_type pos, bool& result) const;
  inline bool test(const size_type pos) const;

  /**
   * same as test(pos);
   */
  inline bool operator[](const size_type pos) const
  {
    return test(pos);
  }

  /**
   * Expand @valid_bits_ to @valid_bits,
   * reserve more memblocks if needed.
   */
  inline int expand_size(const size_type valid_bits);

  inline block_type get_block(const size_type pos) const;

  /**
   * Reserve more memory if @size is larger than the capacity now
   */
  inline int reserve(const size_type capacity);

  /**
   * Bitwise AND operation with another bitmap with equal @valid_bits_
   * Current bitmap is the left value, where the result of operation is stored.
   */
  int bit_and(const ObBitmap& right);

  /**
   * Bitwise OR operation with another bitmap with equal @valid_bits_
   * Current bitmap is the left value, where the result of operation is stored.
   */
  int bit_or(const ObBitmap& right);

  /**
   * Bitwise NOT operation on the bitmap. Only the bits in range @valid_bits_ will be flipped
   */
  int bit_not();

  /**
   * Update blocks of Bitmap from array @*block_data
   */
  int load_blocks_from_array(block_type* block_data, size_type num_bits);

public:
  inline size_type capacity() const
  {
    return num_bits_;
  }
  inline size_type size() const
  {
    return valid_bits_;
  }
  inline uint64_t popcnt() const
  {
    return popcnt_;
  }
  inline bool is_all_false() const
  {
    return popcnt_ == 0;
  }
  inline bool is_all_true() const
  {
    return popcnt_ == valid_bits_;
  }
  inline bool is_inited() const
  {
    return is_inited_;
  }
  inline int init(const size_type valid_bits, const bool is_all_true = false);

private:
  struct MemBlock {
    MemBlock* next_;
    block_type bits_[BLOCKS_PER_MEM_BLOCK];
    inline void clear_block_bits(const bool is_all_true)
    {
      if (is_all_true) {
        MEMSET(static_cast<void*>(bits_), 255, MEM_BLOCK_BYTES);
      } else {
        MEMSET(static_cast<void*>(bits_), 0, MEM_BLOCK_BYTES);
      }
    }
  };

private:
  inline static size_type block_index(size_type pos)
  {
    return pos >> BLOCK_MOD_BITS;
  }
  inline static size_type bit_index(size_type pos)
  {
    return static_cast<block_type>(pos & BLOCK_MOD_MASK);
  }
  inline static block_type bit_mask(size_type pos)
  {
    return static_cast<block_type>(1ULL << bit_index(pos));
  }
  inline static size_type round_up(size_type n, size_type align)
  {
    return (n + align - 1) & ~(align - 1);
  }

  int allocate_blocks(const size_type num_blocks, MemBlock*& head, MemBlock*& tail, const bool value = false);
  MemBlock* find_block(const size_type pos, size_type& inner_pos) const;
  int expand_block(const size_type pos, size_type& inner_pos, MemBlock*& mem_block);

  DISALLOW_COPY_AND_ASSIGN(ObBitmap);

private:
  size_type valid_bits_;
  size_type num_bits_;
  size_type popcnt_;
  MemBlock* header_;
  MemBlock* tailer_;
  ObIAllocator& allocator_;
  bool is_inited_;
};

inline int ObBitmap::reserve(const size_type capacity)
{
  int ret = OB_SUCCESS;
  size_type inner_pos = 0;
  if (capacity > num_bits_) {
    MemBlock* new_block = NULL;
    if (OB_FAIL(expand_block(capacity - 1, inner_pos, new_block))) {
      LIB_LOG(WARN, "Failed to expand blocks", K(ret));
    } else if (OB_ISNULL(new_block)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "Failed to expand MemBlock when reserve space for ObBitmap", K(capacity));
    }
  }
  return ret;
}

inline int ObBitmap::init(const size_type valid_bits, const bool is_all_true)
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
      if (is_all_true) {
        popcnt_ = valid_bits;
      } else {
        popcnt_ = 0;
      }
      valid_bits_ = valid_bits;
      is_inited_ = true;
    }
  }
  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

inline void ObBitmap::destroy()
{
  if (OB_NOT_NULL(header_)) {
    MemBlock* walk = header_;
    MemBlock* next;
    while (NULL != walk) {
      next = walk->next_;
      // deallocate block
      walk->next_ = NULL;
      allocator_.free(walk);
      walk = next;
    }
  }
}

inline bool ObBitmap::empty() const
{
  return (NULL == header_ || 0 == num_bits_);
}

inline int ObBitmap::set(const size_type pos, const bool value)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "Not inited", K(ret));
  } else if (OB_UNLIKELY(pos >= valid_bits_)) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LIB_LOG(WARN, "Pos out of valid bits", K(ret), K(pos), K(valid_bits_));
  } else {
    size_type inner_pos = pos;
    MemBlock* mem_block = find_block(pos, inner_pos);
    if (OB_NOT_NULL(mem_block)) {
      if (value) {
        if ((mem_block->bits_[block_index(inner_pos)] & bit_mask(inner_pos)) == 0) {
          popcnt_++;
        }
        mem_block->bits_[block_index(inner_pos)] |= bit_mask(inner_pos);
      } else {
        if ((mem_block->bits_[block_index(inner_pos)] & bit_mask(inner_pos)) != 0) {
          popcnt_--;
        }
        mem_block->bits_[block_index(inner_pos)] &= static_cast<block_type>(~bit_mask(inner_pos));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "Failed to find memblock or allocate memory Bitmap", K(ret), K(pos));
    }
  }
  return ret;
}

inline int ObBitmap::wipe(const size_type pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "Not inited", K(ret));
  } else {
    size_type inner_pos = pos;
    MemBlock* mem_block = find_block(pos, inner_pos);
    if (OB_NOT_NULL(mem_block)) {
      if (OB_LIKELY((mem_block->bits_[block_index(inner_pos)] & bit_mask(inner_pos)) != 0)) {
        popcnt_--;
      }
      mem_block->bits_[block_index(inner_pos)] &= ~bit_mask(inner_pos);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "Failed to find Bitmap's memblock", K(ret), K(pos));
    }
  }
  return ret;
}

inline void ObBitmap::reuse(const bool is_all_true)
{
  if (OB_NOT_NULL(header_)) {
    MemBlock* walk = header_;
    while (NULL != walk) {
      walk->clear_block_bits(is_all_true);
      walk = walk->next_;
    }
  }
  if (is_all_true) {
    popcnt_ = valid_bits_;
  } else {
    popcnt_ = 0;
  }
}

inline int ObBitmap::expand_size(const size_type valid_bits)
{
  int ret = OB_SUCCESS;
  valid_bits_ = valid_bits;
  if (OB_FAIL(reserve(valid_bits))) {
    LIB_LOG(WARN, "Failed to reserve bitmap with num_bits", K(ret), K(valid_bits));
  }
  return ret;
}

inline int ObBitmap::flip(const size_type pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "Not inited", K(ret));
  } else {
    size_type inner_pos = pos;
    MemBlock* mem_block = find_block(pos, inner_pos);
    if (OB_NOT_NULL(mem_block)) {
      if ((mem_block->bits_[block_index(inner_pos)] & bit_mask(inner_pos)) != 0) {
        popcnt_--;
      } else {
        popcnt_++;
      }
      mem_block->bits_[block_index(inner_pos)] ^= bit_mask(inner_pos);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "Failed to find Bitmap's memblock", K(ret), K(pos));
    }
  }
  return ret;
}

inline int ObBitmap::test(const size_type pos, bool& result) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "Not inited", K(ret));
  } else {
    size_type inner_pos = pos;
    MemBlock* mem_block = find_block(pos, inner_pos);
    if (OB_NOT_NULL(mem_block)) {
      result = (mem_block->bits_[block_index(inner_pos)] & bit_mask(inner_pos)) != 0;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "Failed to find Bitmap's memblock", K(ret), K(pos));
    }
  }
  return ret;
}

inline bool ObBitmap::test(const size_type pos) const
{
  bool result = false;
  test(pos, result);
  return result;
}

inline typename ObBitmap::block_type ObBitmap::get_block(const size_type pos) const
{
  ObBitmap::block_type result = static_cast<ObBitmap::block_type>(0);
  if (OB_UNLIKELY(valid_bits_ < pos)) {
    LIB_LOG(WARN, "Index out of range when getting block from bitmap", K_(valid_bits), K(pos));
  } else {
    size_type inner_pos = pos;
    MemBlock* mem_block = find_block(pos, inner_pos);
    if (OB_ISNULL(mem_block)) {
      LIB_LOG(WARN, "Null pointer error when finding memblock", K(pos));
    } else {
      result = mem_block->bits_[block_index(inner_pos)];
    }
  }
  return result;
}

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_BITMAP_H_
