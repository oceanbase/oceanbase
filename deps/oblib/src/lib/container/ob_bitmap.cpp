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

namespace oceanbase
{
namespace common
{
ObBitmap::ObBitmap(ObIAllocator &allocator)
    : valid_bits_(0), num_bits_(0), header_(NULL),
    tailer_(NULL), allocator_(allocator), is_inited_(false) {}

ObBitmap::~ObBitmap()
{
  destroy();
}


int ObBitmap::bit_and(const ObBitmap &right)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "Not inited", K(ret));
  } else if (OB_UNLIKELY(right.size() != valid_bits_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Bitmaps for bitwise AND have different valid bits length", K(ret), K(right.size()), K_(valid_bits));
  } else {
    MemBlock *left_ptr = header_;
    MemBlock *right_ptr = right.header_;
    if (OB_ISNULL(left_ptr) || OB_ISNULL(right_ptr)) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "Null pointer from bitmaps for bitwise AND operation.", K(ret), K(right.size()), K_(valid_bits));
    } else {
      left_ptr = header_;
      right_ptr = right.header_;
      size_type traverse_count = 0;
      while (NULL != left_ptr && NULL != right_ptr && traverse_count < valid_bits_) {
        for (size_type i = 0; i < BLOCKS_PER_MEM_BLOCK && traverse_count < valid_bits_; ++i) {
          left_ptr->bits_[i] &= right_ptr->bits_[i];
          traverse_count += BITS_PER_BLOCK;
        }
        left_ptr = left_ptr->next_;
        right_ptr = right_ptr->next_;
      }
    }
  }
  return ret;
}

int ObBitmap::bit_or(const ObBitmap &right)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)){
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "Not inited", K(ret));
  } else if (OB_UNLIKELY(right.size() != valid_bits_)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Bitmaps for bitwise AND have different valid bits length", K(ret), K(right.size()), K_(valid_bits));
  } else {
    MemBlock *left_ptr = header_;
    MemBlock *right_ptr = right.header_;
    if (OB_ISNULL(left_ptr) || OB_ISNULL(right_ptr)) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "Null pointer from bitmaps for bitwise AND operation.", K(ret), K(right.size()), K_(valid_bits));
    } else {
      left_ptr = header_;
      right_ptr = right.header_;
      size_type traverse_count = 0;
      while (NULL != left_ptr && NULL != right_ptr && traverse_count < valid_bits_) {
        for (size_type i = 0; i < BLOCKS_PER_MEM_BLOCK && traverse_count < valid_bits_; ++i) {
          left_ptr->bits_[i] |= right_ptr->bits_[i];
          traverse_count += BITS_PER_BLOCK;
        }
        left_ptr = left_ptr->next_;
        right_ptr = right_ptr->next_;
      }
    }
  }
  return ret;
}

int ObBitmap::bit_not()
{
  int ret = OB_SUCCESS;
  MemBlock *walk_ptr = header_;
  if (OB_UNLIKELY(!is_inited_)){
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "Not inited", K(ret));
  } else if (OB_ISNULL(walk_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Null pointer from bitmap for bitwise NOT operation.", K(ret), K_(valid_bits));
  } else {
    size_type traverse_count = 0;
    while (NULL != walk_ptr && traverse_count < valid_bits_) {
      for (size_type i = 0; i < BLOCKS_PER_MEM_BLOCK && traverse_count < valid_bits_; ++i) {
        walk_ptr->bits_[i] = ~walk_ptr->bits_[i];
        traverse_count += BITS_PER_BLOCK;
      }
      walk_ptr = walk_ptr->next_;
    }
  }
  return ret;
}

int ObBitmap::load_blocks_from_array(size_type *block_data, size_type num_bits)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(block_data)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Trying to load data to Bitmap from null array", K(ret), K(block_data), K(num_bits));
  } else {
    valid_bits_ = num_bits;
    MemBlock *walk_ptr = header_;
    if (OB_FAIL(reserve(num_bits))) {
      LIB_LOG(WARN, "Failed to reserve bitmap with num_bits", K(ret), K(num_bits));
    } else if (OB_ISNULL(walk_ptr)) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "Null pointer from bitmap memblock header", K_(valid_bits), K_(num_bits), K_(header), K_(tailer));
    } else {
      // Copy data from block_data to linked list
      size_type copy_offset = 0;
      size_type require_blocks = round_up(valid_bits_, BITS_PER_BLOCK) >> BLOCK_MOD_BITS;
      while (NULL != walk_ptr && copy_offset < require_blocks) {
        size_type need_blocks = require_blocks - copy_offset;
        size_type copy_block = need_blocks > BLOCKS_PER_MEM_BLOCK ? BLOCKS_PER_MEM_BLOCK : need_blocks;
        MEMCPY(walk_ptr->bits_, block_data + copy_offset, copy_block * sizeof(size_type));
        copy_offset += copy_block;
        walk_ptr = walk_ptr->next_;
      }
      // Set bits in the same block after @valid_bits_ to false
      if (0 != (valid_bits_ & BLOCK_MOD_MASK)) {
        size_type inner_pos = valid_bits_;
        walk_ptr = find_block(valid_bits_, inner_pos);
        for (size_type i = bit_index(inner_pos); i < BITS_PER_BLOCK; ++i) {
          walk_ptr->bits_[block_index(inner_pos)] &= ~bit_mask(i);
        }
      }
    }
  }
  return ret;
}

// Allocate a linked list with @head as head pointer and @tail as tail pointer
int ObBitmap::allocate_blocks(const size_type num_blocks, MemBlock* &head, MemBlock* &tail, const bool value)
{
  int ret = OB_SUCCESS;
  if (num_blocks <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument for bitmap allocate blocks", K(ret));
  } else {
    head = reinterpret_cast<MemBlock*>(allocator_.alloc(sizeof(MemBlock)));
    if (OB_ISNULL(head)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "Failed to allocate memory for bitmao block", K(ret), K(num_blocks));
    } else {
      head->clear_block_bits(value);
      head->next_ = NULL;
      MemBlock *walk = head;
      size_type allocated_blocks_num = BLOCKS_PER_MEM_BLOCK;
      while (OB_SUCC(ret) && allocated_blocks_num < num_blocks) {
        walk->next_ = reinterpret_cast<MemBlock*>(allocator_.alloc(sizeof(MemBlock)));
        if (OB_ISNULL(walk->next_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(WARN, "Failed to allocate memory for bitmap block", K(ret), K(num_blocks), K(allocated_blocks_num));
        } else {
          walk = walk->next_;
          walk->clear_block_bits(value);
          walk->next_ = NULL;
          allocated_blocks_num += BLOCKS_PER_MEM_BLOCK;
        }
      }
      tail = walk;
    }
  }
  return ret;
}

int ObBitmap::expand_block(const size_type pos, size_type &inner_pos, MemBlock* &mem_block)
{
  int ret = OB_SUCCESS;
  size_type need_bits = 0;
  size_type num_blocks = 0;
  inner_pos = 0;
  MemBlock *new_head = NULL;
  MemBlock *new_tail = NULL;

  if (pos >= num_bits_) {
    need_bits = pos - (num_bits_ - 1);
    if (need_bits < MEM_BLOCK_BITS) {
      need_bits = MEM_BLOCK_BITS;
    }
    need_bits = round_up(need_bits, BITS_PER_BLOCK);
    num_blocks = need_bits >> BLOCK_MOD_BITS;
    if (OB_FAIL(allocate_blocks(num_blocks, new_head, new_tail))) {
      LIB_LOG(WARN, "Failed to allocate mem blocks", K(ret), K(num_blocks));
    } else if (OB_ISNULL(new_head) || OB_ISNULL(new_tail)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "Failed to allocate mem blocks", K(ret), K(num_blocks));
    } else {
      if (OB_NOT_NULL(tailer_)) {
        OB_ASSERT(NULL != header_);
        tailer_->next_ = new_head;
        tailer_ = new_tail;
      } else {
        OB_ASSERT(NULL == header_);
        OB_ASSERT(is_inited_);
        header_ = new_head;
        tailer_ = new_tail;
      }
      inner_pos = pos - num_bits_;
      num_bits_ += need_bits;
    }
  }
  if (OB_SUCC(ret)) {
    mem_block = find_block(pos, inner_pos);
  }
  return ret;
}

} //end namespace oceanbase

} //end namespace common
