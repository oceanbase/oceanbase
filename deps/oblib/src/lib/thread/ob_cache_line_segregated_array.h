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

#ifndef OB_CACHE_LINE_SEGREGATED_ARRAY_H_
#define OB_CACHE_LINE_SEGREGATED_ARRAY_H_

#include <lib/allocator/ob_malloc.h>
#include <lib/lock/ob_latch.h>
#include <lib/cpu/ob_cpu_topology.h>

namespace oceanbase {
namespace common {

/**
 * The array provided by the multi-entry data structure,
 * Ensure that each element of the array falls in a different cache line, regardless of the size of T.
 * Usually the data structure defines each element of the array as CACHE_ALIGNED,
 * At this time, small data structures will also occupy at least one cache line
 *
 * ObCacheLineSegregatedArray must be allocated from ObCacheLineSegregatedArrayBase from calling alloc_array,
 * Therefore, the number of elements in the array is fixedly specified when the Base class is allocated,
 * The Base class will disperse the elements of the array in different areas.
 * The elements of the same area of multiple arrays are arranged compactly,
 * Storage space will not be wasted due to cache line alignment

 * The ObCacheLineSegregatedArray class provides operator[] to take the address operator overload, so it can be used
 like a normal array
 *
 * Interfaces:
 *   operator[]
 *   size
 */
template <typename T>
class ObCacheLineSegregatedArray {
private:
  void* array_;
  int64_t step_len_;
  int64_t arena_num_;

public:
  TO_STRING_KV(KP(this), K_(array), K_(step_len), K_(arena_num));

public:
  int init(void* array, int64_t step_len, int64_t arena_num)
  {
    int ret = OB_SUCCESS;
    array_ = array;
    step_len_ = step_len;
    arena_num_ = arena_num;
    return ret;
  }
  const T& operator[](int64_t idx) const
  {
    if (idx < arena_num_) {
      return *reinterpret_cast<const T*>(static_cast<const char*>(array_) + idx * step_len_);
    } else {
      COMMON_LOG(ERROR, "FATAL ERROR UNEXPECTED ERROR OUT OF BOUND", K(idx), K(*this));
      return *reinterpret_cast<const T*>(static_cast<const char*>(array_));
    }
  }
  T& operator[](int64_t idx)
  {
    if (idx < arena_num_) {
      return *reinterpret_cast<T*>(static_cast<char*>(array_) + idx * step_len_);
    } else {
      COMMON_LOG(ERROR, "FATAL ERROR UNEXPECTED ERROR OUT OF BOUND", K(idx), K(*this));
      return *reinterpret_cast<T*>(static_cast<char*>(array_));
    }
  }
  int64_t size()
  {
    return arena_num_;
  }
  int64_t get_step_len()
  {
    return step_len_;
  }
};

/**
 * Multi-entry data structure at the global level.
 *
 * Arena_num is 2 times the number of machine cores
 * After applying for OB_MALLOC_BIG_BLOCK_SIZE memory, it is divided into arena_num areas (the area is cache line
 * aligned) If arena_num is 128, each area is 16320 bytes If arena_num is 64, each area is 32704 bytes Addresses are
 * allocated to external users on demand, and the relative position of each user in each area is the same
 *
 * If 5 ObCacheLineSegregatedArrays A, B, C, D, and E are allocated in sequence, the memory arrangement is as follows:
 * When A, B, and C are allocated, their location is in the corresponding arena in the first block.
 * When the first block is used up when D is allocated, another block will be allocated. At the end of the new block
 * there is a pointer to the previous block Then allocate E in the new block
 *
 *             +---------------------------------------+
 *             |  0  |  1  |  2  |     ...     |  127  |
 *             |     |     |     |             |       |
 *             |  A  |  A  |  A  |             |   A   |
 *             |  B  |  B  |  B  |             |   B   |
 *             |  C  |  C  |  C  |             |   C   |
 *             +---------------------------------------+
 *             |     MAGIC       |       prev          |----> NULL
 *             +---------------------------------------+
 *                                        ^
 * chunk_ -------+                        |
 *               |                        +-----------------+
 *               V                                          |
 *             +---------------------------------------+    |
 *             |  0  |  1  |  2  |     ...     |  127  |    |
 *             |     |     |     |             |       |    |
 *             |  D  |  D  |  D  |             |   D   |    |
 *             |  E  |  E  |  E  |             |   E   |    |
 *             |     |     |     |             |       |    |
 *             +---------------------------------------+    |
 *             |     MAGIC       |       prev          |----+
 *             +---------------------------------------+
 */
class ObCacheLineSegregatedArrayBase {
public:
  static const int64_t ALLOC_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  static const int64_t CHUNK_SIZE = ALLOC_SIZE - 16;
  static const int64_t CHUNK_MAGIC = 0xFFEEE000000EEEFF;

public:
  static ObCacheLineSegregatedArrayBase& get_instance()
  {
    static ObCacheLineSegregatedArrayBase instance_;
    return instance_;
  }
  template <typename T>
  int alloc_array(ObCacheLineSegregatedArray<T>& array)
  {
    int ret = OB_SUCCESS;
    int64_t apos = -1;
    {
      ObLatchWGuard mutex_guard(mutex_, ObLatchIds::CACHE_LINE_SEGREGATED_ARRAY_BASE_LOCK);
      int64_t new_pos = alloc_pos_ + sizeof(T);
      if (NULL != chunk_ && new_pos <= arena_size_) {
        apos = alloc_pos_;
        alloc_pos_ = new_pos;
      } else if (OB_FAIL(alloc_chunk())) {
      } else {
        apos = 0;
        alloc_pos_ = sizeof(T);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(array.init(static_cast<void*>(static_cast<char*>(chunk_) + apos), arena_size_, arena_num_))) {
        COMMON_LOG(ERROR, "array init failed", K(ret), K(apos), K(*this));
      }
    }
    return ret;
  }

public:
  TO_STRING_KV(KP(this), K_(chunk), K_(arena_num), K_(arena_size), K_(alloc_pos), K_(mutex));

private:
  ObCacheLineSegregatedArrayBase() : chunk_(0), arena_num_(0), arena_size_(0), alloc_pos_(0)
  {
    arena_num_ = get_cpu_count();
    alloc_chunk();
  }
  int alloc_chunk()
  {
    int ret = OB_SUCCESS;
    void* old_chunk = chunk_;
    if ((chunk_ = ob_malloc(ALLOC_SIZE, ObModIds::OB_CACHE_LINE_SEGREGATED_ARRAY)) != NULL) {
      arena_size_ = lower_align(CHUNK_SIZE / arena_num_, CACHE_ALIGN_SIZE);
      alloc_pos_ = 0;
      int64_t* magic_ptr = reinterpret_cast<int64_t*>(static_cast<char*>(chunk_) + CHUNK_SIZE);
      *magic_ptr = CHUNK_MAGIC;
      *reinterpret_cast<void**>(magic_ptr + 1) = old_chunk;
    } else {
      COMMON_LOG(ERROR, "allocate memory failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    return ret;
  }

private:
  void* chunk_;
  int64_t arena_num_;
  int64_t arena_size_;
  int64_t alloc_pos_;
  ObLatch mutex_;
};

}  // namespace common
}  // namespace oceanbase
#endif  // OB_CACHE_LINE_SEGREGATED_ARRAY_H_
