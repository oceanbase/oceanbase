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

namespace oceanbase
{
namespace common
{

/**
 * 多入口的数据结构提供的数组，
 * 保证数组的每个元素都落在不同的 cache line 中，无论 T 的大小。
 * 通常数据结构会把数组的每个元素定义成 CACHE_ALIGNED 的，
 * 此时很小的数据结构也会至少占用一个 cache line
 *
 * ObCacheLineSegregatedArray 必须要从 ObCacheLineSegregatedArrayBase 从调用 alloc_array 分配，
 * 所以数组的元素个数是 Base 类分配的时固定指定的，
 * Base 类会把数组的元素分散在不同的区域，多个数组同一个区域的元素是紧凑排列的，
 * 存储空间不会因为 cache line 对齐而浪费

 * ObCacheLineSegregatedArray 类提供 operator[] 取地址运算符重载，所以可以像普通数组一样使用
 *
 * 接口：
 *   operator[]
 *   size
 */
template <typename T>
class ObCacheLineSegregatedArray
{
private:
  void *array_;
  int64_t step_len_;
  int64_t arena_num_;
public:
  TO_STRING_KV(KP(this), K_(array), K_(step_len), K_(arena_num));
public:
  int init(void *array, int64_t step_len, int64_t arena_num) {
    int ret = OB_SUCCESS;
    array_ = array;
    step_len_ = step_len;
    arena_num_ = arena_num;
    return ret;
  }
  const T& operator[](int64_t idx) const {
    if (idx < arena_num_) {
      return *reinterpret_cast<const T*>(static_cast<const char*>(array_) + idx * step_len_);
    } else {
      COMMON_LOG(ERROR, "FATAL ERROR UNEXPECTED ERROR OUT OF BOUND", K(idx), K(*this));
      return *reinterpret_cast<const T*>(static_cast<const char*>(array_));
    }
  }
  T& operator[](int64_t idx) {
    if (idx < arena_num_) {
      return *reinterpret_cast<T*>(static_cast<char*>(array_) + idx * step_len_);
    } else {
      COMMON_LOG(ERROR, "FATAL ERROR UNEXPECTED ERROR OUT OF BOUND", K(idx), K(*this));
      return *reinterpret_cast<T*>(static_cast<char*>(array_));
    }
  }
  int64_t size() {return arena_num_;}
  int64_t get_step_len() {return step_len_;}
};

/**
 * 全局级别的数据结构多入口.
 *
 * arena_num 是机器核数的 2 倍
 * 申请 OB_MALLOC_BIG_BLOCK_SIZE 大小的内存后切分成 arena_num 个区域（区域是 cache line 对齐）
 * 如果 arena_num 是 128，每个区域 16320 字节
 * 如果 arena_num 是 64，每个区域 32704 字节
 * 按需分配给外部使用者地址，每个使用者在每个区域的相对位置是一样的
 *
 * 如果依次分配 A、B、C、D、E 5个 ObCacheLineSegregatedArray，那么内存排列如下：
 * 当 A、B、C 分配完后，其所在位置在第一个块中对应 arena 里。
 * 分配 D 时第一块用完了，会再分配一块，新块尾部有一个指针指向之前的块
 * 接着在新的块中分配 E
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
class ObCacheLineSegregatedArrayBase
{
public:
  static const int64_t ALLOC_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  static const int64_t CHUNK_SIZE = ALLOC_SIZE - 16;
  static const int64_t CHUNK_MAGIC = 0xFFEEE000000EEEFF;
public:
  static ObCacheLineSegregatedArrayBase& get_instance() {
    static ObCacheLineSegregatedArrayBase instance_;
    return instance_;
  }
  template <typename T>
  int alloc_array(ObCacheLineSegregatedArray<T> &array) {
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
  ObCacheLineSegregatedArrayBase() : chunk_(0), arena_num_(0), arena_size_(0), alloc_pos_(0) {
    arena_num_ = get_cpu_count();
    alloc_chunk();
  }
  int alloc_chunk() {
    int ret = OB_SUCCESS;
    void *old_chunk = chunk_;
    if ((chunk_ = ob_malloc(ALLOC_SIZE, ObModIds::OB_CACHE_LINE_SEGREGATED_ARRAY)) != NULL) {
      arena_size_ = lower_align(CHUNK_SIZE / arena_num_, CACHE_ALIGN_SIZE);
      alloc_pos_ = 0;
      int64_t *magic_ptr = reinterpret_cast<int64_t*>(static_cast<char*>(chunk_) + CHUNK_SIZE);
      *magic_ptr = CHUNK_MAGIC;
      *reinterpret_cast<void**>(magic_ptr + 1) = old_chunk;
    } else {
      COMMON_LOG(ERROR, "allocate memory failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    return ret;
  }
private:
  void *chunk_;
  int64_t arena_num_;
  int64_t arena_size_;
  int64_t alloc_pos_;
  ObLatch mutex_;
};

}
}
#endif // OB_CACHE_LINE_SEGREGATED_ARRAY_H_
