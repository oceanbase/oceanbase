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

#ifndef STORAGE_OB_COMPACTION_MEMORY_POOL_H_
#define STORAGE_OB_COMPACTION_MEMORY_POOL_H_

#include "lib/ob_define.h"
#include "lib/list/ob_dlist.h"
#include "lib/task/ob_timer.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/allocator/page_arena.h"
#include "lib/literals/ob_literals.h"
#include "storage/blocksstable/ob_data_buffer.h"


namespace oceanbase
{
namespace compaction
{
class ObCompactionMemoryContext;
}

namespace storage
{
class ObCompactionBufferChunk;

class ObCompactionBufferBlock
{
public:
  enum BlockType : uint8_t {
    CHUNK_TYPE,
    PIECE_TYPE,
    MTL_PIECE_TYPE,
    INVALID_TYPE = 3
  };

  ObCompactionBufferBlock();
  ~ObCompactionBufferBlock();
  void reset();
  void move(ObCompactionBufferBlock &other);
  void *get_header() const { return header_; }
  void *get_buffer() const { return buffer_; }
  uint64_t get_buffer_size() const { return buffer_size_; }
  const BlockType &get_type() const { return type_; }
  bool empty() const { return header_ == nullptr && buffer_ == nullptr && buffer_size_ == 0 && INVALID_TYPE == type_; }
  int set_fixed_block(void *header, void *buf, uint64_t size);
  int set_piece_block(void *buf, uint64_t size, const BlockType block_type);

  TO_STRING_KV(KP_(header), KP_(buffer), K_(buffer_size), K_(type));
private:
  void *header_;
  void *buffer_;
  uint64_t buffer_size_;
  BlockType type_;
  DISALLOW_COPY_AND_ASSIGN(ObCompactionBufferBlock);
};



class ObCompactionBufferChunk : public common::ObDLinkBase<ObCompactionBufferChunk>
{
public:
  ObCompactionBufferChunk();
  virtual ~ObCompactionBufferChunk();
  void reset();
  int init(void *buf, const int64_t buf_len, const int64_t block_num);
  int alloc_block(ObCompactionBufferBlock &buffer_block);
  int free_block(ObCompactionBufferBlock &buffer_block);
  OB_INLINE bool has_free_block() const { return pending_idx_ > alloc_idx_; }
  OB_INLINE bool is_free_chunk() const { return pending_idx_ - alloc_idx_ == DEFAULT_BLOCK_CNT; }
  TO_STRING_KV(K_(start), K_(len), K_(alloc_idx), K_(pending_idx));
public:
  static constexpr int64_t DEFAULT_BLOCK_SIZE = 2 << 20; //2MB
  static constexpr int64_t DEFAULT_BLOCK_CNT = 8;

  void *free_blocks_[DEFAULT_BLOCK_CNT];
  void *start_;
  uint64_t len_;
  uint64_t alloc_idx_;
  uint64_t pending_idx_;
};


class ObTenantCompactionMemPool
{
public:
  enum MemoryMode : int64_t
  {
    NORMAL_MODE = 0,
    EMERGENCY_MODE = 1
  };

  static int mtl_init(ObTenantCompactionMemPool* &mem_pool);
  ObTenantCompactionMemPool();
  virtual ~ObTenantCompactionMemPool();
  void wait();
  void stop();
  void destroy();
  void reset();
  int init();
  int alloc(const int64_t size, ObCompactionBufferBlock &buffer_block);
  void free(ObCompactionBufferBlock &buffer_block);
  bool acquire_reserve_mem();
  bool release_reserve_mem();
  void set_memory_mode(const MemoryMode mem_mode) { ATOMIC_STORE(&mem_mode_, mem_mode); }
  bool is_emergency_mode() { return MemoryMode::EMERGENCY_MODE == ATOMIC_LOAD(&mem_mode_); }

  OB_INLINE int64_t get_total_block_num() const { return total_block_num_; }
  OB_INLINE int64_t get_max_block_num() const { return max_block_num_; }
  OB_INLINE int64_t get_block_size() const { return ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE; }

private:
  int alloc_chunk(ObCompactionBufferBlock &buffer_block);
  int alloc_piece(const int64_t size, ObCompactionBufferBlock &buffer_block);
  int expand();
  int try_shrink();

private:
  class MemPoolShrinkTask : public common::ObTimerTask
  {
  public:
    MemPoolShrinkTask(ObTenantCompactionMemPool &mem_pool)
      : mem_pool_(mem_pool),
        last_check_dag_cnt_(-1) {}
    virtual ~MemPoolShrinkTask() {}
    virtual void runTimerTask();
  private:
    ObTenantCompactionMemPool &mem_pool_;
    int64_t last_check_dag_cnt_;
  };

public:
  static constexpr int64_t CHUNK_MEMORY_LIMIT = 128_MB;
  static constexpr int64_t MINI_MODE_CHUNK_MEMORY_LIMIT = 32_MB;
  static constexpr int64_t RESERVE_MEM_SIZE = 32_MB;
  static constexpr int64_t CHECK_SHRINK_INTERVAL = 120_s;
private:
  MemPoolShrinkTask mem_shrink_task_;
  common::DefaultPageAllocator chunk_allocator_;
  common::DefaultPageAllocator piece_allocator_;
  common::ObSpinLock chunk_lock_;
  common::ObSpinLock piece_lock_;
  ObDList<ObCompactionBufferChunk> chunk_list_;
  int64_t max_block_num_;
  int64_t total_block_num_;
  int64_t used_block_num_;
  MemoryMode mem_mode_;
  int tg_id_;
  int8_t reserve_mode_signal_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObTenantCompactionMemPool);
};


class ObCompactionBufferWriter : public blocksstable::ObBufferWriter
{
public:
  virtual int expand(const int64_t size)
  {
    int64_t new_size = std::max(capacity_ * 2,
                                static_cast<int64_t>(size + capacity_));
    return ensure_space(new_size);
  }

public:
  ObCompactionBufferWriter(
      const char *label,
      const int64_t size = 0,
      const bool use_mem_pool = true);
  virtual ~ObCompactionBufferWriter();
  int ensure_space(const int64_t size);
  bool is_dirty() const { return OB_NOT_NULL(data_) && pos_ > 0; }
  void reset();
  inline void reuse() { pos_ = 0; }
private:
  int resize(const int64_t size);
  int alloc_block(const int64_t size, ObCompactionBufferBlock &block);
  void free_block();
private:
  const char *label_;
  bool use_mem_pool_;
  compaction::ObCompactionMemoryContext *ref_mem_ctx_;
  ObCompactionBufferBlock block_; // when block is valid, the buffer mem came from pool.
};


} // storage
} // oceanbase

#endif // STORAGE_OB_COMPACTION_MEMORY_POOL_H_
