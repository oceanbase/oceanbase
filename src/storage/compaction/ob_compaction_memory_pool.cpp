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

#define USING_LOG_PREFIX STORAGE

#include "ob_compaction_memory_pool.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_force_print_log.h"
#include "share/ob_thread_mgr.h"
#include "lib/allocator/ob_mod_define.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/compaction/ob_compaction_memory_context.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;


ObCompactionBufferBlock::ObCompactionBufferBlock()
  : header_(nullptr),
    buffer_(nullptr),
    buffer_size_(0),
    type_(BlockType::INVALID_TYPE)
{
}

ObCompactionBufferBlock::~ObCompactionBufferBlock()
{
  reset();
}

void ObCompactionBufferBlock::reset()
{
  header_ = nullptr;
  buffer_ = nullptr;
  buffer_size_ = 0;
  type_ = BlockType::INVALID_TYPE;
}

void ObCompactionBufferBlock::move(ObCompactionBufferBlock &other)
{
  header_ = other.header_;
  buffer_ = other.buffer_;
  buffer_size_ = other.buffer_size_;
  type_ = other.type_;
  other.reset();
}

int ObCompactionBufferBlock::set_fixed_block(
    void *header,
    void *buf,
    uint64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == header || NULL == buf || 0 == size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(header), K(buf), K(size));
  } else {
    header_ = header;
    buffer_ = buf;
    buffer_size_ = size;
    type_ = BlockType::CHUNK_TYPE;
  }
  return ret;
}

int ObCompactionBufferBlock::set_piece_block(
    void *buf,
    uint64_t size,
    const BlockType block_type)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == buf || 0 == size || BlockType::CHUNK_TYPE == block_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(buf), K(size), K(block_type));
  } else {
    buffer_ = buf;
    buffer_size_ = size;
    type_ = block_type;
  }
  return ret;
}



/* ********************************************** ObCompactionBufferChunk ********************************************** */
ObCompactionBufferChunk::ObCompactionBufferChunk()
  : free_blocks_(),
    start_(nullptr),
    len_(0),
    alloc_idx_(0),
    pending_idx_(0)
{
}

ObCompactionBufferChunk::~ObCompactionBufferChunk()
{
  reset();
}

void ObCompactionBufferChunk::reset()
{
  MEMSET(free_blocks_, 0, sizeof(void *) * DEFAULT_BLOCK_CNT);
  alloc_idx_ = 0;
  pending_idx_ = 0;
  len_ = 0;
  start_ = nullptr;
}

int ObCompactionBufferChunk::init(
    void *buf,
    const int64_t buf_len,
    const int64_t block_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf ||
                  0 >= buf_len ||
                  DEFAULT_BLOCK_CNT != block_num ||
                  buf_len != DEFAULT_BLOCK_CNT * DEFAULT_BLOCK_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), KP(buf), K(buf_len), K(block_num));
  } else {
    start_ = buf;
    len_ = buf_len;

    for (int64_t i = 0; i < DEFAULT_BLOCK_CNT; ++i) {
      void *cur_block = (void *) ((uint64_t) buf + i * DEFAULT_BLOCK_SIZE);
      free_blocks_[i] = cur_block;
    }
    alloc_idx_ = 0;
    pending_idx_ = DEFAULT_BLOCK_CNT;
  }
  return ret;
}

int ObCompactionBufferChunk::alloc_block(ObCompactionBufferBlock &block)
{
  int ret = OB_SUCCESS;
  block.reset();

  if (!has_free_block()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("all blocks were used, need to expand", K(ret));
  } else if (OB_ISNULL(free_blocks_[alloc_idx_ % DEFAULT_BLOCK_CNT])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null free block", K(ret), KPC(this));
  } else if (OB_FAIL(block.set_fixed_block(
        start_, free_blocks_[alloc_idx_ % DEFAULT_BLOCK_CNT], DEFAULT_BLOCK_SIZE))) {
    LOG_WARN("failed to set fixed block", K(ret), KPC(this));
  } else {
    free_blocks_[alloc_idx_++ % DEFAULT_BLOCK_CNT] = nullptr;
  }
  return ret;
}

int ObCompactionBufferChunk::free_block(ObCompactionBufferBlock &block)
{
  int ret = OB_SUCCESS;

  if (block.empty()) {
    // do nothing
  } else if (block.get_header() != start_) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LOG_ERROR("[MEMORY LEAK] free block doesn't belog to current chunk", K(ret), K(block), KPC(this));
  } else if (NULL == block.get_buffer() || DEFAULT_BLOCK_SIZE != block.get_buffer_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected block", K(ret), K(block), KPC(this));
  } else if (OB_UNLIKELY(NULL != free_blocks_[pending_idx_ % DEFAULT_BLOCK_CNT])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[MEMORY LEAK] free_blocks must be null", K(ret), KPC(this));
  } else {
    free_blocks_[pending_idx_++ % DEFAULT_BLOCK_CNT] = block.get_buffer();
    block.reset();
  }
  return ret;
}


/* ***************************************** ObTenantCompactionMemPool ***************************************** */
int ObTenantCompactionMemPool::mtl_init(ObTenantCompactionMemPool* &mem_pool)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObMallocAllocator *malloc_allocator = nullptr;

  if (OB_FAIL(mem_pool->init())) {
    LOG_WARN("failed to init compaction memory pool", K(ret), K(tenant_id));
  } else if (OB_ISNULL(malloc_allocator = ObMallocAllocator::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null malloc allocator, cannnot reserve memory for mini compaction", K(ret), K(tenant_id));
  } else if (OB_FAIL(malloc_allocator->set_tenant_ctx_idle(tenant_id, ObCtxIds::MERGE_RESERVE_CTX_ID, RESERVE_MEM_SIZE, !MTL_IS_MINI_MODE()/*reserve*/))) {
    LOG_WARN("failed to reserve memory for mini compaction", K(ret));
  } else {
    mem_pool->reserve_mode_signal_ = 1;
    LOG_INFO("success to init ObTenantCompactionMemPool", K(tenant_id));
  }
  return ret;
}

ObTenantCompactionMemPool::ObTenantCompactionMemPool()
  : mem_shrink_task_(*this),
    chunk_allocator_("MrgMemPoolChk"),
    piece_allocator_("MrgMemPoolPce"),
    chunk_lock_(),
    piece_lock_(),
    chunk_list_(),
    max_block_num_(0),
    total_block_num_(0),
    used_block_num_(0),
    mem_mode_(NORMAL_MODE),
    tg_id_(0),
    reserve_mode_signal_(0),
    is_inited_(false)
{
}

ObTenantCompactionMemPool::~ObTenantCompactionMemPool()
{
  destroy();
}

void ObTenantCompactionMemPool::wait()
{
  TG_WAIT(tg_id_);
}

void ObTenantCompactionMemPool::stop()
{
  TG_STOP(tg_id_);
}

void ObTenantCompactionMemPool::destroy()
{
  if (IS_INIT) {
    reset();
  }
}

void ObTenantCompactionMemPool::reset()
{
  stop();
  wait();
  TG_DESTROY(tg_id_);
  tg_id_ = -1;
  {
    ObSpinLockGuard guard(chunk_lock_);

    ObCompactionBufferChunk *cur_chunk = nullptr;
    DLIST_REMOVE_ALL_NORET(cur_chunk, chunk_list_) {
      chunk_list_.remove(cur_chunk);
      cur_chunk->~ObCompactionBufferChunk();
      chunk_allocator_.free(cur_chunk);
      cur_chunk = nullptr;
    }

    chunk_list_.reset();
    chunk_allocator_.~DefaultPageAllocator();
    max_block_num_ = 0;
    total_block_num_ = 0;
    used_block_num_ = 0;
    reserve_mode_signal_ = 0;
    mem_mode_ = MemoryMode::NORMAL_MODE;
    is_inited_ = false;
  }

  {
    ObSpinLockGuard guard(piece_lock_);
    piece_allocator_.~DefaultPageAllocator();
  }
  FLOG_INFO("ObTenantCompactionMemPool destroyed!");
}

int ObTenantCompactionMemPool::init()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantCompactionMemPool has been inited", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MergeMemPool, tg_id_))) {
    LOG_WARN("failed to create MergeMemPool thread", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("failed to start stat MergeMemPool thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, mem_shrink_task_, CHECK_SHRINK_INTERVAL, repeat))) {
    LOG_WARN("failed to schedule tablet stat update task", K(ret));
  } else {
    chunk_allocator_.set_tenant_id(MTL_ID());
    piece_allocator_.set_tenant_id(MTL_ID());
    max_block_num_ = MTL_IS_MINI_MODE()
                   ? MINI_MODE_CHUNK_MEMORY_LIMIT / ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE
                   : CHUNK_MEMORY_LIMIT / ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE;
    total_block_num_ = 0;
    is_inited_ = true;
  }
  if (!is_inited_) {
    reset();
    COMMON_LOG(WARN, "failed to init ObTenantCompactionMemPool", K(ret));
  }
  return ret;
}

int ObTenantCompactionMemPool::alloc(const int64_t size, ObCompactionBufferBlock &buffer_block)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCompactionMemPool not inited", K(ret));
  } else if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(size));
  } else if (OB_UNLIKELY(!buffer_block.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("buffer block must be empty", K(ret), K(buffer_block));
  } else if (size != get_block_size()) {
    // should alloc mem by piece allocator
  } else if (OB_FAIL(alloc_chunk(buffer_block))) {
    if (OB_EXCEED_MEM_LIMIT != ret) {
      LOG_WARN("failed to alloc buffer block from chunk list", K(ret), K(size));
    } else {
      LOG_INFO("chunk list reached the upper limit, alloc mem from piece allocator", K(ret), K(size));
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCCESS == ret && buffer_block.empty()) {
    if (OB_FAIL(alloc_piece(size, buffer_block))) {
      LOG_WARN("failed to alloc buffer block from piece allocator", K(ret), K(size));
    }
  }
  return ret;
}

int ObTenantCompactionMemPool::alloc_chunk(ObCompactionBufferBlock &buffer_block)
{
  int ret = OB_SUCCESS;
  buffer_block.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCompactionMemPool not inited", K(ret));
  } else {
    ObSpinLockGuard guard(chunk_lock_);
    while (buffer_block.empty() && OB_SUCC(ret)) {
      ObCompactionBufferChunk *header = chunk_list_.get_header();
      ObCompactionBufferChunk *cur = header->get_next();

      while (NULL != cur && cur != header && OB_SUCC(ret) && buffer_block.empty()) {
        if (!cur->has_free_block()) {
          cur = cur->get_next();
        } else if (OB_FAIL(cur->alloc_block(buffer_block))) {
          LOG_WARN("failed to alloc free block", K(ret));
        }
      }

      if (OB_SUCC(ret) && buffer_block.empty()) {
        ret = expand();
      }
    } // end while

    if (OB_SUCC(ret)) {
      ++used_block_num_;
    } else {
      buffer_block.reset();
    }
  }
  return ret;
}

int ObTenantCompactionMemPool::alloc_piece(const int64_t size, ObCompactionBufferBlock &buffer_block)
{
  int ret = OB_SUCCESS;
  buffer_block.reset();

  if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(size));
  } else {
    ObSpinLockGuard guard(piece_lock_);
    void *buf = nullptr;
    if (OB_ISNULL(buf = piece_allocator_.alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc mem", K(ret), K(size));
    } else if (OB_FAIL(buffer_block.set_piece_block(buf, size, ObCompactionBufferBlock::PIECE_TYPE))) {
      LOG_WARN("failed to set piece block", K(ret), K(size), K(buf));
    }

    if (OB_FAIL(ret) && NULL != buf) {
      piece_allocator_.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

void ObTenantCompactionMemPool::free(ObCompactionBufferBlock &buffer_block)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCompactionMemPool not inited", K(ret));
  } else if (OB_UNLIKELY(buffer_block.empty())) {
    // do nothing
  } else if (ObCompactionBufferBlock::PIECE_TYPE == buffer_block.get_type()) {
    ObSpinLockGuard guard(piece_lock_);
    piece_allocator_.free(buffer_block.get_buffer());
    buffer_block.reset();
  } else if (ObCompactionBufferBlock::CHUNK_TYPE == buffer_block.get_type()) {
    ObSpinLockGuard guard(chunk_lock_);

    ObCompactionBufferChunk *header = chunk_list_.get_header();
    ObCompactionBufferChunk *cur = header->get_next();
    bool is_contained = false;
    while (NULL != cur && cur != header && OB_SUCC(ret) && !is_contained) {
      if (cur->start_ != buffer_block.get_header()) {
        cur = cur->get_next();
      } else if (FALSE_IT(is_contained = true)) {
      } else if (OB_FAIL(cur->free_block(buffer_block))) {
        LOG_ERROR("[MEMORY LEAK] failed to free block ptr", K(ret), K(buffer_block));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!is_contained)) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_ERROR("[MEMORY LEAK] free block doesn't belong to the mem pool", K(ret), K(buffer_block));
    } else {
      --used_block_num_;
      buffer_block.reset();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[MEMORY LEAK] get unexpected block type", K(ret), K(buffer_block));
    ob_abort(); // tmp code, remove later
  }
}

// should hold lock before calling this func.
int ObTenantCompactionMemPool::expand()
{
  int ret = OB_SUCCESS;
  const int64_t expand_block_num = ObCompactionBufferChunk::DEFAULT_BLOCK_CNT;
  const int64_t chunk_size = expand_block_num * ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE;

  char *buf = nullptr;
  const uint64_t buf_len = sizeof(ObCompactionBufferChunk) + chunk_size;
  ObCompactionBufferChunk *new_chunk = nullptr;
  void *chunk_start_buf = nullptr;
  if (max_block_num_ == total_block_num_) {
    ret = OB_EXCEED_MEM_LIMIT;
    LOG_WARN("reach maximum block num", K(ret), K_(total_block_num), K_(max_block_num));
  } else if (OB_ISNULL(buf = static_cast<char *>(chunk_allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buffer for new chunk", K(ret));
  } else {
    MEMSET(buf, 0, buf_len);
    chunk_start_buf = buf + sizeof(ObCompactionBufferChunk);
    new_chunk = new (buf) ObCompactionBufferChunk();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_chunk->init(chunk_start_buf, chunk_size, expand_block_num))) {
    LOG_WARN("failed to init new chunk", K(ret), K(buf), K(chunk_size), K(expand_block_num));
  } else if (OB_UNLIKELY(!chunk_list_.add_last(new_chunk))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add new chunk", K(ret), K(new_chunk));
  } else {
    total_block_num_ += expand_block_num;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    chunk_allocator_.free(buf);
  }
  return ret;
}

int ObTenantCompactionMemPool::try_shrink()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(chunk_lock_);
  // not reserve mem in mini mode
  if (!MTL_IS_MINI_MODE() && max_block_num_ > total_block_num_) {
    // do nothing
  } else if (used_block_num_ <= total_block_num_ / 2) {
    // Less than half of blocks were used, need shrink
    const int64_t max_gc_chunk_cnt = chunk_list_.get_size() / 2;
    int64_t gc_chunk_cnt = 0;

    ObCompactionBufferChunk *header = chunk_list_.get_header();
    ObCompactionBufferChunk *cur = header->get_next();
    while (NULL != cur && cur != header && gc_chunk_cnt < max_gc_chunk_cnt && OB_SUCC(ret)) {
      ObCompactionBufferChunk *gc_chunk = cur;
      cur = cur->get_next();
      if (!gc_chunk->is_free_chunk()) {
      } else if (OB_ISNULL(chunk_list_.remove(gc_chunk))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("failed to remove gc_chunk", K(ret), KPC(gc_chunk));
      } else {
        total_block_num_ -= ObCompactionBufferChunk::DEFAULT_BLOCK_CNT;
        chunk_allocator_.free(gc_chunk);
        gc_chunk = nullptr;
        ++gc_chunk_cnt;
      }
    }
  }
  LOG_INFO("Compaction Mem Pool current stat: ", K(ret), K_(max_block_num), K_(total_block_num),
           "Chunk list size: ", chunk_list_.get_size());
  return ret;
}

// shrink the mem pool
void ObTenantCompactionMemPool::MemPoolShrinkTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t compaction_dag_cnt = 0;

  if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->get_compaction_dag_count(compaction_dag_cnt))) {
    LOG_WARN("failed to get compaction dag count", K(ret));
  } else if (0 == compaction_dag_cnt && 0 == last_check_dag_cnt_) {
    if (OB_FAIL(mem_pool_.try_shrink())) {
      LOG_WARN("failed to try shrink", K(ret));
    }
  } else {
    // exist compaction task, just update last check dag cnt
    last_check_dag_cnt_ = compaction_dag_cnt;
  }
}

bool ObTenantCompactionMemPool::acquire_reserve_mem()
{
  bool bret = false;
  if (!MTL_IS_MINI_MODE()) {
    bret = ATOMIC_BCAS(&reserve_mode_signal_, 1, 0);
  }
  return bret;
}

bool ObTenantCompactionMemPool::release_reserve_mem()
{
  bool bret = false;
  bret = ATOMIC_BCAS(&reserve_mode_signal_, 0, 1);
  return bret;
}


/* ********************************************** ObCompactionBufferWriter ********************************************** */
ObCompactionBufferWriter::ObCompactionBufferWriter(
    const char *label,
    const int64_t size,
    const bool use_mem_pool)
  : ObBufferWriter(NULL, 0, 0),
    label_(label),
    use_mem_pool_(use_mem_pool),
    ref_mem_ctx_(nullptr),
    block_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ensure_space(size))) {
    LOG_WARN("cannot allocate memory for data buffer.", K(size), K(ret));
  }
}

ObCompactionBufferWriter::~ObCompactionBufferWriter()
{
  reset();
}

void ObCompactionBufferWriter::reset()
{
  free_block();
  if (NULL != ref_mem_ctx_) {
    ref_mem_ctx_->inc_buffer_free_mem(capacity_);
    ref_mem_ctx_ = nullptr;
  }

  block_.reset();
  data_ = nullptr;
  pos_ = 0;
  capacity_ = 0;
}

int ObCompactionBufferWriter::ensure_space(int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t old_capacity = capacity_;

  if (size <= 0) {
    // do nothing
  } else if (nullptr == data_) { // first alloc
    if (OB_UNLIKELY(!block_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("block is unexpected not empty", K(ret), K(block_)); // lower log level later
    } else if (OB_FAIL(alloc_block(size, block_))) {
      LOG_WARN("failed to alloc block", K(ret), K(size));
    } else {
      data_ = (char *) block_.get_buffer();
      pos_ = 0;
      capacity_ = size;
    }
  } else if (capacity_ < size) {
    if (OB_FAIL(resize(size))) {
      LOG_WARN("failed to resize buffer writer", K(ret), K(size));
    } else {
      LOG_INFO("success to resize buffer writer", K(ret), K(size));
    }
  }

  if (OB_SUCC(ret) && old_capacity != capacity_) {
    ref_mem_ctx_ = NULL == ref_mem_ctx_
                  ? CURRENT_MEM_CTX()
                  : ref_mem_ctx_;

    if (NULL != ref_mem_ctx_) {
      ref_mem_ctx_->inc_buffer_hold_mem(capacity_ - old_capacity);
    } else {
      LOG_TRACE("no mem ctx has setted to thread", K(ret), K(label_), K(size), K(capacity_), K(old_capacity));
    }
  }
  return ret;
}

int ObCompactionBufferWriter::resize(const int64_t size)
{
  int ret = OB_SUCCESS;
  ObCompactionBufferBlock new_block;
  char *new_data = nullptr;

  if (OB_UNLIKELY(size <= 0 || block_.get_buffer_size() == size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(size), K(block_));
  } else if (OB_FAIL(alloc_block(size, new_block))) {
    LOG_WARN("failed to alloc block", K(ret), K(size));
  } else {
    new_data = (char *) new_block.get_buffer();
    MEMCPY(new_data, data_, pos_);
    data_ = new_data;
    capacity_ = size;

    free_block();
    block_.move(new_block);
  }
  return ret;
}

int ObCompactionBufferWriter::alloc_block(
    const int64_t size,
    ObCompactionBufferBlock &block)
{
  int ret = OB_SUCCESS;

  if (use_mem_pool_ && OB_FAIL(MTL(ObTenantCompactionMemPool *)->alloc(size, block))) {
    LOG_WARN("failed to alloc mem for new block", K(ret), K(size));
  } else if (!use_mem_pool_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = mtl_malloc(size, label_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc mem", K(ret), K(size));
    } else if (OB_FAIL(block.set_piece_block(buf, size, ObCompactionBufferBlock::MTL_PIECE_TYPE))) {
      LOG_WARN("failed to set piece block", K(ret), K(size), K(buf));
    }
  }

  return ret;
}

void ObCompactionBufferWriter::free_block()
{
  if (block_.empty()) {
    // do nothing
  } else if (OB_UNLIKELY((!use_mem_pool_ && ObCompactionBufferBlock::MTL_PIECE_TYPE != block_.get_type())
      || (use_mem_pool_ && ObCompactionBufferBlock::MTL_PIECE_TYPE == block_.get_type()))) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "[MEMORY LEAK] get unexpected block", K(use_mem_pool_), K(block_));
    ob_abort(); // tmp code, remove later
  } else if (!use_mem_pool_) {
    mtl_free(block_.get_buffer());
    block_.reset();
  } else {
    ObTenantCompactionMemPool * mem_pool = MTL(ObTenantCompactionMemPool *);
    if (OB_NOT_NULL(mem_pool)) {
      mem_pool->free(block_);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "[MEMORY LEAK] get unexpected NULL mem pool", K(mem_pool), K(block_));
      ob_abort(); // tmp code, remove later
    }
  }
}
