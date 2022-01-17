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

#include "ob_batch_buffer.h"

#include "lib/allocator/ob_malloc.h"
#include "lib/atomic/atomic128.h"
#include "ob_disk_log_buffer.h"

namespace oceanbase {
using namespace common;
namespace clog {
int ObBatchBuffer::IncPos::try_freeze(IncPos& cur_pos, const int64_t seq)
{
  int ret = OB_SUCCESS;
  IncPos next_pos;
  LOAD128(cur_pos, this);
  do {
    if (!cur_pos.can_freeze(seq)) {
      ret = OB_EAGAIN;
      break;
    } else {
      next_pos.seq_ = cur_pos.seq_ + 1;
      next_pos.offset_ = 0;
      next_pos.entry_cnt_ = 0;
      ret = OB_BLOCK_SWITCHED;
    }
  } while (!CAS128(this, cur_pos, next_pos));
  return ret;
}

int ObBatchBuffer::IncPos::append(IncPos& cur_pos, const int64_t len, const int64_t entry_cnt, const int64_t limit)
{
  int ret = OB_SUCCESS;
  IncPos next_pos;
  LOAD128(cur_pos, this);
  do {
    const int32_t tmp_entry_cnt = static_cast<int32_t>(entry_cnt);
    if (cur_pos.offset_ + len <= limit && cur_pos.entry_cnt_ + tmp_entry_cnt <= MAX_ENTRY_CNT) {
      next_pos.seq_ = cur_pos.seq_;
      next_pos.offset_ = cur_pos.offset_ + static_cast<offset_t>(len);
      next_pos.entry_cnt_ = cur_pos.entry_cnt_ + tmp_entry_cnt;
      ret = OB_SUCCESS;
    } else {
      next_pos.seq_ = cur_pos.seq_ + 1;
      next_pos.offset_ = static_cast<offset_t>(len);
      next_pos.entry_cnt_ = tmp_entry_cnt;
      ret = OB_BLOCK_SWITCHED;
    }
  } while (!CAS128(this, cur_pos, next_pos));
  return ret;
}

ObBatchBuffer::IncPos& ObBatchBuffer::IncPos::next_block()
{
  seq_++;
  offset_ = 0;
  entry_cnt_ = 0;
  return *this;
}

class ObBatchBuffer::Block : public ObIBatchBufferTask {
public:
  Block(ObBatchBuffer& host, int64_t seq, char* buf, int64_t block_count, bool auto_freeze)
      : host_(host),
        seq_(seq),
        status_seq_(seq),
        ref_(0),
        buf_(buf),
        block_count_(block_count),
        auto_freeze_(auto_freeze)
  {}
  virtual ~Block()
  {}
  int init(ObLogWriterWrapper* handler);
  ObICLogItem* get_flush_task();
  int64_t ref(const int64_t delta);
  int64_t wait(const int64_t seq);
  void fill(const offset_t offset, ObIBufferTask* task);
  void freeze(const offset_t offset);
  void set_submitted();
  void wait_submitted(const int64_t seq);
  void after_consume();
  int64_t get_seq() const
  {
    return seq_;
  }
  void reuse();

private:
  ObBatchBuffer& host_;
  int64_t seq_;
  int64_t status_seq_;
  int64_t ref_;
  char* buf_;
  int64_t block_count_;
  ObCLogItem flush_task_;
  bool auto_freeze_;
};

int ObBatchBuffer::Block::init(ObLogWriterWrapper* handler)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_task_.init(handler, this, &host_))) {
    CLOG_LOG(WARN, "flush_task_.init failed", K(ret));
  }
  return ret;
}

ObICLogItem* ObBatchBuffer::Block::get_flush_task()
{
  return &flush_task_;
}
void ObBatchBuffer::Block::reuse()
{
  ObIBatchBufferTask::reset();
  wait_submitted(seq_);
  ATOMIC_STORE(&seq_, seq_ + block_count_);
}

void ObBatchBuffer::Block::after_consume()
{
  int tmp_ret = OB_SUCCESS;
  reuse();
  int64_t next_flush_block_id = seq_ - block_count_ + 1;
  host_.update_next_flush_block_id(next_flush_block_id);
  if (auto_freeze_ && OB_SUCCESS != (tmp_ret = host_.try_freeze(next_flush_block_id))) {
    CLOG_LOG(ERROR, "try_freeze failed", K(tmp_ret));
  }
}

int64_t ObBatchBuffer::Block::ref(const int64_t delta)
{
  return ATOMIC_AAF(&ref_, delta);
}

int64_t ObBatchBuffer::Block::wait(const int64_t seq)
{
  int64_t real_seq = 0;
  while ((real_seq = ATOMIC_LOAD(&seq_)) < seq) {
    usleep(200);
  }
  return real_seq;
}

void ObBatchBuffer::Block::fill(const offset_t offset, ObIBufferTask* task)
{
  int tmp_ret = OB_SUCCESS;
  if (NULL == task || offset < 0) {
    tmp_ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", "ret", tmp_ret, KP(task), K(offset));
  } else if (OB_SUCCESS != (tmp_ret = task->fill_buffer(buf_, offset))) {
    CLOG_LOG(WARN, "fill_buffer fail", "ret", tmp_ret, KP_(buf), K(offset));
  } else {
    add_callback_to_list(task);
  }
}

void ObBatchBuffer::Block::freeze(const offset_t offset)
{
  set_batch_buffer(buf_, offset);
}

void ObBatchBuffer::Block::set_submitted()
{
  ATOMIC_STORE(&status_seq_, ATOMIC_LOAD(&seq_) + 1);
}

void ObBatchBuffer::Block::wait_submitted(const int64_t seq)
{
  int64_t status_seq = 0;
  while ((status_seq = ATOMIC_LOAD(&status_seq_)) < seq + 1) {
    usleep(200);
  }
  return;
}

ObBatchBuffer::ObBatchBuffer()
    : is_inited_(false),
      handler_(NULL),
      block_array_(NULL),
      block_count_(OB_INVALID_COUNT),
      block_size_(OB_INVALID_SIZE),
      next_flush_block_id_(OB_INVALID_ID),
      next_pos_(),
      auto_freeze_(true)
{}

ObBatchBuffer::~ObBatchBuffer()
{
  if (NULL != block_array_) {
    for (int64_t i = 0; i < block_count_; i++) {
      block_array_[i].~Block();
    }
    ob_free_align(block_array_);
    block_array_ = NULL;
  }
  handler_ = NULL;
  block_count_ = OB_INVALID_COUNT;
  block_size_ = OB_INVALID_SIZE;
  next_flush_block_id_ = OB_INVALID_ID;
  is_inited_ = false;
}

int ObBatchBuffer::init(ObIBufferArena* buffer_pool, ObIBatchBufferConsumer* handler, bool auto_freeze)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (NULL == buffer_pool || NULL == handler) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (block_array_ = (Block*)ob_malloc_align(
                          16, buffer_pool->get_buffer_count() * sizeof(Block), ObModIds::OB_CLOG_MGR))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    block_count_ = buffer_pool->get_buffer_count();
    block_size_ = buffer_pool->get_buffer_size();
    handler_ = handler;
    next_flush_block_id_ = 0;
    for (int64_t i = 0; i < block_count_; i++) {
      new (block_array_ + i) Block(*this, i, buffer_pool->alloc(), block_count_, auto_freeze);
    }
    auto_freeze_ = auto_freeze;
    is_inited_ = true;
  }
  return ret;
}

int ObBatchBuffer::submit(ObIBufferTask* task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  IncPos cur_pos;
  int64_t data_len = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (NULL == task || (data_len = task->get_data_len()) <= 0 || data_len >= block_size_) {
    ret = OB_INVALID_ARGUMENT;
    if (data_len >= block_size_) {
      CLOG_LOG(ERROR, "invalid arguments", K(ret), K(data_len), K(block_size_));
    } else {
      ret = OB_INVALID_BATCH_SIZE;
      CLOG_LOG(WARN, "invalid arguments", K(ret), K(data_len), K(block_size_));
    }
  } else {
    int64_t entry_cnt = task->get_entry_cnt();
    if (OB_BLOCK_SWITCHED == (ret = next_pos_.append(cur_pos, data_len, entry_cnt, block_size_))) {
      ret = fill_buffer(cur_pos, NULL);
      cur_pos.next_block();
    }
    if (OB_SUCC(ret)) {
      ret = fill_buffer(cur_pos, task);
    }
  }
  if (OB_SUCC(ret)) {
    if (auto_freeze_ && OB_SUCCESS != (tmp_ret = try_freeze(next_flush_block_id_))) {
      CLOG_LOG(ERROR, "try_freeze failed", K(tmp_ret), K_(next_flush_block_id));
    }
  }
  return ret;
}

void ObBatchBuffer::update_next_flush_block_id(int64_t block_id)
{
  inc_update(next_flush_block_id_, block_id);
}

bool ObBatchBuffer::is_all_consumed() const
{
  return next_flush_block_id_ == next_pos_.seq_ && 0 == next_pos_.offset_;
}

int ObBatchBuffer::try_freeze_next_block()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID != next_flush_block_id_) {
    ret = try_freeze(next_flush_block_id_);
  }
  return ret;
}

int ObBatchBuffer::try_freeze(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (block_id < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    IncPos cur_pos;
    if (OB_BLOCK_SWITCHED == (ret = next_pos_.try_freeze(cur_pos, block_id))) {
      if (OB_FAIL(fill_buffer(cur_pos, NULL))) {
        CLOG_LOG(WARN, "fill_buffer fail", K(ret));
      }
    } else if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObBatchBuffer::fill_buffer(const IncPos cur_pos, ObIBufferTask* task)
{
  int ret = OB_SUCCESS;
  Block* block = NULL;
  if (!is_inited_ || NULL == handler_ || NULL == block_array_) {
    ret = OB_NOT_INIT;
  } else if (NULL == (block = get_block(cur_pos.seq_))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t ref_cnt = 0;
    ObLogTimerUtility timer;
    timer.start_timer();
    block->wait(cur_pos.seq_);
    timer.finish_timer(__FILE__, __LINE__, CLOG_PERF_WARN_THRESHOLD);
    if (NULL != task) {
      block->fill(cur_pos.offset_, task);
      ref_cnt = block->ref(task->get_data_len());
    } else {
      block->freeze(cur_pos.offset_);
      ref_cnt = block->ref(-cur_pos.offset_);
    }
    timer.finish_timer(__FILE__, __LINE__, CLOG_PERF_WARN_THRESHOLD);
    if (0 == ref_cnt) {
      if (OB_FAIL(wait_block(cur_pos.seq_ - 1))) {
        CLOG_LOG(ERROR, "wait_block failed", K(ret), "block_id", cur_pos.seq_ - 1);
      } else if (OB_FAIL(handler_->submit(block))) {
        CLOG_LOG(WARN, "handle submit fail", K(ret));
      }
      block->set_submitted();
    }
    timer.finish_timer(__FILE__, __LINE__, CLOG_PERF_WARN_THRESHOLD);
  }
  return ret;
}

int ObBatchBuffer::wait_block(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  Block* block = NULL;
  if (!is_inited_ || NULL == block_array_) {
    ret = OB_NOT_INIT;
  } else if (-1 == block_id) {
    ret = OB_SUCCESS;
  } else if (NULL == (block = get_block(block_id))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    block->wait_submitted(block_id);
  }
  return ret;
}

ObBatchBuffer::Block* ObBatchBuffer::get_block(const int64_t block_id)
{
  int tmp_ret = OB_SUCCESS;
  ObBatchBuffer::Block* ptr_ret = NULL;
  if (!is_inited_ || NULL == block_array_ || 0 == block_count_ || OB_INVALID_COUNT == block_count_) {
    tmp_ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(tmp_ret), K_(block_array), K_(block_count));
  } else {
    ptr_ret = block_array_ + (block_id % block_count_);
  }
  return ptr_ret;
}
};  // end namespace clog
};  // end namespace oceanbase
