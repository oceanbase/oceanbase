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
 *
 * implement ObLogBatchBuffer from ObBatchBuffer
 * 1. aggregate small buffer, then submit to BufferConsumer,
 *    after buffer consumed, handle callback of each small buffer
 * 2. Support big buffer for big row
 * 3. API is more simple
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_batch_buffer.h"
#include "lib/atomic/atomic128.h"
#include "lib/atomic/ob_atomic.h"  // inc_update
#include "ob_log_factory.h"        // BigBlock

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;

int ObLogBatchBuffer::IncPos::try_freeze(IncPos &cur_pos, const int64_t seq)
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

int ObLogBatchBuffer::IncPos::append(IncPos &cur_pos,
                                  const int64_t len,
                                  const int64_t entry_cnt,
                                  const int64_t limit)
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

ObLogBatchBuffer::IncPos &ObLogBatchBuffer::IncPos::next_block()
{
  seq_ ++;
  offset_ = 0;
  entry_cnt_ = 0;
  return *this;
}

ObLogBatchBuffer::ObLogBatchBuffer():
    is_inited_(false),
    handler_(NULL),
    block_array_(NULL),
    block_count_(OB_INVALID_COUNT),
    block_size_(OB_INVALID_SIZE),
    next_flush_block_id_(OB_INVALID_ID),
    next_pos_(),
    auto_freeze_(true)
{}

ObLogBatchBuffer::~ObLogBatchBuffer()
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

int ObLogBatchBuffer::init(const int64_t buffer_size,
    const int64_t buffer_count,
    IObBatchBufferConsumer *handler,
    bool auto_freeze)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (NULL == handler) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (block_array_ = (Block *)ob_malloc_align(16, buffer_count * sizeof(Block), "CDCBlockArray"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    block_count_ = buffer_count;
    block_size_ = buffer_size;
    handler_ = handler;
    next_flush_block_id_ = 0;

    for (int64_t i = 0; i < block_count_; i++) {
      char *alloc_buf = static_cast<char *>(ob_malloc(buffer_size, "CDCBufBlock"));
      if (OB_ISNULL(alloc_buf)) {
        LOG_ERROR("alloc_buf is NULL");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        new(block_array_ + i)Block(i, alloc_buf, block_count_);
        LOG_INFO("Construct block succ", K(i), "block", *(block_array_ + i));
      }
    }
    auto_freeze_ = auto_freeze;
    is_inited_ = true;

    LOG_INFO("ObLogBatchBuffer init succ", K(buffer_size), K(buffer_count), K(auto_freeze));
  }

  return ret;
}

int ObLogBatchBuffer::submit(IObLogBufTask *task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  IncPos cur_pos;

  if (! is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task) || OB_UNLIKELY(! task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR( "invalid arguments", KR(ret), K(task));
  } else {
    int64_t data_len = task->get_data_len();
    int64_t entry_cnt = task->get_entry_cnt();

    // Support big row
    if (data_len >= block_size_) {
      BigBlock *big_block = NULL;

      if (OB_ISNULL(big_block = BigBlockFactory::alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("BigBlock is NULL", KR(ret));
      } else {
        if (OB_FAIL(big_block->init(data_len))) {
          LOG_ERROR("BigBlock init fail", KR(ret));
        } else {
          big_block->fill(0, task);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(handler_->submit(big_block))) {
          LOG_ERROR( "handle submit fail", KR(ret));
        }
      }
    } else {
      if (OB_BLOCK_SWITCHED == (ret = next_pos_.append(cur_pos, data_len, entry_cnt, block_size_))) {
        ret = fill_buffer_(cur_pos, NULL);
        cur_pos.next_block();
      }
      if (OB_SUCC(ret)) {
        ret = fill_buffer_(cur_pos, task);
      }

      LOG_DEBUG("IncPos 1", K(next_pos_));
      if (OB_SUCC(ret)) {
        if (auto_freeze_
            && OB_SUCCESS != (tmp_ret = try_freeze(next_flush_block_id_))) {
          if (OB_IN_STOP_STATE != tmp_ret) {
            LOG_ERROR( "try_freeze failed", K(tmp_ret), K_(next_flush_block_id));
          }
        }
      }
      LOG_DEBUG("IncPos 2", K(next_pos_));
    }
  }

  return ret;
}

void ObLogBatchBuffer::update_next_flush_block_id(const int64_t block_id)
{
  inc_update(&next_flush_block_id_, block_id);
}

bool ObLogBatchBuffer::is_all_consumed() const
{
  return next_flush_block_id_ == next_pos_.seq_ && 0 == next_pos_.offset_;
}

int ObLogBatchBuffer::try_freeze(const int64_t block_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (block_id < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    IncPos cur_pos;
    if (OB_BLOCK_SWITCHED == (ret = next_pos_.try_freeze(cur_pos, block_id))) {
      if (OB_FAIL(fill_buffer_(cur_pos, NULL))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR( "fill_buffer_ fail", K(ret));
        }
      }
    } else if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObLogBatchBuffer::fill_buffer_(const IncPos cur_pos, IObLogBufTask *task)
{
  int ret = OB_SUCCESS;
  Block *block = NULL;

  if (! is_inited_ || OB_ISNULL(handler_) || OB_ISNULL(block_array_)) {
    ret = OB_NOT_INIT;
  } else if (NULL == (block = get_block_(cur_pos.seq_))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t ref_cnt = 0;
    // TODO Stat
    block->wait(cur_pos.seq_);
    if (NULL != task) {
      block->fill(cur_pos.offset_, task);
      ref_cnt = block->ref(task->get_data_len());
    } else {
      block->freeze(cur_pos.offset_);
      ref_cnt = block->ref(-cur_pos.offset_);
    }
    if (0 == ref_cnt) {
      if (OB_FAIL(wait_block_(cur_pos.seq_ - 1))) {
        LOG_ERROR( "wait_block_ failed", K(ret), "block_id", cur_pos.seq_ - 1);
      } else if (OB_FAIL(handler_->submit(block))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR( "handle submit fail", K(ret));
        }
      }
      block->set_submitted();
    }
  }

  return ret;
}

int ObLogBatchBuffer::wait_block_(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  Block *block = NULL;

  if (!is_inited_ || NULL == block_array_) {
    ret = OB_NOT_INIT;
  } else if (-1 == block_id) {
    ret = OB_SUCCESS;
  } else if (NULL == (block = get_block_(block_id))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    block->wait_submitted(block_id);
  }

  return ret;
}

Block *ObLogBatchBuffer::get_block_(const int64_t block_id)
{
  int tmp_ret = OB_SUCCESS;
  Block *ptr_ret = NULL;

  if (!is_inited_ || NULL == block_array_ || 0 == block_count_ || OB_INVALID_COUNT == block_count_) {
    tmp_ret = OB_NOT_INIT;
    LOG_ERROR_RET(tmp_ret, "not init", K(tmp_ret), K_(block_array), K_(block_count));
  } else {
    ptr_ret =  block_array_ + (block_id % block_count_);
  }

  return ptr_ret;
}

}
}
