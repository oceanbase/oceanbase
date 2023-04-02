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
 * Block: aggregate small log
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_block.h"

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;

Block::Block(int64_t seq, char *buf, int64_t block_count)
    : IObLogBatchBufTask(IObLogBatchBufTask::NORMAL_BLOCK),
    seq_(seq),
    status_seq_(seq),
    ref_(0),
    buf_(buf),
    block_count_(block_count)
{
}

int Block::init()
{
  int ret = OB_SUCCESS;
  return ret;
}

void Block::after_consume()
{
  // do nothing
}

void Block::reuse()
{
  IObLogBatchBufTask::reset();
  wait_submitted(seq_);
  ATOMIC_STORE(&seq_, seq_ + block_count_);
}

int64_t Block::ref(const int64_t delta)
{
  return ATOMIC_AAF(&ref_, delta);
}

int64_t Block::wait(const int64_t seq)
{
  int64_t real_seq = 0;
  while ((real_seq = ATOMIC_LOAD(&seq_)) < seq) {
    ob_usleep(200);
  }
  return real_seq;
}

void Block::fill(const offset_t offset, IObLogBufTask *task)
{
  int tmp_ret = OB_SUCCESS;

  if (NULL == task || offset < 0) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LOG_ERROR_RET(tmp_ret, "invalid argument", "ret", tmp_ret, KP(task), K(offset));
  } else if (OB_SUCCESS != (tmp_ret = task->fill_buffer(buf_, offset))) {
    LOG_ERROR_RET(tmp_ret, "fill_buffer fail", "ret", tmp_ret, KP_(buf), K(offset));
  } else {
    IObLogBatchBufTask::add_callback_to_list(task);
  }
}

void Block::freeze(const offset_t offset)
{
  IObLogBatchBufTask::set_batch_buffer(buf_, offset);
}

void Block::set_submitted()
{
  ATOMIC_STORE(&status_seq_, ATOMIC_LOAD(&seq_) + 1);
}

void Block::wait_submitted(const int64_t seq)
{
  int64_t status_seq = 0;
  while ((status_seq = ATOMIC_LOAD(&status_seq_)) < seq + 1) {
    ob_usleep(200);
  }
  return;
}

BigBlock::BigBlock()
    : IObLogBatchBufTask(IObLogBatchBufTask::BIG_BLOCK),
    buf_(NULL)
{
}

int BigBlock::init(const int64_t buf_size)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_ = static_cast<char *>(ob_malloc(buf_size, "CDCBigBlock")))) {
    LOG_ERROR("allocate memory for mutator row data fail", K(buf_size));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    IObLogBatchBufTask::set_batch_buffer(buf_, buf_size);
  }

  return ret;
}

void BigBlock::after_consume()
{
  // do nothing
}

void BigBlock::reuse()
{
  if (NULL != buf_) {
    ob_free(buf_);
    buf_ = NULL;
  }

  IObLogBatchBufTask::reset();
}

void BigBlock::fill(const offset_t offset, IObLogBufTask *task)
{
  int tmp_ret = OB_SUCCESS;

  if (NULL == task || offset < 0) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LOG_ERROR_RET(tmp_ret, "invalid argument", "ret", tmp_ret, KP(task), K(offset));
  } else if (OB_SUCCESS != (tmp_ret = task->fill_buffer(buf_, offset))) {
    LOG_ERROR_RET(tmp_ret, "fill_buffer fail", "ret", tmp_ret, KP_(buf), K(offset));
  } else {
    IObLogBatchBufTask::add_callback_to_list(task);
  }
}

void BigBlock::destroy()
{
  buf_ = NULL;
}

}
}
