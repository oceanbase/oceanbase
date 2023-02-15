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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_batch_buffer_task.h"

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;

IObLogBufTask::IObLogBufTask()
    : next_(NULL), need_callback_(false)
{}

void IObLogBufTask::reset()
{
  next_ = NULL;
  need_callback_ = false;
}

bool DummyBufferTask::is_valid() const
{
  return true;
}

int64_t DummyBufferTask::get_data_len() const
{
  LOG_ERROR_RET(OB_ERR_UNEXPECTED, "the function should not be called");
  return 0;
}

int64_t DummyBufferTask::get_entry_cnt() const
{
  LOG_ERROR_RET(OB_ERR_UNEXPECTED, "the function should not be called");
  return 0;
}

int DummyBufferTask::fill_buffer(char *buf, const offset_t offset)
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(offset);
  LOG_ERROR("the function should not be called");
  return ret;
}

int DummyBufferTask::st_after_consume(const int handle_err)
{
  int ret = OB_SUCCESS;
  UNUSED(handle_err);
  LOG_ERROR("the function should not be called");
  return ret;
}

IObLogBatchBufTask::IObLogBatchBufTask(BlockType block_type) :
    block_type_(block_type),
    batch_buf_(NULL),
    batch_size_(0),
    subtask_count_(0),
    head_(),
    task_list_tail_(&head_)
{
}

IObLogBatchBufTask::~IObLogBatchBufTask()
{
  reset();
}

void IObLogBatchBufTask::reset()
{
  // Note: do not reset block_type
  batch_buf_ = NULL;
  batch_size_ = 0;
  subtask_count_ = 0;
  task_list_tail_ = &head_;
  head_.next_ = NULL;
}

IObLogBatchBufTask &IObLogBatchBufTask::set_batch_buffer(char *buf, const int64_t len)
{
  batch_buf_ = buf;
  batch_size_ = len;

  return *this;
}

void IObLogBatchBufTask::add_callback_to_list(IObLogBufTask *task)
{
  (void)ATOMIC_FAA(&subtask_count_, 1);
  if (NULL != task && task->need_callback_) {
    task->next_ = NULL;
    while (true) {
      IObLogBufTask *prev_tail = task_list_tail_;
      if (ATOMIC_BCAS(&task_list_tail_, prev_tail, task)) {
        prev_tail->next_ = task;
        break;
      }
      PAUSE();
    }
  }
}

int IObLogBatchBufTask::st_handle_callback_list(const int handle_err, int64_t &task_num)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  task_num = 0;
  IObLogBufTask *curr_task = head_.next_;
  IObLogBufTask *next_task = NULL;

  while (NULL != curr_task) {
    next_task = curr_task->next_;
    if (OB_SUCCESS != (tmp_ret = curr_task->st_after_consume(handle_err))) {
      LOG_ERROR("st_after_consume failed", K(tmp_ret), K(handle_err));
    }
    curr_task = next_task;
    task_num++;
  }

  return ret;
}

}
}
