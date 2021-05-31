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

#include "ob_buffer_task.h"

namespace oceanbase {
using namespace common;
namespace clog {
int64_t DummyBuffferTask::get_data_len() const
{
  CLOG_LOG(ERROR, "the function should not be called");
  return 0;
}

int64_t DummyBuffferTask::get_entry_cnt() const
{
  CLOG_LOG(ERROR, "the function should not be called");
  return 0;
}

int DummyBuffferTask::fill_buffer(char* buf, const offset_t offset)
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(offset);
  CLOG_LOG(ERROR, "the function should not be called");
  return ret;
}

int DummyBuffferTask::st_after_consume(const int handle_err)
{
  int ret = OB_SUCCESS;
  UNUSED(handle_err);
  CLOG_LOG(ERROR, "the function should not be called");
  return ret;
}

int DummyBuffferTask::after_consume(const int handle_err, const void* arg, const int64_t before_push_cb_ts)
{
  int ret = OB_SUCCESS;
  UNUSED(handle_err);
  UNUSED(arg);
  UNUSED(before_push_cb_ts);
  CLOG_LOG(ERROR, "the function should not be called");
  return ret;
}

// BatchBuffer will submit a batch to BufferConsumer, which will construct a header and submit to disk/net.
void ObIBatchBufferTask::reset()
{
  batch_buf_ = NULL;
  batch_size_ = 0;
  subtask_count_ = 0;
  task_list_tail_ = &head_;
  head_.next_ = NULL;
  alloc_.reset();
}

ObIBatchBufferTask& ObIBatchBufferTask::set_batch_buffer(char* buf, const int64_t len)
{
  batch_buf_ = buf;
  batch_size_ = len;
  return *this;
}

void ObIBatchBufferTask::add_callback_to_list(ObIBufferTask* task)
{
  (void)ATOMIC_FAA(&subtask_count_, 1);
  if (NULL != task && task->need_callback_) {
    task->next_ = NULL;
    while (true) {
      ObIBufferTask* prev_tail = task_list_tail_;
      if (ATOMIC_BCAS(&task_list_tail_, prev_tail, task)) {
        prev_tail->next_ = task;
        break;
      }
      PAUSE();
    }
  }
}

int ObIBatchBufferTask::st_handle_callback_list(const int handle_err, int64_t& task_num)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  task_num = 0;
  ObIBufferTask* curr_task = head_.next_;
  ObIBufferTask* next_task = NULL;
  while (NULL != curr_task) {
    next_task = curr_task->next_;
    if (OB_SUCCESS != (tmp_ret = curr_task->st_after_consume(handle_err))) {
      CLOG_LOG(WARN, "st_after_consume failed", K(tmp_ret), K(handle_err));
    }
    curr_task = next_task;
    task_num++;
  }
  return ret;
}

void ObIBufferTask::reset()
{
  next_ = NULL;
  need_callback_ = false;
}
};  // end namespace clog
};  // end namespace oceanbase
