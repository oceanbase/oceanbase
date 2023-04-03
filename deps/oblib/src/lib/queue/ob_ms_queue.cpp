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

#define USING_LOG_PREFIX  LIB

#include "lib/queue/ob_ms_queue.h"
#include "lib/allocator/ob_allocator.h"   // ObIAllocator

namespace oceanbase
{
namespace common
{
////////////////////////////////////////////// ObMsQueue::TaskHead ///////////////////////////////////
void ObMsQueue::TaskHead::add(ObMsQueue::Task* node)
{
  if (NULL == node) {
  } else {
    node->next_ = NULL;
    if (NULL == head_) {
      head_ = node;
      tail_ = node;
    } else {
      tail_->next_ = node;
      tail_ = node;
    }
  }
}

ObMsQueue::Task* ObMsQueue::TaskHead::pop()
{
  ObMsQueue::Task* pret = NULL;
  if (NULL == head_) {
  } else {
    pret = head_;
    head_ = head_->next_;
    if (NULL == head_) {
      tail_ = NULL;
    }
  }
  return pret;
}

////////////////////////////////////////////// ObMsQueue::QueueInfo ///////////////////////////////////
int ObMsQueue::QueueInfo::init(char* buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid args", K(ret), KP(buf), K(len));
  } else {
    array_ = reinterpret_cast<TaskHead*>(buf);
    memset(array_, 0, sizeof(TaskHead) * len);
    len_ = len;
    pop_ = 0;
  }
  return ret;
}

int ObMsQueue::QueueInfo::destroy()
{
  array_ = NULL;
  len_ = 0;
  pop_ = 0;
  return OB_SUCCESS;
}

int ObMsQueue::QueueInfo::add(const int64_t seq, ObMsQueue::Task* task)
{
  int ret = OB_SUCCESS;
  int64_t pop = ATOMIC_LOAD(&pop_);
  if (seq < pop) {
    ret = OB_ERR_UNEXPECTED;
  } else if (seq >= pop + len_) {
    ret = OB_EAGAIN;
  } else if (NULL == array_) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "invalid array", K(ret), K(array_));
  } else {
    array_ [seq % len_].add(task);
  }
  return ret;
}
// NOT thread-safe
int ObMsQueue::QueueInfo::get(const int64_t ready_seq, ObMsQueue::Task*& task)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (pop_ >= ready_seq) {
      ret = OB_EAGAIN;
    } else if (NULL == (task = array_[pop_ % len_].pop())) {
      pop_++;
    } else {
      break;
    }
  }
  return ret;
}

bool ObMsQueue::QueueInfo::next_is_ready(const int64_t ready_seq) const
{
  return pop_ < ready_seq;
}
////////////////////////////////////////////// ObMsQueue::TaskHead ///////////////////////////////////

ObMsQueue::ObMsQueue() : inited_(false),
                         qlen_(0),
                         qcount_(0),
                         qinfo_(NULL),
                         seq_queue_(),
                         allocator_(NULL)
{}

ObMsQueue::~ObMsQueue()
{
  destroy();
}

int ObMsQueue::init(const int64_t n_queue, const int64_t queue_len, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (n_queue <= 0 || queue_len <= 0 || NULL == allocator) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", K(n_queue), K(queue_len), K(allocator));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (NULL == (qinfo_ = static_cast<QueueInfo*>(allocator->alloc(n_queue * sizeof(QueueInfo))))) {
    LOG_ERROR("allocate memory for QueueInfo fail", K(n_queue), K(sizeof(QueueInfo)));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < n_queue; i++) {
      char *ptr = NULL;

      if (NULL == (ptr = static_cast<char*>(allocator->alloc(queue_len * sizeof(TaskHead))))) {
        LOG_ERROR("allocate memory for TaskHead fail", "size", queue_len * sizeof(TaskHead));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        new(qinfo_ + i)QueueInfo();

        if (OB_FAIL(qinfo_[i].init(ptr, queue_len))) {
          LOG_ERROR("init queue info fail", K(i), K(ret), KP(ptr), K(queue_len));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = seq_queue_.init(queue_len, allocator))) {
      LOG_ERROR("init co-seq queue fail", K(queue_len), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    qcount_ = n_queue;
    qlen_ = queue_len;
    allocator_ = allocator;

    inited_ = true;
  }

  return ret;
}

int ObMsQueue::destroy()
{
  inited_ = false;

  seq_queue_.destroy();

  if (NULL != qinfo_ && NULL != allocator_) {
    for (int64_t index = 0; index < qcount_; index++) {
      if (NULL != qinfo_[index].array_) {
        allocator_->free(qinfo_[index].array_);
      }

      qinfo_[index].~QueueInfo();
    }

    allocator_->free(qinfo_);
    qinfo_ = NULL;
  }

  allocator_ = NULL;
  qcount_ = 0;
  qlen_ = 0;

  return OB_SUCCESS;
}

// it must be one thread in common slot
int ObMsQueue::push(Task* task, const int64_t seq, const uint64_t hash)
{
  int ret = OB_SUCCESS;
  if (NULL == task || seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", K(ret), K(task), K(seq), K(hash));
  } else if (! inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = qinfo_[hash % qcount_].add(seq, task))
           && OB_EAGAIN != ret) {
    LOG_ERROR("push to ms_queue: unexpected error", K(seq), K(task), K(hash));
  } else {
    // succ
  }
  return ret;
}

// different threads operate different qinfo
int ObMsQueue::get(Task*& task, const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= qcount_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", K(ret), K(idx));
  } else if (! inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = qinfo_[idx].get(seq_queue_.get_next(), task))
           && OB_EAGAIN != ret) {
    LOG_ERROR("get task from queue info fail", K(ret), K(idx));
  }
  return ret;
}

bool ObMsQueue::next_is_ready(const int64_t queue_index) const
{
  bool bool_ret = false;
  if (inited_ && queue_index >= 0 && queue_index < qcount_) {
    bool_ret = qinfo_[queue_index].next_is_ready(seq_queue_.get_next());
  }
  return bool_ret;
}

int ObMsQueue::end_batch(const int64_t seq, const int64_t count)
{
  int ret = OB_SUCCESS;
  UNUSED(count);

  if (! inited_) {
    ret = OB_NOT_INIT;
  } else {
    int64_t next_seq = seq_queue_.add(seq);
    UNUSED(next_seq);
  }
  return ret;
}
}; // end namespace clog
}; // end namespace oceanbase
