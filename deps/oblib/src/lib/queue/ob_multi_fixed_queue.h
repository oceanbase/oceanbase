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

#ifndef OCEANBASE_LIB_QUEUE_M_FIXED_QUEUE_H_
#define OCEANBASE_LIB_QUEUE_M_FIXED_QUEUE_H_

#include "lib/queue/ob_fixed_queue.h"    // ObFixedQueue
#include "common/ob_queue_thread.h"      // ObCond

namespace oceanbase
{
namespace common
{
const int64_t DefaultMaxQueueNum = 32;
const int64_t DefaultQueueNum = 0;
template <int MAX_QUEUE_NUM = DefaultMaxQueueNum>
class ObMultiFixedQueue
{
public:
  ObMultiFixedQueue() : inited_(false), queue_num_(DefaultQueueNum) {}
  virtual ~ObMultiFixedQueue()
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = destroy())) {
      LIB_LOG(ERROR, "err destroy multi fixed queue", K(ret));
    }
  }

public:
  int init(const int64_t queue_size, const int64_t queue_num);
  int destroy();

  int push(void *data, const uint64_t hash_val, const int64_t timeout);
  int pop(void  *&data, const int64_t queue_index, const int64_t timeout);

  int get_task_count(const int64_t queue_index, int64_t &task_count);

private:
  int init_queue_(const int64_t queue_num, const int64_t queue_size);
  void destroy_queue_(const int64_t queue_num);

private:
  bool                        inited_;
  ObFixedQueue<void>          queue_[MAX_QUEUE_NUM];
  ObCond                      queue_conds_[MAX_QUEUE_NUM];
  int64_t                     queue_num_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiFixedQueue);
};

template <int MAX_QUEUE_NUM>
int ObMultiFixedQueue<MAX_QUEUE_NUM>::init(const int64_t queue_size, const int64_t queue_num)
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (0 >= queue_size || 0 >= queue_num || queue_num > MAX_QUEUE_NUM) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = init_queue_(queue_num, queue_size))) {
    LIB_LOG(ERROR, "err init queue", K(ret));
  } else {
    inited_ = true;
  }

  return ret;
}

template <int MAX_QUEUE_NUM>
int ObMultiFixedQueue<MAX_QUEUE_NUM>::destroy()
{
  inited_ = false;

  if (0 < queue_num_) {
    destroy_queue_(queue_num_);
  }

  queue_num_ = 0;

  return OB_SUCCESS;
}

template <int MAX_QUEUE_NUM>
int ObMultiFixedQueue<MAX_QUEUE_NUM>::init_queue_(const int64_t queue_num, const int64_t queue_size)
{
  int ret = OB_SUCCESS;
  if (!(0 < queue_num && MAX_QUEUE_NUM >= queue_num && 0 < queue_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid arguments", K(ret), K(queue_num), K(queue_size));
  } else {
    queue_num_ = queue_num;
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < queue_num; index++) {
    if (OB_SUCCESS != (ret = queue_[index].init(queue_size))) {
      LIB_LOG(ERROR, "err init queue", K(ret));
    }
  }

  return ret;
}

template <int MAX_QUEUE_NUM>
void ObMultiFixedQueue<MAX_QUEUE_NUM>::destroy_queue_(const int64_t queue_num)
{
  if (0 < queue_num) {
    for (int64_t index = 0; index < queue_num; index++) {
      queue_[index].destroy();
    }
  }
}

template <int MAX_QUEUE_NUM>
int ObMultiFixedQueue<MAX_QUEUE_NUM>::push(void *data, const uint64_t hash_val, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (! inited_) {
    ret = OB_NOT_INIT;
  } else if (NULL == data) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t index = hash_val % queue_num_;
    ObCond &cond = queue_conds_[index];
    int64_t end_time = timeout + ::oceanbase::common::ObTimeUtility::current_time();

    while (true) {
      ret = queue_[index].push(data);

      if (OB_SIZE_OVERFLOW != ret) {
        break;
      }

      int64_t left_time = end_time - ::oceanbase::common::ObTimeUtility::current_time();

      if (left_time <= 0) {
        ret = OB_TIMEOUT;
        break;
      }

      cond.timedwait(left_time);
    }

    if (OB_SUCC(ret)) {
      cond.signal();
    }
  }

  return ret;
}

template <int MAX_QUEUE_NUM>
int ObMultiFixedQueue<MAX_QUEUE_NUM>::pop(void *&data, const int64_t queue_index, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (! inited_) {
    ret = OB_NOT_INIT;
  } else if (0 > queue_index || queue_index >= MAX_QUEUE_NUM) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObCond &cond = queue_conds_[queue_index];
    int64_t end_time = timeout + ::oceanbase::common::ObTimeUtility::current_time();

    data = NULL;
    while (true) {
      ret = queue_[queue_index].pop(data);

      if (OB_ENTRY_NOT_EXIST != ret) {
        break;
      }

      int64_t left_time = end_time - ::oceanbase::common::ObTimeUtility::current_time();

      if (left_time <= 0) {
        ret = OB_TIMEOUT;
        break;
      }

      cond.timedwait(left_time);
    }

    if (OB_SUCC(ret)) {
      cond.signal();
    }
  }

  return ret;
}

template <int MAX_QUEUE_NUM>
int ObMultiFixedQueue<MAX_QUEUE_NUM>::get_task_count(const int64_t queue_index, int64_t &task_count)
{
  int ret = OB_SUCCESS;
  task_count = 0;

  if (OB_UNLIKELY(queue_index < 0) || OB_UNLIKELY(queue_index >= queue_num_)) {
    LIB_LOG(ERROR, "invalid argument", K(queue_index), K(queue_num_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    task_count = queue_[queue_index].get_total();
  }

  return ret;
}
} // namespace common
} // namespace oceanbase
#endif /* OCEANBASE_LIB_QUEUE_M_FIXED_QUEUE_H_ */
