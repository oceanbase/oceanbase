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

#define USING_LOG_PREFIX LIB
#include "ob_work_queue.h"
using namespace oceanbase::common;
using namespace oceanbase::share;

void ObAsyncTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(work_queue_.add_async_task(*this))) {
    LOG_ERROR("failed to submit async task", K(ret), KPC(this));
  } else {
    LOG_INFO("add async task", KPC(this));
  }
}
////////////////////////////////////////////////////////////////
ObWorkQueue::ObWorkQueue()
    :inited_(false),
     timer_(),
     task_queue_()
{}

ObWorkQueue::~ObWorkQueue()
{}

// @note queue_size should be 2^n
int ObWorkQueue::init(const int64_t thread_count, const int64_t queue_size,
                      const char *thread_name)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("rs task queue already inited", K(ret));
  } else if (OB_FAIL(timer_.init(thread_name))) {
    LOG_WARN("failed to init timer", K(ret));
  } else if (OB_FAIL(task_queue_.init(thread_count, queue_size, thread_name))) {
    LOG_WARN("failed to init work queue", K(ret), K(thread_count), K(queue_size));
    timer_.destroy();
  } else {
    inited_ = true;
  }
  return ret;
}

void ObWorkQueue::destroy()
{
  if (inited_) {
    timer_.destroy();
    task_queue_.destroy();
    inited_ = false;
    LOG_INFO("work queue destroy");
  }
}

int ObWorkQueue::add_timer_task(ObAsyncTimerTask &task, const int64_t delay, bool did_repeat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("work queue not init", K(ret));
  } else {
    ret = timer_.schedule(task, delay, did_repeat);
  }
  return ret;
}

int ObWorkQueue::add_repeat_timer_task_schedule_immediately(ObAsyncTimerTask &task, const int64_t delay)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("work queue not init", K(ret));
  } else {
    ret = timer_.schedule_repeate_task_immediately(task, delay);
  }
  return ret;
}

bool ObWorkQueue::exist_timer_task(const ObAsyncTimerTask &task)
{
  bool exist = false;
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("work queue not init", K(ret));
  } else {
    exist = timer_.task_exist(task);
  }
  return exist;
}


int ObWorkQueue::cancel_timer_task(const ObAsyncTimerTask &task)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("work queue not init", K(ret));
  } else {
    ret = timer_.cancel(task);
  }
  return ret;
}

int ObWorkQueue::add_async_task(const ObAsyncTask &task)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("work queue not init", K(ret));
  } else {
    ret = task_queue_.push(task);
  }
  return ret;
}

int ObWorkQueue::start()
{
  int ret = OB_SUCCESS;
  // start work_queue before the timer
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("work queue not init", K(ret));
  } else if (OB_FAIL(task_queue_.start())) {
    LOG_WARN("failed to start work queue", K(ret));
  } else if (OB_FAIL(timer_.start())) {
    LOG_WARN("failed to start timer", K(ret));
  } else {
    LOG_INFO("work queue started");
  }
  return ret;
}

int ObWorkQueue::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("work queue not init", K(ret));
  } else {
    timer_.cancel_all();
    timer_.stop();
    task_queue_.stop();
    LOG_INFO("work queue stopped");
  }
  return ret;
}

int ObWorkQueue::wait()
{
  int ret = OB_SUCCESS;
  // wait timer first
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("work queue not init", K(ret));
  } else {
    timer_.wait();
    task_queue_.wait();
    LOG_INFO("work queue waited");
  }
  return ret;
}
