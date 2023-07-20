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

#define USING_LOG_PREFIX SERVER
#include "storage/tx_storage/ob_safe_destroy_handler.h"

namespace oceanbase
{
namespace storage
{

void ObSafeDestroyTask::set_receive_timestamp(const int64_t receive_timestamp)
{
  // only set the first time.
  if (0 == recv_timestamp_) {
    recv_timestamp_ = receive_timestamp;
  }
}

void ObSafeDestroyTask::update_retry_info()
{
  int64_t current_time = ObTimeUtility::current_time();

  retry_times_++;
  last_execute_timestamp_ = current_time;
  if ((current_time - recv_timestamp_) >= PRINT_INTERVAL_TIME &&
      retry_times_ % PRINT_TIMES_INTERVAL == 0) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "safe destroy task alive too long", KPC(this));
  }
}

void ObSafeDestroyTask::update_execute_timestamp()
{
  last_execute_timestamp_ = ObTimeUtility::current_time();
}

bool ObSafeDestroyProcessFunctor::operator()(
    ObSafeDestroyTask *task,
    bool &need_requeue) const
{
  int ret = OB_SUCCESS;
  need_requeue = false;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("queue pop NULL task", K(task), K(ret));
  } else {
    if (task->safe_to_destroy()) {
      task->destroy();
      task->~ObSafeDestroyTask();
      ob_free(task);
    } else {
      task->update_retry_info();
      need_requeue = true;
    }
  }
  return true; // always continue to loop
}

ObSafeDestroyTaskQueue::ObSafeDestroyTaskQueue()
  : looping_(false),
    stop_(false),
    queue_()
{
}

ObSafeDestroyTaskQueue::~ObSafeDestroyTaskQueue()
{
  destroy();
}

int ObSafeDestroyTaskQueue::init(int queue_capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue_.init(queue_capacity))) {
    LOG_WARN("init queue failed", K(ret));
  }
  return ret;
}

int ObSafeDestroyTaskQueue::push(ObSafeDestroyTask *task)
{
  int ret = OB_SUCCESS;
  // allow mutil thread push into the queue.
  RDLockGuard guard(rwlock_);
  if (stop_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("thread is stopped", K(stop_), K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task should not be null", K(ret), KP(task));
  } else {
    task->set_receive_timestamp(ObTimeUtility::current_time());
    if (OB_FAIL(queue_.push(task))) {
      LOG_WARN("push into queue failed", K(ret));
    }
  }
  return ret;
}

bool ObSafeDestroyTaskQueue::stop_finished()
{
  return (stop_ &&
          !looping_ &&
          0 == queue_.size());
}

void ObSafeDestroyTaskQueue::push_into_queue_(void *task)
{
  int ret = OB_SUCCESS;
  static const int64_t SLEEP_TS = 100 * 1000; // 100_ms
  // retry until success.
  while (OB_FAIL(queue_.push(task))) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      LOG_WARN("push into the queue failed, retry again", K(ret), K(task));
    }
    ob_usleep(SLEEP_TS);
  }
}

void ObSafeDestroyTaskQueue::loop()
{
  int ret = OB_SUCCESS;
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    LOG_INFO("ObSafeDestroyTaskQueue::loop begin", K(queue_.size()));
  }
  ObSafeDestroyProcessFunctor fn;
  if (OB_FAIL(for_each(fn))) {
    LOG_WARN("loop failed", K(ret));
  }
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    LOG_INFO("ObSafeDestroyTaskQueue::loop finish", K(ret), K(queue_.size()));
  }
}

void ObSafeDestroyTaskQueue::destroy()
{
  queue_.destroy();
  stop_ = true;
  looping_ = false;
}

ObSafeDestroyHandler::ObSafeDestroyHandler()
  : is_inited_(false),
    queue_(),
    last_process_timestamp_(0)
{}

ObSafeDestroyHandler::~ObSafeDestroyHandler()
{
  destroy();
}

int ObSafeDestroyHandler::init()
{
	int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("safe destroy thread init twice.", K(ret));
  } else if (OB_FAIL(queue_.init())) {
    LOG_WARN("queue init failed", K(ret));
  } else {
    last_process_timestamp_ = ObTimeUtility::current_time();
    is_inited_ = true;
  }
  return ret;
}

int ObSafeDestroyHandler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("safe destroy thread not inited", K(ret));
  } else {
    LOG_INFO("ObSafeDestroyHandler start");
  }
  return ret;
}

int ObSafeDestroyHandler::stop()
{
	int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("safe destroy thread not inited", K(ret));
  } else {
    queue_.stop();
    // we only stop the queue to prevent push task into the queue.
    // the timer will be stop at the wait function.
    LOG_INFO("ObSafeDestroyHandler stopped");
  }
  return ret;
}

void ObSafeDestroyHandler::wait()
{
  int ret = OB_SUCCESS;
  static const int64_t SLEEP_TS = 100 * 1000; // 100_ms
  int64_t start_ts = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("safe destroy thread not inited", K(ret));
  } else {
    while (!queue_.stop_finished()) {
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) { // every minute
        LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "the safe destroy thread wait cost too much time",
                     K(ObTimeUtility::current_time() - start_ts));
      }
      ob_usleep(SLEEP_TS);
    }
  }
}

void ObSafeDestroyHandler::destroy()
{
  LOG_INFO("ObSafeDestroyHandler::destroy");
  is_inited_ = false;
  last_process_timestamp_ = 0;
  queue_.destroy();
}

int ObSafeDestroyHandler::handle()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("safe destroy thread not inited", K(ret));
  } else {
    int64_t curr_time = ObTimeUtility::current_time();
    if (curr_time - last_process_timestamp_ >= TASK_SCHEDULER_INTERVAL) {
      LOG_INFO("ObSafeDestroyHandler start process");
      queue_.loop();
      last_process_timestamp_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObSafeDestroyHandler::push(ObSafeDestroyTask &task)
{
	int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("safe destroy thread not inited", K(ret));
  } else if (OB_FAIL(queue_.push(&task))) {
    LOG_WARN("push safe destroy task failed", K(ret));
  } else {
    LOG_INFO("success add a safe destroy task", K(&task), K(task));
  }
  return ret;
}

} // observer
} // oceanbase
