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

#include "ob_log_event_scheduler.h"
#include "ob_log_event_task_V2.h"

namespace oceanbase {
namespace clog {
ObLogEventScheduler::ObLogEventScheduler() : time_wheel_(), is_inited_(false)
{}

ObLogEventScheduler::~ObLogEventScheduler()
{
  destroy();
}

int ObLogEventScheduler::init()
{
  int ret = common::OB_SUCCESS;
  const char* CLOG_EVENT_TIME_WHEEL_NAME = "ClogEventTimeWheel";
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    CLOG_LOG(ERROR, "ObLogEventScheduler init twice", K(ret));
  } else if (OB_FAIL(time_wheel_.init(
                 CLOG_EVENT_TIME_WHEEL_PRECISION, get_time_wheel_thread_num_(), CLOG_EVENT_TIME_WHEEL_NAME))) {
    CLOG_LOG(ERROR, "ObTimeWheel init fail", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObLogEventScheduler::destroy()
{
  time_wheel_.destroy();
  is_inited_ = false;
}

int ObLogEventScheduler::start()
{
  return time_wheel_.start();
}

int ObLogEventScheduler::stop()
{
  return time_wheel_.stop();
}

int ObLogEventScheduler::wait()
{
  return time_wheel_.wait();
}

int ObLogEventScheduler::add_state_change_event(ObLogStateEventTaskV2* task)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
  } else if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_inited())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(schedule_task_(task, CLOG_EVENT_TIME_WHEEL_PRECISION))) {
    CLOG_LOG(WARN, "schedule task failed", K(ret));
  } else {
    CLOG_LOG(TRACE, "add_state_change_event success", K(ret));
  }
  return ret;
}

int ObLogEventScheduler::add_state_change_delay_event(ObLogStateEventTaskV2* task)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
  } else if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_inited())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(schedule_task_(task, 5 * CLOG_EVENT_TIME_WHEEL_PRECISION))) {
    CLOG_LOG(WARN, "schedule_task_ failed", K(ret));
  } else {
    CLOG_LOG(TRACE, "schedule delay task success", K(ret));
  }
  return ret;
}

int ObLogEventScheduler::schedule_task_(ObLogStateEventTaskV2* task, const int64_t delay)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(task->set_expected_ts(delay))) {
    CLOG_LOG(WARN, "set expected_ts failed", K(ret), K(delay));
  } else if (OB_FAIL(time_wheel_.schedule(task, delay))) {
    CLOG_LOG(WARN, "schedule delay task failed", K(ret));
  }
  return ret;
}

int64_t ObLogEventScheduler::get_time_wheel_thread_num_() const
{
  int64_t thread_num = MAX(common::get_cpu_num() / 4, 4);
  return thread_num;
}
}  // namespace clog
}  // namespace oceanbase
