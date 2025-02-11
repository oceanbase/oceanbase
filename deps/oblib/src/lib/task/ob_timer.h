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

#ifndef OCEANBASE_COMMON_OB_TIMER_
#define OCEANBASE_COMMON_OB_TIMER_

#include "lib/task/ob_timer_service.h"

namespace oceanbase
{

namespace common
{

class ObTimer
{
  friend class TaskToken;
public:
  ObTimer()
      : is_inited_(false),
        is_stopped_(true),
        timer_service_(nullptr),
        run_wrapper_(nullptr)
  {
    timer_name_[0] = '\0';
    timer_service_ = &(ObTimerService::get_instance());
  }
  ~ObTimer();
  int init(const char* timer_name = nullptr,
           const ObMemAttr &attr = ObMemAttr(OB_SERVER_TENANT_ID, "timer")); // init and start
  bool inited() const;
  int start();  // only start
  void stop();  // only stop
  void wait();  // wait all running task finish
  void destroy();
  int set_run_wrapper_with_ret(lib::IRunWrapper *run_wrapper);
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (NULL != buf && buf_len > 0) {
      databuff_printf(buf, buf_len, pos, "timer_type:%s", typeid(*this).name());
    }
    return pos;
  }
public:
  int schedule(ObTimerTask &task, const int64_t delay, bool repeate = false, bool immediate = false);
  int schedule_repeate_task_immediately(ObTimerTask &task, const int64_t delay);
  bool task_exist(const common::ObTimerTask &task);
  int cancel(const ObTimerTask &task);
  int cancel_task(const ObTimerTask &task);
  int wait_task(const ObTimerTask &task);
  void cancel_all();
private:
  DISALLOW_COPY_AND_ASSIGN(ObTimer);
private:
  bool is_inited_;
  bool is_stopped_;
  char timer_name_[OB_THREAD_NAME_BUF_LEN];
  ObTimerService *timer_service_;
  lib::IRunWrapper *run_wrapper_;
};

} /* common */
} /* oceanbase */

#endif // OCEANBASE_COMMON_OB_TIMER_
