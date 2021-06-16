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

#ifndef OCEANBASE_CLOG_OB_LOG_EVENT_SCHEDULER_
#define OCEANBASE_CLOG_OB_LOG_EVENT_SCHEDULER_

#include "storage/transaction/ob_time_wheel.h"

namespace oceanbase {
namespace clog {
class ObLogStateEventTaskV2;
class ObLogEventScheduler {
public:
  ObLogEventScheduler();
  ~ObLogEventScheduler();

public:
  virtual int init();
  virtual void destroy();
  virtual int start();
  virtual int stop();
  virtual int wait();
  virtual int add_state_change_event(ObLogStateEventTaskV2* task);
  virtual int add_state_change_delay_event(ObLogStateEventTaskV2* task);

private:
  int schedule_task_(ObLogStateEventTaskV2* task, const int64_t delay);
  int64_t get_time_wheel_thread_num_() const;

private:
  static const int64_t CLOG_EVENT_TIME_WHEEL_PRECISION = 1000;  // 1ms
private:
  common::ObTimeWheel time_wheel_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogEventScheduler);
};  // class ObLogEventScheduler
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_EVENT_SCHEDULER_
