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

#ifndef _OB_TABLE_THROTTLE_MANAGER_H
#define _OB_TABLE_THROTTLE_MANAGER_H

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace observer
{

class ObTableThrottleTimerTask : public common::ObTimerTask
{
public:
  ObTableThrottleTimerTask() {}
  virtual ~ObTableThrottleTimerTask() {}
  virtual void runTimerTask() override;
};

class OBTableThrottleMgr
{
public:
  static uint64_t epoch_;

  int init();
  int start();
  static OBTableThrottleMgr &get_instance();

private:
  void stop();
  void destroy();
  OBTableThrottleMgr()
      :is_timer_start_(false),
      periodic_delay_(THROTTLE_PERIODIC_DELAY),
      throttle_timer_(),
      periodic_task_(),
      allocator_()
  {
  }
  ~OBTableThrottleMgr() {}

private:
  static const int64_t THROTTLE_PERIODIC_DELAY = 2*1000*1000; //2s

  bool is_timer_start_;
  int64_t periodic_delay_;
  common::ObTimer throttle_timer_;
  ObTableThrottleTimerTask periodic_task_;
  common::ObArenaAllocator allocator_;

  static int64_t inited_;
  static OBTableThrottleMgr *instance_;
};


} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_THROTTLE_MANAGER_H */

