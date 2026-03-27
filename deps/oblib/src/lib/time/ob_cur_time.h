/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_CUR_TIME_H
#define _OB_CUR_TIME_H 1

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace common
{
extern volatile int64_t g_cur_time;

class TimeUpdateDuty : public common::ObTimerTask
{
public:
  static const int64_t SCHEDULE_PERIOD = 2000;
  TimeUpdateDuty() {};
  virtual ~TimeUpdateDuty() {};
  virtual void runTimerTask();
};
}
}

#endif /* _OB_CUR_TIME_H */

