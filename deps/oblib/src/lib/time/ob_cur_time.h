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

