/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_THREAD_IDLING_H_
#define OCEANBASE_ROOTSERVER_OB_THREAD_IDLING_H_

#include "lib/lock/ob_thread_cond.h"

namespace oceanbase
{
namespace rootserver
{

class ObThreadIdling
{
public:
  explicit ObThreadIdling(volatile bool &stop);
  virtual ~ObThreadIdling() {}

  virtual void wakeup();
  virtual int idle();
  virtual int idle(const int64_t max_idle_time_us);

  virtual int64_t get_idle_interval_us() = 0;

private:
  common::ObThreadCond cond_;
  volatile bool &stop_;
  int64_t wakeup_cnt_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_THREAD_IDLING_H_
