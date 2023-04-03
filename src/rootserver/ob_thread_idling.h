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
