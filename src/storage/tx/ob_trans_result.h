/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_COND_
#define OCEANBASE_TRANSACTION_OB_TRANS_COND_

#include <stdint.h>
#include "lib/lock/ob_monitor.h"
#include "lib/lock/mutex.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace transaction
{

class ObTransCond
{
public:
  ObTransCond() : monitor_(common::ObLatchIds::TRANS_RESULT_COND_WAIT) { reset(); }
  ~ObTransCond() {}
  void reset();
public:
  // when SQL submit or abort transaction, it must wait transaction response for some time.
  // safer to call wait(wait_time_us, result)
  int wait(const int64_t wait_time_us, int &result);
  // @deprecated
  //int wait(const int64_t wait_time_us);

  // notify and set transaction result
  void notify(const int result);

  static void usleep(const int64_t us);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransCond);
private:
  bool finished_;
  mutable obutil::ObMonitor<obutil::Mutex> monitor_;
  int result_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TRANS_COND_
