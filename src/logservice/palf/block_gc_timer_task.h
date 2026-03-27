/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_PALF_FILE_GC_TIMER_TASK_
#define OCEANBASE_PALF_FILE_GC_TIMER_TASK_
#include "lib/task/ob_timer.h"
namespace oceanbase
{
namespace palf
{
class PalfEnvImpl;
class BlockGCTimerTask : public common::ObTimerTask
{
public:
  BlockGCTimerTask();
  virtual ~BlockGCTimerTask();
  int init(PalfEnvImpl *palf_env_impl);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask();
private:
  static const int64_t BLOCK_GC_TIMER_INTERVAL_MS = 30 * 1000;
  PalfEnvImpl *palf_env_impl_;
  int64_t warn_time_;
  int tg_id_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase
#endif
