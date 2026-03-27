/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_PALF_LOG_UPDATER_
#define OCEANBASE_PALF_LOG_UPDATER_
#include "lib/task/ob_timer.h"
namespace oceanbase
{
namespace palf
{
class IPalfEnvImpl;
class LogUpdater : public common::ObTimerTask
{
public:
  LogUpdater();
  virtual ~LogUpdater();
  int init(IPalfEnvImpl *palf_env_impl);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask();
private:
  IPalfEnvImpl *palf_env_impl_;
  int tg_id_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase
#endif
