/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/task/ob_timer.h"
#include "rootserver/mview/ob_mview_timer_task.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewUpdateDepsTask : public ObMViewTimerTask
{
public:
  ObMViewUpdateDepsTask();
  virtual ~ObMViewUpdateDepsTask();
  DISABLE_COPY_ASSIGN(ObMViewUpdateDepsTask);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  // for TimerTask
  void runTimerTask() override;
  int need_schedule(bool &need_sche);
  static const int64_t MVIEW_UPDATE_DEPS_INTERVAL = 5 * 1000 * 1000; // 5s
private:
  bool is_inited_;
  bool in_sched_;
  bool is_stop_;
  uint64_t tenant_id_;
  int64_t last_sched_ts_;
};

} // namespace rootserver
} // namespace oceanbase
