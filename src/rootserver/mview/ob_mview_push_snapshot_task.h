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
class ObMViewPushSnapshotTask : public ObMViewTimerTask
{
public:
  ObMViewPushSnapshotTask();
  virtual ~ObMViewPushSnapshotTask();
  DISABLE_COPY_ASSIGN(ObMViewPushSnapshotTask);
  // for Service
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  // for TimerTask
  void runTimerTask() override;
  int check_space_occupy_(bool &space_danger);
  static const int64_t MVIEW_PUSH_SNAPSHOT_INTERVAL = 60 * 1000 * 1000; // 1min
private:
  bool is_inited_;
  bool in_sched_;
  bool is_stop_;
  uint64_t tenant_id_;
};

} // namespace rootserver
} // namespace oceanbase
