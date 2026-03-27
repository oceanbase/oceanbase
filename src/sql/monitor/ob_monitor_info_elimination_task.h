/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_MONITOR_INFO_ELIMINATION_TASK_H
#define OCEANBASE_SQL_OB_MONITOR_INFO_ELIMINATION_TASK_H
#include "lib/task/ob_timer.h"
namespace oceanbase
{
namespace sql
{
class ObMonitorInfoManager;
class ObMonitorInfoEliminationTask : public common::ObTimerTask
{
public:
  ObMonitorInfoEliminationTask() : monitor_info_(NULL) {}
  ~ObMonitorInfoEliminationTask() {}
  int init(ObMonitorInfoManager *monitor_info);
  virtual void runTimerTask();
private:
  DISALLOW_COPY_AND_ASSIGN(ObMonitorInfoEliminationTask);
private:
  ObMonitorInfoManager *monitor_info_;
};

} //namespace sql
} //namespace oceanbase
#endif


