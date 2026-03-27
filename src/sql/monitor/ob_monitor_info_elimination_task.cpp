/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_monitor_info_elimination_task.h"
#include "sql/monitor/ob_monitor_info_manager.h"
namespace oceanbase
{
namespace sql
{
int ObMonitorInfoEliminationTask::init(ObMonitorInfoManager *info)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_MONITOR_LOG(WARN, "invalid argument", K(ret), K(info));
  } else {
    monitor_info_ = info;
  }
  return ret;
}
void ObMonitorInfoEliminationTask::runTimerTask()
{
  if (OB_ISNULL(monitor_info_)) {
    SQL_MONITOR_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid history info", K(monitor_info_));
  } else {
    monitor_info_->print_memory_size();
    monitor_info_->gc();
  }
}
} //namespace sql
} //namespace oceanbase


