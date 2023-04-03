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

#include "sql/monitor/ob_monitor_info_elimination_task.h"
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


