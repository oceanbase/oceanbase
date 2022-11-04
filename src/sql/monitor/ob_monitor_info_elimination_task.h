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


