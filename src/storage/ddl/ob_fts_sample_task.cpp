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

#include "storage/ddl/ob_fts_sample_task.h"

#include "storage/ddl/ob_column_clustered_dag.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::storage;

ObFtsSampleTask::ObFtsSampleTask(const ObITaskType type)
  : ObITaskWithMonitor(type),
    ddl_dag_(nullptr)
{
}

ObFtsSampleTask::ObFtsSampleTask()
  : ObFtsSampleTask(TASK_TYPE_DDL_FTS_SAMPLE_TASK)
{
}

ObFtsSampleTask::~ObFtsSampleTask()
{
}

int ObFtsSampleTask::init(ObColumnClusteredDag *ddl_dag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ddl_dag_ = ddl_dag;
  }
  return ret;
}

share::ObITask::ObITaskPriority ObFtsSampleTask::get_priority()
{
  int ret = OB_SUCCESS;
  ObITask::ObITaskPriority priority = ObITask::get_priority();
  if (OB_ISNULL(ddl_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret), KP(ddl_dag_));
  } else {
    priority = ddl_dag_->is_sample_scan_finished() ? ObITask::TASK_PRIO_2 : ObITask::TASK_PRIO_0;
  }
  return priority;
}

int ObFtsSampleTask::process()
{
  int ret = OB_SUCCESS;
  // TODO@xuzhuo: execute sample logic here
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ddl_dag_->notify_sample_finished())) {
      LOG_WARN("fail to notify sample finished", K(ret));
    }
  }
  return ret;
}
