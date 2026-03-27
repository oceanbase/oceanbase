/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_DDL_OB_FTS_SAMPLE_TASK_H_
#define OCEANBASE_STORAGE_DDL_OB_FTS_SAMPLE_TASK_H_

#include "share/scheduler/ob_independent_dag.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace storage
{

class ObColumnClusteredDag;

class ObFtsSampleTask : public share::ObITaskWithMonitor
{
public:
  ObFtsSampleTask(const ObITaskType type);
  ObFtsSampleTask();
  virtual ~ObFtsSampleTask();
  int init(ObColumnClusteredDag *ddl_dag);
  virtual share::ObITask::ObITaskPriority get_priority() override;
  int process() override;
private:
  ObColumnClusteredDag *ddl_dag_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_DDL_OB_FTS_SAMPLE_TASK_H_
