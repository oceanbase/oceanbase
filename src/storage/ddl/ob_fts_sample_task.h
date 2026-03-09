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
