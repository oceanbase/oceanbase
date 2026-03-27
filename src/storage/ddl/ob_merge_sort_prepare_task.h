/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_DDL_OB_MERGE_SORT_PREPARE_TASK_H_
#define OCEANBASE_STORAGE_DDL_OB_MERGE_SORT_PREPARE_TASK_H_

#include "share/scheduler/ob_independent_dag.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h" // for ObITaskPriority

namespace oceanbase
{
namespace storage
{

class ObDDLIndependentDag;
class ObDDLTabletContext;
class ObDDLSlice;

class ObMergeSortPrepareTask : public share::ObITaskWithMonitor
{
public:
  ObMergeSortPrepareTask(const ObITaskType type);
  ObMergeSortPrepareTask();
  virtual ~ObMergeSortPrepareTask();
  int init(ObDDLIndependentDag *ddl_dag);
  virtual share::ObITask::ObITaskPriority get_priority() override;
  virtual int process() override;
private:
  int schedule_slice_merge(ObDDLTabletContext *tablet_ctx,
                           ObDDLSlice *ddl_slice,
                           const share::ObLSID &ls_id,
                           const ObTabletID &tablet_id);
  int schedule_tablet_merge(ObDDLTabletContext *tablet_ctx,
                            const share::ObLSID &ls_id,
                            const ObTabletID &tablet_id);
  ObDDLIndependentDag *ddl_dag_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_DDL_OB_MERGE_SORT_PREPARE_TASK_H_
