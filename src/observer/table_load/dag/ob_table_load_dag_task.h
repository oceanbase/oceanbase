/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadDag;
class ObTableLoadOp;

class ObTableLoadDagTaskBase
{
public:
  ObTableLoadDagTaskBase(ObTableLoadDag *dag);
  virtual ~ObTableLoadDagTaskBase();

protected:
  static share::ObITask::ObITaskPriority get_priority(const bool can_sched)
  {
    return can_sched ? share::ObITask::TASK_PRIO_1 : share::ObITask::TASK_PRIO_0;
  }

protected:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadDag *dag_;
};

class ObTableLoadDagOpTaskBase : public ObTableLoadDagTaskBase
{
public:
  ObTableLoadDagOpTaskBase(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadDagOpTaskBase();

  static int create_op_task(ObTableLoadDag *dag, ObTableLoadOp *op, share::ObITask *&op_task);

protected:
  ObTableLoadOp *op_;
};

// start_merge
class ObTableLoadDagStartMergeTask final : public share::ObITask, public ObTableLoadDagTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadDagStartMergeTask(ObTableLoadDag *dag);
  virtual ~ObTableLoadDagStartMergeTask() = default;
  ObITaskPriority get_priority() override;
  int process() override;
};

// open
class ObTableLoadDagTableOpOpenOpTask final : public share::ObITask, public ObTableLoadDagOpTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadDagTableOpOpenOpTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadDagTableOpOpenOpTask() = default;
  int process() override;
};

// close
class ObTableLoadDagTableOpCloseOpTask final : public share::ObITask,
                                               public ObTableLoadDagOpTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadDagTableOpCloseOpTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadDagTableOpCloseOpTask() = default;
  int process() override;
};

// finish
class ObTableLoadDagFinishOpTask final : public share::ObITask, public ObTableLoadDagOpTaskBase
{
public:
  ObTableLoadDagFinishOpTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadDagFinishOpTask() = default;
  int process() override;
};

// op_type, OpType, OpTaskType
#define OB_TABLE_LOAD_DAG_OP_TASK(DEF)                                                  \
  DEF(DIRECT_WRITE_OP, ObTableLoadDirectWriteOp, ObTableLoadDirectWriteOpTask)          \
  DEF(STORE_WRITE_OP, ObTableLoadStoreWriteOp, ObTableLoadStoreWriteOpTask)             \
  DEF(PRE_SORT_WRITE_OP, ObTableLoadPreSortWriteOp, ObTableLoadPreSortWriteOpTask)      \
  DEF(MEM_SORT_OP, ObTableLoadMemSortOp, ObTableLoadMemSortOpTask)                      \
  DEF(COMPACT_DATA_OP, ObTableLoadCompactDataOp, ObTableLoadDagCompactTableOpTask)      \
  DEF(INSERT_SSTABLE_OP, ObTableLoadInsertSSTableOp, ObTableLoadDagInsertSSTableOpTask) \
  DEF(TABLE_OP_OPEN_OP, ObTableLoadTableOpOpenOp, ObTableLoadDagTableOpOpenOpTask)      \
  DEF(TABLE_OP_CLOSE_OP, ObTableLoadTableOpCloseOp, ObTableLoadDagTableOpCloseOpTask)   \
  DEF(FINISH_OP, ObTableLoadFinishOp, ObTableLoadDagFinishOpTask)

} // namespace observer
} // namespace oceanbase
