/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "observer/table_load/dag/ob_table_load_dag_task.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadPreSortWriteOp;

// start
class ObTableLoadPreSortWriteOpTask final : public share::ObITask, public ObTableLoadDagOpTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadPreSortWriteOpTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadPreSortWriteOpTask() = default;
  int process() override;
};

// finish
class ObTableLoadPreSortWriteOpFinishTask final : public share::ObITask,
                                                  public ObTableLoadDagOpTaskBase
{
public:
  ObTableLoadPreSortWriteOpFinishTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadPreSortWriteOpFinishTask() = default;
  int process() override;
  static void reset_op(ObTableLoadPreSortWriteOp *op);
};

} // namespace observer
} // namespace oceanbase
