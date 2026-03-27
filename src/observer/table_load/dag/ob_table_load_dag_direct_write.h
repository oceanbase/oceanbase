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
class ObTableLoadDirectWriteOp;

// start
class ObTableLoadDirectWriteOpTask final : public share::ObITask, public ObTableLoadDagOpTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadDirectWriteOpTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadDirectWriteOpTask() = default;
  int process() override;
};

// finish
class ObTableLoadDirectWriteOpFinishTask final : public share::ObITask,
                                                 public ObTableLoadDagOpTaskBase
{
public:
  ObTableLoadDirectWriteOpFinishTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadDirectWriteOpFinishTask() = default;
  int process() override;
  static void reset_op(ObTableLoadDirectWriteOp *op);
};

} // namespace observer
} // namespace oceanbase
