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
class ObTableLoadMemSortOp;

// start
class ObTableLoadMemSortOpTask final : public share::ObITask, public ObTableLoadDagOpTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadMemSortOpTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadMemSortOpTask() = default;
  int process() override;
};

// finish
class ObTableLoadMemSortOpFinishTask final : public share::ObITask, public ObTableLoadDagOpTaskBase
{
public:
  ObTableLoadMemSortOpFinishTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadMemSortOpFinishTask() = default;
  int process() override;
  static void reset_op(ObTableLoadMemSortOp *op);
};

} // namespace observer
} // namespace oceanbase
