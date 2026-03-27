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
class ObTableLoadStoreWriteOp;

// start
class ObTableLoadStoreWriteOpTask final : public share::ObITask, public ObTableLoadDagOpTaskBase
{
  using ObTableLoadDagTaskBase::dag_;

public:
  ObTableLoadStoreWriteOpTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadStoreWriteOpTask() = default;
  int process() override;
};

// finish
class ObTableLoadStoreWriteOpFinishTask final : public share::ObITask,
                                                public ObTableLoadDagOpTaskBase
{
public:
  ObTableLoadStoreWriteOpFinishTask(ObTableLoadDag *dag, ObTableLoadOp *op);
  virtual ~ObTableLoadStoreWriteOpFinishTask() = default;
  int process() override;
  static void reset_op(ObTableLoadStoreWriteOp *op);
};

} // namespace observer
} // namespace oceanbase
