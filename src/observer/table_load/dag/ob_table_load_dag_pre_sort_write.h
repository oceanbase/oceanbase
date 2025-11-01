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
