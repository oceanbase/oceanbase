/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace share
{
class ObITask;
} // namespace share
namespace observer
{
class ObTableLoadDagExecCtx;
class ObTableLoadDag;
class ObTableLoadOp;
class ObTableLoadTableOp;

class ObTableLoadDagGenerator
{
public:
  static int generate(ObTableLoadDagExecCtx &dag_exec_ctx);

private:
  // Generate a topological sort of the dependency graph
  static int generate_table_op_topological_order(ObTableLoadTableOp *root_op,
                                                 ObIArray<ObTableLoadTableOp *> &table_ops);

  static int table_op_list_to_executable_op_list(const ObIArray<ObTableLoadTableOp *> &table_ops,
                                                 ObIArray<ObTableLoadOp *> &executable_ops);
  static int executable_op_list_to_dag_task_list(const ObIArray<ObTableLoadOp *> &executable_ops,
                                                 ObTableLoadDag *dag,
                                                 ObIArray<share::ObITask *> &dag_tasks);
};

} // namespace observer
} // namespace oceanbase
