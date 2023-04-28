// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

#pragma once

#include "lib/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObPhysicalPlan;
class ObOptimizerContext;
class ObDMLStmt;

class ObTableDirectInsertService
{
public:
  static bool is_direct_insert(const ObPhysicalPlan &phy_plan);
  // all insert-tasks within an insert into select clause are wrapped by a single direct insert instance
  static int start_direct_insert(ObExecContext &ctx, ObPhysicalPlan &plan);
  static int commit_direct_insert(ObExecContext &ctx, ObPhysicalPlan &plan);
  static int finish_direct_insert(ObExecContext &ctx, ObPhysicalPlan &plan);
  // each insert-task is processed in a single thread and is wrapped by a table load trans
  static int open_task(const uint64_t table_id, const int64_t task_id);
  static int close_task(const uint64_t table_id,
                   const int64_t task_id,
                   const int error_code = OB_SUCCESS);
};
} // namespace sql
} // namespace oceanbase