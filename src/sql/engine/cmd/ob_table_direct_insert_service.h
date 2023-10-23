/**
 * Copyright (c) 2023 OceanBase
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

#include "lib/ob_define.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;
}
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
  static int open_task(const uint64_t table_id,
                       const int64_t task_id,
                       observer::ObTableLoadTableCtx *&table_ctx);
  static int close_task(const uint64_t table_id,
                        const int64_t task_id,
                        observer::ObTableLoadTableCtx *table_ctx,
                        const int error_code = OB_SUCCESS);
};
} // namespace sql
} // namespace oceanbase