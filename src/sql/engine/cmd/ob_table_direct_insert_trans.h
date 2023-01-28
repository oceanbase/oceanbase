// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <yuya.yu@oceanbase.com>

#pragma once

#include "lib/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObPhysicalPlan;

class ObTableDirectInsertTrans
{
public:
  // all insert-tasks within an insert into select clause are wrapped by a single direct insert instance
  static int try_start_direct_insert(ObExecContext &ctx, ObPhysicalPlan &plan);
  static int try_finish_direct_insert(ObExecContext &ctx, ObPhysicalPlan &plan);
  // each insert-task is processed in a single thread and is wrapped by a table load trans
  static int start_trans(const uint64_t table_id, const int64_t task_id);
  static int finish_trans(const uint64_t table_id, const int64_t task_id);
};
} // namespace sql
} // namespace oceanbase