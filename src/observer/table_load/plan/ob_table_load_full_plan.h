/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "observer/table_load/plan/ob_table_load_plan.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadFullPlan final : public ObTableLoadPlan
{
public:
  ObTableLoadFullPlan(ObTableLoadStoreCtx *store_ctx) : ObTableLoadPlan(store_ctx) {}
  virtual ~ObTableLoadFullPlan() = default;
  int generate() override;

  static int create_plan(ObTableLoadStoreCtx *store_ctx, ObIAllocator &allocator,
                         ObTableLoadPlan *&plan);

private:
  int alloc_channel(ObTableLoadTableOp *up_table_op, ObTableLoadTableOp *down_table_op,
                    ObTableLoadTableChannel *&table_channel) override
  {
    return OB_ERR_UNEXPECTED;
  }
};

} // namespace observer
} // namespace oceanbase
