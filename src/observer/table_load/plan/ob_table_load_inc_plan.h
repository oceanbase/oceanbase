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
class ObTableLoadIncPlan : public ObTableLoadPlan
{
public:
  ObTableLoadIncPlan(ObTableLoadStoreCtx *store_ctx) : ObTableLoadPlan(store_ctx) {}
  virtual ~ObTableLoadIncPlan() = default;
  static int create_plan(ObTableLoadStoreCtx *store_ctx, ObIAllocator &allocator,
                         ObTableLoadPlan *&plan);
};

} // namespace observer
} // namespace oceanbase
