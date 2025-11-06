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
