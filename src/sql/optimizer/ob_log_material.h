/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OB_LOG_MATERIAL_H_
#define OCEANBASE_SQL_OB_LOG_MATERIAL_H_

#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase {
namespace sql {
class ObLogMaterial : public ObLogicalOperator {
public:
  ObLogMaterial(ObLogPlan& plan) : ObLogicalOperator(plan)
  {}
  virtual ~ObLogMaterial()
  {}
  virtual int copy_without_child(ObLogicalOperator*& out)
  {
    return clone(out);
  }
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  int allocate_exchange(AllocExchContext* ctx, ObExchangeInfo& exch_info) override;
  virtual int est_cost() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;
  virtual bool is_block_op() const override
  {
    return true;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogMaterial);
};
}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_MATERIAL_H_
