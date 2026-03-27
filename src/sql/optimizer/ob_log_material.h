/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_LOG_MATERIAL_H_
#define OCEANBASE_SQL_OB_LOG_MATERIAL_H_

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase
{
namespace sql
{
  template<typename R, typename C>
  class PlanVisitor;
  class ObLogMaterial : public ObLogicalOperator
  {
  public:
    ObLogMaterial(ObLogPlan &plan) : ObLogicalOperator(plan)
    {}
    virtual ~ObLogMaterial() {}
    virtual int est_cost() override;
    virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
    virtual bool is_block_op() const override { return true; }
  private:
    DISALLOW_COPY_AND_ASSIGN(ObLogMaterial);
  };
}
}



#endif // OCEANBASE_SQL_OB_LOG_MATERIAL_H_
