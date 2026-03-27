/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_LOG_SEQUENCE_H
#define OCEANBASE_SQL_OB_LOG_SEQUENCE_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_set.h"
namespace oceanbase
{
namespace sql
{
  template<typename R, typename C>
  class PlanVisitor;
  class ObLogSequence : public ObLogicalOperator
  {
  private:
    typedef common::ObSEArray<uint64_t, 4> SequenceIdArray;
  public:
    ObLogSequence(ObLogPlan &plan) : ObLogicalOperator(plan) {}
    virtual ~ObLogSequence() {}
    virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
    virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
    const common::ObIArray<uint64_t> &get_sequence_ids() const
    { return nextval_seq_ids_; }
    common::ObIArray<uint64_t> &get_sequence_ids()
    { return nextval_seq_ids_; }
    virtual int est_cost() override;
    virtual int est_width() override;
    virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
    virtual int compute_op_parallel_and_server_info() override;
  private:
    SequenceIdArray nextval_seq_ids_;
  };
}
}
#endif // OCEANBASE_SQL_OB_LOG_SEQUENCE_H
