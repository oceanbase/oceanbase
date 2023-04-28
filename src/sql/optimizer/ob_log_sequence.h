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

#ifndef OCEANBASE_SQL_OB_LOG_SEQUENCE_H
#define OCEANBASE_SQL_OB_LOG_SEQUENCE_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_set.h"
namespace oceanbase
{
namespace sql
{
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
