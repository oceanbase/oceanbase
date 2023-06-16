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

#ifndef OB_SQL_OPTIMIZER_OB_LOG_COUNT_H_
#define OB_SQL_OPTIMIZER_OB_LOG_COUNT_H_

#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{

class ObLogCount: public ObLogicalOperator
{
public:
	ObLogCount(ObLogPlan &plan)
    : ObLogicalOperator(plan),
      rownum_limit_expr_(NULL),
      rownum_expr_(NULL)
  {}
  virtual ~ObLogCount() {}
  inline ObRawExpr *get_rownum_limit_expr() const { return rownum_limit_expr_; }
  inline void set_rownum_limit_expr(ObRawExpr *rownum_limit_expr) { rownum_limit_expr_ = rownum_limit_expr; }
  inline ObRawExpr *get_rownum_expr() const { return rownum_expr_; }
  inline void set_rownum_expr(ObRawExpr *rownum_expr) { rownum_expr_ = rownum_expr; }
  virtual int est_cost() override;
  virtual int est_width() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  int inner_est_cost(double &child_card, 
                     double &child_cost, 
                     bool need_re_est_child_cost, 
                     double sel, 
                     double &op_cost);
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;

  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
private:
  ObRawExpr *rownum_limit_expr_;
  ObRawExpr *rownum_expr_;
  DISALLOW_COPY_AND_ASSIGN(ObLogCount);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OB_SQL_OPTIMIZER_OB_LOG_COUNT_H_ */
