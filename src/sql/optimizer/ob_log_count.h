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
      rownum_limit_expr_(NULL)
  {}
  virtual ~ObLogCount() {}
  virtual int generate_link_sql_post(GenLinkStmtPostContext &link_ctx) override;
  inline ObRawExpr *get_rownum_limit_expr() const { return rownum_limit_expr_; }
  inline void set_rownum_limit_expr(ObRawExpr *rownum_limit_expr) { rownum_limit_expr_ = rownum_limit_expr; }
  virtual int est_cost() override;
  virtual int est_width() override;
  virtual int re_est_cost(EstimateCostInfo &param, double &card, double &cost) override;
  int inner_est_cost(double &child_card, 
                     double &child_cost, 
                     bool need_re_est_child_cost, 
                     double sel, 
                     double &op_cost);
  virtual int print_my_plan_annotation(char *buf,
                                       int64_t &buf_len,
                                       int64_t &pos,
                                       ExplainType type);
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
private:
  ObRawExpr *rownum_limit_expr_;
  DISALLOW_COPY_AND_ASSIGN(ObLogCount);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OB_SQL_OPTIMIZER_OB_LOG_COUNT_H_ */
