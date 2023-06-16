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

#ifndef OCEANBASE_SQL_OB_LOG_LIMIT_H
#define OCEANBASE_SQL_OB_LOG_LIMIT_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_set.h"
namespace oceanbase
{
namespace sql
{
  class ObLogLimit : public ObLogicalOperator
  {
  public:
    ObLogLimit(ObLogPlan &plan)
        : ObLogicalOperator(plan),
          is_calc_found_rows_(false),
          is_top_limit_(false),
          is_fetch_with_ties_(false),
          limit_expr_(NULL),
          offset_expr_(NULL),
          percent_expr_(NULL)
    {}
    virtual ~ObLogLimit() {}
    virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
    inline ObRawExpr *get_limit_expr() const { return limit_expr_; }
    inline ObRawExpr *get_offset_expr() const { return offset_expr_;}
    inline ObRawExpr *get_percent_expr() const { return percent_expr_;}
    inline void set_limit_expr(ObRawExpr *limit_expr) { limit_expr_ = limit_expr; }
    inline void set_offset_expr(ObRawExpr *offset_expr) { offset_expr_ = offset_expr; }
    inline void set_percent_expr(ObRawExpr *percent_expr) { percent_expr_ = percent_expr; }
    inline int set_ties_ordering(const common::ObIArray<OrderItem> &order_items)
    {
      return order_items_.assign(order_items);
    }
    inline ObIArray<OrderItem> &get_ties_ordering() { return order_items_; }
    void set_is_calc_found_rows(bool found_rows)
    {
      is_calc_found_rows_ = found_rows;
    }
    int get_is_calc_found_rows()
    {
      return is_calc_found_rows_;
    }
    void set_top_limit(bool is_top_limit)
    {
      is_top_limit_ = is_top_limit;
    }
    inline bool is_top_limit()
    {
      return is_top_limit_;
    }
    virtual int est_cost() override;
    virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
    int check_output_dep_specific(ObRawExprCheckDep &checker);
    void set_fetch_with_ties(bool is_fetch_with_ties)
    {
      is_fetch_with_ties_ = is_fetch_with_ties;
    }
    inline bool is_fetch_with_ties()
    {
      return is_fetch_with_ties_;
    }
    virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
    virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
  private:
    bool is_calc_found_rows_;
    bool is_top_limit_;
    bool is_fetch_with_ties_;
    ObRawExpr *limit_expr_;
    ObRawExpr *offset_expr_;
    ObRawExpr *percent_expr_;
    ObSEArray<OrderItem, 4, common::ModulePageAllocator, true> order_items_;

  };
}
}
#endif // OCEANBASE_SQL_OB_LOG_LIMIT_H
