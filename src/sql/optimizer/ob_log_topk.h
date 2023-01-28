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

#ifndef OCEANBASE_SQL_OB_LOG_TOPK_H
#define OCEANBASE_SQL_OB_LOG_TOPK_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_set.h"
namespace oceanbase
{
namespace sql
{
  class ObLogTopk : public ObLogicalOperator
  {
  public:
    ObLogTopk(ObLogPlan &plan)
        : ObLogicalOperator(plan),
          minimum_row_count_(0),
          topk_precision_(0),
          topk_limit_count_(NULL),
          topk_limit_offset_(NULL)
    {}
    virtual ~ObLogTopk() {}
    inline ObRawExpr *get_topk_limit_count() const { return topk_limit_count_; }
    inline ObRawExpr *get_topk_limit_offset() const { return topk_limit_offset_;}
    int set_topk_params(ObRawExpr *limit_count,
                        ObRawExpr *limit_offset,
                        int64_t minimum_row_cuont,
                        int64_t topk_precision);
    virtual int est_cost() override;
    virtual int est_width() override;
    virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
    inline int64_t get_minimum_row_count() const {return minimum_row_count_;}
    inline int64_t get_topk_precision() const {return topk_precision_;}
    int get_topk_output_exprs(ObIArray<ObRawExpr *> &output_exprs);
    virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
  private:
    int64_t minimum_row_count_;
    int64_t topk_precision_;
    ObRawExpr *topk_limit_count_;
    ObRawExpr *topk_limit_offset_;
  };
}
}
#endif // OCEANBASE_SQL_OB_LOG_TOPK_H
