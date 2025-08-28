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

#ifndef _OB_LOG_UNPIVOT_H_
#define _OB_LOG_UNPIVOT_H_
#include "sql/optimizer/ob_logical_operator.h"


namespace oceanbase
{
namespace sql
{
class ObLogUnpivot : public ObLogicalOperator
{
public:
  ObLogUnpivot(ObLogPlan &plan)
  : ObLogicalOperator(plan),
    origin_exprs_(),
    label_exprs_(),
    value_exprs_(),
    is_include_null_(false)
  {}

  ~ObLogUnpivot() {}

  inline common::ObIArray<ObRawExpr *> &get_origin_exprs() { return origin_exprs_; }
  inline common::ObIArray<ObRawExpr *> &get_label_exprs() { return label_exprs_; }
  inline common::ObIArray<ObRawExpr *> &get_value_exprs() { return value_exprs_; }
  int set_origin_exprs(const common::ObIArray<ObRawExpr *> &origin_exprs) { return origin_exprs_.assign(origin_exprs); }
  int set_label_exprs(const common::ObIArray<ObRawExpr *> &label_exprs) { return label_exprs_.assign(label_exprs); }
  int set_value_exprs(const common::ObIArray<ObRawExpr *> &value_exprs) { return value_exprs_.assign(value_exprs); }

  inline bool is_include_null() { return is_include_null_; }
  inline bool is_exclude_null() { return !is_include_null_; }
  inline void set_include_null(bool is_include_null) { is_include_null_ = is_include_null; }
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  virtual int get_plan_item_info(PlanText &plan_text, 
                                ObSqlPlanItem &plan_item) override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;

  virtual int est_cost() override;
  virtual int est_width() override;
  virtual int compute_op_ordering() override;
  virtual int compute_fd_item_set() override;
  virtual int compute_one_row_info() override;
  virtual uint64_t hash(uint64_t seed) const override;
  virtual int compute_sharding_info() override;

  VIRTUAL_TO_STRING_KV(K_(origin_exprs), K_(label_exprs), K_(value_exprs), K_(is_include_null));

private:
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> origin_exprs_;
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> label_exprs_;
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> value_exprs_;
  bool is_include_null_;
  DISALLOW_COPY_AND_ASSIGN(ObLogUnpivot);
};

} // end of namespace sql
} // end of namespace oceanbase


#endif /* OB_LOG_SUBQUERY_SCAN_H_ */
