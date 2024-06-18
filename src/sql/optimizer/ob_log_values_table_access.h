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

#ifndef _OB_LOG_VALUES_TABLE_ACCESS_H
#define _OB_LOG_VALUES_TABLE_ACCESS_H
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{
class ObLogValuesTableAccess : public ObLogicalOperator
  {
  public:
    ObLogValuesTableAccess(ObLogPlan &plan)
        : ObLogicalOperator(plan),
          table_def_(NULL),
          table_name_(),
          table_id_(common::OB_INVALID_ID),
          values_path_(NULL)
    {}
    virtual ~ObLogValuesTableAccess() {}
    virtual int est_cost() override;
    virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost);
    virtual int compute_equal_set() override;
    virtual int compute_table_set() override;
    virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
    virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
    virtual int allocate_expr_post(ObAllocExprContext &ctx) override;
    virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
    virtual int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item) override;
    int mark_probably_local_exprs();
    int allocate_dummy_output();

    const common::ObIArray<ObColumnRefRawExpr *> &get_column_exprs() const { return column_exprs_; }
    common::ObIArray<ObColumnRefRawExpr *> &get_column_exprs() { return column_exprs_; }
    inline const ObValuesTableDef *get_values_table_def() { return table_def_; }
    inline void set_values_table_def(ObValuesTableDef *table_def) { table_def_ = table_def; }
    inline common::ObString &get_table_name() { return table_name_; }
    inline const common::ObString &get_table_name() const { return table_name_; }
    inline void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }
    inline uint64_t get_table_id() const { return table_id_; }
    inline void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
    inline void set_values_path(ValuesTablePath *values_path) { values_path_ = values_path; }
    inline const ValuesTablePath *get_values_path() { return values_path_; }
  private:

  private:
    common::ObSEArray<ObColumnRefRawExpr*, 4, common::ModulePageAllocator, true> column_exprs_;
    ObValuesTableDef *table_def_;
    common::ObString table_name_;
    uint64_t table_id_;
    ValuesTablePath *values_path_;
    DISALLOW_COPY_AND_ASSIGN(ObLogValuesTableAccess);
  };
}
}
#endif
