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

#ifndef _OB_LOG_FUNCTION_TABLE_H
#define _OB_LOG_FUNCTION_TABLE_H
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{
class ObLogFunctionTable : public ObLogicalOperator
{
public:
  ObLogFunctionTable(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        table_id_(OB_INVALID_ID),
        value_expr_(NULL),
        table_name_(),
        access_exprs_() { }
  virtual ~ObLogFunctionTable() {}
  void add_values_expr(ObRawExpr* expr) { value_expr_ = expr; }
  const ObRawExpr* get_value_expr() const { return value_expr_; }
  ObRawExpr* get_value_expr() { return value_expr_; }
  virtual uint64_t hash(uint64_t seed) const override;
  int generate_access_exprs();
  ObIArray<ObRawExpr*> &get_access_exprs() { return access_exprs_; }
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  virtual int allocate_expr_post(ObAllocExprContext &ctx) override;
  void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  uint64_t get_table_id() const { return table_id_; }
  /**
   *  Get table name
   */
  inline common::ObString &get_table_name() { return table_name_; }
  inline const common::ObString &get_table_name() const { return table_name_; }
  inline void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
private:
  uint64_t table_id_;
  ObRawExpr* value_expr_;
  common::ObString table_name_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> access_exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObLogFunctionTable);
};
}
}
#endif
