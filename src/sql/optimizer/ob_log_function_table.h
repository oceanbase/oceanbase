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

namespace oceanbase {
namespace sql {
class ObLogFunctionTable : public ObLogicalOperator {
public:
  ObLogFunctionTable(ObLogPlan& plan) : ObLogicalOperator(plan), table_id_(OB_INVALID_ID), value_expr_(NULL)
  {}

  virtual ~ObLogFunctionTable()
  {}

  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int copy_without_child(ObLogicalOperator*& out);
  void add_values_expr(ObRawExpr* expr)
  {
    value_expr_ = expr;
  }
  const ObRawExpr* get_value_expr() const
  {
    return value_expr_;
  }
  ObRawExpr* get_value_expr()
  {
    return value_expr_;
  }
  virtual int est_cost() override;
  virtual int compute_op_ordering() override;
  virtual int compute_equal_set() override;
  virtual int compute_fd_item_set() override;
  virtual int compute_table_set() override;
  uint64_t hash(uint64_t seed) const;
  int generate_access_exprs(ObIArray<ObRawExpr*>& access_exprs) const;
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  virtual int allocate_expr_post(ObAllocExprContext& ctx) override;
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker) override;
  void set_table_id(uint64_t table_id)
  {
    table_id_ = table_id;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;

private:
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);

private:
  uint64_t table_id_;
  ObRawExpr* value_expr_;
  DISALLOW_COPY_AND_ASSIGN(ObLogFunctionTable);
};
}  // namespace sql
}  // namespace oceanbase
#endif
