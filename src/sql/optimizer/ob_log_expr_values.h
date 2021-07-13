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

#ifndef _OB_LOG_EXPR_VALUES_H
#define _OB_LOG_EXPR_VALUES_H
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase {
namespace sql {
class ObLogExprValues : public ObLogicalOperator {
public:
  ObLogExprValues(ObLogPlan& plan) : ObLogicalOperator(plan), need_columnlized_(false)
  {}
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int copy_without_child(ObLogicalOperator*& out)
  {
    return clone(out);
  }
  void set_need_columnlized(bool need_columnlized)
  {
    need_columnlized_ = need_columnlized;
  }
  bool is_need_columnlized() const
  {
    return need_columnlized_;
  }
  int add_values_expr(const common::ObIArray<ObRawExpr*>& value_exprs);
  int add_str_values_array(const common::ObIArray<ObRawExpr*>& expr);

  const common::ObIArray<ObRawExpr*>& get_value_exprs() const
  {
    return value_exprs_;
  }
  common::ObIArray<ObRawExpr*>& get_value_exprs()
  {
    return value_exprs_;
  }
  const common::ObIArray<ObStrValues>& get_str_values_array() const
  {
    return str_values_array_;
  }
  int check_range_param_continuous(bool& use_range_param) const;
  int get_value_param_range(int64_t row_index, int64_t& param_idx_start, int64_t& param_idx_end) const;
  virtual int est_cost() override;
  virtual int compute_op_ordering() override;
  virtual int compute_equal_set() override;
  virtual int compute_table_set() override;
  virtual int compute_fd_item_set() override;
  virtual int compute_one_row_info() override;
  virtual int allocate_dummy_output();
  uint64_t hash(uint64_t seed) const;
  virtual int allocate_expr_post(ObAllocExprContext& ctx) override;
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const override;

private:
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);

private:
  bool need_columnlized_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> value_exprs_;
  // only engine 3.0 used
  common::ObSEArray<ObStrValues, 4, common::ModulePageAllocator, true> str_values_array_;
  DISALLOW_COPY_AND_ASSIGN(ObLogExprValues);
};
}  // namespace sql
}  // namespace oceanbase
#endif
