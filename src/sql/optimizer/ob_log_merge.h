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

#ifndef _OB_LOG_MERGE_H
#define _OB_LOG_MERGE_H 1
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_insert.h"
namespace oceanbase {
namespace sql {
class ObLogMerge : public ObLogInsert {
public:
  ObLogMerge(ObLogPlan& plan)
      : ObLogInsert(plan),
        match_condition_exprs_(NULL),
        insert_condition_exprs_(NULL),
        update_condition_exprs_(NULL),
        delete_condition_exprs_(NULL),
        value_vector_(NULL),
        rowkey_exprs_(NULL)
  {}
  virtual ~ObLogMerge()
  {}
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  void set_match_condition(const common::ObIArray<ObRawExpr*>* expr)
  {
    match_condition_exprs_ = expr;
  }
  const common::ObIArray<ObRawExpr*>* get_match_condition() const
  {
    return match_condition_exprs_;
  }

  void set_insert_condition(const common::ObIArray<ObRawExpr*>* expr)
  {
    insert_condition_exprs_ = expr;
  }
  const common::ObIArray<ObRawExpr*>* get_insert_condition() const
  {
    return insert_condition_exprs_;
  }

  void set_update_condition(const common::ObIArray<ObRawExpr*>* expr)
  {
    update_condition_exprs_ = expr;
  }
  const common::ObIArray<ObRawExpr*>* get_update_condition() const
  {
    return update_condition_exprs_;
  }

  void set_delete_condition(const common::ObIArray<ObRawExpr*>* expr)
  {
    delete_condition_exprs_ = expr;
  }
  const common::ObIArray<ObRawExpr*>* get_delete_condition() const
  {
    return delete_condition_exprs_;
  }

  void set_rowkey_exprs(const common::ObIArray<ObRawExpr*>* expr)
  {
    rowkey_exprs_ = expr;
  }
  const common::ObIArray<ObRawExpr*>* get_rowkey_exprs() const
  {
    return rowkey_exprs_;
  }

  void set_value_vector(const common::ObIArray<ObRawExpr*>* expr)
  {
    value_vector_ = expr;
  }
  int add_delete_exprs_to_ctx(ObAllocExprContext& ctx);
  int add_all_table_assignments_to_ctx(ObAllocExprContext& ctx);
  virtual uint64_t hash(uint64_t seed) const;
  const char* get_name() const;

  int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;
  int add_merge_exprs_to_ctx(ObAllocExprContext& ctx, const ObIArray<ObRawExpr*>& exprs);
  int classify_merge_subquery_expr(const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& subquery_exprs,
      ObIArray<ObRawExpr*>& non_subquery_exprs);

private:
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker) override;
  int add_all_source_table_columns_to_ctx(ObAllocExprContext& ctx);
  DISALLOW_COPY_AND_ASSIGN(ObLogMerge);

private:
  const common::ObIArray<ObRawExpr*>* match_condition_exprs_;
  const common::ObIArray<ObRawExpr*>* insert_condition_exprs_;
  const common::ObIArray<ObRawExpr*>* update_condition_exprs_;
  const common::ObIArray<ObRawExpr*>* delete_condition_exprs_;
  const common::ObIArray<ObRawExpr*>* value_vector_;
  const common::ObIArray<ObRawExpr*>* rowkey_exprs_;
};
}  // namespace sql
}  // namespace oceanbase

#endif
