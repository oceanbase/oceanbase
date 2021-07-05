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

#ifndef OCEANBASE_SQL_SQL_ENGINE_DML_OB_MULTI_TABLE_INSERT_H_
#define OCEANBASE_SQL_SQL_ENGINE_DML_OB_MULTI_TABLE_INSERT_H_
#include "lib/allocator/ob_allocator.h"
#include "sql/engine/dml/ob_table_insert.h"
namespace oceanbase {
namespace sql {
class InsertTableInfo {
  friend class ObMultiTableInsert;

public:
  InsertTableInfo() : check_constraint_exprs_(), virtual_column_exprs_(), match_conds_exprs_(), when_conds_idx_(-1)
  {}
  virtual ~InsertTableInfo()
  {
    reset();
  }
  TO_STRING_KV(K_(when_conds_idx));
  int add_virtual_column_expr(ObColumnExpression* expr)
  {
    return ObSqlExpressionUtil::add_expr_to_list(virtual_column_exprs_, expr);
  }
  int add_check_constraint_expr(ObSqlExpression* expr)
  {
    return ObSqlExpressionUtil::add_expr_to_list(check_constraint_exprs_, expr);
  }
  int add_match_conds_exprs(ObSqlExpression* expr)
  {
    return ObSqlExpressionUtil::add_expr_to_list(match_conds_exprs_, expr);
  }
  void set_when_conds_idx(int64_t idx)
  {
    when_conds_idx_ = idx;
  }
  void reset()
  {
    check_constraint_exprs_.reset();
    virtual_column_exprs_.reset();
    match_conds_exprs_.reset();
    when_conds_idx_ = -1;
  }
  static int create_insert_table_info(common::ObIAllocator& alloc, InsertTableInfo*& insert_table_info);
  // protected:
  common::ObDList<ObSqlExpression> check_constraint_exprs_;
  common::ObDList<ObSqlExpression> virtual_column_exprs_;
  common::ObDList<ObSqlExpression> match_conds_exprs_;
  int64_t when_conds_idx_;
};
class ObTableLocation;
class ObMultiTableInsert : public ObTableInsert, public ObMultiDMLInfo {
  class ObMultiTableInsertCtx;

public:
  static const int64_t INSERT_OP = 0;
  static const int64_t DML_OP_CNT = 1;

public:
  explicit ObMultiTableInsert(common::ObIAllocator& alloc);
  virtual ~ObMultiTableInsert();

  virtual int create_operator_input(ObExecContext& ctx) const
  {
    UNUSED(ctx);
    return common::OB_SUCCESS;
  }
  virtual bool has_foreign_key() const override
  {
    return subplan_has_foreign_key();
  }
  void reset();
  int check_match_conditions(ObExprCtx& expr_ctx, const ObNewRow& row, bool have_insert_row,
      const InsertTableInfo* table_info, int64_t& pre_when_conds_idx, bool& continue_insert, bool& is_match) const;
  bool is_multi_insert_first() const
  {
    return is_multi_insert_first_;
  }
  void set_is_multi_insert_first(bool v)
  {
    is_multi_insert_first_ = v;
  }
  int calculate_virtual_column(const common::ObDList<ObSqlExpression>& calc_exprs, common::ObExprCtx& expr_ctx,
      common::ObNewRow& calc_row) const;
  int filter_row(ObExprCtx& expr_ctx, const ObNewRow& row, const common::ObDList<ObSqlExpression>& filters,
      bool& is_filtered) const;
  int filter_row_inner(ObExprCtx& expr_ctx, const ObNewRow& row, const ObISqlExpression* expr, bool& is_filtered) const;
  int init_multi_table_insert_infos(int64_t count)
  {
    return init_array_size<>(multi_table_insert_infos_, count);
  }
  int add_multi_table_insert_infos(InsertTableInfo*& insert_info);
  InsertTableInfo* get_insert_table_info(uint64_t idx)
  {
    return idx < multi_table_insert_infos_.count() ? multi_table_insert_infos_.at(idx) : NULL;
  }
  virtual bool is_multi_dml() const
  {
    return true;
  }

protected:
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int get_next_row(ObExecContext& ctx, const ObNewRow*& row) const;
  virtual int inner_close(ObExecContext& ctx) const;
  int shuffle_insert_row(ObExecContext& ctx, bool& got_row) const;
  int process_row(ObExecContext& ctx, ObMultiTableInsertCtx& insert_ctx, const ObNewRow*& insert_row) const;
  int prepare_insert_row(const ObNewRow* input_row, const DMLSubPlan& insert_dml_sub, ObNewRow& new_row) const;
  int deep_copy_rows(ObMultiTableInsertCtx*& insert_ctx, const ObNewRow& row, ObNewRow& new_row) const;

private:
  bool is_multi_insert_first_;
  common::ObFixedArray<InsertTableInfo*, common::ObIAllocator> multi_table_insert_infos_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_SQL_ENGINE_DML_OB_MULTI_TABLE_INSERT_H_ */
