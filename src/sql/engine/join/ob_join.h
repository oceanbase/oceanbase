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

#ifndef OCEANBASE_SQL_ENGINE_JOIN_JOIN_H_
#define OCEANBASE_SQL_ENGINE_JOIN_JOIN_H_

#include "lib/utility/ob_tracepoint.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_double_children_phy_operator.h"
#include "sql/engine/connect_by/ob_connect_by_utility.h"

namespace oceanbase {
namespace sql {
class ObHashJoin;
typedef common::ObFixedArray<int64_t, common::ObIAllocator> ConnectByRowDesc;
class ObJoin : public ObDoubleChildrenPhyOperator {
  OB_UNIS_VERSION_V(1);

public:
  const static int64_t DUMMY_OUPUT;
  const static int64_t UNUSED_POS;
  class ObJoinCtx : public ObPhyOperatorCtx {
    friend class ObJoin;
    friend class ObHashJoin;

  public:
    explicit ObJoinCtx(ObExecContext& ctx);
    virtual ~ObJoinCtx()
    {}
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }

  protected:
    const common::ObNewRow* left_row_;
    const common::ObNewRow* right_row_;
    const common::ObNewRow* last_left_row_;
    const common::ObNewRow* last_right_row_;
    bool left_row_joined_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObJoinCtx);
  };

public:
  explicit ObJoin(common::ObIAllocator& alloc);
  virtual ~ObJoin();
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& exec_ctx) const;
  virtual int set_join_type(const ObJoinType join_type);
  virtual int add_equijoin_condition(ObSqlExpression* expr);
  virtual int add_other_join_condition(ObSqlExpression* expr);
  inline int get_other_join_condition_count()
  {
    return other_join_conds_.get_size();
  }
  inline ObJoinType get_join_type() const
  {
    return join_type_;
  }
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  int init_op_ctx(ObExecContext& ctx) const;
  int set_pump_row_desc(const common::ObIArray<int64_t>& pump_row_desc);
  int set_root_row_desc(const common::ObIArray<int64_t>& root_row_desc);
  int init_pseudo_column_row_desc();
  int add_pseudo_column(const ObPseudoColumnRawExpr* expr, int64_t pos);
  int add_connect_by_prior_expr(ObSqlExpression* expr)
  {
    return ObSqlExpressionUtil::add_expr_to_list(connect_by_prior_exprs_, expr);
  }
  int add_sort_siblings_expr(ObSqlExpression* expr)
  {
    return ObSqlExpressionUtil::add_expr_to_list(sort_siblings_exprs_, expr);
  }
  int add_connect_by_root_expr(ObSqlExpression* expr)
  {
    return ObSqlExpressionUtil::add_expr_to_list(connect_by_root_exprs_, expr);
  }
  const common::ObDList<ObSqlExpression>* get_connect_by_prior_exprs() const
  {
    return &connect_by_prior_exprs_;
  }
  const common::ObDList<ObSqlExpression>* get_sort_siblings_exprs() const
  {
    return &sort_siblings_exprs_;
  }
  const common::ObDList<ObSqlExpression>* get_connect_by_root_exprs() const
  {
    return &connect_by_root_exprs_;
  }
  const ConnectByRowDesc* get_pump_row_desc() const
  {
    return &pump_row_desc_;
  }
  const ConnectByRowDesc* get_root_row_desc() const
  {
    return &root_row_desc_;
  }
  const ConnectByRowDesc* get_pseudo_column_row_desc() const
  {
    return &pseudo_column_row_desc_;
  }
  const ObIArray<ObSortColumn>* get_sort_siblings_columns() const
  {
    return &sort_siblings_columns_;
  }
  void set_nocycle()
  {
    is_nocycle_ = true;
  }
  bool get_nocycle() const
  {
    return is_nocycle_;
  }
  int set_sort_siblings_columns(const common::ObIArray<ObSortColumn>& sort_siblings_columns);
  TO_STRING_KV(N_ID, id_, N_COLUMN_COUNT, column_count_, N_PROJECTOR,
      common::ObArrayWrap<int32_t>(projector_, projector_size_), N_FILTER_EXPRS, filter_exprs_, N_CALC_EXPRS,
      calc_exprs_, N_JOIN_TYPE, ob_join_type_str(join_type_), N_JOIN_EQ_COND, equal_join_conds_, N_JOIN_OTHER_COND,
      other_join_conds_);

protected:
  inline bool need_left_join() const;
  inline bool need_right_join() const;
  int join_rows(ObJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int left_join_rows(ObJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int right_join_rows(ObJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int calc_equal_conds(
      ObJoinCtx& join_ctx, int64_t& equal_cmp, const common::ObIArray<int64_t>* merge_directions = NULL) const;
  int calc_other_conds(ObJoinCtx& join_ctx, bool& is_match) const;

  virtual int get_left_row(ObJoinCtx& join_ctx) const;
  virtual int get_right_row(ObJoinCtx& join_ctx) const;
  virtual int get_next_left_row(ObJoinCtx& join_ctx) const;
  virtual int get_next_right_row(ObJoinCtx& join_ctx) const;

  inline int get_last_left_row(ObJoinCtx& join_ctx) const;
  inline int get_last_right_row(ObJoinCtx& join_ctx) const;
  inline int save_left_row(ObJoinCtx& join_ctx) const;
  inline int save_right_row(ObJoinCtx& join_ctx) const;
  virtual int inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const = 0;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& exec_ctx) const;

protected:
  ObJoinType join_type_;
  common::ObDList<ObSqlExpression> equal_join_conds_;
  common::ObDList<ObSqlExpression> other_join_conds_;
  ConnectByRowDesc pump_row_desc_;           // to gen pump row
  ConnectByRowDesc root_row_desc_;           // to gen root row
  ConnectByRowDesc pseudo_column_row_desc_;  // desc pseudo_column's location in outputrow
  common::ObFixedArray<ObSortColumn, common::ObIAllocator> sort_siblings_columns_;  // for order siblings
  common::ObDList<ObSqlExpression> connect_by_prior_exprs_;                         // for cycle check
  common::ObDList<ObSqlExpression> sort_siblings_exprs_;                            // for order siblings
  common::ObDList<ObSqlExpression> connect_by_root_exprs_;                          // calc connect_by_root expr
  bool is_nocycle_;                                                                 // deal with connect loop
private:
  DISALLOW_COPY_AND_ASSIGN(ObJoin);
};

inline bool ObJoin::need_left_join() const
{
  return (LEFT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_);
}

inline bool ObJoin::need_right_join() const
{
  return (RIGHT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_);
}

inline int ObJoin::get_last_left_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(join_ctx.last_left_row_)) {
    ret = common::OB_BAD_NULL_ERROR;
  } else {
    join_ctx.left_row_ = join_ctx.last_left_row_;
    join_ctx.last_left_row_ = NULL;
  }
  return ret;
}

inline int ObJoin::get_last_right_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(join_ctx.last_right_row_)) {
    ret = common::OB_BAD_NULL_ERROR;
  } else {
    join_ctx.right_row_ = join_ctx.last_right_row_;
    join_ctx.last_right_row_ = NULL;
  }
  return ret;
}

inline int ObJoin::save_left_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  if (!OB_ISNULL(join_ctx.last_left_row_)) {
    ret = common::OB_BAD_NULL_ERROR;
  } else {
    join_ctx.last_left_row_ = join_ctx.left_row_;
  }
  return ret;
}

inline int ObJoin::save_right_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  if (!OB_ISNULL(join_ctx.last_right_row_)) {
    ret = common::OB_BAD_NULL_ERROR;
  } else {
    join_ctx.last_right_row_ = join_ctx.right_row_;
  }
  return ret;
}

const char* join_type_str(const ObJoinType type);
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_JOIN_JOIN_H_ */
