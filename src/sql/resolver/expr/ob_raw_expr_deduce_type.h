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

#ifndef _OB_RAW_EXPR_DEDUCE_TYPE_H
#define _OB_RAW_EXPR_DEDUCE_TYPE_H 1
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/container/ob_iarray.h"
#include "common/ob_accuracy.h"
#include "share/ob_i_sql_expression.h"
namespace oceanbase
{
namespace sql
{
class ObRawExprDeduceType: public ObRawExprVisitor
{
public:
  ObRawExprDeduceType(const ObSQLSessionInfo *my_session,
                      bool solidify_session_vars,
                      const ObLocalSessionVar *local_vars,
                      int64_t local_vars_id)
    : ObRawExprVisitor(),
      my_session_(my_session),
      alloc_(),
      expr_factory_(NULL),
      my_local_vars_(local_vars),
      local_vars_id_(local_vars_id),
      solidify_session_vars_(solidify_session_vars)
  {}
  virtual ~ObRawExprDeduceType()
  {
    alloc_.reset();
  }
  void set_expr_factory(ObRawExprFactory *expr_factory)
  {
    expr_factory_ = expr_factory;
  }
  int deduce(ObRawExpr &expr);
  /// interface of ObRawExprVisitor
  virtual int visit(ObConstRawExpr &expr);
  virtual int visit(ObVarRawExpr &expr);
  virtual int visit(ObOpPseudoColumnRawExpr &expr);
  virtual int visit(ObExecParamRawExpr &expr);
  virtual int visit(ObQueryRefRawExpr &expr);
  virtual int visit(ObColumnRefRawExpr &expr);
  virtual int visit(ObOpRawExpr &expr);
  virtual int visit(ObCaseOpRawExpr &expr);
  virtual int visit(ObAggFunRawExpr &expr);
  virtual int visit(ObSysFunRawExpr &expr);
  virtual int visit(ObSetOpRawExpr &expr);
  virtual int visit(ObAliasRefRawExpr &expr);
  virtual int visit(ObWinFunRawExpr &expr);
  virtual int visit(ObPseudoColumnRawExpr &expr);
  virtual int visit(ObUDFRawExpr &expr);
  virtual int visit(ObPlQueryRefRawExpr &expr);
  virtual int visit(ObMatchFunRawExpr &expr);

  int add_implicit_cast(ObOpRawExpr &parent, const ObCastMode &cast_mode);
  int add_implicit_cast(ObCaseOpRawExpr &parent, const ObCastMode &cast_mode);
  int add_implicit_cast(ObAggFunRawExpr &parent, const ObCastMode &cast_mode);

  int check_type_for_case_expr(ObCaseOpRawExpr &case_expr, common::ObIAllocator &alloc);
  static bool skip_cast_expr(const ObRawExpr &parent, const int64_t child_idx);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprDeduceType);
  // function members
  int check_lob_param_allowed(const common::ObObjType from,
                              const common::ObCollationType from_cs_type,
                              const common::ObObjType to,
                              const common::ObCollationType to_cs_type,
                              ObExprOperatorType expr_type);
  int push_back_types(const ObRawExpr *param_expr, ObExprResTypes &types);
  int calc_result_type(ObNonTerminalRawExpr &expr, ObIExprResTypes &types,
                       common::ObCastMode &cast_mode, int32_t row_dimension);
  int calc_result_type_with_const_arg(
    ObNonTerminalRawExpr &expr,
    ObIExprResTypes &types,
    common::ObExprTypeCtx &type_ctx,
    ObExprOperator *op,
    ObExprResType &result_type,
    int32_t row_dimension);
  int check_expr_param(ObOpRawExpr &expr);
  int check_row_param(ObOpRawExpr &expr);
  int check_param_expr_op_row(ObRawExpr *param_expr, int64_t column_count);
  int visit_left_param(ObRawExpr &expr);
  //观察右操作符的参数个数，由于要知道左操作符参数个数，所以传入根操作符
  int visit_right_param(ObOpRawExpr &expr);
  int64_t get_expr_output_column(const ObRawExpr &expr);
  int get_row_expr_param_type(const ObRawExpr &expr, ObIExprResTypes &types);
  int deduce_type_visit_for_special_func(int64_t param_index, const ObRawExpr &expr, ObIExprResTypes &types);
  // init udf expr
  int init_normal_udf_expr(ObNonTerminalRawExpr &expr, ObExprOperator *op);
  // get agg udf result type
  int set_agg_udf_result_type(ObAggFunRawExpr &expr);

  int set_agg_group_concat_result_type(ObAggFunRawExpr &expr, ObExprResType &result_type);
  int set_json_agg_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type, bool &need_add_cast);
  int set_asmvt_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type);
  int set_rb_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type);
  int set_agg_json_array_result_type(ObAggFunRawExpr &expr, ObExprResType &result_type);

  int set_agg_min_max_result_type(ObAggFunRawExpr &expr, ObExprResType &result_type,
                                  bool &need_add_cast);
  int set_agg_regr_result_type(ObAggFunRawExpr &expr, ObExprResType &result_type);
  int set_xmlagg_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type);

  int set_agg_xmlagg_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type);

  // helper functions for add_implicit_cast
  int add_implicit_cast_for_op_row(ObRawExpr *&child_ptr,
                                   const common::ObIArray<ObExprResType> &input_types,
                                   const ObCastMode &cast_mode);
  // try add cast expr on subquery stmt's output && update column types.
  int add_implicit_cast_for_subquery(ObQueryRefRawExpr &expr,
                                     const common::ObIArray<ObExprResType> &input_types,
                                     const ObCastMode &cast_mode);
  // try add cast expr above %child_idx child,
  // %input_type.get_calc_meta() is the destination type!
  template<typename RawExprType>
  int try_add_cast_expr(RawExprType &parent, int64_t child_idx,
                        const ObExprResType &input_type, const ObCastMode &cast_mode);

  // try add cast expr above %expr , set %new_expr to &expr if no cast added.
  // %input_type.get_calc_meta() is the destination type!
  int try_add_cast_expr_above_for_deduce_type(ObRawExpr &expr, ObRawExpr *&new_expr,
                                              const ObExprResType &input_type,
                                              const ObCastMode &cm);
  int check_group_aggr_param(ObAggFunRawExpr &expr);
  int check_group_rank_aggr_param(ObAggFunRawExpr &expr);
  int check_median_percentile_param(ObAggFunRawExpr &expr);
  int add_median_percentile_implicit_cast(ObAggFunRawExpr &expr,
                                          const ObCastMode& cast_mode,
                                          const bool keep_type);
  int add_group_aggr_implicit_cast(ObAggFunRawExpr &expr, const ObCastMode& cast_mode);
  int adjust_cast_as_signed_unsigned(ObSysFunRawExpr &expr);

  bool ignore_scale_adjust_for_decimal_int(const ObItemType expr_type);
  int try_replace_casts_with_questionmarks_ora(ObRawExpr *row_expr);

  int try_replace_cast_with_questionmark_ora(ObRawExpr &parent, ObRawExpr *cast_expr, int param_idx);
private:
  const sql::ObSQLSessionInfo *my_session_;
  common::ObArenaAllocator alloc_;
  ObRawExprFactory *expr_factory_;
  //deduce with current session vars if solidify_session_vars_ is true,
  //otherwise deduce with my_local_vars_ if my_local_vars_ is not null
  const ObLocalSessionVar *my_local_vars_;
  int64_t local_vars_id_;
  bool solidify_session_vars_;
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RAW_EXPR_DEDUCE_TYPE_H */
