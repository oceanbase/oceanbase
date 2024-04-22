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

#ifndef _OB_EXPR_GENERATOR_IMPL_H
#define _OB_EXPR_GENERATOR_IMPL_H 1
#include "ob_expr_generator.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "ob_column_index_provider.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/engine/expr/ob_expr_interval.h"

namespace oceanbase
{
using jit::expr::ObColumnIndexProvider;
namespace sql
{
class ObExprRegexp;
class ObExprInOrNotIn;
class ObExprLike;
class ObExprField;
class ObExprStrcmp;
class ObExprAbs;
class ObExprArgCase;
class ObExprOracleDecode;
class ObSubQueryRelationalExpr;
class ObExprRand;
class ObExprRandom;
class ObExprTypeToStr;
class ObExprUDF;
class ObExprNullif;
class ObBaseExprColumnConv;
class ObExprDllUdf;
class ObExprPLIntegerChecker;
class ObExprPLGetCursorAttr;
class ObExprPLSQLCodeSQLErrm;
class ObExprPLSQLVariable;
class ObExprPLAssocIndex;
class ObExprCollectionConstruct;
class ObExprObjectConstruct;
class ObExprCalcPartitionId;
class ObExprOpSubQueryInPl;
class ObExprUdtConstruct;
class ObExprUDTAttributeAccess;
typedef common::ObSEArray<ObIterExprOperator*, 2> PhyIterExprDesc;

class ObExprGeneratorImpl: public ObExprGenerator, public ObRawExprVisitor
{
public:
  ObExprGeneratorImpl(int16_t cur_regexp_op_count,
                      int16_t cur_like_op_count,
                      uint32_t *next_expr_id,
                      ObColumnIndexProvider &column_idx_provider);

  ObExprGeneratorImpl(ObExprOperatorFactory &factory,
                      int16_t cur_regexp_op_count,
                      int16_t cur_like_op_count,
                      uint32_t *next_expr_id,
                      ObColumnIndexProvider &column_idx_provider);
  virtual ~ObExprGeneratorImpl();
  virtual int generate(ObRawExpr &raw_expr, ObSqlExpression &out_expr);

  // generate ObExprOperator
  int generate_expr_operator(ObRawExpr &raw_expr, ObExprOperatorFetcher &fetcher);
  int generate_infix_expr(ObRawExpr &raw_expr);
  inline int16_t get_cur_regexp_op_count() const { return cur_regexp_op_count_; }
  inline int16_t get_cur_like_op_count() const { return cur_like_op_count_; }
  template <typename T>
  static int gen_expression_with_row_desc(ObSqlExpressionFactory &sql_expr_factory,
                                          ObExprOperatorFactory &expr_op_factory,
                                          RowDesc &row_desc,
                                          const ObRawExpr *raw_expr,
                                          T *&expression);
  int gen_fast_column_conv_expr(ObRawExpr &raw_expr);
  int gen_fast_expr(ObRawExpr &raw_expr);
private:
    /// interface of ObRawExprVisitor
  virtual int visit(ObConstRawExpr &expr);
  virtual int visit(ObExecParamRawExpr &expr);
  virtual int visit(ObVarRawExpr &expr);
  virtual int visit(ObOpPseudoColumnRawExpr &expr);
  virtual int visit(ObQueryRefRawExpr &expr);
  virtual int visit(ObColumnRefRawExpr &expr);
  virtual int visit(ObOpRawExpr &expr);
  virtual int visit(ObCaseOpRawExpr &expr);
  virtual int visit(ObAggFunRawExpr &expr);
  virtual int visit(ObSysFunRawExpr &expr);
  virtual int visit(ObSetOpRawExpr &expr);
  virtual int visit(ObWinFunRawExpr &expr);
  virtual int visit(ObPseudoColumnRawExpr &expr);
  virtual int visit(ObPlQueryRefRawExpr &expr);
  virtual int visit(ObMatchFunRawExpr &expr);
  virtual bool skip_child(ObRawExpr &expr);
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprGeneratorImpl);
  // 重新标记已经生成的子表达式的root
  int visit_simple_op(ObNonTerminalRawExpr &expr);
  inline int visit_regex_expr(ObOpRawExpr &expr, ObExprRegexp *&regexp_op);
  inline int visit_in_expr(ObOpRawExpr &expr, ObExprInOrNotIn *&in_op);
  inline int visit_like_expr(ObOpRawExpr &expr, ObExprLike *&like_op);
  inline int visit_field_expr(ObNonTerminalRawExpr &expr, ObExprField *field_op);
  inline int visit_decode_expr(ObNonTerminalRawExpr &expr, ObExprOracleDecode *decode_op);
  inline int visit_relational_expr(ObNonTerminalRawExpr &expr, ObRelationalExprOperator *relational_op);
  inline int visit_minmax_expr(ObNonTerminalRawExpr &expr, ObMinMaxExprOperator *minmax_op);
  inline int visit_maybe_row_expr(ObOpRawExpr &expr, ObExprOperator *&op);
  inline int visit_subquery_cmp_expr(ObOpRawExpr &expr, ObSubQueryRelationalExpr *&subquery_op);
  inline int visit_strcmp_expr(ObNonTerminalRawExpr &expr, ObExprStrcmp *strcmp_op);
  inline int visit_abs_expr(ObNonTerminalRawExpr &expr, ObExprAbs *abs_op);
  inline int visit_argcase_expr(ObNonTerminalRawExpr &expr, ObExprArgCase *argcase_op);
  inline int visit_rand_expr(ObOpRawExpr &expr, ObExprRand * rand_op);
  inline int visit_random_expr(ObOpRawExpr &expr, ObExprRandom * rand_op);
  inline int visit_column_conv_expr(ObRawExpr &expr, ObBaseExprColumnConv *column_conv_op);
  inline int visit_enum_set_expr(ObNonTerminalRawExpr &expr, ObExprTypeToStr *enum_set_op);
  inline int visit_udf_expr(ObOpRawExpr &expr, ObExprUDF *udf);
  inline int visit_nullif_expr(ObNonTerminalRawExpr &expr, ObExprNullif *&nullif_expr);
  inline int visit_fun_interval(ObNonTerminalRawExpr &expr, ObExprInterval *fun_interval);
  inline int visit_normal_udf_expr(ObNonTerminalRawExpr &expr, ObExprDllUdf *normal_udf_op);
  inline int visit_pl_integer_checker_expr(ObOpRawExpr &expr, ObExprPLIntegerChecker *checker);
  inline int visit_pl_assoc_index_expr(ObOpRawExpr &expr, ObExprPLAssocIndex *pl_assoc_index);
  inline int visit_pl_sqlcode_sqlerrm_expr(ObRawExpr &expr, ObExprPLSQLCodeSQLErrm *pl_sqlcode_sqlerrm);
  inline int visit_plsql_variable_expr(ObRawExpr &expr, ObExprPLSQLVariable *plsql_variable);
  inline int visit_pl_collection_construct_expr(ObRawExpr &expr, ObExprCollectionConstruct *pl_coll_construct);
  inline int visit_pl_object_construct_expr(ObRawExpr &expr, ObExprObjectConstruct *pl_object_construct);
  inline int visit_pl_get_cursor_attr_expr(
    ObRawExpr &expr, ObExprPLGetCursorAttr *pl_get_cursor_attr);
  inline int visit_sql_udt_construct_expr(ObRawExpr &expr, ObExprUdtConstruct *udt_construct);
  inline int visit_sql_udt_attr_access_expr(ObRawExpr &expr, ObExprUDTAttributeAccess *udt_attr_access);

  // %item_pos is the position of %raw_expr (infix expr item) in infix_expr_.exprs_ array.
  int add_child_infix_expr(ObRawExpr &raw_expr, const int64_t item_pos,
      common::ObIArray<ObRawExpr *> &visited_exprs);
  // visit child of %raw_expr and add visited child to %visited_exprs
  int infix_visit_child(ObRawExpr &raw_expr, common::ObIArray<ObRawExpr *> &visited_exprs);
private:
  int set_need_cast(ObNonTerminalRawExpr &expr, bool &need_cast);
  int generate_top_fre_hist_expr_operator(ObAggFunRawExpr &expr);
  int generate_hybrid_hist_expr_operator(ObAggFunRawExpr &expr);
private:
    // data members
  ObSqlExpression *sql_expr_;  // generated postfix expression
  int16_t cur_regexp_op_count_;
  int16_t cur_like_op_count_;
  ObColumnIndexProvider &column_idx_provider_;
  common::ObArenaAllocator inner_alloc_;
  ObExprOperatorFactory inner_factory_;
  ObExprOperatorFactory &factory_;
  PhyIterExprDesc iter_expr_desc_;
};

//generated expression with row desc
template<typename T>
int ObExprGeneratorImpl::gen_expression_with_row_desc(ObSqlExpressionFactory &sql_expr_factory,
                                                      ObExprOperatorFactory &expr_op_factory,
                                                      RowDesc &row_desc,
                                                      const ObRawExpr *raw_expr,
                                                      T *&expression)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_CG_LOG(WARN, "raw expr is NULL", K(ret));
  } else if (OB_UNLIKELY(NULL != expression)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_CG_LOG(WARN, "expression should not be inited", K(ret));
  } else if (OB_FAIL(sql_expr_factory.alloc(expression))) {
    SQL_CG_LOG(WARN, "Failed to alloc sql-expr", K(ret));
  } else if (OB_ISNULL(expression)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_CG_LOG(WARN, "Alloc partition_expr succ, but partition_expr is NULL", K(ret));
  } else {
    ObExprGeneratorImpl expr_generator(expr_op_factory, 0, 0, NULL, row_desc);
    if (OB_FAIL(expr_generator.generate(const_cast<ObRawExpr&>(*raw_expr), *expression))) {
      SQL_CG_LOG(WARN, "Generate partition expression error", K(ret), K(row_desc));
    } else if (0 != expr_generator.get_cur_regexp_op_count()) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_CG_LOG(WARN, "there must be no regexp op in partition expr",
                K(ret), K(expr_generator.get_cur_regexp_op_count()));
    } else if (OB_UNLIKELY(0 != expr_generator.get_cur_like_op_count())) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_CG_LOG(WARN, "there must be no like op in partition expr", K(ret),
                K(expr_generator.get_cur_like_op_count()));
    } else { /*do nothing*/ }
  }
  return ret;
}
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_EXPR_GENERATOR_IMPL_H */
