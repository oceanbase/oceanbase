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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/expr/ob_raw_expr_wrap_enum_set.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "common/ob_smart_call.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

int ObRawExprWrapEnumSet::wrap_enum_set(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  cur_stmt_ = &stmt;
  if (stmt.is_select_stmt()) {
    //handle the target list of first level
    ObSelectStmt &select_stmt = static_cast<ObSelectStmt &>(stmt);
    if (OB_FAIL(wrap_target_list(select_stmt))) {
      LOG_WARN("failed to wrap target list", K(ret));
    }
  } else if (stmt.is_insert_stmt()) {
    ObInsertStmt &insert_stmt = static_cast<ObInsertStmt &>(stmt);

    if (insert_stmt.value_from_select()) {
      if (OB_FAIL(wrap_sub_select(insert_stmt))) {
        LOG_WARN("failed to wrap value_vector", K(ret));
      }
    } else if (OB_FAIL(wrap_value_vector(insert_stmt))) {
      LOG_WARN("failed to wrap value_vector", K(ret));
    }
  } else {}

  if (OB_SUCC(ret)) {
    if (OB_FAIL(analyze_all_expr(stmt))) {
      LOG_WARN("failed to analyze all expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprWrapEnumSet::wrap_sub_select(ObInsertStmt &stmt)
{
  int ret = OB_SUCCESS;
  common::ObIArray<ObRawExpr*> &column_conv_exprs = stmt.get_column_conv_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_conv_exprs.count(); ++i) {
    ObRawExpr *conv_expr = column_conv_exprs.at(i);
    ObSysFunRawExpr *wrapped_expr = NULL;
    const bool is_same_need = true;
    if (OB_ISNULL(conv_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("convert expr is null", K(ret), K(conv_expr));
    } else if (T_FUN_COLUMN_CONV != conv_expr->get_expr_type()) {
      // do nothing
    } else if (OB_UNLIKELY(
      conv_expr->get_param_count() != ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO
      && conv_expr->get_param_count() != ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("convert expr have invalid param number", K(ret));
    } else if (conv_expr->get_param_expr(4)->is_column_ref_expr()) {
      int32_t const_value = -1;
      if (OB_UNLIKELY(!conv_expr->get_param_expr(0)->is_const_raw_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("convert expr have invalid param", K(ret));
      } else if (OB_FAIL(static_cast<ObConstRawExpr *>(conv_expr->get_param_expr(0))
                         ->get_value().get_int32(const_value))) {
        LOG_WARN("failed to get obj type from convert expr", K(ret));
      } else if (OB_FAIL(wrap_type_to_str_if_necessary(conv_expr->get_param_expr(4),
                                                       static_cast<ObObjType>(const_value),
                                                       is_same_need,
                                                       wrapped_expr))) {
        LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
      } else if (NULL != wrapped_expr) {
        conv_expr->get_param_expr(4) = wrapped_expr;
      }
    }
    LOG_DEBUG("finish wrap_sub_select", K(i), KPC(conv_expr));
  }
  return ret;
}

int ObRawExprWrapEnumSet::wrap_value_vector(ObInsertStmt &stmt)
{
  int ret = OB_SUCCESS;
  int64_t desc_count = stmt.get_values_desc().count();
  if ((desc_count > 0)) {
    //只有当value_desc中有enum或者set列的时候才需要做以下操作
    bool need_check = false;
    ObColumnRefRawExpr *value_desc = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && (!need_check) && (i < desc_count); ++i) {
      if (OB_ISNULL(value_desc = stmt.get_values_desc().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value desc is null", K(i), K(ret));
      } else if (ob_is_enum_or_set_type(value_desc->get_data_type())) {
        need_check = true;
      } else {}
    }

    int64_t vector_count = stmt.get_values_vector().count();
    const bool is_same_need = true;
    for (int64_t i = 0; OB_SUCC(ret) && need_check && i < vector_count; ++i) {
      ObRawExpr *&value_expr = stmt.get_values_vector().at(i);
      if (OB_ISNULL(value_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value expr is null", K(i), K(ret));
      } else if (OB_FAIL(analyze_expr(value_expr))) {
        LOG_WARN("failed to analyze expr", KPC(value_expr), K(ret));
      } else {
        int64_t index = i % desc_count;
        ObSysFunRawExpr *new_expr = NULL;
        if (OB_FAIL(wrap_type_to_str_if_necessary(value_expr, stmt.get_values_desc().at(index)->get_data_type(),
                                                  is_same_need, new_expr))) {
          LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
        } else if (NULL != new_expr) {
          value_expr = new_expr;
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObRawExprWrapEnumSet::wrap_target_list(ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  const bool is_type_to_str = true;
  common::ObIArray<SelectItem> &select_items = select_stmt.get_select_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    ObRawExpr *target_expr = select_items.at(i).expr_;
    if (OB_ISNULL(target_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr of select_items should not be NULL", K(i), K(ret));
    } else if (ob_is_enumset_tc(target_expr->get_data_type())) {
      ObSysFunRawExpr *new_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::create_type_to_str_expr(expr_factory_, target_expr,
                                                          new_expr,
                                                          my_session_, is_type_to_str))) {
        LOG_WARN("failed to create_type_to_string_expr", K(i), K(target_expr), K(ret));
      } else if (OB_ISNULL(new_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("created expr is NULL", K(ret));
      } else {
        select_items.at(i).expr_ = new_expr;
      }
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRawExprWrapEnumSet::analyze_all_expr(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> relation_exprs;
  ObArray<ObSelectStmt*> child_stmts;
  cur_stmt_ = &stmt;
  if (OB_FAIL(stmt.get_relation_exprs_for_enum_set_wrapper(relation_exprs))) {
    LOG_WARN("get relation exprs failed", K(ret));
  } else if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
    LOG_WARN("get child stmt failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
      const TableItem *table_item = stmt.get_table_item(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_item is null", K(i));
      } else if (table_item->is_temp_table()) {
        if (OB_FAIL(child_stmts.push_back(table_item->ref_query_))) {
          LOG_WARN("store child stmt failed", K(ret));
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(analyze_expr(relation_exprs.at(i)))) {
      LOG_WARN("failed to analyze expr", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    ObDMLStmt *child_stmt = child_stmts.at(i);
    if (OB_ISNULL(child_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child stmt is null", K(ret));
    } else if (OB_FAIL(SMART_CALL(analyze_all_expr(*child_stmt)))) {
      LOG_WARN("analyze child stmt all expr failed", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRawExprWrapEnumSet::analyze_expr(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is NULL", K(ret));
  // extract info before in case that IS/CNT_ENUM_OR_SET flag has not been set.
  } else if (OB_FAIL(expr->extract_info())) {
    LOG_WARN("extract info failed", K(ret));
  } else if (OB_FAIL(expr->postorder_accept(*this))) {
    LOG_WARN("failed to postorder_accept", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObRawExprWrapEnumSet::visit(ObConstRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprWrapEnumSet::visit(ObExecParamRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprWrapEnumSet::visit(ObVarRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprWrapEnumSet::visit(ObOpPseudoColumnRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprWrapEnumSet::visit(ObQueryRefRawExpr &expr)
{
  UNUSED(expr);
  // QueryRef expr for the children of `ObOpRawExpr` will be visited at `visit_query_ref_expr`.
  // because it depends on the input_type of the parent node.
  return OB_SUCCESS;
}

int ObRawExprWrapEnumSet::visit(ObPlQueryRefRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprWrapEnumSet::visit(ObColumnRefRawExpr &expr)
{
  int ret = OB_SUCCESS;

  if (expr.is_generated_column()) {
    if (OB_FAIL(analyze_expr(expr.get_dependant_expr()))) {
      LOG_WARN("failed to analyze columnrefrawexpr", K(expr));
    }
  }
  return ret;
}

int ObRawExprWrapEnumSet::visit(ObWinFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.has_enum_set_column() || expr.has_flag(CNT_SUB_QUERY)) {
    if (T_WIN_FUN_LEAD == expr.get_func_type() ||
          T_WIN_FUN_LAG == expr.get_func_type()) {
      ObIArray<ObRawExpr*> &real_parm_exprs = expr.get_func_params();
      if (OB_FAIL(wrap_param_expr(real_parm_exprs, expr.get_data_type()))) {
        LOG_WARN("failed to warp param expr", K(ret));
      }
    } else {
      ObAggFunRawExpr *agg_raw_expr = expr.get_agg_expr();
      if (OB_ISNULL(agg_raw_expr)) {
      } else if (OB_FAIL(ObRawExprWrapEnumSet::visit(*agg_raw_expr))) {
        LOG_WARN("fail to visit agg expr in window function", K(ret), K(agg_raw_expr));
      }
    }
  }
  return ret;
}

int ObRawExprWrapEnumSet::visit(ObPseudoColumnRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprWrapEnumSet::visit(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObExprOperator *op = NULL;
  if (!expr.has_enum_set_column() && !expr.has_flag(CNT_SUB_QUERY)) {
    //不含有enum或者set，则不需要做任何转换
  } else if (T_OP_ROW != expr.get_expr_type()) {
    if (OB_ISNULL(op = expr.get_op())) {
      ret  = OB_ERR_UNEXPECTED;
      LOG_WARN("op is NULL", K(expr), K(ret));
    } else {
      int64_t row_dimension = op->get_row_dimension();
      if (ObExprOperator::NOT_ROW_DIMENSION != row_dimension) {
        //处理向量的情况或者1对n的情况
        ObRawExpr *left_expr = NULL;
        ObRawExpr *right_expr = NULL;
        const ObIArray<ObExprCalcType> &cmp_types = op->get_result_type().get_row_calc_cmp_types();
        if (OB_UNLIKELY(2 != expr.get_param_count() || !IS_COMPARISON_OP(expr.get_expr_type()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid expr", K(expr), K(ret));
        } else if (OB_ISNULL(left_expr = expr.get_param_expr(0)) || OB_ISNULL(right_expr = expr.get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child expr is NULL", K(left_expr), K(right_expr), K(ret));
        } else if ((left_expr->has_enum_set_column() || left_expr->has_flag(CNT_SUB_QUERY)) && 
                   OB_FAIL(visit_left_expr(expr, row_dimension, cmp_types))) {
          LOG_WARN("failed to visit left expr", K(expr), K(ret));
        } else if ((right_expr->has_enum_set_column() || right_expr->has_flag(CNT_SUB_QUERY))
                    && OB_FAIL(visit_right_expr(*right_expr, row_dimension,
                                                cmp_types, expr.get_expr_type()))) {
          LOG_WARN("failed to visit right expr", K(expr), K(ret));
        } else {/*do nothing*/}
      } else if (expr.get_input_types().count() == expr.get_param_count()) {
        //处理标量的情况
        const bool is_same_need = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
          ObRawExpr *param_expr = expr.get_param_expr(i);
          ObObjType calc_type = expr.get_input_types().at(i).get_calc_type();
          ObSysFunRawExpr *new_expr = NULL;
          if (param_expr->is_query_ref_expr() && !ob_is_enumset_tc(param_expr->get_data_type())) {
            ObQueryRefRawExpr *query_ref_expr = static_cast<ObQueryRefRawExpr*>(param_expr);
            OZ(visit_query_ref_expr(*query_ref_expr, calc_type, is_same_need));
          } else if (OB_FAIL(wrap_type_to_str_if_necessary(param_expr,
                                                           calc_type,
                                                           is_same_need,
                                                           new_expr))) {
            LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
          } else if ((NULL != new_expr) && OB_FAIL(expr.replace_param_expr(i, new_expr))) {
            LOG_WARN("replace param expr failed", K(ret));
          } else {/*do nothing*/}
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr, param_count is not equal to input_types_count", K(expr), K(ret));
      }
    }
  } else {/*do nothing*/}
  return ret;
}

int ObRawExprWrapEnumSet::visit_left_expr(ObOpRawExpr &expr, int64_t row_dimension,
                                          const ObIArray<ObExprCalcType> &cmp_types)
{
  int ret = OB_SUCCESS;
  ObRawExpr *left_expr = expr.get_param_expr(0);
  if (OB_ISNULL(left_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left param expr is NULL", K(expr), K(ret));
  } else if (1 == row_dimension) {
    int64_t idx = 0;
    ObSysFunRawExpr *wrapped_expr = NULL;
    if (OB_FAIL(check_and_wrap_left(*left_expr, idx, cmp_types, row_dimension,
                                     wrapped_expr))) {
      LOG_WARN("failed to check_and_wrap_left", K(ret));
    } else if (NULL != wrapped_expr) {
      if (OB_FAIL(expr.replace_param_expr(0, wrapped_expr))) {
        LOG_WARN("replace left expr failed", K(ret));
      }
    } else {/*do nothing*/}
  } else if (T_OP_ROW == left_expr->get_expr_type()) {
    ObOpRawExpr *left_op_expr = static_cast<ObOpRawExpr *>(left_expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < left_expr->get_param_count(); ++i) {
      ObRawExpr *param_expr = left_op_expr->get_param_expr(i);
      ObSysFunRawExpr *wrapped_expr = NULL;
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is NULL", K(i), K(ret));
      } else if (OB_FAIL(check_and_wrap_left(*param_expr, i, cmp_types, row_dimension,
                                             wrapped_expr))) {
        LOG_WARN("failed to check_and_wrap_left", K(ret));
      } else if (NULL != wrapped_expr) {
        if (OB_FAIL(left_op_expr->replace_param_expr(i, wrapped_expr))) {
          LOG_WARN("replace param expr failed", K(i), K(ret));
        }
      } else {/*do nothing*/}
    }
  } else if (left_expr->has_flag(IS_SUB_QUERY) && left_expr->get_output_column() > 1) {
    ObQueryRefRawExpr *left_ref = static_cast<ObQueryRefRawExpr*>(left_expr);
    if (OB_ISNULL(left_ref)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left_ref is NULL", K(ret));
    } else {
      ObSelectStmt *ref_stmt = left_ref->get_ref_stmt();
      if (OB_ISNULL(ref_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref_stmt should not be NULL", K(expr), K(ret));
      } else if (row_dimension != ref_stmt->get_select_item_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item size and row_dimension should be equal", "slect_item_size",
                 ref_stmt->get_select_item_size(), K(row_dimension), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ref_stmt->get_select_item_size(); ++i) {
          ObRawExpr *target_expr = ref_stmt->get_select_item(i).expr_;
          ObSysFunRawExpr *wrapped_expr = NULL;
          if (OB_ISNULL(target_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr of select item is NULL", K(left_expr), K(ret));
          } else if (OB_FAIL(check_and_wrap_left(*target_expr, i, cmp_types, row_dimension,
                                                 wrapped_expr))) {
            LOG_WARN("failed to check_and_wrap_left", K(ret));
          } else if (NULL != wrapped_expr) {
            ref_stmt->get_select_item(i).expr_ = wrapped_expr;
            left_ref->get_column_types().at(i) = wrapped_expr->get_result_type();
          } else {/*do nothing*/}
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not come here", K(*left_expr), K(ret));
  }
  return ret;
}

int ObRawExprWrapEnumSet::check_and_wrap_left(ObRawExpr &expr, int64_t idx,
                                              const ObIArray<ObExprCalcType> &cmp_types,
                                              int64_t row_dimension,
                                              ObSysFunRawExpr *&wrapped_expr) const
{
  int ret = OB_SUCCESS;
  wrapped_expr = NULL;
  int64_t target_num = cmp_types.count() / row_dimension;
  ObObjType expr_type = expr.get_data_type();
  if (OB_UNLIKELY(idx >= row_dimension)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(idx), K(row_dimension), K(ret));
  } else if (ob_is_enumset_tc(expr_type)) {
    bool need_numberic = false;
    bool need_varchar = false;
    const bool is_same_type_need = false;
    ObObjType dest_type = ObMaxType;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_num && !(need_numberic && need_varchar); ++i) {
      bool need_wrap = false;
      dest_type = cmp_types.at(row_dimension * i + idx).get_type();
      if (OB_FAIL(ObRawExprUtils::need_wrap_to_string(expr_type,
                                                      dest_type,
                                                      is_same_type_need, need_wrap))) {
        LOG_WARN("failed to check whether need wrap", K(i), K(expr), K(ret));
      } else if (need_wrap) {
        need_varchar = true;
      } else if (!need_wrap) {
        need_numberic = true;
      } else {/*do nothing*/}
    }

    if (OB_SUCC(ret) && need_varchar) {
      const bool is_type_to_str = !need_numberic;
      if (OB_FAIL(ObRawExprUtils::create_type_to_str_expr(expr_factory_, &expr,
                                                          wrapped_expr, my_session_, is_type_to_str, dest_type))) {
        LOG_WARN("failed to create_type_to_string_expr",K(expr), K(ret));
      }
    }
  } else {/*do nothing*/}
  return ret;
}

int ObRawExprWrapEnumSet::visit_right_expr(ObRawExpr &right_expr, int64_t row_dimension,
                                           const ObIArray<ObExprCalcType> &cmp_types,
                                           const ObItemType &root_type)
{
  int ret = OB_SUCCESS;
  int64_t cmp_types_count = cmp_types.count();
  if (OB_UNLIKELY(row_dimension < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row dimension should be no less than 1", K(row_dimension), K(ret));
  } else if (T_OP_ROW == right_expr.get_expr_type() 
             && (!right_expr.has_flag(CNT_SUB_QUERY)
                 || T_OP_IN == root_type
                 || T_OP_NOT_IN == root_type)) {
    // TODO [zongmei.zzm] select (tinyint_t, tinyint_t) = (enum_t, enum_t) from t limit 1
    // 在这里会报错，预期应该成功，BUG
    ObOpRawExpr &right_op_expr = static_cast<ObOpRawExpr &>(right_expr);
    int64_t right_param_count = right_op_expr.get_param_count();
    if (OB_UNLIKELY(cmp_types_count != (row_dimension * right_param_count))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cmp_types is invalid", K(cmp_types_count), K(row_dimension), K(right_param_count), K(ret));
    } else if (1 == row_dimension) {
      // 'aa' in (c2, c3, c4) or select * from t2 where (1) in ((c1), (select c1 from t22));
      const bool is_same_need = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < right_op_expr.get_param_count(); ++i) {
        ObRawExpr *param_expr = right_op_expr.get_param_expr(i);
        ObObjType calc_type = cmp_types.at(i).get_type();
        ObSysFunRawExpr *wrapped_expr = NULL;
        if (OB_FAIL(wrap_type_to_str_if_necessary(param_expr, calc_type, is_same_need,  wrapped_expr))) {
          LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
        } else if ((NULL != wrapped_expr) && OB_FAIL(right_op_expr.replace_param_expr(i, wrapped_expr))) {
          LOG_WARN("replace param expr failed", K(i), K(ret));
        } else {/*do nothing*/}
      }
    } else {
      //('1','2') in ((c1,c2), (c3, '4')) or (1,2) in ((1,2)); and ((c1,c1), (select c1,c1 from t1)) is
      //not supported so far;
      for (int64_t i = 0; OB_SUCC(ret) && i < right_op_expr.get_param_count(); ++i) {
        ObRawExpr *param_expr = right_op_expr.get_param_expr(i);
        if (OB_ISNULL(param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is NULL", K(i), K(ret));
        } else if (T_OP_ROW == param_expr->get_expr_type()) {
          if (OB_UNLIKELY(row_dimension != param_expr->get_param_count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pram_count should be equal to row_dimension", K(row_dimension), "param_count",
                     param_expr->get_param_count(), K(ret));
          } else {
            ObOpRawExpr *param_op_expr = static_cast<ObOpRawExpr *>(param_expr);
            const bool is_same_need = false;
            for (int64_t j = 0; OB_SUCC(ret) && j < param_op_expr->get_param_count(); ++j) {
              ObRawExpr *inner_param_expr = param_op_expr->get_param_expr(j);
              ObObjType calc_type = cmp_types.at(i * row_dimension + j).get_type();
              ObSysFunRawExpr *wrapped_expr = NULL;
              if (OB_FAIL(wrap_type_to_str_if_necessary(inner_param_expr, calc_type,
                                                        is_same_need, wrapped_expr))) {
                LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
              } else if ((NULL != wrapped_expr) && OB_FAIL(param_op_expr->replace_param_expr(i, wrapped_expr))) {
                LOG_WARN("replace param expr failed", K(ret));
              } else {/*do nothing*/}
            }
          }
        } else if (param_expr->has_flag(IS_SUB_QUERY)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("subquery in row is not supported", K(ret));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid param expr", KPC(param_expr), K(ret));
        }
      }// end inner for
    }
  } else if (T_OP_ROW == right_expr.get_expr_type()
             && right_expr.has_flag(CNT_SUB_QUERY)
             && (root_type >= T_OP_EQ && root_type <= T_OP_NE)) {
    //(1,1) <> ((select '1' from dual), 3)
    ObOpRawExpr &right_op_expr = static_cast<ObOpRawExpr &>(right_expr);
    if (OB_UNLIKELY(cmp_types_count != row_dimension)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cmp_types is invalid", K(cmp_types_count), K(row_dimension), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < right_op_expr.get_param_count(); ++i) {
        ObRawExpr *param_expr = right_op_expr.get_param_expr(i);
        if (OB_ISNULL(param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is NULL", K(i), K(ret));
        } else if (T_OP_ROW == param_expr->get_expr_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subquery in row must is scalar", K(ret));
        } else if (param_expr->has_flag(IS_SUB_QUERY)) {
          ObQueryRefRawExpr &right_ref = static_cast<ObQueryRefRawExpr &>(*param_expr);
          ObSelectStmt *ref_stmt = right_ref.get_ref_stmt();
          if (OB_ISNULL(ref_stmt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ref_stmt should not be NULL", K(param_expr), K(ret));
          } else if (OB_UNLIKELY(1 != ref_stmt->get_select_item_size())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("select item size and row_dimension should be equal", "slect_item_size",
                    ref_stmt->get_select_item_size(), K(ret));
          } else {
            const bool is_same_need = false;
            ObRawExpr *target_expr = ref_stmt->get_select_item(0).expr_;
            ObSysFunRawExpr *wrapped_expr = NULL;
            ObObjType calc_type = cmp_types.at(0).get_type();
            if (OB_FAIL(wrap_type_to_str_if_necessary(target_expr, calc_type,
                                        is_same_need, wrapped_expr))) {
              LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
            } else if (NULL != wrapped_expr) {
              ref_stmt->get_select_item(0).expr_ = wrapped_expr;
            } else {/*do nothing*/}
          }
        } else {
          const bool is_same_need = false;
          ObRawExpr *inner_param_expr = param_expr;
          ObObjType calc_type = cmp_types.at(i).get_type();
          ObSysFunRawExpr *wrapped_expr = NULL;
          if (OB_FAIL(wrap_type_to_str_if_necessary(inner_param_expr, calc_type,
                                            is_same_need, wrapped_expr))) {
            LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
          } else if ((NULL != wrapped_expr)
                      && OB_FAIL(right_op_expr.replace_param_expr(i, wrapped_expr))) {
            LOG_WARN("replace param expr failed", K(ret));
          } else {/*do nothing*/}
        }
      }// end inner for
    }
  } else if (right_expr.has_flag(IS_SUB_QUERY)) {
    ObQueryRefRawExpr &right_ref = static_cast<ObQueryRefRawExpr &>(right_expr);
    ObSelectStmt *ref_stmt = right_ref.get_ref_stmt();
    if (OB_ISNULL(ref_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref_stmt should not be NULL", K(right_expr), K(ret));
    } else if (OB_UNLIKELY(row_dimension != ref_stmt->get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item size and row_dimension should be equal", "slect_item_size",
               ref_stmt->get_select_item_size(), K(row_dimension), K(ret));
    } else if (OB_UNLIKELY(row_dimension != cmp_types_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row dimension and cmp_types_count should be equal when right expr is subquery",
               K(row_dimension), K(cmp_types_count), K(ret));
    } else {
      const bool is_same_need = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_dimension; ++i) {
        ObRawExpr *target_expr = ref_stmt->get_select_item(i).expr_;
        ObSysFunRawExpr *wrapped_expr = NULL;
        ObObjType calc_type = cmp_types.at(i).get_type();
        if (OB_FAIL(wrap_type_to_str_if_necessary(target_expr, calc_type,
                                                  is_same_need, wrapped_expr))) {
          LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
        } else if (NULL != wrapped_expr) {
          ref_stmt->get_select_item(i).expr_ = wrapped_expr;
          right_ref.get_column_types().at(i) = wrapped_expr->get_result_type();
        } else {/*do nothing*/}
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("should not come here", K(right_expr), K(ret));
  }
  LOG_TRACE("succeed to visit right expr", K(root_type), K(row_dimension), K(right_expr));
  return ret;
}

int ObRawExprWrapEnumSet::wrap_type_to_str_if_necessary(ObRawExpr *expr,
                                                        ObObjType dest_type,
                                                        bool is_same_need,
                                                        ObSysFunRawExpr *&wrapped_expr)
{
  int ret = OB_SUCCESS;
  bool need_wrap = false;
  const bool is_type_to_str = true;
  wrapped_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::need_wrap_to_string(expr->get_data_type(), dest_type,
                                                         is_same_need, need_wrap))) {
    LOG_WARN("failed to check_need_wrap_to_string", K(ret));
  } else if (need_wrap && OB_FAIL(ObRawExprUtils::create_type_to_str_expr(expr_factory_, expr,
      wrapped_expr, my_session_, is_type_to_str, dest_type))) {
    LOG_WARN("failed to create_type_to_string_expr", KPC(expr), K(is_type_to_str), K(ret));
  } else {
    LOG_DEBUG("finish wrap_type_to_str_if_necessary", K(ret), K(need_wrap), K(dest_type), KPC(expr),
              KPC(wrapped_expr), K(lbt()));
  }
  return ret;
}

int ObRawExprWrapEnumSet::visit(ObCaseOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_OP_CASE != expr.get_expr_type() && T_OP_ARG_CASE != expr.get_expr_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid case when expr", K(expr), K(ret));
  } else if (OB_UNLIKELY(expr.get_input_types().count() < expr.get_when_expr_size() * 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case when expr", K(expr), K(ret));
  } else {
    ObSysFunRawExpr *wrapped_expr = NULL;
    const bool is_same_need = false;
    if (T_OP_ARG_CASE == expr.get_expr_type()) {
      ObRawExpr *arg_param_expr = expr.get_arg_param_expr();
      if (OB_ISNULL(arg_param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg param expr is NULL", K(ret));
      } else {
        bool need_numberic = false;
        bool need_varchar = false;
        ObObjType arg_type = arg_param_expr->get_data_type();
        ObObjType calc_type = ObMaxType;
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_when_expr_size(); ++i) {
          calc_type = expr.get_input_types().at(i + 1).get_calc_type();
          wrapped_expr = NULL;
          ObRawExpr *when_expr = expr.get_when_param_expr(i);
          bool need_wrap = false;
          if (OB_FAIL(wrap_type_to_str_if_necessary(when_expr, calc_type,
                                                    is_same_need, wrapped_expr))) {
            LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
          } else if (NULL != wrapped_expr && OB_FAIL(expr.replace_when_param_expr(i, wrapped_expr))){
            LOG_WARN("failed to replace_when_param_expr", K(i), K(ret));
          } else if (OB_FAIL(ObRawExprUtils::need_wrap_to_string(arg_type, calc_type, is_same_need, need_wrap))) {
            LOG_WARN("failed to check whether need wrap", K(arg_type), K(calc_type), K(ret));
          } else if (need_wrap) {
            need_varchar = true;
          } else if (!need_wrap) {
            need_numberic = true;
          } else {/*do nothing*/}
        }
        if (OB_SUCC(ret) && ob_is_enumset_tc(arg_type) && need_varchar) {
          const bool is_type_to_str = !need_numberic;
          wrapped_expr = NULL;
          if (OB_FAIL(ObRawExprUtils::create_type_to_str_expr(expr_factory_, arg_param_expr,
                                                              wrapped_expr, my_session_, is_type_to_str, calc_type))) {
            LOG_WARN("failed to create_type_to_string_expr", K(ret));
          } else if (OB_ISNULL(wrapped_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("created wrapped expr is NULL", K(ret));
          } else {
            expr.set_arg_param_expr(wrapped_expr);
          }
        }
      }
    } else {
      //handle T_OP_CASE
      ObObjType calc_type = expr.get_result_type().get_calc_type();
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_when_expr_size(); i++) {
        wrapped_expr = NULL;
        ObRawExpr *when_expr = expr.get_when_param_expr(i);
        if (OB_FAIL(wrap_type_to_str_if_necessary(when_expr, calc_type,
                                                  is_same_need, wrapped_expr))) {
          LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
        } else if (NULL != wrapped_expr && OB_FAIL(expr.replace_when_param_expr(i, wrapped_expr))){
          LOG_WARN("failed to replace_when_param_expr", K(i), K(ret));
        } else {/*do nothing*/}
      }
    }

    ObObjType result_type = expr.get_result_type().get_type();
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_then_expr_size(); i++) {
      ObRawExpr *then_expr = expr.get_then_param_expr(i);
      wrapped_expr = NULL;
      if (OB_FAIL(wrap_type_to_str_if_necessary(then_expr, result_type,
                                                is_same_need, wrapped_expr))) {
        LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
      } else if (NULL != wrapped_expr && OB_FAIL(expr.replace_then_param_expr(i, wrapped_expr))){
        LOG_WARN("failed to replace_when_param_expr", K(i), K(ret));
      } else {/*do nothing*/}
    }

    if (OB_SUCC(ret) && expr.get_default_param_expr() != NULL) {
      ObRawExpr *default_expr = expr.get_default_param_expr();
      wrapped_expr = NULL;
      if (OB_FAIL(wrap_type_to_str_if_necessary(default_expr, result_type,
                                                is_same_need, wrapped_expr))) {
        LOG_WARN("failed to wrap_type_to_str_if_necessary", K(ret));
      } else if (NULL != wrapped_expr) {
        expr.set_default_param_expr(wrapped_expr);
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObRawExprWrapEnumSet::visit(ObAggFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if ((expr.has_enum_set_column() || expr.has_flag(CNT_SUB_QUERY)) &&
      (T_FUN_GROUP_CONCAT == expr.get_expr_type() ||
      T_FUN_MAX == expr.get_expr_type() ||
      T_FUN_MIN == expr.get_expr_type() ||
      T_FUN_JSON_OBJECTAGG == expr.get_expr_type() ||
      T_FUN_JSON_ARRAYAGG == expr.get_expr_type() ||
      T_FUN_ORA_JSON_ARRAYAGG == expr.get_expr_type() ||
      T_FUN_ORA_JSON_OBJECTAGG == expr.get_expr_type())) {
    ObIArray<ObRawExpr*> &real_parm_exprs = expr.get_real_param_exprs_for_update();
    if (OB_FAIL(wrap_param_expr(real_parm_exprs, expr.get_data_type()))) {
      LOG_WARN("failed to warp param expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprWrapEnumSet::visit(ObMatchFunRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

bool ObRawExprWrapEnumSet::can_wrap_type_to_str(const ObRawExpr &expr) const
{
  bool bret = false;
  if ((expr.has_enum_set_column() || expr.has_flag(CNT_SUB_QUERY)) && 
      !expr.is_type_to_str_expr()) {
    bret = true;
    if (cur_stmt_ != nullptr && cur_stmt_->is_insert_stmt()) {
      ObInsertStmt *ins_stmt = static_cast<ObInsertStmt*>(cur_stmt_);
      if (T_FUN_COLUMN_CONV == expr.get_expr_type()
          && ob_is_enumset_tc(expr.get_data_type())
          && !ins_stmt->value_from_select()) {
        //for insert values... enum set column convert in ObExprValues,
        //no need to wrap type to str here
        bret = false;
      }
    }
  }
  LOG_TRACE("succeed to check can wrap type to str", K(bret), K(expr));
  return bret;
}

int ObRawExprWrapEnumSet::visit(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObExprOperator *op = expr.get_op();
  int64_t param_count = expr.get_param_count();
  int64_t input_types_count = expr.get_input_types().count();
  if (OB_UNLIKELY(NULL == op)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Get expression operator failed", "expr type", expr.get_expr_type());
  } else if (OB_UNLIKELY(param_count != input_types_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param_count is not equal to input_types_count", K(ret), K(param_count), K(input_types_count));
  } else if (can_wrap_type_to_str(expr)) {
    if (T_FUN_SYS_NULLIF == expr.get_expr_type()) {
      if (OB_FAIL(wrap_nullif_expr(expr))) {
        LOG_WARN("failed to wrap nullif expr", K(ret));
      }
    } else {
      ObExprResTypes types;
      const bool is_same_need = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); i++) {
        ObRawExpr *param_expr = expr.get_param_expr(i);
        if (OB_ISNULL(param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(i), K(ret));
        } else if (ob_is_enumset_tc(param_expr->get_data_type())) {
          ObObjType calc_type = expr.get_input_types().at(i).get_calc_type();
          ObSysFunRawExpr *wrapped_expr = NULL;
          // Enumset warp to string in CAST expr will be handled here.
          if (OB_FAIL(wrap_type_to_str_if_necessary(param_expr, calc_type,
                                                    is_same_need, wrapped_expr))) {
            LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(expr), K(ret));
          } else if ((NULL != wrapped_expr) && OB_FAIL(expr.replace_param_expr(i, wrapped_expr))) {
            LOG_WARN("failed to replace param expr", K(i), K(ret));
          } else {/*do nothing*/}
        }
      }
    }
  } else {/*do nothing*/}
  return ret;
}

int ObRawExprWrapEnumSet::visit(ObSetOpRawExpr &expr)
{
  UNUSED(expr);
  int ret = OB_SUCCESS;
  return ret;
}

int ObRawExprWrapEnumSet::visit(ObAliasRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *ref_expr = expr.get_ref_expr();
  if (OB_ISNULL(ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref expr is null", K(ret));
  } else if (expr.has_enum_set_column() && OB_FAIL(analyze_expr(ref_expr))) {
    LOG_WARN("failed to analyze expr", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObRawExprWrapEnumSet::wrap_nullif_expr(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  int64_t param_count = expr.get_param_count();
  int64_t input_types_count = expr.get_input_types().count();
  if (OB_UNLIKELY(OB_ISNULL(my_session_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret));
  } else if (OB_UNLIKELY(T_FUN_SYS_NULLIF != expr.get_expr_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(ret));
  } else if (OB_UNLIKELY(2 != param_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param count", K(param_count), K(ret));
  } else if (OB_UNLIKELY(param_count != input_types_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param_count is not equal to input_types_count", K(param_count),
             K(input_types_count), K(ret));
  } else {
    ObRawExpr *left_param = expr.get_param_expr(0);
    ObRawExpr *right_param = expr.get_param_expr(1);
    if (OB_ISNULL(left_param) || OB_ISNULL(right_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is NULL", KP(left_param), KP(right_param), K(ret));
    } else {
      if (ob_is_enumset_tc(left_param->get_data_type())) {
        ObObjType calc_type = expr.get_input_types().at(0).get_calc_type();
        const bool is_type_to_str = ObVarcharType == calc_type;
        ObSysFunRawExpr *wrapped_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::create_type_to_str_expr(expr_factory_,
                                                            left_param,
                                                            wrapped_expr,
                                                            my_session_,
                                                            is_type_to_str,
                                                            calc_type))) {
          LOG_WARN("failed to create_type_to_string_expr", K(ret));
        } else if ((NULL != wrapped_expr) && OB_FAIL(expr.replace_param_expr(0, wrapped_expr))) {
          LOG_WARN("failed to replace left param expr", K(ret));
        } else {/*do nothing*/}
      }

      if (OB_SUCC(ret) && ob_is_enumset_tc(right_param->get_data_type())) {
        ObObjType calc_type = expr.get_input_types().at(1).get_calc_type();
        ObSysFunRawExpr *wrapped_expr = NULL;
        const bool is_same_need = false;
        if (OB_FAIL(wrap_type_to_str_if_necessary(right_param, calc_type,
                                                  is_same_need, wrapped_expr))) {
          LOG_WARN("failed to wrap_type_to_str_if_necessary", K(ret));
        } else if ((NULL != wrapped_expr) && OB_FAIL(expr.replace_param_expr(1, wrapped_expr))) {
          LOG_WARN("failed to replace right param expr", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObRawExprWrapEnumSet::visit_query_ref_expr(ObQueryRefRawExpr &expr,
                                               const ObObjType dest_type,
                                               const bool is_same_need)
{
  int ret = OB_SUCCESS;
  if (!expr.has_enum_set_column() && !expr.has_flag(CNT_SUB_QUERY)) {
    // no-op if expr doesn't have enumset column
  } else if (1 == expr.get_output_column() && expr.is_set() &&
              ob_is_enumset_tc(expr.get_column_types().at(0).get_type())) {
    ObSelectStmt *ref_stmt = expr.get_ref_stmt();
    if (OB_ISNULL(ref_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref_stmt should not be NULL", K(expr), K(ret));
    } else if (OB_UNLIKELY(1 != ref_stmt->get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item size should be 1", "size", ref_stmt->get_select_item_size(),
                                                       K(expr), K(ret));
    } else if (OB_ISNULL(ref_stmt->get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr of select item is NULL", K(expr), K(ret));
    } else {
      ObRawExpr *enumset_expr = ref_stmt->get_select_item(0).expr_;
      ObSysFunRawExpr *new_expr = NULL;
      if (OB_FAIL(wrap_type_to_str_if_necessary(enumset_expr, dest_type,
                                                is_same_need, new_expr))) {
        LOG_WARN("failed to wrap_type_to_str_if_necessary", K(ret));
      } else if (NULL != new_expr) {
        // replace with new wrapped expr
        ref_stmt->get_select_item(0).expr_ = new_expr;
        expr.get_column_types().at(0) = new_expr->get_result_type();
        LOG_TRACE("succeed to wrap enum to str", K(dest_type), K(*new_expr), K(expr));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObRawExprWrapEnumSet::wrap_param_expr(ObIArray<ObRawExpr*> &param_exprs, ObObjType dest_type)
{
  int ret = OB_SUCCESS;
  const bool is_same_need = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); ++i) {
    ObRawExpr *param_expr = param_exprs.at(i);
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("real param expr is null", K(i));
    } else if (ob_is_enumset_tc(param_expr->get_data_type())) {
      ObSysFunRawExpr *wrapped_expr = NULL;
      if (OB_FAIL(wrap_type_to_str_if_necessary(param_expr, dest_type,
                                                is_same_need, wrapped_expr))) {
        LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
      } else if (NULL != wrapped_expr) {
        param_exprs.at(i) = wrapped_expr;
      } else {/*do nothing*/}
    } else {/*do nothing*/}
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
