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
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
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
    // handle the target list of first level
    // In the enum/set type with subschema, we keep this behavior now, as the obj meta information
    // of the original expr is not valid that can be directly returned to the client.
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
      } else if (conv_expr->get_param_expr(4)->is_enum_set_with_subschema()) {
        ObRawExpr *arg_expr = conv_expr->get_param_expr(4);
        if (arg_expr->get_data_type() == const_value) {
          bool need_to_str = true;
          // same type
          if (OB_ISNULL(my_session_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("session is null", K(ret));
          } else if (my_session_->get_ddl_info().is_ddl()) {
            uint16_t subschema_id = 0;
            need_to_str = (arg_expr->get_subschema_id() != conv_expr->get_subschema_id());
          }
          if (OB_FAIL(ret) || !need_to_str) {
          } else if (OB_FAIL(ObRawExprUtils::create_type_to_str_expr(expr_factory_,
                                                              arg_expr,
                                                              wrapped_expr,
                                                              my_session_,
                                                              true /*is_type_to_str*/,
                                                              static_cast<ObObjType>(const_value)))) {
            LOG_WARN("failed to create_type_to_string_expr", K(ret));
          }
        }
      } else if (OB_FAIL(wrap_type_to_str_if_necessary(conv_expr->get_param_expr(4),
                                                       static_cast<ObObjType>(const_value),
                                                       is_same_need,
                                                       wrapped_expr))) {
        LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
      }
      if (OB_SUCC(ret) && NULL != wrapped_expr) {
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
        const ObRawExprResType &dst_type = stmt.get_values_desc().at(index)->get_result_type();
        if (value_expr->is_enum_set_with_subschema()) {
          if (!ob_is_enum_or_set_type(dst_type.get_type())) {
            // skip wrap to string, it can cast directly
          } else if (dst_type.is_enum_set_with_subschema() &&
              dst_type.get_subschema_id() == value_expr->get_subschema_id()) {
            // same type, no need to cast
          } else if (OB_FAIL(ObRawExprUtils::create_type_to_str_expr(expr_factory_,
                                                              value_expr,
                                                              new_expr,
                                                              my_session_,
                                                              true /*is_type_to_str*/,
                                                              dst_type.get_type()))) {
            LOG_WARN("failed to create_type_to_string_expr", K(ret));
          }
        } else if (OB_FAIL(wrap_type_to_str_if_necessary(value_expr, dst_type.get_type(),
                                                  is_same_need, new_expr))) {
          LOG_WARN("failed to wrap_type_to_str_if_necessary", K(i), K(ret));
        }
        if (OB_SUCC(ret) && NULL != new_expr) {
          value_expr = new_expr;
        }
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
      // the return type of mysql client for enum/set is FIELD_TYPE_STRING instead of
      // FIELD_TYPE_VAR_STRING.
      const ObObjType dst_type = target_expr->is_enum_set_with_subschema() ?
          ObCharType : ObVarcharType;
      if (OB_FAIL(ObRawExprUtils::create_type_to_str_expr(expr_factory_, target_expr,
                                                          new_expr,
                                                          my_session_,
                                                          is_type_to_str,
                                                          dst_type))) {
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
  if (has_enumset_expr_need_wrap(expr) || expr.has_flag(CNT_SUB_QUERY)) {
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
  UNUSED(expr);
  return OB_SUCCESS;
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
  } else if (OB_FAIL(ObRawExprUtils::need_wrap_to_string(expr->get_result_type(), dest_type,
                                                         is_same_need, need_wrap, false))) {
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
  } else {
    ObSysFunRawExpr *wrapped_expr = NULL;
    const bool is_same_need = false;
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
  if ((has_enumset_expr_need_wrap(expr) || expr.has_flag(CNT_SUB_QUERY)) &&
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

int ObRawExprWrapEnumSet::visit(ObUnpivotRawExpr &expr)
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
  if (can_wrap_type_to_str(expr)) {
    if (T_FUN_SYS_NULLIF == expr.get_expr_type()) {
      LOG_TRACE("wrap nullif expr", K(expr));
      if (OB_FAIL(wrap_nullif_expr(expr))) {
        LOG_WARN("failed to wrap nullif expr", K(ret));
      }
    }
  }
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
  } else if (has_enumset_expr_need_wrap(expr) && OB_FAIL(analyze_expr(ref_expr))) {
    LOG_WARN("failed to analyze expr", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObRawExprWrapEnumSet::wrap_nullif_expr(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  int64_t param_count = expr.get_param_count();
  if (OB_UNLIKELY(OB_ISNULL(my_session_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret));
  } else if (OB_UNLIKELY(T_FUN_SYS_NULLIF != expr.get_expr_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(ret));
  } else if (OB_UNLIKELY(2 != param_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param count", K(param_count), K(ret));
  } else {
    ObRawExpr *left_param = expr.get_param_expr(0);
    ObRawExpr *right_param = expr.get_param_expr(1);
    if (OB_ISNULL(left_param) || OB_ISNULL(right_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is NULL", KP(left_param), KP(right_param), K(ret));
    } else {
      if (ob_is_enumset_tc(left_param->get_data_type())) {
        ObObjType calc_type = expr.get_extra_calc_meta().get_type();
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
        ObObjType calc_type = expr.get_extra_calc_meta().get_type();
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
  if (!has_enumset_expr_need_wrap(expr) && !expr.has_flag(CNT_SUB_QUERY)) {
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

bool ObRawExprWrapEnumSet::has_enumset_expr_need_wrap(const ObRawExpr &expr)
{
  int need_wrap = false;
  if (expr.has_enum_set_column()) {
    if (expr.get_result_type().is_enum_or_set()) {
      need_wrap = !expr.is_enum_set_with_subschema();
    }
    for (int64_t i = 0; !need_wrap && i < expr.get_param_count(); ++i) {
      const ObRawExpr *param_expr = expr.get_param_expr(i);
      if (OB_ISNULL(param_expr)) {
      } else {
        need_wrap = has_enumset_expr_need_wrap(*param_expr);
      }
    }
  }
  return need_wrap;
}

}  // namespace sql
}  // namespace oceanbase
