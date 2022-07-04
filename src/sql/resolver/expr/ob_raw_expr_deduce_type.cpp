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
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/resolver/expr/ob_raw_expr_deduce_type.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/engine/expr/ob_expr_dll_udf.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_case.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/expr/ob_expr_between.h"

namespace oceanbase {
using namespace common;
namespace sql {

int ObRawExprDeduceType::deduce(ObRawExpr& expr)
{
  return expr.postorder_accept(*this);
}

int ObRawExprDeduceType::visit(ObConstRawExpr& expr)
{
  int ret = OB_SUCCESS;
  switch (expr.get_expr_type()) {
    case T_QUESTIONMARK: {
      // For parameterized value, the result type has been set already when
      // the expr is created. See ob_raw_expr_resolver_impl.cpp
      break;
    }
    default: {
      // for testing
      if (expr.get_expr_obj_meta() != expr.get_value().get_meta()) {
        LOG_DEBUG("meta is not suited", K(expr.get_value().get_type()), K(expr.get_expr_obj_meta().get_type()), K(ret));
      }
      expr.set_meta_type(expr.get_expr_obj_meta());
      // expr.set_meta_type(expr.get_value().get_meta());
      expr.set_param(expr.get_value());
      if (!(expr.get_result_type().is_null() || (lib::is_oracle_mode() && expr.get_value().is_null_oracle()))) {
        expr.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
      }
      break;
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObVarRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (!(expr.get_result_type().is_null())) {
    expr.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObQueryRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (expr.is_cursor()) {
    expr.set_data_type(ObExtendType);
  } else if ((1 == expr.get_output_column()) && !expr.is_set()) {
    expr.set_result_type(expr.get_column_types().at(0));
    if (ob_is_enumset_tc(expr.get_data_type())) {
      const ObSelectStmt* ref_stmt = expr.get_ref_stmt();
      if (OB_ISNULL(ref_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref_stmt should not be NULL", K(expr), K(ret));
      } else if (OB_UNLIKELY(1 != ref_stmt->get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item size should be 1", "size", ref_stmt->get_select_item_size(), K(expr), K(ret));
      } else if (OB_ISNULL(ref_stmt->get_select_item(0).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr of select item is NULL", K(expr), K(ret));
      } else if (OB_FAIL(expr.set_enum_set_values(ref_stmt->get_select_item(0).expr_->get_enum_set_values()))) {
        LOG_WARN("failed to set enum_set_values", K(expr), K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    // for enumset query ref `is_set`, need warp enum_to_str/set_to_str expr at
    // `ObRawExprWrapEnumSet::visit_query_ref_expr`
    expr.set_data_type(ObIntType);
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObColumnRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  // @see ObStmt::create_raw_column_expr()
  if (ob_is_string_or_lob_type(expr.get_data_type()) &&
      (CS_TYPE_INVALID == expr.get_collation_type() || CS_LEVEL_INVALID == expr.get_collation_level())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta of binary_ref", K(expr));
  }
  return ret;
}

int ObRawExprDeduceType::calc_result_type_with_const_arg(ObNonTerminalRawExpr& expr, ObIExprResTypes& types,
    ObExprTypeCtx& type_ctx, ObExprOperator* op, ObExprResType& result_type, int32_t row_dimension)
{
#define GET_TYPE_ARRAY(types) (types.count() == 0 ? NULL : &(types.at(0)))

  int ret = OB_SUCCESS;
  bool all_const = false;
  ObArray<ObObj*> arg_arrs;
  if (0 <= expr.get_param_count()) {
    all_const = true;
    for (int64_t i = 0; all_const && i < expr.get_param_count(); ++i) {
      ObRawExpr* arg = expr.get_param_expr(i);
      if (OB_ISNULL(arg)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument.", K(ret));
      } else if (!arg->is_const_expr()) {
        all_const = false;
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument.", K(ret));
      } else {
        ObObj& value = static_cast<ObConstRawExpr*>(arg)->get_value();
        if (value.is_unknown()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument.", K(ret));
        } else if (OB_FAIL(arg_arrs.push_back(&value))) {
          LOG_WARN("fail to push back argument", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && all_const) {
    if (ObExprOperator::NOT_ROW_DIMENSION != row_dimension) {
      ret = op->calc_result_typeN(result_type, GET_TYPE_ARRAY(types), types.count(), type_ctx, arg_arrs);
    } else {
      switch (op->get_param_num()) {
        case 0:
          if (OB_UNLIKELY(types.count() != 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param type count is mismatch", K(types.count()));
          } else if (OB_FAIL(op->calc_result_type0(result_type, type_ctx, arg_arrs))) {
            LOG_WARN("calc result type0 failed", K(ret));
          }
          break;
        case 1:
          if (OB_UNLIKELY(types.count() != 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param type count is mismatch", K(types.count()));
          } else if (OB_FAIL(op->calc_result_type1(result_type, types.at(0), type_ctx, arg_arrs))) {
            LOG_WARN("calc result type1 failed", K(ret));
          }
          break;
        case 2:
          if (OB_UNLIKELY(types.count() != 2)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param type count is mismatch", K(expr), K(types.count()));
          } else if (OB_FAIL(op->calc_result_type2(result_type, types.at(0), types.at(1), type_ctx, arg_arrs))) {
            LOG_WARN("calc result type2 failed", K(ret));
          }
          break;
        case 3:
          if (OB_UNLIKELY(types.count() != 3)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param type count is mismatch", K(types.count()));
          } else if (OB_FAIL(op->calc_result_type3(
                         result_type, types.at(0), types.at(1), types.at(2), type_ctx, arg_arrs))) {
            LOG_WARN("calc result type3 failed", K(ret));
          }
          break;
        default:
          ret = op->calc_result_typeN(result_type, GET_TYPE_ARRAY(types), types.count(), type_ctx, arg_arrs);
          break;
      }  // end switch
    }
  }
#undef GET_TYPE_ARRAY
  return ret;
}

int ObRawExprDeduceType::assign_var_expr_result_type(ObRawExpr* expr, const ObRawExpr* origin_param, bool& assigned)
{
  int ret = OB_SUCCESS;
  assigned = false;
  if (OB_ISNULL(expr) || OB_ISNULL(origin_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    if (ObRawExpr::EXPR_VAR == expr->get_expr_class()) {
      ObVarRawExpr* var_expr = static_cast<ObVarRawExpr*>(expr);
      if (!var_expr->get_result_type_assigned()) {
        assigned = true;
        expr->set_meta_type(origin_param->get_result_meta());
        var_expr->set_result_type_assigned(true);
        if (ObEnumType == expr->get_data_type() || ObSetType == expr->get_data_type()) {
          expr->set_enum_set_values(origin_param->get_enum_set_values());
        }
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::assign_var_exprs_result_type(ObNonTerminalRawExpr& expr, ObIExprResTypes& types)
{
  int ret = OB_SUCCESS;
  ObItemType expr_type = expr.get_expr_type();
  bool assigned = false;
  if (T_FUN_SYS_GREATEST_INNER == expr_type || T_FUN_SYS_LEAST_INNER == expr_type ||
      (T_FUN_SYS_NULLIF == expr_type && 6 == expr.get_param_count())) {
    int64_t real_param_num = expr.get_param_count() / 3;
    for (int64_t i = 0; i < real_param_num; i++) {
      if (OB_FAIL(
              assign_var_expr_result_type(expr.get_param_expr(i + real_param_num), expr.get_param_expr(i), assigned))) {
        LOG_WARN("assign var expr result type failed", K(ret));
      } else if (assigned) {
        types.at(i + real_param_num).set_meta(types.at(i).get_obj_meta());
      }
      if (OB_FAIL(assign_var_expr_result_type(
              expr.get_param_expr(i + real_param_num * 2), expr.get_param_expr(i), assigned))) {
        LOG_WARN("assign var expr result type failed", K(ret));
      } else if (assigned) {
        types.at(i + real_param_num * 2).set_meta(types.at(i).get_obj_meta());
      }
    }
  } else if (T_FUN_SYS_FROM_UNIX_TIME == expr_type && 3 == expr.get_param_count()) {
    if (OB_FAIL(assign_var_expr_result_type(expr.get_param_expr(2), expr.get_param_expr(1), assigned))) {
      LOG_WARN("assign var expr result type failed", K(ret));
    } else if (assigned) {
      types.at(2).set_meta(types.at(1).get_obj_meta());
    }
  }
  return ret;
}

bool need_calc_json_as_int(ObItemType item_type) 
{
  return item_type == T_OP_BOOL;
}

bool need_calc_json_as_text(ObItemType item_type)
{
  bool bool_ret = true;
  if (T_FUN_SYS < item_type && item_type < T_FUN_SYS_END) {
    if (T_FUN_SYS_JSON_OBJECT <= item_type && item_type <= T_FUN_SYS_JSON_VALUE) {
      bool_ret = false; // json calc type is decided by json functions
    }
  }
  return bool_ret; // json calc type set to long text in other sql functions
}

int ObRawExprDeduceType::calc_result_type(
    ObNonTerminalRawExpr& expr, ObIExprResTypes& types, ObCastMode& cast_mode, int32_t row_dimension)
{
#define GET_TYPE_ARRAY(types) (types.count() == 0 ? NULL : &(types.at(0)))

  int ret = OB_SUCCESS;
  ObExprTypeCtx type_ctx;
  type_ctx.set_raw_expr(&expr);
  ObExprOperator* op = expr.get_op();
  ObExprResTypes ori_types;
  const bool is_explicit_cast = (T_FUN_SYS_CAST == expr.get_expr_type()) && CM_IS_EXPLICIT_CAST(expr.get_extra());
  bool use_static_engine = false;
  if (NULL == op) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Get expression operator failed", "expr type", expr.get_expr_type());
  } else if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (FALSE_IT(use_static_engine = my_session_->use_static_typing_engine())) {
  } else if (use_static_engine && op->is_default_expr_cg()) {
    ret = STATIC_ENG_NOT_IMPLEMENT;
    LOG_INFO("not implemented in sql static typing engine, "
             "will retry the old engine automatically",
        K(ret),
        K(op->get_type()),
        K(op->get_name()));
  } else if (use_static_engine &&
             OB_FAIL(ObSQLUtils::get_default_cast_mode(is_explicit_cast, 0, my_session_, cast_mode))) {
    LOG_WARN("get_default_cast_mode failed", K(ret));
  } else if (use_static_engine && assign_var_exprs_result_type(expr, types)) {
    LOG_WARN("assign var expr result type failed", K(ret));
  } else if (expr.get_expr_type() == T_FUN_NORMAL_UDF && OB_FAIL(init_normal_udf_expr(expr, op))) {
    LOG_WARN("failed to init normal udf", K(ret));
  } else if (OB_FAIL(ori_types.assign(types))) {
    LOG_WARN("array assign failed", K(ret));
  } else {
    op->set_raw_expr(&expr);
    FOREACH_CNT(type, types) {
      if (ObLobType == type->get_type()) {
        type->set_type(ObLongTextType);
      }
      if (ObJsonType == type->get_type()) {
        if (need_calc_json_as_text(expr.get_expr_type())) {
          // ToDo: test and fix, not all sql functions need calc json as long text
          type->set_calc_type(ObLongTextType);  
        }
      }
    }
    op->set_row_dimension(row_dimension);
    op->set_real_param_num(static_cast<int32_t>(types.count()));
    ObSQLUtils::init_type_ctx(my_session_, type_ctx);
    type_ctx.set_cast_mode(cast_mode);
    //    type_ctx.my_session_ = this->my_session_;
    ObExprResType result_type(alloc_);

    // Set the calc_type of all parameters to be consistent with type in advance,
    // in case calc_result_typeX is not set
    // Ideally, this loop should not be required, all calc_type settings are done in calc_result_typeX

    // For avg(), internally it will call 'divison', which requires that both input are
    // casted into number. However, this requirements are not remembered in the input_types
    // for the avg() expression but as the calc_type for the input expression itself. This
    // demands that we set the calculation type here.
    for (int64_t i = 0; i < types.count(); ++i) {
      types.at(i).set_calc_meta(types.at(i));
    }
    if (ObExprOperator::NOT_ROW_DIMENSION != row_dimension) {
      ret = op->calc_result_typeN(result_type, GET_TYPE_ARRAY(types), types.count(), type_ctx);
    } else {
      switch (op->get_param_num()) {
        case 0:
          if (OB_UNLIKELY(types.count() != 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param type count is mismatch", K(types.count()));
          } else if (OB_FAIL(op->calc_result_type0(result_type, type_ctx))) {
            LOG_WARN("calc result type0 failed", K(ret));
          }
          break;
        case 1:
          if (OB_UNLIKELY(types.count() != 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param type count is mismatch", K(types.count()));
          } else if (OB_FAIL(op->calc_result_type1(result_type, types.at(0), type_ctx))) {
            LOG_WARN("calc result type1 failed", K(ret));
          }
          break;
        case 2:
          if (OB_UNLIKELY(types.count() != 2)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param type count is mismatch", K(expr), K(types.count()));
          } else if (OB_FAIL(op->calc_result_type2(result_type, types.at(0), types.at(1), type_ctx))) {
            LOG_WARN("calc result type2 failed", K(ret));
          }
          break;
        case 3:
          if (OB_UNLIKELY(types.count() != 3)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param type count is mismatch", K(types.count()));
          } else if (OB_FAIL(op->calc_result_type3(result_type, types.at(0), types.at(1), types.at(2), type_ctx))) {
            LOG_WARN("calc result type3 failed", K(ret));
          }
          break;
        default:
          ret = op->calc_result_typeN(result_type, GET_TYPE_ARRAY(types), types.count(), type_ctx);
          break;
      }  // end switch
    }
    if (OB_NOT_IMPLEMENT == ret) {
      if (OB_FAIL(calc_result_type_with_const_arg(expr, types, type_ctx, op, result_type, row_dimension))) {
        if (OB_NOT_IMPLEMENT == ret) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("function not implement calc result type", K(ret));
        }
        LOG_WARN("fail to calc result type with const arguments", K(ret));
      }
    }
    // check parameters can cast to expected type
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ori_types.count(); i++) {
        const ObObjType from = ori_types.at(i).get_type();
        const ObCollationType from_cs_type = ori_types.at(i).get_collation_type();
        const ObObjType to = types.at(i).get_calc_type();
        const ObCollationType to_cs_type = types.at(i).get_calc_collation_type();
        LOG_DEBUG("check parameters can cast to expected type", K(ret), K(i), K(from), K(to));
        if (from != to && !cast_supported(from, from_cs_type, to, to_cs_type)) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("cast parameter to expected type not supported", K(ret), K(i), K(from), K(to));
        }
      }
    }

    LOG_DEBUG("debug for expr params calc meta", K(types));

    if (OB_SUCC(ret)
        && share::is_oracle_mode()
        && expr.get_expr_type() != T_FUN_SYS_NVL
        && !my_session_->use_static_typing_engine()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < types.count(); i++) {
        ObExprResType& param = types.at(i);
        if (param.get_calc_meta().is_character_type()) {
          if (ob_is_nstring_type(param.get_calc_type())) {
            param.set_calc_collation_type(my_session_->get_nls_collation_nation());
            param.set_length_semantics(LS_CHAR);
          } else {
            param.set_calc_collation_type(my_session_->get_nls_collation());
          }
        }
      }
      LOG_DEBUG("after auto adjusting expr params collation for oracle mode", K(types));
    }

    // The check here needs to ignore the implicit cast, because the conversion function can only
    // handle utf8 strings, so the implicit cast when it encounters non-utf8 input, it will be
    // converted to utf8, so the result of cast deduction may not meet the expectations of nls_collation_xxx
    const bool is_implicit_cast = (T_FUN_SYS_CAST == expr.get_expr_type()) && CM_IS_IMPLICIT_CAST(expr.get_extra());
    if (OB_SUCC(ret) && share::is_oracle_mode() && !is_implicit_cast && my_session_->use_static_typing_engine()) {
      if (result_type.is_nstring() && result_type.get_collation_type() != my_session_->get_nls_collation_nation()) {
        // ret = OB_ERR_UNEXPECTED;
        LOG_TRACE("[CHARSET DEDUCE RESULT TYPE]"
                  "result is nchar, but charset is not consistent with session nchar charset",
            "result collation",
            ObCharset::collation_name(result_type.get_collation_type()),
            "session nchar collation",
            ObCharset::collation_name(my_session_->get_nls_collation_nation()));
      } else if (result_type.is_varchar_or_char() && T_FUN_SYS_DBMS_LOB_CONVERT_CLOB_CHARSET != expr.get_expr_type() &&
                 result_type.get_collation_type() != my_session_->get_nls_collation()) {
        // ret = OB_ERR_UNEXPECTED;
        LOG_TRACE("[CHARSET DEDUCE RESULT TYPE]"
                  "result is char, but charset is not consistent with session char charset",
            "result collation",
            ObCharset::collation_name(result_type.get_collation_type()),
            "session char collation",
            ObCharset::collation_name(my_session_->get_nls_collation()));
      }
    }

    if (OB_SUCC(ret)) {
      ObItemType item_type = expr.get_expr_type();
      if (T_FUN_SYS_UTC_TIME == item_type || T_FUN_SYS_UTC_TIMESTAMP == item_type || T_FUN_SYS_CUR_TIMESTAMP == item_type ||
          T_FUN_SYS_LOCALTIMESTAMP == item_type || T_FUN_SYS_CUR_TIME == item_type || T_FUN_SYS_SYSDATE == item_type ||
          T_FUN_SYS_SYSTIMESTAMP == item_type) {
        /*
         * the precision N has been set in expr.result_type_.scale_, but result_type returned by
         * op->calc_result_type0() has no precision, so we have to copy this info to result_type first.
         */
        result_type.set_accuracy(expr.get_accuracy());
      }

      op->set_result_type(result_type);
      ARRAY_FOREACH_N(types, idx, cnt)
      {
        ObExprResType& type = types.at(idx);
        if (ObLobType == ori_types.at(idx).get_type()) {
          if (OB_UNLIKELY(ObLongTextType != type.get_type())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("param with origin lob type should be replace to longtext type", K(ret), K(idx), K(expr), K(type));
          } else {
            type.set_type(ObLobType);
          }
        }
      }

      expr.set_result_type(result_type);
      ret = expr.set_input_types(types);
    }

    if (OB_SUCC(ret)) {
      cast_mode = type_ctx.get_cast_mode();
      if (expr.get_result_type().has_result_flag(OB_MYSQL_ZEROFILL_FLAG)) {
        cast_mode |= CM_ZERO_FILL;
      }
    }
    LOG_DEBUG("calc_result_type", K(ret), K(expr), K(types));
  }
#undef GET_TYPE_ARRAY
  return ret;
}

int ObRawExprDeduceType::visit(ObOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_session_ is NULL", K(ret));
  } else if (OB_FAIL(check_expr_param(expr))) {
    LOG_WARN("check expr param failed", K(ret));
  } else if (OB_UNLIKELY(expr.get_expr_type() == T_OBJ_ACCESS_REF)) {
    // ObObjAccessRawExpr &obj_access_expr = static_cast<ObObjAccessRawExpr &>(expr);
    // ObExprResType result_type;
    // pl::ObPLDataType final_type;
    // if (OB_FAIL(obj_access_expr.get_final_type(final_type))) {
    //   LOG_WARN("failed to get final type", K(obj_access_expr), K(ret));
    // } else if (final_type.is_user_type()) {
    //   result_type.set_ext();
    //   result_type.set_extend_type(final_type.get_type());
    //   result_type.set_udt_id(final_type.get_user_type_id());
    // } else if (OB_ISNULL(final_type.get_data_type())) {
    //   ret = OB_ERR_UNEXPECTED;
    //   LOG_WARN("basic type must not be null", K(ret));
    // } else {
    //   result_type.set_meta(final_type.get_data_type()->get_meta_type());
    //   result_type.set_accuracy(final_type.get_data_type()->get_accuracy());
    // }

    // expr.set_result_type(result_type);
  } else if (T_OP_ORACLE_OUTER_JOIN_SYMBOL == expr.get_expr_type()) {
    ObRawExpr* param_expr = NULL;
    if (OB_UNLIKELY(1 != expr.get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get expr", K(ret));
    } else if (OB_ISNULL(param_expr = expr.get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL param expr", K(ret));
    } else {
      expr.set_result_type(param_expr->get_result_type());
    }
  } else if (T_OP_MULTISET == expr.get_expr_type()) {
    ObRawExpr* left = expr.get_param_expr(0);
    ObRawExpr* right = expr.get_param_expr(1);
    if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("multiset op' children is null.", K(expr), K(ret));
    } else {
      expr.set_result_type(left->get_result_type());
    }
  } else if (T_OP_COLL_PRED == expr.get_expr_type()) {
    ObExprResType result_type;
    result_type.set_tinyint();
    result_type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    result_type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    expr.set_result_type(result_type);
  } else {
    if (T_OP_ROW != expr.get_expr_type()) {
      ObExprOperator* op = expr.get_op();
      if (NULL == op) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Get expression operator failed", "expr type", expr.get_expr_type());
      } else {
        ObExprResTypes types;
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
          const ObRawExpr* param_expr = expr.get_param_expr(i);
          if (OB_ISNULL(param_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param expr is null", K(i));
          } else if (T_OP_ROW == param_expr->get_expr_type()) {
            if (OB_FAIL(get_row_expr_param_type(*param_expr, types))) {
              LOG_WARN("get row expr param type failed", K(ret));
            }
          } else if (T_REF_QUERY == param_expr->get_expr_type() && T_OP_EXISTS != expr.get_expr_type() &&
                     T_OP_NOT_EXISTS != expr.get_expr_type()) {
            const ObQueryRefRawExpr* ref_expr = static_cast<const ObQueryRefRawExpr*>(param_expr);
            const ObIArray<ObExprResType>& column_types = ref_expr->get_column_types();
            for (int64_t j = 0; OB_SUCC(ret) && j < column_types.count(); ++j) {
              if (OB_FAIL(types.push_back(column_types.at(j)))) {
                LOG_WARN("push back param type failed", K(ret));
              }
            }
          } else if (OB_FAIL(types.push_back(param_expr->get_result_type()))) {
            LOG_WARN("push back param type failed", K(ret));
          }
        } /* end for */
        if (OB_SUCC(ret)) {
          int32_t row_dimension = ObExprOperator::NOT_ROW_DIMENSION;
          ObCastMode cast_mode = CM_NONE;
          if (expr.get_param_count() > 0) {
            if (OB_ISNULL(expr.get_param_expr(0))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("param expr is null");
            } else if (T_OP_ROW == expr.get_param_expr(0)->get_expr_type()) {
              row_dimension = static_cast<int32_t>(expr.get_param_expr(0)->get_param_count());
            } else if (expr.get_param_expr(0)->has_flag(IS_SUB_QUERY)) {
              ObQueryRefRawExpr* ref_expr = static_cast<ObQueryRefRawExpr*>(expr.get_param_expr(0));
              if (T_OP_EXISTS == expr.get_expr_type() || T_OP_NOT_EXISTS == expr.get_expr_type()) {
                // let row_dimension of exists be ObExprOperator::NOT_ROW_DIMENSION
              } else if (ref_expr->get_output_column() > 1) {
                // The result of the subquery as a vector
                row_dimension = static_cast<int32_t>(ref_expr->get_output_column());
              } else if (T_OP_IN == expr.get_expr_type() || T_OP_NOT_IN == expr.get_expr_type()) {
                row_dimension = 1;
              }
            } else if (T_OP_IN == expr.get_expr_type() || T_OP_NOT_IN == expr.get_expr_type()) {
              row_dimension = 1;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(calc_result_type(expr, types, cast_mode, row_dimension))) {
            LOG_WARN("fail calc result type", K(ret));
          } else if (OB_FAIL(op->set_input_types(types))) {
            LOG_WARN("fail convert expr calc result type to op func input types", K(ret), K(types));
          } else if (OB_ISNULL(my_session_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("my_session_ is NULL", K(ret));
          } else if (my_session_->use_static_typing_engine() && expr.deduce_type_adding_implicit_cast() &&
                     OB_FAIL(add_implicit_cast(expr, cast_mode))) {
            LOG_WARN("fail add_implicit_cast", K(ret), K(expr));
          }
        } /* end if */
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::check_row_param(ObOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  bool cnt_row = false;
  bool cnt_scalar = false;
  if (T_OP_ROW == expr.get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
      if (OB_ISNULL(expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get param expr failed", K(i));
      } else if (T_OP_ROW == expr.get_param_expr(i)->get_expr_type()) {
        cnt_row = true;
      } else {
        cnt_scalar = true;
      }
      if (OB_SUCC(ret) && cnt_row && cnt_scalar) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::check_param_expr_op_row(ObRawExpr* param_expr, int64_t column_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null param expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_expr->get_param_count(); ++i) {
      if (OB_ISNULL(param_expr->get_param_expr(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null param expr", K(ret));
      } else if (T_OP_ROW == param_expr->get_param_expr(i)->get_expr_type()) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_WARN("invalid relational operator", K(ret));
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, column_count);
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::check_expr_param(ObOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_row_param(expr))) {
    LOG_WARN("check row param failed", K(ret));
  } else if (T_OP_IN == expr.get_expr_type() || T_OP_NOT_IN == expr.get_expr_type()) {
    if (OB_ISNULL(expr.get_param_expr(0)) || OB_ISNULL(expr.get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null");
    } else if (expr.get_param_expr(0)->has_flag(IS_SUB_QUERY)) {
      ObRawExpr* left_expr = expr.get_param_expr(0);
      int64_t right_output_column = 0;
      ObQueryRefRawExpr* left_ref = static_cast<ObQueryRefRawExpr*>(left_expr);
      int64_t left_output_column = left_ref->get_output_column();
      // oracle mode not allow: select 1 from dual where (select 1,2 from dual) in (1,2)
      if (is_oracle_mode() && left_output_column > 1) {
        ret = OB_ERR_TOO_MANY_VALUES;
        LOG_WARN("invalid relational operator", K(ret), K(left_output_column));
      } else if (expr.get_param_expr(1)->get_expr_type() == T_OP_ROW) {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_expr(1)->get_param_count(); i++) {
          if (T_OP_ROW == expr.get_param_expr(1)->get_param_expr(i)->get_expr_type()) {
            if (left_output_column != expr.get_param_expr(1)->get_param_expr(i)->get_param_count()) {
              ret = OB_ERR_INVALID_COLUMN_NUM;
              LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_output_column);
            }
          } else {
            if (left_output_column != 1) {
              ret = OB_ERR_INVALID_COLUMN_NUM;
              LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_output_column);
            }
          }
        }
      } else if (expr.get_param_expr(1)->has_flag(IS_SUB_QUERY)) {
        right_output_column = get_expr_output_column(*expr.get_param_expr(1));
        if (left_output_column != right_output_column) {
          ret = OB_ERR_INVALID_COLUMN_NUM;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_ref->get_output_column());
        }
      } else {
        right_output_column = 1;
        if (left_output_column != right_output_column) {
          ret = OB_ERR_INVALID_COLUMN_NUM;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_ref->get_output_column());
        }
      }
    } else if (T_OP_ROW == expr.get_param_expr(0)->get_expr_type()) {
      // (c1, c2, c3) in ((0, 1, 2), (3, 4, 5)).
      int64_t column_count = expr.get_param_expr(0)->get_param_count();
      if (lib::is_oracle_mode() && 1 > column_count &&
          T_OP_ROW == expr.get_param_expr(0)->get_param_expr(0)->get_expr_type()) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_WARN("invalid relational operator", K(ret), K(column_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_expr(1)->get_param_count(); i++) {
        if (T_OP_ROW == expr.get_param_expr(1)->get_param_expr(i)->get_expr_type()) {
          if (column_count != expr.get_param_expr(1)->get_param_expr(i)->get_param_count()) {
            ret = OB_ERR_INVALID_COLUMN_NUM;
            LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, column_count);
          } else if (OB_FAIL(check_param_expr_op_row(expr.get_param_expr(1)->get_param_expr(i), column_count))) {
            LOG_WARN("failed to check param expr op row", K(ret));
          }
        } else {
          if (column_count != 1) {
            ret = OB_ERR_INVALID_COLUMN_NUM;
            LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, column_count);
          }
        }
      }
    } else if (T_OP_ROW == expr.get_param_expr(1)->get_expr_type() &&
               OB_FAIL(check_param_expr_op_row(expr.get_param_expr(1), 1))) {
      // c1 in (1, 2, 3)
      LOG_WARN("failed to check param expr op row", K(ret));
    }
  } else if (expr.has_flag(CNT_SUB_QUERY) && T_OP_ROW != expr.get_expr_type()) {
    if (IS_COMPARISON_OP(expr.get_expr_type())) {
      if (OB_UNLIKELY(expr.get_param_count() != 2) || OB_ISNULL(expr.get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr status is invalid", K(expr));
      } else if (OB_FAIL(visit_left_param(*expr.get_param_expr(0)))) {
        LOG_WARN("visit left param failed", K(ret));
      } else if (OB_FAIL(visit_right_param(expr))) {
        LOG_WARN("visit right param failed", K(ret));
      }
    } else if (T_OP_EXISTS != expr.get_expr_type() && T_OP_NOT_EXISTS != expr.get_expr_type()) {
      // In other cases, if a subquery appears in the operator, it can only be used as a scalar
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
        ObRawExpr* param_expr = expr.get_param_expr(i);
        if (OB_ISNULL(param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(i));
        } else if (get_expr_output_column(*param_expr) != 1) {
          ret = OB_ERR_INVALID_COLUMN_NUM;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
        }
      }
    }
  } else if (lib::is_oracle_mode() && (T_OP_EQ == expr.get_expr_type() || T_OP_NE == expr.get_expr_type()) &&
             (T_OP_ROW == expr.get_param_expr(0)->get_expr_type())) {
    if (1 > expr.get_param_expr(0)->get_param_count() ||
        T_OP_ROW == expr.get_param_expr(0)->get_param_expr(0)->get_expr_type() ||
        T_OP_ROW != expr.get_param_expr(1)->get_expr_type()) {
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_WARN("invalid relational operator", K(ret), K(expr.get_param_expr(0)->get_param_count()));
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, expr.get_param_expr(0)->get_param_count());
      //(a, b) = (c, d) or (a, b) = ((c, d)) both are legal
    } else if (expr.get_param_expr(1)->get_param_count() == 1 &&
               T_OP_ROW == expr.get_param_expr(1)->get_param_expr(0)->get_expr_type()) {
      if (expr.get_param_expr(1)->get_param_expr(0)->get_param_count() != expr.get_param_expr(0)->get_param_count()) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, expr.get_param_expr(0)->get_param_count());
      } else if (OB_FAIL(check_param_expr_op_row(
                     expr.get_param_expr(1)->get_param_expr(0), expr.get_param_expr(0)->get_param_count()))) {
        LOG_WARN("failed to check param expr op row", K(ret));
      }
    } else if (expr.get_param_expr(1)->get_param_count() > 1) {
      if (expr.get_param_expr(1)->get_param_count() != expr.get_param_expr(0)->get_param_count()) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, expr.get_param_expr(0)->get_param_count());
      } else if (OB_FAIL(check_param_expr_op_row(expr.get_param_expr(1), expr.get_param_expr(0)->get_param_count()))) {
        LOG_WARN("failed to check expr op row", K(ret));
      }
    } else {
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, expr.get_param_expr(0)->get_param_count());
    }
  } else if (IS_COMMON_COMPARISON_OP(expr.get_expr_type())) {
    ObRawExpr* left_expr = expr.get_param_expr(0);
    ObRawExpr* right_expr = expr.get_param_expr(1);
    if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(left_expr), K(right_expr));
    } else {
      int64_t left_param_num = (T_OP_ROW == left_expr->get_expr_type()) ? left_expr->get_param_count() : 1;
      int64_t right_param_num = (T_OP_ROW == right_expr->get_expr_type()) ? right_expr->get_param_count() : 1;
      if (left_param_num != right_param_num) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_param_num);
      } else if ((T_OP_ROW == left_expr->get_expr_type()) && (T_OP_ROW == right_expr->get_expr_type())) {
        for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
          ObRawExpr* the_expr = expr.get_param_expr(i);
          if (OB_ISNULL(the_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param expr is null", K(i));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < the_expr->get_param_count(); ++j) {
            if (OB_ISNULL(the_expr->get_param_expr(j))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("param expr is null", K(j));
            } else if (T_OP_ROW == the_expr->get_param_expr(j)->get_expr_type()) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "Nested row in expression");
            }
          }
        }  // end for
      }
    }
  } else if (T_OP_ROW != expr.get_expr_type() && OB_FAIL(check_param_expr_op_row(&expr, 1))) {
    LOG_WARN("failed to check param expr op row", K(ret));
  }
  return ret;
}

int ObRawExprDeduceType::visit_left_param(ObRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (T_OP_ROW == expr.get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
      // The operator on the left is a vector, so each element in the vector can only be a scalar
      ObRawExpr* left_param = expr.get_param_expr(i);
      if (OB_ISNULL(left_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left param is null", K(ret));
      } else if (left_param->has_flag(IS_SUB_QUERY)) {
        ObQueryRefRawExpr* left_ref = static_cast<ObQueryRefRawExpr*>(left_param);
        if (left_ref->get_output_column() != 1) {
          ret = OB_ERR_INVALID_COLUMN_NUM;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
        }
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit_right_param(ObOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* right_expr = expr.get_param_expr(1);
  OB_ASSERT(right_expr);
  int64_t left_output_column = 0;
  if (OB_ISNULL(expr.get_param_expr(0)) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(expr.get_param_expr(0)), K(right_expr));
  } else if (expr.get_param_expr(0)->get_expr_type() == T_OP_ROW) {
    // If it is a vector, then the number of output columns on the left is the number of vector expressions
    left_output_column = expr.get_param_expr(0)->get_param_count();
  } else if (expr.get_param_expr(0)->has_flag(IS_SUB_QUERY)) {
    // oracle mode not allow:
    //  select 1 from dual where (select 1,2 from dual) in (select 1,2 from dual)
    left_output_column = get_expr_output_column(*expr.get_param_expr(0));
    if (is_oracle_mode() && left_output_column > 1) {
      ret = OB_ERR_TOO_MANY_VALUES;
      LOG_WARN("invalid relational operator", K(ret), K(left_output_column));
    }
  } else {
    left_output_column = 1;
  }
  if (OB_SUCC(ret)) {
    if (right_expr->has_flag(IS_SUB_QUERY)) {
      // If the right operator is composed of a subquery, compare the number of output
      // columns of the left and right operators
      ObQueryRefRawExpr* right_ref = static_cast<ObQueryRefRawExpr*>(right_expr);
      // According to the semantics of mysql, only =[ANY/ALL](subquery) allows multiple column comparisons
      // eg: select * from t1 where (c1, c2)=ANY(select c1, c2 from t2)
      // if the result of the subquery on the right is not a set, but a vector, no matter what
      // the comparison operation is, multiple columns can appear
      // eg: select * from t1 where ROW(1, 2)=(select c1, c2 from t2 where c1=1)
      // other operators can only be single-column comparison
      // eg: select * from t1 where c1>ANY(select c1 from t2)
      if (T_OP_SQ_EQ == expr.get_expr_type() || T_OP_SQ_NSEQ == expr.get_expr_type() ||
          T_OP_SQ_NE == expr.get_expr_type() || expr.has_flag(IS_WITH_SUBQUERY)) {
        if (right_ref->get_output_column() != left_output_column) {
          ret = OB_ERR_INVALID_COLUMN_NUM;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_output_column);
        }
      } else {
        if (right_ref->get_output_column() != 1 || left_output_column != 1) {
          ret = OB_ERR_INVALID_COLUMN_NUM;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
        }
      }
    } else if (right_expr->get_expr_type() == T_OP_ROW) {
      ObOpRawExpr* right_op_expr = static_cast<ObOpRawExpr*>(right_expr);
      if (expr.has_flag(IS_IN)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < right_op_expr->get_param_count(); ++i) {
          if (get_expr_output_column(*(right_op_expr->get_param_expr(i))) != left_output_column) {
            ret = OB_ERR_INVALID_COLUMN_NUM;
            LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_output_column);
          }
        }
      } else {
        if (lib::is_oracle_mode()) {
          if (right_op_expr->get_param_count() != 1 || right_op_expr->get_param_expr(0)->get_expr_type() != T_OP_ROW) {
            ret = OB_ERR_INVALID_COLUMN_NUM;
            LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
          } else {
            right_op_expr = static_cast<ObOpRawExpr*>(right_op_expr->get_param_expr(0));
          }
        }
        if (OB_SUCC(ret) && get_expr_output_column(*right_op_expr) != left_output_column) {
          ret = OB_ERR_INVALID_COLUMN_NUM;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_output_column);
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < right_op_expr->get_param_count(); ++i) {
          if (get_expr_output_column(*(right_op_expr->get_param_expr(i))) != 1) {
            ret = OB_ERR_INVALID_COLUMN_NUM;
            LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
          }
        }
      }
    } else {
      // The right operator is neither a subquery nor a vector. As a normal operator, the expression
      // on the left must be an output column
      if (left_output_column != 1) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_output_column);
      }
    }
  }
  return ret;
}

int64_t ObRawExprDeduceType::get_expr_output_column(const ObRawExpr& expr)
{
  int64_t output_column_cnt = 1;
  if (expr.has_flag(IS_SUB_QUERY)) {
    output_column_cnt = static_cast<const ObQueryRefRawExpr&>(expr).is_cursor()
                            ? 1
                            : static_cast<const ObQueryRefRawExpr&>(expr).get_output_column();
  } else if (T_OP_ROW == expr.get_expr_type()) {
    output_column_cnt = expr.get_param_count();
  }
  return output_column_cnt;
}

int ObRawExprDeduceType::visit(ObCaseOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObExprOperator* op = expr.get_op();
  if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (NULL == op) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Get expression operator failed", "expr type", expr.get_expr_type());
  } else {
    ObExprResTypes types;
    ObRawExpr* arg_param = expr.get_arg_param_expr();
    if (NULL != arg_param) {
      if (1 != get_expr_output_column(*arg_param)) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      } else {
        ret = types.push_back(arg_param->get_result_type());
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_when_expr_size(); i++) {
      const ObRawExpr* when_expr = expr.get_when_param_expr(i);
      if (OB_ISNULL(when_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("when exprs is null");
      } else if (1 != get_expr_output_column(*when_expr)) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      } else {
        ret = types.push_back(when_expr->get_result_type());
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_then_expr_size(); i++) {
      const ObRawExpr* then_expr = expr.get_then_param_expr(i);
      if (OB_ISNULL(then_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("then exprs is null");
      } else if (1 != get_expr_output_column(*then_expr)) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      } else {
        ret = types.push_back(then_expr->get_result_type());
      }
    }
    if (OB_SUCC(ret) && expr.get_default_param_expr() != NULL) {
      const ObRawExpr* def_expr = expr.get_default_param_expr();
      if (OB_ISNULL(def_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("default expr of case expr is NULL", K(ret));
      } else if (1 != get_expr_output_column(*def_expr)) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      } else {
        ret = types.push_back(def_expr->get_result_type());
      }
    }
    if (OB_SUCC(ret)) {
      ObCastMode cast_mode = CM_NONE;
      if (OB_FAIL(calc_result_type(expr, types, cast_mode, ObExprOperator::NOT_ROW_DIMENSION))) {
        LOG_WARN("calc_result_type failed", K(ret));
      } else if (my_session_->use_static_typing_engine() && T_OP_ARG_CASE != expr.get_expr_type() &&
                 OB_FAIL(add_implicit_cast(expr, cast_mode))) {
        // only add_implicit_cast for T_OP_CASE, T_OP_ARG_CASE will be transformed
        // to T_OP_CASE in transform phase
        LOG_WARN("add_implicit_cast failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObAggFunRawExpr& expr)
{
  ObScale avg_scale_increment_ = 4;
  ObScale sum_scale_increment_ = 0;
  ObScale scale_increment_recover = -2;
  int ret = OB_SUCCESS;
  ObExprResType result_type(alloc_);
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    ObRawExpr* param_expr = NULL;
    if (OB_ISNULL(param_expr = expr.get_param_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get param expr failed", K(i));
    } else if (T_FUN_GROUP_CONCAT != expr.get_expr_type() && T_FUN_COUNT != expr.get_expr_type() &&
               T_FUN_APPROX_COUNT_DISTINCT != expr.get_expr_type() &&
               T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS != expr.get_expr_type() &&
               1 != get_expr_output_column(*param_expr)) {
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
    } else if (OB_UNLIKELY(
                   is_oracle_mode() &&
                   (ObLongTextType == param_expr->get_data_type() || ob_is_lob_locator(param_expr->get_data_type())) &&
                   T_FUN_MEDIAN != expr.get_expr_type() && T_FUN_GROUP_PERCENTILE_CONT != expr.get_expr_type() &&
                   T_FUN_GROUP_PERCENTILE_DISC != expr.get_expr_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("lob type parameter not expected", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    bool need_add_cast = false;
    switch (expr.get_expr_type()) {
      case T_FUN_COUNT:
      case T_FUN_REGR_COUNT:
      case T_FUN_COUNT_SUM:
      case T_FUN_APPROX_COUNT_DISTINCT:
      case T_FUN_KEEP_COUNT: {
        if (lib::is_oracle_mode()) {
          result_type.set_number();
          result_type.set_scale(0);
          result_type.set_precision(OB_MAX_NUMBER_PRECISION);
          expr.set_result_type(result_type);
        } else {
          expr.set_data_type(ObIntType);
          expr.set_scale(0);
          expr.set_precision(MAX_BIGINT_WIDTH);
        }
        break;
      }
      case T_FUN_WM_CONCAT:
      case T_FUN_KEEP_WM_CONCAT: {
        need_add_cast = true;
        result_type.set_clob_locator();
        const ObRawExpr* param_expr = expr.get_param_expr(0);
        if (OB_ISNULL(param_expr) || OB_ISNULL(my_session_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected NULL", K(param_expr), K(my_session_), K(ret));
        } else {
          result_type.set_accuracy(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][ObLobType]);
          result_type.set_collation_type(my_session_->get_nls_collation());
          result_type.set_calc_collation_type(my_session_->get_nls_collation());
          result_type.set_collation_level(CS_LEVEL_IMPLICIT);
          expr.set_result_type(result_type);
        }
        break;
      }
      case T_FUN_JSON_ARRAYAGG: {
        result_type.set_json();
        expr.set_result_type(result_type);
        break;
      }
      case T_FUN_JSON_OBJECTAGG: {
        ObRawExpr *param_expr1 = NULL;
        ObRawExpr *param_expr2 = NULL;
        if (OB_UNLIKELY(expr.get_real_param_count() != 2) ||
            OB_ISNULL(param_expr1 = expr.get_param_expr(0)) ||
            OB_ISNULL(param_expr2 = expr.get_param_expr(1))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()),
                                           K(expr.get_real_param_count()), K(expr));
        } else {
          ObExprResType& expr_type1 = const_cast<ObExprResType&>(param_expr1->get_result_type());
          if (expr_type1.get_type() == ObNullType) {
            ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
            LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
          } else {
            need_add_cast = true;
          }
          result_type.set_json();
          expr.set_result_type(result_type);
        }
        break;
      }
      case T_FUN_GROUP_CONCAT: {
        need_add_cast = true;
        ObSEArray<ObExprResType, 6> types;
        expr.set_data_type(ObVarcharType);
        const ObIArray<ObRawExpr*>& real_parm_exprs = expr.get_real_param_exprs();
        if (share::is_oracle_mode()) {
          if (expr.get_real_param_count() > 2) {
            ret = OB_ERR_PARAM_SIZE;
            LOG_WARN("listagg has 2 params at most", K(ret), K(expr.get_real_param_count()));
          } else if (expr.get_real_param_count() == 2) {
            ObRawExpr* param_expr = NULL;
            if (OB_ISNULL(param_expr = expr.get_real_param_exprs().at(1))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret), K(param_expr));
            } else if (OB_UNLIKELY(!param_expr->has_const_or_const_expr_flag())) {
              ret = OB_ERR_ARGUMENT_SHOULD_CONSTANT;
              LOG_WARN("separator expr should be const expr", K(ret), K(*param_expr));
            } else { /*do nothing */
            }
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < real_parm_exprs.count(); ++i) {
          ObRawExpr* real_param_expr = real_parm_exprs.at(i);
          if (OB_ISNULL(real_param_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("real param expr is null", K(i));
          } else if (get_expr_output_column(*real_param_expr) != 1) {
            ret = OB_ERR_INVALID_COLUMN_NUM;
            LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, 1L);
          } else if (OB_FAIL(types.push_back(real_param_expr->get_result_type()))) {
            LOG_WARN("fail to push back result type", K(ret), K(i), K(real_param_expr->get_result_type()));
          }
        }
        CK(OB_NOT_NULL(my_session_));
        CK(OB_NOT_NULL(expr_factory_));
        ObCollationType coll_type = CS_TYPE_INVALID;
        OC((my_session_->get_collation_connection)(coll_type));

        if (OB_SUCC(ret)) {
          ObExprVersion dummy_op(alloc_);
          result_type.set_length(OB_MAX_SQL_LENGTH);
          result_type.set_varchar();
          if (OB_FAIL(dummy_op.aggregate_charsets_for_string_result(
                  result_type, (types.count() == 0 ? NULL : &(types.at(0))), types.count(), coll_type))) {
            LOG_WARN("fail to aggregate charsets for string result", K(ret), K(types));
          } else {
            expr.set_result_type(result_type);
          }
        }

        ObRawExpr* separator_expr = expr.get_separator_param_expr();
        if (OB_SUCC(ret) && my_session_->use_static_typing_engine() && NULL != separator_expr &&
            (!separator_expr->get_result_meta().is_string_type() ||
                expr.get_result_type().get_collation_type() !=
                    separator_expr->get_result_type().get_collation_type())) {
          ObExprResType result_type(alloc_);
          result_type.set_varchar();
          result_type.set_collation_type(expr.get_result_type().get_collation_type());
          ObSysFunRawExpr* cast_expr = NULL;
          if (OB_ISNULL(expr_factory_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null pointer", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                         *expr_factory_, separator_expr, result_type, cast_expr, my_session_))) {
            LOG_WARN("failed to create raw expr.", K(ret));
          } else if (OB_ISNULL(cast_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cast_expr is UNEXPECTED", K(ret));
          } else {
            expr.set_separator_param_expr(static_cast<ObRawExpr*>(cast_expr));
          }
        }
        break;
      }
      case T_FUN_VAR_POP:
      case T_FUN_VAR_SAMP:
      case T_FUN_AVG:
      case T_FUN_SUM:
      case T_FUN_KEEP_AVG:
      case T_FUN_KEEP_SUM:
      case T_FUN_KEEP_STDDEV:
      case T_FUN_KEEP_VARIANCE:
      case T_FUN_VARIANCE:
      case T_FUN_STDDEV:
      case T_FUN_STDDEV_POP:
      case T_FUN_STDDEV_SAMP: {
        need_add_cast = true;
        ObRawExpr* child_expr = NULL;
        if (OB_ISNULL(child_expr = expr.get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null");
        } else if (lib::is_oracle_mode()) {
          ObObjType from_type = child_expr->get_result_type().get_type();
          ObCollationType from_cs_type = child_expr->get_result_type().get_collation_type();
          const ObObjType to_type =
              ((ob_is_double_type(from_type) || ob_is_float_type(from_type)) ? from_type : ObNumberType);
          if (from_type != to_type && !cast_supported(from_type, from_cs_type, to_type, CS_TYPE_BINARY)) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("cast to expected type not supported", K(ret), K(from_type), K(to_type));
          } else {
            result_type.set_type(to_type);
            result_type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_scale());
            result_type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_precision());
            if (my_session_->use_static_typing_engine()) {
              result_type.set_calc_type(result_type.get_type());
              result_type.set_calc_accuracy(result_type.get_accuracy());
            }
            expr.set_result_type(result_type);
          }
        } else {  // mysql mode
          result_type = child_expr->get_result_type();
          ObObjType obj_type = result_type.get_type();
          ObScale scale_increment = 0;
          if (T_FUN_AVG == expr.get_expr_type()) {
            int64_t increment = 0;
            if (OB_ISNULL(my_session_)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument. session pointer is null", K(ret), K(my_session_));
            } else if (OB_FAIL(my_session_->get_div_precision_increment(increment))) {
              LOG_WARN("get div precision increment from session failed", K(ret));
            } else if (OB_UNLIKELY(increment < 0)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("unexpected error. negative div precision increment", K(ret), K(increment));
            } else {
              avg_scale_increment_ = static_cast<ObScale>(increment);
              scale_increment = avg_scale_increment_;
            }
          } else {
            scale_increment = sum_scale_increment_;
          }

          if (OB_FAIL(ret)) {
          } else if (T_FUN_VARIANCE == expr.get_expr_type() || T_FUN_STDDEV == expr.get_expr_type() ||
                     T_FUN_STDDEV_POP == expr.get_expr_type() || T_FUN_STDDEV_SAMP == expr.get_expr_type() ||
                     T_FUN_VAR_POP == expr.get_expr_type() || T_FUN_VAR_SAMP == expr.get_expr_type()) {
            ObObjType from_type = child_expr->get_result_type().get_type();
            const ObObjType to_type = (ob_is_double_type(from_type) ? from_type : ObDoubleType);
            result_type.set_type(to_type);
            result_type.set_scale(ObAccuracy(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET).get_scale());
            result_type.set_precision(ObAccuracy(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET).get_precision());
            if (my_session_->use_static_typing_engine()) {
              result_type.set_calc_type(result_type.get_type());
              result_type.set_calc_accuracy(result_type.get_accuracy());
            }
          } else if (ObNullType == obj_type) {
            result_type.set_double();
            if (result_type.get_scale() == -1) {
              scale_increment_recover = static_cast<ObScale>(-1);
              result_type.set_scale(static_cast<ObScale>(scale_increment));
            } else {
              scale_increment_recover = result_type.get_scale();
              result_type.set_scale(static_cast<ObScale>(result_type.get_scale() + scale_increment));
            }
          } else if (ob_is_float_tc(obj_type) || ob_is_double_tc(obj_type) || ob_is_json(obj_type)
                    || ob_is_string_type(obj_type) || ob_is_enumset_tc(obj_type)) {
            result_type.set_double();
            // todo blob and text
            if (result_type.get_scale() >= 0) {
              scale_increment_recover = result_type.get_scale();
              result_type.set_scale(static_cast<ObScale>(result_type.get_scale() + scale_increment));
            }
          } else {
            result_type.set_number();
            if (result_type.get_scale() == -1) {
              scale_increment_recover = static_cast<ObScale>(-1);
              result_type.set_scale(static_cast<ObScale>(scale_increment));
            } else {
              scale_increment_recover = result_type.get_scale();
              result_type.set_scale(static_cast<ObScale>(result_type.get_scale() + scale_increment));
            }
            result_type.set_precision(static_cast<ObPrecision>(result_type.get_precision() + scale_increment));
          }
          expr.set_result_type(result_type);
          ObObjTypeClass from_tc = expr.get_param_expr(0)->get_type_class();
          // use fast path
          need_add_cast = (ObIntTC != from_tc && ObUIntTC != from_tc);
        }
        break;
      }
      case T_FUN_MEDIAN:
      case T_FUN_GROUP_PERCENTILE_CONT:
      case T_FUN_GROUP_PERCENTILE_DISC: {
        ObRawExpr* child_expr = NULL;
        if (OB_FAIL(check_median_percentile_param(expr))) {
          LOG_WARN("failed to check median/percentile param", K(ret));
        } else if (lib::is_oracle_mode()) {
          const ObObjType from_type = expr.get_order_items().at(0).expr_->get_result_type().get_type();
          const ObCollationType from_cs_type =
              expr.get_order_items().at(0).expr_->get_result_type().get_collation_type();
          bool keep_from_type = false;
          // old sql engine can't support order by lob, So temporarily ban it.
          if (T_FUN_GROUP_PERCENTILE_DISC == expr.get_expr_type()) {
            if (OB_UNLIKELY(ob_is_lob_locator(from_type) && is_oracle_mode())) {
              ret = OB_ERR_INVALID_TYPE_FOR_OP;
              LOG_WARN("lob type parameter not expected", K(ret));
            } else {
              keep_from_type = true;
            }
          } else if (ob_is_oracle_datetime_tc(from_type) || ob_is_interval_tc(from_type) || ob_is_float_tc(from_type) ||
                     ob_is_double_tc(from_type)) {
            keep_from_type = true;
          } else if (ob_is_oracle_numeric_type(from_type)) {
            keep_from_type = false;
          } else {
            ret = OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE;
            LOG_WARN("expected numeric or date/datetime type", K(ret), K(from_type));
          }
          if (OB_SUCC(ret)) {
            const ObObjType to_type =
                keep_from_type ? from_type
                               : (T_FUN_GROUP_PERCENTILE_DISC == expr.get_expr_type() ? ObLongTextType : ObNumberType);
            const ObCollationType to_cs_type =
                keep_from_type ? from_cs_type
                               : (T_FUN_GROUP_PERCENTILE_DISC == expr.get_expr_type() ? from_cs_type : CS_TYPE_BINARY);
            if (from_type != to_type && !cast_supported(from_type, from_cs_type, to_type, to_cs_type)) {
              ret = OB_ERR_INVALID_TYPE_FOR_OP;
              LOG_WARN("cast to expected type not supported", K(ret), K(from_type), K(to_type));
            } else {
              result_type.assign(expr.get_order_items().at(0).expr_->get_result_type());
              if (from_type != to_type) {
                result_type.set_type(to_type);
              }
              result_type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_scale());
              result_type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_precision());
              expr.set_result_type(result_type);
              if (my_session_->use_static_typing_engine()) {
                ObCastMode def_cast_mode = CM_NONE;
                result_type.set_calc_type(result_type.get_type());
                result_type.set_calc_accuracy(result_type.get_accuracy());
                expr.set_result_type(result_type);
                if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_, def_cast_mode))) {
                  LOG_WARN("get_default_cast_mode failed", K(ret));
                } else if (OB_FAIL(add_median_percentile_implicit_cast(expr, def_cast_mode, keep_from_type))) {
                  LOG_WARN("failed to add median/percentile implicit cast", K(ret));
                }
              }
            }
          }
        } else {  // mysql mode
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected mysql mode", K(ret));
        }
        break;
      }
      case T_FUN_CORR:
      case T_FUN_REGR_INTERCEPT:
      case T_FUN_REGR_R2:
      case T_FUN_REGR_SLOPE:
      case T_FUN_REGR_SXX:
      case T_FUN_REGR_SYY:
      case T_FUN_REGR_SXY:
        need_add_cast = true;  // for compatible with oracle
      case T_FUN_REGR_AVGX:
      case T_FUN_REGR_AVGY:
      case T_FUN_COVAR_POP:
      case T_FUN_COVAR_SAMP: {
        if (OB_UNLIKELY(expr.get_param_count() != 2) || OB_ISNULL(expr.get_param_expr(0)) ||
            OB_ISNULL(expr.get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          ObObjType from_type1 = expr.get_param_expr(0)->get_result_type().get_type();
          ObObjType from_type2 = expr.get_param_expr(1)->get_result_type().get_type();
          ObCollationType from_cs_type1 = expr.get_param_expr(0)->get_result_type().get_collation_type();
          ObCollationType from_cs_type2 = expr.get_param_expr(1)->get_result_type().get_collation_type();
          if (expr.get_expr_type() == T_FUN_REGR_SXX ||
              expr.get_expr_type() == T_FUN_REGR_AVGX) {  // for compatible with oracle
            from_type1 = ObNumberType;
          } else if (expr.get_expr_type() == T_FUN_REGR_SYY ||
                     expr.get_expr_type() == T_FUN_REGR_AVGY) {  // for compatible with oracle
            from_type2 = ObNumberType;
          }
          ObObjType to_type = ObNumberType;
          ObCollationType to_cs_type = CS_TYPE_BINARY;
          if (ob_is_double_type(from_type1) || ob_is_float_type(from_type1) || ob_is_double_type(from_type2) ||
              ob_is_float_type(from_type2)) {
            if (ob_is_double_type(from_type1) || ob_is_double_type(from_type2)) {
              to_type = ob_is_double_type(from_type1) ? from_type1 : from_type2;
            } else {
              to_type = ob_is_float_type(from_type1) ? from_type1 : from_type2;
            }
          }
          if (from_type1 != to_type && !cast_supported(from_type1, from_cs_type1, to_type, to_cs_type)) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("cast to expected type not supported", K(ret), K(from_type1), K(to_type));
          } else if (from_type2 != to_type && !cast_supported(from_type2, from_cs_type2, to_type, to_cs_type)) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("cast to expected type not supported", K(ret), K(from_type2), K(to_type));
          } else {
            result_type.set_type(to_type);
            result_type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_scale());
            result_type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_precision());
            expr.set_result_type(result_type);
          }
        }
        break;
      }
      case T_FUN_GROUPING: {
        if (my_session_->use_static_typing_engine() && !lib::is_oracle_mode()) {
          result_type.set_int();
          expr.set_result_type(result_type);
        } else {
          result_type.set_number();
          result_type.set_scale(0);
          result_type.set_precision(OB_MAX_NUMBER_PRECISION);
          expr.set_result_type(result_type);
        }
        break;
      }
      case T_FUN_AGG_UDF: {
        if (OB_FAIL(set_agg_udf_result_type(expr))) {
          LOG_WARN("failed to set agg udf result type", K(ret));
        }
        break;
      }
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
        if (my_session_->use_static_typing_engine()) {
          const int64_t llc_bitmap_size = ObAggregateProcessor::get_llc_size();
          result_type.set_varchar();
          result_type.set_length(ObAggregateProcessor::get_llc_size());
          ObCollationType coll_type = CS_TYPE_INVALID;
          CK(OB_NOT_NULL(my_session_));
          OC((my_session_->get_collation_connection)(coll_type));
          result_type.set_collation_type(coll_type);
          result_type.set_collation_level(CS_LEVEL_IMPLICIT);
          expr.set_result_type(result_type);
        } else {
          expr.set_result_type(expr.get_param_expr(0)->get_result_type());
          expr.unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
        }
        break;
      }
      case T_FUN_GROUP_RANK:
      case T_FUN_GROUP_DENSE_RANK:
      case T_FUN_GROUP_PERCENT_RANK:
      case T_FUN_GROUP_CUME_DIST: {
        if (OB_FAIL(check_group_aggr_param(expr))) {
          LOG_WARN("failed to check group aggr param", K(ret));
        } else {
          result_type.set_type(ObNumberType);
          result_type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_scale());
          result_type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_precision());
          expr.set_result_type(result_type);
          if (my_session_->use_static_typing_engine()) {
            ObCastMode def_cast_mode = CM_NONE;
            if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_, def_cast_mode))) {
              LOG_WARN("get_default_cast_mode failed", K(ret));
            } else if (OB_FAIL(add_group_aggr_implicit_cast(expr, def_cast_mode))) {
              LOG_WARN("failed to add group aggr implicit cast", K(ret));
            }
          }
        }
        break;
      }
      case T_FUN_MAX:
      case T_FUN_MIN: {
        ObRawExpr *child_expr = NULL;
        if (OB_ISNULL(child_expr = expr.get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null");
        } else if (OB_UNLIKELY(ob_is_enumset_tc(child_expr->get_data_type()))) {
          // To compatible with MySQL, we need to add cast expression that enumset to varchar
          // to evalute MIN/MAX aggregate functions.
          need_add_cast = true;
          const ObExprResType& res_type = child_expr->get_result_type();
          result_type.set_varchar();
          result_type.set_length(res_type.get_length());
          result_type.set_collation_type(res_type.get_collation_type());
          result_type.set_collation_level(CS_LEVEL_IMPLICIT);
          expr.set_result_type(result_type);
        } else {
          expr.set_result_type(child_expr->get_result_type());
          expr.unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
        }
        break;
      }
      default: {
        expr.set_result_type(expr.get_param_expr(0)->get_result_type());
        expr.unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
      }
    }

    if (OB_SUCC(ret) && my_session_->use_static_typing_engine() && need_add_cast) {
      result_type.set_calc_type(result_type.get_type());
      result_type.set_calc_accuracy(result_type.get_accuracy());
      if (T_FUN_AVG == expr.get_expr_type() && -2 != scale_increment_recover) {
        result_type.set_calc_scale(scale_increment_recover);
      }
      expr.set_result_type(result_type);
      ObCastMode def_cast_mode = CM_NONE;
      if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_, def_cast_mode))) {
        LOG_WARN("get_default_cast_mode failed", K(ret));
      } else if (OB_FAIL(add_implicit_cast(expr, def_cast_mode))) {
        LOG_WARN("add_implicit_cast failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::add_group_aggr_implicit_cast(ObAggFunRawExpr& expr, const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.get_real_param_count() != expr.get_order_items().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(expr.get_real_param_count()), K(ret), K(expr.get_order_items().count()));
  } else {
    ObIArray<ObRawExpr*>& real_param_exprs = expr.get_real_param_exprs_for_update();
    for (int64_t i = 0; OB_SUCC(ret) && i < real_param_exprs.count(); ++i) {
      ObRawExpr* parent = expr.get_order_items().at(i).expr_;
      ObRawExpr*& child_ptr = real_param_exprs.at(i);
      if (OB_ISNULL(parent) || OB_ISNULL(child_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(parent), K(child_ptr));
      } else {
        ObExprResType res_type = parent->get_result_type();
        res_type.set_calc_meta(res_type.get_obj_meta());
        res_type.set_calc_accuracy(res_type.get_accuracy());
        if (skip_cast_expr(*parent, i)) {
          // do nothing
        } else if (OB_FAIL(try_add_cast_expr(expr, i, res_type, cast_mode))) {
          LOG_WARN("try_add_cast_expr failed", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::add_median_percentile_implicit_cast(
    ObAggFunRawExpr& expr, const ObCastMode& cast_mode, const bool keep_type)
{
  int ret = OB_SUCCESS;
  UNUSED(keep_type);
  if (OB_UNLIKELY(1 != expr.get_real_param_count() || 1 != expr.get_order_items().count())) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid number of arguments", K(ret), K(expr.get_real_param_count()), K(expr.get_order_items().count()));
  } else {
    ObExprResType res_type = expr.get_result_type();
    res_type.set_calc_meta(res_type.get_obj_meta());
    res_type.set_calc_accuracy(res_type.get_accuracy());
    ObExprResType res_number_type;
    res_number_type.set_number();
    res_number_type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_scale());
    res_number_type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_precision());
    res_number_type.set_calc_meta(res_number_type.get_obj_meta());
    res_number_type.set_calc_accuracy(res_number_type.get_accuracy());
    const int64_t cast_order_idx = expr.get_real_param_count();  // order item expr pos
    const int64_t cast_param_idx = 0;
    const ObObjType to_type = res_type.get_obj_meta().get_type();
    if (!keep_type && OB_FAIL(try_add_cast_expr(expr, cast_order_idx, res_type, cast_mode))) {
      LOG_WARN("try_add_cast_expr failed", K(ret), K(expr), K(cast_order_idx), K(res_type));
    } else if (T_FUN_MEDIAN != expr.get_expr_type()) {  // percentile param
      if (OB_FAIL(try_add_cast_expr(expr, cast_param_idx, res_number_type, cast_mode))) {
        LOG_WARN("try_add_cast_expr failed", K(ret), K(expr), K(cast_param_idx), K(res_number_type));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::check_median_percentile_param(ObAggFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  const ObItemType expr_type = expr.get_expr_type();
  const int64_t real_param_count = expr.get_real_param_count();
  const int64_t order_count = expr.get_order_items().count();
  if (OB_UNLIKELY(1 != order_count || 1 != real_param_count)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid number of arguments", K(ret), K(real_param_count), K(order_count));
  } else if (OB_ISNULL(expr.get_param_expr(0)) || OB_ISNULL(expr.get_order_items().at(0).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (T_FUN_GROUP_PERCENTILE_CONT == expr_type || T_FUN_GROUP_PERCENTILE_DISC == expr_type) {
    if (!expr.get_param_expr(0)->has_const_or_const_expr_flag()) {
      ret = OB_ERR_ARGUMENT_SHOULD_CONSTANT;
      LOG_WARN("Argument should be a constant.", K(ret));
    } else if (!ob_is_numeric_type(expr.get_param_expr(0)->get_result_type().get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(expr), K(expr));
    }
  }
  return ret;
}

/**
 * Check the aggregation of rank, dense_rank, percent_rank, cume_dist, etc.
 * whether the function parameters are valid
 *  1.the aggr parameter needs to have a one-to-one correspondence with order by item, eg:
 *    select rank(1,2) within group(order by c1, c2) from t1; ==> (v)
 *    select rank(1,2) within group(order by c1) from t1; ==> (x)
 *    select rank(2) within group(order by c1,c2) from t1; ==> (x)
 *  2.the aggr parameter is a constant expression, eg:
 *    select rank(c1) within group(order by c1,c2) from t1; ==> (x)
 */
int ObRawExprDeduceType::check_group_aggr_param(ObAggFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (expr.get_real_param_count() != expr.get_order_items().count()) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid number of arguments", K(ret), K(expr.get_real_param_count()), K(expr.get_order_items().count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_real_param_count(); ++i) {
      const ObRawExpr* param_expr = expr.get_param_expr(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(param_expr));
      } else if (!param_expr->has_const_or_const_expr_flag()) {
        ret = OB_ERR_ARGUMENT_SHOULD_CONSTANT;
        LOG_WARN("Argument should be a constant.", K(ret));
      } else {
        /*do nothing*/
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::deduce_type_visit_for_special_func(
    int64_t param_index, const ObRawExpr& expr, ObIExprResTypes& types)
{
  int ret = OB_SUCCESS;
  ObExprResType dest_type(alloc_);
  const int CONV_PARAM_NUM = 6;
  if (OB_UNLIKELY(param_index < 0) || OB_UNLIKELY(param_index >= CONV_PARAM_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(param_index));
  } else if (OB_UNLIKELY(CONV_PARAM_NUM - 2 == param_index) || OB_UNLIKELY(CONV_PARAM_NUM - 1 == param_index)) {
    dest_type = expr.get_result_type();
    // ignore the last param of column_conv
  } else if (OB_UNLIKELY(!expr.is_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column conv function other params are const expr", K(expr), K(param_index));
  } else {
    const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(&expr);
    switch (param_index) {
      case 0: {
        int32_t type_value = -1;
        if (OB_FAIL(const_expr->get_value().get_int32(type_value))) {
          LOG_WARN("get int32 value failed", K(*const_expr));
        } else {
          dest_type.set_type(static_cast<ObObjType>(type_value));
        }
        break;
      }
      case 1: {
        int32_t collation_value = -1;
        if (OB_FAIL(const_expr->get_value().get_int32(collation_value))) {
          LOG_WARN("get int32 value failed", K(*const_expr));
        } else {
          dest_type.set_collation_type(static_cast<ObCollationType>(collation_value));
        }
        break;
      }
      case 2: {
        int64_t accuracy_value = -1;
        ObAccuracy accuracy;
        if (OB_FAIL(const_expr->get_value().get_int(accuracy_value))) {
          LOG_WARN("get int value failed", K(ret));
        } else {
          accuracy.set_accuracy(accuracy_value);
          dest_type.set_accuracy(accuracy);
        }
        break;
      }
      case 3: {
        bool is_nullable = false;
        if (OB_FAIL(const_expr->get_value().get_bool(is_nullable))) {
          LOG_WARN("get bool from value failed", K(ret), KPC(const_expr));
        } else if (!is_nullable) {
          dest_type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
        }
        break;
      }
      default: {
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(types.push_back(dest_type))) {
      LOG_WARN("fail to to push back dest type", K(ret));
    }
  }
  return ret;
}

static ObObjType INT_OPPOSITE_SIGNED_INT_TYPE[] = {
    ObNullType,
    ObUTinyIntType,
    ObUSmallIntType,
    ObUMediumIntType,
    ObUInt32Type,
    ObUInt64Type,
    ObTinyIntType,
    ObSmallIntType,
    ObMediumIntType,
    ObInt32Type,
    ObIntType,
};

int ObRawExprDeduceType::adjust_cast_as_signed_unsigned(ObSysFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* param_expr1 = NULL;
  ObRawExpr* param_expr2 = NULL;
  if (OB_UNLIKELY(T_FUN_SYS_CAST != expr.get_expr_type()) || OB_UNLIKELY(2 != expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cast expr", K(ret));
  } else if (expr.has_flag(IS_INNER_ADDED_EXPR)) {
    /*do nothing*/
  } else if (OB_ISNULL(param_expr1 = expr.get_param_expr(0)) || OB_ISNULL(param_expr2 = expr.get_param_expr(1)) ||
             OB_UNLIKELY(!param_expr2->is_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret));
  } else {
    ObObjType src_type = param_expr1->get_result_type().get_type();
    ObObjTypeClass src_tc = ob_obj_type_class(src_type);
    ParseNode node;
    node.value_ = param_expr2->get_result_type().get_param().get_int();
    const ObObjType obj_type = static_cast<ObObjType>(node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
    ObObjType dst_type = ObMaxType;
    if (ObIntTC == src_tc && ObUInt64Type == obj_type) {
      dst_type = INT_OPPOSITE_SIGNED_INT_TYPE[src_type];
    } else if (ObIntTC == src_tc && ObIntType == obj_type) {
      dst_type = src_type;
    } else if (ObUIntTC == src_tc && ObUInt64Type == obj_type) {
      dst_type = src_type;
    } else if (ObUIntTC == src_tc && ObIntType == obj_type) {
      dst_type = INT_OPPOSITE_SIGNED_INT_TYPE[src_type];
    }
    if (ObMaxType != dst_type && obj_type != dst_type) {
      ObObj val;
      node.int16_values_[OB_NODE_CAST_TYPE_IDX] = static_cast<int16_t>(dst_type);
      val.set_int(node.value_);
      static_cast<ObConstRawExpr*>(param_expr2)->set_value(val);
      param_expr2->set_param(val);
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObSysFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObExprOperator* op = expr.get_op();
  if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (NULL == op) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Get expression operator failed", "expr type", expr.get_expr_type());
  } else if (T_FUN_SYS_CAST == expr.get_expr_type() && OB_FAIL(adjust_cast_as_signed_unsigned(expr))) {
    LOG_WARN("failed to adjust cast as signed unsigned", K(ret), K(expr));
  } else {
    ObExprResTypes types;
    ObCastMode expr_cast_mode = CM_NONE;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); i++) {
      const ObRawExpr* param_expr = expr.get_param_expr(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(param_expr));
      } else if (T_FUN_SYS_CALC_PARTITION_ID != expr.get_expr_type() && get_expr_output_column(*param_expr) != 1) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      } else if (T_FUN_COLUMN_CONV == expr.get_expr_type() || T_FUN_SYS_DEFAULT == expr.get_expr_type()) {
        // column_conv(type, collation_type, accuracy_expr, nullable, value)
        if (OB_FAIL(deduce_type_visit_for_special_func(i, *param_expr, types))) {
          LOG_WARN("fail to visit for column_conv", K(ret), K(i));
        }
      } else {
        ObExprResType res_type = param_expr->get_result_type();
        if (param_expr->is_bool_expr()) {
          res_type.set_result_flag(IS_BOOL_FLAG);
        }
        if (OB_FAIL(types.push_back(res_type))) {
          LOG_WARN("push back param type failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(calc_result_type(expr, types, expr_cast_mode, ObExprOperator::NOT_ROW_DIMENSION))) {
        LOG_WARN("fail to calc result type", K(ret));
      }
    }

    if (OB_SUCC(ret) && ob_is_enumset_tc(expr.get_data_type()) &&
        (T_FUN_SYS_NULLIF == expr.get_expr_type() || T_FUN_SYS_VALUES == expr.get_expr_type())) {
      ObRawExpr* first_param = NULL;
      if (OB_ISNULL(first_param = expr.get_param_expr(0)) || !(ob_is_enumset_tc(first_param->get_data_type()))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr", KPC(first_param), K(expr), K(ret));
      } else {
        const ObIArray<ObString>& enum_set_values = first_param->get_enum_set_values();
        if (OB_UNLIKELY(enum_set_values.count() < 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ref_expr", K(first_param), K(ret));
        } else if (OB_FAIL(expr.set_enum_set_values(enum_set_values))) {
          LOG_WARN("failed to set enum_set_values", K(enum_set_values), K(expr), K(ret));
        } else { /*do nothing*/
        }
      }
    }
    CK(OB_NOT_NULL(my_session_));
    if (OB_SUCC(ret) && my_session_->use_static_typing_engine()) {
      if (OB_FAIL(add_implicit_cast(expr, expr_cast_mode))) {
        LOG_WARN("add_implicit_cast failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObSetOpRawExpr& expr)
{
  UNUSED(expr);
  int ret = OB_SUCCESS;
  return ret;
}

int ObRawExprDeduceType::visit(ObAliasRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* ref_expr = expr.get_ref_expr();
  if (OB_ISNULL(ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref expr is null");
  } else {
    expr.set_result_type(ref_expr->get_result_type());
    if (ob_is_enum_or_set_type(ref_expr->get_data_type())) {
      const ObIArray<ObString>& enum_set_values = ref_expr->get_enum_set_values();
      if (OB_UNLIKELY(enum_set_values.count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ref_expr", KPC(ref_expr), K(ret));
      } else if (OB_FAIL(expr.set_enum_set_values(enum_set_values))) {
        LOG_WARN("failed to set enum_set_values", K(expr), K(ret));
      } else {
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::get_row_expr_param_type(const ObRawExpr& expr, ObIExprResTypes& types)
{
  int ret = OB_SUCCESS;
  ObExprResType result_type(alloc_);
  if (OB_UNLIKELY(expr.get_expr_type() != T_OP_ROW)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is not row", K(expr));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < expr.get_param_count(); ++j) {
    const ObRawExpr* row_param = expr.get_param_expr(j);
    if (OB_ISNULL(row_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row param is null");
    } else if (T_OP_ROW == row_param->get_expr_type()) {
      if (OB_FAIL(get_row_expr_param_type(*row_param, types))) {
        LOG_WARN("get row expr param type failed", K(ret));
      }
    } else if (OB_FAIL(types.push_back(row_param->get_result_type()))) {
      LOG_WARN("push back param type failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObFunMatchAgainst& expr)
{
  ObExprResType result_type(alloc_);
  result_type.set_double();
  expr.set_result_type(result_type);
  return OB_SUCCESS;
}

int ObRawExprDeduceType::visit(ObWinFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObExprResType result_number_type;
  result_number_type.set_accuracy(ObAccuracy::MAX_ACCURACY2[share::is_oracle_mode()][ObNumberType]);
  result_number_type.set_calc_accuracy(ObAccuracy::MAX_ACCURACY2[share::is_oracle_mode()][ObNumberType]);
  result_number_type.set_number();

  common::ObIArray<ObRawExpr*>& func_params = expr.get_func_params();
  if (func_params.count() <= 0) {
    if (NULL == expr.get_agg_expr()) {
      ObExprResType result_type(alloc_);
      if (T_WIN_FUN_CUME_DIST == expr.get_func_type() || T_WIN_FUN_PERCENT_RANK == expr.get_func_type()) {
        result_type.set_accuracy(ObAccuracy::MAX_ACCURACY2[is_oracle_mode()][ObNumberType]);
        result_type.set_calc_accuracy(ObAccuracy::MAX_ACCURACY2[is_oracle_mode()][ObNumberType]);
        result_type.set_number();
      } else if (is_oracle_mode()) {
        result_type.set_number();
        result_type.set_scale(0);
        result_type.set_precision(OB_MAX_NUMBER_PRECISION);
      } else {
        result_type.set_int();
        result_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
      }
      expr.set_result_type(result_type);
    } else {
      // The agg function func_params is also empty, at this time it needs to be set to the result_type of agg
      expr.set_result_type(expr.get_agg_expr()->get_result_type());
    }
  } else if (OB_ISNULL(func_params.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func param is null", K(ret));
  } else if (T_WIN_FUN_NTILE == expr.get_func_type()) {
    ObExprResType result_type(alloc_);
    result_type.set_int();
    result_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
    expr.set_result_type(result_type);
    if (my_session_->use_static_typing_engine() && is_oracle_mode() &&
        !func_params.at(0)->get_result_type().is_numeric_type()) {
      ObSysFunRawExpr* cast_expr = NULL;
      if (OB_ISNULL(expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null pointer", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                     *expr_factory_, func_params.at(0), result_number_type, cast_expr, my_session_))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else {
        func_params.at(0) = cast_expr;
      }
    }
  } else if (T_WIN_FUN_NTH_VALUE == expr.get_func_type()) {
    expr.set_result_type(func_params.at(0)->get_result_type());
    expr.unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
    if (my_session_->use_static_typing_engine() && !func_params.at(1)->get_result_type().is_numeric_type()) {
      ObSysFunRawExpr* cast_expr = NULL;
      if (OB_ISNULL(expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null pointer", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                     *expr_factory_, func_params.at(1), result_number_type, cast_expr, my_session_))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else {
        func_params.at(1) = cast_expr;
      }
    }
  } else if (T_WIN_FUN_LEAD == expr.get_func_type() || T_WIN_FUN_LAG == expr.get_func_type()) {
    expr.set_result_type(func_params.at(0)->get_result_type());
    expr.unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
    if (func_params.count() == 3) {
      ObSysFunRawExpr* cast_expr = NULL;
      if (OB_ISNULL(expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null pointer", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*expr_factory_,
                     func_params.at(2),
                     func_params.at(0)->get_result_type(),
                     cast_expr,
                     my_session_))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else {
        func_params.at(2) = cast_expr;
      }
    }
    if (func_params.count() >= 2 && my_session_->use_static_typing_engine() &&
        !func_params.at(1)->get_result_type().is_numeric_type()) {
      ObSysFunRawExpr* cast_expr = NULL;
      if (OB_ISNULL(expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null pointer", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                     *expr_factory_, func_params.at(1), result_number_type, cast_expr, my_session_))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else {
        func_params.at(1) = cast_expr;
      }
    }
  } else {
    expr.set_result_type(func_params.at(0)->get_result_type());
  }

  if (OB_SUCC(ret) && my_session_->use_static_typing_engine()) {
    if (OB_SUCC(ret) && expr.lower_.is_nmb_literal_ && expr.lower_.interval_expr_ != NULL &&
        !expr.lower_.interval_expr_->get_result_type().is_numeric_type()) {
      if (is_oracle_mode()) {
        ObSysFunRawExpr* cast_expr = NULL;
        if (OB_ISNULL(expr_factory_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null pointer", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                       *expr_factory_, expr.lower_.interval_expr_, result_number_type, cast_expr, my_session_))) {
          LOG_WARN("failed to create raw expr.", K(ret));
        } else {
          expr.lower_.interval_expr_ = cast_expr;
        }
      } else {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("interval is not numberic", K(ret), KPC(expr.lower_.interval_expr_));
      }
    }
    if (OB_SUCC(ret) && expr.upper_.is_nmb_literal_ && expr.upper_.interval_expr_ != NULL &&
        !expr.upper_.interval_expr_->get_result_type().is_numeric_type()) {
      if (is_oracle_mode()) {
        ObSysFunRawExpr* cast_expr = NULL;
        if (OB_ISNULL(expr_factory_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null pointer", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                       *expr_factory_, expr.upper_.interval_expr_, result_number_type, cast_expr, my_session_))) {
          LOG_WARN("failed to create raw expr.", K(ret));
        } else {
          expr.upper_.interval_expr_ = cast_expr;
        }
      } else {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("interval is not numberic", K(ret), KPC(expr.lower_.interval_expr_));
      }
    }
    LOG_DEBUG("finish add cast for window function", K(result_number_type), K(expr.lower_), K(expr.upper_));
  }

  if (OB_FAIL(ret) || OB_UNLIKELY(expr.win_type_ != WINDOW_RANGE) ||
      OB_UNLIKELY(BOUND_INTERVAL != expr.upper_.type_ && BOUND_INTERVAL != expr.lower_.type_)) {
    /*do nothing*/
  } else if (OB_UNLIKELY(1 != expr.get_order_items().count())) {
    ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
    LOG_WARN("invalid window specification", K(ret), K(expr.get_order_items().count()));
  } else if (OB_ISNULL(expr.get_order_items().at(0).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("order by expr should not be null!", K(ret));
  } else {
    ObRawExpr* bound_expr_arr[2] = {expr.upper_.interval_expr_, expr.lower_.interval_expr_};
    ObRawExpr* order_expr = expr.get_order_items().at(0).expr_;
    const ObObjType& order_res_type = order_expr->get_data_type();
    for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
      if (OB_ISNULL(bound_expr_arr[i])) {
        /*do nothing*/
      } else if (share::is_oracle_mode()) {
        if (ob_is_numeric_type(bound_expr_arr[i]->get_data_type()) ||
            ob_is_string_tc(bound_expr_arr[i]->get_data_type())) {
          if (!ob_is_numeric_type(order_res_type) && !ob_is_datetime_tc(order_res_type)) {
            ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
            LOG_WARN("invalid datatype in order by for range clause", K(ret), K(order_res_type));
          }
        } else {
          ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
          LOG_WARN(
              "invalid datatype in order by", K(i), K(bound_expr_arr[i]->get_data_type()), K(ret), K(order_res_type));
        }
      } else {  // mysql mode
        if (ob_is_numeric_type(order_res_type) || ob_is_temporal_type(order_res_type) ||
            ob_is_otimestampe_tc(order_res_type) || ob_is_datetime_tc(order_res_type)) {
          /*do nothing*/
        } else {
          ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
          LOG_WARN("invalid datatype in order by for range clause", K(ret), K(order_res_type));
        }
      }
    }
    if (OB_SUCC(ret) && my_session_->use_static_typing_engine()) {
      bool is_asc = expr.get_order_items().at(0).is_ascending();
      ObRawExpr*& upper_raw_expr = (expr.upper_.is_preceding_ ^ is_asc) ? expr.upper_.exprs_[0] : expr.upper_.exprs_[1];
      ObRawExpr*& lower_raw_expr = (expr.lower_.is_preceding_ ^ is_asc) ? expr.lower_.exprs_[0] : expr.lower_.exprs_[1];
      bool need_no_cast = false;
      ObExprResType result_type;
      ObSEArray<ObExprResType, 3> types;
      ObExprBetween dummy_op(expr_factory_->get_allocator());
      if (OB_FAIL(types.push_back(order_expr->get_result_type()))) {
        LOG_WARN("fail to push_back", K(ret));
      } else if (OB_NOT_NULL(upper_raw_expr) && OB_FAIL(types.push_back(upper_raw_expr->get_result_type()))) {
        LOG_WARN("fail to push_back", K(ret));
      } else if (OB_NOT_NULL(lower_raw_expr) && OB_FAIL(types.push_back(lower_raw_expr->get_result_type()))) {
        LOG_WARN("fail to push_back", K(ret));
      } else if (OB_FAIL(dummy_op.get_cmp_result_type3(
                     result_type, need_no_cast, &types.at(0), types.count(), *my_session_))) {
        LOG_WARN("fail to get_cmp_result_type3", K(ret));
      } else {
        result_type.set_accuracy(result_type.get_calc_accuracy());
        result_type.set_meta(result_type.get_calc_meta());
      }
      ObRawExpr* cast_expr_upper = NULL;
      ObRawExpr* cast_expr_lower = NULL;
      ObRawExpr* cast_expr_order = NULL;
      ObCastMode def_cast_mode = CM_NONE;
      if (OB_FAIL(ret) || need_no_cast) {
        /*do nothing*/
      } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_, def_cast_mode))) {
        LOG_WARN("get_default_cast_mode failed", K(ret));
      } else if (OB_NOT_NULL(upper_raw_expr) && OB_FAIL(try_add_cast_expr_above_for_deduce_type(
                                                    *upper_raw_expr, cast_expr_upper, result_type, def_cast_mode))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else if (OB_NOT_NULL(lower_raw_expr) && OB_FAIL(try_add_cast_expr_above_for_deduce_type(
                                                    *lower_raw_expr, cast_expr_lower, result_type, def_cast_mode))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else if (OB_FAIL(try_add_cast_expr_above_for_deduce_type(
                     *order_expr, cast_expr_order, result_type, def_cast_mode))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else {
        upper_raw_expr = cast_expr_upper;
        lower_raw_expr = cast_expr_lower;
        expr.get_order_items().at(0).expr_ = cast_expr_order;
      }
      LOG_DEBUG("finish add cast for window function", K(need_no_cast), K(result_type), K(types), K(expr));
    }
  }
  if (OB_SUCC(ret) && my_session_->use_static_typing_engine() && share::is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_partition_exprs().count(); i++) {
      ObRawExpr* param_expr = expr.get_partition_exprs().at(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param is null", K(ret));
      } else if (ob_is_lob_locator(param_expr->get_result_type().get_type())) {
        ObCastMode def_cast_mode = CM_NONE;
        ObExprResType param_type = param_expr->get_result_type();
        param_type.set_calc_meta(param_type.get_obj_meta());
        param_type.set_calc_accuracy(param_type.get_accuracy());
        param_type.set_calc_type(ObLongTextType);
        int64_t cast_param_idx = OB_ISNULL(expr.get_agg_expr())
                                     ? 0
                                     : expr.get_agg_expr()->get_param_count() + expr.get_func_params().count() + i;
        if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_, def_cast_mode))) {
          LOG_WARN("get_default_cast_mode failed", K(ret));
        } else if (OB_FAIL(try_add_cast_expr(expr, cast_param_idx, param_type, def_cast_mode))) {
          LOG_WARN("try_add_cast_expr failed", K(ret), K(expr), K(cast_param_idx), K(param_type));
        }
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObPseudoColumnRawExpr& expr)
{
  // result type has been set in resolver
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprDeduceType::init_normal_udf_expr(ObNonTerminalRawExpr& expr, ObExprOperator* op)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(op);
  ObExprDllUdf* normal_udf_op = nullptr;
  ObNormalDllUdfRawExpr& fun_sys = static_cast<ObNormalDllUdfRawExpr&>(expr);
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(expr.get_expr_type()));
  } else {
    normal_udf_op = static_cast<ObExprDllUdf*>(op);
    /* set udf meta, load so func */
    if (OB_FAIL(normal_udf_op->set_udf_meta(fun_sys.get_udf_meta()))) {
      LOG_WARN("failed to set udf to expr", K(ret));
    } else if (OB_FAIL(normal_udf_op->init_udf(fun_sys.get_param_exprs()))) {
      LOG_WARN("failed to init udf", K(ret));
    } else {
    }
  }
  return ret;
}

int ObRawExprDeduceType::set_agg_udf_result_type(ObAggFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr*>& param_exprs = expr.get_real_param_exprs_for_update();
  common::ObSEArray<common::ObString, 16> udf_attributes;    /* udf's input args' name */
  common::ObSEArray<ObExprResType, 16> udf_attributes_types; /* udf's aatribute type */
  common::ObSEArray<ObUdfConstArgs, 16> const_results;       /* const input expr' result */
  ObAggUdfFunction udf_func;
  const share::schema::ObUDFMeta& udf_meta = expr.get_udf_meta();
  ObExprResType type;
  ARRAY_FOREACH_X(param_exprs, idx, cnt, OB_SUCC(ret))
  {
    ObRawExpr* expr = param_exprs.at(idx);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the expr is null", K(ret));
    } else if (expr->is_column_ref_expr()) {
      // if the input expr is a column, we should set the column name as the expr name.
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
      const ObString& real_expr_name =
          col_expr->get_alias_column_name().empty() ? col_expr->get_column_name() : col_expr->get_alias_column_name();
      expr->set_expr_name(real_expr_name);
    } else if (expr->has_flag(IS_CALCULABLE_EXPR) || expr->has_flag(IS_CONST_EXPR) || expr->has_flag(IS_CONST)) {
      // if the input expr is a const expr, we will set the result val to UDF_INIT's args.
      ObUdfConstArgs const_args;
      ObConstRawExpr* c_expr = static_cast<ObConstRawExpr*>(expr);
      ObObj& param_obj = c_expr->get_value();
      const_args.idx_in_udf_arg_ = idx;
      UNUSED(param_obj);
      if (OB_FAIL(const_results.push_back(const_args))) {
        LOG_WARN("failed to push back const args", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(udf_attributes.push_back(expr->get_expr_name()))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(udf_attributes_types.push_back(expr->get_result_type()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(udf_func.init(udf_meta))) {
      LOG_WARN("udf function init failed", K(ret));
    } else if (OB_FAIL(ObUdfUtil::calc_udf_result_type(
                   alloc_, &udf_func, udf_meta, udf_attributes, udf_attributes_types, type))) {
      LOG_WARN("failed to cale udf result type");
    } else {
      expr.set_result_type(type);
    }
  }
  return ret;
}

bool ObRawExprDeduceType::skip_cast_expr(const ObRawExpr& parent, const int64_t child_idx)
{
  ObItemType parent_expr_type = parent.get_expr_type();
  bool bret = false;
  if ((T_FUN_COLUMN_CONV == parent_expr_type && child_idx < 4) ||
      (T_FUN_SYS_DEFAULT == parent_expr_type && child_idx < 4) || T_FUN_SET_TO_STR == parent_expr_type ||
      T_FUN_ENUM_TO_STR == parent_expr_type || T_FUN_SET_TO_INNER_TYPE == parent_expr_type ||
      T_FUN_ENUM_TO_INNER_TYPE == parent_expr_type || T_OP_EXISTS == parent_expr_type ||
      T_OP_NOT_EXISTS == parent_expr_type ||
      (T_FUN_SYS_CAST == parent_expr_type && !CM_IS_EXPLICIT_CAST(parent.get_extra()))) {
    bret = true;
  }
  return bret;
}

// this function will add implicit cast to the case expression as needed
// for case when x1 then y1 when x2 then y2 else y3
// the order of input_types is: x1 x2 y1 y2 y3
// the order required by ObCaseOpRawExpr::get_param_expr() is: x1 y1 x2 y2 y3
// so the input_type needs to be reordered
int ObRawExprDeduceType::add_implicit_cast(ObCaseOpRawExpr& parent, const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_OP_CASE != parent.get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("all T_OP_ARG_CASE should be resolved as T_OP_CASE", K(ret), K(parent.get_expr_type()));
  } else {
    int64_t when_size = parent.get_when_expr_size();
    const ObExprResTypes& input_types = parent.get_input_types();
    ObArenaAllocator allocator;
    ObFixedArray<ObExprResType, ObIAllocator> input_types_reorder(&allocator, input_types.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < when_size; ++i) {
      if (OB_FAIL(input_types_reorder.push_back(input_types.at(i)))) {
        LOG_WARN("push back res type failed", K(ret), K(i));
      } else if (OB_FAIL(input_types_reorder.push_back(input_types.at(i + when_size)))) {
        LOG_WARN("push back res type failed", K(ret), K(i + when_size));
      }
    }

    if (OB_SUCC(ret)) {
      if (input_types_reorder.count() + 1 == input_types.count()) {
        if (OB_FAIL(input_types_reorder.push_back(input_types.at(input_types.count() - 1)))) {
          LOG_WARN("push back res type failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(input_types_reorder.count() != input_types.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected input type array", K(ret), K(input_types), K(input_types_reorder));
      }
    }
    LOG_DEBUG("input types reorder done", K(ret), K(input_types_reorder), K(input_types));
    ObRawExpr* child_ptr = NULL;
    for (int64_t child_idx = 0; OB_SUCC(ret) && (child_idx < parent.get_param_count()); ++child_idx) {
      if (OB_ISNULL(child_ptr = parent.get_param_expr(child_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child_ptr raw expr is NULL", K(ret));
      } else {
        if (skip_cast_expr(parent, child_idx)) {
          // do nothing
        } else if (OB_FAIL(try_add_cast_expr(parent, child_idx, input_types_reorder.at(child_idx), cast_mode))) {
          LOG_WARN("try_add_cast_expr failed", K(ret), K(child_idx));
        }
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::add_implicit_cast(ObOpRawExpr& parent, const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  ObRawExpr* child_ptr = NULL;
  const ObIArray<ObExprResType>& input_types = parent.get_input_types();
  typedef ObArrayHelper<ObExprResType> ObExprTypeArrayHelper;
  // idx is the index of input_types
  // child_idx is the index of parent.get_param_count()
  // (T_OP_ROW, input_types.count() != parent.get_param_count())
  int64_t idx = 0;
  if (T_FUN_SYS_CALC_PARTITION_ID != parent.get_expr_type()) {
    for (int64_t child_idx = 0; OB_SUCC(ret) && (child_idx < parent.get_param_count()); ++child_idx) {
      if (OB_ISNULL(child_ptr = parent.get_param_expr(child_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child_ptr raw expr is NULL", K(ret));
      } else {
        if (skip_cast_expr(parent, child_idx)) {
          idx += 1;
          // do nothing
        } else if (T_OP_ROW == child_ptr->get_expr_type()) {
          int64_t ele_cnt = child_ptr->get_param_count();
          CK(OB_NOT_NULL(child_ptr->get_param_expr(0)));
          if (OB_SUCC(ret)) {
            if (T_OP_ROW == child_ptr->get_param_expr(0)->get_expr_type()) {
              // (1, 2) in ((2, 2), (1, 2)), the right branch is a vector of vectors
              ele_cnt = ele_cnt * child_ptr->get_param_expr(0)->get_param_count();
            }
          }
          CK(idx + ele_cnt <= input_types.count());
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(add_implicit_cast_for_op_row(parent,
                         child_ptr,
                         ObExprTypeArrayHelper(ele_cnt, const_cast<ObExprResType*>(&input_types.at(idx)), ele_cnt),
                         cast_mode))) {
            LOG_WARN("add_implicit_cast_for_op_row failed", K(ret));
          }
          idx += ele_cnt;
        } else if (T_REF_QUERY == child_ptr->get_expr_type() &&
                   (static_cast<ObQueryRefRawExpr*>(child_ptr)->get_output_column() > 1 ||
                       static_cast<ObQueryRefRawExpr*>(child_ptr)->is_set())) {
          // subquery result not scalar (is row or set), add cast on subquery stmt's output
          ObQueryRefRawExpr* query_ref_expr = static_cast<ObQueryRefRawExpr*>(child_ptr);
          const int64_t column_cnt = query_ref_expr->get_output_column();
          CK(idx + column_cnt <= input_types.count());
          OZ(add_implicit_cast_for_subquery(*query_ref_expr,
              ObExprTypeArrayHelper(column_cnt, const_cast<ObExprResType*>(&input_types.at(idx)), column_cnt),
              cast_mode));
          idx += column_cnt;
        } else {
          // general case
          if (input_types.count() <= idx) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("count of input_types must be greater than child_idx",
                K(ret),
                K(child_idx),
                K(idx),
                K(input_types.count()));
          } else if (OB_FAIL(try_add_cast_expr(parent, child_idx, input_types.at(idx), cast_mode))) {
            LOG_WARN("try_add_cast_expr failed", K(ret), K(child_idx), K(idx));
          }
          idx += 1;
        }
      }
      LOG_DEBUG("add_implicit_cast debug", K(parent));
    }  // for end
  }
  return ret;
}

int ObRawExprDeduceType::add_implicit_cast(ObAggFunRawExpr& parent, const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  ObExprResType res_type = parent.get_result_type();
  res_type.set_calc_meta(res_type.get_obj_meta());
  ObIArray<ObRawExpr*>& real_param_exprs = parent.get_real_param_exprs_for_update();
  for (int64_t i = 0; OB_SUCC(ret) && i < real_param_exprs.count(); ++i) {
    ObRawExpr*& child_ptr = real_param_exprs.at(i);
    if (skip_cast_expr(parent, i)) {
      // do nothing
    } else if ((parent.get_expr_type() == T_FUN_REGR_SXX && i == 0) ||
               (parent.get_expr_type() == T_FUN_REGR_SYY && i == 1) ||
               (parent.get_expr_type() == T_FUN_REGR_SXY && i == 1) ||
               (parent.get_expr_type() == T_FUN_JSON_OBJECTAGG && i == 1)) {
      // do nothing
    } else if (parent.get_expr_type() == T_FUN_WM_CONCAT ||
               parent.get_expr_type() == T_FUN_KEEP_WM_CONCAT ||
              (parent.get_expr_type() == T_FUN_JSON_OBJECTAGG && i == 0)) {
      if (ob_is_string_type(child_ptr->get_result_type().get_type())) {
        /*do nothing*/
      } else {
        ObExprResType result_type(alloc_);
        result_type.set_varchar();
        result_type.set_length(OB_MAX_SQL_LENGTH);
        result_type.set_collation_type(res_type.get_calc_collation_type());
        result_type.set_collation_level(res_type.get_collation_level());
        result_type.set_calc_meta(result_type.get_obj_meta());
        if (OB_FAIL(try_add_cast_expr(parent, i, result_type, cast_mode))) {
          LOG_WARN("try_add_cast_expr failed", K(ret));
        } else {
          LOG_DEBUG("add_implicit_cast for ObAggFunRawExpr", K(i), K(res_type), KPC(child_ptr));
        }
      }
    } else if (OB_FAIL(try_add_cast_expr(parent, i, res_type, cast_mode))) {
      LOG_WARN("try_add_cast_expr failed", K(ret));
    } else {
      LOG_DEBUG("add_implicit_cast for ObAggFunRawExpr", K(i), K(res_type), KPC(child_ptr));
    }
  }
  return ret;
}

template <typename RawExprType>
int ObRawExprDeduceType::try_add_cast_expr(
    RawExprType& parent, int64_t child_idx, const ObExprResType& input_type, const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  ObRawExpr* child_ptr = NULL;
  if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_session_ is NULL", K(ret));
  } else if (OB_UNLIKELY(parent.get_param_count() <= child_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("child_idx is invalid", K(ret), K(parent.get_param_count()), K(child_idx));
  } else if (OB_ISNULL(child_ptr = parent.get_param_expr(child_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child_ptr raw expr is NULL", K(ret));
  } else {
    ObRawExpr* new_expr = NULL;
    OZ(try_add_cast_expr_above_for_deduce_type(*child_ptr, new_expr, input_type, cast_mode));
    CK(NULL != new_expr);
    if (OB_SUCC(ret) && child_ptr != new_expr) {  // cast expr added
      OZ(parent.replace_param_expr(child_idx, new_expr));
    }
  }
  return ret;
};

int ObRawExprDeduceType::try_add_cast_expr_above_for_deduce_type(
    ObRawExpr& expr, ObRawExpr*& new_expr, const ObExprResType& dst_type, const ObCastMode& cm)
{
  int ret = OB_SUCCESS;
  ObExprResType cast_dst_type;
  // cast child_res_type to cast_dst_type
  const ObExprResType& child_res_type = expr.get_result_type();

  // calc meta of dst_type is the real destination type!!!
  cast_dst_type.set_meta(dst_type.get_calc_meta());
  cast_dst_type.set_calc_meta(ObObjMeta());
  cast_dst_type.set_result_flag(child_res_type.get_result_flag());
  cast_dst_type.set_accuracy(dst_type.get_calc_accuracy());
  if (lib::is_mysql_mode() && (dst_type.get_calc_meta().is_number() || dst_type.get_calc_meta().is_unumber()) &&
      dst_type.get_calc_scale() == -1) {
    cast_dst_type.set_accuracy(child_res_type.get_accuracy());
  } else if (lib::is_mysql_mode() && ObDateTimeTC == child_res_type.get_type_class() &&
             ObDateTimeTC == dst_type.get_calc_meta().get_type_class()) {
    cast_dst_type.set_accuracy(child_res_type.get_accuracy());
  }

  // Here only set the accuracy of some cases, the accuracy information of other cases is
  // given to the cast type derivation setting
  if (lib::is_mysql_mode() && cast_dst_type.is_string_type() && cast_dst_type.has_result_flag(OB_MYSQL_ZEROFILL_FLAG)) {
    cast_dst_type.set_length(child_res_type.get_length());
  }
  OZ(ObRawExprUtils::try_add_cast_expr_above(expr_factory_, my_session_, expr, cast_dst_type, cm, new_expr));
  if (OB_SUCC(ret)) {
    ObRawExpr* e = new_expr;
    while (NULL != e && e != &expr && T_FUN_SYS_CAST == e->get_expr_type()) {
      e->add_flag(IS_OP_OPERAND_IMPLICIT_CAST);
      e = e->get_param_expr(0);
    }
  }
  return ret;
}

int ObRawExprDeduceType::add_implicit_cast_for_op_row(ObOpRawExpr& parent, ObRawExpr* child_ptr,
    const common::ObIArray<ObExprResType>& input_types, const ObCastMode& cast_mode)
{
  UNUSED(parent);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_ptr) || OB_UNLIKELY(T_OP_ROW != child_ptr->get_expr_type()) ||
      OB_ISNULL(child_ptr->get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("child_ptr is NULL", K(ret), K(child_ptr));
  } else if (T_OP_ROW == child_ptr->get_param_expr(0)->get_expr_type()) {
    // (1, 1) in ((1, 2), (3, 4))
    // row_dimension = 2, input_types = 6
    int64_t top_row_dim = child_ptr->get_param_count();
    int64_t ele_row_dim = child_ptr->get_param_expr(0)->get_param_count();
    ObOpRawExpr* cur_parent = dynamic_cast<ObOpRawExpr*>(child_ptr);
    CK(OB_NOT_NULL(cur_parent));
    for (int64_t i = 0; OB_SUCC(ret) && i < top_row_dim; i++) {
      if (OB_ISNULL(cur_parent->get_param_expr(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null param expr", K(ret));
      } else if (OB_FAIL(add_implicit_cast_for_op_row(*cur_parent,
                     cur_parent->get_param_expr(i),
                     ObArrayHelper<ObExprResType>(
                         ele_row_dim, const_cast<ObExprResType*>(&input_types.at(i * ele_row_dim)), ele_row_dim),
                     cast_mode))) {
        LOG_WARN("failed to add implicit cast for op row", K(ret));
      }
    }
  } else {
    const int64_t row_dim = child_ptr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < row_dim; i++) {
      ObOpRawExpr* child_op_expr = dynamic_cast<ObOpRawExpr*>(child_ptr);
      if (OB_ISNULL(child_op_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null pointer", K(ret), K(child_op_expr));
      } else if (OB_FAIL(try_add_cast_expr(*child_op_expr, i, input_types.at(i), cast_mode))) {
        LOG_WARN("failed to add cast expr", K(ret), K(i));
      }
    }  // end for
  }
  return ret;
}

int ObRawExprDeduceType::add_implicit_cast_for_subquery(
    ObQueryRefRawExpr& expr, const common::ObIArray<ObExprResType>& input_types, const ObCastMode& cast_mode)
{
  // Only subquery as row or is set need to add cast inside subquery, e.g.:
  //   (select c1, c2 from t1) > (a, b) // subquery as row
  //   a = ALL (select c1 from t1) // subquery is set
  //
  // the scalar result subquery add cast above query ref expr, e.g.:
  //   (select c1 from t1) + a
  int ret = OB_SUCCESS;
  CK(expr.get_output_column() > 1 || expr.is_set());
  CK(expr.get_output_column() == input_types.count());
  CK(NULL != expr.get_ref_stmt());
  CK(expr.get_column_types().count() == expr.get_output_column() &&
      expr.get_output_column() == expr.get_ref_stmt()->get_select_item_size());
  if (OB_SUCC(ret)) {
    auto& items = expr.get_ref_stmt()->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); i++) {
      ObRawExpr* new_expr = NULL;
      SelectItem& item = items.at(i);
      CK(NULL != item.expr_);
      OZ(try_add_cast_expr_above_for_deduce_type(*item.expr_, new_expr, input_types.at(i), cast_mode));
      CK(NULL != new_expr);
      if (OB_SUCC(ret) && item.expr_ != new_expr) {  // cast expr added
        // update select item expr && column types of %expr
        item.expr_ = new_expr;
        const_cast<ObExprResType&>(expr.get_column_types().at(i)) = new_expr->get_result_type();
      }
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
