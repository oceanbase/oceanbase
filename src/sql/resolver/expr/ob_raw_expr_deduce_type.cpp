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
#include "share/object/ob_obj_cast.h"
#include "share/object/ob_obj_cast_util.h"
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
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "share/ob_lob_access_utils.h"
#include "sql/parser/ob_parser.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObRawExprDeduceType::deduce(ObRawExpr &expr)
{
  return expr.postorder_accept(*this);
}

int ObRawExprDeduceType::visit(ObConstRawExpr &expr)
{
  int ret = OB_SUCCESS;
  switch (expr.get_expr_type()) {
  case T_QUESTIONMARK: {
    // For parameterized value, the result type has been set already when
    // the expr is created. See ob_raw_expr_resolver_impl.cpp
    break;
  }
  default: {
    //for testing
    if (expr.get_expr_obj_meta()!= expr.get_value().get_meta()) {
      LOG_DEBUG("meta is not suited",
                K(expr.get_value().get_type()),
                K(expr.get_expr_obj_meta().get_type()),
                K(ret));
    }
    if (expr.is_enum_set_with_subschema()) {
      expr.reset_enum_set_meta_state();
    }
    expr.set_meta_type(expr.get_expr_obj_meta());
    //expr.set_meta_type(expr.get_value().get_meta());
    expr.set_param(expr.get_value());
    if (!(expr.get_result_type().is_null()
          || (lib::is_oracle_mode() && expr.get_value().is_null_oracle()))) {
      expr.set_result_flag(NOT_NULL_FLAG);
    }
    break;
  }
  }
  //add local vars to expr
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_subschema_for_enum_set_type(expr))) {
      LOG_WARN("fail to build subschema for enum set type", K(ret));
    } else if (solidify_session_vars_) {
      if (OB_FAIL(expr.set_local_session_vars(NULL, my_session_, local_vars_id_))) {
        LOG_WARN("fail to set session vars", K(ret), K(expr));
      }
    } else if (NULL != my_local_vars_) {
      if (OB_FAIL(expr.set_local_session_vars(my_local_vars_, NULL, local_vars_id_))) {
        LOG_WARN("fail to set local vars", K(ret), K(expr));
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObVarRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.get_ref_expr() != NULL) {
    if (OB_FAIL(expr.get_ref_expr()->postorder_accept(*this))) {
      LOG_WARN("failed to deduce ref expr", K(ret));
    } else if (expr.get_ref_expr()->get_result_type().is_null()) {
      expr.set_result_type(expr.get_ref_expr()->get_result_type());
    } else if (expr.get_ref_expr()->get_result_type().is_collection_sql_type()) {
      ObRawExpr *ref_expr = expr.get_ref_expr();
      // get array element tyoe
      ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(my_session_);
      ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
      uint32_t depth = 0;
      bool is_vec = false;
      ObDataType coll_elem_type;
      uint16_t subschema_id = ref_expr->get_result_type().get_subschema_id();
      if (OB_ISNULL(exec_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exec_ctx should not be NULL", K(ret));
      } else if (OB_FAIL(ObArrayExprUtils::get_array_element_type(exec_ctx, subschema_id, coll_elem_type, depth, is_vec))) {
        LOG_WARN("failed to get array element type", K(ret));
      } else if (depth > 1) {
        uint16_t child_subid = 0;
        if (OB_FAIL(ObArrayExprUtils::get_child_subschema_id(exec_ctx, subschema_id, child_subid))) {
          LOG_WARN("failed to get child subschema id", K(ret));
        } else {
          ObExprResType res_type;
          res_type.set_collection(child_subid);
          expr.set_result_type(res_type);
        }
      } else {
        expr.set_meta_type(coll_elem_type.get_meta_type());
        expr.set_accuracy(coll_elem_type.get_accuracy());
      }
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("unexpected ref expr result type", K(expr), K(ret), K(expr.get_ref_expr()->get_result_type().get_type()));
    }
  } else if (!(expr.get_result_type().is_null())) {
    expr.set_result_flag(NOT_NULL_FLAG);
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObOpPseudoColumnRawExpr &)
{
  // result type should be assigned
  return OB_SUCCESS;
}

int ObRawExprDeduceType::visit(ObQueryRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.is_cursor()) {
    expr.set_data_type(ObExtendType);
  } else if (expr.is_scalar()) {
    expr.set_result_type(expr.get_column_types().at(0));
    if (ob_is_enumset_tc(expr.get_data_type())) {
      const ObSelectStmt *ref_stmt = expr.get_ref_stmt();
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
      } else {/*do nothing*/}
    }
  } else {
    // for enumset query ref `is_set`, need warp enum_to_str/set_to_str expr at
    // `ObRawExprWrapEnumSet::visit_query_ref_expr`
    expr.set_data_type(ObIntType);
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObPlQueryRefRawExpr &expr)
{
  expr.set_result_type(expr.get_subquery_result_type());
  return OB_SUCCESS;
}

int ObRawExprDeduceType::visit(ObExecParamRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.is_eval_by_storage()) {
    // do nothing
  } else if (OB_ISNULL(expr.get_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref expr is null", K(ret));
  } else if (OB_FAIL(expr.get_ref_expr()->postorder_accept(*this))) {
    LOG_WARN("failed to deduce ref expr", K(ret));
  } else {
    expr.set_result_type(expr.get_ref_expr()->get_result_type());
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObColumnRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  // @see ObStmt::create_raw_column_expr()
  if (ob_is_string_or_lob_type(expr.get_data_type())
      && (CS_TYPE_INVALID == expr.get_collation_type()
          || CS_LEVEL_INVALID == expr.get_collation_level())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta of binary_ref", K(expr));
  } else if (expr.get_result_type().is_user_defined_sql_type()
             || expr.get_result_type().is_collection_sql_type()) {
    // need const cast to modify subschema ctx, in physcial plan ctx belong to cur exec_ctx;
    ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(my_session_);
    ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
    uint64_t udt_id = expr.get_result_type().get_udt_id();
    uint16_t subschema_id = expr.get_result_type().get_subschema_id();
    if (subschema_id == ObXMLSqlType) {
      // compact with xmltype implement, udt_id may not set, if this column ref is from sub querys
      // example: SELECT id, a_t3.po FROM t1 LEFT JOIN (SELECT t3_id, XMLAGG()) ...
      // all other udt type should use udt_id to get subschma_id when deduce types
      expr.set_subschema_id(ObXMLSqlType);
      expr.set_udt_id(T_OBJ_XML);
    } else if (OB_ISNULL(exec_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("need context to search subschema mapping", K(ret), K(udt_id));
    } else if (ObObjUDTUtil::ob_is_supported_sql_udt(udt_id)) {
      subschema_id = ObMaxSystemUDTSqlType;
      if (OB_FAIL(exec_ctx->get_subschema_id_by_udt_id(udt_id, subschema_id))) {
        LOG_WARN("failed to get subschema id by udt id", K(ret), K(udt_id));
      } else {
        expr.set_subschema_id(subschema_id);
      }
    } else if (expr.get_enum_set_values().count() > 0) {
      // array type
      if (OB_FAIL(exec_ctx->get_subschema_id_by_type_string(expr.get_enum_set_values().at(0), subschema_id))) {
        LOG_WARN("failed to get array type subschema id", K(ret));
      } else if (FALSE_IT(expr.set_subschema_id(subschema_id))) {
      } else if (OB_FAIL(construct_collecton_attr_expr(expr))) {
        LOG_WARN("failed to construct collection attr expr", K(ret));
      }
    } else {
      // stmt : insert into arr_t1 select array(), array() is mock colunmn_expr which isn't with enum_set_values
      // just check subschema_id validity
      ObSubSchemaValue meta_unused;
      if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(subschema_id, meta_unused))) {
        LOG_WARN("invalid subschema id", K(ret), K(subschema_id));
      } else if (OB_FAIL(construct_collecton_attr_expr(expr))) {
        LOG_WARN("failed to construct collection attr expr", K(ret));
      }
    }
  } else if (OB_FAIL(build_subschema_for_enum_set_type(expr))) {
    LOG_WARN("fail to build subschema for enum set type", K(ret), K(expr));
  }

  if (OB_SUCC(ret)) {
    if (expr.get_result_type().is_lob_storage() && !IS_CLUSTER_VERSION_BEFORE_4_1_0_0) {
      expr.set_has_lob_header();
    }
  }
  return ret;
}

int ObRawExprDeduceType::calc_result_type_with_const_arg(
  ObNonTerminalRawExpr &expr,
  ObIExprResTypes &types,
  ObExprTypeCtx &type_ctx,
  ObExprOperator *op,
  ObExprResType &result_type,
  int32_t row_dimension)
{
#define GET_TYPE_ARRAY(types) (types.count() == 0 ? NULL : &(types.at(0)))

  int ret = OB_SUCCESS;
  bool all_const = false;
  ObArray<ObObj*> arg_arrs;
  if (0 <= expr.get_param_count()) {
    all_const = true;
    for (int64_t i = 0; all_const && i < expr.get_param_count(); ++i) {
      ObRawExpr *arg = expr.get_param_expr(i);
      if (OB_ISNULL(arg)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument.", K(ret));
      } else if (!arg->is_const_raw_expr()) {
        all_const = false;
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument.", K(ret));
      } else {
        ObObj &value = static_cast<ObConstRawExpr*>(arg)->get_value();
        if (value.is_unknown()) {
          // 由const 参数决定类型，不可以被参数化
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
        } else if (OB_FAIL(op->calc_result_type3(result_type, types.at(0), types.at(1), types.at(2), type_ctx, arg_arrs))) {
          LOG_WARN("calc result type3 failed", K(ret));
        }
        break;
      default:
        ret = op->calc_result_typeN(result_type, GET_TYPE_ARRAY(types), types.count(), type_ctx, arg_arrs);
        break;
      }  // end switch
    }
    if (OB_FAIL(ret) && my_session_->is_varparams_sql_prepare()) {
      // the ps prepare stage does not do type deduction, and directly gives a default type.
      result_type.set_null();
      ret = OB_SUCCESS;
    }
  }
#undef GET_TYPE_ARRAY
  return ret;
}

/* Most expressions not accept lob type parameters. It reports an error in two situations before:
 * 1. report an error in calc_result_type function of expression;
 * 2. cast lob to calc_type not supported/expected.
 * Only a few expressions deal with lob type parameters in calc_result_type, so most errors caused by 2.
 * However, this makes some problems:
 * For example, cast lob to number is not supported before, and this results that nvl(lob, number)
 * reports inconsistent type error.
 * Since to_number accepts lob type parameter, we support cast lob to number now, and this results
 * that many expression including nvl not report error with lob type parameter any more.
 * It is impractical to modify calc_resut_type functions of all these expressions, so we add
 * function check_lob_param_allowed to make an extra check: whether cast lob parameter to other type is allowed.
 * But we should be cautious to add new rules here considering compatible with previous version.
*/
int ObRawExprDeduceType::check_lob_param_allowed(const ObObjType from,
                                                 const ObCollationType from_cs_type,
                                                 const ObObjType to,
                                                 const ObCollationType to_cs_type,
                                                 ObExprOperatorType expr_type)
{
  UNUSED(from);
  UNUSED(from_cs_type);
  UNUSED(to_cs_type);
  int ret = OB_SUCCESS;
  ObObjTypeClass to_tc = ob_obj_type_class(to);
  if (ObNumberTC == to_tc || ObDecimalIntTC == to_tc) {
    if (T_FUN_SYS_TO_NUMBER != expr_type) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
    }
  }

  return ret;
}

bool need_calc_json(ObItemType item_type)
{
  bool bool_ret = false;
  if (T_FUN_SYS < item_type && item_type < T_FUN_SYS_END) {
    if ((T_FUN_SYS_JSON_OBJECT <= item_type && item_type <= T_FUN_JSON_OBJECTAGG)
      || (T_FUN_SYS_JSON_SCHEMA_VALID <= item_type && item_type <= T_FUN_SYS_JSON_APPEND)) {
      bool_ret = true; // json calc type is decided by json functions
    }
  }
  return bool_ret; // json calc type set to long text in other sql functions
}

bool need_reject_geometry_type(ObItemType item_type)
{
  bool bool_ret = false;
  if ((item_type >= T_OP_BIT_AND && item_type <= T_OP_BIT_RIGHT_SHIFT)
      || (item_type >= T_OP_BTW && item_type <= T_OP_OR)
      || (item_type >= T_FUN_MAX && item_type <= T_FUN_AVG)
      || item_type == T_OP_POW
      || item_type == T_FUN_SYS_EXP
      || (item_type >= T_FUN_SYS_SQRT && item_type <= T_FUN_SYS_TRUNCATE)
      || (item_type >= T_FUN_SYS_POWER && item_type <= T_FUN_SYS_LOG)
      || (item_type >= T_FUN_SYS_ASIN && item_type <= T_FUN_SYS_ATAN2)
      || (item_type >= T_FUN_SYS_COS && item_type <= T_FUN_SYS_TANH)
      || item_type == T_FUN_SYS_ROUND
      || item_type == T_FUN_SYS_CEILING
      || (item_type >= T_OP_NEG && item_type <= T_OP_ABS)
      || item_type == T_FUN_SYS_RAND
      || item_type == T_FUN_SYS_RANDOM
      || item_type == T_OP_SIGN
      || item_type == T_FUN_SYS_DEGREES
      || item_type == T_FUN_SYS_RADIANS
      || item_type == T_FUN_SYS_FORMAT
      || item_type == T_FUN_SYS_COT
      || item_type == T_OP_CONV) {
    bool_ret = true;
  }
  return bool_ret;
}

int ObRawExprDeduceType::push_back_types(const ObRawExpr *param_expr, ObExprResTypes &types)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(ret));
  } else if (OB_FAIL(types.push_back(param_expr->get_result_type()))) {
    LOG_WARN("push back param type failed", K(ret));
  } else {
    const int64_t idx = types.count() - 1;
    const bool is_mysql_mode = lib::is_mysql_mode();
    const char *p_normal_start = nullptr;
    bool is_explain_stmt =
      (my_session_ != NULL && my_session_->get_cur_exec_ctx() != NULL
       && my_session_->get_cur_exec_ctx()->get_sql_ctx() != NULL
       && ObParser::is_explain_stmt(my_session_->get_cur_exec_ctx()->get_sql_ctx()->cur_sql_,
                                    p_normal_start));
    bool is_ddl_stmt =
      (my_session_ != NULL && ObStmt::is_ddl_stmt(my_session_->get_stmt_type(), false));
    bool is_show_stmt = (my_session_ != NULL && ObStmt::is_show_stmt(my_session_->get_stmt_type()));

    LOG_DEBUG("stmt type", K(is_explain_stmt), K(lbt()), K(is_ddl_stmt));
    // `select integer_column + 1.123 from t1`
    // integer_column's precision should be deduced as max_integer_precision
    // same as `create table t2 as select integer_column + 1.234 from t1`
    //
    // `select 1234 + 1.123 from dual`
    //  constant integer's precision should also be deduced as max_integer_precision
    //
    //  `create table t2 as select 1234 as a from dual`
    //  however, the result type of column a is int(4) (mysql)
    //  same as `create table t2 as select 1 + int_prec_10_column as a from t1`
    //  result type of a is int(12)
    //  same as `crreate view v as select int_col_10 + 1 as v_col from t1`
    //  v_col's type is int(12)
    //
    //  note that question mark only exists in non-ddl query, so that if integer expr is a
    //  questionmark expr or column ref expr, its precision should be max integer precision.
    //
    //  explain stmt does not proceduce questionmark exprs, special processing is needed in order to
    //  print precise sql plan.
    if (is_mysql_mode && ob_is_int_uint_tc(types.at(idx).get_type())
        && (param_expr->is_column_ref_expr())) {
      ObPrecision max_prec =
        ObAccuracy::MAX_ACCURACY2[0 /*mysql*/][types.at(idx).get_type()].get_precision();
      const ObPrecision prec = MAX(types.at(idx).get_precision(), max_prec);
      types.at(idx).set_precision(prec);
      types.at(idx).set_scale(0);
    } else if (is_mysql_mode && ob_is_decimal_int_tc(types.at(idx).get_type())) {
      // for decimal int type in mysql, reset calc accuracy to itself to avoid accuracy reuse
      // during type deduce
      types.at(idx).set_calc_accuracy(types.at(idx).get_accuracy());
    } else if (!is_mysql_mode && (is_ddl_stmt || is_show_stmt) && types.at(idx).is_decimal_int()
               && param_expr->is_column_ref_expr()) {
      // If c1 and c2 are both ObDecimalIntType columns, result type of c1 + c2 is ObDecimalIntType.
      // However, result type of `c1 + c2` in ddl stmt needs to be ObNumberType for oracle compatiblity's sake.
      // Hence, we change ObDecimalIntType to ObNumberType heere.
      // same as:
      // create view v as select c1 + c2 from t;
      // desc v;
      types.at(idx).set_number();
    }
  }
  return ret;
}

int ObRawExprDeduceType::calc_result_type(ObNonTerminalRawExpr &expr,
                                          ObIExprResTypes &types,
                                          ObCastMode &cast_mode,
                                          int32_t row_dimension)
{
#define GET_TYPE_ARRAY(types) (types.count() == 0 ? NULL : &(types.at(0)))

  int ret = OB_SUCCESS;
  ObExprTypeCtx type_ctx; // 用于将session等全局变量传入calc_result_type
  type_ctx.set_raw_expr(&expr);
  ObExprOperator *op = expr.get_op();
  ObExprResTypes ori_types;
  const bool is_explicit_cast = (T_FUN_SYS_CAST == expr.get_expr_type()) &&
                                CM_IS_EXPLICIT_CAST(expr.get_extra());
  if (NULL == op) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Get expression operator failed", "expr type", expr.get_expr_type());
  } else if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (op->is_default_expr_cg()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("not implemented in sql static typing engine, ",
             K(ret), K(op->get_type()), K(op->get_name()));
  } else if (expr.get_expr_type() == T_FUN_NORMAL_UDF
             && OB_FAIL(init_normal_udf_expr(expr, op))) {
    LOG_WARN("failed to init normal udf", K(ret));
  } else if (OB_FAIL(ori_types.assign(types))) {
    LOG_WARN("array assign failed", K(ret));
  } else {
    op->set_raw_expr(&expr);
    if (T_FUN_COLUMN_CONV == expr.get_expr_type()
        || T_OP_OUTPUT_PACK == expr.get_expr_type()
        || T_FUN_SYS_REMOVE_CONST == expr.get_expr_type()) {
      // do nothing
    } else {
      //T_OP_OUTPUT_PACK only encode params, so it can process any type without convert
      if (OB_LIKELY(T_OP_OUTPUT_PACK != expr.get_expr_type())) {
        FOREACH_CNT(type, types) {
          if (ObLobType == type->get_type()) {
            type->set_type(ObLongTextType);
          }
          // ToDo: test and fix, not all sql functions need calc json as long text
          if (ObJsonType == type->get_type() && !need_calc_json(expr.get_expr_type())) {
            type->set_calc_type(ObLongTextType);
          } else if (!lib::is_oracle_mode()
                     && ObGeometryType == type->get_type()
                     && need_reject_geometry_type(expr.get_expr_type())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Incorrect geometry arguments", K(expr.get_expr_type()), K(ret));
          }
        }
      }
    }
    op->set_row_dimension(row_dimension);
    op->set_real_param_num(static_cast<int32_t>(types.count()));
    op->set_is_called_in_sql(expr.is_called_in_sql());
    ObSQLUtils::init_type_ctx(my_session_, type_ctx);
    if (OB_SUCC(ret) && !solidify_session_vars_) {
      if (NULL != my_local_vars_) {
        if (OB_FAIL(ObSQLUtils::merge_solidified_vars_into_type_ctx(type_ctx,
                                                                    *my_local_vars_))) {
          LOG_WARN("fail to merge_solidified_vars_into_type_ctx", K(ret));
        }
      } else if (OB_FAIL(ObSQLUtils::merge_solidified_vars_into_type_ctx(type_ctx,
                                                                  expr.get_local_session_var()))) {
        LOG_WARN("fail to merge_solidified_vars_into_type_ctx", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObSQLUtils::get_default_cast_mode(is_explicit_cast, 0,
                                        my_session_->get_stmt_type(),
                                        my_session_->is_ignore_stmt(),
                                        type_ctx.get_sql_mode(),
                                        cast_mode);
    }
    type_ctx.set_cast_mode(cast_mode);
//    type_ctx.my_session_ = this->my_session_;
    ObExprResType result_type(alloc_);
    if (op->get_result_type().has_result_flag(DECIMAL_INT_ADJUST_FLAG)) {
      result_type.set_result_flag(DECIMAL_INT_ADJUST_FLAG);
    }
    if (ob_is_decimal_int_tc(expr.get_result_type().get_type())) {
      result_type.add_cast_mode(expr.get_result_type().get_cast_mode());
    }

    // 预先把所有参数的calc_type都设置成和type一致，
    // 以防calc_result_typeX没有对其进行设置
    // 理想情况下，不应该要这个循环，所有calc_type的设置都在calc_result_typeX中完成

    // For avg(), internally it will call 'division', which requires that both input are
    // casted into number. However, this requirements are not remembered in the input_types
    // for the avg() expression but as the calc_type for the input expression itself. This
    // demands that we set the calculation type here.
    for (int64_t i = 0; i < types.count() && OB_SUCC(ret); ++i) {
      types.at(i).set_calc_meta(types.at(i));
      if (lib::is_mysql_mode() && types.at(i).is_double()) {
        const ObPrecision p = types.at(i).get_precision();
        const ObScale s = types.at(i).get_scale();
        // check whether the precision and scale is valid
        if ((PRECISION_UNKNOWN_YET == p && s == SCALE_UNKNOWN_YET) ||
              (s >= 0 && s <= OB_MAX_DOUBLE_FLOAT_SCALE && p >= s)) {
          types.at(i).set_calc_accuracy(types.at(i).get_accuracy());
        }
      } else if (ob_is_enumset_tc(types.at(i).get_type())) {
        ObObjMeta param_obj_meta;
        if (OB_FAIL(ObRawExprUtils::extract_enum_set_collation(types.at(i),
                                                               my_session_,
                                                               param_obj_meta))) {
          LOG_WARN("fail to extract enum set cs type", K(ret));
        } else {
          // restore enum/set collation there, the expr type deduce is not aware of enum/set
          // subschema meta.
          types.at(i).set_collation(param_obj_meta);
          types.at(i).set_calc_collation_type(param_obj_meta.get_collation_type());
          types.at(i).set_calc_collation_level(param_obj_meta.get_collation_level());
          types.at(i).reset_enum_set_meta_state();
        }
      }
    }
    if (ignore_scale_adjust_for_decimal_int(expr.get_expr_type())) {
      // if no need to adjust scale/precision for decimal int type, e.g. T_FUN_SYS_PART_HASH,
      // just set calc accuracy to type's accuracy to avoid adding cast expr
      for (int64_t i = 0; i < types.count(); i++) {
        if (types.at(i).is_decimal_int()) {
          types.at(i).set_calc_accuracy(types.at(i).get_accuracy());
        }
      }
    }

    if (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0) {
      result_type.set_has_lob_header();
    }
    if (OB_FAIL(ret)) {
    } else if (ObExprOperator::NOT_ROW_DIMENSION != row_dimension) {
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
    if (OB_SUCC(ret)) {
      // refine result type precision and scale here
      if (lib::is_mysql_mode() && result_type.is_decimal_int()) {
        result_type.set_precision(MIN(result_type.get_precision(),
                                      OB_MAX_DECIMAL_POSSIBLE_PRECISION));
      }
    }
    if (OB_FAIL(ret) && my_session_->is_varparams_sql_prepare()) {
      // the ps prepare stage does not do type deduction, and directly gives a default type.
      result_type.set_null();
      ret = OB_SUCCESS;
    }
    // check parameters can cast to expected type
    if (OB_SUCC(ret)) {
      const bool is_oracle_mode = lib::is_oracle_mode();
      for (int64_t i = 0; OB_SUCC(ret) && i < ori_types.count(); i++) {
        const ObObjType from = ori_types.at(i).get_type();
        const ObCollationType from_cs_type = ori_types.at(i).get_collation_type();
        const ObObjType to = types.at(i).get_calc_type();
        const ObCollationType to_cs_type = types.at(i).get_calc_collation_type();
        LOG_DEBUG("check parameters can cast to expected type", K(ret), K(i), K(from), K(to));
        // for most exprs in oracle mode, do not allow bool type param
        if (is_oracle_mode && ObTinyIntType == to
            && !ALLOW_BOOL_INPUT(expr.get_expr_type())) {
          ret = OB_ERR_CALL_WRONG_ARG;
          LOG_WARN("PLS-00306: wrong number or types of arguments in call", K(ret));
        } else if (ObExtendType == from && ob_is_character_type(to, to_cs_type) && !op->is_called_in_sql()) {
          ret = OB_ERR_CALL_WRONG_ARG;
          LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, static_cast<int>(strlen(op->get_name())), op->get_name());
          LOG_WARN("PLS-00306: wrong number or types of arguments in call",
                   K(ret), K(from), K(to), K(op->get_name()), K(op->is_called_in_sql()));
        }

        if (OB_FAIL(ret)) {
        } else if (from != to && !cast_supported(from, from_cs_type, to, to_cs_type)
          && !my_session_->is_varparams_sql_prepare()
          && !(my_session_->is_pl_prepare_stage() && to == ObGeometryType && is_oracle_mode)) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("cast parameter to expected type not supported", K(ret), K(i), K(from), K(to));
        } else if (is_oracle_mode && (ob_is_lob_locator(from) || ob_is_text_tc(from))) {
          if (!my_session_->is_varparams_sql_prepare()
            && OB_FAIL(check_lob_param_allowed(from, from_cs_type, to,
                                              to_cs_type, expr.get_expr_type()))) {
            LOG_WARN("lob parameter not allowed", K(ret));
          }
        }
      }
    }

    LOG_DEBUG("debug for expr params calc meta", K(types));

    //这里是一个验证：
    //新框架oracle模式string类型的结果的字符集与session上定义的charset一致
    //不一致可能是表达式推导有问题
    //参考
    //
    //
    //新引擎稳定后，去掉这里的判断，改为trace日志用于调试

    //这里的check需要忽略隐式cast，因为底层转换函数只能处理utf8的string，所以隐式cast
    //再遇到非utf8的输入时，会将其转为utf8，所以cast推导的结果有可能不符合
    //nls_collation_xxx()的要求
    const bool is_implicit_cast = (T_FUN_SYS_CAST == expr.get_expr_type()) &&
                                  CM_IS_IMPLICIT_CAST(expr.get_extra());
    if (OB_SUCC(ret)
        && lib::is_oracle_mode()
        && !is_implicit_cast) {
      if (result_type.is_nstring()
          && result_type.get_collation_type() != my_session_->get_nls_collation_nation()) {
        //ret = OB_ERR_UNEXPECTED;
        LOG_TRACE("[CHARSET DEDUCE RESULT TYPE]"
                  "result is nchar, but charset is not consistent with session nchar charset",
                  "result collation",
                  ObCharset::collation_name(result_type.get_collation_type()),
                  "session nchar collation",
                  ObCharset::collation_name(my_session_->get_nls_collation_nation()));
      } else if (result_type.is_varchar_or_char()
                 && T_FUN_SYS_DBMS_LOB_CONVERT_CLOB_CHARSET != expr.get_expr_type()
                 && result_type.get_collation_type() != my_session_->get_nls_collation()) {
        //ret = OB_ERR_UNEXPECTED;
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
      if (T_FUN_SYS_UTC_TIME == item_type
          || T_FUN_SYS_UTC_TIMESTAMP == item_type
          || T_FUN_SYS_CUR_TIMESTAMP == item_type
          || T_FUN_SYS_LOCALTIMESTAMP == item_type
          || T_FUN_SYS_CUR_TIME == item_type
          || T_FUN_SYS_SYSDATE == item_type
          || T_FUN_SYS_SYSTIMESTAMP == item_type) {
        /*
         * the precision N has been set in expr.result_type_.scale_, but result_type returned by
         * op->calc_result_type0() has no precision, so we have to copy this info to result_type first.
         */
        result_type.set_accuracy(expr.get_accuracy());
      }

      // FIXME (xiaochu.yh) 这一句的意义是什么?
      // op后面哪里会用到呢，CG阶段会重新分配一个op，并不使用这个。
      op->set_result_type(result_type);
      if (expr.get_expr_type() == T_FUN_COLUMN_CONV) {
        // do nothing
      } else {
        ARRAY_FOREACH_N(types, idx, cnt) {
          ObExprResType &type = types.at(idx);
          if (ObLobType == ori_types.at(idx).get_type()) {
            if (OB_UNLIKELY(ObLongTextType != type.get_type())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("param with origin lob type should be replace to longtext type",
                        K(ret), K(idx), K(expr), K(type));
            } else {
              type.set_type(ObLobType);
            }
          }
        }
      }

      // result_type和input_type都记录到expr中，
      // CG阶段利用expr中这些信息生成ObExprOperator
      expr.set_result_type(result_type);
      ret = expr.set_input_types(types);
    }

    if (OB_SUCC(ret)) {
      cast_mode = type_ctx.get_cast_mode();
      if (expr.get_result_type().has_result_flag(ZEROFILL_FLAG)) {
        cast_mode |= CM_ZERO_FILL;
      }
      if (ob_is_collection_sql_type(expr.get_result_type().get_type())
          && !ObObjUDTUtil::ob_is_supported_sql_udt(expr.get_result_type().get_udt_id())) {
        if (expr.get_expr_class() == ObRawExpr::EXPR_OPERATOR
            || expr.get_expr_class() == ObRawExpr::EXPR_SYS_FUNC
            || expr.get_expr_class() == ObRawExpr::EXPR_SET_OP) {
          ObOpRawExpr *op_expr = static_cast<ObOpRawExpr*>(&expr);
          if (OB_FAIL(construct_collecton_attr_expr(*op_expr))) {
            LOG_WARN("failed to construct collection attr expr", K(ret));
          }
        } else if (expr.get_expr_class() == ObRawExpr::EXPR_CASE_OPERATOR) {
          ObCaseOpRawExpr *op_expr = static_cast<ObCaseOpRawExpr*>(&expr);
          if (OB_FAIL(construct_collecton_attr_expr(*op_expr))) {
            LOG_WARN("failed to construct collection attr expr", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected expr class type", K(ret), K(expr));
        }
      }
    }
    LOG_DEBUG("calc_result_type", K(ret), K(expr), K(types), K(cast_mode));
  }
#undef GET_TYPE_ARRAY
  return ret;
}

template<typename RawExprType>
int ObRawExprDeduceType::add_attr_exprs(const ObCollectionTypeBase *coll_meta, RawExprType &expr)
{
  int ret = OB_SUCCESS;
  ObItemType expr_type = T_REF_COLUMN;
  if (coll_meta->type_id_ == ObNestedType::OB_ARRAY_TYPE) {
    ObColumnRefRawExpr *attr_expr = NULL;
    const ObCollectionArrayType *arr_meta = static_cast<const ObCollectionArrayType*>(coll_meta);
    if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                 expr_type, ArrayAttr::ATTR_NULL_BITMAP,
                                                 attr_expr))) {
      LOG_WARN("failed to create nullbitmap attr expr", K(ret));
    } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
      LOG_WARN("failed to add attr expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                 expr_type, ArrayAttr::ATTR_OFFSETS,
                                                 attr_expr))) {
      LOG_WARN("failed to create offset attr expr", K(ret));
    } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
      LOG_WARN("failed to add attr expr", K(ret));
    } else if (OB_FAIL(add_attr_exprs(arr_meta->element_type_, expr))) {
      LOG_WARN("failed to add attr expr", K(ret));
    }
  } else if (coll_meta->type_id_ == ObNestedType::OB_BASIC_TYPE) {
    ObColumnRefRawExpr *attr_expr = NULL;
    const ObCollectionBasicType *elem_type = static_cast<const ObCollectionBasicType*>(coll_meta);
    if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                 expr_type, ArrayAttr::ATTR_NULL_BITMAP,
                                                 attr_expr))) {
      LOG_WARN("failed to create nullbitmap attr expr", K(ret));
    } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
      LOG_WARN("failed to add attr expr", K(ret));
    } else if (!is_fixed_length(elem_type->basic_meta_.get_obj_type())) {
      if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                 expr_type, ArrayAttr::ATTR_OFFSETS,
                                                 attr_expr))) {
        LOG_WARN("failed to create nullbitmap attr expr", K(ret));
      } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
        LOG_WARN("failed to add attr expr", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                        expr_type, ArrayAttr::ATTR_DATA,
                                                        attr_expr))) {
      LOG_WARN("failed to create nullbitmap attr expr", K(ret));
    } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
      LOG_WARN("failed to add attr expr", K(ret));
    }
  }
  return ret;
}

template<typename RawExprType>
int ObRawExprDeduceType::construct_collecton_attr_expr(RawExprType &expr)
{
  int ret = OB_SUCCESS;
  uint16_t subschema_id = expr.get_result_type().get_subschema_id();
  ObExecContext *exec_ctx = const_cast<ObExecContext *>(my_session_->get_cur_exec_ctx());
  ObSubSchemaValue value;
  const ObSqlCollectionInfo *coll_info = NULL;
  ObItemType expr_type = expr.get_expr_type();
  bool need_set_values = expr.get_enum_set_values().empty();
  bool need_construct_attrs = true;
  if (expr.get_attr_count() > 0) {
    // attrs constructed already, do nothing
    need_construct_attrs = false;
  } else if (expr.is_const_expr()) {
    // is uniform format, do nothing
    need_construct_attrs = false;
  }
  if (!need_set_values && !need_construct_attrs) {
    // do nothing
  } else if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (OB_ISNULL(expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null raw expr", K(ret));
  } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(subschema_id, value))) {
    LOG_WARN("failed to get subschema ctx", K(ret));
  } else if (OB_ISNULL(value.value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subschema is null", K(ret));
  } else {
    coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_);
    ObCollectionTypeBase *coll_meta = coll_info->collection_meta_;
    ObColumnRefRawExpr *attr_expr = NULL;
    if (coll_meta->type_id_ != ObNestedType::OB_ARRAY_TYPE && coll_meta->type_id_ != ObNestedType::OB_VECTOR_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected meta type", K(ret), K(coll_meta->type_id_));
    } else if (OB_ISNULL(coll_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subschema is null", K(ret));
    } else if (need_construct_attrs) {
      if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                          T_REF_COLUMN, ArrayAttr::ATTR_LENGTH,
                                                          attr_expr))) {
        LOG_WARN("failed to create nullbitmap attr expr", K(ret));
      } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
        LOG_WARN("failed to add attr expr", K(ret));
      } else if (OB_FAIL(add_attr_exprs(reinterpret_cast<ObCollectionArrayType *>(coll_meta)->element_type_, expr))) {
        LOG_WARN("failed to add attr expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && need_set_values) {
      ObString def = coll_info->get_def_string();
      ObSEArray<ObString, 1> enum_set_values;
      if (OB_FAIL(enum_set_values.push_back(def))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(expr.set_enum_set_values(enum_set_values))) {
        LOG_WARN("failed to set values", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_session_ is NULL", K(ret));
  } else if (OB_FAIL(check_expr_param(expr))) {
    LOG_WARN("check expr param failed", K(ret));
  } else if (OB_UNLIKELY(expr.get_expr_type() == T_OBJ_ACCESS_REF)) {
    ObObjAccessRawExpr &obj_access_expr = static_cast<ObObjAccessRawExpr &>(expr);
    ObExprResType result_type;
    pl::ObPLDataType final_type;
    if (OB_FAIL(obj_access_expr.get_final_type(final_type))) {
      LOG_WARN("failed to get final type", K(obj_access_expr), K(ret));
    } else if (final_type.is_user_type()) {
      result_type.set_ext();
      result_type.set_extend_type(final_type.get_type());
      result_type.set_udt_id(final_type.get_user_type_id());
    } else if (OB_ISNULL(final_type.get_data_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("basic type must not be null", K(ret));
    } else {
      if (obj_access_expr.for_write()) {
        // We return the target object's address by the extend value of result.
        result_type.set_ext();
      } else {
        result_type.set_meta(final_type.get_data_type()->get_meta_type());
        result_type.set_accuracy(final_type.get_data_type()->get_accuracy());
      }
    }

    expr.set_result_type(result_type);
  } else if (T_OP_ORACLE_OUTER_JOIN_SYMBOL == expr.get_expr_type()) {
    ObRawExpr *param_expr = NULL;
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
    ObRawExpr *left = expr.get_param_expr(0);
    ObRawExpr *right = expr.get_param_expr(1);
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
  } else if (T_OP_ROW == expr.get_expr_type()) {
    expr.set_data_type(ObNullType);
  // During the prepare phase, some boolean expressions do not undergo recursive type deduction.
  // T_OP_EQ, T_OP_NSEQ, T_OP_LE, T_OP_LT, T_OP_GE, T_OP_GT, T_OP_NE.
  } else if (my_session_->is_varparams_sql_prepare() && T_OP_EQ <= expr.get_expr_type() && expr.get_expr_type() <= T_OP_NE) {
    ObExprResType result_type;
    result_type.set_tinyint();
    result_type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    result_type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    expr.set_result_type(result_type);
  } else {
    ObExprOperator *op = expr.get_op();
    if (NULL == op) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Get expression operator failed", "expr type", expr.get_expr_type());
    } else {
      ObExprResTypes types;
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
        const ObRawExpr *param_expr = expr.get_param_expr(i);
        if (OB_ISNULL(param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(i));
        } else if (T_OP_ROW == param_expr->get_expr_type()) {
          if (OB_FAIL(get_row_expr_param_type(*param_expr, types))) {
            LOG_WARN("get row expr param type failed", K(ret));
          }
        } else if (T_REF_QUERY == param_expr->get_expr_type()
                    && T_OP_EXISTS != expr.get_expr_type()
                    && T_OP_NOT_EXISTS != expr.get_expr_type()) {
          //exist/not exist(subquery)的参数类型没有意义
          const ObQueryRefRawExpr *ref_expr = static_cast<const ObQueryRefRawExpr*>(param_expr);
          const ObIArray<ObExprResType> &column_types = ref_expr->get_column_types();
          for (int64_t j = 0; OB_SUCC(ret) && j < column_types.count(); ++j) {
            if (OB_FAIL(types.push_back(column_types.at(j)))) {
              LOG_WARN("push back param type failed", K(ret));
            }
          }
        } else if (OB_FAIL(push_back_types(param_expr, types))) {
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
            ObQueryRefRawExpr *ref_expr = static_cast<ObQueryRefRawExpr*>(expr.get_param_expr(0));
            if (T_OP_EXISTS == expr.get_expr_type() || T_OP_NOT_EXISTS == expr.get_expr_type()) {
              //let row_dimension of exists be ObExprOperator::NOT_ROW_DIMENSION
            } else if (ref_expr->get_output_column() > 1) {
              //subquery的结果作为向量
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
          LOG_WARN("fail convert expr calc result type to op func input types",
                    K(ret), K(types));
        } else if (OB_ISNULL(my_session_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("my_session_ is NULL", K(ret));
        } else if (expr.deduce_type_adding_implicit_cast() &&
                    OB_FAIL(add_implicit_cast(expr, cast_mode))) {
          LOG_WARN("fail add_implicit_cast", K(ret), K(expr));
        }
      } /* end if */
    }
  }
  return ret;
}

int ObRawExprDeduceType::check_row_param(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool cnt_row = false; //向量中的元素仍然是一个向量表达式
  bool cnt_scalar = false; //向量的元素是一个标量表达式
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

int ObRawExprDeduceType::check_param_expr_op_row(ObRawExpr *param_expr, int64_t column_count)
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
        // refer
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_WARN("invalid relational operator", K(ret));
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, column_count);
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::check_expr_param(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_row_param(expr))) {
    LOG_WARN("check row param failed", K(ret));
  } else if (T_OP_IN == expr.get_expr_type() || T_OP_NOT_IN == expr.get_expr_type()) {
    if (OB_ISNULL(expr.get_param_expr(0)) || OB_ISNULL(expr.get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null");
    } else if (expr.get_param_expr(0)->has_flag(IS_SUB_QUERY)) {
      ObRawExpr *left_expr = expr.get_param_expr(0);
      int64_t right_output_column = 0;
      ObQueryRefRawExpr *left_ref = static_cast<ObQueryRefRawExpr *>(left_expr);
      int64_t left_output_column = left_ref->get_output_column();
      //oracle mode not allow: select 1 from dual where (select 1,2 from dual) in (1,2)
      if (is_oracle_mode() && left_output_column > 1) {
        ret = OB_ERR_TOO_MANY_VALUES;
        LOG_WARN("invalid relational operator", K(ret), K(left_output_column));
      } else if (expr.get_param_expr(1)->get_expr_type() == T_OP_ROW) {
        //如果是向量，那么右边输出列的个数就是向量表达式的个数
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_expr(1)->get_param_count(); i++) {
          if (T_OP_ROW == expr.get_param_expr(1)->get_param_expr(i)->get_expr_type()) {
            if(left_output_column != expr.get_param_expr(1)->get_param_expr(i)->get_param_count()) {
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
      if (lib::is_oracle_mode()
          && 1 > column_count
          && T_OP_ROW == expr.get_param_expr(0)->get_param_expr(0)->get_expr_type()) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_WARN("invalid relational operator", K(ret), K(column_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_expr(1)->get_param_count(); i++) {
        if (T_OP_ROW == expr.get_param_expr(1)->get_param_expr(i)->get_expr_type()) {
          if (column_count != expr.get_param_expr(1)->get_param_expr(i)->get_param_count()) {
            ret = OB_ERR_INVALID_COLUMN_NUM;
            LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, column_count);
          } else if (OB_FAIL(check_param_expr_op_row(expr.get_param_expr(1)->get_param_expr(i), column_count))) {
            // refer
            LOG_WARN("failed to check param expr op row", K(ret));
          }
        } else {//如果expr(1)的孩子不为T_OP_ROW,那么expr(0)只能输出1列数据，否则报错
          if (column_count != 1) {
            ret = OB_ERR_INVALID_COLUMN_NUM;
            LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, column_count);
          }
        }
      }
    } else if (T_OP_ROW == expr.get_param_expr(1)->get_expr_type()
               && OB_FAIL(check_param_expr_op_row(expr.get_param_expr(1), 1))) {
      //c1 in (1, 2, 3)
      LOG_WARN("failed to check param expr op row", K(ret));
    }
  } else if (expr.has_flag(CNT_SUB_QUERY) && T_OP_ROW != expr.get_expr_type()) {
    if (IS_COMPARISON_OP(expr.get_expr_type())) {
      //二元操作符，先处理左边操作符，再处理右边操作符
      if (OB_UNLIKELY(expr.get_param_count() != 2)
          || OB_ISNULL(expr.get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr status is invalid", K(expr));
      } else if (OB_FAIL(visit_left_param(*expr.get_param_expr(0)))) {
        LOG_WARN("visit left param failed", K(ret));
      } else if (OB_FAIL(visit_right_param(expr))) {
        LOG_WARN("visit right param failed", K(ret));
      }
    } else if (T_OP_EXISTS != expr.get_expr_type() && T_OP_NOT_EXISTS != expr.get_expr_type()) {
      //在其它情况下如果操作符中出现了子查询，只能作为标量
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
        ObRawExpr *param_expr = expr.get_param_expr(i);
        if (OB_ISNULL(param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(i));
        } else if (get_expr_output_column(*param_expr) != 1) {
          ret = OB_ERR_INVALID_COLUMN_NUM;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
        }
      }
    }
  } else if (lib::is_oracle_mode()
             && (MAYBE_ROW_OP(expr.get_expr_type()))
             && (T_OP_ROW == expr.get_param_expr(0)->get_expr_type() ||
                 T_OP_ROW == expr.get_param_expr(1)->get_expr_type())) {
    if (expr.get_param_expr(0)->get_expr_type() != T_OP_ROW
        && expr.get_param_expr(1)->get_expr_type() == T_OP_ROW) {
      // scalar = vector is not allowed
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_WARN("invalid relational operator", K(ret));
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, static_cast<long>(1));
    } else if (1 > expr.get_param_expr(0)->get_param_count()
        || T_OP_ROW == expr.get_param_expr(0)->get_param_expr(0)->get_expr_type()
        || T_OP_ROW != expr.get_param_expr(1)->get_expr_type()) {
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_WARN("invalid relational operator", K(ret), K(expr.get_param_expr(0)->get_param_count()));
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, expr.get_param_expr(0)->get_param_count());
    //(a, b) = (c, d) or (a, b) = ((c, d)) both are legal
    } else if (expr.get_param_expr(1)->get_param_count() == 1
               && T_OP_ROW == expr.get_param_expr(1)->get_param_expr(0)->get_expr_type()) {
      if (expr.get_param_expr(1)->get_param_expr(0)->get_param_count() != expr.get_param_expr(0)->get_param_count()) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, expr.get_param_expr(0)->get_param_count());
      } else if (OB_FAIL(check_param_expr_op_row(expr.get_param_expr(1)->get_param_expr(0),
                                                 expr.get_param_expr(0)->get_param_count()))) {
        LOG_WARN("failed to check param expr op row", K(ret));
      }
    } else if (expr.get_param_expr(1)->get_param_count() > 1) {
      if (expr.get_param_expr(1)->get_param_count() != expr.get_param_expr(0)->get_param_count()) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, expr.get_param_expr(0)->get_param_count());
      } else if (OB_FAIL(check_param_expr_op_row(expr.get_param_expr(1),
                                                 expr.get_param_expr(0)->get_param_count()))) {
        LOG_WARN("failed to check expr op row", K(ret));
      }
    } else {
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, expr.get_param_expr(0)->get_param_count());
    }
  } else if (IS_COMMON_COMPARISON_OP(expr.get_expr_type())) {
    //普通的二元比较符,左右参数个数应该相等
    ObRawExpr *left_expr = expr.get_param_expr(0);
    ObRawExpr *right_expr = expr.get_param_expr(1);
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
          ObRawExpr *the_expr = expr.get_param_expr(i);
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
        } // end for
      }
    }
  } else if (T_OP_ROW != expr.get_expr_type()
             && OB_FAIL(check_param_expr_op_row(&expr, 1))) {
    //其它普通操作符不能包含向量
    LOG_WARN("failed to check param expr op row", K(ret));
  }
  return ret;
}

int ObRawExprDeduceType::visit_left_param(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (T_OP_ROW == expr.get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
      //左边的操作符是向量，那么向量里面的每个元素只能是一个标量
      ObRawExpr *left_param = expr.get_param_expr(i);
      if (OB_ISNULL(left_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left param is null", K(ret));
      } else if (left_param->has_flag(IS_SUB_QUERY)) {
        ObQueryRefRawExpr *left_ref = static_cast<ObQueryRefRawExpr*>(left_param);
        if (left_ref->get_output_column() != 1) {
          ret = OB_ERR_INVALID_COLUMN_NUM;
          LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
        }
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit_right_param(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *right_expr = expr.get_param_expr(1);
  OB_ASSERT(right_expr);
  int64_t left_output_column = 0;
  if (OB_ISNULL(expr.get_param_expr(0)) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(expr.get_param_expr(0)), K(right_expr));
  } else if (expr.get_param_expr(0)->get_expr_type() == T_OP_ROW) {
    //如果是向量，那么左边输出列的个数就是向量表达式的个数
    left_output_column = expr.get_param_expr(0)->get_param_count();
  } else if (expr.get_param_expr(0)->has_flag(IS_SUB_QUERY)) {
    //oracle mode not allow:
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
      //如果右操作符是由子查询构成的，那么比较左右操作符的输出列个数
      ObQueryRefRawExpr *right_ref = static_cast<ObQueryRefRawExpr*>(right_expr);
      //根据mysql的语义，只有=[ANY/ALL](subquery)才允许出现多列的比较
      //例如：select * from t1 where (c1, c2)=ANY(select c1, c2 from t2)
      //或者右边的子查询结果不是集合，而是一个向量，无论是什么比较操作，都可以出现多列
      //例如：select * from t1 where ROW(1, 2)=(select c1, c2 from t2 where c1=1)
      //其他的操作符只能是单列比较
      //例如：select * from t1 where c1>ANY(select c1 from t2)
      if (T_OP_SQ_EQ == expr.get_expr_type()
          || T_OP_SQ_NSEQ == expr.get_expr_type()
          || T_OP_SQ_NE == expr.get_expr_type()
          || expr.has_flag(IS_WITH_SUBQUERY)) {
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
      //右操作符是向量并且根操作符是in表达式，那么向量中的每个元素的输出列需要和左边相等
      ObOpRawExpr *right_op_expr = static_cast<ObOpRawExpr*>(right_expr);
      if (expr.has_flag(IS_IN)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < right_op_expr->get_param_count(); ++i) {
          if (get_expr_output_column(*(right_op_expr->get_param_expr(i))) != left_output_column) {
            ret = OB_ERR_INVALID_COLUMN_NUM;
            LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_output_column);
          }
        }
      } else {
        if (T_OP_ROW == right_op_expr->get_param_expr(0)->get_expr_type()) {
          right_op_expr = static_cast<ObOpRawExpr*>(right_op_expr->get_param_expr(0));
        }
        //如果根操作符不是in表达式，那么向量的个数应该和左边输出列相等，并且向量中的每个元素必须是标量
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
      //右操作符既不是子查询，也不是向量，那么作为普通操作符，左边的表达式必须是一个输出列
      if (left_output_column != 1) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, left_output_column);
      }
    }
  }
  return ret;
}

int64_t ObRawExprDeduceType::get_expr_output_column(const ObRawExpr &expr)
{
  int64_t output_column_cnt = 1;
  if (expr.has_flag(IS_SUB_QUERY)) {
    output_column_cnt = static_cast<const ObQueryRefRawExpr&>(expr).is_cursor()
        ? 1 : static_cast<const ObQueryRefRawExpr&>(expr).get_output_column();
  } else if (T_OP_ROW == expr.get_expr_type()) {
    output_column_cnt = expr.get_param_count();
  }
  return output_column_cnt;
}

int ObRawExprDeduceType::visit(ObCaseOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObExprOperator *op = expr.get_op();
  if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (NULL == op) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Get expression operator failed", "expr type", expr.get_expr_type());
  } else {
    ObExprResTypes types;
    ObRawExpr *arg_param = expr.get_arg_param_expr();
    if (NULL != arg_param) {
      if (1 != get_expr_output_column(*arg_param)) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      } else {
        ret = push_back_types(arg_param, types);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_when_expr_size(); i++) {
      const ObRawExpr *when_expr = expr.get_when_param_expr(i);
      if (OB_ISNULL(when_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("when exprs is null");
      } else if (1 != get_expr_output_column(*when_expr)) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      } else {
        ret = push_back_types(when_expr, types);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_then_expr_size(); i++) {
      const ObRawExpr *then_expr = expr.get_then_param_expr(i);
      if (OB_ISNULL(then_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("then exprs is null");
      } else if (1 != get_expr_output_column(*then_expr)) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      } else {
        ret = push_back_types(then_expr, types);
      }
    }
    if (OB_SUCC(ret) && expr.get_default_param_expr() != NULL) {
      const ObRawExpr *def_expr = expr.get_default_param_expr();
      if (OB_ISNULL(def_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("default expr of case expr is NULL", K(ret));
      } else if (1 != get_expr_output_column(*def_expr)) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      } else {
        ret = push_back_types(def_expr, types);
      }
    }
    if (OB_SUCC(ret)) {
      ObCastMode cast_mode = CM_NONE;
      if (OB_FAIL(calc_result_type(expr, types, cast_mode,
                                   ObExprOperator::NOT_ROW_DIMENSION))) {
        LOG_WARN("calc_result_type failed", K(ret));
      } else if (T_OP_ARG_CASE != expr.get_expr_type() &&
                 OB_FAIL(add_implicit_cast(expr, cast_mode))) {
        // only add_implicit_cast for T_OP_CASE, T_OP_ARG_CASE will be transformed
        // to T_OP_CASE in transform phase
        LOG_WARN("add_implicit_cast failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::set_json_agg_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type, bool &need_add_cast)
{
  int ret = OB_SUCCESS;

  switch (expr.get_expr_type()) {
    case T_FUN_JSON_ARRAYAGG: {
      ObRawExpr *param_expr1 = NULL;
      if (OB_UNLIKELY(expr.get_real_param_count() != 1) ||
          OB_ISNULL(param_expr1 = expr.get_param_expr(0))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()),
                                         K(expr.get_real_param_count()), K(expr));
      } else {
        ObExprResType& expr_type1 = const_cast<ObExprResType&>(param_expr1->get_result_type());
        need_add_cast = expr_type1.is_enum_set_with_subschema();
      }
      result_type.set_json();
      result_type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
      expr.set_result_type(result_type);
      break;
    }
    case T_FUN_ORA_JSON_ARRAYAGG: {
      ObRawExpr *col_expr = NULL;
      ObRawExpr *format_json_expr = NULL;
      if (OB_UNLIKELY(expr.get_real_param_count() < DEDUCE_JSON_ARRAYAGG_FORMAT) ||
          OB_ISNULL(col_expr = expr.get_param_expr(DEDUCE_JSON_ARRAYAGG_EXPR)) ||
          OB_ISNULL(format_json_expr = expr.get_param_expr(DEDUCE_JSON_ARRAYAGG_FORMAT))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()),
                                         K(expr.get_real_param_count()), K(expr));
      } else {
        bool format_json = (is_oracle_mode()
                            && format_json_expr->get_data_type() == ObIntType
                            && static_cast<ObConstRawExpr *>(format_json_expr)->get_value().get_int())
                           ? true
                           : false;
        ObExprResType& col_type = const_cast<ObExprResType&>(col_expr->get_result_type());
        // check format json constrain
        if (format_json && col_type.get_type_class() != ObStringTC && col_type.get_type_class() != ObNullTC
            && col_type.get_type_class() != ObTextTC && col_type.get_type_class() != ObRawTC
            && col_expr->get_expr_class() != ObRawExpr::EXPR_OPERATOR) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(col_type.get_type()));
        } else {
          // check order by constrain
          const common::ObIArray<OrderItem>& order_item = expr.get_order_items();
          for (int64_t i = 0; OB_SUCC(ret) && i < order_item.count(); ++i) {
            ObRawExpr* order_expr = order_item.at(i).expr_;
            if (OB_ISNULL(order_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("internal order expr is null", K(ret));
            } else if (order_expr->get_expr_type() == T_REF_COLUMN) {
              const ObColumnRefRawExpr *order_column = static_cast<const ObColumnRefRawExpr *>(order_expr);
              if (order_column->is_lob_column()) {
                ret = OB_ERR_LOB_TYPE_NOT_SORTING;
                LOG_WARN("Column of LOB type cannot be used for sorting", K(ret));
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(set_agg_json_array_result_type(expr, result_type))) {
            LOG_WARN("set json_arrayagg result type failed", K(ret));
          } else {
            expr.set_result_type(result_type);
          }
        }
      }
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
        result_type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
        expr.set_result_type(result_type);
      }
      break;
    }
    case T_FUN_ORA_JSON_OBJECTAGG: {
      ObRawExpr *key_expr = NULL;
      ObRawExpr *value_expr = NULL;
      ObRawExpr *return_type_expr = NULL;
      ObRawExpr *format_json_expr = NULL;
      if (OB_ISNULL(key_expr = expr.get_param_expr(PARSE_JSON_OBJECTAGG_KEY)) ||
          OB_ISNULL(value_expr = expr.get_param_expr(PARSE_JSON_OBJECTAGG_VALUE)) ||
          OB_ISNULL(format_json_expr = expr.get_param_expr(PARSE_JSON_OBJECTAGG_FORMAT)) ||
          OB_ISNULL(return_type_expr = expr.get_param_expr(PARSE_JSON_OBJECTAGG_RETURNING))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()),
                                         K(expr.get_real_param_count()), K(expr));
      } else {
        bool format_json = (is_oracle_mode()
                            && format_json_expr->get_data_type() == ObIntType
                            && static_cast<ObConstRawExpr *>(format_json_expr)->get_value().get_int())
                           ? true
                           : false;
        ObExprResType& col_type = const_cast<ObExprResType&>(value_expr->get_result_type());
        ObObjType key_type = const_cast<ObExprResType&>(key_expr->get_result_type()).get_type();
        if (key_type == ObNullType) {
          ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
          LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
        } else if (!ob_is_string_tc(key_type)) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          if (key_type == ObLongTextType) {
            LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", "LOB");
          } else {
            LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(key_type));
          }
        } else if (format_json && col_type.get_type_class() != ObStringTC && col_type.get_type_class() != ObNullTC
            && col_type.get_type_class() != ObLobTC && col_type.get_type_class() != ObRawTC && col_type.get_type_class() != ObTextTC
            && value_expr->get_expr_class() != ObRawExpr::EXPR_OPERATOR) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(col_type.get_type()));
        } else {
          ParseNode parse_node;
          parse_node.value_ = static_cast<ObConstRawExpr *>(return_type_expr)->get_value().get_int();
          ObScale scale = static_cast<ObConstRawExpr *>(return_type_expr)->get_accuracy().get_scale();
          bool is_json_type = (scale == 1) && (col_type.get_type_class() == ObJsonTC);
          is_json_type = (is_json_type || parse_node.value_ == 0);
          ObObjType obj_type = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
          result_type.set_collation_type(static_cast<ObCollationType>(parse_node.int16_values_[OB_NODE_CAST_COLL_IDX]));
          if (ob_is_string_type(obj_type) && !is_json_type) {
            result_type.set_type(obj_type);
            result_type.set_length(OB_MAX_SQL_LENGTH);
            result_type.set_length_semantics(my_session_->get_actual_nls_length_semantics());
            if (ob_is_blob(obj_type, result_type.get_collation_type())) {
              result_type.set_collation_type(CS_TYPE_BINARY);
              result_type.set_calc_collation_type(CS_TYPE_BINARY);
            } else {
              result_type.set_collation_type(my_session_->get_nls_collation());
              result_type.set_calc_collation_type(my_session_->get_nls_collation());
            }
            result_type.set_collation_level(CS_LEVEL_IMPLICIT);
            expr.set_result_type(result_type);
          } else if (ob_is_lob_locator(obj_type)) {
            result_type.set_clob_locator();
            result_type.set_collation_type(my_session_->get_nls_collation());
            result_type.set_calc_collation_type(my_session_->get_nls_collation());
            result_type.set_collation_level(CS_LEVEL_IMPLICIT);
            expr.set_result_type(result_type);
          } else if (ob_is_json(obj_type) || is_json_type) {
            result_type.set_json();
            result_type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
          } else if (ob_is_raw(obj_type)) {
            result_type.set_type(obj_type);
            result_type.set_full_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX],
                                        return_type_expr->get_result_type().get_accuracy().get_length_semantics());
            result_type.set_collation_type(CS_TYPE_BINARY);
            result_type.set_calc_collation_type(CS_TYPE_BINARY);
            result_type.set_collation_level(CS_LEVEL_NUMERIC);
          }
          expr.set_result_type(result_type);
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to visit json agg function", K(ret), K(expr.get_expr_type()));
    }
  }

  return ret;
}

int ObRawExprDeduceType::visit(ObAggFunRawExpr &expr)
{
  ObScale avg_scale_increment_ = 4;
  ObScale sum_scale_increment_ = 0;
  ObScale scale_increment_recover = -2;
  int ret = OB_SUCCESS;
  ObExprResType result_type(alloc_);
  if (OB_FAIL(check_group_aggr_param(expr))) {
    LOG_WARN("failed to check group aggr param", K(ret));
  } else {
    bool need_add_cast = false;
    bool override_calc_meta = true;
    switch (expr.get_expr_type()) {
      //count_sum是在分布式的count(*)中上层为了避免select a, count(a) from t1这种语句a出现NULL这种非期望值
      //而生成的内部表达式
      case T_FUN_COUNT:
      case T_FUN_REGR_COUNT:
      case T_FUN_COUNT_SUM:
      case T_FUN_APPROX_COUNT_DISTINCT:
      case T_FUN_KEEP_COUNT:
      case T_FUN_SUM_OPNSIZE: {
        if (lib::is_oracle_mode()) {
          result_type.set_number();
          result_type.set_scale(0);
          result_type.set_precision(OB_MAX_NUMBER_PRECISION);
          expr.set_result_type(result_type);
        } else {
          //mysql中暂时没有支持approx_count_distinct，这里我们mysql模式也支持，返回类型
          //和count函数返回相同，ob的oracle模式则和oracle保持兼容，为decimal类型。
          expr.set_data_type(ObIntType);
          expr.set_scale(0);
          expr.set_precision(MAX_BIGINT_WIDTH);
        }
        break;
      }
      case T_FUN_ORA_XMLAGG: {
        if (OB_FAIL(set_xmlagg_result_type(expr, result_type))) {
          LOG_WARN("set xmlagg result type failed", K(ret));
        }
        break;
      }
      case T_FUN_WM_CONCAT:
      case T_FUN_KEEP_WM_CONCAT: {
        need_add_cast = true;
        const ObRawExpr *param_expr = expr.get_param_expr(0);
        if (OB_ISNULL(param_expr) || OB_ISNULL(my_session_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected NULL", K(param_expr), K(my_session_), K(ret));
        } else {
          result_type.set_clob_locator();
          result_type.set_accuracy(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][ObLobType]);
          // should set result_type to longtext type after enabled lob locator v2,
          // However, ObLobType is used for compatiablity, refer to static_engine.subplan_scan_oracle
          // bug:
          result_type.set_collation_type(my_session_->get_nls_collation());
          result_type.set_calc_collation_type(my_session_->get_nls_collation());
          result_type.set_collation_level(CS_LEVEL_IMPLICIT);
          result_type.set_length(OB_MAX_LONGTEXT_LENGTH);
          expr.set_result_type(result_type);
        }
        break;
      }
      case T_FUN_JSON_ARRAYAGG:
      case T_FUN_ORA_JSON_ARRAYAGG:
      case T_FUN_JSON_OBJECTAGG:
      case T_FUN_ORA_JSON_OBJECTAGG: {
        if (OB_FAIL(set_json_agg_result_type(expr, result_type, need_add_cast))) {
          LOG_WARN("set json agg result type failed", K(ret));
         }
        break;
      }
      case T_FUN_SYS_ST_ASMVT: {
        if (OB_FAIL(set_asmvt_result_type(expr, result_type))) {
          LOG_WARN("set asmvt result type failed", K(ret));
        }
        break;
      }
      case T_FUNC_SYS_ARRAY_AGG: {
        if (OB_FAIL(set_array_agg_result_type(expr, result_type))) {
          LOG_WARN("set array agg result type failed", K(ret));
        }
        break;
      }
      case T_FUN_SYS_RB_BUILD_AGG:
      case T_FUN_SYS_RB_OR_AGG:
      case T_FUN_SYS_RB_AND_AGG: {
        if (OB_FAIL(set_rb_result_type(expr, result_type))) {
          LOG_WARN("set rb_agg result type failed", K(ret));
        }
        break;
      }
      case T_FUN_GROUP_CONCAT: {
        need_add_cast = true;
        if (OB_FAIL(set_agg_group_concat_result_type(expr, result_type))) {
          LOG_WARN("set agg group concat result type failed", K(ret));
        }
        break;
      }
      case T_FUN_SYS_BIT_AND:
      case T_FUN_SYS_BIT_OR:
      case T_FUN_SYS_BIT_XOR: {
        ObRawExpr *child_expr = NULL;
        if (OB_ISNULL(child_expr = expr.get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(expr));
        } else {
          result_type.set_type(ObUInt64Type);
          result_type.set_calc_type(ob_is_unsigned_type(child_expr->get_data_type()) ?
            ObUInt64Type : ObIntType);
          override_calc_meta = false;
          result_type.set_accuracy(ObAccuracy::MAX_ACCURACY2[0/*is_oracle*/][ObUInt64Type]);
          expr.set_result_type(result_type);
          ObObjTypeClass from_tc = child_expr->get_type_class();
          need_add_cast = (ObUIntTC != from_tc && ObIntTC != from_tc && ObBitTC != from_tc);
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
        ObRawExpr *child_expr = NULL;
        bool enable_decimaint = false;
        if (OB_ISNULL(child_expr = expr.get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null");
        } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(my_session_, enable_decimaint))) {
          LOG_WARN("fail to get decimal int configure", K(ret));
        } else if (OB_UNLIKELY(ob_is_geometry(child_expr->get_data_type()))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Incorrect geometry arguments", K(child_expr->get_data_type()), K(ret));
        } else if (OB_UNLIKELY(ob_is_roaringbitmap(child_expr->get_data_type()))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Incorrect roaringbitmap arguments", K(child_expr->get_data_type()), K(ret));
        } else if (lib::is_oracle_mode()) {
          ObObjType from_type = child_expr->get_result_type().get_type();
          ObCollationType from_cs_type = child_expr->get_result_type().get_collation_type();
          const ObObjType to_type = ((ob_is_double_type(from_type) || ob_is_float_type(from_type))
                                      ? from_type
                                      : ObNumberType);
          if (from_type != to_type && !cast_supported(from_type, from_cs_type,
                                                      to_type, CS_TYPE_BINARY)
              && !my_session_->is_varparams_sql_prepare()) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("cast to expected type not supported", K(ret), K(from_type), K(to_type));
          } else {
            result_type.set_type(to_type);
            result_type.set_scale(
                ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_scale());
            result_type.set_precision(
                ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_precision());
            result_type.set_calc_type(result_type.get_type());
            result_type.set_calc_accuracy(result_type.get_accuracy());
            expr.set_result_type(result_type);
          }
          if (T_FUN_SUM == expr.get_expr_type() || T_FUN_AVG == expr.get_expr_type()) {
            const int16_t MAX_PRECISION_NO_NEED_CAST = 34;
            if (ObDecimalIntType == from_type &&
                child_expr->get_result_type().get_precision() <= MAX_PRECISION_NO_NEED_CAST) {
              need_add_cast = false;
            } else {
              need_add_cast = true;
            }
          }
        } else { //mysql mode
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

          bool need_wrap_to_double = false;
          if (OB_FAIL(ret)) {
          } else if (T_FUN_VARIANCE == expr.get_expr_type() ||
                     T_FUN_STDDEV == expr.get_expr_type() ||
                     T_FUN_STDDEV_POP == expr.get_expr_type() ||
                     T_FUN_STDDEV_SAMP == expr.get_expr_type() ||
                     T_FUN_VAR_POP == expr.get_expr_type() ||
                     T_FUN_VAR_SAMP == expr.get_expr_type()) {
            //mysql模式返回类型为double
            ObObjType from_type = child_expr->get_result_type().get_type();
            const ObObjType to_type = (ob_is_double_type(from_type) ? from_type : ObDoubleType);
            result_type.set_type(to_type);
            result_type.set_scale(ObAccuracy(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET).get_scale());
            result_type.set_precision(
                              ObAccuracy(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET).get_precision());
            result_type.set_calc_type(result_type.get_type());
            result_type.set_calc_accuracy(result_type.get_accuracy());
            need_wrap_to_double = true;
          } else if (ObNullType == obj_type) {
            result_type.set_double();
            // todo jiuren
            if (result_type.get_scale() == -1) {
              scale_increment_recover = static_cast<ObScale>(-1);
              result_type.set_scale(static_cast<ObScale>(scale_increment));
            } else {
              scale_increment_recover = result_type.get_scale();
              result_type.set_scale(static_cast<ObScale>(result_type.get_scale() + scale_increment));
            }
          } else if (ob_is_float_tc(obj_type) || ob_is_double_tc(obj_type)) {
            result_type.set_double();
            if (result_type.get_scale() >= 0) {
              scale_increment_recover = result_type.get_scale();
              result_type.set_scale(static_cast<ObScale>(result_type.get_scale() + scale_increment));
              if (T_FUN_AVG == expr.get_expr_type()) {
                result_type.set_precision(
                  static_cast<ObPrecision>(result_type.get_precision() + scale_increment));
              } else {
                result_type.set_precision(
                  static_cast<ObPrecision>(ObMySQLUtil::float_length(result_type.get_scale())));
              }
            }
            // recheck precision and scale overflow
            if (result_type.get_precision() > OB_MAX_DOUBLE_FLOAT_DISPLAY_WIDTH ||
                  result_type.get_scale() > OB_MAX_DOUBLE_FLOAT_SCALE) {
              result_type.set_scale(SCALE_UNKNOWN_YET);
              result_type.set_precision(PRECISION_UNKNOWN_YET);
            }
          } else if (ob_is_json(obj_type) || ob_is_string_type(obj_type) ||
                       ob_is_enumset_tc(obj_type)) {
            // string to double no need scale information
            result_type.set_double();
            // todo jiuren
            // todo blob and text@hanhui
            if (ob_is_enumset_tc(obj_type)) {
              result_type.set_scale(SCALE_UNKNOWN_YET);
              result_type.set_precision(PRECISION_UNKNOWN_YET);
            } else if (result_type.get_scale() >= 0) {
              scale_increment_recover = result_type.get_scale();
              result_type.set_scale(static_cast<ObScale>(result_type.get_scale() + scale_increment));
            }
          } else if (ob_is_collection_sql_type(obj_type)) {
            if (T_FUN_SUM == expr.get_expr_type() || T_FUN_AVG == expr.get_expr_type()) {
              ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(my_session_);
              ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
              uint16_t subschema_id = result_type.get_subschema_id();
              ObSubSchemaValue value;
              const ObSqlCollectionInfo *coll_info = NULL;
              if (OB_ISNULL(exec_ctx)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("need context to search subschema mapping", K(ret), K(subschema_id));
              } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(subschema_id, value))) {
                LOG_WARN("failed to get subschema ctx", K(ret));
              } else if (value.type_ >= OB_SUBSCHEMA_MAX_TYPE) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid subschema type", K(ret), K(value));
              } else if (FALSE_IT(coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_))) {
              } else if (coll_info->collection_meta_->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
                result_type.set_collection(subschema_id);
                expr.set_result_type(result_type);
                if (OB_FAIL(construct_collecton_attr_expr(expr))) {
                  LOG_WARN("failed to construct collection attr expr", K(ret));
                }
              } else {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("not supported collection type", K(ret), "type", coll_info->collection_meta_->type_id_);
              }
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("Incorrect collection arguments", K(child_expr->get_data_type()), K(ret));
            }
          } else {
            if (ob_is_number_tc(obj_type)) {
              result_type.set_number();
            } else if (ob_is_decimal_int(obj_type)) {
              result_type.set_decimal_int();
            } else if (enable_decimaint) {
              result_type.set_decimal_int();
            } else {
              result_type.set_number();
            }
            if (T_FUN_AVG == expr.get_expr_type()) {
              // FIXME: @zuojiao.hzj : remove this after we can keep high division calc scale
              // using decimal int
              result_type.set_number();
            }
            // todo jiuren
            if (result_type.get_scale() == -1) {
              scale_increment_recover = static_cast<ObScale>(-1);
              result_type.set_scale(static_cast<ObScale>(scale_increment));
            } else {
              scale_increment_recover = result_type.get_scale();
              result_type.set_scale(static_cast<ObScale>(
                MIN(OB_MAX_DOUBLE_FLOAT_SCALE, result_type.get_scale() + scale_increment)));
            }
            int64_t precision_increment = scale_increment;
            int16_t result_precision = result_type.get_precision();
            // In mysql mode, the precision of sum() will increase by OB_DECIMAL_LONGLONG_DIGITS.
            // But for expression like sum(sum()), the outer sum() is generated by
            // aggregation pushdown, so we don't need to accumulate precision for the outer expr.
            if (T_FUN_SUM == expr.get_expr_type() && (T_FUN_SUM != child_expr->get_expr_type() ||
                                                      !expr.has_flag(IS_INNER_ADDED_EXPR))) {
              if (ob_is_integer_type(obj_type)) {
                const int16_t int_max_prec = ObAccuracy::MAX_ACCURACY2[0/*mysql mode*/][obj_type].get_precision();
                result_precision = MAX(result_precision, int_max_prec) + OB_DECIMAL_LONGLONG_DIGITS;
                result_precision = MIN(OB_MAX_DECIMAL_PRECISION, result_precision);
              } else {
                result_precision += OB_DECIMAL_LONGLONG_DIGITS;
                result_precision = MIN(OB_MAX_DECIMAL_POSSIBLE_PRECISION, result_precision);
              }
            } else {
              result_precision += precision_increment;
            }
            result_type.set_precision(static_cast<ObPrecision>(result_precision));
          }
          result_type.unset_result_flag(ZEROFILL_FLAG);
          expr.set_result_type(result_type);
          ObObjTypeClass from_tc = expr.get_param_expr(0)->get_type_class();
          //use fast path
          if ((ObIntTC == from_tc || ObUIntTC == from_tc) ||
               (ObDecimalIntTC == from_tc && !need_wrap_to_double)) {
            need_add_cast = false;
          } else {
            need_add_cast = true;
          }
        }
        break;
      }
      case T_FUN_MEDIAN:
      case T_FUN_GROUP_PERCENTILE_CONT:
      case T_FUN_GROUP_PERCENTILE_DISC: {
        if (OB_FAIL(check_median_percentile_param(expr))) {
          LOG_WARN("failed to check median/percentile param", K(ret));
        } else if (lib::is_oracle_mode()) {
          const ObObjType from_type = expr.get_order_items().at(0).expr_->get_result_type().get_type();
          const ObCollationType from_cs_type = expr.get_order_items().at(0).expr_->
                                            get_result_type().get_collation_type();
          bool keep_from_type = false;
          //old sql engine can't support order by lob, So temporarily ban it.
          if (T_FUN_GROUP_PERCENTILE_DISC == expr.get_expr_type()) {
            if (OB_UNLIKELY(ob_is_lob_locator(from_type))) {
              ret = OB_ERR_INVALID_TYPE_FOR_OP;
              LOG_WARN("lob type parameter not expected", K(ret));
            } else if (ob_is_clob(from_type, from_cs_type) || ob_is_blob(from_type, from_cs_type)) {
              if (expr.get_order_items().at(0).is_descending()) {
                ret = OB_ERR_INVALID_TYPE_FOR_OP;
                LOG_WARN("lob type parameter not expected", K(ret));
              }
            } else if (ob_is_decimal_int(from_type)) {
              // cast decimal int to number to calc result
              keep_from_type = false;
            } else {
              keep_from_type = true;
            }
          } else if (ob_is_oracle_datetime_tc(from_type) || ob_is_interval_tc(from_type)
                     || ob_is_float_tc(from_type) || ob_is_double_tc(from_type)) {
            keep_from_type = true;
          } else if (ob_is_oracle_numeric_type(from_type)) {
            keep_from_type = false;
          } else {
            ret = OB_ERR_ARGUMENT_SHOULD_NUMERIC_DATE_DATETIME_TYPE;
            LOG_WARN("expected numeric or date/datetime type", K(ret), K(from_type));
          }
          if (OB_SUCC(ret)) {
            const ObObjType to_type = keep_from_type ? from_type
                                      : ((T_FUN_GROUP_PERCENTILE_DISC == expr.get_expr_type()
                                            && !ob_is_decimal_int(from_type))
                                              ? ObLongTextType : ObNumberType);
            const ObCollationType to_cs_type = keep_from_type ? from_cs_type
                                      : ((T_FUN_GROUP_PERCENTILE_DISC == expr.get_expr_type()
                                            && !ob_is_decimal_int(from_type))
                                              ? from_cs_type : CS_TYPE_BINARY);
            if (from_type != to_type && !cast_supported(from_type, from_cs_type,
                                                        to_type, to_cs_type)
                && !my_session_->is_varparams_sql_prepare()) {
              ret = OB_ERR_INVALID_TYPE_FOR_OP;
              LOG_WARN("cast to expected type not supported", K(ret), K(from_type), K(to_type));
            } else {
               result_type.assign(expr.get_order_items().at(0).expr_->get_result_type());
              if (from_type != to_type) {
                result_type.set_type(to_type);
              }
              result_type.set_scale(
                  ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_scale());
              result_type.set_precision(
                  ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_precision());
              expr.set_result_type(result_type);
              ObCastMode def_cast_mode = CM_NONE;
              result_type.set_calc_type(result_type.get_type());
              result_type.set_calc_accuracy(result_type.get_accuracy());
              expr.set_result_type(result_type);
              if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_,
                                                            def_cast_mode))) {
                LOG_WARN("get_default_cast_mode failed", K(ret));
              } else if (OB_FAIL(add_median_percentile_implicit_cast(expr,
                                                                     def_cast_mode,
                                                                     keep_from_type))) {
                LOG_WARN("failed to add median/percentile implicit cast", K(ret));
              }
            }
          }
        } else { //mysql mode
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
        need_add_cast = true;//兼容oracle行为，covar_pop/covar_samp不用添加cast
      case T_FUN_REGR_AVGX:
      case T_FUN_REGR_AVGY:
      case T_FUN_COVAR_POP:
      case T_FUN_COVAR_SAMP: {
        ret = set_agg_regr_result_type(expr, result_type);
        break;
      }
      case T_FUN_GROUPING:
      case T_FUN_GROUPING_ID:
      case T_FUN_GROUP_ID: {
        if (!lib::is_oracle_mode()) {
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
        result_type.set_varchar();
        result_type.set_length(ObAggregateProcessor::get_llc_size());
        ObCollationType coll_type = CS_TYPE_INVALID;
        CK(OB_NOT_NULL(my_session_));
        OC( (my_session_->get_collation_connection)(coll_type) );
        result_type.set_collation_type(coll_type);
        result_type.set_collation_level(CS_LEVEL_IMPLICIT);
        expr.set_result_type(result_type);
        break;
      }
      case T_FUN_GROUP_RANK:
      case T_FUN_GROUP_DENSE_RANK:
      case T_FUN_GROUP_PERCENT_RANK:
      case T_FUN_GROUP_CUME_DIST: {
        if (OB_FAIL(check_group_rank_aggr_param(expr))) {
          LOG_WARN("failed to check group aggr param", K(ret));
        } else {
          result_type.set_type(ObNumberType);
          result_type.set_scale(
            ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_scale());
          result_type.set_precision(
            ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_precision());
          expr.set_result_type(result_type);
          //group相关的rank比较特殊，新引擎需要单独进行cast判定
          ObCastMode def_cast_mode = CM_NONE;
          if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_,
                                                        def_cast_mode))) {
            LOG_WARN("get_default_cast_mode failed", K(ret));
          } else if (OB_FAIL(add_group_aggr_implicit_cast(expr, def_cast_mode))) {
            LOG_WARN("failed to add group aggr implicit cast", K(ret));
          }
        }
        break;
      }
      case T_FUN_TOP_FRE_HIST: {
        result_type.set_blob();
        result_type.set_collation_level(CS_LEVEL_IMPLICIT);
        result_type.set_length(OB_MAX_LONGTEXT_LENGTH);
        ObRawExpr *param_expr = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
          if (OB_ISNULL(param_expr = expr.get_param_expr(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(param_expr), K(expr.get_param_count()));
          } else if (i == 0 || i == 2 || i == 3) {
            if (lib::is_oracle_mode()) {
              const_cast<ObExprResType&>(param_expr->get_result_type()).set_calc_type(ObNumberType);
            } else {
              const_cast<ObExprResType&>(param_expr->get_result_type()).set_calc_type(ObIntType);
            }
          } else if (i == 1) {
            if (param_expr->is_enum_set_with_subschema()) {
              ObObjMeta org_obj_meta;
              if (OB_FAIL(ObRawExprUtils::extract_enum_set_collation(param_expr->get_result_type(),
                                                                     my_session_,
                                                                     org_obj_meta))) {
                LOG_WARN("fail to extract enum set cs type", K(ret));
              } else {
                result_type.set_collation_type(org_obj_meta.get_collation_type());
              }
            } else {
              result_type.set_collation_type(param_expr->get_result_type().get_collation_type());
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected NULL", K(expr.get_param_count()), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          expr.set_result_type(result_type);
        }
        break;
      }
      case T_FUN_PL_AGG_UDF: {
        if (OB_ISNULL(expr.get_pl_agg_udf_expr()) ||
            OB_UNLIKELY(!expr.get_pl_agg_udf_expr()->is_udf_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(expr.get_pl_agg_udf_expr()));
        } else {
          ObUDFRawExpr *udf_expr = static_cast<ObUDFRawExpr *>(expr.get_pl_agg_udf_expr());
          result_type = udf_expr->get_result_type();
          expr.set_result_type(udf_expr->get_result_type());
          if (result_type.is_character_type() && result_type.get_length() < 0) {
            if (result_type.is_char() || result_type.is_nchar()) {
              result_type.set_length(OB_MAX_ORACLE_PL_CHAR_LENGTH_BYTE);
            } else if (result_type.is_nvarchar2() || result_type.is_varchar()) {
              result_type.set_length(OB_MAX_ORACLE_VARCHAR_LENGTH);
            }
          }
          expr.set_result_type(result_type);
          expr.unset_result_flag(NOT_NULL_FLAG);
        }
        break;
      }
      case T_FUN_HYBRID_HIST: {
        ObRawExpr *param_expr1 = NULL;
        ObRawExpr *param_expr2 = NULL;
        if (OB_UNLIKELY(expr.get_param_count() != 3 || expr.get_real_param_count() != 2) ||
            OB_ISNULL(param_expr1 = expr.get_param_expr(0)) ||
            OB_ISNULL(param_expr2 = expr.get_param_expr(1)) ||
            OB_UNLIKELY(!param_expr2->is_const_expr())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()),
                                           K(expr.get_real_param_count()), K(expr));
        } else {
          result_type.set_blob();
          result_type.set_length(OB_MAX_LONGTEXT_LENGTH);
          result_type.set_collation_level(CS_LEVEL_IMPLICIT);
          if (param_expr1->is_enum_set_with_subschema()) {
            ObObjMeta org_obj_meta;
            if (OB_FAIL(ObRawExprUtils::extract_enum_set_collation(param_expr1->get_result_type(),
                                                                    my_session_,
                                                                    org_obj_meta))) {
              LOG_WARN("fail to extract enum set cs type", K(ret));
            } else {
              result_type.set_collation_type(org_obj_meta.get_collation_type());
            }
          } else {
            result_type.set_collation_type(param_expr1->get_result_type().get_collation_type());
          }
          if (lib::is_oracle_mode()) {
            const_cast<ObExprResType&>(param_expr2->get_result_type()).set_calc_type(ObNumberType);
          } else {
            const_cast<ObExprResType&>(param_expr2->get_result_type()).set_calc_type(ObIntType);
          }
          expr.set_result_type(result_type);
        }
        break;
      }
      case T_FUN_MAX:
      case T_FUN_MIN: {
        ret = set_agg_min_max_result_type(expr, result_type, need_add_cast);
        break;
      }
      default: {
        expr.set_result_type(expr.get_param_expr(0)->get_result_type());
        expr.unset_result_flag(NOT_NULL_FLAG);
        expr.unset_result_flag(ZEROFILL_FLAG);
      }
    }
    LOG_DEBUG("aggregate function deduced result type", K(result_type), K(need_add_cast), K(expr));
    if (OB_SUCC(ret) && need_add_cast) {
      if (override_calc_meta) {
        result_type.set_calc_type(result_type.get_type());
        result_type.set_calc_accuracy(result_type.get_accuracy());
        result_type.set_calc_meta(result_type.get_obj_meta());
      }
      if (T_FUN_AVG == expr.get_expr_type() && -2 != scale_increment_recover) {
        result_type.set_calc_scale(scale_increment_recover);
      }
      ObObjType child_type = expr.get_param_expr(0)->get_result_type().get_type();
      if (T_FUN_SUM == expr.get_expr_type() && result_type.get_calc_meta().is_decimal_int()
          && (!ob_is_integer_type(child_type) && !ob_is_decimal_int(child_type))) {
        // set calc precision as child precision for types other than integers
        ObPrecision child_prec = expr.get_param_expr(0)->get_result_type().get_precision();
        if (child_prec == PRECISION_UNKNOWN_YET) {
          // unknown precision, use default precision
          child_prec = ObAccuracy::DDL_DEFAULT_ACCURACY2[lib::is_oracle_mode()][child_type].get_precision();
        }
        result_type.set_calc_precision(child_prec);
      }
      expr.set_result_type(result_type);
      ObCastMode def_cast_mode = CM_NONE;
      if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_,
                                                    def_cast_mode))) {
        LOG_WARN("get_default_cast_mode failed", K(ret));
      } else if (OB_FAIL(add_implicit_cast(expr, def_cast_mode))) {
        LOG_WARN("add_implicit_cast failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::add_group_aggr_implicit_cast(ObAggFunRawExpr &expr,
                                                      const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.get_real_param_count() != expr.get_order_items().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(expr.get_real_param_count()), K(ret),
                                     K(expr.get_order_items().count()));
  } else {
    ObIArray<ObRawExpr*> &real_param_exprs = expr.get_real_param_exprs_for_update();
    for (int64_t i = 0; OB_SUCC(ret) && i < real_param_exprs.count(); ++i) {
      ObRawExpr *parent = expr.get_order_items().at(i).expr_;
      ObRawExpr *&child_ptr = real_param_exprs.at(i);
      if (OB_ISNULL(parent) || OB_ISNULL(child_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(parent), K(child_ptr));
      } else {
        ObExprResType res_type = parent->get_result_type();
        res_type.set_calc_meta(res_type.get_obj_meta());
        res_type.set_calc_accuracy(res_type.get_accuracy());
        ObCastMode real_cast_mode = cast_mode;
        if ((child_ptr->get_result_type().is_number()
             || child_ptr->get_result_type().is_decimal_int())
            && res_type.is_decimal_int()) {
          // When the const data type is number/decimal_int and the input column is decimal_int,
          // need to cast the number to decimal_int and it should be one-sided cast.
          // for example, when c2 type is NUMBER(3, 0),
          // query `SELECT CUME_DIST(123.89) WITHIN GROUP (ORDER BY C2) FROM T1;`
          // should cast 123.89 to 123 to compare the less or equal result
          real_cast_mode |= ObExprBetween::get_const_cast_mode(T_OP_LE, true);
        }
        if (skip_cast_expr(*parent, i)) {
          // do nothing
        } else if (OB_FAIL(try_add_cast_expr(expr, i, res_type, real_cast_mode))) {
          LOG_WARN("try_add_cast_expr failed", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::add_median_percentile_implicit_cast(ObAggFunRawExpr &expr,
                                                             const ObCastMode& cast_mode,
                                                             const bool keep_type)
{
  int ret = OB_SUCCESS;
  UNUSED(keep_type);
  if (OB_UNLIKELY(1 != expr.get_real_param_count() ||
                  1 != expr.get_order_items().count())) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid number of arguments", K(ret),
                                            K(expr.get_real_param_count()),
                                            K(expr.get_order_items().count()));
  } else {
    ObExprResType res_type = expr.get_result_type();
    res_type.set_calc_meta(res_type.get_obj_meta());
    res_type.set_calc_accuracy(res_type.get_accuracy());
    ObExprResType res_number_type;
    res_number_type.set_number();
    res_number_type.set_scale(
        ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_scale());
    res_number_type.set_precision(
        ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_precision());
    res_number_type.set_calc_meta(res_number_type.get_obj_meta());
    res_number_type.set_calc_accuracy(res_number_type.get_accuracy());
    const int64_t cast_order_idx = expr.get_real_param_count();//order item expr pos
    const int64_t cast_param_idx = 0;
    if (!keep_type && OB_FAIL(try_add_cast_expr(expr, cast_order_idx, res_type, cast_mode))) {
      LOG_WARN("try_add_cast_expr failed", K(ret), K(expr), K(cast_order_idx), K(res_type));
    } else if (T_FUN_MEDIAN != expr.get_expr_type()) {//percentile param
      if (OB_FAIL(try_add_cast_expr(expr, cast_param_idx, res_number_type, cast_mode))) {
        LOG_WARN("try_add_cast_expr failed", K(ret), K(expr),
                                             K(cast_param_idx), K(res_number_type));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObRawExprDeduceType::check_median_percentile_param(ObAggFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  const ObItemType expr_type = expr.get_expr_type();
  const int64_t real_param_count = expr.get_real_param_count();
  const int64_t order_count = expr.get_order_items().count();
  if (OB_UNLIKELY(1 != order_count
                  || 1 != real_param_count)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid number of arguments", K(ret), K(real_param_count), K(order_count));
  } else if (OB_ISNULL(expr.get_param_expr(0)) ||
             OB_ISNULL(expr.get_order_items().at(0).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (T_FUN_GROUP_PERCENTILE_CONT == expr_type ||
             T_FUN_GROUP_PERCENTILE_DISC == expr_type) {
    if (expr.get_param_expr(0)->get_result_type().is_user_defined_sql_type()) {
      ret = OB_ERR_INVALID_XML_DATATYPE;
      LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "NUMBER", "ANYDATA");
    } else if (!expr.get_param_expr(0)->is_const_expr()) {
      ret = OB_ERR_ARGUMENT_SHOULD_CONSTANT;
      LOG_WARN("Argument should be a constant.", K(ret));
    } else if (!ob_is_numeric_type(expr.get_param_expr(0)->get_result_type().get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(expr), K(expr));
    }
  }
  return ret;
}

/*
 * check group aggregate param whether is valid.
 */
int ObRawExprDeduceType::check_group_aggr_param(ObAggFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    ObRawExpr *param_expr = NULL;
    if (OB_ISNULL(param_expr = expr.get_param_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get param expr failed", K(i));
    } else if (T_FUN_GROUP_CONCAT != expr.get_expr_type()
               && T_FUN_COUNT != expr.get_expr_type()
               && T_FUN_APPROX_COUNT_DISTINCT != expr.get_expr_type()
               && T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS != expr.get_expr_type()
               && T_FUN_TOP_FRE_HIST != expr.get_expr_type()
               && T_FUN_HYBRID_HIST != expr.get_expr_type()
               && T_FUN_SUM_OPNSIZE != expr.get_expr_type()
               && 1 != get_expr_output_column(*param_expr)) {
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
    } else if (OB_UNLIKELY(
                is_oracle_mode()
                && ((ObLongTextType == param_expr->get_data_type()
                        || ob_is_lob_locator(param_expr->get_data_type())
                        || ob_is_json(param_expr->get_data_type())
                        || ob_is_xml_pl_type(param_expr->get_data_type(), param_expr->get_udt_id())
                        || ob_is_user_defined_sql_type(param_expr->get_data_type()))
                    && (T_FUN_ORA_JSON_OBJECTAGG != expr.get_expr_type()
                        && T_FUN_ORA_JSON_ARRAYAGG != expr.get_expr_type()
                        && T_FUN_ORA_XMLAGG != expr.get_expr_type()
                        && T_FUN_GROUP_CUME_DIST != expr.get_expr_type()
                        && T_FUN_GROUP_DENSE_RANK != expr.get_expr_type()
                        && T_FUN_GROUP_CONCAT != expr.get_expr_type()
                        && T_FUN_GROUP_PERCENT_RANK != expr.get_expr_type()
                        && T_FUN_GROUP_RANK != expr.get_expr_type()))
                && !(ob_is_user_defined_sql_type(param_expr->get_data_type())
                      && (T_FUN_APPROX_COUNT_DISTINCT == expr.get_expr_type() || T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == expr.get_expr_type()))
                && !(T_FUN_COUNT == expr.get_expr_type() && ob_is_json(param_expr->get_data_type()))
                && !(T_FUN_COUNT == expr.get_expr_type() && (ob_is_user_defined_sql_type(param_expr->get_data_type()) ||
                                                             ob_is_user_defined_pl_type(param_expr->get_data_type())))
                && T_FUN_MEDIAN != expr.get_expr_type()
                && T_FUN_GROUP_PERCENTILE_CONT != expr.get_expr_type()
                && T_FUN_GROUP_PERCENTILE_DISC != expr.get_expr_type()
                && T_FUN_SUM_OPNSIZE != expr.get_expr_type()
                && !expr.is_need_deserialize_row()
                && !(T_FUN_PL_AGG_UDF == expr.get_expr_type() && !expr.is_param_distinct())
                && !(T_FUN_WM_CONCAT == expr.get_expr_type() && !expr.is_param_distinct()))) {
      if (ob_is_json(param_expr->get_data_type())
          && !(expr.get_expr_type() == T_FUN_SUM
               || expr.get_expr_type() == T_FUN_AVG)) {
          ret = OB_ERR_INVALID_CMP_OP;
          LOG_WARN("lob or json type parameter not expected", K(ret));
      } else if ((ob_is_user_defined_sql_type(param_expr->get_data_type())
                    || ob_is_user_defined_pl_type(param_expr->get_data_type()))
                 && (expr.get_expr_type() == T_FUN_MAX
                     || expr.get_expr_type() == T_FUN_MIN
                     || expr.get_expr_type() == T_FUN_GROUPING)) {
        // other udt types not run here, xmltype does not have order or map member function for compare
        ret = OB_ERR_NO_ORDER_MAP_SQL;
        LOG_WARN("does not have order or map member function for compare",
          K(ret), K(param_expr->get_subschema_id()));
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("lob or json type parameter not expected", K(ret), K(expr));
      }
    } else if (ob_is_collection_sql_type(param_expr->get_data_type())
               && (T_FUN_SUM == expr.get_expr_type()
                   || T_FUN_AVG == expr.get_expr_type()
                   || T_FUN_COUNT == expr.get_expr_type())
               && expr.is_param_distinct()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "vector aggregation with distinct is");
    }
  }
  return ret;
}

/*@brief,ObRawExprDeduceType::check_group_rank_aggr_param检查rank、dense_rank、percent_rank、
 * cume_dist等聚合函数参数的有效:
 *  1.aggr参数需要与order by item的一一对应,eg：
 *    select rank(1,2) within group(order by c1, c2) from t1; ==> (v)
 *    select rank(1,2) within group(order by c1) from t1; ==> (x)
 *    select rank(2) within group(order by c1,c2) from t1; ==> (x)
 *  2.aggr参数为常量表达式，eg:
 *    select rank(c1) within group(order by c1,c2) from t1; ==> (x)
 */
int ObRawExprDeduceType::check_group_rank_aggr_param(ObAggFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.get_real_param_count() != expr.get_order_items().count()) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid number of arguments", K(ret), K(expr.get_real_param_count()),
                                            K(expr.get_order_items().count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_real_param_count(); ++i) {
      const ObRawExpr *param_expr = expr.get_param_expr(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(param_expr));
      } else if (!param_expr->is_const_expr()) {
        ret = OB_ERR_ARGUMENT_SHOULD_CONSTANT;
        LOG_WARN("Argument should be a constant.", K(ret));
      } else {
        /*do nothing*/
      }
    }
  }
  return ret;
}


int ObRawExprDeduceType::deduce_type_visit_for_special_func(int64_t param_index,
                                                            const ObRawExpr &expr,
                                                            ObIExprResTypes &types)
{
  int ret = OB_SUCCESS;
  ObExprResType dest_type(alloc_);
  const int CONV_PARAM_NUM = 6;
  if (OB_UNLIKELY(param_index < 0)
      || OB_UNLIKELY(param_index >= CONV_PARAM_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(param_index));
  } else if (OB_UNLIKELY(CONV_PARAM_NUM - 2 == param_index)
            || OB_UNLIKELY(CONV_PARAM_NUM - 1 == param_index)) {
    dest_type = expr.get_result_type();
    //ignore the last param of column_conv
  } else if (OB_UNLIKELY(!expr.is_const_raw_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column conv function other params are const expr", K(expr), K(param_index));
  } else {
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(&expr);
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
        dest_type.set_result_flag(NOT_NULL_FLAG);
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

int ObRawExprDeduceType::adjust_cast_as_signed_unsigned(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *param_expr1 = NULL;
  ObRawExpr *param_expr2 = NULL;
  if (OB_UNLIKELY(T_FUN_SYS_CAST != expr.get_expr_type())
      || OB_UNLIKELY(2 != expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cast expr", K(ret));
  } else if (expr.has_flag(IS_INNER_ADDED_EXPR)) {
    /*do nothing*/
  } else if (OB_ISNULL(param_expr1 = expr.get_param_expr(0)) ||
             OB_ISNULL(param_expr2 = expr.get_param_expr(1)) ||
             OB_UNLIKELY(!param_expr2->is_const_raw_expr())) {
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

int ObRawExprDeduceType::visit(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObExprOperator *op = expr.get_op();
  if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (NULL == op) {
    if (T_RB_ITERATE_EXPRESSION == expr.get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "rb_iterate usage");
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Get expression operator failed", "expr type", expr.get_expr_type());
    }
  } else if (T_FUN_SYS_CAST == expr.get_expr_type() &&
             OB_FAIL(adjust_cast_as_signed_unsigned(expr))) {
    LOG_WARN("failed to adjust cast as signed unsigned", K(ret), K(expr));
  } else {
    ObExprResTypes types;
    ObCastMode expr_cast_mode = CM_NONE;
    bool is_default_col = false;
    if (T_FUN_SYS_DEFAULT == expr.get_expr_type()) {
      if (OB_ISNULL(expr.get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(expr));
      } else {
        is_default_col = expr.get_param_expr(0)->is_column_ref_expr();
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); i++) {
      ObRawExpr *param_expr = expr.get_param_expr(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(param_expr));
      } else if (!expr.is_calc_part_expr() &&
                 !param_expr->is_multiset_expr() &&
                 get_expr_output_column(*param_expr) != 1) {
        //函数的每个参数的值都应该是标量，包括子查询的结果作为参数,不能是row or table
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      } else if (T_FUN_COLUMN_CONV == expr.get_expr_type()
                || (T_FUN_SYS_DEFAULT == expr.get_expr_type() && !is_default_col)) {
        //column_conv(type, collation_type, accuracy_expr, nullable, value)
        //前面四个参数都要特殊处理
        if (OB_FAIL(deduce_type_visit_for_special_func(i, *param_expr, types))) {
          LOG_WARN("fail to visit for column_conv", K(ret), K(i));
        }
      } else if (lib::is_oracle_mode() && !expr.is_pl_expr() && expr.is_called_in_sql()
        && T_FUN_SYS_CAST != expr.get_expr_type() && param_expr->get_expr_type() != T_FUN_SYS_CAST
        && param_expr->get_result_type().get_type() == ObExtendType
        && ObObjUDTUtil::ob_is_supported_sql_udt(param_expr->get_result_type().get_udt_id())) {
        if (OB_FAIL(ObRawExprUtils::implict_cast_pl_udt_to_sql_udt(expr_factory_, my_session_, param_expr))) {
          LOG_WARN("add implict cast to pl udt expr failed", K(ret));
        } else if (OB_FAIL(types.push_back(param_expr->get_result_type()))) {
          LOG_WARN("push back param type failed", K(ret));
        }
      } else {
        if (OB_FAIL(push_back_types(param_expr, types))) {
          LOG_WARN("push back param type failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(calc_result_type(expr, types, expr_cast_mode,
                                   ObExprOperator::NOT_ROW_DIMENSION))) {
        LOG_WARN("fail to calc result type", K(ret), K(types));
      }
    }
    if (OB_SUCC(ret) && T_FUN_SYS_ANY_VALUE == expr.get_expr_type()) {
      ObRawExpr *first_param = NULL;
      if (OB_ISNULL(first_param = expr.get_param_expr(0))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr", K(expr), K(ret));
      } else if (ob_is_enumset_tc(first_param->get_data_type())) {
        const ObIArray<ObString> &enum_set_values = first_param->get_enum_set_values();
        if (OB_UNLIKELY(enum_set_values.count() < 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ref_expr", K(first_param), K(ret));
        } else if (OB_FAIL(expr.set_enum_set_values(enum_set_values))) {
          LOG_WARN("failed to set enum_set_values", K(enum_set_values), K(expr), K(ret));
        } else {/*do nothing*/}
      }
      expr.unset_result_flag(ZEROFILL_FLAG);
    }
    if (OB_SUCC(ret) && T_FUN_SYS_FROM_UNIX_TIME == expr.get_expr_type()
        && expr.get_param_count() == 2) {
      if (!expr.get_param_expr(1)->get_result_type().is_string_type()
          && !expr.get_param_expr(1)->get_result_type().is_enum_or_set()) {
        expr.set_extra(1);
      }
    }
    if (OB_SUCC(ret) && ob_is_enumset_tc(expr.get_data_type())
        && (T_FUN_SYS_NULLIF == expr.get_expr_type() || T_FUN_SYS_VALUES == expr.get_expr_type() )) {
      ObRawExpr *first_param = NULL;
      if (OB_ISNULL(first_param = expr.get_param_expr(0))
          || !(ob_is_enumset_tc(first_param->get_data_type()))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr", KPC(first_param), K(expr), K(ret));
      } else {
        const ObIArray<ObString> &enum_set_values = first_param->get_enum_set_values();
        if (OB_UNLIKELY(enum_set_values.count() < 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ref_expr", K(first_param), K(ret));
        } else if (OB_FAIL(expr.set_enum_set_values(enum_set_values))) {
          LOG_WARN("failed to set enum_set_values", K(enum_set_values), K(expr), K(ret));
        } else {/*do nothing*/}
      }
    }
    // There are some exprs such as nullif, column convert, etc. that require build subschema for
    // enum or set types
    if (OB_SUCC(ret) && ob_is_enumset_tc(expr.get_data_type())) {
      const ObItemType expr_type = expr.get_expr_type();
      if (T_FUN_SYS_REMOVE_CONST == expr_type ||
           T_FUN_SYS_CAST == expr_type ||
           T_FUN_SYS_WRAPPER_INNER == expr_type ||
           T_FUN_SYS_DEFAULT == expr_type) {
        // skip some inner added expr, their result type meta should be determined by the args.
      } else if (OB_FAIL(build_subschema_for_enum_set_type(expr))) {
        LOG_WARN("fail to build subschema for enum set type", K(ret), K(expr));
      }
    }
    CK(OB_NOT_NULL(my_session_));
    if (OB_SUCC(ret)) {
      // Casting from bit to binary depends on this flag to be compatible with MySQL,
      // see bit_string in ob_datum_cast.cpp.
      if (expr.get_expr_type() == T_FUN_PAD) {
        expr_cast_mode = expr_cast_mode | CM_COLUMN_CONVERT;
      }
      if (OB_FAIL(add_implicit_cast(expr, expr_cast_mode))) {
        LOG_WARN("add_implicit_cast failed", K(ret));
      }
    }
    //add local vars to expr
    if (OB_SUCC(ret)) {
      if (solidify_session_vars_) {
        if (OB_FAIL(expr.set_local_session_vars(NULL, my_session_, local_vars_id_))) {
          LOG_WARN("fail to set session vars", K(ret), K(expr));
        }
      } else if (NULL != my_local_vars_) {
        if (OB_FAIL(expr.set_local_session_vars(my_local_vars_, NULL, local_vars_id_))) {
          LOG_WARN("fail to set local vars", K(ret), K(expr));
        }
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObSetOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (ob_is_collection_sql_type(expr.get_result_type().get_type())
      && !ObObjUDTUtil::ob_is_supported_sql_udt(expr.get_result_type().get_udt_id())) {
    if (OB_FAIL(construct_collecton_attr_expr(expr))) {
      LOG_WARN("failed to construct collection attr expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObAliasRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *ref_expr = expr.get_ref_expr();
  if (OB_ISNULL(ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref expr is null");
  } else {
    expr.set_result_type(ref_expr->get_result_type());
    if (ob_is_enum_or_set_type(ref_expr->get_data_type())) {
      const ObIArray<ObString> &enum_set_values = ref_expr->get_enum_set_values();
      if (OB_UNLIKELY(enum_set_values.count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ref_expr", KPC(ref_expr), K(ret));
      } else if (OB_FAIL(expr.set_enum_set_values(enum_set_values))) {
        LOG_WARN("failed to set enum_set_values", K(expr), K(ret));
      } else {}
    }
  }
  return ret;
}

int ObRawExprDeduceType::get_row_expr_param_type(const ObRawExpr &expr, ObIExprResTypes &types)
{
  int ret = OB_SUCCESS;
  ObExprResType result_type(alloc_);
  if (OB_UNLIKELY(expr.get_expr_type() != T_OP_ROW)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is not row", K(expr));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < expr.get_param_count(); ++j) {
    const ObRawExpr *row_param = expr.get_param_expr(j);
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

int ObRawExprDeduceType::visit(ObWinFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObExprResType result_number_type;
  result_number_type.set_accuracy(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][ObNumberType]);
  result_number_type.set_calc_accuracy(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][ObNumberType]);
  result_number_type.set_number();

  common::ObIArray<ObRawExpr *> &func_params = expr.get_func_params();
  ObExprTypeCtx type_ctx;
  ObSQLUtils::init_type_ctx(my_session_, type_ctx);
  if (func_params.count() <= 0) {
    if (NULL == expr.get_agg_expr()) {
      ObExprResType result_type(alloc_);
      // @TODO : nijia.nj, 细分各种window_funciton
      if (T_WIN_FUN_CUME_DIST == expr.get_func_type() ||
          T_WIN_FUN_PERCENT_RANK == expr.get_func_type()) {
        const uint64_t ob_version = GET_MIN_CLUSTER_VERSION();
        if (is_oracle_mode() ||
            !((ob_version >= CLUSTER_VERSION_2277 && ob_version < CLUSTER_VERSION_3000)
              || (ob_version >= CLUSTER_VERSION_312 && ob_version < CLUSTER_VERSION_3200)
              || ob_version >= CLUSTER_VERSION_3_2_3_0)) {
          result_type.set_accuracy(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObNumberType]);
          result_type.set_calc_accuracy(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObNumberType]);
          result_type.set_number();
        } else {
          result_type.set_accuracy(ObAccuracy::DML_DEFAULT_ACCURACY[ObDoubleType]);
          result_type.set_calc_accuracy(ObAccuracy::MAX_ACCURACY2[MYSQL_MODE][ObDoubleType]);
          result_type.set_double();
          result_type.set_result_flag(NOT_NULL_FLAG);
        }
      } else if (T_WIN_FUN_DENSE_RANK == expr.get_func_type() ||
                  T_WIN_FUN_RANK == expr.get_func_type() ||
                  T_WIN_FUN_ROW_NUMBER == expr.get_func_type()) {
        if (is_oracle_mode()) {
          result_type.set_number();
          result_type.set_scale(0);
          result_type.set_precision(OB_MAX_NUMBER_PRECISION);
        } else {
          result_type.set_uint64();
          result_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObUInt64Type]);
          result_type.set_result_flag(NOT_NULL_FLAG);
        }
      } else if (is_oracle_mode()) {
        result_type.set_number();
        result_type.set_scale(0);
        result_type.set_precision(OB_MAX_NUMBER_PRECISION);
      } else {
        result_type.set_int();
        result_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
      }
      expr.set_result_type(result_type);
    } else if (OB_FAIL(expr.get_agg_expr()->deduce_type(my_session_))) {
      LOG_WARN("deduce type failed", K(ret));
    } else {
      expr.set_result_type(expr.get_agg_expr()->get_result_type());
      if (expr.get_result_type().is_collection_sql_type()) {
        if (OB_FAIL(expr.set_enum_set_values(expr.get_agg_expr()->get_enum_set_values()))) {
          LOG_WARN("failed to set_enum_set_values", K(ret));
        }
      }
    }
  //here pl_agg_udf_expr_ in win_expr must be null, defensive check!!!
  } else if (OB_UNLIKELY(expr.get_pl_agg_udf_expr() != NULL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (OB_ISNULL(func_params.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func param is null", K(ret));
  } else if (T_WIN_FUN_NTILE == expr.get_func_type()) {
    ObExprResType result_type(alloc_);
    if (is_oracle_mode()) {
      result_type.set_scale(0);
      result_type.set_precision(OB_MAX_NUMBER_PRECISION);
      result_type.set_number();
    } else {
      result_type.set_int();
      result_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
    }
    expr.set_result_type(result_type);
    if (is_oracle_mode()
        && !func_params.at(0)->get_result_type().is_numeric_type()) {
      ObSysFunRawExpr *cast_expr = NULL;
      if (OB_ISNULL(expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null pointer", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*expr_factory_,
                                                          func_params.at(0),
                                                          result_number_type,
                                                          cast_expr,
                                                          my_session_))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else {
        func_params.at(0) = cast_expr;
      }
    } else if (OB_UNLIKELY(lib::is_mysql_mode() &&
                           (!func_params.at(0)->is_const_expr() ||
                            !func_params.at(0)->get_result_type().is_integer_type()))) {
      // nile(N), N cannot be NULL, and must be an integer in the range 0 to 2^63, inclusive, in any of the following forms:
      // - an unsigned integer constant literal
      // - a positional parameter marker (?) (in ps protocol)
      // - a user-defined variable
      // - a local variable in a stored routine
      if (func_params.at(0)->get_expr_type() == T_OP_GET_SYS_VAR
          && func_params.at(0)->get_result_type().is_integer_type()) {
        // do nothing
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Incorrect arguments to ntile", K(ret), KPC(func_params.at(0)));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ntile");
      }
    }
  } else if (T_WIN_FUN_NTH_VALUE == expr.get_func_type()) {
    // nth_value函数的返回类型可以为null. lead和lag也是
    // bug:
    expr.set_result_type(func_params.at(0)->get_result_type());
    expr.set_enum_set_values(func_params.at(0)->get_enum_set_values());
    expr.unset_result_flag(NOT_NULL_FLAG);
    if (!func_params.at(1)->get_result_type().is_numeric_type()) {
      ObSysFunRawExpr *cast_expr = NULL;
      if (OB_ISNULL(expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null pointer", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*expr_factory_,
                                                          func_params.at(1),
                                                          result_number_type,
                                                          cast_expr,
                                                          my_session_))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else {
        func_params.at(1) = cast_expr;
      }
    }
  } else if (T_WIN_FUN_LEAD == expr.get_func_type()
             || T_WIN_FUN_LAG == expr.get_func_type()) {
    if (is_mysql_mode() && func_params.count() == 3) { //compatiable with mysql
      ObExprResType res_type;
      ObSEArray<ObExprResType, 2> types;
      ObCollationType coll_type = CS_TYPE_INVALID;
      if (OB_FAIL(types.push_back(func_params.at(0)->get_result_type()))) {
        LOG_WARN("fail to push back type of the first param.",K(ret));
      } else if (OB_FAIL(types.push_back(func_params.at(2)->get_result_type()))) {
        LOG_WARN("fail to push back type of the third param.",K(ret));
      } else if (OB_FAIL(my_session_->get_collation_connection(coll_type))) {
        LOG_WARN("fail to get_collation_connection", K(ret));
      } else if (OB_FAIL(ObExprOperator::aggregate_result_type_for_merge(res_type,
                                                                  &types.at(0),
                                                                  types.count(),
                                                                  false,
                                                                  type_ctx))) {
        LOG_WARN("fail to aggregate_result_type_for_merge", K(ret), K(types));
      } else {
        if (res_type.is_json()) {
          ObExprResType merged_type = func_params.at(0)->get_result_type();
          if (merged_type.is_json()) {
            merged_type = func_params.at(2)->get_result_type();
          } else {}
          if (merged_type.get_type() >= ObTinyIntType &&
              merged_type.get_type() <= ObHexStringType) {
            res_type.set_varchar();
          } else if (merged_type.is_blob()) {
            res_type.set_blob();
          } else {
            // json or max, do nothing
          }
        } else if (ob_is_real_type(res_type.get_type())) {
          res_type.set_double();
        } else {}
        ObCastMode def_cast_mode = CM_NONE;
        ObRawExpr *cast_expr = NULL;
        if (!func_params.at(0)->get_result_type().has_result_flag(NOT_NULL_FLAG) ||
            !func_params.at(2)->get_result_type().has_result_flag(NOT_NULL_FLAG)) {
          res_type.unset_result_flag(NOT_NULL_FLAG);
        }
        res_type.set_calc_meta(res_type.get_obj_meta());
        res_type.set_calc_accuracy(res_type.get_accuracy());
        if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_, def_cast_mode))) {
          LOG_WARN("get_default_cast_mode failed", K(ret));
        } else if (OB_FAIL(try_add_cast_expr_above_for_deduce_type(*func_params.at(0), cast_expr, res_type, def_cast_mode))) {
          LOG_WARN("failed to create raw expr.", K(ret));
        } else {
          func_params.at(0) = cast_expr;
          expr.set_result_type(res_type);
          expr.set_enum_set_values(func_params.at(0)->get_enum_set_values());
        }
      }
    } else {
      ObExprResType res_type = func_params.at(0)->get_result_type();
      res_type.unset_result_flag(NOT_NULL_FLAG);
      //set calc type for explain stmts and cases that the param0 is paramlized
      res_type.set_calc_meta(res_type.get_obj_meta());
      res_type.set_calc_accuracy(res_type.get_accuracy());
      expr.set_result_type(res_type);
      expr.set_enum_set_values(func_params.at(0)->get_enum_set_values());
    }
    // lead和lag函数的第三个参数，应当转换为第一个参数的类型，加cast，这里不能在执行层转。
    // bug:
    if (OB_SUCC(ret) && func_params.count() == 3) {
      ObRawExpr *cast_expr = NULL;
      ObCastMode def_cast_mode = CM_NONE;
      if (OB_ISNULL(expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null pointer", K(ret));
      } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_, def_cast_mode))) {
          LOG_WARN("get_default_cast_mode failed", K(ret));
      } else if (OB_FAIL(try_add_cast_expr_above_for_deduce_type(*func_params.at(2), cast_expr, expr.get_result_type(), def_cast_mode))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else {
        func_params.at(2) = cast_expr;
      }
    }
    if (OB_SUCC(ret) && func_params.count() >= 2
        && !func_params.at(1)->get_result_type().is_numeric_type()) {
      ObSysFunRawExpr *cast_expr = NULL;
      if (OB_ISNULL(expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null pointer", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*expr_factory_,
                                                          func_params.at(1),
                                                          result_number_type,
                                                          cast_expr,
                                                          my_session_))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else {
        func_params.at(1) = cast_expr;
      }
    }
  } else {
    expr.set_result_type(func_params.at(0)->get_result_type());
    expr.set_enum_set_values(func_params.at(0)->get_enum_set_values());
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCC(ret)
        && expr.lower_.is_nmb_literal_
        && expr.lower_.interval_expr_ != NULL
        && !(expr.lower_.interval_expr_->get_result_type().is_numeric_type()
             || expr.lower_.interval_expr_->get_result_type().is_interval_type())) {// cast interval to number is forbidden, just do
      if (is_oracle_mode()) {
        ObSysFunRawExpr *cast_expr = NULL;
        if (OB_ISNULL(expr_factory_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null pointer", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*expr_factory_,
                                                            expr.lower_.interval_expr_,
                                                            result_number_type,
                                                            cast_expr,
                                                            my_session_))) {
          LOG_WARN("failed to create raw expr.", K(ret));
        } else {
          expr.lower_.interval_expr_ = cast_expr;
        }
      } else {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("interval is not numberic", K(ret), KPC(expr.lower_.interval_expr_));
      }
    }
    if (OB_SUCC(ret)
        && expr.upper_.is_nmb_literal_
        && expr.upper_.interval_expr_ != NULL
        && !(expr.upper_.interval_expr_->get_result_type().is_numeric_type()
             || expr.upper_.interval_expr_->get_result_type().is_interval_type())) {
      if (is_oracle_mode()) {
        ObSysFunRawExpr *cast_expr = NULL;
        if (OB_ISNULL(expr_factory_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null pointer", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*expr_factory_,
                                                            expr.upper_.interval_expr_,
                                                            result_number_type,
                                                            cast_expr,
                                                            my_session_))) {
          LOG_WARN("failed to create raw expr.", K(ret));
        } else {
          expr.upper_.interval_expr_ = cast_expr;
        }
      } else {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("interval is not numberic", K(ret), KPC(expr.lower_.interval_expr_));
      }
    }
    if (OB_SUCC(ret) &&
        lib::is_mysql_mode() &&
        expr.get_window_type() == WINDOW_RANGE &&
        (expr.upper_.interval_expr_ != NULL || expr.lower_.interval_expr_ != NULL)) {
      if (expr.get_order_items().empty()) {
        //do nothing
      } else if (OB_UNLIKELY(expr.get_order_items().count() != 1)) {
        ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
        LOG_WARN("invalid window specification", K(ret), K(expr.get_order_items()));
      } else if (OB_UNLIKELY(((expr.upper_.interval_expr_ != NULL && !expr.upper_.is_nmb_literal_) ||
                              (expr.lower_.interval_expr_ != NULL && !expr.lower_.is_nmb_literal_)) &&
                              expr.get_order_items().at(0).expr_->get_result_type().is_numeric_type())) {
        ret = OB_ERR_WINDOW_RANGE_FRAME_NUMERIC_TYPE;
        LOG_WARN("Window with RANGE frame has ORDER BY expression of numeric type. INTERVAL bound value not allowed.", K(ret));
        ObString tmp_name = expr.get_win_name().empty() ? ObString("<unnamed window>") : expr.get_win_name();
        LOG_USER_ERROR(OB_ERR_WINDOW_RANGE_FRAME_NUMERIC_TYPE, tmp_name.length(), tmp_name.ptr());
      } else if (OB_UNLIKELY(((expr.upper_.interval_expr_ != NULL && expr.upper_.is_nmb_literal_) ||
                              (expr.lower_.interval_expr_ != NULL && expr.lower_.is_nmb_literal_)) &&
                              expr.get_order_items().at(0).expr_->get_result_type().is_temporal_type())) {
        ret = OB_ERR_WINDOW_RANGE_FRAME_TEMPORAL_TYPE;
        LOG_WARN("Window with RANGE frame has ORDER BY expression of datetime type. Only INTERVAL bound value allowed.", K(ret));
        ObString tmp_name = expr.get_win_name().empty() ? ObString("<unnamed window>") : expr.get_win_name();
        LOG_USER_ERROR(OB_ERR_WINDOW_RANGE_FRAME_TEMPORAL_TYPE, tmp_name.length(), tmp_name.ptr());
      }
    }
    LOG_DEBUG("finish add cast for window function", K(result_number_type), K(expr.lower_), K(expr.upper_));
  }

  if (OB_FAIL(ret) || OB_UNLIKELY(expr.win_type_ != WINDOW_RANGE)
      || OB_UNLIKELY(BOUND_INTERVAL != expr.upper_.type_ && BOUND_INTERVAL != expr.lower_.type_)) {
        //do nothing.
  } else if (expr.get_order_items().empty()) { 
  } else if (OB_UNLIKELY(1 != expr.get_order_items().count())) {
    ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
    LOG_WARN("invalid window specification", K(ret), K(expr.get_order_items().count()));
  } else if (OB_ISNULL(expr.get_order_items().at(0).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("order by expr should not be null!", K(ret));
  } else {
    //检查frame是range时数据类型的有效性
    ObRawExpr *bound_expr_arr[2] = {expr.upper_.interval_expr_, expr.lower_.interval_expr_};
    ObRawExpr *order_expr = expr.get_order_items().at(0).expr_;
    const ObObjType &order_res_type = order_expr->get_data_type();
    const ObItemType &item_type = order_expr->get_expr_type();
    if (lib::is_mysql_mode() && item_type == T_INT) {
      ret = OB_ERR_WINDOW_ILLEGAL_ORDER_BY;
      LOG_WARN("int not expected in window function's orderby ", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++ i) {
      if (OB_ISNULL(bound_expr_arr[i])) {
        /*do nothing*/
      } else if (lib::is_oracle_mode()) {
        if (ob_is_numeric_type(bound_expr_arr[i]->get_data_type())
            || ob_is_string_tc(bound_expr_arr[i]->get_data_type())
            || ob_is_interval_tc(bound_expr_arr[i]->get_data_type())) {
         if (ob_is_otimestampe_tc(order_res_type)) {
            if (!ob_is_interval_tc(bound_expr_arr[i]->get_data_type())) {
              ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
              LOG_WARN("invalid datatype in order by for range clause", K(ret), K(order_res_type));
            }
          } else  if (!ob_is_numeric_type(order_res_type) && !ob_is_datetime_tc(order_res_type)
              && !ob_is_date_tc(order_res_type)) {
            ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
            LOG_WARN("invalid datatype in order by for range clause", K(ret), K(order_res_type));
          }
        } else {
          //to do: 支持interval后这里要处理interval的情况
          ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
          LOG_WARN("invalid datatype in order by", K(i), K(bound_expr_arr[i]->get_data_type()),
                                                   K(ret), K(order_res_type));
        }
      } else {//mysql mode
        if (ob_is_numeric_type(order_res_type) || ob_is_temporal_type(order_res_type)
            || ob_is_otimestampe_tc(order_res_type) || ob_is_datetime_tc(order_res_type)) {
          /*do nothing*/
        } else {
          ret = OB_ERR_WINDOW_RANGE_FRAME_ORDER_TYPE;
          LOG_WARN("RANGE N PRECEDING/FOLLOWING frame order by type miss match", K(ret), K(order_res_type));
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool is_asc = expr.get_order_items().at(0).is_ascending();
      ObRawExpr *&upper_raw_expr = (expr.upper_.is_preceding_ ^ is_asc)
                                  ? expr.upper_.exprs_[0] : expr.upper_.exprs_[1];
      ObRawExpr *&lower_raw_expr = (expr.lower_.is_preceding_ ^ is_asc)
                                  ? expr.lower_.exprs_[0] : expr.lower_.exprs_[1];
      bool need_no_cast = false;
      ObExprResType result_type;
      ObSEArray<ObExprResType, 3> types;
      ObExprBetween dummy_op(expr_factory_->get_allocator());
      ObOpRawExpr dummy_raw_expr;
      dummy_raw_expr.set_expr_type(T_OP_BTW);
      dummy_op.set_raw_expr(&dummy_raw_expr);
      bool has_lower = (lower_raw_expr != NULL);
      if (OB_FAIL(types.push_back(order_expr->get_result_type()))) {
        LOG_WARN("fail to push_back", K(ret));
      } else if (OB_NOT_NULL(upper_raw_expr)
                 && OB_FAIL(types.push_back(upper_raw_expr->get_result_type()))) {
        LOG_WARN("fail to push_back", K(ret));
      } else if (OB_NOT_NULL(lower_raw_expr)
                 && OB_FAIL(types.push_back(lower_raw_expr->get_result_type()))) {
        LOG_WARN("fail to push_back", K(ret));
      } else if (OB_FAIL(dummy_op.get_cmp_result_type3(result_type, need_no_cast,
                                                       &types.at(0), types.count(), has_lower,
                                                       type_ctx))) {
        LOG_WARN("fail to get_cmp_result_type3", K(ret));
      } else {
        result_type.set_accuracy(result_type.get_calc_accuracy());
        result_type.set_meta(result_type.get_calc_meta());
      }
      ObRawExpr *cast_expr_upper = NULL;
      ObRawExpr *cast_expr_lower = NULL;
      ObRawExpr *cast_expr_order = NULL;
      ObCastMode def_cast_mode = CM_NONE;
      if (OB_FAIL(ret) || need_no_cast) {
        /*do nothing*/
      } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_, def_cast_mode))) {
        LOG_WARN("get_default_cast_mode failed", K(ret));
      } else if (OB_NOT_NULL(upper_raw_expr)
                 && OB_FAIL(try_add_cast_expr_above_for_deduce_type(*upper_raw_expr,
                                                                    cast_expr_upper,
                                                                    types[1],
                                                                    def_cast_mode))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else if (OB_NOT_NULL(lower_raw_expr)
                 && OB_FAIL(try_add_cast_expr_above_for_deduce_type(
                   *lower_raw_expr, cast_expr_lower, upper_raw_expr != NULL ? types[2] : types[1],
                   def_cast_mode))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else if (OB_FAIL(try_add_cast_expr_above_for_deduce_type(*order_expr, cast_expr_order,
                                                                 types[0], def_cast_mode))) {
        LOG_WARN("failed to create raw expr.", K(ret));
      } else {
        upper_raw_expr = cast_expr_upper;
        lower_raw_expr = cast_expr_lower;
        expr.get_order_items().at(0).expr_ = cast_expr_order;
      }
      LOG_DEBUG("finish add cast for window function", K(need_no_cast), K(result_type),
                                                       K(types), K(expr));
    }
  }
  if (OB_SUCC(ret) && lib::is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_partition_exprs().count(); i++) {
      ObRawExpr *param_expr = expr.get_partition_exprs().at(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param is null", K(ret));
      } else if (ob_is_lob_locator(param_expr->get_result_type().get_type())) {
        ObCastMode def_cast_mode = CM_NONE;
        ObExprResType param_type = param_expr->get_result_type();
        param_type.set_calc_meta(param_type.get_obj_meta());
        param_type.set_calc_accuracy(param_type.get_accuracy());
        param_type.set_calc_type(ObLongTextType);
        int64_t cast_param_idx = expr.get_partition_param_index(i);
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

int ObRawExprDeduceType::visit(ObPseudoColumnRawExpr &expr)
{
  //result type has been set in resolver
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprDeduceType::visit(ObUDFRawExpr &expr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); i++) {
    ObRawExpr *param_expr = expr.get_param_expr(i);
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(param_expr));
    } else if (get_expr_output_column(*param_expr) != 1) {
      ret = OB_ERR_INVALID_COLUMN_NUM;
		  LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
    }
  }
  return ret;
}

int ObRawExprDeduceType::visit(ObMatchFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObExprResType result_type(alloc_);
  result_type.set_double();
  expr.set_result_type(result_type);
  ObExprResType col_result_type;
  // cast search key if need
  if (OB_ISNULL(expr.get_search_key())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(expr.get_match_column_type(col_result_type))) {
    LOG_WARN("failed to get match column type", K(ret));
  } else if (expr.get_search_key()->get_result_type().get_type() != ObVarcharType ||
             col_result_type.get_collation_type() != expr.get_search_key()->get_result_type().get_collation_type()) {
    ObExprResType search_key_type = expr.get_search_key()->get_result_type();
    ObCastMode def_cast_mode = CM_NONE;
    search_key_type.set_varchar();
    search_key_type.set_length(OB_MAX_MYSQL_VARCHAR_LENGTH);
    search_key_type.set_collation_type(col_result_type.get_collation_type());
    search_key_type.set_collation_level(search_key_type.get_collation_level());
    search_key_type.set_calc_meta(search_key_type.get_obj_meta());
    if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_,
                                                  def_cast_mode))) {
      LOG_WARN("get_default_cast_mode failed", K(ret));
    } else if (OB_FAIL(try_add_cast_expr(expr, expr.get_search_key_idx(), search_key_type, def_cast_mode))) {
      LOG_WARN("add_implicit_cast failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprDeduceType::init_normal_udf_expr(ObNonTerminalRawExpr &expr, ObExprOperator *op)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(op);
  ObExprDllUdf *normal_udf_op = nullptr;
  ObNormalDllUdfRawExpr &fun_sys = static_cast<ObNormalDllUdfRawExpr &>(expr);
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

int ObRawExprDeduceType::set_agg_udf_result_type(ObAggFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr*> &param_exprs = expr.get_real_param_exprs_for_update();
  common::ObSEArray<common::ObString, 16> udf_attributes; /* udf's input args' name */
  common::ObSEArray<ObExprResType, 16> udf_attributes_types; /* udf's attribute type */
  common::ObSEArray<ObUdfConstArgs, 16> const_results; /* const input expr' result */
  ObAggUdfFunction udf_func;
  const share::schema::ObUDFMeta &udf_meta = expr.get_udf_meta();
  ObExprResType type;
  ObExprResTypes param_types;
  ARRAY_FOREACH_X(param_exprs, idx, cnt, OB_SUCC(ret)) {
    ObRawExpr *expr = param_exprs.at(idx);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the expr is null", K(ret));
    } else if (expr->is_column_ref_expr()) {
      //if the input expr is a column, we should set the column name as the expr name.
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(expr);
      const ObString &real_expr_name = col_expr->get_alias_column_name().empty() ? col_expr->get_column_name() : col_expr->get_alias_column_name();
      expr->set_expr_name(real_expr_name);
    } else if (expr->is_const_expr()) {
      //if the input expr is a const expr, we will set the result val to UDF_INIT's args.
      ObUdfConstArgs const_args;
      ObConstRawExpr *c_expr = static_cast<ObConstRawExpr*>(expr);
      ObObj &param_obj = c_expr->get_value();
      const_args.idx_in_udf_arg_ = idx;
      UNUSED(param_obj);
      //FIXME muhang
      //这里实在是不具备计算的条件，没有办法产生和计算物理表达式。
      //如果用户的init中强依赖于可计算表达式的结果，那么可能会在calc_udf_result_type
      //出错。
      if (OB_FAIL(const_results.push_back(const_args))) {
        LOG_WARN("failed to push back const args", K(ret));
      }
    }
    OZ(param_types.push_back(expr->get_result_type()));
    OX(param_types.at(param_types.count() - 1).set_calc_meta(
            param_types.at(param_types.count() - 1)));

    if (OB_SUCC(ret)) {
      if (OB_FAIL(udf_attributes.push_back(expr->get_expr_name()))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(udf_attributes_types.push_back(expr->get_result_type()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObExprTypeCtx type_ctx;
    type_ctx.set_raw_expr(&expr);
    ObSQLUtils::init_type_ctx(my_session_, type_ctx);
    if (OB_FAIL(udf_func.init(udf_meta))) {
      LOG_WARN("udf function init failed", K(ret));
    } else if (OB_FAIL(ObUdfUtil::calc_udf_result_type(
                alloc_, &udf_func, udf_meta,
                udf_attributes, udf_attributes_types,
                type,
                param_types.count() > 0 ? &param_types.at(0) : NULL,
                param_types.count(),
                type_ctx))) {
      LOG_WARN("failed to cale udf result type");
    } else {
      expr.set_result_type(type);
      ObCastMode cast_mode = CM_NONE;
      OZ(ObSQLUtils::get_default_cast_mode(false, 0, my_session_, cast_mode));
      for (int64_t idx = 0; OB_SUCC(ret) && idx < param_exprs.count(); idx++) {
        OZ(try_add_cast_expr(expr, idx, param_types.at(idx), cast_mode));
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::set_agg_group_concat_result_type(ObAggFunRawExpr &expr,
                                                          ObExprResType &result_type)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(my_session_));
  CK(OB_NOT_NULL(expr_factory_));
  ObArray<ObExprResType> types;
  expr.set_data_type(ObVarcharType);
  const ObIArray<ObRawExpr*> &real_parm_exprs = expr.get_real_param_exprs();
  ObExprTypeCtx type_ctx;
  ObSQLUtils::init_type_ctx(my_session_, type_ctx);
  for (int64_t i = 0; OB_SUCC(ret) && i < real_parm_exprs.count(); ++i) {
    ObRawExpr *real_param_expr = real_parm_exprs.at(i);
    if (OB_ISNULL(real_param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("real param expr is null", K(i));
    } else if (get_expr_output_column(*real_param_expr) != 1) {
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, 1L);
    } else if (OB_FAIL(types.push_back(real_param_expr->get_result_type()))) {
      LOG_WARN("fail to push back result type", K(ret), K(i),
                                                K(real_param_expr->get_result_type()));
    }
  }
  ObCollationType coll_type = CS_TYPE_INVALID;
  OC( (my_session_->get_collation_connection)(coll_type) );

  if (OB_SUCC(ret)) {
    ObExprVersion dummy_op(alloc_);
    if (lib::is_oracle_mode()) {
      //oracle max length is 4k, but the maximum length of the early implementation is 32767.
      //In order to maintain compatibility with the previous version,the maximum length is still
      //temporarily set to 32767.
      result_type.set_length(OB_MAX_ORACLE_VARCHAR_LENGTH);
    } else {
      //bug16528381, mysql结果为text, 在ob支持text前采用varchar(65536)
      result_type.set_length(OB_MAX_SQL_LENGTH);
    }
    result_type.set_varchar();
    if (lib::is_oracle_mode()) {
      if (expr.get_real_param_count() > 2) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("listagg has 2 params at most", K(ret), K(expr.get_real_param_count()));
      } else {
        ObSEArray<ObExprResType*, 2, ObNullAllocator> params;
        for (int64_t i = 0; OB_SUCC(ret) && i < types.count(); ++i) {
          OZ (params.push_back(&types[i]));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(dummy_op.aggregate_length_semantics_oracle(
                            *my_session_, params, result_type))) {
            LOG_WARN("fail to aggregate length semantics for string result", K(ret), K(types));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dummy_op.aggregate_charsets_for_string_result(
                result_type, (types.count() == 0 ? NULL : &(types.at(0))),
                types.count(), type_ctx))) {
      LOG_WARN("fail to aggregate charsets for string result", K(ret), K(types));
    } else {
      expr.set_result_type(result_type);
    }
  }

  ObRawExpr *separator_expr = expr.get_separator_param_expr();
  if (OB_SUCC(ret)
      && NULL != separator_expr
      && (!separator_expr->get_result_meta().is_string_type()
          || expr.get_result_type().get_collation_type()
              != separator_expr->get_result_type().get_collation_type())) {
    ObExprResType result_type(alloc_);
    result_type.set_varchar();
    result_type.set_collation_type(expr.get_result_type().get_collation_type());
    result_type.set_collation_level(expr.get_result_type().get_collation_level());
    if (lib::is_oracle_mode()) {
      result_type.set_length_semantics(expr.get_result_type().get_length_semantics());
    }
    ObSysFunRawExpr *cast_expr = NULL;
    if (OB_ISNULL(expr_factory_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null pointer", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*expr_factory_,
                                                        separator_expr,
                                                        result_type,
                                                        cast_expr,
                                                        my_session_))) {
      LOG_WARN("failed to create raw expr.", K(ret));
    } else if (OB_ISNULL(cast_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast_expr is UNEXPECTED", K(ret));
    } else {
      expr.set_separator_param_expr(static_cast<ObRawExpr *>(cast_expr));
    }
  }
  return ret;
}

int ObRawExprDeduceType::set_agg_json_array_result_type(ObAggFunRawExpr &expr,
                                                        ObExprResType &result_type)
{
  int ret = OB_SUCCESS;
  ObRawExpr *returning_type = NULL;
  if (OB_UNLIKELY(expr.get_real_param_count() < DEDUCE_JSON_ARRAYAGG_RETURNING) ||
      OB_ISNULL(returning_type = expr.get_param_expr(DEDUCE_JSON_ARRAYAGG_RETURNING)) ||
      returning_type->get_data_type() != ObIntType) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()),
                                      K(expr.get_real_param_count()), K(expr));
  } else {
    ParseNode parse_node;
    parse_node.value_ = static_cast<ObConstRawExpr *>(returning_type)->get_value().get_int();
    ObObjType obj_type = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
    result_type.set_collation_type(static_cast<ObCollationType>(parse_node.int16_values_[OB_NODE_CAST_COLL_IDX]));
    result_type.set_type(obj_type);
    if (ob_is_string_type(obj_type) || ob_is_lob_locator(obj_type)) {
      result_type.set_full_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX],
                                  returning_type->get_result_type().get_accuracy().get_length_semantics());
      if (ob_is_blob(obj_type, result_type.get_collation_type())) {
        result_type.set_collation_type(CS_TYPE_BINARY);
        result_type.set_calc_collation_type(CS_TYPE_BINARY);
      } else {
        result_type.set_collation_type(my_session_->get_nls_collation());
        result_type.set_calc_collation_type(my_session_->get_nls_collation());
      }
      result_type.set_collation_level(CS_LEVEL_IMPLICIT);
    } else if (ob_is_json(obj_type) || parse_node.value_ == 0) {
      result_type.set_json();
      result_type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    } else if (ob_is_raw(obj_type)) {
      result_type.set_full_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX],
                                  returning_type->get_result_type().get_accuracy().get_length_semantics());
      result_type.set_collation_type(CS_TYPE_BINARY);
      result_type.set_calc_collation_type(CS_TYPE_BINARY);
      result_type.set_collation_level(CS_LEVEL_NUMERIC);
    }
  }
  return ret;
}

int ObRawExprDeduceType::set_agg_min_max_result_type(ObAggFunRawExpr &expr,
                                                     ObExprResType &result_type,
                                                     bool &need_add_cast)
{
  int ret = OB_SUCCESS;
  ObRawExpr *child_expr = NULL;
  if (OB_ISNULL(child_expr = expr.get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null");
  } else if (OB_UNLIKELY(ob_is_geometry(child_expr->get_data_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Incorrect geometry arguments", K(child_expr->get_data_type()), K(ret));
  } else if (OB_UNLIKELY(ob_is_roaringbitmap(child_expr->get_data_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Incorrect roaringbitmap arguments", K(child_expr->get_data_type()), K(ret));
  } else if (OB_UNLIKELY(ob_is_collection_sql_type(child_expr->get_data_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Incorrect collection arguments", K(child_expr->get_data_type()), K(ret));
  } else if (OB_UNLIKELY(ob_is_enumset_tc(child_expr->get_data_type()))) {
    // To compatible with MySQL, we need to add cast expression that enumset to varchar
    // to evalute MIN/MAX aggregate functions.
    need_add_cast = true;
    const ObExprResType& res_type = child_expr->get_result_type();
    result_type.set_varchar();
    result_type.set_length(res_type.get_length());
    ObObjMeta obj_meta;
    if (OB_FAIL(ObRawExprUtils::extract_enum_set_collation(res_type, my_session_, obj_meta))) {
      LOG_WARN("fail to extract enum set cs type", K(ret));
    } else {
      result_type.set_collation(obj_meta);
    }
    expr.set_result_type(result_type);
  } else {
    // keep same with default path
    expr.set_result_type(child_expr->get_result_type());
    expr.unset_result_flag(NOT_NULL_FLAG);
    expr.unset_result_flag(ZEROFILL_FLAG);
  }
  return ret;
}

int ObRawExprDeduceType::set_agg_regr_result_type(ObAggFunRawExpr &expr, ObExprResType &result_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.get_param_count() != 2) ||
      OB_ISNULL(expr.get_param_expr(0)) ||
      OB_ISNULL(expr.get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObObjType from_type1 = expr.get_param_expr(0)->get_result_type().get_type();
    ObObjType from_type2 = expr.get_param_expr(1)->get_result_type().get_type();
    ObCollationType from_cs_type1 = expr.get_param_expr(0)->get_result_type().get_collation_type();
    ObCollationType from_cs_type2 = expr.get_param_expr(1)->get_result_type().get_collation_type();
    if (expr.get_expr_type() == T_FUN_REGR_SXX ||
        expr.get_expr_type() == T_FUN_REGR_AVGX) {//这里根据函数特性兼容oracle行为设置
      from_type1 = ObNumberType;
    } else if (expr.get_expr_type() == T_FUN_REGR_SYY ||
                expr.get_expr_type() == T_FUN_REGR_AVGY) {//这里根据函数特性兼容oracle行为设置
      from_type2 = ObNumberType;
    }
    ObObjType to_type = ObNumberType;
    ObCollationType to_cs_type = CS_TYPE_BINARY;
    if (ob_is_double_type(from_type1) || ob_is_float_type(from_type1) ||
        ob_is_double_type(from_type2) || ob_is_float_type(from_type2)) {
      if (ob_is_double_type(from_type1) || ob_is_double_type(from_type2)) {
        to_type = ob_is_double_type(from_type1) ? from_type1 : from_type2;
      } else {
        to_type = ob_is_float_type(from_type1) ? from_type1 : from_type2;
      }
    }
    if (from_type1 != to_type && !cast_supported(from_type1, from_cs_type1,
                                                to_type, to_cs_type)
        && !my_session_->is_varparams_sql_prepare()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("cast to expected type not supported", K(ret), K(from_type1), K(to_type));
    } else if (from_type2 != to_type && !cast_supported(from_type2, from_cs_type2,
                                                        to_type, to_cs_type)
      && !my_session_->is_varparams_sql_prepare()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("cast to expected type not supported", K(ret), K(from_type2), K(to_type));
    } else {
      result_type.set_type(to_type);
      result_type.set_scale(
        ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_scale());
      result_type.set_precision(
        ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][to_type].get_precision());
      expr.set_result_type(result_type);
    }
    }
  return ret;
}
int ObRawExprDeduceType::set_asmvt_result_type(ObAggFunRawExpr &expr,
                                               ObExprResType& result_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.get_real_param_count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()), K(expr.get_real_param_count()), K(expr));
  } else {
    result_type.set_type(ObLongTextType);
    result_type.set_collation_type(CS_TYPE_BINARY);
    result_type.set_collation_level(CS_LEVEL_IMPLICIT);
    result_type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
    expr.set_result_type(result_type);
  }
  return ret;
}

int ObRawExprDeduceType::set_array_agg_result_type(ObAggFunRawExpr &expr,
                                                   ObExprResType& result_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.get_real_param_count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()), K(expr.get_real_param_count()), K(expr));
  } else {
    // check order by constrain
    const common::ObIArray<OrderItem>& order_item = expr.get_order_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < order_item.count(); ++i) {
      ObRawExpr* order_expr = order_item.at(i).expr_;
      if (OB_ISNULL(order_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("internal order expr is null", K(ret));
      } else if (order_expr->get_result_type().get_type() == ObCollectionSQLType) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("array type used for sorting isn't supported", K(ret));
      }
    }

    ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(my_session_);
    ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
    const ObRawExpr *param_expr = expr.get_param_expr(0);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(param_expr) || OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected NULL", K(param_expr), K(session), K(ret));
    } else if (OB_ISNULL(session->get_cur_exec_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected NULL", K(param_expr), K(session), K(ret));
    } else {
      ObExecContext *exec_ctx = session->get_cur_exec_ctx();
      ObDataType elem_type;
      uint16_t subschema_id;
      elem_type.set_meta_type(param_expr->get_result_meta());
      if (ob_is_collection_sql_type(elem_type.get_obj_type())) {
        if (OB_FAIL(ObArrayExprUtils::deduce_nested_array_subschema_id(exec_ctx, elem_type, subschema_id))) {
          LOG_WARN("failed to deduce nested array subschema id", K(ret));
        }
      } else {
        if (!ob_is_array_supported_type(elem_type.get_obj_type())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported element type", K(ret), K(elem_type.get_obj_type()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "array element type");
        } else if (ob_is_varbinary_or_binary(elem_type.get_obj_type(), elem_type.get_collation_type())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("array element in binary type isn't supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "array element in binary type");
        } else if (elem_type.get_obj_type() == ObVarcharType) {
          elem_type.set_accuracy(param_expr->get_accuracy());
          if (elem_type.get_length() < 0) {
            elem_type.set_length(OB_MAX_VARCHAR_LENGTH / 4);
          }
        } else if (elem_type.get_obj_type() == ObDecimalIntType
                   || elem_type.get_obj_type() == ObNumberType
                   || elem_type.get_obj_type() == ObUNumberType) {
          ObObjMeta meta;
          if (param_expr->get_scale() != 0) {
            meta.set_double();
          } else {
            meta.set_int();
          }
          ObAccuracy acc = ObAccuracy::DDL_DEFAULT_ACCURACY[meta.get_type()];
          elem_type.set_meta_type(meta);
          elem_type.set_accuracy(acc);
          ObExprResType param_res_type = param_expr->get_result_type();
          param_res_type.set_calc_meta(meta);
          param_res_type.set_calc_accuracy(acc);
          ObCastMode def_cast_mode = CM_NONE;
          if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, my_session_, def_cast_mode))) {
            LOG_WARN("get_default_cast_mode failed", K(ret));
          } else if (OB_FAIL(try_add_cast_expr(expr, 0, param_res_type, def_cast_mode))) {
            LOG_WARN("try_add_cast_expr failed", K(ret), K(expr), K(param_res_type));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(exec_ctx->get_subschema_id_by_collection_elem_type(ObNestedType::OB_ARRAY_TYPE,
                                                                                       elem_type, subschema_id))) {
          LOG_WARN("failed to get collection subschema id", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        result_type.set_collection(subschema_id);
        expr.set_result_type(result_type);
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::set_rb_result_type(ObAggFunRawExpr &expr,
                                               ObExprResType& result_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.get_real_param_count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()), K(expr.get_real_param_count()), K(expr));
  } else {
    result_type.set_type(ObRoaringBitmapType);
    result_type.set_collation_type(CS_TYPE_BINARY);
    result_type.set_collation_level(CS_LEVEL_IMPLICIT);
    result_type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObRoaringBitmapType]);
    expr.set_result_type(result_type);
  }
  return ret;
}

int ObRawExprDeduceType::set_xmlagg_result_type(ObAggFunRawExpr &expr,
                                                ObExprResType& result_type)
{
  int ret = OB_SUCCESS;
  ObRawExpr *col_expr = NULL;
  if (OB_UNLIKELY(expr.get_real_param_count() < 1) ||
      OB_ISNULL(col_expr = expr.get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()), K(expr.get_real_param_count()), K(expr));
  } else if (ObUserDefinedSQLType != col_expr->get_data_type() &&
              ObNullType != col_expr->get_data_type() &&
              ObExtendType != col_expr->get_data_type()) {
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("invalid expr", K(col_expr->get_data_type()));
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, 11, "SYS_IXMLAGG");
  } else {
    ObExprResType& col_type = const_cast<ObExprResType&>(col_expr->get_result_type());
    const common::ObIArray<OrderItem>& order_item = expr.get_order_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < order_item.count(); ++i) {
      ObRawExpr* order_expr = order_item.at(i).expr_;
      if (OB_ISNULL(order_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("internal order expr is null", K(ret));
      } else if (order_expr->get_expr_type() == T_REF_COLUMN) {
        const ObColumnRefRawExpr *order_column = static_cast<const ObColumnRefRawExpr *>(order_expr);
        if (order_column->is_lob_column()) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("Column of LOB type cannot be used for sorting", K(ret));
        } else if (order_column->is_xml_column()) {
          ret = OB_ERR_NO_ORDER_MAP_SQL;
          LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
        }
      } else {
        ObObjType result_type = order_expr->get_result_type().get_type();
        if (result_type == ObUserDefinedSQLType || result_type == ObExtendType) {
          ret = OB_ERR_NO_ORDER_MAP_SQL;
          LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
        } else if (ob_is_text_tc(result_type) ||
                    ob_is_lob_tc(result_type)) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("Column of LOB type cannot be used for sorting", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(set_agg_xmlagg_result_type(expr, result_type))) {
      LOG_WARN("set xmlagg result type failed", K(ret));
    } else {
      expr.set_result_type(result_type);
    }
  }
  return ret;
}

int ObRawExprDeduceType::set_agg_xmlagg_result_type(ObAggFunRawExpr &expr,
                                                    ObExprResType &result_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.get_real_param_count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(ret), K(expr.get_param_count()), K(expr.get_real_param_count()), K(expr));
  } else {
    result_type.set_sql_udt(ObXMLSqlType);
  }
  return ret;
}

bool ObRawExprDeduceType::skip_cast_expr(const ObRawExpr &parent,
                                         const int64_t child_idx)
{
  ObItemType parent_expr_type = parent.get_expr_type();
  bool bret = false;
  if ((T_FUN_COLUMN_CONV == parent_expr_type && child_idx < 4) ||
      (T_FUN_SYS_DEFAULT == parent_expr_type && child_idx < 4) ||
      T_FUN_SET_TO_STR == parent_expr_type  || T_FUN_ENUM_TO_STR == parent_expr_type ||
      T_FUN_SET_TO_INNER_TYPE  == parent_expr_type ||
      T_FUN_ENUM_TO_INNER_TYPE == parent_expr_type ||
      T_OP_EXISTS == parent_expr_type  ||
      T_OP_NOT_EXISTS == parent_expr_type ||
      (T_FUN_SYS_CAST == parent_expr_type && !CM_IS_EXPLICIT_CAST(parent.get_extra()))) {
    bret = true;
  }
  return bret;
}


static inline bool skip_cast_json_expr(const ObRawExpr *expr,
  const ObExprResType &input_type, ObItemType parent_expr_type)
{
  bool b_ret = (expr->get_expr_type() == T_FUN_SYS_CAST &&
          need_calc_json(parent_expr_type) &&
          (input_type.get_calc_type() == expr->get_result_meta().get_type() ||
          input_type.get_calc_collation_type() == expr->get_result_meta().get_collation_type()));

  return b_ret;
}

// 该函数会给case表达式按需增加隐式cast
// 对于case when x1 then y1 when x2 then y2 else y3
// input_types的顺序是: x1 x2 y1 y2 y3
// 而ObCaseOpRawExpr::get_param_expr()要求的顺序是: x1 y1 x2 y2 y3
// 所以需要对input_type进行重新排序
int ObRawExprDeduceType::add_implicit_cast(ObCaseOpRawExpr &parent,
                                           const ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_OP_CASE != parent.get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("all T_OP_ARG_CASE should be resolved as T_OP_CASE", K(ret),
              K(parent.get_expr_type()));
  } else {
    int64_t when_size = parent.get_when_expr_size();
    const ObExprResTypes &input_types = parent.get_input_types();
    ObArenaAllocator allocator;
    ObFixedArray<ObExprResType, ObIAllocator> input_types_reorder(&allocator,
                                                                  input_types.count());
    // push_back when_expr以及对应的then_expr结果类型
    for (int64_t i = 0; OB_SUCC(ret) && i < when_size; ++i) {
      if (OB_FAIL(input_types_reorder.push_back(input_types.at(i)))) {
        LOG_WARN("push back res type failed", K(ret), K(i));
      } else if (OB_FAIL(input_types_reorder.push_back(input_types.at(i + when_size)))) {
        LOG_WARN("push back res type failed", K(ret), K(i + when_size));
      }
    }

    // push_back else_expr的结果类型
    if (OB_SUCC(ret)) {
      if (input_types_reorder.count() + 1 == input_types.count()) {
        if (OB_FAIL(input_types_reorder.push_back(
                                          input_types.at(input_types.count()-1)))) {
          LOG_WARN("push back res type failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(input_types_reorder.count() != input_types.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected input type array", K(ret), K(input_types),
                                                K(input_types_reorder));
      }
    }
    LOG_DEBUG("input types reorder done", K(ret), K(input_types_reorder), K(input_types));
    ObRawExpr *child_ptr = NULL;
    // 开始插入隐式cast
    for (int64_t child_idx = 0; OB_SUCC(ret) && (child_idx < parent.get_param_count());
                                                                          ++child_idx) {
      if (OB_ISNULL(child_ptr = parent.get_param_expr(child_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child_ptr raw expr is NULL", K(ret));
      } else {
        if (skip_cast_expr(parent, child_idx)) {
          // do nothing
        } else if (OB_FAIL(try_add_cast_expr(parent, child_idx,
                                             input_types_reorder.at(child_idx), cast_mode))) {
          LOG_WARN("try_add_cast_expr failed", K(ret), K(child_idx));
        }
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::add_implicit_cast(ObOpRawExpr &parent,
                                           const ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  ObRawExpr *child_ptr = NULL;
  const ObIArray<ObExprResType> &input_types = parent.get_input_types();
  typedef ObArrayHelper<ObExprResType> ObExprTypeArrayHelper;
  // idx is the index of input_types
  // child_idx is the index of parent.get_param_count()
  // (T_OP_ROW, input_types.count() != parent.get_param_count())
  int64_t idx = 0;
  if (!parent.is_calc_part_expr()) {
    for (int64_t child_idx = 0; OB_SUCC(ret) && (child_idx < parent.get_param_count()); ++child_idx) {
      if (OB_ISNULL(child_ptr = parent.get_param_expr(child_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child_ptr raw expr is NULL", K(ret));
      } else {
        if (OB_UNLIKELY(idx >= input_types.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected idx", K(ret), K(idx), K(input_types.count()), K(parent));
        } else if (skip_cast_expr(parent, child_idx) ||
            skip_cast_json_expr(child_ptr, input_types.at(idx), parent.get_expr_type()) ||
            child_ptr->is_multiset_expr()) {
          idx += 1;
          // do nothing
        } else if (T_OP_ROW == child_ptr->get_expr_type()) {
          int64_t ele_cnt = child_ptr->get_param_count();
          CK(OB_NOT_NULL(child_ptr->get_param_expr(0)));
          if (OB_SUCC(ret)) {
            if (T_OP_ROW == child_ptr->get_param_expr(0)->get_expr_type()) {
              // (1, 2) in ((2, 2), (1, 2)), 右支是向量的向量
              ele_cnt = ele_cnt * child_ptr->get_param_expr(0)->get_param_count();
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(idx + ele_cnt > input_types.count())) {
            ret = OB_INVALID_ARGUMENT_NUM;
            LOG_WARN("invalid argument num", K(idx), K(ele_cnt), K(input_types.count()));
          } else if (OB_FAIL(add_implicit_cast_for_op_row(
                      child_ptr,
                      ObExprTypeArrayHelper(
                        ele_cnt,
                        const_cast<ObExprResType *>(&input_types.at(idx)), ele_cnt),
                      cast_mode))) {
            LOG_WARN("add_implicit_cast_for_op_row failed", K(ret));
          } else if (lib::is_oracle_mode()
                     && (parent.get_expr_type() == T_OP_IN
                         || parent.get_expr_type() == T_OP_NOT_IN)) {
            // try replace cast expr with questionmark
            if (OB_FAIL(try_replace_casts_with_questionmarks_ora(child_ptr))) {
              LOG_WARN("replace casts with questionmarks failed", K(ret));
            } else {
              parent.get_param_expr(child_idx) = child_ptr;
            }
          } else {
            parent.get_param_expr(child_idx) = child_ptr;
          }
          idx += ele_cnt;
        } else if (T_REF_QUERY == child_ptr->get_expr_type()
                   && !static_cast<ObQueryRefRawExpr *>(child_ptr)->is_cursor()
                   && !static_cast<ObQueryRefRawExpr *>(child_ptr)->is_scalar()) {
          // subquery result not scalar (is row or set), add cast on subquery stmt's output
          ObQueryRefRawExpr *query_ref_expr = static_cast<ObQueryRefRawExpr *>(child_ptr);
          const int64_t column_cnt = query_ref_expr->get_output_column();
          CK(idx + column_cnt <= input_types.count());
          OZ(add_implicit_cast_for_subquery(*query_ref_expr,
                ObExprTypeArrayHelper(column_cnt,
                  const_cast<ObExprResType *>(&input_types.at(idx)), column_cnt), cast_mode));
          idx += column_cnt;
        } else {
          // general case
          if (input_types.count() <= idx) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("count of input_types must be greater than child_idx",
                      K(ret), K(child_idx), K(idx), K(input_types.count()));
          } else if (OB_FAIL(try_add_cast_expr(parent, child_idx, input_types.at(idx), cast_mode))) {
            LOG_WARN("try_add_cast_expr failed", K(ret), K(child_idx), K(idx));
          }
          idx += 1;
        }
      }
      LOG_DEBUG("add_implicit_cast debug", K(parent));
    } // for end
  }
  return ret;
}

int ObRawExprDeduceType::add_implicit_cast(ObAggFunRawExpr &parent,
                                           const ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  ObExprResType res_type = parent.get_result_type();
  ObIArray<ObRawExpr*> &real_param_exprs = parent.get_real_param_exprs_for_update();
  for (int64_t i = 0; OB_SUCC(ret) && i < real_param_exprs.count(); ++i) {
    ObRawExpr *&child_ptr = real_param_exprs.at(i);
    if (skip_cast_expr(parent, i)) {
      // do nothing
    //兼容oracle行为,regr_sxx和regr_syy只需在计算的参数加cast,regr_sxy行为和regr_syy一致，比较诡异，暂时兼容
    } else if ((parent.get_expr_type() == T_FUN_JSON_OBJECTAGG ||
                parent.get_expr_type() == T_FUN_JSON_ARRAYAGG) &&
                child_ptr->get_result_type().is_enum_set_with_subschema()) {
      ObExprResType result_type(alloc_);
      result_type.set_varchar();
      result_type.set_length(child_ptr->get_result_type().get_length());
      ObObjMeta obj_meta;
      if (OB_FAIL(ObRawExprUtils::extract_enum_set_collation(child_ptr->get_result_type(),
                                                             my_session_,
                                                             obj_meta))) {
        LOG_WARN("fail to extract enum set cs type", K(ret));
      } else {
        result_type.set_collation(obj_meta);
      }
      result_type.set_calc_meta(result_type.get_obj_meta());
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(try_add_cast_expr(parent, i, result_type, cast_mode))) {
        LOG_WARN("try_add_cast_expr failed", K(ret));
      } else {
        LOG_DEBUG("add_implicit_cast for ObAggFunRawExpr", K(i), K(res_type), KPC(child_ptr));
      }
    } else if ((parent.get_expr_type() == T_FUN_REGR_SXX && i == 0) ||
               (parent.get_expr_type() == T_FUN_REGR_SYY && i == 1) ||
               (parent.get_expr_type() == T_FUN_REGR_SXY && i == 1) ||
               (parent.get_expr_type() == T_FUN_JSON_OBJECTAGG && i == 1) ||
               (parent.get_expr_type() == T_FUN_ORA_JSON_OBJECTAGG && i > 0) ||
               (parent.get_expr_type() == T_FUN_ORA_XMLAGG && i > 0) ||
               parent.get_expr_type() == T_FUN_SYS_ST_ASMVT ||
               ((parent.get_expr_type() == T_FUN_SUM ||
                 parent.get_expr_type() == T_FUN_AVG ||
                 parent.get_expr_type() == T_FUN_COUNT) &&
                 child_ptr->get_expr_type() == T_FUN_SYS_OP_OPNSIZE) ||
                (lib::is_mysql_mode() &&
                 (T_FUN_VARIANCE == parent.get_expr_type() ||
                  T_FUN_STDDEV == parent.get_expr_type() ||
                  T_FUN_STDDEV_POP == parent.get_expr_type() ||
                  T_FUN_STDDEV_SAMP == parent.get_expr_type() ||
                  T_FUN_VAR_POP == parent.get_expr_type() ||
                  T_FUN_VAR_SAMP == parent.get_expr_type()))) {
      //do nothing
    } else if (parent.get_expr_type() == T_FUN_WM_CONCAT ||
               parent.get_expr_type() == T_FUN_KEEP_WM_CONCAT ||
               (parent.get_expr_type() == T_FUN_JSON_OBJECTAGG && i == 0) ||
               (parent.get_expr_type() == T_FUN_ORA_JSON_OBJECTAGG && i == 0) ||
               (parent.get_expr_type() == T_FUN_ORA_XMLAGG && i == 0)) {
      if (ob_is_string_type(child_ptr->get_result_type().get_type())
          && !ob_is_blob(child_ptr->get_result_type().get_type(), child_ptr->get_collation_type())) {
        /*do nothing*/
      } else {
        ObExprResType result_type(alloc_);
        result_type.set_varchar();
        result_type.set_length(OB_MAX_LONGTEXT_LENGTH);
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

template<typename RawExprType>
int ObRawExprDeduceType::try_add_cast_expr(RawExprType &parent,
                                           int64_t child_idx,
                                           const ObExprResType &input_type,
                                           const ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  ObRawExpr *child_ptr = NULL;
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
    ObRawExpr *new_expr = NULL;
    OZ(try_add_cast_expr_above_for_deduce_type(*child_ptr, new_expr, input_type,
                                               cast_mode));
    CK(NULL != new_expr);
    if (OB_SUCC(ret) && child_ptr != new_expr) { // cast expr added
      ObObjTypeClass ori_tc = ob_obj_type_class(child_ptr->get_data_type());
      ObObjTypeClass expect_tc = ob_obj_type_class(input_type.get_calc_type());
      if (T_FUN_UDF == parent.get_expr_type()
          && ObNumberTC == ori_tc
          && ((ObTextTC == expect_tc && lib::is_oracle_mode()) || ObLobTC == expect_tc)) {
        // oracle mode can not cast number to text, but mysql mode can
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("cast to lob type not allowed", K(ret));
      }

      // for consistent with mysql, if const cast as json, should regard as scalar, don't need parse
      if (ObStringTC == ori_tc && ObJsonTC == expect_tc && IS_JSON_COMPATIBLE_OP(parent.get_expr_type())) {
        uint64_t extra = new_expr->get_extra();
        new_expr->set_extra(CM_SET_SQL_AS_JSON_SCALAR(extra));
      }
      OZ(parent.replace_param_expr(child_idx, new_expr));
      if (OB_FAIL(ret) && my_session_->is_varparams_sql_prepare()) {
        ret = OB_SUCCESS;
        LOG_DEBUG("ps prepare phase ignores type deduce error");
      }
      //add local vars to cast expr
      if (OB_SUCC(ret)) {
        if (solidify_session_vars_) {
          if (OB_FAIL(new_expr->set_local_session_vars(NULL, my_session_, local_vars_id_))) {
            LOG_WARN("fail to set session vars", K(ret), KPC(new_expr));
          }
        } else if (NULL != my_local_vars_) {
          if (OB_FAIL(new_expr->set_local_session_vars(my_local_vars_, NULL, local_vars_id_))) {
            LOG_WARN("fail to set local vars", K(ret), KPC(new_expr));
          }
        }
      }
    }
  }
  return ret;
};

int ObRawExprDeduceType::try_add_cast_expr_above_for_deduce_type(ObRawExpr &expr,
                                                                 ObRawExpr *&new_expr,
                                                                 const ObExprResType &dst_type,
                                                                 const ObCastMode &cm)
{
  int ret = OB_SUCCESS;
  ObExprResType cast_dst_type;
  // cast child_res_type to cast_dst_type
  const ObExprResType &child_res_type = expr.get_result_type();

  // calc meta of dst_type is the real destination type!!!
  cast_dst_type.set_meta(dst_type.get_calc_meta());
  cast_dst_type.set_calc_meta(ObObjMeta());
  cast_dst_type.set_result_flag(child_res_type.get_result_flag());
  cast_dst_type.set_accuracy(dst_type.get_calc_accuracy());
  cast_dst_type.add_cast_mode(dst_type.get_cast_mode());
  if (lib::is_mysql_mode()
      && (dst_type.get_calc_meta().is_number()
          || dst_type.get_calc_meta().is_unumber()
          || ob_is_decimal_int_tc(dst_type.get_calc_meta().get_type()))
      && dst_type.get_calc_scale() == -1) {
    cast_dst_type.set_accuracy(child_res_type.get_accuracy());
    if (child_res_type.is_enum_or_set()) {
      cast_dst_type.set_precision(PRECISION_UNKNOWN_YET);
      cast_dst_type.set_scale(SCALE_UNKNOWN_YET);
    }
  } else if (lib::is_oracle_mode()
             && ob_is_decimal_int_tc(dst_type.get_calc_meta().get_type())
             && dst_type.get_calc_scale() == SCALE_UNKNOWN_YET) {
    cast_dst_type.set_accuracy(child_res_type.get_accuracy());
  } else if (ob_is_decimal_int_tc(dst_type.get_calc_meta().get_type()) &&
              dst_type.get_calc_scale() != SCALE_UNKNOWN_YET) {
    cast_dst_type.set_accuracy(dst_type.get_calc_accuracy());
  } else if (lib::is_mysql_mode()
             && ObDateTimeTC == child_res_type.get_type_class()
             && ObDateTimeTC == dst_type.get_calc_meta().get_type_class()) {
    cast_dst_type.set_accuracy(child_res_type.get_accuracy());
  } else if (lib::is_mysql_mode() && ObDoubleTC == dst_type.get_calc_meta().get_type_class()) {
    if (ob_is_numeric_tc(child_res_type.get_type_class())) {
      // passing scale and precision when casting float/double/decimal to double
      ObScale s = child_res_type.get_calc_accuracy().get_scale();
      ObPrecision p = child_res_type.get_calc_accuracy().get_precision();
      if ((ObNumberTC == child_res_type.get_type_class() ||
              ObDecimalIntTC == child_res_type.get_type_class()) &&
          SCALE_UNKNOWN_YET != s && PRECISION_UNKNOWN_YET != p) {
        p += decimal_to_double_precision_inc(child_res_type.get_type(), s);
        cast_dst_type.set_scale(s);
        cast_dst_type.set_precision(p);
      } else if (ObDoubleTC == child_res_type.get_type_class()) {
        // child_res_type and cast_dst_type are the same, which is double type cast of the expr
        // aligned to scale, accuracy need based on the cast_dst_type set by the expr
        // calc_resul_type.
      } else if (s != SCALE_UNKNOWN_YET && PRECISION_UNKNOWN_YET != p &&
                s <= OB_MAX_DOUBLE_FLOAT_SCALE && p >= s) {
        cast_dst_type.set_accuracy(child_res_type.get_calc_accuracy());
      }
    } else {
      cast_dst_type.set_scale(SCALE_UNKNOWN_YET);
      cast_dst_type.set_precision(PRECISION_UNKNOWN_YET);
    }
  } else if (lib::is_oracle_mode() && dst_type.get_calc_meta().is_number() && expr.is_const_expr()) {
    cast_dst_type.set_precision(PRECISION_UNKNOWN_YET);
    cast_dst_type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
  }
  if (cast_dst_type.is_user_defined_sql_type() || cast_dst_type.is_collection_sql_type()) {
    uint64_t udt_id = (dst_type.is_user_defined_sql_type() || dst_type.is_collection_sql_type())
                      ? dst_type.get_udt_id()
                      : dst_type.get_calc_accuracy().get_accuracy();
    cast_dst_type.set_udt_id(udt_id);
  }
  if (lib::is_oracle_mode() && cast_dst_type.is_ext()) {
    uint64_t udt_id = dst_type.is_ext() ? dst_type.get_udt_id() : dst_type.get_calc_accuracy().get_accuracy();
    cast_dst_type.set_udt_id(udt_id);
  }
  // 这里仅设置部分情况的accuracy，其他情况的accuracy信息交给cast类型推导设置
  if (lib::is_mysql_mode() && cast_dst_type.is_string_type() &&
      cast_dst_type.has_result_flag(ZEROFILL_FLAG)) {
    // get_length()必须手动调用，里面会有根据int precision设定长度的代码
    cast_dst_type.set_length(child_res_type.get_length());
  }
  OZ(ObRawExprUtils::try_add_cast_expr_above(expr_factory_, my_session_, expr,
                                             cast_dst_type, cm, new_expr, my_local_vars_, local_vars_id_));
  ObRawExpr *e = new_expr;
  while (OB_SUCC(ret) && NULL != e &&
         e != &expr && T_FUN_SYS_CAST == e->get_expr_type()) {
    if (OB_FAIL(e->add_flag(IS_OP_OPERAND_IMPLICIT_CAST))) {
      LOG_WARN("failed to add flag", K(ret));
    } else {
      e = e->get_param_expr(0);
    }
  }
  return ret;
}

int ObRawExprDeduceType::add_implicit_cast_for_op_row(
    ObRawExpr *&child_ptr,
    const common::ObIArray<ObExprResType> &input_types,
    const ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_ptr)
     || OB_UNLIKELY(T_OP_ROW != child_ptr->get_expr_type())
     || OB_ISNULL(child_ptr->get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("child_ptr is NULL", K(ret), K(child_ptr));
  } else if (OB_FAIL(ObRawExprCopier::copy_expr_node(*child_ptr->get_expr_factory(),
                                                     child_ptr,
                                                     child_ptr))) {
    LOG_WARN("failed to copy expr node", K(ret));
  } else if (T_OP_ROW == child_ptr->get_param_expr(0)->get_expr_type()){
    // (1, 1) in ((1, 2), (3, 4))
    // row_dimension = 2, input_types = 6
    int64_t top_row_dim = child_ptr->get_param_count();
    int64_t ele_row_dim = child_ptr->get_param_expr(0)->get_param_count();
    ObOpRawExpr *cur_parent = dynamic_cast<ObOpRawExpr *>(child_ptr);
    CK(OB_NOT_NULL(cur_parent));
    for (int64_t i = 0; OB_SUCC(ret) && i < top_row_dim; i++) {
      if (OB_ISNULL(cur_parent->get_param_expr(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null param expr", K(ret));
      } else if (OB_FAIL(add_implicit_cast_for_op_row(cur_parent->get_param_expr(i),
                   ObArrayHelper<ObExprResType>(ele_row_dim,
                                   const_cast<ObExprResType *>(&input_types.at(i * ele_row_dim)),
                                 ele_row_dim),
                   cast_mode))) {
        LOG_WARN("failed to add implicit cast for op row", K(ret));
      }
    }
  } else {
    const int64_t row_dim = child_ptr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < row_dim; i++) {
      ObOpRawExpr *child_op_expr = static_cast<ObOpRawExpr *>(child_ptr);
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
    ObQueryRefRawExpr &expr,
    const common::ObIArray<ObExprResType> &input_types, const ObCastMode &cast_mode)
{
  // Only subquery as row or is set need to add cast inside subquery, e.g.:
  //   (select c1, c2 from t1) > (a, b) // subquery as row
  //   a = ALL (select c1 from t1) // subquery is set
  //
  // the scalar result subquery add cast above query ref expr, e.g.:
  //   (select c1 from t1) + a
  int ret = OB_SUCCESS;
  CK(expr.get_output_column() > 1 || expr.is_set());
  CK(!expr.is_multiset_expr());
  CK(expr.get_output_column() == input_types.count());
  CK(NULL != expr.get_ref_stmt());
  CK(expr.get_column_types().count() == expr.get_output_column()
     && expr.get_output_column() == expr.get_ref_stmt()->get_select_item_size());
  if (OB_SUCC(ret)) {
    auto &items = expr.get_ref_stmt()->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); i++) {
      ObRawExpr *new_expr = NULL;
      SelectItem &item = items.at(i);
      CK(NULL != item.expr_);
      OZ(try_add_cast_expr_above_for_deduce_type(*item.expr_, new_expr, input_types.at(i),
                                                 cast_mode));
      CK(NULL != new_expr);
      if (OB_SUCC(ret) && item.expr_ != new_expr) { // cast expr added
        // update select item expr && column types of %expr
        item.expr_ = new_expr;
        const_cast<ObExprResType &>(expr.get_column_types().at(i)) = new_expr->get_result_type();
      }
    }
  }
  return ret;
}

bool ObRawExprDeduceType::ignore_scale_adjust_for_decimal_int(const ObItemType expr_type)
{
  bool bret = false;
  switch (expr_type) {
  case T_FUN_SYS_PART_HASH:
  case T_OP_OUTPUT_PACK:
    bret = true;
    break;
  default:
    break;
  }
  return bret;
}

int ObRawExprDeduceType::try_replace_casts_with_questionmarks_ora(ObRawExpr *row_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret));
  } else if (row_expr->get_expr_type() == T_OP_ROW && row_expr->get_param_count() > 0) {
    if (row_expr->get_param_expr(0)->get_expr_type() != T_OP_ROW) {
      for (int i = 0; OB_SUCC(ret) && i < row_expr->get_param_count(); i++) {
        if (OB_FAIL(try_replace_cast_with_questionmark_ora(*row_expr, row_expr->get_param_expr(i), i))) {
          LOG_WARN("try replacing failed", K(ret));
        }
      }
    } else {
      for (int i = 0; OB_SUCC(ret) && i < row_expr->get_param_count(); i++) {
        if (OB_FAIL(try_replace_casts_with_questionmarks_ora(row_expr->get_param_expr(i)))) {
          LOG_WARN("try replacing failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRawExprDeduceType::try_replace_cast_with_questionmark_ora(ObRawExpr &parent, ObRawExpr *cast_expr, int child_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cast_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(cast_expr));
  } else if (cast_expr->get_expr_type() == T_FUN_SYS_CAST
             && cast_expr->has_flag(IS_INNER_ADDED_EXPR)
             && cast_expr->has_flag(IS_OP_OPERAND_IMPLICIT_CAST)) {
    if (OB_UNLIKELY(cast_expr->get_param_count() != 2) || OB_ISNULL(cast_expr->get_param_expr(0))
        || OB_ISNULL(cast_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cast expr", K(ret));
    } else {
      ObRawExpr *param_expr = cast_expr->get_param_expr(0);
      bool is_decint2nmb = param_expr->get_result_type().is_decimal_int() && cast_expr->get_result_type().is_number();
      bool is_nmb2decint = param_expr->get_result_type().is_number() && cast_expr->get_result_type().is_decimal_int();
      bool is_decint2decint = param_expr->get_result_type().is_decimal_int() && cast_expr->get_result_type().is_decimal_int();
      if (param_expr->is_static_const_expr() && param_expr->get_expr_type() == T_QUESTIONMARK
          && !static_cast<ObConstRawExpr *>(param_expr)->is_dynamic_eval_questionmark() // already replaced
          && (is_decint2nmb || is_nmb2decint || is_decint2decint)) {
        ObConstRawExpr *c_expr = static_cast<ObConstRawExpr *>(param_expr);
        ObExprResType res_type = cast_expr->get_result_type();
        res_type.add_cast_mode(cast_expr->get_extra());
        res_type.set_param(c_expr->get_result_type().get_param());
        int64_t param_idx = 0;
        if (OB_ISNULL(expr_factory_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null raw expr", K(ret));
        } else if (OB_FAIL(c_expr->get_value().get_unknown(param_idx))) {
          LOG_WARN("get param idx failed", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::create_param_expr(*expr_factory_, param_idx, param_expr))) {
          // create new param store to avoid unexpected problem
          LOG_WARN("create param expr failed", K(ret));
        } else if (OB_FAIL(static_cast<ObConstRawExpr *>(param_expr)->set_dynamic_eval_questionmark(res_type))) {
          LOG_WARN("set dynamic eval question mark failed", K(ret));
        } else {
          parent.get_param_expr(child_idx) = param_expr;
        }
      }
      LOG_DEBUG("replace cast with questionmark", K(*cast_expr), K(is_decint2nmb), K(is_nmb2decint),
                K(is_decint2decint));
    }
  }
  return ret;
}

int ObRawExprDeduceType::build_subschema_for_enum_set_type(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  uint16_t subschema_id = 0;
  if (!expr.get_result_type().is_enum_or_set()) {
    // Non-enum or set types do not need to build subschema
  } else if (expr.skip_build_subschema_for_enumset() || !expr.is_called_in_sql()) {
    // skiping pl scenario, because compilation and calling do not share the same plan ctx
    // the subschema has been built, do nothing
  } else if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(my_session_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session can not be null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx()) ||
              OB_ISNULL(exec_ctx->get_physical_plan_ctx()) ||
              OB_NOT_NULL(exec_ctx->get_physical_plan_ctx()->get_phy_plan())) {
    // exec_ctx may be null in ddl generated column scenarios, and it all use column_convert,
    // so the subschema meta build for this scenario is skipped
    LOG_INFO("exec ctx is null", K(ret), K(*session));
  } else if (!exec_ctx->support_enum_set_type_subschema(*session)) {
  } else if (OB_UNLIKELY(expr.get_enum_set_values().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("str values for enum set expr is empty", K(ret), K(expr.get_enum_set_values()));
  } else if (OB_FAIL(exec_ctx->get_subschema_id_by_type_info(
                        expr.get_result_type().get_obj_meta(),
                        expr.get_enum_set_values(),
                        subschema_id))) {
    LOG_WARN("failed to get subschema id by udt id", K(ret));
  } else {
    expr.set_subschema_id(subschema_id);
    expr.mark_enum_set_with_subschema();
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
