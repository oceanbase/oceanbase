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
#include "sql/resolver/expr/ob_raw_expr_util.h"

#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/json/ob_json.h"
#include "lib/json/ob_json_print_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/parser/ob_parser_utils.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/ob_sql_parser.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_to_type.h"
#include "sql/engine/expr/ob_expr_type_to_str.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "common/ob_smart_call.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_type.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_select_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
#define RESOLVE_ORALCE_IMLICIT_CAST_WARN_OR_ERR(warn_code, err_code) \
if ((session_info->get_sql_mode() & SMO_ERROR_ON_RESOLVE_CAST) > 0) {\
  ret = err_code;\
} else {\
  LOG_USER_WARN(warn_code);\
}

inline int ObRawExprUtils::resolve_op_expr_add_implicit_cast(ObRawExprFactory &expr_factory,
                                                      const ObSQLSessionInfo *session_info,
                                                      ObRawExpr *src_expr,
                                                      const ObExprResType &dst_type,
                                                      ObSysFunRawExpr *&func_expr)
{
  int ret = OB_SUCCESS;
  if (dst_type.is_varying_len_char_type() &&
      !ob_is_rowid_tc(src_expr->get_result_type().get_type())) {
    ObItemType func_type = T_MAX;
    const char* func_name = NULL;
    if (OB_ISNULL(src_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), KP(src_expr));
    } else if (src_expr->get_result_type().is_raw()) {
      func_type = T_FUN_SYS_RAWTOHEX;
      func_name = N_RAWTOHEX;
    } else if (dst_type.is_nvarchar2()) {
      func_type = T_FUN_SYS_TO_NCHAR;
      func_name = N_TO_NCHAR;
    } else if (dst_type.is_varchar()) {
      func_type = T_FUN_SYS_TO_CHAR;
      func_name = N_TO_CHAR;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr_factory.create_raw_expr(func_type, func_expr))) {
        LOG_WARN("create cast expr failed", K(ret));
      } else if (OB_FAIL(func_expr->add_param_expr(src_expr))) {
        LOG_WARN("add real param expr failed", K(ret));
      } else {
        func_expr->set_func_name(ObString::make_string(func_name));
        if (OB_FAIL(func_expr->formalize(session_info))) {
          LOG_WARN("formalize current expr failed", K(ret));
        }
        LOG_DEBUG("succ to create to char expr", K(dst_type));
      }
    }
  } else {
    OZ(ObRawExprUtils::create_cast_expr(expr_factory, src_expr, dst_type, func_expr, session_info,
                                        dst_type.get_cast_mode() == CM_NONE,
                                        dst_type.get_cast_mode()));
    CK(OB_NOT_NULL(func_expr));
  }

  return ret;
}

int ObRawExprUtils::resolve_op_expr_implicit_cast(ObRawExprFactory &expr_factory,
                                                  const ObSQLSessionInfo *session_info,
                                                  ObItemType op_type,
                                                  ObRawExpr* &sub_expr1,
                                                  ObRawExpr* &sub_expr2)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_expr1) || OB_ISNULL(sub_expr2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(sub_expr1), K(sub_expr2));
  } else {
    ObSysFunRawExpr *new_expr = NULL;
    ObSysFunRawExpr *new_expr2 = NULL;
    ObObjType r_type1 = ObMaxType;
    ObObjType r_type2 = ObMaxType;
    ObObjType r_type3 = ObMaxType;

    r_type1 = sub_expr1->get_result_type().get_type();
    r_type2 = sub_expr2->get_result_type().get_type();
    //formalize to get expr's type
    if (OB_SUCC(ret) && !ob_is_valid_obj_o_type(r_type1)) {
      if (OB_FAIL(sub_expr1->formalize(session_info))) {
        LOG_WARN("expr fail to formalize");
      } else if (!ob_is_valid_obj_o_type(r_type1 = sub_expr1->get_result_type().get_type())) {
        RESOLVE_ORALCE_IMLICIT_CAST_WARN_OR_ERR(OB_OBJ_TYPE_ERROR, OB_OBJ_TYPE_ERROR);
        LOG_WARN("invalid oracle type after formalize type1", K(r_type1), K(*sub_expr1));
      }
    }
    if (OB_SUCC(ret) && !ob_is_valid_obj_o_type(r_type2)) {
      if (OB_FAIL(sub_expr2->formalize(session_info))) {
        LOG_WARN("expr fail to formalize");
      } else if (!ob_is_valid_obj_o_type(r_type2 = sub_expr2->get_result_type().get_type())) {
        RESOLVE_ORALCE_IMLICIT_CAST_WARN_OR_ERR(OB_OBJ_TYPE_ERROR, OB_OBJ_TYPE_ERROR);
        LOG_WARN("invalid oracle type after formalize type1", K(r_type2), K(*sub_expr2));
      }
    }
    r_type1 = ObLobType == r_type1 ? ObLongTextType : r_type1;
    r_type2 = ObLobType == r_type2 ? ObLongTextType : r_type2;
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      // oracle 模式下不支持 lob 进行条件比较
      // oracle 模式 不支持json类型的比较
      if (IS_COMPARISON_OP(op_type)
          && sub_expr1->is_called_in_sql() && sub_expr2->is_called_in_sql()) {
        if (ObLongTextType == r_type1 || ObLongTextType == r_type2) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("can't calculate with lob type in oracle mode",
                  K(ret), K(r_type1), K(r_type2), K(op_type));
        } else if (ObJsonType == r_type1 || ObJsonType == r_type2) {
          ret = OB_ERR_INVALID_CMP_OP;
          LOG_WARN("can't calculate with json type in oracle mode",
                  K(ret), K(r_type1), K(r_type2), K(op_type));
        } else if (ob_is_user_defined_sql_type(r_type1) && ob_is_user_defined_sql_type(r_type2)) {
          // other udt types not supported, xmltype does not have order or map member function
          ret = OB_ERR_NO_ORDER_MAP_SQL;
          LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObObjOType type1 = ob_obj_type_to_oracle_type(r_type1);
      ObObjOType type2 = ob_obj_type_to_oracle_type(r_type2);

      if (type1 >= ObOMaxType || type2 >= ObOMaxType) {
        LOG_WARN("INVALID ORACLE TYPE", K(type1), K(type2), K(*sub_expr1), K(*sub_expr2));
        RESOLVE_ORALCE_IMLICIT_CAST_WARN_OR_ERR(OB_OBJ_TYPE_ERROR, OB_ERR_INVALID_TYPE_FOR_OP);
      } else if (ObONullType == type1 || ObONullType == type2) {
        LOG_DEBUG("No need to cast with null", K(type1), K(type2));
      } else {
        ImplicitCastDirection dir = ImplicitCastDirection::IC_NOT_SUPPORT;
        ObObjType middle_type = ObMaxType;
        ObObjTypeClass tc1 = OBJ_O_TYPE_TO_CLASS[type1];
        ObObjTypeClass tc2 = OBJ_O_TYPE_TO_CLASS[type2];
        bool is_arith_op = false;
        switch (op_type) {
        case T_OP_CNN: {
          // the accuracy deduced in this function is conflict with ObExprConcat::calc_result_typeN,
          // so postpone deduce type of T_OP_CNN to ObExprConcat.
          dir = ImplicitCastDirection::IC_NO_CAST;
          break;
        }
        case T_OP_LIKE: {
          /* for non-string data type: select c1||c2 from tab; => cast(c1 as varchar)||cast(c2 as varchar) */
          bool cast_left = true;
          bool cast_right = true;
          bool has_nstring = ob_is_nstring_type(r_type1) || ob_is_nstring_type(r_type2);
          if (ob_is_string_or_lob_type(r_type1) && ob_is_nstring_type(r_type1) == has_nstring) {
              cast_left = false;
            }
          if (ob_is_string_or_lob_type(r_type2) && ob_is_nstring_type(r_type2) == has_nstring) {
            cast_right = false;
          }
          if (T_OP_LIKE == op_type) {
            if (ob_is_rowid_tc(r_type1) && ob_is_string_tc(r_type2)) {
              cast_left = true;
              cast_right = false;
            } else if (ob_is_string_tc(r_type1) && ob_is_rowid_tc(r_type2)) {
              cast_right = true;
              cast_left = false;
            }
            }
          if (cast_left && cast_right) {
            dir = ImplicitCastDirection::IC_TO_MIDDLE_TYPE;
            middle_type = has_nstring ? ObNVarchar2Type : ObVarcharType;
          } else if (cast_left) {
            dir = ImplicitCastDirection::IC_A_TO_B;
            r_type2 = has_nstring ? ObNVarchar2Type : ObVarcharType;
          } else if (cast_right) {
            dir = ImplicitCastDirection::IC_B_TO_A;
            r_type1 = has_nstring ? ObNVarchar2Type : ObVarcharType;
          } else {
              dir = ImplicitCastDirection::IC_NO_CAST;
            }
          break;
          }
        case T_OP_ADD: {
          is_arith_op = true;
          if (ob_is_oracle_datetime_tc(r_type1)) {
            if (ob_is_numeric_type(r_type2)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            } else if (ob_is_string_tc(r_type2)) {
              r_type3 = ObNumberType;
              dir = ImplicitCastDirection::IC_B_TO_C;
            }
          } else if (ob_is_oracle_datetime_tc(r_type2)) {
            if (ob_is_numeric_type(r_type1)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            } else if (ob_is_string_tc(r_type1)) {
              r_type3 = ObNumberType;
              dir = ImplicitCastDirection::IC_A_TO_C;
            }
            }
          if (ob_is_raw_tc(r_type1)) {
            if (ob_is_string_tc(r_type2)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            }
          } else if (ob_is_raw_tc(r_type2)) {
            if (ob_is_string_tc(r_type1)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            }
            }
          if (ImplicitCastDirection::IC_NOT_SUPPORT != dir) {
            break;
          }
          }
        case T_OP_MINUS: {
          is_arith_op = true;
          if (ob_is_oracle_datetime_tc(r_type1)) {
            if (ob_is_numeric_type(r_type2)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            } else if (ob_is_string_tc(r_type2)) {
              r_type3 = ObNumberType;
              dir = ImplicitCastDirection::IC_B_TO_C;
            } else if (ob_is_oracle_datetime_tc(r_type2)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            }
            } else if (ob_is_oracle_datetime_tc(r_type2)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            }
          if (ob_is_raw_tc(r_type1)) {
            if (ob_is_string_tc(r_type2)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            }
          } else if (ob_is_raw_tc(r_type2)) {
            if (ob_is_string_tc(r_type1)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            }
            }

          if (ImplicitCastDirection::IC_NOT_SUPPORT != dir) {
            break;
            }
          }
        case T_OP_MUL:
        case T_OP_DIV:
        case T_OP_MOD: {
          is_arith_op = true;
          if (ob_is_raw_tc(r_type1)) {
            if (ob_is_string_tc(r_type2)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            }
          } else if (ob_is_raw_tc(r_type2)) {
            if (ob_is_string_tc(r_type1)) {
              dir = ImplicitCastDirection::IC_NO_CAST;
            }
          }

          if (ImplicitCastDirection::IC_NOT_SUPPORT != dir) {
            break;
            }
        }
        //todo timestamp with tz/ltz...
        default:
          if (OB_UNLIKELY(ObLongTextType == r_type1 || ObLongTextType == r_type2)) {
            /* lob type return warning */
          } else {
            middle_type = ObNumberType;
            dir = OB_OBJ_IMPLICIT_CAST_DIRECTION_FOR_ORACLE[type1][type2];
            if ((ImplicitCastDirection::IC_B_TO_C == dir && ob_is_nchar(r_type1))
                || (ImplicitCastDirection::IC_A_TO_C == dir && ob_is_nchar(r_type2))) {
              //nchar and varchar2, convert varchar to nvarchar2
              r_type3 = ObNVarchar2Type;
            }
          }
        break;
        }

        // raw 类型向 char 类型隐式转换的时候，实际会把 raw 类型隐式转换成 varchar 类型
        if (ImplicitCastDirection::IC_A_TO_B == dir && ob_is_raw(r_type1) && ObCharType == r_type2) {
          r_type3 = ObVarcharType;
          dir = ImplicitCastDirection::IC_A_TO_C;
        }

        // 与interval的运算，不要做任何转换
        if (ob_is_interval_tc(r_type1) || ob_is_interval_tc(r_type2)) {
          dir = ImplicitCastDirection::IC_NO_CAST;
        }
        LOG_DEBUG("Molly ORACLE IMPLICIT CAST DIR",
            K(dir), K(type1), K(type2), K(r_type3), K(*sub_expr1), K(*sub_expr2), K(op_type));
        ObExprResType dest_type;
        switch (dir) {
        case ImplicitCastDirection::IC_NO_CAST: {
          //int is number(38) in oracle, when number div number, the result can be decimal.
          //So we add cast int as number b4 do div
          if (tc1 == ObIntTC && tc2 == ObIntTC && op_type == T_OP_DIV) {
            ObAccuracy    acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[1][ObNumberType];
            dest_type.set_precision(acc.get_precision());
            dest_type.set_scale(acc.get_scale());
            dest_type.set_collation_level(CS_LEVEL_NUMERIC);
            dest_type.set_type(ObNumberType);
            if (OB_FAIL(resolve_op_expr_add_implicit_cast(expr_factory,
                                                          session_info,
                                                          sub_expr1,
                                                          dest_type,
                                                          new_expr))) {
              LOG_WARN("create cast expr for implicit failed", K(ret));
            } else if (OB_FAIL(resolve_op_expr_add_implicit_cast(expr_factory,
                                                                  session_info,
                                                                  sub_expr2,
                                                                  dest_type,
                                                                  new_expr2))) {
              LOG_WARN("create cast expr for implicit failed", K(ret));
            } else if (OB_ISNULL(new_expr) || OB_ISNULL(new_expr2)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpect null expr", K(ret));
            } else if (OB_FAIL(new_expr->add_flag(IS_INNER_ADDED_EXPR))) {
              LOG_WARN("failed to add flag", K(ret));
            } else if (OB_FAIL(new_expr2->add_flag(IS_INNER_ADDED_EXPR))) {
              LOG_WARN("failed to add flag", K(ret));
            } else {
              sub_expr1 = new_expr;
              sub_expr2 = new_expr2;
            }
          }
          break;
        }
        case ImplicitCastDirection::IC_A_TO_B: {
          ObAccuracy acc;
          ObObjType dst_type = r_type2;
          if (is_arith_op && ob_is_string_tc(r_type1) && ObDecimalIntType == r_type2) {
            dst_type = ObNumberType;
          }
          // if col cmps decint_const, do not cast col to ObDecimalIntType, use ObNumber instead
          // i.e. cast(col as number) = cast(decint_const as number)
          if (dst_type == ObDecimalIntType && sub_expr2->is_const_expr() && !sub_expr1->is_const_expr()) {
            dst_type = ObNumberType;
          }
          // oracle mode
          // create table t (a float);
          // column 'a' type is ObNumberFloatType
          //
          // select 1 - a from t;
          // '1' parsed as ObDecimalIntType, cast direction is 'IC_B_TO_A', got:
          // select 1 - cast(a as decimal_int(1, 0)) from t;
          // which is wrong, for that 'a' may equal to 1.235.
          // here, we change calc type back to ObNumberType.
          if (ob_is_decimal_int_tc(dst_type)
              && r_type1 == ObNumberFloatType) {
            dst_type = ObNumberType;
          }
          ObCastMode cast_mode = CM_NONE;
          if (ObDecimalIntType == dst_type) {
            // 如果是decimal int类型，accuracy和decimal int一致即可
            acc = sub_expr2->get_result_type().get_accuracy();
            if (!ob_is_decimal_int(r_type1) && IS_STRICT_OP(op_type)) {
              cast_mode |= ObRelationalExprOperator::get_const_cast_mode(op_type, false);
            }
          } else {
            acc = (ObNumberType == dst_type ? ObAccuracy::DDL_DEFAULT_ACCURACY2[1][dst_type] :
                                             ObAccuracy::MAX_ACCURACY2[1][dst_type]);
          }
          dest_type.set_precision(acc.get_precision());
          dest_type.set_scale(acc.get_scale());
          dest_type.set_type(dst_type);
          dest_type.add_cast_mode(cast_mode);
          if (ob_is_nstring_type(dest_type.get_type())) {
            dest_type.set_collation_type(OB_NOT_NULL(session_info)
                                ? session_info->get_nls_collation_nation() : CS_TYPE_UTF16_BIN);
          }
          if (OB_FAIL(resolve_op_expr_add_implicit_cast(expr_factory,
                                                        session_info,
                                                        sub_expr1,
                                                        dest_type,
                                                        new_expr))) {
            LOG_WARN("create cast expr for implicit failed", K(ret));
          } else if (OB_ISNULL(new_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null expr", K(ret));
          } else if (OB_FAIL(new_expr->add_flag(IS_INNER_ADDED_EXPR))) {
            LOG_WARN("failed to add flag", K(ret));
          } else {
            if (sub_expr1->is_const_expr() && !sub_expr2->is_const_expr()
                && ObDecimalIntType == dst_type) {
              ObCastMode extra_cm = ObRelationalExprOperator::get_const_cast_mode(op_type, false);
              new_expr->set_extra(new_expr->get_extra() | extra_cm);
            }
            sub_expr1 = new_expr;
          }
          break;
        }
        case ImplicitCastDirection::IC_B_TO_A: {
          ObAccuracy acc;
          ObCastMode cast_mode = CM_NONE;
          ObObjType dst_type = r_type1;
          if (is_arith_op && ob_is_string_tc(r_type2) && ObDecimalIntType == r_type1) {
            dst_type = ObNumberType;
          }
          // if col cmps decint_const, do not cast col to ObDecimalIntType, use ObNumber instead
          // i.e. cast(col as number) = cast(decint_const as number)
          if (dst_type == ObDecimalIntType && sub_expr1->is_const_expr() && !sub_expr2->is_const_expr()) {
            dst_type = ObNumberType;
          }
          if (ob_is_decimal_int_tc(dst_type)
              && r_type2 == ObNumberFloatType) {
            dst_type = ObNumberType;
          }
          if (ObDecimalIntType == dst_type) {
            // 如果是decimal int类型，accuracy和decimal int一致即可
            acc = sub_expr1->get_result_type().get_accuracy();
            if (!ob_is_decimal_int(r_type2) && IS_STRICT_OP(op_type)) {
              cast_mode |= ObRelationalExprOperator::get_const_cast_mode(op_type, true);
            }
          } else {
            acc = (ObNumberType == dst_type ? ObAccuracy::DDL_DEFAULT_ACCURACY2[1][dst_type] :
                                             ObAccuracy::MAX_ACCURACY2[1][dst_type]);
          }
          dest_type.set_precision(acc.get_precision());
          dest_type.set_scale(acc.get_scale());
          //dest_type.set_collation_level(CS_LEVEL_NUMERIC);
          dest_type.set_type(dst_type);
          dest_type.add_cast_mode(cast_mode);
          if (ob_is_nstring_type(dest_type.get_type())) {
            dest_type.set_collation_type(OB_NOT_NULL(session_info)
                                ? session_info->get_nls_collation_nation() : CS_TYPE_UTF16_BIN);
          }
          if (OB_FAIL(resolve_op_expr_add_implicit_cast(expr_factory,
                                                        session_info,
                                                        sub_expr2,
                                                        dest_type,
                                                        new_expr))) {
              LOG_WARN("create cast expr for implicit failed", K(ret));
          } else if (OB_ISNULL(new_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null expr", K(ret));
          } else {
            if (sub_expr2->is_const_expr() && !sub_expr1->is_const_expr() && ObDecimalIntType == dst_type) {
              ObCastMode extra_cm = ObRelationalExprOperator::get_const_cast_mode(op_type, true);
              new_expr->set_extra(new_expr->get_extra() | extra_cm);
            }
            sub_expr2 = new_expr;
          }
          break;
        }
        case ImplicitCastDirection::IC_A_TO_C: {
          ObAccuracy acc = (ObNumberType == r_type3 ? ObAccuracy::DDL_DEFAULT_ACCURACY2[1][r_type3] : ObAccuracy::MAX_ACCURACY2[1][r_type3]);
          dest_type.set_precision(acc.get_precision());
          dest_type.set_scale(acc.get_scale());
          dest_type.set_type(r_type3);
          if (ob_is_nstring_type(dest_type.get_type())) {
            dest_type.set_collation_type(OB_NOT_NULL(session_info)
                                ? session_info->get_nls_collation_nation() : CS_TYPE_UTF16_BIN);
          }
          if (OB_FAIL(resolve_op_expr_add_implicit_cast(expr_factory,
                                                        session_info,
                                                        sub_expr1,
                                                        dest_type,
                                                        new_expr))) {
            LOG_WARN("create cast expr for implicit failed", K(ret));
          } else if (OB_ISNULL(new_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null expr", K(ret));
          } else if (OB_FAIL(new_expr->add_flag(IS_INNER_ADDED_EXPR))) {
            LOG_WARN("failed to add flag", K(ret));
          } else {
            sub_expr1 = new_expr;
          }
          break;
        }
        case ImplicitCastDirection::IC_B_TO_C: {
          ObAccuracy acc = (ObNumberType == r_type3 ? ObAccuracy::DDL_DEFAULT_ACCURACY2[1][r_type3] : ObAccuracy::MAX_ACCURACY2[1][r_type3]);
          dest_type.set_precision(acc.get_precision());
          dest_type.set_scale(acc.get_scale());
          dest_type.set_type(r_type3);
          if (ob_is_nstring_type(dest_type.get_type())) {
            dest_type.set_collation_type(OB_NOT_NULL(session_info)
                                    ? session_info->get_nls_collation_nation() : CS_TYPE_UTF16_BIN);
          }
          if (OB_FAIL(resolve_op_expr_add_implicit_cast(expr_factory,
                                                        session_info,
                                                        sub_expr2,
                                                        dest_type,
                                                        new_expr))) {
          LOG_WARN("create cast expr for implicit failed", K(ret));
          } else if (OB_ISNULL(new_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null expr", K(ret));
          } else if (OB_FAIL(new_expr->add_flag(IS_INNER_ADDED_EXPR))) {
            LOG_WARN("failed to add flag", K(ret));
          } else {
            sub_expr2 = new_expr;
          }
          break;
        }
        case ImplicitCastDirection::IC_TO_MIDDLE_TYPE: {
          ObAccuracy acc = (ObNumberType == middle_type ? ObAccuracy::DDL_DEFAULT_ACCURACY2[1][middle_type] : ObAccuracy::MAX_ACCURACY2[1][middle_type]);
          dest_type.set_precision(acc.get_precision());
          dest_type.set_scale(acc.get_scale());
          dest_type.set_collation_level(CS_LEVEL_NUMERIC);
          dest_type.set_type(middle_type);
          if (ob_is_nstring_type(dest_type.get_type())) {
            dest_type.set_collation_type(OB_NOT_NULL(session_info)
                                    ? session_info->get_nls_collation_nation() : CS_TYPE_UTF16_BIN);
          }
          ObObjTypeClass middle_tc = OBJ_O_TYPE_TO_CLASS[middle_type];

          //optimization: only need to cast string type to number type
          if (middle_type == ObNumberType) {
            if (ObStringTC == tc1 && (ObIntTC == tc2 || ObFloatTC == tc2 ||
                                      ObDoubleTC == tc2 || ObNumberTC == tc2)) {
              if (OB_FAIL(ObRawExprUtils::create_cast_expr(expr_factory,
                                                            sub_expr1,
                                                            dest_type,
                                                            new_expr,
                                                            session_info))) {
                LOG_WARN("create cast expr for implicit failed", K(ret));
              } else if (OB_ISNULL(new_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpect null expr", K(ret));
              } else if (OB_FAIL(new_expr->add_flag(IS_INNER_ADDED_EXPR))) {
                LOG_WARN("failed to add flag", K(ret));
              } else {
                sub_expr1 = new_expr;
              }
            } else if (ObStringTC == tc2 && (ObIntTC == tc1 || ObFloatTC == tc1 ||
                                            ObDoubleTC == tc1 || ObNumberTC == tc1)) {
              if (OB_FAIL(ObRawExprUtils::create_cast_expr(expr_factory,
                                                            sub_expr2,
                                                            dest_type,
                                                            new_expr2,
                                                            session_info))) {
                LOG_WARN("create cast expr for implicit failed", K(ret));
              } else if (OB_ISNULL(new_expr2)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpect null expr", K(ret));
              } else if (OB_FAIL(new_expr2->add_flag(IS_INNER_ADDED_EXPR))) {
                LOG_WARN("failed to add flag", K(ret));
              } else {
                sub_expr2 = new_expr2;
              }
            } else {
              LOG_WARN("create cast expr for implicit failed unexpected TO_MIDDLE_TYPE", K(tc1), K(tc2));
            }
          } else {
            if (middle_tc != tc1) {
              if (OB_FAIL(resolve_op_expr_add_implicit_cast(expr_factory,
                                                            session_info,
                                                            sub_expr1,
                                                            dest_type,
                                                            new_expr))) {
                LOG_WARN("create cast expr for implicit failed", K(ret));
              } else if (OB_ISNULL(new_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpect null expr", K(ret));
              } else if (OB_FAIL(new_expr->add_flag(IS_INNER_ADDED_EXPR))) {
                LOG_WARN("failed to add flag", K(ret));
              } else {
                sub_expr1 = new_expr;
              }
            }
            if (middle_tc != tc2) {
              if (OB_FAIL(resolve_op_expr_add_implicit_cast(expr_factory,
                                                            session_info,
                                                            sub_expr2,
                                                            dest_type,
                                                            new_expr2))) {
                LOG_WARN("create cast expr for implicit failed", K(ret));
              } else if (OB_ISNULL(new_expr2)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpect null expr", K(ret));
              } else if (OB_FAIL(new_expr2->add_flag(IS_INNER_ADDED_EXPR))) {
                LOG_WARN("failed to add flag", K(ret));
              } else {
                sub_expr2 = new_expr2;
              }
            }
          }
          break;
        }
        case ImplicitCastDirection::IC_NOT_SUPPORT:
        {
          //ObString type1_str = ObString(ob_obj_type_str(r_type1));
          //ObString type2_str = ObString(ob_obj_type_str(r_type2));
          //return warning or error when can't do implicite datatype convert
          bool is_error = (session_info != NULL && ((session_info->get_sql_mode() & SMO_ERROR_ON_RESOLVE_CAST) > 0))
                        || (sub_expr1->get_result_type().is_blob()
                        || sub_expr2->get_result_type().is_blob()
                        || sub_expr1->get_result_type().is_blob_locator()
                        || sub_expr2->get_result_type().is_blob_locator());
          if (is_error) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(r_type1), ob_obj_type_str(r_type2));
          } else {
            if (sub_expr1->is_called_in_sql() && sub_expr2->is_called_in_sql()) {
              LOG_USER_WARN(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(r_type1), ob_obj_type_str(r_type2));
            }
          }
          LOG_WARN("expr get oracle implicit cast direction failed", K(is_error));
          break;
        }
        }
      }
    }
  }
  return ret;
}

int ObRawExprUtils::resolve_op_expr_for_oracle_implicit_cast(ObRawExprFactory &expr_factory,
                                                             const ObSQLSessionInfo *session_info,
                                                             ObOpRawExpr* &b_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr   *sub_expr1 = NULL;
  ObRawExpr   *sub_expr2 = NULL;
  sub_expr1 = b_expr->get_param_expr(0);
  sub_expr2 = b_expr->get_param_expr(1);

  if (OB_ISNULL(sub_expr1) || OB_ISNULL(sub_expr2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(sub_expr1), K(sub_expr2));
  } else if (T_OP_ROW == sub_expr1->get_expr_type() ||
             T_OP_ROW == sub_expr2->get_expr_type()) {
    //左(右)子节点为 T_OP_ROW 类型的 ObOpRawExpr 不会显式 cast
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get T_OP_ROW expr", K(ret), K(*sub_expr1), K(*sub_expr2));
  } else {
    if (!sub_expr1->is_query_ref_expr() && sub_expr2->is_query_ref_expr()) {
      // t1.c1 =any (select c1 from t2)
      // 按正常流程走query_ref_expr的返回类型固定为bigint，应该取返回列的类型比较
      ObQueryRefRawExpr *query_ref_expr = static_cast<ObQueryRefRawExpr *>(sub_expr2);
      ObSelectStmt *query_stmt = query_ref_expr->get_ref_stmt();
      ObRawExpr *select_expr = NULL;
      if (OB_ISNULL(query_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(query_stmt));
      } else if (OB_UNLIKELY(1 != query_stmt->get_select_item_size())) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, 1L);
        LOG_WARN("query_ref_expr should have 1 output column", K(query_ref_expr->get_output_column()));
      } else if (OB_ISNULL(select_expr = query_stmt->get_select_item(0).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(select_expr));
      } else if (OB_FAIL(resolve_op_expr_implicit_cast(expr_factory,
                                                      session_info,
                                                      b_expr->get_expr_type(),
                                                      sub_expr1,
                                                      select_expr))) {
        LOG_WARN("failed to resolve_op_expr_implicit_cast", K(ret));
      } else if (OB_ISNULL(select_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FAIL(b_expr->replace_param_expr(0, sub_expr1))) {
        LOG_WARN("failed to replace_param_expr", K(ret));
      } else {
        query_stmt->get_select_item(0).expr_ = select_expr;
        ObIArray<ObExprResType> &column_types = query_ref_expr->get_column_types();
        column_types.reset();
        if (OB_FAIL(column_types.push_back(select_expr->get_result_type()))) {
          LOG_WARN("add column type failed", K(ret));
        }
      }
    } else if (sub_expr1->is_query_ref_expr() && sub_expr2->is_query_ref_expr()) {
      // (select c1 from t2 where t2.c1=1) =any (select c1 from t2)
      ObQueryRefRawExpr *query_ref_expr1 = static_cast<ObQueryRefRawExpr *>(sub_expr1);
      ObQueryRefRawExpr *query_ref_expr2 = static_cast<ObQueryRefRawExpr *>(sub_expr2);
      ObSelectStmt *query_stmt1 = query_ref_expr1->get_ref_stmt();
      ObSelectStmt *query_stmt2 = query_ref_expr2->get_ref_stmt();
      ObRawExpr *select_expr1 = NULL;
      ObRawExpr *select_expr2 = NULL;
      if (OB_ISNULL(query_stmt1) || OB_ISNULL(query_stmt2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(query_stmt1), K(query_stmt2));
      } else if (query_stmt1->get_select_item_size() != query_stmt2->get_select_item_size()) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_WARN("query expr should same output column", K(query_ref_expr1->get_output_column()),
                                                         K(query_ref_expr2->get_output_column()));
      } else {
        ObIArray<ObExprResType> &column_types1 = query_ref_expr1->get_column_types();
        column_types1.reset();
        ObIArray<ObExprResType> &column_types2 = query_ref_expr2->get_column_types();
        column_types2.reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < query_stmt1->get_select_item_size(); ++i) {
          if (OB_ISNULL(select_expr1 = query_stmt1->get_select_item(i).expr_) ||
              OB_ISNULL(select_expr2 = query_stmt2->get_select_item(i).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(select_expr1), K(select_expr2));
          } else if (OB_FAIL(resolve_op_expr_implicit_cast(expr_factory,
                                                          session_info,
                                                          b_expr->get_expr_type(),
                                                          select_expr1,
                                                          select_expr2))) {
            LOG_WARN("failed to resolve_op_expr_implicit_cast", K(ret));
          } else if (OB_ISNULL(select_expr1) || OB_ISNULL(select_expr2)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null expr", K(ret));
          } else if (OB_FAIL(column_types1.push_back(select_expr1->get_result_type()))) {
            LOG_WARN("add column type failed", K(ret));
          } else if (OB_FAIL(column_types2.push_back(select_expr2->get_result_type()))) {
            LOG_WARN("add column type failed", K(ret));
          } else {
            query_stmt1->get_select_item(i).expr_ = select_expr1;
            query_stmt2->get_select_item(i).expr_ = select_expr2;
          }
        }
      }
    } else {
      if (OB_FAIL(resolve_op_expr_implicit_cast(expr_factory,
                                                session_info,
                                                b_expr->get_expr_type(),
                                                sub_expr1,
                                                sub_expr2))) {
        LOG_WARN("failed to resolve_op_expr_implicit_cast", K(ret));
      } else if (OB_FAIL(b_expr->replace_param_expr(0, sub_expr1))) {
        LOG_WARN("failed to replace_param_expr", K(ret));
      } else if (OB_FAIL(b_expr->replace_param_expr(1, sub_expr2))) {
        LOG_WARN("failed to replace_param_expr", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::resolve_op_exprs_for_oracle_implicit_cast(ObRawExprFactory &expr_factory,
                                                              const ObSQLSessionInfo *session_info,
                                                              ObIArray<ObOpRawExpr*> &op_exprs)
{
  int ret = OB_SUCCESS;
  if (session_info == NULL){
    LOG_WARN("can't get compatibility mode from session_info", K(session_info));
  } else {
    ObOpRawExpr *b_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < op_exprs.count(); i++) {
      b_expr = op_exprs.at(i);
      if (OB_ISNULL(b_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (b_expr->get_param_count() != 2) {
        //TODO Molly case when
        LOG_WARN("IMPLICIT UNEXPECTED BEXPR", K(*b_expr));
      } else if (OB_FAIL(resolve_op_expr_for_oracle_implicit_cast(expr_factory, session_info,
          b_expr))){
        LOG_WARN("IMPLICIT UNEXPECTED BEXPR", K(*b_expr));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::resolve_udf_common_info(const ObString &db_name,
                                            const ObString &package_name,
                                            int64_t udf_id,
                                            int64_t package_id,
                                            const ObIArray<int64_t> &subprogram_path,
                                            int64_t udf_schema_version,
                                            int64_t pkg_schema_version,
                                            bool is_deterministic,
                                            bool is_parallel_enable,
                                            bool is_pkg_body_udf,
                                            bool is_pl_agg,
                                            int64_t type_id,
                                            ObUDFInfo &udf_info,
                                            uint64_t dblink_id,
                                            const ObString &dblink_name)
{
  int ret = OB_SUCCESS;
  ObUDFRawExpr *udf_raw_expr = udf_info.ref_expr_;
  CK (OB_NOT_NULL(udf_raw_expr));
  OZ (udf_raw_expr->set_database_name(db_name));
  OX (udf_raw_expr->set_package_name(package_name));
  OZ (udf_raw_expr->set_subprogram_path(subprogram_path));
  OX (udf_raw_expr->set_udf_id(udf_id));
  OX (udf_raw_expr->set_pkg_id(package_id));
  OX (udf_raw_expr->set_is_deterministic(is_deterministic));
  OX (udf_raw_expr->set_parallel_enable(is_parallel_enable));
  OX (udf_raw_expr->set_udf_schema_version(udf_schema_version));
  OX (udf_raw_expr->set_pkg_schema_version(pkg_schema_version));
  OX (udf_raw_expr->set_pkg_body_udf(is_pkg_body_udf));
  OX (udf_raw_expr->set_type_id(type_id));
  OX (udf_raw_expr->set_is_aggregate_udf(is_pl_agg));
  OX (udf_raw_expr->set_dblink_id(dblink_id));
  OX (udf_raw_expr->set_dblink_name(dblink_name));
  return ret;
}

int ObRawExprUtils::resolve_udf_param_types(const ObIRoutineInfo* func_info,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            sql::ObSQLSessionInfo &session_info,
                                            common::ObIAllocator &allocator,
                                            common::ObMySQLProxy &sql_proxy,
                                            ObUDFInfo &udf_info,
                                            pl::ObPLDbLinkGuard &dblink_guard)
{
  int ret = OB_SUCCESS;

#define SET_RES_TYPE_BY_PL_TYPE(res_type, pl_type) \
  if (OB_SUCC(ret)) { \
    ObObjMeta meta; \
    if (pl_type.is_obj_type()) { \
      meta = pl_type.get_data_type()->get_meta_type(); \
      meta.set_scale(meta.is_bit() ? \
          pl_type.get_data_type()->get_accuracy().get_precision() : \
          pl_type.get_data_type()->get_accuracy().get_scale()); \
      res_type.set_meta(meta); \
      res_type.set_accuracy(pl_type.get_data_type()->get_accuracy()); \
    } else { \
      meta.set_ext(); \
      res_type.set_meta(meta); \
      res_type.set_extend_type(pl_type.get_type());\
      res_type.set_udt_id(pl_type.get_user_type_id()); \
    } \
  }

  ObUDFRawExpr *udf_raw_expr = udf_info.ref_expr_;
  CK (OB_NOT_NULL(udf_raw_expr));

  ObExprResType result_type;
  ObSEArray<ObExprResType, 5> params_type;
  const ObIArray<ObString> *extended_type_info = NULL;

  // Step1: 处理Routine返回值
  const ObIRoutineParam *ret_param = func_info->get_ret_info();
  pl::ObPLDataType ret_pl_type;
  if (OB_SUCC(ret) && OB_NOT_NULL(ret_param)) {
    if (ret_param->is_schema_routine_param()) {
      const ObRoutineParam *iparam = static_cast<const ObRoutineParam*>(ret_param);
      CK (OB_NOT_NULL(iparam));
      OZ (pl::ObPLDataType::transform_from_iparam(iparam,
                                                  schema_guard,
                                                  session_info,
                                                  allocator,
                                                  sql_proxy,
                                                  ret_pl_type,
                                                  NULL,
                                                  &dblink_guard));
    } else {
      OX (ret_pl_type = ret_param->get_pl_data_type());
    }
    if (ret_pl_type.is_ref_cursor_type() || ret_pl_type.is_sys_refcursor_type()) {
      OX (udf_raw_expr->set_is_return_sys_cursor(true));
    }
    SET_RES_TYPE_BY_PL_TYPE(result_type, ret_pl_type);
    if (OB_SUCC(ret)
        && lib::is_oracle_mode()
        && OB_INVALID_ID != func_info->get_package_id()
        && is_sys_tenant(pl::get_tenant_id_by_object_id(func_info->get_package_id()))
        && OB_NOT_NULL(ret_pl_type.get_data_type())
        && !(ret_pl_type.get_data_type()->get_meta_type().get_type() == ObLongTextType)
        && ob_is_string_type(ret_pl_type.get_data_type()->get_meta_type().get_type())) {
      result_type.set_collation_type(ob_is_nstring(ret_pl_type.get_data_type()
                                                           ->get_meta_type().get_type())
           ? session_info.get_nls_collation_nation()
             : session_info.get_nls_collation());
    }
    OX (udf_raw_expr->set_pls_type(ret_pl_type.get_pl_integer_type()));
    if (OB_FAIL(ret)) {
    } else if (!ret_pl_type.is_obj_type()) {
      OX (result_type.set_udt_id(ret_pl_type.get_user_type_id()));
    } else if (result_type.is_enum_or_set()) {
      const ObRoutineParam* r_param = static_cast<const ObRoutineParam*>(ret_param);
      CK (OB_NOT_NULL(r_param));
      OX (extended_type_info = &(r_param->get_extended_type_info()));
    }
  }
  // Step2: 处理入参
  for (int64_t i = 0; OB_SUCC(ret) && i < func_info->get_param_count(); ++i) {
    ObIRoutineParam *iparam = NULL;
    pl::ObPLDataType param_pl_type;
    ObExprResType param_type;
    OZ (func_info->get_routine_param(i, iparam));
    CK (OB_NOT_NULL(iparam));
    if (OB_FAIL(ret)) {
    } else if (iparam->is_schema_routine_param()) {
      const ObRoutineParam *rparam = static_cast<const ObRoutineParam*>(iparam);
      CK (OB_NOT_NULL(rparam));
      OZ (pl::ObPLDataType::transform_from_iparam(rparam,
                                                  schema_guard,
                                                  session_info,
                                                  allocator,
                                                  sql_proxy,
                                                  param_pl_type,
                                                  NULL,
                                                  &dblink_guard));
    } else {
      OX (param_pl_type = iparam->get_pl_data_type());
    }
    SET_RES_TYPE_BY_PL_TYPE(param_type, param_pl_type);
    OZ (params_type.push_back(param_type));
  }
  // Step3: 将入参返回值类型加入rawexpr
  OX (udf_raw_expr->set_result_type(result_type));
  OZ (udf_raw_expr->set_params_type(params_type));
  if (OB_NOT_NULL(extended_type_info)) {
    OX (udf_raw_expr->set_enum_set_values(*extended_type_info));
  }

#undef SET_RES_TYPE_BY_PL_TYPE
  return ret;
}

int ObRawExprUtils::resolve_udf_param_exprs(const ObIRoutineInfo* func_info,
                                            pl::ObPLBlockNS &secondary_namespace_,
                                            ObSchemaChecker &schema_checker,
                                            sql::ObSQLSessionInfo &session_info,
                                            ObIAllocator &allocator,
                                            bool is_prepare_protocol,
                                            sql::ObRawExprFactory &expr_factory,
                                            common::ObMySQLProxy &sql_proxy,
                                            ExternalParams *extern_param_info,
                                            ObUDFInfo &udf_info)
{
  int ret = OB_SUCCESS;
  ObResolverParams params;
  params.secondary_namespace_ = &(secondary_namespace_);
  params.schema_checker_ = &(schema_checker);
  params.session_info_ = &(session_info);
  params.allocator_ = &(allocator);
  params.is_prepare_protocol_ = is_prepare_protocol;
  params.expr_factory_ = &(expr_factory);
  params.sql_proxy_ = &(sql_proxy);
  if (OB_NOT_NULL(extern_param_info)) {
    params.external_param_info_.assign(*extern_param_info);
  }
  if (OB_FAIL(resolve_udf_param_exprs(params, func_info, udf_info))) {
    SQL_LOG(WARN, "failed to exec resovle udf exprs", K(ret), K(udf_info));
  }
  return ret;
}

/*!
 * 解析UDF的参数列表(主要处理参数有默认值的情况, 以及通过名字指定参数的情况):
 * 通过名字指定参数, 一定在参数列表的最后面
 * 如: func(1, 2, x=>3, y=>4); 合法
 *     func(1, x=>3, 2); 非法(无法确定2的位置)
 * 走到这个函数时在参数列表中没有通过名字指定的参数已经被加入到udf的raw expr
 * 因此这个函数主要处理通过名字指定参数的部分
 * 如果处理完所有的参数后还有参数空缺, 则尝试下是不是有默认值, 如果没有默认值则报错
 */
int ObRawExprUtils::resolve_udf_param_exprs(ObResolverParams &params,
                                            const ObIRoutineInfo *func_info,
                                            ObUDFInfo &udf_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> param_exprs;
  ObArray<ObString> param_names;
  ObUDFRawExpr *udf_raw_expr = udf_info.ref_expr_;
  // 通过名字指定参数统一记录在param_names_和param_exprs里面, 所以这里一定相等
  if (udf_info.param_names_.count() != udf_info.param_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "names array not equal to exprs array count",
             K(ret), K(udf_info.param_names_.count()), K(udf_info.param_exprs_.count()));
  } else if ((udf_info.udf_param_num_ + udf_info.param_names_.count()) > func_info->get_param_count()) {
    ret = OB_ERR_SP_WRONG_ARG_NUM;
    LOG_USER_ERROR(OB_ERR_SP_WRONG_ARG_NUM, "FUNCTION", udf_info.udf_name_.ptr(),
                   static_cast<uint32_t>(func_info->get_param_count()),
                   static_cast<uint32_t>(udf_info.udf_param_num_ + udf_info.param_names_.count()));
    SQL_LOG(WARN, "params count mismatch",
             K(ret), K(udf_info.udf_name_), K(func_info->get_param_count()), K(udf_info));
  } else {
    // 处理剩余的参数, 默认值或者通过名字指定的参数
    // Step 1: 首先初始化一个空的参数列表
    int64_t count = func_info->get_param_count() - udf_info.udf_param_num_;
    for (int64_t i = 0; OB_SUCC(ret) && i < udf_info.udf_param_num_; ++i) {
      ObString empty;
      OZ (udf_raw_expr->add_param_name(empty));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(param_exprs.push_back(NULL))) {
        SQL_LOG(WARN, "failed to push back", K(ret), K(i), K(udf_info));
      } else if (OB_FAIL(param_names.push_back(ObString()))) {
        SQL_LOG(WARN, "failed to push back", K(ret), K(i), K(udf_info));
      }
    }
    // Step 2: 将通过名字指定的参数加入参数列表
    for (int64_t i = 0; OB_SUCC(ret) && i < udf_info.param_names_.count(); ++i) {
      const ObString &name = udf_info.param_names_.at(i);
      int64_t position = -1;
      if (OB_FAIL(func_info->find_param_by_name(name, position))) {
        SQL_LOG(WARN, "failed to find param by name", K(ret));
      } else if (position < udf_info.udf_param_num_) {
        ret = OB_ERR_SP_DUP_VAR;
        SQL_LOG(WARN, "parameter dup", K(ret), K(name), K(position), K(i), K(udf_info));
      } else {
        // 注意: 加入到参数列表中需要减去未通过名字指定的参数个数
        param_exprs.at(position - udf_info.udf_param_num_) = udf_info.param_exprs_.at(i);
        param_names.at(position - udf_info.udf_param_num_) = name;
      }
    }
    // Step 3: 处理空缺的参数
    for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); ++i) {
      if (OB_ISNULL(param_exprs.at(i))) {
        const ParseNode *default_node = NULL;
        ObRawExpr *default_expr = NULL;
        ObConstRawExpr *const_default_expr = NULL;
        ObString default_val;
        ObIRoutineParam *routine_param = NULL;

        if (OB_FAIL(func_info->get_routine_param(i + udf_info.udf_param_num_, routine_param))) {
          SQL_LOG(WARN, "failed to get routine param", K(ret), K(i), K(udf_info));
        } else if (FALSE_IT(default_val = routine_param->get_default_value())) {
        } else if (OB_FAIL(ObSQLUtils::convert_sql_text_from_schema_for_resolve(
                      *(params.allocator_), params.session_info_->get_dtc_params(), default_val))) {
          LOG_WARN("fail to get default value", K(ret));
        } else if (OB_UNLIKELY(default_val.empty())) {
          ret = OB_ERR_SP_WRONG_ARG_NUM;
          LOG_USER_ERROR(OB_ERR_SP_WRONG_ARG_NUM, "FUNCTION", udf_info.udf_name_.ptr(),
                         static_cast<uint32_t>(func_info->get_param_count()),
                         static_cast<uint32_t>(udf_info.udf_param_num_ + udf_info.param_names_.count()));
          SQL_LOG(WARN, "param count mismatch", K(ret), K(i), K(default_val));
        } else if (OB_FAIL(ObRawExprUtils::parse_default_expr_from_str(
            default_val, params.session_info_->get_charsets4parser(),
            *(params.allocator_), default_node))) {
          SQL_LOG(WARN, "failed to parse expr node from str", K(ret), K(i), K(default_val), K(udf_info));
        } else if (OB_ISNULL(default_node)
                   || OB_ISNULL(params.allocator_)
                   || OB_ISNULL(params.expr_factory_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "parameter is null",
                  K(ret), K(i), K(default_val), K(udf_info), K(default_node),
                  K(params.allocator_), K(params.expr_factory_), K(params.secondary_namespace_));
        } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                            *(params.expr_factory_), ObNullType, 0, const_default_expr))) {
          SQL_LOG(WARN, "failed build const int expr for default expr", K(ret), K(i));
        } else {
          ObObjMeta null_meta;
          null_meta.set_null();
          OX (default_expr = const_default_expr);
          CK (OB_NOT_NULL(default_expr));
          OX (const_default_expr->set_meta_type(null_meta));
          OX (const_default_expr->set_expr_obj_meta(null_meta));
          OX (param_exprs.at(i) = default_expr);
          // rewrite param type, for do not cast default value
          CK(udf_raw_expr->get_param_count() < udf_raw_expr->get_params_type().count());
          OX (udf_raw_expr->get_params_type().at(
            udf_raw_expr->get_param_count()).set_meta(null_meta));
        }
      }
      OZ (udf_raw_expr->add_param_expr(param_exprs.at(i)));
      OZ (udf_raw_expr->add_param_name(param_names.at(i)));
    }
    OV ((udf_info.udf_param_num_ + param_exprs.count()) == udf_raw_expr->get_param_count(),
      OB_ERR_UNEXPECTED, K(udf_info.udf_param_num_), K(param_exprs.count()), K(udf_raw_expr->get_param_count()));
  }
  if (OB_SUCC(ret)
      && (func_info->get_param_count() != udf_info.udf_param_num_ + param_exprs.count())) {
    ret = OB_ERR_SP_WRONG_ARG_NUM;
    LOG_USER_ERROR(OB_ERR_SP_WRONG_ARG_NUM, "FUNCTION", udf_info.udf_name_.ptr(),
                   static_cast<uint32_t>(func_info->get_param_count()),
                   static_cast<uint32_t>(udf_info.udf_param_num_ + udf_info.param_names_.count()));
    SQL_LOG(WARN, "params count mismatch",
             K(ret), K(udf_info.udf_name_),
             K(func_info->get_param_count()), K(udf_info));
  }
  // Step 4: 处理function的OUT参数
  for (int64_t i = 0; OB_SUCC(ret) && i < func_info->get_param_count(); ++i) {
    ObIRoutineParam* iparam = NULL;
    pl::ObPLRoutineParamMode mode = pl::ObPLRoutineParamMode::PL_PARAM_INVALID;
    OZ (func_info->get_routine_param(i, iparam));
    CK (OB_NOT_NULL(iparam));
    OX (mode = static_cast<pl::ObPLRoutineParamMode>(iparam->get_mode()));
    if (OB_SUCC(ret)) {
#ifdef OB_BUILD_ORACLE_PL
      if (iparam->is_nocopy_param()
          && pl::ObPLRoutineParamMode::PL_PARAM_INOUT == mode
          && OB_NOT_NULL(udf_raw_expr->get_param_expr(i))
          && pl::ObPlJsonUtil::is_pl_jsontype(udf_raw_expr->get_param_expr(i)->get_udt_id())) {
        OZ (udf_raw_expr->add_param_desc(ObUDFParamDesc()));
      } else
#endif
      if (pl::ObPLRoutineParamMode::PL_PARAM_OUT == mode
          || pl::ObPLRoutineParamMode::PL_PARAM_INOUT == mode) {
        ObRawExpr* iexpr = udf_raw_expr->get_param_expr(i);
        CK (OB_NOT_NULL(iexpr));
        if (OB_SUCC(ret)) { // udf output parameter, change param type to output type.
          ObExprResType result_type;
          OZ (iexpr->formalize(params.session_info_));
          OX (result_type = iexpr->get_result_type());
          if (OB_SUCC(ret) && result_type.is_valid() && !result_type.is_null()) {
            CK (udf_raw_expr->get_params_type().count() > i);
            OX (udf_raw_expr->get_params_type().at(i) = result_type);
          }
        }
        if (OB_SUCC(ret)) {
          bool is_anonymos_const_var = false;
          if (T_QUESTIONMARK == iexpr->get_expr_type() && nullptr != params.secondary_namespace_) {
            ObConstRawExpr *c_expr = static_cast<ObConstRawExpr*>(iexpr);
            const pl::ObPLVar* var = NULL;
            const pl::ObPLSymbolTable* symbol_table = params.secondary_namespace_->get_symbol_table();
            if (!params.is_prepare_protocol_ &&
                OB_NOT_NULL(symbol_table) &&
                OB_NOT_NULL(var = symbol_table->get_symbol(c_expr->get_value().get_unknown())) &&
                0 == var->get_name().case_compare(pl::ObPLResolver::ANONYMOUS_ARG)) {
              OX (is_anonymos_const_var = true);
            }
          }
          if (T_QUESTIONMARK == iexpr->get_expr_type() && !is_anonymos_const_var) {
            // 如果UDF出现在PL的DML语句中, 走到这里的ObjAccessRawExpr已经被替换为QuestionMark
            // 我们需要找到原始的ObjAccessRawExpr, 并设置fow_write属性
            ObConstRawExpr *c_expr = static_cast<ObConstRawExpr*>(iexpr);
            ExternalParams& extern_params = params.external_param_info_;
            for (int i = 0; OB_SUCC(ret) && i < extern_params.count(); ++i) {
              if (extern_params.at(i).element<1>()->same_as(*c_expr)) {
                ObRawExpr *rawexpr = extern_params.at(i).element<0>();
                if (T_OBJ_ACCESS_REF == rawexpr->get_expr_type()) {
                  OZ (pl::ObPLResolver::set_write_property(
                    rawexpr, *(params.expr_factory_), params.session_info_, params.schema_checker_->get_schema_guard(), true));
                }
                break;
              }
            }
          }
#define GET_CONST_EXPR_VALUE(expr, val)                                         \
do {                                                                            \
  const ObConstRawExpr *c_expr = static_cast<const ObConstRawExpr*>(expr);      \
  CK (OB_NOT_NULL(c_expr));                                                     \
  CK (c_expr->get_value().is_uint64()                                           \
      || c_expr->get_value().is_int()                                           \
      || c_expr->get_value().is_unknown());                                     \
  OX (val = c_expr->get_value().is_uint64() ? c_expr->get_value().get_uint64()  \
        : c_expr->get_value().is_int() ? c_expr->get_value().get_int()          \
        : c_expr->get_value().get_unknown());                                   \
} while (0)
          if (OB_FAIL(ret)) {
          } else if(T_NULL == iexpr->get_expr_type() && 0 == i && udf_info.is_udf_udt_cons()) {
            // do nothing, udt constructor first param is mocked with null expr
            OZ (udf_raw_expr->add_param_desc(ObUDFParamDesc()));
          } else if (T_QUESTIONMARK != iexpr->get_expr_type()
                     && T_OBJ_ACCESS_REF != iexpr->get_expr_type()
                     && T_OP_GET_PACKAGE_VAR != iexpr->get_expr_type()
                     && T_OP_GET_SUBPROGRAM_VAR != iexpr->get_expr_type()) {
            ret = OB_ER_SP_NOT_VAR_ARG;
            LOG_WARN("OUT or INOUT argument for routine is not a variable",
                     K(iexpr->get_expr_type()), K(ret));
          } else if (T_QUESTIONMARK == iexpr->get_expr_type() && is_anonymos_const_var) {
            ret = OB_ER_SP_NOT_VAR_ARG;
            LOG_WARN("OUT or INOUT argument for routine is not a variable",
                     K(iexpr->get_expr_type()), K(ret));
          } else if (T_OBJ_ACCESS_REF == iexpr->get_expr_type()) {
            ObObjAccessRawExpr* obj = static_cast<ObObjAccessRawExpr*>(iexpr);
            uint64_t pkg_id = OB_INVALID_ID;
            uint64_t var_id = OB_INVALID_ID;
            OZ (pl::ObPLResolver::set_write_property(
                iexpr, *(params.expr_factory_), params.session_info_, params.schema_checker_->get_schema_guard(), true));
            if (obj->get_access_idxs().count() > 0 &&
                OB_NOT_NULL(obj->get_access_idxs().at(0).get_sysfunc_) &&
                T_OP_GET_PACKAGE_VAR == obj->get_access_idxs().at(0).get_sysfunc_->get_expr_type()) {
              const ObSysFunRawExpr *f_expr = static_cast<const ObSysFunRawExpr *>(obj->get_access_idxs().at(0).get_sysfunc_);
              CK (OB_NOT_NULL(f_expr) && f_expr->get_param_count() >= 2);
              GET_CONST_EXPR_VALUE(f_expr->get_param_expr(0), pkg_id);
              GET_CONST_EXPR_VALUE(f_expr->get_param_expr(1), var_id);
            }
            OZ (udf_raw_expr->add_param_desc(
                ObUDFParamDesc(ObUDFParamDesc::OBJ_ACCESS_OUT, var_id, OB_INVALID_ID, pkg_id)));
          } else if (T_QUESTIONMARK == iexpr->get_expr_type()) {
            ObConstRawExpr *c_expr = static_cast<ObConstRawExpr*>(iexpr);
            pl::ObPLDataType param_type;
            CK (OB_NOT_NULL(c_expr));
            CK (c_expr->get_value().is_unknown());
            OZ (udf_raw_expr->add_param_desc(
              ObUDFParamDesc(ObUDFParamDesc::LOCAL_OUT, c_expr->get_value().get_unknown())));
            if (OB_FAIL(ret) || !pl::ObPLResolver::is_question_mark_value(iexpr, params.secondary_namespace_)) {
              // do nothing ...
            } else {
              if (iparam->is_schema_routine_param()) {
                ObRoutineParam *param = static_cast<ObRoutineParam*>(iparam);
                CK (OB_NOT_NULL(param));
                CK (OB_NOT_NULL(params.schema_checker_));
                CK (OB_NOT_NULL(params.schema_checker_->get_schema_guard()));
                CK (OB_NOT_NULL(params.session_info_));
                CK (OB_NOT_NULL(params.allocator_));
                CK (OB_NOT_NULL(params.sql_proxy_));
                OZ (pl::ObPLDataType::transform_from_iparam(param,
                                                          *(params.schema_checker_->get_schema_guard()),
                                                          *(params.session_info_),
                                                          *(params.allocator_),
                                                          *(params.sql_proxy_),
                                                          param_type));
              } else {
                param_type = iparam->get_pl_data_type();
              }
              OZ (pl::ObPLResolver::set_question_mark_type(iexpr, params.secondary_namespace_, &param_type));
            }
          } else if (T_OP_GET_PACKAGE_VAR == iexpr->get_expr_type()) {
            const ObSysFunRawExpr *f_expr = static_cast<const ObSysFunRawExpr *>(iexpr);
            uint64_t pkg_id = OB_INVALID_ID;
            uint64_t var_id = OB_INVALID_ID;
            CK (OB_NOT_NULL(f_expr) && f_expr->get_param_count() >= 2);
            GET_CONST_EXPR_VALUE(f_expr->get_param_expr(0), pkg_id);
            GET_CONST_EXPR_VALUE(f_expr->get_param_expr(1), var_id);
            OZ (udf_raw_expr->add_param_desc(
              ObUDFParamDesc(ObUDFParamDesc::PACKAGE_VAR_OUT, var_id, OB_INVALID_ID, pkg_id)));
          } else if (T_OP_GET_SUBPROGRAM_VAR == iexpr->get_expr_type()) {
            const ObSysFunRawExpr *f_expr = static_cast<const ObSysFunRawExpr *>(iexpr);
            uint64_t pkg_id = OB_INVALID_ID;
            uint64_t sub_id = OB_INVALID_ID;
            uint64_t var_id = OB_INVALID_ID;
            CK (OB_NOT_NULL(f_expr) && f_expr->get_param_count() >= 3);
            GET_CONST_EXPR_VALUE(f_expr->get_param_expr(0), pkg_id);
            GET_CONST_EXPR_VALUE(f_expr->get_param_expr(1), sub_id);
            GET_CONST_EXPR_VALUE(f_expr->get_param_expr(2), var_id);
            OZ (udf_raw_expr->add_param_desc(
              ObUDFParamDesc(ObUDFParamDesc::SUBPROGRAM_VAR_OUT, var_id, sub_id, pkg_id)));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpecte function out expr", K(ret), KPC(iexpr));
          }
        }
#undef GET_CONST_EXPR_VALUE
      } else {
        OZ (udf_raw_expr->add_param_desc(ObUDFParamDesc()));
      }
    }
  }
  OZ (pl::ObPLResolver::resolve_nocopy_params(func_info, udf_info));
  OV (udf_raw_expr->get_params_desc().count() == udf_raw_expr->get_param_count(), OB_ERR_UNEXPECTED, KPC(udf_raw_expr));
  return ret;
}

int ObRawExprUtils::rebuild_expr_params(ObUDFInfo &udf_info,
                                        ObRawExprFactory *expr_factory,
                                        ObIArray<ObRawExpr*> &expr_params)
{
  int ret = OB_SUCCESS;
  ObUDFRawExpr *udf_expr = static_cast<ObUDFRawExpr*>(udf_info.ref_expr_);
  CK (OB_NOT_NULL(udf_expr));
  CK (OB_NOT_NULL(expr_factory));
  CK (udf_info.param_exprs_.count() == udf_info.param_names_.count());
  for (int i = 0; OB_SUCC(ret) && i < udf_expr->get_children_count(); ++i) {
    OZ (expr_params.push_back(udf_expr->get_param_expr(i)));
  }
  for (int i = 0; OB_SUCC(ret) && i < udf_info.param_exprs_.count(); ++i) {
    ObCallParamRawExpr *call_param_expr = NULL;
    OZ (expr_factory->create_raw_expr(T_SP_CPARAM, call_param_expr));
    CK (OB_NOT_NULL(call_param_expr));
    OX (call_param_expr->set_name(udf_info.param_names_.at(i)));
    OX (call_param_expr->set_expr(udf_info.param_exprs_.at(i)));
    OZ (expr_params.push_back(call_param_expr));
  }
  return ret;
}

int ObRawExprUtils::resolve_udf_info(common::ObIAllocator &allocator,
                                     sql::ObRawExprFactory &expr_factory,
                                     sql::ObSQLSessionInfo &session_info,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     ObUDFInfo &udf_info)
{
  int ret = OB_SUCCESS;
  pl::ObPLPackageGuard *package_guard = NULL;
  CK (OB_NOT_NULL(session_info.get_cur_exec_ctx()));
  OZ (session_info.get_cur_exec_ctx()->get_package_guard(package_guard));
  CK (OB_NOT_NULL(package_guard));
  if (OB_SUCC(ret)) {
    pl::ObPLResolver pl_resolver(allocator,
                                session_info,
                                schema_guard,
                                *package_guard,
                                *GCTX.sql_proxy_,
                                expr_factory,
                                NULL,
                                false);
    HEAP_VAR(pl::ObPLFunctionAST, func_ast, allocator) {
      ObSEArray<pl::ObObjAccessIdx, 1> access_idxs;
      if (OB_FAIL(pl_resolver.init(func_ast))) {
        LOG_WARN("pl resolver init failed", K(ret));
      } else if (OB_FAIL(pl_resolver.resolve_udf_info(udf_info, access_idxs, func_ast))) {
        LOG_WARN("failed to resolve udf info", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::function_alias(ObRawExprFactory &expr_factory, ObSysFunRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  int64_t decimal = 10;
  int64_t binary = 2;
  int64_t octal = 8;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr));
  } else if (0 == expr->get_func_name().case_compare("bin")) {
    // bin(N) is equivalent to CONV(N,10,2)
    ObConstRawExpr *from_base = NULL;
    ObConstRawExpr *to_base = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, decimal, from_base))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, binary, to_base))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_UNLIKELY(1 != expr->get_param_count())) {
      ret = OB_ERR_PARAM_SIZE;
      LOG_USER_ERROR(OB_ERR_PARAM_SIZE, expr->get_func_name().length(), expr->get_func_name().ptr());
      LOG_WARN("invalid param count", K(expr->get_param_count()));
    } else if (OB_FAIL(expr->add_param_expr(from_base))) {
      LOG_WARN("fail to add param expr", K(from_base));
    } else if (OB_FAIL(expr->add_param_expr(to_base))) {
      LOG_WARN("fail to add param expr", K(to_base));
    } else {
      //do nothing
    }
  } else if (0 == expr->get_func_name().case_compare("oct")) {
    // oct(N) is equivalent to CONV(N,10,8)
    ObConstRawExpr *from_base = NULL;
    ObConstRawExpr *to_base = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, decimal, from_base))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, octal, to_base))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_UNLIKELY(1 != expr->get_param_count())) {
      ret = OB_ERR_PARAM_SIZE;
      LOG_USER_ERROR(OB_ERR_PARAM_SIZE, expr->get_func_name().length(), expr->get_func_name().ptr());
      LOG_WARN("invalid param count", K(expr->get_param_count()));
    } else if (OB_FAIL(expr->add_param_expr(from_base))) {
      LOG_WARN("fail to add param expr", K(from_base), K(ret));
    } else if (OB_FAIL(expr->add_param_expr(to_base))) {
      LOG_WARN("fail to add param expr", K(to_base), K(ret));
    } else {
      //do nothing
    }
  }
  return ret;
}

int ObRawExprUtils::make_raw_expr_from_str(const char *expr_str,
                                           const int64_t buf_len,
                                           ObExprResolveContext &resolve_ctx,
                                           ObRawExpr *&expr,
                                           ObIArray<ObQualifiedName> &columns,
                                           ObIArray<ObVarInfo> &sys_vars,
                                           ObIArray<ObSubQueryInfo> *sub_query_info,
                                           ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                           ObIArray<ObWinFunRawExpr*> &win_exprs,
                                           ObIArray<ObUDFInfo> &udf_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  ParseResult parse_result;
  ObParser parser(resolve_ctx.expr_factory_.get_allocator(), SMO_DEFAULT);
  if (OB_ISNULL(expr_str) || OB_ISNULL(sub_query_info)) {
    ret = OB_INVALID_ARGUMENT;
    _LOG_WARN("expr_str is %p, sub_query_info = %p", expr_str, sub_query_info);
  } else if (OB_FAIL(sql_str.append_fmt("SELECT %.*s", static_cast<int>(buf_len), expr_str))) {
    LOG_WARN("fail to concat string", K(expr_str), K(ret));
  } else if (OB_FAIL(parser.parse(sql_str.string(), parse_result))) {
    _OB_LOG(WARN, "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], "
            "line_[%d], yycolumn[%d], yylineno_[%d]",
            parse_result.yyscan_info_,
            parse_result.result_tree_,
            parse_result.malloc_pool_,
            parse_result.error_msg_,
            parse_result.start_col_,
            parse_result.end_col_,
            parse_result.line_,
            parse_result.yycolumn_,
            parse_result.yylineno_);
  } else {
    if (OB_UNLIKELY(OB_LOGGER.get_log_level() >= OB_LOG_LEVEL_DEBUG)) {
      LOG_DEBUG("", "parser result", SJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
    }
    ParseNode *stmt_node = NULL;
    ParseNode *select_node = NULL;
    ParseNode *select_expr_list = NULL;
    ParseNode *select_expr = NULL;
    ParseNode *parsed_expr = NULL;
    stmt_node = parse_result.result_tree_;
    if (NULL == stmt_node) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("internal arg is not correct", KP(stmt_node));
    } else if (T_STMT_LIST != stmt_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("internal arg is not correct", K(stmt_node->type_));
    }
    if (OB_SUCC(ret)) {
      select_node = stmt_node->children_[0];
      if (NULL == select_node) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", KP(select_node));
      } else if (T_SELECT != select_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", K(select_node->type_));
      }
    }
    if (OB_SUCC(ret)) {
      select_expr_list = select_node->children_[PARSE_SELECT_SELECT];
      if (NULL == select_expr_list) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", KP(select_expr_list));
      } else if (T_PROJECT_LIST != select_expr_list->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", K(select_expr_list->type_));
      }
    }
    if (OB_SUCC(ret)) {
      select_expr = select_expr_list->children_[0];
      if (NULL == select_expr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", KP(select_expr));
      } else if (T_PROJECT_STRING != select_expr->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", K(select_expr->type_));
      }
    }

    if (OB_SUCC(ret)) {
      parsed_expr = select_expr->children_[0];
      if (OB_ISNULL(parsed_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", KP(parsed_expr));
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<ObOpRawExpr*> op_exprs;
      ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
      ObArray<ObInListInfo> inlist_infos;
      ObSEArray<ObMatchFunRawExpr*, 1> match_exprs;
      ObRawExprResolverImpl expr_resolver(resolve_ctx);
      // generate raw expr
      if (OB_FAIL(expr_resolver.resolve(parsed_expr, expr, columns, sys_vars,
                                        *sub_query_info, aggr_exprs, win_exprs,
                                        udf_info, op_exprs, user_var_exprs, inlist_infos, match_exprs))) {
        _LOG_WARN("failed to resolve expr tree, err=%d", ret);
      } else if (OB_UNLIKELY(!inlist_infos.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in expr is not supported here", K(parsed_expr->type_));
      } else {/* do nothing */}
    }
    // destroy syntax tree
    parser.free_result(parse_result);
  }
  return ret;
}

int ObRawExprUtils::make_raw_expr_from_str(const ObString &expr_str,
                                           ObExprResolveContext &resolve_ctx,
                                           ObRawExpr *&expr,
                                           ObIArray<ObQualifiedName> &column,
                                           ObIArray<ObVarInfo> &sys_vars,
                                           ObIArray<ObSubQueryInfo> *sub_query_info,
                                           ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                           ObIArray<ObWinFunRawExpr*> &win_exprs,
                                           ObIArray<ObUDFInfo> &udf_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_query_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sub_query_info));
  } else if (OB_FAIL(make_raw_expr_from_str(expr_str.ptr(),
                                            expr_str.length(),
                                            resolve_ctx,
                                            expr,
                                            column,
                                            sys_vars,
                                            sub_query_info,
                                            aggr_exprs,
                                            win_exprs,
                                            udf_info))) {
    LOG_WARN("fail to make_raw_expr_from_str", K(ret));
  }
  return ret;
}

int ObRawExprUtils::parse_default_expr_from_str(const ObString &expr_str,
  ObCharsets4Parser expr_str_cs_type, ObIAllocator &allocator, const ParseNode *&node,
  bool is_for_trigger)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  ParseResult parse_result;
  ObSQLMode sql_mode = SMO_DEFAULT;
  if (lib::is_oracle_mode()) {
    sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
  }
  ObSQLParser parser(allocator, sql_mode);
  MEMSET(&parse_result, 0, sizeof(ParseResult));
  parse_result.malloc_pool_ = &allocator;
  parse_result.pl_parse_info_.is_pl_parse_ = true;
  parse_result.pl_parse_info_.is_pl_parse_expr_ = true;
  parse_result.sql_mode_ = sql_mode;
  parse_result.charset_info_ = ObCharset::get_charset(expr_str_cs_type.string_collation_);
  parse_result.charset_info_oracle_db_ = ObCharset::is_valid_collation(expr_str_cs_type.nls_collation_) ?
        ObCharset::get_charset(expr_str_cs_type.nls_collation_) : NULL;
  parse_result.is_not_utf8_connection_ = ObCharset::is_valid_collation(expr_str_cs_type.string_collation_) ?
        (ObCharset::charset_type_by_coll(expr_str_cs_type.string_collation_) != CHARSET_UTF8MB4) : false;
  parse_result.connection_collation_ = expr_str_cs_type.string_collation_;
  parse_result.semicolon_start_col_ = INT32_MAX;
  parse_result.is_for_trigger_ = is_for_trigger;
  if (OB_FAIL(sql_str.append_fmt("DO %.*s", expr_str.length(), expr_str.ptr()))) {
    LOG_WARN("failed to concat expr str", K(expr_str), K(ret));
  } else if (OB_FAIL(parser.parse(
    sql_str.string().ptr(), sql_str.string().length(), parse_result))) {
    _OB_LOG(WARN, "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], "
            "line_[%d], yycolumn[%d], yylineno_[%d]",
            parse_result.yyscan_info_,
            parse_result.result_tree_,
            parse_result.malloc_pool_,
            parse_result.error_msg_,
            parse_result.start_col_,
            parse_result.end_col_,
            parse_result.line_,
            parse_result.yycolumn_,
            parse_result.yylineno_);
  } else {
    if (OB_UNLIKELY(OB_LOGGER.get_log_level() >= OB_LOG_LEVEL_DEBUG)) {
      LOG_DEBUG("", "parser result", SJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
    }
    ParseNode *expr_node = NULL;
    if (OB_ISNULL(expr_node = parse_result.result_tree_->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to parse default expr node", K(ret), K(expr_str));
    } else if (OB_UNLIKELY(T_DEFAULT != expr_node->type_)
               || OB_UNLIKELY(1 != expr_node->num_child_)
               || OB_ISNULL(expr_node->children_)
               || OB_ISNULL(expr_node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node type is illegal",
               K(ret), K(expr_node->type_), K(expr_node->num_child_), K(expr_node->children_));
    } else {
      node = expr_node->children_[0];
    }
  }
  return ret;
}

int ObRawExprUtils::parse_expr_list_node_from_str(const ObString &expr_str,
                                                  ObCharsets4Parser expr_str_cs_type,
                                                  ObIAllocator &allocator,
                                                  const ParseNode *&node,
                                                  const ObSQLMode &sql_mode)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  ParseResult parse_result;
  ObSQLMode inner_sql_mode = SMO_DEFAULT;
  // in NO_BACKSLASH_ESCAPES mode, the inner sql should also follow the rule of NO_BACKSLASH_ESCAPES
  bool is_no_backslash_escapes = false;
  IS_NO_BACKSLASH_ESCAPES(sql_mode, is_no_backslash_escapes);
  if (is_no_backslash_escapes) {
    inner_sql_mode = inner_sql_mode | SMO_NO_BACKSLASH_ESCAPES;
  }
  if (lib::is_oracle_mode()) {
    inner_sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
  }

  ObParser parser(allocator, inner_sql_mode, expr_str_cs_type);
  if (OB_FAIL(sql_str.append_fmt("SELECT %.*s FROM DUAL", expr_str.length(), expr_str.ptr()))) {
    LOG_WARN("fail to concat string", K(expr_str), K(ret));
  } else if (OB_FAIL(parser.parse(sql_str.string(), parse_result))) {
    _OB_LOG(WARN, "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], "
            "line_[%d], yycolumn[%d], yylineno_[%d]",
            parse_result.yyscan_info_,
            parse_result.result_tree_,
            parse_result.malloc_pool_,
            parse_result.error_msg_,
            parse_result.start_col_,
            parse_result.end_col_,
            parse_result.line_,
            parse_result.yycolumn_,
            parse_result.yylineno_);
  } else {
    if (OB_UNLIKELY(OB_LOGGER.get_log_level() >= OB_LOG_LEVEL_DEBUG)) {
      LOG_DEBUG("", "parser result", SJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
    }
    ParseNode *stmt_node = NULL;
    ParseNode *select_node = NULL;
    ParseNode *select_expr_list = NULL;
    stmt_node = parse_result.result_tree_;
    if (NULL == stmt_node) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("internal arg is not correct", KP(stmt_node));
    } else if (T_STMT_LIST != stmt_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("internal arg is not correct", K(stmt_node->type_));
    }
    if (OB_SUCC(ret)) {
      select_node = stmt_node->children_[0];
      if (NULL == select_node) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", KP(select_node));
      } else if (T_SELECT != select_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", K(select_node->type_));
      }
    }
    if (OB_SUCC(ret)) {
      select_expr_list = select_node->children_[PARSE_SELECT_SELECT];
      if (NULL == select_expr_list) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", KP(select_expr_list));
      } else if (T_PROJECT_LIST != select_expr_list->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", K(select_expr_list->type_));
      }
    }
    if (OB_SUCC(ret)) {
      node = select_expr_list;
    }
  }
  return ret;
}

int ObRawExprUtils::parse_expr_node_from_str(const ObString &expr_str,
                                             ObCharsets4Parser expr_str_cs_type,
                                             ObIAllocator &allocator,
                                             const ParseNode *&node,
                                             const ObSQLMode &sql_mode)
{
  int ret = OB_SUCCESS;
  const ParseNode *expr_list = NULL;
  const ParseNode *select_expr = NULL;
  if (OB_FAIL(parse_expr_list_node_from_str(expr_str, expr_str_cs_type, allocator,
                                            expr_list, sql_mode))) {
    LOG_WARN("fail to parse node list");
  } else if (OB_ISNULL(expr_list)){
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("internal arg is not correct", KP(expr_list));
  } else {
    select_expr = expr_list->children_[0];
    if (NULL == select_expr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("internal arg is not correct", KP(select_expr));
    } else if (T_PROJECT_STRING != select_expr->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("internal arg is not correct", K(select_expr->type_));
    } else {
      node = select_expr->children_[0];
    }
  }
  return ret;
}

int ObRawExprUtils::parse_bool_expr_node_from_str(const common::ObString &expr_str,
                                                  common::ObIAllocator &allocator,
                                                  const ParseNode *&node)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  ParseResult parse_result;
  ObSQLMode sql_mode = SMO_DEFAULT;

  if (lib::is_oracle_mode()) {
    sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
  }
  ObParser parser(allocator, sql_mode);
  if (OB_FAIL(sql_str.append_fmt("SELECT 1 FROM DUAL WHERE %.*s", expr_str.length(), expr_str.ptr()))) {
    LOG_WARN("fail to concat string", K(expr_str), K(ret));
  } else if (OB_FAIL(parser.parse(sql_str.string(), parse_result))) {
    _OB_LOG(WARN, "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], "
            "line_[%d], yycolumn[%d], yylineno_[%d]",
            parse_result.yyscan_info_,
            parse_result.result_tree_,
            parse_result.malloc_pool_,
            parse_result.error_msg_,
            parse_result.start_col_,
            parse_result.end_col_,
            parse_result.line_,
            parse_result.yycolumn_,
            parse_result.yylineno_);
  } else {
    if (OB_UNLIKELY(OB_LOGGER.get_log_level() >= OB_LOG_LEVEL_DEBUG)) {
      LOG_DEBUG("", "parser result", SJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
    }
    ParseNode *stmt_node = NULL;
    ParseNode *select_node = NULL;
    ParseNode *where_node = NULL;
    ParseNode *expr_node = NULL;
    stmt_node = parse_result.result_tree_;
    if (NULL == stmt_node) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("internal arg is not correct", KP(stmt_node));
    } else if (T_STMT_LIST != stmt_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("internal arg is not correct", K(stmt_node->type_));
    }
    if (OB_SUCC(ret)) {
      select_node = stmt_node->children_[0];
      if (NULL == select_node) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", KP(select_node));
      } else if (T_SELECT != select_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", K(select_node->type_));
      }
    }
    if (OB_SUCC(ret)) {
      where_node = select_node->children_[PARSE_SELECT_WHERE];
      if (NULL == where_node) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", KP(where_node));
      } else if (T_WHERE_CLAUSE != where_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", K(where_node->type_));
      }
    }
    if (OB_SUCC(ret)) {
      expr_node = where_node->children_[0];
      if (NULL == expr_node) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("internal arg is not correct", KP(expr_node));
      }
    }
    if (OB_SUCC(ret)) {
      node = expr_node;
    }
  }

  return ret;
}

int ObRawExprUtils::build_generated_column_expr(const ObString &expr_str,
                                                ObRawExprFactory &expr_factory,
                                                const ObSQLSessionInfo &session_info,
                                                ObRawExpr *&expr,
                                                ObIArray<ObQualifiedName> &columns,
                                                const ObTableSchema * table_schema,
                                                const bool sequence_allowed,
                                                ObDMLResolver *dml_resolver,
                                                const ObSchemaChecker *schema_checker,
                                                const ObResolverUtils::PureFunctionCheckStatus
                                                  check_status,
                                                const bool need_check_simple_column)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_generated_column_expr(expr_str,
                                          expr_factory,
                                          session_info,
                                          session_info.get_sql_mode(),
                                          session_info.get_local_collation_connection(),
                                          expr,
                                          columns,
                                          table_schema,
                                          sequence_allowed,
                                          dml_resolver,
                                          schema_checker,
                                          check_status,
                                          need_check_simple_column))) {
    LOG_WARN("build generated column expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::build_generated_column_expr(const ObString &expr_str,
                                                ObRawExprFactory &expr_factory,
                                                const ObSQLSessionInfo &session_info,
                                                ObSQLMode def_sql_mode,
                                                ObCollationType def_cs_type,
                                                ObRawExpr *&expr,
                                                ObIArray<ObQualifiedName> &columns,
                                                const ObTableSchema * table_schema,
                                                const bool sequence_allowed,
                                                ObDMLResolver *dml_resolver,
                                                const ObSchemaChecker *schema_checker,
                                                const ObResolverUtils::PureFunctionCheckStatus
                                                  check_status,
                                                const bool need_check_simple_column)
{
  int ret = OB_SUCCESS;
  const ParseNode *node = NULL;
   ObCharsets4Parser charsets4parser = session_info.get_charsets4parser();
   charsets4parser.string_collation_ = def_cs_type;
  if (OB_FAIL(parse_expr_node_from_str(expr_str,
      charsets4parser,
      expr_factory.get_allocator(), node, def_sql_mode))) {
    LOG_WARN("parse expr node from string failed", K(ret), K(expr_str));
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null");
  } else if (OB_FAIL(build_generated_column_expr(expr_factory, session_info, *node,
                                                 expr, columns, table_schema, sequence_allowed,
                                                 dml_resolver,
                                                 schema_checker, check_status,
                                                 need_check_simple_column,
                                                 true,
                                                 def_cs_type))) {
    LOG_WARN("build generated column expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::build_seq_nextval_expr(ObRawExpr *&expr,
                                          const ObSQLSessionInfo *session_info,
                                          ObRawExprFactory *expr_factory,
                                          const ObQualifiedName &q_name,
                                          uint64_t seq_id,
                                          ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else {
    const ObString &database_name = q_name.database_name_.empty() ?
                                    session_info->get_database_name() : q_name.database_name_;
    if (OB_FAIL(build_seq_nextval_expr(expr, session_info, expr_factory, database_name,
                                q_name.tbl_name_, q_name.col_name_, seq_id, stmt))) {
      LOG_WARN("build seq nextval expr failed", K(ret));
    }
  }
  return ret;
}

// build oracle sequence_object.currval, sequence_object.nextval expr
int ObRawExprUtils::build_seq_nextval_expr(ObRawExpr *&expr,
                                          const ObSQLSessionInfo *session_info,
                                          ObRawExprFactory *expr_factory,
                                          const ObString &database_name,
                                          const ObString &tbl_name,
                                          const ObString &col_name,
                                          uint64_t seq_id,
                                          ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObRawExpr *exists_seq_expr = NULL;
  ObSequenceRawExpr *func_expr = NULL;
  ObConstRawExpr *col_id_expr = NULL;
  if (OB_ISNULL(session_info) || OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret), K(session_info), K(expr_factory));
  } else if (NULL != stmt && OB_FAIL(stmt->get_sequence_expr(exists_seq_expr,
                                                   tbl_name,
                                                   col_name,
                                                   seq_id))) {
    LOG_WARN("failed to get sequence expr", K(ret));
  } else if (exists_seq_expr != NULL) {
    expr = exists_seq_expr;
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_SEQ_NEXTVAL, func_expr))) {
    LOG_WARN("create nextval failed", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_UINT64, col_id_expr))) {
    LOG_WARN("create const raw expr failed", K(ret));
  } else {
    ObObj col_id;
    col_id.set_uint64(seq_id);
    col_id_expr->set_value(col_id);
    if (OB_FAIL(func_expr->set_sequence_meta(database_name, tbl_name, col_name, seq_id))) {
      LOG_WARN("failed to set sequence meta", K(ret));
    } else if (OB_FAIL(func_expr->add_flag(IS_SEQ_EXPR))) {
      LOG_WARN("failed to add flag", K(ret));
    } else if (OB_FAIL(func_expr->add_param_expr(col_id_expr))) {
      LOG_WARN("set function param expr failed", K(ret));
    } else if (OB_FAIL(func_expr->formalize(session_info))) {
      LOG_WARN("failed to extract info", K(ret));
    } else if (NULL != stmt && OB_FAIL(stmt->get_pseudo_column_like_exprs().push_back(func_expr))) {
      LOG_WARN("failed to push back sequence expr", K(ret));
    } else {
      expr = func_expr;
    }
  }
  return ret;
}

int ObRawExprUtils::resolve_sequence_object(const ObQualifiedName &q_name,
                                           ObDMLResolver *dml_resolver,
                                           const ObSQLSessionInfo *session_info,
                                           ObRawExprFactory *expr_factory,
                                           ObSequenceNamespaceChecker &sequence_namespace_checker,
                                           ObRawExpr *&real_ref_expr,
                                           bool is_generated_column)
{
  int ret = OB_SUCCESS;
  uint64_t sequence_id = OB_INVALID_ID;
  ObRawExpr *column_expr = NULL;
  ObDMLStmt *stmt = NULL == dml_resolver ? NULL : dml_resolver->get_stmt();
  ObSynonymChecker syn_checker;
  uint64_t dblink_id = OB_INVALID_ID;
  if (!q_name.tbl_name_.empty() &&
        ObSequenceNamespaceChecker::is_curr_or_next_val(q_name.col_name_)) {
    LOG_DEBUG("sequence object", K(q_name));
    // don't check scope for sequence in definition of generated column.
    // sequence expr will only be used later when insert or update column with default value.
    ObStmtScope current_scope = NULL != dml_resolver && !is_generated_column
                                  ? dml_resolver->get_current_scope()
                                  : T_FIELD_LIST_SCOPE;
    if (OB_FAIL(sequence_namespace_checker.check_sequence_namespace(q_name,
                                                                    syn_checker,
                                                                    sequence_id,
                                                                    &dblink_id))) {
      // Except for table creation, sequence_not_exist error can be ignored in the resolver phase
      // for sequence in definition of default column. If an error needs to be reported such as when
      // insert or update column with default value, check it again in the execution phase.
      if (NULL != stmt && stmt::T_CREATE_TABLE != stmt->get_stmt_type() && is_generated_column
          && ret == OB_ERR_BAD_FIELD_ERROR) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN_IGNORE_COL_NOTFOUND(ret, "check basic column namespace failed", K(ret), K(q_name));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_UNLIKELY(T_FIELD_LIST_SCOPE != current_scope &&
                           T_UPDATE_SCOPE != current_scope &&
                           T_INSERT_SCOPE != current_scope)) {
      // sequence 只能出现在以下三种场景：
      //  - select seq from ...
      //  - insert into t1 values (seq...
      //  - update t1 set c1 = seq xxxx
      // 不可以出现在 where、group by、limit、having 等上下文中
      ret = OB_ERR_SEQ_NOT_ALLOWED_HERE;
    } else if (OB_FAIL(build_seq_nextval_expr(column_expr, session_info, expr_factory, q_name,
                                              sequence_id, stmt))) {
      LOG_WARN("resolve column item failed", K(ret));
    } else {
      if (OB_INVALID_ID != dblink_id && T_FUN_SYS_SEQ_NEXTVAL == column_expr->get_expr_type()) {
        ObSequenceRawExpr *seq_expr = static_cast<ObSequenceRawExpr*>(column_expr);
        seq_expr->set_dblink_name(q_name.dblink_name_);
        seq_expr->set_dblink_id(dblink_id);
      }
      real_ref_expr = column_expr;
      StmtType type = stmt::T_NONE;
      if (NULL != dml_resolver) {
        if (OB_ISNULL(stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("stmt of dml resolver is null", K(ret));
        } else if (FALSE_IT(type = stmt->get_stmt_type())) {
        } else if (is_generated_column && stmt::T_INSERT != type && stmt::T_UPDATE != type
                   && stmt::T_MERGE != type) {
          // do nothing. only generate sequence operator in INSERT/UPDATE stmt.
        } else if (!is_generated_column) {
          if (0 == q_name.col_name_.case_compare("NEXTVAL")) {
            // 将 sequence id 记录到 plan 里
            if (OB_FAIL(dml_resolver->add_sequence_id_to_stmt(sequence_id))) {
              LOG_WARN("fail add id to stmt", K(sequence_id), K(ret));
            }
          } else if (0 == q_name.col_name_.case_compare("CURRVAL")) {
            if (OB_FAIL(dml_resolver->add_sequence_id_to_stmt(sequence_id, true))) {
              LOG_WARN("fail add id to stmt", K(sequence_id), K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (syn_checker.has_synonym()) {
          // add synonym depedency schemas
          if (OB_FAIL(dml_resolver->add_object_versions_to_dependency(
                DEPENDENCY_SYNONYM, SYNONYM_SCHEMA, syn_checker.get_synonym_ids(),
                syn_checker.get_database_ids()))) {
            LOG_WARN("add synonym version failed", K(ret));
          } else {
            // do nothing
          }
        } else {
          // do nothing
        }
      }
    }
  } else {
    // 没有发现 nextval，currval 字样，
    // 不是一个 sequence 对象，抛给外面处理
    ret = OB_ERR_BAD_FIELD_ERROR;
  }
  return ret;
}
/**
 * @brief [Oracle兼容] 用于判断一个函数或伪列是否是 “pure” 的函数，根据Oracle官方文档：
 * Any function you specify in column_expression must return a repeatable value. For example,
 * you cannot specify the SYSDATE or USER function or the ROWNUM pseudocolumn.
 * 目前仅支持对 Oracle 模式下的系统函数进行检查
 * 考虑到表达式可能是嵌套的，因此逐个遍历 child，找到任何可能是非 pure 的函数或伪列
 * @param expr 表达式
 * @param allocator 用于为 ObList 分配内存
 * @return
 */
int ObRawExprUtils::check_deterministic(const ObRawExpr *expr,
                                        ObIAllocator &allocator,
                                        const ObResolverUtils::PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  CK (expr->get_children_count() >= 0);
  ObList<const ObRawExpr *, ObIAllocator> expr_queue(allocator);
  OZ (expr_queue.push_back(expr));
  const ObRawExpr *cur_expr = NULL;
  while (OB_SUCC(ret) && expr_queue.size() > 0) {
    OZ (expr_queue.pop_front(cur_expr));
    CK (OB_NOT_NULL(cur_expr));
    OZ (check_deterministic_single(cur_expr, check_status));
    for (int i = 0; OB_SUCC(ret) && i < cur_expr->get_param_count(); ++i) {
      OZ (expr_queue.push_back(cur_expr->get_param_expr(i)));
    }
  }
  return ret;
}

/**
 * @brief 检查单个 expr 是否是 pure 的，并根据 expr 的类型返回不同的错误码
 * @param expr 被检查的表达式
 * @return
 */
int ObRawExprUtils::check_deterministic_single(const ObRawExpr *expr,
                                               const ObResolverUtils::PureFunctionCheckStatus
                                                check_status)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(expr));
  if (OB_SUCC(ret) && ObResolverUtils::DISABLE_CHECK != check_status) {
    if (is_oracle_mode()
        && (ObResolverUtils::CHECK_FOR_GENERATED_COLUMN == check_status
            || ObResolverUtils::CHECK_FOR_FUNCTION_INDEX == check_status)
        && T_OP_IS == expr->get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Use ISNULL() in generated column or functional index");
      LOG_WARN("special function is not suppored in generated column", K(ret), KPC(expr));
    } else if (is_oracle_mode()
              && T_FUN_SYS_DEFAULT == expr->get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Use DEFAULT() in generated column or functional index or check constraint");
      LOG_WARN("special function is not suppored in generated column", K(ret), KPC(expr));
    } else if (expr->is_sys_func_expr()) {
      bool is_non_pure_func = false;
      if (OB_FAIL(expr->is_non_pure_sys_func_expr(is_non_pure_func))) {
        LOG_WARN("check is non pure sys func expr failed", K(ret));
      } else if (OB_UNLIKELY(is_non_pure_func)) {
        if (ObResolverUtils::CHECK_FOR_GENERATED_COLUMN == check_status) {
          ret = OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION;
        } else if (ObResolverUtils::CHECK_FOR_FUNCTION_INDEX == check_status) {
          ret = OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED;
        } else if (ObResolverUtils::CHECK_FOR_CHECK_CONSTRAINT == check_status) {
          if (is_oracle_mode()) {
            ret = OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST;
            LOG_WARN("deterministic expr is wrongly specified in CHECK constraint", K(ret), K(expr->get_expr_type()));
          } else {
            ret = OB_ERR_CHECK_CONSTRAINT_NAMED_FUNCTION_IS_NOT_ALLOWED;
            LOG_WARN("deterministic expr is wrongly specified in CHECK constraint", K(ret), K(expr->get_expr_type()));
          }
        }
        LOG_WARN("only pure sys function can be indexed",
                 K(ret), K(check_status), K(*expr));
      } else if (T_FUN_SYS_ROWNUM == expr->get_expr_type()) {
        ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
        LOG_WARN("ROWNUM is not allowed", K(ret), K(*expr));
      }
    } else {
      if (expr->is_pseudo_column_expr()) {
        if (expr->is_specified_pseudocolumn_expr()) {
          ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
          LOG_WARN("not allowed pseudo column", K(ret), K(*expr));
        }
      } else if (expr->is_udf_expr()) {
        if (lib::is_mysql_mode()) {
          ret = OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION;
          LOG_WARN("user-defined functions are not allowd in generated column", K(ret), K(*expr));
        } else {
          const ObUDFRawExpr *udf_expr = dynamic_cast<const ObUDFRawExpr *>(expr);
          CK(OB_NOT_NULL(udf_expr));
          if (!udf_expr->is_deterministic()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("user-defined function is not deterministic", K(ret), K(*expr));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "The user-defined function is not deterministic");
          }
        }
      }
      // need to change err code if check_status is CHECK_FOR_CHECK_CONSTRAINT
      if (OB_FAIL(ret) && ObResolverUtils::CHECK_FOR_CHECK_CONSTRAINT == check_status) {
        if (is_oracle_mode()) {
          ret = OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST;
          LOG_WARN("deterministic expr is wrongly specified in CHECK constraint", K(ret), K(expr->get_expr_type()));
        } else {
          ret = OB_ERR_CHECK_CONSTRAINT_NAMED_FUNCTION_IS_NOT_ALLOWED;
          LOG_WARN("deterministic expr is wrongly specified in CHECK constraint", K(ret), K(expr->get_expr_type()));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief 创建生成列表达式
 * @param arg 需要用到 arg 中的 nls_xx_format，当 arg 为空时，使用 session 中的 nls_xx_format
 * @param expr_str 表达式定义
 * @param expr_factory factory
 * @param session_info session，注意，在RS端调用时，传进来的时 default session，不是当前session
 * @param table_schema table_schema
 * @param expr 生成好的表达式
 * @param schema_checker checker
 * @param resolved_cols Default null. Only use in 'alter table'. Columns which have been resolved in alter table.
 * @return ret
 */
int ObRawExprUtils::build_generated_column_expr(const obrpc::ObCreateIndexArg *arg,
                                                const ObString &expr_str,
                                                ObRawExprFactory &expr_factory,
                                                const ObSQLSessionInfo &session_info,
                                                const ObTableSchema &table_schema,
                                                ObRawExpr *&expr,
                                                const ObSchemaChecker *schema_checker,
                                                const ObResolverUtils::PureFunctionCheckStatus
                                                  check_status,
                                                ObIArray<share::schema::ObColumnSchemaV2*> *resolved_cols)
{
  int ret = OB_SUCCESS;
  const ParseNode *node = NULL;
  ObSEArray<ObQualifiedName, 2> columns;
  ObSEArray<ObRawExpr *, 6> real_exprs;
  const ObColumnSchemaV2 *col_schema = NULL;
  if (OB_FAIL(parse_expr_node_from_str(expr_str,
      session_info.get_charsets4parser(),
      expr_factory.get_allocator(), node))) {
    LOG_WARN("parse expr node from string failed", K(ret));
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null");
  } else if (OB_FAIL(build_generated_column_expr(expr_factory,
                                                 session_info,
                                                 *node,
                                                 expr,
                                                 columns,
                                                 &table_schema,
                                                 false,
                                                 NULL,
                                                 schema_checker,
                                                 check_status))) {
    LOG_WARN("build generated column expr failed", K(ret), K(expr_str));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    const ObQualifiedName &q_name = columns.at(i);
    if (q_name.is_pl_udf()) {
      OZ (resolve_gen_column_udf_expr(expr,
                                      const_cast<ObQualifiedName &>(q_name),
                                      expr_factory,
                                      session_info,
                                      const_cast<ObSchemaChecker *>(schema_checker),
                                      columns,
                                      real_exprs,
                                      NULL));
      OZ (real_exprs.push_back(expr), q_name);
    } else if (table_schema.is_external_table()) {
      const ObColumnSchemaV2 *column_schema = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); i++) {
        if (OB_ISNULL(table_schema.get_column_schema_by_idx(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        } else if (0 == table_schema.get_column_schema_by_idx(i)->get_cur_default_value().get_string().compare(expr_str)) {
          column_schema = table_schema.get_column_schema_by_idx(i);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObResolverUtils::resolve_external_table_column_def(expr_factory, session_info, q_name, real_exprs, expr, column_schema))) {
        LOG_WARN("fail to resolve external table column def", K(ret));
      }
    } else {
      if (OB_UNLIKELY(!q_name.database_name_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid generated column name", K(q_name));
      } else if (!q_name.tbl_name_.empty()
          && !ObCharset::case_insensitive_equal(q_name.tbl_name_, table_schema.get_table_name_str())) {
        ret = OB_ERR_BAD_TABLE;
        LOG_USER_ERROR(OB_ERR_BAD_TABLE, q_name.tbl_name_.length(), q_name.tbl_name_.ptr());
      } else if (OB_ISNULL(col_schema = table_schema.get_column_schema(q_name.col_name_))) {
        if (OB_NOT_NULL(resolved_cols)) {
          col_schema = ObResolverUtils::get_column_schema_from_array(*resolved_cols, q_name.col_name_);
        }
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, q_name.col_name_.length(), q_name.col_name_.ptr());
        }
      }
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (OB_UNLIKELY(col_schema->is_generated_column())) {
        ret = OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
        LOG_USER_ERROR(OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN,
                       "Generated column in column expression");
      } else if (OB_FAIL(ObRawExprUtils::init_column_expr(*col_schema, *q_name.ref_expr_))) {
        LOG_WARN("init column expr failed", K(ret), K(q_name));
      } else {
        q_name.ref_expr_->set_ref_id(table_schema.get_table_id(), col_schema->get_column_id());
        OZ (real_exprs.push_back(q_name.ref_expr_), q_name);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr->formalize(&session_info, true))) {
      LOG_WARN("formalize expr failed", K(ret), KPC(expr));
    }
  }

  // 改写上面生成的表达式
  if (OB_SUCC(ret) && NULL != arg) {
    bool expr_changed = false;
    if (OB_FAIL(ObRawExprUtils::erase_operand_implicit_cast(expr, expr))) {
      LOG_WARN("fail to remove implicit cast", K(ret));
    } else if (OB_FAIL(try_modify_expr_for_gen_col_recursively(session_info, arg, expr_factory,
                                                    expr, expr_changed))) {
      LOG_WARN("try_add_to_char_on_expr failed", K(ret));
    } else if (lib::is_oracle_mode()) {
      ObSEArray<ObColumnSchemaV2 *, 1> resolved_cols;
      if (OB_FAIL(ObRawExprUtils::try_modify_udt_col_expr_for_gen_col_recursively(session_info,
                                                                                  table_schema,
                                                                                  resolved_cols,
                                                                                  expr_factory,
                                                                                  expr))) {
        LOG_WARN("transform udt col expr for generated column failed", K(ret));
      }
    }
    // 只在必要的时候才会在做一次 formalize
    if (OB_SUCC(ret)) {
      OZ (expr->formalize(&session_info, true));
    }
  }
  if (OB_SUCC(ret)
      && expr->get_result_type().is_decimal_int()
      && lib::is_mysql_mode()
      && expr->get_result_type().get_precision() > OB_MAX_DECIMAL_PRECISION) {
    // maximum stored precision is 65, need truncating
    LOG_INFO("truncate precision to `OB_MAX_DECIMAL_PRECISION` for deicmal_int", K(*expr));
    ObAccuracy res_acc = expr->get_accuracy();
    res_acc.set_precision(OB_MAX_DECIMAL_PRECISION);
    expr->set_accuracy(res_acc);
  }
  if (OB_SUCC(ret) &&
      (ObResolverUtils::CHECK_FOR_FUNCTION_INDEX == check_status ||
        ObResolverUtils::CHECK_FOR_GENERATED_COLUMN == check_status)) {
    if (OB_FAIL(check_is_valid_generated_col(expr, expr_factory.get_allocator()))) {
      if (OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION == ret
                && ObResolverUtils::CHECK_FOR_FUNCTION_INDEX == check_status) {
        ret = OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED;
        LOG_WARN("sysfunc in expr is not valid for generated column", K(ret), K(*expr));
      } else {
        LOG_WARN("fail to check if the sysfunc exprs are valid in generated columns", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::build_check_constraint_expr(ObRawExprFactory &expr_factory,
                                                const ObSQLSessionInfo &session_info,
                                                const ParseNode &node,
                                                ObRawExpr *&expr,
                                                common::ObIArray<ObQualifiedName> &columns)
{
  int ret = OB_SUCCESS;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObUDFInfo> udf_info;
  ObArray<ObOpRawExpr*> op_exprs;

  if (OB_FAIL(build_raw_expr(expr_factory,
                             session_info,
                             node,
                             expr,
                             columns,
                             sys_vars,
                             aggr_exprs,
                             win_exprs,
                             sub_query_info,
                             udf_info,
                             op_exprs))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_UNLIKELY(udf_info.count() > 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use user defined function in check constraint");
  } else if (OB_UNLIKELY(expr->has_flag(CNT_SUB_QUERY))) {
    ret = lib::is_mysql_mode() ? OB_ERR_CHECK_CONSTRAINT_FUNCTION_IS_NOT_ALLOWED : OB_ERR_INVALID_SUBQUERY_USE;
    LOG_WARN("subquery not allowed here", K(ret));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_AGG))) {
    ret = OB_ERR_GROUP_FUNC_NOT_ALLOWED;
    LOG_WARN("group function is not allowed here", K(ret));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_ROWNUM) || expr->has_flag(CNT_PSEUDO_COLUMN))) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("Specified pseudo column or operator not allowed here", K(ret), K(expr->has_flag(CNT_ROWNUM)), K(expr->has_flag(CNT_PSEUDO_COLUMN)));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_WINDOW_FUNC))) {
    ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(sys_vars.count() > 0)) {
      if (is_oracle_mode()) {
        ret = OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST;
        LOG_WARN("system variable wrongly specified in CHECK constraint", K(ret));
      } else {
        ret = OB_ERR_CHECK_CONSTRAINT_VARIABLES;
        LOG_WARN("system variable wrongly specified in CHECK constraint", K(ret));
      }
    } else {
      ObResolverUtils::PureFunctionCheckStatus check_status = ObResolverUtils::CHECK_FOR_CHECK_CONSTRAINT;
      if (OB_FAIL(check_deterministic(expr, expr_factory.get_allocator(), check_status))) {
        LOG_WARN("fail to check_deterministic for check constraint", K(ret));
      }
    }
  }

  return ret;
}

int ObRawExprUtils::extract_metadata_fileurl_expr(ObRawExpr *expr, ObRawExpr *&file_name_expr)
{
  int ret = OB_SUCCESS;
  if (file_name_expr != NULL) {
    //do nothing
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_pseudo_column_expr() && expr->get_expr_type() == T_PSEUDO_EXTERNAL_FILE_URL) {
    file_name_expr = expr;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(extract_metadata_fileurl_expr(expr->get_param_expr(i), file_name_expr)))) {
        LOG_WARN("extract metadata filename expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::build_generated_column_expr(ObRawExprFactory &expr_factory,
                                                const ObSQLSessionInfo &session_info,
                                                const ParseNode &node,
                                                ObRawExpr *&expr,
                                                ObIArray<ObQualifiedName> &columns,
                                                const ObTableSchema *new_table_schema,
                                                const bool sequence_allowed,
                                                ObDMLResolver *dml_resolver,
                                                const ObSchemaChecker *schema_checker,
                                                const ObResolverUtils::PureFunctionCheckStatus
                                                  check_status,
                                                const bool need_check_simple_column,
                                                bool use_def_collation,
                                                ObCollationType def_collation)
{
  int ret = OB_SUCCESS;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObUDFInfo> udf_info;
  ObArray<ObOpRawExpr*> op_exprs;

  if (OB_FAIL(build_raw_expr(expr_factory,
                             session_info,
                             const_cast<ObSchemaChecker*>(schema_checker),
                             NULL,
                             T_NONE_SCOPE,
                             NULL,
                             NULL,
                             NULL,
                             node,
                             expr,
                             columns,
                             sys_vars,
                             aggr_exprs,
                             win_exprs,
                             sub_query_info,
                             udf_info,
                             op_exprs,
                             false,
                             TgTimingEvent::TG_TIMING_EVENT_INVALID,
                             use_def_collation,
                             def_collation))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_UNLIKELY(sys_vars.count() > 0)) {
    //todo:yuming:mysql ERROR 3102 (HY000): Expression of generated column 'column' contains a disallowed function.
    //mysql error code: ER_GENERATED_COLUMN_FUNCTION_IS_NOT_ALLOWED
    //format: Expression of generated column '%s' contains a disallowed function
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use variables in generated column");
  } else if (OB_UNLIKELY(sub_query_info.count() > 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use subquery in generated column");
  } else if (OB_UNLIKELY(aggr_exprs.count() > 0)) {
    ret = OB_ERR_INVALID_GROUP_FUNC_USE;
    LOG_USER_ERROR(OB_ERR_INVALID_GROUP_FUNC_USE);
  } else if (OB_UNLIKELY(win_exprs.count() > 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use window function in generated column");
  } else if (OB_UNLIKELY(udf_info.count() > 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use user defined function in generated column");
  } else if (lib::is_oracle_mode()) {
    //generated column contain only two types of access obj:sysfunc expression and column expression
    //only column expression will be handled in the upper function call,
    //so sysfunc expression should be filtered in this function and can't return to the upper function call
    ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 1> ref_sys_exprs;
    ObSEArray<ObQualifiedName, 1> real_columns;
    ObSequenceNamespaceChecker sequence_checker(schema_checker, &session_info);
    ObArray<ObRawExpr*> real_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
      const ObQualifiedName &q_name = columns.at(i);
      if (q_name.is_sys_func()) {
        // alter table t add  b char(10) as(concat(a, '1'));，在oracle模式下类似concat的系统函数也被解析成T_OBJ_ACCESS_REF
        ObRawExpr *sys_func = q_name.access_idents_.at(0).sys_func_expr_;
        CK (OB_NOT_NULL(sys_func));
        for (int64_t i = 0; OB_SUCC(ret) && i < ref_sys_exprs.count(); ++i) {
          OZ (ObRawExprUtils::replace_ref_column(sys_func, ref_sys_exprs.at(i).first, ref_sys_exprs.at(i).second));
        }
        OZ (q_name.access_idents_.at(0).check_param_num());
        OZ (ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, sys_func));
        OZ (ref_sys_exprs.push_back(std::pair<ObRawExpr*, ObRawExpr*>(q_name.ref_expr_, sys_func)));
        OZ(real_exprs.push_back(sys_func));
      } else if (q_name.is_pl_udf()) {
        ObRawExpr *udf_info = NULL;
        for (uint64_t j = 0; OB_SUCC(ret) && j < q_name.access_idents_.count(); ++j) {
          ObObjAccessIdent &access_ident = columns.at(i).access_idents_.at(j);
          if (access_ident.is_pl_udf()) {
            OZ (pl::ObPLResolver::replace_udf_param_expr(access_ident, columns, real_exprs));
            const ObUDFInfo &udf_info = access_ident.udf_info_;
            if (OB_NOT_NULL(udf_info.ref_expr_)) {
              OZ (ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, udf_info.ref_expr_), q_name);
            }
          }
        }
        OZ(real_columns.push_back(q_name), q_name, i);
      } else if (!sequence_allowed || OB_ISNULL(schema_checker)) {
        OZ(real_columns.push_back(q_name), q_name, i);
      } else {
        ObRawExpr *sequence_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::resolve_sequence_object(q_name, dml_resolver, &session_info, &expr_factory,
                          sequence_checker, sequence_expr, true /* is_generated_column*/))) {
          LOG_WARN("resolve sequence object failed", K(ret));
          if (OB_ERR_BAD_FIELD_ERROR == ret) {
            ret = OB_SUCCESS;
            OZ(real_columns.push_back(q_name), q_name, i);
          }
        } else {
          OZ (ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, sequence_expr));
          OZ (ref_sys_exprs.push_back(std::pair<ObRawExpr*, ObRawExpr*>(q_name.ref_expr_, sequence_expr)));
        }
      }
    }

    // 检查 real_column 中是否存在简单列
    // create table t1 (x date, y date as (x)); 其中 y 就是一个简单列
    // Oracle 不允许生成列为简单列
    // 因为 partition 中也会使用本函数检查解析分区函数，因此用 need_check_simple_column 加以区分，
    // 在分区时不检查
    // 比如 create table t1 (x int) partition by hash(x);
    if (OB_SUCC(ret)
        && true == need_check_simple_column
        && T_REF_COLUMN == expr->get_expr_type()
        && !(columns.count() == 1
             && ObResolverUtils::is_external_pseudo_column_name(columns.at(0).col_name_))) {
      ret = OB_ERR_INVALID_COLUMN_EXPRESSION;
      LOG_WARN("simple column is not allowed in Oracle mode", K(ret), K(*expr));
    }

    if (OB_SUCC(ret) && real_columns.count() != columns.count()) {
      columns.reuse();
      OZ(columns.assign(real_columns), real_columns);
    }
  } else if (!is_oracle_mode()) {
    /* deal with default function */
    ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 1> ref_sys_exprs;
    ObSEArray<ObQualifiedName, 1> real_columns;
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
      const ObQualifiedName &q_name = columns.at(i);
      if (q_name.ref_expr_ != NULL) {
        q_name.ref_expr_->set_database_name(q_name.database_name_);
        //q_name.ref_expr_->set_table_id(table_id);
        q_name.ref_expr_->set_column_name(q_name.col_name_);
      }
    }
  }
  // check whether the expression is deterministic recursively
  if (OB_SUCC(ret) && ObResolverUtils::DISABLE_CHECK != check_status) {
    OZ (check_deterministic(expr, expr_factory.get_allocator(), check_status));
  }
  if (OB_SUCC(ret) && !is_oracle_mode() && (OB_NOT_NULL(new_table_schema))) {
    bool has_default = false;
    OZ (sql::ObDMLResolver::resolve_special_expr_static(
                            new_table_schema,
                            session_info,
                            expr_factory,
                            expr,
                            has_default,
                            check_status));
    if (OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION == ret
        && (ObResolverUtils::CHECK_FOR_FUNCTION_INDEX == check_status)) {
      ret = OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED;
      LOG_WARN("only pure sys function can be indexed");
    }
  }
  return ret;
}
/*
int ObRawExprUtils::try_transform_udt_col_expr_for_gen_col_recursively(const ObSQLSessionInfo &session,
                                                           ObRawExprFactory &expr_factory,
                                                           ObRawExpr *expr,
                                                           bool &expr_changed)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check_stack_overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(expr);
    if (col_expr->is_xml_column()) {

 if (OB_SUCC(ret)) {
      column_ref.parents_expr_info_ = ctx_.parents_expr_info_;
      ObColumnRefRawExpr *b_expr = NULL;
      if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_COLUMN, b_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else if (OB_ISNULL(b_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column ref expr is null");
      } else {
        column_ref.ref_expr_ = b_expr;
        if (OB_FAIL(ctx_.columns_->push_back(column_ref))) {
          LOG_WARN("Add column failed", K(ret));
        } else {
          expr = b_expr;
        }
      }
    }


    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *child_expr = expr->get_param_expr(i);
      CK (OB_NOT_NULL(child_expr));
      OZ (try_transform_udt_col_expr_for_gen_col_recursively(session, arg, expr_factory,
                                              child_expr, expr_changed));
    }
  }
  return ret;
}*/

int ObRawExprUtils::transform_query_udt_column_expr(const ObSQLSessionInfo& session,
                                                    ObRawExprFactory &expr_factory,
                                                    ObRawExpr *hidden_blob_expr,
                                                    ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *sys_makexml = NULL;
  ObConstRawExpr *c_expr = NULL;
  ObColumnRefRawExpr *hidd_col = NULL;
  if (OB_ISNULL(hidden_blob_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(hidden_blob_expr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_MAKEXML, sys_makexml))) {
    LOG_WARN("failed to create fun sys_makexml expr", K(ret));
  } else if (OB_ISNULL(sys_makexml)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys_makexml expr is null", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_INT, c_expr))) {
    LOG_WARN("create dest type expr failed", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null");
  } else {
    ObObj val;
    val.set_int(0);
    c_expr->set_value(val);
    c_expr->set_param(val);
    if (OB_FAIL(sys_makexml->set_param_exprs(c_expr, hidden_blob_expr))) {
      LOG_WARN("set param expr fail", K(ret));
    } else if (FALSE_IT(sys_makexml->set_func_name(ObString::make_string("SYS_MAKEXML")))) {
    } else if (OB_FAIL(sys_makexml->formalize(&session))) {
      LOG_WARN("failed to formalize", K(ret));
    } else {
      new_expr = sys_makexml;
    }
  }
  return ret;
}

int ObRawExprUtils::try_modify_udt_col_expr_for_gen_col_recursively(const ObSQLSessionInfo &session,
                                                                    const ObTableSchema &table_schema,
                                                                    ObIArray<ObColumnSchemaV2 *> &resolved_cols,
                                                                    ObRawExprFactory &expr_factory,
                                                                    ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check_stack_overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (expr->is_column_ref_expr() &&
             static_cast<ObColumnRefRawExpr *>(expr)->is_xml_column()) {
    ObColumnSchemaV2 *hidden_col = table_schema.get_xml_hidden_column_schema(static_cast<ObColumnRefRawExpr *>(expr)->get_column_id(),
                                                                             static_cast<ObColumnRefRawExpr *>(expr)->get_udt_set_id());
    for (int64_t i = 0; i < resolved_cols.count() && OB_SUCC(ret) && OB_ISNULL(hidden_col); ++i) {
      if (OB_ISNULL(resolved_cols.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN( "resolved_cols has null pointer", K(i), K(resolved_cols));
      } else if (static_cast<ObColumnRefRawExpr *>(expr)->get_udt_set_id() == resolved_cols.at(i)->get_udt_set_id() &&
                 static_cast<ObColumnRefRawExpr *>(expr)->get_column_id() != resolved_cols.at(i)->get_column_id()) {
          hidden_col = resolved_cols.at(i);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(hidden_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hidden column is null", K(ret));
    } else {
      ObColumnRefRawExpr *hidden_expr = NULL;
      ObRawExpr *new_expr = NULL;
      if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, hidden_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else if (OB_ISNULL(hidden_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column ref expr is null");
      } else if (OB_FAIL(ObRawExprUtils::init_column_expr(*hidden_col, *hidden_expr))) {
        LOG_WARN("init column expr failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::transform_query_udt_column_expr(session, expr_factory, hidden_expr, new_expr))) {
        LOG_WARN("transform query udt column expr failed", K(ret));
      } else {
        expr = new_expr;
      }
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *child_expr = expr->get_param_expr(i);
      ObRawExpr *tmp_expr = child_expr;
      CK (OB_NOT_NULL(child_expr));
      OZ (try_modify_udt_col_expr_for_gen_col_recursively(session, table_schema, resolved_cols, expr_factory, child_expr));
      if (OB_SUCC(ret) && (tmp_expr != child_expr)) {
        ObSysFunRawExpr *sys_func_expr = NULL;
        // replace happened
        CK (OB_NOT_NULL(sys_func_expr = dynamic_cast<ObSysFunRawExpr *>(expr)));
        OZ (sys_func_expr->replace_param_expr(i, child_expr));
      }
    }
  }
  return ret;
}

/**
 * @brief 递归地修改生成列表达式
 * @param session session
 * @param arg arg 允许为空，此时使用 session 中的 nls_xx_format
 * @param expr_factory
 * @param expr 被修改的表达式
 * @param expr_changed 表示 expr 是否被改写过的 flag
 * @return ret
 */
int ObRawExprUtils::try_modify_expr_for_gen_col_recursively(const ObSQLSessionInfo &session,
                                                           const obrpc::ObCreateIndexArg *arg,
                                                           ObRawExprFactory &expr_factory,
                                                           ObRawExpr *expr,
                                                           bool &expr_changed)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check_stack_overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_FAIL(try_add_to_char_on_expr(session, arg, expr_factory,
                                             expr, expr_changed))) {
    LOG_WARN("try_add_to_char_on_expr failed", K(ret), KPC(expr));
  } else if (OB_FAIL(try_add_nls_fmt_in_to_char_expr(session, arg, expr_factory,
                                                     expr, expr_changed))) {
    LOG_WARN("try_add_nls_fmt_in_to_char_expr failed", K(ret), KPC(expr));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *child_expr = expr->get_param_expr(i);
      CK (OB_NOT_NULL(child_expr));
      OZ (try_modify_expr_for_gen_col_recursively(session, arg, expr_factory,
                                              child_expr, expr_changed));
    }
  }
  return ret;
}

/**
 * @brief 在 oracle 中，当expr为to_date/timestamp/timestamptz 时，将其第一个参数套上 to_char，以存储
 * 当前 session 的 nls_xx_format 信息
 * to_date(c1, 'yyyy-mm-dd') => to_date(to_char(c1, nls_date_format), 'yyyy-mm-dd')
 * @param session session
 * @param arg arg允许为空，为空时，使用 session 中的 nls_xx_format
 * @param expr_factory factory
 * @param expr 被修改的表达式
 * @param expr_changed 表示 expr 是否被改写过的 flag
 * @return
 */
int ObRawExprUtils::try_add_to_char_on_expr(const ObSQLSessionInfo &session,
                                            const obrpc::ObCreateIndexArg *arg,
                                            ObRawExprFactory &expr_factory,
                                            ObRawExpr *expr,
                                            bool &expr_changed)
{
  int ret = OB_SUCCESS;
  bool need_add_to_char = false;
  ObRawExpr *first_param_expr = NULL;
  ObSysFunRawExpr *sys_func_expr = NULL;
  ObObjType data_type = ObNullType;
  CK (OB_NOT_NULL(expr));
  // 检查expr是否为to_date/timestamp/timestamptz其中之一，只有在这三种函数中，才有必要添加 to_char
  if (OB_SUCC(ret) && is_oracle_mode()) {
    if (expr->is_sys_func_expr() && expr->is_oracle_to_time_expr()) {
      need_add_to_char = true;
    } else {
      need_add_to_char = false;
    }
  }
  // 只有在第一个参数是时间类型的列，或者是一个返回值为时间类型的表达式时，才需要添加 to_char
  if (OB_SUCC(ret) && need_add_to_char) {
    need_add_to_char = false;
    CK (OB_NOT_NULL(first_param_expr = expr->get_param_expr(0)));
    if (OB_SUCC(ret)) {
      data_type = first_param_expr->get_data_type();
      if (ObTimestampTZType == data_type
          || ObTimestampLTZType == data_type
          || ObTimestampNanoType == data_type
          || ObTimestampType == data_type
          || ObDateTimeType == data_type) {
        need_add_to_char = true;
      }
    }
  }
  // 在 first_param_expr 外面套上一个 to_char expr
  if (OB_SUCC(ret) && need_add_to_char) {
    ObSysFunRawExpr *to_char_expr = NULL;
    OZ (actual_add_to_char_on_expr(session, arg, expr_factory, *first_param_expr,
                                   data_type, to_char_expr));
    CK (OB_NOT_NULL(to_char_expr));
    if (OB_SUCC(ret) && first_param_expr != to_char_expr) {
      CK (OB_NOT_NULL(sys_func_expr = dynamic_cast<ObSysFunRawExpr *>(expr)));
      OZ (sys_func_expr->replace_param_expr(0, to_char_expr));
      OX (expr_changed = true;)
    }
  }

  return ret;
}

/**
 * @brief 根据 src_expr 生成 to_char 表达式，供 try_add_to_char_on_expr 使用
 * src_expr => to_char_expr(src_expr, nls_xx_format)
 * @param session session
 * @param arg arg允许为空，为空时，使用 session 中的 nls_xx_format
 * @param expr_factory factory
 * @param src_expr to_date/to_timestamp/to_timestamp_tz 三种表达式之一
 * @param data_type src_expr 第一个参数的类型
 * @param to_char_expr 生成的 to_char 表达式, to_char_expr 的第一个参数是 src_expr
 * @return
 */
int ObRawExprUtils::actual_add_to_char_on_expr(const ObSQLSessionInfo& session,
                                               const obrpc::ObCreateIndexArg *arg,
                                               ObRawExprFactory &expr_factory,
                                               ObRawExpr &src_expr,
                                               const ObObjType &data_type,
                                               ObSysFunRawExpr *&to_char_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *dst_expr = NULL;
  ObCollationType collation_server = CS_TYPE_INVALID;
  // 1. 创建空白的表达式
  OZ (expr_factory.create_raw_expr(T_FUN_SYS_TO_CHAR, to_char_expr));
  CK (OB_NOT_NULL(to_char_expr));
  OZ (expr_factory.create_raw_expr(T_CHAR, dst_expr));
  CK (OB_NOT_NULL(dst_expr));
  // 2. 配置 to_char 中的第二个参数, dst_expr
  if (OB_SUCC(ret)) {
    ObObjParam val;
    switch (data_type) {
      case ObDateTimeType: {
        if (NULL == arg) {
          val.set_char(session.get_local_nls_date_format());
        } else {
          val.set_char(arg->nls_date_format_);
        }
        break;
      }
      case ObTimestampType:
      case ObTimestampLTZType:
      case ObTimestampNanoType: {
        if (NULL == arg) {
          val.set_char(session.get_local_nls_timestamp_format());
        } else {
          val.set_char(arg->nls_timestamp_format_);
        }
        break;
      }
      case ObTimestampTZType: {
        if (NULL == arg) {
          val.set_char(session.get_local_nls_timestamp_tz_format());
        } else {
          val.set_char(arg->nls_timestamp_tz_format_);
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid column_type", K(ret), K(data_type));
        break;
      }
    }

    OZ (session.get_collation_server(collation_server));
    OX (val.set_collation_type(collation_server));
    OX (val.set_collation_level(CS_LEVEL_COERCIBLE));
    OX (val.set_length(static_cast<ObLength>(ObCharset::strlen_char(val.get_collation_type(),
                                                                    val.get_string_ptr(),
                                                                    val.get_string_len()))));
    OX (val.set_length_semantics(LS_CHAR));
    OX (val.set_param_meta());
    OX (dst_expr->set_value(val));
    OX (dst_expr->set_param(val));
    OX (dst_expr->set_expr_obj_meta(val.get_param_meta()));
    OX (dst_expr->set_accuracy(val.get_accuracy()));
    OX (dst_expr->set_result_flag(val.get_result_flag()));
    OX (dst_expr->set_data_type(ObCharType));
  }
  // 3. 配置 to_char
  OZ (to_char_expr->add_param_expr(&src_expr));
  OX (to_char_expr->set_func_name(ObString::make_string(N_TO_CHAR)));
  OZ (to_char_expr->add_param_expr(dst_expr));
  if (OB_SUCC(ret) && src_expr.is_for_generated_column()) {
    to_char_expr->set_for_generated_column();
  }

  return ret;
}


/**
 * @brief 在 Oracle 中，当 to_char 只有一个参数，且第一个参数为时间类型时，会将 nls_xx_format 添加为第二个参数
 * to_char(date) => to_char(date, nls_xx_format)
 * @param session session
 * @param arg arg允许为空，为空时，使用 session 中的 nls_xx_format
 * @param expr_factory factory
 * @param expr 被修改的 to_char 表达式
 * @param expr_changed 表示 expr 是否被改写过的 flag
 * @return ret
 */
int ObRawExprUtils::try_add_nls_fmt_in_to_char_expr(const ObSQLSessionInfo &session,
                                                    const obrpc::ObCreateIndexArg *arg,
                                                    ObRawExprFactory &expr_factory,
                                                    ObRawExpr *expr,
                                                    bool &expr_changed)
{
  int ret = OB_SUCCESS;
  bool need_add_nls_fmt = false;
  ObRawExpr *first_param_expr = NULL;
  ObObjType data_type = ObNullType;
  CK (OB_NOT_NULL(expr));
  // 检查expr是否为to_char
  if (OB_SUCC(ret) && is_oracle_mode()) {
    if (expr->is_sys_func_expr() && T_FUN_SYS_TO_CHAR == expr->get_expr_type()) {
      need_add_nls_fmt = true;
    } else {
      need_add_nls_fmt = false;
    }
  }
  // 只有在第一个参数是时间类型的列，或者是一个返回值为时间类型的表达式时，才需要修改 to_Char
  if (OB_SUCC(ret) && need_add_nls_fmt) {
    need_add_nls_fmt = false;
    CK (OB_NOT_NULL(first_param_expr = expr->get_param_expr(0)));
    // 当且仅当 to_char 只有一个参数时
    if (OB_SUCC(ret) && 1 == expr->get_param_count()) {
      data_type = first_param_expr->get_data_type();
      if (ObTimestampTZType == data_type
          || ObTimestampLTZType == data_type
          || ObTimestampNanoType == data_type
          || ObTimestampType == data_type
          || ObDateTimeType == data_type
          || ObIntervalYMType == data_type
          || ObIntervalDSType == data_type) {
        need_add_nls_fmt = true;
      }
    }
  }
  // 为 to_char 添加第二个参数
  if (OB_SUCC(ret) && need_add_nls_fmt) {
    ObSysFunRawExpr *to_char_expr = dynamic_cast<ObSysFunRawExpr *>(expr);
    CK (OB_NOT_NULL(to_char_expr));
    OZ (actual_add_nls_fmt_in_to_char_expr(session, arg, expr_factory,
                                           data_type, to_char_expr));
    OX (expr_changed = true;)
  }

  return ret;
}

/**
 * @brief 为 to_char_expr 添加第二个参数 nls_xx_format
 * @param session session
 * @param arg arg允许为空，为空时，使用 session 中的 nls_xx_format
 * @param expr_factory factory
 * @param data_type 第一个参数类型
 * @param to_char_expr 被修改的 to_char 表达式
 * @return
 */
int ObRawExprUtils::actual_add_nls_fmt_in_to_char_expr(const ObSQLSessionInfo& session,
                                                       const obrpc::ObCreateIndexArg *arg,
                                                       ObRawExprFactory &expr_factory,
                                                       const ObObjType &data_type,
                                                       ObSysFunRawExpr *to_char_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *fmt_expr = NULL;
  ObCollationType collation_server = CS_TYPE_INVALID;
  // 1. 创建空白的表达式
  OZ (expr_factory.create_raw_expr(T_CHAR, fmt_expr));
  CK (OB_NOT_NULL(fmt_expr));
  CK (OB_NOT_NULL(to_char_expr));
  // 2. 配置 to_char 中的第二个参数, fmt_expr
  if (OB_SUCC(ret)) {
    ObObjParam val;
    switch (data_type) {
      case ObDateTimeType: {
        if (NULL == arg) {
          val.set_char(session.get_local_nls_date_format());
        } else {
          val.set_char(arg->nls_date_format_);
        }
        break;
      }
      case ObTimestampType:
      case ObTimestampLTZType:
      case ObTimestampNanoType: {
        if (NULL == arg) {
          val.set_char(session.get_local_nls_timestamp_format());
        } else {
          val.set_char(arg->nls_timestamp_format_);
        }
        break;
      }
      case ObTimestampTZType: {
        if (NULL == arg) {
          val.set_char(session.get_local_nls_timestamp_tz_format());
        } else {
          val.set_char(arg->nls_timestamp_tz_format_);
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid column_type", K(ret), K(data_type));
        break;
      }
    }

    OZ (session.get_collation_server(collation_server));
    OX (val.set_collation_type(collation_server));
    OX (val.set_collation_level(CS_LEVEL_COERCIBLE));
    OX (val.set_length(static_cast<ObLength>(ObCharset::strlen_char(val.get_collation_type(),
                                                                    val.get_string_ptr(),
                                                                    val.get_string_len()))));
    OX (val.set_length_semantics(LS_CHAR));
    OX (val.set_param_meta());
    OX (fmt_expr->set_value(val));
    OX (fmt_expr->set_param(val));
    OX (fmt_expr->set_expr_obj_meta(val.get_param_meta()));
    OX (fmt_expr->set_accuracy(val.get_accuracy()));
    OX (fmt_expr->set_result_flag(val.get_result_flag()));
    OX (fmt_expr->set_data_type(ObCharType));
  }
  // 3. 配置 to_char
  OZ (to_char_expr->add_param_expr(fmt_expr));

  return ret;
}

int ObRawExprUtils::build_raw_expr(ObRawExprFactory &expr_factory,
                                   const ObSQLSessionInfo &session_info,
                                   const ParseNode &node,
                                   ObRawExpr *&expr,
                                   ObIArray<ObQualifiedName> &columns,
                                   ObIArray<ObVarInfo> &sys_vars,
                                   ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                   ObIArray<ObWinFunRawExpr*> &win_exprs,
                                   ObIArray<ObSubQueryInfo> &sub_query_info,
                                   ObIArray<ObUDFInfo> &udf_info,
                                   common::ObIArray<ObOpRawExpr*> &op_exprs,
                                   bool is_prepare_protocol/*= false*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_raw_expr(expr_factory,
                             session_info,
                             NULL,
                             NULL,
                             T_NONE_SCOPE,
                             NULL,
                             NULL,
                             NULL,
                             node,
                             expr,
                             columns,
                             sys_vars,
                             aggr_exprs,
                             win_exprs,
                             sub_query_info,
                             udf_info,
                             op_exprs,
                             is_prepare_protocol))) {
    LOG_WARN("failed to build raw expr", K(ret));
  }
  return ret;
}

int ObRawExprUtils::build_raw_expr(ObRawExprFactory &expr_factory,
                                   const ObSQLSessionInfo &session_info,
                                   ObSchemaChecker *schema_checker,
                                   pl::ObPLBlockNS *ns,
                                   ObStmtScope current_scope,
                                   ObStmt *stmt,
                                   const ParamStore *param_list,
                                   ExternalParams *external_param_info,
                                   const ParseNode &node,
                                   ObRawExpr *&expr,
                                   ObIArray<ObQualifiedName> &columns,
                                   ObIArray<ObVarInfo> &sys_vars,
                                   ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                   ObIArray<ObWinFunRawExpr*> &win_exprs,
                                   ObIArray<ObSubQueryInfo> &sub_query_info,
                                   ObIArray<ObUDFInfo> &udf_info,
                                   ObIArray<ObOpRawExpr*> &op_exprs,
                                   bool is_prepare_protocol/*= false*/,
                                   TgTimingEvent tg_timing_event,
                                   bool use_def_collation,
                                   ObCollationType def_collation)
{
  int ret = OB_SUCCESS;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  if (OB_FAIL(session_info.get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(session_info.get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObExprResolveContext ctx(expr_factory, session_info.get_timezone_info(), OB_NAME_CASE_INVALID);
    if (use_def_collation) {
      ctx.dest_collation_ = def_collation;
    } else {
      ctx.dest_collation_ = collation_connection;
    }
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = param_list;
    ctx.is_extract_param_type_ = !is_prepare_protocol; //when prepare do not extract
    ctx.external_param_info_ = external_param_info;
    ctx.current_scope_ = current_scope;
    ctx.stmt_ = stmt;
    ctx.schema_checker_ = schema_checker;
    ctx.session_info_ = &session_info;
    ctx.secondary_namespace_ = ns;
    ctx.tg_timing_event_ = tg_timing_event;
    ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
    ObArray<ObInListInfo> inlist_infos;
    ObSEArray<ObMatchFunRawExpr*, 1> match_exprs;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(session_info.get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&node, expr, columns, sys_vars,
                                             sub_query_info, aggr_exprs, win_exprs,
                                             udf_info, op_exprs, user_var_exprs, inlist_infos, match_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObRawExprUtils::build_generated_column_expr(const ObString &expr_str,
                                                ObRawExprFactory &expr_factory,
                                                const ObSQLSessionInfo &session_info,
                                                uint64_t table_id,
                                                const ObTableSchema &table_schema,
                                                const ObColumnSchemaV2 &gen_col_schema,
                                                ObRawExpr *&expr,
                                                const bool sequence_allowed,
                                                ObDMLResolver *dml_resolver,
                                                const ObSchemaChecker *schema_checker,
                                                const ObResolverUtils::PureFunctionCheckStatus
                                                  check_status)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObSEArray<ObRawExpr *, 6> real_exprs;
  const ObColumnSchemaV2 *col_schema = NULL;
  ObSQLMode sql_mode = session_info.get_sql_mode();
  ObCollationType cs_type = session_info.get_local_collation_connection();
  if (OB_FAIL(ObSQLUtils::merge_solidified_var_into_collation(
        gen_col_schema.get_local_session_var(), cs_type))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::merge_solidified_var_into_sql_mode(
              &gen_col_schema.get_local_session_var(), sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(build_generated_column_expr(expr_str, expr_factory, session_info, sql_mode, cs_type,
                                          expr, columns, &table_schema, sequence_allowed, dml_resolver,
                                          schema_checker, check_status))) {
    LOG_WARN("build generated column expr failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObQualifiedName &q_name = columns.at(i);
    if (q_name.is_pl_udf()) {
      OZ (resolve_gen_column_udf_expr(expr,
                                      const_cast<ObQualifiedName &>(q_name),
                                      expr_factory,
                                      session_info,
                                      const_cast<ObSchemaChecker *>(schema_checker),
                                      columns,
                                      real_exprs,
                                      NULL));
      OZ (real_exprs.push_back(expr), q_name);
    } else if (table_schema.is_external_table()) {
      if (OB_FAIL(ObResolverUtils::resolve_external_table_column_def(expr_factory, session_info, q_name, real_exprs, expr, &gen_col_schema))) {
        LOG_WARN("fail to resolve external table column", K(ret));
      }
    } else {
      if (OB_UNLIKELY(!q_name.database_name_.empty()) || OB_UNLIKELY(!q_name.tbl_name_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid generated column name", K(q_name));
      } else if (OB_ISNULL(col_schema = table_schema.get_column_schema(q_name.col_name_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null");
      } else if (OB_FAIL(init_column_expr(*col_schema, *q_name.ref_expr_))) {
        LOG_WARN("init column expr failed", K(ret));
      } else {
        q_name.ref_expr_->set_ref_id(table_id, col_schema->get_column_id());
        OZ (real_exprs.push_back(q_name.ref_expr_), q_name);
      }
    }
  }
  if (OB_SUCC(ret)) {
    //set local session info for generate columns
    if (OB_FAIL(expr->formalize_with_local_vars(&session_info, &gen_col_schema.get_local_session_var(), OB_INVALID_INDEX_INT64))) {
      LOG_WARN("fail to formalize real dependant expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && !gen_col_schema.is_hidden()) {
    //只有用户定义的生成列才需要添加类型转换的表达式，内部生成的生成列不需要进行类型转换
    ObExprResType expected_type;
    expected_type.set_meta(gen_col_schema.get_meta_type());
    expected_type.set_accuracy(gen_col_schema.get_accuracy());
    expected_type.set_result_flag(calc_column_result_flag(gen_col_schema));
    if (ObRawExprUtils::need_column_conv(expected_type, *expr)) {
      if (OB_FAIL(build_column_conv_expr(expr_factory, &gen_col_schema, expr, &session_info,
                                        &gen_col_schema.get_local_session_var()))) {
        LOG_WARN("create cast expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::resolve_gen_column_udf_expr(ObRawExpr *&udf_expr,
                                                ObQualifiedName &q_name, // udf q_name
                                                ObRawExprFactory &expr_factory,
                                                const ObSQLSessionInfo &session_info,
                                                ObSchemaChecker *schema_checker,
                                                ObIArray<ObQualifiedName> &columns, // all columns after raw expr resolver
                                                ObIArray<ObRawExpr*> &real_exprs, // all resolved exprs before this one
                                                ObDMLStmt *stmt
                                                )
{
  int ret = OB_SUCCESS;
  // ObRawExpr *expr = NULL;
  // ObResolverParams params;
  // ObStmtFactory stmt_factory(expr_factory.get_allocator());
  // params.expr_factory_ = &expr_factory;
  // params.allocator_ = &(expr_factory.get_allocator());
  // params.session_info_ = const_cast<ObSQLSessionInfo *>(&session_info);
  // params.schema_checker_ = const_cast<ObSchemaChecker *>(schema_checker);
  // params.sql_proxy_ = GCTX.sql_proxy_;
  // params.stmt_factory_ = &stmt_factory;
  // params.query_ctx_ = NULL;
  // // indicate not from pl scope; all symbol is searched inside schema
  // params.secondary_namespace_ = NULL;

  // CK (OB_NOT_NULL(schema_checker));
  // CK (OB_NOT_NULL(schema_checker->get_schema_guard()));
  // if (OB_FAIL(ret)) {
  //   LOG_WARN("faile to check params, NULL schema guard", K(ret));
  // } else if (OB_FAIL(ObResolverUtils::resolve_external_symbol(*params.allocator_,
  //                                                       *params.expr_factory_,
  //                                                       *params.session_info_,
  //                                                       *params.schema_checker_->get_schema_guard(),
  //                                                       params.sql_proxy_,
  //                                                       &(params.external_param_info_),
  //                                                       params.secondary_namespace_,
  //                                                       q_name,
  //                                                       columns,
  //                                                       real_exprs,
  //                                                       expr))) {
  //   LOG_WARN("failed to resolve var", K(q_name), K(ret));
  // } else if (OB_ISNULL(expr)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("Invalid expr", K(expr), K(ret));
  // } else if (expr->is_udf_expr() && !expr->is_deterministic()) {
  //   ret = OB_ERR_USE_UDF_NOT_DETERMIN;
  //   LOG_WARN("generated column expect deterministic udf", K(q_name), K(ret));
  //   LOG_USER_ERROR(OB_ERR_USE_UDF_NOT_DETERMIN);
  // } else if (expr->is_udf_expr()) {
  //   ObUDFRawExpr *udf1_expr = static_cast<ObUDFRawExpr*>(expr);
  //   ObSchemaObjVersion udf_version;
  //   CK (OB_NOT_NULL(udf1_expr));
  //   if (OB_SUCC(ret) && udf1_expr->need_add_dependency() && OB_NOT_NULL(stmt)) {
  //     OZ (udf1_expr->get_schema_object_version(udf_version));
  //     OZ (stmt->add_global_dependency_table(udf_version));
  //   }
  //   //for udf without params, we just set called_in_sql = true,
  //   //if this expr go through pl :: build_raw_expr later,
  //   //the flag will change to false;
  //   OX (expr->set_is_called_in_sql(true));
  //   OX (udf_expr = expr);
  // } else {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("unexpected expr, expect udf expr", K(ret), K(q_name));
  // }
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "using udf as generated column");
  LOG_WARN("using udf as generated column is not supported", K(ret));
  return ret;
}

int ObRawExprUtils::check_generated_column_expr_str(const common::ObString &expr_str,
                                                    const ObSQLSessionInfo &session,
                                                    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
  ObRawExprFactory expr_factory(allocator);
  ObRawExpr *expr = NULL;
  ObArray<ObQualifiedName> columns;
  if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(expr_str, expr_factory, session,
      expr, columns, &table_schema, false /* allow_sequence*/, NULL))) {
    LOG_WARN("generated column expr str after printer is valid", K(expr_str), K(ret));
  }
  return ret;
}

int ObRawExprUtils::build_pad_expr_recursively(ObRawExprFactory &expr_factory,
                                               const ObSQLSessionInfo &session,
                                               const ObTableSchema &table_schema,
                                               const ObColumnSchemaV2 &gen_col_schema,
                                               ObRawExpr *&expr,
                                               const ObLocalSessionVar *local_vars,
                                               int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));

  if (OB_SUCC(ret) && expr->get_param_count() > 0) {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      OZ (SMART_CALL(build_pad_expr_recursively(expr_factory,
                                                session,
                                                table_schema,
                                                gen_col_schema,
                                                expr->get_param_expr(i),
                                                local_vars,
                                                local_var_id)));
    }
  }
  if (OB_SUCC(ret) && expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *b_expr = static_cast<ObColumnRefRawExpr*>(expr);
    uint64_t column_id = b_expr->get_column_id();
    if (OB_SUCC(ret)) {
      const ObColumnSchemaV2 *column_schema = table_schema.get_column_schema(column_id);
      if (NULL == column_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column schema fail", K(column_schema));
      } else if (ObObjMeta::is_binary(column_schema->get_data_type(), column_schema->get_collation_type())) {
        if (OB_FAIL(build_pad_expr(expr_factory, false, column_schema, expr, &session, local_vars, local_var_id))) {
          LOG_WARN("fail to build pading expr for binary", K(ret));
        }
      } else if (ObCharType == column_schema->get_data_type()
                || ObNCharType == column_schema->get_data_type()) {
        if (gen_col_schema.has_column_flag(PAD_WHEN_CALC_GENERATED_COLUMN_FLAG)) {
          if (OB_FAIL(build_pad_expr(expr_factory, true, column_schema, expr, &session, local_vars, local_var_id))) {
            LOG_WARN("fail to build pading expr for char", K(ret));
          }
        } else {
          if (OB_FAIL(build_trim_expr(column_schema, expr_factory, &session, expr, local_vars, local_var_id))) {
            LOG_WARN("fail to build trime expr for char", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRawExprUtils::build_rls_predicate_expr(const ObString &expr_str,
                                             ObRawExprFactory &expr_factory,
                                             const ObSQLSessionInfo &session_info,
                                             ObIArray<ObQualifiedName> &columns,
                                             ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const ParseNode *node = NULL;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObUDFInfo> udf_info;
  ObArray<ObOpRawExpr*> op_exprs;

  if (OB_FAIL(parse_bool_expr_node_from_str(expr_str,
      expr_factory.get_allocator(), node))) {
    LOG_WARN("parse expr node from string failed", K(ret));
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null");
  } else if (OB_FAIL(build_raw_expr(expr_factory,
                                    session_info,
                                    *node,
                                    expr,
                                    columns,
                                    sys_vars,
                                    aggr_exprs,
                                    win_exprs,
                                    sub_query_info,
                                    udf_info,
                                    op_exprs))) {
    LOG_WARN("failed to get collation_connection", K(ret));
  } else if (OB_UNLIKELY(udf_info.count() > 0)) {
    ret = OB_ERR_POLICY_FUNCTION;
    LOG_WARN("user defined function in rls predicate", K(ret));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_SUB_QUERY))) {
    ret = OB_ERR_POLICY_FUNCTION;
    LOG_WARN("subquery in rls predicate", K(ret));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_AGG))) {
    ret = OB_ERR_POLICY_FUNCTION;
    LOG_WARN("group function in rls predicate", K(ret));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_ROWNUM) || expr->has_flag(CNT_PSEUDO_COLUMN))) {
    ret = OB_ERR_POLICY_FUNCTION;
    LOG_WARN("pseudo column or operator in rls predicate", K(ret));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_WINDOW_FUNC))) {
    ret = OB_ERR_POLICY_FUNCTION;
    LOG_WARN("window function in rls predicate", K(ret));
  } else if (OB_UNLIKELY(!IS_COMPARISON_OP(expr->get_expr_type()))) {
    ret = OB_ERR_POLICY_PREDICATE;
    LOG_WARN("predicate is not comparison op", KPC(expr), K(ret));
  }
  return ret;
}

// compare two raw expressions
bool ObRawExprUtils::is_same_raw_expr(const ObRawExpr *src, const ObRawExpr *dst)
{
  bool is_same = false;
  if ((NULL == src && NULL != dst) || (NULL != src && NULL == dst)) {
    is_same = false;
  } else if (NULL == src && NULL == dst) {
    is_same = true;
  } else {
    if (!src->same_as(*dst)) {
      is_same = false;
    } else {
      is_same = true;
    }
  }
  return is_same;
}

//把计算表达式中的所有column表达式替换掉
int ObRawExprUtils::replace_all_ref_column(ObRawExpr *&raw_expr, const common::ObIArray<ObRawExpr *> &exprs, int64_t &offset)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(raw_expr));
  } else if (offset >= exprs.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("beyond the index", K(offset), K(exprs.count()));
  }else if (T_REF_COLUMN == raw_expr->get_expr_type()) {
    LOG_DEBUG("replace leaf node", K(*raw_expr), K(*exprs.at(offset)), K(offset));
    raw_expr = exprs.at(offset);
    ++offset;
  } else {
    int64_t N = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N && offset < exprs.count(); ++i) {
      ObRawExpr *&child_expr = raw_expr->get_param_expr(i);
      if (OB_FAIL(replace_all_ref_column(child_expr, exprs, offset))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    } // end for
  }
  return ret;
}

// if %expr_factory is not NULL, will deep copy %to expr. default behavior is shallow copy
// if except_exprs is not NULL, will skip the expr in except_exprs
int ObRawExprUtils::replace_ref_column(ObRawExpr *&raw_expr, ObRawExpr *from,
                                       ObRawExpr *to,
                                       const ObIArray<ObRawExpr*> *except_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr) || OB_ISNULL(from) || OB_ISNULL(to)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(raw_expr), K(from), K(to));
  } else if (raw_expr == to) {
    // do nothing
    // in case:    parent(child) = to (from)
    // replace as: parenet(child) = to (to)
  } else if (NULL != except_exprs && is_contain(*except_exprs, raw_expr)) {
    // do nothing
  } else if (raw_expr == from) {
    raw_expr = to;
  } else if (raw_expr->is_query_ref_expr()) {
    ObSelectStmt *ref_stmt = static_cast<ObQueryRefRawExpr*>(raw_expr)->get_ref_stmt();
    ObSEArray<ObRawExpr*, 16> relation_exprs;
    if (OB_ISNULL(ref_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(ref_stmt));
    } else if (OB_FAIL(ref_stmt->get_relation_exprs(relation_exprs))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    } else if (OB_FAIL(SMART_CALL(replace_ref_column(relation_exprs, from, to, except_exprs)))) {
      LOG_WARN("replace reference column failed", K(ret));
    }
  } else {
    int64_t N = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ObRawExpr *&child_expr = raw_expr->get_param_expr(i);
      if (OB_FAIL(SMART_CALL(replace_ref_column(child_expr, from, to, except_exprs)))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    } // end for
  }
  return ret;
}

int ObRawExprUtils::replace_ref_column(ObRawExpr *&raw_expr,
                                ObIArray<ObRawExpr *> &from,
                                ObIArray<ObRawExpr *> &to,
                                const ObIArray<ObRawExpr*> *except_exprs)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  if (OB_ISNULL(raw_expr) || OB_UNLIKELY(from.count() != to.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(raw_expr), K(from), K(to));
  } else if (from.count() == 0) {
    //do nothing
  } else if (NULL != except_exprs && is_contain(*except_exprs, raw_expr)) {
    //do nothing
  } else if (ObOptimizerUtil::find_item(from, raw_expr, &idx)) {
    if (OB_UNLIKELY(idx < 0 || idx >= to.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("find item failed", K(ret));
    } else {
      raw_expr = to.at(idx);
    }
  } else if (raw_expr->is_query_ref_expr()) {
    ObSelectStmt *ref_stmt = static_cast<ObQueryRefRawExpr*>(raw_expr)->get_ref_stmt();
    ObSEArray<ObRawExpr*, 16> relation_exprs;
    if (OB_ISNULL(ref_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(ref_stmt));
    } else if (OB_FAIL(ref_stmt->get_relation_exprs(relation_exprs))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    } else if (OB_FAIL(SMART_CALL(replace_ref_column(relation_exprs, from, to, except_exprs)))) {
      LOG_WARN("replace reference column failed", K(ret));
    }
  } else {
    int64_t N = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ObRawExpr *&child_expr = raw_expr->get_param_expr(i);
      if (OB_FAIL(SMART_CALL(replace_ref_column(child_expr, from, to, except_exprs)))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    } // end for
  }
  return ret;
}


int ObRawExprUtils::replace_ref_column(ObIArray<ObRawExpr *>&exprs, ObRawExpr *from, ObRawExpr *to,
                                       const ObIArray<ObRawExpr*> *except_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_ISNULL(from) || OB_ISNULL(to)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(from), K(to), K(ret));
  } else {
    ObRawExpr *tmp_raw_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      ObRawExpr *&raw_expr = tmp_raw_expr;
      if (OB_FAIL(exprs.at(i, raw_expr))) {
        LOG_WARN("failed to get raw expr", K(i), K(ret));
      } else if (OB_FAIL(SMART_CALL(replace_ref_column(raw_expr, from, to, except_exprs)))) {
        LOG_WARN("failed to replace_ref_column", K(from), K(to), K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObRawExprUtils::replace_ref_column(ObIArray<ObRawExpr *> &exprs,
                                      ObIArray<ObRawExpr *> &from,
                                      ObIArray<ObRawExpr *> &to,
                                      const ObIArray<ObRawExpr*> *except_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_UNLIKELY(from.count() != to.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(from), K(to), K(ret));
  } else {
    ObRawExpr *tmp_raw_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      ObRawExpr *&raw_expr = tmp_raw_expr;
      if (OB_FAIL(exprs.at(i, raw_expr))) {
        LOG_WARN("failed to get raw expr", K(i), K(ret));
      } else if (OB_FAIL(SMART_CALL(replace_ref_column(raw_expr, from, to, except_exprs)))) {
        LOG_WARN("failed to replace_ref_column", K(from), K(to), K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

bool ObRawExprUtils::is_all_column_exprs(const common::ObIArray<ObRawExpr*> &exprs)
{
  bool is_all_column = true;
  for (int64_t i = 0; is_all_column && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      is_all_column = false;
    } else if (!exprs.at(i)->is_column_ref_expr()) {
      is_all_column =false;
    } else { /*do nothing*/ }
  }
  return is_all_column;
}

int ObRawExprUtils::extract_set_op_exprs(const ObRawExpr *raw_expr,
                                         common::ObIArray<ObRawExpr*> &set_op_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else if (raw_expr->is_set_op_expr()) {
    if (OB_FAIL(add_var_to_array_no_dup(set_op_exprs, const_cast<ObRawExpr*>(raw_expr)))) {
      LOG_WARN("failed to append expr", K(ret));
    }
  } else {
    int64_t N = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(extract_set_op_exprs(raw_expr->get_param_expr(i),
                                                  set_op_exprs)))) {
        LOG_WARN("failed to extract set op exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_var_assign_exprs(const ObRawExpr *raw_expr,
                                             ObIArray<ObRawExpr*> &assign_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else if (raw_expr->has_flag(IS_ASSIGN_EXPR) && raw_expr->get_relation_ids().is_empty()) {
    if (OB_FAIL(add_var_to_array_no_dup(assign_exprs, const_cast<ObRawExpr*>(raw_expr)))) {
      LOG_WARN("failed to append expr", K(ret));
    }
  } else {
    int64_t N = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(extract_var_assign_exprs(raw_expr->get_param_expr(i),
                                                      assign_exprs)))) {
        LOG_WARN("failed to extract var assign op exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_set_op_exprs(const ObIArray<ObRawExpr*> &exprs,
                                         common::ObIArray<ObRawExpr*> &set_op_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Expr is NULL", K(ret), K(i));
    } else if (OB_FAIL(extract_set_op_exprs(exprs.at(i), set_op_exprs))) {
      LOG_WARN("Failed to extract column exprs", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::extract_column_exprs(const ObRawExpr *raw_expr,
                                         ObIArray<ObRawExpr*> &column_exprs,
                                         bool need_pseudo_column)
{
  int ret = OB_SUCCESS;
  if (NULL == raw_expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else {
    if (T_REF_COLUMN == raw_expr->get_expr_type()) {
      ret = add_var_to_array_no_dup(column_exprs, const_cast<ObRawExpr*>(raw_expr));
    } else if ((raw_expr->is_pseudo_column_expr() ||
                raw_expr->is_op_pseudo_column_expr() ||
                raw_expr->is_specified_pseudocolumn_expr()) && need_pseudo_column) {
      ret = add_var_to_array_no_dup(column_exprs, const_cast<ObRawExpr*>(raw_expr));
    } else {
      int64_t N = raw_expr->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        ret = extract_column_exprs(raw_expr->get_param_expr(i), column_exprs, need_pseudo_column);
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_contain_exprs(ObRawExpr *raw_expr,
                                          const common::ObIArray<ObRawExpr*> &src_exprs,
                                          common::ObIArray<ObRawExpr *> &contain_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (NULL == raw_expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (is_contain(src_exprs, raw_expr) &&
             OB_FAIL(add_var_to_array_no_dup(contain_exprs, raw_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else {
    int64_t N = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(extract_contain_exprs(raw_expr->get_param_expr(i), src_exprs, contain_exprs)))) {
        LOG_WARN("fail to extract contain expr", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_column_exprs(ObIArray<ObRawExpr*> &exprs,
                                         ObRelIds &rel_ids,
                                         ObIArray<ObRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Expr is NULL", K(ret), K(i));
    } else if (OB_FAIL(extract_column_exprs(exprs.at(i), rel_ids, column_exprs))) {
      LOG_WARN("Failed to extract column exprs", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRawExprUtils::extract_column_exprs(ObRawExpr* expr,
                                         ObRelIds &rel_ids,
                                         ObIArray<ObRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (expr->has_flag(IS_COLUMN) &&
             expr->get_relation_ids().is_subset(rel_ids)) {
    if (OB_FAIL(add_var_to_array_no_dup(column_exprs, expr))) {
      LOG_WARN("fail to add col expr to column_exprs", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(extract_column_exprs(expr->get_param_expr(i),
                                                  rel_ids,
                                                  column_exprs)))) {
        LOG_WARN("fail to extract column exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_invalid_sequence_expr(ObRawExpr *raw_expr,
                                                ObRawExpr *&sequence_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else if (T_FUN_SYS_SEQ_NEXTVAL == raw_expr->get_expr_type()
             && reinterpret_cast<const ObSequenceRawExpr *>(raw_expr)->get_sequence_id()
                  == OB_INVALID_ID) {
    sequence_expr = raw_expr;
  } else {
    int64_t N = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(extract_invalid_sequence_expr(raw_expr->get_param_expr(i),
                                                      sequence_expr)))) {
        LOG_WARN("failed to extract invalid sequence expr", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_col_aggr_winfunc_exprs(ObIArray<ObRawExpr*> &exprs,
                                                   ObIArray<ObRawExpr*> &column_aggr_winfunc_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Expr is NULL", K(ret), K(i));
    } else if (OB_FAIL(extract_col_aggr_winfunc_exprs(exprs.at(i), column_aggr_winfunc_exprs))) {
      LOG_WARN("Failed to extract col or aggr exprs", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRawExprUtils::extract_col_aggr_winfunc_exprs(ObRawExpr* expr,
                                                   ObIArray<ObRawExpr*> &column_aggr_winfunc_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (expr->has_flag(IS_AGG)) {
    if (OB_FAIL(add_var_to_array_no_dup(column_aggr_winfunc_exprs, expr))) {
      LOG_WARN("failed to add aggr exprs", K(ret));
    }
  } else if (expr->has_flag(IS_WINDOW_FUNC)) {
    if (OB_FAIL(add_var_to_array_no_dup(column_aggr_winfunc_exprs, expr))) {
      LOG_WARN("failed to add winfunc exprs", K(ret));
    }
  } else if (expr->has_flag(IS_COLUMN)) {
    if (OB_FAIL(add_var_to_array_no_dup(column_aggr_winfunc_exprs, expr))) {
      LOG_WARN("fail to add col expr to column_exprs", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(extract_col_aggr_winfunc_exprs(expr->get_param_expr(i),
                                                            column_aggr_winfunc_exprs)))) {
        LOG_WARN("fail to extract exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_col_aggr_exprs(ObIArray<ObRawExpr*> &exprs,
                                           ObIArray<ObRawExpr*> &column_or_aggr_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Expr is NULL", K(ret), K(i));
    } else if (OB_FAIL(extract_col_aggr_exprs(exprs.at(i), column_or_aggr_exprs))) {
      LOG_WARN("Failed to extract column or aggr exprs", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRawExprUtils::extract_col_aggr_exprs(ObRawExpr* expr,
                                           ObIArray<ObRawExpr*> &column_or_aggr_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (expr->has_flag(IS_AGG)) {
    if (OB_FAIL(add_var_to_array_no_dup(column_or_aggr_exprs, expr))) {
      LOG_WARN("failed to add aggr exprs", K(ret));
    }
  } else if (expr->has_flag(IS_COLUMN)) {
    if (OB_FAIL(add_var_to_array_no_dup(column_or_aggr_exprs, expr))) {
      LOG_WARN("fail to add col expr to column_exprs", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(extract_col_aggr_exprs(expr->get_param_expr(i),
                                                    column_or_aggr_exprs)))) {
        LOG_WARN("fail to extract exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::contain_virtual_generated_column(ObRawExpr *&expr, bool &is_contain_vir_gen_column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_column_ref_expr() &&
      static_cast<ObColumnRefRawExpr *>(expr)->is_virtual_generated_column() &&
      !static_cast<ObColumnRefRawExpr *>(expr)->is_xml_column()) {
    is_contain_vir_gen_column = true;
  }
  for (int64_t j = 0; OB_SUCC(ret) && is_contain_vir_gen_column == false && j < expr->get_param_count(); j++) {
    if (OB_ISNULL(expr->get_param_expr(j))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param_expr is NULL", K(j), K(ret));
    } else if (OB_FAIL(SMART_CALL(contain_virtual_generated_column(expr->get_param_expr(j), is_contain_vir_gen_column)))) {
      LOG_WARN("fail to contain virtual gen column", K(j), K(ret));
    } else {
      LOG_TRACE("conclude virtual generated column", K(is_contain_vir_gen_column));
    }
  }
  return ret;
}

// Extract the parent node of the generated column for
// deep copying to avoid bugs in shared expression scenarios
int ObRawExprUtils::extract_virtual_generated_column_parents(
  ObRawExpr *&par_expr, ObRawExpr *&child_expr, ObIArray<ObRawExpr*> &vir_gen_par_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(par_expr) || OB_ISNULL(child_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (child_expr->is_column_ref_expr() &&
      static_cast<ObColumnRefRawExpr *>(child_expr)->is_virtual_generated_column() &&
      !static_cast<ObColumnRefRawExpr *>(child_expr)->is_xml_column()) {
    if (OB_FAIL(add_var_to_array_no_dup(vir_gen_par_exprs, par_expr))) {
      LOG_WARN("failed to add winfunc exprs", K(ret));
    }
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < child_expr->get_param_count(); j++) {
    if (OB_ISNULL(child_expr->get_param_expr(j))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param_expr is NULL", K(j), K(ret));
    } else if (OB_FAIL(SMART_CALL(extract_virtual_generated_column_parents(
        child_expr, child_expr->get_param_expr(j), vir_gen_par_exprs)))) {
      LOG_WARN("fail to extract virtual gen column", K(j), K(ret));
    } else {
    }
  }
  return ret;
}

int ObRawExprUtils::extract_column_exprs(const ObIArray<ObRawExpr*> &exprs,
                                         ObIArray<ObRawExpr*> &column_exprs,
                                         bool need_pseudo_column)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Expr is NULL", K(ret), K(i));
    } else if (OB_FAIL(extract_column_exprs(exprs.at(i), column_exprs, need_pseudo_column))) {
      LOG_WARN("Failed to extract column exprs", K(ret));
    } else { } //do nothing
  }
  return ret;
}

int ObRawExprUtils::extract_column_exprs(const ObRawExpr *raw_expr,
                                         int64_t table_id,
                                         ObIArray<ObRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else {
    if (T_REF_COLUMN == raw_expr->get_expr_type()) {
      const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr*>(raw_expr);
      if (table_id != col_expr->get_table_id()) {
        //do nothing
      } else if (OB_FAIL(add_var_to_array_no_dup(column_exprs, const_cast<ObRawExpr*>(raw_expr)))) {
        LOG_WARN("failed to add var to array", K(ret));
      }
    } else {
      int64_t N = raw_expr->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        if (OB_FAIL(SMART_CALL(extract_column_exprs(raw_expr->get_param_expr(i),
                                                    table_id,
                                                    column_exprs)))) {
          LOG_WARN("failed to extract column exprs", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_column_exprs(const ObRawExpr *expr,
                                         ObIArray<const ObRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else if (expr->has_flag(IS_COLUMN)) {
    if (OB_FAIL(add_var_to_array_no_dup(column_exprs, expr))) {
      LOG_WARN("fail to add col expr to column_exprs", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(extract_column_exprs(expr->get_param_expr(i),
                                                  column_exprs)))) {
        LOG_WARN("fail to extract column ref exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_column_exprs(const ObIArray<ObRawExpr*> &exprs,
                                         int64_t table_id,
                                         ObIArray<ObRawExpr *> &column_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Expr is NULL", K(ret), K(i));
    } else if (OB_FAIL(extract_column_exprs(exprs.at(i),
                                            table_id,
                                            column_exprs))) {
      LOG_WARN("Failed to extract column exprs", K(ret));
    } else { } //do nothing
  }
  return ret;
}

int ObRawExprUtils::extract_column_exprs(const ObIArray<ObRawExpr*> &exprs,
                                         const common::ObIArray<int64_t> &table_ids,
                                         ObIArray<ObRawExpr *> &column_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    if (OB_FAIL(extract_column_exprs(exprs,
                                     table_ids.at(i),
                                     column_exprs))) {
      LOG_WARN("Failed to extract column exprs", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::mark_column_explicited_reference(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 2> column_exprs;
  if (OB_FAIL(extract_column_exprs(&expr, column_exprs))) {
    LOG_WARN("extract column exprs failed", K(ret), K(expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
    if (!column_exprs.at(i)->is_column_ref_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is unexpected", K(ret));
    } else {
      static_cast<ObColumnRefRawExpr*>(column_exprs.at(i))->set_explicited_reference();
    }
  }
  return ret;
}

int ObRawExprUtils::extract_column_ids(const ObIArray<ObRawExpr*> &exprs,
                                       common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(extract_column_ids(exprs.at(i), column_ids))) {
      LOG_WARN("failed to extract column ids", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObRawExprUtils::extract_column_ids(const ObRawExpr *raw_expr, common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (NULL == raw_expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else {
    if (T_REF_COLUMN == raw_expr->get_expr_type()) {
      ret = add_var_to_array_no_dup(column_ids,
          static_cast<const ObColumnRefRawExpr*>(raw_expr)->get_column_id());
    } else {
      int64_t N = raw_expr->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        ret = SMART_CALL(extract_column_ids(raw_expr->get_param_expr(i), column_ids));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_table_ids(const ObRawExpr *raw_expr, common::ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else if (T_REF_COLUMN == raw_expr->get_expr_type()) {
    if (OB_FAIL(add_var_to_array_no_dup(table_ids,
                static_cast<const ObColumnRefRawExpr*>(raw_expr)->get_table_id()))) {
      LOG_WARN("failed to add var to array", K(ret));
    }
  } else if (raw_expr->has_flag(IS_PSEUDO_COLUMN)) {
    if (OB_FAIL(add_var_to_array_no_dup(table_ids,
                static_cast<const ObPseudoColumnRawExpr*>(raw_expr)->get_table_id()))) {
      LOG_WARN("failed to add var to array", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); ++i) {
      if (OB_FAIL(extract_table_ids(raw_expr->get_param_expr(i), table_ids))) {
        LOG_WARN("failed to extract table ids", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_table_ids_from_exprs(const common::ObIArray<ObRawExpr *> &exprs,
                                                 common::ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    ObSEArray<uint64_t, 8> expr_table_ids;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null expr", K(ret));
    } else if (OB_FAIL(extract_table_ids(expr, expr_table_ids))) {
      LOG_WARN("failed to extract table ids", K(ret));
    } else if (OB_FAIL(append_array_no_dup(table_ids, expr_table_ids))) {
      LOG_WARN("failed to append table ids", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::try_add_cast_expr_above(ObRawExprFactory *expr_factory,
                                            const ObSQLSessionInfo *session,
                                            ObRawExpr &src_expr,
                                            const ObExprResType &dst_type,
                                            ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  ObCastMode cm = CM_NONE;
  OZ(ObSQLUtils::get_default_cast_mode(false,/* explicit_cast */
                                        0,    /* result_flag */
                                        session, cm));
  OZ(try_add_cast_expr_above(expr_factory, session, src_expr, dst_type, cm, new_expr));
  return ret;
}

int ObRawExprUtils::try_add_cast_expr_above(ObRawExprFactory *expr_factory,
                                            const ObSQLSessionInfo *session,
                                            ObRawExpr &expr,
                                            const ObExprResType &dst_type,
                                            const ObCastMode &cm,
                                            ObRawExpr *&new_expr,
                                            const ObLocalSessionVar *local_vars,
                                            int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  new_expr = &expr;
  bool need_cast = false;
  bool ignore_dup_cast_error = false;
  const ObExprResType &src_type = expr.get_result_type();
  CK(OB_NOT_NULL(session) && OB_NOT_NULL(expr_factory));
  OZ(ObRawExprUtils::check_need_cast_expr(src_type, dst_type, need_cast, ignore_dup_cast_error));
  if (ret == OB_ERR_INVALID_TYPE_FOR_OP &&
      const_cast<ObSQLSessionInfo *>(session)->is_pl_prepare_stage() &&
      dst_type.is_geometry() && lib::is_oracle_mode()) {
    ret = OB_SUCCESS;
    need_cast = true;
  }
  if (OB_SUCC(ret) && need_cast) {
    if (T_FUN_SYS_CAST == expr.get_expr_type()
        && expr.has_flag(IS_OP_OPERAND_IMPLICIT_CAST)
        && !ignore_dup_cast_error
        && !((src_type.is_user_defined_sql_type()|| src_type.is_collection_sql_type())
                  && (dst_type.is_character_type() || dst_type.is_null()))) {
      // cases like: select xmltype(var)||xmltype(var) as "res1" from t1 t;
      // xmltype is a lp constructor, an implicit cast is added to cast PL xmltype to SQL xmltype
      // when deduce concat, another cast is needed to cast SQL xmltype to string
      // it is a unary cast (cast from string to xmltype is not allowed)
      ret = (lib::is_oracle_mode()) ? OB_ERR_INVALID_TYPE_FOR_OP : OB_ERR_UNEXPECTED;
#ifdef DEBUG
      LOG_ERROR("try to add implicit cast again, check if type deduction is correct",
                K(ret), K(expr), K(dst_type),
                K(session->get_current_query_string()));
#else
      LOG_WARN("try to add implicit cast again, check if type deduction is correct",
                K(ret), K(expr), K(dst_type), K(src_type), K(ignore_dup_cast_error),
                K(session->get_current_query_string()));
#endif
    } else {
      //setup implicit cast charset convert ignore error
      ObCastMode cm_zf = cm;
      if ((cm_zf & CM_COLUMN_CONVERT) != 0) {
       //if CM_CHARSET_CONVERT_IGNORE_ERR should be set is judged in column_conv expr.
      } else {
        cm_zf |= CM_CHARSET_CONVERT_IGNORE_ERR;
      }
      // setup zerofill cm
      // eg: select concat(cast(c_zf as char(10)), cast(col_no_zf as char(10))) from t1;
      if (lib::is_mysql_mode() && dst_type.is_string_type() &&
          expr.get_result_type().has_result_flag(ZEROFILL_FLAG)) {
        cm_zf |= CM_ZERO_FILL;
      }
      if (dst_type.get_cast_mode() != 0) {
        cm_zf |= dst_type.get_cast_mode();
      }
      ObExprResType final_dst_type = dst_type;

      ObSysFunRawExpr *cast_expr = NULL;
      OZ(ObRawExprUtils::create_cast_expr(*expr_factory, &expr, dst_type, cast_expr,
                                          session, false, cm_zf, local_vars, local_var_id));
      CK(OB_NOT_NULL(new_expr = dynamic_cast<ObRawExpr*>(cast_expr)));
    }
    LOG_DEBUG("in try_add_cast", K(ret), K(dst_type), K(cm));
  }
  return ret;
}


int ObRawExprUtils::implict_cast_pl_udt_to_sql_udt(ObRawExprFactory *expr_factory,
                                                   const ObSQLSessionInfo *session,
                                                   ObRawExpr* &real_ref_expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(real_ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("real_ref_expr is null", K(ret));
  } else if (real_ref_expr->get_result_type().is_ext()) {
    uint64_t udt_id = real_ref_expr->get_result_type().get_udt_id();
    if (ObObjUDTUtil::ob_is_supported_sql_udt(udt_id)) {
      ObRawExpr *new_expr = NULL;
      ObCastMode cast_mode = CM_NONE;
      ObExprResType sql_udt_type;
      if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, session, cast_mode))) {
        LOG_WARN("get default cast mode failed", K(ret));
      } else if (udt_id == T_OBJ_SDO_GEOMETRY) {
        sql_udt_type.set_geometry();
      } else {
        ObExecContext * exec_ctx = const_cast<ObSQLSessionInfo *>(session)->get_cur_exec_ctx();
        uint16_t subschema_id;
        if (OB_ISNULL(exec_ctx)) {
          ret = OB_NOT_INIT;
          LOG_WARN("exec context is null", K(ret), K(udt_id));
        } else if (OB_FAIL(exec_ctx->get_subschema_id_by_udt_id(udt_id, subschema_id))) {
          LOG_WARN("failed to get ssubschema meta", K(ret), K(udt_id));
        } else if (subschema_id == ObMaxSystemUDTSqlType) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid udt id", K(ret), K(udt_id));
        } else {
          // Add implicit cast from pl extend to sql udt
          sql_udt_type.set_sql_udt(subschema_id); // set subschema id
          sql_udt_type.set_udt_id(udt_id);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(
                                              expr_factory, session,
                                              *real_ref_expr,  sql_udt_type, cast_mode, new_expr))) {
        LOG_WARN("try add cast expr above failed", K(ret));
      } else if (OB_FAIL(new_expr->add_flag(IS_OP_OPERAND_IMPLICIT_CAST))) {
        LOG_WARN("failed to add flag", K(ret));
      } else {
        real_ref_expr = new_expr;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::implict_cast_sql_udt_to_pl_udt(ObRawExprFactory *expr_factory,
                                                   const ObSQLSessionInfo *session,
                                                   ObRawExpr* &real_ref_expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(real_ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("real_ref_expr is null", K(ret));
  } else if (real_ref_expr->get_result_type().is_user_defined_sql_type()) {
    ObRawExpr *new_expr = NULL;
    ObCastMode cast_mode = CM_NONE;
    ObExprResType pl_udt_type;
    pl_udt_type.set_ext();

    uint16_t subschema_id = real_ref_expr->get_result_type().get_subschema_id();
    const uint64_t expr_udt_id = (subschema_id == ObXMLSqlType)
                                 ? T_OBJ_XML
                                 : real_ref_expr->get_result_type().get_udt_id();

    OB_ASSERT(ObObjUDTUtil::ob_is_supported_sql_udt(expr_udt_id));
    ObSqlUDTMeta udt_meta;
    ObExecContext * exec_ctx = const_cast<ObSQLSessionInfo *>(session)->get_cur_exec_ctx();
    if (!ObObjUDTUtil::ob_is_supported_sql_udt(expr_udt_id)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported udt type for sql udt",
                K(ret), K(real_ref_expr->get_result_type()), K(expr_udt_id));
    } else if (OB_ISNULL(exec_ctx)) {
      ret = OB_NOT_INIT;
      LOG_WARN("exec context is null", K(ret), K(expr_udt_id));
    } else if (subschema_id == ObInvalidSqlType) { // called in resolver, cast sql udt to pl udt
      if (OB_FAIL(exec_ctx->get_subschema_id_by_udt_id(expr_udt_id, subschema_id))) {
        LOG_WARN("failed to get subschema id by udt id", K(ret), K(expr_udt_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(subschema_id, udt_meta))) {
      LOG_WARN("failed to get udt meta", K(ret), K(udt_meta.udt_id_));
    } else if (!ObObjUDTUtil::ob_is_supported_sql_udt(udt_meta.udt_id_)) {
      // Just bypass un-supported type, not return error here.
    } else {
      // can get extend type by schema guard, leave use of subschema when deduce.
      OB_ASSERT(expr_udt_id == udt_meta.udt_id_);
      pl_udt_type.set_udt_id(udt_meta.udt_id_);
      // set pl extend type, use udt_meta extend type, or get extend type from udtinfoschema
      if (subschema_id == ObXMLSqlType) {
        pl_udt_type.set_extend_type(pl::PL_OPAQUE_TYPE);
      } else {
        // PL_RECORD_TYPE or PL_VARRAY_TYPE is supported
        pl_udt_type.set_extend_type(udt_meta.pl_type_);
      }
      // add implicit cast from sql udt to pl extend
      if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session, cast_mode))) {
        LOG_WARN("get default cast mode failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(
                                             expr_factory, session,
                                             *real_ref_expr,  pl_udt_type, cast_mode, new_expr))) {
        LOG_WARN("try add cast expr above failed", K(ret));
      } else if (OB_FAIL(new_expr->add_flag(IS_OP_OPERAND_IMPLICIT_CAST))) {
        LOG_WARN("failed to add flag", K(ret));
      } else {
        real_ref_expr = new_expr;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::create_cast_expr(ObRawExprFactory &expr_factory,
                                     ObRawExpr *src_expr,
                                     const ObExprResType &dst_type,
                                     ObSysFunRawExpr *&func_expr,
                                     const ObSQLSessionInfo *session,
                                     bool use_def_cm,
                                     ObCastMode cm,
                                     const ObLocalSessionVar *local_vars,
                                     int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(src_expr));
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret) && use_def_cm) {
    OZ(ObSQLUtils::get_default_cast_mode(false,/* explicit_cast */
                                         0,    /* result_flag */
                                         session, cm));
  }
  if (OB_SUCC(ret)) {
    const ObExprResType &src_type = src_expr->get_result_type();
    bool need_extra_cast_for_src_type = false;
    bool need_extra_cast_for_dst_type = false;

    need_extra_cast(src_type, dst_type,
                    need_extra_cast_for_src_type,
                    need_extra_cast_for_dst_type);

    // extra cast expr: cast non-utf8 to utf8
    ObSysFunRawExpr *extra_cast = NULL;
    if (need_extra_cast_for_src_type) {
      ObExprResType src_type_utf8;
      OZ(setup_extra_cast_utf8_type(src_type, src_type_utf8));
      OZ(create_real_cast_expr(expr_factory, src_expr, src_type_utf8, extra_cast, session));
      OZ(create_real_cast_expr(expr_factory, extra_cast, dst_type, func_expr, session));
    } else if (need_extra_cast_for_dst_type) {
      ObExprResType dst_type_utf8;
      OZ(setup_extra_cast_utf8_type(dst_type, dst_type_utf8));
      OZ(create_real_cast_expr(expr_factory, src_expr, dst_type_utf8, extra_cast, session));
      OZ(create_real_cast_expr(expr_factory, extra_cast, dst_type, func_expr, session));
    } else if (src_type.get_type() == ObExtendType
               && src_type.get_udt_id() == T_OBJ_XML
               && dst_type.is_character_type()
               && src_expr->is_called_in_sql()) {
      // pl xmltype -> sql xmltype -> char type is supported only in sql scenario
      ObExprResType sql_udt_type;
      sql_udt_type.set_sql_udt(ObXMLSqlType); // set subschema id
      sql_udt_type.set_udt_id(T_OBJ_XML);
      OZ(create_real_cast_expr(expr_factory, src_expr, sql_udt_type, extra_cast, session));
      OZ(create_real_cast_expr(expr_factory, extra_cast, dst_type, func_expr, session));
    } else {
      OZ(create_real_cast_expr(expr_factory, src_expr, dst_type, func_expr, session));
    }
    if (NULL != extra_cast) {
      OX(extra_cast->set_extra(cm));
      OZ(extra_cast->add_flag(IS_INNER_ADDED_EXPR));
    }
    CK(OB_NOT_NULL(func_expr));
    OX(func_expr->set_extra(cm));
    OZ(func_expr->add_flag(IS_INNER_ADDED_EXPR));
    if (NULL != local_vars) {
      OZ(func_expr->formalize_with_local_vars(session, local_vars, local_var_id));
    } else {
      OZ(func_expr->formalize(session));
    }
  }
  return ret;
}

void ObRawExprUtils::need_extra_cast(const ObExprResType &src_type,
                                     const ObExprResType &dst_type,
                                     bool &need_extra_cast_for_src_type,
                                     bool &need_extra_cast_for_dst_type)
{
  need_extra_cast_for_src_type = false;
  need_extra_cast_for_dst_type = false;
  const ObCharsetType &src_cs = ObCharset::charset_type_by_coll(src_type.get_collation_type());
  const ObCharsetType &dst_cs = ObCharset::charset_type_by_coll(dst_type.get_collation_type());
  bool nonstr_to_str = (!src_type.is_string_or_lob_locator_type() &&
                        dst_type.is_string_or_lob_locator_type());
  bool str_to_nonstr = (src_type.is_string_or_lob_locator_type() &&
                        !dst_type.is_string_or_lob_locator_type());

  if (src_type.is_null() || dst_type.is_null()) {
    // do nothing
  } else if (str_to_nonstr) {
    if (CHARSET_BINARY != src_cs && ObCharset::get_default_charset() != src_cs && !dst_type.is_bit()) {
      need_extra_cast_for_src_type = true;
    }
  } else if (nonstr_to_str) {
    if (CHARSET_BINARY != dst_cs && ObCharset::get_default_charset() != dst_cs && !src_type.is_bit()) {
      need_extra_cast_for_dst_type = true;
    }
  }
}

int ObRawExprUtils::setup_extra_cast_utf8_type(const ObExprResType &type,
                                               ObExprResType &utf8_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!type.is_string_or_lob_locator_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in type must be string type", K(ret), K(type));
  } else {
    utf8_type = type;
    utf8_type.set_collation_type(ObCharset::get_system_collation());
    if (ObNVarchar2Type == type.get_type()) {
      utf8_type.set_type(ObVarcharType);
    } else if (ObNCharType == type.get_type()) {
      utf8_type.set_type(ObCharType);
    }
  }
  return ret;
}

int ObRawExprUtils::erase_inner_added_exprs(ObRawExpr *src_expr, ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(src_expr));
  OX(out_expr = src_expr);
  if (OB_SUCC(ret) && src_expr->has_flag(IS_INNER_ADDED_EXPR)) {
    switch (src_expr->get_expr_type()) {
      case T_OP_BOOL:
      case T_FUN_SYS_REMOVE_CONST: {
        CK(1 == src_expr->get_param_count());
        OZ(erase_inner_added_exprs(src_expr->get_param_expr(0), out_expr));
        break;
      }
      case T_FUN_SYS_CAST: {
        CK(2 == src_expr->get_param_count());
        OZ(erase_inner_added_exprs(src_expr->get_param_expr(0), out_expr));
        break;
      }
      case T_FUN_SYS_WRAPPER_INNER: {
        CK(1 == src_expr->get_param_count());
        OZ(erase_inner_added_exprs(src_expr->get_param_expr(0), out_expr));
        break;
      }
      default: {
        break;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::erase_inner_cast_exprs(ObRawExpr *src_expr, ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(src_expr));
  OX(out_expr = src_expr);
  if (OB_SUCC(ret) && src_expr->has_flag(IS_INNER_ADDED_EXPR) &&
      T_FUN_SYS_CAST == src_expr->get_expr_type()) {
      CK(2 == src_expr->get_param_count());
      OZ(erase_inner_added_exprs(src_expr->get_param_expr(0), out_expr));
  }
  return ret;
}

int ObRawExprUtils::erase_operand_implicit_cast(ObRawExpr *src, ObRawExpr *&out)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(src));
  if (OB_SUCC(ret)) {
    if (src->has_flag(IS_OP_OPERAND_IMPLICIT_CAST)) {
      OZ(erase_operand_implicit_cast(src->get_param_expr(0), out));
    } else {
      for (int64_t i = 0; i < src->get_param_count() && OB_SUCC(ret); i++) {
        OZ(erase_operand_implicit_cast(src->get_param_expr(i), src->get_param_expr(i)));
      }
      // reset DECIMAL_INT_ADJUST_FLAG anyway, otherwise the result precision maybe affected
      // after removing implicit cast.
      ObOpRawExpr *op_expr = dynamic_cast<ObOpRawExpr *>(src);
      if (op_expr != NULL && op_expr->get_op() != NULL) {
        op_expr->get_op()->get_result_type().unset_result_flag(DECIMAL_INT_ADJUST_FLAG);
      }
      if (OB_SUCC(ret)) {
        out = src;
      }
    }
  }
  return ret;
}

const ObRawExpr *ObRawExprUtils::skip_inner_added_expr(const ObRawExpr *expr)
{
  expr = skip_implicit_cast(expr);
  if (NULL != expr && T_OP_BOOL == expr->get_expr_type()) {
    expr = skip_inner_added_expr(expr->get_param_expr(0));
  }
  return expr;
}

const ObColumnRefRawExpr *ObRawExprUtils::get_column_ref_expr_recursively(const ObRawExpr *expr)
{
  const ObColumnRefRawExpr *res = NULL;
  if (OB_ISNULL(expr)) {
  } else if (expr->is_column_ref_expr()) {
    res = static_cast<const ObColumnRefRawExpr *>(expr);
  } else {
    for (int i = 0; OB_ISNULL(res) && i < expr->get_param_count(); i++) {
      res = get_column_ref_expr_recursively(expr->get_param_expr(i));
    }
  }
  return res;
}

ObRawExpr *ObRawExprUtils::get_sql_udt_type_expr_recursively(ObRawExpr *expr)
{
  ObRawExpr *res = NULL;
  if (OB_ISNULL(expr)) {
  } else if (ob_is_user_defined_sql_type(expr->get_result_type().get_type())) {
    res = expr;
  } else {
    for (int i = 0; OB_ISNULL(res) && i < expr->get_param_count(); i++) {
      res = get_sql_udt_type_expr_recursively(expr->get_param_expr(i));
    }
  }
  return res;
}

ObRawExpr *ObRawExprUtils::skip_implicit_cast(ObRawExpr *e)
{
  ObRawExpr *res = e;
  while (res != NULL
         && T_FUN_SYS_CAST == res->get_expr_type()
         && res->has_flag(IS_OP_OPERAND_IMPLICIT_CAST)) {
    res = res->get_param_expr(0);
  }
  return res;
}

ObRawExpr *ObRawExprUtils::skip_inner_added_expr(ObRawExpr *expr)
{
  expr = skip_implicit_cast(expr);
  if (NULL != expr && T_OP_BOOL == expr->get_expr_type()) {
    expr = skip_inner_added_expr(expr->get_param_expr(0));
  }
  return expr;
}

int ObRawExprUtils::create_to_type_expr(ObRawExprFactory &expr_factory,
                                        ObRawExpr *src_expr,
                                        const ObObjType &dst_type,
                                        ObSysFunRawExpr *&to_type,
                                        ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObExprOperator *op = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_TO_TYPE, to_type))) {
    LOG_WARN("create to_type expr failed", K(ret));
  } else if (OB_ISNULL(to_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("to_type is null");
  } else if (OB_FAIL(to_type->add_param_expr(src_expr))) {
    LOG_WARN("add param expr failed", K(ret));
  } else if (OB_ISNULL(op = to_type->get_op())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate expr operator failed");
  } else {
    to_type->set_func_name(N_TO_TYPE);
    ObExprToType *to_type_op = static_cast<ObExprToType*>(op);
    to_type_op->set_expect_type(dst_type);
    if (OB_FAIL(to_type->formalize(session_info))) {
      LOG_WARN("formalize to_type expr failed", K(ret));
    }
  }
  return ret;
}
int ObRawExprUtils::create_instr_expr(ObRawExprFactory &expr_factory,
                                       ObSQLSessionInfo *session_info,
                                       ObRawExpr *first_expr,
                                       ObRawExpr *second_expr,
                                       ObRawExpr *third_expr,
                                       ObRawExpr *fourth_expr,
                                       ObSysFunRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_INSTR, out_expr))) {
    LOG_WARN("create to_type expr failed", K(ret));
  } else if (OB_ISNULL(out_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("to_type is null");
  } else {
    out_expr->set_func_name(N_INSTR);
    if (NULL == third_expr) {
      if (OB_FAIL(out_expr->set_param_exprs(first_expr, second_expr))) {
        LOG_WARN("add param expr failed", K(ret));
      }
    } else if (NULL == fourth_expr) {
      if (OB_FAIL(out_expr->set_param_exprs(first_expr, second_expr, third_expr))) {
        LOG_WARN("add param expr failed", K(ret));
      }
    } else {
      if (OB_FAIL(out_expr->add_param_expr(first_expr)) ||
          OB_FAIL(out_expr->add_param_expr(second_expr)) ||
          OB_FAIL(out_expr->add_param_expr(third_expr)) ||
          OB_FAIL(out_expr->add_param_expr(fourth_expr))) {
        LOG_WARN("add param expr failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(out_expr->formalize(session_info))) {
    LOG_WARN("formalize to_type expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::replace_json_wrapper_expr_if_need(ObRawExpr* qual,
                                                     int64_t qual_idx,
                                                     ObRawExpr *depend_expr,
                                                     ObRawExprFactory &expr_factory,
                                                     ObSQLSessionInfo *session_info,
                                                     bool& is_need_replace)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *const_depend_expr = depend_expr;
  if (depend_expr->get_expr_type() == T_FUN_SYS_CAST) {
    ObRawExpr* qual_expr = qual->get_param_expr(qual_idx);
    const ObConstRawExpr* cast_val = static_cast<const ObConstRawExpr*>(depend_expr->get_param_expr(1));
    int64_t depend_cast_data_type = cast_val->get_value().get_int();

    ParseNode parse_node;
    parse_node.value_ = depend_cast_data_type;

    bool is_need_create = false;
    ObRawExpr *param_expr = nullptr;

    depend_expr = depend_expr->get_param_expr(0);

    if ((depend_expr->get_expr_type() == T_FUN_SYS_JSON_EXTRACT || depend_expr->get_expr_type() == T_FUN_SYS_JSON_UNQUOTE)
        && qual_expr->get_expr_type() == T_FUN_SYS_JSON_EXTRACT) {
      is_need_create = true;
      param_expr = qual_expr;
    }

    if (OB_SUCC(ret) && is_need_create) {
      ObSysFunRawExpr* new_expr = nullptr;
      ObConstRawExpr *qual_cast_type_expr = nullptr;
      if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_JSON_UNQUOTE, new_expr))) {
        LOG_WARN("create to_type expr failed", K(ret));
      } else if (OB_ISNULL(new_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrapper json unquote is null");
      } else if (OB_FAIL(new_expr->set_param_expr(param_expr))) {
        LOG_WARN("add param expr failed", K(ret));
      } else if (OB_FAIL(expr_factory.create_raw_expr(T_INT, qual_cast_type_expr))) {
        LOG_WARN("build const int expr failed", K(ret));
      } else {
        ObObj val;
        val.set_int(parse_node.value_);
        qual_cast_type_expr->set_length_semantics(LS_CHAR);
        qual_cast_type_expr->set_value(val);
        qual_cast_type_expr->set_param(val);

        if (OB_FAIL(static_cast<ObOpRawExpr *>((qual)->get_param_expr(1))->replace_param_expr(1, qual_cast_type_expr))) {
          LOG_WARN("replace const int expr failed", K(ret));
        } else if (OB_FAIL(qual->get_param_expr(1)->formalize(session_info))) {
          LOG_WARN("formalize expr failed", K(ret));
        } else {
          new_expr->set_func_name(N_JSON_UNQUOTE);
          if (OB_FAIL(new_expr->formalize(session_info))) {
            LOG_WARN("formalize expr failed", K(ret));
          } else if (OB_FAIL(static_cast<ObOpRawExpr *>(qual)->replace_param_expr(qual_idx, new_expr))) {
            LOG_WARN("replace failed", K(ret));
          }
        }
      }
    }
  } else if (const_depend_expr->get_expr_type() == T_FUN_SYS_JSON_QUERY ||
            const_depend_expr->extract_multivalue_json_expr(const_depend_expr)) {
    ObRawExpr* qual_expr = qual->get_param_expr(qual_idx);
    if (qual_expr->is_domain_json_expr()) {
      is_need_replace = true;
    }
  }

  return ret;
}

int ObRawExprUtils::replace_qual_param_if_need(ObRawExpr* qual,
                                               int64_t qual_idx,
                                               ObColumnRefRawExpr *col_expr)
{
  INIT_SUCC(ret);
  ObRawExpr* qual_expr = nullptr;
  const ObRawExpr* param_expr = nullptr;
  int32_t idx = 0;

  if ((qual->get_expr_type() == T_OP_BOOL
      || (OB_NOT_NULL(qual = qual->get_param_expr(qual_idx)) && qual->get_expr_type() == T_OP_BOOL))
      && OB_NOT_NULL(qual_expr = qual->get_param_expr(0))
      && qual_expr->is_domain_json_expr()) {
    if (qual_expr->get_expr_type() == T_FUN_SYS_JSON_MEMBER_OF) {
      if (OB_FAIL(static_cast<ObOpRawExpr *>(qual_expr)->replace_param_expr(1, col_expr))) {
        LOG_WARN("replace const int expr failed", K(ret));
      }
    } else {
      if (OB_NOT_NULL(param_expr = qual_expr->get_param_expr(0)) && param_expr->is_const_expr()) {
        idx = 1;
      }

      if (OB_FAIL(static_cast<ObOpRawExpr *>(qual_expr)->replace_param_expr(idx, col_expr))) {
        LOG_WARN("replace const int expr failed", K(ret));
      }
    }
  }

  return ret;
}

bool ObRawExprUtils::is_domain_expr_need_special_replace(ObRawExpr* qual_expr,
                                                         ObRawExpr *depend_expr)
{
  bool b_ret = false;

  const ObRawExpr *const_depend_expr = depend_expr;
  if (depend_expr->get_expr_type() == T_FUN_SYS_CAST) {
    depend_expr = depend_expr->get_param_expr(0);

    b_ret = (depend_expr->get_expr_type() == T_FUN_SYS_JSON_EXTRACT || depend_expr->get_expr_type() == T_FUN_SYS_JSON_UNQUOTE)
             && qual_expr->get_expr_type() == T_FUN_SYS_JSON_EXTRACT;
  } else if (depend_expr->get_expr_type() == T_FUN_SYS_JSON_QUERY) {
    b_ret = qual_expr->is_domain_json_expr();
  } else if (const_depend_expr->extract_multivalue_json_expr(const_depend_expr)
    && const_depend_expr->get_expr_type() == T_FUN_SYS_JSON_QUERY) {
    b_ret = qual_expr->is_domain_json_expr();
  }

  return b_ret;
}

int ObRawExprUtils::replace_domain_wrapper_expr(ObRawExpr *depend_expr,
                                                ObColumnRefRawExpr *col_expr,
                                                ObRawExprCopier& copier,
                                                ObRawExprFactory& factory,
                                                ObSQLSessionInfo *session_info,
                                                ObRawExpr *&qual,
                                                int64_t qual_idx,
                                                ObRawExpr *&new_qual)
{
  INIT_SUCC(ret);

  ObSEArray<ObRawExpr *, 4> column_exprs;
  bool need_specific_replace = false;

  if (OB_ISNULL(qual)) {
  } else if (qual->get_expr_type() != T_OP_BOOL &&
    qual->get_param_expr(qual_idx)->get_expr_type() != T_OP_BOOL) {
  } else if (OB_FAIL(replace_json_wrapper_expr_if_need(
      qual, qual_idx, depend_expr, factory, session_info, need_specific_replace))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(extract_column_exprs(qual, column_exprs))) {
    LOG_WARN("extract_column_exprs error", K(ret));
  } else if (OB_FAIL(copier.add_skipped_expr(column_exprs))) {
    LOG_WARN("failed to add skipped exprs", K(ret));
  } else if (OB_FAIL(copier.copy(qual, new_qual))) {
    LOG_WARN("failed to copy expr node", K(ret));
    //depend_expr's res type may be diff from its column's. copy real_qual and deduce type again.
  } else if (!need_specific_replace
             && OB_FAIL(static_cast<ObOpRawExpr *>(new_qual)->replace_param_expr(qual_idx, col_expr))) {
    LOG_WARN("replace failed", K(ret));
  } else if (need_specific_replace
             && OB_FAIL(replace_qual_param_if_need(new_qual, qual_idx, col_expr))) {
    LOG_WARN("specific replace failed", K(ret));
  }

  return ret;
}

int ObRawExprUtils::create_substr_expr(ObRawExprFactory &expr_factory,
                                       ObSQLSessionInfo *session_info,
                                       ObRawExpr *first_expr,
                                       ObRawExpr *second_expr,
                                       ObRawExpr *third_expr,
                                       ObSysFunRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_SUBSTR, out_expr))) {
    LOG_WARN("create to_type expr failed", K(ret));
  } else if (OB_ISNULL(out_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("to_type is null");
  } else {
    out_expr->set_func_name(N_SUBSTR);
    if (NULL == third_expr) {
      if (OB_FAIL(out_expr->set_param_exprs(first_expr, second_expr))) {
        LOG_WARN("add param expr failed", K(ret));
      }
    } else {
      if (OB_FAIL(out_expr->set_param_exprs(first_expr, second_expr, third_expr))) {
        LOG_WARN("add param expr failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(out_expr->formalize(session_info))) {
    LOG_WARN("formalize to_type expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::create_concat_expr(ObRawExprFactory &expr_factory,
                                       ObSQLSessionInfo *session_info,
                                       ObRawExpr *first_expr,
                                       ObRawExpr *second_expr,
                                       ObOpRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_CNN, out_expr))) {
   LOG_WARN("create expr failed", K(ret));
  } else if (OB_FAIL(out_expr->add_param_expr(first_expr)) ||
               OB_FAIL(out_expr->add_param_expr(second_expr))) {
    LOG_WARN("add param expr failed", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(out_expr->formalize(session_info))) {
    LOG_WARN("formalize to_type expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::create_prefix_pattern_expr(ObRawExprFactory &expr_factory,
                                              ObSQLSessionInfo *session_info,
                                              ObRawExpr *first_expr,
                                              ObRawExpr *second_expr,
                                              ObRawExpr *third_expr,
                                              ObSysFunRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_PREFIX_PATTERN, out_expr))) {
    LOG_WARN("create to_type expr failed", K(ret));
  } else if (OB_ISNULL(out_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("to_type is null");
  } else if (OB_FAIL(out_expr->set_param_exprs(first_expr, second_expr, third_expr))) {
    LOG_WARN("add param expr failed", K(ret));
  } else {
    out_expr->set_func_name(N_PREFIX_PATTERN);
  }
  if (OB_SUCC(ret) && OB_FAIL(out_expr->formalize(session_info))) {
    LOG_WARN("formalize to_type expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::create_type_to_str_expr(ObRawExprFactory &expr_factory,
                                            ObRawExpr *src_expr,
                                            ObSysFunRawExpr *&out_expr,
                                            ObSQLSessionInfo *session_info,
                                            bool is_type_to_str,
                                            ObObjType dst_type)
{
  int ret = OB_SUCCESS;
  ObExprOperator *op = NULL;
  ObObjType data_type = ObNullType;
  if (OB_ISNULL(src_expr) || OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src_expr or session_info is NULL", KP(src_expr), KP(session_info), K(ret));
  } else if (OB_UNLIKELY(!src_expr->is_terminal_expr()
                         && !src_expr->is_sys_func_expr()
                         && !src_expr->is_udf_expr()
                         && !src_expr->is_win_func_expr()
                         && !(src_expr->is_op_expr() && ob_is_enum_or_set_type(src_expr->get_data_type())))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src_expr should be terminal expr or func expr", KPC(src_expr), K(ret));
  } else if (FALSE_IT(data_type = src_expr->get_data_type())) {
  } else if (OB_UNLIKELY(!ob_is_enumset_tc(data_type))) {
    LOG_WARN("data_type of src_expr is invalid", K(data_type), KPC(src_expr), K(ret));
  } else {
    ObItemType item_type = (true == is_type_to_str) ?
        (ObEnumType == data_type ? T_FUN_ENUM_TO_STR : T_FUN_SET_TO_STR) :
        (ObEnumType == data_type ? T_FUN_ENUM_TO_INNER_TYPE : T_FUN_SET_TO_INNER_TYPE) ;

    const char *func_name = (true == is_type_to_str) ?
        (ObEnumType == data_type ? N_ENUM_TO_STR : N_SET_TO_STR) :
        (ObEnumType == data_type ? N_ENUM_TO_INNER_TYPE : N_SET_TO_INNER_TYPE) ;
    if (OB_FAIL(expr_factory.create_raw_expr(item_type, out_expr))) {
      LOG_WARN("create out_expr expr failed", K(ret));
    } else if (OB_ISNULL(out_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("created out_expr is null", K(ret));
    } else if (OB_ISNULL(op = out_expr->get_op())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate expr operator failed", K(ret));
    } else {
      out_expr->set_func_name(ObString::make_string(func_name));
      if (ob_is_large_text(dst_type)) {
        out_expr->set_extra(static_cast<uint64_t>(dst_type));
      } else {
        out_expr->set_extra(0);
      }
    }

    ObConstRawExpr *col_accuracy_expr = NULL;
    if (OB_SUCC(ret)) {
      ObString str_col_accuracy;
      if (OB_FAIL(build_const_string_expr(expr_factory, ObVarcharType, str_col_accuracy,
                                          src_expr->get_collation_type(), col_accuracy_expr))) {
        LOG_WARN("fail to build type expr", K(ret));
      } else if (OB_ISNULL(col_accuracy_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_accuracy_expr is NULL", K(ret));
      } else {
        col_accuracy_expr->set_collation_type(src_expr->get_collation_type());
        col_accuracy_expr->set_accuracy(src_expr->get_accuracy());
      }
    }

    if (OB_SUCC(ret)) {
      ObExprTypeToStr *type_to_str = static_cast<ObExprTypeToStr *>(op);
      const ObIArray<ObString> &enum_set_values = src_expr->get_enum_set_values();
      if (OB_ISNULL(type_to_str)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to cast ObExprOperator* to ObExprTypeToStr*", K(ret));
      } else if (OB_UNLIKELY(enum_set_values.count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("enum_set_values is empty", K(ret));
      } else if (OB_FAIL(out_expr->add_param_expr(col_accuracy_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (OB_FAIL(out_expr->add_param_expr(src_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (OB_FAIL(type_to_str->shallow_copy_str_values(enum_set_values))) {
        LOG_WARN("failed to shallow_copy_str_values", K(ret));
      } else if (OB_FAIL(out_expr->formalize(session_info))) {
        LOG_WARN("formalize to_type expr failed", K(ret));
      } else {}
    }
  }
  return ret;
}

int ObRawExprUtils::wrap_enum_set_for_stmt(ObRawExprFactory &expr_factory,
                                           ObSelectStmt *stmt,
                                           ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret));
  } else if (stmt->is_set_stmt()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_set_query().count(); i ++) {
      ret = wrap_enum_set_for_stmt(expr_factory,
                                   stmt->get_set_query().at(i),
                                   session_info);
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_item_size(); i ++) {
      ObRawExpr *&expr = stmt->get_select_item(i).expr_;
      if (OB_ISNULL(expr)){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr");
      } else if (ob_is_enumset_tc(expr->get_result_type().get_type())) {
        ObSysFunRawExpr *cast_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::create_type_to_str_expr(expr_factory,
                                                            expr,
                                                            cast_expr,
                                                            session_info,
                                                            true))) {
          LOG_WARN("create to str expr for stmt failed", K(ret));
        } else {
          expr = cast_expr;
        }
      }
    }
  }
  return ret;
}

int ObRawExprUtils::get_exec_param_expr(ObRawExprFactory &expr_factory,
                                        ObQueryRefRawExpr *query_ref,
                                        ObRawExpr *outer_val_expr,
                                        ObRawExpr *&param_expr)
{
  int ret = OB_SUCCESS;
  param_expr = NULL;
  if (OB_ISNULL(query_ref) || OB_ISNULL(outer_val_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid params", K(ret), K(query_ref), K(outer_val_expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < query_ref->get_param_count(); ++i) {
    ObExecParamRawExpr *param = NULL;
    if (OB_ISNULL(param = query_ref->get_exec_param(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret), K(i));
    } else if (param->get_ref_expr() == outer_val_expr) {
      param_expr = param;
      break;
    }
  }
  // the exec param is not found in the query ref,
  // we create a new one here
  if (OB_SUCC(ret) && NULL == param_expr) {
    ObExecParamRawExpr *exec_param = NULL;
    if (OB_FAIL(ObRawExprUtils::create_new_exec_param(expr_factory,
                                                      outer_val_expr,
                                                      exec_param,
                                                      false))) {
      LOG_WARN("failed to create new exec param", K(ret));
    } else if (OB_FAIL(query_ref->add_exec_param_expr(exec_param))) {
      LOG_WARN("failed to add exec param expr", K(ret));
    } else {
      param_expr = exec_param;
    }
  }
  return ret;
}

int ObRawExprUtils::create_new_exec_param(ObRawExprFactory &expr_factory,
                                          ObRawExpr *ref_expr,
                                          ObExecParamRawExpr *&exec_param,
                                          bool is_onetime /*=false*/)
{
  int ret = OB_SUCCESS;
  exec_param = NULL;
  if (OB_ISNULL(ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(ref_expr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_QUESTIONMARK, exec_param))) {
    LOG_WARN("failed to create exec param expr", K(ret));
  } else if (OB_ISNULL(exec_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec param is null", K(ret), K(exec_param));
  } else if (OB_FAIL(exec_param->set_enum_set_values(ref_expr->get_enum_set_values()))) {
    LOG_WARN("failed to set enum set values", K(ret));
  } else if (OB_FAIL(exec_param->add_flag(IS_CONST))) {
    LOG_WARN("failed to add flag", K(ret));
  } else if (OB_FAIL(exec_param->add_flag(IS_DYNAMIC_PARAM))) {
    LOG_WARN("failed to add flag", K(ret));
  } else {
    exec_param->set_ref_expr(ref_expr, is_onetime);
    exec_param->set_param_index(-1);
    exec_param->set_result_type(ref_expr->get_result_type());
    if (is_onetime) {
      exec_param->add_flag(IS_ONETIME);
    }
  }
  return ret;
}

int ObRawExprUtils::get_exec_param_expr(ObRawExprFactory &expr_factory,
                                        ObIArray<ObExecParamRawExpr*> *query_ref_exec_params,
                                        ObRawExpr *outer_val_expr,
                                        ObRawExpr *&param_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(outer_val_expr) || OB_ISNULL(query_ref_exec_params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid params", K(ret), K(outer_val_expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < query_ref_exec_params->count(); ++i) {
    ObExecParamRawExpr *param = NULL;
    if (OB_ISNULL(param = query_ref_exec_params->at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret), K(i));
    } else if (param->get_ref_expr() == outer_val_expr) {
      param_expr = param;
      break;
    }
  }
  // the exec param is not found in the query ref,
  // we create a new one here
  if (OB_SUCC(ret) && NULL == param_expr) {
    ObExecParamRawExpr *exec_param = NULL;
    if (OB_FAIL(expr_factory.create_raw_expr(T_QUESTIONMARK, exec_param))) {
      LOG_WARN("failed to create raw expr", K(ret));
    } else if (OB_ISNULL(exec_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param is null", K(ret), K(exec_param));
    } else if (OB_FAIL(query_ref_exec_params->push_back(exec_param))) {
      LOG_WARN("failed to add exec param expr", K(ret));
    } else if (OB_FAIL(exec_param->set_enum_set_values(outer_val_expr->get_enum_set_values()))) {
      LOG_WARN("failed to set enum set values", K(ret));
    } else if (OB_FAIL(exec_param->add_flag(IS_CONST))) {
      LOG_WARN("failed to add flag", K(ret));
    } else if (OB_FAIL(exec_param->add_flag(IS_DYNAMIC_PARAM))) {
      LOG_WARN("failed to add flag", K(ret));
    } else {
      exec_param->set_ref_expr(outer_val_expr);
      exec_param->set_param_index(-1);
      exec_param->set_result_type(outer_val_expr->get_result_type());
      param_expr = exec_param;
    }
  }
  return ret;
}

int ObRawExprUtils::create_new_exec_param(ObQueryCtx *query_ctx,
                                          ObRawExprFactory &expr_factory,
                                          ObRawExpr *&expr,
                                          bool is_onetime /*=false*/)
{
  int ret = OB_SUCCESS;
  ObExecParamRawExpr *exec_param = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_QUESTIONMARK, exec_param))) {
    LOG_WARN("failed to create exec param expr", K(ret));
  } else if (OB_ISNULL(exec_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec param is null", K(ret), K(exec_param));
  } else if (OB_FAIL(exec_param->set_enum_set_values(expr->get_enum_set_values()))) {
    LOG_WARN("failed to set enum set values", K(ret));
  } else if (OB_FAIL(exec_param->add_flag(IS_CONST))) {
    LOG_WARN("failed to add flag", K(ret));
  } else if (OB_FAIL(exec_param->add_flag(IS_DYNAMIC_PARAM))) {
    LOG_WARN("failed to add flag", K(ret));
  } else {
    exec_param->set_ref_expr(expr, is_onetime);
    exec_param->set_param_index(query_ctx->question_marks_count_);
    exec_param->set_result_type(expr->get_result_type());
    if (is_onetime) {
      exec_param->add_flag(IS_ONETIME);
    }
    ++query_ctx->question_marks_count_;

    expr = exec_param;
  }
  return ret;
}

int ObRawExprUtils::create_param_expr(ObRawExprFactory &expr_factory, int64_t param_idx, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null");
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_QUESTIONMARK, c_expr))) {
    LOG_WARN("create const raw expr failed", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const raw expr is null", K(ret));
  } else {
    ObObjParam val;
    val.set_unknown(param_idx);
    val.set_param_meta();
    c_expr->set_value(val);
    c_expr->set_result_type(expr->get_result_type());
    if (expr->is_bool_expr()) {
      c_expr->set_is_literal_bool(true);
    }
    if (ob_is_enumset_tc(expr->get_result_type().get_type())) {
      if (OB_FAIL(c_expr->set_enum_set_values(expr->get_enum_set_values()))) {
        LOG_WARN("failed to set enum_set_values", K(*expr), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(c_expr->extract_info())) {
        LOG_WARN("extract const raw expr info failed", K(ret));
      } else {
        expr = c_expr;
      }
    }
  }
  return ret;
}

bool ObRawExprUtils::need_column_conv(const ObExprResType &expected_type, const ObRawExpr &expr)
{
  int bret = true;
  if (expected_type.get_type() == expr.get_data_type()) {
    bool type_matched = false;
    if (expected_type.is_integer_type() || expected_type.is_temporal_type()) {
      type_matched = true;
    } else if (expected_type.get_collation_type() == expr.get_collation_type()
	       && expected_type.get_accuracy().get_accuracy() == expr.get_accuracy().get_accuracy()) {
      type_matched = true;
    }
    if (type_matched) {
      // null check is not performed in column_convert after supporting trigger, there is no need
      // to check nullable attribute
      bret = false;
    }
  }
  return bret;
}

bool ObRawExprUtils::need_column_conv(const ColumnItem &column, ObRawExpr &expr)
{
  int bret = true;
  if (column.get_expr() != NULL
    && (column.get_expr()->is_fulltext_column()
       || column.get_expr()->is_spatial_generated_column()
       || column.get_expr()->is_multivalue_generated_column()
       || column.get_expr()->is_multivalue_generated_array_column())) {
    //全文索引的生成列是内部生成的隐藏列，不需要做column convert
    bret = false;
  } else if (column.get_column_type() != NULL) {
    const ObExprResType &column_type = *column.get_column_type();
    if (column_type.get_type() == expr.get_data_type()
        && column_type.get_collation_type() == expr.get_collation_type()
        && column_type.get_accuracy().get_accuracy() == expr.get_accuracy().get_accuracy()) {
      //类型相同，满足不做类型转换的条件
      if (column.is_not_null_for_write() && expr.is_not_null_for_read()) {
        //从表达式可以判断两个类型都为not null
        //类型相同，并且唯一性约束满足，不需要加column convert检查
        bret = false;
      } else if (!column.is_not_null_for_write()) {
        //column没有唯一性约束限制
        bret = false;
      } else { /*do nothing*/ }
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }
  return bret;
}

bool ObRawExprUtils::check_exprs_type_collation_accuracy_equal(const ObRawExpr *expr1, const ObRawExpr *expr2)
{
  int equal = false;
  if (expr1->get_data_type() == expr2->get_data_type()
      && expr1->get_collation_type() == expr2->get_collation_type()
      && expr1->get_accuracy() == expr2->get_accuracy()) {
    equal = true;
  }
  return equal;
}

// 此方法请谨慎使用,会丢失enum类型的 enum_set_values
int ObRawExprUtils::build_column_conv_expr(ObRawExprFactory &expr_factory,
                                           const share::schema::ObColumnSchemaV2 *column_schema,
                                           ObRawExpr *&expr,
                                           const ObSQLSessionInfo *session_info,
                                           const ObLocalSessionVar *local_vars)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(session_info));
  CK(OB_NOT_NULL(column_schema));
  if (OB_SUCC(ret)) {
    if (column_schema->is_fulltext_column()
        || column_schema->is_spatial_generated_column()
        || column_schema->is_multivalue_generated_column()
        || column_schema->is_multivalue_generated_array_column()) {
      //全文列不会破坏约束性，且数据不会存储，跳过强转
      // 空间索引列是虚拟列，跳过强转
    } else if (OB_FAIL(build_column_conv_expr(session_info,
                                              expr_factory,
                                              column_schema->get_data_type(),
                                              column_schema->get_collation_type(),
                                              column_schema->get_accuracy().get_accuracy(),
                                              !column_schema->is_not_null_for_write(),
                                              NULL,
                                              NULL,
                                              expr,
                                              false,
                                              false,
                                              local_vars))) {
      LOG_WARN("failed to build column convert expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::build_column_conv_expr(ObRawExprFactory &expr_factory,
                                           common::ObIAllocator &allocator,
                                           const ObColumnRefRawExpr &col_ref,
                                           ObRawExpr *&expr,
                                           const ObSQLSessionInfo *session_info,
                                           bool is_generated_column,
                                           const ObLocalSessionVar *local_vars,
                                           int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  ObString column_conv_info;
  const ObString &database_name = col_ref.get_database_name();
  const ObString &table_name = col_ref.get_table_name();
  const ObString &column_name = col_ref.get_column_name();
  int64_t buff_len = database_name.length() + table_name.length() + column_name.length() + 20;
  char *temp_str_buf = static_cast<char *>(allocator.alloc(buff_len));
  if (OB_UNLIKELY(OB_ISNULL(temp_str_buf))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (snprintf(temp_str_buf, buff_len, "\"%.*s\".\"%.*s\".\"%.*s\"",
                      database_name.length(), database_name.ptr(),
                      table_name.length(), table_name.ptr(),
                      column_name.length(), column_name.ptr()) < 0) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("failed to generate buffer for temp_str_buf", K(ret));
  } else {
    column_conv_info = ObString(buff_len, static_cast<int32_t>(strlen(temp_str_buf)),
                               temp_str_buf);
  }
  CK(session_info);
  if (OB_SUCC(ret)) {
    if (col_ref.is_fulltext_column() ||
        col_ref.is_spatial_generated_column() ||
        col_ref.is_multivalue_generated_column() ||
        col_ref.is_multivalue_generated_array_column()) {
      // 全文列不会破坏约束性，且数据不会存储，跳过强转
      // 空间索引列是虚拟列，跳过强转
    } else if (OB_FAIL(build_column_conv_expr(session_info,
                                              expr_factory,
                                              col_ref.get_data_type(),
                                              col_ref.get_collation_type(),
                                              // accuracy used as udt id for udt columns
                                              col_ref.get_accuracy().get_accuracy(),
                                              !col_ref.is_not_null_for_write(),
                                              &column_conv_info,
                                              &col_ref.get_enum_set_values(),
                                              expr, false, is_generated_column,
                                              local_vars,
                                              local_var_id))) {
      LOG_WARN("fail to build column convert expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::build_column_conv_expr(const ObSQLSessionInfo *session_info,
                                           ObRawExprFactory &expr_factory,
                                           const ObObjType &type,
                                           const ObCollationType &collation,
                                           const int64_t &accuracy,
                                           const bool &is_nullable,
                                           const common::ObString *column_conv_info,
                                           const ObIArray<ObString> *type_infos,
                                           ObRawExpr *&expr,
                                           bool is_in_pl,
                                           bool is_generated_column,
                                           const ObLocalSessionVar *local_vars,
                                           int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  ObObjType dest_type = type;
  if (!is_in_pl && ObLobType == type) {
    dest_type = ObLongTextType;
  }
  ObSysFunRawExpr *f_expr = NULL;
  ObConstRawExpr  *is_nullable_expr = NULL;
  ObConstRawExpr  *type_expr = NULL;
  ObConstRawExpr  *collation_expr = NULL;
  ObConstRawExpr  *accuracy_expr = NULL;
  ObConstRawExpr  *column_info_expr = NULL;
  uint64_t def_cast_mode = 0;
  ObSQLMode sql_mode = 0;
  CK(OB_NOT_NULL(session_info));
  CK(OB_NOT_NULL(expr));
  ObString column_info;
  if (OB_NOT_NULL(column_conv_info)) {
    column_info = *column_conv_info;
  }
  if (OB_SUCC(ret) && is_in_pl) {
    ObObjTypeClass ori_tc = ob_obj_type_class(expr->get_data_type());
    ObObjTypeClass expect_tc = ob_obj_type_class(dest_type);
    if ((ObNumberTC == ori_tc || ObDecimalIntTC == ori_tc)
        && (ObTextTC == expect_tc || ObLobTC == expect_tc)) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("cast to lob type not allowed", K(ret), K(expect_tc), K(ori_tc));
    } else if (ObIntTC == ori_tc && ObLongTextType == dest_type) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("cast to lob type not allowed", K(ret), K(dest_type), K(ori_tc));
    } else if (ori_tc == ObDateTimeTC && ObLongTextType == dest_type) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("cast to lob type not allowed", K(ret), K(dest_type), K(ori_tc));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing ...
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_COLUMN_CONV, f_expr))) {
    LOG_WARN("fail to create T_FUN_COLUMN_CONV raw expr", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory,
                                          ObInt32Type,
                                          static_cast<int32_t>(dest_type),
                                          type_expr))) {
    LOG_WARN("fail to build const int expr", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory,
                                          ObInt32Type,
                                          static_cast<int32_t>(collation),
                                          collation_expr))) {
    LOG_WARN("fail to build type expr", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory,
                                          ObIntType,
                                          accuracy,
                                          accuracy_expr))) {
    LOG_WARN("fail to build int expr", K(ret), K(accuracy));
  } else if (OB_FAIL(build_const_int_expr(expr_factory,
                                          ObTinyIntType,
                                          is_nullable,
                                          is_nullable_expr))) {
    LOG_WARN("fail to build bool expr", K(ret));
  } else if (OB_FAIL(build_const_string_expr(expr_factory,
                                             ObCharType,
                                             column_info,
                                             CS_TYPE_UTF8MB4_GENERAL_CI,
                                             column_info_expr))) {
    LOG_WARN("fail to build column info expr", K(ret));
  } else if (OB_FAIL(f_expr->add_param_expr(type_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(f_expr->add_param_expr(collation_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(f_expr->add_param_expr(accuracy_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(f_expr->add_param_expr(is_nullable_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(f_expr->add_param_expr(expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(f_expr->add_param_expr(column_info_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (FALSE_IT(sql_mode = session_info->get_sql_mode())) {
  } else if (NULL != local_vars
            && OB_FAIL(ObSQLUtils::merge_solidified_var_into_sql_mode(local_vars, sql_mode))) {
    LOG_WARN("try get local sql mode failed", K(ret));
  }
  stmt::StmtType stmt_type_bak = stmt::T_NONE;
  if (OB_SUCC(ret)) {
    stmt_type_bak = session_info->get_stmt_type();
    bool is_ddl = const_cast<sql::ObSQLSessionInfo *>(session_info)->get_ddl_info().is_ddl();
    bool is_strict = lib::is_mysql_mode() && is_strict_mode(sql_mode);
    bool ignore_charset_error = is_generated_column && stmt_type_bak==stmt::T_SELECT;
    (const_cast<ObSQLSessionInfo *>(session_info))->set_stmt_type(stmt::T_NONE);
    ObSQLUtils::get_default_cast_mode(false,/* explicit_cast */
                                      0,    /* result_flag */
                                      session_info->get_stmt_type(),
                                      session_info->is_ignore_stmt(),
                                      sql_mode,
                                      def_cast_mode);
    if ((!is_ddl && !is_strict) || ignore_charset_error ) {
      def_cast_mode |= CM_CHARSET_CONVERT_IGNORE_ERR;
    }
  }
  if (OB_SUCC(ret)) {
    if (ob_is_enumset_tc(dest_type) && OB_NOT_NULL(type_infos)) {
      ObExprColumnConv *column_conv = NULL;
      ObExprOperator *op = NULL;
      if (OB_ISNULL(op = f_expr->get_op())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate expr operator failed", K(ret));
      } else if (OB_ISNULL(column_conv = static_cast<ObExprColumnConv *>(op))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to cast ObExprOperator* to ObExprColumnConv*", K(ret));
      } else if (OB_FAIL(column_conv->shallow_copy_str_values(*type_infos))) {
        LOG_WARN("fail to shallow_copy_str_values", K(ret));
      } else if (OB_FAIL(f_expr->set_enum_set_values(*type_infos))) {
        LOG_WARN("fail to set_enum_set_values", K(ret));
      } else {/*success*/}
    }
    if (OB_SUCC(ret)) {
      f_expr->set_extra(def_cast_mode);
      f_expr->set_func_name(ObString::make_string(N_COLUMN_CONV));
      f_expr->set_data_type(dest_type);
      f_expr->set_expr_type(T_FUN_COLUMN_CONV);
      if (ob_is_user_defined_type(dest_type) || ob_is_collection_sql_type(dest_type)) {
        f_expr->set_udt_id(accuracy);
      }
      if (expr->is_for_generated_column()) {
        f_expr->set_for_generated_column();
      }
      expr = f_expr;
      if (NULL != local_vars) {
        if (OB_FAIL(expr->formalize_with_local_vars(session_info, local_vars, local_var_id))) {
          LOG_WARN("fail to extract info", K(ret));
        }
      } else if (OB_FAIL(expr->formalize(session_info))) {
        LOG_WARN("fail to extract info", K(ret));
      }
    }
    (const_cast<ObSQLSessionInfo *>(session_info))->set_stmt_type(stmt_type_bak);
  }
  return ret;
}

//invoker should remember to add to stmt's expr_store to free the memory
//stmt->store_expr(expr)
int ObRawExprUtils::build_const_int_expr(ObRawExprFactory &expr_factory, ObObjType type, int64_t int_value, ObConstRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(type), c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else if (OB_FAIL(c_expr->extract_info())) {
    LOG_WARN("failed to extract expr info", K(ret));
  } else {
    ObObj obj;
    obj.set_int(type, int_value);
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_const_uint_expr(ObRawExprFactory &expr_factory, ObObjType type, uint64_t uint_value, ObConstRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(type), c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    ObObj obj;
    obj.set_uint(type, uint_value);
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_const_float_expr(ObRawExprFactory &expr_factory, ObObjType type, float value, ObConstRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(type), c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    ObObj obj;
    obj.set_float(type, value);
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_const_double_expr(ObRawExprFactory &expr_factory, ObObjType type, double value, ObConstRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(type), c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    ObObj obj;
    obj.set_double(type, value);
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_const_number_expr(ObRawExprFactory &expr_factory,
                                            ObObjType type,
                                            const number::ObNumber value,
                                            ObConstRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(type), c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    ObObj obj;
    obj.set_number(type, value);
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_const_datetime_expr(ObRawExprFactory &expr_factory,
                                              int64_t int_value,
                                              ObConstRawExpr *&expr){
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(ObDateTimeType), c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    ObObj obj;
    obj.set_datetime(int_value);
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_const_date_expr(ObRawExprFactory &expr_factory,
                                          int64_t int_value,
                                          ObConstRawExpr *&expr){
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(ObDateType), c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    ObObj obj;
    obj.set_date(int_value);
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_mul_expr(
    ObRawExprFactory &raw_expr_factory,
    ObRawExpr *expr1,
    ObRawExpr *expr2,
    ObOpRawExpr *&expr_out)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(expr1));
  CK (OB_NOT_NULL(expr2));
  OZ (raw_expr_factory.create_raw_expr(T_OP_MUL, expr_out));
  OZ (expr_out->set_param_exprs(expr1, expr2));

  return ret;
}

int ObRawExprUtils::build_div_expr(
    ObRawExprFactory &raw_expr_factory,
    ObRawExpr *expr1,
    ObRawExpr *expr2,
    ObOpRawExpr *&expr_out)
{
  int ret = OB_SUCCESS;

  CK (OB_NOT_NULL(expr1));
  CK (OB_NOT_NULL(expr2));
  OZ (raw_expr_factory.create_raw_expr(T_OP_DIV, expr_out));
  OZ (expr_out->set_param_exprs(expr1, expr2));

  return ret;
}

int ObRawExprUtils::build_add_all_expr(
    ObRawExprFactory &raw_expr_factory,
    ObRawExpr *expr1,
    ObRawExpr *expr2,
    ObRawExpr *expr3,
    ObRawExpr *expr4,
    ObOpRawExpr *&sum_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *tmp_add1 = NULL;
  ObOpRawExpr *tmp_add2 = NULL;

  CK (OB_NOT_NULL(expr1) && OB_NOT_NULL(expr2) && OB_NOT_NULL(expr3) && OB_NOT_NULL(expr4));

  OZ (build_add_expr(raw_expr_factory, expr1, expr2, tmp_add1));
  OZ (build_add_expr(raw_expr_factory, tmp_add1, expr3, tmp_add2));
  OZ (build_add_expr(raw_expr_factory, tmp_add2, expr4, sum_expr));

  return ret;
}

int ObRawExprUtils::build_datepart_to_second_expr(
    ObRawExprFactory &raw_expr_factory,
    ObRawExpr *interval_ds_expr,
    int datepart,
    int n,
    ObRawExpr *&expr_out)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *extract_day = NULL;
  ObConstRawExpr *timepart_expr = NULL;

  CK (OB_NOT_NULL(interval_ds_expr));
  CK (DATE_UNIT_DAY == datepart ||
      DATE_UNIT_HOUR == datepart ||
      DATE_UNIT_MINUTE == datepart ||
      DATE_UNIT_SECOND == datepart ||
      DATE_UNIT_MONTH == datepart ||
      DATE_UNIT_YEAR == datepart);
  OZ (build_const_int_expr(raw_expr_factory, ObIntType, datepart, timepart_expr));
  OZ (raw_expr_factory.create_raw_expr(T_FUN_SYS_EXTRACT, extract_day));
  OZ (extract_day->add_param_expr(timepart_expr));
  OZ (extract_day->add_param_expr(interval_ds_expr));

  if (OB_SUCC(ret)) {
    if (n > 1) {
      ObConstRawExpr *n_expr = NULL;
      ObOpRawExpr* tmp_out_expr = NULL;
      OZ (build_const_int_expr(raw_expr_factory, ObIntType, n,n_expr));

      OZ (build_mul_expr(raw_expr_factory, extract_day, n_expr, tmp_out_expr));
      OX (expr_out = tmp_out_expr);
    } else {
      OX (expr_out = extract_day);
    }
  }

return ret;
}

int ObRawExprUtils::build_second_expr_from_interval_ds(
    ObRawExprFactory &raw_expr_factory,
    ObRawExpr *interval_ds_expr,
    ObOpRawExpr *&second_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr1 = NULL;
  ObRawExpr* expr2 = NULL;
  ObRawExpr* expr3 = NULL;
  ObRawExpr* expr4 = NULL;

  CK (OB_NOT_NULL(interval_ds_expr));
  OZ (build_datepart_to_second_expr(raw_expr_factory, interval_ds_expr, DATE_UNIT_DAY, 24 * 3600, expr1));
  OZ (build_datepart_to_second_expr(raw_expr_factory, interval_ds_expr, DATE_UNIT_HOUR, 3600, expr2));
  OZ (build_datepart_to_second_expr(raw_expr_factory, interval_ds_expr, DATE_UNIT_MINUTE, 60, expr3));
  OZ (build_datepart_to_second_expr(raw_expr_factory, interval_ds_expr, DATE_UNIT_SECOND, 1, expr4));
  OZ (build_add_all_expr(raw_expr_factory, expr1, expr2, expr3, expr4, second_expr));

  return ret;
}

int ObRawExprUtils::build_month_expr_from_interval_ym(
    ObRawExprFactory &raw_expr_factory,
    ObRawExpr *interval_ym_expr,
    ObOpRawExpr *&month_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr1 = NULL;
  ObRawExpr* expr2 = NULL;

  CK (OB_NOT_NULL(interval_ym_expr));
  OZ (build_datepart_to_second_expr(raw_expr_factory, interval_ym_expr, DATE_UNIT_YEAR, 12, expr1));
  OZ (build_datepart_to_second_expr(raw_expr_factory, interval_ym_expr, DATE_UNIT_MONTH, 1, expr2));
  OZ (build_add_expr(raw_expr_factory, expr1, expr2, month_expr));
  return ret;
}


int ObRawExprUtils::build_interval_ym_diff_exprs(
    ObRawExprFactory &raw_expr_factory,
    ObObj &const_val,
    const ObObj &transition_val,
    const ObObj &interval_val,
    ObRawExpr *&diff_1_out,
    ObRawExpr *&diff_2_out,
    ObConstRawExpr *&transition_expr,
    ObConstRawExpr *&interval_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *const_expr = NULL;
  ObOpRawExpr *diff_1 = NULL;
  ObOpRawExpr *diff_2 = NULL;
  ObOpRawExpr *year_part_expr = NULL;
  ObConstRawExpr *month_expr = NULL;
  ObConstRawExpr *year_expr = NULL;


  /* 1.1 prepare 2 params of months_between */
  OZ (build_const_obj_expr(raw_expr_factory, const_val, const_expr));
  OZ (build_const_obj_expr(raw_expr_factory, transition_val, transition_expr));

  /* 1.2. build months_between(const_val,transition_val) */
  OZ (raw_expr_factory.create_raw_expr(T_FUN_SYS_MONTHS_BETWEEN, diff_1));
  OZ (diff_1->add_param_expr(const_expr));
  OZ (diff_1->add_param_expr(transition_expr));

  /* 2. build 12 * extract(year, interval_val) + extract(month, interval_val) */
  OZ (build_const_obj_expr(raw_expr_factory, interval_val, interval_expr));
  OZ (build_month_expr_from_interval_ym(raw_expr_factory, interval_expr, diff_2));

  OX (diff_1_out = diff_1);
  OX (diff_2_out = diff_2);

  return ret;
}

int ObRawExprUtils::build_interval_ds_diff_exprs(
    ObRawExprFactory &raw_expr_factory,
    ObObj &const_val,
    const ObObj &transition_val,
    const ObObj &interval_val,
    ObRawExpr *&diff_1_out,
    ObRawExpr *&diff_2_out,
    ObConstRawExpr *&transition_expr,
    ObConstRawExpr *&interval_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *const_expr = NULL;
  ObOpRawExpr *sub_expr = NULL;
  ObOpRawExpr *diff_1 = NULL;
  ObOpRawExpr *diff_2 = NULL;

  /* 1. build diff_1 */
  /* 1.1 build const - transition*/
  OZ (build_const_obj_expr(raw_expr_factory, const_val, const_expr));
  OZ (build_const_obj_expr(raw_expr_factory, transition_val, transition_expr));
  OZ (build_minus_expr(raw_expr_factory, const_expr, transition_expr, sub_expr));

  /* 1.2
    build diff_1 from sub_expr
   */
  if (transition_val.is_timestamp_nano() || const_val.is_timestamp_nano()) {
    /* case 1: timestamp - timestamp = interval day to second */
    OZ (build_second_expr_from_interval_ds(raw_expr_factory, sub_expr, diff_1));
  } else {
    /* case 2: datetime - datetime = number(day) */
    CK (transition_val.is_datetime() && const_val.is_datetime());
    ObConstRawExpr *n_expr = NULL;
    OZ (build_const_int_expr(raw_expr_factory, ObIntType, 24 * 3600, n_expr));

    OZ (build_mul_expr(raw_expr_factory, sub_expr, n_expr, diff_1));
  }
  /* 2. build diff_2, extract(all seconds from interval ds) */
  /* 2.1 build interval_expr */
  OZ (build_const_obj_expr(raw_expr_factory, interval_val, interval_expr));
  /* 2.2 build all seconds */
  OZ (build_second_expr_from_interval_ds(raw_expr_factory, interval_expr, diff_2));

  OX (diff_1_out = diff_1);
  OX (diff_2_out = diff_2);

  return ret;
}

int ObRawExprUtils::build_common_diff_exprs(
  ObRawExprFactory &raw_expr_factory,
  ObObj &const_val,
  const ObObj &transition_val,
  const ObObj &interval_val,
  ObRawExpr *&diff_1_out,
  ObRawExpr *&diff_2_out,
  ObConstRawExpr *&transition_expr,
  ObConstRawExpr *&interval_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *const_expr = NULL;
  ObOpRawExpr *sub_expr = NULL;
  ObOpRawExpr *diff_1 = NULL;
  ObOpRawExpr *diff_2 = NULL;
  ObConstRawExpr *day_expr = NULL;

  /* 1. build diff_1 */
  /* 1.1 build const_expr and transition_expr */
  CK (!const_val.is_datetime() && !const_val.is_timestamp_nano());
  OZ (build_const_obj_expr(raw_expr_factory, const_val, const_expr));
  OZ (build_const_obj_expr(raw_expr_factory, transition_val, transition_expr));

  /* 1.2. build  const_expr - transition_expr */
  OZ (build_minus_expr(raw_expr_factory, const_expr, transition_expr, sub_expr));
  OX (diff_1 = sub_expr);

  /* 2. build diff_2 */
  OZ (build_const_obj_expr(raw_expr_factory, interval_val, interval_expr));

  OX (diff_1_out = diff_1);
  OX (diff_2_out = interval_expr);

  return ret;
}

int ObRawExprUtils::build_high_bound_raw_expr(
    ObRawExprFactory &raw_expr_factory,
    ObSQLSessionInfo* session,
    ObObj &const_val,
    const ObObj &transition_val,
    const ObObj &interval_val,
    ObRawExpr *&result_expr_out,
    ObRawExpr *&n_part_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *transition_expr = NULL;
  ObOpRawExpr *div_expr = NULL;
  ObOpRawExpr *mul_expr = NULL;
  ObOpRawExpr *add_expr = NULL;
  ObOpRawExpr *trunc_expr = NULL;
  ObOpRawExpr *result_expr = NULL;
  ObConstRawExpr *interval_expr = NULL;
  ObRawExpr *diff_1 = NULL;
  ObRawExpr *diff_2 = NULL;
  ObConstRawExpr *int_expr = NULL;
  ObOpRawExpr *interval_round_expr = NULL;

  CK (OB_NOT_NULL(session));
  if (interval_val.is_interval_ym()) {
    OZ (build_interval_ym_diff_exprs(raw_expr_factory,
                                      const_val,
                                      transition_val,
                                      interval_val,
                                      diff_1,
                                      diff_2,
                                      transition_expr,
                                      interval_expr));
  } else if (interval_val.is_interval_ds()) {
    OZ (build_interval_ds_diff_exprs(raw_expr_factory,
                                     const_val,
                                     transition_val,
                                     interval_val,
                                     diff_1,
                                     diff_2,
                                     transition_expr,
                                     interval_expr));
  } else {
    OZ (build_common_diff_exprs(raw_expr_factory,
                                const_val,
                                transition_val,
                                interval_val,
                                diff_1,
                                diff_2,
                                transition_expr,
                                interval_expr));
  }

  /* build v1 / v2 */
  OZ (build_div_expr(raw_expr_factory, diff_1, diff_2, div_expr));

  /* build trunc(v) */
  OZ (raw_expr_factory.create_raw_expr(T_FUN_SYS_ORA_TRUNC, trunc_expr));
  OZ (trunc_expr->add_param_expr(div_expr));

  /* build v1 + 1*/
  OZ (build_const_int_expr(raw_expr_factory,
                                           ObIntType, 1, int_expr));
  OZ (build_add_expr(raw_expr_factory, trunc_expr, int_expr, add_expr));

  /* build round(v) */
  if (interval_expr->get_result_type().is_interval_ym()
     || interval_expr->get_result_type().is_interval_ds()) {
    /* build interval * n */
    OZ (build_mul_expr(raw_expr_factory, interval_expr, add_expr, mul_expr));
  } else {
    OZ (raw_expr_factory.create_raw_expr(T_FUN_SYS_ROUND, interval_round_expr));
    OZ (interval_round_expr->add_param_expr(interval_expr));
    /* build interval * n */
    OZ (build_mul_expr(raw_expr_factory, interval_round_expr, add_expr, mul_expr));
  }

  /* build transiton + interval * n  */
  OZ (build_add_expr(raw_expr_factory, transition_expr, mul_expr, result_expr));

  OZ (result_expr->formalize(session));
  OX (result_expr_out = result_expr);
  OX (n_part_expr = add_expr);

  return ret;
}

int ObRawExprUtils::build_const_obj_expr(ObRawExprFactory &expr_factory,
                                         const ObObj &obj,
                                         ObConstRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObObjType objtype = obj.get_type();
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(objtype), c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    c_expr->set_value(obj);
    expr = c_expr;
    if (ob_is_oracle_temporal_type(objtype) ||
        ob_is_interval_tc(objtype)) {
      expr->set_scale(obj.get_scale());
    }
  }
  return ret;
}

int ObRawExprUtils::build_sign_expr(ObRawExprFactory &expr_factory,
                                    ObRawExpr *param, ObRawExpr *&sign_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *sexpr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_SIGN, sexpr))) {
    LOG_WARN("failed to create sign expr", K(ret));
  } else {
    OZ (sexpr->add_param_expr(param));
    OX (sign_expr = sexpr);
  }
  return ret;
}

int ObRawExprUtils::build_less_than_expr(ObRawExprFactory &expr_factory,
                                         ObRawExpr *left,
                                         ObRawExpr *right,
                                         ObOpRawExpr *&less_than_expr)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(left));
  CK (OB_NOT_NULL(right));
  OZ (expr_factory.create_raw_expr(T_OP_LE, less_than_expr));
  OZ (less_than_expr->set_param_exprs(left, right));
  return ret;
}

int ObRawExprUtils::build_var_int_expr(ObRawExprFactory &expr_factory,
                                       ObConstRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_VAR_INT, c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    ObObj obj;
    obj.set_int(0);
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_const_string_expr(ObRawExprFactory &expr_factory, ObObjType type,
                                            const ObString &string_value, ObCollationType cs_type,
                                            ObConstRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(type), c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    ObObj obj;
    obj.set_string(type, string_value);
    obj.set_collation_type(cs_type);
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_variable_expr(ObRawExprFactory &expr_factory,
                                        const ObExprResType &result_type,
                                        ObVarRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObVarRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_EXEC_VAR, c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    c_expr->set_result_type(result_type);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_op_pseudo_column_expr(ObRawExprFactory &expr_factory,
                                                const ObItemType expr_type,
                                                const char *expr_name,
                                                const ObExprResType &res_type,
                                                ObOpPseudoColumnRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  CK(NULL != expr_name);

  OZ(expr_factory.create_raw_expr(expr_type, expr));
  CK(NULL != expr);

  if (OB_SUCC(ret)) {
    expr->set_name(expr_name);
    expr->set_result_type(res_type);
  }

  return ret;
}

int ObRawExprUtils::build_null_expr(ObRawExprFactory &expr_factory, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_NULL, c_expr))) {
    LOG_WARN("create const expr failed", K(ret));
  } else {
    ObObj obj;
    obj.set_null();
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_trim_expr(const ObColumnSchemaV2 *column_schema,
                                    ObRawExprFactory &expr_factory,
                                    const ObSQLSessionInfo *session_info,
                                    ObRawExpr *&expr,
                                    const ObLocalSessionVar *local_vars,
                                    int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *trim_expr = NULL;
  int64_t trim_type = 2;
  ObConstRawExpr *type_expr = NULL;
  ObConstRawExpr *pattem_expr = NULL;
  ObString padding_char(1, &OB_PADDING_CHAR);
  ObCollationType padding_char_cs_type = CS_TYPE_INVALID;
  if (NULL == column_schema || NULL == expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_schema), K(expr));
  } else {
    bool is_cs_nonascii = ObCharset::is_cs_nonascii(column_schema->get_collation_type());
    //如果是ascii兼容的，直接用 ' ' with column collation，这样可以避免转换
    //非ascii，用 ' ' with utf8mb4，trim类型推导之后，输入参数' '会被转换
    padding_char_cs_type = is_cs_nonascii ? ObCharset::get_default_collation(CHARSET_UTF8MB4)
                                          : column_schema->get_collation_type();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_INNER_TRIM, trim_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(trim_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to store expr", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory, ObIntType, trim_type, type_expr))) {
    LOG_WARN("fail to build type expr", K(ret));
  } else if (OB_FAIL(trim_expr->add_param_expr(type_expr))) {
    LOG_WARN("fail to add param expr", K(ret), K(*type_expr));
  } else if (OB_FAIL(build_const_string_expr(expr_factory, ObCharType, padding_char,
                                             padding_char_cs_type,
                                             pattem_expr))) {
    LOG_WARN("fail to build pattem expr", K(ret));
  } else if (FALSE_IT(static_cast<ObConstRawExpr*>(pattem_expr)->get_value().set_collation_level(
        CS_LEVEL_IMPLICIT))) {
    LOG_WARN("fail to set collation type", K(ret));
  } else if (OB_FAIL(trim_expr->add_param_expr(pattem_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(trim_expr->add_param_expr(expr))) {
    LOG_WARN("fail to add param expr", K(ret), K(*expr));
  } else {
    trim_expr->set_data_type(ObCharType);
    trim_expr->set_func_name(ObString::make_string(N_INNER_TRIM));
    if (expr->is_for_generated_column()) {
      trim_expr->set_for_generated_column();
    }
    expr = trim_expr;
    if (NULL != local_vars) {
      if (OB_FAIL(expr->formalize_with_local_vars(session_info, local_vars, local_var_id))) {
        LOG_WARN("fail to formalize expr", K(ret));
      }
    } else if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("fail to extract info", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::build_pad_expr(ObRawExprFactory &expr_factory,
                                   bool is_char,
                                   const ObColumnSchemaV2 *column_schema,
                                   ObRawExpr *&expr,
                                   const sql::ObSQLSessionInfo *session_info,
                                   const ObLocalSessionVar *local_vars,
                                   int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *pad_expr = NULL;
  ObConstRawExpr *pading_word_expr = NULL;
  ObConstRawExpr *length_expr = NULL;
  ObString padding_char(1, &OB_PADDING_CHAR);
  ObString padding_binary(1, &OB_PADDING_BINARY);
  ObObjType padding_expr_type = ObMaxType;
  ObCollationType padding_expr_collation = CS_TYPE_INVALID;
  if (NULL == column_schema) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inalid argument", K(column_schema));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_PAD, pad_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(pad_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pad expr is null", K(ret));
  } else if (FALSE_IT(padding_expr_type = ob_is_nstring_type(column_schema->get_data_type()) ?
                      ObNVarchar2Type : ObVarcharType)) {
  } else if (FALSE_IT(padding_expr_collation = ob_is_nstring_type(column_schema->get_data_type()) ?
                      CS_TYPE_UTF8MB4_BIN : column_schema->get_collation_type())) {
  } else if (is_char && OB_FAIL(build_const_string_expr(expr_factory,
                                                        padding_expr_type,
                                                        ObCharsetUtils::get_const_str(padding_expr_collation, OB_PADDING_CHAR),
                                                        padding_expr_collation,
                                                        pading_word_expr))) {
    LOG_WARN("fail to build pading word expr", K(ret));
  } else if (!is_char && OB_FAIL(build_const_string_expr(expr_factory,
                                                         padding_expr_type,
                                                         padding_binary,
                                                         padding_expr_collation,
                                                         pading_word_expr))) {
    LOG_WARN("fail to build pading word expr", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory, ObIntType, column_schema->get_data_length(),
                                          length_expr))) {
    LOG_WARN("fail to build length expr", K(ret));
  } else if (OB_FAIL(pad_expr->add_param_expr(expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(pad_expr->add_param_expr(pading_word_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(pad_expr->add_param_expr(length_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else {
    ObAccuracy padding_accuracy = pading_word_expr->get_accuracy();
    padding_accuracy.set_length_semantics(
          column_schema->get_accuracy().get_length_semantics());
    pading_word_expr->set_accuracy(padding_accuracy);

    pad_expr->set_data_type(padding_expr_type);
    pad_expr->set_func_name(ObString::make_string(N_PAD));
    if (expr->is_for_generated_column()) {
      pad_expr->set_for_generated_column();
    }
    pad_expr->set_extra(1); //mark for column convert
    expr = pad_expr;
    if (NULL != local_vars) {
      if (OB_FAIL(expr->formalize_with_local_vars(session_info, local_vars, local_var_id))) {
        LOG_WARN("fail to formalize expr", K(ret));
      }
    } else if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("fail to extract info", K(ret));
    }
    LOG_DEBUG("build pad expr", KPC(pading_word_expr), KPC(pad_expr));
  }
  return ret;
}

//外部需要调用replace_param_expr()来特殊处理now();
//这个表达式没有formalize
int ObRawExprUtils::build_nvl_expr(ObRawExprFactory &expr_factory, const ColumnItem *column_item, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *nvl_func_expr = NULL;
  ObSysFunRawExpr *now_func_expr = NULL;
  if (NULL == column_item || NULL == expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail bo build length expr", K(column_item), K(expr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_TIMESTAMP_NVL, nvl_func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_CUR_TIMESTAMP, now_func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(nvl_func_expr) || OB_ISNULL(now_func_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func expr is null", K(nvl_func_expr), K(now_func_expr), K(ret));
  } else {
    nvl_func_expr->set_expr_type(T_FUN_SYS_TIMESTAMP_NVL);
    nvl_func_expr->set_func_name(ObString::make_string(N_TIMESTAMP_NVL));
    nvl_func_expr->set_data_type(ObTimestampType);
    nvl_func_expr->set_accuracy(column_item->get_column_type()->get_accuracy());
    now_func_expr->set_expr_type(T_FUN_SYS_CUR_TIMESTAMP);
    now_func_expr->set_data_type(ObDateTimeType);
    now_func_expr->set_accuracy(column_item->get_column_type()->get_accuracy());// the accuracy of column
    now_func_expr->set_func_name(ObString::make_string(N_CUR_TIMESTAMP));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(nvl_func_expr->add_param_expr(expr))) {
      LOG_WARN("fail to add param expr", K(ret));
    } else if (OB_FAIL(nvl_func_expr->add_param_expr(now_func_expr))) {
      LOG_WARN("fail to add param expr", K(ret));
    } else {
      expr = nvl_func_expr;
    }
  }
  return ret;
}

int ObRawExprUtils::build_nvl_expr(ObRawExprFactory &expr_factory, const ColumnItem *column_item, ObRawExpr *&expr1, ObRawExpr *&expr2)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *nvl_func_expr = NULL;
  if (OB_ISNULL(column_item) || OB_ISNULL(expr1) || OB_ISNULL(expr2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to build length expr", K(column_item), K(expr1), K(expr2), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_NVL, nvl_func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(nvl_func_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func expr is null", K(nvl_func_expr), K(ret));
  } else {
    nvl_func_expr->set_expr_type(T_FUN_SYS_NVL);
    nvl_func_expr->set_func_name(ObString::make_string(N_NVL));
    nvl_func_expr->set_data_type(expr2->get_data_type());
    nvl_func_expr->set_accuracy(column_item->get_column_type()->get_accuracy());
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(nvl_func_expr->add_param_expr(expr1))) {
      LOG_WARN("fail to add param expr", K(ret));
    } else if (OB_FAIL(nvl_func_expr->add_param_expr(expr2))) {
      LOG_WARN("fail to add param expr", K(ret));
    } else {
      expr1 = nvl_func_expr;
    }
  }
  return ret;
}

int ObRawExprUtils::build_nvl_expr(ObRawExprFactory &expr_factory,
                                   ObRawExpr *param_expr1,
                                   ObRawExpr *param_expr2,
                                   ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *nvl_func_expr = NULL;
  if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to build length expr", K(param_expr1), K(param_expr2), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_NVL, nvl_func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(nvl_func_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func expr is null", K(nvl_func_expr), K(ret));
  } else {
    nvl_func_expr->set_expr_type(T_FUN_SYS_NVL);
    nvl_func_expr->set_func_name(ObString::make_string(N_NVL));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(nvl_func_expr->add_param_expr(param_expr1))) {
      LOG_WARN("fail to add param expr", K(ret));
    } else if (OB_FAIL(nvl_func_expr->add_param_expr(param_expr2))) {
      LOG_WARN("fail to add param expr", K(ret));
    } else {
      expr = nvl_func_expr;
    }
  }
  return ret;
}

int ObRawExprUtils::build_lnnvl_expr(ObRawExprFactory &expr_factory,
                                     ObRawExpr *param_expr,
                                     ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *lnnvl_expr = NULL;
  expr = NULL;
  if (OB_ISNULL(param_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(param_expr), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_LNNVL, lnnvl_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(lnnvl_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret));
  } else {
    lnnvl_expr->set_func_name(ObString::make_string(N_LNNVL));
    lnnvl_expr->set_data_type(ObTinyIntType);
    if (OB_FAIL(lnnvl_expr->add_param_expr(param_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    } else {
      expr = lnnvl_expr;
    }
  }
  return ret;
}

int ObRawExprUtils::build_equal_last_insert_id_expr(ObRawExprFactory &expr_factory,
                                                    ObRawExpr *&expr,
                                                    ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *equal_expr = NULL;
  ObSysFunRawExpr *last_insert_id = NULL;
  if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->is_op_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_EQ, equal_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_LAST_INSERT_ID, last_insert_id))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(equal_expr) || OB_ISNULL(last_insert_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(equal_expr), K(last_insert_id));
  } else {
    last_insert_id->set_func_name(ObString::make_string(N_LAST_INSERT_ID));
    last_insert_id->set_data_type(ObIntType);
    if (OB_FAIL(equal_expr->add_param_expr(expr->get_param_expr(0)))) {
      LOG_WARN("fail to add param expr", K(ret));
    } else if (OB_FAIL(equal_expr->add_param_expr(last_insert_id))) {
      LOG_WARN("fail to add param expr", K(ret));
    } else if (OB_FAIL(equal_expr->formalize(session))) {
      LOG_WARN("fail to formalize expr", K(*equal_expr), K(ret));
    } else {
      expr = equal_expr;
    }
  }
  return ret;
}

int ObRawExprUtils::build_get_user_var(ObRawExprFactory &expr_factory,
                                       const ObString &var_name,
                                       ObRawExpr *&expr,
                                       const ObSQLSessionInfo *session_info /* = NULL */,
                                       ObQueryCtx *query_ctx /* = NULL */,
                                       ObIArray<ObUserVarIdentRawExpr*> *user_var_exprs /* = NULL*/)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *f_expr = NULL;
  ObUserVarIdentRawExpr *var_expr = NULL;
  ObIArray<ObUserVarIdentRawExpr *> *all_vars =
      (NULL == query_ctx ? NULL : &query_ctx->all_user_variable_);
  bool query_has_udf = (NULL == query_ctx ? false : query_ctx->has_udf_);
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_GET_USER_VAR, f_expr))) {
    LOG_WARN("create ObOpRawExpr failed", K(ret));
  } else {
    ObString str = var_name;
    ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, str);
    ObObj val;
    val.set_varchar(str);
    val.set_collation_type(ObCharset::get_system_collation());
    val.set_collation_level(CS_LEVEL_IMPLICIT);
    if (NULL != all_vars) {
      for (int64_t i = 0; OB_SUCC(ret) && i < all_vars->count(); ++i) {
        if (OB_ISNULL(all_vars->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null user var ident expr", K(ret));
        } else if (all_vars->at(i)->is_same_variable(val)) {
          var_expr = all_vars->at(i);
        }
      }
    }
    if (OB_SUCC(ret) && NULL == var_expr) {
      if (OB_FAIL(expr_factory.create_raw_expr(T_USER_VARIABLE_IDENTIFIER, var_expr))) {
        LOG_WARN("fail to create user var ident expr", K(ret));
      } else if (NULL != all_vars && OB_FAIL(all_vars->push_back(var_expr))) {
        LOG_WARN("failed to push back var expr", K(ret));
      } else {
        var_expr->set_value(val);
      }
    }

    if (OB_SUCC(ret)) {
      var_expr->set_query_has_udf(query_has_udf);
      if (OB_FAIL(f_expr->set_param_expr(var_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (NULL != user_var_exprs &&
                 OB_FAIL(add_var_to_array_no_dup(*user_var_exprs, var_expr))) {
        LOG_WARN("failed to add var to array no dup", K(ret));
      } else {
        expr = f_expr;
      }
    }
  }

  if (OB_SUCC(ret) && NULL != session_info) {
    if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("failed to formalize", K(ret));
    }
  }

  return ret;
}

int ObRawExprUtils::build_get_sys_var(ObRawExprFactory &expr_factory,
                                      const ObString &var_name,
                                      ObSetVar::SetScopeType var_scope,
                                      ObRawExpr *&expr,
                                      const ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *name_expr = NULL;
  ObConstRawExpr *scope_expr = NULL;
  ObSysFunRawExpr *get_sys_var_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_VARCHAR, name_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_INT, scope_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_GET_SYS_VAR, get_sys_var_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(name_expr) || OB_ISNULL(scope_expr) || OB_ISNULL(get_sys_var_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(name_expr), K(scope_expr), K(get_sys_var_expr));
  } else {
    ObObj name_obj;
    name_obj.set_varchar(var_name);
    name_obj.set_collation_level(CS_LEVEL_SYSCONST);
    name_obj.set_collation_type(ObCharset::get_system_collation());
    name_expr->set_value(name_obj);

    ObObj scope_obj;
    scope_obj.set_int(static_cast<int64_t>(var_scope));
    scope_expr->set_expr_type(static_cast<ObItemType>(scope_obj.get_type()));
    scope_expr->set_value(scope_obj);

    expr = get_sys_var_expr;
    get_sys_var_expr->set_expr_type(T_OP_GET_SYS_VAR);
    get_sys_var_expr->set_func_name(ObString::make_string(N_GET_SYS_VAR));
    ret = get_sys_var_expr->set_param_exprs(name_expr, scope_expr);
  }

  if (OB_SUCC(ret) && NULL != session_info) {
    if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("failed to formalize", K(ret));
    }
  }

  return ret;
}

int ObRawExprUtils::build_calc_part_id_expr(ObRawExprFactory &expr_factory,
                                            const ObSQLSessionInfo &session,
                                            uint64_t ref_table_id,
                                            ObPartitionLevel part_level,
                                            ObRawExpr *part_expr,
                                            ObRawExpr *subpart_expr,
                                            ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  expr = NULL;
  ObSysFunRawExpr *calc_expr = NULL;
  ObRawExpr *copy_part_expr = part_expr;
  ObRawExpr *copy_subpart_expr = subpart_expr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS, calc_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(calc_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to create raw expr", K(calc_expr), K(ret));
  } else {
    if (schema::PARTITION_LEVEL_TWO == part_level) {
      if (OB_ISNULL(copy_part_expr) || OB_ISNULL(copy_subpart_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("subpart_expr is null", K(copy_subpart_expr), K(ret));
      } else if (OB_FAIL(calc_expr->add_param_expr(copy_part_expr))
                 || OB_FAIL(calc_expr->add_param_expr(copy_subpart_expr))) {
        LOG_WARN("fail to add param expr", K(ret), K(*copy_part_expr), K(*copy_subpart_expr));
      }
    } else if (schema::PARTITION_LEVEL_ONE == part_level) {
      if (OB_ISNULL(copy_part_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("part_expr is null", K(part_level), K(part_expr), K(ret));
      } else if (OB_FAIL(calc_expr->add_param_expr(copy_part_expr))) {
        LOG_WARN("fail to add param expr", K(ret), K(*part_expr));
      }
    }
    //for none partition table
    //return default calc part id expr
    if (OB_SUCC(ret)) {
      ObString func_name;
      if (OB_FAIL(ob_write_string(expr_factory.get_allocator(),
                                  ObString("calc_partition_id"),
                                  func_name))) {
        LOG_WARN("Malloc function name failed", K(ret));
      } else {
        calc_expr->set_func_name(func_name);
        calc_expr->set_extra(ref_table_id);
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(calc_expr->formalize(&session))) {
          LOG_WARN("fail to formalize expr", K(ret), K(calc_expr));
        } else {
          expr = calc_expr;
        }
      }
    }
  }

  return ret;
}

int ObRawExprUtils::build_calc_tablet_id_expr(ObRawExprFactory &expr_factory,
                                              const ObSQLSessionInfo &session,
                                              uint64_t ref_table_id,
                                              ObPartitionLevel part_level,
                                              ObRawExpr *part_expr,
                                              ObRawExpr *subpart_expr,
                                              ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  expr = NULL;
  ObSysFunRawExpr *calc_expr = NULL;
  ObRawExpr *copy_part_expr = part_expr;
  ObRawExpr *copy_subpart_expr = subpart_expr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS, calc_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(calc_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to create raw expr", K(calc_expr), K(ret));
  } else {
    if (schema::PARTITION_LEVEL_TWO == part_level) {
      if (OB_ISNULL(copy_part_expr) || OB_ISNULL(copy_subpart_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("subpart_expr is null", K(copy_subpart_expr), K(ret));
      } else if (OB_FAIL(calc_expr->add_param_expr(copy_part_expr))
                 || OB_FAIL(calc_expr->add_param_expr(copy_subpart_expr))) {
        LOG_WARN("fail to add param expr", K(ret), K(*copy_part_expr), K(*copy_subpart_expr));
      }
    } else if (schema::PARTITION_LEVEL_ONE == part_level) {
      if (OB_ISNULL(copy_part_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("part_expr is null", K(part_level), K(part_expr), K(ret));
      } else if (OB_FAIL(calc_expr->add_param_expr(copy_part_expr))) {
        LOG_WARN("fail to add param expr", K(ret), K(*part_expr));
      }
    }
    if (OB_SUCC(ret)) {
      ObString func_name;
      if (OB_FAIL(ob_write_string(expr_factory.get_allocator(),
                                  ObString("calc_tablet_id"),
                                  func_name))) {
        LOG_WARN("Malloc function name failed", K(ret));
      } else {
        calc_expr->set_func_name(func_name);
        calc_expr->set_extra(ref_table_id);
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(calc_expr->formalize(&session))) {
          LOG_WARN("fail to formalize expr", K(ret), K(calc_expr));
        } else {
          expr = calc_expr;
        }
      }
    }
  }

  return ret;
}

int ObRawExprUtils::build_calc_partition_tablet_id_expr(ObRawExprFactory &expr_factory,
                                                        const ObSQLSessionInfo &session,
                                                        uint64_t ref_table_id,
                                                        ObPartitionLevel part_level,
                                                        ObRawExpr *part_expr,
                                                        ObRawExpr *subpart_expr,
                                                        ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  expr = NULL;
  ObSysFunRawExpr *calc_expr = NULL;
  ObRawExpr *copy_part_expr = part_expr;
  ObRawExpr *copy_subpart_expr = subpart_expr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS, calc_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(calc_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to create raw expr", K(calc_expr), K(ret));
  } else {
    if (schema::PARTITION_LEVEL_TWO == part_level) {
      if (OB_ISNULL(copy_part_expr) || OB_ISNULL(copy_subpart_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("subpart_expr is null", K(copy_subpart_expr), K(ret));
      } else if (OB_FAIL(calc_expr->add_param_expr(copy_part_expr))
                 || OB_FAIL(calc_expr->add_param_expr(copy_subpart_expr))) {
        LOG_WARN("fail to add param expr", K(ret), K(*copy_part_expr), K(*copy_subpart_expr));
      }
    } else if (schema::PARTITION_LEVEL_ONE == part_level) {
      if (OB_ISNULL(copy_part_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("part_expr is null", K(part_level), K(part_expr), K(ret));
      } else if (OB_FAIL(calc_expr->add_param_expr(copy_part_expr))) {
        LOG_WARN("fail to add param expr", K(ret), K(*part_expr));
      }
    }
    if (OB_SUCC(ret)) {
      ObString func_name;
      if (OB_FAIL(ob_write_string(expr_factory.get_allocator(),
                                  ObString("calc_partition_tablet_id"),
                                  func_name))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Malloc function name failed", K(ret));
      } else {
        calc_expr->set_func_name(func_name);
        calc_expr->set_extra(ref_table_id);
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(calc_expr->formalize(&session))) {
          LOG_WARN("fail to formalize expr", K(ret), K(calc_expr));
        } else {
          expr = calc_expr;
        }
      }
    }
  }
  return ret;
}

int ObRawExprUtils::get_package_var_ids(ObRawExpr *expr, uint64_t &package_id, int64_t &var_idx)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *pkg_id_expr = NULL;
  ObConstRawExpr *var_id_expr = NULL;
  CK (OB_NOT_NULL(expr));
  CK (T_OP_GET_PACKAGE_VAR == expr->get_expr_type());
  CK (OB_NOT_NULL(pkg_id_expr = static_cast<ObConstRawExpr *>(expr->get_param_expr(0))));
  CK (OB_NOT_NULL(var_id_expr = static_cast<ObConstRawExpr *>(expr->get_param_expr(1))));
  OX (package_id = pkg_id_expr->get_value().get_uint64());
  OX (var_idx = var_id_expr->get_value().get_int());
  return ret;
}

int ObRawExprUtils::set_package_var_ids(ObRawExpr *expr, uint64_t package_id, int64_t var_idx)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *pkg_id_expr = NULL;
  ObConstRawExpr *var_id_expr = NULL;
  CK (OB_NOT_NULL(expr));
  CK (T_OP_GET_PACKAGE_VAR == expr->get_expr_type());
  CK (OB_NOT_NULL(pkg_id_expr = static_cast<ObConstRawExpr *>(expr->get_param_expr(0))));
  CK (OB_NOT_NULL(var_id_expr = static_cast<ObConstRawExpr *>(expr->get_param_expr(1))));
  OX (pkg_id_expr->get_value().set_uint64(package_id));
  OX (var_id_expr->get_value().set_int(var_idx));
  return ret;
}

int ObRawExprUtils::build_get_package_var(ObRawExprFactory &expr_factory,
                                          ObSchemaGetterGuard &schema_guard,
                                          uint64_t package_id,
                                          int64_t var_idx,
                                          ObExprResType *result_type,
                                          ObRawExpr *&expr,
                                          const ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *f_expr = NULL;
  ObConstRawExpr *c_expr1 = NULL;
  ObConstRawExpr *c_expr2 = NULL;
  ObConstRawExpr *c_expr3 = NULL;
  ObConstRawExpr *c_expr4 = NULL;
  ObConstRawExpr *c_expr5 = NULL;
  const ObPackageInfo *spec_info = NULL;
  const ObPackageInfo *body_info = NULL;
  if (package_id != OB_INVALID_ID) {
    OZ (pl::ObPLPackageManager::get_package_schema_info(schema_guard, package_id, spec_info, body_info));
  }
  OZ (expr_factory.create_raw_expr(T_OP_GET_PACKAGE_VAR, f_expr));
  OZ (build_const_int_expr(expr_factory, ObUInt64Type, package_id, c_expr1));
  OZ (f_expr->add_param_expr(c_expr1));
  OZ (build_const_int_expr(expr_factory, ObIntType, var_idx, c_expr2));
  OZ (f_expr->add_param_expr(c_expr2));
  OZ (build_const_int_expr(
    expr_factory, ObIntType, reinterpret_cast<int64>(result_type), c_expr3));
  OZ (f_expr->add_param_expr(c_expr3));
  OZ (build_const_int_expr(expr_factory, ObIntType,
    spec_info != NULL ? spec_info->get_schema_version() : OB_INVALID_VERSION, c_expr4));
  OZ (f_expr->add_param_expr(c_expr4));
  OZ (build_const_int_expr(expr_factory, ObIntType,
    body_info != NULL ? body_info->get_schema_version() : OB_INVALID_VERSION, c_expr5));
  OZ (f_expr->add_param_expr(c_expr5));
  OX (f_expr->set_func_name(ObString::make_string(N_GET_PACKAGE_VAR)));
  OX (expr = f_expr);
  CK (OB_NOT_NULL(session_info));
  OZ (expr->formalize(session_info));
  return ret;
}

int ObRawExprUtils::build_get_subprogram_var(ObRawExprFactory &expr_factory,
                                             uint64_t package_id,
                                             uint64_t routine_id,
                                             int64_t var_idx,
                                             const ObExprResType *result_type,
                                             ObRawExpr *&expr,
                                             const ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *f_expr = NULL;
  ObConstRawExpr *c_expr1 = NULL;
  ObConstRawExpr *c_expr2 = NULL;
  ObConstRawExpr *c_expr3 = NULL;
  ObConstRawExpr *c_expr4 = NULL;
  OZ (expr_factory.create_raw_expr(T_OP_GET_SUBPROGRAM_VAR, f_expr));
  OZ (build_const_int_expr(expr_factory, ObUInt64Type, package_id, c_expr1));
  OZ (f_expr->add_param_expr(c_expr1));
  OZ (build_const_int_expr(expr_factory, ObUInt64Type, routine_id, c_expr2));
  OZ (f_expr->add_param_expr(c_expr2));
  OZ (build_const_int_expr(expr_factory, ObIntType, var_idx, c_expr3));
  OZ (f_expr->add_param_expr(c_expr3));
  OZ (build_const_int_expr(
    expr_factory, ObIntType, reinterpret_cast<int64>(result_type), c_expr4));
  OZ (f_expr->add_param_expr(c_expr4));
  OX (f_expr->set_func_name(ObString::make_string(N_GET_SUBPROGRAM_VAR)));
  OX (expr = f_expr);
  CK (OB_NOT_NULL(session_info));
  OZ (expr->formalize(session_info));
  return ret;
}

int ObRawExprUtils::build_exists_expr(ObRawExprFactory &expr_factory,
                                      const ObSQLSessionInfo *session_info,
                                      ObItemType type,
                                      ObRawExpr *param_expr,
                                      ObRawExpr *&exists_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *out_expr = NULL;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(type, out_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_FAIL(out_expr->add_param_expr(param_expr))) {
    LOG_WARN("failed to add param expr", K(ret));
  } else if (OB_FAIL(out_expr->formalize(session_info))) {
    LOG_WARN("failed to formalize expr", K(ret));
  } else {
    exists_expr = out_expr;
  }
  return ret;
}

int ObRawExprUtils::create_equal_expr(ObRawExprFactory &expr_factory,
                                      const ObSQLSessionInfo *session_info,
                                      const ObRawExpr *val_ref,
                                      const ObRawExpr *col_ref,
                                      ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *equal_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_EQ, equal_expr))) {
    LOG_WARN("create equal expr failed", K(ret));
  } else if (OB_ISNULL(expr = equal_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("equal expr is null");
  } else if (OB_FAIL(equal_expr->set_param_exprs(const_cast<ObRawExpr*>(col_ref), const_cast<ObRawExpr*>(val_ref)))) {
    LOG_WARN("set param expr failed", K(ret));
  } else if (OB_FAIL(equal_expr->formalize(session_info))) {
    LOG_WARN("formalize equal expr failed", K(ret));
  } else {}
  return ret;
}

int ObRawExprUtils::create_double_op_expr(ObRawExprFactory &expr_factory,
                                          const ObSQLSessionInfo *session_info,
                                          ObItemType expr_type,
                                          ObRawExpr *&add_expr,
                                          const ObRawExpr *left_expr,
                                          const ObRawExpr *right_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *op_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(expr_type, op_expr))) {
    LOG_WARN("create add op expr failed", K(ret));
  } else if (OB_ISNULL(add_expr = op_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add expr is null");
  } else if (OB_FAIL(op_expr->set_param_exprs(const_cast<ObRawExpr*>(left_expr), const_cast<ObRawExpr*>(right_expr)))) {
    LOG_WARN("set param exprs failed", K(ret));
  } else if (OB_FAIL(op_expr->formalize(session_info))) {
    LOG_WARN("formalize add operator failed", K(ret));
  }
  return ret;
}

// set op expr 不保存 child 的任何 expr
int ObRawExprUtils::make_set_op_expr(ObRawExprFactory &expr_factory,
                                     int64_t idx,
                                     ObItemType set_op_type,
                                     const ObExprResType &res_type,
                                     ObSQLSessionInfo *session_info,
                                     ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  ObSetOpRawExpr *set_expr = NULL;
  if (OB_UNLIKELY(set_op_type <= T_OP_SET) || OB_UNLIKELY(set_op_type > T_OP_EXCEPT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknow set operator type", K(set_op_type));
  } else if (OB_FAIL(expr_factory.create_raw_expr(set_op_type, set_expr))) {
    LOG_WARN("create set op raw expr failed", K(ret));
  } else if (OB_ISNULL(out_expr = set_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("out expr is null");
  } else if (OB_FAIL(set_expr->add_flag(IS_SET_OP))) {
    LOG_WARN("failed to add flag IS_SET_OP", K(ret));
  } else {
    set_expr->set_result_type(res_type);
    set_expr->set_idx(idx);
    if (session_info != NULL && OB_FAIL(set_expr->formalize(session_info))) {
      LOG_WARN("formalize set expr failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::merge_variables(const ObIArray<ObVarInfo> &src_vars, ObIArray<ObVarInfo> &dst_vars)
{
  int ret = OB_SUCCESS;
  int64_t N = src_vars.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObVarInfo &var = src_vars.at(i);
    bool found = false;
    int64_t M = dst_vars.count();
    for (int64_t j = 0; j < M; ++j) {
      if (var == dst_vars.at(j)) {
        found = true;
        break;
      }
    } // end for
    if (!found) {
      ret = dst_vars.push_back(var);
    }
  } // end for
  return ret;
}

int ObRawExprUtils::get_array_param_index(const ObRawExpr *expr, int64_t &param_index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr));
  } else if (expr->is_const_raw_expr()) {
    if (expr->get_result_type().get_param().is_ext()) {
      const ObConstRawExpr *c_expr = static_cast<const ObConstRawExpr *>(expr);
      if (OB_FAIL(c_expr->get_value().get_unknown(param_index))) {
        LOG_WARN("get param index failed", K(ret), K(c_expr->get_value()));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_INDEX == param_index && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(get_array_param_index(expr->get_param_expr(i), param_index))) {
        LOG_WARN("get array param index failed", K(ret), KPC(expr));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::get_item_count(const ObRawExpr *expr, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr), K(lbt()));
  } else if (expr->is_column_ref_expr()) {
    const ObColumnRefRawExpr *col_ref = static_cast<const ObColumnRefRawExpr*>(expr);
    if (col_ref->is_generated_column()) {
      if (col_ref->is_spatial_generated_column()) {
        ++count;
      } else {
        ret = get_item_count(col_ref->get_dependant_expr(), count);
      }
    } else {
      ++count;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalie expr", K(i), K(ret));
      } else if (OB_FAIL(get_item_count(expr->get_param_expr(i), count))) {
        LOG_WARN("fail to get item count", K(ret), KPC(expr), KPC(expr->get_param_expr(i)));
      }
    }
    if (OB_SUCC(ret)) {
      count++;
    }
  }
  return ret;
}

int ObRawExprUtils::build_column_expr(ObRawExprFactory &expr_factory,
                                      const ObColumnSchemaV2 &column_schema,
                                      ObColumnRefRawExpr *&column_expr)
{
  int ret = OB_SUCCESS;
  ObString database_name;
  ObString table_name;
  ObString column_name;

  if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, column_expr))) {
    LOG_WARN("create column expr failed", K(ret));
  } else if (OB_ISNULL(column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_expr is null");
  } else if (OB_FAIL(init_column_expr(column_schema, *column_expr))) {
    LOG_WARN("init column expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::build_alias_column_expr(ObRawExprFactory &expr_factory,
                                            ObRawExpr *ref_expr,
                                            int32_t alias_level,
                                            ObAliasRefRawExpr *&alias_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_factory.create_raw_expr(T_REF_ALIAS_COLUMN, alias_expr))) {
    LOG_WARN("create alias column expr failed", K(ret));
  } else if (OB_ISNULL(alias_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alias column expr is null", K(ret));
  } else {
    alias_expr->set_ref_expr(ref_expr);
  }
  return ret;
}

int ObRawExprUtils::build_query_output_ref(ObRawExprFactory &expr_factory,
                                           ObQueryRefRawExpr *query_ref,
                                           int64_t project_index,
                                           ObAliasRefRawExpr *&alias_expr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *sel_stmt = NULL;
  ObRawExpr *real_ref_expr = NULL;
  if (OB_ISNULL(query_ref) || OB_ISNULL(sel_stmt = query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ref expr is invalid", K(ret), K(query_ref), K(sel_stmt));
  } else if (project_index < 0 ||
             project_index >= query_ref->get_ref_stmt()->get_select_item_size() ||
             OB_ISNULL(real_ref_expr = sel_stmt->get_select_item(project_index).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("project index is invalid", K(ret), K(project_index), K(real_ref_expr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_REF_ALIAS_COLUMN, alias_expr))) {
    LOG_WARN("create alias column expr failed", K(ret));
  } else if (OB_ISNULL(alias_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alias column expr is null", K(ret));
  } else {
    alias_expr->set_query_output(query_ref, project_index);
    alias_expr->set_result_type(real_ref_expr->get_result_type());
  }
  return ret;
}

bool ObRawExprUtils::is_same_column_ref(const ObRawExpr *column_ref1, const ObRawExpr *column_ref2)
{
  //如果是alias ref，需要比较alias ref里的真实节点
  const ObRawExpr *left_real_ref = column_ref1;
  const ObRawExpr *right_real_ref = column_ref2;
  while (left_real_ref != NULL && T_REF_ALIAS_COLUMN == left_real_ref->get_expr_type()) {
    left_real_ref = static_cast<const ObAliasRefRawExpr*>(left_real_ref)->get_ref_expr();
  }
  while (right_real_ref != NULL && T_REF_ALIAS_COLUMN == right_real_ref->get_expr_type()) {
    right_real_ref = static_cast<const ObAliasRefRawExpr*>(right_real_ref)->get_ref_expr();
  }
  bool bret = (left_real_ref == right_real_ref);
  return bret;
}


ObCollationLevel ObRawExprUtils::get_column_collation_level(const common::ObObjType &type)
{
  if (ob_is_string_type(type)
      || ob_is_enumset_tc(type)
      || ob_is_json_tc(type)
      || ob_is_geometry_tc(type)) {
    return CS_LEVEL_IMPLICIT;
  } else {
    return CS_LEVEL_NUMERIC;
  }
}

int ObRawExprUtils::init_column_expr(const ObColumnSchemaV2 &column_schema, ObColumnRefRawExpr &column_expr)
{
  int ret = OB_SUCCESS;
  const ObAccuracy &accuracy = column_schema.get_accuracy();
  column_expr.set_ref_id(column_schema.get_table_id(), column_schema.get_column_id());
  column_expr.set_data_type(column_schema.get_data_type());
  column_expr.set_result_flag(calc_column_result_flag(column_schema));
  column_expr.get_column_name().assign_ptr(column_schema.get_column_name_str().ptr(),
                                           column_schema.get_column_name_str().length());
  column_expr.set_column_flags(column_schema.get_column_flags());
  column_expr.set_hidden_column(column_schema.is_hidden());
  column_expr.set_lob_column(is_lob_storage(column_schema.get_data_type()));
  column_expr.set_is_rowkey_column(column_schema.is_rowkey_column());
  column_expr.set_srs_id(column_schema.get_srs_id());
  column_expr.set_udt_set_id(column_schema.get_udt_set_id());
  if (ob_is_string_type(column_schema.get_data_type())
      || ob_is_enumset_tc(column_schema.get_data_type())
      || ob_is_json_tc(column_schema.get_data_type())
      || ob_is_geometry_tc(column_schema.get_data_type())) {
    column_expr.set_collation_type(column_schema.get_collation_type());
  } else {
    column_expr.set_collation_type(CS_TYPE_BINARY);
  }
  // extract set collation level for reuse
  column_expr.set_collation_level(get_column_collation_level(column_schema.get_data_type()));

  if (OB_SUCC(ret)) {
    column_expr.set_accuracy(accuracy);
    if (column_schema.is_decimal_int()) {
      ObObjMeta data_meta = column_schema.get_meta_type();
      data_meta.set_scale(column_schema.get_accuracy().get_scale());
      column_expr.set_meta_type(data_meta);
    }
    if (OB_FAIL(column_expr.extract_info())) {
      LOG_WARN("extract column expr info failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && column_schema.is_enum_or_set()) {
    if (OB_FAIL(column_expr.set_enum_set_values(column_schema.get_extended_type_info()))) {
      LOG_WARN("failed to set enum set values", K(ret));
    }
  }
  if (OB_SUCC(ret) && column_schema.is_xmltype()) {
    column_expr.set_data_type(ObUserDefinedSQLType);
    column_expr.set_udt_id(column_schema.get_sub_data_type());
    column_expr.set_subschema_id(ObXMLSqlType);
  }

  return ret;
}

int ObRawExprUtils::expr_is_order_consistent(const ObRawExpr *from, const ObRawExpr *to, bool &is_consistent)
{
  int ret = OB_SUCCESS;
  is_consistent = false;
  if (OB_ISNULL(from) || OB_ISNULL(to)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null", K(from), K(to));
  } else if (OB_FAIL(ObObjCaster::is_order_consistent(from->get_result_type(),
                                                      to->get_result_type(),
                                                      is_consistent))) {
    LOG_WARN("check is order consistent failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::is_expr_comparable(const ObRawExpr *expr, bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), K(expr));
  } else if (ob_is_text_tc(expr->get_data_type())
            || ob_is_lob_tc(expr->get_data_type())
            || ob_is_json_tc(expr->get_data_type())
            || ob_is_geometry_tc(expr->get_data_type())
            || ob_is_extend(expr->get_data_type())) {
    can_be = false;
  }
  return ret;
}

int ObRawExprUtils::exprs_contain_subquery(const ObIArray<ObRawExpr*> &exprs, bool &cnt_subquery)
{
  int ret = OB_SUCCESS;
  cnt_subquery = false;
  for (int64_t i = 0; OB_SUCC(ret) && !cnt_subquery && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(i));
    } else if (exprs.at(i)->has_flag(CNT_SUB_QUERY)) {
      cnt_subquery = true;
    }
  }
  return ret;
}

uint32_t ObRawExprUtils::calc_column_result_flag(const ObColumnSchemaV2 &column_schema)
{
  uint32_t flag = 0;
  if (column_schema.is_autoincrement()) {
    flag |= AUTO_INCREMENT_FLAG;
  }

  if (column_schema.is_not_null_for_read()) {
    flag |= NOT_NULL_FLAG;
  }
  if (column_schema.is_not_null_for_write()) {
    flag |= NOT_NULL_WRITE_FLAG;
  }
  // only for create table as select c1, new column is not null only if not null constraint exists on c1
  if (column_schema.is_not_null_validate_column()) {
    flag |= HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG;
  }
  if (column_schema.is_rowkey_column()) {
    flag |= PRI_KEY_FLAG;
    flag |= PART_KEY_FLAG;
  }
  if (column_schema.is_index_column()) {
    flag |= PART_KEY_FLAG;
    if (column_schema.get_index_position() == 1) {
      flag |= MULTIPLE_KEY_FLAG;  /* 和UNIQUE_FLAG相对的概念 */
    }
  }
  if (column_schema.is_zero_fill()) {
    flag |= ZEROFILL_FLAG;
  }
  LOG_DEBUG("calc result flag", K(column_schema), K(flag), K(flag & HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG));
  return flag;
}

int ObRawExprUtils::extract_int_value(const ObRawExpr *expr, int64_t &val)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(expr) || !expr->is_const_raw_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr should not be null", K(ret), K(expr));
  } else {
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(expr);
    if (OB_ISNULL(const_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("const expr should not be null", K(ret));
    } else {
      const ObObj &result = const_expr->get_value();
      if (OB_FAIL(result.get_int(count))) {
        LOG_WARN("failed to get int", K(ret));
      } else {
        val = count;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::need_wrap_to_string(ObObjType param_type, ObObjType calc_type, const bool is_same_type_need, bool &need_wrap)
{
  //TODO(yaoying.yyy):这个函数需要在case中覆盖 且ObExtendType 和ObUnknownType
  int ret = OB_SUCCESS;
  need_wrap = false;
  if (!ob_is_enumset_tc(param_type)) {
    //输入参数不是enum 类型 则不需要转换
  } else if (param_type == calc_type && (!is_same_type_need)) {
    need_wrap = false;
  } else {
    switch (calc_type) {
      case ObNullType :
      case ObTinyIntType :
      case ObSmallIntType :
      case ObMediumIntType :
      case ObInt32Type :
      case ObIntType :
      case ObUTinyIntType :
      case ObUSmallIntType :
      case ObUMediumIntType :
      case ObUInt32Type :
      case ObUInt64Type :
      case ObFloatType :
      case ObDoubleType :
      case ObUFloatType :
      case ObUDoubleType :
      case ObNumberType :
      case ObUNumberType :
      case ObNumberFloatType :
      case ObBitType :
      case ObYearType :
      case ObDecimalIntType:
        {
          need_wrap = false;
          break;
        }
      case ObGeometryType:
      case ObJsonType :
      case ObDateTimeType :
      case ObTimestampType :
      case ObDateType :
      case ObTimeType :
      case ObVarcharType :
      case ObCharType :
      case ObTinyTextType :
      case ObTextType :
      case ObMediumTextType :
      case ObLongTextType :
      case ObEnumType :
      case ObSetType :
      case ObTimestampTZType:
      case ObTimestampLTZType:
      case ObTimestampNanoType:
      case ObRawType:
      case ObIntervalDSType:
      case ObIntervalYMType:
      case ObNVarchar2Type:
      case ObNCharType: {
        need_wrap = true;
        break;
      }
      default : {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid calc_type", K(param_type), K(calc_type), K(ret));
      }
    }
  }
  LOG_DEBUG("finish need_wrap_to_string", K(need_wrap), K(param_type),
            K(calc_type), K(ret), K(lbt()));
  return ret;
}

int ObRawExprUtils::extract_param_idxs(const ObRawExpr *expr, ObIArray<int64_t> &param_idxs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (expr->get_expr_type() == T_QUESTIONMARK) {
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(expr);
    const ObObj &val = const_expr->get_value();
    int64_t param_idx = OB_INVALID_INDEX;
    if (OB_FAIL(val.get_unknown(param_idx))) {
      LOG_WARN("get unknown of value failed", K(ret));
    } else if (OB_FAIL(param_idxs.push_back(param_idx))) {
      LOG_WARN("store param idx failed", K(ret));
    }
  } else if (expr->has_flag(CNT_STATIC_PARAM) || expr->has_flag(CNT_DYNAMIC_PARAM)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(extract_param_idxs(expr->get_param_expr(i), param_idxs))) {
        LOG_WARN("extract param idxs failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::find_alias_expr(ObRawExpr *expr, ObAliasRefRawExpr *&alias_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_alias_ref_expr()) {
    alias_expr = static_cast<ObAliasRefRawExpr*>(expr);
  } else if (expr->has_flag(CNT_ALIAS)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(find_alias_expr(expr->get_param_expr(i), alias_expr)))) {
        LOG_WARN("failed to find alias expr", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::find_flag(const ObRawExpr *expr, ObExprInfoFlag flag, bool &is_found)
{
  int ret = OB_SUCCESS;
  is_found = false;
  if (OB_FAIL(SMART_CALL(find_flag_rec(expr, flag, is_found)))) {
    LOG_WARN("failed to find flag", K(ret));
  }
  return ret;
}

int ObRawExprUtils::find_flag_rec(const ObRawExpr *expr, ObExprInfoFlag flag, bool &is_found)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->has_flag(flag)) {
    is_found = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < expr->get_param_count(); ++i) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(find_flag_rec(expr->get_param_expr(i), flag, is_found)))) {
        LOG_WARN("failed to find flag", K(ret));
      }
    }
  }
  return ret;
}

bool ObRawExprUtils::contain_id(const ObIArray<uint64_t> &ids, const uint64_t target)
{
  bool found = false;
  for (int64_t i = 0; !found && i < ids.count(); ++i) {
    if (ids.at(i) == target) {
      found = true;
    }
  }
  return found;
}

int ObRawExprUtils::clear_exprs_flag(const ObIArray<ObRawExpr*> &exprs, ObExprInfoFlag flag)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(exprs, i) {
    ObRawExpr *raw_expr =  exprs.at(i);
    if (OB_ISNULL(raw_expr)) {
      LOG_WARN("get output expr fail", K(i), K(exprs));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(raw_expr->clear_flag(flag))) {
      LOG_WARN("fail to clear flag", K(ret));
    } else {}
  }
  return ret;
}

bool ObRawExprUtils::has_prefix_str_expr(const ObRawExpr &expr,
                                         const ObColumnRefRawExpr &orig_column_expr,
                                         const ObRawExpr *&substr_expr)
{
  bool bret = false;
  const ObRawExpr *tmp = &expr;
  if (T_FUN_COLUMN_CONV == expr.get_expr_type()) {
    tmp = expr.get_param_expr(4);
  }
  if (T_FUN_SYS_SUBSTR == tmp->get_expr_type()) {
    const ObRawExpr *param_expr1 = tmp->get_param_expr(0);
    if (param_expr1 != NULL
        && param_expr1->is_column_ref_expr()
        && param_expr1->get_result_type().is_string_or_lob_locator_type()
        && param_expr1->same_as(orig_column_expr)) {
      if (3 == tmp->get_param_count()) {
        int64_t one = 1;
        int cmp_ret = 0;
        const ObRawExpr *param_expr2 = tmp->get_param_expr(1);
        if (param_expr2 != NULL && param_expr2->is_const_raw_expr()) {
          const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(param_expr2);
          if ((const_expr->get_value().is_int() && const_expr->get_value().get_int() == 1)
              || (const_expr->get_value().is_oracle_decimal()
                  && OB_SUCCESS == ora_cmp_integer(*const_expr, one, cmp_ret) && cmp_ret == 0))
            {
              bret = true;
              substr_expr = tmp;
            }
        }
      }
    }
  }
  return bret;
}

int ObRawExprUtils::build_like_expr(ObRawExprFactory &expr_factory,
                                    ObSQLSessionInfo *session_info,
                                    ObRawExpr *text_expr,
                                    ObRawExpr *pattern_expr,
                                    ObRawExpr *escape_expr,
                                    ObOpRawExpr *&like_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_LIKE, like_expr))) {
    LOG_WARN("create like expr failed", K(ret));
  } else if (OB_FAIL(like_expr->add_param_expr(text_expr))) {
    LOG_WARN("add text expr to like expr failed", K(ret));
  } else if (OB_FAIL(like_expr->add_param_expr(pattern_expr))) {
    LOG_WARN("add pattern expr to like expr failed", K(ret));
  } else if (OB_FAIL(like_expr->add_param_expr(escape_expr))) {
    LOG_WARN("add escape expr to like expr failed", K(ret));
  } else if (OB_FAIL(like_expr->formalize(session_info))) {
    LOG_WARN("formalize like expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::replace_level_column(ObRawExpr *&raw_expr, ObRawExpr *to, bool &replaced)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr) || OB_ISNULL(to)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(raw_expr), K(to));
  } else if (raw_expr->get_expr_type() == T_LEVEL) {
    raw_expr = to;
    replaced = true;
    LOG_DEBUG("replace leaf node", K(*to), K(to));
  } else {
    int64_t N = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ObRawExpr *&child_expr = raw_expr->get_param_expr(i);
      if (OB_FAIL(replace_level_column(child_expr, to, replaced))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    } // end for
  }
  return ret;
}
int ObRawExprUtils::build_const_bool_expr(ObRawExprFactory *expr_factory, ObRawExpr *&expr, bool b_value)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *bool_expr = NULL;
  if (OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_factory is null", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_BOOL, bool_expr))) {
    LOG_WARN("build const bool expr failed", K(ret));
  } else if (OB_ISNULL(bool_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bool expr is null", K(ret));
  } else if (OB_FAIL(bool_expr->extract_info())) {
    LOG_WARN("failed to extract expr info", K(ret));
  } else {
    ObObj val;
    val.set_bool(b_value);
    bool_expr->set_value(val);
    expr = bool_expr;
  }
  return ret;
}

int ObRawExprUtils::build_ora_decode_expr(ObRawExprFactory *expr_factory,
                                          const ObSQLSessionInfo &session_info,
                                          ObRawExpr *&expr,
                                          ObIArray<ObRawExpr *> &params_exprs)
{
  int ret = OB_SUCCESS;
  expr = NULL;
  ObSysFunRawExpr *ora_decode = NULL;
  if (OB_ISNULL(expr_factory) || params_exprs.count() < 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), KP(expr_factory), K(params_exprs.count()));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_ORA_DECODE, ora_decode))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(ora_decode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new expr is NULL", K(ret), KP(ora_decode));
  } else if (OB_FAIL(append(ora_decode->get_param_exprs(), params_exprs))) {
    LOG_WARN("failed to append into ora_decode param exprs", K(ret));
  } else {
    ora_decode->set_func_name(ObString::make_string(N_ORA_DECODE));
    if (OB_FAIL(ora_decode->formalize(&session_info))) {
      LOG_WARN("failed to formalize ora_decode", K(ret));
    } else {
      expr = ora_decode;
    }
  }
  return ret;
}

int ObRawExprUtils::check_composite_cast(ObRawExpr *&expr, ObSchemaChecker &schema_checker)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret));
  } else if (T_FUN_SYS_CAST == expr->get_expr_type()) {
    ObConstRawExpr *const_expr = static_cast<ObConstRawExpr *>(expr->get_param_expr(1));
    ObObj param;
    ParseNode parse_node;
    ObObjType obj_type;
    ObRawExpr *src = expr->get_param_expr(0);
    uint64_t udt_id = expr->get_param_expr(1)->get_udt_id();
    CK (OB_NOT_NULL(src), OB_NOT_NULL(const_expr));
    CK (expr->get_param_expr(1)->is_const_raw_expr());
    OX (param = const_expr->get_value());
    OX (parse_node.value_ = param.get_int());
    OX (obj_type = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]));
    if (OB_FAIL(ret)) {
    } else if (T_REF_QUERY == src->get_expr_type() &&
               static_cast<ObQueryRefRawExpr *>(src)->is_multiset()) {
      // cast(multiset(...) as udt)
      if (ObExtendType != obj_type || OB_INVALID_ID == udt_id) {
        ret = OB_ERR_INVALID_MULTISET;
        LOG_WARN("MULTISET expression not allowed", K(ret));
      } else {
        const share::schema::ObUDTTypeInfo *dest_info = NULL;
        const uint64_t dest_tenant_id = pl::get_tenant_id_by_object_id(udt_id);
        OZ (schema_checker.get_udt_info(dest_tenant_id, udt_id, dest_info));
        CK (OB_NOT_NULL(dest_info));
        if (OB_SUCC(ret) && !dest_info->is_collection()) {
          ret = OB_ERR_INVALID_CAST_UDT;
          LOG_WARN("invalid CAST to a type that is not a nested table or VARRAY", K(ret));
        }
      }
    } else if (ObExtendType == obj_type
               && OB_INVALID_ID != udt_id
               && !(src->get_expr_type() == T_QUESTIONMARK ||
                    (udt_id == T_OBJ_XML && src->get_expr_type() == T_FUN_SYS_CAST
                    && src->get_param_expr(0)->get_expr_type() == T_QUESTIONMARK))) {
      if (ObNullType == src->get_result_type().get_type()) {
        // do nothing
      } else if (src->get_result_type().is_user_defined_sql_type() ||
                 (src->get_result_type().is_geometry() && is_oracle_mode() && udt_id == T_OBJ_SDO_GEOMETRY)) {
        // allow pl udt cast to sql udt type
        // allow oracle gis cast to pl extend
      } else if (ObExtendType != src->get_result_type().get_type()) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("invalid cast a normal type to udt", K(ret));
      } else if (udt_id == src->get_udt_id()) { //同类型cast，直接返回原始表达式
        expr = src;
      } else {
        const share::schema::ObUDTTypeInfo *src_info = NULL;
        const share::schema::ObUDTTypeInfo *dest_info = NULL;
        const uint64_t src_tenant_id = pl::get_tenant_id_by_object_id(src->get_udt_id());
        const uint64_t dest_tenant_id = pl::get_tenant_id_by_object_id(udt_id);
        OZ (schema_checker.get_udt_info(src_tenant_id, src->get_udt_id(), src_info));
        OZ (schema_checker.get_udt_info(dest_tenant_id, udt_id, dest_info));
        CK (OB_NOT_NULL(src_info), OB_NOT_NULL(dest_info));
        if (OB_SUCC(ret)) {
          if (src_info->is_collection() && dest_info->is_collection()) {
            CK (OB_NOT_NULL(src_info->get_coll_info()), OB_NOT_NULL(dest_info->get_coll_info()));
            if (OB_SUCC(ret)) {
              if (src_info->get_coll_info()->get_elem_type_id() != dest_info->get_coll_info()->get_elem_type_id()) {
                ret = OB_ERR_INVALID_TYPE_FOR_OP;
                LOG_WARN("collection to cast has different elements",
                         K(src_info->get_coll_info()->get_elem_type_id()),
                         K(dest_info->get_coll_info()->get_elem_type_id()),
                         K(ret));
              }
            }
          } else {
            ret = OB_ERR_INVALID_CAST_UDT;
            LOG_WARN("src or dst info can not cast", K(ret), K(src_info->is_collection()), K(dest_info->is_collection()));
          }
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    OZ (SMART_CALL(check_composite_cast(expr->get_param_expr(i), schema_checker)));
  }

  return ret;
}
/*
int ObRawExprUtils::add_expr_to_set(ObRawExprUniqueSet &expr_set, ObRawExpr *expr)
{
  int ret = expr_set.exist_refactored(expr);
  if (OB_HASH_EXIST == ret) {
    ret = OB_SUCCESS;
    // do nothing
  } else if (OB_HASH_NOT_EXIST == ret) {
    OZ(expr_set.set_refactored(expr));
  } else {
    LOG_WARN("fail to search raw expr", K(ret));
  }

  return ret;
}

int ObRawExprUtils::add_exprs_to_set(ObRawExprUniqueSet &expr_set, const ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    OZ(add_expr_to_set(expr_set, exprs.at(i)));
  }
  return ret;
}
*/

int ObRawExprUniqueSet::flatten_and_add_raw_exprs(const ObIArray<ObRawExpr *> &raw_exprs,
                                                  std::function<bool(ObRawExpr *)> filter,
                                                  bool need_flatten_gen_col)
{
  int ret = OB_SUCCESS;
  if (!need_unique_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("need_unique_ of unique set should be true for flatten expr ", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    if (OB_FAIL(this->flatten_and_add_raw_exprs(raw_exprs.at(i), need_flatten_gen_col, filter))) {
      LOG_WARN("fail to flatten raw expr", K(ret));
    }
  }
  OZ(ObRawExprUtils::clear_exprs_flag(get_expr_array(), IS_MARKED));

  return ret;
}

int ObRawExprUniqueSet::flatten_and_add_raw_exprs(const ObRawExprUniqueSet &raw_exprs,
                                           bool need_flatten_gen_col,
                                           std::function<bool(ObRawExpr *)> filter)
{
  return flatten_and_add_raw_exprs(raw_exprs.get_expr_array(), filter, need_flatten_gen_col);
}

int ObRawExprUniqueSet::flatten_and_add_raw_exprs(ObRawExpr *raw_expr,
                                                  bool need_flatten_gen_col,
                                                  std::function<bool(ObRawExpr *)> filter)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_ISNULL(raw_expr) || !filter(raw_expr)) {
    // do nothing
  } else if (OB_FAIL(append(raw_expr))) {
    LOG_WARN("fail to push raw expr", K(ret), KPC(raw_expr));
  } else {
    for (int64_t i = 0;
         OB_SUCC(ret) && i < raw_expr->get_param_count();
         i++) {
      if (OB_ISNULL(raw_expr->get_param_expr(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("expr is null");
      } else if (OB_FAIL(SMART_CALL(flatten_and_add_raw_exprs(raw_expr->get_param_expr(i),
                                                              need_flatten_gen_col,
                                                              filter)))) {
        LOG_WARN("fail to flatten raw expr", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && raw_expr->get_expr_type() == T_CTE_CYCLE_COLUMN) {
      ObPseudoColumnRawExpr* cycle_expr = static_cast<ObPseudoColumnRawExpr*>(raw_expr);
      ObRawExpr *v_raw_expr, *d_v_raw_expr;
      cycle_expr->get_cte_cycle_value(v_raw_expr, d_v_raw_expr);
      if (OB_ISNULL(v_raw_expr) || OB_ISNULL(d_v_raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid raw expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(flatten_and_add_raw_exprs(v_raw_expr,
                                                              need_flatten_gen_col,
                                                              filter)))) {
        LOG_WARN("fail to flatten raw expr", K(ret), K(v_raw_expr));
      } else if (OB_FAIL(SMART_CALL(flatten_and_add_raw_exprs(d_v_raw_expr,
                                                              need_flatten_gen_col,
                                                              filter)))) {
        LOG_WARN("fail to flatten raw expr", K(ret), K(d_v_raw_expr));
      }
    }
    // flatten dependent expr
    if (OB_SUCC(ret) && T_REF_COLUMN == raw_expr->get_expr_type()) {
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(raw_expr);
      ObRawExpr *dependant_expr = col_expr->get_dependant_expr();
      if (NULL == dependant_expr || !need_flatten_gen_col) {
        // do nothing
      } else if (OB_FAIL(SMART_CALL(flatten_and_add_raw_exprs(dependant_expr,
                                                              need_flatten_gen_col,
                                                              filter)))) {
        LOG_WARN("failed to flatten raw expr", K(ret), K(*dependant_expr));
      }
    }
  }

  return ret;
}

int ObRawExprUniqueSet::flatten_temp_expr(ObRawExpr *raw_expr)
{
  int ret = OB_SUCCESS;
  OZ(flatten_and_add_raw_exprs(raw_expr, false/*need_flatten_gen_col*/));

  OZ(ObRawExprUtils::clear_exprs_flag(get_expr_array(), IS_MARKED));
  return ret;
}

int ObRawExprUtils::extract_metadata_filename_expr(ObRawExpr *expr, ObRawExpr *&file_name_expr)
{
  int ret = OB_SUCCESS;
  if (file_name_expr != NULL) {
    //do nothing
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_pseudo_column_expr() && expr->get_expr_type() == T_PSEUDO_EXTERNAL_FILE_URL) {
    file_name_expr = expr;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(extract_metadata_filename_expr(expr->get_param_expr(i), file_name_expr)))) {
        LOG_WARN("extract metadata filename expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::try_add_bool_expr(ObCaseOpRawExpr *parent,
                                      ObRawExprFactory &expr_factory)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in expr is NULL", K(ret));
  } else if (OB_UNLIKELY(T_OP_ARG_CASE == parent->get_expr_type())) {
    // ignore T_OP_ARG_CASE
    LOG_DEBUG("ignore adding bool expr for arg_case expr", K(ret), K(*parent));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent->get_when_expr_size(); ++i) {
      ObRawExpr *when_expr = parent->get_when_param_expr(i);
      ObRawExpr *new_when_expr = NULL;
      if (OB_FAIL(try_create_bool_expr(when_expr, new_when_expr, expr_factory))) {
        LOG_WARN("create_bool_expr failed", K(ret));
      } else if (OB_ISNULL(new_when_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new param_expr is NULL", K(ret));
      } else {
        parent->replace_when_param_expr(i, new_when_expr);
      }
    }
  }
  return ret;
}

int ObRawExprUtils::try_add_bool_expr(ObOpRawExpr *parent, ObRawExprFactory &expr_factory)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in expr is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent->get_param_count(); ++i) {
      ObRawExpr *param_expr = parent->get_param_expr(i);
      ObRawExpr *new_param_expr = NULL;
      if (OB_FAIL(try_create_bool_expr(param_expr, new_param_expr, expr_factory))) {
        LOG_WARN("create_bool_expr failed", K(ret));
      } else if (OB_ISNULL(new_param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new param_expr is NULL", K(ret));
      } else {
        parent->replace_param_expr(i, new_param_expr);
      }
    }
  }
  return ret;
}

int ObRawExprUtils::try_create_bool_expr(ObRawExpr *src_expr,
                                      ObRawExpr *&out_expr,
                                      ObRawExprFactory &expr_factory)
{
  int ret = OB_SUCCESS;
  out_expr = NULL;
  if (OB_ISNULL(src_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("in expr is NULL", K(ret));
  } else {
    ObOpRawExpr *bool_expr = NULL;
    bool need_bool_expr = true;
    if (OB_FAIL(check_need_bool_expr(src_expr, need_bool_expr))) {
      LOG_WARN("check_need_bool_expr failed", K(ret));
    } else if (!need_bool_expr) {
      out_expr = src_expr;
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_BOOL, bool_expr))) {
      LOG_WARN("create bool expr failed", K(ret));
    } else if (OB_ISNULL(bool_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bool_expr is NULL", K(ret));
    } else {
      OZ(bool_expr->add_flag(IS_INNER_ADDED_EXPR));
      OZ(bool_expr->add_param_expr(src_expr));
      OX(out_expr = bool_expr);
    }
  }
  return ret;
}

int ObRawExprUtils::check_need_bool_expr(const ObRawExpr *expr, bool &need_bool_expr)
{
  int ret = OB_SUCCESS;
  need_bool_expr = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in expr is NULL", K(ret));
  } else if (is_oracle_mode()) {
    need_bool_expr = false;
  } else {
    ObItemType expr_type = expr->get_expr_type();
    switch (expr_type) {
      case T_OP_EQ:
      case T_OP_NSEQ:
      case T_OP_LE:
      case T_OP_LT:
      case T_OP_GE:
      case T_OP_GT:
      case T_OP_NE:

      case T_OP_SQ_EQ:
      case T_OP_SQ_NSEQ:
      case T_OP_SQ_LE:
      case T_OP_SQ_LT:
      case T_OP_SQ_GE:
      case T_OP_SQ_GT:
      case T_OP_SQ_NE:

      case T_OP_IS:
      case T_OP_IS_NOT:
      case T_OP_BTW:
      case T_OP_NOT_BTW:
      case T_OP_LIKE:
      case T_OP_NOT_LIKE:
      case T_OP_REGEXP:
      case T_OP_NOT_REGEXP:

      case T_OP_NOT:
      case T_OP_AND:
      case T_OP_OR:
      case T_OP_IN:
      case T_OP_NOT_IN:
      case T_OP_EXISTS:
      case T_OP_NOT_EXISTS:
      case T_OP_XOR:
      case T_OP_BOOL:  {
        need_bool_expr = false;
        break;
      }
      default: {
        need_bool_expr = true;
        break;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::check_is_bool_expr(const ObRawExpr *expr, bool &is_bool_expr)
{
  int ret = OB_SUCCESS;
  is_bool_expr = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in expr is NULL", K(ret));
  } else {
    ObItemType expr_type = expr->get_expr_type();
    switch (expr_type) {
      case T_OP_EQ:
      case T_OP_NSEQ:
      case T_OP_LE:
      case T_OP_LT:
      case T_OP_GE:
      case T_OP_GT:
      case T_OP_NE:

      case T_OP_SQ_EQ:
      case T_OP_SQ_NSEQ:
      case T_OP_SQ_LE:
      case T_OP_SQ_LT:
      case T_OP_SQ_GE:
      case T_OP_SQ_GT:
      case T_OP_SQ_NE:

      case T_OP_IS:
      case T_OP_IS_NOT:
      case T_OP_BTW:
      case T_OP_NOT_BTW:
      case T_OP_LIKE:
      case T_OP_NOT_LIKE:
      case T_OP_REGEXP:
      case T_OP_NOT_REGEXP:

      case T_OP_NOT:
      case T_OP_AND:
      case T_OP_OR:
      case T_OP_IN:
      case T_OP_NOT_IN:
      case T_OP_EXISTS:
      case T_OP_NOT_EXISTS:

      case T_OP_XOR:
      case T_OP_BOOL:  {
        is_bool_expr = true;
        break;
      }
      default: {
        is_bool_expr = false;
        break;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::get_real_expr_without_cast(const ObRawExpr *expr,
                                               const ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is NULL", K(ret));
  } else {
    while (T_FUN_SYS_CAST == expr->get_expr_type() &&
           CM_IS_IMPLICIT_CAST(expr->get_extra())) {
      if (OB_UNLIKELY(2 != expr->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param_count of cast expr should be 2", K(ret), K(*expr));
      } else if (OB_ISNULL(expr = expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("first child of cast expr is NULL", K(ret), K(*expr));
      }
    }
    if (OB_SUCC(ret)) {
      out_expr = expr;
      LOG_DEBUG("get_real_expr_without_cast done", K(*out_expr));
    }
  }
  return ret;
}

int ObRawExprUtils::build_wrapper_inner_expr(ObRawExprFactory &factory,
                                            ObSQLSessionInfo &session_info,
                                            ObRawExpr *arg,
                                            ObRawExpr *&out)
{
  int ret = OB_SUCCESS;
  CK(NULL != arg);
  if (OB_SUCC(ret)) {
    ObSysFunRawExpr *rc_expr = NULL;
    OZ(factory.create_raw_expr(T_FUN_SYS_WRAPPER_INNER, rc_expr));
    CK(NULL != rc_expr);
    OZ(rc_expr->add_param_expr(arg));
    OX(rc_expr->set_func_name(ObString::make_string(N_WRAPPER_INNER)));
    OZ(rc_expr->formalize(&session_info));
    OZ(rc_expr->add_flag(IS_INNER_ADDED_EXPR));
    OX(out = rc_expr);
    LOG_DEBUG("debug build wrapper inner expr", K(ret), K(lbt()), K(*rc_expr));
  }
  return ret;
}

int ObRawExprUtils::build_dup_data_expr(ObRawExprFactory &factory,
                                        ObRawExpr *param,
                                        ObRawExpr *&new_param)
{
  int ret = OB_SUCCESS;
  char name_buf[128];
  char *name = NULL;
  int64_t pos = 0;
  ObOpPseudoColumnRawExpr* pseudo = NULL;
  if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(ret), K(param));
  } else if (OB_FAIL(databuff_printf(name_buf, 128, pos, "dup("))) {
    LOG_WARN("failed to print buf", K(ret));
  } else if (OB_FAIL(param->get_name(name_buf, 128, pos))) {
    LOG_WARN("failed to print param name", K(ret));
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(name = (char *)factory.get_allocator().alloc(pos + 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      memcpy(name, name_buf, pos);
      name[pos] = ')';
      name[pos + 1] = '\0';
      if (OB_FAIL(build_op_pseudo_column_expr(factory,
                                              T_PSEUDO_DUP_EXPR,
                                              name,
                                              param->get_result_type(),
                                              pseudo))) {
        LOG_WARN("failed to build op pseudo column expr", K(ret));
      } else {
        new_param = pseudo;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::build_inner_aggr_code_expr(ObRawExprFactory &factory,
                                            const ObSQLSessionInfo &session_info,
                                            ObRawExpr *&out)
{
  int ret = OB_SUCCESS;
  ObOpPseudoColumnRawExpr *expr = NULL;
  ObExprResType res_type;
  res_type.set_int();
  res_type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  res_type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  OZ(build_op_pseudo_column_expr(factory, T_INNER_AGGR_CODE, "AGGR_CODE", res_type, expr));
  OZ(expr->formalize(&session_info));
  OZ(expr->add_flag(IS_INNER_ADDED_EXPR));
  if (OB_SUCC(ret)) {
    out = expr;
    LOG_DEBUG("debug build wrapper inner expr", K(ret), K(lbt()), K(expr));
  }
  return ret;
}

int ObRawExprUtils::build_inner_wf_aggr_status_expr(ObRawExprFactory &factory,
                                                    const ObSQLSessionInfo &session_info,
                                                    ObOpPseudoColumnRawExpr *&out)
{
  int ret = OB_SUCCESS;
  ObOpPseudoColumnRawExpr *expr = NULL;
  ObExprResType res_type;
  res_type.set_int();
  res_type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  res_type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  OZ(build_op_pseudo_column_expr(factory, T_INNER_WF_AGGR_STAUTS, "AGGR_STATUS", res_type, expr));
  OZ(expr->formalize(&session_info));
  if (OB_SUCC(ret)) {
    expr->add_flag(IS_INNER_ADDED_EXPR);
    out = expr;
    LOG_DEBUG("debug build wrapper inner expr", K(ret), K(lbt()), K(expr));
  }
  return ret;
}

// only for rollup distributor and collector
int ObRawExprUtils::build_pseudo_rollup_id(ObRawExprFactory &factory,
                                           const ObSQLSessionInfo &session_info,
                                           ObRawExpr *&out)
{
  int ret = OB_SUCCESS;
  ObOpPseudoColumnRawExpr *expr = NULL;
  ObExprResType res_type;
  res_type.set_int();
  res_type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  res_type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  OZ(build_op_pseudo_column_expr(factory, T_PSEUDO_ROLLUP_ID, "ROLLUP_ID", res_type, expr));
  OZ(expr->formalize(&session_info));
  OZ(expr->add_flag(IS_INNER_ADDED_EXPR));
  if (OB_SUCC(ret)) {
    out = expr;
    LOG_DEBUG("debug build wrapper inner expr", K(ret), K(lbt()), K(expr));
  }
  return ret;
}

int ObRawExprUtils::build_pseudo_random(ObRawExprFactory &factory,
                                        const ObSQLSessionInfo &session_info,
                                        ObRawExpr *&out)
{
  int ret = OB_SUCCESS;
  ObOpPseudoColumnRawExpr *expr = NULL;
  ObExprResType res_type;
  res_type.set_int();
  res_type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  res_type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  OZ(build_op_pseudo_column_expr(factory, T_PSEUDO_RANDOM, "RANDOM", res_type, expr));
  OZ(expr->formalize(&session_info));
  OZ(expr->add_flag(IS_INNER_ADDED_EXPR));
  if (OB_SUCC(ret)) {
    out = expr;
  }
  return ret;
}

int ObRawExprUtils::build_remove_const_expr(ObRawExprFactory &factory,
                                            ObSQLSessionInfo &session_info,
                                            ObRawExpr *arg,
                                            ObRawExpr *&out)
{
  int ret = OB_SUCCESS;
  CK(NULL != arg);
  if (OB_SUCC(ret)) {
    ObSysFunRawExpr *rc_expr = NULL;
    OZ(factory.create_raw_expr(T_FUN_SYS_REMOVE_CONST, rc_expr));
    CK(NULL != rc_expr);
    OZ(rc_expr->add_param_expr(arg));
    OX(rc_expr->set_func_name(ObString::make_string(N_REMOVE_CONST)));
    OZ(rc_expr->formalize(&session_info));
    OZ(rc_expr->add_flag(IS_INNER_ADDED_EXPR));
    OX(out = rc_expr);
  }
  return ret;
}

int ObRawExprUtils::build_returning_lob_expr(ObRawExprFactory &factory,
                                             ObSQLSessionInfo &session_info,
                                             ObColumnRefRawExpr *ref_expr,
                                             ObSysFunRawExpr *&out)
{
  int ret = OB_SUCCESS;
  UNUSED(session_info);
  CK(NULL != ref_expr);
  if (OB_SUCC(ret)) {
    ObSysFunRawExpr *returning_lob_expr = NULL;
    OZ(factory.create_raw_expr(T_FUN_RETURNING_LOB, returning_lob_expr));
    CK(NULL != returning_lob_expr);
    OZ(returning_lob_expr->add_param_expr(ref_expr));
    OX(returning_lob_expr->set_func_name(ObString::make_string("returning_lob")));
    OZ(returning_lob_expr->add_flag(IS_INNER_ADDED_EXPR));
    returning_lob_expr->set_data_type(ref_expr->get_data_type());
    OX(out = returning_lob_expr);
  }
  return ret;
}

bool ObRawExprUtils::is_pseudo_column_like_expr(const ObRawExpr &expr)
{
  bool bret = false;
  if (ObRawExpr::EXPR_PSEUDO_COLUMN == expr.get_expr_class() ||
      T_FUN_SYS_ROWNUM == expr.get_expr_type() ||
      T_FUN_SYS_SEQ_NEXTVAL == expr.get_expr_type()) {
    bret = true;
  }
  return bret;
}

bool ObRawExprUtils::is_sharable_expr(const ObRawExpr &expr)
{
  int bret = false;
  if (expr.is_query_ref_expr() || expr.is_column_ref_expr() || expr.is_aggr_expr() ||
      expr.is_win_func_expr() || is_pseudo_column_like_expr(expr)) {
    bret = true;
  }
  return bret;
}

int ObRawExprUtils::check_need_cast_expr(const ObExprResType &src_type,
                                         const ObExprResType &dst_type,
                                         bool &need_cast,
                                         bool &ignore_dup_cast_error)
{
  int ret = OB_SUCCESS;
  need_cast = false;
  ignore_dup_cast_error = false;
  ObObjType in_type  = src_type.get_type();
  ObObjType out_type = dst_type.get_type();
  ObScale in_scale = src_type.get_scale();
  ObScale out_scale = dst_type.get_scale();
  ObCollationType in_cs_type  = src_type.get_collation_type();
  ObCollationType out_cs_type = dst_type.get_collation_type();
  const bool is_same_need = true;
  bool need_wrap = false;
  if (!ob_is_valid_obj_type(in_type) || !ob_is_valid_obj_type(out_type) ||
      (ob_is_string_or_lob_type(in_type) && !ObCharset::is_valid_collation(in_cs_type)) ||
      (ob_is_string_or_lob_type(out_type) && !ObCharset::is_valid_collation(out_cs_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src type or dst type", K(ret), K(src_type), K(dst_type));
  } else if ((ob_is_string_or_lob_type(in_type) && in_type == out_type && in_cs_type == out_cs_type)
             || (!ob_is_string_or_lob_type(in_type) && in_type == out_type)) {
    need_cast = false;
    if (lib::is_mysql_mode() && ob_is_double_type(in_type) &&
          src_type.get_scale() != dst_type.get_scale() &&
          src_type.get_precision() != PRECISION_UNKNOWN_YET) {
      // for the conversion between doubles with increased scale in mysql mode,
      // there are tow cases need to explicitly add the cast expression:
      // case 1: the scale of dst_type is unknow(-1), the scale represents the largest float/double
      //         scale in mysql.
      // case 2: dst_scale is greater than src_scale, need to be aligned
      need_cast = (SCALE_UNKNOWN_YET == dst_type.get_scale()) ||
                     (src_type.get_scale() < dst_type.get_scale());
    } else if (ob_is_decimal_int(in_type) && decimal_int_need_cast(src_type.get_accuracy(),
                                                                   dst_type.get_accuracy())) {
      need_cast = true;
    }
    // mark as scale adjust cast to avoid repeat cast error
    if (need_cast) {
      ignore_dup_cast_error = true; // scale adjust cast need ignore duplicate cast error.
    }
  } else if (ob_is_enumset_tc(out_type)) {
    //no need add cast, will add column_conv later
    need_cast = false;
  } else if ((ob_is_xml_sql_type(in_type, src_type.get_subschema_id()) || ob_is_xml_pl_type(in_type, src_type.get_udt_id())) &&
              ob_is_blob(out_type, out_cs_type)) {
    //no need add cast, will transform in make_xmlbinary
    // there are cases cannot skip cast expr, and xmltype cast to clob is not support and cast func will check:
    // case: select xmlserialize(content xmltype_var as clob) || xmltype_var from t;
    need_cast = false;
  } else if (OB_FAIL(ObRawExprUtils::need_wrap_to_string(in_type, out_type,
                                                         is_same_need, need_wrap))) {
    LOG_WARN("failed to check_need_wrap_to_string", K(ret));
  } else if (need_wrap) {
    //no need add cast, add enumset_str expr later
    need_cast = false;
  } else if (!cast_supported(in_type, in_cs_type, out_type, out_cs_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("transition does not support", K(in_type), K(out_type));
  } else if (src_type.is_null() || dst_type.is_null()) {
    // null -> xxx or xxx -> null
    // xxx->null 底层转换函数会报错
    need_cast = true;
  } else {
    need_cast = true;
    if (src_type.is_decimal_int() && dst_type.is_number()) {
      // ignore duplicate cast error caused by decimal int fallback to number
      ignore_dup_cast_error = true;
    }
  }
  LOG_DEBUG("check_need_cast_expr", K(ret), K(need_cast), K(src_type), K(dst_type));
  return ret;
}

// private member, use create_cast_expr() instead.
int ObRawExprUtils::create_real_cast_expr(ObRawExprFactory &expr_factory,
                                          ObRawExpr *src_expr,
                                          const ObExprResType &dst_type,
                                          ObSysFunRawExpr *&func_expr,
                                          const ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *dst_expr = NULL;
  if (OB_ISNULL(src_expr) || OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(src_expr), KP(session_info));
  } else {
    if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_CAST, func_expr))) {
      LOG_WARN("create cast expr failed", K(ret));
    } else if (OB_FAIL(create_type_expr(expr_factory, dst_expr, dst_type))) {
      LOG_WARN("create type expr failed", K(ret));
    } else if (OB_FAIL(func_expr->add_param_expr(src_expr))) {
      LOG_WARN("add real param expr failed", K(ret));
    } else {
      ObString func_name = ObString::make_string(N_CAST);
      func_expr->set_func_name(func_name);
      if (src_expr->is_for_generated_column()) {
        func_expr->set_for_generated_column();
      }
      if (OB_FAIL(func_expr->add_param_expr(dst_expr))) {
        LOG_WARN("add dest type expr failed", K(ret));
      } else {
        func_expr->set_result_type(dst_type);
      }
      LOG_DEBUG("create_cast_expr debug", K(ret), K(*src_expr), K(dst_type),
                                          K(*func_expr), K(lbt()), K(func_expr));
    }
  }
  return ret;
}

int ObRawExprUtils::create_type_expr(ObRawExprFactory &expr_factory,
                                     ObConstRawExpr *&type_expr,
                                     const ObExprResType &dst_type,
                                     bool avoid_zero_len)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *dst_expr = NULL;
  ParseNode parse_node;
  memset(&parse_node, 0, sizeof(ParseNode));
  ObObj val;
  if (OB_FAIL(expr_factory.create_raw_expr(T_INT, dst_expr))) {
    LOG_WARN("create dest type expr failed", K(ret));
  } else {
    parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX] = static_cast<int16_t>(dst_type.get_type());
    parse_node.int16_values_[OB_NODE_CAST_COLL_IDX] = static_cast<int16_t>(
                                                        dst_type.get_collation_type());
    if (ob_is_string_or_lob_type(dst_type.get_type())) {
      parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX] = (avoid_zero_len && dst_type.get_length() == 0) ?
                                                          1 : dst_type.get_length();
      if (lib::is_oracle_mode()) {
        dst_expr->set_length_semantics(dst_type.get_length_semantics());
      }
    } else if (ob_is_rowid_tc(dst_type.get_type())) {
      int32_t urowid_len = dst_type.get_length();
      if (urowid_len <= -1) {
        urowid_len = 4000;
      }
      parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX] = 4000;
    } else if (ObIntervalYMType == dst_type.get_type()) {
      // TODO: @shaoge 针对ObIntervalYMType和ObIntervalDSType，parse_node的设置需要写case验证
      if (dst_type.get_scale() == -1) {
        // scale=-1 is invalid, update to default value
        ObCompatibilityMode compatibility_mode = get_compatibility_mode();
        parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] =
            ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode]
                                             [ObIntervalYMType]
                                                 .get_scale();
      } else {
        parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] =
            dst_type.get_scale(); // year
      }
    } else if (ObIntervalDSType == dst_type.get_type()) {
      ObCompatibilityMode compatibility_mode = get_compatibility_mode();
      parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX] = (dst_type.get_scale() / 10); // day
      if (dst_type.get_scale() == -1) {
        // scale=-1 is invalid, update to default value
        parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = 0;
      } else {
        parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = (dst_type.get_scale() % 10);// second
      }
    } else if ((ObTimestampNanoType == dst_type.get_type() ||
                ObTimestampTZType == dst_type.get_type() ||
                ObTimestampLTZType == dst_type.get_type()) &&
               dst_type.get_scale() == -1) {
      // scale=-1 is invalid, update to default value
      ObCompatibilityMode compatibility_mode = get_compatibility_mode();
      parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX] = dst_type.get_precision();
      parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] =
            ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode]
                                             [dst_type.get_type()]
                                                 .get_scale();
    } else if (dst_type.is_decimal_int()) {
      // precision of decimal int value may be larger than 81, e.g. int(255) + decimal(10, 2)
      // truncate precision here.
      ObPrecision prec = dst_type.get_precision();
      if (prec > OB_MAX_DECIMAL_POSSIBLE_PRECISION) {
        prec = OB_MAX_DECIMAL_POSSIBLE_PRECISION;
      }
      parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX] = prec;
      parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = dst_type.get_scale();
    } else {
      parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX] = dst_type.get_precision();
      parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = dst_type.get_scale();
    }
    if (dst_type.is_ext()
        || dst_type.is_user_defined_sql_type()
        || dst_type.is_collection_sql_type()) {
      // it is not possible to use arg[1] (u32) to record udt_id
      // use subschema id before type deduce maybe also not good idea
      // but we can always record real udt id on ObExprResType
      if (dst_type.is_xml_sql_type()) {
        dst_expr->set_udt_id(T_OBJ_XML);
      } else {
        dst_expr->set_udt_id(dst_type.get_udt_id());
      }
    }

    val.set_int(parse_node.value_);
    dst_expr->set_value(val);
    dst_expr->set_param(val);
    type_expr = dst_expr;
  }
  return ret;
}

int ObRawExprUtils::build_add_expr(ObRawExprFactory &expr_factory,
                                   ObRawExpr *param_expr1,
                                   ObRawExpr *param_expr2,
                                   ObOpRawExpr *&add_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(param_expr1), K(param_expr2), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ADD, add_expr))) {
    LOG_WARN("create add expr failed", K(ret));
  } else if (OB_ISNULL(add_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add expr is null", K(ret), K(add_expr));
  } else if (OB_FAIL(add_expr->add_param_expr(param_expr1))) {
    LOG_WARN("add text expr to add expr failed", K(ret));
  } else if (OB_FAIL(add_expr->add_param_expr(param_expr2))) {
    LOG_WARN("add pattern expr to add expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::build_minus_expr(ObRawExprFactory &expr_factory,
                                     ObRawExpr *param_expr1,
                                     ObRawExpr *param_expr2,
                                     ObOpRawExpr *&minus_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(param_expr1), K(param_expr2), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_MINUS, minus_expr))) {
    LOG_WARN("create minus expr failed", K(ret));
  } else if (OB_ISNULL(minus_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("minus expr is null", K(ret), K(minus_expr));
  } else if (OB_FAIL(minus_expr->add_param_expr(param_expr1))) {
    LOG_WARN("add text expr to minus expr failed", K(ret));
  } else if (OB_FAIL(minus_expr->add_param_expr(param_expr2))) {
    LOG_WARN("add pattern expr to minus expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::build_date_add_expr(ObRawExprFactory &expr_factory,
                                        ObRawExpr *param_expr1,
                                        ObRawExpr *param_expr2,
                                        ObRawExpr *param_expr3,
                                        ObSysFunRawExpr *&date_add_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2) || OB_ISNULL(param_expr3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(param_expr1), K(param_expr2), K(param_expr3), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_DATE_ADD, date_add_expr))) {
    LOG_WARN("create to_type expr failed", K(ret));
  } else if (OB_ISNULL(date_add_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("date add expr is null", K(ret), K(date_add_expr));
  } else if (OB_FAIL(date_add_expr->set_param_exprs(param_expr1, param_expr2, param_expr3))) {
    LOG_WARN("add param expr failed", K(ret));
  } else {
    ObString func_name = ObString::make_string("date_add");
    date_add_expr->set_func_name(func_name);
  }
  return ret;
}

int ObRawExprUtils::build_date_sub_expr(ObRawExprFactory &expr_factory,
                                        ObRawExpr *param_expr1,
                                        ObRawExpr *param_expr2,
                                        ObRawExpr *param_expr3,
                                        ObSysFunRawExpr *&date_sub_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2) || OB_ISNULL(param_expr3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(param_expr1), K(param_expr2), K(param_expr3), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_DATE_SUB, date_sub_expr))) {
    LOG_WARN("create to_type expr failed", K(ret));
  } else if (OB_ISNULL(date_sub_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("date sub expr is null", K(ret), K(date_sub_expr));
  } else if (OB_FAIL(date_sub_expr->set_param_exprs(param_expr1, param_expr2, param_expr3))) {
    LOG_WARN("add param expr failed", K(ret));
  } else {
    ObString func_name = ObString::make_string("date_sub");
    date_sub_expr->set_func_name(func_name);
  }
  return ret;
}

int ObRawExprUtils::build_common_binary_op_expr(ObRawExprFactory &expr_factory,
                                                const ObItemType expect_op_type,
                                                ObRawExpr *param_expr1,
                                                ObRawExpr *param_expr2,
                                                ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *op_expr = NULL;
  if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(param_expr1), K(param_expr2), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(expect_op_type, op_expr))) {
    LOG_WARN("create op expr failed", K(ret));
  } else if (OB_ISNULL(op_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add expr is null", K(ret), K(op_expr));
  } else if (OB_FAIL(op_expr->add_param_expr(param_expr1))) {
    LOG_WARN("add text expr to add expr failed", K(ret));
  } else if (OB_FAIL(op_expr->add_param_expr(param_expr2))) {
    LOG_WARN("add pattern expr to add expr failed", K(ret));
  } else {
    expr = op_expr;
  }
  return ret;
}

int ObRawExprUtils::build_case_when_expr(ObRawExprFactory &expr_factory,
                                         ObRawExpr *when_expr,
                                         ObRawExpr *then_expr,
                                         ObRawExpr *default_expr,
                                         ObRawExpr *&case_when_expr)
{
  int ret = OB_SUCCESS;
  ObCaseOpRawExpr *c_case_when_expr = NULL;
  if (OB_ISNULL(when_expr) || OB_ISNULL(then_expr) || OB_ISNULL(default_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(when_expr), K(then_expr), K(default_expr), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_CASE, c_case_when_expr))) {
    LOG_WARN("create add expr failed", K(ret));
  } else if (OB_ISNULL(c_case_when_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(c_case_when_expr));
  } else if (OB_FAIL(c_case_when_expr->add_when_param_expr(when_expr))) {
    LOG_WARN("failed to add when param expr", K(ret));
  } else if (OB_FAIL(c_case_when_expr->add_then_param_expr(then_expr))) {
    LOG_WARN("failed to add then expr", K(ret));
  } else {
    c_case_when_expr->set_default_param_expr(default_expr);
    case_when_expr = c_case_when_expr;
  }
  return ret;
}

int ObRawExprUtils::build_is_not_expr(ObRawExprFactory &expr_factory,
                                      ObRawExpr *param_expr1,
                                      ObRawExpr *param_expr2,
                                      ObRawExpr *&is_not_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *not_expr = NULL;
  if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(param_expr1), K(param_expr2), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_IS_NOT, not_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(not_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not_expr is null", K(ret), K(not_expr));
  } else if (OB_FAIL(not_expr->set_param_exprs(param_expr1, param_expr2))) {
    LOG_WARN("failed to set param for not expr", K(ret), K(not_expr));
  } else {
    is_not_expr = not_expr;
  }
  return ret;
}

// is_not_null is true: build is not null for param_expr
// is_not_null is false: build is null for param_expr
int ObRawExprUtils::build_is_not_null_expr(ObRawExprFactory &expr_factory,
                                          ObRawExpr *param_expr,
                                          bool is_not_null,
                                          ObRawExpr *&is_not_null_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *null_expr = NULL;
  ObOpRawExpr *is_not_expr = NULL;
  ObItemType expr_type = is_not_null ? T_OP_IS_NOT : T_OP_IS;
  is_not_null_expr = NULL;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(param_expr), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(expr_type, is_not_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_NULL, null_expr))) {
    LOG_WARN("failed to create const null expr", K(ret));
  } else if (OB_ISNULL(is_not_expr) || OB_ISNULL(null_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create not null expr", K(ret));
  } else {
    ObObjParam null_val;
    null_val.set_null();
    null_val.set_param_meta();
    null_expr->set_param(null_val);
    null_expr->set_value(null_val);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(is_not_expr->set_param_exprs(param_expr, null_expr))) {
    LOG_WARN("failed to set param for not_null op", K(ret), KPC(is_not_expr));
  } else {
    is_not_null_expr = is_not_expr;
  }
  return ret;
}

int ObRawExprUtils::process_window_complex_agg_expr(ObRawExprFactory &expr_factory,
                                                    const ObItemType func_type,
                                                    ObWinFunRawExpr *win_func,
                                                    ObRawExpr *&window_agg_expr,
                                                    ObIArray<ObWinFunRawExpr*> *win_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(win_func) || OB_ISNULL(window_agg_expr) || OB_ISNULL(win_exprs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ret), K(win_func), K(window_agg_expr), K(win_exprs));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (ObOptimizerUtil::find_item(*win_exprs, window_agg_expr)) {
    /*do nothing*/
  } else if (T_FUN_SUM == func_type
             || T_FUN_COUNT == func_type
             || T_FUN_KEEP_SUM == func_type
             || T_FUN_KEEP_COUNT == func_type
             || T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == func_type) {
    ObWinFunRawExpr *win_func_expr = NULL;
    ObAggFunRawExpr *agg_expr = static_cast<ObAggFunRawExpr *>(window_agg_expr);
    if (agg_expr->is_param_distinct()
        && (win_func->has_order_items() || win_func->has_frame_orig())) {
      ret = OB_ORDERBY_CLAUSE_NOT_ALLOWED;
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_WINDOW_FUNCTION, win_func_expr))) {
      LOG_WARN("failed to create window function expr", K(ret));
    } else if (OB_ISNULL(win_func_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("win_func_expr is null", K(ret), K(win_func_expr));
    } else if (OB_FAIL(win_func_expr->assign(*win_func))) {
      LOG_WARN("failed to assigned win func expr.", K(ret));
    } else if (FALSE_IT(win_func_expr->set_agg_expr(agg_expr))) {
      LOG_WARN("failed to set agg expr.", K(ret));
    } else if (FALSE_IT(window_agg_expr = win_func_expr)) {
      LOG_WARN("failed to replace the agg expr.", K(ret));
    } else if (FALSE_IT(win_func_expr->set_func_type(func_type))) {
      LOG_WARN("failed to set func type.", K(ret));
    } else if (OB_FAIL(win_exprs->push_back(win_func_expr))) {
      LOG_WARN("failed to push back win func epxr.", K(ret));
    } else {/*do nothing */}
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < window_agg_expr->get_param_count(); i++) {
      ObRawExpr *&sub_expr = window_agg_expr->get_param_expr(i);
      if (OB_ISNULL(sub_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("error argument.", K(ret));
      } else if (OB_FAIL(process_window_complex_agg_expr(expr_factory,
                                                         sub_expr->get_expr_type(),
                                                         win_func,
                                                         sub_expr,
                                                         win_exprs))) {
        LOG_WARN("failed to process window complex agg node.", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObRawExprUtils::build_common_aggr_expr(ObRawExprFactory &expr_factory,
                                           ObSQLSessionInfo *session_info,
                                           const ObItemType expect_op_type,
                                           ObRawExpr *param_expr,
                                           ObAggFunRawExpr *&aggr_expr)
{
  int ret = OB_SUCCESS;
  aggr_expr = NULL;
  if (OB_ISNULL(param_expr) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(param_expr), K(session_info));
  } else if (OB_FAIL(expr_factory.create_raw_expr(expect_op_type, aggr_expr))) {
    LOG_WARN("create ObAggFunRawExpr failed", K(ret));
  } else if (OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("agg_expr is null", K(ret), K(aggr_expr));
  } else if (OB_FAIL(aggr_expr->add_flag(IS_INNER_ADDED_EXPR))) {
    LOG_WARN("failed to add flag", K(ret));
  } else if (OB_FAIL(aggr_expr->add_real_param_expr(param_expr))) {
    LOG_WARN("failed to add param expr to agg expr", K(ret));
  } else if (OB_FAIL(aggr_expr->formalize(session_info))) {
    LOG_WARN("failed to extract info", K(ret));
  } else {/*do nothing */}
  return ret;
}

//专用于limit_expr/offset_expr中，用于构造case when limit_expr < 0 then 0 else limit_expr end
int ObRawExprUtils::build_case_when_expr_for_limit(ObRawExprFactory &expr_factory,
                                                   ObRawExpr *limit_expr,
                                                   ObRawExpr *&expected_case_when_expr)
{
  int ret = OB_SUCCESS;
  expected_case_when_expr = NULL;
  ObRawExpr *case_when_expr = NULL;
  ObRawExpr *less_expr = NULL;
  ObConstRawExpr *zero_expr = NULL;
  if (OB_ISNULL(limit_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory,
                                          ObIntType,
                                          0,
                                          zero_expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(build_common_binary_op_expr(expr_factory,
                                                  T_OP_LT,
                                                  limit_expr,
                                                  zero_expr,
                                                  less_expr))) {
    LOG_WARN("failed to build common binary op expr", K(ret));
  } else if (OB_FAIL(build_case_when_expr(expr_factory,
                                          less_expr,
                                          zero_expr,
                                          limit_expr,
                                          case_when_expr))) {
    LOG_WARN("failed to build case when expr", K(ret));
  } else {
    expected_case_when_expr = case_when_expr;
  }
  return ret;
}

int ObRawExprUtils::build_not_expr(ObRawExprFactory &expr_factory,
                                  ObRawExpr *expr,
                                  ObRawExpr* &not_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *new_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr has null", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_NOT, new_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(new_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create not expr", K(ret));
  } else if (OB_FAIL(new_expr->set_param_expr(expr))) {
    LOG_WARN("failed to set param for not op", K(ret), K(*not_expr));
  } else {
    not_expr = new_expr;
  }
  return ret;
}

int ObRawExprUtils::build_or_exprs(ObRawExprFactory &expr_factory,
                                  const ObIArray<ObRawExpr*> &exprs,
                                  ObRawExpr* &or_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *new_expr = NULL;
  or_expr = NULL;
  if (exprs.count() == 0) {
    or_expr = NULL;
  } else if (exprs.count() ==1) {
    or_expr = exprs.at(0);
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_OR, new_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(new_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create or expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      ObRawExpr *expr = exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exprs has null child", K(i));
      } else if (OB_FAIL(new_expr->add_param_expr(expr))) {
        LOG_WARN("add param expr to or expr failed", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      or_expr = new_expr;
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRawExprUtils::build_and_expr(ObRawExprFactory &expr_factory,
                                   const ObIArray<ObRawExpr*> &exprs,
                                   ObRawExpr * &and_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *new_expr = NULL;
  and_expr = NULL;
  if (exprs.count() == 0) {
    and_expr = NULL;
  } else if (exprs.count() ==1) {
    and_expr = exprs.at(0);
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_AND, new_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(new_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create or expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      ObRawExpr *expr = exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exprs has null child", K(i));
      } else if (OB_FAIL(new_expr->add_param_expr(expr))) {
        LOG_WARN("add param expr to or expr failed", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      and_expr = new_expr;
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRawExprUtils::new_parse_node(ParseNode *&node, ObRawExprFactory &expr_factory,
                                   ObItemType type, int num, ObString str_val)
{
  int ret = OB_SUCCESS;
  char *str_value = NULL;
  if (OB_FAIL(new_parse_node(node, expr_factory, type, num))) {
    LOG_WARN("fail to alloc new node", K(ret));
  } else if (str_val.length() <= 0) {
    /*nothing*/
  } else if (OB_ISNULL(str_value = static_cast<char *>(expr_factory.get_allocator().alloc(
                                                                              str_val.length())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc buf", K(ret));
  } else {
    MEMCPY(str_value, str_val.ptr(), str_val.length());
    node->str_value_ = str_value;
    node->str_len_ = str_val.length();
  }
  return ret;
}

int ObRawExprUtils::new_parse_node(ParseNode *& node, ObRawExprFactory &expr_factory,
                                   ObItemType type, int num)
{
  int ret = OB_SUCCESS;
  ObIAllocator *alloc_buf = &(expr_factory.get_allocator());
  node = (ParseNode *)alloc_buf->alloc(sizeof(ParseNode));
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocate parse node failed", K(ret), K(node));
  } else {
    node->type_ = type;
    node->num_child_ = num;
    node->param_num_ = 0;
    node->is_neg_ = 0;
    node->is_hidden_const_ = 0;
    node->is_date_unit_ = 0;
    node->is_tree_not_param_ = 0;
    node->is_multiset_ = 0;
    node->value_ = INT64_MAX;
    node->str_len_ = 0;
    node->str_value_ = NULL;
    node->text_len_ = 0;
    node->raw_text_ = NULL;
    node->pos_ = 0;
    if (num > 0) {
      int64_t alloc_size = sizeof(ParseNode *) * num ;
      node->children_ = (ParseNode **)alloc_buf->alloc(alloc_size);
      if (OB_ISNULL(node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocate children buffer failed", K(ret), K(node->children_));
      }
    } else {
      node->children_ = NULL;
    }
  }
  return ret;
}

int ObRawExprUtils::build_rowid_expr(const ObDMLStmt *dml_stmt,
                                     ObRawExprFactory &expr_factory,
                                     ObIAllocator &alloc,
                                     const ObSQLSessionInfo &session_info,
                                     const ObTableSchema &table_schema,
                                     const uint64_t logical_table_id,
                                     const ObIArray<ObRawExpr *> &rowkey_exprs,
                                     ObSysFunRawExpr *&rowid_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> col_ids;
  int64_t rowkey_cnt = 0;

  OZ(table_schema.get_column_ids_serialize_to_rowid(col_ids, rowkey_cnt));
  CK(col_ids.count() > 0);
  CK(rowkey_exprs.count() == col_ids.count());

  if (OB_SUCC(ret)) {
    // setup rowid version
    int64_t version = ObURowIDData::INVALID_ROWID_VERSION;
    ObObj version_obj;
    number::ObNumber version_nmb;
    ObConstRawExpr *version_expr = NULL;
    OZ(table_schema.get_rowid_version(rowkey_cnt, col_ids.count(), version));
    OZ(version_nmb.from(version, alloc));
    OX(version_obj.set_number(version_nmb));
    OZ(expr_factory.create_raw_expr(T_NUMBER, version_expr));
    CK(OB_NOT_NULL(version_expr));
    OX(version_expr->set_value(version_obj));

    // calc_urowid expr
    ObSysFunRawExpr *calc_urowid_expr = NULL;
    OZ(expr_factory.create_raw_expr(T_FUN_SYS_CALC_UROWID, calc_urowid_expr));
    CK(OB_NOT_NULL(calc_urowid_expr));
    calc_urowid_expr->set_func_name(ObString::make_string(N_CALC_UROWID));
    OZ(calc_urowid_expr->add_param_expr(version_expr));
    if (table_schema.is_external_table()) {
      OZ(add_calc_partition_id_on_calc_rowid_expr(dml_stmt, expr_factory, session_info,
                                                  table_schema, logical_table_id,
                                                  calc_urowid_expr));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs.count(); ++i) {
      OZ(calc_urowid_expr->add_param_expr(rowkey_exprs.at(i)));
    }
    if (OB_SUCC(ret) && !table_schema.is_external_table()) {
      //set calc tablet id for heap table calc_urowid expr
      OZ(add_calc_tablet_id_on_calc_rowid_expr(dml_stmt, expr_factory, session_info,
                                               table_schema, logical_table_id, calc_urowid_expr));
    }
    OZ(calc_urowid_expr->formalize(&session_info));
    rowid_expr = calc_urowid_expr;
    LOG_TRACE("succeed to build rowid expr", KPC(rowid_expr));
  }
  return ret;
}

int ObRawExprUtils::build_empty_rowid_expr(ObRawExprFactory &expr_factory,
                                           uint64_t table_id,
                                           ObRawExpr *&rowid_expr)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *col_expr = NULL;
  OZ(expr_factory.create_raw_expr(T_REF_COLUMN, col_expr));
  CK(OB_NOT_NULL(col_expr));
  col_expr->set_ref_id(table_id, OB_INVALID_ID);
  col_expr->set_data_type(ObURowIDType);
  col_expr->set_column_name(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME);

  ObAccuracy accuracy;
  accuracy.set_length(OB_MAX_USER_ROW_KEY_LENGTH);
  accuracy.set_precision(-1);
  col_expr->set_accuracy(accuracy);
  rowid_expr = col_expr;
  return ret;
}

int ObRawExprUtils::build_rownum_expr(ObRawExprFactory &expr_factory,
                                      ObRawExpr* &rownum_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *rownum = NULL;
  rownum_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_ROWNUM, rownum))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(rownum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret));
  } else {
    rownum->set_func_name(ObString::make_string("rownum"));
    rownum->set_data_type(ObNumberType);
    rownum_expr = rownum;
  }
  return ret;
}

int ObRawExprUtils::build_to_outfile_expr(ObRawExprFactory &expr_factory,
                                          const ObSQLSessionInfo *session_info,
                                          const ObSelectIntoItem *into_item,
                                          const ObIArray<ObRawExpr*> &exprs,
                                          ObRawExpr* &to_outfile_expr)
{
  int ret = OB_SUCCESS;
  to_outfile_expr = NULL;
  ObOpRawExpr *new_expr = NULL;
  ObConstRawExpr *field_expr = NULL;
  ObConstRawExpr *line_expr = NULL;
  ObConstRawExpr *closed_cht_expr = NULL;
  ObRawExpr *is_optional_expr = NULL;
  ObConstRawExpr *escaped_cht_expr = NULL;
  ObConstRawExpr *charset_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_TO_OUTFILE_ROW, new_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(build_const_string_expr(expr_factory, ObVarcharType,
      into_item->field_str_.get_varchar(), into_item->field_str_.get_collation_type(), field_expr))) {
    LOG_WARN("fail to create string expr", K(ret));
  } else if (OB_FAIL(build_const_string_expr(expr_factory, ObVarcharType,
      into_item->line_str_.get_varchar(), into_item->line_str_.get_collation_type(), line_expr))) {
    LOG_WARN("fail to create string expr", K(ret));
  } else if (OB_FAIL(build_const_string_expr(expr_factory, ObVarcharType,
      into_item->closed_cht_.get_varchar(), into_item->closed_cht_.get_collation_type(), closed_cht_expr))) {
    LOG_WARN("fail to create string expr", K(ret));
  } else if (OB_FAIL(build_const_bool_expr(&expr_factory, is_optional_expr,
      into_item->is_optional_))) {
    LOG_WARN("fail to create bool expr", K(ret));
  } else if (OB_FAIL(build_const_string_expr(expr_factory, ObVarcharType,
      into_item->escaped_cht_.get_varchar(), into_item->escaped_cht_.get_collation_type(), escaped_cht_expr))) {
    LOG_WARN("fail to create string expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                            expr_factory, ObIntType, into_item->cs_type_, charset_expr))) {
    LOG_WARN("fail to create string expr", K(ret));
  } else if (OB_FAIL(new_expr->add_param_expr(field_expr))) {
    LOG_WARN("fail to add field_expr", K(ret));
  } else if (OB_FAIL(new_expr->add_param_expr(line_expr))) {
    LOG_WARN("fail to add line_expr", K(ret));
  } else if (OB_FAIL(new_expr->add_param_expr(closed_cht_expr))) {
    LOG_WARN("fail to add closed_cht_expr", K(ret));
  } else if (OB_FAIL(new_expr->add_param_expr(is_optional_expr))) {
    LOG_WARN("fail to add is_optional_expr", K(ret));
  } else if (OB_FAIL(new_expr->add_param_expr(escaped_cht_expr))) {
    LOG_WARN("fail to add escaped_cht_expr", K(ret));
  } else if (OB_FAIL(new_expr->add_param_expr(charset_expr))) {
    LOG_WARN("fail to add charset_expr", K(ret));
  } else {
    for (int i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(new_expr->add_param_expr(exprs.at(i)))) {
        LOG_WARN("fail to add param expr", K(i), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(new_expr->formalize(session_info))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else {
      to_outfile_expr = new_expr;
    }
  }
  return ret;
}

int ObRawExprUtils::build_pack_expr(ObRawExprFactory &expr_factory,
                                    const bool is_ps,
                                    const ObSQLSessionInfo *session_info,
                                    const ObIArray<common::ObField> *field_array,
                                    const ObIArray<ObRawExpr*> &input_exprs,
                                    ObRawExpr *&pack_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *out_expr = nullptr;
  ObConstRawExpr *encode_expr = nullptr;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_OUTPUT_PACK, out_expr))) {
    LOG_WARN("failed to create output_expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_INT, encode_expr))) {
    LOG_WARN("failed to create encode_type expr", K(ret));
  } else if (OB_ISNULL(out_expr) || OB_ISNULL(encode_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(out_expr), K(encode_expr), K(ret));
  } else {
    ObObj val;
    //0 for BINARY, 1 for TEXT
    val.set_int(is_ps ? 0 : 1);
    encode_expr->set_value(val);
    out_expr->set_extra(reinterpret_cast<uint64_t>(field_array));
    if (OB_FAIL(out_expr->add_param_expr(encode_expr))) {
      LOG_WARN("failed to add encode type", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < input_exprs.count(); ++i) {
        if (OB_FAIL(out_expr->add_param_expr(input_exprs.at(i)))) {
          LOG_WARN("failed to add param expr", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(out_expr->formalize(session_info))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else {
        pack_expr = out_expr;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::build_inner_row_cmp_expr(ObRawExprFactory &expr_factory,
                                             const ObSQLSessionInfo *session_info,
                                             ObRawExpr *cast_expr,
                                             ObRawExpr *input_expr,
                                             ObRawExpr *next_expr,
                                             const uint64_t ret_code,
                                             ObSysFunRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  bool is_implicit_cast_input = false;
  if (OB_ISNULL(session_info) || OB_ISNULL(cast_expr) ||
        OB_ISNULL(input_expr) || OB_ISNULL(next_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ret), K(session_info), K(cast_expr), K(input_expr),
                                    K(next_expr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_INNER_ROW_CMP_VALUE, new_expr))) {
    LOG_WARN("create inner row cmp value expr failed", K(ret));
  } else if (OB_ISNULL(new_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner row cmp value expr is null", K(ret));
  } else if (OB_FAIL(new_expr->add_param_expr(cast_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(new_expr->add_param_expr(input_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(new_expr->add_param_expr(next_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (T_FUN_SYS_CAST == input_expr->get_expr_type()
      && input_expr->has_flag(IS_OP_OPERAND_IMPLICIT_CAST)) {
    // The inner_row_cmp_expr will add one-sided cast to the second parameter. If the second
    // parameter itself is already a cast expr, the following formalize will report duplicate cast
    // error, so temporarily clear the flag here and restore it after formalize is completed.
    is_implicit_cast_input = true;
    input_expr->clear_flag(IS_OP_OPERAND_IMPLICIT_CAST);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_expr->formalize(session_info))) {
    LOG_WARN("fail to formalize expr", K(*new_expr), K(ret));
  } else {
    new_expr->set_func_name("INTERNAL_FUNCTION");
    new_expr->set_extra(ret_code);
    if (is_implicit_cast_input) {
      // restore input expr flag after formalize is finished.
      input_expr->add_flag(IS_OP_OPERAND_IMPLICIT_CAST);
    }
  }
  return ret;
}

// 这个函数只会在 pl 里被调到，会设置 ObRawExpr 的被调用模式，用于区分是在 pl 还是 sql 中被调用
int ObRawExprUtils::set_call_in_pl(ObRawExpr *&raw_expr)
{
  int ret = OB_SUCCESS;

  if (NULL == raw_expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else if (T_FUN_SYS_ORA_DECODE == raw_expr->get_expr_type() && lib::is_oracle_mode()) {
    ret = OB_ERR_FUNC_ONLY_IN_SQL;
    LOG_WARN("function DECODE may be used inside a SQL statement only");
    LOG_USER_ERROR(OB_ERR_FUNC_ONLY_IN_SQL, "DECODE");
  } else {
    int64_t N = raw_expr->get_param_count();
    raw_expr->set_is_called_in_sql(false);
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      OZ (set_call_in_pl(raw_expr->get_param_expr(i)));
    }
  }

  return ret;
}

int ObRawExprUtils::get_real_expr_without_generated_column(
    ObRawExpr *expr,
    ObRawExpr *&real_expr)
{
  int ret = OB_SUCCESS;
  real_expr = expr;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, expr must not be nullptr", K(ret));
  } else if (expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *column_ref_expr = static_cast<ObColumnRefRawExpr *>(expr);
    if (column_ref_expr->is_generated_column()) {
      real_expr = column_ref_expr->get_dependant_expr();
    }
  } else if (expr->has_flag(CNT_COLUMN)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(get_real_expr_without_generated_column(expr->get_param_expr(i), expr->get_param_expr(i)))) {
        LOG_WARN("fail to get real expr for child", K(ret));
      }
    }
  }
  return ret;
}

bool ObRawExprUtils::is_new_old_column_ref(const ParseNode *node)
{
  return T_COLUMN_REF == node->type_ && NULL == node->children_[0] && NULL != node->children_[1] &&
  (0 == STRNCASECMP(node->children_[1]->str_value_, "NEW", 3)
   || 0 == STRNCASECMP(node->children_[1]->str_value_, "OLD", 3))
  && NULL != node->children_[2];
}

int ObRawExprUtils::mock_obj_access_ref_node(common::ObIAllocator &allocator,
                                             ParseNode *&obj_access_node,
                                             const ParseNode *col_ref_node,
                                             const TgTimingEvent tg_timing_event)
{
  int ret = OB_SUCCESS;
  ParseNode *col_name_node = NULL;
  ParseNode *col_obj_node = NULL;
  ParseNode *tbl_name_node = NULL;
  ParseNode *full_obj_node = NULL;
  CK (T_COLUMN_REF == col_ref_node->type_);
  OX (col_name_node = new_terminal_node(&allocator, T_IDENT));
  CK (OB_NOT_NULL(col_name_node));
  OX (col_name_node->str_value_ = col_ref_node->children_[2]->str_value_);
  CK (OB_NOT_NULL(col_name_node->str_value_));
  OX (col_name_node->str_len_ = col_ref_node->children_[2]->str_len_);
  OX (tbl_name_node = new_terminal_node(&allocator, T_IDENT));
  CK (OB_NOT_NULL(tbl_name_node));
  OX (tbl_name_node->str_value_ = col_ref_node->children_[1]->str_value_);
  CK (OB_NOT_NULL(tbl_name_node->str_value_));
  OX (tbl_name_node->str_len_ = col_ref_node->children_[1]->str_len_);
  if (OB_SUCC(ret)) {
    if (0 == STRNCASECMP(tbl_name_node->str_value_, "NEW", 3)) {
      if (TgTimingEvent::TG_BEFORE_DELETE == tg_timing_event
          || TgTimingEvent::TG_AFTER_DELETE == tg_timing_event) {
      ret = OB_ERR_TRIGGER_NO_SUCH_ROW;
      LOG_WARN("there is no NEW row in on DELETE trigger");
      LOG_USER_ERROR(OB_ERR_TRIGGER_NO_SUCH_ROW, "NEW", "DELETE");
      }
    } else {
      if (TgTimingEvent::TG_BEFORE_INSERT == tg_timing_event
          || TgTimingEvent::TG_AFTER_INSERT == tg_timing_event) {
        ret = OB_ERR_TRIGGER_NO_SUCH_ROW;
        LOG_WARN("there is no OLD row in on INSERT trigger");
        LOG_USER_ERROR(OB_ERR_TRIGGER_NO_SUCH_ROW, "OLD", "INSERT");
      }
    }
  }
  OX (col_obj_node = new_non_terminal_node(&allocator, T_OBJ_ACCESS_REF,
                                           2, col_name_node, NULL));
  OX (full_obj_node = new_non_terminal_node(&allocator, T_OBJ_ACCESS_REF,
                                            2, tbl_name_node, col_obj_node));
  OX (obj_access_node = full_obj_node);
  return ret;
}

const ObRawExpr *ObRawExprUtils::skip_implicit_cast(const ObRawExpr *e)
{
  const ObRawExpr *res = e;
  while (res != NULL
         && T_FUN_SYS_CAST == res->get_expr_type()
         && res->has_flag(IS_OP_OPERAND_IMPLICIT_CAST)) {
    res = res->get_param_expr(0);
  }
  return res;
}

int ObRawExprUtils::extract_params(ObRawExpr *expr, ObIArray<ObRawExpr*> &params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr passed in is NULL", K(ret));
  } else if (expr->is_param_expr()) {
    if (OB_FAIL(params.push_back(expr))) {
      LOG_WARN("failed to push back param", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_params(expr->get_param_expr(i), params)))) {
        LOG_WARN("failed to extract params", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_params(common::ObIArray<ObRawExpr*> &exprs,
                                    common::ObIArray<ObRawExpr*> &params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(extract_params(exprs.at(i), params))) {
      LOG_WARN("failed to extract params", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::is_contain_params(const common::ObIArray<ObRawExpr*> &exprs,
                                      bool &is_contain)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && !is_contain && i < exprs.count(); ++i) {
    if (OB_FAIL(is_contain_params(exprs.at(i), is_contain))) {
     }
  }
  return ret;
}

int ObRawExprUtils::add_calc_tablet_id_on_calc_rowid_expr(const ObDMLStmt *dml_stmt,
                                                          ObRawExprFactory &expr_factory,
                                                          const ObSQLSessionInfo &session_info,
                                                          const ObTableSchema &table_schema,
                                                          const uint64_t logical_table_id,
                                                          ObSysFunRawExpr *&calc_rowid_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dml_stmt) || OB_ISNULL(calc_rowid_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(dml_stmt),K(calc_rowid_expr));
  } else if (table_schema.is_heap_table()) {
    // add calc_tablet_id param such that calc_urowid(version, xxx) becomes
    // calc_urowid(version, xxx, calc_tablet_id) since we need part id to generate
    // physical rowid for heap organized table.
    ObRawExprCopier copier(expr_factory);
    ObSEArray<ObRawExpr *, 4> column_exprs;
    const ObRawExpr *part_expr = dml_stmt->get_part_expr(logical_table_id, table_schema.get_table_id());
    const ObRawExpr *subpart_expr = dml_stmt->get_subpart_expr(logical_table_id, table_schema.get_table_id());
    ObRawExpr *calc_part_id_expr = nullptr;
    schema::ObPartitionLevel part_level = table_schema.get_part_level();
    ObRawExpr *new_part_expr = NULL;
    ObRawExpr *new_subpart_expr = NULL;
    //why we deep copy part/subpart expr ?
    //because rowid in trgger will modify relation param expr, if we don't deep copy, this will
    //influence origin part/subpart expr and introduce other problems.
    if (OB_FAIL(dml_stmt->get_column_exprs(logical_table_id, column_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(copier.add_skipped_expr(column_exprs))) {
      LOG_WARN("failed to add skipped expr", K(ret));
    } else if (OB_FAIL(copier.copy(part_expr, new_part_expr))) {
      LOG_WARN("fail to copy part expr", K(ret));
    } else if (OB_FAIL(copier.copy(subpart_expr, new_subpart_expr))) {
      LOG_WARN("fail to copy subpart expr", K(ret));
    } else if (OB_FAIL(build_calc_tablet_id_expr(expr_factory,
                                                 session_info,
                                                 table_schema.get_table_id(),
                                                 part_level,
                                                 new_part_expr,
                                                 new_subpart_expr,
                                                 calc_part_id_expr))) {
      LOG_WARN("fail to build table location expr", K(ret));
    } else if (OB_ISNULL(calc_part_id_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (OB_FAIL(calc_rowid_expr->add_param_expr(calc_part_id_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRawExprUtils::add_calc_partition_id_on_calc_rowid_expr(const ObDMLStmt *dml_stmt,
                                                             ObRawExprFactory &expr_factory,
                                                             const ObSQLSessionInfo &session_info,
                                                             const ObTableSchema &table_schema,
                                                             const uint64_t logical_table_id,
                                                             ObSysFunRawExpr *&calc_rowid_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dml_stmt) || OB_ISNULL(calc_rowid_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(dml_stmt),K(calc_rowid_expr));
  } else if (table_schema.is_external_table()) {
    ObRawExprCopier copier(expr_factory);
    ObSEArray<ObRawExpr *, 4> column_exprs;
    const ObRawExpr *part_expr = dml_stmt->get_part_expr(logical_table_id, table_schema.get_table_id());
    const ObRawExpr *subpart_expr = dml_stmt->get_subpart_expr(logical_table_id, table_schema.get_table_id());
    ObRawExpr *calc_part_id_expr = nullptr;
    schema::ObPartitionLevel part_level = table_schema.get_part_level();
    ObRawExpr *new_part_expr = NULL;
    ObRawExpr *new_subpart_expr = NULL;
    if (OB_FAIL(dml_stmt->get_column_exprs(logical_table_id, column_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(copier.add_skipped_expr(column_exprs))) {
      LOG_WARN("failed to add skipped expr", K(ret));
    } else if (OB_FAIL(copier.copy(part_expr, new_part_expr))) {
      LOG_WARN("fail to copy part expr", K(ret));
    } else if (OB_FAIL(copier.copy(subpart_expr, new_subpart_expr))) {
      LOG_WARN("fail to copy subpart expr", K(ret));
    } else if (OB_FAIL(build_calc_part_id_expr(expr_factory,
                                               session_info,
                                               table_schema.get_table_id(),
                                               part_level,
                                               new_part_expr,
                                               new_subpart_expr,
                                               calc_part_id_expr))) {
      LOG_WARN("fail to build table location expr", K(ret));
    } else if (OB_ISNULL(calc_part_id_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (OB_FAIL(calc_rowid_expr->add_param_expr(calc_part_id_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::build_shadow_pk_expr(uint64_t table_id,
                                         uint64_t column_id,
                                         const ObDMLStmt &dml_stmt,
                                         ObRawExprFactory &expr_factory,
                                         const ObSQLSessionInfo &session_info,
                                         const ObTableSchema &index_schema,
                                         ObColumnRefRawExpr *&spk_expr)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *spk_schema = NULL;
  const ObRowkeyInfo &rowkey_info = index_schema.get_rowkey_info();
  ObOpRawExpr *spk_project_expr = NULL;
  if (OB_UNLIKELY(!is_shadow_column(column_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_id is invalid", K(column_id));
  } else if (OB_ISNULL(spk_schema = index_schema.get_column_schema(column_id))) {
    ret = OB_ERR_COLUMN_NOT_FOUND;
    LOG_WARN("column not found", K(column_id), K(index_schema));
  } else {
    ObString index_name;
    if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *spk_schema, spk_expr))) {
      LOG_WARN("create column ref raw expr failed", K(ret));
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_SHADOW_UK_PROJECT, spk_project_expr))) {
      LOG_WARN("create shadow unique key projector failed", K(ret));
    } else if (OB_FAIL(index_schema.get_index_name(index_name))) {
      LOG_WARN("get index name from index schema failed", K(ret));
    } else {
      spk_expr->set_table_name(index_name);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
    uint64_t rowkey_column_id = OB_INVALID_ID;
    if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
      LOG_WARN("get rowkey column id failed", K(ret));
    } else if (!is_shadow_column(rowkey_column_id)) {
      const ColumnItem *col = dml_stmt.get_column_item_by_base_id(table_id, rowkey_column_id);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find col is null", K(table_id), K(rowkey_column_id));
      } else if (OB_FAIL(spk_project_expr->add_param_expr(col->expr_))) {
        LOG_WARN("add param expr to shadow unique key project expr failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t real_column_id = column_id - OB_MIN_SHADOW_COLUMN_ID;
    const ColumnItem *col = dml_stmt.get_column_item_by_base_id(table_id, real_column_id);
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("find col is null", K(table_id), K(real_column_id), K(dml_stmt.get_column_items()));
    } else if (OB_FAIL(spk_project_expr->add_param_expr(col->expr_))) {
      LOG_WARN("add param expr to spk project expr failed", K(ret));
    } else if (OB_FAIL(spk_project_expr->formalize(&session_info))) {
      LOG_WARN("formalize shadow unique key failed", K(ret));
    } else {
      spk_expr->set_dependant_expr(spk_project_expr);
      //将shadow unique rowkey标记为生成列，它依赖T_OP_SHADOW_UK_PROJECT表达式生成
      spk_expr->set_column_flags(VIRTUAL_GENERATED_COLUMN_FLAG);
    }
  }
  return ret;
}

int ObRawExprUtils::get_col_ref_expr_recursively(ObRawExpr *expr,
                                                 ObColumnRefRawExpr *&column_expr)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr passed in is NULL", K(ret));
  } else if (expr->is_column_ref_expr()) {
    column_expr = static_cast<ObColumnRefRawExpr*>(expr);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *sub_expr = expr->get_param_expr(i);
      if (OB_ISNULL(sub_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub_expr should not be null", K(ret));
      } else if (OB_FAIL(get_col_ref_expr_recursively(sub_expr, column_expr))) {
        LOG_WARN("failed to get col ref expr recursively", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::is_contain_params(const ObRawExpr *expr, bool &is_contain)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr passed in is NULL", K(ret));
  } else if (expr->is_param_expr()) {
    is_contain = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_contain && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_contain_params(expr->get_param_expr(i), is_contain)))) {
        LOG_WARN("failed to extract params", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::check_contain_case_when_exprs(const ObRawExpr *raw_expr, bool &contain)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr passed in is NULL", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (raw_expr->get_expr_type() == T_OP_CASE) {
    contain = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !contain && i < raw_expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(check_contain_case_when_exprs(raw_expr->get_param_expr(i), contain)))) {
        LOG_WARN("failed to replace_ref_column", KPC(raw_expr), K(i));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::check_contain_lock_exprs(const ObRawExpr *raw_expr, bool &contain)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr passed in is NULL", K(ret));
  } else if (T_FUN_SYS_GET_LOCK == raw_expr->get_expr_type() ||
             T_FUN_SYS_RELEASE_LOCK == raw_expr->get_expr_type() ||
             T_FUN_SYS_RELEASE_ALL_LOCKS == raw_expr->get_expr_type()) {
    contain = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !contain && i < raw_expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(check_contain_lock_exprs(raw_expr->get_param_expr(i), contain)))) {
        LOG_WARN("failed to replace_ref_column", KPC(raw_expr), K(i));
      }
    }
  }
  return ret;
}

bool ObRawExprUtils::decimal_int_need_cast(const ObAccuracy &src_acc, const ObAccuracy &dst_acc)
{
  return decimal_int_need_cast(src_acc.get_precision(), src_acc.get_scale(),
                               dst_acc.get_precision(), dst_acc.get_scale());
}

bool ObRawExprUtils::decimal_int_need_cast(const ObPrecision src_p, const ObScale src_s,
                                           const ObPrecision dst_p, const ObScale dst_s)
{
  bool bret = false;
  // if the scale or the integer width of precision is different, cast is required.
  if (src_s < 0 || dst_p < 0) {
    // do nothing
  } else if (src_s != dst_s) {
    bret = true;
  } else if (get_decimalint_type(src_p) != get_decimalint_type(dst_p)) {
    bret = true;
  }
  return bret;
}

int ObRawExprUtils::transform_udt_column_value_expr(ObRawExprFactory &expr_factory, ObRawExpr *old_expr, ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  // to do: add SYS_MAKEXMLBinary
  ObSysFunRawExpr *make_xml_expr = NULL;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_PRIV_MAKE_XML_BINARY, make_xml_expr))) {
    LOG_WARN("failed to create fun make xml binary expr", K(ret));
  } else if (OB_ISNULL(make_xml_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml expr is null", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_INT, c_expr))) {
    LOG_WARN("create dest type expr failed", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null");
  } else {
    ObObj val;
    val.set_int(0);
    c_expr->set_value(val);
    c_expr->set_param(val);
    if (OB_FAIL(make_xml_expr->set_param_exprs(c_expr, old_expr))) {
      LOG_WARN("set param expr fail", K(ret));
    } else {
      make_xml_expr->set_func_name(ObString::make_string("_make_xml_binary"));
      new_expr = make_xml_expr;
    }
  }
  return ret;
}

int ObRawExprUtils::check_is_valid_generated_col(ObRawExpr *expr, ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  ObList<ObRawExpr *, ObIAllocator> expr_queue(allocator);
  ObRawExpr *cur_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is invalid null", K(ret));
  } else if (OB_FAIL(expr_queue.push_back(expr))) {
    LOG_WARN("fail to push back expr", K(ret));
  } else {
    while (OB_SUCC(ret) && expr_queue.size() > 0) {
      if (OB_FAIL(expr_queue.pop_front(cur_expr))) {
        LOG_WARN("fail to pop expr", K(ret));
      } else if (cur_expr->is_sys_func_expr()) {
        ObSysFunRawExpr *sys_expr = static_cast<ObSysFunRawExpr *>(cur_expr);
        const ObExprOperator *op = sys_expr->get_op();
        bool is_valid = false;
        if (OB_ISNULL(op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("operator is unexpected null", K(ret));
        } else if (OB_FAIL(op->is_valid_for_generated_column(sys_expr, sys_expr->get_param_exprs(), is_valid))) {
          LOG_WARN("fail to check if op is valid for generated column", K(ret));
        } else if (!is_valid) {
          ret = OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION;
          LOG_WARN("sysfunc in expr is not valid for generated column", K(ret), K(*cur_expr));
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_expr->get_param_count(); ++i) {
          if (OB_FAIL(expr_queue.push_back(cur_expr->get_param_expr(i)))) {
            LOG_WARN("fail to push back expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

bool ObRawExprUtils::is_column_ref_skip_implicit_cast(const ObRawExpr *expr)
{
  bool bret = false;
  if (OB_NOT_NULL(expr)) {
    if (expr->is_column_ref_expr()) {
      bret = true;
    } else if (T_FUN_SYS_CAST == expr->get_expr_type() &&
               expr->has_flag(IS_INNER_ADDED_EXPR)) {
      bret = is_column_ref_skip_implicit_cast(expr->get_param_expr(0));
    }
  }
  return bret;
}

int ObRawExprUtils::build_default_match_filter(ObRawExprFactory &expr_factory,
                                               ObRawExpr *relevance_expr,
                                               ObRawExpr *threshold,
                                               ObOpRawExpr *&match_filter,
                                               const ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *greater_than = nullptr;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_GT, greater_than))) {
    LOG_WARN("create cmp op failed", K(ret));
  } else if (OB_ISNULL(greater_than)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer to created greater than expr", K(ret));
  } else if (OB_FAIL(greater_than->set_param_exprs(relevance_expr, threshold))) {
    LOG_WARN("failed to set param exprs", K(ret));
  } else if (OB_FAIL(greater_than->formalize(session))) {
    LOG_WARN("failed to formalize greater than expr", K(ret));
  } else {
    match_filter = greater_than;
  }
  return ret;
}

int ObRawExprUtils::build_bm25_expr(ObRawExprFactory &expr_factory,
                                    ObRawExpr *related_doc_cnt,
                                    ObRawExpr *related_token_cnt,
                                    ObRawExpr *total_doc_cnt,
                                    ObRawExpr *doc_token_cnt,
                                    ObOpRawExpr *&bm25,
                                    const ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *approx_avg_token_cnt = nullptr;
  ObOpRawExpr *bm25_expr = nullptr;
  // TODO: @Salton implement approx avg token cnt storage in fulltext index and rm this mock
  constexpr double mock_approx_avg_cnt = 10;
  if (OB_FAIL(build_const_double_expr(expr_factory, ObDoubleType, mock_approx_avg_cnt, approx_avg_token_cnt))) {
    LOG_WARN("create approx average token count failed", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_BM25, bm25_expr))) {
    LOG_WARN("create bm25 func failed", K(ret));
  } else if (OB_ISNULL(bm25_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer to created bm25 related exprs", K(ret), KP(bm25));
  } else {
    OZ(approx_avg_token_cnt->formalize(session));
    OZ(bm25_expr->add_param_expr(related_doc_cnt));
    OZ(bm25_expr->add_param_expr(total_doc_cnt));
    OZ(bm25_expr->add_param_expr(doc_token_cnt));
    OZ(bm25_expr->add_param_expr(approx_avg_token_cnt));
    OZ(bm25_expr->add_param_expr(related_token_cnt));
    OZ(bm25_expr->formalize(session));
    OX(bm25 = bm25_expr);
  }
  return ret;
}

int ObRawExprUtils::extract_match_against_filters(const ObIArray<ObRawExpr *> &filters,
                                                  ObIArray<ObRawExpr *> &other_filters,
                                                  ObIArray<ObRawExpr *> &match_filters)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    if (OB_ISNULL(expr = filters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (expr->has_flag(CNT_MATCH_EXPR)) {
      if (OB_FAIL(add_var_to_array_no_dup(match_filters, expr))) {
        LOG_WARN("failed to push text ir filters", K(ret));
      }
    } else if (OB_FAIL(add_var_to_array_no_dup(other_filters, expr))) {
      LOG_WARN("failed to push other filters", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::build_dummy_count_expr(ObRawExprFactory &expr_factory,
                                           const ObSQLSessionInfo *session_info,
                                           ObAggFunRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *one_expr = NULL;
  ObAggFunRawExpr *count_expr = NULL;
  if (OB_FAIL(build_const_int_expr(expr_factory, ObInt32Type, static_cast<int32_t>(1), one_expr))) {
    LOG_WARN("failed build const int expr for default expr", K(ret));
  } else if (OB_ISNULL(one_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is unexpected null", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_COUNT, count_expr))) {
    LOG_WARN("fail to create const raw expr", K(ret));
  } else if (OB_FAIL(count_expr->add_real_param_expr(one_expr))) {
    LOG_WARN("fail to push back", K(ret));
  } else if (OB_FAIL(count_expr->formalize(session_info))) {
    LOG_WARN("failed to extract expr info", K(ret));
  } else {
    expr = count_expr;
  }
  return ret;
}

int ObRawExprUtils::extract_local_vars_for_gencol(ObRawExpr *expr,
                                         const ObSQLMode sql_mode,
                                         ObColumnSchemaV2 &gen_col)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> dep_columns;
  ObSEArray<const share::schema::ObSessionSysVar *, 4> var_array;
  if (OB_FAIL(expr->extract_local_session_vars_recursively(var_array))) {
    LOG_WARN("extract sysvars failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, dep_columns))) {
    LOG_WARN("extract column exprs failed", K(ret), K(expr));
  } else {
    bool has_char_dep_col = false;
    ObSessionSysVar local_sql_mode;
    for (int64_t i = 0; !has_char_dep_col && i < dep_columns.count(); ++i) {
      if (ObCharType == dep_columns.at(i)->get_result_type().get_type()
          || ObNCharType == dep_columns.at(i)->get_result_type().get_type()) {
        has_char_dep_col = true;
      }
    }
    if (has_char_dep_col) {
      //add sql mode
      bool is_sql_mode_solidified = false;
      for (int64_t i = 0; OB_SUCC(ret) && !is_sql_mode_solidified && i < var_array.count(); ++i) {
        if (OB_ISNULL(var_array.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (share::SYS_VAR_SQL_MODE == var_array.at(i)->type_) {
          is_sql_mode_solidified = true;
        }
      }
      if (OB_SUCC(ret) && !is_sql_mode_solidified) {
        local_sql_mode.type_ = SYS_VAR_SQL_MODE;
        local_sql_mode.val_.set_uint64(sql_mode);
        if (OB_FAIL(var_array.push_back(&local_sql_mode))) {
          LOG_WARN("push back local var failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(gen_col.get_local_session_var().set_local_vars(var_array))) {
        LOG_WARN("set local vars failed", K(ret), KPC(expr));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::ora_cmp_integer(const ObConstRawExpr &const_expr, const int64_t v, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (const_expr.get_result_type().is_number()) {
    cmp_ret = const_expr.get_value().get_number().compare(v);
  } else if (const_expr.get_result_type().is_decimal_int()) {
    const ObObj &obj = const_expr.get_value();
    ObDatum d;
    d.ptr_ = reinterpret_cast<const char *>(&v);
    d.pack_ = sizeof(int64_t);
    ret = wide::compare(obj, d, cmp_ret);
    if (OB_FAIL(ret)) {
      LOG_WARN("decimal int comparing failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ora integer", K(ret), K(const_expr));
  }
  return ret;
}

int ObRawExprUtils::check_contain_op_row_expr(const ObRawExpr *raw_expr, bool &contain)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL pointer", K(ret));
  } else if (raw_expr->get_expr_type() == T_OP_ROW && !raw_expr->is_const_expr()) {
    contain = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !contain && i < raw_expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(check_contain_op_row_expr(raw_expr->get_param_expr(i), contain)))) {
        LOG_WARN("failed to replace_ref_column", KPC(raw_expr), K(i));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::copy_and_formalize(ObRawExpr *&expr,
                                       ObRawExprCopier *copier,
                                       ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObRawExpr *new_expr = NULL;
  if (OB_ISNULL(copier) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(copier), K(session_info));
  } else if (OB_FAIL(copier->copy_on_replace(expr, new_expr))) {
    LOG_WARN("failed to copy expr");
  } else if (OB_ISNULL(new_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (new_expr == expr) {
    // do nothing
  } else if (OB_FAIL(new_expr->formalize(session_info))) {
    LOG_WARN("failed to formalize expr", K(ret));
  } else {
    expr = new_expr;
  }
  return ret;
}

int ObRawExprUtils::copy_and_formalize(const ObIArray<ObRawExpr *> &exprs,
                                       ObIArray<ObRawExpr *> &new_exprs,
                                       ObRawExprCopier *copier,
                                       ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(copier) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(copier), K(session_info));
  } else if (OB_FAIL(copier->copy_on_replace(exprs, new_exprs))) {
    LOG_WARN("failed to copy expr");
  } else if (OB_UNLIKELY(exprs.count() != new_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr number mismatch", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (exprs.at(i) == new_exprs.at(i)) {
      // do nothing
    } else if (OB_FAIL(new_exprs.at(i)->formalize(session_info))) {
      LOG_WARN("failed to formalize expr", K(ret));
    }
  }
  return ret;
}

}
}
