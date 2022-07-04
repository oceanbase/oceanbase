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
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql {
#define RESOLVE_ORALCE_IMLICIT_CAST_WARN_OR_ERR(warn_code, err_code)    \
  if ((session_info->get_sql_mode() & SMO_ERROR_ON_RESOLVE_CAST) > 0) { \
    ret = err_code;                                                     \
  } else {                                                              \
    LOG_USER_WARN(warn_code);                                           \
  }

inline int ObRawExprUtils::resolve_op_expr_add_implicit_cast(ObRawExprFactory& expr_factory,
    const ObSQLSessionInfo* session_info, ObRawExpr* src_expr, const ObExprResType& dst_type,
    ObSysFunRawExpr*& func_expr)
{
  int ret = OB_SUCCESS;
  if (dst_type.is_varying_len_char_type() && !ob_is_rowid_tc(src_expr->get_result_type().get_type())) {
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
    OZ(ObRawExprUtils::create_cast_expr(expr_factory, src_expr, dst_type, func_expr, session_info));
    CK(OB_NOT_NULL(func_expr));
  }

  return ret;
}

int ObRawExprUtils::resolve_op_expr_implicit_cast(ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info,
    ObItemType op_type, ObRawExpr*& sub_expr1, ObRawExpr*& sub_expr2)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_expr1) || OB_ISNULL(sub_expr2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(sub_expr1), K(sub_expr2));
  } else {
    ObSysFunRawExpr* new_expr = NULL;
    ObSysFunRawExpr* new_expr2 = NULL;
    ObObjType r_type1 = ObMaxType;
    ObObjType r_type2 = ObMaxType;
    ObObjType r_type3 = ObMaxType;

    r_type1 = sub_expr1->get_result_type().get_type();
    r_type2 = sub_expr2->get_result_type().get_type();
    // formalize to get expr's type
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
    if (OB_SUCC(ret) && share::is_oracle_mode()) {
      // compare lob type date in oracle mode not allowed.
      if (OB_UNLIKELY(ObLongTextType == r_type1 || ObLongTextType == r_type2) && IS_COMPARISON_OP(op_type)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("can't calculate with lob type in oracle mode", K(ret), K(r_type1), K(r_type2), K(op_type));
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
        switch (op_type) {
          case T_OP_CNN:
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
          // todo timestamp with tz/ltz...
          default:
            if (OB_UNLIKELY(ObLongTextType == r_type1 || ObLongTextType == r_type2)) {
              /* lob type return warning */
            } else {
              middle_type = ObNumberType;
              dir = OB_OBJ_IMPLICIT_CAST_DIRECTION_FOR_ORACLE[type1][type2];
              if ((ImplicitCastDirection::IC_B_TO_C == dir && ob_is_nchar(r_type1)) ||
                  (ImplicitCastDirection::IC_A_TO_C == dir && ob_is_nchar(r_type2))) {
                // nchar and varchar2, convert varchar to nvarchar2
                r_type3 = ObNVarchar2Type;
              }
            }
            break;
        }

        // when cast raw to char implicitly, it is cast to varchar actually.
        if (ImplicitCastDirection::IC_A_TO_B == dir && ob_is_raw(r_type1) && ObCharType == r_type2) {
          r_type3 = ObVarcharType;
          dir = ImplicitCastDirection::IC_A_TO_C;
        }

        if (ob_is_interval_tc(r_type1) || ob_is_interval_tc(r_type2)) {
          dir = ImplicitCastDirection::IC_NO_CAST;
        }
        LOG_DEBUG(
            "Molly ORACLE IMPLICIT CAST DIR", K(dir), K(type1), K(type2), K(r_type3), K(*sub_expr1), K(*sub_expr2));
        ObExprResType dest_type;
        switch (dir) {
          case ImplicitCastDirection::IC_NO_CAST: {
            // int is number(38) in oracle, when number div number, the result can be decimal.
            // So we add cast int as number b4 do div
            if (tc1 == ObIntTC && tc2 == ObIntTC && op_type == T_OP_DIV) {
              ObAccuracy acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[1][ObNumberType];
              dest_type.set_precision(acc.get_precision());
              dest_type.set_scale(acc.get_scale());
              dest_type.set_collation_level(CS_LEVEL_NUMERIC);
              dest_type.set_type(ObNumberType);
              if (OB_FAIL(
                      resolve_op_expr_add_implicit_cast(expr_factory, session_info, sub_expr1, dest_type, new_expr))) {
                LOG_WARN("create cast expr for implicit failed", K(ret));
              } else if (OB_FAIL(resolve_op_expr_add_implicit_cast(
                             expr_factory, session_info, sub_expr2, dest_type, new_expr2))) {
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
            ObAccuracy acc = (ObNumberType == r_type2 ? ObAccuracy::DDL_DEFAULT_ACCURACY2[1][r_type2]
                                                      : ObAccuracy::MAX_ACCURACY2[1][r_type2]);
            dest_type.set_precision(acc.get_precision());
            dest_type.set_scale(acc.get_scale());
            dest_type.set_type(r_type2);
            if (OB_FAIL(
                    resolve_op_expr_add_implicit_cast(expr_factory, session_info, sub_expr1, dest_type, new_expr))) {
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
          case ImplicitCastDirection::IC_B_TO_A: {
            ObAccuracy acc = (ObNumberType == r_type1 ? ObAccuracy::DDL_DEFAULT_ACCURACY2[1][r_type1]
                                                      : ObAccuracy::MAX_ACCURACY2[1][r_type1]);
            dest_type.set_precision(acc.get_precision());
            dest_type.set_scale(acc.get_scale());
            // dest_type.set_collation_level(CS_LEVEL_NUMERIC);
            dest_type.set_type(r_type1);
            if (OB_FAIL(
                    resolve_op_expr_add_implicit_cast(expr_factory, session_info, sub_expr2, dest_type, new_expr))) {
              LOG_WARN("create cast expr for implicit failed", K(ret));
            } else if (OB_ISNULL(new_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpect null expr", K(ret));
            } else {
              sub_expr2 = new_expr;
            }
            break;
          }
          case ImplicitCastDirection::IC_A_TO_C: {
            ObAccuracy acc = (ObNumberType == r_type3 ? ObAccuracy::DDL_DEFAULT_ACCURACY2[1][r_type3]
                                                      : ObAccuracy::MAX_ACCURACY2[1][r_type3]);
            dest_type.set_precision(acc.get_precision());
            dest_type.set_scale(acc.get_scale());
            dest_type.set_type(r_type3);
            if (OB_FAIL(
                    resolve_op_expr_add_implicit_cast(expr_factory, session_info, sub_expr1, dest_type, new_expr))) {
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
            ObAccuracy acc = (ObNumberType == r_type3 ? ObAccuracy::DDL_DEFAULT_ACCURACY2[1][r_type3]
                                                      : ObAccuracy::MAX_ACCURACY2[1][r_type3]);
            dest_type.set_precision(acc.get_precision());
            dest_type.set_scale(acc.get_scale());
            dest_type.set_type(r_type3);
            if (OB_FAIL(
                    resolve_op_expr_add_implicit_cast(expr_factory, session_info, sub_expr2, dest_type, new_expr))) {
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
            ObAccuracy acc = (ObNumberType == middle_type ? ObAccuracy::DDL_DEFAULT_ACCURACY2[1][middle_type]
                                                          : ObAccuracy::MAX_ACCURACY2[1][middle_type]);
            dest_type.set_precision(acc.get_precision());
            dest_type.set_scale(acc.get_scale());
            dest_type.set_collation_level(CS_LEVEL_NUMERIC);
            dest_type.set_type(middle_type);
            ObObjTypeClass middle_tc = OBJ_O_TYPE_TO_CLASS[middle_type];

            // optimization: only need to cast string type to number type
            if (middle_type == ObNumberType) {
              if (ObStringTC == tc1 && (ObIntTC == tc2 || ObFloatTC == tc2 || ObDoubleTC == tc2 || ObNumberTC == tc2)) {
                if (OB_FAIL(
                        ObRawExprUtils::create_cast_expr(expr_factory, sub_expr1, dest_type, new_expr, session_info))) {
                  LOG_WARN("create cast expr for implicit failed", K(ret));
                } else if (OB_ISNULL(new_expr)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpect null expr", K(ret));
                } else if (OB_FAIL(new_expr->add_flag(IS_INNER_ADDED_EXPR))) {
                  LOG_WARN("failed to add flag", K(ret));
                } else {
                  sub_expr1 = new_expr;
                }
              } else if (ObStringTC == tc2 &&
                         (ObIntTC == tc1 || ObFloatTC == tc1 || ObDoubleTC == tc1 || ObNumberTC == tc1)) {
                if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                        expr_factory, sub_expr2, dest_type, new_expr2, session_info))) {
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
                if (OB_FAIL(resolve_op_expr_add_implicit_cast(
                        expr_factory, session_info, sub_expr1, dest_type, new_expr))) {
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
                if (OB_FAIL(resolve_op_expr_add_implicit_cast(
                        expr_factory, session_info, sub_expr2, dest_type, new_expr2))) {
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
          case ImplicitCastDirection::IC_NOT_SUPPORT: {
            // ObString type1_str = ObString(ob_obj_type_str(r_type1));
            // ObString type2_str = ObString(ob_obj_type_str(r_type2));
            // return warning or error when can't do implicite datatype convert
            bool is_error =
                (session_info != NULL && ((session_info->get_sql_mode() & SMO_ERROR_ON_RESOLVE_CAST) > 0)) ||
                (sub_expr1->get_result_type().is_blob() || sub_expr2->get_result_type().is_blob() ||
                    sub_expr1->get_result_type().is_blob_locator() || sub_expr2->get_result_type().is_blob_locator());
            if (is_error) {
              ret = OB_ERR_INVALID_TYPE_FOR_OP;
              LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(r_type1), ob_obj_type_str(r_type2));
            } else {
              LOG_USER_WARN(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(r_type1), ob_obj_type_str(r_type2));
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

int ObRawExprUtils::resolve_op_expr_for_oracle_implicit_cast(
    ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info, ObOpRawExpr*& b_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* sub_expr1 = NULL;
  ObRawExpr* sub_expr2 = NULL;
  sub_expr1 = b_expr->get_param_expr(0);
  sub_expr2 = b_expr->get_param_expr(1);

  if (OB_ISNULL(sub_expr1) || OB_ISNULL(sub_expr2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(sub_expr1), K(sub_expr2));
  } else if (T_OP_ROW == sub_expr1->get_expr_type() || T_OP_ROW == sub_expr2->get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get T_OP_ROW expr", K(ret), K(*sub_expr1), K(*sub_expr2));
  } else {
    if (!sub_expr1->is_query_ref_expr() && sub_expr2->is_query_ref_expr()) {
      // t1.c1 =any (select c1 from t2)
      ObQueryRefRawExpr* query_ref_expr = static_cast<ObQueryRefRawExpr*>(sub_expr2);
      ObSelectStmt* query_stmt = query_ref_expr->get_ref_stmt();
      ObRawExpr* select_expr = NULL;
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
      } else if (OB_FAIL(resolve_op_expr_implicit_cast(
                     expr_factory, session_info, b_expr->get_expr_type(), sub_expr1, select_expr))) {
        LOG_WARN("failed to resolve_op_expr_implicit_cast", K(ret));
      } else if (OB_ISNULL(select_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else {
        query_stmt->get_select_item(0).expr_ = select_expr;
        ObIArray<ObExprResType>& column_types = query_ref_expr->get_column_types();
        column_types.reset();
        if (OB_FAIL(column_types.push_back(select_expr->get_result_type()))) {
          LOG_WARN("add column type failed", K(ret));
        }
      }
    } else if (sub_expr1->is_query_ref_expr() && sub_expr2->is_query_ref_expr()) {
      // (select c1 from t2 where t2.c1=1) =any (select c1 from t2)
      ObQueryRefRawExpr* query_ref_expr1 = static_cast<ObQueryRefRawExpr*>(sub_expr1);
      ObQueryRefRawExpr* query_ref_expr2 = static_cast<ObQueryRefRawExpr*>(sub_expr2);
      ObSelectStmt* query_stmt1 = query_ref_expr1->get_ref_stmt();
      ObSelectStmt* query_stmt2 = query_ref_expr2->get_ref_stmt();
      ObRawExpr* select_expr1 = NULL;
      ObRawExpr* select_expr2 = NULL;
      if (OB_ISNULL(query_stmt1) || OB_ISNULL(query_stmt2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(query_stmt1), K(query_stmt2));
      } else if (query_stmt1->get_select_item_size() != query_stmt2->get_select_item_size()) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_WARN("query expr should same output column",
            K(query_ref_expr1->get_output_column()),
            K(query_ref_expr2->get_output_column()));
      } else {
        ObIArray<ObExprResType>& column_types1 = query_ref_expr1->get_column_types();
        column_types1.reset();
        ObIArray<ObExprResType>& column_types2 = query_ref_expr2->get_column_types();
        column_types2.reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < query_stmt1->get_select_item_size(); ++i) {
          if (OB_ISNULL(select_expr1 = query_stmt1->get_select_item(i).expr_) ||
              OB_ISNULL(select_expr2 = query_stmt2->get_select_item(i).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(select_expr1), K(select_expr2));
          } else if (OB_FAIL(resolve_op_expr_implicit_cast(
                         expr_factory, session_info, b_expr->get_expr_type(), select_expr1, select_expr2))) {
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
      if (OB_FAIL(resolve_op_expr_implicit_cast(
              expr_factory, session_info, b_expr->get_expr_type(), sub_expr1, sub_expr2))) {
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

int ObRawExprUtils::resolve_op_exprs_for_oracle_implicit_cast(
    ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info, ObIArray<ObOpRawExpr*>& op_exprs)
{
  int ret = OB_SUCCESS;
  if (session_info == NULL) {
    LOG_WARN("can't get compatibility mode from session_info", K(session_info));
  } else {
    ObOpRawExpr* b_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < op_exprs.count(); i++) {
      b_expr = op_exprs.at(i);
      if (OB_ISNULL(b_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (b_expr->get_param_count() != 2) {
        // TODO Molly case when
        LOG_WARN("IMPLICIT UNEXPECTED BEXPR", K(*b_expr));
      } else if (OB_FAIL(resolve_op_expr_for_oracle_implicit_cast(expr_factory, session_info, b_expr))) {
        LOG_WARN("IMPLICIT UNEXPECTED BEXPR", K(*b_expr));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::function_alias(ObRawExprFactory& expr_factory, ObSysFunRawExpr*& expr)
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
    ObConstRawExpr* from_base = NULL;
    ObConstRawExpr* to_base = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, decimal, from_base))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, binary, to_base))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_UNLIKELY(1 != expr->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param count", K(expr->get_param_count()));
    } else if (OB_FAIL(expr->add_param_expr(from_base))) {
      LOG_WARN("fail to add param expr", K(from_base));
    } else if (OB_FAIL(expr->add_param_expr(to_base))) {
      LOG_WARN("fail to add param expr", K(to_base));
    } else {
      expr->set_func_name("conv");
    }
  } else if (0 == expr->get_func_name().case_compare("oct")) {
    // oct(N) is equivalent to CONV(N,10,8)
    ObConstRawExpr* from_base = NULL;
    ObConstRawExpr* to_base = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, decimal, from_base))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, octal, to_base))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_UNLIKELY(1 != expr->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param count", K(expr->get_param_count()));
    } else if (OB_FAIL(expr->add_param_expr(from_base))) {
      LOG_WARN("fail to add param expr", K(from_base), K(ret));
    } else if (OB_FAIL(expr->add_param_expr(to_base))) {
      LOG_WARN("fail to add param expr", K(to_base), K(ret));
    } else {
      expr->set_func_name("conv");
    }
  } else if (0 == expr->get_func_name().case_compare("lcase")) {
    // lcase is synonym for lower
    expr->set_func_name("lower");
  } else if (0 == expr->get_func_name().case_compare("ucase")) {
    // ucase is synonym for upper
    expr->set_func_name("upper");
  } else if (0 == expr->get_func_name().case_compare("power")) {
    // don't alias "power" to "pow" in oracle mode, because oracle has no
    // "pow" function.
    if (!share::is_oracle_mode()) {
      expr->set_func_name("pow");
    }
  } else if (0 == expr->get_func_name().case_compare("ws")) {
    // ws is synonym for word_segment
    expr->set_func_name("word_segment");
  } else if (0 == expr->get_func_name().case_compare("inet_ntoa")) {
    // inet_ntoa is synonym for int2ip
    expr->set_func_name("int2ip");
  } else {
  }
  return ret;
}

int ObRawExprUtils::make_raw_expr_from_str(const char* expr_str, const int64_t buf_len,
    ObExprResolveContext& resolve_ctx, ObRawExpr*& expr, ObIArray<ObQualifiedName>& columns,
    ObIArray<ObVarInfo>& sys_vars, ObIArray<ObSubQueryInfo>* sub_query_info, ObIArray<ObAggFunRawExpr*>& aggr_exprs,
    ObIArray<ObWinFunRawExpr*>& win_exprs)
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
    _OB_LOG(WARN,
        "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], "
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
    ParseNode* stmt_node = NULL;
    ParseNode* select_node = NULL;
    ParseNode* select_expr_list = NULL;
    ParseNode* select_expr = NULL;
    ParseNode* parsed_expr = NULL;
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
        LOG_ERROR("internal arg is not correct", KP(parsed_expr));
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<ObOpRawExpr*> op_exprs;
      ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
      ObRawExprResolverImpl expr_resolver(resolve_ctx);
      // generate raw expr
      if (OB_FAIL(expr_resolver.resolve(parsed_expr,
              expr,
              columns,
              sys_vars,
              *sub_query_info,
              aggr_exprs,
              win_exprs,
              op_exprs,
              user_var_exprs))) {
        _LOG_WARN("failed to resolve expr tree, err=%d", ret);
      }
    }
    // destroy syntax tree
    parser.free_result(parse_result);
  }
  return ret;
}

int ObRawExprUtils::make_raw_expr_from_str(const ObString& expr_str, ObExprResolveContext& resolve_ctx,
    ObRawExpr*& expr, ObIArray<ObQualifiedName>& column, ObIArray<ObVarInfo>& sys_vars,
    ObIArray<ObSubQueryInfo>* sub_query_info, ObIArray<ObAggFunRawExpr*>& aggr_exprs,
    ObIArray<ObWinFunRawExpr*>& win_exprs)
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
                 win_exprs))) {
    LOG_WARN("fail to make_raw_expr_from_str", K(ret));
  }
  return ret;
}

int ObRawExprUtils::parse_default_expr_from_str(
    const ObString& expr_str, ObCollationType expr_str_cs_type, ObIAllocator& allocator, const ParseNode*& node)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  ParseResult parse_result;
  ObSQLMode sql_mode = SMO_DEFAULT;
  if (share::is_oracle_mode()) {
    sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
  }
  ObSQLParser parser(allocator, sql_mode);
  MEMSET(&parse_result, 0, sizeof(ParseResult));
  parse_result.malloc_pool_ = &allocator;
  parse_result.pl_parse_info_.is_pl_parse_ = true;
  parse_result.pl_parse_info_.is_pl_parse_expr_ = true;
  parse_result.sql_mode_ = sql_mode;
  parse_result.charset_info_ = ObCharset::get_charset(expr_str_cs_type);
  parse_result.is_not_utf8_connection_ = ObCharset::is_valid_collation(expr_str_cs_type)
                                             ? (ObCharset::charset_type_by_coll(expr_str_cs_type) != CHARSET_UTF8MB4)
                                             : false;

  if (OB_FAIL(sql_str.append_fmt("DO %.*s", expr_str.length(), expr_str.ptr()))) {
    LOG_WARN("failed to concat expr str", K(expr_str), K(ret));
  } else if (OB_FAIL(parser.parse(sql_str.string().ptr(), sql_str.string().length(), parse_result))) {
    _OB_LOG(WARN,
        "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], "
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
    ParseNode* expr_node = NULL;
    if (OB_ISNULL(expr_node = parse_result.result_tree_->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to parse default expr node", K(ret), K(expr_str));
    } else if (OB_UNLIKELY(T_DEFAULT != expr_node->type_) || OB_UNLIKELY(1 != expr_node->num_child_) ||
               OB_ISNULL(expr_node->children_) || OB_ISNULL(expr_node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "expr node type is illegal", K(ret), K(expr_node->type_), K(expr_node->num_child_), K(expr_node->children_));
    } else {
      node = expr_node->children_[0];
    }
  }
  return ret;
}

int ObRawExprUtils::parse_expr_node_from_str(const ObString& expr_str, ObIAllocator& allocator, const ParseNode*& node)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  ParseResult parse_result;
  ObSQLMode sql_mode = SMO_DEFAULT;
  if (share::is_oracle_mode()) {
    sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
  }
  ObParser parser(allocator, sql_mode);
  if (OB_FAIL(sql_str.append_fmt("SELECT %.*s FROM DUAL", expr_str.length(), expr_str.ptr()))) {
    LOG_WARN("fail to concat string", K(expr_str), K(ret));
  } else if (OB_FAIL(parser.parse(sql_str.string(), parse_result))) {
    _OB_LOG(WARN,
        "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], "
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
    ParseNode* stmt_node = NULL;
    ParseNode* select_node = NULL;
    ParseNode* select_expr_list = NULL;
    ParseNode* select_expr = NULL;
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
      node = select_expr->children_[0];
    }
  }
  return ret;
}

int ObRawExprUtils::parse_bool_expr_node_from_str(
    const common::ObString& expr_str, common::ObIAllocator& allocator, const ParseNode*& node)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_str;
  ParseResult parse_result;
  ObSQLMode sql_mode = SMO_DEFAULT;

  if (share::is_oracle_mode()) {
    sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
  }
  ObParser parser(allocator, sql_mode);
  if (OB_FAIL(sql_str.append_fmt("SELECT 1 FROM DUAL WHERE %.*s", expr_str.length(), expr_str.ptr()))) {
    LOG_WARN("fail to concat string", K(expr_str), K(ret));
  } else if (OB_FAIL(parser.parse(sql_str.string(), parse_result))) {
    _OB_LOG(WARN,
        "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], "
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
    ParseNode* stmt_node = NULL;
    ParseNode* select_node = NULL;
    ParseNode* where_node = NULL;
    ParseNode* expr_node = NULL;
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

int ObRawExprUtils::build_generated_column_expr(const ObString& expr_str, ObRawExprFactory& expr_factory,
    const ObSQLSessionInfo& session_info, ObRawExpr*& expr, ObIArray<ObQualifiedName>& columns,
    const ObSchemaChecker* schema_checker, const ObResolverUtils::PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  const ParseNode* node = NULL;
  if (OB_FAIL(parse_expr_node_from_str(expr_str, expr_factory.get_allocator(), node))) {
    LOG_WARN("parse expr node from string failed", K(ret), K(expr_str));
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null");
  } else if (OB_FAIL(build_generated_column_expr(
                 expr_factory, session_info, *node, expr, columns, schema_checker, check_status))) {
    LOG_WARN("build generated column expr failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::check_deterministic(
    const ObRawExpr* expr, ObIAllocator& allocator, const ObResolverUtils::PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));
  CK(expr->get_children_count() >= 0);
  ObList<const ObRawExpr*, ObIAllocator> expr_queue(allocator);
  OZ(expr_queue.push_back(expr));
  const ObRawExpr* cur_expr = NULL;
  while (OB_SUCC(ret) && expr_queue.size() > 0) {
    OZ(expr_queue.pop_front(cur_expr));
    CK(OB_NOT_NULL(cur_expr));
    OZ(check_deterministic_single(cur_expr, check_status));
    for (int i = 0; OB_SUCC(ret) && i < cur_expr->get_param_count(); ++i) {
      OZ(expr_queue.push_back(cur_expr->get_param_expr(i)));
    }
  }
  return ret;
}

int ObRawExprUtils::check_deterministic_single(
    const ObRawExpr* expr, const ObResolverUtils::PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));
  if (OB_SUCC(ret) && ObResolverUtils::DISABLE_CHECK != check_status) {
    if (expr->is_sys_func_expr()) {
      if (expr->is_non_pure_sys_func_expr()) {
        if (ObResolverUtils::CHECK_FOR_GENERATED_COLUMN == check_status) {
          ret = OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION;
        } else if (ObResolverUtils::CHECK_FOR_FUNCTION_INDEX == check_status) {
          ret = OB_ERR_ONLY_PURE_FUNC_CANBE_INDEXED;
        }
        LOG_WARN("only pure sys function can be indexed", K(ret), K(check_status), K(*expr));
      } else if (T_FUN_SYS_ROWNUM == expr->get_expr_type()) {
        ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
        LOG_WARN("ROWNUM is not allowed", K(ret), K(*expr));
      }
    } else if (expr->is_pseudo_column_expr()) {
      if (expr->is_specified_pseudocolumn_expr()) {
        ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
        LOG_WARN("not allowed pesudo column", K(ret), K(*expr));
      }
    } else if (expr->is_udf_expr()) {
      if (lib::is_mysql_mode()) {
        ret = OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION;
        LOG_WARN("user-defined functions are not allowd in generated column", K(ret), K(*expr));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("user-defined function is not supported", K(ret), K(*expr));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "The user-defined function");
      }
    }
  }
  return ret;
}

int ObRawExprUtils::build_generated_column_expr(const ObString& expr_str, ObRawExprFactory& expr_factory,
    const ObSQLSessionInfo& session_info, const ObTableSchema& table_schema, ObRawExpr*& expr,
    const ObSchemaChecker* schema_checker, const ObResolverUtils::PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  const ParseNode* node = NULL;
  ObSEArray<ObQualifiedName, 4> columns;
  const ObColumnSchemaV2* col_schema = NULL;
  if (OB_FAIL(parse_expr_node_from_str(expr_str, expr_factory.get_allocator(), node))) {
    LOG_WARN("parse expr node from string failed", K(ret));
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null");
  } else if (OB_FAIL(build_generated_column_expr(
                 expr_factory, session_info, *node, expr, columns, schema_checker, check_status))) {
    LOG_WARN("build generated column expr failed", K(ret), K(expr_str));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    const ObQualifiedName& q_name = columns.at(i);
    if (OB_UNLIKELY(!q_name.database_name_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid generated column name", K(q_name));
    } else if (!q_name.tbl_name_.empty() &&
               !ObCharset::case_insensitive_equal(q_name.tbl_name_, table_schema.get_table_name_str())) {
      ret = OB_ERR_BAD_TABLE;
      LOG_USER_ERROR(OB_ERR_BAD_TABLE, q_name.tbl_name_.length(), q_name.tbl_name_.ptr());
    } else if (OB_ISNULL(col_schema = table_schema.get_column_schema(q_name.col_name_))) {
      ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, q_name.col_name_.length(), q_name.col_name_.ptr());
    } else if (OB_UNLIKELY(col_schema->is_generated_column())) {
      ret = OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
      LOG_USER_ERROR(OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN, "Generated column in column expression");
    } else if (OB_FAIL(ObRawExprUtils::init_column_expr(*col_schema, *q_name.ref_expr_))) {
      LOG_WARN("init column expr failed", K(ret), K(q_name));
    } else {
      q_name.ref_expr_->set_ref_id(table_schema.get_table_id(), col_schema->get_column_id());
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr->formalize(&session_info))) {
      LOG_WARN("formalize expr failed", K(ret), KPC(expr));
    }
  }
  return ret;
}

int ObRawExprUtils::build_check_constraint_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session_info,
    const ParseNode& node, ObRawExpr*& expr, common::ObIArray<ObQualifiedName>& columns)
{
  int ret = OB_SUCCESS;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObSubQueryInfo> sub_query_info;
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
          op_exprs))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_SUB_QUERY))) {
    ret = OB_ERR_INVALID_SUBQUERY_USE;
    LOG_WARN("subquery not allowed here", K(ret));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_AGG))) {
    ret = OB_ERR_GROUP_FUNC_NOT_ALLOWED;
    LOG_WARN("group function is not allowed here", K(ret));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_ROWNUM) || expr->has_flag(CNT_PSEUDO_COLUMN))) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("Specified pseudo column or operator not allowed here",
        K(ret),
        K(expr->has_flag(CNT_ROWNUM)),
        K(expr->has_flag(CNT_PSEUDO_COLUMN)));
  } else if (OB_UNLIKELY(expr->has_flag(CNT_WINDOW_FUNC))) {
    ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
  } else if (OB_UNLIKELY(sys_vars.count() > 0 || expr->has_flag(CNT_CUR_TIME) || expr->has_flag(CNT_STATE_FUNC))) {
    ret = OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST;
    LOG_WARN("date or system variable wrongly specified in CHECK constraint",
        K(ret),
        K(expr->has_flag(CNT_CUR_TIME)),
        K(expr->has_flag(CNT_STATE_FUNC)));
  } else { /*do nothing*/
  }
  return ret;
}

int ObRawExprUtils::build_generated_column_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session_info,
    const ParseNode& node, ObRawExpr*& expr, ObIArray<ObQualifiedName>& columns, const ObSchemaChecker* schema_checker,
    const ObResolverUtils::PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObOpRawExpr*> op_exprs;

  if (OB_FAIL(build_raw_expr(expr_factory,
          session_info,
          const_cast<ObSchemaChecker*>(schema_checker),
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
          op_exprs,
          false))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_UNLIKELY(sys_vars.count() > 0)) {
    // todo:mysql ERROR 3102 (HY000): Expression of generated column 'column' contains a disallowed function.
    // mysql error code: ER_GENERATED_COLUMN_FUNCTION_IS_NOT_ALLOWED
    // format: Expression of generated column '%s' contains a disallowed function
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
  } else if (lib::is_oracle_mode()) {
    // generated column contain only two types of access obj:sysfunc expression and column expression
    // only column expression will be handled in the upper function call,
    // so sysfunc expression should be filtered in this function and can't return to the upper function call
    ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 2> ref_sys_exprs;
    ObSEArray<ObQualifiedName, 2> real_columns;
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
      const ObQualifiedName& q_name = columns.at(i);
      if (q_name.is_sys_func()) {
        ObRawExpr* sys_func = q_name.access_idents_.at(0).sys_func_expr_;
        CK(OB_NOT_NULL(sys_func));
        for (int64_t i = 0; OB_SUCC(ret) && i < ref_sys_exprs.count(); ++i) {
          OZ(ObRawExprUtils::replace_ref_column(sys_func, ref_sys_exprs.at(i).first, ref_sys_exprs.at(i).second));
        }
        OZ(q_name.access_idents_.at(0).sys_func_expr_->check_param_num());
        OZ(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, sys_func));
        OZ(ref_sys_exprs.push_back(std::pair<ObRawExpr*, ObRawExpr*>(q_name.ref_expr_, sys_func)));
      } else {
        OZ(real_columns.push_back(q_name), q_name, i);
      }
    }
    if (OB_SUCC(ret) && real_columns.count() != columns.count()) {
      columns.reuse();
      OZ(columns.assign(real_columns), real_columns);
    }
  }
  // check whether the expression is deterministic recursively
  if (OB_SUCC(ret) && ObResolverUtils::DISABLE_CHECK != check_status) {
    OZ(check_deterministic(expr, expr_factory.get_allocator(), check_status));
  }
  return ret;
}

int ObRawExprUtils::build_raw_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session_info,
    const ParseNode& node, ObRawExpr*& expr, ObIArray<ObQualifiedName>& columns, ObIArray<ObVarInfo>& sys_vars,
    ObIArray<ObAggFunRawExpr*>& aggr_exprs, ObIArray<ObWinFunRawExpr*>& win_exprs,
    ObIArray<ObSubQueryInfo>& sub_query_info, common::ObIArray<ObOpRawExpr*>& op_exprs,
    bool is_prepare_protocol /*= false*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_raw_expr(expr_factory,
          session_info,
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
          op_exprs,
          is_prepare_protocol))) {
    LOG_WARN("failed to build raw expr", K(ret));
  }
  return ret;
}

int ObRawExprUtils::build_raw_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session_info,
    ObSchemaChecker* schema_checker, ObStmtScope current_scope, ObStmt* stmt, const ParamStore* param_list,
    common::ObIArray<std::pair<ObRawExpr*, ObConstRawExpr*>>* external_param_info, const ParseNode& node,
    ObRawExpr*& expr, ObIArray<ObQualifiedName>& columns, ObIArray<ObVarInfo>& sys_vars,
    ObIArray<ObAggFunRawExpr*>& aggr_exprs, ObIArray<ObWinFunRawExpr*>& win_exprs,
    ObIArray<ObSubQueryInfo>& sub_query_info, ObIArray<ObOpRawExpr*>& op_exprs, bool is_prepare_protocol /*= false*/)
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
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = param_list;
    ctx.is_extract_param_type_ = !is_prepare_protocol;  // when prepare do not extract
    ctx.external_param_info_ = external_param_info;
    ctx.current_scope_ = current_scope;
    ctx.stmt_ = stmt;
    ctx.schema_checker_ = schema_checker;
    ctx.session_info_ = &session_info;
    ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(session_info.get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(
                   &node, expr, columns, sys_vars, sub_query_info, aggr_exprs, win_exprs, op_exprs, user_var_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObRawExprUtils::build_generated_column_expr(const ObString& expr_str, ObRawExprFactory& expr_factory,
    const ObSQLSessionInfo& session_info, uint64_t table_id, const ObTableSchema& table_schema,
    const ObColumnSchemaV2& gen_col_schema, ObRawExpr*& expr, const ObSchemaChecker* schema_checker,
    const ObResolverUtils::PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  const ObColumnSchemaV2* col_schema = NULL;
  if (OB_FAIL(build_generated_column_expr(
          expr_str, expr_factory, session_info, expr, columns, schema_checker, check_status))) {
    LOG_WARN("build generated column expr failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObQualifiedName& q_name = columns.at(i);
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
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr->formalize(&session_info))) {
      LOG_WARN("formalize expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && !gen_col_schema.is_hidden()) {
    // only generated column defined by user need to do cast.
    ObExprResType expected_type;
    expected_type.set_meta(gen_col_schema.get_meta_type());
    expected_type.set_accuracy(gen_col_schema.get_accuracy());
    expected_type.set_result_flag(calc_column_result_flag(gen_col_schema));
    if (ObRawExprUtils::need_column_conv(expected_type, *expr)) {
      if (OB_FAIL(build_column_conv_expr(expr_factory, &gen_col_schema, expr, &session_info))) {
        LOG_WARN("create cast expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::build_pad_expr_recursively(ObRawExprFactory &expr_factory, const ObSQLSessionInfo &session,
    const ObTableSchema &table_schema, const ObColumnSchemaV2 &gen_col_schema, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));

  if (OB_SUCC(ret) && expr->get_param_count() > 0) {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      OZ(SMART_CALL(
          build_pad_expr_recursively(expr_factory, session, table_schema, gen_col_schema, expr->get_param_expr(i))));
    }
  }
  if (OB_SUCC(ret) && expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *b_expr = static_cast<ObColumnRefRawExpr *>(expr);
    uint64_t column_id = b_expr->get_column_id();
    if (OB_SUCC(ret)) {
      const ObColumnSchemaV2 *column_schema = table_schema.get_column_schema(column_id);
      if (NULL == column_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column schema fail", K(column_schema));
      } else if (ObObjMeta::is_binary(column_schema->get_data_type(), column_schema->get_collation_type())) {
        if (OB_FAIL(build_pad_expr(expr_factory, false, column_schema, expr, &session))) {
          LOG_WARN("fail to build pading expr for binary", K(ret));
        }
      } else if (ObCharType == column_schema->get_data_type() || ObNCharType == column_schema->get_data_type()) {
        if (gen_col_schema.has_column_flag(PAD_WHEN_CALC_GENERATED_COLUMN_FLAG)) {
          if (OB_FAIL(build_pad_expr(expr_factory, true, column_schema, expr, &session))) {
            LOG_WARN("fail to build pading expr for char", K(ret));
          }
        } else {
          if (OB_FAIL(build_trim_expr(column_schema, expr_factory, &session, expr))) {
            LOG_WARN("fail to build trime expr for char", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// compare two raw expressions
bool ObRawExprUtils::is_same_raw_expr(const ObRawExpr* src, const ObRawExpr* dst)
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

int ObRawExprUtils::replace_all_ref_column(
    ObRawExpr*& raw_expr, const common::ObIArray<ObRawExpr*>& exprs, int64_t& offset)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(raw_expr));
  } else if (offset >= exprs.count()) {
    LOG_WARN("beyond the index", K(offset), K(exprs.count()));
  } else if (T_REF_COLUMN == raw_expr->get_expr_type()) {
    LOG_DEBUG("replace leaf node", K(*raw_expr), K(*exprs.at(offset)), K(offset));
    raw_expr = exprs.at(offset);
    ++offset;
  } else {
    int64_t N = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N && offset < exprs.count(); ++i) {
      ObRawExpr*& child_expr = raw_expr->get_param_expr(i);
      if (OB_FAIL(replace_all_ref_column(child_expr, exprs, offset))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    }  // end for
  }
  return ret;
}

// if %expr_factory is not NULL, will deep copy %to expr. default behavior is shallow copy
// if except_exprs is not NULL, will skip the expr in except_exprs
int ObRawExprUtils::replace_ref_column(ObRawExpr*& raw_expr, ObRawExpr* from, ObRawExpr* to,
    ObRawExprFactory* expr_factory, const ObIArray<ObRawExpr*>* except_exprs)
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
    if (NULL != expr_factory) {
      ObRawExpr* new_to = NULL;
      if (OB_FAIL(copy_expr(*expr_factory, to, new_to, COPY_REF_DEFAULT))) {
        LOG_WARN("copy expr failed", K(ret), K(*to));
      } else if (OB_ISNULL(new_to)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copied expr is NULL", K(ret), KP(new_to));
      } else {
        raw_expr = new_to;
      }
    } else {
      raw_expr = to;
      LOG_DEBUG("replace leaf node", K(*from), K(*to), K(from), K(to));
    }
  } else if (raw_expr->is_query_ref_expr()) {
    ObSelectStmt* ref_stmt = static_cast<ObQueryRefRawExpr*>(raw_expr)->get_ref_stmt();
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
      ObRawExpr*& child_expr = raw_expr->get_param_expr(i);
      if (OB_FAIL(SMART_CALL(replace_ref_column(child_expr, from, to, expr_factory, except_exprs)))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    }  // end for
  }
  return ret;
}

int ObRawExprUtils::replace_ref_column(
    ObIArray<ObRawExpr*>& exprs, ObRawExpr* from, ObRawExpr* to, const ObIArray<ObRawExpr*>* except_exprs)
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
    ObRawExpr* tmp_raw_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      ObRawExpr*& raw_expr = tmp_raw_expr;
      if (OB_FAIL(exprs.at(i, raw_expr))) {
        LOG_WARN("failed to get raw expr", K(i), K(ret));
      } else if (OB_FAIL(SMART_CALL(replace_ref_column(raw_expr, from, to, NULL, except_exprs)))) {
        LOG_WARN("failed to replace_ref_column", K(from), K(to), K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

bool ObRawExprUtils::all_column_exprs(const common::ObIArray<ObRawExpr*>& exprs)
{
  bool all_column = true;
  for (int64_t i = 0; all_column && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      all_column = false;
    } else if (!exprs.at(i)->is_column_ref_expr()) {
      all_column = false;
    } else {
    }
  }
  return all_column;
}

int ObRawExprUtils::extract_column_exprs(const ObRawExpr* raw_expr, ObIArray<ObRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  if (NULL == raw_expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else {
    if (T_REF_COLUMN == raw_expr->get_expr_type()) {
      ret = add_var_to_array_no_dup(column_exprs, const_cast<ObRawExpr*>(raw_expr));
    } else {
      int64_t N = raw_expr->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        ret = extract_column_exprs(raw_expr->get_param_expr(i), column_exprs);
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_column_exprs(const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Expr is NULL", K(ret), K(i));
    } else if (OB_FAIL(extract_column_exprs(exprs.at(i), column_exprs))) {
      LOG_WARN("Failed to extract column exprs", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObRawExprUtils::mark_column_explicited_reference(ObRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 2> column_exprs;
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

int ObRawExprUtils::extract_column_ids(const ObIArray<ObRawExpr*>& exprs, common::ObIArray<uint64_t>& column_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(extract_column_ids(exprs.at(i), column_ids))) {
      LOG_WARN("failed to extract column ids", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObRawExprUtils::extract_column_ids(const ObRawExpr* raw_expr, common::ObIArray<uint64_t>& column_ids)
{
  int ret = OB_SUCCESS;
  if (NULL == raw_expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else {
    if (T_REF_COLUMN == raw_expr->get_expr_type()) {
      ret = add_var_to_array_no_dup(column_ids, static_cast<const ObColumnRefRawExpr*>(raw_expr)->get_column_id());
    } else {
      int64_t N = raw_expr->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        ret = SMART_CALL(extract_column_ids(raw_expr->get_param_expr(i), column_ids));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::extract_table_ids(const ObRawExpr* raw_expr, common::ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  if (NULL == raw_expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(raw_expr));
  } else {
    if (T_REF_COLUMN == raw_expr->get_expr_type()) {
      ret = add_var_to_array_no_dup(table_ids, static_cast<const ObColumnRefRawExpr*>(raw_expr)->get_table_id());
    } else {
      int64_t N = raw_expr->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        ret = extract_table_ids(raw_expr->get_param_expr(i), table_ids);
      }
    }
  }
  return ret;
}

int ObRawExprUtils::cnt_current_level_aggr_expr(const ObRawExpr* expr, int32_t cur_level, bool& cnt_aggr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (expr->has_flag(IS_AGG) && expr->get_expr_level() == cur_level) {
    cnt_aggr = true;
  } else if (expr->has_flag(IS_SUB_QUERY)) {
    const ObQueryRefRawExpr* query_ref = static_cast<const ObQueryRefRawExpr*>(expr);
    const ObSelectStmt* subquery = NULL;
    if ((subquery = query_ref->get_ref_stmt()) != NULL) {
      if (OB_FAIL(cnt_current_level_aggr_expr(subquery, cur_level, cnt_aggr))) {
        LOG_WARN("get generalized column in subquery failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid query ref expr", K(*expr), K(ret));
    }
  } else if ((expr->has_flag(CNT_AGG) || expr->has_flag(CNT_SUB_QUERY)) && !expr->is_set_op_expr()) {
    for (int64_t i = 0; OB_SUCC(ret) && !cnt_aggr && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(cnt_current_level_aggr_expr(expr->get_param_expr(i), cur_level, cnt_aggr))) {
        LOG_WARN("cnt current level aggr expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::cnt_current_level_aggr_expr(const ObDMLStmt* stmt, int32_t column_level, bool& cnt_aggr)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> relation_exprs;
  ObArray<ObSelectStmt*> child_stmts;

  cnt_aggr = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null");
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("get stmt relation exprs failed", K(ret));
  } else if (OB_FAIL(stmt->get_from_subquery_stmts(child_stmts))) {
    LOG_WARN("get child stmt failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !cnt_aggr && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(cnt_current_level_aggr_expr(relation_exprs.at(i), column_level, cnt_aggr))) {
      LOG_WARN("get generalized column in expr tree failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !cnt_aggr && i < child_stmts.count(); ++i) {
    if (OB_FAIL(cnt_current_level_aggr_expr(child_stmts.at(i), column_level, cnt_aggr))) {
      LOG_WARN("get generalized column in stmt failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::try_add_cast_expr_above(ObRawExprFactory* expr_factory, const ObSQLSessionInfo* session,
    ObRawExpr& src_expr, const ObExprResType& dst_type, ObRawExpr*& new_expr)
{
  int ret = OB_SUCCESS;
  ObCastMode cm = CM_NONE;
  OZ(ObSQLUtils::get_default_cast_mode(false, /* explicit_cast */
      0,                                      /* result_flag */
      session,
      cm));
  OZ(try_add_cast_expr_above(expr_factory, session, src_expr, dst_type, cm, new_expr));
  return ret;
}

int ObRawExprUtils::try_add_cast_expr_above(ObRawExprFactory* expr_factory, const ObSQLSessionInfo* session,
    ObRawExpr& expr, const ObExprResType& dst_type, const ObCastMode& cm, ObRawExpr*& new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = &expr;
  bool need_cast = false;
  const ObExprResType& src_type = expr.get_result_type();
  CK(OB_NOT_NULL(session) && OB_NOT_NULL(expr_factory));
  OZ(ObRawExprUtils::check_need_cast_expr(src_type, dst_type, need_cast));
  if (OB_SUCC(ret) && need_cast) {
    if (T_FUN_SYS_CAST == expr.get_expr_type() && expr.has_flag(IS_OP_OPERAND_IMPLICIT_CAST)) {
      ret = OB_ERR_UNEXPECTED;
#ifdef DEBUG
      LOG_ERROR("try to add implicit cast again, check if type deduction is correct",
          K(ret),
          K(expr),
          K(dst_type),
          K(session->get_current_query_string()));
#else
      LOG_WARN("try to add implicit cast again, check if type deduction is correct",
          K(ret),
          K(expr),
          K(dst_type),
          K(session->get_current_query_string()));
#endif
    } else {
      // setup zerofill cm
      // eg: select concat(cast(c_zf as char(10)), cast(col_no_zf as char(10))) from t1;
      ObCastMode cm_zf = cm;
      if (expr.get_result_type().has_result_flag(OB_MYSQL_ZEROFILL_FLAG)) {
        cm_zf |= CM_ZERO_FILL;
      }
      ObSysFunRawExpr* cast_expr = NULL;
      OZ(ObRawExprUtils::create_cast_expr(*expr_factory, &expr, dst_type, cast_expr, session, false, cm_zf));
      CK(OB_NOT_NULL(new_expr = dynamic_cast<ObRawExpr*>(cast_expr)));
    }
    LOG_DEBUG("in try_add_cast", K(ret), K(dst_type), K(cm));
  }
  return ret;
}

int ObRawExprUtils::create_cast_expr(ObRawExprFactory& expr_factory, ObRawExpr* src_expr, const ObExprResType& dst_type,
    ObSysFunRawExpr*& func_expr, const ObSQLSessionInfo* session, bool use_def_cm, ObCastMode cm)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(src_expr));
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret) && use_def_cm) {
    OZ(ObSQLUtils::get_default_cast_mode(false, /* explicit_cast */
        0,                                      /* result_flag */
        session,
        cm));
  }
  if (OB_SUCC(ret)) {
    if (session->use_static_typing_engine()) {
      const ObExprResType& src_type = src_expr->get_result_type();
      bool need_extra_cast_for_src_type = false;
      bool need_extra_cast_for_dst_type = false;

      need_extra_cast(src_type, dst_type, need_extra_cast_for_src_type, need_extra_cast_for_dst_type);

      // extra cast expr: cast non-utf8 to utf8
      ObSysFunRawExpr* extra_cast = NULL;
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
    } else {
      OZ(create_real_cast_expr(expr_factory, src_expr, dst_type, func_expr, session));
      OZ(func_expr->add_flag(IS_INNER_ADDED_EXPR));
    }

    OZ(func_expr->formalize(session));
  }
  return ret;
}

void ObRawExprUtils::need_extra_cast(const ObExprResType& src_type, const ObExprResType& dst_type,
    bool& need_extra_cast_for_src_type, bool& need_extra_cast_for_dst_type)
{
  need_extra_cast_for_src_type = false;
  need_extra_cast_for_dst_type = false;
  const ObCharsetType& src_cs = ObCharset::charset_type_by_coll(src_type.get_collation_type());
  const ObCharsetType& dst_cs = ObCharset::charset_type_by_coll(dst_type.get_collation_type());
  bool nonstr_to_str = (!src_type.is_string_or_lob_locator_type() && dst_type.is_string_or_lob_locator_type());
  bool str_to_nonstr = (src_type.is_string_or_lob_locator_type() && !dst_type.is_string_or_lob_locator_type());

  if (src_type.is_null() || dst_type.is_null()) {
    // do nothing
  } else if (str_to_nonstr) {
    if (CHARSET_BINARY != src_cs && ObCharset::get_default_charset() != src_cs) {
      need_extra_cast_for_src_type = true;
    }
  } else if (nonstr_to_str) {
    if (CHARSET_BINARY != dst_cs && ObCharset::get_default_charset() != dst_cs) {
      need_extra_cast_for_dst_type = true;
    }
  }
}

int ObRawExprUtils::setup_extra_cast_utf8_type(const ObExprResType& type, ObExprResType& utf8_type)
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

int ObRawExprUtils::erase_inner_added_exprs(ObRawExpr* src_expr, ObRawExpr*& out_expr)
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
      default: {
        break;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::erase_operand_implicit_cast(ObRawExpr* src, ObRawExpr*& out)
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
      if (OB_SUCC(ret)) {
        out = src;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::create_to_type_expr(ObRawExprFactory& expr_factory, ObRawExpr* src_expr, const ObObjType& dst_type,
    ObSysFunRawExpr*& to_type, ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  ObExprOperator* op = NULL;
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
    ObExprToType* to_type_op = static_cast<ObExprToType*>(op);
    to_type_op->set_expect_type(dst_type);
    if (OB_FAIL(to_type->formalize(session_info))) {
      LOG_WARN("formalize to_type expr failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::create_substr_expr(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
    ObRawExpr* first_expr, ObRawExpr* second_expr, ObRawExpr* third_expr, ObSysFunRawExpr*& out_expr)
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

int ObRawExprUtils::create_type_to_str_expr(ObRawExprFactory& expr_factory, ObRawExpr* src_expr,
    ObSysFunRawExpr*& out_expr, ObSQLSessionInfo* session_info, bool is_type_to_str)
{
  int ret = OB_SUCCESS;
  ObExprOperator* op = NULL;
  ObObjType data_type = ObNullType;
  if (OB_ISNULL(src_expr) || OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src_expr or session_info is NULL", KP(src_expr), KP(session_info), K(ret));
  } else if (OB_UNLIKELY(!src_expr->is_terminal_expr() && !src_expr->is_sys_func_expr() && !src_expr->is_udf_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src_expr should be terminal expr or func expr", KPC(src_expr), K(ret));
  } else if (FALSE_IT(data_type = src_expr->get_data_type())) {
  } else if (OB_UNLIKELY(!ob_is_enumset_tc(data_type))) {
    LOG_WARN("data_type of src_expr is invalid", K(data_type), KPC(src_expr), K(ret));
  } else {
    ObItemType item_type = (true == is_type_to_str)
                               ? (ObEnumType == data_type ? T_FUN_ENUM_TO_STR : T_FUN_SET_TO_STR)
                               : (ObEnumType == data_type ? T_FUN_ENUM_TO_INNER_TYPE : T_FUN_SET_TO_INNER_TYPE);

    const char* func_name = (true == is_type_to_str)
                                ? (ObEnumType == data_type ? N_ENUM_TO_STR : N_SET_TO_STR)
                                : (ObEnumType == data_type ? N_ENUM_TO_INNER_TYPE : N_SET_TO_INNER_TYPE);
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
    }

    ObConstRawExpr* col_accuracy_expr = NULL;
    if (OB_SUCC(ret)) {
      ObString str_col_accuracy;
      if (OB_FAIL(build_const_string_expr(
              expr_factory, ObVarcharType, str_col_accuracy, src_expr->get_collation_type(), col_accuracy_expr))) {
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
      ObExprTypeToStr* type_to_str = static_cast<ObExprTypeToStr*>(op);
      const ObIArray<ObString>& enum_set_values = src_expr->get_enum_set_values();
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
      } else {
      }
    }
  }
  return ret;
}

int ObRawExprUtils::create_type_to_str_expr(ObRawExprFactory& expr_factory, ObRawExpr* src_expr, int32_t expr_level,
    ObSysFunRawExpr*& out_expr, ObSQLSessionInfo* session_info, bool is_type_to_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_type_to_str_expr(expr_factory, src_expr, out_expr, session_info, is_type_to_str))) {
    LOG_WARN("failed to create type_to_str_expr", K(ret));
  } else if (OB_ISNULL(out_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret));
  } else if (OB_FAIL(out_expr->pull_relation_id_and_levels(expr_level))) {
    LOG_WARN("failed to pull_relation_id_and_levels", K(ret));
  }
  return ret;
}

// init_expr are initial expressions before parameterization;
// src_expr output expression after parameterization.
int ObRawExprUtils::create_exec_param_expr(ObStmt* stmt, ObRawExprFactory& expr_factory, ObRawExpr*& src_expr,
    ObSQLSessionInfo* session_info, std::pair<int64_t, ObRawExpr*>& init_expr)
{
  UNUSED(session_info);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(stmt));
  } else {
    init_expr.first = stmt->get_question_marks_count();
    init_expr.second = src_expr;
    stmt->increase_question_marks_count();
    if (OB_FAIL(create_param_expr(expr_factory, init_expr.first, src_expr, true))) {
      LOG_WARN("create param expr failed", K(ret));
    } else if (OB_FAIL(stmt->get_exec_param_ref_exprs().push_back(src_expr))) {
      LOG_WARN("store param expr failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::create_param_expr(
    ObRawExprFactory& expr_factory, int64_t param_idx, ObRawExpr*& expr, bool is_exec_param)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
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
    if (is_exec_param) {
      c_expr->add_flag(IS_EXEC_PARAM);
    }
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

int ObRawExprUtils::copy_exprs(ObRawExprFactory& expr_factory, const ObIArray<ObGroupbyExpr>& origin,
    ObIArray<ObGroupbyExpr>& dest, const uint64_t copy_types, bool use_new_allocator)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < origin.count(); i++) {
    ObGroupbyExpr dest_item;
    if (OB_FAIL(ObRawExprUtils::copy_exprs(
            expr_factory, origin.at(i).groupby_exprs_, dest_item.groupby_exprs_, copy_types, use_new_allocator))) {
      LOG_WARN("failed to copy exprs.", K(ret));
    } else if (OB_FAIL(dest.push_back(dest_item))) {
      LOG_WARN("failed to push back into dest.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObRawExprUtils::copy_expr(ObRawExprFactory& expr_factory, const ObRawExpr* origin, ObRawExpr*& dest,
    const uint64_t copy_types, bool use_new_allocator)
{
  int ret = OB_SUCCESS;
  bool need_copy = true;
  bool copy_share_expr = (COPY_REF_SHARED == copy_types) || use_new_allocator;
  bool is_stack_overflow = false;
  if (NULL == origin) {
    need_copy = false;
    dest = NULL;
  } else if (copy_share_expr) {
    // current expr should be copied no matter it is shared or not
  } else if (origin->is_column_ref_expr() || origin->is_query_ref_expr() || origin->is_aggr_expr() ||
             origin->is_win_func_expr() || origin->is_set_op_expr() || is_pseudo_column_like_expr(*origin) ||
             origin->has_flag(IS_SHARED_REF) || origin->has_flag(IS_PARAM)) {
    // a shared expr is not copied in default mode
    dest = const_cast<ObRawExpr*>(origin);
    need_copy = false;
  }

  if (OB_FAIL(ret) || !need_copy) {
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    // deep copy here
    ObRawExpr::ExprClass expr_class = origin->get_expr_class();
    switch (expr_class) {
      case ObRawExpr::EXPR_QUERY_REF: {
        // this is not deep copy, should change !!!
        ObQueryRefRawExpr* dest_query_ref = NULL;
        const ObQueryRefRawExpr* origin_query_ref = static_cast<const ObQueryRefRawExpr*>(origin);
        if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_query_ref)) ||
            OB_ISNULL(dest_query_ref)) {
          LOG_WARN("failed to allocate raw expr", K(dest_query_ref), K(ret));
        } else if (OB_FAIL(dest_query_ref->assign(*origin_query_ref))) {
          LOG_WARN("failed to assign query ref", K(ret));
        } else {
          dest = dest_query_ref;
        }
        break;
      }
      case ObRawExpr::EXPR_COLUMN_REF: {
        ObColumnRefRawExpr* dest_column_ref = NULL;
        const ObColumnRefRawExpr* origin_column_ref = static_cast<const ObColumnRefRawExpr*>(origin);
        if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, dest_column_ref)) || OB_ISNULL(dest_column_ref)) {
          LOG_WARN("failed to allocate raw expr", K(dest_column_ref), K(ret));
        } else if (OB_FAIL(dest_column_ref->deep_copy(expr_factory, *origin_column_ref, use_new_allocator))) {
          LOG_WARN("failed to deep copy column ref", K(ret));
        } else {
          dest = dest_column_ref;
        }
        break;
      }
      case ObRawExpr::EXPR_AGGR: {
        ObAggFunRawExpr* dest_agg = NULL;
        const ObAggFunRawExpr* origin_agg = static_cast<const ObAggFunRawExpr*>(origin);
        if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_agg)) || OB_ISNULL(dest_agg)) {
          LOG_WARN("failed to allocate raw expr", K(dest_agg), K(ret));
        } else if (OB_FAIL(dest_agg->deep_copy(expr_factory, *origin_agg, COPY_REF_DEFAULT, use_new_allocator))) {
          LOG_WARN("failed to deep copy agg expr", K(ret));
        } else {
          dest = dest_agg;
        }
        break;
      }
      case ObRawExpr::EXPR_CONST: {
        if (T_USER_VARIABLE_IDENTIFIER == origin->get_expr_type()) {
          ObUserVarIdentRawExpr* dest_expr = NULL;
          const ObUserVarIdentRawExpr* origin_expr = static_cast<const ObUserVarIdentRawExpr*>(origin);
          if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_expr)) || OB_ISNULL(dest_expr)) {
            LOG_WARN("failed to allocate raw expr", K(dest_expr), K(ret));
          } else if (OB_FAIL(dest_expr->deep_copy(expr_factory, *origin_expr, COPY_REF_DEFAULT, use_new_allocator))) {
            LOG_WARN("failed to deep copy const expr", K(ret));
          } else {
            dest = dest_expr;
          }
        } else {
          ObConstRawExpr *dest_const = NULL;
          const ObConstRawExpr *origin_const = static_cast<const ObConstRawExpr *>(origin);
          if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_const)) || OB_ISNULL(dest_const)) {
            LOG_WARN("failed to allocate raw expr", K(dest_const), K(ret));
          } else if (OB_FAIL(dest_const->deep_copy(expr_factory, *origin_const, COPY_REF_DEFAULT, use_new_allocator))) {
            LOG_WARN("failed to deep copy const expr", K(ret));
          } else {
            dest = dest_const;
          }
        }
        break;
      }
      case ObRawExpr::EXPR_OPERATOR: {
        const ObOpRawExpr* origin_op = static_cast<const ObOpRawExpr*>(origin);
        if (origin->is_obj_access_expr()) {
          const ObObjAccessRawExpr* origin_oa = static_cast<const ObObjAccessRawExpr*>(origin);
          ObObjAccessRawExpr* dest_oa = NULL;
          if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_oa))) {
            LOG_WARN("failed to allocate raw expr", K(ret));
          } else if (OB_ISNULL(dest_oa)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is NULL", K(dest_oa), K(ret));
          } else if (OB_FAIL(dest_oa->deep_copy(expr_factory, *origin_oa, COPY_REF_DEFAULT, use_new_allocator))) {
            LOG_WARN("failed to assign", K(*dest_oa), K(ret));
          } else {
            dest = dest_oa;
          }
        } else if (origin->is_assoc_index_expr()) {
          ret = OB_NOT_SUPPORTED;
        } else if (T_SP_CPARAM == origin->get_expr_type()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "call procedure");
        } else if (T_FUN_PL_INTEGER_CHECKER == origin->get_expr_type()) {
          ret = OB_NOT_SUPPORTED;
        } else {
          ObOpRawExpr* dest_op = NULL;
          if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_op)) || OB_ISNULL(dest_op)) {
            LOG_WARN("failed to allocate raw expr", K(dest_op), K(ret));
          } else if (OB_FAIL(dest_op->deep_copy(expr_factory, *origin_op, COPY_REF_DEFAULT, use_new_allocator))) {
            LOG_WARN("failed to deep copy op expr", K(ret));
          } else {
            dest = dest_op;
          }
        }
        break;
      }
      case ObRawExpr::EXPR_CASE_OPERATOR: {
        ObCaseOpRawExpr* dest_case = NULL;
        const ObCaseOpRawExpr* origin_case = static_cast<const ObCaseOpRawExpr*>(origin);
        if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_case)) || OB_ISNULL(dest_case)) {
          LOG_WARN("failed to allocate raw expr", K(dest_case), K(ret));
        } else if (OB_FAIL(dest_case->deep_copy(expr_factory, *origin_case, COPY_REF_DEFAULT, use_new_allocator))) {
          LOG_WARN("failed to deep copy case expr", K(ret));
        } else {
          dest = dest_case;
        }
        break;
      }
      case ObRawExpr::EXPR_SYS_FUNC: {
        if (T_FUN_SYS_SEQ_NEXTVAL == origin->get_expr_type()) {
          ObSequenceRawExpr* dest_seq = NULL;
          const ObSequenceRawExpr* origin_seq = static_cast<const ObSequenceRawExpr*>(origin);
          if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_seq))) {
            LOG_WARN("failed to allocate raw expr", K(dest_seq), K(ret));
          } else if (OB_ISNULL(dest_seq)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is NULL", K(dest_seq), K(ret));
          } else if (OB_FAIL(dest_seq->deep_copy(expr_factory, *origin_seq, COPY_REF_DEFAULT, use_new_allocator))) {
            LOG_WARN("failed to deep copy sys func expr", K(ret));
          } else {
            dest = dest_seq;
          }
        } else if (T_FUN_NORMAL_UDF == origin->get_expr_type()) {
          ObNormalDllUdfRawExpr* dest_nudf = NULL;
          const ObNormalDllUdfRawExpr* origin_nudf = static_cast<const ObNormalDllUdfRawExpr*>(origin);
          if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_nudf))) {
            LOG_WARN("failed to allocate raw expr", K(dest_nudf), K(ret));
          } else if (OB_ISNULL(dest_nudf)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is NULL", K(dest_nudf), K(ret));
          } else if (OB_FAIL(dest_nudf->deep_copy(expr_factory, *origin_nudf, COPY_REF_DEFAULT, use_new_allocator))) {
            LOG_WARN("failed to deep copy sys func expr", K(ret));
          } else {
            dest = dest_nudf;
          }
        } else if (T_FUN_PL_COLLECTION_CONSTRUCT == origin->get_expr_type()) {
          ret = OB_NOT_SUPPORTED;
        } else if (T_FUN_PL_OBJECT_CONSTRUCT == origin->get_expr_type()) {
          ret = OB_NOT_SUPPORTED;
        } else if (T_FUN_PL_GET_CURSOR_ATTR == origin->get_expr_type()) {
          ret = OB_NOT_SUPPORTED;
        } else if (T_FUN_PL_SQLCODE_SQLERRM == origin->get_expr_type()) {
          ObPLSQLCodeSQLErrmRawExpr* dest_scse = NULL;
          const ObPLSQLCodeSQLErrmRawExpr* origin_scse = static_cast<const ObPLSQLCodeSQLErrmRawExpr*>(origin);
          if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_scse))) {
            LOG_WARN("failed to allocate raw expr", K(dest_scse), K(ret));
          } else if (OB_ISNULL(dest_scse)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is NULL", K(dest_scse), K(ret));
          } else if (OB_FAIL(dest_scse->deep_copy(expr_factory, *origin_scse, COPY_REF_DEFAULT, use_new_allocator))) {
            LOG_WARN("failed to deep copy sys func expr", K(ret));
          } else {
            dest = dest_scse;
          }
        } else {
          ObSysFunRawExpr* dest_sys = NULL;
          const ObSysFunRawExpr* origin_sys = static_cast<const ObSysFunRawExpr*>(origin);
          if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_sys))) {
            LOG_WARN("failed to allocate raw expr", K(dest_sys), K(ret));
          } else if (OB_ISNULL(dest_sys)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is NULL", K(dest_sys), K(ret));
          } else if (OB_FAIL(dest_sys->deep_copy(expr_factory, *origin_sys, COPY_REF_DEFAULT, use_new_allocator))) {
            LOG_WARN("failed to deep copy sys func expr", K(ret));
          } else {
            dest = dest_sys;
          }
        }
        break;
      }
      case ObRawExpr::EXPR_UDF: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case ObRawExpr::EXPR_ALIAS_REF: {
        ObAliasRefRawExpr* dest_alias = NULL;
        const ObAliasRefRawExpr* origin_alias = static_cast<const ObAliasRefRawExpr*>(origin);
        if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_alias)) || OB_ISNULL(dest_alias)) {
          LOG_WARN("failed to allocate raw expr", K(dest_alias), K(ret));
        } else if (OB_FAIL(dest_alias->deep_copy(expr_factory, *origin_alias, COPY_REF_DEFAULT, use_new_allocator))) {
          LOG_WARN("failed to copy alias", K(ret));
        } else {
          dest = dest_alias;
        }
        break;
      }
      case ObRawExpr::EXPR_PSEUDO_COLUMN: {
        ObPseudoColumnRawExpr* dest_pseudo_column = NULL;
        const ObPseudoColumnRawExpr* origin_pseudo_column = static_cast<const ObPseudoColumnRawExpr*>(origin);
        if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_pseudo_column)) ||
            OB_ISNULL(dest_pseudo_column)) {
          LOG_WARN("failed to alocate raw expr", K(ret));
        } else if (OB_FAIL(dest_pseudo_column->deep_copy(
                       expr_factory, *origin_pseudo_column, COPY_REF_DEFAULT, use_new_allocator))) {
          LOG_WARN("failed to assign pseudo column", K(ret));
        } else {
          dest = dest_pseudo_column;
        }
        break;
      }
      case ObRawExpr::EXPR_WINDOW: {
        ObWinFunRawExpr* dest_win_fun = NULL;
        const ObWinFunRawExpr* origin_win_fun = static_cast<const ObWinFunRawExpr*>(origin);
        if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_win_fun)) || OB_ISNULL(dest_win_fun)) {
          LOG_WARN("failed to allocate raw expr", K(dest_win_fun), K(ret));
        } else if (OB_FAIL(
                       dest_win_fun->deep_copy(expr_factory, *origin_win_fun, COPY_REF_DEFAULT, use_new_allocator))) {
          LOG_WARN("failed to deep copy window function", K(ret));
        } else {
          dest = dest_win_fun;
        }
        break;
      }
      case ObRawExpr::EXPR_DOMAIN_INDEX: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported deep copy of domain index expr", K(ret));
        break;
      }
      case ObRawExpr::EXPR_VAR: {
        ObVarRawExpr* dest_var_expr = NULL;
        const ObVarRawExpr* origin_var_expr = static_cast<const ObVarRawExpr*>(origin);
        if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_var_expr)) || OB_ISNULL(dest_var_expr)) {
          LOG_WARN("failed to allocate raw expr", K(dest_var_expr), K(ret));
        } else if (OB_FAIL(
                       dest_var_expr->deep_copy(expr_factory, *origin_var_expr, COPY_REF_DEFAULT, use_new_allocator))) {
          LOG_WARN("failed to deep copy var raw expr", K(ret));
        } else {
          dest = dest_var_expr;
        }
        break;
      }
      case ObRawExpr::EXPR_SET_OP: {
        ObSetOpRawExpr* dest_set_op_expr = NULL;
        const ObSetOpRawExpr* origin_set_op_expr = static_cast<const ObSetOpRawExpr*>(origin);
        if (OB_FAIL(expr_factory.create_raw_expr(origin->get_expr_type(), dest_set_op_expr)) ||
            OB_ISNULL(dest_set_op_expr)) {
          LOG_WARN("failed to allocate raw expr", K(dest_set_op_expr), K(ret));
        } else if (OB_FAIL(dest_set_op_expr->deep_copy(
                       expr_factory, *origin_set_op_expr, COPY_REF_DEFAULT, use_new_allocator))) {
          LOG_WARN("failed to deep copy set op raw expr", K(ret));
        } else {
          dest = dest_set_op_expr;
        }
        break;
      }
      case ObRawExpr::EXPR_INVALID_CLASS:
      default:
        // should not reach here
        break;
    }
  }
  return ret;
}

bool ObRawExprUtils::need_column_conv(const ObExprResType& expected_type, const ObRawExpr& expr)
{
  int bret = true;
  if (expected_type.get_type() == expr.get_data_type()) {
    bool type_matched = false;
    if (expected_type.is_integer_type() || expected_type.is_temporal_type()) {
      type_matched = true;
    } else if (expected_type.get_collation_type() == expr.get_collation_type() &&
               expected_type.get_accuracy().get_accuracy() == expr.get_accuracy().get_accuracy()) {
      type_matched = true;
    }
    if (type_matched) {
      if (expected_type.has_result_flag(OB_MYSQL_NOT_NULL_FLAG) &&
          expr.get_result_type().has_result_flag(OB_MYSQL_NOT_NULL_FLAG)) {
        bret = false;
      } else if (!expected_type.has_result_flag(OB_MYSQL_NOT_NULL_FLAG)) {
        bret = false;
      } else { /*do nothing*/
      }
    }
  }
  return bret;
}

bool ObRawExprUtils::need_column_conv(const ColumnItem& column, ObRawExpr& expr)
{
  int bret = true;
  if (column.get_expr() != NULL && column.get_expr()->is_fulltext_column()) {
    // generated column of fulltext idnex is a inner hidden column, don't need to do column convert.
    bret = false;
  } else if (column.get_column_type() != NULL) {
    const ObExprResType& column_type = *column.get_column_type();
    if (column_type.get_type() == expr.get_data_type() &&
        column_type.get_collation_type() == expr.get_collation_type() &&
        column_type.get_accuracy().get_accuracy() == expr.get_accuracy().get_accuracy()) {
      if (column.is_not_null() && expr.get_result_type().has_result_flag(OB_MYSQL_NOT_NULL_FLAG)) {
        bret = false;
      } else if (!column.is_not_null()) {
        bret = false;
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }
  return bret;
}
int ObRawExprUtils::build_column_conv_expr(ObRawExprFactory& expr_factory,
    const share::schema::ObColumnSchemaV2* column_schema, ObRawExpr*& expr, const ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(session_info));
  CK(OB_NOT_NULL(column_schema));
  if (OB_SUCC(ret)) {
    if (column_schema->is_fulltext_column()) {
    } else if (OB_FAIL(build_column_conv_expr(session_info,
                   expr_factory,
                   column_schema->get_data_type(),
                   column_schema->get_collation_type(),
                   column_schema->get_accuracy().get_accuracy(),
                   column_schema->is_nullable(),
                   NULL,
                   NULL,
                   expr))) {
      LOG_WARN("failed to build column convert expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::build_column_conv_expr(ObRawExprFactory& expr_factory, common::ObIAllocator& allocator,
    const ObColumnRefRawExpr& col_ref, ObRawExpr*& expr, const ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  ObString column_conv_info;
  const ObString& database_name = col_ref.get_database_name();
  const ObString& table_name = col_ref.get_table_name();
  const ObString& column_name = col_ref.get_column_name();
  int64_t buff_len = database_name.length() + table_name.length() + column_name.length() + 20;
  char* temp_str_buf = static_cast<char*>(allocator.alloc(buff_len));
  if (OB_UNLIKELY(OB_ISNULL(temp_str_buf))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (snprintf(temp_str_buf,
                 buff_len,
                 "\"%.*s\".\"%.*s\".\"%.*s\"",
                 database_name.length(),
                 database_name.ptr(),
                 table_name.length(),
                 table_name.ptr(),
                 column_name.length(),
                 column_name.ptr()) < 0) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("failed to generate buffer for temp_str_buf", K(ret));
  } else {
    column_conv_info = ObString(buff_len, static_cast<int32_t>(strlen(temp_str_buf)), temp_str_buf);
  }
  CK(session_info);
  if (OB_SUCC(ret)) {
    if (col_ref.is_fulltext_column()) {
    } else if (OB_FAIL(build_column_conv_expr(session_info,
                   expr_factory,
                   col_ref.get_data_type(),
                   col_ref.get_collation_type(),
                   col_ref.get_accuracy().get_accuracy(),
                   !col_ref.is_not_null(),
                   &column_conv_info,
                   &col_ref.get_enum_set_values(),
                   expr))) {
      LOG_WARN("fail to build column convert expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::build_column_conv_expr(const ObSQLSessionInfo* session_info, ObRawExprFactory& expr_factory,
    const ObObjType& type, const ObCollationType& collation, const int64_t& accuracy, const bool& is_nullable,
    const common::ObString* column_conv_info, const ObIArray<ObString>* type_infos, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObObjType dest_type = ObLobType == type ? ObLongTextType : type;
  ObSysFunRawExpr* f_expr = NULL;
  ObConstRawExpr* is_nullable_expr = NULL;
  ObConstRawExpr* type_expr = NULL;
  ObConstRawExpr* collation_expr = NULL;
  ObConstRawExpr* accuracy_expr = NULL;
  ObConstRawExpr* column_info_expr = NULL;
  uint64_t def_cast_mode = 0;
  CK(OB_NOT_NULL(session_info));
  CK(OB_NOT_NULL(expr));
  ObString column_info;
  if (OB_NOT_NULL(column_conv_info)) {
    column_info = *column_conv_info;
  }
  if (OB_FAIL(ret)) {
    // do nothing ...
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_COLUMN_CONV, f_expr))) {
    LOG_WARN("fail to create T_FUN_COLUMN_CONV raw expr", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory, ObInt32Type, static_cast<int32_t>(dest_type), type_expr))) {
    LOG_WARN("fail to build const int expr", K(ret));
  } else if (OB_FAIL(
                 build_const_int_expr(expr_factory, ObInt32Type, static_cast<int32_t>(collation), collation_expr))) {
    LOG_WARN("fail to build type expr", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory, ObIntType, accuracy, accuracy_expr))) {
    LOG_WARN("fail to build int expr", K(ret), K(accuracy));
  } else if (OB_FAIL(build_const_int_expr(expr_factory, ObTinyIntType, is_nullable, is_nullable_expr))) {
    LOG_WARN("fail to build bool expr", K(ret));
  } else if (OB_FAIL(build_const_string_expr(
                 expr_factory, ObCharType, column_info, CS_TYPE_UTF8MB4_GENERAL_CI, column_info_expr))) {
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
  } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2273 && OB_FAIL(f_expr->add_param_expr(column_info_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (ObSQLUtils::get_default_cast_mode(false, /* explicit_cast */
                 0,                                   /* result_flag */
                 session_info,
                 def_cast_mode)) {
    LOG_WARN("fail to get_default_cast_mode", K(ret));
  } else {
    if (ob_is_enumset_tc(dest_type) && OB_NOT_NULL(type_infos)) {
      ObExprColumnConv* column_conv = NULL;
      ObExprOperator* op = NULL;
      if (OB_ISNULL(op = f_expr->get_op())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate expr operator failed", K(ret));
      } else if (OB_ISNULL(column_conv = static_cast<ObExprColumnConv*>(op))) {
        LOG_WARN("fail to cast ObExprOperator* to ObExprColumnConv*", K(ret));
      } else if (OB_FAIL(column_conv->shallow_copy_str_values(*type_infos))) {
        LOG_WARN("fail to shallow_copy_str_values", K(ret));
      } else if (f_expr->set_enum_set_values(*type_infos)) {
        LOG_WARN("fail to set_enum_set_values", K(ret));
      } else { /*success*/
      }
    }
    if (OB_SUCC(ret)) {
      f_expr->set_extra(def_cast_mode);
      f_expr->set_func_name(ObString::make_string(N_COLUMN_CONV));
      f_expr->set_data_type(dest_type);
      f_expr->set_expr_type(T_FUN_COLUMN_CONV);
      if (expr->is_for_generated_column()) {
        f_expr->set_for_generated_column();
      }
      expr = f_expr;
      if (OB_FAIL(expr->formalize(session_info))) {
        LOG_WARN("fail to extract info", K(ret));
      }
    }
  }
  return ret;
}

// invoker should remember to add to stmt's expr_store to free the memory
// stmt->store_expr(expr)
int ObRawExprUtils::build_const_int_expr(
    ObRawExprFactory& expr_factory, ObObjType type, int64_t int_value, ObConstRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(static_cast<ObItemType>(type), c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    ObObj obj;
    obj.set_int(type, int_value);
    c_expr->set_value(obj);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_const_number_expr(
    ObRawExprFactory& expr_factory, ObObjType type, const number::ObNumber value, ObConstRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
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

int ObRawExprUtils::build_var_int_expr(ObRawExprFactory& expr_factory, ObConstRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
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

int ObRawExprUtils::build_const_string_expr(ObRawExprFactory& expr_factory, ObObjType type,
    const ObString& string_value, ObCollationType cs_type, ObConstRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
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

int ObRawExprUtils::build_variable_expr(
    ObRawExprFactory& expr_factory, const ObExprResType& result_type, ObVarRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObVarRawExpr* c_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_EXEC_VAR, c_expr))) {
    LOG_WARN("fail to create const raw c_expr", K(ret));
  } else {
    c_expr->set_result_type(result_type);
    expr = c_expr;
  }
  return ret;
}

int ObRawExprUtils::build_null_expr(ObRawExprFactory& expr_factory, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
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

int ObRawExprUtils::build_trim_expr(const ObColumnSchemaV2* column_schema, ObRawExprFactory& expr_factory,
    const ObSQLSessionInfo* session_info, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* trim_expr = NULL;
  int64_t trim_type = 2;
  ObConstRawExpr* type_expr = NULL;
  ObConstRawExpr* pattem_expr = NULL;
  ObString padding_char(1, &OB_PADDING_CHAR);
  ObCollationType padding_char_cs_type = CS_TYPE_INVALID;
  if (NULL == column_schema || NULL == expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_schema), K(expr));
  } else {
    bool is_cs_nonascii = ObCharset::is_cs_nonascii(column_schema->get_collation_type());
    padding_char_cs_type =
        is_cs_nonascii ? ObCharset::get_default_collation(CHARSET_UTF8MB4) : column_schema->get_collation_type();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_INNER_TRIM, trim_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(trim_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to store expr", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory, ObIntType, trim_type, type_expr))) {
    LOG_WARN("fail to build type expr", K(ret));
  } else if (trim_expr->add_param_expr(type_expr)) {
    LOG_WARN("fail to add param expr", K(ret), K(*type_expr));
  } else if (OB_FAIL(
                 build_const_string_expr(expr_factory, ObCharType, padding_char, padding_char_cs_type, pattem_expr))) {
    LOG_WARN("fail to build pattem expr", K(ret));
  } else if (FALSE_IT(static_cast<ObConstRawExpr*>(pattem_expr)->get_value().set_collation_level(CS_LEVEL_IMPLICIT))) {
    LOG_WARN("fail to set collation type", K(ret));
  } else if (trim_expr->add_param_expr(pattem_expr)) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (trim_expr->add_param_expr(expr)) {
    LOG_WARN("fail to add param expr", K(ret), K(*expr));
  } else {
    trim_expr->set_data_type(ObCharType);
    trim_expr->set_func_name(ObString::make_string(N_INNER_TRIM));
    if (expr->is_for_generated_column()) {
      trim_expr->set_for_generated_column();
    }
    expr = trim_expr;
    if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("fail to extract info", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::build_pad_expr(ObRawExprFactory& expr_factory, bool is_char, const ObColumnSchemaV2* column_schema,
    ObRawExpr*& expr, const sql::ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* pad_expr = NULL;
  ObConstRawExpr* pading_word_expr = NULL;
  ObConstRawExpr* length_expr = NULL;
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
  } else if (FALSE_IT(padding_expr_type =
                          ob_is_nstring_type(column_schema->get_data_type()) ? ObNVarchar2Type : ObVarcharType)) {
  } else if (FALSE_IT(padding_expr_collation = ob_is_nstring_type(column_schema->get_data_type())
                                                   ? CS_TYPE_UTF8MB4_BIN
                                                   : column_schema->get_collation_type())) {
  } else if (is_char && OB_FAIL(build_const_string_expr(
                            expr_factory, padding_expr_type, padding_char, padding_expr_collation, pading_word_expr))) {
    LOG_WARN("fail to build pading word expr", K(ret));
  } else if (!is_char &&
             OB_FAIL(build_const_string_expr(
                 expr_factory, padding_expr_type, padding_binary, padding_expr_collation, pading_word_expr))) {
    LOG_WARN("fail to build pading word expr", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory, ObIntType, column_schema->get_data_length(), length_expr))) {
    LOG_WARN("fail to build length expr", K(ret));
  } else if (OB_FAIL(pad_expr->add_param_expr(expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(pad_expr->add_param_expr(pading_word_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(pad_expr->add_param_expr(length_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else {
    ObAccuracy padding_accuracy = pading_word_expr->get_accuracy();
    padding_accuracy.set_length_semantics(column_schema->get_accuracy().get_length_semantics());
    pading_word_expr->set_accuracy(padding_accuracy);

    pad_expr->set_data_type(padding_expr_type);
    pad_expr->set_func_name(ObString::make_string(N_PAD));
    if (expr->is_for_generated_column()) {
      pad_expr->set_for_generated_column();
    }
    expr = pad_expr;
    if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("fail to extract info", K(ret));
    }
    LOG_DEBUG("build pad expr", KPC(pading_word_expr), KPC(pad_expr));
  }
  return ret;
}

int ObRawExprUtils::build_nvl_expr(ObRawExprFactory& expr_factory, const ColumnItem* column_item, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* nvl_func_expr = NULL;
  ObSysFunRawExpr* now_func_expr = NULL;
  if (NULL == column_item || NULL == expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail bo build length expr", K(column_item), K(expr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_TIMESTAMP_NVL, nvl_func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_CUR_TIMESTAMP, now_func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(nvl_func_expr) || OB_ISNULL(now_func_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func expr is null", K(nvl_func_expr), K(now_func_expr));
  } else {
    nvl_func_expr->set_expr_type(T_FUN_SYS_TIMESTAMP_NVL);
    nvl_func_expr->set_func_name(ObString::make_string(N_TIMESTAMP_NVL));
    nvl_func_expr->set_data_type(ObTimestampType);
    nvl_func_expr->set_accuracy(column_item->get_column_type()->get_accuracy());
    now_func_expr->set_expr_type(T_FUN_SYS_CUR_TIMESTAMP);
    now_func_expr->set_data_type(ObDateTimeType);
    now_func_expr->set_accuracy(column_item->get_column_type()->get_accuracy());  // the accuracy of column
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

int ObRawExprUtils::build_lnnvl_expr(ObRawExprFactory& expr_factory, ObRawExpr* param_expr, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* lnnvl_expr = NULL;
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

int ObRawExprUtils::build_equal_last_insert_id_expr(
    ObRawExprFactory& expr_factory, ObRawExpr*& expr, ObSQLSessionInfo* session)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* equal_expr = NULL;
  ObSysFunRawExpr* last_insert_id = NULL;
  if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->is_op_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr));
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

int ObRawExprUtils::build_get_user_var(ObRawExprFactory& expr_factory, const ObString& var_name, ObRawExpr*& expr,
    const ObSQLSessionInfo* session_info /* = NULL */, ObQueryCtx* query_ctx /* = NULL */,
    ObIArray<ObUserVarIdentRawExpr*>* user_var_exprs /* = NULL*/)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* f_expr = NULL;
  ObUserVarIdentRawExpr* var_expr = NULL;
  ObIArray<ObUserVarIdentRawExpr*>* all_vars = (NULL == query_ctx ? NULL : &query_ctx->all_user_variable_);
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
      } else if (NULL != user_var_exprs && OB_FAIL(add_var_to_array_no_dup(*user_var_exprs, var_expr))) {
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

int ObRawExprUtils::build_get_sys_var(ObRawExprFactory& expr_factory, const ObString& var_name,
    ObSetVar::SetScopeType var_scope, ObRawExpr*& expr, const ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* name_expr = NULL;
  ObConstRawExpr* scope_expr = NULL;
  ObSysFunRawExpr* get_sys_var_expr = NULL;
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

int ObRawExprUtils::build_calc_part_id_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session,
    uint64_t ref_table_id, ObPartitionLevel part_level, ObRawExpr* part_expr, ObRawExpr* subpart_expr, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  expr = NULL;
  ObSysFunRawExpr* calc_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS, calc_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(calc_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to create raw expr", K(calc_expr), K(ret));
  } else {
    if (schema::PARTITION_LEVEL_TWO == part_level) {
      if (OB_ISNULL(part_expr) || OB_ISNULL(subpart_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("subpart_expr is null", K(subpart_expr), K(ret));
      } else if (OB_FAIL(calc_expr->add_param_expr(part_expr)) || OB_FAIL(calc_expr->add_param_expr(subpart_expr))) {
        LOG_WARN("fail to add param expr", K(ret), K(*part_expr), K(*subpart_expr));
      }
    } else if (schema::PARTITION_LEVEL_ONE == part_level) {
      if (OB_ISNULL(part_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("part_expr is null", K(part_level), K(part_expr), K(ret));
      } else if (OB_FAIL(calc_expr->add_param_expr(part_expr))) {
        LOG_WARN("fail to add param expr", K(ret), K(*part_expr));
      }
    }
    if (OB_SUCC(ret)) {
      ObString func_name;
      if (OB_FAIL(ob_write_string(expr_factory.get_allocator(), ObString("calc_partition_id"), func_name))) {
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

int ObRawExprUtils::build_get_package_var(ObRawExprFactory& expr_factory, uint64_t package_id, int64_t var_idx,
    ObExprResType* result_type, ObRawExpr*& expr, const ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* f_expr = NULL;
  ObConstRawExpr* c_expr1 = NULL;
  ObConstRawExpr* c_expr2 = NULL;
  ObConstRawExpr* c_expr3 = NULL;
  OZ(expr_factory.create_raw_expr(T_OP_GET_PACKAGE_VAR, f_expr));
  OZ(build_const_int_expr(expr_factory, ObUInt64Type, package_id, c_expr1));
  OZ(f_expr->add_param_expr(c_expr1));
  OZ(build_const_int_expr(expr_factory, ObIntType, var_idx, c_expr2));
  OZ(f_expr->add_param_expr(c_expr2));
  OZ(build_const_int_expr(expr_factory, ObIntType, reinterpret_cast<int64>(result_type), c_expr3));
  OZ(f_expr->add_param_expr(c_expr3));
  OX(f_expr->set_func_name(ObString::make_string(N_GET_PACKAGE_VAR)));
  OX(expr = f_expr);
  CK(OB_NOT_NULL(session_info));
  OZ(expr->formalize(session_info));
  return ret;
}

int ObRawExprUtils::build_get_subprogram_var(ObRawExprFactory& expr_factory, uint64_t package_id, uint64_t routine_id,
    int64_t var_idx, ObExprResType* result_type, ObRawExpr*& expr, const ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* f_expr = NULL;
  ObConstRawExpr* c_expr1 = NULL;
  ObConstRawExpr* c_expr2 = NULL;
  ObConstRawExpr* c_expr3 = NULL;
  ObConstRawExpr* c_expr4 = NULL;
  OZ(expr_factory.create_raw_expr(T_OP_GET_SUBPROGRAM_VAR, f_expr));
  OZ(build_const_int_expr(expr_factory, ObUInt64Type, package_id, c_expr1));
  OZ(f_expr->add_param_expr(c_expr1));
  OZ(build_const_int_expr(expr_factory, ObUInt64Type, routine_id, c_expr2));
  OZ(f_expr->add_param_expr(c_expr2));
  OZ(build_const_int_expr(expr_factory, ObIntType, var_idx, c_expr3));
  OZ(f_expr->add_param_expr(c_expr3));
  OZ(build_const_int_expr(expr_factory, ObIntType, reinterpret_cast<int64>(result_type), c_expr4));
  OZ(f_expr->add_param_expr(c_expr4));
  OX(f_expr->set_func_name(ObString::make_string(N_GET_SUBPROGRAM_VAR)));
  OX(expr = f_expr);
  CK(OB_NOT_NULL(session_info));
  OZ(expr->formalize(session_info));
  return ret;
}

int ObRawExprUtils::create_equal_expr(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
    const ObRawExpr* val_ref, const ObRawExpr* col_ref, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* equal_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_EQ, equal_expr))) {
    LOG_WARN("create equal expr failed", K(ret));
  } else if (OB_ISNULL(expr = equal_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("equal expr is null");
  } else if (OB_FAIL(equal_expr->set_param_exprs(const_cast<ObRawExpr*>(col_ref), const_cast<ObRawExpr*>(val_ref)))) {
    LOG_WARN("set param expr failed", K(ret));
  } else if (OB_FAIL(equal_expr->formalize(session_info))) {
    LOG_WARN("formalize equal expr failed", K(ret));
  } else {
  }
  return ret;
}

int ObRawExprUtils::create_double_op_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info,
    ObItemType expr_type, ObRawExpr*& add_expr, const ObRawExpr* left_expr, const ObRawExpr* right_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* op_expr = NULL;
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

int ObRawExprUtils::make_set_op_expr(ObRawExprFactory& expr_factory, int64_t idx, ObItemType set_op_type,
    const ObExprResType& res_type, ObSQLSessionInfo* session_info, ObRawExpr*& out_expr)
{
  int ret = OB_SUCCESS;
  ObSetOpRawExpr* set_expr = NULL;
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

int ObRawExprUtils::merge_variables(const ObIArray<ObVarInfo>& src_vars, ObIArray<ObVarInfo>& dst_vars)
{
  int ret = OB_SUCCESS;
  int64_t N = src_vars.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObVarInfo& var = src_vars.at(i);
    bool found = false;
    int64_t M = dst_vars.count();
    for (int64_t j = 0; j < M; ++j) {
      if (var == dst_vars.at(j)) {
        found = true;
        break;
      }
    }  // end for
    if (!found) {
      ret = dst_vars.push_back(var);
    }
  }  // end for
  return ret;
}

int ObRawExprUtils::get_array_param_index(const ObRawExpr* expr, int64_t& param_index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr));
  } else if (expr->is_const_expr()) {
    if (expr->get_result_type().get_param().is_ext()) {
      const ObConstRawExpr* c_expr = static_cast<const ObConstRawExpr*>(expr);
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

int ObRawExprUtils::get_item_count(const ObRawExpr* expr, int64_t& count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr));
  } else if (expr->is_column_ref_expr()) {
    const ObColumnRefRawExpr* col_ref = static_cast<const ObColumnRefRawExpr*>(expr);
    if (col_ref->is_generated_column()) {
      ret = get_item_count(col_ref->get_dependant_expr(), count);
    } else {
      ++count;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalie expr", K(i), K(ret));
      } else if (OB_FAIL(get_item_count(expr->get_param_expr(i), count))) {
        LOG_WARN("fail to get item count", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      count++;
    }
  }
  return ret;
}

int ObRawExprUtils::build_column_expr(
    ObRawExprFactory& expr_factory, const ObColumnSchemaV2& column_schema, ObColumnRefRawExpr*& column_expr)
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

int ObRawExprUtils::build_alias_column_expr(
    ObRawExprFactory& expr_factory, ObRawExpr* ref_expr, int32_t alias_level, ObAliasRefRawExpr*& alias_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_factory.create_raw_expr(T_REF_ALIAS_COLUMN, alias_expr))) {
    LOG_WARN("create alias column expr failed", K(ret));
  } else if (OB_ISNULL(alias_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alias column expr is null", K(ret));
  } else {
    alias_expr->set_expr_level(alias_level);
    alias_expr->set_ref_expr(ref_expr);
  }
  return ret;
}

int ObRawExprUtils::build_query_output_ref(
    ObRawExprFactory& expr_factory, ObQueryRefRawExpr* query_ref, int64_t project_index, ObAliasRefRawExpr*& alias_expr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* sel_stmt = NULL;
  ObRawExpr* real_ref_expr = NULL;
  if (OB_ISNULL(query_ref) || OB_ISNULL(sel_stmt = query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ref expr is invalid", K(ret), K(query_ref), K(sel_stmt));
  } else if (project_index < 0 || project_index >= query_ref->get_ref_stmt()->get_select_item_size() ||
             OB_ISNULL(real_ref_expr = sel_stmt->get_select_item(project_index).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("project index is invalid", K(ret), K(project_index), K(real_ref_expr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_REF_ALIAS_COLUMN, alias_expr))) {
    LOG_WARN("create alias column expr failed", K(ret));
  } else if (OB_ISNULL(alias_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alias column expr is null", K(ret));
  } else {
    alias_expr->set_expr_level(query_ref->get_expr_level());
    alias_expr->set_query_output(query_ref, project_index);
    alias_expr->set_result_type(real_ref_expr->get_result_type());
  }
  return ret;
}

bool ObRawExprUtils::is_same_column_ref(const ObRawExpr* column_ref1, const ObRawExpr* column_ref2)
{
  // need to compare real node in alias ref.
  const ObRawExpr* left_real_ref = column_ref1;
  const ObRawExpr* right_real_ref = column_ref2;
  while (left_real_ref != NULL && T_REF_ALIAS_COLUMN == left_real_ref->get_expr_type()) {
    left_real_ref = static_cast<const ObAliasRefRawExpr*>(left_real_ref)->get_ref_expr();
  }
  while (right_real_ref != NULL && T_REF_ALIAS_COLUMN == right_real_ref->get_expr_type()) {
    right_real_ref = static_cast<const ObAliasRefRawExpr*>(right_real_ref)->get_ref_expr();
  }
  bool bret = (left_real_ref == right_real_ref);
  return bret;
}

int32_t ObRawExprUtils::get_generalized_column_level(const ObRawExpr& generalized_column)
{
  int32_t column_level = -1;
  if (generalized_column.is_column_ref_expr()) {
    column_level = static_cast<const ObColumnRefRawExpr&>(generalized_column).get_expr_level();
  } else if (generalized_column.is_query_ref_expr()) {
    column_level = static_cast<const ObQueryRefRawExpr&>(generalized_column).get_expr_level();
  } else if (generalized_column.is_aggr_expr()) {
    column_level = static_cast<const ObAggFunRawExpr&>(generalized_column).get_expr_level();
  } else if (generalized_column.is_set_op_expr()) {
    column_level = static_cast<const ObSetOpRawExpr&>(generalized_column).get_expr_level();
  }
  return column_level;
}

int ObRawExprUtils::init_column_expr(const ObColumnSchemaV2& column_schema, ObColumnRefRawExpr& column_expr)
{
  int ret = OB_SUCCESS;
  const ObAccuracy& accuracy = column_schema.get_accuracy();
  column_expr.set_ref_id(column_schema.get_table_id(), column_schema.get_column_id());
  column_expr.set_data_type(column_schema.get_data_type());
  column_expr.set_result_flag(calc_column_result_flag(column_schema));
  column_expr.get_column_name().assign_ptr(
      column_schema.get_column_name_str().ptr(), column_schema.get_column_name_str().length());
  column_expr.set_column_flags(column_schema.get_column_flags());
  column_expr.set_hidden_column(column_schema.is_hidden());
  if (ob_is_string_type(column_schema.get_data_type()) 
      || ob_is_enumset_tc(column_schema.get_data_type())
      || ob_is_json_tc(column_schema.get_data_type())) {
    column_expr.set_collation_type(column_schema.get_collation_type());
    column_expr.set_collation_level(CS_LEVEL_IMPLICIT);
  } else {
    column_expr.set_collation_type(CS_TYPE_BINARY);
    column_expr.set_collation_level(CS_LEVEL_NUMERIC);
  }
  if (OB_SUCC(ret)) {
    column_expr.set_accuracy(accuracy);
    if (OB_FAIL(column_expr.extract_info())) {
      LOG_WARN("extract column expr info failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && column_schema.is_enum_or_set()) {
    if (OB_FAIL(column_expr.set_enum_set_values(column_schema.get_extended_type_info()))) {
      LOG_WARN("failed to set enum set values", K(ret));
    }
  }
  return ret;
}

int ObRawExprUtils::expr_is_order_consistent(const ObRawExpr* from, const ObRawExpr* to, bool& is_consistent)
{
  int ret = OB_SUCCESS;
  is_consistent = false;
  if (OB_ISNULL(from) || OB_ISNULL(to)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is null", K(from), K(to));
  } else if (OB_FAIL(ObObjCaster::is_order_consistent(from->get_result_type(), to->get_result_type(), is_consistent))) {
    LOG_WARN("check is order consistent failed", K(ret));
  }
  return ret;
}

int ObRawExprUtils::exprs_contain_subquery(const ObIArray<ObRawExpr*>& exprs, bool& cnt_subquery)
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

uint32_t ObRawExprUtils::calc_column_result_flag(const ObColumnSchemaV2& column_schema)
{
  uint32_t flag = 0;
  if (column_schema.is_autoincrement()) {
    flag |= OB_MYSQL_AUTO_INCREMENT_FLAG;
  }
  if (!column_schema.is_nullable()) {
    flag |= OB_MYSQL_NOT_NULL_FLAG;
  }
  if (column_schema.is_rowkey_column()) {
    flag |= OB_MYSQL_PRI_KEY_FLAG;
    flag |= OB_MYSQL_PART_KEY_FLAG;
  }
  if (column_schema.is_index_column()) {
    flag |= OB_MYSQL_PART_KEY_FLAG;
    if (column_schema.get_index_position() == 1) {
      flag |= OB_MYSQL_MULTIPLE_KEY_FLAG;
    }
  }
  if (column_schema.is_zero_fill()) {
    flag |= OB_MYSQL_ZEROFILL_FLAG;
  }
  return flag;
}

int ObRawExprUtils::extract_int_value(const ObRawExpr* expr, int64_t& val)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_ISNULL(expr) || !expr->is_const_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr should not be null", K(ret), K(expr));
  } else {
    const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(expr);
    if (OB_ISNULL(const_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("const expr should not be null", K(ret));
    } else {
      const ObObj& result = const_expr->get_value();
      if (OB_FAIL(result.get_int(count))) {
        LOG_WARN("failed to get int", K(ret));
      } else {
        val = count;
      }
    }
  }
  return ret;
}

int ObRawExprUtils::need_wrap_to_string(
    ObObjType param_type, ObObjType calc_type, const bool is_same_type_need, bool& need_wrap)
{
  int ret = OB_SUCCESS;
  need_wrap = false;
  if (!ob_is_enumset_tc(param_type)) {
    // only need to convert when parameter is enum type.
  } else if (param_type == calc_type && (!is_same_type_need)) {
    need_wrap = false;
  } else {
    switch (calc_type) {
      case ObNullType:
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType:
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type:
      case ObFloatType:
      case ObDoubleType:
      case ObUFloatType:
      case ObUDoubleType:
      case ObNumberType:
      case ObUNumberType:
      case ObNumberFloatType:
      case ObBitType:
      case ObYearType: {
        need_wrap = false;
        break;
      }
      case ObJsonType:
      case ObDateTimeType:
      case ObTimestampType:
      case ObDateType:
      case ObTimeType:
      case ObVarcharType:
      case ObCharType:
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType:
      case ObEnumType:
      case ObSetType:
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
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid calc_type", K(param_type), K(calc_type), K(ret));
      }
    }
  }
  LOG_DEBUG("finish need_wrap_to_string", K(need_wrap), K(param_type), K(calc_type), K(ret), K(lbt()));
  return ret;
}

int ObRawExprUtils::extract_param_idxs(const ObRawExpr* expr, ObIArray<int64_t>& param_idxs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (expr->get_expr_type() == T_QUESTIONMARK) {
    const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(expr);
    const ObObj& val = const_expr->get_value();
    int64_t param_idx = OB_INVALID_INDEX;
    if (OB_FAIL(val.get_unknown(param_idx))) {
      LOG_WARN("get unknown of value failed", K(ret));
    } else if (OB_FAIL(param_idxs.push_back(param_idx))) {
      LOG_WARN("store param idx failed", K(ret));
    }
  } else if (expr->has_flag(CNT_PARAM)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(extract_param_idxs(expr->get_param_expr(i), param_idxs))) {
        LOG_WARN("extract param idxs failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprUtils::find_alias_expr(ObRawExpr* expr, ObAliasRefRawExpr*& alias_expr)
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

bool ObRawExprUtils::contain_id(const ObIArray<uint64_t>& ids, const uint64_t target)
{
  bool found = false;
  for (int64_t i = 0; !found && i < ids.count(); ++i) {
    if (ids.at(i) == target) {
      found = true;
    }
  }
  return found;
}

int ObRawExprUtils::clear_exprs_flag(const ObIArray<ObRawExpr*>& exprs, ObExprInfoFlag flag)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(exprs, i)
  {
    ObRawExpr* raw_expr = exprs.at(i);
    if (OB_ISNULL(raw_expr)) {
      LOG_WARN("get output expr fail", K(i), K(exprs));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(raw_expr->clear_flag(flag))) {
      LOG_WARN("fail to clear flag", K(ret));
    } else {
    }
  }
  return ret;
}

bool ObRawExprUtils::has_prefix_str_expr(
    const ObRawExpr& expr, const ObColumnRefRawExpr& orig_column_expr, const ObRawExpr*& substr_expr)
{
  bool bret = false;
  const ObRawExpr* tmp = &expr;
  if (T_FUN_COLUMN_CONV == expr.get_expr_type()) {
    tmp = expr.get_param_expr(4);
  }
  if (T_FUN_SYS_SUBSTR == tmp->get_expr_type()) {
    const ObRawExpr* param_expr1 = tmp->get_param_expr(0);
    if (param_expr1 != NULL && param_expr1->is_column_ref_expr() &&
        param_expr1->get_result_type().is_string_or_lob_locator_type() && param_expr1->same_as(orig_column_expr)) {
      if (3 == tmp->get_param_count()) {
        const ObRawExpr* param_expr2 = tmp->get_param_expr(1);
        if (param_expr2 != NULL && param_expr2->is_const_expr()) {
          const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(param_expr2);
          if (const_expr->get_value().is_int() && const_expr->get_value().get_int() == 1) {
            bret = true;
            substr_expr = tmp;
          }
        }
      }
    }
  }
  return bret;
}

int ObRawExprUtils::build_like_expr(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
    ObRawExpr* text_expr, ObRawExpr* pattern_expr, ObRawExpr* escape_expr, ObOpRawExpr*& like_expr)
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

int ObRawExprUtils::replace_level_column(ObRawExpr*& raw_expr, ObRawExpr* to, bool& replaced)
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
      ObRawExpr*& child_expr = raw_expr->get_param_expr(i);
      if (OB_FAIL(replace_level_column(child_expr, to, replaced))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    }  // end for
  }
  return ret;
}
int ObRawExprUtils::build_const_bool_expr(ObRawExprFactory* expr_factory, ObRawExpr*& expr, bool b_value)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* bool_expr = NULL;
  if (OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_factory is null", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_BOOL, bool_expr))) {
    LOG_WARN("build const bool expr failed", K(ret));
  } else if (OB_ISNULL(bool_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bool expr is null", K(ret));
  } else {
    ObObj val;
    val.set_bool(b_value);
    bool_expr->set_value(val);
    expr = bool_expr;
  }
  return ret;
}

int ObRawExprUtils::check_composite_cast(ObRawExpr*& expr, ObSchemaChecker& schema_checker)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret));
  } else if (T_FUN_SYS_CAST == expr->get_expr_type()) {
    CK(OB_NOT_NULL(expr->get_param_expr(0)), OB_NOT_NULL(expr->get_param_expr(1)));
    CK(expr->get_param_expr(1)->is_const_expr());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      OZ(SMART_CALL(check_composite_cast(expr->get_param_expr(i), schema_checker)));
    }
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

int ObRawExprUtils::flatten_raw_exprs(
    const ObIArray<ObRawExpr*>& raw_exprs, ObRawExprUniqueSet& flattened_exprs, std::function<bool(ObRawExpr*)> filter)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    if (OB_FAIL(flatten_raw_expr(raw_exprs.at(i), flattened_exprs, filter))) {
      LOG_WARN("fail to flatten raw expr", K(ret));
    }
  }

  return ret;
}

int ObRawExprUtils::flatten_raw_exprs(
    const ObRawExprUniqueSet& raw_exprs, ObRawExprUniqueSet& flattened_exprs, std::function<bool(ObRawExpr*)> filter)
{
  return flatten_raw_exprs(raw_exprs.get_expr_array(), flattened_exprs, filter);
}

int ObRawExprUtils::flatten_raw_expr(
    ObRawExpr* raw_expr, ObRawExprUniqueSet& flattened_exprs, std::function<bool(ObRawExpr*)> filter)
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
  } else if (OB_FAIL(flattened_exprs.append(raw_expr))) {
    LOG_WARN("fail to push raw expr", K(ret), K(raw_expr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); i++) {
      if (OB_ISNULL(raw_expr->get_param_expr(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("expr is null");
      } else if (OB_FAIL(SMART_CALL(flatten_raw_expr(raw_expr->get_param_expr(i), flattened_exprs, filter)))) {
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
      } else if (OB_FAIL(SMART_CALL(flatten_raw_expr(v_raw_expr, flattened_exprs, filter)))) {
        LOG_WARN("fail to flatten raw expr", K(ret), K(v_raw_expr));
      } else if (OB_FAIL(SMART_CALL(flatten_raw_expr(d_v_raw_expr, flattened_exprs, filter)))) {
        LOG_WARN("fail to flatten raw expr", K(ret), K(d_v_raw_expr));
      }
    }
    // flatten dependent expr
    if (OB_SUCC(ret) && T_REF_COLUMN == raw_expr->get_expr_type()) {
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
      ObRawExpr* dependant_expr = col_expr->get_dependant_expr();
      if (NULL == dependant_expr) {
        // do nothing
      } else if (OB_FAIL(SMART_CALL(flatten_raw_expr(dependant_expr, flattened_exprs, filter)))) {
        LOG_WARN("failed to flatten raw expr", K(ret), K(*dependant_expr));
      }
    }
  }

  return ret;
}

int ObRawExprUtils::try_add_bool_expr(ObCaseOpRawExpr* parent, ObRawExprFactory& expr_factory)
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
      ObRawExpr* when_expr = parent->get_when_param_expr(i);
      ObRawExpr* new_when_expr = NULL;
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

int ObRawExprUtils::try_add_bool_expr(ObOpRawExpr* parent, ObRawExprFactory& expr_factory)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in expr is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent->get_param_count(); ++i) {
      ObRawExpr* param_expr = parent->get_param_expr(i);
      ObRawExpr* new_param_expr = NULL;
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

int ObRawExprUtils::try_create_bool_expr(ObRawExpr* src_expr, ObRawExpr*& out_expr, ObRawExprFactory& expr_factory)
{
  int ret = OB_SUCCESS;
  out_expr = NULL;
  if (OB_ISNULL(src_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("in expr is NULL", K(ret));
  } else {
    ObOpRawExpr* bool_expr = NULL;
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

bool ObRawExprUtils::check_need_bool_expr(const ObRawExpr* expr, bool& need_bool_expr)
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

      // all int is 8 byte in datum, so safe for expr to use get_int() for tinyint type
      case T_TINYINT:
      case T_SMALLINT:
      case T_MEDIUMINT:
      case T_INT32:
      case T_INT:

      case T_UTINYINT:
      case T_USMALLINT:
      case T_UMEDIUMINT:
      case T_UINT32:
      case T_UINT64:

      case T_OP_XOR:
      case T_OP_BOOL: {
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

int ObRawExprUtils::get_real_expr_without_cast(
    const bool is_static_typing_engine, const ObRawExpr* expr, const ObRawExpr*& out_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is NULL", K(ret));
  } else {
    if (is_static_typing_engine) {
      while (T_FUN_SYS_CAST == expr->get_expr_type() && CM_IS_IMPLICIT_CAST(expr->get_extra())) {
        if (OB_UNLIKELY(2 != expr->get_param_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param_count of cast expr should be 2", K(ret), K(*expr));
        } else if (OB_ISNULL(expr = expr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("first child of cast expr is NULL", K(ret), K(*expr));
        }
      }
    }
    if (OB_SUCC(ret)) {
      out_expr = expr;
      LOG_DEBUG("get_real_expr_without_cast done", K(*out_expr));
    }
  }
  return ret;
}

int ObRawExprUtils::build_remove_const_expr(
    ObRawExprFactory& factory, ObSQLSessionInfo& session_info, ObRawExpr* arg, ObRawExpr*& out)
{
  int ret = OB_SUCCESS;
  CK(NULL != arg);
  if (OB_SUCC(ret)) {
    ObSysFunRawExpr* rc_expr = NULL;
    OZ(factory.create_raw_expr(T_FUN_SYS_REMOVE_CONST, rc_expr));
    CK(NULL != rc_expr);
    OZ(rc_expr->add_param_expr(arg));
    OX(rc_expr->set_func_name(ObString::make_string(N_REMOVE_CONST)));
    OZ(rc_expr->formalize(&session_info));
    OZ(rc_expr->add_flag(IS_INNER_ADDED_EXPR));
    OX(out = rc_expr);
  }
  return ret;
};

bool ObRawExprUtils::is_pseudo_column_like_expr(const ObRawExpr& expr)
{
  bool bret = false;
  if (ObRawExpr::EXPR_PSEUDO_COLUMN == expr.get_expr_class() || T_FUN_SYS_ROWNUM == expr.get_expr_type() ||
      T_FUN_SYS_SEQ_NEXTVAL == expr.get_expr_type()) {
    bret = true;
  }
  return bret;
}

bool ObRawExprUtils::is_sharable_expr(const ObRawExpr& expr)
{
  int bret = false;
  if (expr.is_query_ref_expr() || expr.is_column_ref_expr() || expr.is_aggr_expr() || expr.is_win_func_expr() ||
      is_pseudo_column_like_expr(expr)) {
    bret = true;
  }
  return bret;
}

int ObRawExprUtils::check_need_cast_expr(const ObExprResType& src_type, const ObExprResType& dst_type, bool& need_cast)
{
  int ret = OB_SUCCESS;
  need_cast = false;
  ObObjType in_type = src_type.get_type();
  ObObjType out_type = dst_type.get_type();
  ObCollationType in_cs_type = src_type.get_collation_type();
  ObCollationType out_cs_type = dst_type.get_collation_type();
  bool is_both_string = (ob_is_string_type(in_type) && ob_is_string_type(out_type));
  const bool is_same_need = true;
  bool need_wrap = false;

  if (!ob_is_valid_obj_type(in_type) || !ob_is_valid_obj_type(out_type) ||
      (ob_is_string_type(in_type) && !ObCharset::is_valid_collation(in_cs_type)) ||
      (ob_is_string_type(out_type) && !ObCharset::is_valid_collation(out_cs_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src type or dst type", K(ret), K(src_type), K(dst_type));
  } else if ((!is_both_string && in_type == out_type) ||
             (is_both_string && in_type == out_type && in_cs_type == out_cs_type)) {
    need_cast = false;
  } else if (ob_is_enumset_tc(out_type)) {
    // no need add cast, will add column_conv later
    need_cast = false;
  } else if (OB_FAIL(ObRawExprUtils::need_wrap_to_string(in_type, out_type, is_same_need, need_wrap))) {
    LOG_WARN("failed to check_need_wrap_to_string", K(ret));
  } else if (need_wrap) {
    // no need add cast, add enumset_str expr later
    need_cast = false;
  } else if (!cast_supported(in_type, in_cs_type, out_type, out_cs_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("transition does not support", K(in_type), K(out_type));
  } else if (src_type.is_null() || dst_type.is_null()) {
    // null -> xxx or xxx -> null
    // xxx->null will report an error.
    need_cast = true;
  } else {
    need_cast = true;
  }
  LOG_DEBUG("check_need_cast_expr", K(ret), K(need_cast), K(src_type), K(dst_type));
  return ret;
}

// private member, use create_cast_expr() instead.
int ObRawExprUtils::create_real_cast_expr(ObRawExprFactory& expr_factory, ObRawExpr* src_expr,
    const ObExprResType& dst_type, ObSysFunRawExpr*& func_expr, const ObSQLSessionInfo* session_info)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* dst_expr = NULL;
  ParseNode parse_node;
  ObObj val;

  if (OB_ISNULL(src_expr) || OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(src_expr), KP(session_info));
  } else {
    if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_CAST, func_expr))) {
      LOG_WARN("create cast expr failed", K(ret));
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_INT, dst_expr))) {
      LOG_WARN("create dest type expr failed", K(ret));
    } else if (OB_FAIL(func_expr->add_param_expr(src_expr))) {
      LOG_WARN("add real param expr failed", K(ret));
    } else {
      func_expr->set_orig_expr(src_expr);
      ObString func_name = ObString::make_string(N_CAST);
      parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX] = static_cast<int16_t>(dst_type.get_type());
      parse_node.int16_values_[OB_NODE_CAST_COLL_IDX] = static_cast<int16_t>(dst_type.get_collation_type());
      if (ob_is_string_or_lob_type(dst_type.get_type())) {
        parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX] = dst_type.get_length();
        if (share::is_oracle_mode()) {
          dst_expr->set_length_semantics(dst_type.get_length_semantics());
        }
      } else if (ob_is_rowid_tc(dst_type.get_type())) {
        int32_t urowid_len = dst_type.get_length();
        if (urowid_len <= -1) {
          urowid_len = 4000;
        }
        parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX] = 4000;
      } else if (ObIntervalYMType == dst_type.get_type()) {
        parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = dst_type.get_scale();  // year
      } else if (ObIntervalDSType == dst_type.get_type()) {
        parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX] = (dst_type.get_scale() / 10);   // day
        parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = (dst_type.get_scale() % 10);  // second
      } else {
        parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX] = dst_type.get_precision();
        parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX] = dst_type.get_scale();
      }

      val.set_int(parse_node.value_);
      dst_expr->set_value(val);
      dst_expr->set_param(val);
      func_expr->set_func_name(func_name);
      if (src_expr->is_for_generated_column()) {
        func_expr->set_for_generated_column();
      }
      if (OB_FAIL(func_expr->add_param_expr(dst_expr))) {
        LOG_WARN("add dest type expr failed", K(ret));
      }
      LOG_DEBUG("create_cast_expr debug", K(ret), K(*src_expr), K(dst_type), K(*func_expr), K(lbt()));
    }
  }
  return ret;
}

int ObRawExprUtils::build_add_expr(
    ObRawExprFactory& expr_factory, ObRawExpr* param_expr1, ObRawExpr* param_expr2, ObOpRawExpr*& add_expr)
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

int ObRawExprUtils::build_minus_expr(
    ObRawExprFactory& expr_factory, ObRawExpr* param_expr1, ObRawExpr* param_expr2, ObOpRawExpr*& minus_expr)
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

int ObRawExprUtils::build_date_add_expr(ObRawExprFactory& expr_factory, ObRawExpr* param_expr1, ObRawExpr* param_expr2,
    ObRawExpr* param_expr3, ObSysFunRawExpr*& date_add_expr)
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

int ObRawExprUtils::build_date_sub_expr(ObRawExprFactory& expr_factory, ObRawExpr* param_expr1, ObRawExpr* param_expr2,
    ObRawExpr* param_expr3, ObSysFunRawExpr*& date_sub_expr)
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

int ObRawExprUtils::build_common_binary_op_expr(ObRawExprFactory& expr_factory, const ObItemType expect_op_type,
    ObRawExpr* param_expr1, ObRawExpr* param_expr2, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* op_expr = NULL;
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

int ObRawExprUtils::build_case_when_expr(ObRawExprFactory& expr_factory, ObRawExpr* when_expr, ObRawExpr* then_expr,
    ObRawExpr* default_expr, ObRawExpr*& case_when_expr)
{
  int ret = OB_SUCCESS;
  ObCaseOpRawExpr* c_case_when_expr = NULL;
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

int ObRawExprUtils::build_is_not_expr(ObRawExprFactory& expr_factory, ObRawExpr* param_expr1, ObRawExpr* param_expr2,
    ObRawExpr* param_expr3, ObRawExpr*& is_not_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* not_expr = NULL;
  if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2) || OB_ISNULL(param_expr3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(param_expr1), K(param_expr2), K(param_expr3), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_IS_NOT, not_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(not_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not_expr is null", K(ret), K(not_expr));
  } else if (OB_FAIL(not_expr->set_param_exprs(param_expr1, param_expr2, param_expr3))) {
    LOG_WARN("failed to set param for not expr", K(ret), K(not_expr));
  } else {
    is_not_expr = not_expr;
  }
  return ret;
}

int ObRawExprUtils::build_is_not_null_expr(
    ObRawExprFactory& expr_factory, ObRawExpr* param_expr, ObRawExpr*& is_not_null_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* null_expr = NULL;
  ObConstRawExpr* flag_expr = NULL;
  ObOpRawExpr* is_not_expr = NULL;
  is_not_null_expr = NULL;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(param_expr), K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_IS_NOT, is_not_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_NULL, null_expr))) {
    LOG_WARN("failed to create const null expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_BOOL, flag_expr))) {
    LOG_WARN("failed to create flag bool expr", K(ret));
  } else if (OB_ISNULL(is_not_expr) || OB_ISNULL(null_expr) || OB_ISNULL(flag_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create not null expr", K(ret));
  } else {
    ObObjParam null_val;
    null_val.set_null();
    null_val.set_param_meta();
    null_expr->set_param(null_val);
    null_expr->set_value(null_val);
    ObObjParam flag_val;
    flag_val.set_bool(false);
    flag_val.set_param_meta();
    flag_expr->set_param(flag_val);
    flag_expr->set_value(flag_val);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(is_not_expr->set_param_exprs(param_expr, null_expr, flag_expr))) {
    LOG_WARN("failed to set param for not_null op", K(ret), K(*is_not_expr));
  } else {
    is_not_null_expr = is_not_expr;
  }
  return ret;
}

int ObRawExprUtils::process_window_complex_agg_expr(ObRawExprFactory& expr_factory, const ObItemType func_type,
    ObWinFunRawExpr* win_func, ObRawExpr*& window_agg_expr, ObIArray<ObWinFunRawExpr*>* win_exprs)
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
  } else if (T_FUN_SUM == func_type || T_FUN_COUNT == func_type || T_FUN_KEEP_SUM == func_type ||
             T_FUN_KEEP_COUNT == func_type) {
    ObWinFunRawExpr* win_func_expr = NULL;
    ObAggFunRawExpr* agg_expr = static_cast<ObAggFunRawExpr*>(window_agg_expr);
    if (agg_expr->is_param_distinct() && (win_func->has_order_items() || win_func->has_frame_orig())) {
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
    } else { /*do nothing */
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < window_agg_expr->get_param_count(); i++) {
      ObRawExpr*& sub_expr = window_agg_expr->get_param_expr(i);
      if (OB_ISNULL(sub_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("error argument.", K(ret));
      } else if (OB_FAIL(process_window_complex_agg_expr(
                     expr_factory, sub_expr->get_expr_type(), win_func, sub_expr, win_exprs))) {
        LOG_WARN("failed to process window complex agg node.", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObRawExprUtils::build_common_aggr_expr(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
    const ObItemType expect_op_type, ObRawExpr* param_expr, ObAggFunRawExpr*& aggr_expr)
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
  } else if (OB_FAIL(aggr_expr->add_real_param_expr(param_expr))) {
    LOG_WARN("failed to add param expr to agg expr", K(ret));
  } else if (OB_FAIL(aggr_expr->formalize(session_info))) {
    LOG_WARN("failed to extract info", K(ret));
  } else { /*do nothing */
  }
  return ret;
}

// only use for limit_expr/offset_expr, for constructing case when limit_expr < 0 then 0 else limit_expr end
int ObRawExprUtils::build_case_when_expr_for_limit(
    ObRawExprFactory& expr_factory, ObRawExpr* limit_expr, ObRawExpr*& expected_case_when_expr)
{
  int ret = OB_SUCCESS;
  expected_case_when_expr = NULL;
  ObRawExpr* case_when_expr = NULL;
  ObRawExpr* less_expr = NULL;
  ObConstRawExpr* zero_expr = NULL;
  if (OB_ISNULL(limit_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(build_const_int_expr(expr_factory, ObIntType, 0, zero_expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(build_common_binary_op_expr(expr_factory, T_OP_LT, limit_expr, zero_expr, less_expr))) {
    LOG_WARN("failed to build common binary op expr", K(ret));
  } else if (OB_FAIL(build_case_when_expr(expr_factory, less_expr, zero_expr, limit_expr, case_when_expr))) {
    LOG_WARN("failed to build case when expr", K(ret));
  } else {
    expected_case_when_expr = case_when_expr;
  }
  return ret;
}

int ObRawExprUtils::build_not_expr(ObRawExprFactory& expr_factory, ObRawExpr* expr, ObRawExpr*& not_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* new_expr = NULL;
  if (OB_ISNULL(expr)) {
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

int ObRawExprUtils::build_or_exprs(
    ObRawExprFactory& expr_factory, const ObIArray<ObRawExpr*>& exprs, ObRawExpr*& or_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* new_expr = NULL;
  or_expr = NULL;
  if (exprs.count() == 0) {
    or_expr = NULL;
  } else if (exprs.count() == 1) {
    or_expr = exprs.at(0);
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_OR, new_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(new_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create or expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      ObRawExpr* expr = exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exprs has null child", K(i));
      } else if (OB_FAIL(new_expr->add_param_expr(expr))) {
        LOG_WARN("add param expr to or expr failed", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      or_expr = new_expr;
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObRawExprUtils::build_and_expr(
    ObRawExprFactory& expr_factory, const ObIArray<ObRawExpr*>& exprs, ObRawExpr*& and_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* new_expr = NULL;
  and_expr = NULL;
  if (exprs.count() == 0) {
    and_expr = NULL;
  } else if (exprs.count() == 1) {
    and_expr = exprs.at(0);
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_AND, new_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(new_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create or expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      ObRawExpr* expr = exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exprs has null child", K(i));
      } else if (OB_FAIL(new_expr->add_param_expr(expr))) {
        LOG_WARN("add param expr to or expr failed", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      and_expr = new_expr;
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObRawExprUtils::new_parse_node(
    ParseNode*& node, ObRawExprFactory& expr_factory, ObItemType type, int num, ObString str_val)
{
  int ret = OB_SUCCESS;
  char* str_value = NULL;
  if (OB_FAIL(new_parse_node(node, expr_factory, type, num))) {
    LOG_WARN("fail to alloc new node", K(ret));
  } else if (str_val.length() <= 0) {
    /*nothing*/
  } else if (OB_ISNULL(str_value = static_cast<char*>(expr_factory.get_allocator().alloc(str_val.length())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc buf", K(ret));
  } else {
    MEMCPY(str_value, str_val.ptr(), str_val.length());
    node->str_value_ = str_value;
    node->str_len_ = str_val.length();
  }
  return ret;
}

int ObRawExprUtils::new_parse_node(ParseNode*& node, ObRawExprFactory& expr_factory, ObItemType type, int num)
{
  int ret = OB_SUCCESS;
  ObIAllocator* alloc_buf = &(expr_factory.get_allocator());
  node = (ParseNode*)alloc_buf->alloc(sizeof(ParseNode));
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
    node->value_ = INT64_MAX;
    node->str_len_ = 0;
    node->str_value_ = NULL;
    node->text_len_ = 0;
    node->raw_text_ = NULL;
    node->pos_ = 0;
    if (num > 0) {
      int64_t alloc_size = sizeof(ParseNode*) * num;
      node->children_ = (ParseNode**)alloc_buf->alloc(alloc_size);
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

int ObRawExprUtils::build_rownum_expr(ObRawExprFactory& expr_factory, ObRawExpr*& rownum_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* rownum = NULL;
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

int ObRawExprUtils::build_to_outfile_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo* session_info,
    const ObSelectIntoItem* into_item, const ObIArray<ObRawExpr*>& exprs, ObRawExpr*& to_outfile_expr)
{
  int ret = OB_SUCCESS;
  to_outfile_expr = NULL;
  ObOpRawExpr* new_expr = NULL;
  ObConstRawExpr* filed_expr = NULL;
  ObConstRawExpr* line_expr = NULL;
  char* closed_cht_buf = NULL;
  ObConstRawExpr* closed_cht_expr = NULL;
  ObRawExpr* is_optional_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_TO_OUTFILE_ROW, new_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(build_const_string_expr(expr_factory,
                 ObVarcharType,
                 into_item->filed_str_.get_varchar(),
                 into_item->filed_str_.get_collation_type(),
                 filed_expr))) {
    LOG_WARN("fail to create string expr", K(ret));
  } else if (OB_FAIL(build_const_string_expr(expr_factory,
                 ObVarcharType,
                 into_item->line_str_.get_varchar(),
                 into_item->line_str_.get_collation_type(),
                 line_expr))) {
    LOG_WARN("fail to create string expr", K(ret));
  } else if (OB_ISNULL(closed_cht_buf = static_cast<char*>(expr_factory.get_allocator().alloc(1)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc buf", K(ret));
  } else if (FALSE_IT(closed_cht_buf[0] = into_item->closed_cht_)) {
    // do nothing
  } else if (OB_FAIL(build_const_string_expr(expr_factory,
                 ObVarcharType,
                 into_item->closed_cht_ == 0 ? ObString() : ObString(1, closed_cht_buf),
                 into_item->filed_str_.get_collation_type(),
                 closed_cht_expr))) {
    LOG_WARN("fail to create string expr", K(ret));
  } else if (OB_FAIL(build_const_bool_expr(&expr_factory, is_optional_expr, into_item->is_optional_))) {
    LOG_WARN("fail to create bool expr", K(ret));
  } else if (new_expr->add_param_expr(filed_expr)) {
    LOG_WARN("fail to add filed_expr", K(ret));
  } else if (new_expr->add_param_expr(line_expr)) {
    LOG_WARN("fail to add line_expr", K(ret));
  } else if (new_expr->add_param_expr(closed_cht_expr)) {
    LOG_WARN("fail to add closed_cht_expr", K(ret));
  } else if (new_expr->add_param_expr(is_optional_expr)) {
    LOG_WARN("fail to add is_optional_expr", K(ret));
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

}  // namespace sql
}  // namespace oceanbase
