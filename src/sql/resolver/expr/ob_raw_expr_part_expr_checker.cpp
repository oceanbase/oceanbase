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
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/expr/ob_raw_expr_part_expr_checker.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObRawExprPartExprChecker::check_args_of_from_days(const ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    const ObRawExpr *sub_expr = expr.get_param_expr(i);
    CK(sub_expr = ObRawExprUtils::skip_implicit_cast(sub_expr));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_time_expr(*sub_expr)
        || is_date_expr(*sub_expr) || is_datetime_expr(*sub_expr)) {
      ret = OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
      LOG_WARN("expect no time expr", K(ret),
               "type", sub_expr->get_result_type().get_type());
    }
  }
  return ret;
}

int ObRawExprPartExprChecker::visit(ObPlQueryRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprPartExprChecker::check_args_of_extract(const ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.get_param_count() != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param count is not corrent", K(ret),
             "param_count", expr.get_param_count());
  } else {
    const ObRawExpr *sub_expr = expr.get_param_expr(0);
    const ObRawExpr *sub_expr2= expr.get_param_expr(1);
    if (OB_UNLIKELY(NULL == sub_expr || NULL == sub_expr2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub_expr or sub_expr2 should not be null", K(ret),
               K(sub_expr), K(sub_expr2));
    } else if (ObRawExpr::EXPR_CONST != sub_expr->get_expr_class()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub_expr type class should be EXPR_CONST", K(ret),
               "expr_class", sub_expr->get_expr_class());
    } else {
      CK (OB_NOT_NULL(sub_expr2 = ObRawExprUtils::skip_implicit_cast(sub_expr2)));
      int64_t date_unit_type = DATE_UNIT_MAX;
      const ObConstRawExpr *con_expr = static_cast<const ObConstRawExpr*>(sub_expr);
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(con_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("con_expr should not be NULL", K(ret));
      } else {
        con_expr->get_value().get_int(date_unit_type);
        switch(date_unit_type) {
          case DATE_UNIT_YEAR:
          case DATE_UNIT_YEAR_MONTH:
          case DATE_UNIT_QUARTER:
          case DATE_UNIT_MONTH:
          case DATE_UNIT_DAY: {
            if (!is_date_expr(*sub_expr2) && !is_datetime_expr(*sub_expr2)) {
              ret = OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
              LOG_WARN("expect date or datetime expr", K(ret),
                       "type", sub_expr2->get_result_type().get_type());
            }
            break;
          }
          case DATE_UNIT_DAY_HOUR:
          case DATE_UNIT_DAY_MINUTE:
          case DATE_UNIT_DAY_SECOND:
          case DATE_UNIT_DAY_MICROSECOND: {
            if (!is_datetime_expr(*sub_expr2)) {
              ret = OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
              LOG_WARN("expect datetime expr", K(ret),
                       "type", sub_expr2->get_result_type().get_type());
            }
            break;
          }
          case DATE_UNIT_HOUR:
          case DATE_UNIT_HOUR_SECOND:
          case DATE_UNIT_HOUR_MINUTE:
          case DATE_UNIT_MINUTE:
          case DATE_UNIT_MINUTE_SECOND:
          case DATE_UNIT_SECOND:
          case DATE_UNIT_MICROSECOND:
          case DATE_UNIT_HOUR_MICROSECOND:
          case DATE_UNIT_MINUTE_MICROSECOND:
          case DATE_UNIT_SECOND_MICROSECOND: {
            if (!is_time_expr(*sub_expr2) && !is_datetime_expr(*sub_expr2)) {
              ret = OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
              LOG_WARN("expect time or datetime expr", K(ret),
                       "type", sub_expr2->get_result_type().get_type());
            }
            break;
          }
          default: {
            //DATE_UNIT_MAX is only an end marker
            //DATE_UNIT_WEEK depends on default_week_format which is a session
            //variable and cannot be used for partitioning. See bug#57071 of mysql.
            ret = OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
            LOG_WARN("this expr can't no exist is partitioning", K(ret),
                     "type", sub_expr2->get_result_type().get_type());
          }
        }
      }
    }
  }
  return ret;
}

int ObRawExprPartExprChecker::default_check_args(const ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    const ObRawExpr *sub_expr = expr.get_param_expr(i);
    if (OB_ISNULL(sub_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub_expr should not be null", K(ret));
    } else if (is_timestamp_expr(*sub_expr)) {
      ret = OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
      LOG_WARN("expect date or datetime type", K(ret),
               "type", sub_expr->get_result_type().get_type());
    }
  }
  return ret;
}

int ObRawExprPartExprChecker::visit(ObConstRawExpr &expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprPartExprChecker::visit(ObExecParamRawExpr &expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprPartExprChecker::visit(ObVarRawExpr &expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprPartExprChecker::visit(ObOpPseudoColumnRawExpr &expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprPartExprChecker::visit(ObQueryRefRawExpr &expr)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(expr);
  return ret;
}

int ObRawExprPartExprChecker::visit(ObColumnRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawExprPartExprChecker::visit(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  const ObObjType type = expr.get_result_type().get_type();
  if (PARTITION_FUNC_TYPE_RANGE_COLUMNS != func_type_ &&
      PARTITION_FUNC_TYPE_LIST_COLUMNS != func_type_ &&
      !ob_is_integer_type(type)) {
    ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
    LOG_WARN("part_value_expr type is not correct", K(ret), K(type));
  } else {
    switch (expr.get_expr_type()) {
      case T_OP_REGEXP: {
        ret = OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED;
        LOG_WARN("invalid partition function", K(ret), "item_type", expr.get_expr_type());
        break;
      }
      default: {
        if (OB_FAIL(default_check_args(expr))) {
          LOG_WARN("default_check_args failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRawExprPartExprChecker::visit(ObCaseOpRawExpr &expr)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(expr);
  return ret;
}

int ObRawExprPartExprChecker::visit(ObAggFunRawExpr &expr)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(expr);
  return ret;
}

int ObRawExprPartExprChecker::visit(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  switch(expr.get_expr_type()) {
    //need time or datetime args
    case T_FUN_SYS_HOUR:
    case T_FUN_SYS_MINUTE:
    case T_FUN_SYS_SECOND:
    case T_FUN_SYS_TIME_TO_SEC:
    case T_FUN_SYS_MICROSECOND:
    {
     if (OB_FAIL(has_time_or_datetime_args(expr))) {
       LOG_WARN("check args of time or datetime failed", K(ret));
     }
     break;
    }
   //need date or datetime args
   /** in mysql, date_diff(date1,date2) is implement by
    * to_days(date1) - to_days(date2)
    */
   case T_FUN_SYS_DATE_DIFF:
   case T_FUN_SYS_DAY:
   case T_FUN_SYS_TO_DAYS:
   case T_FUN_SYS_TO_SECONDS:
   case T_FUN_SYS_DAY_OF_MONTH:
   case T_FUN_SYS_MONTH:
   case T_FUN_SYS_DAY_OF_YEAR:
   case T_FUN_SYS_WEEK_OF_YEAR:
   case T_FUN_SYS_WEEKDAY_OF_DATE:
   case T_FUN_SYS_YEARWEEK_OF_DATE:
   case T_FUN_SYS_YEAR:
   case T_FUN_SYS_DAY_OF_WEEK:
   case T_FUN_SYS_WEEK:
   case T_FUN_SYS_QUARTER:
   {
     if (OB_FAIL(has_date_or_datetime_args(expr))) {
       LOG_WARN("check_args_of_to_days failed", K(ret));
     }
     break;
   }
   //need or timestamp args
   //time_of_usec is  like unix_timestamp, with different precision
   case T_FUN_SYS_TIME_TO_USEC:
   case T_FUN_SYS_UNIX_TIMESTAMP: {
     if (OB_FAIL(has_timestamp_args(expr))) {
       LOG_WARN("check_args_of_unix_timestamp failed", K(ret));
     }
     break;
   }
   case T_FUN_SYS_EXTRACT: {
     if (OB_FAIL(check_args_of_extract(expr))) {
       LOG_WARN("check_args_of_extract_failed", K(ret));
     }
     break;
   }
   case T_FUN_SYS_FROM_DAYS: {
     if (OB_FAIL(check_args_of_from_days(expr))) {
       LOG_WARN("check_args_of_from_days failed", K(ret));
     }
     break;
   }
   default : {
     if (OB_FAIL(default_check_args(expr))) {
       LOG_WARN("default_check_args failed", K(ret));
     }
   }
  }
  return ret;
}

int ObRawExprPartExprChecker::visit(ObSetOpRawExpr &expr)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(expr);
  return ret;
}

int ObRawExprPartExprChecker::visit(ObAliasRefRawExpr &expr)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(expr);
  return ret;
}

} //namespace sql
} //namespace oceanbase

