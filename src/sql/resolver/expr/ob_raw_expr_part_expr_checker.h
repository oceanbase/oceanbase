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

#ifndef OCEANBASE_SQL_RESOLVER_OB_RAW_EXPR_PART_EXPR_CHECKER_H_
#define OCEANBASE_SQL_RESOLVER_OB_RAW_EXPR_PART_EXPR_CHECKER_H_ 1
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{

namespace sql
{
class ObRawExprPartExprChecker : public ObRawExprVisitor
{
public:
  ObRawExprPartExprChecker(const ObPartitionFuncType type)
    : ObRawExprVisitor(),
      func_type_(type) {}
  virtual ~ObRawExprPartExprChecker() {}

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
  virtual int visit(ObAliasRefRawExpr &expr);
  virtual int visit(ObPlQueryRefRawExpr &expr);
  virtual int visit(ObMatchFunRawExpr &expr);
private:
  // types and constants
  const ObPartitionFuncType func_type_;
private:
  inline static bool is_time_expr(const ObRawExpr &expr);
  inline static bool is_date_expr(const ObRawExpr &expr);
  inline static bool is_timestamp_expr(const ObRawExpr &expr);
  inline static bool is_datetime_expr(const ObRawExpr &expr);

  inline static int has_date_or_datetime_args(const ObRawExpr &expr);
  inline static int has_time_or_datetime_args(const ObRawExpr &expr);
  inline static int has_timestamp_args(const ObRawExpr &expr);

  static int check_args_of_from_days(const ObSysFunRawExpr &expr);
  static int check_args_of_extract(const ObSysFunRawExpr &expr);
  static int default_check_args(const ObRawExpr &expr);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprPartExprChecker);
};

bool ObRawExprPartExprChecker::is_time_expr(const ObRawExpr &expr)
{
  return expr.is_column_ref_expr() &&
      common::ObTimeType == expr.get_result_type().get_type();
}

bool ObRawExprPartExprChecker::is_timestamp_expr(const ObRawExpr &expr)
{
  return expr.is_column_ref_expr() &&
      common::ObTimestampType == expr.get_result_type().get_type();
}

bool ObRawExprPartExprChecker::is_date_expr(const ObRawExpr &expr)
{
  return expr.is_column_ref_expr() &&
      common::ObDateType == expr.get_result_type().get_type();
}

bool ObRawExprPartExprChecker::is_datetime_expr(const ObRawExpr &expr)
{
  return expr.is_column_ref_expr() &&
      common::ObDateTimeType == expr.get_result_type().get_type();
}

/**
 * return OB_SUCCESS if has date or datetime args
 */
inline int ObRawExprPartExprChecker::has_date_or_datetime_args(const ObRawExpr &expr)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    const ObRawExpr *sub_expr = expr.get_param_expr(i);
    if (OB_ISNULL(sub_expr)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN,"sub_expr should not be null", K(ret));
    } else if (OB_ISNULL(sub_expr = ObRawExprUtils::skip_implicit_cast(sub_expr))) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN,"sub_expr should not be null", K(ret));
    } else if (!is_date_expr(*sub_expr) && !is_datetime_expr(*sub_expr)) {
      ret = common::OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
      SQL_RESV_LOG(WARN,"expect date or datetime type", K(ret),
                   "type", sub_expr->get_result_type().get_type());
    }
  }
  return ret;
}

/**
 * return OB_SUCCESS if has time or datetime args
 */
inline int ObRawExprPartExprChecker::has_time_or_datetime_args(const ObRawExpr &expr)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    const ObRawExpr *sub_expr = expr.get_param_expr(i);
    if (OB_ISNULL(sub_expr)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN,"sub_expr should not be null", K(ret));
    } else if (OB_ISNULL(sub_expr = ObRawExprUtils::skip_implicit_cast(sub_expr))) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN,"sub_expr should not be null", K(ret));
    } else if (!is_time_expr(*sub_expr) && !is_datetime_expr(*sub_expr)) {
      ret = common::OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
      SQL_RESV_LOG(WARN,"expect time or datetime type", K(ret), KPC(sub_expr),
                   "type", sub_expr->get_result_type().get_type());
    }
  }
  return ret;
}

/**
 * return OB_SUCCESS if has timestamp args
 */
inline int ObRawExprPartExprChecker::has_timestamp_args(const ObRawExpr &expr)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    const ObRawExpr *sub_expr = expr.get_param_expr(i);
    if (OB_ISNULL(sub_expr)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN,"sub_expr should not be null", K(ret));
    } else if (OB_ISNULL(sub_expr = ObRawExprUtils::skip_implicit_cast(sub_expr))) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN,"sub_expr should not be null", K(ret));
    } else if (!is_timestamp_expr(*sub_expr)) {
      ret = common::OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
      SQL_RESV_LOG(WARN,"expect timestamp type", K(ret),
                   "type", sub_expr->get_result_type().get_type());
    }
  }
  return ret;
}

} //end of sql
} //end of oceanbase

#endif //OCEANBASE_SQL_RESOLVER_OB_RAW_EXPR_PART_EXPR_CHECKER_H_

