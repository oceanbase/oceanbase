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
#include "sql/parser/ob_item_type_str.h"
#include "sql/resolver/expr/ob_raw_expr_print_visitor.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
ObRawExprPrintVisitor::ObRawExprPrintVisitor(ObRawExpr &expr_root)
    :expr_root_(expr_root),
     buf_(NULL),
     buf_len_(0),
     pos_(0)
{}

ObRawExprPrintVisitor::~ObRawExprPrintVisitor()
{}

int64_t ObRawExprPrintVisitor::to_string(char* buf, const int64_t buf_len) const
{
  pos_ = 0;
  buf_ = buf;
  buf_len_ = buf_len;
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_root_.preorder_accept(*const_cast<ObRawExprPrintVisitor*>(this)))) {
    LOG_WARN("failed to print raw expr", K(ret));
    pos_ = 0;
  }
  return pos_;
}

int ObRawExprPrintVisitor::visit(ObConstRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "%s(%s)",
                              get_type_name(expr.get_expr_type()),
                              S(expr.get_value())))) {
    LOG_WARN("databuff print failed", K(ret));
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObExecParamRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr.get_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref expr is null", K(ret));
  } else if (expr.get_ref_expr()->preorder_accept(*this)) {
    LOG_WARN("failed to visit ref expr", K(ret));
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObVarRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "%s",
                              get_type_name(expr.get_expr_type())))) {
    LOG_WARN("databuff print failed", K(ret));
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObOpPseudoColumnRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "%s", expr.get_name()))) {
    LOG_WARN("databuff print failed", K(ret));
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObQueryRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "SUBQUERY(%lu)", expr.get_ref_id()))) {
    LOG_WARN("databuff print failed", K(ret));
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObPlQueryRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "%s<%ld>|", get_type_name(expr.get_expr_type()),
                              expr.get_param_count()))) {
    LOG_WARN("databuff print failed", K(ret));
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObColumnRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "%s(%lu:%lu)",
                              get_type_name(expr.get_expr_type()),
                              expr.get_table_id(), expr.get_column_id()))) {
    LOG_WARN("databuff print failed", K(ret));
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "%s<%ld>|", get_type_name(expr.get_expr_type()),
                              expr.get_param_count()))) {
    LOG_WARN("databuff print failed", K(ret));
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObCaseOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.get_arg_param_expr()) {
    if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "CASE<%ld>|", expr.get_param_count()))) {
      LOG_WARN("databuff print failed", K(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "CASE_WHEN<%ld>|", expr.get_param_count()))) {
      LOG_WARN("databuff print failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObAggFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "%s<%ld>|",
                              ob_aggr_func_str(expr.get_expr_type()),
                              expr.get_param_count()))) {
    LOG_WARN("databuff aggr func failed", K(ret));
  } else if (expr.is_param_distinct()) {
    if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "DISTINCT|"))) {
      LOG_WARN("databuff distinct failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && NULL == expr.get_param_expr(0)) {
    if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "*"))) {
      LOG_WARN("databuff star failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "%.*s<%ld>|",
                              expr.get_func_name().length(), expr.get_func_name().ptr(),
                              expr.get_param_count()))) {
    LOG_WARN("databuff sysfunc failed", K(ret));
  }
  return ret;
}

int ObRawExprPrintVisitor::visit(ObSetOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "%s<%ld>|", get_type_name(expr.get_expr_type()),
                              expr.get_param_count()))) {
    LOG_WARN("databuff setop failed", K(ret));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
