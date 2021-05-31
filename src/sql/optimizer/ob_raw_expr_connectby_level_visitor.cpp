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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_raw_expr_connectby_level_visitor.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase {
using namespace common;
namespace sql {
/*
 * select concat('', level - 1) ORDER_DATE, 0 totnum, 0 FAILNUM, rownum rn, level from dual connect by 2 + level - 1 <=
 * 4; Say, 2 + level - 1 <= 4 is connectby filter. select c1, c2, connect_by_isleaf from tlevel start with c1 = 1
 * connect by prior c1 = level; Say, c1 = level is not a connectby filter. Actually, we don't support now.
 *
 */
int ObRawConnectByLevelVisitor::check_connectby_filter(ObRawExpr* expr, ObIArray<ObRawExpr*>& level_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_SUCCESS;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->has_flag(CNT_PSEUDO_COLUMN)) {
    level_exprs_ = &level_exprs;
    if (OB_FAIL(expr->postorder_accept(*this))) {
      LOG_WARN("failed to postorder accept", K(ret));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObRawConnectByLevelVisitor::visit(ObConstRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawConnectByLevelVisitor::visit(ObVarRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

// not supported
int ObRawConnectByLevelVisitor::visit(ObQueryRefRawExpr& expr)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(expr);
  LOG_WARN("query ref in connect by clause does not supported", K(ret));
  return ret;
}

// select c1, c2, connect_by_isleaf from tlevel start with c1 = 1 connect by prior c1 = level;
int ObRawConnectByLevelVisitor::visit(ObColumnRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  LOG_WARN("query ref in connect by clause does not supported", K(ret));
  return ret;
}

// need check the whole expression
// eg:
//    prior c1
//    connect_by_root c1
//    SYS_CONNECT_BY_PATH(c1,'/')
// and it's different from oracle, I think it's oracle's bug
// eg:
//  select c1, c2, connect_by_isleaf from tlevel start with c1 = 1 connect by SYS_CONNECT_BY_PATH(c1,'/');
//  ORA-30002: SYS_CONNECT_BY_PATH is forbid
//
int ObRawConnectByLevelVisitor::visit(ObOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  switch (expr.get_expr_type()) {
    case T_OP_CONNECT_BY_ROOT: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Forbid SYS_CONNECT_BY_PATH in connect by clause", K(ret));
      break;
    }
    default:
      break;
  }
  return ret;
}

int ObRawConnectByLevelVisitor::visit(ObCaseOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

// Shouldn't agg function in connect by clause
int ObRawConnectByLevelVisitor::visit(ObAggFunRawExpr& expr)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(expr);
  LOG_WARN("agg fun in connect by clause does not supported", K(ret));
  return ret;
}

int ObRawConnectByLevelVisitor::visit(ObSysFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

// never got there
int ObRawConnectByLevelVisitor::visit(ObSetOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

// never got there
int ObRawConnectByLevelVisitor::visit(ObAliasRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawConnectByLevelVisitor::visit(ObFunMatchAgainst& expr)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  return ret;
}

int ObRawConnectByLevelVisitor::visit(ObWinFunRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawConnectByLevelVisitor::visit(ObPseudoColumnRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (expr.get_expr_type() != T_LEVEL) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("Only Level pseudo allow here", K(ret));
  } else if (OB_ISNULL(level_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The level exprs is null", K(ret));
  } else if (OB_FAIL(level_exprs_->push_back(&expr))) {
    LOG_WARN("Failed to push back expr", K(ret));
  }
  return ret;
}

int ObRawConnectByLevelVisitor::visit(ObSetIterRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawConnectByLevelVisitor::visit(ObRowIterRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
