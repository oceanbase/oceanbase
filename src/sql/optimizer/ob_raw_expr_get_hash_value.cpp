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
#include "ob_raw_expr_get_hash_value.h"

/// interface of ObRawExprVisitor
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObRawExprGetHashValue::get_hash_value(ObRawExpr &expr)
{
  return expr.postorder_accept(*this);
}

int ObRawExprGetHashValue::visit(ObConstRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}

int ObRawExprGetHashValue::visit(ObVarRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}

int ObRawExprGetHashValue::visit(ObOpPseudoColumnRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}

int ObRawExprGetHashValue::visit(ObQueryRefRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}

int ObRawExprGetHashValue::visit(ObPlQueryRefRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}

int ObRawExprGetHashValue::visit(ObColumnRefRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}
int ObRawExprGetHashValue::visit(ObOpRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}
int ObRawExprGetHashValue::visit(ObCaseOpRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}
int ObRawExprGetHashValue::visit(ObAggFunRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}
int ObRawExprGetHashValue::visit(ObSysFunRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}

int ObRawExprGetHashValue::visit(ObSetOpRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}

int ObRawExprGetHashValue::visit(ObMatchFunRawExpr &expr)
{
  seed_ = expr.hash(seed_);
  return OB_SUCCESS;
}
