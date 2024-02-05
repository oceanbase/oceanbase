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

#include "sql/resolver/prepare/ob_prepare_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

int ObPrepareResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObPrepareStmt *prepare_stmt = NULL;
  const ParseNode *name_node = parse_tree.children_[0];
  const ParseNode *stmt_node = parse_tree.children_[1];
  if (OB_ISNULL(name_node) || OB_ISNULL(stmt_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid prepare node", K(name_node), K(stmt_node), K(ret));
  } else if (OB_ISNULL(prepare_stmt = create_stmt<ObPrepareStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create execute stmt", K(ret));
  } else {
    stmt_ = prepare_stmt;
  }

  if (OB_SUCC(ret)) {
    if (T_IDENT != name_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid name node", K(name_node->type_), K(ret));
    } else {
      prepare_stmt->set_prepare_name(ObString(name_node->str_len_, name_node->str_value_));
    }
  }

  if (OB_SUCC(ret)) {
    if (T_VARCHAR == stmt_node->type_ || T_HEX_STRING == stmt_node->type_ || T_OP_GET_USER_VAR == stmt_node->type_) {
      ObRawExpr *stmt_expr = NULL;
      if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *stmt_node, stmt_expr, NULL))) {
        LOG_WARN("failed to resolve const expr", K(ret));
      } else {
        prepare_stmt->set_prepare_sql(stmt_expr);
      }
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid name node", K(name_node->type_), K(ret));
    }
  }
  return ret;
}

}
}


