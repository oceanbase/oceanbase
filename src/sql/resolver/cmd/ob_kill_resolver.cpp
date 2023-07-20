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
#include "sql/resolver/cmd/ob_kill_resolver.h"
#include "sql/resolver/cmd/ob_kill_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
namespace oceanbase
{
using namespace oceanbase::common;
namespace sql
{
int ObKillResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObKillStmt *kill_stmt = NULL;
  ObRawExpr *tmp_expr = NULL;
  if (OB_UNLIKELY(!(parse_tree.type_ == T_KILL || parse_tree.type_ == T_ALTER_SYSTEM_KILL)
                  || parse_tree.num_child_ != 2
                  || NULL == parse_tree.children_[0]
                  || parse_tree.children_[0]->type_ != T_BOOL
                  || NULL == parse_tree.children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree", K(ret), K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_UNLIKELY(NULL == (kill_stmt = create_stmt<ObKillStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create kill stmt");
  } else if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *(parse_tree.children_[1]), tmp_expr, NULL))) {
    LOG_WARN("resolve const expr failed", K(ret));
  } else if (parse_tree.type_ == T_KILL) {
    kill_stmt->set_is_query(1 == parse_tree.children_[0]->value_);
    kill_stmt->set_value_expr(tmp_expr);
  } else if (parse_tree.type_ == T_ALTER_SYSTEM_KILL) {
    if (1 == parse_tree.children_[0]->value_) {
      // TO DO.
    }
    kill_stmt->set_is_alter_system_kill(true);
    kill_stmt->set_value_expr(tmp_expr);
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_));
      OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_KILL,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  }
  return ret;
}
} // sql
} // oceanbase
