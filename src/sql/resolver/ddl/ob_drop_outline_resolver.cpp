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
#include "sql/resolver/ddl/ob_drop_outline_resolver.h"

#include "sql/ob_sql_utils.h"
#include "sql/resolver/ddl/ob_drop_outline_stmt.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
int ObDropOutlineResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode *>(&parse_tree);
  ObDropOutlineStmt *drop_outline_stmt = NULL;
  if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_DROP_OUTLINE)
      || OB_UNLIKELY(node->num_child_ != OUTLINE_CHILD_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(node), K(node->children_));
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL");
  } else if (OB_ISNULL(drop_outline_stmt = create_stmt<ObDropOutlineStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create drop_outline_stmt", K(ret));
  } else {
    stmt_ = drop_outline_stmt;
    //resolve database_name and outline_name
    if (OB_SUCC(ret)) {
      ObString db_name;
      ObString outline_name;
      if (OB_FAIL(resolve_outline_name(node->children_[0], db_name, outline_name))) {
        LOG_WARN("fail to resolve outline name", K(ret));
      } else {
        static_cast<ObDropOutlineStmt *>(stmt_)->set_database_name(db_name);
        static_cast<ObDropOutlineStmt *>(stmt_)->set_outline_name(outline_name);
        static_cast<ObDropOutlineStmt *>(stmt_)->set_tenant_id(params_.session_info_->get_effective_tenant_id());
      }
    }
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_)); 
      OZ (schema_checker_->check_ora_ddl_priv(
            params_.session_info_->get_effective_tenant_id(),
            params_.session_info_->get_priv_user_id(),
            ObString(""),
            stmt::T_DROP_OUTLINE,
            session_info_->get_enable_role_array()),
            params_.session_info_->get_effective_tenant_id(), 
            params_.session_info_->get_priv_user_id());
    }
  }
  return ret;
}
}//namespace sql
}//namespace oceanbase
