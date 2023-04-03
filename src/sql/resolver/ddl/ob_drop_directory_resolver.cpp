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

#include "sql/resolver/ddl/ob_drop_directory_resolver.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_drop_directory_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
namespace sql
{
ObDropDirectoryResolver::ObDropDirectoryResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObDropDirectoryResolver::~ObDropDirectoryResolver()
{
}

int ObDropDirectoryResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObDropDirectoryStmt *drop_directory_stmt = NULL;
  if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_DROP_DIRECTORY)
      || OB_UNLIKELY(node->num_child_ != DIRECTORY_NODE_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node children", K(ret), K(node), K(node->children_));
  } else if (OB_ISNULL(drop_directory_stmt = create_stmt<ObDropDirectoryStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to get drop directory stmt", K(ret));
  } else {
    stmt_ = drop_directory_stmt;
    drop_directory_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
  }

  // directory name
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(drop_directory_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, drop directory stmt is NULL", K(ret), KP(drop_directory_stmt));
  } else {
    ObString directory_name;
    ParseNode *child_node = node->children_[DIRECTORY_NAME];
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (child_node->str_len_ >= OB_MAX_DIRECTORY_NAME_LENGTH) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(child_node->str_len_), child_node->str_value_);
    } else {
      directory_name.assign_ptr(child_node->str_value_, static_cast<int32_t>(child_node->str_len_));
      drop_directory_stmt->set_directory_name(directory_name);
    }
  }

  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ(schema_checker_->check_ora_ddl_priv(
        session_info_->get_effective_tenant_id(),
        session_info_->get_priv_user_id(),
        ObString(""),
        stmt::T_DROP_DIRECTORY,
        session_info_->get_enable_role_array()),
        session_info_->get_effective_tenant_id(), session_info_->get_user_id());
  }
  LOG_INFO("resolve drop directory finish", K(ret));
  return ret;
}
} // end namespace sql
} // end namespace oceanbase