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

#include "sql/resolver/ddl/ob_create_directory_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_create_directory_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
namespace sql
{
ObCreateDirectoryResolver::ObCreateDirectoryResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObCreateDirectoryResolver::~ObCreateDirectoryResolver()
{
}

int ObCreateDirectoryResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCreateDirectoryStmt *create_directory_stmt = NULL;
  if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_CREATE_DIRECTORY)
      || OB_UNLIKELY(node->num_child_ != DIRECTORY_NODE_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node children", K(ret), K(node), K(node->children_));
  } else if (OB_ISNULL(create_directory_stmt = create_stmt<ObCreateDirectoryStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to get create directory stmt", K(ret));
  } else {
    stmt_ = create_directory_stmt;
    create_directory_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    create_directory_stmt->set_user_id(session_info_->get_user_id());
  }

  // or replace
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(create_directory_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, create directory stmt is NULL", K(ret), KP(create_directory_stmt));
  } else {
    bool or_replace = (NULL != node->children_[DIRECTORY_REPLACE]);
    create_directory_stmt->set_or_replace(or_replace);
  }

  // directory name
  if (OB_SUCC(ret)) {
    ObString directory_name;
    ParseNode *child_node = node->children_[DIRECTORY_NAME];
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (child_node->str_len_ >= OB_MAX_DIRECTORY_NAME_LENGTH) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(child_node->str_len_), child_node->str_value_);
    } else if (FALSE_IT(directory_name.assign_ptr(child_node->str_value_, static_cast<int32_t>(child_node->str_len_)))) {
      // do nothing
    } else if (OB_FAIL(create_directory_stmt->set_directory_name(directory_name))) {
      LOG_WARN("set directory name failed", K(ret));
    }
  }

  // directory path
  if (OB_SUCC(ret)) {
    ObString directory_path;
    ObString secure_file_priv;
    ParseNode *child_node = node->children_[DIRECTORY_PATH];
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (child_node->str_len_ >= OB_MAX_DIRECTORY_PATH_LENGTH) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(child_node->str_len_), child_node->str_value_);
    } else {
      ObString real_directory_path;
      directory_path.assign_ptr(child_node->str_value_, static_cast<int32_t>(child_node->str_len_));
      if (!directory_path.empty()) {
        ObArrayWrap<char> buffer;
        OZ (buffer.allocate_array(*allocator_, PATH_MAX));
        if (OB_SUCC(ret)) {
          real_directory_path = ObString(realpath(to_cstring(directory_path), buffer.get_data()));
          if (real_directory_path.empty()) {
            real_directory_path = directory_path;
          }
        }
      }
      OZ (session_info_->get_secure_file_priv(secure_file_priv));
      OZ (ObResolverUtils::check_secure_path(secure_file_priv, real_directory_path));
      OZ (create_directory_stmt->set_directory_path(directory_path));
    }
  }

  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ(schema_checker_->check_ora_ddl_priv(
        session_info_->get_effective_tenant_id(),
        session_info_->get_priv_user_id(),
        ObString(""),
        stmt::T_CREATE_DIRECTORY,
        session_info_->get_enable_role_array()),
        session_info_->get_effective_tenant_id(), session_info_->get_user_id());
  }
  LOG_INFO("resolve create directory finish", K(ret));
  return ret;
}
} // end namespace sql
} // end namespace oceanbase
