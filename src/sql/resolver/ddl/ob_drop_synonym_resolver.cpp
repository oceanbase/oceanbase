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
#include "sql/resolver/ddl/ob_drop_synonym_resolver.h"

#include "sql/ob_sql_utils.h"
#include "sql/resolver/ddl/ob_drop_synonym_stmt.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
int ObDropSynonymResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode *>(&parse_tree);
  ObDropSynonymStmt *drop_synonym_stmt = NULL;
  if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_DROP_SYNONYM)
      || OB_UNLIKELY(node->num_child_ != SYNONYM_CHILD_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(node->type_), K(node->num_child_), K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(node), K(node->children_));
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL");
  } else if (OB_ISNULL(drop_synonym_stmt = create_stmt<ObDropSynonymStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create drop_synonym_stmt", K(ret));
  } else {
    drop_synonym_stmt->set_tenant_id(session_info_->get_effective_tenant_id());

    //resolve public and force
    bool is_public = (node->children_[0] != NULL);
    bool has_database = (node->children_[1] != NULL);
    bool is_force = (node->children_[3] != NULL);
    if (OB_UNLIKELY(is_public && has_database)) {
      ret = OB_ERR_INVALID_SYNONYM_NAME;
      LOG_USER_ERROR(OB_ERR_INVALID_SYNONYM_NAME);
    } else {
      drop_synonym_stmt->set_force(is_force);
    }

    //resolve database name
    if (OB_FAIL(ret)) {
    } else if (has_database) {
      ObString db_name;
      db_name.assign_ptr(node->children_[1]->str_value_,
                         static_cast<ObString::obstr_size_t>(node->children_[1]->str_len_));
      drop_synonym_stmt->set_database_name(db_name);
    } else if (is_public) {
      drop_synonym_stmt->set_database_name(OB_PUBLIC_SCHEMA_NAME);
    } else {
      drop_synonym_stmt->set_database_name(session_info_->get_database_name());
    }

    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK(OB_NOT_NULL(schema_checker_));
      OZ (schema_checker_->check_ora_ddl_priv(
            session_info_->get_effective_tenant_id(),
            session_info_->get_priv_user_id(),
            drop_synonym_stmt->get_database_name(),
            is_public ? 
              stmt::T_DROP_PUB_SYNONYM : stmt::T_DROP_SYNONYM,
              session_info_->get_enable_role_array()),
            session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }

    //resolve synonym name
    if (OB_FAIL(ret)) {
    } else {
      ObString synonym_name;
      synonym_name.assign_ptr(node->children_[2]->str_value_,
                         static_cast<ObString::obstr_size_t>(node->children_[2]->str_len_));
      drop_synonym_stmt->set_synonym_name(synonym_name);
    }
  }

  if (OB_SUCC(ret)) {
    stmt_ = drop_synonym_stmt;
  }
  return ret;
}
}
}
