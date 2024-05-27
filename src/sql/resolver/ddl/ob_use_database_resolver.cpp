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
#include "sql/resolver/ddl/ob_use_database_resolver.h"

#include "sql/resolver/ddl/ob_use_database_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"


namespace oceanbase
{
using namespace share::schema;
using namespace common;
namespace sql
{
ObUseDatabaseResolver::ObUseDatabaseResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObUseDatabaseResolver::~ObUseDatabaseResolver()
{
}

int ObUseDatabaseResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObUseDatabaseStmt *use_database_stmt = NULL;

  if (OB_ISNULL(node)
      || T_USE_DATABASE != node->type_
      || 1 != node->num_child_
      || OB_ISNULL(node->children_)
      || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node));
  } else if (OB_ISNULL(node->children_[0])
             || T_IDENT != node->children_[0]->type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node), K(node->children_[0]));
  } else if (OB_ISNULL(use_database_stmt = create_stmt<ObUseDatabaseStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create use_database_stmt");
  } else {
    ObString db_name;
    db_name.assign_ptr(node->children_[0]->str_value_,
                       static_cast<int32_t>(node->children_[0]->str_len_));
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid session info", K(session_info_), K(schema_checker_));
    } else if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
      SERVER_LOG(WARN, "fail to get name case mode", K(mode), K(ret));
    } else {
      bool perserve_lettercase = lib::is_oracle_mode() ?
          true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
      ObCollationType cs_type = CS_TYPE_INVALID;
      if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
        LOG_WARN("fail to get collation_connection", K(ret));
      } else if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(
                  cs_type, perserve_lettercase, db_name))) {
        LOG_WARN("fail to check and convert database name", K(db_name), K(ret));
      } else {
        CK (OB_NOT_NULL(schema_checker_));
        CK (OB_NOT_NULL(schema_checker_->get_schema_guard()));
        OZ (ObSQLUtils::cvt_db_name_to_org(*schema_checker_->get_schema_guard(),
                                           session_info_,
                                           db_name,
                                           allocator_));
        use_database_stmt->set_db_name(db_name);
        uint64_t tenant_id = session_info_->get_effective_tenant_id();
        share::schema::ObSessionPrivInfo session_priv;
        uint64_t database_id = OB_INVALID_ID;
        const share::schema::ObDatabaseSchema *db_schema = NULL;
        if (OB_FAIL(session_info_->get_session_priv_info(session_priv))) {
          LOG_WARN("faile to get session priv info", K(ret));
        } else  if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, database_id))) {
          LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
          LOG_WARN("invalid database name. ", K(db_name));
        } else if (OB_FAIL(schema_checker_->check_db_access(session_priv, db_name))) {
          SQL_ENG_LOG(WARN, "fail to check user privilege", K(db_name),K(ret));
          if (params_.disable_privilege_check_ == PRIV_CHECK_FLAG_DISABLE) {
            LOG_WARN("db access privilege check is disabled");
            ret = OB_SUCCESS;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(schema_checker_->get_database_schema(tenant_id, database_id, db_schema))) {
            LOG_WARN("failed to get db schema", K(ret), K(database_id));
          } else {
            use_database_stmt->set_db_id(database_id);
            use_database_stmt->set_db_priv_set(session_priv.db_priv_set_);
            use_database_stmt->set_db_charset(
                ObString::make_string(ObCharset::charset_name(db_schema->get_charset_type())));
            use_database_stmt->set_db_collation(
                ObString::make_string(ObCharset::collation_name(db_schema->get_collation_type())));
          }
        }
      }
    }
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase
