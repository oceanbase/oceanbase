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
#include "sql/resolver/ddl/ob_alter_database_resolver.h"

#include "sql/resolver/ddl/ob_database_resolver.h"
#include "sql/resolver/ddl/ob_alter_database_stmt.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{
ObAlterDatabaseResolver::ObAlterDatabaseResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObAlterDatabaseResolver::~ObAlterDatabaseResolver()
{
}

int ObAlterDatabaseResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);

  if (OB_ISNULL(node)
      || OB_UNLIKELY(T_ALTER_DATABASE != node->type_)
      || OB_UNLIKELY(node->num_child_ != DATABASE_NODE_COUNT)
      || OB_ISNULL(node->children_)
      || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObAlterDatabaseStmt *alter_database_stmt = NULL;
    if (OB_ISNULL(alter_database_stmt = create_stmt<ObAlterDatabaseStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create alter_database_stmt", K(ret));
    } else {
      stmt_ = alter_database_stmt;
      alter_database_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    }
    // resolve database name
    if (OB_SUCC(ret)) {
      ObString database_name;
      ParseNode *dbname_node = node->children_[DBNAME];
      if (NULL == dbname_node) {
        // sql 语句在缺省 database_name 的时候，默认使用当前 database_name
        database_name = session_info_->get_database_name();
      } else {
        if (OB_UNLIKELY(T_IDENT != dbname_node->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid parse tree node", K(ret), K(dbname_node->type_));
        } else {
          database_name.assign_ptr(dbname_node->str_value_,
                                   static_cast<int32_t>(dbname_node->str_len_));
          ObNameCaseMode mode = OB_NAME_CASE_INVALID;
          if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
              LOG_WARN("fail to get name case mode", K(mode), K(ret));
          } else {
            bool perserve_lettercase = lib::is_oracle_mode() ?
                true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
            ObCollationType cs_type = CS_TYPE_INVALID;
            if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
              LOG_WARN("fail to get collation_connection", K(ret));
            } else if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(
                        cs_type, perserve_lettercase, database_name))) {
              LOG_WARN("fail to check and convert database name", K(database_name), K(ret));
            } else {
              CK (OB_NOT_NULL(schema_checker_));
              CK (OB_NOT_NULL(schema_checker_->get_schema_guard()));
              OZ (ObSQLUtils::cvt_db_name_to_org(*schema_checker_->get_schema_guard(),
                                                 session_info_,
                                                 database_name,
                                                 allocator_));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(alter_database_stmt->set_database_name(database_name))) {
          LOG_WARN("set database name failed", K(database_name), K(ret));
        }
      }
    }
    // resolve database options
    if (OB_SUCC(ret)) {
      ParseNode *dboption_node = node->children_[DATABASE_OPTION];
      if (OB_ISNULL(dboption_node) || OB_UNLIKELY(T_DATABASE_OPTION_LIST != dboption_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse tree", K(ret));
      } else {
        ObDatabaseResolver<ObAlterDatabaseStmt> resolver;
        if (OB_FAIL(resolver.resolve_database_options(alter_database_stmt,
                                                      dboption_node,
                                                      session_info_))) {
          LOG_WARN("resolve database option failed", K(ret));
        } else {
          if(resolver.get_alter_option_bitset().has_member(obrpc::ObAlterDatabaseArg::PRIMARY_ZONE)) {
            bool is_sync_ddl_user = false;
            if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
              LOG_WARN("Failed to check sync_ddl_user", K(ret));
            } else if (is_sync_ddl_user) {
              ret = OB_IGNORE_SQL_IN_RESTORE;
              LOG_WARN("Cannot support for sync ddl user to alter primary zone", K(ret), K(session_info_->get_user_name()));
            }
          }
          if (OB_SUCC(ret)) {
            alter_database_stmt->set_alter_option_set(resolver.get_alter_option_bitset());
          }
        }
      }
    }
  }
  LOG_INFO("resolve alter database finish", K(ret));
  return ret;
}
}//namespace sql
}//namespace oceanbase
