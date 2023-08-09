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

#define USING_LOG_PREFIX SERVER
#include "sql/resolver/ddl/ob_truncate_table_resolver.h"
#include "share/ob_define.h"
#include "share/ob_rpc_struct.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;

namespace sql
{

ObTruncateTableResolver::ObTruncateTableResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObTruncateTableResolver::~ObTruncateTableResolver()
{
}

int ObTruncateTableResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parser_tree);
  ObTruncateTableStmt *truncate_table_stmt = NULL;
  bool is_mysql_tmp_table = false;
  if (OB_ISNULL(session_info_) || OB_ISNULL(node) ||
      T_TRUNCATE_TABLE != node->type_ ||
      OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null or parser error", K(ret));
  }

  //create alter table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (truncate_table_stmt = create_stmt<ObTruncateTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create alter table stmt", K(ret));
    } else {
      stmt_ = truncate_table_stmt;
    }
    ParseNode *relation_node = node->children_[TABLE_NODE];
    if (OB_SUCC(ret)) {
      if (NULL != relation_node) {
      //resolve table
        ObString table_name;
        ObString database_name;
        if (OB_FAIL(resolve_table_relation_node(relation_node,
                                                table_name,
                                                database_name))) {
          LOG_WARN("failed to resolve table name.",
                       K(table_name), K(database_name), K(ret));
        } else {
          truncate_table_stmt->set_table_name(table_name);
          truncate_table_stmt->set_database_name(database_name);
          truncate_table_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
          if (ObSchemaChecker::is_ora_priv_check()) {
            uint64_t tenant_id = session_info_->get_effective_tenant_id();
            const share::schema::ObTableSchema *table_schema = NULL;
            if (OB_ISNULL(schema_checker_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("schema checker ptr is null", K(ret));
            } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id,
                                                                 database_name,
                                                                 table_name,
                                                                 false,
                                                                 table_schema))) {
              if (OB_TABLE_NOT_EXIST == ret) {
                LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(database_name),
                                                   to_cstring(table_name));
              }
              LOG_WARN("fail to get table schema", K(ret));
            } else if (OB_FAIL(schema_checker_->check_ora_ddl_priv(
                                    tenant_id,
                                    session_info_->get_priv_user_id(),
                                    database_name,
                                    table_schema->get_table_id(),
                                    static_cast<uint64_t>(share::schema::ObObjectType::TABLE),
                                    stmt::T_TRUNCATE_TABLE,
                                    session_info_->get_enable_role_array()))) {
              if (OB_TABLE_NOT_EXIST == ret) {
                LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(database_name),
                                                   to_cstring(table_name));
              }
              LOG_WARN("failed to check ora ddl priv", K(database_name), K(table_name),
                       K(session_info_->get_priv_user_id()), K(ret));
            }
          }
        }
      } else {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("relation node should not be null!", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const ObTableSchema *orig_table_schema = NULL;
    if (OB_FAIL(schema_checker_->get_table_schema(truncate_table_stmt->get_tenant_id(),
                                                  truncate_table_stmt->get_database_name(),
                                                  truncate_table_stmt->get_table_name(),
                                                  false,
                                                  orig_table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(truncate_table_stmt->get_table_name()));
      if (NULL == orig_table_schema && OB_TABLE_NOT_EXIST == ret) {
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST,
                       to_cstring(truncate_table_stmt->get_database_name()),
                       to_cstring(truncate_table_stmt->get_table_name()));
      }
    } else {
      if (orig_table_schema->is_oracle_tmp_table()) {
        truncate_table_stmt->set_truncate_oracle_temp_table();
        truncate_table_stmt->set_oracle_temp_table_type(orig_table_schema->get_table_type());
      }
      if (orig_table_schema->is_mysql_tmp_table()) {
        is_mysql_tmp_table = true; 
      }
    }
  }

  return ret;
}

} //namespace common
} //namespace oceanbase
