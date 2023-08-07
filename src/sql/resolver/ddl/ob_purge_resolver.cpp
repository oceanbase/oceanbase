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
#include "sql/resolver/ddl/ob_purge_resolver.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace common;

namespace sql
{
/**
 * Purge table
 */
int ObPurgeTableResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObPurgeTableStmt *purge_table_stmt = NULL;
  if (OB_ISNULL(session_info_) || T_PURGE_TABLE != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  }
  //create Purge table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (purge_table_stmt = create_stmt<ObPurgeTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create purege table stmt", K(ret));
    } else {
      stmt_ = purge_table_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    //Purge table
    ParseNode *table_node = parser_tree.children_[TABLE_NODE];
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    uint64_t db_id = OB_INVALID_ID;
    ObString db_name;
    ObString table_name;
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_node should not be null", K(ret));
    } else if (OB_FAIL(resolve_table_relation_node(table_node, table_name, db_name))) {
      LOG_WARN("failed to resolve_table_relation_node", K(ret));
    } else if (session_info_->get_database_name() != db_name) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "purge tables in recyclebin dropped from other schema");
      LOG_WARN("purge tables in recyclebin dropped from other schema is not supported",
               K(ret), K(db_name), K(session_info_->get_database_name()));
      LOG_WARN("purge table db.xx should not specified with db name", K(ret));
    } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, db_id))) {
      LOG_WARN("fail to get database id", K(ret), K(tenant_id), K(db_name));
    } else if (table_name.empty()){
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table name should not be empty", K(ret));
    } else {
      purge_table_stmt->set_tenant_id(tenant_id);
      purge_table_stmt->set_database_id(db_id);
      purge_table_stmt->set_table_name(table_name);
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                                session_info_->get_priv_user_id(),
                                                ObString(OB_RECYCLEBIN_SCHEMA_NAME),
                                                stmt::T_PURGE_TABLE,
                                                session_info_->get_enable_role_array()));
      }
    }
  }
  return ret;
}

/**
 * Purge index
 */
int ObPurgeIndexResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObPurgeIndexStmt *purge_index_stmt = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info or schema_checker is null", K(ret), K(schema_checker_), K(session_info_));
  } else if (T_PURGE_INDEX != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree",  K(parser_tree.type_));
  }
  //create Purge table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (purge_index_stmt = create_stmt<ObPurgeIndexStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create purege table stmt", K(ret));
    } else {
      stmt_ = purge_index_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    //Purge table
    ParseNode *table_node = parser_tree.children_[TABLE_NODE];
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    uint64_t db_id = OB_INVALID_ID;
    ObString db_name;
    ObString table_name;

    const share::schema::ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_node should not be null", K(ret));
    } else if (OB_FAIL(resolve_table_relation_node(table_node,
                                           table_name,
                                           db_name))){
      LOG_WARN("failed to resolve_table_relation_node", K(ret));
    } else if (session_info_->get_database_name() != db_name){
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "purge indexes in recyclebin dropped from other schema");
      LOG_WARN("purge indexes in recyclebin dropped from other schema is not supported",
               K(ret), K(db_name), K(session_info_->get_database_name()));
    } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, db_id))) {
      LOG_WARN("fail to get database id", K(ret), K(tenant_id), K(db_name));
    } else if (table_name.empty()){
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table name should not be empty", K(ret));
    } else {
      UNUSED(schema_checker_->get_table_schema(tenant_id,
                                               OB_RECYCLEBIN_SCHEMA_ID,
                                               table_name,
                                               true, /*is_index*/
                                               false, /*cte_table_fisrt*/
                                               false/*is_hidden*/,
                                               table_schema));
      purge_index_stmt->set_tenant_id(tenant_id);
      purge_index_stmt->set_database_id(db_id);
      purge_index_stmt->set_table_name(table_name);
      purge_index_stmt->set_table_id(OB_NOT_NULL(table_schema) ? table_schema->get_table_id() : OB_INVALID_ID);
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
        OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                                session_info_->get_priv_user_id(),
                                                ObString(OB_RECYCLEBIN_SCHEMA_NAME),
                                                stmt::T_PURGE_INDEX,
                                                session_info_->get_enable_role_array()));
      }
    }
  }
  return ret;
}
/**
 * Purge database
 */
int ObPurgeDatabaseResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObPurgeDatabaseStmt *purge_database_stmt = NULL;
  if (OB_ISNULL(session_info_) || T_PURGE_DATABASE != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  }
  //create Purge table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (purge_database_stmt = create_stmt<ObPurgeDatabaseStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create rename table stmt", K(ret));
    } else {
      stmt_ = purge_database_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    purge_database_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    ObString db_name;
    ParseNode *dbname_node = parser_tree.children_[DATABASE_NODE];
    int32_t max_database_name_length = OB_MAX_DATABASE_NAME_LENGTH;
    if (OB_ISNULL(dbname_node) || OB_UNLIKELY(T_IDENT != dbname_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (OB_UNLIKELY(
            static_cast<int32_t>(dbname_node->str_len_) > max_database_name_length)) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)dbname_node->str_len_, dbname_node->str_value_);
    } else {
      db_name.assign_ptr(dbname_node->str_value_,
                         static_cast<int32_t>(dbname_node->str_len_));
      if (db_name.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("database_name is empty()", K(ret));
      } else {
        purge_database_stmt->set_db_name(db_name);
      }
    }
  }
  return ret;
}


/**
 * Purge tenant
 */
int ObPurgeTenantResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObPurgeTenantStmt *purge_tenant_stmt = NULL;
  if (OB_ISNULL(session_info_) || T_PURGE_TENANT != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  }
  //create Purge table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (purge_tenant_stmt = create_stmt<ObPurgeTenantStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create Purge tenant stmt", K(ret));
    } else {
      stmt_ = purge_tenant_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    purge_tenant_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    ObString tenant_name;
    ParseNode *tenant_node = parser_tree.children_[TENANT_NODE];
    int32_t max_database_name_length = OB_MAX_DATABASE_NAME_LENGTH;
    if (OB_ISNULL(tenant_node) || OB_UNLIKELY(T_IDENT != tenant_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (OB_UNLIKELY(
            static_cast<int32_t>(tenant_node->str_len_) > max_database_name_length)) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)tenant_node->str_len_, tenant_node->str_value_);
    } else {
      tenant_name.assign_ptr(tenant_node->str_value_,
                             static_cast<int32_t>(tenant_node->str_len_));
      purge_tenant_stmt->set_tenant_name(tenant_name);
    }
  }
  return ret;
}

/**
 * Purge Recyclebin
 */
int ObPurgeRecycleBinResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObPurgeRecycleBinStmt *purge_recyclebin_stmt = NULL;
  if (OB_ISNULL(session_info_) || T_PURGE_RECYCLEBIN != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  }
  //create Purge table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (purge_recyclebin_stmt = create_stmt<ObPurgeRecycleBinStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create Purge tenant stmt", K(ret));
    } else {
      stmt_ = purge_recyclebin_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    int64_t current_time = ObTimeUtility::current_time();
    purge_recyclebin_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    purge_recyclebin_stmt->set_expire_time(current_time);
    purge_recyclebin_stmt->set_purge_num(obrpc::ObPurgeRecycleBinArg::DEFAULT_PURGE_EACH_TIME);
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                              session_info_->get_priv_user_id(),
                                              ObString(""),
                                              stmt::T_PURGE_RECYCLEBIN,
                                              session_info_->get_enable_role_array()));
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
