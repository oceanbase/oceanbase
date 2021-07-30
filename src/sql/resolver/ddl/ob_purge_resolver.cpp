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

namespace oceanbase {
using namespace common;

namespace sql {
/**
 * Purge table
 */
int ObPurgeTableResolver::resolve(const ParseNode& parser_tree)
{
  int ret = OB_SUCCESS;
  ObPurgeTableStmt* purge_table_stmt = NULL;
  if (OB_ISNULL(session_info_) || T_PURGE_TABLE != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  }
  // create Purge table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (purge_table_stmt = create_stmt<ObPurgeTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create purege table stmt", K(ret));
    } else {
      stmt_ = purge_table_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    purge_table_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    // Purge table
    ParseNode* table_node = parser_tree.children_[TABLE_NODE];
    ObString tb_name;
    ObString db_name;
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_node should not be null", K(ret));
    } else if (resolve_table_relation_node(table_node, tb_name, db_name, false /*get origin db_name*/)) {
      LOG_WARN("failed to resolve_table_relation_node", K(ret));
    } else if (tb_name.empty() || db_name.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table name should not be empty", K(ret));
    } else {
      purge_table_stmt->set_database_name(db_name);
      purge_table_stmt->set_table_name(tb_name);
    }
  }
  return ret;
}

/**
 * Purge index
 */
int ObPurgeIndexResolver::resolve(const ParseNode& parser_tree)
{
  int ret = OB_SUCCESS;
  ObPurgeIndexStmt* purge_index_stmt = NULL;
  ObString db_name;
  if (OB_ISNULL(session_info_) || T_PURGE_INDEX != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  }
  // create Purge index stmt
  if (OB_SUCC(ret)) {
    if (NULL == (purge_index_stmt = create_stmt<ObPurgeIndexStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create purege index stmt", K(ret));
    } else {
      stmt_ = purge_index_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    purge_index_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    // Purge table
    ParseNode* table_node = parser_tree.children_[TABLE_NODE];
    ObString tb_name;
    ObString db_name;
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_node should not be null", K(ret));
    } else if (resolve_table_relation_node(table_node, tb_name, db_name, false /*get origin db_name*/)) {
      LOG_WARN("failed to resolve_table_relation_node", K(ret));
    } else if (tb_name.empty() || db_name.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table name should not be empty", K(ret));
    } else {
      purge_index_stmt->set_database_name(db_name);
      purge_index_stmt->set_table_name(tb_name);
    }
  }
  return ret;
}
/**
 * Purge database
 */
int ObPurgeDatabaseResolver::resolve(const ParseNode& parser_tree)
{
  int ret = OB_SUCCESS;
  ObPurgeDatabaseStmt* purge_database_stmt = NULL;
  if (OB_ISNULL(session_info_) || T_PURGE_DATABASE != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  }
  // create Purge table stmt
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
    ParseNode* dbname_node = parser_tree.children_[DATABASE_NODE];
    int32_t max_database_name_length = GET_MIN_CLUSTER_VERSION() < CLUSTER_CURRENT_VERSION
                                           ? OB_MAX_DATABASE_NAME_LENGTH - 1
                                           : OB_MAX_DATABASE_NAME_LENGTH;
    if (OB_ISNULL(dbname_node) || OB_UNLIKELY(T_IDENT != dbname_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (OB_UNLIKELY(static_cast<int32_t>(dbname_node->str_len_) > max_database_name_length)) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)dbname_node->str_len_, dbname_node->str_value_);
    } else {
      db_name.assign_ptr(dbname_node->str_value_, static_cast<int32_t>(dbname_node->str_len_));
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
int ObPurgeTenantResolver::resolve(const ParseNode& parser_tree)
{
  int ret = OB_SUCCESS;
  ObPurgeTenantStmt* purge_tenant_stmt = NULL;
  if (OB_ISNULL(session_info_) || T_PURGE_TENANT != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  }
  // create Purge table stmt
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
    ParseNode* tenant_node = parser_tree.children_[TENANT_NODE];
    int32_t max_database_name_length = GET_MIN_CLUSTER_VERSION() < CLUSTER_CURRENT_VERSION
                                           ? OB_MAX_DATABASE_NAME_LENGTH - 1
                                           : OB_MAX_DATABASE_NAME_LENGTH;
    if (OB_ISNULL(tenant_node) || OB_UNLIKELY(T_IDENT != tenant_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (OB_UNLIKELY(static_cast<int32_t>(tenant_node->str_len_) > max_database_name_length)) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)tenant_node->str_len_, tenant_node->str_value_);
    } else {
      tenant_name.assign_ptr(tenant_node->str_value_, static_cast<int32_t>(tenant_node->str_len_));
      purge_tenant_stmt->set_tenant_name(tenant_name);
    }
  }
  return ret;
}

/**
 * Purge Recyclebin
 */
int ObPurgeRecycleBinResolver::resolve(const ParseNode& parser_tree)
{
  int ret = OB_SUCCESS;
  ObString db_name;
  ObPurgeRecycleBinStmt* purge_recyclebin_stmt = NULL;
  if (OB_ISNULL(session_info_) || T_PURGE_RECYCLEBIN != parser_tree.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  }
  // create Purge table stmt
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
    db_name.assign_ptr(session_info_->get_database_name().ptr(), session_info_->get_database_name().length());
    if (db_name.empty()) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("no database selected");
    }
    purge_recyclebin_stmt->set_database_name(db_name);
    purge_recyclebin_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    purge_recyclebin_stmt->set_expire_time(current_time);
    purge_recyclebin_stmt->set_purge_num(obrpc::ObPurgeRecycleBinArg::DEFAULT_PURGE_EACH_TIME);
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      OZ(schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_PURGE_RECYCLEBIN,
          session_info_->get_enable_role_array()));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
