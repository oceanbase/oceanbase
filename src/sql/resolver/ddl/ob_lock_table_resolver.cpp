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
#include "ob_lock_table_resolver.h"
#include "ob_lock_table_stmt.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

int ObLockTableResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObLockTableStmt *lock_stmt = NULL;

  if (T_LOCK_TABLE != parse_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong node type", K(ret), K(parse_tree.type_));
  } else if (is_mysql_mode()) {
    ret = resolve_mysql_mode(parse_tree);
  } else {
    ret = resolve_oracle_mode(parse_tree);
  }
  return ret;
}

int ObLockTableResolver::resolve_mysql_mode(const ParseNode &parse_tree)
{
  // TODO: yanyuan.cxf deal with mysql mode later.
  int ret = OB_SUCCESS;
  ObLockTableStmt *lock_stmt = NULL;

  if (OB_ISNULL(lock_stmt = create_stmt<ObLockTableStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create lock stmt failed");
  } else {
    stmt_ = lock_stmt;
  }
  return ret;
}

int ObLockTableResolver::resolve_oracle_mode(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObLockTableStmt *lock_stmt = NULL;

  if (2 != parse_tree.num_child_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong node number", K(ret), K(parse_tree.num_child_));
  } else if (OB_ISNULL(parse_tree.children_)
             || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node child and session should not be null", K(ret), K(parse_tree.children_),
             K(session_info_));
  } else if (OB_ISNULL(lock_stmt = create_stmt<ObLockTableStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create lock stmt failed");
  } else {
    stmt_ = lock_stmt;
  }

  // 1. resolve table items
  if (OB_SUCC(ret)) {
    ParseNode *table_node = parse_tree.children_[TABLE];
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (OB_FAIL(resolve_table_relation_node(*table_node))) {
      LOG_WARN("resolve table failed", K(ret));
    }
  }
  // 2. resolve lock mode
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_lock_mode(*parse_tree.children_[LOCK_MODE]))) {
      LOG_WARN("resolve where clause failed", K(ret));
    }
  }

  return ret;
}

// TODO: yanyuan.cxf only deal with one table name.
int ObLockTableResolver::resolve_table_relation_node(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  // TODO: yanyuan.cxf release TableItem
  ObString table_name;
  ObString database_name;
  const ObTableSchema *table_schema = NULL;
  int64_t tenant_id = session_info_->get_effective_tenant_id();
  uint64_t database_id = OB_INVALID_ID;
  ObLockTableStmt *lock_stmt = get_lock_table_stmt();

  if (OB_ISNULL(lock_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock stmt should not be null", K(ret));
  } else if (OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_checker should not be null", K(ret));
  } else if (OB_FAIL(ObDDLResolver::resolve_table_relation_node(&parse_tree,
                                                                table_name,
                                                                database_name))) {
    LOG_WARN("failed to resolve table relation node", K(ret));
  } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id,
                                                      database_name,
                                                      database_id))) {
    LOG_WARN("failed to get database id", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id,
                                                       database_name,
                                                       table_name,
                                                       false,
                                                       table_schema))){
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    LOG_WARN("null table schema", K(ret));
  } else {
    lock_stmt->set_table_id(table_schema->get_table_id());
  }
  return ret;
}

int ObLockTableResolver::resolve_lock_mode(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObLockTableStmt *lock_stmt = get_lock_table_stmt();
  if (OB_ISNULL(lock_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock stmt should not be null");
  } else {
    lock_stmt->set_lock_mode(parse_tree.value_);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
