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

namespace oceanbase
{
namespace sql
{
using namespace common;

int ObLockTableResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObLockTableStmt *lock_stmt = nullptr;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session should not be null", K(ret), K(session_info_));
  } else if (T_LOCK_TABLE != parse_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong node type", K(ret), K(parse_tree.type_));
  } else if (OB_ISNULL(lock_stmt = create_stmt<ObLockTableStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create lock stmt failed");
  } else {
    stmt_ = lock_stmt;
  }
  if (OB_FAIL(ret)) {
  } else if (is_mysql_mode()) {
    ret = resolve_mysql_mode_(parse_tree);
  } else {
    ret = resolve_oracle_mode_(parse_tree);
  }
  return ret;
}

int ObLockTableResolver::resolve_mysql_mode_(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *lock_list = NULL;
  ObString db_name;

  uint64_t tenant_id = session_info_->get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    // if tenant config is invalid, this config will be set as false
    LOG_WARN("tenant config is invalid", K(tenant_id));
  } else if (tenant_config->enable_lock_priority) {
    ObLockTableStmt *lock_stmt = static_cast<ObLockTableStmt *>(stmt_);
    if (parse_tree.num_child_ == 0) {
      // it is unlock table stmt
    } else if (parse_tree.num_child_ != 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("mysql lock table should only has one parameter which is mysql lock list", K(ret));
    } else if (OB_ISNULL(parse_tree.children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child may be lost", K(ret));
    } else if (FALSE_IT(lock_list = parse_tree.children_[MYSQL_LOCK_LIST])) {
    } else if (lock_list->type_ != T_MYSQL_LOCK_LIST) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong lock list", K(ret), K(lock_list->type_));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < lock_list->num_child_; ++i) {
        const ParseNode *lock_node = lock_list->children_[i];
        if (OB_ISNULL(lock_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("lock node is null");
        } else if (OB_FAIL(resolve_mysql_lock_node_(*lock_node))) {
          LOG_WARN("resolve mysql lock node failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (parse_tree.num_child_ == 0) {
        lock_stmt->set_lock_stmt_type(ObLockTableStmt::MYSQL_UNLOCK_TABLE_STMT);
      } else {
        lock_stmt->set_lock_stmt_type(ObLockTableStmt::MYSQL_LOCK_TABLE_STMT);
      }
    }
  }
  return ret;
}

int ObLockTableResolver::resolve_mysql_lock_node_(const ParseNode &lock_node)
{
  // TODO: forbid dblink、function table、json table's lock table.
  int ret = OB_SUCCESS;
  if (lock_node.type_ != T_MYSQL_LOCK_NODE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this is not a lock node", K(ret), K(lock_node.type_));
  } else if (lock_node.num_child_ != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock node should only have 2 argument", K(ret), K(lock_node.num_child_));
  } else {
    const ParseNode *table_node = lock_node.children_[LOCK_TABLE_NODE];
    const ParseNode *lock_type = lock_node.children_[LOCK_MODE];
    const ObTableSchema *table_schema = nullptr;
    TableItem *table_item = nullptr;
    ObString dblink_name; //no use
    bool is_reverse_link = false; //no use
    bool has_dblink_node = false;
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table node is null");
    } else if (OB_FAIL(ObStmtResolver::resolve_dblink_name(table_node->children_[0],
                                                           session_info_->get_effective_tenant_id(),
                                                           dblink_name,
                                                           is_reverse_link,
                                                           has_dblink_node))) {
      LOG_WARN("failed to resolve dblink", K(ret));
    } else if (has_dblink_node) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("dblink not support lock table", K(dblink_name), K(is_reverse_link), K(ret));
    } else if (OB_FAIL(ObDMLResolver::resolve_table(*table_node, table_item))) {
      LOG_WARN("failed to resolve table", K(ret));
    } else if (table_item->is_function_table() || table_item->is_json_table()) {//兼容oracle行为
      ret = OB_WRONG_TABLE_NAME;
      LOG_WARN("invalid table name", K(ret));
    } else {
      ObMySQLLockNode node;
      node.table_item_ = table_item;
      node.lock_mode_ = lock_type->value_;
      if (OB_FAIL(static_cast<ObLockTableStmt *>(stmt_)->add_mysql_lock_node(node))) {
        LOG_WARN("add mysql lock node failed", K(ret), K(node));
      } else {
        LOG_DEBUG("succ to add lock table item", K(node));
      }
    }
  }
  return ret;
}

int ObLockTableResolver::resolve_oracle_mode_(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;

  ObLockTableStmt *lock_stmt = static_cast<ObLockTableStmt *>(stmt_);
  ParseNode *table_node = NULL;
  // 1. resolve table item
  // 2. resolve lock mode
  // 3. resolve wait
  // 4. set stmt type
  if (3 != parse_tree.num_child_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrong node number", K(ret), K(parse_tree.num_child_));
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node child should not be null", K(ret), K(parse_tree.children_));
  } else if (OB_ISNULL(table_node = parse_tree.children_[TABLE_LIST])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_FAIL(resolve_oracle_table_list_(*table_node))) {
    LOG_WARN("resolve table failed", K(ret));
  } else if (OB_FAIL(resolve_oracle_lock_mode_(*parse_tree.children_[LOCK_MODE]))) {
    LOG_WARN("resolve lock mode failed", K(ret));
  } else if (OB_NOT_NULL(parse_tree.children_[WAIT])
             && OB_FAIL(resolve_oracle_wait_lock_(*parse_tree.children_[WAIT]))) {
    // this node maybe null if user didn't input opt about wait
    LOG_WARN("resolve wait opt for table lock failed", K(ret));
  } else {
    lock_stmt->set_lock_stmt_type(ObLockTableStmt::ORACLE_LOCK_TABLE_STMT);
  }

  return ret;
}

int ObLockTableResolver::resolve_oracle_table_list_(const ParseNode &table_list)
{
  int ret = OB_SUCCESS;
  ObLockTableStmt *lock_stmt = get_lock_table_stmt();
  TableItem *table_item = nullptr;

  if (OB_UNLIKELY(T_TABLE_REFERENCES != table_list.type_ &&
                  T_RELATION_FACTOR != table_list.type_) ||
      OB_UNLIKELY(OB_ISNULL(table_list.children_)) ||
      OB_UNLIKELY(table_list.num_child_ < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(table_list.type_), K(table_list.num_child_));
  } else if (OB_ISNULL(lock_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock table stmt", K(lock_stmt));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_list.num_child_; ++i) {
    const ParseNode *table_node = table_list.children_[i];
    const ObTableSchema *table_schema = nullptr;
    ObString dblink_name; //no use
    bool is_reverse_link = false; //no use
    bool has_dblink_node = false;
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table node is null");
    } else if (OB_FAIL(ObStmtResolver::resolve_dblink_name(table_node->children_[0],
                                                           session_info_->get_effective_tenant_id(),
                                                           dblink_name,
                                                           is_reverse_link,
                                                           has_dblink_node))) {
      LOG_WARN("failed to resolve dblink", K(ret));
    } else if (has_dblink_node) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("dblink not support lock table", K(dblink_name), K(is_reverse_link), K(ret));
    } else if (OB_FAIL(ObDMLResolver::resolve_table(*table_node, table_item))) {
      LOG_WARN("failed to resolve table", K(ret));
    } else if (table_item->is_function_table() || table_item->is_json_table()) {//兼容oracle行为
      ret = OB_WRONG_TABLE_NAME;
      LOG_WARN("invalid table name", K(ret));
    } else {
      LOG_DEBUG("succ to add lock table item", KPC(table_item));
    }
  }
  return ret;
}

int ObLockTableResolver::resolve_oracle_lock_mode_(const ParseNode &parse_tree)
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

int ObLockTableResolver::resolve_oracle_wait_lock_(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObLockTableStmt *lock_stmt = get_lock_table_stmt();
  int64_t wait_lock_seconds = parse_tree.value_;
  if (OB_ISNULL(lock_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock stmt should not be null");
  } else if (wait_lock_seconds < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lock wait time should not be negative", K(wait_lock_seconds));
  } else {
    lock_stmt->set_wait_lock_seconds(parse_tree.value_);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
