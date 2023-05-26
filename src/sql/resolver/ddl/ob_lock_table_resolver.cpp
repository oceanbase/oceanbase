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
#include "sql/parser/ob_parser_utils.h"

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
  // TODO: forbid dblink lock table after yanyuan.cxf deal with mysql mode.
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
  ObLockTableStmt *lock_stmt = nullptr;

  if (3 != parse_tree.num_child_) {
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

  // 1. resolve table item
  if (OB_SUCC(ret)) {
    ParseNode *table_node = parse_tree.children_[TABLE_LIST];
    if (OB_ISNULL(table_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (OB_FAIL(resolve_table_list(*table_node))) {
      LOG_WARN("resolve table failed", K(ret));
    }
  }

  // 2. resolve lock mode
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_lock_mode(*parse_tree.children_[LOCK_MODE]))) {
      LOG_WARN("resolve lock mode failed", K(ret));
    }
  }
  // 3. resolve wait
  if (OB_SUCC(ret)) {
    // this node maybe null if user didn't input opt about wait
    if (OB_NOT_NULL(parse_tree.children_[WAIT])) {
      if (OB_FAIL(resolve_wait_lock(*parse_tree.children_[WAIT]))) {
        LOG_WARN("resolve wait opt for table lock failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLockTableResolver::resolve_table_list(const ParseNode &table_list)
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

int ObLockTableResolver::resolve_wait_lock(const ParseNode &parse_tree)
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
