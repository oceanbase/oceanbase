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
#include "ob_optimize_resolver.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::sql;

int ObOptimizeTableResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObOptimizeTableStmt *stmt = nullptr;
  if (OB_ISNULL(session_info_) || T_OPTIMIZE_TABLE != parser_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(parser_tree.type_));
  } else {
    if (OB_ISNULL(stmt = create_stmt<ObOptimizeTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create optimize table stmt", K(ret));
    } else {
      stmt_ = stmt;
    }
  }
  if (OB_SUCC(ret)) {
    stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    obrpc::ObOptimizeTableArg &arg = stmt->get_optimize_table_arg();
    arg.tenant_id_ = session_info_->get_effective_tenant_id();
    void *buf = nullptr;
    ObPlacementHashSet<obrpc::ObTableItem> *table_item_set = nullptr;
    if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObPlacementHashSet<obrpc::ObTableItem>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      table_item_set = new(buf)ObPlacementHashSet<obrpc::ObTableItem>();
      ObString database_name;
      ObString table_name;
      obrpc::ObTableItem table_item;
      ParseNode *table_node = nullptr;
      int64_t max_table_num = 1;
      if (lib::is_oracle_mode()) {
        max_table_num = 1;
      } else {
        if (OB_UNLIKELY(!parser_tree.children_[TABLE_LIST_NODE])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to parse node", K(ret));
        } else {
          max_table_num = parser_tree.children_[TABLE_LIST_NODE]->num_child_;
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < max_table_num; ++i) {
        table_node = lib::is_oracle_mode() ? parser_tree.children_[TABLE_LIST_NODE] :
          parser_tree.children_[TABLE_LIST_NODE]->children_[i];
        if (nullptr == table_node) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, table node must not be NULL", K(ret));
        } else {
          database_name.reset();
          table_name.reset();
          if (OB_FAIL(resolve_table_relation_node(table_node, table_name, database_name))) {
            LOG_WARN("fail to resolve table relation node", K(ret));
          } else {
            table_item.reset();
            if (OB_FAIL(session_info_->get_name_case_mode(table_item.mode_))) {
              LOG_WARN("fail to get name case mode", K(ret));
            } else {
              table_item.database_name_ = database_name;
              table_item.table_name_ = table_name;
              if (OB_HASH_EXIST == table_item_set->exist_refactored(table_item)) {
                ret = OB_ERR_NONUNIQ_TABLE;
                LOG_USER_ERROR(OB_ERR_NONUNIQ_TABLE, table_item.table_name_.length(), table_item.table_name_.ptr());
              } else if (OB_FAIL(table_item_set->set_refactored(table_item))) {
                LOG_WARN("fail to add table item", K(ret));
              } else if (OB_FAIL(stmt->add_table_item(table_item))) {
                LOG_WARN("fail to add table item", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObOptimizeTenantResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObOptimizeTenantStmt *stmt = nullptr;
  if (OB_ISNULL(session_info_) || T_OPTIMIZE_TENANT != parser_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(parser_tree.type_));
  } else {
    if (OB_ISNULL(stmt = create_stmt<ObOptimizeTenantStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create optimize tenant stmt", K(ret));
    } else {
      stmt_ = stmt;
    }
  }
  if (OB_SUCC(ret)) {
    ObString tenant_name;
    ParseNode *node = const_cast<ParseNode*>(&parser_tree);
    if (nullptr == node) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, node must not be NULL", K(ret));
    } else if (OB_UNLIKELY(T_IDENT != node->children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse node", K(ret));
    } else {
      tenant_name.assign_ptr(const_cast<char *>(node->children_[0]->str_value_), static_cast<int32_t>(node->children_[0]->str_len_));
      stmt->set_tenant_name(tenant_name);
    }
  }
  return ret;
}

int ObOptimizeAllResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ObOptimizeAllStmt *stmt = nullptr;
  if (OB_ISNULL(session_info_) || T_OPTIMIZE_ALL != parser_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(parser_tree.type_));
  } else {
    if (OB_ISNULL(stmt = create_stmt<ObOptimizeAllStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for optimize all stmt", K(ret));
    } else {
      stmt_ = stmt;
    }
  }
  return ret;
}
