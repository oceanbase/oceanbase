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
#include "sql/resolver/ddl/ob_rename_table_resolver.h"
#include "share/ob_define.h"
#include "share/ob_rpc_struct.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;

namespace sql
{

ObRenameTableResolver::ObRenameTableResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObRenameTableResolver::~ObRenameTableResolver()
{
}

int ObRenameTableResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parser_tree);
  ObRenameTableStmt *rename_table_stmt = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_) || OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info or schema_checker is null", K(ret), K(schema_checker_), K(session_info_), K(node));
  } else if (T_RENAME_TABLE != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree",  K(node->type_));
  }
  //create rename table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (rename_table_stmt = create_stmt<ObRenameTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create rename table stmt", K(ret));
    } else {
      stmt_ = rename_table_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    int64_t count = node->num_child_;
    rename_table_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ParseNode *rename_node = node->children_[i];
      if (OB_ISNULL(rename_node)) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("node should not be null!", K(ret));
      } else if (OB_FAIL(resolve_rename_action(*rename_node))) {
        LOG_WARN("failed to resolve rename action node!", K(ret));
      }
    }
  }

  return ret;
}

int ObRenameTableResolver::resolve_rename_action(const ParseNode &rename_action_node)
{
  int ret = OB_SUCCESS;
  //resolve table
  //rename table db.t1 to db2.t2, db.t3 to db.t4 ...
  ObString origin_db_name;
  ObString origin_table_name;
  ObString new_db_name;
  ObString new_table_name;
  ObRenameTableStmt *rename_table_stmt = get_rename_table_stmt();
  if (OB_ISNULL(rename_table_stmt) || NAME_NODE_COUNT != rename_action_node.num_child_
      || OB_ISNULL(rename_action_node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rename_table_stmt is null or parser error", K(ret));
  }
  if (OB_SUCC(ret)) {
    //上面判断了rename_action_node.children_指针
    ParseNode *origin_node = rename_action_node.children_[OLD_NAME_NODE];
    ParseNode *new_node = rename_action_node.children_[NEW_NAME_NODE];
    const share::schema::ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(origin_node) || OB_ISNULL(new_node)) {
      ret = OB_ERR_PARSE_SQL;
      LOG_WARN("origin_node or new node is null",
                   K(origin_node), K(new_node), K(ret));
    } else if (lib::is_oracle_mode()
              && OB_UNLIKELY((NULL != origin_node->children_[0] || NULL != new_node->children_[0]))) {
      ret = OB_ERR_SPECIFY_DATABASE_NOT_ALLOWED;
      LOG_WARN("can not specify owner name of table", K(ret)); 
    } else if (OB_FAIL(resolve_table_relation_node(origin_node,
                                                   origin_table_name,
                                                   origin_db_name))) {
      LOG_WARN("failed to resolve origin table node.",
                   K(origin_table_name), K(origin_db_name), K(ret));
    } else if (OB_FAIL(resolve_table_relation_node(new_node,
                                                   new_table_name,
                                                   new_db_name))){
      LOG_WARN("failed to resolve new table node.",
                   K(new_table_name), K(new_db_name), K(ret));
    } else {
      UNUSED(schema_checker_->get_table_schema(rename_table_stmt->get_tenant_id(),
                                                               origin_db_name,
                                                               origin_table_name,
                                                               false, /*is_index*/
                                                               table_schema));
      ParseNode *new_db_node = new_node->children_[0];
      obrpc::ObRenameTableItem rename_table_item;
      rename_table_item.origin_db_name_ = origin_db_name;
      rename_table_item.origin_table_name_ = origin_table_name;
      if (lib::is_oracle_mode()) {
        rename_table_item.new_db_name_ = NULL == new_db_node ? origin_db_name : new_db_name;
      } else {
        rename_table_item.new_db_name_ = new_db_name;
      }
      rename_table_item.new_table_name_ = new_table_name;
      rename_table_item.origin_table_id_ = NULL != table_schema ? table_schema->get_table_id() : common::OB_INVALID_ID;
      if (OB_FAIL(rename_table_stmt->add_rename_table_item(rename_table_item))) {
        LOG_WARN("failed to add rename table item", K(rename_table_item), K(ret));
      }
    }
  }
  return ret;
}


} //namespace common
} //namespace oceanbase
