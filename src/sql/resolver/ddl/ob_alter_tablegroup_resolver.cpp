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
#include "sql/resolver/ddl/ob_alter_tablegroup_resolver.h"
#include "sql/resolver/ddl/ob_tablegroup_resolver.h"
#include "share/ob_define.h"
#include "share/ob_rpc_struct.h"
#include "sql/session/ob_sql_session_info.h"


namespace oceanbase
{
using namespace share::schema;
using namespace common;

namespace sql
{

using share::schema::AlterColumnSchema;

ObAlterTablegroupResolver::ObAlterTablegroupResolver(ObResolverParams &params)
    : ObTableGroupResolver(params)
{
}

ObAlterTablegroupResolver::~ObAlterTablegroupResolver()
{
}

int ObAlterTablegroupResolver::resolve(const ParseNode &parser_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parser_tree);
  if (OB_ISNULL(session_info_) || OB_ISNULL(node) ||
      T_ALTER_TABLEGROUP != node->type_ || OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null or parser error", K(ret));
  } else {
    uint64_t compat_version = OB_INVALID_VERSION;
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      LOG_WARN("get min data_version failed", K(ret), K(tenant_id));
    } else if (compat_version < DATA_VERSION_4_2_0_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("can not alter tablegroup while observer is upgrading", KR(ret), K(tenant_id));
    }
  }
  ObAlterTablegroupStmt *alter_tablegroup_stmt = NULL;
  if (OB_SUCC(ret)) {
    //create alter table stmt
    if (NULL == (alter_tablegroup_stmt = create_stmt<ObAlterTablegroupStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create alter table stmt", K(ret));
    } else {
      stmt_ = alter_tablegroup_stmt;
    }

    if (OB_SUCC(ret)) {
      if (NULL != node->children_[TG_NAME] && T_IDENT == node->children_[TG_NAME]->type_) {
        ObString tablegroup_name;
        tablegroup_name.assign_ptr(node->children_[TG_NAME]->str_value_,
                                   static_cast<int32_t>(node->children_[TG_NAME]->str_len_));
        alter_tablegroup_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
        alter_tablegroup_stmt->set_tablegroup_name(tablegroup_name);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null or node type is not T_IDENT", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(node->children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is null", K(ret));
    } else if (T_TABLE_LIST == node->children_[1]->type_) {
      ParseNode *table_list_node = node->children_[TABLE_LIST];
      for(int32_t i = 0; OB_SUCC(ret) && i < table_list_node->num_child_; ++i) {
        ParseNode *relation_node = table_list_node->children_[i];
        if (NULL != relation_node) {
          ObString table_name;
          ObString database_name;
          obrpc::ObTableItem table_item;
          if (OB_FAIL(resolve_table_relation_node(relation_node,
                                                  table_name,
                                                  database_name))) {
            LOG_WARN("failed to resolve table name", K(ret), K(table_item));
          } else {
            table_item.table_name_ = table_name;
            table_item.database_name_ = database_name;
            if (OB_FAIL(alter_tablegroup_stmt->add_table_item(table_item))) {
              LOG_WARN("failed to add table item!", K(ret), K(table_item));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node is null", K(ret));
        }
      } //end for
    } else if (T_ALTER_TABLEGROUP_ACTION_LIST == node->children_[1]->type_) {
      if (OB_FAIL(resolve_tablegroup_option(alter_tablegroup_stmt, node->children_[1]))) {
        LOG_WARN("fail to resolve tablegroup option", K(ret));
      } else {
        alter_tablegroup_stmt->set_alter_option_set(get_alter_option_bitset());
      }
    } else if (T_ALTER_PARTITION_OPTION == node->children_[1]->type_) {
      // ignore partition info after 4.2
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported to alter tablegroup partition", KR(ret), K(alter_tablegroup_stmt->get_tablegroup_name()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter tablegroup partition option");
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node type", K(ret), K(node->children_[1]->type_));
    }
  }
  return ret;
}

} //namespace common
} //namespace oceanbase
