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

namespace oceanbase {
using namespace share::schema;
using namespace common;

namespace sql {

using share::schema::AlterColumnSchema;

ObAlterTablegroupResolver::ObAlterTablegroupResolver(ObResolverParams& params) : ObTableGroupResolver(params)
{}

ObAlterTablegroupResolver::~ObAlterTablegroupResolver()
{}

int ObAlterTablegroupResolver::resolve(const ParseNode& parser_tree)
{
  int ret = OB_SUCCESS;
  ParseNode* node = const_cast<ParseNode*>(&parser_tree);
  if (OB_ISNULL(session_info_) || OB_ISNULL(node) || T_ALTER_TABLEGROUP != node->type_ || OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null or parser error", K(ret));
  }
  ObAlterTablegroupStmt* alter_tablegroup_stmt = NULL;
  if (OB_SUCC(ret)) {
    // create alter table stmt
    if (NULL == (alter_tablegroup_stmt = create_stmt<ObAlterTablegroupStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create alter table stmt", K(ret));
    } else {
      stmt_ = alter_tablegroup_stmt;
    }

    if (OB_SUCC(ret)) {
      if (NULL != node->children_[TG_NAME] && T_IDENT == node->children_[TG_NAME]->type_) {
        ObString tablegroup_name;
        tablegroup_name.assign_ptr(
            node->children_[TG_NAME]->str_value_, static_cast<int32_t>(node->children_[TG_NAME]->str_len_));
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
      ParseNode* table_list_node = node->children_[TABLE_LIST];
      for (int32_t i = 0; OB_SUCC(ret) && i < table_list_node->num_child_; ++i) {
        ParseNode* relation_node = table_list_node->children_[i];
        if (NULL != relation_node) {
          ObString table_name;
          ObString database_name;
          obrpc::ObTableItem table_item;
          if (OB_FAIL(resolve_table_relation_node(relation_node, table_name, database_name))) {
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
      }  // end for
    } else if (T_ALTER_TABLEGROUP_ACTION_LIST == node->children_[1]->type_) {
      if (OB_FAIL(resolve_tablegroup_option(alter_tablegroup_stmt, node->children_[1]))) {
        LOG_WARN("fail to resolve tablegroup option", K(ret));
      } else {
        alter_tablegroup_stmt->set_alter_option_set(get_alter_option_bitset());
      }
    } else if (T_ALTER_PARTITION_OPTION == node->children_[1]->type_) {
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support to modify partitions while cluster not upgrade to 2.0", K(ret));
      } else if (OB_FAIL(resolve_partition_options(*(node->children_[1])))) {
        LOG_WARN("fail to resolve tablegroup partition option", K(ret));
      } else {
        // Whether partition_array is serialized depends on part_level,
        // because the resolver side does not get the schema,
        // To avoid data loss during serialization, part_level can be set to PARTITION_LEVEL_ONE
        // (Currently, only one-level range/range column partitions can be dynamically added or deleted)
        alter_tablegroup_stmt->get_alter_tablegroup_arg().alter_tablegroup_schema_.set_part_level(PARTITION_LEVEL_ONE);
        bool strict_mode = true;
        if (OB_FAIL(session_info_->is_create_table_strict_mode(strict_mode))) {
          SQL_RESV_LOG(WARN, "failed to get variable ob_create_table_strict_mode");
        } else {
          obrpc::ObCreateTableMode create_mode =
              strict_mode ? obrpc::OB_CREATE_TABLE_MODE_STRICT : obrpc::OB_CREATE_TABLE_MODE_LOOSE;
          alter_tablegroup_stmt->get_alter_tablegroup_arg().create_mode_ = create_mode;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node type", K(ret), K(node->children_[1]->type_));
    }
  }
  return ret;
}

int ObAlterTablegroupResolver::resolve_partition_options(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_ALTER_PARTITION_OPTION != node.type_ || 0 >= node.num_child_ || OB_ISNULL(node.children_) ||
                  OB_ISNULL(node.children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    ObAlterTablegroupStmt* alter_tablegroup_stmt = get_alter_tablegroup_stmt();
    if (OB_SUCC(ret)) {
      obrpc::ObAlterTablegroupArg& arg = alter_tablegroup_stmt->get_alter_tablegroup_arg();
      switch (node.children_[0]->type_) {
        case T_ALTER_PARTITION_ADD: {
          ParseNode* partition_node = node.children_[0];
          if (OB_FAIL(resolve_add_partition(*partition_node))) {
            SQL_RESV_LOG(WARN, "Resolve add partition error!", K(ret));
          } else {
            arg.alter_option_bitset_.add_member(obrpc::ObAlterTablegroupArg::ADD_PARTITION);
          }
          break;
        }
        case T_ALTER_PARTITION_DROP: {
          ParseNode* partition_node = node.children_[0];
          if (OB_FAIL(resolve_drop_partition(*partition_node))) {
            SQL_RESV_LOG(WARN, "Resolve drop partition error!", K(ret));
          } else {
            arg.alter_option_bitset_.add_member(obrpc::ObAlterTablegroupArg::DROP_PARTITION);
          }
          break;
        }
        case T_ALTER_PARTITION_PARTITIONED: {
          ParseNode* partition_node = node.children_[0];
          bool enable_split_partition = false;
          if (OB_ISNULL(partition_node) || 1 != partition_node->num_child_ || OB_ISNULL(partition_node->children_[0])) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "invalid partition node", K(ret), K(partition_node));
          } else if (OB_FAIL(get_enable_split_partition(
                         session_info_->get_effective_tenant_id(), enable_split_partition))) {
            LOG_WARN("failed to get enable split partition config",
                K(ret),
                "tenant_id",
                session_info_->get_effective_tenant_id());
          } else if (!enable_split_partition) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("partitioned table not allow", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "partition table");
          } else if (OB_FAIL(resolve_partition_table_option(
                         alter_tablegroup_stmt, partition_node->children_[0], arg.alter_tablegroup_schema_))) {
            LOG_WARN("fail to resolve partition node", K(ret), K(partition_node));
          } else {
            arg.alter_option_bitset_.add_member(obrpc::ObAlterTablegroupArg::PARTITIONED_TABLE);
          }
          break;
        }
        case T_ALTER_PARTITION_REORGANIZE: {
          ParseNode* partition_node = node.children_[0];
          bool enable_split_partition = false;
          if (OB_ISNULL(partition_node) || 2 != partition_node->num_child_ || OB_ISNULL(partition_node->children_[0]) ||
              OB_ISNULL(partition_node->children_[1])) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "invalid partition node", K(ret), K(partition_node));
          } else if (OB_FAIL(get_enable_split_partition(
                         session_info_->get_effective_tenant_id(), enable_split_partition))) {
            LOG_WARN("failed to get enable split partition config",
                K(ret),
                "tenant_id",
                session_info_->get_effective_tenant_id());
          } else if (!enable_split_partition) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("reorganize partition not allow", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "reorganize partition");
          } else if (OB_FAIL(resolve_reorganize_partition(*partition_node))) {
            LOG_WARN("failed to reorganize partition", K(ret));
          } else {
            arg.alter_option_bitset_.add_member(obrpc::ObAlterTablegroupArg::REORGANIZE_PARTITION);
          }
          break;
        }
        case T_ALTER_PARTITION_SPLIT: {
          ParseNode* partition_node = node.children_[0];
          bool enable_split_partition = false;
          if (OB_ISNULL(partition_node) || 2 != partition_node->num_child_ || OB_ISNULL(partition_node->children_[0]) ||
              OB_ISNULL(partition_node->children_[1])) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "invalid partition node", K(ret), K(partition_node));
          } else if (OB_FAIL(get_enable_split_partition(
                         session_info_->get_effective_tenant_id(), enable_split_partition))) {
            LOG_WARN("failed to get enable split partition config",
                K(ret),
                "tenant_id",
                session_info_->get_effective_tenant_id());
          } else if (!enable_split_partition) {
            ret = OB_OP_NOT_ALLOW;
            LOG_WARN("split partition not allow", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "split partition");
          } else if (OB_FAIL(resolve_split_partition(*partition_node))) {
            LOG_WARN("failed to reorganize partition", K(ret));
          } else {
            arg.alter_option_bitset_.add_member(obrpc::ObAlterTablegroupArg::SPLIT_PARTITION);
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          SQL_RESV_LOG(WARN, "Unknown alter partition option %d!", "option type", node.children_[0]->type_, K(ret));
          break;
        }
      }
    }
  }
  return ret;
}

int ObAlterTablegroupResolver::resolve_add_partition(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  int64_t expr_num = OB_INVALID_INDEX;
  ParseNode* range_part_elements_node = NULL;
  PartitionInfo part_info;
  bool in_tablegroup = true;
  ObAlterTablegroupStmt* alter_tablegroup_stmt = get_alter_tablegroup_stmt();
  const ObTablegroupSchema* tablegroup_schema = NULL;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(alter_tablegroup_stmt) || OB_ISNULL(node.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema, stmt or node ",
        K(ret),
        KP(schema_checker_),
        KP(alter_tablegroup_stmt),
        "node",
        node.children_[0]);
  } else if (OB_FAIL(schema_checker_->get_tablegroup_schema(session_info_->get_effective_tenant_id(),
                 alter_tablegroup_stmt->get_tablegroup_name(),
                 tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret));
  } else if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("invalid tablegroup schema", K(ret));
  } else if (OB_ISNULL(range_part_elements_node = node.children_[0]->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (tablegroup_schema->is_range_part()) {
    const ObPartitionFuncType part_type = ObPartitionFuncType::PARTITION_FUNC_TYPE_RANGE_COLUMNS;
    // Here expr_num is parsed by resolver and does not represent the actual expr_num of tablegroup
    if (OB_FAIL(resolve_range_partition_elements(range_part_elements_node,
            false,  // is_subpartition
            part_type,
            part_info.range_value_exprs_,
            part_info.parts_,
            part_info.subparts_,
            expr_num,
            in_tablegroup))) {
      LOG_WARN("resolve range partition elements fail", K(ret));
    } else if (OB_FAIL(alter_tablegroup_stmt->get_part_values_exprs().assign(part_info.range_value_exprs_))) {
      LOG_WARN("assign faield", K(ret));
    }
  } else if (tablegroup_schema->is_list_part()) {
    const ObPartitionFuncType part_type = ObPartitionFuncType::PARTITION_FUNC_TYPE_LIST_COLUMNS;
    if (OB_FAIL(resolve_list_partition_elements(range_part_elements_node,
            false,
            part_type,
            expr_num,
            part_info.list_value_exprs_,
            part_info.parts_,
            part_info.subparts_,
            in_tablegroup))) {
      LOG_WARN("resolve list partition elememts fail", K(ret));
    } else if (OB_FAIL(alter_tablegroup_stmt->get_part_values_exprs().assign(part_info.list_value_exprs_))) {
      LOG_WARN("assign list values failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {

  } else {
    share::schema::ObPartition* part = NULL;
    ObTablegroupSchema& alter_tablegroup_schema =
        get_alter_tablegroup_stmt()->get_alter_tablegroup_arg().alter_tablegroup_schema_;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_info.parts_.count(); ++i) {
      part = &(part_info.parts_.at(i));
      if (!part->get_part_name().empty()) {
        if (OB_FAIL(alter_tablegroup_schema.check_part_name(*part))) {
          LOG_WARN("check part name failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(alter_tablegroup_schema.add_partition(*part))) {
        LOG_WARN("add partition failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      alter_tablegroup_stmt->set_part_func_expr_num(expr_num);
      alter_tablegroup_schema.get_part_option().set_part_func_type(
          tablegroup_schema->get_part_option().get_part_func_type());
      alter_tablegroup_schema.get_part_option().set_part_num(alter_tablegroup_schema.get_partition_num());
    }
  }
  return ret;
}

int ObAlterTablegroupResolver::resolve_drop_partition(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node.children_) || (T_ALTER_PARTITION_DROP != node.type_ && T_ALTER_PARTITION_TRUNCATE != node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret), "type", node.type_);
  } else {
    const ParseNode* name_list = node.children_[0];
    if (OB_ISNULL(name_list)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
    } else {
      ObAlterTablegroupStmt* alter_tablegroup_stmt = get_alter_tablegroup_stmt();
      if (OB_ISNULL(alter_tablegroup_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "alter tablegroup stmt should not be null", K(ret));
      }
      ObTablegroupSchema& alter_tablegroup_schema =
          alter_tablegroup_stmt->get_alter_tablegroup_arg().alter_tablegroup_schema_;
      for (int64_t i = 0; OB_SUCC(ret) && i < name_list->num_child_; ++i) {
        ObPartition part;
        ObString partition_name(
            static_cast<int32_t>(name_list->children_[i]->str_len_), name_list->children_[i]->str_value_);
        if (OB_FAIL(part.set_part_name(partition_name))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_RESV_LOG(ERROR, "set partition name failed", K(partition_name), K(ret));
        } else if (OB_FAIL(alter_tablegroup_schema.check_part_name(part))) {
          SQL_RESV_LOG(WARN, "check part name failed!", K(part), K(ret));
        } else if (OB_FAIL(alter_tablegroup_schema.add_partition(part))) {
          SQL_RESV_LOG(WARN, "add partition failed!", K(part), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterTablegroupResolver::resolve_reorganize_partition(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  ObAlterTablegroupStmt* alter_tablegroup_stmt = get_alter_tablegroup_stmt();
  if (T_ALTER_PARTITION_REORGANIZE != node.type_ || 2 != node.num_child_ || OB_ISNULL(node.children_[0]) ||
      OB_ISNULL(node.children_[1]) || OB_ISNULL(alter_tablegroup_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement",
        K(ret),
        "children num",
        node.num_child_,
        "children[0]",
        node.children_[0],
        "children[1]",
        node.children_[1]);
  } else {
    ObTablegroupSchema& alter_tablegroup_schema =
        alter_tablegroup_stmt->get_alter_tablegroup_arg().alter_tablegroup_schema_;
    ParseNode* name_list = node.children_[1];
    if (OB_ISNULL(name_list)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(name_list));
    } else if (1 != name_list->num_child_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("alter tablegroup reorganize multi partition not supported now", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter tablegroup reorganize multiple partitions");
    } else if (OB_NOT_NULL(name_list->children_[0])) {
      ObPartition part;
      ObString partition_name(
          static_cast<int32_t>(name_list->children_[0]->str_len_), name_list->children_[0]->str_value_);
      if (OB_FAIL(alter_tablegroup_schema.set_split_partition(partition_name))) {
        LOG_WARN("failed to set split partition name", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("name list can not be null", K(ret), K(name_list));
    }
    if (OB_FAIL(ret)) {
      // nothing
    } else if (OB_FAIL(resolve_add_partition(node))) {
      LOG_WARN("failed resolve add partition", K(ret));
    } else if (1 == alter_tablegroup_schema.get_partition_num()) {
      ret = OB_ERR_SPLIT_INTO_ONE_PARTITION;
      LOG_USER_ERROR(OB_ERR_SPLIT_INTO_ONE_PARTITION);
      LOG_WARN("can not split partition into one partition", K(ret), K(alter_tablegroup_schema));
    }
  }
  return ret;
}

int ObAlterTablegroupResolver::resolve_split_partition(const ParseNode& node)
{
  int ret = OB_SUCCESS;
  ObAlterTablegroupStmt* alter_tablegroup_stmt = get_alter_tablegroup_stmt();
  const ObTablegroupSchema* tablegroup_schema = NULL;
  if (OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema check", K(ret));
  } else if (OB_FAIL(schema_checker_->get_tablegroup_schema(session_info_->get_effective_tenant_id(),
                 alter_tablegroup_stmt->get_tablegroup_name(),
                 tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret));
  } else if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("invalid tablegroup schema", K(ret));
  } else if (T_ALTER_PARTITION_SPLIT != node.type_ || 2 != node.num_child_ || OB_ISNULL(node.children_[0]) ||
             OB_ISNULL(node.children_[1]) || T_SPLIT_ACTION != node.children_[1]->type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "node_type", node.type_, "node_num", node.num_child_);
  } else if (OB_ISNULL(alter_tablegroup_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alter table stmt should not be null", K(ret));
  } else {
    ParseNode* name_list = node.children_[0];
    ObTablegroupSchema& alter_tablegroup_schema =
        alter_tablegroup_stmt->get_alter_tablegroup_arg().alter_tablegroup_schema_;
    if (OB_ISNULL(name_list)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      ObString partition_name(static_cast<int32_t>(name_list->str_len_), name_list->str_value_);
      if (OB_FAIL(alter_tablegroup_schema.set_split_partition(partition_name))) {
        LOG_WARN("failed to set split partition name", K(ret));
      }
    }

    int64_t partition_count = OB_INVALID_COUNT;
    int64_t expr_value_num = OB_INVALID_COUNT;
    const ParseNode* split_node = node.children_[1];
    PartitionInfo part_info;

    if (OB_FAIL(ret)) {
      // nothing
    } else if (OB_ISNULL(split_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("split node is null", K(ret), K(split_node));
    } else if (OB_FAIL(check_split_type_valid(split_node, tablegroup_schema->get_part_option().get_part_func_type()))) {
      LOG_WARN("failed to check split type valid", K(ret), K(tablegroup_schema));
    } else if (OB_NOT_NULL(split_node->children_[AT_VALUES_NODE])) {
      // split at [into ()]
      partition_count = 1;
      if (OB_FAIL(resolve_split_at_partition(alter_tablegroup_stmt,
              split_node,
              tablegroup_schema->get_part_option().get_part_func_type(),
              part_info.part_func_exprs_,
              alter_tablegroup_schema,
              expr_value_num,
              true))) {
        LOG_WARN("failed to resolve split at partition", K(ret));
      }
    } else if (OB_NOT_NULL(split_node->children_[PARTITION_DEFINE_NODE]) &&
               OB_NOT_NULL(split_node->children_[PARTITION_DEFINE_NODE]->children_[0])) {
      // split into ()
      ParseNode* range_element_node = split_node->children_[PARTITION_DEFINE_NODE]->children_[0];
      if (OB_FAIL(resolve_split_into_partition(alter_tablegroup_stmt,
              range_element_node,
              tablegroup_schema->get_part_option().get_part_func_type(),
              part_info.part_func_exprs_,
              partition_count,
              expr_value_num,
              alter_tablegroup_schema,
              true))) {
        LOG_WARN("failed to resolve split at partition", K(ret));
      }
    } else {
      ret = OB_ERR_MISS_AT_VALUES;
      LOG_WARN("miss at and less than values", K(ret));
      LOG_USER_ERROR(OB_ERR_MISS_AT_VALUES);
    }

    if (OB_FAIL(ret)) {
    } else {
      alter_tablegroup_stmt->set_part_func_expr_num(expr_value_num);
      alter_tablegroup_schema.get_part_option().set_part_func_type(
          tablegroup_schema->get_part_option().get_part_func_type());
      alter_tablegroup_schema.get_part_option().set_part_num(
          partition_count);  // last part may have no max value. rs will deal with it
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
