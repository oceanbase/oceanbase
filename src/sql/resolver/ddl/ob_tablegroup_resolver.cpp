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
#include "ob_tablegroup_resolver.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ddl/ob_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/parser/parse_node.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
int ObTableGroupResolver::resolve_partition_table_option(ObTablegroupStmt *stmt,
                                                         ParseNode *node,
                                                         ObTablegroupSchema &tablegroup_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(stmt), K(node));
  } else {
    if (T_HASH_PARTITION == node->type_ || T_KEY_PARTITION == node->type_) {
      ret = resolve_partition_hash_or_key(stmt, node, false, tablegroup_schema);
    } else if (T_RANGE_PARTITION == node->type_ || T_RANGE_COLUMNS_PARTITION == node->type_) {
      ret = resolve_partition_range(stmt, node, false, tablegroup_schema);
    } else if (T_LIST_PARTITION == node->type_ || T_LIST_COLUMNS_PARTITION == node->type_) {
      ret = resolve_partition_list(stmt, node, false, tablegroup_schema);
    } else {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "node type is invalid.", K(ret), K(node->type_));
    }
  }
  return ret;
}

//FIXME:支持非模版化二级分区
int ObTableGroupResolver::resolve_partition_hash_or_key(ObTablegroupStmt *stmt,
                                                        ParseNode *node,
                                                        const bool is_subpartition,
                                                        ObTablegroupSchema &tablegroup_schema)
{
  int ret = OB_SUCCESS;
  ObString partition_expr;
  ObPartitionFuncType partition_func_type = PARTITION_FUNC_TYPE_HASH;
  int64_t partition_num = 0;
  ObPartitionOption *partition_option = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(stmt), K(node));
  } else {
    if (is_subpartition) {
      partition_option = &(tablegroup_schema.get_sub_part_option());
      tablegroup_schema.set_part_level(PARTITION_LEVEL_TWO);
    } else {
      partition_option = &(tablegroup_schema.get_part_option());
      tablegroup_schema.set_part_level(PARTITION_LEVEL_ONE);
    }

    if (T_HASH_PARTITION == node->type_ || T_KEY_PARTITION == node->type_) {
      if (OB_UNLIKELY(NULL == node->children_
                      || (3 != node->num_child_ && 2 != node->num_child_)
                      || NULL == node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->num_child_), K(node->children_));
      } else {
        partition_func_type = T_HASH_PARTITION == node->type_
            ? PARTITION_FUNC_TYPE_HASH
            : PARTITION_FUNC_TYPE_KEY;
        //if not specify partitions, default partition num is 1
        if (NULL != node->children_[1]) {
          partition_num = node->children_[1]->value_;
        } else {
          partition_num = 1;
        }
      }
      if (OB_SUCC(ret)) {
        //key分区必须设置column_list_num, hash分区expr_num设置为1
        int64_t expr_num = -1;
        expr_num = PARTITION_FUNC_TYPE_KEY == partition_func_type ? node->children_[0]->children_[1]->value_ : 1;
        if (expr_num <= 0 || INT64_MAX == expr_num) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid expr num", K(ret), K(expr_num));
        } else if (!is_subpartition) {
          stmt->set_part_func_expr_num(expr_num);
        } else {
          stmt->set_sub_part_func_expr_num(expr_num);
        }
      }
      if (OB_SUCC(ret)) {
        if (partition_num <= 0) {
          ret = OB_NO_PARTS_ERROR;
          LOG_USER_ERROR(OB_NO_PARTS_ERROR);
        } else if (OB_FAIL(partition_option->set_part_expr(partition_expr))) {//deep copy
          LOG_WARN("set partition express string failed", K(ret));
        } else {
          partition_option->set_part_func_type(partition_func_type);
          partition_option->set_part_num(partition_num);
        }
      }
      if (OB_SUCC(ret)) {
        if (is_subpartition && NULL != node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpartition cannot has another subpartition");
        }
      }
      if (OB_SUCC(ret)) {
        const bool is_hash_or_key_part = T_HASH_PARTITION == node->type_ || T_KEY_PARTITION == node->type_;
        //如果为二级分区这里需要做防御
        if (is_hash_or_key_part && NULL != node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]) {
          if (T_RANGE_PARTITION == node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]->type_ ||
              T_RANGE_COLUMNS_PARTITION == node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]->type_) {
            if (OB_FAIL(resolve_partition_range(stmt, node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE], true, tablegroup_schema))) {
              LOG_WARN("resolve partition range or list fail", K(ret));
            }
          } else if (T_LIST_PARTITION == node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]->type_ ||
                     T_LIST_COLUMNS_PARTITION == node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]->type_) {
            if (OB_FAIL(resolve_partition_list(stmt, node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE], true, tablegroup_schema))) {
              LOG_WARN("resolve partition range or list fail", K(ret));
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("node type is invalid.", K(ret), K(node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]->type_));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "composite partition type which is hash/key - hash/key");
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (tablegroup_schema.get_all_part_num() > (lib::is_oracle_mode()
             ? OB_MAX_PARTITION_NUM_ORACLE : OB_MAX_PARTITION_NUM_MYSQL)) {
          ret = OB_TOO_MANY_PARTITIONS_ERROR;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("resolve partition hash node success", K(ret), K(*stmt), K(tablegroup_schema));
  }
  return ret;
}

//FIXME:支持非模版化二级分区
int ObTableGroupResolver::resolve_partition_range(ObTablegroupStmt *tablegroup_stmt,
                                                  ParseNode *node,
                                                  const bool is_subpartition,
                                                  ObTablegroupSchema &tablegroup_schema)
{
  int ret = OB_SUCCESS;
  PartitionInfo part_info;
  if (OB_ISNULL(tablegroup_stmt) || OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablegroup_stmt), K(node));
  } else if (OB_FAIL(resolve_partition_range(tablegroup_stmt, node, is_subpartition, tablegroup_schema, part_info))) {
    LOG_WARN("resolve partition range failed", K(ret));
  } else {
    ObIArray<ObRawExpr *> &range_value_exprs = is_subpartition ? 
      tablegroup_stmt->get_template_subpart_values_exprs() : tablegroup_stmt->get_part_values_exprs();
    tablegroup_schema.set_part_level(part_info.part_level_);
    if (OB_FAIL(range_value_exprs.assign(part_info.range_value_exprs_))) {
      LOG_WARN("assgin failed", K(ret));
    } else {
      share::schema::ObPartition *part = NULL;
      if (is_subpartition) {
        tablegroup_schema.get_sub_part_option() = part_info.subpart_option_;
        for (int64_t i = 0; i < part_info.subparts_.count(); ++i) {
          if (OB_FAIL(tablegroup_schema.add_def_subpartition(part_info.subparts_.at(i)))) {
            LOG_WARN("add subpartition failed", K(ret));
          }
        }
      } else {
        tablegroup_schema.get_part_option() = part_info.part_option_;
        for (int64_t i = 0; OB_SUCC(ret) && i < part_info.parts_.count(); ++i) {
          part = &(part_info.parts_.at(i));
          if (!part->get_part_name().empty()) {
            if (OB_FAIL(tablegroup_schema.check_part_name(*part))) {
              LOG_WARN("check part name failed", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(tablegroup_schema.add_partition(*part))) {
            LOG_WARN("add partition failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
        if (tablegroup_schema.get_all_part_num() > (lib::is_oracle_mode()
             ? OB_MAX_PARTITION_NUM_ORACLE : OB_MAX_PARTITION_NUM_MYSQL)) {
        ret = OB_TOO_MANY_PARTITIONS_ERROR;
      }
    }
  }
  return ret;
}

int ObTableGroupResolver::resolve_partition_range(ObTablegroupStmt *tablegroup_stmt,
                                                  ParseNode *node,
                                                  const bool is_subpartition,
                                                  ObTablegroupSchema &tablegroup_schema,
                                                  PartitionInfo &part_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tablegroup_stmt) || OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablegroup_stmt), K(node));
  } else if (OB_ISNULL(node->children_)
      || OB_ISNULL(node->children_[ObTableGroupResolver::RANGE_ELEMENTS_NODE])
      || (T_RANGE_PARTITION != node->type_ && T_RANGE_COLUMNS_PARTITION != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node or node type is error", K(ret), K(node));
  } else {
    int64_t partition_num = 0;
    ObPartitionFuncType part_func_type = PARTITION_FUNC_TYPE_RANGE;
    if (T_RANGE_COLUMNS_PARTITION == node->type_) {
      part_func_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
    }
    if (!OB_ISNULL(node->children_[ObTableGroupResolver::RANGE_PARTITION_NUM_NODE])) {
      partition_num = node->children_[ObTableGroupResolver::RANGE_PARTITION_NUM_NODE]->value_;
      if (partition_num != node->children_[ObTableGroupResolver::RANGE_ELEMENTS_NODE]->num_child_) {
        ret = OB_ERR_PARSE_PARTITION_RANGE;
      }
    } else {
      partition_num = node->children_[ObTableGroupResolver::RANGE_ELEMENTS_NODE]->num_child_;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDDLResolver::check_partition_name_duplicate(node->children_[ObTableGroupResolver::RANGE_ELEMENTS_NODE],
                                                                lib::is_oracle_mode()))) {
        LOG_WARN("duplicate partition name", K(ret));
      }
    }
    //resolve partition expr num
    int64_t expr_num = OB_INVALID_INDEX;
    if (OB_SUCC(ret)) {
      ParseNode *partition_func_node = node->children_[ObTableGroupResolver::RANGE_FUN_EXPR_NODE];
      if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type
          && OB_ISNULL(partition_func_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part func node should not be null", K(ret));
      } else {
        //range column分区必须设置column_list_num, range分区expr_num设置为1
        expr_num = PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type ? partition_func_node->value_: 1;
        if (expr_num <= 0 || INT64_MAX == expr_num) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid expr num", K(ret), K(expr_num));
        } else if (!is_subpartition) {
          tablegroup_stmt->set_part_func_expr_num(expr_num);
        } else {
          tablegroup_stmt->set_sub_part_func_expr_num(expr_num);
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObPartitionOption *partition_option = NULL;
      if (is_subpartition) {
        part_info.part_level_ = PARTITION_LEVEL_TWO;
        partition_option = &part_info.subpart_option_;
      } else {
        part_info.part_level_ = PARTITION_LEVEL_ONE;
        partition_option = &part_info.part_option_;
      }
      partition_option->set_part_func_type(part_func_type);
      partition_option->set_part_num(partition_num);
    }
    if (OB_SUCC(ret)) {
      bool in_tablegroup = true;
      if (OB_FAIL(ObDDLResolver::resolve_range_partition_elements(node->children_[ObTableGroupResolver::RANGE_ELEMENTS_NODE],
                                                                  is_subpartition,
                                                                  part_func_type,
                                                                  expr_num,
                                                                  part_info.range_value_exprs_,
                                                                  part_info.parts_,
                                                                  part_info.subparts_,
                                                                  in_tablegroup))) {
        LOG_WARN("resolve reange partition elements fail", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !is_subpartition) {
    ParseNode *subpart_node = node->children_[ObTableGroupResolver::RANGE_SUBPARTITIOPPN_NODE];
    if (NULL != subpart_node) {
      part_info.part_level_ = PARTITION_LEVEL_TWO;
      //only support hash partition now
      if (T_HASH_PARTITION == subpart_node->type_ || T_KEY_PARTITION == subpart_node->type_) {
        if (OB_FAIL(resolve_partition_hash_or_key(tablegroup_stmt, subpart_node, true, tablegroup_schema))) {
          LOG_WARN("resolve partition hash or key fail", K(ret));
        }
      } else if (T_LIST_PARTITION == node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]->type_ ||
        T_LIST_COLUMNS_PARTITION == node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]->type_) {
        if (OB_FAIL(resolve_partition_list(tablegroup_stmt, node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE], true, tablegroup_schema))) {
          LOG_WARN("resolve partition range or list fail", K(ret));
        }
      } else {
        ret = OB_NOT_IMPLEMENT;
      }
    }
  }
  return ret;
}

//FIXME:支持非模版化二级分区
int ObTableGroupResolver::resolve_partition_list(ObTablegroupStmt *stmt,
                                                 ParseNode *node,
                                                 const bool is_subpartition,
                                                 ObTablegroupSchema &tablegroup_schema)
{
  int ret = OB_SUCCESS;
  PartitionInfo part_info;
  if (OB_ISNULL(stmt) || OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(stmt), K(node));
  } else if (OB_FAIL(resolve_partition_list(stmt, node, is_subpartition, tablegroup_schema, part_info))) {
    LOG_WARN("resolve partition list failed", K(ret));
  } else {
    ObDDLStmt::array_t &list_value_exprs = is_subpartition ?
      stmt->get_template_subpart_values_exprs() : stmt->get_part_values_exprs();
    tablegroup_schema.set_part_level(part_info.part_level_);
    if (OB_FAIL(list_value_exprs.assign(part_info.list_value_exprs_))) {
      LOG_WARN("fail to push to array", K(ret));
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else {
      share::schema::ObPartition *part = NULL;
      if (is_subpartition) {
        tablegroup_schema.get_sub_part_option() = part_info.subpart_option_;
        for (int64_t i = 0; i < part_info.subparts_.count(); ++i) {
          if (OB_FAIL(tablegroup_schema.add_def_subpartition(part_info.subparts_.at(i)))) {
            LOG_WARN("add subpartition failed", K(ret));
          }
        }
      } else {
        tablegroup_schema.get_part_option() = part_info.part_option_;
        for (int64_t i = 0; OB_SUCC(ret) && i < part_info.parts_.count(); ++i) {
          part = &(part_info.parts_.at(i));
          if (!part->get_part_name().empty()) {
            if (OB_FAIL(tablegroup_schema.check_part_name(*part))) {
              LOG_WARN("check part name failed", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(tablegroup_schema.add_partition(*part))) {
            LOG_WARN("add partition failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
        if (tablegroup_schema.get_all_part_num() > (lib::is_oracle_mode()
             ? OB_MAX_PARTITION_NUM_ORACLE : OB_MAX_PARTITION_NUM_MYSQL)) {
        ret = OB_TOO_MANY_PARTITIONS_ERROR;
      }
    }
  }
  return ret;
}

int ObTableGroupResolver::resolve_partition_list(ObTablegroupStmt *tablegroup_stmt,
                                                 ParseNode *node,
                                                 const bool is_subpartition,
                                                 ObTablegroupSchema &tablegroup_schema,
                                                 PartitionInfo &part_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tablegroup_stmt) || OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablegroup_stmt), K(node));
  } else if (OB_ISNULL(node) || OB_ISNULL(node->children_)
      || OB_ISNULL(node->children_[ObTableGroupResolver::LIST_ELEMENTS_NODE])
      || (T_LIST_PARTITION != node->type_ && T_LIST_COLUMNS_PARTITION != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node or node type is error", K(ret), K(node));
  } else {
    int64_t partition_num = 0;
    ObPartitionFuncType part_func_type = PARTITION_FUNC_TYPE_LIST;
    if (T_LIST_COLUMNS_PARTITION == node->type_) {
      part_func_type = PARTITION_FUNC_TYPE_LIST_COLUMNS;
    }
    if (!OB_ISNULL(node->children_[ObTableGroupResolver::LIST_PARTITION_NUM_NODE])) {
      partition_num = node->children_[ObTableGroupResolver::LIST_PARTITION_NUM_NODE]->value_;
      if (partition_num != node->children_[ObTableGroupResolver::LIST_ELEMENTS_NODE]->num_child_) {
        ret = OB_ERR_PARSE_PARTITION_LIST;
      }
    } else {
      partition_num = node->children_[ObTableGroupResolver::LIST_ELEMENTS_NODE]->num_child_;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDDLResolver::check_partition_name_duplicate(node->children_[ObTableGroupResolver::LIST_ELEMENTS_NODE],
                                                                lib::is_oracle_mode()))) {
        LOG_WARN("duplicate partition name", K(ret));
      }
    }

    //resolve partition expr num
    int64_t expr_num = OB_INVALID_COUNT;
    if (OB_SUCC(ret)) {
      ObPartitionOption *partition_option = NULL;
      if (is_subpartition) {
        part_info.part_level_ = PARTITION_LEVEL_TWO;
        partition_option = &part_info.subpart_option_;
      } else {
        part_info.part_level_ = PARTITION_LEVEL_ONE;
        partition_option = &part_info.part_option_;
      }
      partition_option->set_part_func_type(part_func_type);
      partition_option->set_part_num(partition_num);
    }
    if (OB_SUCC(ret)) { //这里应该能用原来range的代码
      bool in_tablegroup = true;
      if (OB_FAIL(ObDDLResolver::resolve_list_partition_elements(node->children_[ObTableGroupResolver::LIST_ELEMENTS_NODE],
                                                                 is_subpartition,
                                                                 part_func_type,
                                                                 expr_num,
                                                                 part_info.list_value_exprs_,
                                                                 part_info.parts_,
                                                                 part_info.subparts_,
                                                                 in_tablegroup))) {
        LOG_WARN("resolve reange partition elements fail", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ParseNode *partition_func_node = node->children_[ObTableGroupResolver::RANGE_FUN_EXPR_NODE];
      if (PARTITION_FUNC_TYPE_LIST_COLUMNS == part_func_type
          && OB_ISNULL(partition_func_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part func node should not be null", K(ret));
      } else {
        //list column分区必须设置column_list_num, list分区expr_num设置为1
        int64_t real_expr_num = PARTITION_FUNC_TYPE_LIST_COLUMNS == part_func_type ? partition_func_node->value_ : 1;
        if (real_expr_num <= 0 || INT64_MAX == real_expr_num) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid expr num", K(ret), K(real_expr_num));
        } else if (OB_INVALID_COUNT != expr_num
                   && real_expr_num != expr_num) {
          //当只有一个default分区不进行比较
          ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
          LOG_WARN("expr num is not equal", K(ret), K(expr_num), K(real_expr_num));
        } else if (!is_subpartition) {
          tablegroup_stmt->set_part_func_expr_num(real_expr_num);
        } else {
          tablegroup_stmt->set_sub_part_func_expr_num(real_expr_num);
        }
      }
    }
  }

  if (OB_SUCC(ret) && !is_subpartition) {
    ParseNode *subpart_node = node->children_[ObTableGroupResolver::LIST_SUBPARTITIOPPN_NODE];
    if (NULL != subpart_node) {
      part_info.part_level_ = PARTITION_LEVEL_TWO;
      //only support hash partition now
      if (T_HASH_PARTITION == subpart_node->type_ || T_KEY_PARTITION == subpart_node->type_) {
        ret = resolve_partition_hash_or_key(tablegroup_stmt, subpart_node, true, tablegroup_schema);
      } else if (T_RANGE_PARTITION == node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]->type_ ||
        T_RANGE_COLUMNS_PARTITION == node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE]->type_) {
        if (OB_FAIL(resolve_partition_range(tablegroup_stmt, node->children_[ObTableGroupResolver::HASH_SUBPARTITIOPPN_NODE], true, tablegroup_schema))) {
          LOG_WARN("resolve partition range or list fail", K(ret));
        }
      } else {
        ret = OB_NOT_IMPLEMENT;
      }
    }
  }
  return ret;
}
}
}
