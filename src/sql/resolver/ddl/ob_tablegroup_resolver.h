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

#ifndef OCEANBASE_SQL_RESOLVE_OB_TABLEGROUP_RESOLVER_H
#define OCEANBASE_SQL_RESOLVE_OB_TABLEGROUP_RESOLVER_H
#include "lib/ob_define.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_stmt.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace sql
{
class ObTablegroupStmt;
class ObTableGroupResolver : public ObDDLResolver
{
  enum RangeNode {
    RANGE_FUN_EXPR_NODE = 0,
    RANGE_ELEMENTS_NODE = 1,
    RANGE_SUBPARTITIOPPN_NODE = 2,
    RANGE_PARTITION_NUM_NODE = 3
  };
  enum ListNode {
    LIST_FUN_EXPR_NODE = 0,
    LIST_ELEMENTS_NODE = 1,
    LIST_SUBPARTITIOPPN_NODE = 2,
    LIST_PARTITION_NUM_NODE = 3
  };
  enum RangeElementsNode {
    PARTITION_NAME_NODE = 0,
    PARTITION_ELEMENT_NODE = 1,
    PART_ID_NODE = 2
  };
  enum HashOrKeyNode {
    HASH_FUN_EXPR_NODE = 0,
    HASH_PARTITION_NUM_NODE = 1,
    HASH_SUBPARTITIOPPN_NODE = 2
  };
public:
  ObTableGroupResolver(ObResolverParams &params) : ObDDLResolver(params), alter_option_bitset_() {}
  ~ObTableGroupResolver() {}
  template<class T>
      int resolve_tablegroup_option(T *stmt, ParseNode *node);
  int resolve_partition_table_option(ObTablegroupStmt *stmt, ParseNode *node,
                                     share::schema::ObTablegroupSchema &tablegroup_schema);
  const common::ObBitSet<> &get_alter_option_bitset() const { return alter_option_bitset_; }
private:
  int resolve_partition_hash_or_key(ObTablegroupStmt *stmt,
                                    ParseNode *node,
                                    const bool is_subpartition,
                                    share::schema::ObTablegroupSchema &tablegroup_schema);
  int resolve_partition_range(ObTablegroupStmt *tablegroup_stmt,
                              ParseNode *node,
                              const bool is_subpartition,
                              share::schema::ObTablegroupSchema &tablegroup_schema);
  int resolve_partition_range(ObTablegroupStmt *tablegroup_stmt,
                              ParseNode *node,
                              const bool is_subpartition,
                              share::schema::ObTablegroupSchema &tablegroup_schema,
                              PartitionInfo &part_info);
  int resolve_partition_list(ObTablegroupStmt *stmt,
                             ParseNode *node,
                             const bool is_subpartition,
                             share::schema::ObTablegroupSchema &tablegroup_schema);
  int resolve_partition_list(ObTablegroupStmt *tablegroup_stmt,
                             ParseNode *node,
                             const bool is_subpartition,
                             share::schema::ObTablegroupSchema &tablegroup_schema,
                             PartitionInfo &part_info);


private:
  common::ObBitSet<> alter_option_bitset_;
};

template<class T>
int ObTableGroupResolver::resolve_tablegroup_option(T *stmt, ParseNode *node)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(node)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(stmt), K(node));
  } else {
    ParseNode *option_node = NULL;
    int32_t num = node->num_child_;
    bool has_tablegroup_sharding_option = false;
    for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
      option_node = node->children_[i];
      switch (option_node->type_) {
        case T_LOCALITY: {
          if (NULL == option_node->children_ || option_node->num_child_ != 2) {
            ret = common::OB_INVALID_ARGUMENT;
            SQL_LOG(WARN, "invalid locality argument", K(ret), "num_child", option_node->num_child_);
          } else if (T_DEFAULT == option_node->children_[0]->type_) {
            // do nothing
          } else {
            int64_t locality_length = option_node->children_[0]->str_len_;
            const char *locality_str = option_node->children_[0]->str_value_;
            common::ObString locality;
            locality.assign_ptr(locality_str, static_cast<int32_t>(locality_length));
            if (OB_UNLIKELY(locality_length > common::MAX_LOCALITY_LENGTH)) {
              ret = common::OB_ERR_TOO_LONG_IDENT;
              LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, locality.length(), locality.ptr());
            } else if (0 == locality_length) {
              ret = OB_OP_NOT_ALLOW;
              SQL_LOG(WARN, "set locality empty is not allowed now", K(ret));
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set locality empty");
            } else if (OB_FAIL(stmt->set_locality(locality))) {
              SQL_LOG(WARN, "fail to set locality", K(ret), K(locality));
            }
          }
          if (OB_SUCC(ret) && stmt->get_stmt_type() == stmt::T_ALTER_TABLEGROUP) {
            if (OB_FAIL(alter_option_bitset_.add_member(obrpc::ObAlterTablegroupArg::LOCALITY))) {
              SQL_LOG(WARN, "fail to add member", K(ret));
            } else if (nullptr == option_node->children_[1]) {
              // not force alter locality
            } else if (option_node->children_[1]->type_ != T_FORCE) {
              ret = common::OB_ERR_UNEXPECTED;
              SQL_LOG(ERROR, "invalid node", K(ret));
            } else if (OB_FAIL(alter_option_bitset_.add_member(
                    obrpc::ObAlterTablegroupArg::FORCE_LOCALITY))) {
              SQL_LOG(WARN, "fail to add member", K(ret));
            }
          }
          break;
        }
        case T_PRIMARY_ZONE: {
          if (NULL == option_node->children_ || option_node->num_child_ != 1) {
            ret = common::OB_INVALID_ARGUMENT;
            SQL_LOG(WARN, "invalid primary_zone argument", K(ret), "num_child", option_node->num_child_);
          } else if (option_node->children_[0]->type_ == T_DEFAULT) {
            // do nothing
          } else if (T_RANDOM == option_node->children_[0]->type_) {
            if (OB_FAIL(stmt->set_primary_zone(common::OB_RANDOM_PRIMARY_ZONE))) {
              SQL_LOG(WARN, "fail to set primary_zone", K(ret));
            }
          } else {
            common::ObString primary_zone;
            primary_zone.assign_ptr(const_cast<char *>(option_node->children_[0]->str_value_),
                                    static_cast<int32_t>(option_node->children_[0]->str_len_));
            if (primary_zone.empty()) {
              ret = OB_OP_NOT_ALLOW;
              SQL_RESV_LOG(WARN, "set primary_zone empty is not allowed now", K(ret));
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set primary_zone empty");
            } else if (OB_FAIL(stmt->set_primary_zone(primary_zone))) {
              SQL_LOG(WARN, "fail to set primary_zone", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(alter_option_bitset_.add_member(obrpc::ObAlterTablegroupArg::PRIMARY_ZONE))) {
            SQL_LOG(WARN, "fail to add member", K(ret));
          }
          break;
        }
        case T_TABLEGROUP_ID: {
          bool is_sync_ddl_user = false;
          if (NULL == option_node->children_ || option_node->num_child_ != 1) {
            ret = common::OB_INVALID_ARGUMENT;
            SQL_LOG(WARN, "invalid tablegroup_id argument", K(ret), "num_child", option_node->num_child_);
          } else if (stmt::T_ALTER_TABLEGROUP == stmt_->get_stmt_type()) {
            ret = OB_ERR_PARSE_SQL;
            SQL_RESV_LOG(WARN, "Not support to alter table id", K(ret));
          } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
            SQL_RESV_LOG(WARN, "Failed to check sync_dll_user", K(ret));
          } else if (!is_sync_ddl_user) {
            ret = OB_ERR_PARSE_SQL;
            SQL_RESV_LOG(WARN, "Only support for sync ddl user to specify part id",
                         K(ret), K(session_info_->get_user_name()));
          } else {
            uint64_t tablegroup_id = static_cast<uint64_t>(option_node->children_[0]->value_);
            if (tablegroup_id <= 0) {
              ret = OB_INVALID_ARGUMENT;
              SQL_LOG(WARN, "tablegroup_id is invalid", K(ret), K(tablegroup_id));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tablegroup_id");
            } else if (OB_FAIL(stmt->set_tablegroup_id(tablegroup_id))) {
              SQL_LOG(WARN, "fail to set tablegroup_id", K(ret));
            }
          }
          break;
        }
        case T_TABLEGROUP_BINDING: {
          if (nullptr == option_node->children_ || option_node->num_child_ != 1) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "invalid tablegroup binding attribute", K(ret),
                         "num_child", option_node->num_child_);
          } else if (stmt::T_ALTER_TABLEGROUP == stmt_->get_stmt_type()) {
            ret = OB_ERR_PARSE_SQL;
            SQL_RESV_LOG(WARN, "Not support to alter tablegroup binding attribute", K(ret));
          } else if (OB_UNLIKELY(nullptr == option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(ret));
          } else {
            // ignore, just for compatibility
          }
          break;
        }
        case T_TABLEGROUP_SHARDING: {
          if (nullptr == option_node->children_ || option_node->num_child_ != 1) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "invalid tablegroup sharding attribute", K(ret),
                         "num_child", option_node->num_child_);
          } else if (OB_UNLIKELY(nullptr == option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(ret));
          } else {
            has_tablegroup_sharding_option = true;
            int64_t sharding_length = option_node->children_[0]->str_len_;
            const char *sharding_str = option_node->children_[0]->str_value_;
            common::ObString tablegroup_sharding;
            tablegroup_sharding.assign_ptr(sharding_str, static_cast<int32_t>(sharding_length));
            if (tablegroup_sharding != OB_PARTITION_SHARDING_NONE && tablegroup_sharding != OB_PARTITION_SHARDING_PARTITION
                && tablegroup_sharding != OB_PARTITION_SHARDING_ADAPTIVE) {
              ret = OB_INVALID_ARGUMENT;
              SQL_RESV_LOG(WARN, "invalid tablegroup sharding attribute", K(ret),
                         "sharding", tablegroup_sharding);
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sharding");
            } else if (OB_FAIL(stmt->set_tablegroup_sharding(tablegroup_sharding))) {
              SQL_LOG(WARN, "set_tablegroup_sharding", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(alter_option_bitset_.add_member(obrpc::ObAlterTablegroupArg::SHARDING))) {
            SQL_LOG(WARN, "fail to add member", K(ret));
          }
          break;
        }
        case T_MAX_USED_PART_ID: {
          // max_used_part_id is deprecated in 4.0, we just ignore and show warnings
          LOG_USER_WARN(OB_NOT_SUPPORTED, "max_used_part_id");
          break;
        }
        default: {
          /* won't be here */
          ret = common::OB_ERR_UNEXPECTED;
          SQL_LOG(ERROR, "code should not reach here", K(ret), "type", option_node->type_);
          break;
        }
      }
    } //end for
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
#endif
