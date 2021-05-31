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
#include "sql/resolver/ddl/ob_create_tablegroup_resolver.h"
#include "sql/resolver/ddl/ob_tablegroup_resolver.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

ObCreateTablegroupResolver::ObCreateTablegroupResolver(ObResolverParams& params) : ObTableGroupResolver(params)
{}

ObCreateTablegroupResolver::~ObCreateTablegroupResolver()
{}

int ObCreateTablegroupResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode* node = const_cast<ParseNode*>(&parse_tree);
  ObCreateTablegroupStmt* create_tablegroup_stmt = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(node) || T_CREATE_TABLEGROUP != node->type_ || OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null or parser is error", K(ret));
  }
  ObString tablegroup_name;
  if (OB_SUCC(ret)) {
    if (NULL == (create_tablegroup_stmt = create_stmt<ObCreateTablegroupStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to create create_tablegroup_stmt", K(ret));
    } else {
      stmt_ = create_tablegroup_stmt;
    }

    if (OB_SUCC(ret)) {
      if (NULL != node->children_[IF_NOT_EXIST]) {
        if (T_IF_NOT_EXISTS == node->children_[IF_NOT_EXIST]->type_) {
          create_tablegroup_stmt->set_if_not_exists(true);
        } else {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "node type is not T_IF_NOT_EXISTS", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(node->children_[TG_NAME]) || T_IDENT != node->children_[TG_NAME]->type_) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "node is null or node type is not T_IDENT", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (static_cast<int32_t>(node->children_[TG_NAME]->str_len_) >= OB_MAX_TABLEGROUP_NAME_LENGTH) {
          ret = OB_ERR_TOO_LONG_IDENT;
          LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT,
              static_cast<int32_t>(node->children_[TG_NAME]->str_len_),
              node->children_[TG_NAME]->str_value_);
        } else {
          tablegroup_name.assign_ptr(
              node->children_[TG_NAME]->str_value_, static_cast<int32_t>(node->children_[TG_NAME]->str_len_));
          if (OB_FAIL(create_tablegroup_stmt->set_tablegroup_name(tablegroup_name))) {
            SQL_RESV_LOG(WARN, "set tablegroup name failed", K(ret));
          } else {
            create_tablegroup_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    bool strict_mode = true;
    if (OB_FAIL(session_info_->is_create_table_strict_mode(strict_mode))) {
      SQL_RESV_LOG(WARN, "fail to get variable ob_create_table_strict_mode", K(ret));
    } else {
      const obrpc::ObCreateTableMode create_mode =
          strict_mode ? obrpc::OB_CREATE_TABLE_MODE_STRICT : obrpc::OB_CREATE_TABLE_MODE_LOOSE;
      create_tablegroup_stmt->set_create_mode(create_mode);
    }
  }

  if (OB_SUCC(ret)) {
    if (GCONF.enable_pg) {
      create_tablegroup_stmt->set_binding(true);
    } else {
    }
  }

  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (OB_ISNULL(node->children_[TABLEGROUP_OPTION])) {
    // nothing todo
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000) {
    // In the process of upgrading from 1.x to 2.0, in order to avoid losing
    // the table option when the new server sends a create tablegroup
    // request to the old rs, precautions need to be taken here
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("not support to set tablegroup option while cluster is upgrading", K(ret));
  } else {
    if (OB_FAIL(resolve_tablegroup_option(create_tablegroup_stmt, node->children_[TABLEGROUP_OPTION]))) {
      LOG_WARN("fail to resolve tabelgroup option", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (OB_ISNULL(node->children_[PARTITION_OPTION])) {
    // skip
    // max_used_part_id of the newly created tablegroup should be 0
    if (0 > create_tablegroup_stmt->get_max_used_part_id()) {
      create_tablegroup_stmt->set_max_used_part_id(0);
    }
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000) {
    // In the process of upgrading from 1.x to 2.0, in order to avoid losing
    // the table option when the new server sends a create tablegroup
    // request to the old rs, precautions need to be taken here
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("not support to set partition option while cluster is upgrading", K(ret));
  } else {
    ObTablegroupSchema& tablegroup_schema = create_tablegroup_stmt->get_create_tablegroup_arg().tablegroup_schema_;
    if (0 < create_tablegroup_stmt->get_max_used_part_id() && !tablegroup_schema.get_binding()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not binding tablegroup no need has part id", K(ret), K(tablegroup_schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "not binding tablegroup specify part id");
    } else if (OB_FAIL(resolve_partition_table_option(
                   create_tablegroup_stmt, node->children_[PARTITION_OPTION], tablegroup_schema))) {
      SQL_RESV_LOG(WARN, "resolve partition option failed", K(ret));
    } else {
    }  // do nothing
  }
  SQL_RESV_LOG(INFO, "resolve create tablegroup finish", K(ret), K(tablegroup_name));
  return ret;
}
