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
#include "sql/resolver/ddl/ob_truncate_table_resolver.h"
#include "share/ob_define.h"
#include "share/ob_rpc_struct.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;

namespace sql {

ObTruncateTableResolver::ObTruncateTableResolver(ObResolverParams& params) : ObDDLResolver(params)
{}

ObTruncateTableResolver::~ObTruncateTableResolver()
{}

int ObTruncateTableResolver::resolve(const ParseNode& parser_tree)
{
  int ret = OB_SUCCESS;
  ParseNode* node = const_cast<ParseNode*>(&parser_tree);
  ObTruncateTableStmt* truncate_table_stmt = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(node) || T_TRUNCATE_TABLE != node->type_ || OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null or parser error", K(ret));
  }

  // create alter table stmt
  if (OB_SUCC(ret)) {
    if (NULL == (truncate_table_stmt = create_stmt<ObTruncateTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create alter table stmt", K(ret));
    } else {
      stmt_ = truncate_table_stmt;
    }
    ParseNode* relation_node = node->children_[TABLE_NODE];
    if (OB_SUCC(ret)) {
      if (NULL != relation_node) {
        // resolve table
        ObString table_name;
        ObString database_name;
        if (OB_FAIL(resolve_table_relation_node(relation_node, table_name, database_name))) {
          LOG_WARN("failed to resolve table name.", K(table_name), K(database_name), K(ret));
        } else {
          truncate_table_stmt->set_table_name(table_name);
          truncate_table_stmt->set_database_name(database_name);
          truncate_table_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
        }
      } else {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("relation node should not be null!", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    bool strict_mode = true;
    obrpc::ObTruncateTableArg& truncate_table_arg = truncate_table_stmt->get_truncate_table_arg();
    ObObj is_ob_enable_truncate_table_flashback;
    ObObj is_recyclebin;
    if (OB_FAIL(session_info_->is_create_table_strict_mode(strict_mode))) {
      SQL_RESV_LOG(WARN, "failed to get variable ob_create_table_strict_mode");
    } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_RECYCLEBIN, is_recyclebin))) {
      SQL_RESV_LOG(WARN, "get sys variable SYS_VAR_RECYCLEBIN failed", K(ret));
    } else if (OB_FAIL(session_info_->get_sys_variable(
                   share::SYS_VAR_OB_ENABLE_TRUNCATE_FLASHBACK, is_ob_enable_truncate_table_flashback))) {
      SQL_RESV_LOG(WARN, "get sys variable failed", K(ret));
    } else {
      obrpc::ObCreateTableMode create_mode =
          strict_mode ? obrpc::OB_CREATE_TABLE_MODE_STRICT : obrpc::OB_CREATE_TABLE_MODE_LOOSE;
      truncate_table_stmt->set_create_mode(create_mode);
      truncate_table_arg.to_recyclebin_ =
          (is_ob_enable_truncate_table_flashback.get_bool() && is_recyclebin.get_bool());
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
