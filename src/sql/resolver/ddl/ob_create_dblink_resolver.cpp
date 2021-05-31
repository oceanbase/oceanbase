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
#include "sql/resolver/ddl/ob_create_dblink_resolver.h"

#include "sql/ob_sql_utils.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_create_dblink_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObCreateDbLinkResolver::ObCreateDbLinkResolver(ObResolverParams& params) : ObDDLResolver(params)
{}

ObCreateDbLinkResolver::~ObCreateDbLinkResolver()
{}

int ObCreateDbLinkResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode* node = const_cast<ParseNode*>(&parse_tree);
  ObCreateDbLinkStmt* create_dblink_stmt = NULL;
  if (OB_ISNULL(node) || OB_UNLIKELY(node->type_ != T_CREATE_DBLINK) ||
      OB_UNLIKELY(node->num_child_ != DBLINK_NODE_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node children", K(ret), K(node), K(node->children_));
  } else if (OB_ISNULL(create_dblink_stmt = create_stmt<ObCreateDbLinkStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create create_dblink_stmt", K(ret));
  } else {
    stmt_ = create_dblink_stmt;
    create_dblink_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    create_dblink_stmt->set_user_id(session_info_->get_user_id());
  }
  if (OB_SUCC(ret)) {
    ObString dblink_name;
    ParseNode* name_node = node->children_[DBLINK_NAME];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (name_node->str_len_ >= OB_MAX_DBLINK_NAME_LENGTH) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(name_node->str_len_), name_node->str_value_);
    } else if (FALSE_IT(dblink_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_)))) {
      ;
    } else if (OB_FAIL(create_dblink_stmt->set_dblink_name(dblink_name))) {
      LOG_WARN("set dblink name failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObString cluster_name;
    ParseNode* name_node = node->children_[OPT_CLUSTER];
    if (!OB_ISNULL(name_node)) {
      cluster_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
    }
    if (OB_FAIL(create_dblink_stmt->set_cluster_name(cluster_name))) {
      LOG_WARN("set cluster name failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObString tenant_name;
    ParseNode* name_node = node->children_[TENANT_NAME];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(tenant_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_)))) {
      ;
    } else if (OB_FAIL(create_dblink_stmt->set_tenant_name(tenant_name))) {
      LOG_WARN("set tenant name failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObString user_name;
    ParseNode* name_node = node->children_[USER_NAME];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(user_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_)))) {
      ;
    } else if (OB_FAIL(create_dblink_stmt->set_user_name(user_name))) {
      LOG_WARN("set user name failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObString password;
    ParseNode* pwd_node = node->children_[PASSWORD];
    if (OB_ISNULL(pwd_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(password.assign_ptr(pwd_node->str_value_, static_cast<int32_t>(pwd_node->str_len_)))) {
      ;
    } else if (OB_FAIL(create_dblink_stmt->set_password(password))) {
      LOG_WARN("set password failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObAddr host;
    ObString host_str;
    ParseNode* host_node = NULL;
    if (OB_ISNULL(node->children_[IP_PORT]) || OB_ISNULL(node->children_[IP_PORT]->children_) ||
        OB_ISNULL(host_node = node->children_[IP_PORT]->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", K(ret));
    } else if (FALSE_IT(host_str.assign_ptr(host_node->str_value_, static_cast<int32_t>(host_node->str_len_)))) {
      ;
    } else if (OB_FAIL(host.parse_from_string(host_str))) {
      LOG_WARN("parse ip port failed", K(ret), K(host_str));
    } else {
      create_dblink_stmt->set_host_addr(host);
    }
  }
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ(schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
           session_info_->get_priv_user_id(),
           ObString(""),
           stmt::T_CREATE_DBLINK,
           session_info_->get_enable_role_array()),
        session_info_->get_effective_tenant_id(),
        session_info_->get_user_id());
  }
  LOG_INFO("resolve create dblink finish", K(ret));
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
