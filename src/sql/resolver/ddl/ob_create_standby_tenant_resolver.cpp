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

#include "sql/resolver/ddl/ob_create_standby_tenant_resolver.h"
#include "sql/resolver/ddl/ob_create_tenant_stmt.h"
#include "sql/resolver/ddl/ob_tenant_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/cmd/ob_variable_set_resolver.h"
#include "sql/resolver/ob_resolver_utils.h" // ObResolverUtils

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

ObCreateStandbyTenantResolver::ObCreateStandbyTenantResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObCreateStandbyTenantResolver::~ObCreateStandbyTenantResolver()
{
}

int ObCreateStandbyTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateTenantStmt *mystmt = NULL;

  if (OB_UNLIKELY(T_CREATE_STANDBY_TENANT != parse_tree.type_)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(4 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret), K(parse_tree.type_), K(parse_tree.num_child_), KP(parse_tree.children_));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObCreateTenantStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt");
    } else {
      (void)mystmt->set_stmt_type(stmt::T_CREATE_STANDBY_TENANT);
      stmt_ = mystmt;
    }
  }

  if (OB_SUCC(ret)) {
    (void)mystmt->set_create_standby_tenant();
  }

  /* [if not exists] */
  if (OB_SUCC(ret)) {
    if (NULL != parse_tree.children_[0]) {
      if (OB_UNLIKELY(T_IF_NOT_EXISTS != parse_tree.children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid parse_tree", KR(ret));
      } else {
        mystmt->set_if_not_exist(true);
      }
    } else {
      mystmt->set_if_not_exist(false);
    }
  }

  /* tenant name */
  if (OB_SUCC(ret)) {
    ObTenantResolver<ObCreateTenantStmt> resolver;
    if (OB_FAIL(resolver.resolve_tenant_name(mystmt, parse_tree.children_[1]))) {
      LOG_WARN("resolve tenant name failed", KR(ret));
    } else {
      const ObString &tenant_name = mystmt->get_create_tenant_arg().tenant_schema_.get_tenant_name_str();
      if (OB_FAIL(ObResolverUtils::check_not_supported_tenant_name(tenant_name))) {
        LOG_WARN("unsupported tenant name", KR(ret), K(tenant_name));
      }
    }
  }

  /* log restore source */
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_log_restore_source_(mystmt, parse_tree.children_[2]))) {
      LOG_WARN("resolve log_restore_source failed", KR(ret));
    }
  }

  /* tenant options */
  if (OB_SUCC(ret)) {
    if (NULL != parse_tree.children_[3]) {
      if (OB_UNLIKELY(T_TENANT_OPTION_LIST != parse_tree.children_[3]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid option node type", K(parse_tree.children_[3]->type_), KR(ret));
      } else {
        ObTenantResolver<ObCreateTenantStmt> resolver;
        ret = resolver.resolve_tenant_options(mystmt, parse_tree.children_[3], session_info_, *allocator_);
      }
    }
  }

  return ret;
}

int ObCreateStandbyTenantResolver::resolve_log_restore_source_(ObCreateTenantStmt *stmt, ParseNode *log_restore_source_node) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(ERROR, "null ptr", KR(ret), KP(stmt));
  } else if (OB_ISNULL(log_restore_source_node)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "should specify log restore source", KR(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "LOG_RESTORE_SOURCE");
  } else if (OB_UNLIKELY(T_LOG_RESTORE_SOURCE != log_restore_source_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid parse_tree", KR(ret), K(log_restore_source_node->type_));
  } else if (OB_ISNULL(log_restore_source_node->children_)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "log restore source invalid", KR(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "LOG_RESTORE_SOURCE");
  } else {
    ObString log_restore_source("");
    if (OB_FAIL(ObResolverUtils::resolve_string(log_restore_source_node->children_[0], log_restore_source))) {
      LOG_WARN("resolve string failed", KR(ret), K(log_restore_source_node->type_));
    } else {
      stmt->set_log_restore_source(log_restore_source);
    }
  }

  return ret;
}

} /* sql */
} /* oceanbase */
