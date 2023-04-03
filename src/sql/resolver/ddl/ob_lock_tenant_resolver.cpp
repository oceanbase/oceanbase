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

#include "sql/resolver/ddl/ob_lock_tenant_resolver.h"
#include "sql/resolver/ddl/ob_lock_tenant_stmt.h"
#include "sql/resolver/ddl/ob_tenant_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

/**
 *  ALTER TENANT tenant_name LOCKED|UNLOCKED
 *
 */

ObLockTenantResolver::ObLockTenantResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObLockTenantResolver::~ObLockTenantResolver()
{
}

int ObLockTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObLockTenantStmt *mystmt = NULL;

  if (OB_ISNULL(node)
      || OB_UNLIKELY(T_LOCK_TENANT != node->type_)
      || OB_UNLIKELY(2 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObLockTenantStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  /* tenant name */
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(T_IDENT != node->children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", K(ret));
    } else {
      ObString tenant_name;
      tenant_name.assign_ptr((char *)(node->children_[0]->str_value_),
                             static_cast<int32_t>(node->children_[0]->str_len_));
      mystmt->set_tenant_name(tenant_name);
    }
  }

  /* options */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[1]) {
      if (OB_UNLIKELY(T_BOOL != node->children_[1]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", K(ret));
      } else {
        bool is_locked = static_cast<bool>(node->children_[1]->value_);
        mystmt->set_locked(is_locked);
      }
    }
  }

  return ret;
}


} /* sql */
} /* oceanbase */
