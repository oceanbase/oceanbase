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

#include "sql/resolver/ddl/ob_drop_tenant_resolver.h"
#include "sql/resolver/ddl/ob_drop_tenant_stmt.h"
#include "sql/resolver/ddl/ob_tenant_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"


namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

/**
 *  DROP TENANT tenant_name
 */

ObDropTenantResolver::ObDropTenantResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObDropTenantResolver::~ObDropTenantResolver()
{
}

int ObDropTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObDropTenantStmt *mystmt = NULL;

  if (lib::is_oracle_mode()) {
    // oracle 模式的租户尝试删除租户时直接报错
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("dropping tenant in oracle mode is not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dropping tenant in oracle mode is");
  } else if (OB_ISNULL(node)
      || OB_UNLIKELY(T_DROP_TENANT != node->type_)
      || OB_UNLIKELY(3 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObDropTenantStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  if (OB_SUCC(ret)) {
    bool if_exist = false;
    if (NULL != node->children_[0]) {
      if (OB_UNLIKELY(T_IF_EXISTS != node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invaid node", K(ret));
      } else {
        if_exist = true;
      }
    }
    mystmt->set_if_exist(if_exist);
  }

  /* tenant name */
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(T_IDENT != node->children_[1]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", K(ret));
    } else {
      ObString tenant_name;
      tenant_name.assign_ptr((char *)(node->children_[1]->str_value_),
                             static_cast<int32_t>(node->children_[1]->str_len_));
      mystmt->set_tenant_name(tenant_name);
    }
  }

  

  /* resolve force and purge */
  if (OB_SUCC(ret)) {
    ObObj is_recyclebin_open;
    if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_RECYCLEBIN, is_recyclebin_open))) {
      SQL_RESV_LOG(WARN, "get sys variable failed", K(ret));
    } else {
      if (NULL == node->children_[2]) {
        mystmt->set_delay_to_drop(true);
        mystmt->set_open_recyclebin(is_recyclebin_open.get_bool());
      } else {
        if (OB_UNLIKELY(T_FORCE != node->children_[2]->type_) &&
            OB_UNLIKELY(T_PURGE != node->children_[2]->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid node", K(ret));
        } else if (T_PURGE == node->children_[2]->type_) {
          mystmt->set_delay_to_drop(true);
          mystmt->set_open_recyclebin(false);
        } else if (T_FORCE == node->children_[2]->type_) {
          mystmt->set_delay_to_drop(false);
          mystmt->set_open_recyclebin(is_recyclebin_open.get_bool());
        }
      }
    }
  }

  return ret;
}

} /* sql */
} /* oceanbase */
