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

#include "sql/resolver/cmd/ob_tenant_clone_stmt.h"
#include "sql/resolver/cmd/ob_tenant_clone_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

ObCloneTenantResolver::ObCloneTenantResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObCloneTenantResolver::~ObCloneTenantResolver()
{
}

int ObCloneTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCloneTenantStmt *mystmt = NULL;
  ObString new_tenant_name;
  ObString source_tenant_name;
  ObString tenant_snapshot_name;
  ObString resource_pool_name;
  ObString unit_config_name;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", KR(ret));
  } else if (OB_UNLIKELY(T_CLONE_TENANT != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", KR(ret));
  } else if (OB_UNLIKELY(5 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", KR(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", KR(ret));
  } else {
    bool is_compatible = false;
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    if (OB_FAIL(share::ObShareUtil::check_compat_version_for_clone_tenant_with_tenant_role(
                    tenant_id, is_compatible))) {
      LOG_WARN("fail to check compat version", KR(ret), K(tenant_id));
    } else if (!is_compatible) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("tenant data version is below 4.3", KR(ret), K(tenant_id), K(is_compatible));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "clone tenant below 4.3");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObCloneTenantStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  /* if not exist */
  if (OB_SUCC(ret)) {
    if (NULL != parse_tree.children_[0]) {
      if (OB_UNLIKELY(T_IF_NOT_EXISTS != parse_tree.children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else {
        mystmt->set_if_not_exists(true);
      }
    } else {
      mystmt->set_if_not_exists(false);
    }
  }

  /* new tenant name */
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == node->children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", KR(ret));
    } else if (OB_UNLIKELY(T_IDENT != node->children_[1]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", KR(ret));
    } else {
      new_tenant_name.assign_ptr((char *)(node->children_[1]->str_value_),
                                  static_cast<int32_t>(node->children_[1]->str_len_));
      if (OB_FAIL(ObResolverUtils::check_not_supported_tenant_name(new_tenant_name))) {
        LOG_WARN("unsupported tenant name", KR(ret), K(new_tenant_name));
      }
    }
  }

  /* source tenant name */
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == node->children_[2])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", KR(ret));
    } else if (OB_UNLIKELY(T_IDENT != node->children_[2]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", KR(ret));
    } else {
      source_tenant_name.assign_ptr((char *)(node->children_[2]->str_value_),
                                     static_cast<int32_t>(node->children_[2]->str_len_));
    }
  }

  /* tenant snapshot name */
  if (OB_SUCC(ret)) {
    if (NULL == node->children_[3]) {
    } else if (OB_UNLIKELY(T_IDENT != node->children_[3]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", KR(ret));
    } else {
      tenant_snapshot_name.assign_ptr((char *)(node->children_[3]->str_value_),
                                       static_cast<int32_t>(node->children_[3]->str_len_));
    }
  }

  /* clone option list */
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == node->children_[4])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", KR(ret));
    } else if (OB_UNLIKELY(T_LINK_NODE != node->children_[4]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node", KR(ret));
    } else {
      if (OB_FAIL(resolve_option_list_(node->children_[4], resource_pool_name, unit_config_name))) {
        LOG_WARN("resolve option list failed", KR(ret));
      } else if (resource_pool_name.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("resource pool name is empty", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "clone tenant without resource pool");
      } else if (unit_config_name.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unit config name is empty", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "clone tenant without unit config");
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mystmt->init(new_tenant_name, source_tenant_name,
          tenant_snapshot_name, resource_pool_name, unit_config_name))) {
      LOG_WARN("mystmt init failed", KR(ret), K(new_tenant_name), K(source_tenant_name),
          K(tenant_snapshot_name), K(resource_pool_name), K(unit_config_name));
    }
  }

  return ret;
}

int ObCloneTenantResolver::resolve_option_list_(const ParseNode *node,
                                                ObString &resource_pool_name,
                                                ObString &unit_config_name)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", KR(ret));
  } else if (OB_UNLIKELY(2 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      const ParseNode *option_node = node->children_[i];
      if (OB_ISNULL(option_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else if (OB_UNLIKELY(1 != option_node->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else if (OB_ISNULL(option_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else if (T_RESOURCE_POOL_LIST == option_node->type_) {
        resource_pool_name.assign_ptr((char *)(option_node->children_[0]->str_value_),
                              static_cast<int32_t>(option_node->children_[0]->str_len_));
      } else if (T_UNIT == option_node->type_) {
        unit_config_name.assign_ptr((char *)(option_node->children_[0]->str_value_),
                              static_cast<int32_t>(option_node->children_[0]->str_len_));
      }
    }
  }

  return ret;
}

}
}
