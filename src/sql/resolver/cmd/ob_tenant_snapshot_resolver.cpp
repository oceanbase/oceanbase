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

#include "sql/resolver/cmd/ob_tenant_snapshot_stmt.h"
#include "sql/resolver/cmd/ob_tenant_snapshot_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_share_util.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

ObCreateTenantSnapshotResolver::ObCreateTenantSnapshotResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObCreateTenantSnapshotResolver::~ObCreateTenantSnapshotResolver()
{
}

int ObCreateTenantSnapshotResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCreateTenantSnapshotStmt *mystmt = NULL;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  if (OB_ISNULL(node)
      || OB_UNLIKELY(T_CREATE_TENANT_SNAPSHOT != node->type_)
      || OB_UNLIKELY(2 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", KR(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", KR(ret));
  } else {
    bool is_compatible = false;
    tenant_id = session_info_->get_login_tenant_id();
    if (OB_FAIL(share::ObShareUtil::check_compat_version_for_clone_tenant_with_tenant_role(
                    tenant_id, is_compatible))) {
      LOG_WARN("fail to check compat version", KR(ret), K(tenant_id));
    } else if (!is_compatible) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("tenant data version is below 4.3", KR(ret), K(tenant_id), K(is_compatible));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create tenant snapshot below 4.3");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObCreateTenantSnapshotStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  /* snapshot name */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[0]) {
      if (OB_UNLIKELY(T_IDENT != node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else {
        ObString snapshot_name;
        snapshot_name.assign_ptr((char *)(node->children_[0]->str_value_),
                              static_cast<int32_t>(node->children_[0]->str_len_));
        if (OB_FAIL(mystmt->set_tenant_snapshot_name(snapshot_name))) {
          LOG_WARN("fail to set tenant snapshot name", KR(ret), K(snapshot_name));
        }
      }
    } else {
      //create snapshot name later
    }
  }

   /* tenant name */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[1]) {
      if (OB_SYS_TENANT_ID != tenant_id) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("Only sys tenant can add suffix opt(for tenant name)", KR(ret), K(tenant_id));
      } else if (OB_UNLIKELY(T_IDENT != node->children_[1]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else {
        ObString tenant_name;
        tenant_name.assign_ptr((char *)(node->children_[1]->str_value_),
                              static_cast<int32_t>(node->children_[1]->str_len_));
        if (OB_FAIL(mystmt->set_tenant_name(tenant_name))) {
          LOG_WARN("fail to set tenant name", KR(ret), K(tenant_name));
        }
      }
    }
  }

  return ret;
}

ObDropTenantSnapshotResolver::ObDropTenantSnapshotResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObDropTenantSnapshotResolver::~ObDropTenantSnapshotResolver()
{
}

int ObDropTenantSnapshotResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObDropTenantSnapshotStmt *mystmt = NULL;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  if (OB_ISNULL(node)
      || OB_UNLIKELY(T_DROP_TENANT_SNAPSHOT != node->type_)
      || OB_UNLIKELY(2 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", KR(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", KR(ret));
  } else {
    bool is_compatible = false;
    tenant_id = session_info_->get_login_tenant_id();
    if (OB_FAIL(share::ObShareUtil::check_compat_version_for_clone_tenant_with_tenant_role(
                    tenant_id, is_compatible))) {
      LOG_WARN("fail to check compat version", KR(ret), K(tenant_id));
    } else if (!is_compatible) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("tenant data version is below 4.3", KR(ret), K(tenant_id), K(is_compatible));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop tenant snapshot below 4.3");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObDropTenantSnapshotStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  /* snapshot name */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[0]) {
      if (OB_UNLIKELY(T_IDENT != node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else {
        ObString snapshot_name;
        snapshot_name.assign_ptr((char *)(node->children_[0]->str_value_),
                              static_cast<int32_t>(node->children_[0]->str_len_));
        if (OB_FAIL(mystmt->set_tenant_snapshot_name(snapshot_name))) {
          LOG_WARN("fail to set tenant snapshot name", KR(ret), K(snapshot_name));
        }
      }
    }
  }

   /* tenant name */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[1]) {
      if (OB_SYS_TENANT_ID != tenant_id) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("Only sys tenant can add suffix opt(for tenant name)", KR(ret), K(tenant_id));
      } else if (OB_UNLIKELY(T_IDENT != node->children_[1]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else {
        ObString tenant_name;
        tenant_name.assign_ptr((char *)(node->children_[1]->str_value_),
                              static_cast<int32_t>(node->children_[1]->str_len_));
        if (OB_FAIL(mystmt->set_tenant_name(tenant_name))) {
          LOG_WARN("fail to set tenant name", KR(ret), K(tenant_name));
        }
      }
    }
  }

  return ret;
}

}
}
