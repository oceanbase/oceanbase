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

#include "sql/resolver/cmd/ob_trigger_storage_cache_stmt.h"
#include "sql/resolver/cmd/ob_trigger_storage_cache_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

ObTriggerStorageCacheResolver::ObTriggerStorageCacheResolver(ObResolverParams &params)
  : ObCMDResolver(params)
{
}

ObTriggerStorageCacheResolver::~ObTriggerStorageCacheResolver()
{
}
int ObTriggerStorageCacheResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObTriggerStorageCacheStmt *stmt = nullptr;
  uint64_t compat_version = 0;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("shared nothing do not support trigger storage cache", KR(ret));
  } else if (OB_UNLIKELY(T_TRIGGER_STORAGE_CACHE != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is not T_TRIGGER_STORAGE_CACHE", K(ret), "type", get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt = create_stmt<ObTriggerStorageCacheStmt>())) {
  } else if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children should not be null", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children is null", K(ret), "num_child", parse_tree.num_child_);
  } else {
    stmt_ = stmt;
    obrpc::ObTriggerStorageCacheArg::ObStorageCacheOp op = static_cast<obrpc::ObTriggerStorageCacheArg::ObStorageCacheOp>(parse_tree.children_[0]->value_);
    stmt->set_storage_cache_op(op);
    if (obrpc::ObTriggerStorageCacheArg::TRIGGER == op) {
      // TRIGGER operation: ALTER SYSTEM TRIGGER STORAGE_CACHE_POLICY_EXECUTOR [TENANT = tenant_name]
      // parse_tree has 2 children: [action_type, tenant_name]
      if (OB_UNLIKELY(2 != parse_tree.num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children num not match for TRIGGER op", K(ret), "num_child", parse_tree.num_child_);
      } else if (OB_FAIL(resolve_tenant_name_(stmt, parse_tree.children_[1]))) {
        LOG_WARN("fail to resolve tenant name", K(ret));
      }
    } else if (obrpc::ObTriggerStorageCacheArg::SET_STATUS == op) {
      // SET_STATUS operation: ALTER SYSTEM SET STATUS STORAGE_CACHE_POLICY_EXECUTOR
      //                       POLICY_STATUS = 'xxx' TABLET_ID = xxx [TENANT = tenant_name]
      // parse_tree has 4 children: [action_type, policy_status, tablet_id, tenant_name]
      if (OB_UNLIKELY(4 != parse_tree.num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children num not match for SET_STATUS op", K(ret), "num_child", parse_tree.num_child_);
      } else if (OB_ISNULL(parse_tree.children_[1]) || OB_ISNULL(parse_tree.children_[2]) || OB_ISNULL(parse_tree.children_[3])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("policy_status or tablet_id or tenant_name is null", K(ret));
      } else if (OB_ISNULL(parse_tree.children_[2]->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet_id value node is null", K(ret));
      } else {
        ObString policy_status_str(parse_tree.children_[1]->str_len_, parse_tree.children_[1]->str_value_);
        // parse_tree.children_[2] is T_TABLET_ID node, its children_[0] is INTNUM node containing the actual value
        const int64_t tablet_id = parse_tree.children_[2]->children_[0]->value_;
        int8_t policy_status = -1;
        if (tablet_id <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid tablet_id", K(ret), K(tablet_id));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tablet_id must be positive");
        } else if (0 == policy_status_str.case_compare("hot")) {
          policy_status = static_cast<int8_t>(storage::PolicyStatus::HOT);
        } else if (0 == policy_status_str.case_compare("auto")) {
          policy_status = static_cast<int8_t>(storage::PolicyStatus::AUTO);
        } else if (0 == policy_status_str.case_compare("cold")) {
          policy_status = static_cast<int8_t>(storage::PolicyStatus::COLD);
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid policy_status", K(ret), K(policy_status_str));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "policy_status must be 'hot', 'auto' or 'cold'");
        }
        if (OB_SUCC(ret)) {
          stmt->set_tablet_id(tablet_id);
          stmt->set_policy_status(policy_status);
          if (OB_FAIL(resolve_tenant_name_(stmt, parse_tree.children_[3]))) {
            LOG_WARN("fail to resolve tenant name", K(ret));
          }
        }
        LOG_INFO("[SCP]resolver: set storage cache policy status resolved successfully",
            K(ret), K(op), K(tablet_id), K(policy_status), K(policy_status_str),
            "tenant_id", stmt->get_rpc_arg().get_tenant_id());
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown storage cache op", K(ret), K(op));
    }
  }
  LOG_INFO("[SCP]resolver: resolve finished", K(ret),
      "stmt_type", (stmt ? stmt->get_rpc_arg().get_op() : -1),
      "tenant_id", (stmt ? stmt->get_rpc_arg().get_tenant_id() : 0),
      "tablet_id", (stmt ? stmt->get_rpc_arg().get_tablet_id() : 0),
      "policy_status", (stmt ? stmt->get_rpc_arg().get_policy_status() : -1));
  return ret;
}

int ObTriggerStorageCacheResolver::resolve_tenant_name_(ObTriggerStorageCacheStmt *stmt, ParseNode *name)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = session_info_->get_login_tenant_id();
  if (OB_ISNULL(session_info_) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info or stmt should not be null", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_NOT_NULL(name)) {
      uint64_t specified_tenant_id = OB_INVALID_ID;
      ObSchemaGetterGuard schema_guard;
      if (OB_ISNULL(name->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children should not be null", K(ret));
      } else {
        ObString tenant_name(name->children_[0]->str_len_, name->children_[0]->str_value_);
        if (tenant_name.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tenant name is invalid", K(ret), K(name), K(tenant_name));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant name is invalid");
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
          LOG_WARN("get_schema_guard failed", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, specified_tenant_id))) {
          LOG_WARN("fail to get tenant id with the given tenant name", K(ret), K(tenant_name), K(specified_tenant_id));
        } else if (!is_valid_tenant_id(specified_tenant_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid tenant id to switch", K(ret), K(specified_tenant_id));
        } else {
          stmt->set_tenant_id(specified_tenant_id);
        }
      }
    } else {
      stmt->set_tenant_id(tenant_id);
    }
  } else { // OB_SYS_TENANT_ID != tenant_id
    if (OB_ISNULL(name)) {
      stmt->set_tenant_id(tenant_id);
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("user tenant cannot specify tenant name", K(ret), K(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "user tenant cannot specify tenant names");
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
