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
  } else if (OB_UNLIKELY(2 != parse_tree.num_child_) || 
             OB_ISNULL(parse_tree.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num not match or the children is null", K(ret), "num_child", 
        parse_tree.num_child_, K(parse_tree.children_[0]));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    stmt_ = stmt;
    obrpc::ObTriggerStorageCacheArg::ObStorageCacheOp op = static_cast<obrpc::ObTriggerStorageCacheArg::ObStorageCacheOp>(parse_tree.children_[0]->value_);
    stmt->set_storage_cache_op(op);
    ParseNode *name = parse_tree.children_[1];
    const uint64_t tenant_id = session_info_->get_login_tenant_id();
    if (OB_SYS_TENANT_ID == tenant_id) {
      if (OB_ISNULL(name)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("To trigger storage cache, the tenant_name must be given", K(ret), K(tenant_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "trigger storage cache in sys tenant without specified tenant");
      } else {
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
          } else if (!is_user_tenant(specified_tenant_id)) {
            ret = OB_OP_NOT_ALLOW;
            LOG_ERROR("can't trigger non-user tenant for storage cache", K(ret), K(specified_tenant_id), K(tenant_name));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "can't trigger non-user tenant for storage cache");
          } else {
            stmt->set_tenant_id(specified_tenant_id);
          }
          LOG_TRACE("[SCP]trigger storage cache", K(ret), K(op), K(tenant_name));
        }
      }
    } else { // OB_SYS_TENANT_ID != tenant_id
      if (OB_ISNULL(name)){
        stmt->set_tenant_id(tenant_id);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("user tenant cannot specify tenant name", K(ret), K(tenant_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "user tenant cannot specify tenant names");
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
