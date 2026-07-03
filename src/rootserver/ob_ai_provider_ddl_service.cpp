/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/ob_ai_provider_ddl_service.h"
#include "rootserver/ob_ai_provider_ddl_operator.h"
#include "share/schema/ob_ai_provider_mgr.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ai_service/ob_ai_builtin_provider.h"

namespace oceanbase
{
namespace rootserver
{

int ObAIProviderDDLService::register_provider(const obrpc::ObRegisterProviderArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.exec_tenant_id_;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_0_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.6.0.1 is not supported", K(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.6.0.1");
  } else {
    ObAIProviderSchema new_schema;
    new_schema.set_tenant_id(tenant_id);
    new_schema.set_name(arg.provider_name_);
    if (arg.has_protocol_) {
      new_schema.set_protocol(arg.protocol_);
    }
    if (arg.has_base_url_) {
      new_schema.set_base_url(arg.base_url_);
    }
    new_schema.set_access_key(arg.access_key_);

    ObSchemaGetterGuard schema_guard;
    const ObAIProviderSchema *old_provider_schema = nullptr;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_ai_provider_schema(tenant_id, arg.provider_name_, old_provider_schema))) {
      LOG_WARN("fail to get ai provider schema", K(ret), K(tenant_id), K(arg.provider_name_));
    } else if (OB_ISNULL(old_provider_schema)) {
      // New provider: check if it's a builtin provider first
      const ObAIBuiltinProvider *builtin = nullptr;
      for (int64_t i = 0; i < share::BUILTIN_PROVIDER_COUNT; ++i) {
        if (0 == arg.provider_name_.case_compare(share::BUILTIN_PROVIDERS[i].name_)) {
          builtin = &share::BUILTIN_PROVIDERS[i];
          break;
        }
      }

      if (OB_NOT_NULL(builtin)) {
        // Builtin provider: fill in defaults from builtin config
        if (!arg.has_protocol_) {
          const ObString default_protocol(builtin->protocol_);
          if (OB_FAIL(new_schema.set_protocol(default_protocol))) {
            LOG_WARN("failed to set builtin protocol", K(ret));
          }
        }
        if (OB_SUCC(ret) && !arg.has_base_url_) {
          const ObString default_base_url(builtin->default_base_url_);
          if (OB_FAIL(new_schema.set_base_url(default_base_url))) {
            LOG_WARN("failed to set builtin base_url", K(ret));
          }
        }
      } else {
        // Non-builtin provider: protocol defaults to "openai", base_url is required
        if (!arg.has_protocol_) {
          const ObString default_protocol("openai");
          if (OB_FAIL(new_schema.set_protocol(default_protocol))) {
            LOG_WARN("failed to set default protocol for new provider", K(ret));
          }
        }
        if (OB_SUCC(ret) && !arg.has_base_url_) {
          ret = OB_AI_FUNC_PARAM_EMPTY;
          ObString var_name = "base_url";
          LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
          LOG_WARN("base_url is required for new non-builtin provider", K(ret));
        }
      }
    } else {
      // Provider already exists: reject re-registration, use ALTER_PROVIDER to modify
      ret = OB_ENTRY_EXIST;
      LOG_WARN("ai provider already exists, cannot register again", K(ret), K(tenant_id), K(arg.provider_name_));
      LOG_USER_ERROR(OB_ENTRY_EXIST, "ai provider already exists");
    }

    if (OB_SUCC(ret)) {
      ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
      ObAIProviderDDLOperator ddl_operator(ddl_service_.get_schema_service());
      int64_t refreshed_schema_version = 0;
      if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ddl_operator.register_provider(new_schema, old_provider_schema, arg.ddl_stmt_str_, trans))) {
        LOG_WARN("failed to register ai provider", K(new_schema), K(ret));
      }

      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
          ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
          LOG_WARN("publish schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAIProviderDDLService::unregister_provider(const obrpc::ObUnregisterProviderArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.exec_tenant_id_;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_0_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.6.0.1 is not supported", K(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.6.0.1");
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObAIProviderSchema *old_provider_schema = nullptr;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_ai_provider_schema(tenant_id, arg.provider_name_, old_provider_schema))) {
      LOG_WARN("fail to get ai provider schema", K(ret), K(tenant_id), K(arg.provider_name_));
    } else if (OB_ISNULL(old_provider_schema)) {
      ret = OB_AI_FUNC_MODEL_NOT_FOUND;
      LOG_USER_ERROR(OB_AI_FUNC_MODEL_NOT_FOUND, arg.provider_name_.length(), arg.provider_name_.ptr());
      LOG_WARN("ai provider not found", K(ret), K(tenant_id), K(arg.provider_name_));
    } else {
      ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
      ObAIProviderDDLOperator ddl_operator(ddl_service_.get_schema_service());
      int64_t refreshed_schema_version = 0;
      if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ddl_operator.unregister_provider(*old_provider_schema, arg.ddl_stmt_str_, trans))) {
        LOG_WARN("failed to unregister ai provider", KPC(old_provider_schema), K(ret));
      }

      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
          ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
          LOG_WARN("publish schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAIProviderDDLService::alter_provider(const obrpc::ObAlterProviderArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.exec_tenant_id_;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_0_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version less than 4.6.0.1 is not supported", K(ret), K(tenant_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version less than 4.6.0.1");
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObAIProviderSchema *old_provider_schema = nullptr;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_ai_provider_schema(tenant_id, arg.provider_name_, old_provider_schema))) {
      LOG_WARN("fail to get ai provider schema", K(ret), K(tenant_id), K(arg.provider_name_));
    } else if (OB_ISNULL(old_provider_schema)) {
      ret = OB_AI_FUNC_MODEL_NOT_FOUND;
      LOG_USER_ERROR(OB_AI_FUNC_MODEL_NOT_FOUND, arg.provider_name_.length(), arg.provider_name_.ptr());
      LOG_WARN("ai provider not found", K(ret), K(tenant_id), K(arg.provider_name_));
    } else {
      // Construct new_schema: inherit from old_schema, override base_url/access_key if provided
      ObAIProviderSchema new_schema;
      new_schema.set_tenant_id(tenant_id);
      new_schema.set_name(old_provider_schema->get_name());
      // protocol cannot be modified, always inherit
      new_schema.set_protocol(old_provider_schema->get_protocol());
      // base_url: update if provided, otherwise inherit
      if (arg.has_base_url_) {
        new_schema.set_base_url(arg.base_url_);
      } else {
        new_schema.set_base_url(old_provider_schema->get_base_url());
      }
      // access_key: update if provided (already encrypted in PL layer), otherwise inherit
      if (arg.has_access_key_) {
        new_schema.set_access_key(arg.access_key_);
      } else {
        new_schema.set_access_key(old_provider_schema->get_access_key());
      }

      ObDDLSQLTransaction trans(&ddl_service_.get_schema_service());
      ObAIProviderDDLOperator ddl_operator(ddl_service_.get_schema_service());
      int64_t refreshed_schema_version = 0;
      if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(trans.start(&ddl_service_.get_sql_proxy(), tenant_id, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ddl_operator.register_provider(new_schema, old_provider_schema, arg.ddl_stmt_str_, trans))) {
        // Reuse register_provider's update branch:
        // When old_provider_schema != nullptr, it internally calls alter_ai_provider
        LOG_WARN("failed to alter ai provider", K(new_schema), K(ret));
      }

      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
          ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
          LOG_WARN("publish schema failed", K(ret));
        }
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
