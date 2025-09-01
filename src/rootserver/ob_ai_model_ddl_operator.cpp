/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RS
#include "rootserver/ob_ai_model_ddl_operator.h"
#include "share/schema/ob_ai_model_sql_service.h"

namespace oceanbase
{
namespace rootserver
{

int ObAiModelDDLOperator::create_ai_model(ObAiModelSchema &ai_model_schema,
                                          const ObString &ddl_stmt,
                                          common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = ai_model_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t new_ai_model_id = OB_INVALID_ID;
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_impl->fetch_new_ai_model_id(tenant_id, new_ai_model_id))) {
    LOG_WARN("failed to fetch new ai model id", K(ret), K(ai_model_schema));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("failed to gen new schema version", K(ret), K(ai_model_schema));
  } else {
    ai_model_schema.set_model_id(new_ai_model_id);
    ai_model_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service_impl->get_ai_model_sql_service().create_ai_model(ai_model_schema, ddl_stmt, trans))) {
      LOG_WARN("failed to create ai model", K(ret), K(ai_model_schema));
    }
  }
  return ret;
}

int ObAiModelDDLOperator::drop_ai_model(const ObAiModelSchema &ai_model_schema,
                                        const ObString &ddl_stmt,
                                        common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  int64_t new_schema_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = ai_model_schema.get_tenant_id();
  if (OB_UNLIKELY(!ai_model_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ai_model_schema));
  } else if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service_impl must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("failed to gen new schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_impl->get_ai_model_sql_service().drop_ai_model(
      ai_model_schema, new_schema_version, ddl_stmt, trans))) {
    LOG_WARN("failed to drop ai model", K(ret), K(tenant_id));
  }
  return ret;
}


} // namespace rootserver
}