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

#define USING_LOG_PREFIX RS
#include "rootserver/ob_ccl_ddl_operator.h"
#include "share/schema/ob_ccl_rule_sql_service.h"

namespace oceanbase
{
namespace rootserver
{
int ObCclDDLOperator::create_ccl_rule(ObCCLRuleSchema &ccl_rule_schema,
                                   ObMySQLTransaction &trans,
                                   const ObString *ddl_stmt_str /*=NULL*/) {
  int ret = OB_SUCCESS;
  uint64_t new_ccl_rule_id = OB_INVALID_ID;
  const uint64_t tenant_id = ccl_rule_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_new_ccl_rule_id(tenant_id,
                                                           new_ccl_rule_id))) {
    LOG_WARN("failed to fetch new_outline_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(
                 tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    ccl_rule_schema.set_ccl_rule_id(new_ccl_rule_id);
    ccl_rule_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_ccl_rule_sql_service().insert_ccl_rule(
            ccl_rule_schema, trans, ddl_stmt_str))) {
      LOG_WARN("insert outline info failed", K(ccl_rule_schema), K(ret));
    }
  }
  return ret;
}

int ObCclDDLOperator::drop_ccl_rule(uint64_t tenant_id,
                                 const ObCCLRuleSchema &ccl_rule_schema,
                                 ObMySQLTransaction &trans,
                                 const ObString *ddl_stmt_str /*=NULL*/) {
  int ret = OB_SUCCESS;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_UNLIKELY(OB_INVALID_ID == ccl_rule_schema.get_tenant_id() ||
                  OB_INVALID_ID == ccl_rule_schema.get_ccl_rule_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ccl_rule_schema), K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(
                 tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_ccl_rule_sql_service().delete_ccl_rule(
                 ccl_rule_schema, new_schema_version, trans, ddl_stmt_str))) {
    LOG_WARN("drop ccl rule failed", K(ccl_rule_schema), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}
} // namespace rootserver
}