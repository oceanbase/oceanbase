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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_schema_getter_guard.h"

#include "ob_schema_mgr.h"

namespace oceanbase
{

using namespace common;
using namespace observer;

namespace share
{

namespace schema
{

int ObSchemaGetterGuard::get_java_policy_schema(const uint64_t tenant_id,
                                                const uint64_t key,
                                                const ObSimpleJavaPolicySchema *&schema)
{
  int ret = OB_SUCCESS;

  schema = nullptr;
  const ObSchemaMgr *mgr = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)
              || (OB_INVALID_ID == key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(tenant_id), K(key), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_java_policy_schema(tenant_id, key, schema))){
    LOG_WARN("fail to get java_policy schema", K(ret), K(tenant_id), K(key));
  }

  return ret;
}

int ObSchemaGetterGuard::get_java_policy_schemas_of_grantee(const uint64_t tenant_id,
                                                   const uint64_t grantee_id,
                                                   common::ObIArray<const ObSimpleJavaPolicySchema *> &schemas)
{
  int ret = OB_SUCCESS;
  schemas.reset();
  const ObSchemaMgr *mgr = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == grantee_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(tenant_id), K(grantee_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_java_policy_schemas_of_grantee(tenant_id, grantee_id, schemas))) {
    LOG_WARN("fail to get java policy schemas", KR(ret), K(tenant_id), K(grantee_id));
  }
  return ret;

}

int ObSchemaGetterGuard::get_java_policy_schemas_in_tenant(const uint64_t tenant_id,
                                                           common::ObIArray<const ObSimpleJavaPolicySchema *> &schemas)
{
  int ret = OB_SUCCESS;
  schemas.reset();
  const ObSchemaMgr *mgr = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_java_policy_schemas_in_tenant(tenant_id, schemas))){
    LOG_WARN("fail to get java_policy schemas", K(ret), K(tenant_id));
  }

  return ret;
}

int ObSchemaGetterGuard::check_java_policy_exist(const uint64_t tenant_id,
                                                 const ObSimpleJavaPolicySchema::JavaPolicyKind kind,
                                                 const uint64_t grantee,
                                                 const uint64_t type_schema,
                                                 const common::ObString &type_name,
                                                 const common::ObString &name,
                                                 const common::ObString &action,
                                                 bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  common::ObSEArray<const ObSimpleJavaPolicySchema *, 16> schemas;
  if (OB_FAIL(get_java_policy_schemas_of_grantee(tenant_id, grantee, schemas))) {
    LOG_WARN("failed to get java policy schemas", KR(ret), K(tenant_id), K(grantee));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < schemas.count(); ++i) {
      const ObSimpleJavaPolicySchema *schema = schemas.at(i);
      CK(OB_NOT_NULL(schema));
      if (OB_SUCC(ret)
          && schema->get_kind() == kind
          && schema->get_grantee() == grantee
          && schema->get_type_schema() == type_schema
          && schema->get_type_name() == type_name
          && schema->get_name() == name
          && schema->get_action() == action) {
        is_exist = true;
        break;
      }
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
