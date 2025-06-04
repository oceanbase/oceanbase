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

int ObSchemaGetterGuard::get_external_resource_schema(const uint64_t &tenant_id,
                                                      const uint64_t &database_id,
                                                      const ObString &name,
                                                      const ObSimpleExternalResourceSchema *&schema)
{
  int ret = OB_SUCCESS;

  schema = nullptr;
  const ObSchemaMgr *mgr = nullptr;
  const ObSimpleExternalResourceSchema *simple_schema = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)
              || (OB_INVALID_ID == database_id)
              || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(tenant_id), K(database_id), K(name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_external_resource_schema(tenant_id, database_id, name, schema))){
    LOG_WARN("fail to get external_resource schema", K(ret), K(tenant_id), K(database_id), K(name));
  }

  return ret;
}

int ObSchemaGetterGuard::get_external_resource_schema(const uint64_t &tenant_id,
                                                      const uint64_t &external_resource_id,
                                                      const ObSimpleExternalResourceSchema *&schema)
{
  int ret = OB_SUCCESS;

  schema = nullptr;
  const ObSchemaMgr *mgr = nullptr;
  const ObSimpleExternalResourceSchema *simple_schema = nullptr;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)
              || (OB_INVALID_ID == external_resource_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(tenant_id), K(external_resource_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_external_resource_schema(tenant_id, external_resource_id, schema))){
    LOG_WARN("fail to get external_resource schema", K(ret), K(tenant_id), K(external_resource_id));
  }

  return ret;
}

int ObSchemaGetterGuard::check_external_resource_exist(uint64_t tenant_id,
                                                       uint64_t database_id,
                                                       ObString name,
                                                       bool &is_exist)
{
  int ret = OB_SUCCESS;

  is_exist = false;

  const ObSimpleExternalResourceSchema *schema = nullptr;

  if (OB_FAIL(get_external_resource_schema(tenant_id, database_id, name, schema))) {
    LOG_WARN("failed to get_external_resource_schema", K(ret), K(tenant_id), K(database_id), K(name));
  } else {
    is_exist = OB_NOT_NULL(schema);
  }

  LOG_INFO("finished to check_external_resource_exist",
           K(ret), K(tenant_id), K(database_id), K(name), K(is_exist), KPC(schema));

  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
