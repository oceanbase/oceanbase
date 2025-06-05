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

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_mgr.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/privilege_check/ob_ora_priv_check.h"

namespace oceanbase
{
using namespace common;
using namespace observer;

namespace share
{
namespace schema
{
int ObSchemaGetterGuard::get_location_schema_by_name(const uint64_t tenant_id,
                                                     const common::ObString &name,
                                                     const ObLocationSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
             || OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(name), KR(ret));
  } else if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", K(ret), K(tenant_id));
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->location_mgr_.get_location_schema_by_name(tenant_id, mode, name, schema))) {
    LOG_WARN("get location schema failed", K(name), KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_location_schema_by_id(const uint64_t tenant_id,
                                                   const uint64_t location_id,
                                                   const ObLocationSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
             || OB_UNLIKELY(!is_valid_id(location_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(location_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->get_location_schema(tenant_id, location_id, schema))) {
    LOG_WARN("get schema failed", K(tenant_id), K(location_id), KR(ret));
  }
  return ret;
}

}
}
}