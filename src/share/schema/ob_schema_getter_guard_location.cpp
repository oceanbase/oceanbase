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


int ObSchemaGetterGuard::get_location_schema_by_prefix_match_with_priv(
                                          const ObSessionPrivInfo &session_priv,
                                          const common::ObIArray<uint64_t> &enable_role_id_array,
                                          const uint64_t tenant_id,
                                          const common::ObString &access_path,
                                          const ObLocationSchema *&schema,
                                          const bool is_need_write_priv)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;
  ObArray<const ObLocationSchema*> match_schemas;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(access_path.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(access_path), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(get_schema_mgr(tenant_id, mgr))) {
    LOG_WARN("fail to get schema mgr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get simple schema in lazy mode not supported", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->location_mgr_.get_location_schema_by_prefix_match(tenant_id, // 检查权限需要priv_mgr, 只能先拿出url匹配的对象
                                                                            access_path,
                                                                            match_schemas))) {
    LOG_WARN("get schema failed", K(tenant_id), K(access_path), KR(ret));
  } else {
    // 再检查是否具有所需权限, 选择具有权限的对象中url最长匹配的对象(各个对象的aksk可能不一样)
    int max_match_len = 0;
    for (int i = 0; OB_SUCC(ret) && i < match_schemas.count(); ++i) {
      const ObLocationSchema *tmp_schema = match_schemas.at(i);
      bool has_priv = true;
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got null ptr", K(ret));
      } else if (OB_FAIL(check_location_access(session_priv, enable_role_id_array, tmp_schema->get_location_name_str(), is_need_write_priv))) {
        if (OB_ERR_LOCATION_ACCESS_DENIED == ret) {
          ret = OB_SUCCESS;
          has_priv = false;
        } else {
          LOG_WARN("check location access failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (has_priv && tmp_schema->get_location_name_str().length() > max_match_len) {
        schema = tmp_schema;
        max_match_len = tmp_schema->get_location_name_str().length();
      }
    }
  }
  return ret;
}

}
}
}