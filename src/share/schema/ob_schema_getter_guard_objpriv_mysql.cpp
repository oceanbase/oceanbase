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

int ObSchemaGetterGuard::get_obj_mysql_priv_set(const ObObjMysqlPrivSortKey &obj_mysql_priv_key,
                                                ObPrivSet &priv_set)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  uint64_t tenant_id = obj_mysql_priv_key.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_mysql_priv_set(obj_mysql_priv_key, priv_set))) {
    LOG_WARN("fail to get object priv set", KR(ret), K(obj_mysql_priv_key));
  }
  return ret;
}

int ObSchemaGetterGuard::get_obj_mysql_priv_with_user_id(const uint64_t tenant_id,
                                                         const uint64_t user_id,
                                                         ObIArray<const ObObjMysqlPriv *> &obj_mysql_privs)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  obj_mysql_privs.reset();
  if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(user_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_mysql_privs_in_user(tenant_id, user_id, obj_mysql_privs))) {
    LOG_WARN("get obj mysql priv with user_id failed", KR(ret), K(tenant_id), K(user_id));
  }
  return ret;
}

int ObSchemaGetterGuard::get_obj_mysql_priv_with_obj_name(const uint64_t tenant_id,
                                                          const ObString &obj_name,
                                                          const uint64_t obj_type,
                                                          ObIArray<const ObObjMysqlPriv *> &obj_privs,
                                                          bool reset_flag)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (reset_flag) {
    obj_privs.reset();
  }
  if (OB_INVALID_ID == tenant_id
          || obj_name.empty()
          || OB_INVALID_ID == obj_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(tenant_id), K(obj_name), K(obj_type));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->priv_mgr_.get_obj_mysql_privs_in_obj(tenant_id, obj_name, obj_type,
                  obj_privs, reset_flag))) {
    LOG_WARN("get obj priv with grantee_id failed", KR(ret), K(tenant_id), K(obj_name), K(obj_type));
  }
  return ret;
}

}
}
}