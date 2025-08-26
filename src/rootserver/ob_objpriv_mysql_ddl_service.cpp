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
#include "rootserver/ob_objpriv_mysql_ddl_service.h"
#include "rootserver/ob_objpriv_mysql_ddl_operator.h"
#include "rootserver/ob_ddl_sql_generator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver
{
int ObObjPrivMysqlDDLService::grant_object(
                              const share::schema::ObObjMysqlPrivSortKey &object_key,
                              const ObPrivSet priv_set,
                              const uint64_t option,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const common::ObString &grantor,
                              const common::ObString &grantor_host)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = object_key.tenant_id_;
  int64_t refreshed_schema_version = 0;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_4_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("objauth mysql not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "objauth mysql");
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
  } else if (!object_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("object_key is invalid", K(object_key), K(ret));
  } else {
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else {
      ObObjPrivMysqlDDLOperator ddl_operator(ddl_service_->get_schema_service(), ddl_service_->get_sql_proxy());
      if (OB_FAIL(ddl_operator.grant_object(object_key,
                                          priv_set,
                                          trans,
                                          option,
                                          true,
                                          grantor,
                                          grantor_host))) {
        LOG_WARN("fail to grant object", K(ret), K(object_key), K(priv_set));
      }
      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
        }
      }
    }
  }

  // publish schema
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
      LOG_WARN("publish schema failed", K(ret));
    }
  }
  return ret;
}

int ObObjPrivMysqlDDLService::revoke_object(
                              const share::schema::ObObjMysqlPrivSortKey &object_key,
                              const ObPrivSet priv_set,
                              const common::ObString &grantor,
                              const common::ObString &grantor_host)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = object_key.tenant_id_;
  int64_t refreshed_schema_version = 0;
  ObSchemaGetterGuard schema_guard;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_4_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("objauth mysql not supported", K(ret));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input schema", K(ret), K(tenant_id));
  } else if (!object_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("object_key is invalid", K(object_key), K(ret));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
  } else {
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("Start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else {
      ObObjPrivMysqlDDLOperator ddl_operator(ddl_service_->get_schema_service(), ddl_service_->get_sql_proxy());
      if (OB_FAIL(ddl_operator.revoke_object(object_key, priv_set, trans, true, true, grantor, grantor_host))) {
        LOG_WARN("fail to revoke object", K(ret), K(object_key), K(priv_set));
      }
      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
        }
      }
    }
  }

  // publish schema
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
      LOG_WARN("publish schema failed", K(ret));
    }
  }
  return ret;
}

}
}