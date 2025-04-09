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
#include "rootserver/ob_catalog_ddl_service.h"
#include "rootserver/ob_catalog_ddl_operator.h"
#include "rootserver/ob_ddl_sql_generator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver
{

int ObCatalogDDLService::handle_catalog_ddl(const ObCatalogDDLArg &arg)
{
  int ret = OB_SUCCESS;
  ObCatalogSchema schema = arg.schema_; //make a copy
  uint64_t tenant_id = schema.get_tenant_id();
  int64_t refreshed_schema_version = 0;
  ObSchemaGetterGuard schema_guard;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(arg.exec_tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("external catalog not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external catalog");
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
  } else {
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    ObCatalogDDLOperator ddl_operator(ddl_service_->get_schema_service(), ddl_service_->get_sql_proxy());

    if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", K(ret), K(refreshed_schema_version), K(tenant_id));
    } else if (OB_FAIL(ddl_operator.handle_catalog_function(schema,
                                                            trans,
                                                            arg.ddl_type_,
                                                            arg.ddl_stmt_str_,
                                                            arg.if_exist_,
                                                            arg.if_not_exist_,
                                                            schema_guard,
                                                            arg.user_id_))) {
      LOG_WARN("handle catalog function failed", K(ret), K(arg));
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObCatalogDDLService::grant_revoke_catalog(const ObCatalogPrivSortKey &catalog_priv_key,
                                              const ObString &user_name,
                                              const ObString &host_name,
                                              const ObNeedPriv &need_priv,
                                              const bool grant,
                                              share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  bool is_ora_mode = false;
  int64_t refreshed_schema_version = 0;
  uint64_t tenant_id = catalog_priv_key.tenant_id_;
  uint64_t user_id = catalog_priv_key.user_id_;
  ObSqlString ddl_stmt_sql_str;
  ObString ddl_stmt_str;
  const ObPrivSet priv_set = need_priv.priv_set_;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Tenant id is invalid", K(ret));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_ora_mode))) {
    LOG_WARN("fail to check is oracle mode", K(ret));
  } else if (is_ora_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support grant revoke catalog level privilege in oracle mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "grant revoke catalog level privilege in oracle mode");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("external catalog not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external catalog");
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObDDLSqlGenerator::gen_catalog_priv_sql(ObAccountArg(user_name, host_name),
                                                             need_priv,
                                                             true,
                                                             ddl_stmt_sql_str))) {
    LOG_WARN("gen_catalog_priv sql failed", K(ret), K(user_name), K(host_name));
  } else {
    ddl_stmt_str = ddl_stmt_sql_str.string();
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    ObCatalogDDLOperator ddl_operator(ddl_service_->get_schema_service(), ddl_service_->get_sql_proxy());

    if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", K(ret), K(refreshed_schema_version), K(tenant_id));
    } else if (OB_FAIL(ddl_operator.grant_revoke_catalog(catalog_priv_key, priv_set,
                                                         grant, ddl_stmt_str, trans))) {
      LOG_WARN("failed to grant revoke catalog", K(ret), K(tenant_id), K(user_id), K(priv_set), K(grant));
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObCatalogDDLService::revoke_catalog(const ObRevokeCatalogArg &arg)
{
  int ret = OB_SUCCESS;
  ObSqlString ddl_stmt_str;
  ObString ddl_sql;
  ObCatalogPrivSortKey catalog_priv_key(arg.tenant_id_, arg.user_id_, arg.catalog_);
  ObSchemaGetterGuard schema_guard;
  const ObUserInfo *user_info = NULL;
  ObNeedPriv need_priv;
  need_priv.priv_set_ = arg.priv_set_;
  need_priv.priv_level_ = OB_PRIV_CATALOG_LEVEL;
  need_priv.catalog_ = arg.catalog_;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(arg.tenant_id_,
                                                                                       schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_user_info(arg.tenant_id_, arg.user_id_, user_info))) {
    LOG_WARN("get_user_info failed", K(arg), K(ret));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user not exist", K(ret), K(arg));
  } else if (OB_FAIL(grant_revoke_catalog(catalog_priv_key,
                                          user_info->get_user_name_str(),
                                          user_info->get_host_name_str(),
                                          need_priv,
                                          false,
                                          schema_guard))) {
    LOG_WARN("Grant catalog error", K(ret), K(arg.tenant_id_), K(arg.user_id_), K(ddl_sql));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase