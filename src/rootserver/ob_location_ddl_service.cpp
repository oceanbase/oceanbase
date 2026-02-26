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
#include "rootserver/ob_location_ddl_service.h"
#include "rootserver/ob_location_ddl_operator.h"
#include "rootserver/ob_ddl_sql_generator.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver
{
int ObLocationDDLService::create_location(const obrpc::ObCreateLocationArg &arg,
                                          const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const bool is_or_replace = arg.or_replace_;
  const uint64_t tenant_id = arg.schema_.get_tenant_id();
  const uint64_t user_id = arg.user_id_;
  const ObString &location_name = arg.schema_.get_location_name();
  const ObString &location_url = arg.schema_.get_location_url();
  const ObString &location_access_info = arg.schema_.get_location_access_info();
  const ObLocationSchema *schema_ptr = NULL;
  bool is_exist = false;
  ObLocationSchema new_schema;
  ObSchemaGetterGuard schema_guard;
  int64_t refreshed_schema_version = 0;
  uint64_t loc_id = OB_INVALID_ID;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(arg.exec_tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_4_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("external location not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external location");
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard with version in inner table", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_location_schema_by_name(tenant_id, location_name, schema_ptr))) {
    LOG_WARN("failed to get location schema by name", K(ret), K(tenant_id), K(location_name));
  } else if (NULL != schema_ptr) {
    is_exist = true;
    loc_id = schema_ptr->get_location_id();
    if (OB_FAIL(new_schema.assign(*schema_ptr))) {
      LOG_WARN("failed to assign new location schema", K(ret), K(*schema_ptr));
    } else if (OB_FAIL(new_schema.set_location_url(location_url))) {
      LOG_WARN("failed to set location path", K(ret), K(location_url));
    } else if (OB_FAIL(new_schema.set_location_access_info(location_access_info))) {
      LOG_WARN("failed to set location access id", K(ret), K(location_access_info));
    }
  } else if (NULL == schema_ptr) {
    if (OB_FAIL(new_schema.assign(arg.schema_))) {
      LOG_WARN("failed to assign new location schema", K(ret), K(arg));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_exist && !is_or_replace) {
    ret = OB_LOCATION_OBJ_EXIST;
    LOG_WARN("location already exists and is not replace operation", K(ret),
        K(is_or_replace), K(location_name));
  } else {
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    ObLocationDDLOperator ddl_operator(ddl_service_->get_schema_service(), ddl_service_->get_sql_proxy());
    if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to start transaction", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (is_exist && is_or_replace
        && OB_FAIL(ddl_operator.alter_location(*ddl_stmt_str, new_schema, trans))) {
      LOG_WARN("failed to alter location", K(ret), K(new_schema));
    } else if (!is_exist && OB_FAIL(ddl_operator.create_location(*ddl_stmt_str, user_id, new_schema, trans))) {
      LOG_WARN("failed to create location", K(ret), K(new_schema));
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
      LOG_WARN("publish schema failed", K(ret));
    }
  }
  return ret;
}

int ObLocationDDLService::drop_location(const obrpc::ObDropLocationArg &arg,
                                        const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  const ObString &location_name = arg.location_name_;
  const ObLocationSchema *schema_ptr = NULL;
  bool is_exist = false;
  bool is_oracle_mode = false;
  ObSchemaGetterGuard schema_guard;
  int64_t refreshed_schema_version = 0;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(arg.exec_tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_4_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("external location not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external location");
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard with version in inner table", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_location_schema_by_name(tenant_id, location_name, schema_ptr))) {
    LOG_WARN("failed to get schema by location name", K(ret), K(tenant_id), K(location_name));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
  } else if (NULL != schema_ptr) {
    is_exist = true;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!is_exist) {
    ret = OB_LOCATION_OBJ_NOT_EXIST;
    LOG_WARN("location does not exist", K(ret), K(location_name));
    LOG_USER_ERROR(OB_LOCATION_OBJ_NOT_EXIST,
                   static_cast<int>(location_name.length()),
                   location_name.ptr());
  } else {
    ObDDLSQLTransaction trans(&ddl_service_->get_schema_service());
    ObLocationDDLOperator ddl_operator(ddl_service_->get_schema_service(), ddl_service_->get_sql_proxy());
    ObLocationSchema schema;
    if (OB_FAIL(schema.assign(*schema_ptr))) {
      LOG_WARN("fail to assign location schema", K(ret), K(*schema_ptr));
    } else if (OB_FAIL(trans.start(&ddl_service_->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to start transaction", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(ddl_operator.drop_location(*ddl_stmt_str, schema, trans))) {
      LOG_WARN("failed to drop location", K(ret), K(schema));
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ddl_service_->publish_schema(tenant_id))) {
      LOG_WARN("publish schema failed", K(ret));
    }
  }
  return ret;
}

int ObLocationDDLService::check_location_constraint(const ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  const uint64_t tenant_id = schema.get_tenant_id();
  sql::ObExternalFileFormat::FormatType external_table_type = sql::ObExternalFileFormat::INVALID_FORMAT;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (OB_FAIL(sql::ObSQLUtils::get_external_table_type(&schema, external_table_type))) {
    LOG_WARN("failed to get external table type", K(ret));
  } else if (sql::ObExternalFileFormat::ODPS_FORMAT == external_table_type ||
      sql::ObExternalFileFormat::PLUGIN_FORMAT == external_table_type) {
    // do nothing
  } else {
    if (compat_version < DATA_VERSION_4_4_0_0 && OB_INVALID_ID != schema.get_external_location_id()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("fail to generate schema, not support external location id for this version", K(ret), K(tenant_id), K(compat_version), K(schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "this version not support external location id ");
    } else if (compat_version < DATA_VERSION_4_4_0_0 && !schema.get_external_sub_path().empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("fail to generate schema, not support external sub path for this version", K(ret), K(tenant_id), K(compat_version), K(schema));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "this version not support external sub path");
    } else if ((!schema.get_external_file_location().empty()
      && OB_INVALID_ID != schema.get_external_location_id())
      || (schema.get_external_file_location().empty()
          && OB_INVALID_ID == schema.get_external_location_id())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("both file location and location id are valid", KR(ret), K(schema));
    }
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase
