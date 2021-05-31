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
#include "share/schema/ob_dependency_info.h"
#include "ob_schema_getter_guard.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase {
using namespace common;
namespace share {
namespace schema {

ObDependencyInfo::ObDependencyInfo()
{
  reset();
}

ObDependencyInfo::ObDependencyInfo(ObIAllocator* allocator) : ObSchema(allocator)
{
  reset();
}

ObDependencyInfo::ObDependencyInfo(const ObDependencyInfo& src_schema) : ObSchema()
{
  reset();
  *this = src_schema;
}

ObDependencyInfo::~ObDependencyInfo()
{}

ObDependencyInfo& ObDependencyInfo::operator=(const ObDependencyInfo& src_schema)
{
  if (this != &src_schema) {
    reset();
    int& ret = error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    dep_obj_id_ = src_schema.dep_obj_id_;
    dep_obj_type_ = src_schema.dep_obj_type_;
    order_ = src_schema.order_;
    dep_timestamp_ = src_schema.dep_timestamp_;
    ref_obj_id_ = src_schema.ref_obj_id_;
    ref_obj_type_ = src_schema.ref_obj_type_;
    ref_timestamp_ = src_schema.ref_timestamp_;
    dep_obj_owner_id_ = src_schema.dep_obj_owner_id_;
    property_ = src_schema.property_;
    schema_version_ = src_schema.schema_version_;
    if (OB_FAIL(deep_copy_str(src_schema.dep_attrs_, dep_attrs_))) {
      LOG_WARN("deep copy attr text failed", K(ret), K(src_schema.dep_attrs_));
    } else if (OB_FAIL(deep_copy_str(src_schema.dep_reason_, dep_reason_))) {
      LOG_WARN("deep copy reason text failed", K(ret), K(src_schema.dep_reason_));
    } else if (OB_FAIL(deep_copy_str(src_schema.ref_obj_name_, ref_obj_name_))) {
      LOG_WARN("deep copy ref obj name failed", K(ret), K(src_schema.ref_obj_name_));
    }
    error_ret_ = ret;
  }
  return *this;
}

int ObDependencyInfo::assign(const ObDependencyInfo& other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

bool ObDependencyInfo::is_user_field_valid() const
{
  bool ret = false;
  if (ObSchema::is_valid()) {
    ret = (OB_INVALID_ID != tenant_id_);
  }
  return ret;
}

bool ObDependencyInfo::is_valid() const
{
  bool ret = false;
  if (ObSchema::is_valid()) {
    if (is_user_field_valid()) {
      ret = (OB_INVALID_ID != dep_obj_id_) && (OB_INVALID_ID != ref_obj_id_) && (OB_INVALID_VERSION != schema_version_);
    } else {
    }
  } else {
  }
  return ret;
}

int ObDependencyInfo::gen_dependency_dml(const uint64_t exec_tenant_id, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;

  const ObDependencyInfo& dep_info = *this;
  // uint64_t ref_obj_id = OB_SYS_TENANT_ID == common::extract_tenant_id(dep_info.get_ref_obj_id()) ?
  //                                         dep_info.get_ref_obj_id() :
  //                                         extract_obj_id(exec_tenant_id, dep_info.get_ref_obj_id());
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id_))) ||
      OB_FAIL(dml.add_pk_column("dep_obj_id", extract_obj_id(exec_tenant_id, dep_info.get_dep_obj_id()))) ||
      OB_FAIL(dml.add_pk_column("dep_obj_type", dep_info.get_dep_obj_type())) ||
      OB_FAIL(dml.add_pk_column("dep_order", dep_info.get_order())) ||
      OB_FAIL(dml.add_column("schema_version", dep_info.get_schema_version())) ||
      OB_FAIL(dml.add_time_column("dep_timestamp", dep_info.get_dep_timestamp())) ||
      OB_FAIL(dml.add_column("ref_obj_id", extract_ref_obj_id())) ||
      OB_FAIL(dml.add_column("ref_obj_type", dep_info.get_ref_obj_type())) ||
      OB_FAIL(dml.add_time_column("ref_timestamp", dep_info.get_ref_timestamp())) ||
      OB_FAIL(dml.add_column("dep_obj_owner_id", extract_obj_id(exec_tenant_id, dep_info.get_dep_obj_owner_id()))) ||
      OB_FAIL(dml.add_column("property", dep_info.get_property())) ||
      OB_FAIL(dml.add_column("dep_attrs", ObHexEscapeSqlStr(dep_info.get_dep_attrs()))) ||
      OB_FAIL(dml.add_column("dep_reason", ObHexEscapeSqlStr(dep_info.get_dep_reason()))) ||
      OB_FAIL(dml.add_column("ref_obj_name", ObHexEscapeSqlStr(dep_info.get_ref_obj_name()))) ||
      OB_FAIL(dml.add_gmt_create()) || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

// The ref obj id may be the id of a system package.
// This id is under the system tenant and has a tenant id,
// so special treatment is required here.
uint64_t ObDependencyInfo::extract_ref_obj_id() const
{
  uint64_t ref_oid = get_ref_obj_id();
  if (OB_SYS_TENANT_ID != common::extract_tenant_id(ref_oid)) {
    const uint64_t tenant_id = get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ref_oid = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, get_ref_obj_id());
  }
  return ref_oid;
}

uint64_t ObDependencyInfo::extract_obj_id(uint64_t exec_tenant_id, uint64_t id)
{
  return ObSchemaUtils::get_extract_schema_id(exec_tenant_id, id);
}

int ObDependencyInfo::get_object_create_time(
    ObISQLClient& sql_client, ObObjectType obj_type, int64_t& create_time, ObString& ref_obj_name)
{
  int ret = OB_SUCCESS;
  uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(get_tenant_id());
#define BUILD_OBJ_QUERY_SQL(table_name, field_name, ref_obj_id)                  \
  do {                                                                           \
    OZ(sql.assign_fmt("SELECT * FROM %s WHERE %s = %ld and tenant_id = %ld",     \
        table_name,                                                              \
        field_name,                                                              \
        ref_obj_id,                                                              \
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, get_tenant_id()))); \
  } while (0)

  ObSqlString sql;
  ObString type_name(ob_object_type_str(obj_type));
  const char* all_tbl_name = NULL;
  if (false == is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error info is invalid", K(ret));
  } else if (OB_FAIL(
                 ObSchemaUtils::get_all_table_name(ObSchemaUtils::get_exec_tenant_id(get_tenant_id()), all_tbl_name))) {
    LOG_WARN("failed to get all table name", K(ret));
  } else {
    const char* tbl_name =
        (ObObjectType::PACKAGE_BODY == obj_type || ObObjectType::PACKAGE == obj_type) ? OB_ALL_PACKAGE_TNAME
        : (ObObjectType::TYPE == obj_type || ObObjectType::TYPE_BODY == obj_type)     ? OB_ALL_TYPE_TNAME
        : (ObObjectType::PROCEDURE == obj_type || ObObjectType::FUNCTION == obj_type) ? OB_ALL_ROUTINE_TNAME
        : (ObObjectType::INDEX == obj_type || ObObjectType::TABLE == obj_type || ObObjectType::VIEW == obj_type)
            ? all_tbl_name
            : NULL;
    const char* field_name =
        (ObObjectType::PACKAGE_BODY == obj_type || ObObjectType::PACKAGE == obj_type) ? "package_id"
        : (ObObjectType::TYPE == obj_type || ObObjectType::TYPE_BODY == obj_type)     ? "type_id"
        : (ObObjectType::PROCEDURE == obj_type || ObObjectType::FUNCTION == obj_type) ? "routine_id"
        : (ObObjectType::INDEX == obj_type || ObObjectType::TABLE == obj_type || ObObjectType::VIEW == obj_type)
            ? "table_id"
            : NULL;
    const char* field_obj_name =
        (ObObjectType::PACKAGE_BODY == obj_type || ObObjectType::PACKAGE == obj_type) ? "package_name"
        : (ObObjectType::TYPE == obj_type || ObObjectType::TYPE_BODY == obj_type)     ? "type_name"
        : (ObObjectType::PROCEDURE == obj_type || ObObjectType::FUNCTION == obj_type) ? "routine_name"
        : (ObObjectType::INDEX == obj_type || ObObjectType::TABLE == obj_type || ObObjectType::VIEW == obj_type)
            ? "table_name"
            : NULL;
    if (OB_NOT_NULL(tbl_name) && OB_NOT_NULL(field_name) && OB_NOT_NULL(field_obj_name)) {
      // uint64_t ref_obj_id = OB_SYS_TENANT_ID == common::extract_tenant_id(ref_obj_id_) ?
      //                       ref_obj_id_ : extract_ref_obj_id();
      BUILD_OBJ_QUERY_SQL(tbl_name, field_name, extract_ref_obj_id());
      if (OB_SUCC(ret)) {
        SMART_VAR(ObMySQLProxy::MySQLResult, res)
        {
          if (OB_FAIL(sql_client.read(res, exec_tenant_id, sql.ptr()))) {
            LOG_WARN("execute query failed", K(ret), K(sql));
          } else {
            sqlclient::ObMySQLResult* result = res.get_result();
            if (NULL != result) {
              ObString tmp_ref_name;
              common::ObTimeZoneInfo* tz_info;
              OZ(result->next());
              OZ(result->get_timestamp("gmt_create", tz_info, create_time));
              OZ(result->get_varchar(field_obj_name, tmp_ref_name));
              OZ(deep_copy_str(tmp_ref_name, ref_obj_name));
              // OZ (result->get_int("gmt_create", create_time));
            } else {
              create_time = -1;
            }
          }
        }
      }
    } else {
      create_time = -1;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDependencyInfo::delete_schema_object_dependency(common::ObISQLClient& trans, uint64_t tenant_id,
    uint64_t dep_obj_id, int64_t schema_version, ObObjectType dep_obj_type)
{
  UNUSED(schema_version);
  int ret = OB_SUCCESS;
  if (IS_CLUSTER_VERSION_BEFORE_3100) {
    // do nothing
    LOG_DEBUG("all_dependency schema only support after version 3.1");
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    const uint64_t extract_tid = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == dep_obj_id || ObObjectType::MAX_TYPE == dep_obj_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delete error info unexpected.", K(ret), K(tenant_id), K(dep_obj_id), K(dep_obj_type));
    } else if (sql.assign_fmt("delete FROM %s WHERE dep_obj_id = %ld \
                                                    AND tenant_id = %ld  \
                                                    AND dep_obj_type = %ld",
                   OB_ALL_TENANT_DEPENDENCY_TNAME,
                   extract_obj_id(tenant_id, dep_obj_id),
                   extract_tid,
                   static_cast<uint64_t>(dep_obj_type))) {
      LOG_WARN("delete from __all_tenant_dependency table failed.",
          K(ret),
          K(tenant_id),
          K(extract_tid),
          K(dep_obj_id),
          K(dep_obj_type));
    } else {
      if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute query failed", K(ret), K(sql));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObDependencyInfo::insert_schema_object_dependency(common::ObISQLClient& trans, bool is_replace, bool only_history)
{
  int ret = OB_SUCCESS;

  if (IS_CLUSTER_VERSION_BEFORE_3100) {
    // do nothing
    LOG_DEBUG("all_dependency schema only support after version 3.1");
  } else {
    ObDependencyInfo& dep_info = *this;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(dep_info.get_tenant_id());
    ObDMLSqlSplicer dml;
    int64_t ref_obj_create_time = -1;
    ObString ref_obj_name;
    if (OB_FAIL(ret)) {
      LOG_WARN("get ref object time failed", K(ret), K(dep_info.get_ref_obj_type()), K(dep_info.get_ref_obj_id()));
    } else if (OB_FAIL(gen_dependency_dml(exec_tenant_id, dml))) {
      LOG_WARN("gen table dml failed", K(ret));
    } else {
      ObDMLExecHelper exec(trans, exec_tenant_id);
      int64_t affected_rows = 0;
      if (!only_history) {
        ObDMLExecHelper exec(trans, exec_tenant_id);
        if (is_replace) {
          if (OB_FAIL(exec.exec_update(OB_ALL_TENANT_DEPENDENCY_TNAME, dml, affected_rows))) {
            LOG_WARN("execute update failed", K(ret));
          }
        } else {
          if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_DEPENDENCY_TNAME, dml, affected_rows))) {
            LOG_WARN("execute insert failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && !is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDependencyInfo::collect_dep_infos(ObIArray<ObSchemaObjVersion>& schema_objs, ObIArray<ObDependencyInfo>& deps,
    ObObjectType dep_obj_type, uint64_t property, ObString& dep_attrs, ObString& dep_reason)
{
  int ret = OB_SUCCESS;
  if (IS_CLUSTER_VERSION_BEFORE_3100) {
    // do nothing
    LOG_DEBUG("all_dependency schema only support after version 3.1");
  } else {
    int64_t order = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_objs.count(); ++i) {
      ObDependencyInfo dep;
      ObSchemaObjVersion& s_objs = schema_objs.at(i);
      if (!s_objs.is_valid()
          // object may depend on self
          || dep_obj_type == s_objs.get_schema_object_type()) {
        continue;
      }
      dep.set_dep_obj_id(OB_INVALID_ID);
      dep.set_dep_obj_type(dep_obj_type);
      dep.set_dep_obj_owner_id(OB_INVALID_ID);
      dep.set_ref_obj_id(s_objs.get_object_id());
      dep.set_ref_obj_type(s_objs.get_schema_object_type());
      dep.set_order(order);
      ++order;
      dep.set_dep_timestamp(-1);
      dep.set_ref_timestamp(s_objs.get_version());
      dep.set_property(property);
      if (dep_attrs.length() >= OB_MAX_ORACLE_RAW_SQL_COL_LENGTH ||
          dep_reason.length() >= OB_MAX_ORACLE_RAW_SQL_COL_LENGTH) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dep attrs or dep reason is too long", K(ret), K(dep_attrs.length()), K(dep_reason.length()));
      } else {
        if (!dep_attrs.empty())
          OZ(dep.set_dep_attrs(dep_attrs));
        if (!dep_reason.empty())
          OZ(dep.set_dep_reason(dep_reason));
      }
      OZ(deps.push_back(dep));
    }
  }
  return ret;
}

void ObDependencyInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  dep_obj_id_ = OB_INVALID_ID;
  dep_obj_type_ = ObObjectType::MAX_TYPE;
  order_ = 0;
  dep_timestamp_ = -1;
  ref_obj_id_ = OB_INVALID_ID;
  ref_obj_type_ = ObObjectType::MAX_TYPE;
  ref_timestamp_ = -1;
  dep_obj_owner_id_ = OB_INVALID_ID;
  property_ = 0;
  reset_string(dep_attrs_);
  reset_string(dep_reason_);
  reset_string(ref_obj_name_);
  schema_version_ = OB_INVALID_VERSION;
}

int64_t ObDependencyInfo::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObDependencyInfo));
  len += dep_attrs_.length() + 1;
  len += dep_reason_.length() + 1;
  len += ref_obj_name_.length() + 1;
  return len;
}

OB_DEF_SERIALIZE(ObDependencyInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      tenant_id_,
      dep_obj_id_,
      dep_obj_type_,
      order_,
      dep_timestamp_,
      ref_obj_id_,
      ref_obj_type_,
      ref_timestamp_,
      dep_obj_owner_id_,
      property_,
      dep_attrs_,
      dep_reason_,
      ref_obj_name_,
      schema_version_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDependencyInfo)
{
  int ret = OB_SUCCESS;
  reset();
  LST_DO_CODE(OB_UNIS_DECODE,
      tenant_id_,
      dep_obj_id_,
      dep_obj_type_,
      order_,
      dep_timestamp_,
      ref_obj_id_,
      ref_obj_type_,
      ref_timestamp_,
      dep_obj_owner_id_,
      property_,
      dep_attrs_,
      dep_reason_,
      ref_obj_name_,
      schema_version_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDependencyInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tenant_id_,
      dep_obj_id_,
      dep_obj_type_,
      order_,
      dep_timestamp_,
      ref_obj_id_,
      ref_obj_type_,
      ref_timestamp_,
      dep_obj_owner_id_,
      property_,
      dep_attrs_,
      dep_reason_,
      ref_obj_name_,
      schema_version_);
  return len;
}
}  // namespace schema
}  // namespace share
}  // namespace oceanbase
