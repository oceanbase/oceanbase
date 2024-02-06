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
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_maintain_dependency_info_task.h"
#include "share/schema/ob_schema_struct.h"
#include "rootserver/ob_ddl_operator.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

ObDependencyInfo::ObDependencyInfo()
{
  reset();
}

ObDependencyInfo::ObDependencyInfo(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObDependencyInfo::ObDependencyInfo(const ObDependencyInfo &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObDependencyInfo::~ObDependencyInfo()
{
}

ObDependencyInfo &ObDependencyInfo::operator =(const ObDependencyInfo &src_schema)
{
  if (this != &src_schema) {
    reset();
    int &ret = error_ret_;
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

int ObDependencyInfo::assign(const ObDependencyInfo &other)
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
      ret = (OB_INVALID_ID != dep_obj_id_)
          && (OB_INVALID_ID != ref_obj_id_)
          && (OB_INVALID_VERSION != schema_version_);
    } else {}
  } else {}
  return ret;
}


int ObDependencyInfo::gen_dependency_dml(const uint64_t exec_tenant_id,
                                     ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;

  const ObDependencyInfo &dep_info = *this;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id,
                                                                                  tenant_id_)))
    || OB_FAIL(dml.add_pk_column("dep_obj_id", extract_obj_id(exec_tenant_id,
                                                 dep_info.get_dep_obj_id())))
    || OB_FAIL(dml.add_pk_column("dep_obj_type", dep_info.get_dep_obj_type()))
    || OB_FAIL(dml.add_pk_column("dep_order", dep_info.get_order()))
    || OB_FAIL(dml.add_column("schema_version", dep_info.get_schema_version()))
    || OB_FAIL(dml.add_time_column("dep_timestamp", dep_info.get_dep_timestamp()))
    || OB_FAIL(dml.add_column("ref_obj_id", get_ref_obj_id()))
    || OB_FAIL(dml.add_column("ref_obj_type", dep_info.get_ref_obj_type()))
    || OB_FAIL(dml.add_time_column("ref_timestamp", dep_info.get_ref_timestamp()))
    || OB_FAIL(dml.add_column("dep_obj_owner_id", extract_obj_id(exec_tenant_id,
                                                   dep_info.get_dep_obj_owner_id())))
    || OB_FAIL(dml.add_column("property", dep_info.get_property()))
    || OB_FAIL(dml.add_column("dep_attrs", ObHexEscapeSqlStr(dep_info.get_dep_attrs())))
    || OB_FAIL(dml.add_column("dep_reason", ObHexEscapeSqlStr(dep_info.get_dep_reason())))
    || OB_FAIL(dml.add_column("ref_obj_name", ObHexEscapeSqlStr(dep_info.get_ref_obj_name())))
    || OB_FAIL(dml.add_gmt_create())
    || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

uint64_t ObDependencyInfo::extract_obj_id(uint64_t exec_tenant_id, uint64_t id)
{
  return ObSchemaUtils::get_extract_schema_id(exec_tenant_id, id);
}

int ObDependencyInfo::get_object_create_time(ObISQLClient &sql_client,
                                             ObObjectType obj_type,
                                             int64_t &create_time,
                                             ObString &ref_obj_name)
{
  int ret = OB_SUCCESS;
  uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(get_tenant_id());
  #define BUILD_OBJ_QUERY_SQL(table_name, field_name, ref_obj_id) \
  do {\
      OZ (sql.assign_fmt("SELECT * FROM %s WHERE %s = %ld and tenant_id = %ld", \
              table_name, \
              field_name, \
              ref_obj_id, \
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, get_tenant_id()))); \
  } while(0)

  ObSqlString sql;
  ObString type_name(ob_object_type_str(obj_type));
  const char *all_tbl_name = NULL;
  if (false == is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error info is invalid", K(ret));
  } else if (OB_FAIL(ObSchemaUtils::get_all_table_name(
                          ObSchemaUtils::get_exec_tenant_id(get_tenant_id()), all_tbl_name))) {
    LOG_WARN("failed to get all table name", K(ret));
  } else {
    const char *tbl_name = (ObObjectType::PACKAGE_BODY == obj_type
                           || ObObjectType::PACKAGE == obj_type) ? OB_ALL_PACKAGE_TNAME :
                           (ObObjectType::TYPE == obj_type 
                           || ObObjectType::TYPE_BODY == obj_type) ? OB_ALL_TYPE_TNAME :
                           (ObObjectType::PROCEDURE == obj_type 
                           || ObObjectType::FUNCTION == obj_type) ? OB_ALL_ROUTINE_TNAME :
                           (ObObjectType::INDEX == obj_type 
                           || ObObjectType::TABLE == obj_type
                           || ObObjectType::VIEW == obj_type) ?
                           all_tbl_name : NULL;
    const char *field_name = (ObObjectType::PACKAGE_BODY == obj_type
                           || ObObjectType::PACKAGE == obj_type) ? "package_id" :
                           (ObObjectType::TYPE == obj_type 
                           || ObObjectType::TYPE_BODY == obj_type) ? "type_id" :
                           (ObObjectType::PROCEDURE == obj_type 
                           || ObObjectType::FUNCTION == obj_type) ? "routine_id" :
                           (ObObjectType::INDEX == obj_type 
                           || ObObjectType::TABLE == obj_type
                           || ObObjectType::VIEW == obj_type) ? "table_id" :
                           NULL;
    const char *field_obj_name = (ObObjectType::PACKAGE_BODY == obj_type
                           || ObObjectType::PACKAGE == obj_type) ? "package_name" :
                           (ObObjectType::TYPE == obj_type 
                           || ObObjectType::TYPE_BODY == obj_type) ? "type_name" :
                           (ObObjectType::PROCEDURE == obj_type 
                           || ObObjectType::FUNCTION == obj_type) ? "routine_name" :
                           (ObObjectType::INDEX == obj_type 
                           || ObObjectType::TABLE == obj_type
                           || ObObjectType::VIEW == obj_type) ? "table_name" :
                           NULL;
    if (OB_NOT_NULL(tbl_name) && OB_NOT_NULL(field_name) && OB_NOT_NULL(field_obj_name)) {
      BUILD_OBJ_QUERY_SQL(tbl_name, field_name, get_ref_obj_id());
      if (OB_SUCC(ret)) {
        SMART_VAR(ObMySQLProxy::MySQLResult, res) {
          if (OB_FAIL(sql_client.read(res, exec_tenant_id, sql.ptr()))) {
            LOG_WARN("execute query failed", K(ret), K(sql));
          } else {
            sqlclient::ObMySQLResult *result = res.get_result();
            if (NULL != result) {
              ObString tmp_ref_name;
              common::ObTimeZoneInfo *tz_info = nullptr;
              OZ (result->next());
              OZ (result->get_timestamp("gmt_create", tz_info, create_time));
              OZ (result->get_varchar(field_obj_name, tmp_ref_name));
              OZ (deep_copy_str(tmp_ref_name, ref_obj_name));
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

int ObDependencyInfo::delete_schema_object_dependency(common::ObISQLClient &trans,
                                                      uint64_t tenant_id,
                                                      uint64_t dep_obj_id,
                                                      int64_t schema_version,
                                                      ObObjectType dep_obj_type)
{
  UNUSED(schema_version);
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t extract_tid = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_INVALID_ID == tenant_id
    || OB_INVALID_ID == dep_obj_id
    || ObObjectType::MAX_TYPE == dep_obj_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete error info unexpected.", K(ret), K(tenant_id),
                                              K(dep_obj_id), K(dep_obj_type));
  } else if (sql.assign_fmt("delete FROM %s WHERE dep_obj_id = %ld \
                                                  AND tenant_id = %ld  \
                                                  AND dep_obj_type = %ld",
            OB_ALL_TENANT_DEPENDENCY_TNAME,
            extract_obj_id(tenant_id, dep_obj_id),
            extract_tid,
            static_cast<uint64_t>(dep_obj_type))) {
    LOG_WARN("delete from __all_tenant_dependency table failed.", K(ret), K(tenant_id),
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
  return ret;
}

int ObDependencyInfo::insert_schema_object_dependency(common::ObISQLClient &trans,
                                                      bool is_replace, bool only_history)
{
  int ret = OB_SUCCESS;
  ObDependencyInfo& dep_info = *this;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(dep_info.get_tenant_id());
  ObDMLSqlSplicer dml;
  //这块暂时注释掉，因为系统租户下的__all_package的虚拟表没有实现。
  //int64_t ref_obj_create_time = -1;
  //ObString ref_obj_name;
  // OZ (get_object_create_time(trans, dep_info.get_ref_obj_type(),
  // ref_obj_create_time, ref_obj_name));
  // OX (dep_info.set_ref_timestamp(ref_obj_create_time));
  // OZ (dep_info.set_ref_obj_name(ref_obj_name));
  if (OB_FAIL(ret)) {
    LOG_WARN("get ref object time failed", K(ret),
                                          K(dep_info.get_ref_obj_type()),
                                          K(dep_info.get_ref_obj_id()));
  } else if (get_dep_obj_id() == get_ref_obj_id()) {
    // do nothing. object may depend on self
  } else if (OB_FAIL(gen_dependency_dml(exec_tenant_id, dml))) {
    LOG_WARN("gen table dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(trans, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      ObDMLExecHelper exec(trans, exec_tenant_id);
      if (is_replace) {
        if (OB_FAIL(exec.exec_insert_update(OB_ALL_TENANT_DEPENDENCY_TNAME, dml, affected_rows))) {
          LOG_WARN("execute update failed", K(ret));
        }
      } else {
        if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_DEPENDENCY_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && !is_single_row(affected_rows) && !is_double_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
  }
  return ret;
}

int ObDependencyInfo::collect_dep_infos(const ObIArray<ObSchemaObjVersion> &schema_objs,
                               ObIArray<ObDependencyInfo> &deps,
                               ObObjectType dep_obj_type,
                               uint64_t property,
                               ObString &dep_attrs,
                               ObString &dep_reason,
                               bool is_pl)
{
  int ret = OB_SUCCESS;
  int64_t order = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < schema_objs.count(); ++i) {
    // if (ObObjectType::TRIGGER == dep_obj_type), the schema_objs.at(0) is the trigger itself, need to skip.
    if (!(ObObjectType::TRIGGER == dep_obj_type && 0 == i)) {
      ObDependencyInfo dep;
      const ObSchemaObjVersion &s_objs = schema_objs.at(i);
      if (!s_objs.is_valid()) {
        // only collect valid dependency
        continue;
      }
      dep.set_dep_obj_id(OB_INVALID_ID);
      dep.set_dep_obj_type(dep_obj_type);
      dep.set_dep_obj_owner_id(OB_INVALID_ID);
      dep.set_ref_obj_id(s_objs.get_object_id());
      dep.set_ref_obj_type(ObSchemaObjVersion::get_schema_object_type(s_objs.object_type_));
      dep.set_order(order);
      ++order;
      dep.set_dep_timestamp(-1);
      dep.set_ref_timestamp(s_objs.get_version());
      dep.set_property(property);
      if (dep_attrs.length() >= OB_MAX_ORACLE_RAW_SQL_COL_LENGTH
      || dep_reason.length() >= OB_MAX_ORACLE_RAW_SQL_COL_LENGTH) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dep attrs or dep reason is too long", K(ret),
                                                        K(dep_attrs.length()),
                                                        K(dep_reason.length()));
      } else {
        if (!dep_attrs.empty()) OZ (dep.set_dep_attrs(dep_attrs));
        if (!dep_reason.empty()) OZ (dep.set_dep_reason(dep_reason));
      }
      OZ (deps.push_back(dep));
    }
  }
  return ret;
}

int ObDependencyInfo::collect_dep_infos(ObReferenceObjTable &ref_objs,
                                        ObIArray<ObDependencyInfo> &deps,
                                        ObObjectType dep_obj_type,
                                        uint64_t dep_obj_id,
                                        int64_t &max_version)
{
  int ret = OB_SUCCESS;
  int64_t order = 0;
  max_version = OB_INVALID_VERSION;
  auto &ref_obj_map = ref_objs.get_ref_obj_table();
  for (auto it = ref_obj_map.begin(); OB_SUCC(ret) && it != ref_obj_map.end(); ++it) {
    ObDependencyInfo dep;
    uint64_t curr_dep_obj_id = it->first.dep_obj_id_;
    // create view path, only record directly dependency
    if (curr_dep_obj_id == dep_obj_id) {
      for (int64_t i = 0; OB_SUCC(ret) && i < it->second->ref_obj_versions_.count(); ++i) {
        ObDependencyInfo dep;
        max_version = std::max(it->second->ref_obj_versions_.at(i).version_, max_version);
        dep.set_dep_obj_id(OB_INVALID_ID);
        dep.set_dep_obj_type(it->first.dep_obj_type_);
        dep.set_dep_obj_owner_id(it->first.dep_db_id_);
        dep.set_ref_obj_id(it->second->ref_obj_versions_.at(i).object_id_);
        dep.set_ref_obj_type(ObSchemaObjVersion::get_schema_object_type(it->second->ref_obj_versions_.at(i).object_type_));
        dep.set_order(order);
        ++order;
        dep.set_dep_timestamp(-1);
        dep.set_ref_timestamp(it->second->ref_obj_versions_.at(i).version_);
        OZ (deps.push_back(dep));
      }
    }
  }

  return ret;
}

int ObDependencyInfo::collect_all_dep_objs(uint64_t tenant_id,
                                           uint64_t ref_obj_id,
                                           common::ObISQLClient &sql_proxy,
                                           common::ObIArray<std::pair<uint64_t, share::schema::ObObjectType>> &objs)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  const int64_t init_count = objs.count();
  {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT dep_obj_id, dep_obj_type FROM %s WHERE tenant_id = %lu AND ref_obj_id = %lu",
                                        OB_ALL_TENANT_DEPENDENCY_TNAME,
                                        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                        ref_obj_id))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        while (OB_SUCC(result->next())) {
          int64_t tmp_obj_id = OB_INVALID_ID;
          int64_t tmp_type = static_cast<int64_t> (share::schema::ObObjectType::INVALID);
          EXTRACT_INT_FIELD_MYSQL(*result, "dep_obj_id", tmp_obj_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "dep_obj_type", tmp_type, int64_t);
          if (OB_FAIL(ret)) {
          } else if (tmp_type <= static_cast<int64_t> (share::schema::ObObjectType::INVALID)
                      || tmp_type >= static_cast<int64_t> (share::schema::ObObjectType::MAX_TYPE)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get wrong obj type", K(ret));
          } else if (ref_obj_id == tmp_obj_id) {
            // skip
          } else if (has_exist_in_array(objs, {static_cast<uint64_t> (tmp_obj_id), static_cast<share::schema::ObObjectType> (tmp_type)})) {
            // dedpulicate
          } else if (OB_FAIL(objs.push_back({static_cast<uint64_t> (tmp_obj_id), static_cast<share::schema::ObObjectType> (tmp_type)}))) {
            LOG_WARN("failed to push back obj", K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("read dependency info failed", K(ret));
        }
      }
    }
  }
  bool is_overflow = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recusive", K(ret));
  } else {
    for (int64_t i = init_count; OB_SUCC(ret) && i < objs.count(); ++i) {
      if (OB_FAIL(collect_all_dep_objs(tenant_id, objs.at(i).first, sql_proxy, objs))) {
        LOG_WARN("failed to collect all dep objs", K(ret), K(objs.count()), K(init_count), K(i));
      }
    }
  }
  return ret;
}

int ObDependencyInfo::modify_dep_obj_status(common::ObMySQLTransaction &trans,
                                            uint64_t tenant_id,
                                            uint64_t obj_id,
                                            rootserver::ObDDLOperator &ddl_operator,
                                            share::schema::ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_1_0_0) {
    // do nothing
  } else if (OB_FAIL(cascading_modify_obj_status(trans, tenant_id, obj_id,
                                                 ddl_operator,
                                                 schema_service))) {
    LOG_WARN("failed to modify obj status", K(ret));
  }
  return ret;
}

int ObDependencyInfo::cascading_modify_obj_status(common::ObMySQLTransaction &trans,
                                                  uint64_t tenant_id,
                                                  uint64_t obj_id,
                                                  rootserver::ObDDLOperator &ddl_operator,
                                                  share::schema::ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  ObArray<std::pair<uint64_t, share::schema::ObObjectType>> objs;
  if (OB_FAIL(collect_all_dep_objs(tenant_id, obj_id, trans, objs))) {
    LOG_WARN("failed to collect all objs", K(ret));
  } else if (OB_FAIL(modify_all_obj_status(objs, trans, tenant_id, ddl_operator, schema_service))) {
    LOG_WARN("failed to modify obj status", K(ret));
  }
  return ret;
}

int ObDependencyInfo::modify_all_obj_status(const ObIArray<std::pair<uint64_t, share::schema::ObObjectType>> &objs,
                                            common::ObMySQLTransaction &trans,
                                            uint64_t tenant_id,
                                            rootserver::ObDDLOperator &ddl_operator,
                                            share::schema::ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  const bool update_object_status_ignore_version = false;
  if (OB_ISNULL(schema_service.get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < objs.count(); ++i) {
    if (OB_INVALID_ID == objs.at(i).first) {
      // skipped by ddl
      continue;
    }
    if (OB_SUCC(ret)) {
      ObRefreshSchemaStatus schema_status;
      schema_status.tenant_id_ = tenant_id;
      ObObjectStatus new_status = ObObjectStatus::INVALID;
      int64_t refresh_schema_version = OB_INVALID_SCHEMA_VERSION;
      if (share::schema::ObObjectType::VIEW == objs.at(i).second) {
        HEAP_VAR(ObTableSchema, view_schema) {
          if (OB_FAIL(schema_service.get_schema_service()->get_table_schema_from_inner_table(schema_status, objs.at(i).first, trans, view_schema))) {
            LOG_WARN("failed to get view schema", K(ret));
          } else if (!view_schema.is_view_table()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get wrong schema", K(ret), K(view_schema));
          } else if (OB_FAIL(schema_service.gen_new_schema_version(tenant_id, refresh_schema_version))) {
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else if (OB_FAIL(ddl_operator.update_table_status(view_schema, refresh_schema_version,
                                                              new_status, update_object_status_ignore_version,
                                                              trans))) {
            LOG_WARN("failed to update table status", K(ret));
          }
        }
      } else if (share::schema::ObObjectType::SYNONYM == objs.at(i).second) {
        // TODO:peihan.dph
      }
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

int64_t ObReferenceObjTable::ObDependencyObjKey::hash() const
{
  int64_t hash_val = 0;
  hash_val = murmurhash(&dep_obj_id_, sizeof(int64_t), hash_val);
  hash_val = murmurhash(&dep_db_id_, sizeof(int64_t), hash_val);
  hash_val = murmurhash(&dep_obj_type_, sizeof(ObObjectType), hash_val);
  return hash_val;
}

ObReferenceObjTable::ObDependencyObjKey &ObReferenceObjTable::ObDependencyObjKey::operator=(
    const ObDependencyObjKey &other)
{
  if (this != &other) {
    dep_obj_id_ = other.dep_obj_id_;
    dep_db_id_ = other.dep_db_id_;
    dep_obj_type_ = other.dep_obj_type_;
  }
  return *this;
}

int ObReferenceObjTable::ObDependencyObjKey::assign(const ObDependencyObjKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    dep_obj_id_ = other.dep_obj_id_;
    dep_db_id_ = other.dep_db_id_;
    dep_obj_type_ = other.dep_obj_type_;
  }
  return ret;
}

bool ObReferenceObjTable::ObDependencyObjKey::operator==(
     const ObReferenceObjTable::ObDependencyObjKey &other) const
{
  return dep_obj_id_ == other.dep_obj_id_ &&
    dep_db_id_ == other.dep_db_id_ &&
    dep_obj_type_ == other.dep_obj_type_;
}

OB_SERIALIZE_MEMBER(ObReferenceObjTable::ObDependencyObjKey,
                    dep_obj_id_,
                    dep_db_id_,
                    dep_obj_type_);

ObReferenceObjTable::ObDependencyObjItem& ObReferenceObjTable::ObDependencyObjItem::operator=(
                     const ObReferenceObjTable::ObDependencyObjItem &other)
{
  if (this != &other) {
    reset();
    int &ret = error_ret_;
    ref_obj_op_ = other.ref_obj_op_;
    max_dependency_version_ = other.max_dependency_version_;
    max_ref_obj_schema_version_ = other.max_ref_obj_schema_version_;
    dep_obj_schema_version_ = other.dep_obj_schema_version_;
    if (OB_FAIL(ref_obj_versions_.assign(other.ref_obj_versions_))) {
      LOG_WARN("fail to assign array", K(ret));
    }
    error_ret_ = ret;
  }
  return *this;
}

int ObReferenceObjTable::ObDependencyObjItem::assign(
    const ObReferenceObjTable::ObDependencyObjItem &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

void ObReferenceObjTable::ObDependencyObjItem::reset()
{
  error_ret_ = OB_SUCCESS;
  ref_obj_op_ = INVALID_OP;
  max_dependency_version_ = OB_INVALID_VERSION;
  max_ref_obj_schema_version_ = OB_INVALID_VERSION;
  dep_obj_schema_version_ = OB_INVALID_VERSION;
  ref_obj_versions_.reset();
}

int ObReferenceObjTable::ObDependencyObjItem::add_ref_obj_version(const ObSchemaObjVersion &ref_obj)
{
  int ret = OB_SUCCESS;
  ObSchemaRefObjOp op = INVALID_OP;
  bool is_found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < ref_obj_versions_.count(); ++i) {
    const ObSchemaObjVersion &obj_version = ref_obj_versions_.at(i);
    if (obj_version.get_object_id() == ref_obj.get_object_id()
        && obj_version.object_type_ == ref_obj.object_type_) {
      is_found = true;
    }
  }
  if (OB_SUCC(ret) && !is_found) {
    if (INVALID_OP == ref_obj_op_) {
      if (OB_INVALID_VERSION == max_dependency_version_) {
        ref_obj_op_ = INSERT_OP;
      } else if (max_dependency_version_ < ref_obj.version_) {
        ref_obj_op_ = UPDATE_OP;
      }
    }
    if (max_ref_obj_schema_version_ < ref_obj.version_) {
      max_ref_obj_schema_version_ = ref_obj.version_;
    }
    ret = ref_obj_versions_.push_back(ref_obj);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObReferenceObjTable::ObDependencyObjItem,
                    ref_obj_op_,
                    max_dependency_version_,
                    max_ref_obj_schema_version_,
                    dep_obj_schema_version_,
                    ref_obj_versions_);

void ObReferenceObjTable::DependencyObjKeyItemPair::reset()
{
  dep_obj_key_.reset();
  dep_obj_item_.reset();
}

bool ObReferenceObjTable::DependencyObjKeyItemPair::is_valid()
{
  return dep_obj_key_.is_valid() && dep_obj_item_.is_valid();
}

ObReferenceObjTable::DependencyObjKeyItemPair& ObReferenceObjTable::DependencyObjKeyItemPair::operator=(
                          const ObReferenceObjTable::DependencyObjKeyItemPair &other)
{
  if (this != &other) {
    reset();
    dep_obj_key_ = other.dep_obj_key_;
    dep_obj_item_ = other.dep_obj_item_;
  }
  return *this;
}

int ObReferenceObjTable::DependencyObjKeyItemPair::assign(
    const ObReferenceObjTable::DependencyObjKeyItemPair &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(dep_obj_key_.assign(other.dep_obj_key_))) {
      LOG_WARN("failed to assign dep obj key", K(ret));
    } else if (OB_FAIL(dep_obj_item_.assign(other.dep_obj_item_))) {
      LOG_WARN("failed to assign dep obj item", K(ret));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObReferenceObjTable::DependencyObjKeyItemPair,
                    dep_obj_key_,
                    dep_obj_item_);

int ObReferenceObjTable::ObGetDependencyObjOp::operator()(
     hash::HashMapPair<ObDependencyObjKey, ObDependencyObjItem *> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(insert_dep_objs_) || OB_ISNULL(update_dep_objs_) || OB_ISNULL(delete_dep_objs_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(entry.second)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dependency object item is null", KP(entry.second), K(ret));
  } else if (is_sys_view(entry.first.dep_obj_id_) || is_sys_table(entry.first.dep_obj_id_)) {
    // do nothing
  } else if (OB_INVALID_ID == entry.first.dep_obj_id_) {
    // do nothing
  } else {
    ObSchemaRefObjOp op = entry.second->get_ref_obj_op();
    ObReferenceObjTable::DependencyObjKeyItemPair key_item(entry.first, *entry.second);
    switch (op) {
    case INSERT_OP:
      if (OB_FAIL(insert_dep_objs_->push_back(key_item))) {
        LOG_WARN("failed to push back key", K(ret));
      }
      break;
    case DELETE_OP:
      if (OB_FAIL(delete_dep_objs_->push_back(key_item))) {
        LOG_WARN("failed to push back key", K(ret));
      }
      break;
    case UPDATE_OP:
      if (OB_FAIL(update_dep_objs_->push_back(key_item))) {
        LOG_WARN("failed to push back key", K(ret));
      }
      break;
    default:
      break;
    }
    if (ret != OB_SUCCESS) {
      callback_ret_ = ret;
    }
  }
  return ret;
}

void ObReferenceObjTable::reset()
{
  int ret = OB_SUCCESS;
  auto free_func = [](common::hash::HashMapPair<ObDependencyObjKey, ObDependencyObjItem*> &entry) -> int {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(entry.second)) {
      entry.second->~ObDependencyObjItem();
      entry.second = nullptr;
    }
    return ret;
  };
  if (!ref_obj_version_table_.created()) {
    // do nothing
  } else if (OB_FAIL(ref_obj_version_table_.foreach_refactored(free_func))) {
    OB_LOG(WARN, "traversal ref obj version map failed", K(ret));
  }
  inited_ = false;
  ref_obj_version_table_.destroy();
}

int ObReferenceObjTable::batch_fill_kv_pairs(
    const uint64_t tenant_id,
    const ObDependencyObjKey &dep_obj_key,
    const int64_t new_schema_version,
    common::ObIArray<ObDependencyInfo> &dep_infos,
    share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0 ; OB_SUCC(ret) && i < dep_infos.count(); ++i) {
    ObDependencyInfo & dep = dep_infos.at(i);
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    dep.set_tenant_id(tenant_id);
    dep.set_dep_obj_id(dep_obj_key.dep_obj_id_);
    dep.set_dep_obj_owner_id(dep_obj_key.dep_obj_id_);
    dep.set_schema_version(new_schema_version);
    if (OB_FAIL(dep.gen_dependency_dml(exec_tenant_id, dml))) {
      LOG_WARN("gen table dml failed", K(ret));
    } else if (OB_FAIL(dml.finish_row())) {
      LOG_WARN("failed to finish row", K(ret));
    }
  }
  return ret;
}

int ObReferenceObjTable::fill_rowkey_pairs(
    const uint64_t tenant_id,
    const ObDependencyObjKey &dep_obj_key,
    share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id,
                                                                                  tenant_id)))
      || OB_FAIL(dml.add_pk_column("dep_obj_id", ObSchemaUtils::get_extract_schema_id(
                 exec_tenant_id, dep_obj_key.dep_obj_id_)))
      || OB_FAIL(dml.add_pk_column("dep_obj_type", static_cast<uint64_t>(
                 dep_obj_key.dep_obj_type_)))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(dml.finish_row())) {
    LOG_WARN("failed to finish row", K(ret));
  }
  return ret;
}

int ObReferenceObjTable::batch_execute_insert_or_update_obj_dependency(
    const uint64_t tenant_id,
    const bool is_standby,
    const int64_t new_schema_version,
    const ObReferenceObjTable::DependencyObjKeyItemPairs &dep_objs,
    ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    rootserver::ObDDLOperator &ddl_operator)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (is_standby) {
    // do nothing
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml;
    int64_t affected_rows = 0;
    for (int64_t i = 0 ; OB_SUCC(ret) && i < dep_objs.count(); ++i) {
      ObSArray<ObDependencyInfo> dep_infos;
      ObString dummy;
      const ObDependencyObjKey &dep_obj_key = dep_objs.at(i).dep_obj_key_;
      const ObDependencyObjItem &dep_obj_item = dep_objs.at(i).dep_obj_item_;
      if (!dep_obj_key.is_valid()
          || OB_INVALID_SCHEMA_VERSION == dep_obj_item.max_ref_obj_schema_version_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("illegal schema version or dependency obj key", K(ret), K(dep_obj_key),
        K(dep_obj_item.max_ref_obj_schema_version_));
      } else if (OB_FAIL(ObDependencyInfo::collect_dep_infos(
                  dep_obj_item.get_ref_obj_versions(),
                  dep_infos,
                  dep_obj_key.dep_obj_type_,
                  0, dummy, dummy, false/* is_pl */))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to collect dependency infos", K(ret));
      } else if (OB_FAIL(batch_fill_kv_pairs(tenant_id, dep_obj_key,
                 new_schema_version, dep_infos, dml))) {
        LOG_WARN("failed to batch fill kv pairs", K(ret), K(dep_obj_key));
      } else if (OB_FAIL(update_max_dependency_version(tenant_id,
                 dep_obj_key.dep_obj_id_, dep_obj_item.max_ref_obj_schema_version_,
                 trans, schema_guard, ddl_operator))) {
        LOG_WARN("failed to update max dependency version", K(ret), K(dep_obj_key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dml.splice_batch_insert_update_sql(OB_ALL_TENANT_DEPENDENCY_TNAME, sql))) {
      LOG_WARN("splice sql failed", K(ret));
    } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
      LOG_DEBUG("execute sql dml succ", K(sql));
    }
  }
  return ret;
}

int ObReferenceObjTable::batch_execute_delete_obj_dependency(
    const uint64_t tenant_id,
    const bool is_standby,
    const ObReferenceObjTable::DependencyObjKeyItemPairs &dep_objs,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (is_standby) {
    // do nothing
  } else {
    share::ObDMLSqlSplicer dml;
    ObSqlString sql;
    int64_t affected_rows = 0;
    for (int64_t i = 0 ; OB_SUCC(ret) && i < dep_objs.count(); ++i) {
      ObSArray<ObDependencyInfo> dep_infos;
      const ObDependencyObjKey &dep_obj_key = dep_objs.at(i).dep_obj_key_;
      if (!dep_obj_key.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("illegal schema version or dependency obj key", K(ret), K(dep_obj_key));
      } else if (OB_FAIL(fill_rowkey_pairs(tenant_id, dep_obj_key, dml))) {
        LOG_WARN("failed to fill rowkey pairs", K(ret), K(dep_obj_key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dml.splice_batch_delete_sql(OB_ALL_TENANT_DEPENDENCY_TNAME, sql))) {
      LOG_WARN("splice sql failed", K(ret));
    } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
      LOG_DEBUG("execute sql dml succ", K(sql));
    }
  }
  return ret;
}

int ObReferenceObjTable::update_max_dependency_version(
    const uint64_t tenant_id,
    const int64_t dep_obj_id,
    const int64_t max_dependency_version,
    ObMySQLTransaction &trans,
    ObSchemaGetterGuard &schema_guard,
    rootserver::ObDDLOperator &ddl_operator)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  ObTableSchema new_table_schema;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, dep_obj_id, table_schema))) {
    LOG_WARN("get_table_schema failed", K(tenant_id), "table id", dep_obj_id, KR(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be null", KR(ret));
  } else if (OB_FAIL(new_table_schema.assign(*table_schema))) {
    LOG_WARN("fail to assign schema", K(ret));
  } else {
    new_table_schema.set_max_dependency_version(max_dependency_version);
    ObSchemaOperationType operation_type = OB_DDL_ALTER_TABLE;
    if (OB_FAIL(ddl_operator.update_table_attribute(new_table_schema,
                                                    trans,
                                                    operation_type))) {
      LOG_WARN("failed to update data table schema attribute", K(ret));
    }
  }
  return ret;
}

int ObReferenceObjTable::get_or_add_def_obj_item(const uint64_t dep_obj_id,
                                                 const uint64_t dep_db_id,
                                                 const ObObjectType dep_obj_type,
                                                 ObDependencyObjItem *&dep_obj_item,
                                                 common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  dep_obj_item = nullptr;
  if (!inited_) {
    if (OB_FAIL(ref_obj_version_table_.create(32, "HashBucRefObj"))) {
      LOG_WARN("failed to add create ref obj tbl", K(ret));
    } else {
      inited_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = nullptr;
    ObDependencyObjKey ref_obj_key(dep_obj_id, dep_db_id, dep_obj_type);
    if (OB_FAIL(ref_obj_version_table_.get_refactored(ref_obj_key, dep_obj_item))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        CK (OB_NOT_NULL(buf = static_cast<char *>(allocator.alloc(sizeof(ObDependencyObjItem)))));
        OX (dep_obj_item = new(buf) ObDependencyObjItem);
        OZ (ref_obj_version_table_.set_refactored(ref_obj_key, dep_obj_item));
      } else {
        LOG_WARN("failed to get dep obj item", K(ret));
      }
    }
  }
  return ret;
}

int ObReferenceObjTable::add_ref_obj_version(const uint64_t dep_obj_id,
                                             const uint64_t dep_db_id,
                                             const ObObjectType dep_obj_type,
                                             const ObSchemaObjVersion &ref_obj_version,
                                             common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDependencyObjItem *dep_obj_item = nullptr;
  OZ (get_or_add_def_obj_item(dep_obj_id, dep_db_id, dep_obj_type, dep_obj_item, allocator));
  CK (OB_NOT_NULL(dep_obj_item));
  OZ (dep_obj_item->add_ref_obj_version(ref_obj_version));
  return ret;
}

int ObReferenceObjTable::get_dep_obj_item(const uint64_t dep_obj_id,
                                          const uint64_t dep_db_id,
                                          const ObObjectType dep_obj_type,
                                          ObDependencyObjItem *&dep_obj_item)
{
  int ret = OB_SUCCESS;
  ObDependencyObjKey dep_obj_key(dep_obj_id, dep_db_id, dep_obj_type);
  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref_obj_version_table_ not inited", K(ret));
  } else if (OB_FAIL(ref_obj_version_table_.get_refactored(dep_obj_key, dep_obj_item))) {
    LOG_WARN("failed to get ref obj item", K(ret));
  }
  return ret;
}

int ObReferenceObjTable::set_obj_schema_version(const uint64_t dep_obj_id,
                                                const uint64_t dep_db_id,
                                                const ObObjectType dep_obj_type,
                                                const int64_t max_dependency_version,
                                                const int64_t dep_obj_schema_version,
                                                common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDependencyObjItem *dep_obj_item = nullptr;
  OZ (get_or_add_def_obj_item(dep_obj_id, dep_db_id, dep_obj_type, dep_obj_item, allocator));
  CK (OB_NOT_NULL(dep_obj_item));
  OX (dep_obj_item->set_max_dependency_version(max_dependency_version));
  OX (dep_obj_item->set_dep_obj_schema_version(dep_obj_schema_version));
  return ret;
}

int ObReferenceObjTable::set_ref_obj_op(const uint64_t dep_obj_id,
                                        const uint64_t dep_db_id,
                                        const ObObjectType dep_obj_type,
                                        const ObSchemaRefObjOp ref_obj_op,
                                        common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDependencyObjItem *dep_obj_item = nullptr;
  OZ (get_or_add_def_obj_item(dep_obj_id, dep_db_id, dep_obj_type, dep_obj_item, allocator));
  CK (OB_NOT_NULL(dep_obj_item));
  OX (dep_obj_item->set_ref_obj_op(ref_obj_op));
  return ret;
}

int ObReferenceObjTable::process_reference_obj_table(const uint64_t tenant_id,
                                                     const uint64_t dep_obj_id,
                                                     const ObTableSchema *view_schema,
                                                     sql::ObMaintainDepInfoTaskQueue &task_queue)
{
  int ret = OB_SUCCESS;
  if (!is_inited() || GCTX.is_standby_cluster()) {
    if (OB_INVALID_ID != dep_obj_id) {
      OZ (task_queue.erase_view_id_from_set(dep_obj_id));
    }
  } else {
    SMART_VAR(sql::ObMaintainObjDepInfoTask, task, tenant_id) {
      ObGetDependencyObjOp op(&task.get_insert_dep_objs(),
                              &task.get_update_dep_objs(),
                              &task.get_delete_dep_objs());
      if (OB_FAIL(ref_obj_version_table_.foreach_refactored(op))) {
        LOG_WARN("traverse ref_obj_version_table_ failed", K(ret));
      } else if (nullptr != view_schema && OB_FAIL(task.assign_view_schema(*view_schema))) {
        LOG_WARN("failed to assign view schema", K(ret));
      } else if (OB_FAIL(op.get_callback_ret())) {
        LOG_WARN("traverse ref_obj_version_table_ failed", K(ret));
      } else if (task.is_empty_task()) {
        if (OB_INVALID_ID != dep_obj_id) {
          OZ (task_queue.erase_view_id_from_set(dep_obj_id));
        }
      } else if (task_queue.is_queue_almost_full()) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_FAIL(task_queue.push(task))) {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
          LOG_WARN("push task failed", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret) && OB_INVALID_ID != dep_obj_id) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = task_queue.erase_view_id_from_set(dep_obj_id))) {
      LOG_WARN("failed to erase obj id", K(tmp_ret), K(ret));
    }
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("async queue is full");
    }
  }
  return ret;
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
