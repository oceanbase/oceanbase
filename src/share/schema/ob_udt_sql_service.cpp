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
#include "ob_udt_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "ob_udt_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "pl/ob_pl_stmt.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObUDTSqlService::create_udt(ObUDTTypeInfo &udt_info,
                                ObISQLClient *sql_client,
                                const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  bool not_create_object_body = ((udt_info.is_object_type() && udt_info.is_object_spec_ddl()) 
                               || !udt_info.is_object_type());
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_client is NULL, ", K(ret));
  } else if (!udt_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "udt_info is invalid", K(udt_info), K(ret));
  } else {
    if (not_create_object_body) {
      if (OB_FAIL(add_udt(*sql_client, udt_info))) {
        LOG_WARN("add udt failed", K(ret));
      } else if (OB_FAIL(add_udt_attrs(*sql_client, udt_info))) {
        LOG_WARN("add udt params failed", K(ret));
      } else if (OB_FAIL(add_udt_object(*sql_client, udt_info))) {
        LOG_WARN("add udt object spec failed.", K(ret));
      }
      // OZ (add_udt_object(*sql_client, udt_info));
    } else {
      if (OB_FAIL(add_udt_object(*sql_client, udt_info, false, true, false))) {
        LOG_WARN("failed to add udt object.", K(ret));
      } else if (OB_FAIL(update_type_schema_version(*sql_client, udt_info, ddl_stmt_str))) {
        LOG_WARN("failed to update __all_type schema version", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObSchemaOperation opt;
      opt.tenant_id_ = udt_info.get_tenant_id();
      opt.database_id_ = udt_info.get_database_id();
      opt.table_id_ = udt_info.get_type_id();
      opt.op_type_ = OB_DDL_CREATE_UDT;
      opt.schema_version_ = udt_info.get_schema_version();
      opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
      if (OB_FAIL(log_operation(opt, *sql_client))) {
        LOG_WARN("Failed to log operation", K(ret));
      }
    }
  }
  return ret;
}

int ObUDTSqlService::replace_udt(ObUDTTypeInfo &udt_info,
                                 const ObUDTTypeInfo *old_udt_info,
                                 const int64_t del_param_schema_version,
                                 ObISQLClient *sql_client,
                                 const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(sql_client));
  CK(OB_NOT_NULL(old_udt_info));
  if (OB_SUCC(ret)) {
    bool not_create_object_body = ((udt_info.is_object_type() && udt_info.is_object_spec_ddl()) 
                               || !udt_info.is_object_type());
    LOG_WARN("old_udt_info", K(*old_udt_info), K(old_udt_info->get_attrs().count()));
    if (!udt_info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new udt info is invalid", K(udt_info), K(ret));
    } else {
      if (not_create_object_body) {
        if (OB_FAIL(add_udt(*sql_client, udt_info, true))) {
          LOG_WARN("add udt failed", K(udt_info), K(ret));
        } else if ((old_udt_info->get_attributes() > 0 || old_udt_info->is_collection())
            && OB_FAIL(del_udt_attrs(*sql_client, *old_udt_info, del_param_schema_version))) {
          LOG_WARN("del udt params failed", K(udt_info), K(ret));
        } else if ((udt_info.get_attributes() > 0 || udt_info.is_collection())
                  && OB_FAIL(add_udt_attrs(*sql_client, udt_info))) {
          LOG_WARN("add udt params failed", K(udt_info), K(ret));
        } else if (OB_FAIL(add_udt_object(
          *sql_client, udt_info,
          (old_udt_info->get_object_type_infos().count() > 0), false, false))) {
          LOG_WARN("failed to add udt object", K(ret));
        }
      } else {
        OZ (add_udt_object(*sql_client, udt_info, true, true, false));
        OZ (update_type_schema_version(*sql_client, udt_info, ddl_stmt_str));
      }
      if (OB_SUCC(ret)) {
        ObSchemaOperation opt;
        opt.tenant_id_ = udt_info.get_tenant_id();
        opt.database_id_ = udt_info.get_database_id();
        opt.table_id_ = udt_info.get_type_id();
        opt.op_type_ = OB_DDL_REPLACE_UDT;
        opt.schema_version_ = udt_info.get_schema_version();
        opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
        if (OB_FAIL(log_operation(opt, *sql_client))) {
          LOG_WARN("Failed to log operation", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUDTSqlService::drop_udt(const ObUDTTypeInfo &udt_info,
                              const int64_t new_schema_version,
                              ObISQLClient &sql_client,
                              const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = udt_info.get_tenant_id();
  uint64_t db_id = udt_info.get_database_id();
  uint64_t udt_id = udt_info.get_type_id();
  bool is_object_body = udt_info.is_object_body_ddl();
  bool is_object_type = udt_info.is_object_type();
  ObUDTTypeInfo udt = udt_info;
  udt.set_schema_version(new_schema_version);
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)
      || OB_UNLIKELY(OB_INVALID_ID == db_id)
      || OB_UNLIKELY(OB_INVALID_ID == udt_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid udt info in drop udt", K(tenant_id), K(db_id), K(udt_id));
  } else if (!is_object_body && OB_FAIL(del_udt(sql_client, udt_info, new_schema_version))) {
    LOG_WARN("delete from __all_type failed", K(ret), K(udt_info));
  } else {
    if (!is_object_type) {
      // do nothing
    } else {
      if (!is_object_body) {
       if ((udt_info.get_attributes() > 0 || udt_info.is_collection())
           && OB_FAIL(del_udt_attrs(sql_client, udt_info, new_schema_version))) {
          LOG_WARN("failed to del udt attrs", K(ret), K(udt_info));
        } else if (OB_FAIL(del_udt_objects_in_udt(sql_client, udt_info, new_schema_version))) {
          LOG_WARN("failed to del udt object spec", K(ret), K(udt_info));
        } 
      } else {
       if (OB_FAIL(del_udt_objects_in_udt(sql_client, udt_info, new_schema_version))) {
          LOG_WARN("failed to del udt object spec", K(ret), K(udt_info));
        } else if (OB_FAIL(update_type_schema_version(sql_client, udt, ddl_stmt_str))) {
          LOG_WARN("failed to update type schema", K(ret), K(udt_info));
        } 
      }
    }
    ObSchemaOperation opt;
    opt.tenant_id_ = tenant_id;
    opt.database_id_ = db_id;
    opt.table_id_ = udt_id;
    opt.op_type_ = is_object_body ? OB_DDL_DROP_UDT_BODY : OB_DDL_DROP_UDT;
    opt.schema_version_ = new_schema_version;
    opt.ddl_stmt_str_ = (NULL != ddl_stmt_str) ? *ddl_stmt_str : ObString();
    if (OB_FAIL(log_operation(opt, sql_client))) {
      LOG_WARN("Failed to log operation", K(ret));
    }
  }
  return ret;
}

int ObUDTSqlService::add_udt(ObISQLClient &sql_client,
                             const ObUDTTypeInfo &udt_info,
                             bool is_replace,
                             bool only_history)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(gen_udt_dml(exec_tenant_id, udt_info, dml))) {
    LOG_WARN("gen table dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      if (is_replace) {
        if (OB_FAIL(exec.exec_update(OB_ALL_TYPE_TNAME, dml, affected_rows))) {
          LOG_WARN("execute update failed", K(ret));
        }
      } else {
        if (OB_FAIL(exec.exec_insert(OB_ALL_TYPE_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && !is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t is_deleted = 0;
      if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_TYPE_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
  }
  return ret;
}

int ObUDTSqlService::add_udt_coll(ObISQLClient &sql_client,
                                  const ObUDTTypeInfo &udt_info,
                                  bool only_history)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  ObUDTCollectionType coll_type;
  if (OB_ISNULL(udt_info.get_coll_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("coll info is null", K(ret));
  } else {
    coll_type = *(udt_info.get_coll_info());
    coll_type.set_coll_type_id(udt_info.get_type_id());
    coll_type.set_schema_version(udt_info.get_schema_version());
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gen_udt_coll_dml(exec_tenant_id, coll_type, dml))) {
    LOG_WARN("gen table dml failed", K(ret));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (!only_history) {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      if (OB_FAIL(exec.exec_insert(OB_ALL_COLL_TYPE_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t is_deleted = 0;
      if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_insert(OB_ALL_COLL_TYPE_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
  }
  return ret;
}

int ObUDTSqlService::add_udt_attrs(ObISQLClient &sql_client,
                                   const ObUDTTypeInfo &udt_info,
                                   bool only_history)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (udt_info.is_collection()) {
    if (OB_FAIL(add_udt_coll(sql_client, udt_info, only_history))) {
      LOG_WARN("failed to add udt coll", K(ret));
    }
  } else {
    const ObIArray<ObUDTTypeAttr *> &udt_attrs = udt_info.get_attrs();
    for (int64_t i = 0; OB_SUCC(ret) && i < udt_attrs.count(); ++i) {
      ObUDTTypeAttr *udt_attr = udt_attrs.at(i);
      if (OB_ISNULL(udt_attr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("udt attr is null", K(i));
      } else {
        udt_attr->set_type_id(udt_info.get_type_id());
        udt_attr->set_schema_version(udt_info.get_schema_version());
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(gen_udt_attr_dml(exec_tenant_id, *udt_attr, dml))) {
        LOG_WARN("gen udt param dml failed", K(ret));
      } else {
        ObDMLExecHelper exec(sql_client, exec_tenant_id);
        int64_t affected_rows = 0;
        if (!only_history) {
          ObDMLExecHelper exec(sql_client, exec_tenant_id);
          if (OB_FAIL(exec.exec_insert(OB_ALL_TYPE_ATTR_TNAME, dml, affected_rows))) {
            LOG_WARN("execute insert failed", K(ret));
          } else if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          const int64_t is_deleted = 0;
          if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
            LOG_WARN("add column failed", K(ret));
          } else if (OB_FAIL(exec.exec_insert(OB_ALL_TYPE_ATTR_HISTORY_TNAME, dml, affected_rows))) {
            LOG_WARN("execute insert failed", K(ret));
          } else if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
          }
        }
      }
      dml.reset();
    }
  }
  return ret;
}

int ObUDTSqlService::add_udt_object(ObISQLClient &sql_client,
                                   const ObUDTTypeInfo &udt_info,
                                   bool is_replace,
                                   bool body_only,
                                   bool only_history)
{
  int ret = OB_SUCCESS;
  if (udt_info.is_object_type()) {
    const uint64_t tenant_id = udt_info.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObUDTObjectType *obj_info = NULL;
    int64_t i = body_only ? 1 : 0;
    for (; OB_SUCC(ret) && i < udt_info.get_object_type_infos().count(); ++i) {
      ObDMLSqlSplicer dml;
      if (OB_ISNULL(obj_info = udt_info.get_object_type_infos().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("udt object type is null", K(ret));
      } else {
        obj_info->set_object_type_id(udt_info.get_type_id());
        obj_info->set_schema_version(udt_info.get_schema_version());
        obj_info->set_object_name(udt_info.get_type_name());
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(gen_udt_object_dml(exec_tenant_id, *obj_info, dml))) {
        LOG_WARN("gen table dml failed", K(ret));
      } else {
        ObDMLExecHelper exec(sql_client, exec_tenant_id);
        int64_t affected_rows = 0;
        if (!only_history) {
          // ObDMLExecHelper exec(sql_client, exec_tenant_id);
          if (is_replace) {
            if (OB_FAIL(exec.exec_update(OB_ALL_TENANT_OBJECT_TYPE_TNAME, dml, affected_rows))) {
              LOG_WARN("execute update failed", K(ret));
            }
          } else if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_OBJECT_TYPE_TNAME,
                                             dml, affected_rows))) {
            LOG_WARN("execute insert failed", K(ret));
          } else if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          const int64_t is_deleted = 0;
          if (OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
            LOG_WARN("add column failed", K(ret));
          } else if (OB_FAIL(exec.exec_insert(OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TNAME,
                                              dml, affected_rows))) {
            LOG_WARN("execute insert failed", K(ret));
          } else if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObUDTSqlService::del_udt(ObISQLClient &sql_client,
                             const ObUDTTypeInfo &udt_info,
                             int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, udt_info.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("type_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, udt_info.get_type_id())))) {
    LOG_WARN("add pk column to __all_types failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_delete(OB_ALL_TYPE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret), K(udt_info), K(new_schema_version));
    }
  }
  if (OB_SUCCESS == ret) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    // insert into __all_type_history
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(tenant_id, type_id, schema_version, is_deleted) VALUES(%lu,%lu,%ld,%d)",
        OB_ALL_TYPE_HISTORY_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, udt_info.get_tenant_id()),
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, udt_info.get_type_id()),
        new_schema_version, 1))) {
      LOG_WARN("assign insert into __all_type_history fail", K(udt_info), K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row has inserted", K(ret));
    } else { /* do nothing */ }
  }
  return ret;
}

int ObUDTSqlService::del_udt_coll(ObISQLClient &sql_client,
                                  const ObUDTTypeInfo &udt_info,
                                  int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, udt_info.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("coll_type_id", ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, udt_info.get_type_id())))) {
    LOG_WARN("add pk column to __all_coll_types failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_delete(OB_ALL_COLL_TYPE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret), K(udt_info), K(new_schema_version));
    }
  }
  if (OB_SUCCESS == ret) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(tenant_id, coll_type_id, schema_version, is_deleted) VALUES(%lu,%lu,%ld,%d)",
        OB_ALL_COLL_TYPE_HISTORY_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, udt_info.get_tenant_id()),
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, udt_info.get_type_id()),
        new_schema_version, 1))) {
      LOG_WARN("assign insert into __all_types_history fail", K(udt_info), K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row has inserted", K(ret));
    } else { /* do nothing */ }
  }
  return ret;
}

int ObUDTSqlService::del_udt_attrs(ObISQLClient &sql_client,
                                   const ObUDTTypeInfo &udt_info,
                                   int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (udt_info.is_collection()) {
    if (OB_FAIL(del_udt_coll(sql_client, udt_info, new_schema_version))) {
      LOG_WARN("failed to del udt coll", K(ret));
    }
  } else {
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, udt_info.get_tenant_id())))
        || OB_FAIL(dml.add_pk_column("type_id", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, udt_info.get_type_id())))) {
      LOG_WARN("add pk column to __all_type_attrs failed", K(ret));
    } else {
      int64_t affected_rows = 0;
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      if (OB_FAIL(exec.exec_delete(OB_ALL_TYPE_ATTR_TNAME, dml, affected_rows))) {
        LOG_WARN("execute delete failed", K(ret));
      } else if (affected_rows < udt_info.get_attrs_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(udt_info),
                  K(affected_rows), K(udt_info.get_attrs_count()));
      } else { /*do nothing */ }
    }
    if (OB_SUCC(ret)) {
      ObSqlString sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.append_fmt("INSERT /*+use_plan_cache(none)*/ INTO %s "
          "(tenant_id, type_id, attribute, schema_version, is_deleted) VALUES ",
          OB_ALL_TYPE_ATTR_HISTORY_TNAME))) {
        LOG_WARN("append_fmt failed", K(ret));
      }
      const ObIArray<ObUDTTypeAttr*> &udt_attrs = udt_info.get_attrs();
      for (int64_t i = 0; OB_SUCC(ret) && i < udt_attrs.count(); ++i) {
        const ObUDTTypeAttr *udt_attr = NULL;
        if (OB_ISNULL(udt_attr = udt_attrs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("udt attr is null", K(ret));
        } else if (OB_FAIL(sql.append_fmt("%s(%lu, %lu, %lu, %lu, %d)", (0 == i) ? "" : ",",
            ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, udt_attr->get_tenant_id()),
            ObSchemaUtils::get_extract_schema_id(exec_tenant_id, udt_attr->get_type_id()),
            udt_attr->get_attribute(),
            new_schema_version, 1))) {
          LOG_WARN("append_fmt failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && 0 < udt_attrs.count()) {
        if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else if (udt_info.get_attrs_count() != affected_rows) {
          LOG_WARN("affected_rows not same with udt_attrs_count", K(affected_rows),
                   "attrs_count", udt_info.get_attrs_count(), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUDTSqlService::del_udt_objects_in_udt(common::ObISQLClient &sql_client,
                                            const ObUDTTypeInfo &udt_info,
                                            int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  bool is_object_body = udt_info.is_object_body_ddl();
  const ObIArray<ObUDTObjectType *> &obj_ti = udt_info.get_object_type_infos();
  CK (udt_info.is_object_type_legal());
  if (is_object_body) {
    CK (2 == obj_ti.count());
    OZ (del_udt_object(sql_client,
                      udt_info, 
                      obj_ti.at(1)->get_type(),
                      new_schema_version), udt_info);
  } else {
    CK (0 < obj_ti.count() && 2 >= obj_ti.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_ti.count(); ++i) {
      OZ (del_udt_object(sql_client, 
                      udt_info, 
                      obj_ti.at(i)->get_type(),
                      new_schema_version), udt_info);
    }
  }
  return ret;
}

int ObUDTSqlService::del_udt_object(ObISQLClient &sql_client,
                                    const ObUDTTypeInfo &udt_info,
                                    int64_t object_type,
                                    int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, udt_info.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("object_type_id", ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, udt_info.get_type_id())))
      || OB_FAIL(dml.add_pk_column("type", object_type))) {
    LOG_WARN("add pk column to __all_tenant_object_type failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_delete(OB_ALL_TENANT_OBJECT_TYPE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute delete failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(affected_rows), K(ret), K(udt_info),
                                   K(object_type), K(new_schema_version));
    }
  }
  if (OB_SUCC(ret) && INVALID_UDT_OBJECT_TYPE != object_type) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(tenant_id, object_type_id, type, schema_version, is_deleted) VALUES(%lu,%lu, %ld, %ld,%d)",
        OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, udt_info.get_tenant_id()),
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, udt_info.get_type_id()),
        static_cast<int64_t>(object_type),
        new_schema_version, 1))) {
      LOG_WARN("assign insert into __all_tenant_object_type_history fail", K(udt_info), K(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no row has inserted", K(ret));
    } else { /* do nothing */ }
  }
  return ret;
}

int ObUDTSqlService::update_type_schema_version(common::ObISQLClient &sql_client,
                                  const ObUDTTypeInfo &udt_info,
                                  const common::ObString *ddl_stmt_str)
{
  UNUSED(ddl_stmt_str);
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObDMLSqlSplicer dml;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, udt_info.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("type_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, udt_info.get_type_id())))
      || OB_FAIL(dml.add_column("schema_version", udt_info.get_schema_version()))
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("gen udt dml failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(exec.exec_update(OB_ALL_TYPE_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update failed", K(ret), K(udt_info));
    } else {
      if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(udt_info), K(ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, udt_info.get_type_id())), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_udt(sql_client, udt_info, false, true))) {
        LOG_WARN("execute history insert failed", K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret));
      }
    }
    // log operation
    // if (OB_SUCC(ret)) {
    //   ObSchemaOperation opt;
    //   if (NULL != ddl_stmt_str) {
    //     opt.ddl_stmt_str_ = *ddl_stmt_str;
    //   }
    //   opt.tenant_id_ = udt_info.get_tenant_id();
    //   opt.database_id_ = udt_info.get_database_id();
    //   opt.table_id_ = udt_info.get_type_id();
    //   opt.op_type_ = OB_DDL_CREATE_UDT;
    //   opt.schema_version_ = udt_info.get_schema_version();
    //   if (OB_FAIL(log_operation(opt, sql_client))) {
    //     LOG_WARN("log operation failed", K(opt), K(ret));
    //   }
    // }
  }
  return ret;
}

int ObUDTSqlService::gen_udt_dml(
    const uint64_t exec_tenant_id,
    const ObUDTTypeInfo &udt_info,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, udt_info.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("type_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, udt_info.get_type_id())))
      || OB_FAIL(dml.add_column("database_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, udt_info.get_database_id())))
      || OB_FAIL(dml.add_column("schema_version", udt_info.get_schema_version()))
      || OB_FAIL(dml.add_column("typecode", udt_info.get_typecode()))
      || OB_FAIL(dml.add_column("PROPERTIES", udt_info.get_properties()))
      || OB_FAIL(dml.add_column("ATTRIBUTES", udt_info.get_attributes()))
      || OB_FAIL(dml.add_column("METHODS", udt_info.get_methods()))
      || OB_FAIL(dml.add_column("HIDDENMETHODS", udt_info.get_hiddenmethods()))
      || OB_FAIL(dml.add_column("SUPERTYPES", udt_info.get_supertypes()))
      || OB_FAIL(dml.add_column("SUBTYPES", udt_info.get_subtypes()))
      || OB_FAIL(dml.add_column("EXTERNTYPE", udt_info.get_externtype()))
      || OB_FAIL(dml.add_column("EXTERNNAME", ObHexEscapeSqlStr(udt_info.get_externname())))
      || OB_FAIL(dml.add_column("HELPERCLASSNAME", ObHexEscapeSqlStr(udt_info.get_helperclassname())))
      || OB_FAIL(dml.add_column("LOCAL_ATTRS", udt_info.get_local_attrs()))
      || OB_FAIL(dml.add_column("LOCAL_METHODS", udt_info.get_local_methods()))
      || OB_FAIL(dml.add_column("SUPERTYPEID", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, udt_info.get_supertypeid())))
      || OB_FAIL(dml.add_column("TYPE_NAME", ObHexEscapeSqlStr(udt_info.get_type_name())))
      || OB_FAIL(dml.add_column("PACKAGE_ID", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, udt_info.get_package_id())))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObUDTSqlService::gen_udt_coll_dml(
    const uint64_t exec_tenant_id,
    const ObUDTCollectionType &coll_info,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, coll_info.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("coll_type_id", ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, coll_info.get_coll_type_id())))
      || OB_FAIL(dml.add_column("schema_version", coll_info.get_schema_version()))
      || OB_FAIL(dml.add_column("ELEM_TYPE_ID", ObSchemaUtils::get_extract_schema_id(
                                                exec_tenant_id, coll_info.get_elem_type_id())))
      || OB_FAIL(dml.add_column("ELEM_SCHEMA_VERSION", coll_info.get_elem_schema_version()))
      || OB_FAIL(dml.add_column("PROPERTIES", coll_info.get_properties()))
      || OB_FAIL(dml.add_column("CHARSET_ID", coll_info.get_charset_id()))
      || OB_FAIL(dml.add_column("CHARSET_FORM", coll_info.get_charset_form()))
      || OB_FAIL(dml.add_column("LENGTH", coll_info.get_length()))
      || OB_FAIL(dml.add_column("NUMBER_PRECISION", coll_info.get_precision()))
      || OB_FAIL(dml.add_column("SCALE", coll_info.get_scale()))
      || OB_FAIL(dml.add_column("ZERO_FILL", coll_info.get_zero_fill()))
      || OB_FAIL(dml.add_column("COLL_TYPE", coll_info.get_coll_type()))
      || OB_FAIL(dml.add_column("UPPER_BOUND", coll_info.get_upper_bound()))
      || OB_FAIL(dml.add_column("PACKAGE_ID", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, coll_info.get_package_id())))
      || OB_FAIL(dml.add_column("COLL_NAME", ObHexEscapeSqlStr(coll_info.get_coll_name())))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObUDTSqlService::gen_udt_attr_dml(
    const uint64_t exec_tenant_id,
    const ObUDTTypeAttr &udt_attr,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, udt_attr.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("type_id", ObSchemaUtils::get_extract_schema_id(
                                              exec_tenant_id, udt_attr.get_type_id())))
      || OB_FAIL(dml.add_pk_column("ATTRIBUTE", udt_attr.get_attribute()))
      || OB_FAIL(dml.add_column("schema_version", udt_attr.get_schema_version()))
      || OB_FAIL(dml.add_column("type_attr_id", ObSchemaUtils::get_extract_schema_id(
                                               exec_tenant_id, udt_attr.get_type_attr_id())))
      || OB_FAIL(dml.add_column("NAME", ObHexEscapeSqlStr(udt_attr.get_name())))
      || OB_FAIL(dml.add_column("PROPERTIES", udt_attr.get_properties()))
      || OB_FAIL(dml.add_column("CHARSET_ID", udt_attr.get_charset_id()))
      || OB_FAIL(dml.add_column("CHARSET_FORM", udt_attr.get_charset_form()))
      || OB_FAIL(dml.add_column("LENGTH", udt_attr.get_length()))
      || OB_FAIL(dml.add_column("NUMBER_PRECISION", udt_attr.get_precision()))
      || OB_FAIL(dml.add_column("SCALE", udt_attr.get_scale()))
      || OB_FAIL(dml.add_column("ZERO_FILL", udt_attr.get_zero_fill()))
      || OB_FAIL(dml.add_column("COLL_TYPE", udt_attr.get_coll_type()))
      || OB_FAIL(dml.add_column("EXTERNNAME", ObHexEscapeSqlStr(udt_attr.get_externname())))
      || OB_FAIL(dml.add_column("XFLAGS", udt_attr.get_xflags()))
      || OB_FAIL(dml.add_column("SETTER", udt_attr.get_setter()))
      || OB_FAIL(dml.add_column("GETTER", udt_attr.get_getter()))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObUDTSqlService::gen_udt_object_dml(const uint64_t exec_tenant_id,
                                        const ObUDTObjectType &udt_object,
                                        ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                             exec_tenant_id, udt_object.get_tenant_id())))
      || OB_FAIL(dml.add_pk_column("object_type_id", ObSchemaUtils::get_extract_schema_id(
                                          exec_tenant_id, udt_object.get_object_type_id())))
      || OB_FAIL(dml.add_pk_column("TYPE", udt_object.get_type()))
      || OB_FAIL(dml.add_column("schema_version", udt_object.get_schema_version()))
      || OB_FAIL(dml.add_column("PROPERTIES", udt_object.get_properties()))
      || OB_FAIL(dml.add_column("CHARSET_ID", udt_object.get_charset_id()))
      || OB_FAIL(dml.add_column("CHARSET_FORM", udt_object.get_charset_form()))
      || OB_FAIL(dml.add_column("LENGTH", udt_object.get_length()))
      || OB_FAIL(dml.add_column("NUMBER_PRECISION", udt_object.get_precision()))
      || OB_FAIL(dml.add_column("SCALE", udt_object.get_scale()))
      || OB_FAIL(dml.add_column("ZERO_FILL", udt_object.get_zero_fill()))
      || OB_FAIL(dml.add_column("COLL_TYPE", udt_object.get_coll_type()))
      || OB_FAIL(dml.add_column("DATABASE_ID", ObSchemaUtils::get_extract_schema_id(
                                         exec_tenant_id, udt_object.get_database_id())))
      || OB_FAIL(dml.add_column("FLAG", udt_object.get_flag()))
      || OB_FAIL(dml.add_column("OWNER_ID", ObSchemaUtils::get_extract_schema_id(
                                      exec_tenant_id, udt_object.get_owner_id())))
      || OB_FAIL(dml.add_column("COMP_FLAG", udt_object.get_comp_flag()))
      || OB_FAIL(dml.add_column("OBJECT_NAME", ObHexEscapeSqlStr(udt_object.get_object_name())))
      || OB_FAIL(dml.add_column("EXEC_ENV", ObHexEscapeSqlStr(udt_object.get_exec_env())))
      || OB_FAIL(dml.add_column("SOURCE", ObHexEscapeSqlStr(udt_object.get_source())))
      || OB_FAIL(dml.add_column("COMMENT", ObHexEscapeSqlStr(udt_object.get_comment())))
      || OB_FAIL(dml.add_column("ROUTE_SQL", ObHexEscapeSqlStr(udt_object.get_route_sql())))
      || OB_FAIL(dml.add_gmt_create())
      || OB_FAIL(dml.add_gmt_modified())) {
    LOG_WARN("add column failed", K(ret));
  }

  return ret;
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
