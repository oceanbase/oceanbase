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

#include "ob_pl_ddl_operator.h"
#include "share/schema/ob_routine_sql_service.h"
#include "share/schema/ob_trigger_sql_service.h"
#include "share/schema/ob_udt_sql_service.h"
#include "share/schema/ob_user_sql_service.h"
#include "share/schema/ob_security_audit_sql_service.h"
#include "share/schema/ob_table_sql_service.h"
#include "pl/ob_pl_persistent.h"
#include "pl/pl_cache/ob_pl_cache_mgr.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_package_type.h"
#endif

namespace oceanbase
{
namespace rootserver
{

ObPLDDLOperator::ObPLDDLOperator(ObMultiVersionSchemaService &schema_service,
                                 common::ObMySQLProxy &sql_proxy)
                                : ObDDLOperator(schema_service, sql_proxy)
{
}

ObPLDDLOperator::~ObPLDDLOperator()
{
}


// functions for managing routine
int ObPLDDLOperator::create_routine(share::schema::ObRoutineInfo &routine_info,
                                    common::ObMySQLTransaction &trans,
                                    share::schema::ObErrorInfo &error_info,
                                    common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                                    const common::ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  uint64_t new_routine_id = OB_INVALID_ID;
  const uint64_t tenant_id = routine_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id
          && OB_FAIL(schema_service->fetch_new_sys_pl_object_id(tenant_id, new_routine_id))) {
    LOG_WARN("failed to fetch new_routine_id", K(tenant_id), K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id
          && OB_FAIL(schema_service->fetch_new_routine_id(tenant_id, new_routine_id))) {
    LOG_WARN("failed to fetch new_routine_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    routine_info.set_routine_id(new_routine_id);
    routine_info.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_routine_sql_service().create_routine(routine_info,
                                                                         &trans,
                                                                         ddl_stmt_str))) {
      LOG_WARN("insert routine info failed", K(routine_info), K(ret));
    }
  }
  OZ (insert_dependency_infos(trans, dep_infos, tenant_id, routine_info.get_routine_id(),
                                routine_info.get_schema_version(),
                                routine_info.get_owner_id()));

  // add audit in routine if necessary
  if (OB_SUCC(ret)) {
    int64_t new_schema_version = OB_INVALID_VERSION;
    ObArray<const ObSAuditSchema *> audits;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                       AUDIT_OBJ_DEFAULT,
                                                       OB_AUDIT_MOCK_USER_ID,
                                                       audits))) {
      LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
    } else if (!audits.empty()) {
      ObSchemaService *schema_service = schema_service_.get_schema_service();
      common::ObSqlString public_sql_string;
      ObSAuditSchema new_audit_schema;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
        uint64_t new_audit_id = common::OB_INVALID_ID;
        const ObSAuditSchema *audit_schema = audits.at(i);
        if (OB_ISNULL(audit_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("audit_schema is NULL", K(ret));
        } else if (!audit_schema->is_access_operation_for_procedure()) {
          continue;
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))){
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service->fetch_new_audit_id(tenant_id, new_audit_id))) {
          LOG_WARN("Failed to fetch new_audit_id", K(ret));
        } else if (OB_FAIL(new_audit_schema.assign(*audit_schema))) {
          LOG_WARN("fail to assign audit schema", KR(ret));
        } else {
          new_audit_schema.set_schema_version(new_schema_version);
          new_audit_schema.set_audit_id(new_audit_id);
          new_audit_schema.set_audit_type(AUDIT_PROCEDURE);
          new_audit_schema.set_owner_id(routine_info.get_routine_id());
          if (OB_FAIL(schema_service->get_audit_sql_service().handle_audit_metainfo(
              new_audit_schema,
              AUDIT_MT_ADD,
              false,
              new_schema_version,
              NULL,
              trans,
              public_sql_string))) {
            LOG_WARN("add audit_schema failed",  K(new_audit_schema), K(ret));
          } else {
            LOG_INFO("succ to add audit_schema from routine", K(new_audit_schema));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &routine_info))) {
      LOG_WARN("insert create routine error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObPLDDLOperator::replace_routine(share::schema::ObRoutineInfo &routine_info,
                                      const share::schema::ObRoutineInfo *old_routine_info,
                                      common::ObMySQLTransaction &trans,
                                      share::schema::ObErrorInfo &error_info,
                                      common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                                      const common::ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  CK(OB_NOT_NULL(schema_service));
  CK(OB_NOT_NULL(old_routine_info));
  const uint64_t tenant_id = routine_info.get_tenant_id();
  int64_t del_param_schema_version = OB_INVALID_VERSION;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (old_routine_info->get_routine_params().count() > 0) {
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, del_param_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    routine_info.set_routine_id(old_routine_info->get_routine_id());
    routine_info.set_schema_version(new_schema_version);
  }
  if (OB_SUCC(ret) && ERROR_STATUS_NO_ERROR == error_info.get_error_status()) {
    OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans,
                                                          old_routine_info->get_tenant_id(),
                                                          old_routine_info->get_routine_id(),
                                                          old_routine_info->get_database_id()));
  }
  OZ (ObDependencyInfo::delete_schema_object_dependency(trans, old_routine_info->get_tenant_id(),
                                     old_routine_info->get_routine_id(),
                                     new_schema_version,
                                     old_routine_info->get_object_type()));
  if (OB_SUCC(ret)
      && OB_FAIL(schema_service->get_routine_sql_service().replace_routine(routine_info,
                                                                           old_routine_info,
                                                                           del_param_schema_version,
                                                                           &trans,
                                                                           ddl_stmt_str))) {
    LOG_WARN("replace routine info failed", K(routine_info), K(ret));
  }

  OZ (insert_dependency_infos(trans, dep_infos, tenant_id, routine_info.get_routine_id(),
                                routine_info.get_schema_version(),
                                routine_info.get_owner_id()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &routine_info))) {
      LOG_WARN("replace routine error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObPLDDLOperator::alter_routine(const share::schema::ObRoutineInfo &routine_info,
                                    common::ObMySQLTransaction &trans,
                                    share::schema::ObErrorInfo &error_info,
                                    const common::ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  UNUSEDx(ddl_stmt_str);
  if (OB_FAIL(error_info.handle_error_info(trans, &routine_info))) {
    LOG_WARN("drop routine error info failed.", K(ret), K(error_info));
  }
  return ret;
}

int ObPLDDLOperator::drop_routine(const share::schema::ObRoutineInfo &routine_info,
                                  common::ObMySQLTransaction &trans,
                                  share::schema::ObErrorInfo &error_info,
                                  const common::ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = routine_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(drop_obj_privs(tenant_id, routine_info.get_routine_id(),
                                    static_cast<uint64_t>(routine_info.get_routine_type()),
                                    trans))) {
    LOG_WARN("fail to drop_obj_privs", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_routine_sql_service().drop_routine(
                     routine_info, new_schema_version, trans, ddl_stmt_str))) {
    LOG_WARN("drop routine info failed", K(routine_info), K(ret));
  }
  uint64_t rt_id = routine_info.get_routine_id();
  uint64_t db_id = routine_info.get_database_id();
  OZ (pl::ObPLCacheMgr::flush_pl_cache_by_sql(rt_id, db_id, tenant_id, schema_service_));
  OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, routine_info.get_tenant_id(),
                            routine_info.get_routine_id(), routine_info.get_database_id()));
  OZ (ObDependencyInfo::delete_schema_object_dependency(trans, routine_info.get_tenant_id(),
                                     routine_info.get_routine_id(),
                                     new_schema_version,
                                     routine_info.get_object_type()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &routine_info))) {
      LOG_WARN("drop routine error info failed.", K(ret), K(error_info));
    }
  }

  // delete audit in procedure
  if (OB_SUCC(ret)) {
    ObArray<const ObSAuditSchema *> audits;
    ObSchemaGetterGuard schema_guard;
    ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
    if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                              AUDIT_PROCEDURE,
                                                              routine_info.get_routine_id(),
                                                              audits))) {
      LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
    } else if (!audits.empty()) {
      common::ObSqlString public_sql_string;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
        const ObSAuditSchema *audit_schema = audits.at(i);
        if (OB_ISNULL(audit_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("audit_schema is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
            *audit_schema,
            AUDIT_MT_DEL,
            false,
            new_schema_version,
            NULL,
            trans,
            public_sql_string))) {
          LOG_WARN("drop audit_schema failed",  KPC(audit_schema), K(ret));
        } else {
          LOG_INFO("succ to delete audit_schema from drop procedure", KPC(audit_schema));
        }
      }
    } else {
      LOG_DEBUG("no need to delete audit_schema from drop procedure", K(audits), K(routine_info));
    }
  }
  return ret;
}

// functions for managing udt
int ObPLDDLOperator::create_udt(share::schema::ObUDTTypeInfo &udt_info,
                                common::ObMySQLTransaction &trans,
                                share::schema::ObErrorInfo &error_info,
                                common::ObIArray<share::schema::ObRoutineInfo> &public_routine_infos,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                                const common::ObString *ddl_stmt_str/*=NULL*/)
{
  UNUSED(schema_guard);
  int ret = OB_SUCCESS;
  uint64_t new_udt_id = OB_INVALID_ID;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObUDTObjectType *obj_info = NULL;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id
        && !is_inner_pl_udt_id(new_udt_id = udt_info.get_type_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get new system udt id", K(tenant_id), K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id
        && OB_FAIL(schema_service->fetch_new_udt_id(tenant_id, new_udt_id))) {
    LOG_WARN("failed to fetch new_udt_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    // object spec and collection
    if (udt_info.is_object_spec_ddl() || !udt_info.is_object_type()) {
      udt_info.set_type_id(new_udt_id);
      if (udt_info.is_object_spec_ddl()
          && 0 < udt_info.get_object_type_infos().count()) {
        OX (obj_info = udt_info.get_object_type_infos().at(0));
        CK (OB_NOT_NULL(obj_info));
        // set object spec id
        uint64_t new_obj_id = OB_INVALID_ID;
        if (OB_SYS_TENANT_ID == tenant_id) {
          OZ (schema_service->fetch_new_sys_pl_object_id(tenant_id, new_obj_id));
        } else {
          OZ (schema_service->fetch_new_udt_id(tenant_id, new_obj_id));
        }
        OX (obj_info->set_coll_type(new_obj_id));
      }
    } else { // object body
      CK (2 == udt_info.get_object_type_infos().count());
      OX (obj_info = udt_info.get_object_type_infos().at(1));
      CK (OB_NOT_NULL(obj_info));
      // set object body id
      OX (obj_info->set_coll_type(new_udt_id));
      // udt_info.set_type_id(new_udt_id);
    }
    udt_info.set_schema_version(new_schema_version);
    if (FAILEDx(schema_service->get_udt_sql_service().create_udt(udt_info,
                                                                 &trans,
                                                                 ddl_stmt_str))) {
      LOG_WARN("insert udt info failed", K(udt_info), K(ret));
    } else {
      ARRAY_FOREACH(public_routine_infos, routine_idx) {
        ObRoutineInfo &routine_info = public_routine_infos.at(routine_idx);
        if (udt_info.is_object_spec_ddl()) {
          if (OB_FAIL(update_routine_info(routine_info,
                                          tenant_id,
                                          udt_info.get_type_id(),
                                          udt_info.get_database_id(),
                                          udt_info.get_database_id()))) {
            LOG_WARN("failed to update routine info", K(ret));
          } else if (OB_FAIL(schema_service->get_routine_sql_service().create_routine(routine_info,
                                                                        &trans, NULL))) {
            LOG_WARN("insert routine info failed", K(routine_info), K(ret));
          }
        } else {
          // update routine route sql
          int64_t new_schema_version = OB_INVALID_VERSION;
          OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
          OX (routine_info.set_schema_version(new_schema_version));
          OZ (schema_service->get_routine_sql_service().update_routine(routine_info, &trans));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_inner_pl_udt_id(new_udt_id)) {
    OZ (insert_dependency_infos(trans, dep_infos, tenant_id, udt_info.get_type_id(),
                                new_schema_version,
                                udt_info.get_database_id()));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &udt_info))) {
      LOG_WARN("insert trigger error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObPLDDLOperator::replace_udt(share::schema::ObUDTTypeInfo &udt_info,
                                  const share::schema::ObUDTTypeInfo *old_udt_info,
                                  common::ObMySQLTransaction &trans,
                                  share::schema::ObErrorInfo &error_info,
                                  common::ObIArray<share::schema::ObRoutineInfo> &public_routine_infos,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                                  const common::ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  int64_t del_param_schema_version = OB_INVALID_VERSION;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  CK(OB_NOT_NULL(schema_service));
  CK(OB_NOT_NULL(old_udt_info));
  if (old_udt_info->get_attrs().count() > 0 || OB_NOT_NULL(old_udt_info->get_coll_info())) {
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, del_param_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    udt_info.set_schema_version(new_schema_version);
    udt_info.set_type_id(old_udt_info->get_type_id());
  }

  if (OB_SUCC(ret)) {
    ObUDTObjectType *old_obj_info = NULL, *new_obj_info = NULL;
    if (udt_info.is_object_spec_ddl()) {
      // create or replace xxx is object(a number), no object type info
      // CK (0 < old_udt_info->get_object_type_infos().count());
      // CK (0 < udt_info.get_object_type_infos().count());
      if (udt_info.get_object_type_infos().count() > 0) {
        OX (new_obj_info = udt_info.get_object_type_infos().at(0));
        if (old_udt_info->get_object_type_infos().count() > 0) {
          OX (old_obj_info = old_udt_info->get_object_type_infos().at(0));
          OX (new_obj_info->set_coll_type(old_obj_info->get_coll_type()));
        } else {
          uint64_t new_obj_id = OB_INVALID_ID;
          if (OB_SYS_TENANT_ID == tenant_id) {
            OZ (schema_service->fetch_new_sys_pl_object_id(tenant_id, new_obj_id));
          } else {
            OZ (schema_service->fetch_new_udt_id(tenant_id, new_obj_id));
          }
          OX (new_obj_info->set_coll_type(new_obj_id));
        }
      }
    } else if (udt_info.is_object_body_ddl()) {
      CK (2 == old_udt_info->get_object_type_infos().count());
      CK (2 == udt_info.get_object_type_infos().count());
      OX (old_obj_info = old_udt_info->get_object_type_infos().at(1));
      OX (new_obj_info = udt_info.get_object_type_infos().at(1));
      OX (new_obj_info->set_coll_type(old_obj_info->get_coll_type()));
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret) && public_routine_infos.count() > 0 && ERROR_STATUS_NO_ERROR == error_info.get_error_status()) {
    if (OB_INVALID_ID != ObUDTObjectType::mask_object_id(old_udt_info->get_object_spec_id(tenant_id))) {
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                    ObUDTObjectType::mask_object_id(old_udt_info->get_object_spec_id(tenant_id)),
                                                            old_udt_info->get_database_id()));
      if (old_udt_info->has_type_body() &&
          OB_INVALID_ID != ObUDTObjectType::mask_object_id(old_udt_info->get_object_body_id(tenant_id))) {
        OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                      ObUDTObjectType::mask_object_id(old_udt_info->get_object_body_id(tenant_id)),
                                                              old_udt_info->get_database_id()));
      }
    }
  }
  OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                     old_udt_info->get_type_id(),
                                     new_schema_version,
                                     // old_udt_info->get_object_type())); TODO: @haohao.hao type body id design flaw
                                     udt_info.get_object_type()));
  OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                     udt_info.get_type_id(),
                                     new_schema_version,
                                     udt_info.get_object_type()));
  if (OB_SUCC(ret)) {
    // Discuss whether the built-in functions of the object type need to be deleted and then inserted in several situations
    // 1. spec spec replace , need delete, Because the definition of the function may have changed
    // 2. spec body replace, need delete, Although the function definition cannot be changed,
    // the parameters may change, because some member functions will add a self parameter,
    // which needs to be added to the __all_routine_param table.
    // 3. body body replace, need delete, Because the function body may have changed. type_id has also changed
    // bool need_del_routines = true;
    // if ((udt_info.is_object_type() && udt_info.is_object_body_ddl()
    //      && old_udt_info->is_object_type() && old_udt_info->is_object_body_ddl())) {
    //           need_del_routines = false;
    // }
    if (udt_info.is_object_spec_ddl() && OB_FAIL(del_routines_in_udt(udt_info, trans, schema_guard))) {
      LOG_WARN("failed to delete functions", K(ret));
    } else if (OB_FAIL(schema_service->get_udt_sql_service().replace_udt(udt_info,
                                                                   old_udt_info,
                                                                   del_param_schema_version,
                                                                   &trans,
                                                                   ddl_stmt_str))) {
      LOG_WARN("replace udt info failed", K(udt_info), K(ret));
    } else {
      ARRAY_FOREACH(public_routine_infos, routine_idx) {
        ObRoutineInfo &routine_info = public_routine_infos.at(routine_idx);
        if (udt_info.is_object_spec_ddl()) {
          if (OB_FAIL(update_routine_info(routine_info,
                                          tenant_id,
                                          udt_info.get_type_id(),
                                          udt_info.get_database_id(),
                                          udt_info.get_database_id()))) {
            LOG_WARN("failed to update routine info", K(ret));
          } else if (OB_FAIL(schema_service->get_routine_sql_service().create_routine(routine_info,
                                                                                      &trans,
                                                                                      NULL))) {
            LOG_WARN("insert routine info failed", K(routine_info), K(ret));
          }
        } else {
          // update routine route sql
          int64_t new_schema_version = OB_INVALID_VERSION;
          OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
          OX (routine_info.set_schema_version(new_schema_version));
          OZ (schema_service->get_routine_sql_service().update_routine(routine_info, &trans));
        }
      }
    }
  }
  OZ (insert_dependency_infos(trans, dep_infos, tenant_id, udt_info.get_type_id(),
                                udt_info.get_schema_version(),
                                udt_info.get_database_id()));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &udt_info))) {
      LOG_WARN("insert trigger error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObPLDDLOperator::drop_udt(const share::schema::ObUDTTypeInfo &udt_info,
                              common::ObMySQLTransaction &trans,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const common::ObString *ddl_stmt_str/*=NULL*/)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = udt_info.get_tenant_id();
  const int64_t type_id = udt_info.get_type_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(drop_obj_privs(tenant_id, type_id,
                                    static_cast<uint64_t>(ObObjectType::TYPE),
                                    trans))) {
    LOG_WARN("fail to drop_obj_privs", K(ret), K(udt_info), K(tenant_id));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (udt_info.is_object_spec_ddl() && OB_FAIL(del_routines_in_udt(udt_info,
                                                                      trans, schema_guard))) {
    LOG_WARN("fail to drop routines in udt", K(ret));
  } else if (OB_FAIL(schema_service->get_udt_sql_service().drop_udt(udt_info,
                                                                    new_schema_version,
                                                                    trans, ddl_stmt_str))) {
    LOG_WARN("drop udt info failed", K(udt_info), K(ret));
  }
  if (OB_SUCC(ret)) {
    uint64_t spec_udt_id = ObUDTObjectType::mask_object_id(udt_info.get_object_spec_id(tenant_id));
    if (udt_info.is_object_spec_ddl() &&
        OB_INVALID_ID != spec_udt_id) {
      OZ (pl::ObPLCacheMgr::flush_pl_cache_by_sql(spec_udt_id, udt_info.get_database_id(), tenant_id, schema_service_));
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                    spec_udt_id,
                    udt_info.get_database_id()));
      if (udt_info.has_type_body() &&
          OB_INVALID_ID != ObUDTObjectType::mask_object_id(udt_info.get_object_body_id(tenant_id))) {
        uint64_t body_udt_id = ObUDTObjectType::mask_object_id(udt_info.get_object_body_id(tenant_id));
        OZ (pl::ObPLCacheMgr::flush_pl_cache_by_sql(body_udt_id, udt_info.get_database_id(), tenant_id, schema_service_));
        OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                      body_udt_id,
                      udt_info.get_database_id()));
      }
    } else if (udt_info.is_object_body_ddl() &&
               OB_INVALID_ID != ObUDTObjectType::mask_object_id(udt_info.get_object_body_id(tenant_id))) {
      uint64_t body_udt_id = ObUDTObjectType::mask_object_id(udt_info.get_object_body_id(tenant_id));
      OZ (pl::ObPLCacheMgr::flush_pl_cache_by_sql(body_udt_id, udt_info.get_database_id(), tenant_id, schema_service_));
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                    body_udt_id,
                    udt_info.get_database_id()));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (udt_info.is_object_spec_ddl()
             && OB_FAIL(ObDependencyInfo::delete_schema_object_dependency(
                    trans, tenant_id, type_id, new_schema_version, ObObjectType::TYPE_BODY))) {
    LOG_WARN("delete dependencies of type body related to the type spec failed",
             K(ret), K(tenant_id), K(type_id));
  } else if (OB_FAIL(ObDependencyInfo::delete_schema_object_dependency(
                 trans, tenant_id, type_id, new_schema_version, udt_info.get_object_type()))) {
    LOG_WARN("delete dependency of the type itself failed", K(ret), K(tenant_id), K(type_id));
  }

  if (OB_SUCC(ret)) {
    ObErrorInfo error_info;
    if (OB_FAIL(error_info.handle_error_info(trans, &udt_info))) {
      LOG_WARN("insert trigger error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}

int ObPLDDLOperator::del_routines_in_udt(const share::schema::ObUDTTypeInfo &udt_info,
                                          common::ObMySQLTransaction &trans,
                                          share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = udt_info.get_tenant_id();
  uint64_t udt_id = udt_info.get_type_id();
  if (OB_INVALID_ID == tenant_id
      || OB_INVALID_ID == udt_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObArray<const ObRoutineInfo *> routines;
    if (OB_FAIL(schema_guard.get_routine_infos_in_udt(tenant_id, udt_id, routines))) {
      LOG_WARN("get routines in udt failed", K(tenant_id), K(udt_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < routines.count(); ++i) {
        const ObRoutineInfo *routine_info = routines.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_ISNULL(routine_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("routine info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_routine_sql_service().drop_routine(
                           *routine_info, new_schema_version, trans))) {
          LOG_WARN("drop routine failed", "routine_id", routine_info->get_routine_id(), K(ret));
        }
      }
    }
  }
  return ret;
}


// functions for managing package
int ObPLDDLOperator::create_package(const ObPackageInfo *old_package_info,
                                    ObPackageInfo &new_package_info,
                                    ObMySQLTransaction &trans,
                                    ObSchemaGetterGuard &schema_guard,
                                    ObIArray<ObRoutineInfo> &public_routine_infos,
                                    ObErrorInfo &error_info,
                                    ObIArray<ObDependencyInfo> &dep_infos,
                                    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t new_package_id = OB_INVALID_ID;
  const uint64_t tenant_id = new_package_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  bool is_replace = false;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else {
    if (OB_NOT_NULL(old_package_info)) {
      is_replace = true;
      new_package_id = old_package_info->get_package_id();
      if (old_package_info->is_package() && OB_FAIL(del_routines_in_package(*old_package_info, trans, schema_guard))) {
        LOG_WARN("del routines in package failed", K(ret), K(old_package_info));
      } else {}
    } else {
      if (OB_SYS_TENANT_ID == tenant_id) {
        if (OB_FAIL(schema_service->fetch_new_sys_pl_object_id(tenant_id, new_package_id))) {
          LOG_WARN("failed to fetch new_package_id", K(tenant_id), K(ret));
        }
      } else {
        if (OB_FAIL(schema_service->fetch_new_package_id(tenant_id, new_package_id))) {
          LOG_WARN("failed to fetch new_package_id", K(tenant_id), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      new_package_info.set_package_id(new_package_id);
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (FALSE_IT(new_package_info.set_schema_version(new_schema_version))) {
      } else if (OB_FAIL(schema_service->get_routine_sql_service().create_package(new_package_info,
          &trans, is_replace, ddl_stmt_str))) {
        LOG_WARN("insert package info failed", K(new_package_info), K(ret));
      } else {
        ARRAY_FOREACH(public_routine_infos, routine_idx) {
          ObRoutineInfo &routine_info = public_routine_infos.at(routine_idx);
          if (new_package_info.is_package()) {
            if (OB_FAIL(update_routine_info(routine_info,
                                            tenant_id,
                                            new_package_info.get_package_id(),
                                            new_package_info.get_owner_id(),
                                            new_package_info.get_database_id(),
                                            routine_info.get_routine_id()))) {
              LOG_WARN("failed to update routine info", K(ret));
            } else if (OB_FAIL(schema_service->get_routine_sql_service().create_routine(routine_info,
                                                                         &trans, NULL))) {
              LOG_WARN("insert routine info failed", K(routine_info), K(ret));
            }
          } else if (OB_INVALID_ID == routine_info.get_routine_id()) {
            OZ (update_routine_info(routine_info,
                                    tenant_id,
                                    routine_info.get_package_id(),
                                    routine_info.get_owner_id(),
                                    routine_info.get_database_id()));
            OZ (schema_service->get_routine_sql_service().create_routine(routine_info, &trans, NULL));
          } else {
            // update routine route sql
            int64_t new_schema_version = OB_INVALID_VERSION;
            OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
            OX (routine_info.set_schema_version(new_schema_version));
            OZ (schema_service->get_routine_sql_service().update_routine(routine_info, &trans));
          }
        }
      }
    }

    // add audit in package if necessary
    if (OB_SUCC(ret) && new_package_info.is_package()) {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObArray<const ObSAuditSchema *> audits;
      if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                         AUDIT_OBJ_DEFAULT,
                                                         OB_AUDIT_MOCK_USER_ID,
                                                         audits))) {
        LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
      } else if (!audits.empty()) {
        ObSchemaService *schema_service = schema_service_.get_schema_service();
        common::ObSqlString public_sql_string;
        ObSAuditSchema new_audit_schema;
        for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
          uint64_t new_audit_id = common::OB_INVALID_ID;
          const ObSAuditSchema *audit_schema = audits.at(i);
          if (OB_ISNULL(audit_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("audit_schema is NULL", K(ret));
          } else if (!audit_schema->is_access_operation_for_package()) {
            continue;
          } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))){
            LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_service->fetch_new_audit_id(tenant_id, new_audit_id))) {
            LOG_WARN("Failed to fetch new_audit_id", K(ret));
          } else if (OB_FAIL(new_audit_schema.assign(*audit_schema))) {
            LOG_WARN("fail to assign audit schema", KR(ret));
          } else {
            new_audit_schema.set_schema_version(new_schema_version);
            new_audit_schema.set_audit_id(new_audit_id);
            new_audit_schema.set_audit_type(AUDIT_PACKAGE);
            new_audit_schema.set_owner_id(new_package_info.get_package_id());
            if (OB_FAIL(schema_service->get_audit_sql_service().handle_audit_metainfo(
                new_audit_schema,
                AUDIT_MT_ADD,
                false,
                new_schema_version,
                NULL,
                trans,
                public_sql_string))) {
              LOG_WARN("add audit_schema failed",  K(new_audit_schema), K(ret));
            } else {
              LOG_INFO("succ to add audit_schema from package", K(new_audit_schema));
            }
          }
        }
      }
    }
    if (is_replace) {
      if (OB_SUCC(ret) && ERROR_STATUS_NO_ERROR == error_info.get_error_status()) {
        // when recreate package header, need clear package body cache if exist package body
        if (ObPackageType::PACKAGE_TYPE == old_package_info->get_type()) {
          const ObPackageInfo *del_package_info = NULL;
          if (OB_FAIL(schema_guard.get_package_info(tenant_id,
                                                    old_package_info->get_database_id(),
                                                    old_package_info->get_package_name(),
                                                    ObPackageType::PACKAGE_BODY_TYPE,
                                                    old_package_info->get_compatibility_mode(),
                                                    del_package_info))) {
            LOG_WARN("get package body info failed", K(ret));
          } else if (OB_NOT_NULL(del_package_info)) {
            OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans,
                                                                  tenant_id,
                                                                  del_package_info->get_package_id(),
                                                                  del_package_info->get_database_id()));
          }
        } else {
          // do nothing
        }
        OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans,
                                                              tenant_id,
                                                              old_package_info->get_package_id(),
                                                              old_package_info->get_database_id()));
      }
      OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                     old_package_info->get_package_id(),
                                     new_schema_version,
                                     old_package_info->get_object_type()));
    }
    OZ (insert_dependency_infos(trans, dep_infos, tenant_id, new_package_info.get_package_id(),
                                new_package_info.get_schema_version(),
                                new_package_info.get_owner_id()));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(error_info.handle_error_info(trans, &new_package_info))) {
        LOG_WARN("insert create package error info failed.", K(ret), K(error_info));
      }
    }
  }
  return ret;
}

int ObPLDDLOperator::alter_package(ObPackageInfo &package_info,
                                   ObSchemaGetterGuard &schema_guard,
                                   ObMySQLTransaction &trans,
                                   ObIArray<ObRoutineInfo> &public_routine_infos,
                                   ObErrorInfo &error_info,
                                   const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  UNUSEDx(ddl_stmt_str);
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = package_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t package_id = package_info.get_package_id();
  uint64_t database_id = package_info.get_database_id();
  const ObString &package_name = package_info.get_package_name();
  int64_t compatible_mode = package_info.get_compatibility_mode();
  OV (OB_NOT_NULL(schema_service_impl), OB_ERR_SYS);
  OV (OB_INVALID_ID != tenant_id && OB_INVALID_ID != package_id, OB_INVALID_ARGUMENT);
  if (OB_SUCC(ret)) {
    if (public_routine_infos.count() > 0) {
      bool need_create = false;
      ObArray<const ObRoutineInfo *> routine_infos;
      OZ (schema_guard.get_routine_infos_in_package(tenant_id,
                                                    public_routine_infos.at(0).get_package_id(),
                                                    routine_infos));
      OX (need_create = (0 == routine_infos.count()));
      // update routine route sql
      ARRAY_FOREACH(public_routine_infos, routine_idx) {
        ObRoutineInfo &routine_info = public_routine_infos.at(routine_idx);
        if (need_create) {
          CK (OB_INVALID_ID == routine_info.get_routine_id());
          OZ (update_routine_info(routine_info,
                                  tenant_id,
                                  routine_info.get_package_id(),
                                  routine_info.get_owner_id(),
                                  routine_info.get_database_id()));
          OZ (schema_service_impl->get_routine_sql_service().create_routine(routine_info, &trans, NULL));
        } else {
          OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
          OX (routine_info.set_schema_version(new_schema_version));
          OZ (schema_service_impl->get_routine_sql_service().update_routine(routine_info, &trans));
        }
      }
    }
    // if alter package, we need push up schema version, because we need update package state
    OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
    OX (package_info.set_schema_version(new_schema_version));
    OZ (schema_service_impl->get_routine_sql_service().alter_package(package_info, &trans, ddl_stmt_str));
  }

  OZ (error_info.handle_error_info(trans, &package_info));

  return ret;
}

int ObPLDDLOperator::drop_package(const ObPackageInfo &package_info,
                                ObMySQLTransaction &trans,
                                ObSchemaGetterGuard &schema_guard,
                                ObErrorInfo &error_info,
                                const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = package_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t package_id = package_info.get_package_id();
  if (OB_ISNULL(schema_service_impl)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == package_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (package_info.is_package()
        && OB_FAIL(del_routines_in_package(package_info, trans, schema_guard))) {
      LOG_WARN("del routines in package failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (ObPackageType::PACKAGE_TYPE == package_info.get_type()) {
      // delete privs on package
      OZ (drop_obj_privs(tenant_id,
                         package_info.get_package_id(),
                         static_cast<uint64_t>(ObObjectType::PACKAGE),
                         trans));
      uint64_t database_id = package_info.get_database_id();
      int64_t compatible_mode = package_info.get_compatibility_mode();
      const ObString &package_name = package_info.get_package_name();
      const ObPackageInfo *package_body_info = NULL;
      if (OB_FAIL(schema_guard.get_package_info(tenant_id, database_id, package_name, ObPackageType::PACKAGE_BODY_TYPE,
                                                compatible_mode, package_body_info))) {
        LOG_WARN("get package body info failed", K(tenant_id), K(database_id), K(package_name), K(ret));
      } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
      } else if (NULL != package_body_info) {
        if (OB_FAIL(schema_service_impl->get_routine_sql_service().drop_package(package_body_info->get_tenant_id(),
                                                                                package_body_info->get_database_id(),
                                                                                package_body_info->get_package_id(),
                                                                                new_schema_version, trans))) {
          LOG_WARN("drop package body info failed", K(package_body_info), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // package spec
        OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                                        package_info.get_package_id(),
                                                        new_schema_version,
                                                        package_info.get_object_type()));
        OZ (pl::ObPLCacheMgr::flush_pl_cache_by_sql(package_info.get_package_id(), package_info.get_database_id(), tenant_id,
                                                              schema_service_));
        OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id, package_info.get_package_id(),
                                                              package_info.get_database_id()));
        if (OB_NOT_NULL(package_body_info)) {
          OZ (ObDependencyInfo::delete_schema_object_dependency(trans, tenant_id,
                                                        package_body_info->get_package_id(),
                                                        new_schema_version,
                                                        package_body_info->get_object_type()));
          OZ (pl::ObPLCacheMgr::flush_pl_cache_by_sql(package_body_info->get_package_id(), package_body_info->get_database_id(),
                                                                tenant_id, schema_service_));
          OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id, package_body_info->get_package_id(),
                                                                package_body_info->get_database_id()));
        }
      }
    } else {
      OZ (pl::ObPLCacheMgr::flush_pl_cache_by_sql(package_info.get_package_id(),
                                                  package_info.get_database_id(),
                                                  tenant_id,
                                                  schema_service_));
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans,
                                                            tenant_id,
                                                            package_info.get_package_id(),
                                                            package_info.get_database_id()));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_impl->get_routine_sql_service().drop_package(package_info.get_tenant_id(),
                                                                                   package_info.get_database_id(),
                                                                                   package_info.get_package_id(),
                                                                                   new_schema_version,
                                                                                   trans,
                                                                                   ddl_stmt_str))) {
      LOG_WARN("drop package info failed", K(package_info), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &package_info))) {
      LOG_WARN("delete drop package error info failed", K(ret), K(error_info));
#ifdef OB_BUILD_ORACLE_PL
    } else if (OB_FAIL(pl::ObPLPackageType::delete_all_package_type(trans,
                                                                    package_info.get_tenant_id(),
                                                                    package_info.get_package_id()))) {
      LOG_WARN("delete all package type failed", K(package_info), K(ret));
#endif
    }
  }

    // delete audit in package
  if (OB_SUCC(ret)) {
    ObArray<const ObSAuditSchema *> audits;
    if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id,
                                                       AUDIT_PACKAGE,
                                                       package_info.get_package_id(),
                                                       audits))) {
      LOG_WARN("get get_audit_schema_in_owner failed", K(tenant_id), K(ret));
    } else if (!audits.empty()) {
      common::ObSqlString public_sql_string;
      for (int64_t i = 0; OB_SUCC(ret) && i < audits.count(); ++i) {
        const ObSAuditSchema *audit_schema = audits.at(i);
        if (OB_ISNULL(audit_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("audit_schema is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_audit_sql_service().handle_audit_metainfo(
            *audit_schema,
            AUDIT_MT_DEL,
            false,
            new_schema_version,
            NULL,
            trans,
            public_sql_string))) {
          LOG_WARN("drop audit_schema failed",  KPC(audit_schema), K(ret));
        } else {
          LOG_INFO("succ to delete audit_schema from drop packege", KPC(audit_schema));
        }
      }
    } else {
      LOG_DEBUG("no need to delete audit_schema from drop packege", K(audits), K(package_info));
    }
  }
  return ret;
}

int ObPLDDLOperator::del_routines_in_package(const ObPackageInfo &package_info,
                                           ObMySQLTransaction &trans,
                                           ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service_impl = schema_service_.get_schema_service();
  const uint64_t tenant_id = package_info.get_tenant_id();
  uint64_t package_id = package_info.get_package_id();
  if (OB_INVALID_ID == tenant_id
      || OB_INVALID_ID == package_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObArray<const ObRoutineInfo *> routines;
    if (OB_FAIL(schema_guard.get_routine_infos_in_package(tenant_id, package_id, routines))) {
      LOG_WARN("get routines in package failed", K(tenant_id), K(package_id), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < routines.count(); ++i) {
        const ObRoutineInfo *routine_info = routines.at(i);
        int64_t new_schema_version = OB_INVALID_VERSION;
        if (OB_ISNULL(routine_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("routine info is NULL", K(ret));
        } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_service_impl->get_routine_sql_service().drop_routine(
                           *routine_info, new_schema_version, trans))) {
          LOG_WARN("drop routine failed", "routine_id", routine_info->get_routine_id(), K(ret));
        }
      }
    }
  }
  return ret;
}

// functions for managing trigger
int ObPLDDLOperator::create_trigger(share::schema::ObTriggerInfo &trigger_info,
                                    common::ObMySQLTransaction &trans,
                                    share::schema::ObErrorInfo &error_info,
                                    ObIArray<ObDependencyInfo> &dep_infos,
                                    int64_t &table_schema_version,
                                    const common::ObString *ddl_stmt_str,
                                    bool is_update_table_schema_version,
                                    bool is_for_truncate_table)
                {
  int ret = OB_SUCCESS;
  table_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = trigger_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  bool is_replace = false;
  OV (OB_NOT_NULL(schema_service));
  if (!is_for_truncate_table) {
    // If create_trigger through truncate table, trigger_info already has its own trigger_id.
    // but there is no such trigger in the internal table at this time, so is_replace must be false
    OX (is_replace = (OB_INVALID_ID != trigger_info.get_trigger_id()));
    if (!is_replace) {
      OZ (fill_trigger_id(*schema_service, trigger_info), trigger_info.get_trigger_name());
    }
  }
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version),
      tenant_id, trigger_info.get_trigger_name());
  OX (trigger_info.set_schema_version(new_schema_version));
  OZ (schema_service->get_trigger_sql_service().create_trigger(trigger_info,
                                                               is_replace,
                                                               trans,
                                                               ddl_stmt_str),
      trigger_info.get_trigger_name(), is_replace);
  if (OB_SUCC(ret) && !trigger_info.is_system_type() && is_update_table_schema_version) {
    uint64_t base_table_id = trigger_info.get_base_object_id();
    OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
    OX (table_schema_version = new_schema_version);
    OZ (schema_service->get_table_sql_service().update_data_table_schema_version(
        trans, tenant_id, base_table_id, false/*in offline ddl white list*/, new_schema_version),
        base_table_id, trigger_info.get_trigger_name());
  }
  if (OB_FAIL(ret)) {
  } else if (!is_update_table_schema_version) {
  } else {
    uint64_t base_table_id = trigger_info.get_base_object_id();
    OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
    OX (table_schema_version = new_schema_version);
    if (OB_FAIL(ret)) {
    } else if (trigger_info.is_dml_type()) {
      OZ (schema_service->get_table_sql_service().update_data_table_schema_version(
          trans, tenant_id, base_table_id, false/*in offline ddl white list*/, new_schema_version),
          base_table_id, trigger_info.get_trigger_name());
    } else if (trigger_info.is_system_type()) {
      const ObUserInfo *user_info = NULL;
      ObSchemaGetterGuard schema_guard;
      OZ (schema_service_.get_tenant_schema_guard(tenant_id, schema_guard));
      OZ (schema_guard.get_user_info(tenant_id, base_table_id, user_info));
      OV (OB_NOT_NULL(user_info));
      if (OB_SUCC(ret)) {
        common::ObArray<ObUserInfo> user_array;
        OZ (user_array.push_back(*user_info));
        OZ (schema_service->get_user_sql_service().update_user_schema_version(tenant_id,
                                                                              user_array,
                                                                              ddl_stmt_str,
                                                                              trans));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 == dep_infos.count()) {
    // create trigger in mysql mode or create trigger when truncate table, dep_infos.count() is 0,
    // no need to deal with dependencies.
  } else {
    OZ (ObDependencyInfo::delete_schema_object_dependency(trans, trigger_info.get_tenant_id(),
                                                          trigger_info.get_trigger_id(),
                                                          trigger_info.get_schema_version(),
                                                          trigger_info.get_object_type()));
    OZ (insert_dependency_infos(trans,
                                dep_infos,
                                trigger_info.get_tenant_id(),
                                trigger_info.get_trigger_id(),
                                trigger_info.get_schema_version(),
                                trigger_info.get_owner_id()));

    if (OB_SUCC(ret) && is_replace && ERROR_STATUS_NO_ERROR == error_info.get_error_status()) {
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                share::schema::ObTriggerInfo::get_trigger_spec_package_id(trigger_info.get_trigger_id()),
                                                            trigger_info.get_database_id()));
      OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                    share::schema::ObTriggerInfo::get_trigger_body_package_id(trigger_info.get_trigger_id()),
                                                            trigger_info.get_database_id()));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(error_info.handle_error_info(trans, &trigger_info))) {
      LOG_WARN("insert trigger error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}
int ObPLDDLOperator::drop_trigger(const share::schema::ObTriggerInfo &trigger_info,
                                  common::ObMySQLTransaction &trans,
                                  const common::ObString *ddl_stmt_str,
                                  bool is_update_table_schema_version,
                                  const bool in_offline_ddl_white_list)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = trigger_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  OV (OB_NOT_NULL(schema_service));
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version),
      tenant_id, trigger_info.get_trigger_name());
  OZ (schema_service->get_trigger_sql_service().drop_trigger(trigger_info,
                                                             false,
                                                             new_schema_version,
                                                             trans,
                                                             ddl_stmt_str),
      trigger_info.get_trigger_name());
  OZ (ObDependencyInfo::delete_schema_object_dependency(trans,
                                                        tenant_id,
                                                        trigger_info.get_trigger_id(),
                                                        new_schema_version,
                                                        trigger_info.get_object_type()));
  uint64_t spec_trig_id = share::schema::ObTriggerInfo::get_trigger_spec_package_id(trigger_info.get_trigger_id());
  uint64_t body_trig_id = share::schema::ObTriggerInfo::get_trigger_body_package_id(trigger_info.get_trigger_id());
  OZ (pl::ObPLCacheMgr::flush_pl_cache_by_sql(spec_trig_id,
                                              trigger_info.get_database_id(),
                                              tenant_id,
                                              schema_service_));
  OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans,
                                                        tenant_id,
                                                        spec_trig_id,
                                                        trigger_info.get_database_id()));
  OZ (pl::ObPLCacheMgr::flush_pl_cache_by_sql(body_trig_id,
                                              trigger_info.get_database_id(),
                                              tenant_id,
                                              schema_service_));
  OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans,
                                                        tenant_id,
                                                        body_trig_id,
                                                        trigger_info.get_database_id()));
  if (OB_FAIL(ret)) {
  } else if (!is_update_table_schema_version) {
  } else {
    uint64_t base_table_id = trigger_info.get_base_object_id();
    if (trigger_info.is_dml_type()) {
      OZ (schema_service->get_table_sql_service().update_data_table_schema_version(trans,
          tenant_id, base_table_id, in_offline_ddl_white_list),
          base_table_id, trigger_info.get_trigger_name());
    } else if (trigger_info.is_system_type()) {
      const ObUserInfo *user_info = NULL;
      ObSchemaGetterGuard schema_guard;
      common::ObArray<ObUserInfo> user_array;
      OZ (schema_service_.get_tenant_schema_guard(tenant_id, schema_guard));
      OZ (schema_guard.get_user_info(tenant_id, base_table_id, user_info));
      OV (OB_NOT_NULL(user_info));
      OZ (user_array.push_back(*user_info));
      OZ (schema_service->get_user_sql_service().update_user_schema_version(tenant_id,
                                                                            user_array,
                                                                            ddl_stmt_str,
                                                                            trans));
    }
  }
  if (OB_SUCC(ret)) {
    ObErrorInfo error_info;
    if (OB_FAIL(error_info.handle_error_info(trans, &trigger_info))) {
      LOG_WARN("delete trigger error info failed.", K(ret), K(error_info));
    }
  }
  return ret;
}
int ObPLDDLOperator::drop_trigger_to_recyclebin(const share::schema::ObTriggerInfo &trigger_info,
                                                share::schema::ObSchemaGetterGuard &schema_guard,
                                                common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t tenant_id = trigger_info.get_tenant_id();
  uint64_t recyclebin_id = OB_RECYCLEBIN_SCHEMA_ID;
  uint64_t base_database_id = OB_INVALID_ID;
  bool recyclebin_exist = false;
  int64_t new_schema_version = OB_INVALID_VERSION;
  const ObSimpleTableSchemaV2 *base_table_schema = NULL;
  ObTriggerInfo new_trigger_info;
  ObSqlString new_trigger_name;
  ObRecycleObject recyclebin_object;
  OV (OB_NOT_NULL(schema_service));
  OV (OB_INVALID_TENANT_ID != tenant_id, OB_INVALID_ARGUMENT);
  OZ (schema_guard.check_database_exist(tenant_id, recyclebin_id, recyclebin_exist));
  OV (recyclebin_exist);
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version));
  OZ (new_trigger_info.deep_copy(trigger_info));
  OX (new_trigger_info.set_database_id(recyclebin_id));
  OX (new_trigger_info.set_schema_version(new_schema_version));
  OZ (construct_new_name_for_recyclebin(new_trigger_info, new_trigger_name));
  OZ (new_trigger_info.set_trigger_name(new_trigger_name.string()));
  OZ (schema_guard.get_simple_table_schema(tenant_id, trigger_info.get_base_object_id(), base_table_schema),
      trigger_info.get_base_object_id());
  OV (OB_NOT_NULL(base_table_schema), trigger_info.get_base_object_id());
  OX (base_database_id = base_table_schema->get_database_id());
  OX (recyclebin_object.set_tenant_id(tenant_id));
  OX (recyclebin_object.set_database_id(base_database_id));
  OX (recyclebin_object.set_table_id(trigger_info.get_trigger_id()));
  OX (recyclebin_object.set_tablegroup_id(OB_INVALID_ID));
  OZ (recyclebin_object.set_object_name(new_trigger_name.string()));
  OZ (recyclebin_object.set_original_name(trigger_info.get_trigger_name()));
  OX (recyclebin_object.set_type(ObRecycleObject::TRIGGER));
  OZ (schema_service->insert_recyclebin_object(recyclebin_object, trans));
  OZ (schema_service->get_trigger_sql_service().drop_trigger(new_trigger_info, true,
                                                             new_schema_version, trans),
      trigger_info.get_trigger_name());
  return ret;
}

int ObPLDDLOperator::alter_trigger(share::schema::ObTriggerInfo &trigger_info,
                                   common::ObMySQLTransaction &trans,
                                   const common::ObString *ddl_stmt_str,
                                   bool is_update_table_schema_version)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = trigger_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  OV (OB_NOT_NULL(schema_service));
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version), tenant_id,
      trigger_info.get_trigger_name());
  OX (trigger_info.set_schema_version(new_schema_version));
  OZ (schema_service->get_trigger_sql_service().alter_trigger(trigger_info, new_schema_version, trans, ddl_stmt_str),
      trigger_info.get_trigger_name());
  if (OB_SUCC(ret) && is_update_table_schema_version) {
      uint64_t base_table_id = trigger_info.get_base_object_id();
      if (trigger_info.is_dml_type()) {
        OZ (schema_service->get_table_sql_service().update_data_table_schema_version(
            trans, tenant_id, base_table_id, false/*in offline ddl white list*/),
            base_table_id, trigger_info.get_trigger_name());
      } else if (trigger_info.is_system_type()) {
        const ObUserInfo *user_info = NULL;
        ObSchemaGetterGuard schema_guard;
        common::ObArray<ObUserInfo> user_array;
        OZ (schema_service_.get_tenant_schema_guard(tenant_id, schema_guard));
        OZ (schema_guard.get_user_info(tenant_id, base_table_id, user_info));
        OV (OB_NOT_NULL(user_info));
        OZ (user_array.push_back(*user_info));
        OZ (schema_service->get_user_sql_service().update_user_schema_version(tenant_id,
                                                                              user_array,
                                                                              ddl_stmt_str,
                                                                              trans));
      }
  }
  OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                share::schema::ObTriggerInfo::get_trigger_spec_package_id(trigger_info.get_trigger_id()),
                                                        trigger_info.get_database_id()));
  OZ (pl::ObRoutinePersistentInfo::delete_dll_from_disk(trans, tenant_id,
                share::schema::ObTriggerInfo::get_trigger_body_package_id(trigger_info.get_trigger_id()),
                                                        trigger_info.get_database_id()));
  ObErrorInfo error_info;
  OZ (error_info.handle_error_info(trans, &trigger_info), error_info);
  return ret;
}

int ObPLDDLOperator::flashback_trigger(const share::schema::ObTriggerInfo &trigger_info,
                                       uint64_t new_database_id,
                                       const common::ObString &new_table_name,
                                       share::schema::ObSchemaGetterGuard &schema_guard,
                                       common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t tenant_id = trigger_info.get_tenant_id();
  const ObDatabaseSchema *database_schema = NULL;
  ObSEArray<ObRecycleObject, 1> recycle_objects;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObString new_trigger_name;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObTriggerInfo new_trigger_info;
  OV (OB_NOT_NULL(schema_service));
  OZ (schema_service->fetch_recycle_object(tenant_id, trigger_info.get_trigger_name(),
                                           ObRecycleObject::TRIGGER, trans, recycle_objects));
  OV (1 == recycle_objects.count(), OB_ERR_UNEXPECTED, recycle_objects.count());
  OZ (new_trigger_info.deep_copy(trigger_info));
  // database id.
  if (OB_INVALID_ID == new_database_id) {
    OX (new_database_id = recycle_objects.at(0).get_database_id());
  }
  OZ (schema_guard.get_database_schema(tenant_id, new_database_id, database_schema), new_database_id);
  OV (OB_NOT_NULL(database_schema), new_database_id);
  OV (!database_schema->is_in_recyclebin(), OB_OP_NOT_ALLOW, new_database_id);
  OX (new_trigger_info.set_database_id(new_database_id));
  // trigger name.
  OZ (build_flashback_object_name(new_trigger_info, NULL, "OBTRG",
                                  schema_guard, allocator, new_trigger_name));
  OX (new_trigger_info.set_trigger_name(new_trigger_name));
  // other operation.
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version), tenant_id);
  OZ (schema_service->delete_recycle_object(tenant_id, recycle_objects.at(0), trans));
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (new_table_name.empty()) {
    // If new_table_name is empty, it means that the table does not have a rename, and rebuild_trigger_on_rename is not required.
    // Otherwise, table rename is required, and rebuild_trigger_on_rename is required.
    OZ (schema_service->get_trigger_sql_service().flashback_trigger(new_trigger_info,
                                                                    new_schema_version,
                                                                    trans),
        new_trigger_info.get_trigger_id());
  } else {
    OZ (schema_service->get_trigger_sql_service().rebuild_trigger_on_rename(new_trigger_info,
                                                                            database_schema->get_database_name(),
                                                                            new_table_name,
                                                                            new_schema_version,
                                                                            trans,
                                                                            OB_DDL_FLASHBACK_TRIGGER),
        database_schema->get_database_name(), new_table_name);
  }
  return ret;
}

int ObPLDDLOperator::purge_table_trigger(const share::schema::ObTableSchema &table_schema,
                                        share::schema::ObSchemaGetterGuard &schema_guard,
                                        common::ObMySQLTransaction &trans,
                                        ObDDLOperator &ddl_operator)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const ObIArray<uint64_t> &trigger_id_list = table_schema.get_trigger_list();
  const ObTriggerInfo *trigger_info = NULL;
  ObPLDDLOperator pl_operator(ddl_operator.get_multi_schema_service(), ddl_operator.get_sql_proxy());
  for (int i = 0; OB_SUCC(ret) && i < trigger_id_list.count(); i++) {
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_id_list.at(i), trigger_info), trigger_id_list.at(i));
    OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_id_list.at(i));
    OZ (pl_operator.purge_trigger(*trigger_info, trans), trigger_id_list.at(i));
  }
  return ret;
}

int ObPLDDLOperator::purge_trigger(const share::schema::ObTriggerInfo &trigger_info,
                                   common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  ObRecycleObject::RecycleObjType recycle_type = ObRecycleObject::TRIGGER;
  ObArray<ObRecycleObject> recycle_objects;
  OV (OB_NOT_NULL(schema_service));
  OZ (schema_service->fetch_recycle_object(trigger_info.get_tenant_id(),
                                           trigger_info.get_trigger_name(),
                                           recycle_type,
                                           trans,
                                           recycle_objects));
  OV (1 == recycle_objects.count(), OB_ERR_UNEXPECTED, recycle_objects.count());
  OZ (schema_service->delete_recycle_object(trigger_info.get_tenant_id(),
                                            recycle_objects.at(0),
                                            trans));
  OZ (drop_trigger(trigger_info, trans, NULL));
  return ret;
}

int ObPLDDLOperator::rebuild_trigger_on_rename(const share::schema::ObTriggerInfo &trigger_info,
                                               const common::ObString &database_name,
                                               const common::ObString &table_name,
                                               common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = trigger_info.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  OV (OB_NOT_NULL(schema_service));
  OZ (schema_service_.gen_new_schema_version(tenant_id, new_schema_version),
      tenant_id, trigger_info.get_trigger_name());
  OZ (schema_service->get_trigger_sql_service().rebuild_trigger_on_rename(trigger_info,
                                                                          database_name,
                                                                          table_name,
                                                                          new_schema_version,
                                                                          trans),
      trigger_info.get_trigger_name());
  return ret;
}

int ObPLDDLOperator::drop_trigger_in_drop_database(uint64_t tenant_id,
                                                   const ObDatabaseSchema &db_schema,
                                                   ObDDLOperator &ddl_operator,
                                                   ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  //delete triggers in database, 只删除trigger_database != base_table_database 的trigger
  // trigger_database == base_table_database 的trigger会在drop table时删除
  ObArray<uint64_t> trigger_ids;
  ObSchemaGetterGuard schema_guard;
  const uint64_t database_id = db_schema.get_database_id();
  ObPLDDLOperator pl_operator(ddl_operator.get_multi_schema_service(), ddl_operator.get_sql_proxy());
  if (OB_FAIL(pl_operator.schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_trigger_ids_in_database(tenant_id, database_id, trigger_ids))) {
    LOG_WARN("get trigger infos in database failed", KR(ret), K(tenant_id), K(database_id));
  } else {
    bool is_oracle_mode = false;
    OZ (ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode));
    for (int64_t i = 0; OB_SUCC(ret) && i < trigger_ids.count(); i++) {
      const ObTriggerInfo *tg_info = NULL;
      const uint64_t trigger_id = trigger_ids.at(i);
      if (OB_FAIL(pl_operator.schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_trigger_info(tenant_id, trigger_id, tg_info))) {
        LOG_WARN("fail to get trigger info", KR(ret), K(tenant_id), K(trigger_id));
      } else if (OB_ISNULL(tg_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("trigger info is NULL", K(ret));
      } else if (tg_info->is_system_type()) {
        ObArray<const ObUserInfo *> user_array;
        CK (is_oracle_mode);
        OZ (schema_guard.get_user_info(tenant_id, db_schema.get_database_name_str(), user_array));
        OV (1 == user_array.count(), OB_ERR_UNEXPECTED, user_array.count());
        CK (OB_NOT_NULL(user_array.at(0)));
        if (OB_SUCC(ret) && user_array.at(0)->get_user_id() != tg_info->get_base_object_id()) {
          OZ (pl_operator.drop_trigger(*tg_info, trans, NULL));
        }
      } else {
        const ObSimpleTableSchemaV2 * tbl_schema = NULL;
        OZ (schema_guard.get_simple_table_schema(tenant_id, tg_info->get_base_object_id(), tbl_schema));
        CK (OB_NOT_NULL(tbl_schema));
        if (OB_SUCC(ret) && database_id != tbl_schema->get_database_id()) {
          OZ (pl_operator.drop_trigger(*tg_info, trans, NULL));
        }
      }
    }
  }
  return ret;
}

int ObPLDDLOperator::drop_trigger_cascade(const share::schema::ObTableSchema &table_schema,
                                          common::ObMySQLTransaction &trans,
                                          ObDDLOperator &ddl_operator)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObIArray<uint64_t> &trigger_list = table_schema.get_trigger_list();
  const ObTriggerInfo *trigger_info = NULL;
  uint64_t tenant_id = table_schema.get_tenant_id();
  uint64_t trigger_id = OB_INVALID_ID;
  ObPLDDLOperator pl_operator(ddl_operator.get_multi_schema_service(), ddl_operator.get_sql_proxy());
  OZ (ddl_operator.get_multi_schema_service().get_tenant_schema_guard(tenant_id, schema_guard), tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < trigger_list.count(); i++) {
    OX (trigger_id = trigger_list.at(i));
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_id, trigger_info));
    OV (OB_NOT_NULL(trigger_info));
    OZ (pl_operator.drop_trigger(*trigger_info, trans, NULL), trigger_info);
  }
  return ret;
}

int ObPLDDLOperator::fill_trigger_id(share::schema::ObSchemaService &schema_service,
                                     share::schema::ObTriggerInfo &trigger_info)
{
  int ret = OB_SUCCESS;
  uint64_t new_trigger_id = OB_INVALID_ID;
  OV (OB_INVALID_ID == trigger_info.get_trigger_id());
  OZ (schema_service.fetch_new_trigger_id(trigger_info.get_tenant_id(), new_trigger_id),
      trigger_info.get_trigger_name());
  OX (trigger_info.set_trigger_id(new_trigger_id));
  return ret;
}

int ObPLDDLOperator::insert_dependency_infos(common::ObMySQLTransaction &trans,
                                              common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                                              uint64_t tenant_id,
                                              uint64_t dep_obj_id,
                                              uint64_t schema_version,
                                              uint64_t owner_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == owner_id
   || OB_INVALID_ID == dep_obj_id
   || OB_INVALID_ID == tenant_id
   || OB_INVALID_SCHEMA_VERSION == schema_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("illegal schema version or owner id", K(ret), K(schema_version),
                                                   K(owner_id), K(dep_obj_id));
  } else {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < dep_infos.count(); ++i) {
      ObDependencyInfo & dep = dep_infos.at(i);
      dep.set_tenant_id(tenant_id);
      dep.set_dep_obj_id(dep_obj_id);
      dep.set_dep_obj_owner_id(owner_id);
      dep.set_schema_version(schema_version);
      OZ (dep.insert_schema_object_dependency(trans));
    }
  }
  return ret;
}

int ObPLDDLOperator::update_routine_info(share::schema::ObRoutineInfo &routine_info,
                                        int64_t tenant_id,
                                        int64_t parent_id,
                                        int64_t owner_id,
                                        int64_t database_id,
                                        int64_t routine_id)
{
  int ret = OB_SUCCESS;
  uint64_t new_routine_id = routine_id;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_INVALID_ID == new_routine_id
        && OB_SYS_TENANT_ID == tenant_id
        && OB_FAIL(schema_service->fetch_new_sys_pl_object_id(tenant_id, new_routine_id))) {
    LOG_WARN("failed to fetch new_routine_id", K(tenant_id), K(ret));
  } else if (OB_INVALID_ID == new_routine_id
        && OB_SYS_TENANT_ID != tenant_id
        && OB_FAIL(schema_service->fetch_new_routine_id(tenant_id, new_routine_id))) {
    LOG_WARN("failed to fetch new_routine_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret));
  } else {
    routine_info.set_database_id(database_id);
    if (lib::Worker::CompatMode::ORACLE == compat_mode) {
      routine_info.set_owner_id(owner_id);
    }
    routine_info.set_package_id(parent_id);
    routine_info.set_routine_id(new_routine_id);
    routine_info.set_schema_version(new_schema_version);
  }
  return ret;
}

/* [data_table_prefix]_[data_table_id]_RECYCLE_[object_type_prefix]_[timestamp].
 * data_table_prefix: if not null, use '[data_table_prefix]_[data_table_id]' as prefix,
 *                    otherwise, skip this part.
 * object_type_prefix: OBIDX / OBCHECK / OBTRG ...
 */
template <typename SchemaType>
int ObPLDDLOperator::build_flashback_object_name(const SchemaType &object_schema,
                                                  const char *data_table_prefix,
                                                  const char *object_type_prefix,
                                                  ObSchemaGetterGuard &schema_guard,
                                                  ObIAllocator &allocator,
                                                  ObString &object_name)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  int64_t saved_pos = 0;
  bool object_exist = true;
  static const char *RECYCLE = "RECYCLE";
  static const int64_t INT64_STR_LEN = 20;
  OV (OB_NOT_NULL(object_type_prefix));
  if (OB_NOT_NULL(data_table_prefix)) {
    OX (buf_len += (STRLEN(data_table_prefix) + 1 +   // [data_table_prefix]_
                    INT64_STR_LEN + 1));              // [data_table_id]_
  }
  OX (buf_len += (STRLEN(RECYCLE) + 1 +               // RECYCLE_
                  STRLEN(object_type_prefix) + 1 +    // [object_type_prefix]_
                  INT64_STR_LEN));                    // [timestamp]
  OV (OB_NOT_NULL(buf = static_cast<char *>(allocator.alloc(buf_len))),
      OB_ALLOCATE_MEMORY_FAILED, buf_len);
  if (OB_NOT_NULL(data_table_prefix)) {
    OZ (BUF_PRINTF("%s_%lu_", data_table_prefix, object_schema.get_data_table_id()));
  }
  OZ (BUF_PRINTF("%s_%s_", RECYCLE, object_type_prefix));
  OX (saved_pos = pos);
  while (OB_SUCC(ret) && object_exist) {
    OX (pos = saved_pos);
    OZ (BUF_PRINTF("%ld", ObTimeUtility::current_time()));
    OX (object_name.assign(buf, static_cast<int32_t>(pos)));
    OZ (schema_guard.check_flashback_object_exist(object_schema, object_name, object_exist));
  }
  return ret;
}


} // namespace rootserver
} // namespace oceanbase
