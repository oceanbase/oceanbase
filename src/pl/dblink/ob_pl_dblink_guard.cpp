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

#define USING_LOG_PREFIX PL

#include "ob_pl_dblink_guard.h"
#include "pl/ob_pl_stmt.h"
#ifdef OB_BUILD_ORACLE_PL
#include "lib/oracleclient/ob_oci_metadata.h"
#include "pl/dblink/ob_pl_dblink_util.h"
#endif

namespace oceanbase
{
namespace pl
{
typedef common::sqlclient::DblinkDriverProto DblinkDriverProto;

int ObPLDbLinkGuard::get_routine_infos_with_synonym(sql::ObSQLSessionInfo &session_info,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    const ObString &dblink_name,
                                    const ObString &part1,
                                    const ObString &part2,
                                    const ObString &part3,
                                    common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "PL dblink");
#else
  const uint64_t tenant_id = MTL_ID();
  const ObDbLinkSchema *dblink_schema = NULL;
  DblinkDriverProto link_type = common::sqlclient::DBLINK_UNKNOWN;
  common::sqlclient::dblink_param_ctx param_ctx;
  param_ctx.pool_type_ = common::sqlclient::DblinkPoolType::DBLINK_POOL_SCHEMA;
  common::ObDbLinkProxy *dblink_proxy = NULL;
  common::sqlclient::ObISQLConnection *dblink_conn = NULL;
  ObString full_name;
  ObString schema_name;
  ObString object_name;
  ObString sub_object_name;
  int64_t object_type;
  uint32_t remote_version = 0;
  OZ (schema_guard.get_dblink_schema(tenant_id, dblink_name, dblink_schema), tenant_id, dblink_name);
  OV (OB_NOT_NULL(dblink_schema), OB_DBLINK_NOT_EXIST_TO_ACCESS, dblink_name);
  OZ (ObPLDblinkUtil::init_dblink(dblink_proxy, dblink_conn, session_info, schema_guard, dblink_name, link_type, false));
  CK (OB_NOT_NULL(dblink_proxy));
  CK (OB_NOT_NULL(dblink_conn));
  OZ (check_remote_version(*dblink_proxy, *dblink_conn, remote_version));
  OZ (ObPLDblinkUtil::print_full_name(alloc_, full_name, part1, part2, part3));
  OZ (dblink_name_resolve(dblink_proxy,
                          dblink_conn,
                          dblink_schema,
                          full_name,
                          schema_name,
                          object_name,
                          sub_object_name,
                          object_type,
                          alloc_));
  OZ (get_dblink_routine_infos(dblink_proxy,
                               dblink_conn,
                               session_info,
                               schema_guard,
                               dblink_name,
                               schema_name,
                               object_name,
                               sub_object_name,
                               routine_infos,
                               remote_version));
#define CHECK_NOT_SUPPORT_TYPE(will_check_type) \
  if (ob_is_nvarchar2(will_check_type) || ob_is_nchar(will_check_type)) { \
    ret = OB_NOT_SUPPORTED; \
    LOG_WARN("not support type", K(ret), K(will_check_type)); \
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "NCHAR/NVARCHAR2 in PL dblink"); \
  }

#define CHECK_RECORD_TYPE(udt_record) \
  const ObRecordType *record_type = static_cast<const ObRecordType *>(udt_record);  \
  OV (OB_NOT_NULL(record_type));  \
  for (int64_t mem_idx = 0; OB_SUCC(ret) && mem_idx < record_type->get_member_count(); mem_idx++) { \
    const ObPLDataType *type = record_type->get_record_member_type(mem_idx);  \
    OV (OB_NOT_NULL(type)); \
    if (OB_SUCC(ret) && !ob_is_extend(type->get_obj_type())) {  \
      CHECK_NOT_SUPPORT_TYPE(type->get_obj_type());  \
    } \
  }
  if (OB_SUCC(ret)) {
    const ObIRoutineInfo *info = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < routine_infos.count(); i++) {
      OV (OB_NOT_NULL(info = routine_infos.at(i)));
      for(int64_t param_idx = 0; OB_SUCC(ret) && param_idx < info->get_param_count(); param_idx++) {
        ObIRoutineParam *param = NULL;
        ObRoutineParam *routine_param = NULL;
        OZ (info->get_routine_param(param_idx, param));
        OV (OB_NOT_NULL(param));
        OV (OB_NOT_NULL(routine_param = static_cast<ObRoutineParam *>(param)));
        if (OB_FAIL(ret)) {
        } else if (!ob_is_extend(routine_param->get_pl_data_type().get_obj_type())) {
          CHECK_NOT_SUPPORT_TYPE(routine_param->get_pl_data_type().get_obj_type());
        } else if (routine_param->is_dblink_type()) {
          const pl::ObUserDefinedType *udt = NULL;
          ObString db_name;
          const ObString &pkg_name = routine_param->get_type_subname();
          const ObString &udt_name = routine_param->get_type_name();
          const ObPLDbLinkInfo *dblink_info = NULL;
          const uint64_t udt_id = OB_INVALID_ID;
          bool find_pkg = false;
          OZ (get_dblink_info(info->get_dblink_id(), dblink_info));
          OV (OB_NOT_NULL(dblink_info), OB_ERR_UNEXPECTED, K(info->get_dblink_id()));
          CK (routine_param->get_extended_type_info().count() > 0);
          OX (db_name = routine_param->get_extended_type_info().at(0));
          OZ (dblink_info->get_udt_from_cache(db_name, pkg_name, udt_name, udt, find_pkg));
          CK (OB_NOT_NULL(udt));
          if (OB_FAIL(ret)) {
          } else if (udt->is_record_type()) {
            const ObRecordType *record_type = static_cast<const ObRecordType *>(udt);
            OV (OB_NOT_NULL(record_type));
            for (int64_t mem_idx = 0; OB_SUCC(ret) && mem_idx < record_type->get_member_count(); mem_idx++) {
              const ObPLDataType *type = record_type->get_record_member_type(mem_idx);
              OV (OB_NOT_NULL(type));
              if (OB_SUCC(ret) && !ob_is_extend(type->get_obj_type())) {
                CHECK_NOT_SUPPORT_TYPE(type->get_obj_type());
              }
            }
          } else if (udt->is_collection_type()) {
            const ObCollectionType *coll_type = static_cast<const ObCollectionType *>(udt);
            OV (OB_NOT_NULL(coll_type));
            if (OB_SUCC(ret)) {
              if (coll_type->get_element_type().is_record_type()) {
                const pl::ObUserDefinedType *udt2 = NULL;
                OZ (get_dblink_type_by_id(extract_package_id(coll_type->get_element_type().get_user_type_id()),
                                          coll_type->get_element_type().get_user_type_id(), udt2));
                OV (OB_NOT_NULL(udt2));
                CHECK_RECORD_TYPE(udt2);
              } else if (!ob_is_extend(coll_type->get_element_type().get_obj_type())) {
                CHECK_NOT_SUPPORT_TYPE(coll_type->get_element_type().get_obj_type());
              }
            }
          }
        }
      }
    }
  }
#undef CHECK_RECORD_TYPE
#undef CHECK_NOT_SUPPORT_TYPE
  if (OB_NOT_NULL(dblink_proxy) && OB_NOT_NULL(dblink_conn)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = dblink_proxy->release_dblink(link_type, dblink_conn))) {
      LOG_WARN("failed to relese connection", K(tmp_ret));
    }
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
#endif
  return ret;
}

int ObPLDbLinkGuard::get_dblink_type_with_synonym(sql::ObSQLSessionInfo &session_info,
                                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                                  const ObString &dblink_name,
                                                  const ObString &part1,
                                                  const ObString &part2,
                                                  const ObString &part3,
                                                  const pl::ObUserDefinedType *&udt)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "PL dblink");
#else
  common::ObDbLinkProxy *dblink_proxy = NULL;
  common::sqlclient::ObISQLConnection *dblink_conn = NULL;
  common::sqlclient::DblinkDriverProto link_type = DBLINK_UNKNOWN;
  ObString full_name;
  OZ (ObPLDblinkUtil::init_dblink(dblink_proxy, dblink_conn, session_info, schema_guard, dblink_name, link_type, false));
  CK (OB_NOT_NULL(dblink_proxy));
  CK (OB_NOT_NULL(dblink_conn));
  if (OB_SUCC(ret)) {
    ObString schema_name;
    ObString object_name;
    ObString sub_object_name;
    int64_t object_type;
    const ObDbLinkSchema *dblink_schema = NULL;
    uint32_t remote_version = 0;
    OZ (schema_guard.get_dblink_schema(MTL_ID(), dblink_name, dblink_schema), dblink_name);
    OV (OB_NOT_NULL(dblink_schema), OB_ERR_UNEXPECTED, dblink_name);
    OZ (check_remote_version(*dblink_proxy, *dblink_conn, remote_version));
    OZ (ObPLDblinkUtil::print_full_name(alloc_, full_name, part1, part2, part3));
    OZ (dblink_name_resolve(dblink_proxy,
                            dblink_conn,
                            dblink_schema,
                            full_name,
                            schema_name,
                            object_name,
                            sub_object_name,
                            object_type,
                            alloc_));
    OV (static_cast<int64_t>(ObObjectType::PACKAGE) == object_type);
    OZ (get_dblink_type_by_name(dblink_proxy,
                                dblink_conn,
                                session_info,
                                schema_guard,
                                dblink_name,
                                schema_name,
                                object_name,
                                sub_object_name,
                                udt,
                                remote_version));
  }
  if (OB_NOT_NULL(dblink_proxy) && OB_NOT_NULL(dblink_conn)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = dblink_proxy->release_dblink(link_type, dblink_conn))) {
      LOG_WARN("failed to relese connection", K(tmp_ret));
    }
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  if (OB_SUCC(ret)) {
    if (NULL != udt) {
      if (ObPLTypeFrom::PL_TYPE_DBLINK == udt->get_type_from_origin()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("type is not supported", K(ret), KPC(udt));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, full_name.ptr());
      }
      if (OB_SUCC(ret)) {
        if (!udt->is_collection_type() && !udt->is_record_type()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "in dblink, composite types other than collection type and record type are");
        } else {
          const ObRecordType *record_type = NULL;
          if (udt->is_collection_type()) {
            const ObCollectionType *coll_type = static_cast<const ObCollectionType *>(udt);
            if (OB_ISNULL(coll_type)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("coll_type is NULL", K(ret));
            } else {
              const ObPLDataType &elem_type = coll_type->get_element_type();
              if (elem_type.is_obj_type()) {
                // do nothing
              } else if (elem_type.is_record_type()) {
                const pl::ObUserDefinedType *udt2 = NULL;
                OZ (get_dblink_type_by_id(extract_package_id(elem_type.get_user_type_id()),
                                          elem_type.get_user_type_id(), udt2));
                if (OB_SUCC(ret)) {
                  record_type = static_cast<const ObRecordType *>(udt2);
                }
              } else {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED,
                               "in dblink, collection element type must be basic types or record types, other types are");
              }
            }
          } else if (OB_ISNULL(record_type = static_cast<const ObRecordType *>(udt))){
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("record_type is NULL", K(ret));
          }
          if (OB_SUCC(ret) && OB_NOT_NULL(record_type)) {
            for (int64_t mem_idx = 0; OB_SUCC(ret) && mem_idx < record_type->get_member_count(); mem_idx++) {
              const ObPLDataType *mem_type = record_type->get_record_member_type(mem_idx);
              if (OB_ISNULL(mem_type)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("mem_type is NULL", K(ret));
              } else if (!mem_type->is_obj_type()) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED,
                               "in dblink, record type member types must be basic types, other types are");
              }
            }
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, full_name.ptr());
    }
    if (OB_SUCC(ret)) {
      if (!udt->is_collection_type() && !udt->is_record_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "composite types other than collection type and record type are");
      }
    }
  }
#endif
  return ret;
}

int ObPLDbLinkGuard::get_dblink_routine_infos(common::ObDbLinkProxy *dblink_proxy,
                                              common::sqlclient::ObISQLConnection *dblink_conn,
                                              sql::ObSQLSessionInfo &session_info,
                                              share::schema::ObSchemaGetterGuard &schema_guard,
                                              const ObString &dblink_name,
                                              const ObString &db_name,
                                              const ObString &pkg_name,
                                              const ObString &routine_name,
                                              common::ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos,
                                              uint32_t remote_version)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "PL dblink");
#else
  routine_infos.reset();
  const uint64_t tenant_id = MTL_ID();
  uint64_t dblink_id = OB_INVALID_ID;
  const share::schema::ObDbLinkSchema *dblink_schema = NULL;
  const ObPLDbLinkInfo *dblink_info = NULL;
  OZ (schema_guard.get_dblink_schema(tenant_id, dblink_name, dblink_schema));
  OX (dblink_id = dblink_schema->get_dblink_id());
  OV (OB_INVALID_ID != dblink_id, OB_DBLINK_NOT_EXIST_TO_ACCESS, dblink_id);
  OZ (get_dblink_info(dblink_id, dblink_info));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(dblink_info)) {
    ObPLDbLinkInfo *new_dblink_info = static_cast<ObPLDbLinkInfo *>(alloc_.alloc(sizeof(ObPLDbLinkInfo)));
    if (OB_ISNULL(new_dblink_info)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      new_dblink_info = new (new_dblink_info)ObPLDbLinkInfo(dblink_id,
                                                            next_link_object_id_,
                                                            alloc_,
                                                            session_info,
                                                            schema_guard,
                                                            dblink_proxy,
                                                            dblink_conn);
      new_dblink_info->set_remote_version(remote_version);
      dblink_info = new_dblink_info;
      OZ (dblink_infos_.push_back(dblink_info));
    }
  }
  OZ ((const_cast<ObPLDbLinkInfo *>(dblink_info))->get_routine_infos(dblink_proxy,
                                                                     dblink_conn,
                                                                     session_info,
                                                                     schema_guard,
                                                                     alloc_,
                                                                     dblink_name,
                                                                     db_name,
                                                                     pkg_name,
                                                                     routine_name,
                                                                     routine_infos,
                                                                     next_link_object_id_));
#endif
  return ret;
}

int ObPLDbLinkGuard::get_dblink_routine_info(uint64_t dblink_id,
                                             uint64_t pkg_id,
                                             uint64_t routine_id,
                                             const share::schema::ObRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "PL dblink");
#else
  const ObPLDbLinkInfo *dblink_info = NULL;
  if (OB_FAIL(get_dblink_info(dblink_id, dblink_info))) {
    LOG_WARN("get dblink info failed", K(ret), K(dblink_id));
  } else if (OB_ISNULL(dblink_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink_info is null", K(ret), K(dblink_id));
  } else if (OB_FAIL(dblink_info->get_routine_info(pkg_id, routine_id, routine_info))) {
    LOG_WARN("get routine info failed", K(ret), KP(dblink_info), K(pkg_id), K(routine_id));
  }
#endif
  return ret;
}

int ObPLDbLinkGuard::dblink_name_resolve(common::ObDbLinkProxy *dblink_proxy,
                                         common::sqlclient::ObISQLConnection *dblink_conn,
                                         const ObDbLinkSchema *dblink_schema,
                                         const common::ObString &full_name,
                                         common::ObString &schema,
                                         common::ObString &object_name,
                                         common::ObString &sub_object_name,
                                         int64_t &object_type,
                                         ObIAllocator &alloctor)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "PL dblink");
#else
  /*
  * dbms_utility.sql
  * PROCEDURE NAME_RESOLVE (NAME IN VARCHAR2,
  *                         CONTEXT IN NUMBER,
  *                         SCHEMA1 OUT VARCHAR2,
  *                         PART1 OUT VARCHAR2,
  *                         PART2 OUT VARCHAR2,
  *                         DBLINK OUT VARCHAR2,
  *                         PART1_TYPE OUT NUMBER,
  *                         OBJECT_NUMBER OUT NUMBER);
  *
  */
  const char *call_proc = "declare "
                          " object_number number; "
                          "begin "
                          " dbms_utility.name_resolve(:name, "
                          "                           :context, "
                          "                           :schema1, "
                          "                           :part1, "
                          "                           :part2, "
                          "                           :dblink, "
                          "                           :part1_type, "
                          "                           object_number); "
                          "end; ";
  if (OB_ISNULL(dblink_proxy) || OB_ISNULL(dblink_conn) || OB_ISNULL(dblink_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is NULL", K(ret), K(dblink_proxy), K(dblink_conn), K(dblink_schema));
  } else if (OB_FAIL(dblink_proxy->dblink_prepare(dblink_conn, call_proc, 7, &alloctor))) {
    LOG_WARN("prepare sql failed", K(ret), K(ObString(call_proc)));
  }
  if (OB_SUCC(ret)) {
    ObString full_name_copy = full_name;
    const int64_t ident_size = pl::OB_MAX_PL_IDENT_LENGTH + 1;
    int context = 1;
    char schema1[ident_size];
    char part1[ident_size];
    char part2[ident_size];
    char dblink[ident_size];
    int part1_type = -1;
    int32_t indicator = 0;
    memset(schema1, 0, ident_size);
    memset(part1, 0, ident_size);
    memset(part2, 0, ident_size);
    memset(dblink, 0, ident_size);
    bool is_oracle = (DblinkDriverProto::DBLINK_DRV_OCI
                      == static_cast<DblinkDriverProto>(dblink_schema->get_driver_proto()));
#define BIND_BASIC_BY_POS(param_pos, param, param_size, param_type, is_out_param)         \
    if (FAILEDx(dblink_proxy->dblink_bind_basic_type_by_pos(dblink_conn,    \
                                                            param_pos,      \
                                                            param,          \
                                                            param_size,     \
                                                            param_type,     \
                                                            indicator,      \
                                                            is_out_param))) {  \
      LOG_WARN("bind param failed", K(ret), K(param_pos), K(param_size), K(param_type)); \
    }
    BIND_BASIC_BY_POS(1, full_name_copy.ptr(), static_cast<int64_t>(full_name_copy.length() + (is_oracle ? 1 : 0)), ObObjType::ObVarcharType, false);
    BIND_BASIC_BY_POS(2, &context, static_cast<int64_t>(sizeof(int)), ObObjType::ObInt32Type, false);
    BIND_BASIC_BY_POS(3, schema1, ident_size, ObObjType::ObVarcharType, true);
    BIND_BASIC_BY_POS(4, part1, ident_size, ObObjType::ObVarcharType, true);
    BIND_BASIC_BY_POS(5, part2, ident_size, ObObjType::ObVarcharType, true);
    BIND_BASIC_BY_POS(6, dblink, ident_size, ObObjType::ObVarcharType, true);
    BIND_BASIC_BY_POS(7, &part1_type, static_cast<int64_t>(sizeof(int)), ObObjType::ObInt32Type, true);
    if (FAILEDx(dblink_proxy->dblink_execute_proc(dblink_conn))) {
      const DblinkDriverProto link_type = static_cast<DblinkDriverProto>(dblink_schema->get_driver_proto());
      if (OB_ERR_ILL_OBJ_FLAG == ret
          || OB_ERR_MISSING_IDENTIFIER == ret) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_WARN("invalid identifier", K(ret), K(full_name_copy));
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, full_name_copy.length(), full_name_copy.ptr());
      } else {
        LOG_WARN("read link failed", K(ret), K(ObString(call_proc)));
      }
    } else {
      switch (part1_type) {
        case OracleObjectType::ORA_PROCEUDRE:
          // procedure
          object_type = static_cast<int64_t>(ObObjectType::PROCEDURE);
        break;
        case OracleObjectType::ORA_FUNCTION:
          // function
          object_type = static_cast<int64_t>(ObObjectType::FUNCTION);
        break;
        case OracleObjectType::ORA_PACKAGE:
          // package
          object_type = static_cast<int64_t>(ObObjectType::PACKAGE);
        break;
        default: {
          ret = OB_ERR_NOT_VALID_ROUTINE_NAME;
          LOG_WARN("remote object type not support", K(ret), K(full_name));
        }
      }
      OZ (ob_write_string(alloctor, ObString(schema1), schema));
      OZ (ob_write_string(alloctor, ObString(part1), object_name));
      OZ (ob_write_string(alloctor, ObString(part2), sub_object_name));
    }
#undef BIND_BASIC_BY_POS
  }
  if (NULL != dblink_schema
      && NULL != dblink_proxy
      && NULL != dblink_conn
      && DblinkDriverProto::DBLINK_DRV_OCI == static_cast<DblinkDriverProto>(dblink_schema->get_driver_proto())) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = static_cast<ObOciConnection *>(dblink_conn)->free_oci_stmt())) {
      LOG_WARN("failed to close oci result", K(tmp_ret));
      ret = (OB_SUCC(ret) ? tmp_ret : ret);
    }
  }
#endif
  return ret;
}

int ObPLDbLinkGuard::get_dblink_type_by_name(common::ObDbLinkProxy *dblink_proxy,
                                             common::sqlclient::ObISQLConnection *dblink_conn,
                                             sql::ObSQLSessionInfo &session_info,
                                             share::schema::ObSchemaGetterGuard &schema_guard,
                                             const common::ObString &dblink_name,
                                             const common::ObString &db_name,
                                             const common::ObString &pkg_name,
                                             const common::ObString &udt_name,
                                             const pl::ObUserDefinedType *&udt,
                                             uint32_t remote_version)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "PL dblink");
#else
  const uint64_t tenant_id = MTL_ID();
  uint64_t dblink_id = OB_INVALID_ID;
  const share::schema::ObDbLinkSchema *dblink_schema = NULL;
  const ObPLDbLinkInfo *dblink_info = NULL;
  OZ (schema_guard.get_dblink_schema(tenant_id, dblink_name, dblink_schema));
  OV (OB_NOT_NULL(dblink_schema), OB_DBLINK_NOT_EXIST_TO_ACCESS, dblink_name);
  OV (OB_INVALID_ID != (dblink_id = dblink_schema->get_dblink_id()), OB_DBLINK_NOT_EXIST_TO_ACCESS, dblink_id);
  OZ (get_dblink_info(dblink_id, dblink_info));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(dblink_info)) {
    ObPLDbLinkInfo *new_dblink_info = static_cast<ObPLDbLinkInfo *>(alloc_.alloc(sizeof(ObPLDbLinkInfo)));
    if (OB_ISNULL(new_dblink_info)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      new_dblink_info = new (new_dblink_info)ObPLDbLinkInfo(dblink_id,
                                                            next_link_object_id_,
                                                            alloc_,
                                                            session_info,
                                                            schema_guard,
                                                            dblink_proxy,
                                                            dblink_conn);
      new_dblink_info->set_remote_version(remote_version);
      dblink_info = new_dblink_info;
      OZ (dblink_infos_.push_back(dblink_info));
    }
  }
  OZ ((const_cast<ObPLDbLinkInfo *>(dblink_info))->get_udt_by_name(dblink_proxy, dblink_conn, session_info,
       schema_guard, alloc_, dblink_name, db_name, pkg_name,
       udt_name, udt, next_link_object_id_));
#endif
  return ret;
}

int ObPLDbLinkGuard::get_dblink_type_by_id(const uint64_t mask_dblink_id,
                                           const uint64_t udt_id,
                                           const pl::ObUserDefinedType *&udt)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "PL dblink");
#else
  uint64_t dblink_id = mask_dblink_id & ~common::OB_MOCK_DBLINK_UDT_ID_MASK;
  const ObPLDbLinkInfo *dblink_info = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(dblink_info) && i < dblink_infos_.count(); i++) {
    if (OB_ISNULL(dblink_infos_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dblink_info is null", K(ret), K(i));
    } else if (dblink_id == dblink_infos_.at(i)->get_dblink_id()) {
      dblink_info = dblink_infos_.at(i);
      OZ (dblink_info->get_udt_by_id(udt_id, udt));
    }
  }
#endif
  return ret;
}

int ObPLDbLinkGuard::get_dblink_type_by_name(const uint64_t dblink_id,
                                             const common::ObString &db_name,
                                             const common::ObString &pkg_name,
                                             const common::ObString &udt_name,
                                             const pl::ObUserDefinedType *&udt)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "PL dblink");
#else
  const ObPLDbLinkInfo *dblink_info = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < dblink_infos_.count(); i++) {
    if (OB_ISNULL(dblink_infos_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dblink_info is null", K(ret), K(i));
    } else if (dblink_id == dblink_infos_.at(i)->get_dblink_id()) {
      dblink_info = dblink_infos_.at(i);
      bool find_pkg = false;
      OZ (dblink_info->get_udt_from_cache(db_name, pkg_name, udt_name, udt, find_pkg));
      break;
    }
  }
#endif
  return ret;
}

int ObPLDbLinkGuard::get_dblink_table_by_name(sql::ObSQLSessionInfo &session_info,
                                              share::schema::ObSchemaGetterGuard &schema_guard,
                                              const common::ObString &dblink_name,
                                              const common::ObString &db_name,
                                              const common::ObString &table_name,
                                              const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "PL dblink");
#else
  const uint64_t tenant_id = MTL_ID();
  uint64_t dblink_id = OB_INVALID_ID;
  const share::schema::ObDbLinkSchema *dblink_schema = NULL;
  const ObPLDbLinkInfo *dblink_info = NULL;
  CK (!table_name.empty());
  CK (!dblink_name.empty());
  OZ (schema_guard.get_dblink_schema(tenant_id, dblink_name, dblink_schema));
  OV (OB_NOT_NULL(dblink_schema), OB_DBLINK_NOT_EXIST_TO_ACCESS, dblink_name);
  OV (OB_INVALID_ID != (dblink_id = dblink_schema->get_dblink_id()), OB_DBLINK_NOT_EXIST_TO_ACCESS, dblink_id);
  for (uint64_t i = 0; OB_SUCC(ret) && OB_ISNULL(table_schema) && i < table_schemas_.count(); i++) {
    const ObTableSchema *t = table_schemas_.at(i);
    OV (OB_NOT_NULL(t));
    if (OB_SUCC(ret)
        && t->get_dblink_id() == dblink_id
        && db_name.compare(t->get_link_database_name())
        && table_name.compare(t->get_table_name_str())) {
      table_schema = t;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(table_schema)) {
    ObTableSchema *tmp_schema = NULL;
    uint64_t *scn = NULL;
    OZ (schema_guard.get_link_table_schema(tenant_id,
                                           dblink_id,
                                           db_name,
                                           table_name,
                                           alloc_,
                                           tmp_schema,
                                           &session_info,
                                           dblink_name,
                                           false/*is_reverse_link*/,
                                           scn));
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(tmp_schema)) {
        ObSqlString object_name;
        OZ (object_name.append_fmt("%.*s@%.*s",
                                   table_name.length(), table_name.ptr(),
                                   dblink_name.length(), dblink_name.ptr()));
        ret = OB_ERR_SP_UNDECLARED_VAR;
        LOG_WARN("dblink table not exist", K(ret));
        LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_VAR, object_name.string().length(), object_name.string().ptr());
      } else {
        tmp_schema->set_table_id(next_link_object_id_++);
        tmp_schema->set_link_table_id(tmp_schema->get_table_id());
        table_schema = tmp_schema;
        OZ (table_schemas_.push_back(tmp_schema));
      }
    }
  }
#endif
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLDbLinkGuard::get_dblink_info(const uint64_t dblink_id,
                                     const ObPLDbLinkInfo *&dblink_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(dblink_info) && i < dblink_infos_.count(); i++) {
    if (OB_ISNULL(dblink_infos_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dblink_info is null", K(ret), K(i));
    } else if (dblink_infos_.at(i)->get_dblink_id() == dblink_id) {
      dblink_info = dblink_infos_.at(i);
    }
  }
  return ret;
}

int ObPLDbLinkGuard::check_remote_version(common::ObDbLinkProxy &dblink_proxy,
                                          common::sqlclient::ObISQLConnection &dblink_conn,
                                          uint32_t &remote_version)
{
  int ret = OB_SUCCESS;
  if (DblinkDriverProto::DBLINK_DRV_OB == dblink_conn.get_dblink_driver_proto()) {
    int part1 = 0;
    int part2 = 0;
    int part3 = 0;
    int32_t ind = 0;
    int64_t size = static_cast<int64_t>(sizeof(int));
    const char *anonymous_block = "declare  "
                                  "   version_str varchar2(100); "
                                  " begin "
                                  "   select OB_VERSION() into version_str from dual; "
                                  "   :1 := TO_NUMBER(REGEXP_SUBSTR(version_str, '[^.]+', 1, 1)); "
                                  "   :2 := TO_NUMBER(REGEXP_SUBSTR(version_str, '[^.]+', 1, 2)); "
                                  "   :3 := TO_NUMBER(REGEXP_SUBSTR(version_str, '[^.]+', 1, 3)); "
                                  " end; ";
    OZ (dblink_proxy.dblink_prepare(&dblink_conn, anonymous_block, 3, &alloc_));
    OZ (dblink_proxy.dblink_bind_basic_type_by_pos(&dblink_conn, 1, &part1, size, ObObjType::ObInt32Type, ind, true));
    OZ (dblink_proxy.dblink_bind_basic_type_by_pos(&dblink_conn, 2, &part2, size, ObObjType::ObInt32Type, ind, true));
    OZ (dblink_proxy.dblink_bind_basic_type_by_pos(&dblink_conn, 3, &part3, size, ObObjType::ObInt32Type, ind, true));
    OZ (dblink_proxy.dblink_execute_proc(&dblink_conn));
    if (OB_SUCC(ret)) {
      bool not_support = false;
      if (part1 < 4) {
        not_support = true;
      } else if (part1 == 4) {
        if (part2 < 2) {
          not_support = true;
        } else if (part2 == 2) {
          if (part3 < 4) {
            not_support = true;
          }
        } else if (part2 == 3) {
          if (part3 < 3) {
            not_support = true;
          }
        }
      }
      if (not_support) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support dblink", K(ret), K(part1), K(part2), K(part3));
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
          "oceanbase PL dblink oceanbase PL oracle mode, remote database version less then 4.2.4.0");
      } else {
        remote_version = ObPLDbLinkInfo::gen_remote_version(static_cast<uint32_t>(part1),
                                                            static_cast<uint32_t>(part2),
                                                            static_cast<uint32_t>(part3));
      }
    }
  }
  return ret;
}
#endif

int ObPLDbLinkGuard::get_dblink_table_by_type_id(const uint64_t type_id,
                                                 const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  table_schema = NULL;
  uint64_t dblink_id = extract_package_id(type_id) & ~common::OB_MOCK_DBLINK_UDT_ID_MASK;
  uint64_t table_id = extract_type_id(type_id);
  for (int64_t i = 0; OB_SUCC(ret) && NULL == table_schema && i < table_schemas_.count(); i++) {
    const ObTableSchema *t_schema = table_schemas_.at(i);
    CK (OB_NOT_NULL(t_schema));
    if (OB_SUCC(ret)
        && t_schema->get_dblink_id() == dblink_id
        && t_schema->get_table_id() == table_id) {
      table_schema = t_schema;
    }
  }
#endif
  return ret;
}

}
}
