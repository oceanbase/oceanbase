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

#include "ob_pl_ddl_service.h"
#include "rootserver/ob_ddl_service.h"
#include "share/schema/ob_error_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_routine_info.h"
#include "share/schema/ob_package_info.h"
#include "share/schema/ob_trigger_info.h"

namespace oceanbase
{
using rootserver::ObDDLSQLTransaction;
using rootserver::ObDDLOperator;

namespace rootserver
{

int ObPLDDLService::create_routine(const obrpc::ObCreateRoutineArg &arg,
                                   obrpc::ObRoutineDDLRes *res,
                                   rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_env_before_ddl(schema_guard, arg, ddl_service))) {
    LOG_WARN("check env failed", K(arg), K(ret));
  } else {
    ObRoutineInfo routine_info = arg.routine_info_;
    const ObRoutineInfo* old_routine_info = NULL;
    uint64_t tenant_id = routine_info.get_tenant_id();
    ObString database_name = arg.db_name_;
    bool is_or_replace = lib::is_oracle_mode() ? arg.is_or_replace_ : arg.is_need_alter_;
    bool is_inner = lib::is_mysql_mode() ? arg.is_or_replace_ : false;
    const ObDatabaseSchema *db_schema = NULL;
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
    } else if (!is_inner && db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create routine of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    } else {
      routine_info.set_database_id(db_schema->get_database_id());
    }
    if (OB_SUCC(ret)
        && database_name.case_compare(OB_SYS_DATABASE_NAME) != 0
        && lib::is_oracle_mode()) {
      if (OB_FAIL(schema_guard.get_user_info(tenant_id, database_name, ObString(OB_DEFAULT_HOST_NAME), user_info))) {
        LOG_WARN("failed to get user info", K(ret), K(database_name));
      } else if (OB_ISNULL(user_info)) {
        ret = OB_USER_NOT_EXIST;
        LOG_WARN("user is does not exist", K(ret), K(database_name));
      } else if (OB_INVALID_ID == user_info->get_user_id()) {
        ret = OB_USER_NOT_EXIST;
        LOG_WARN("user id is invalid", K(ret), K(database_name));
      } else {
        routine_info.set_owner_id(user_info->get_user_id());
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<ObSchemaType> conflict_schema_types;
      if (OB_FAIL(schema_guard.check_oracle_object_exist(tenant_id,
          db_schema->get_database_id(), routine_info.get_routine_name(), ROUTINE_SCHEMA,
          routine_info.get_routine_type(), is_or_replace, conflict_schema_types))) {
        LOG_WARN("fail to check oracle_object exist", K(ret), K(routine_info.get_routine_name()));
      } else if (conflict_schema_types.count() > 0) {
        // 这里检查 oracle 模式下新对象的名字是否已经被其他对象占用了
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by an existing object in oralce mode",
                  K(ret), K(routine_info.get_routine_name()),
                  K(conflict_schema_types));
      }
    }
    bool exist = false;
    if (OB_SUCC(ret)) {
      if (routine_info.get_routine_type() == ROUTINE_PROCEDURE_TYPE) {
        if (OB_FAIL(schema_guard.check_standalone_procedure_exist(tenant_id, db_schema->get_database_id(),
                                                                  routine_info.get_routine_name(), exist))) {
          LOG_WARN("failed to check procedure info exist", K(routine_info), K(ret));
        } else if (exist && !is_or_replace) {
          ret = OB_ERR_SP_ALREADY_EXISTS;
          LOG_USER_ERROR(OB_ERR_SP_ALREADY_EXISTS, "PROCEDURE",
                          routine_info.get_routine_name().length(), routine_info.get_routine_name().ptr());
        } else if (exist && is_or_replace) {
          if (OB_FAIL(schema_guard.get_standalone_procedure_info(tenant_id, db_schema->get_database_id(),
                                                                  routine_info.get_routine_name(), old_routine_info))) {
            LOG_WARN("failed to get standalone procedure info", K(routine_info), K(ret));
          } else if (OB_ISNULL(old_routine_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("old routine info is NULL", K(ret));
          }
        }
      } else {
        if (OB_FAIL(schema_guard.check_standalone_function_exist(tenant_id, db_schema->get_database_id(),
                                                                  routine_info.get_routine_name(), exist))) {
          LOG_WARN("failed to check function info exist", K(routine_info), K(ret));
        } else if (exist && !is_or_replace) {
          ret = OB_ERR_SP_ALREADY_EXISTS;
          LOG_USER_ERROR(OB_ERR_SP_ALREADY_EXISTS, "FUNCTION",
                          routine_info.get_routine_name().length(), routine_info.get_routine_name().ptr());
        } else if (exist && is_or_replace) {
          if (OB_FAIL(schema_guard.get_standalone_function_info(tenant_id, db_schema->get_database_id(),
                                                                routine_info.get_routine_name(), old_routine_info))) {
            LOG_WARN("failed to get standalone function info", K(routine_info), K(ret));
          } else if (OB_ISNULL(old_routine_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("old routine info is NULL", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObErrorInfo error_info = arg.error_info_;
        ObSArray<ObDependencyInfo> &dep_infos = const_cast<ObSArray<ObDependencyInfo> &>(arg.dependency_infos_);
        if (OB_FAIL(create_routine(routine_info,
                                   old_routine_info,
                                   (exist && is_or_replace),
                                   error_info,
                                   dep_infos,
                                   &arg.ddl_stmt_str_,
                                   schema_guard,
                                   ddl_service))) {
          LOG_WARN("failed to replace routine", K(routine_info), K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(res)) {
      res->store_routine_schema_version_ = routine_info.get_schema_version();
    }
  }
  return ret;
}

int ObPLDDLService::create_routine(ObRoutineInfo &routine_info,
                                   const ObRoutineInfo* old_routine_info,
                                   bool replace,
                                   ObErrorInfo &error_info,
                                   ObIArray<ObDependencyInfo> &dep_infos,
                                   const ObString *ddl_stmt_str,
                                   share::schema::ObSchemaGetterGuard &schema_guard,
                                   rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  CK((replace && OB_NOT_NULL(old_routine_info)) || (!replace && OB_ISNULL(old_routine_info)));
  CK (OB_NOT_NULL(ddl_service.schema_service_) && OB_NOT_NULL(ddl_service.sql_proxy_));
  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = routine_info.get_tenant_id();
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);

    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    }
    if (OB_SUCC(ret)) {
      if (replace) {
        if (OB_FAIL(pl_operator.replace_routine(routine_info,
                                                 old_routine_info,
                                                 trans,
                                                 error_info,
                                                 dep_infos,
                                                 ddl_stmt_str))) {
          LOG_WARN("replace routine failded", K(routine_info), K(ret));
        }
      } else {
        if (OB_FAIL(pl_operator.create_routine(routine_info,
                                               trans,
                                               error_info,
                                               dep_infos,
                                               ddl_stmt_str))) {
          LOG_WARN("create procedure failed", K(ret), K(routine_info));
        }
      }
    }
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
    uint64_t data_version = 0;
    if (OB_FAIL(ret)) {
    } else if (replace) {
    } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
      LOG_WARN("failed to get compat mode", K(ret), K(tenant_id));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get data version", K(tenant_id));
    } else if (sql::ObSQLUtils::is_data_version_ge_422_or_431(data_version)
               && lib::Worker::CompatMode::MYSQL == compat_mode) {
      const ObSysVarSchema *sys_var = NULL;
      ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
      ObObj val;
      if (OB_FAIL(schema_guard.get_tenant_system_variable(tenant_id, SYS_VAR_AUTOMATIC_SP_PRIVILEGES, sys_var))) {
        LOG_WARN("fail to get tenant var schema", K(ret));
      } else if (OB_ISNULL(sys_var)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys variable schema is null", KR(ret));
      } else if (OB_FAIL(sys_var->get_value(&alloc, NULL, val))) {
        LOG_WARN("fail to get charset var value", K(ret));
      } else {
        bool grant_priv = val.get_bool();
        if (grant_priv) {
          int64_t db_id = routine_info.get_database_id();
          const ObDatabaseSchema* database_schema = NULL;
          if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_id, database_schema))) {
            LOG_WARN("get database schema failed", K(ret));
          } else if (OB_ISNULL(database_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("database schema should not be null", K(ret));
          } else {
            ObRoutinePrivSortKey routine_key(tenant_id, routine_info.get_owner_id(),
                                              database_schema->get_database_name_str(),
                                              routine_info.get_routine_name(), routine_info.is_procedure() ?
                                              ObRoutineType::ROUTINE_PROCEDURE_TYPE : ObRoutineType::ROUTINE_FUNCTION_TYPE);
            ObPrivSet priv_set = (OB_PRIV_EXECUTE | OB_PRIV_ALTER_ROUTINE);
            int64_t option = 0;
            const bool gen_ddl_stmt = false;
            const ObUserInfo *user_info = NULL;
            if (OB_FAIL(schema_guard.get_user_info(tenant_id,
                                                   routine_info.get_owner_id(),
                                                   user_info))) {
              LOG_WARN("failed to get user info", K(ret));
            } else if (OB_ISNULL(user_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("user info is unexpeced null", K(ret));
            } else if (OB_FAIL(pl_operator.grant_routine(routine_key,
                                                          priv_set,
                                                          trans,
                                                          option,
                                                          gen_ddl_stmt,
                                                          user_info->get_user_name_str(),
                                                          user_info->get_host_name_str()))) {
              LOG_WARN("fail to grant routine", K(ret), K(routine_key), K(priv_set));
            }
          }
        }
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPLDDLService::alter_routine(const obrpc::ObCreateRoutineArg &arg,
                                  obrpc::ObRoutineDDLRes* res,
                                  rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_env_before_ddl(schema_guard, arg, ddl_service))) {
    LOG_WARN("check env failed", K(arg), K(ret));
  } else {
    ObErrorInfo error_info = arg.error_info_;
    const ObRoutineInfo *routine_info = NULL;
    const uint64_t tenant_id = arg.routine_info_.get_tenant_id();
    if (OB_FAIL(schema_guard.get_routine_info(tenant_id, arg.routine_info_.get_routine_id(), routine_info))) {
      LOG_WARN("failed to get routine info", K(ret), K(tenant_id));
    } else if (OB_ISNULL(routine_info)) {
      ret = OB_ERR_SP_DOES_NOT_EXIST;
      LOG_WARN("routine info is not exist!", K(ret), K(arg.routine_info_));
    }
    if (OB_FAIL(ret)) {
    } else if ((lib::is_oracle_mode() && arg.is_or_replace_) ||
                (lib::is_mysql_mode() && arg.is_need_alter_)) {
      if (OB_FAIL(create_routine(arg, res, ddl_service))) {
        LOG_WARN("failed to alter routine with create", K(ret));
      }
    } else {
      if (OB_FAIL(alter_routine(*routine_info, error_info, &arg.ddl_stmt_str_, schema_guard, ddl_service))) {
        LOG_WARN("alter routine failed", K(ret), K(arg.routine_info_), K(error_info));
      } else if (OB_NOT_NULL(res)) {
        res->store_routine_schema_version_ = routine_info->get_schema_version();
      }
    }
  }
  return ret;
}

int ObPLDDLService::alter_routine(const ObRoutineInfo &routine_info,
                                  ObErrorInfo &error_info,
                                  const ObString *ddl_stmt_str,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service.schema_service_) || OB_ISNULL(ddl_service.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    const uint64_t tenant_id = routine_info.get_tenant_id();
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed!", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(ObDependencyInfo::modify_dep_obj_status(trans,
                                                                tenant_id,
                                                                routine_info.get_routine_id(),
                                                                pl_operator,
                                                                *ddl_service.schema_service_))) {
      LOG_WARN("failed to modify obj status", K(ret));
    } else if (OB_FAIL(pl_operator.alter_routine(routine_info, trans, error_info, ddl_stmt_str))) {
      LOG_WARN("alter routine failed!", K(ret));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed!", K(ret), K(temp_ret));
        ret = OB_SUCCESS == ret ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed!", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObPLDDLService::drop_routine(const ObDropRoutineArg &arg,
                                 rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    uint64_t tenant_id = arg.tenant_id_;
    const ObString &db_name = arg.db_name_;
    const ObString &routine_name = arg.routine_name_;
    ObRoutineType routine_type = arg.routine_type_;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    /*!
     * 兼容mysql行为:
     * create database test;
     * use test;
     * drop database test;
     * drop function if exists no_such_func; -- warning 1035
     * drop procedure if exists no_such_proc; -- error 1046
     * drop function no_such_func; --error 1035
     * drop procedure no_such_proc; --error 1046
     */
    if (db_name.empty()) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("no database selected", K(ret), K(db_name));
    } else if (OB_FAIL(ddl_service.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(ddl_service.check_parallel_ddl_conflict(schema_guard, arg))) {
      LOG_WARN("check parallel ddl conflict failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create procedure of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    }

    if (OB_SUCC(ret)) {
      bool exist = false;
      const ObRoutineInfo *routine_info = NULL;
      if (ROUTINE_PROCEDURE_TYPE == routine_type) {
        if (OB_FAIL(schema_guard.check_standalone_procedure_exist(tenant_id, db_schema->get_database_id(),
                                                                  routine_name, exist))) {
          LOG_WARN("failed to check standalone procedure info exist", K(routine_name), K(ret));
        } else if (exist) {
          if (OB_FAIL(schema_guard.get_standalone_procedure_info(tenant_id, db_schema->get_database_id(),
                                                                 routine_name, routine_info))) {
            LOG_WARN("get procedure info failed", K(ret));
          }
        } else if (!arg.if_exist_) {
          ret = OB_ERR_SP_DOES_NOT_EXIST;
          LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "PROCEDURE", db_name.length(), db_name.ptr(),
                         routine_name.length(), routine_name.ptr());
        }
      } else {
        if (OB_FAIL(schema_guard.check_standalone_function_exist(tenant_id, db_schema->get_database_id(),
                                                                 routine_name, exist))) {
          LOG_WARN("failed to check standalone function info exist", K(routine_name), K(ret));
        } else if (exist) {
          if (OB_FAIL(schema_guard.get_standalone_function_info(tenant_id, db_schema->get_database_id(),
                                                                routine_name, routine_info))) {
            LOG_WARN("get function info failed", K(ret));
          }
        } else if (!arg.if_exist_) {
          ret = OB_ERR_SP_DOES_NOT_EXIST;
          LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "FUNCTION", db_name.length(), db_name.ptr(),
                         routine_name.length(), routine_name.ptr());
        }
      }

      if (OB_SUCC(ret) && !OB_ISNULL(routine_info)) {
        ObErrorInfo error_info = arg.error_info_;
        if (OB_FAIL(drop_routine(*routine_info,
                                 error_info,
                                 &arg.ddl_stmt_str_,
                                 schema_guard,
                                 ddl_service))) {
          LOG_WARN("drop routine failed", K(ret), K(routine_name), K(routine_info));
        }
      }
    }
    if (OB_ERR_NO_DB_SELECTED == ret && ROUTINE_FUNCTION_TYPE == routine_type) {
      if (arg.if_exist_) {
        ret = OB_SUCCESS;
        LOG_USER_WARN(OB_ERR_SP_DOES_NOT_EXIST, "FUNCTION (UDF)",
                      db_name.length(), db_name.ptr(),
                      routine_name.length(), routine_name.ptr());
      } else {
        ret = OB_ERR_SP_DOES_NOT_EXIST;
        LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "FUNCTION (UDF)",
                      db_name.length(), db_name.ptr(),
                      routine_name.length(), routine_name.ptr());
        LOG_WARN("FUNCTION (UDF) does not exists", K(ret), K(routine_name), K(db_name));
      }
    }
  }
  return ret;
}

int ObPLDDLService::drop_routine(const ObRoutineInfo &routine_info,
                                 ObErrorInfo &error_info,
                                 const ObString *ddl_stmt_str,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service.schema_service_) || OB_ISNULL(ddl_service.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    const uint64_t tenant_id = routine_info.get_tenant_id();
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(ObDependencyInfo::modify_dep_obj_status(trans,
                                                               tenant_id,
                                                               routine_info.get_routine_id(),
                                                               pl_operator,
                                                               *ddl_service.schema_service_))) {
      LOG_WARN("failed to modify obj status", K(ret));
    } else if (OB_FAIL(pl_operator.drop_routine(routine_info, trans, error_info, ddl_stmt_str))) {
      LOG_WARN("drop procedure failed", K(ret), K(routine_info));
    } else {
      lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
      uint64_t data_version = 0;
      if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
          LOG_WARN("failed to get compat mode", K(ret), K(tenant_id));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
        LOG_WARN("fail to get data version", K(tenant_id));
      } else if (sql::ObSQLUtils::is_data_version_ge_422_or_431(data_version)
                 && lib::Worker::CompatMode::MYSQL == compat_mode) {
        const ObSysVarSchema *sys_var = NULL;
        ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
        ObObj val;
        if (OB_FAIL(schema_guard.get_tenant_system_variable(tenant_id, SYS_VAR_AUTOMATIC_SP_PRIVILEGES, sys_var))) {
          LOG_WARN("fail to get tenant var schema", K(ret));
        } else if (OB_ISNULL(sys_var)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sys variable schema is null", KR(ret));
        } else if (OB_FAIL(sys_var->get_value(&alloc, NULL, val))) {
          LOG_WARN("fail to get charset var value", K(ret));
        } else if (val.get_bool()) {
          int64_t db_id = routine_info.get_database_id();
          const ObDatabaseSchema* database_schema = NULL;
          ObSEArray<const ObUserInfo*, 10> user_infos;
          if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_id, database_schema))) {
            LOG_WARN("get database schema failed", K(ret));
          } else if (OB_ISNULL(database_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("database schema is null", K(ret));
          } else if (OB_FAIL(schema_guard.get_user_infos_with_tenant_id(tenant_id, user_infos))) {
            LOG_WARN("fail to get all user in tenant", K(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < user_infos.count(); ++i) {
            const ObUserInfo *user_info = user_infos.at(i);
            if (OB_ISNULL(user_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null", K(ret));
            } else {
              ObRoutinePrivSortKey routine_key(tenant_id, user_info->get_user_id(),
                                            database_schema->get_database_name_str(),
                                            routine_info.get_routine_name(), routine_info.is_procedure() ?
                                            ObRoutineType::ROUTINE_PROCEDURE_TYPE : ObRoutineType::ROUTINE_FUNCTION_TYPE);
              ObPrivSet priv_set = (OB_PRIV_EXECUTE | OB_PRIV_ALTER_ROUTINE);
              bool gen_ddl_stmt = false;
              if (OB_FAIL(pl_operator.revoke_routine(routine_key, priv_set, trans, false, gen_ddl_stmt))) {
                LOG_WARN("fail to grant routine", K(ret), K(routine_key), K(priv_set));
              }
            }
          }
        }
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPLDDLService::create_udt(const obrpc::ObCreateUDTArg &arg,
                                obrpc::ObRoutineDDLRes *res,
                                rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_env_before_ddl(schema_guard, arg, ddl_service))) {
    LOG_WARN("check env failed", K(ret));
  } else {
    ObUDTTypeInfo udt_info = arg.udt_info_;
    const ObUDTTypeInfo* old_udt_info = NULL;
    uint64_t tenant_id = udt_info.get_tenant_id();
    ObString database_name = arg.db_name_;
    bool is_or_replace = arg.is_or_replace_;
    bool exist_valid_udt = arg.exist_valid_udt_;
    const ObDatabaseSchema *db_schema = NULL;
    if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create udt of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    } else {
      udt_info.set_database_id(db_schema->get_database_id());
    }
    if (OB_SUCC(ret)) {
      ObArray<ObSchemaType> conflict_schema_types;
      if (OB_FAIL(schema_guard.check_oracle_object_exist(tenant_id, db_schema->get_database_id(),
          udt_info.get_type_name(), UDT_SCHEMA, INVALID_ROUTINE_TYPE, is_or_replace,
          conflict_schema_types))) {
        LOG_WARN("fail to check oracle_object exist", K(ret), K(udt_info.get_type_name()));
      } else if (1 == conflict_schema_types.count() && UDT_SCHEMA == conflict_schema_types.at(0)) {
        // judge later
      } else if (conflict_schema_types.count() > 0) {
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by an existing object", K(ret), K(udt_info.get_type_name()),
            K(conflict_schema_types));
      }
    }
    bool exist = false;
    ObUDTTypeCode type_code = udt_info.is_object_body_ddl()
                            ? UDT_TYPE_OBJECT_BODY
                            : static_cast<ObUDTTypeCode>(udt_info.get_typecode());
    if (OB_SUCC(ret)) {
      bool udt_dependency_feature_enabled =
          GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_1_0 ||
          (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_2_0 &&
           GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0);
      if (OB_FAIL(schema_guard.check_udt_exist(tenant_id,
                                               db_schema->get_database_id(),
                                               OB_INVALID_ID,
                                               type_code,
                                               udt_info.get_type_name(),
                                               exist))) {
        LOG_WARN("failed to check udt info exist", K(udt_info), K(ret));
      } else if (udt_info.is_object_body_ddl()) {
        // udt type body do not check validation during resolving
        if (exist && !is_or_replace) {
          ret = OB_ERR_SP_ALREADY_EXISTS;
          LOG_USER_ERROR(OB_ERR_SP_ALREADY_EXISTS, "UDT",
                         udt_info.get_type_name().length(), udt_info.get_type_name().ptr());
        }
      } else if (udt_dependency_feature_enabled) {
        if (!exist && exist_valid_udt) {
          ret = OB_ERR_PARALLEL_DDL_CONFLICT;
          LOG_WARN("valid udt at resolve stage can not be found any more at rootservice stage, "
                   "could be dropped by another parallel ddl",
                   K(ret), K(database_name), K(udt_info.get_type_name()));
        } else if (exist && exist_valid_udt && !is_or_replace) {
          ret = OB_ERR_SP_ALREADY_EXISTS;
          LOG_USER_ERROR(OB_ERR_SP_ALREADY_EXISTS, "UDT",
                         udt_info.get_type_name().length(), udt_info.get_type_name().ptr());
        } else {
          // proceed only if 1)udt does not exist or is invalid or 2)ddl specified `or replace`
        }
      } else {
        // not udt_dependency_feature_enabled && not type body
        if (exist && !is_or_replace) {
          ret = OB_ERR_SP_ALREADY_EXISTS;
          LOG_USER_ERROR(OB_ERR_SP_ALREADY_EXISTS, "UDT",
                         udt_info.get_type_name().length(), udt_info.get_type_name().ptr());
        }
      }

      if (exist && OB_SUCC(ret)) {
        if (OB_FAIL(schema_guard.get_udt_info(tenant_id,
                                              db_schema->get_database_id(),
                                              OB_INVALID_ID,
                                              udt_info.get_type_name(),
                                              type_code,
                                              old_udt_info))) {
          LOG_WARN("failed to get udt info", K(udt_info), K(ret));
        } else if (OB_ISNULL(old_udt_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("old udt info is NULL", K(ret));
        }
      }
      if (OB_SUCC(ret)
          && is_inner_pl_udt_id(udt_info.get_type_id())
          && type_code != UDT_TYPE_OBJECT_BODY) {
        if (!exist) {
          if (OB_FAIL(schema_guard.get_udt_info(tenant_id, udt_info.get_type_id(), old_udt_info))) {
            LOG_WARN("failed to get udt info", K(ret), K(tenant_id), K(udt_info.get_type_id()));
          } else if (OB_NOT_NULL(old_udt_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("type id already used by other udt", K(ret), KPC(old_udt_info));
          }
        } else {
          CK (OB_NOT_NULL(old_udt_info));
          OV (old_udt_info->get_type_id() == udt_info.get_type_id(),
              OB_ERR_UNEXPECTED, KPC(old_udt_info), K(udt_info));
        }
      }

      if (OB_SUCC(ret)) {
        ObErrorInfo error_info = arg.error_info_;
        ObSArray<ObRoutineInfo> &public_routine_infos =
            const_cast<ObSArray<ObRoutineInfo> &>(arg.public_routine_infos_);
        ObSArray<ObDependencyInfo> &dep_infos =
            const_cast<ObSArray<ObDependencyInfo> &>(arg.dependency_infos_);
        // 1) replace an existing udt
        // 2) name of the created type confilicts with an invalid type (support after 4.2.2)
        bool need_replace = (exist && is_or_replace);
        if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_2_0) {
          need_replace = need_replace || (exist && !is_or_replace && !exist_valid_udt);
        }
        if (OB_FAIL(create_udt(udt_info,
                               old_udt_info,
                               public_routine_infos,
                               error_info,
                               schema_guard,
                               dep_infos,
                               &arg.ddl_stmt_str_,
                               need_replace,
                               exist_valid_udt,
                               arg.is_force_,
                               ddl_service))) {
          LOG_WARN("failed to create udt", K(udt_info), K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(res)) {
      res->store_routine_schema_version_ = udt_info.get_schema_version();
    }
  }
  return ret;
}

int ObPLDDLService::create_udt(ObUDTTypeInfo &udt_info,
                              const ObUDTTypeInfo* old_udt_info,
                              ObIArray<ObRoutineInfo> &public_routine_infos,
                              ObErrorInfo &error_info,
                              ObSchemaGetterGuard &schema_guard,
                              ObIArray<ObDependencyInfo> &dep_infos,
                              const ObString *ddl_stmt_str,
                              bool need_replace,
                              bool exist_valid_udt,
                              bool specify_force,
                              rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (need_replace == OB_ISNULL(old_udt_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value contradict", K(ret), K(need_replace), K(old_udt_info));
  } else if (OB_ISNULL(ddl_service.schema_service_) || OB_ISNULL(ddl_service.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    const uint64_t tenant_id = udt_info.get_tenant_id();
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    }
    bool udt_dependency_feature_enabled =
        GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_1_0 ||
        (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_2_0 &&
         GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0);
    if (OB_FAIL(ret)) {
    } else if (need_replace) {
      if (udt_dependency_feature_enabled && exist_valid_udt) {
        // check if the udt which is to be replaced has any type or table dependent
        ObArray<CriticalDepInfo> objs;
        if (OB_ISNULL(old_udt_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("value contradict", K(ret), K(need_replace), K(old_udt_info));
        } else if (OB_FAIL(ObDependencyInfo::collect_all_dep_objs(tenant_id,
                                                                  old_udt_info->get_type_id(),
                                                                  udt_info.get_object_type(),
                                                                  trans,
                                                                  objs))) {
          // TODO: @haohao.hao type body id design flaw
          // Here we pass `udt_info.get_object_type()` to `collect_all_dep_objs` instead of
          // `old_udt_info->get_object_type()`, because the type body and type share the same id
          // (which is a design flaw that needs to be fixed), and the schema cache uses the object
          // id as the key for storage, which casues retrieving the wrong schema from the cache.
          LOG_WARN("failed to collect all dependent objects", K(ret));
        } else {
          bool has_type_dep_obj = false;
          bool has_table_dep_obj = false;
          for (int64_t i = 0; i < objs.count(); i++) {
            schema::ObObjectType dep_obj_type =
                static_cast<schema::ObObjectType>(objs.at(i).element<1>());
            if (schema::ObObjectType::TABLE == dep_obj_type) {
              has_table_dep_obj = true;
            } else if (schema::ObObjectType::TYPE == dep_obj_type) {
              has_type_dep_obj = true;
            }
          }
          if (!has_type_dep_obj && !has_table_dep_obj) {
            // pass
          } else if (!specify_force && (has_type_dep_obj || has_table_dep_obj)) {
            ret = OB_ERR_HAS_TYPE_OR_TABLE_DEPENDENT;
            LOG_WARN("cannot drop or replace a type with type or table dependents",
                     K(ret), K(objs));
          } else if (specify_force && has_table_dep_obj) {
            // create or replace type force will not replace types with table dependents,
            // although ob does not support udt column in tables for now.
            ret = OB_ERR_REPLACE_TYPE_WITH_TABLE_DEPENDENT;
            LOG_WARN("cannot replace a type with table dependents", K(ret), K(objs));
          } else if (specify_force && has_type_dep_obj) {
            // pass
          }
          if (OB_SUCC(ret)
              && OB_FAIL(ObDependencyInfo::batch_invalidate_dependents(
                     objs, trans, tenant_id, old_udt_info->get_type_id()))) {
            LOG_WARN("invalidate dependents failed");
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(pl_operator.replace_udt(udt_info,
                                                  old_udt_info,
                                                  trans,
                                                  error_info,
                                                  public_routine_infos,
                                                  schema_guard,
                                                  dep_infos,
                                                  ddl_stmt_str))) {
        LOG_WARN("replace udt failded", K(udt_info), K(ret));
      }
    } else {
      if ((GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_2_0) && exist_valid_udt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value contradict", K(ret), K(need_replace), K(exist_valid_udt));
      } else if (OB_FAIL(pl_operator.create_udt(udt_info,
                                                 trans,
                                                 error_info,
                                                 public_routine_infos,
                                                 schema_guard,
                                                 dep_infos,
                                                 ddl_stmt_str))) {
        LOG_WARN("create udt failed", K(ret), K(udt_info));
      }
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}


int ObPLDDLService::drop_udt(const ObDropUDTArg &arg,
                             rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_env_before_ddl(schema_guard, arg, ddl_service))) {
    LOG_WARN("check env failed", K(arg), K(ret));
  } else {
    uint64_t tenant_id = arg.tenant_id_;
    const ObString &db_name = arg.db_name_;
    const ObString &udt_name = arg.udt_name_;
    const bool is_force = (1 == arg.force_or_validate_); // 0: none, 1: force, 2: valiate
    const bool exist_valid_udt = arg.exist_valid_udt_;
    const ObDatabaseSchema *db_schema = NULL;
    if (db_name.empty()) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("no database selected", K(ret), K(db_name));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create udt of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    }

    if (OB_SUCC(ret)) {
      bool exist = false;
      const ObUDTTypeInfo *udt_info = NULL;
      ObUDTTypeInfo udt;
      const ObUDTTypeCode type_code = arg.is_type_body_ ? UDT_TYPE_OBJECT_BODY : UDT_TYPE_OBJECT;
      if (OB_FAIL(schema_guard.check_udt_exist(tenant_id,
                                              db_schema->get_database_id(),
                                              OB_INVALID_ID, type_code,
                                              udt_name, exist))) {
          LOG_WARN("failed to check udt info exist", K(udt_name), K(ret));
      } else if (!exist && exist_valid_udt) {
        ret = OB_ERR_PARALLEL_DDL_CONFLICT;
        LOG_WARN("valid udt at resolve stage can not be found any more at rootservice stage, "
                 "could be dropped by another parallel ddl", K(ret), K(udt_name));
      } else if (exist) {
        if (OB_FAIL(schema_guard.get_udt_info(tenant_id,
                                              db_schema->get_database_id(),
                                              OB_INVALID_ID,
                                              udt_name, type_code, udt_info))) {
          LOG_WARN("get udt info failed", K(ret));
        }
      } else if (!arg.if_exist_) {
        ret = OB_ERR_SP_DOES_NOT_EXIST;
        LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "UDT", db_name.length(), db_name.ptr(),
                      udt_name.length(), udt_name.ptr());
      }

      if (!arg.is_type_body_) {
        if (OB_SUCC(ret) && !OB_ISNULL(udt_info)) {
          if (OB_FAIL(udt.assign(*udt_info))) {
            LOG_WARN("assign udt info failed", K(ret), KPC(udt_info));
          } else if (udt.is_object_type()) {
            udt.clear_property_flag(ObUDTTypeFlag::UDT_FLAG_OBJECT_TYPE_BODY);
            udt.set_object_ddl_type(ObUDTTypeFlag::UDT_FLAG_OBJECT_TYPE_SPEC);
          }
          if (FAILEDx(drop_udt(udt,
                               schema_guard,
                               &arg.ddl_stmt_str_,
                               is_force,
                               exist_valid_udt,
                               ddl_service))) {
            LOG_WARN("drop udt failed", K(ret), K(udt_name), K(*udt_info));
          }
        }
      } else {
        if (OB_SUCC(ret) && exist && !OB_ISNULL(udt_info)) {
          if (OB_FAIL(udt.assign(*udt_info))) {
            LOG_WARN("assign udt info failed", K(ret), KPC(udt_info));
          } else if (udt.is_object_type()) {
            udt.clear_property_flag(ObUDTTypeFlag::UDT_FLAG_OBJECT_TYPE_SPEC);
            udt.set_object_ddl_type(ObUDTTypeFlag::UDT_FLAG_OBJECT_TYPE_BODY);
          }
          if (FAILEDx(drop_udt(udt,
                               schema_guard,
                               &arg.ddl_stmt_str_,
                               is_force,
                               exist_valid_udt,
                               ddl_service))) {
            LOG_WARN("drop udt failed", K(ret), K(udt_name), K(*udt_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObPLDDLService::drop_udt(const ObUDTTypeInfo &udt_info,
                              ObSchemaGetterGuard &schema_guard,
                              const ObString *ddl_stmt_str,
                              bool specify_force,
                              bool exist_valid_udt,
                              rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service.schema_service_) || OB_ISNULL(ddl_service.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    uint64_t tenant_id = udt_info.get_tenant_id();
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    }

    if (!(GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_1_0 ||
          (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_2_0 &&
           GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0)
          /*udt dependency feature enabled*/) ||
        !exist_valid_udt) {
      // 1) compatible with version without udt dependency feature
      // 2) otherwise depednency should have been invalid already
    } else {
      // check if the udt which is to be drop has any type or table dependent
      bool has_type_or_table_dep_obj = false;
      ObArray<CriticalDepInfo> objs;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDependencyInfo::collect_all_dep_objs(tenant_id,
                                                                udt_info.get_type_id(),
                                                                udt_info.get_object_type(),
                                                                trans,
                                                                objs))) {
        LOG_WARN("failed to collect all dependent objects");
      } else {
        for (int64_t i = 0; i < objs.count(); i++) {
          schema::ObObjectType dep_obj_type =
              static_cast<schema::ObObjectType>(objs.at(i).element<1>());
          if (schema::ObObjectType::TABLE == dep_obj_type
              || schema::ObObjectType::TYPE == dep_obj_type) {
            has_type_or_table_dep_obj = true;
            break;
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!specify_force && has_type_or_table_dep_obj) {
        ret = OB_ERR_HAS_TYPE_OR_TABLE_DEPENDENT;
        LOG_WARN("cannot drop or replace a type with type or table dependents", K(ret));
      } else if ((specify_force || !has_type_or_table_dep_obj)
                 && OB_FAIL(ObDependencyInfo::batch_invalidate_dependents(
                        objs, trans, tenant_id, udt_info.get_type_id()))) {
        LOG_WARN("invalidate dependents failed");
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDependencyInfo::modify_dep_obj_status(trans,
                                                               tenant_id,
                                                               udt_info.get_type_id(),
                                                               pl_operator,
                                                               *ddl_service.schema_service_))) {
      LOG_WARN("failed to modify obj status", K(ret));
    } else if (OB_FAIL(pl_operator.drop_udt(udt_info, trans, schema_guard, ddl_stmt_str))) {
      LOG_WARN("drop procedure failed", K(ret), K(udt_info));
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

//----Functions for managing package----
int ObPLDDLService::create_package(const obrpc::ObCreatePackageArg &arg,
                                    obrpc::ObRoutineDDLRes *res,
                                    rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_env_before_ddl(schema_guard, arg, ddl_service))) {
    LOG_WARN("check env failed", K(arg), K(ret));
  } else {
    ObPackageInfo new_package_info;
    const ObPackageInfo *old_package_info = NULL;
    uint64_t tenant_id = arg.package_info_.get_tenant_id();
    ObString database_name = arg.db_name_;
    const ObDatabaseSchema *db_schema = NULL;
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(new_package_info.assign(arg.package_info_))) {
      LOG_WARN("fail to assign package info", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create package of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    } else {
      new_package_info.set_database_id(db_schema->get_database_id());
    }
    if (OB_SUCC(ret)
        && database_name.case_compare(OB_SYS_DATABASE_NAME) != 0
        && lib::is_oracle_mode()) {
      if (OB_FAIL(schema_guard.get_user_info(
        tenant_id, database_name, ObString(OB_DEFAULT_HOST_NAME), user_info))) {
        LOG_WARN("failed to get user info", K(ret), K(database_name));
      } else if (OB_ISNULL(user_info)) {
        ret = OB_USER_NOT_EXIST;
        LOG_WARN("user is does not exist", K(ret), K(database_name));
      } else if (OB_INVALID_ID == user_info->get_user_id()) {
        ret = OB_USER_NOT_EXIST;
        LOG_WARN("user id is invalid", K(ret), K(database_name));
      } else {
        new_package_info.set_owner_id(user_info->get_user_id());
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<ObSchemaType> conflict_schema_types;
      // package body 查重只比较有没有重复的 package body，package 查重需要比较除 package body 以外的对象
      if (ObPackageType::PACKAGE_TYPE == new_package_info.get_type()
          && OB_FAIL(schema_guard.check_oracle_object_exist(tenant_id,
             db_schema->get_database_id(), new_package_info.get_package_name(), PACKAGE_SCHEMA,
             INVALID_ROUTINE_TYPE, arg.is_replace_, conflict_schema_types))) {
        LOG_WARN("fail to check object exist", K(ret), K(new_package_info.get_package_name()));
      } else if (conflict_schema_types.count() > 0) {
        // 这里检查 oracle 模式下新对象的名字是否已经被其他对象占用了
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by an existing object in oralce mode",
                 K(ret), K(new_package_info.get_package_name()),
                 K(conflict_schema_types));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard.get_package_info(tenant_id, db_schema->get_database_id(), new_package_info.get_package_name(),
                                                new_package_info.get_type(), new_package_info.get_compatibility_mode(),
                                                old_package_info))) {
        LOG_WARN("failed to check package info exist", K(new_package_info), K(ret));
      } else if (OB_ISNULL(old_package_info) || arg.is_replace_) {
        bool need_create = true;
        // 对于系统包, 为了避免多次重建, 比较下新的系统包与已经存在的系统包是否相同
        if (OB_NOT_NULL(old_package_info) && OB_SYS_TENANT_ID == tenant_id) {
          if (old_package_info->get_source().length() == new_package_info.get_source().length()
              && (0 == MEMCMP(old_package_info->get_source().ptr(),
                              new_package_info.get_source().ptr(),
                              old_package_info->get_source().length()))
              && old_package_info->get_exec_env() == new_package_info.get_exec_env()) {
            need_create = false;
            LOG_INFO("do not recreate package with same source",
                     K(ret),
                     K(old_package_info->get_source()),
                     K(new_package_info.get_source()), K(need_create));
          } else {
            LOG_INFO("recreate package with diff source",
                     K(ret),
                     K(old_package_info->get_source()),
                     K(new_package_info.get_source()), K(need_create));
          }
        }
        if (need_create) {
          ObSArray<ObRoutineInfo> &public_routine_infos = const_cast<ObSArray<ObRoutineInfo> &>(arg.public_routine_infos_);
          ObErrorInfo error_info = arg.error_info_;
          ObSArray<ObDependencyInfo> &dep_infos =
                               const_cast<ObSArray<ObDependencyInfo> &>(arg.dependency_infos_);
          if (OB_FAIL(create_package(schema_guard,
                                     old_package_info,
                                     new_package_info,
                                     public_routine_infos,
                                     error_info,
                                     dep_infos,
                                     &arg.ddl_stmt_str_,
                                     ddl_service))) {
            LOG_WARN("create package failed", K(ret), K(new_package_info));
          }
        }
      } else {
        ret = OB_ERR_PACKAGE_ALREADY_EXISTS;
        const char *type = (new_package_info.get_type() == ObPackageType::PACKAGE_TYPE ? "PACKAGE" : "PACKAGE BODY");
        LOG_USER_ERROR(OB_ERR_PACKAGE_ALREADY_EXISTS, type,
                       database_name.length(), database_name.ptr(),
                       new_package_info.get_package_name().length(), new_package_info.get_package_name().ptr());
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(res)) {
      res->store_routine_schema_version_ = new_package_info.get_schema_version();
    }
  }
  return ret;
}

int ObPLDDLService::create_package(ObSchemaGetterGuard &schema_guard,
                                   const ObPackageInfo *old_package_info,
                                   ObPackageInfo &new_package_info,
                                   ObIArray<ObRoutineInfo> &public_routine_infos,
                                   ObErrorInfo &error_info,
                                   ObIArray<ObDependencyInfo> &dep_infos,
                                   const ObString *ddl_stmt_str,
                                   rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service.schema_service_) || OB_ISNULL(ddl_service.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    const uint64_t tenant_id = new_package_info.get_tenant_id();
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(pl_operator.create_package(old_package_info,
                                                   new_package_info,
                                                   trans,
                                                   schema_guard,
                                                   public_routine_infos,
                                                   error_info,
                                                   dep_infos,
                                                   ddl_stmt_str))) {
      LOG_WARN("create package failed", K(ret), K(new_package_info));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPLDDLService::alter_package(const obrpc::ObAlterPackageArg &arg,
                                 obrpc::ObRoutineDDLRes *res,
                                 rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_env_before_ddl(schema_guard, arg, ddl_service))) {
    LOG_WARN("check env failed", K(arg), K(ret));
  } else {
    uint64_t tenant_id = arg.tenant_id_;
    const ObString &db_name = arg.db_name_;
    const ObString &package_name = arg.package_name_;
    ObPackageType package_type = arg.package_type_;
    int64_t compatible_mode =  arg.compatible_mode_;
    const ObDatabaseSchema *db_schema = NULL;
    if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(db_name), K(ret));
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create package of db in recyclebin", K(ret), K(arg), K(*db_schema));
    }
    if (OB_SUCC(ret)) {
      bool exist = false;
      ObSArray<ObRoutineInfo> &public_routine_infos = const_cast<ObSArray<ObRoutineInfo> &>(arg.public_routine_infos_);
      if (OB_FAIL(schema_guard.check_package_exist(tenant_id, db_schema->get_database_id(),
                                                   package_name, package_type, compatible_mode, exist))) {
        LOG_WARN("failed to check package info exist", K(package_name), K(ret));
      } else if (exist) {
        const ObPackageInfo *package_info = NULL;
        if (OB_FAIL(schema_guard.get_package_info(tenant_id, db_schema->get_database_id(), package_name, package_type,
                                                  compatible_mode, package_info))) {
          LOG_WARN("get package info failed", K(ret));
        } else if (OB_ISNULL(package_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("package info is null", K(db_schema->get_database_id()), K(package_name), K(package_type), K(ret));
        }
        if (OB_SUCC(ret)) {
          if (!((GET_MIN_CLUSTER_VERSION() >= MOCK_CLUSTER_VERSION_4_2_4_0
                 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0)
                || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_1_0)
              || PACKAGE_TYPE == package_type) {
            if (OB_FAIL(alter_package(schema_guard,
                                      const_cast<ObPackageInfo &>(*package_info),
                                      public_routine_infos,
                                      const_cast<ObErrorInfo &>(arg.error_info_),
                                      &arg.ddl_stmt_str_,
                                      ddl_service))) {
              LOG_WARN("drop package failed", K(ret), K(package_name));
            }
            if (OB_SUCC(ret) && OB_NOT_NULL(res)) {
              res->store_routine_schema_version_ = package_info->get_schema_version();
            }
          } else {
            ObSArray<ObDependencyInfo> &dep_infos =
                               const_cast<ObSArray<ObDependencyInfo> &>(arg.dependency_infos_);
            ObPackageInfo new_package_info;
            if (OB_FAIL(new_package_info.assign(*package_info))) {
              LOG_WARN("failed to copy new package info", K(ret));
            } else if (OB_FAIL(new_package_info.set_exec_env(arg.exec_env_))) {
              LOG_WARN("fail to set exec env", K(ret));
            } else if (OB_FAIL(create_package(schema_guard,
                                              package_info,
                                              new_package_info,
                                              public_routine_infos,
                                              const_cast<ObErrorInfo &>(arg.error_info_),
                                              dep_infos,
                                              &arg.ddl_stmt_str_,
                                              ddl_service))) {
              LOG_WARN("create package failed", K(ret), K(new_package_info));
            }
            if (OB_SUCC(ret) && OB_NOT_NULL(res)) {
              res->store_routine_schema_version_ = new_package_info.get_schema_version();
            }
          }
        }
      } else {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        const char *type = (package_type == ObPackageType::PACKAGE_TYPE ? "PACKAGE" : "PACKAGE BODY");
        LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, type,
                       db_schema->get_database_name_str().length(), db_schema->get_database_name(),
                       package_name.length(), package_name.ptr());
      }
    }
  }

  return ret;
}

int ObPLDDLService::alter_package(ObSchemaGetterGuard &schema_guard,
                                ObPackageInfo &package_info,
                                ObIArray<ObRoutineInfo> &public_routine_infos,
                                share::schema::ObErrorInfo &error_info,
                                const ObString *ddl_stmt_str,
                                rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service.schema_service_) || OB_ISNULL(ddl_service.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    const uint64_t tenant_id = package_info.get_tenant_id();
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(pl_operator.alter_package(package_info, schema_guard, trans, public_routine_infos,
                                                  error_info, ddl_stmt_str))) {
      LOG_WARN("alter package failed", K(package_info), K(ret));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPLDDLService::drop_package(const obrpc::ObDropPackageArg &arg,
                                 rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_env_before_ddl(schema_guard, arg, ddl_service))) {
    LOG_WARN("check env failed", K(arg), K(ret));
  } else {
    uint64_t tenant_id = arg.tenant_id_;
    const ObString &db_name = arg.db_name_;
    const ObString &package_name = arg.package_name_;
    ObPackageType package_type = arg.package_type_;
    int64_t compatible_mode = arg.compatible_mode_;
    const ObDatabaseSchema *db_schema = NULL;
    if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create package of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    }
    if (OB_SUCC(ret)) {
      bool exist = false;
      if (OB_FAIL(schema_guard.check_package_exist(tenant_id, db_schema->get_database_id(),
          package_name, package_type, compatible_mode, exist))) {
        LOG_WARN("failed to check package info exist", K(package_name), K(ret));
      } else if (exist) {
        const ObPackageInfo *package_info = NULL;
        ObErrorInfo error_info = arg.error_info_;
        if (OB_FAIL(schema_guard.get_package_info(tenant_id, db_schema->get_database_id(), package_name, package_type, compatible_mode, package_info))) {
          LOG_WARN("get package info failed", K(ret));
        } else if (OB_FAIL(drop_package(schema_guard,
                                        *package_info,
                                        error_info,
                                        &arg.ddl_stmt_str_,
                                        ddl_service))) {
          LOG_WARN("drop package failed", K(ret), K(package_name));
        }
      } else {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        const char *type = (package_type == ObPackageType::PACKAGE_TYPE ? "PACKAGE" : "PACKAGE BODY");
        LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, type,
                       db_name.length(), db_name.ptr(),
                       package_name.length(), package_name.ptr());
      }
    }
  }
  return ret;
}

int ObPLDDLService::drop_package(share::schema::ObSchemaGetterGuard &schema_guard,
                                 const ObPackageInfo &package_info,
                                 ObErrorInfo &error_info,
                                 const ObString *ddl_stmt_str,
                                 rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service.schema_service_) || OB_ISNULL(ddl_service.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    const uint64_t tenant_id = package_info.get_tenant_id();
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    } else if (OB_FAIL(pl_operator.drop_package(package_info,
                                                 trans,
                                                 schema_guard,
                                                 error_info,
                                                 ddl_stmt_str))) {
      LOG_WARN("drop procedure failed", K(ret), K(package_info));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}
//----End of functions for managing package----

//----Functions for managing trigger----
int ObPLDDLService::create_trigger(const obrpc::ObCreateTriggerArg &arg,
                                    obrpc::ObCreateTriggerRes *res,
                                    rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_env_before_ddl(schema_guard, arg, ddl_service))) {
    LOG_WARN("check env failed", K(ret));
  } else if (OB_FAIL(create_trigger(arg, schema_guard, res, ddl_service))) {
    LOG_WARN("failed to create trigger", K(ret));
  }
  return ret;
}

int ObPLDDLService::alter_trigger(const obrpc::ObAlterTriggerArg &arg,
                                  obrpc::ObRoutineDDLRes *res,
                                  rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  uint64_t tenant_id = OB_INVALID_ID;
  bool is_enable = false;
  int64_t refreshed_schema_version = 0;
  OZ (check_env_before_ddl(schema_guard, arg, ddl_service));
  OX (is_enable = arg.trigger_infos_.at(0).is_enable());
  OX (tenant_id = arg.exec_tenant_id_);
  OZ (schema_guard.get_schema_version(tenant_id, refreshed_schema_version));
  if (OB_SUCC(ret)) {
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    OZ (trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version), refreshed_schema_version);
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.trigger_infos_.count(); ++i) {
      const ObTriggerInfo *old_tg_info = NULL;
      ObTriggerInfo new_tg_info;
      OZ (new_tg_info.assign(arg.trigger_infos_.at(i)));
      OZ (schema_guard.get_trigger_info(tenant_id, new_tg_info.get_trigger_id(), old_tg_info));
      CK (OB_NOT_NULL(old_tg_info), OB_ERR_TRIGGER_NOT_EXIST);
      if (OB_SUCC(ret)) {
        if (!arg.is_set_status_) {
          const ObTriggerInfo *other_trg_info = NULL;
          ObString new_trg_name = new_tg_info.get_trigger_name();
          ObString new_trg_body = new_tg_info.get_trigger_body();
          OZ (schema_guard.get_trigger_info(new_tg_info.get_tenant_id(),
                                            new_tg_info.get_database_id(),
                                            new_trg_name,
                                            other_trg_info));
          OV (OB_ISNULL(other_trg_info), OB_OBJ_ALREADY_EXIST, new_tg_info);
          OZ (new_tg_info.deep_copy(*old_tg_info));
          OZ (new_tg_info.set_trigger_name(new_trg_name));
          OZ (new_tg_info.set_trigger_body(new_trg_body));
        } else {
          OZ (new_tg_info.deep_copy(*old_tg_info));
          OX (new_tg_info.set_is_enable(is_enable));
        }
        OZ (pl_operator.alter_trigger(new_tg_info, trans, &arg.ddl_stmt_str_));
      }
      // alter trigger scenes
      if (OB_SUCC(ret) && 1 == arg.trigger_infos_.count() && OB_NOT_NULL(res)) {
        res->store_routine_schema_version_ = new_tg_info.get_schema_version();
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPLDDLService::drop_trigger(const obrpc::ObDropTriggerArg &arg,
                                 rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  uint64_t tenant_id = arg.tenant_id_;
  uint64_t trigger_database_id = OB_INVALID_ID;
  const ObString &trigger_database = arg.trigger_database_;
  const ObString &trigger_name = arg.trigger_name_;
  const ObTriggerInfo *trigger_info = NULL;
  bool is_ora_mode = false;
  if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_ora_mode))) {
    LOG_WARN("fail to check is oracle mode", K(ret));
  } else if (OB_FAIL(check_env_before_ddl(schema_guard, arg, ddl_service))) {
    LOG_WARN("check env failed", K(ret));
  } else if (OB_FAIL(ddl_service.get_database_id(schema_guard, tenant_id, trigger_database, trigger_database_id))) {
    LOG_WARN("get database id failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_trigger_info(tenant_id, trigger_database_id, trigger_name, trigger_info))) {
    LOG_WARN("get trigger info failed", K(ret), K(trigger_database), K(trigger_name));
  } else if (OB_ISNULL(trigger_info)) {
    ret = OB_ERR_TRIGGER_NOT_EXIST;
    if (is_ora_mode) {
      LOG_ORACLE_USER_ERROR(OB_ERR_TRIGGER_NOT_EXIST, trigger_name.length(), trigger_name.ptr());
    }
  } else if (trigger_info->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("trigger is in recyclebin", K(ret),
             K(trigger_info->get_trigger_id()), K(trigger_info->get_trigger_name()));
  } else if (OB_FAIL(drop_trigger_in_trans(*trigger_info, &arg.ddl_stmt_str_, schema_guard, ddl_service))) {
    LOG_WARN("drop trigger in trans failed", K(ret), K(trigger_database), K(trigger_name));
  }
  if (!is_ora_mode && (OB_ERR_TRIGGER_NOT_EXIST == ret || OB_ERR_BAD_DATABASE == ret)) {
    ret = OB_ERR_TRIGGER_NOT_EXIST;
    if (arg.if_exist_) {
      ret = OB_SUCCESS;
      LOG_MYSQL_USER_NOTE(OB_ERR_TRIGGER_NOT_EXIST);
    } else {
      LOG_MYSQL_USER_ERROR(OB_ERR_TRIGGER_NOT_EXIST);
    }
    LOG_WARN("trigger not exist", K(arg.trigger_database_), K(arg.trigger_name_), K(ret));
  }
  return ret;
}

int ObPLDDLService::create_trigger(const obrpc::ObCreateTriggerArg &arg,
                                   ObSchemaGetterGuard &schema_guard,
                                   obrpc::ObCreateTriggerRes *res,
                                   rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  ObTriggerInfo new_trigger_info;
  //in_second_stage_ is false, Indicates that the trigger is created normally
  //true Indicates that the error message is inserted into the system table after the trigger is created
  //So the following steps can be skipped
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t trigger_database_id = OB_INVALID_ID;
  uint64_t base_object_id = OB_INVALID_ID;
  ObSchemaType base_object_type = static_cast<ObSchemaType>(arg.trigger_info_.get_base_object_type());
  const ObString &trigger_database = arg.trigger_database_;
  const ObString &base_object_database = arg.base_object_database_;
  const ObString &base_object_name = arg.base_object_name_;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(arg.trigger_info_.get_tenant_id(), data_version))) {
     LOG_WARN("failed to get data version", K(ret));
  } else if (arg.trigger_info_.is_system_type() && (data_version < MOCK_DATA_VERSION_4_2_5_1 ||
            (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_5_1))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "system trigger is");
  } else if (OB_FAIL(new_trigger_info.assign(arg.trigger_info_))) {
    LOG_WARN("assign trigger_info failed", K(ret));
  } else if (FALSE_IT(tenant_id = new_trigger_info.get_tenant_id())) {
  } else {
    const ObTriggerInfo *old_trigger_info = NULL;
    if (OB_FAIL(ddl_service.get_database_id(schema_guard, tenant_id, trigger_database, trigger_database_id))) {
      LOG_WARN("get database id failed", K(ret));
    } else if (OB_FAIL(get_object_info(schema_guard,
                                       tenant_id,
                                       base_object_database,
                                       base_object_name,
                                       base_object_type,
                                       base_object_id,
                                       ddl_service))) {
      LOG_WARN("get base object info failed", K(ret));
    } else if (FALSE_IT(new_trigger_info.set_database_id(trigger_database_id))) {
    } else if (FALSE_IT(new_trigger_info.set_base_object_type(base_object_type))) {
    } else if (FALSE_IT(new_trigger_info.set_base_object_id(base_object_id))) {
    } else if (OB_FAIL(try_get_exist_trigger(schema_guard, new_trigger_info, old_trigger_info, arg.with_replace_))) {
      LOG_WARN("check trigger exist failed", K(ret));
    } else {
      if (NULL != old_trigger_info) {
        new_trigger_info.set_trigger_id(old_trigger_info->get_trigger_id());
      }
    }
  }
  if (OB_SUCC(ret)) {
    bool with_res = (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_1_2);
    int64_t table_schema_version = OB_INVALID_VERSION;
    if (with_res && OB_ISNULL(res)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("res is NULL", K(ret));
    } else if (OB_FAIL(create_trigger_in_trans(new_trigger_info,
                                               const_cast<ObErrorInfo &>(arg.error_info_),
                                               const_cast<ObSArray<ObDependencyInfo> &>(arg.dependency_infos_),
                                               &arg.ddl_stmt_str_,
                                               arg.in_second_stage_,
                                               schema_guard,
                                               table_schema_version,
                                               ddl_service))) {
      LOG_WARN("create trigger in trans failed", K(ret));
    } else if (with_res) {
      res->table_schema_version_ = table_schema_version;
      res->trigger_schema_version_ = new_trigger_info.get_schema_version();
    }
  }
  return ret;
}

int ObPLDDLService::create_trigger_in_trans(share::schema::ObTriggerInfo &trigger_info,
                                            share::schema::ObErrorInfo &error_info,
                                            ObIArray<ObDependencyInfo> &dep_infos,
                                            const common::ObString *ddl_stmt_str,
                                            bool in_second_stage,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            int64_t &table_schema_version,
                                            rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service.schema_service_) || OB_ISNULL(ddl_service.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    const uint64_t tenant_id = trigger_info.get_tenant_id();
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    }
    if (OB_SUCC(ret) && !in_second_stage) {
        OZ (adjust_trigger_action_order(schema_guard, trans, pl_operator, trigger_info, true));
    }
    OZ (pl_operator.create_trigger(trigger_info, trans, error_info, dep_infos, table_schema_version, ddl_stmt_str));
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPLDDLService::drop_trigger_in_trans(const share::schema::ObTriggerInfo &trigger_info,
                                          const common::ObString *ddl_stmt_str,
                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                          rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service.schema_service_) || OB_ISNULL(ddl_service.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    const uint64_t tenant_id = trigger_info.get_tenant_id();
    ObDDLSQLTransaction trans(ddl_service.schema_service_);
    ObPLDDLOperator pl_operator(*ddl_service.schema_service_, *ddl_service.sql_proxy_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(ddl_service.sql_proxy_, tenant_id, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
    }
    OZ (adjust_trigger_action_order(schema_guard, trans, pl_operator, const_cast<ObTriggerInfo &>(trigger_info), false));
    OZ (pl_operator.drop_trigger(trigger_info, trans, ddl_stmt_str));
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service.publish_schema(tenant_id))) {
        LOG_WARN("publish schema failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPLDDLService::try_get_exist_trigger(share::schema::ObSchemaGetterGuard &schema_guard,
                                          const share::schema::ObTriggerInfo &new_trigger_info,
                                          const share::schema::ObTriggerInfo *&old_trigger_info,
                                          bool with_replace)
{
  int ret = OB_SUCCESS;
  const ObString &trigger_name = new_trigger_info.get_trigger_name();
  if (OB_FAIL(schema_guard.get_trigger_info(new_trigger_info.get_tenant_id(),
                                            new_trigger_info.get_database_id(),
                                            trigger_name, old_trigger_info))) {
    LOG_WARN("failed to get old trigger info", K(ret));
  } else if (NULL != old_trigger_info) {
    if (new_trigger_info.get_base_object_id() != old_trigger_info->get_base_object_id()) {
      ret = OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE;
      LOG_USER_ERROR(OB_ERR_TRIGGER_EXIST_ON_OTHER_TABLE, trigger_name.length(), trigger_name.ptr());
    } else if (!with_replace) {
      ret = OB_ERR_TRIGGER_ALREADY_EXIST;
      LOG_USER_ERROR(OB_ERR_TRIGGER_ALREADY_EXIST, trigger_name.length(), trigger_name.ptr());
    }
  }
  return ret;
}

int ObPLDDLService::rebuild_trigger_on_rename(share::schema::ObSchemaGetterGuard &schema_guard,
                                              const share::schema::ObTableSchema &table_schema,
                                              ObDDLOperator &ddl_operator,
                                              ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *database_schema = NULL;
  const ObString *database_name = NULL;
  const ObString &table_name = table_schema.get_table_name_str();
  const uint64_t tenant_id = table_schema.get_tenant_id();
  OZ (schema_guard.get_database_schema(tenant_id, table_schema.get_database_id(), database_schema),
      table_schema.get_database_id());
  OV (OB_NOT_NULL(database_schema), OB_ERR_UNEXPECTED, table_schema.get_database_id());
  OX (database_name = &database_schema->get_database_name_str());
  OZ (rebuild_trigger_on_rename(schema_guard,
                                tenant_id,
                                table_schema.get_trigger_list(),
                                *database_name,
                                table_name,
                                ddl_operator,
                                trans));
  return ret;
}

int ObPLDDLService::rebuild_trigger_on_rename(share::schema::ObSchemaGetterGuard &schema_guard,
                                              const uint64_t tenant_id,
                                              const common::ObIArray<uint64_t> &trigger_list,
                                              const common::ObString &database_name,
                                              const common::ObString &table_name,
                                              ObDDLOperator &ddl_operator,
                                              ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObTriggerInfo *trigger_info = NULL;
  ObPLDDLOperator pl_operator(ddl_operator.get_multi_schema_service(), ddl_operator.get_sql_proxy());
  for (int64_t i = 0; OB_SUCC(ret) && i < trigger_list.count(); i++) {
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_list.at(i), trigger_info), trigger_list.at(i));
    OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_list.at(i));
    OZ (pl_operator.rebuild_trigger_on_rename(*trigger_info, database_name, table_name, trans));
  }
  return ret;
}

int ObPLDDLService::create_trigger_for_truncate_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                                      const common::ObIArray<uint64_t> &origin_trigger_list,
                                                      share::schema::ObTableSchema &new_table_schema,
                                                      ObDDLOperator &ddl_operator,
                                                      ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObTriggerInfo *origin_trigger_info = NULL;
  ObTriggerInfo new_trigger_info;
  ObString spec_source;
  ObString body_source;
  ObErrorInfo error_info;
  ObArenaAllocator inner_alloc;
  new_table_schema.get_trigger_list().reset();
  bool is_update_table_schema_version = false;
  const ObDatabaseSchema *db_schema = NULL;
  const uint64_t tenant_id = new_table_schema.get_tenant_id();
  ObPLDDLOperator pl_operator(ddl_operator.get_multi_schema_service(), ddl_operator.get_sql_proxy());
  OZ (schema_guard.get_database_schema(tenant_id,
                                       new_table_schema.get_database_id(),
                                       db_schema));
  CK (db_schema != NULL);
  for (int64_t i = 0; OB_SUCC(ret) && i < origin_trigger_list.count(); i++) {
    is_update_table_schema_version = i == origin_trigger_list.count() - 1 ? true : false;
    uint64_t new_trigger_id = OB_INVALID_ID;
    OZ (schema_guard.get_trigger_info(tenant_id, origin_trigger_list.at(i), origin_trigger_info),
                                      origin_trigger_list.at(i));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(new_trigger_info.deep_copy(*origin_trigger_info))) {
        LOG_WARN("failed to create trigger for truncate table", K(ret));
      } else if (OB_FAIL(pl_operator.get_multi_schema_service().get_schema_service()->fetch_new_trigger_id(
          origin_trigger_info->get_tenant_id(), new_trigger_id))) {
        LOG_WARN("failed to fetch_new_trigger_id", K(ret));
      } else {
        new_trigger_info.set_trigger_id(new_trigger_id);
        new_trigger_info.set_base_object_id(new_table_schema.get_table_id());
        new_table_schema.get_trigger_list().push_back(new_trigger_id);
        if (OB_SUCC(ret)) {
          ObSEArray<ObDependencyInfo, 1> dep_infos;
          int64_t table_schema_version = OB_INVALID_VERSION;
          if (OB_FAIL(pl_operator.create_trigger(new_trigger_info,
                                                 trans,
                                                 error_info,
                                                 dep_infos,
                                                 table_schema_version,
                                                 &origin_trigger_info->get_trigger_body(),
                                                 is_update_table_schema_version,
                                                 true))) {
            LOG_WARN("failed to create trigger for truncate table", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPLDDLService::adjust_trigger_action_order(share::schema::ObSchemaGetterGuard &schema_guard,
                                                ObDDLSQLTransaction &trans,
                                                ObPLDDLOperator &pl_operator,
                                                ObTriggerInfo &trigger_info,
                                                bool is_create_trigger)
{
  int ret = OB_SUCCESS;
#define ALTER_OLD_TRIGGER(source_trg_info) \
  ObTriggerInfo copy_trg_info;   \
  OZ (copy_trg_info.assign(*source_trg_info)); \
  OX (copy_trg_info.set_action_order(new_action_order)); \
  OZ (pl_operator.alter_trigger(copy_trg_info, trans, NULL, false/*is_update_table_schema_version*/));

  bool is_oracle_mode = false;
  const uint64_t tenant_id = trigger_info.get_tenant_id();
  common::ObSArray<uint64_t> trg_list;
  OZ (ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode));
  if (OB_SUCC(ret)) {
    if (trigger_info.is_dml_type()) {
      const ObTableSchema *table_schema = NULL;
      OZ (schema_guard.get_table_schema(tenant_id, trigger_info.get_base_object_id(), table_schema));
      OV (OB_NOT_NULL(table_schema));
      OZ (trg_list.assign(table_schema->get_trigger_list()));
    } else if (trigger_info.is_system_type()) {
      const ObUserInfo *user_info = NULL;
      OZ (schema_guard.get_user_info(tenant_id, trigger_info.get_base_object_id(), user_info));
      OV (OB_NOT_NULL(user_info));
      OZ (trg_list.assign(user_info->get_trigger_list()));
    }
  }
  if (OB_SUCC(ret)) {
    const ObTriggerInfo *old_trg_info = NULL;
    int64_t new_action_order = 0; // the old trigger's new action order
    if (is_create_trigger) {
      int64_t action_order = 1; // action order for the trigger being created
      const ObTriggerInfo *ref_trg_info = NULL;
      if (OB_SUCC(ret)) {
        if (!trigger_info.get_ref_trg_name().empty()) {
          OZ (schema_guard.get_trigger_info(tenant_id, trigger_info.get_database_id(),
                                            trigger_info.get_ref_trg_name(), ref_trg_info));
          OV (OB_NOT_NULL(ref_trg_info));
        }
        if (OB_FAIL(ret)) {
        } else if (is_oracle_mode) {
          OZ (recursive_check_trigger_ref_cyclic(schema_guard, trigger_info, trg_list,
                                                 trigger_info.get_trigger_name(), trigger_info.get_ref_trg_name()));
          if (OB_SUCC(ret)) {
            if (NULL != ref_trg_info) {
              uint64_t ref_db_id = OB_INVALID_ID;
              OZ (schema_guard.get_database_id(tenant_id, trigger_info.get_ref_trg_db_name(), ref_db_id));
              OZ (schema_guard.get_trigger_info(tenant_id, ref_db_id, trigger_info.get_ref_trg_name(), ref_trg_info));
              if (OB_SUCC(ret) && trigger_info.is_order_follows()) {
                action_order = ref_trg_info->get_action_order() + 1;
                }
            }
            OZ (recursive_alter_ref_trigger(schema_guard, trans, pl_operator, trigger_info,
                                            trg_list, trigger_info.get_trigger_name(), action_order));
          }
        } else {
          if (NULL == ref_trg_info) {
            for (int64_t i = 0; OB_SUCC(ret) && i < trg_list.count(); i++) {
              OZ (schema_guard.get_trigger_info(tenant_id, trg_list.at(i), old_trg_info));
              OV (OB_NOT_NULL(old_trg_info));
              if (OB_SUCC(ret) && ObTriggerInfo::is_same_timing_event(trigger_info, *old_trg_info)) {
                action_order++;
              }
            }
          } else {
            bool is_follows = trigger_info.is_order_follows();
            action_order = is_follows ? ref_trg_info->get_action_order() + 1 : ref_trg_info->get_action_order();
            // ref_trg_info need to modify
            for (int64_t i = 0; OB_SUCC(ret) && i < trg_list.count(); i++) {
              OZ (schema_guard.get_trigger_info(tenant_id, trg_list.at(i), old_trg_info));
              OV (OB_NOT_NULL(old_trg_info));
              if (OB_SUCC(ret) && ObTriggerInfo::is_same_timing_event(trigger_info, *old_trg_info)
                  && trigger_info.get_trigger_id() != old_trg_info->get_trigger_id()
                  && ref_trg_info->get_trigger_id() != old_trg_info->get_trigger_id()) {
                  if (ref_trg_info->get_action_order() < old_trg_info->get_action_order()) {
                    new_action_order = old_trg_info->get_action_order() + 1;
                    ALTER_OLD_TRIGGER(old_trg_info);
                }
              }
            }
            if (OB_SUCC(ret) && !is_follows) {
              // if `PRECEDES`, the ref_trg_info action_order need to +1
              new_action_order = ref_trg_info->get_action_order() + 1;
              ALTER_OLD_TRIGGER(ref_trg_info);
            }
          }
        }
      }
      OX (trigger_info.set_action_order(action_order));
    } else if (!is_oracle_mode) {
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < trg_list.count(); i++) {
          OZ (schema_guard.get_trigger_info(tenant_id, trg_list.at(i), old_trg_info));
          OV (OB_NOT_NULL(old_trg_info));
          if (OB_SUCC(ret) && ObTriggerInfo::is_same_timing_event(trigger_info, *old_trg_info)
              && trigger_info.get_trigger_id() != old_trg_info->get_trigger_id()
              && trigger_info.get_action_order() < old_trg_info->get_action_order()) {
            new_action_order = old_trg_info->get_action_order() - 1;
            ALTER_OLD_TRIGGER(old_trg_info);
          }
        }
      }
    }
  }
#undef ALTER_OLD_TRIGGER
  return ret;
}

int ObPLDDLService::recursive_alter_ref_trigger(share::schema::ObSchemaGetterGuard &schema_guard,
                                                ObDDLSQLTransaction &trans,
                                                ObPLDDLOperator &pl_operator,
                                                const ObTriggerInfo &ref_trigger_info,
                                                const common::ObIArray<uint64_t> &trigger_list,
                                                const ObString &trigger_name,
                                                int64_t action_order)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = ref_trigger_info.get_tenant_id();
  const ObTriggerInfo *trg_info = NULL;
  int64_t new_action_order = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < trigger_list.count(); i++) {
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_list.at(i), trg_info));
    OV (OB_NOT_NULL(trg_info));
    if (0 != trg_info->get_trigger_name().case_compare(trigger_name)) {
      if (OB_SUCC(ret) && 0 == trg_info->get_ref_trg_name().case_compare(ref_trigger_info.get_trigger_name())) {
        ObTriggerInfo copy_trg_info;
        OX (new_action_order = action_order + 1);
        OZ (copy_trg_info.assign(*trg_info));
        OX (copy_trg_info.set_action_order(new_action_order));
        OZ (pl_operator.alter_trigger(copy_trg_info, trans, NULL, false/*is_update_table_schema_version*/));
        OZ (SMART_CALL(recursive_alter_ref_trigger(schema_guard,
                                                   trans,
                                                   pl_operator,
                                                   *trg_info,
                                                   trigger_list,
                                                   trigger_name,
                                                   new_action_order)));
      }
    }
  }
  return ret;
}

int ObPLDDLService::recursive_check_trigger_ref_cyclic(share::schema::ObSchemaGetterGuard &schema_guard,
                                                        const ObTriggerInfo &ref_trigger_info,
                                                        const common::ObIArray<uint64_t> &trigger_list,
                                                        const ObString &create_trigger_name,
                                                        const ObString &generate_cyclic_name)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ref_trigger_info.get_tenant_id();
  const ObTriggerInfo *trg_info = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < trigger_list.count(); i++) {
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_list.at(i), trg_info));
    OV (OB_NOT_NULL(trg_info));
    if (OB_SUCC(ret)) {
      if (0 != trg_info->get_trigger_name().case_compare(create_trigger_name)) {
        if (0 == trg_info->get_ref_trg_name().case_compare(ref_trigger_info.get_trigger_name())) {
          if (0 == trg_info->get_trigger_name().case_compare(generate_cyclic_name)) {
            ret = OB_ERR_REF_CYCLIC_IN_TRG;
            LOG_WARN("ORA-25023: cyclic trigger dependency is not allowed", K(ret),
                     K(generate_cyclic_name), KPC(trg_info));
          }
          OZ (SMART_CALL(recursive_check_trigger_ref_cyclic(schema_guard,
                                                            *trg_info,
                                                            trigger_list,
                                                            create_trigger_name,
                                                            generate_cyclic_name)));
        }
      }
    }
  }
  return ret;
}
int ObPLDDLService::drop_trigger_in_drop_table(ObMySQLTransaction &trans,
                                               ObDDLOperator &ddl_operator,
                                               share::schema::ObSchemaGetterGuard &schema_guard,
                                               const share::schema::ObTableSchema &table_schema,
                                               const bool to_recyclebin)
                  {
  int ret = OB_SUCCESS;
  uint64_t trigger_id = OB_INVALID_ID;
  const ObTriggerInfo *trigger_info = NULL;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const ObIArray<uint64_t> &trigger_id_list = table_schema.get_trigger_list();
  ObPLDDLOperator pl_operator(ddl_operator.get_multi_schema_service(), ddl_operator.get_sql_proxy());
  for (int64_t i = 0; OB_SUCC(ret) && i < trigger_id_list.count(); i++) {
    OX (trigger_id = trigger_id_list.at(i));
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_id, trigger_info), trigger_id);
    OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_id);
    OV (!trigger_info->is_in_recyclebin(), OB_ERR_UNEXPECTED, trigger_id);
    if (to_recyclebin && !table_schema.is_view_table()) {
      // 兼容oracle, drop view的时候trigger不进回收站
      OZ (pl_operator.drop_trigger_to_recyclebin(*trigger_info, schema_guard, trans));
    } else {
      OZ (pl_operator.drop_trigger(*trigger_info,
                                   trans,
                                   NULL,
                                   true /*is_update_table_schema_version, default true*/,
                                   table_schema.get_in_offline_ddl_white_list()));
    }
  }
  return ret;
}

int ObPLDDLService::flashback_trigger(const share::schema::ObTableSchema &table_schema,
                                      const uint64_t new_database_id,
                                      const common::ObString &new_table_name,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      ObMySQLTransaction &trans,
                                      ObDDLOperator &ddl_operator)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const ObIArray<uint64_t> &trigger_id_list = table_schema.get_trigger_list();
  const ObTriggerInfo *trigger_info = NULL;
  ObPLDDLOperator pl_operator(ddl_operator.get_multi_schema_service(), ddl_operator.get_sql_proxy());
  for (int i = 0; OB_SUCC(ret) && i < trigger_id_list.count(); i++) {
    uint64_t trigger_id = trigger_id_list.at(i);
    OZ (schema_guard.get_trigger_info(tenant_id, trigger_id, trigger_info), trigger_id);
    OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_id);
    OZ (pl_operator.flashback_trigger(*trigger_info, new_database_id, new_table_name, schema_guard, trans));
  }
  return ret;
}

int ObPLDDLService::get_object_info(ObSchemaGetterGuard &schema_guard,
                                    const uint64_t tenant_id,
                                    const ObString &object_database,
                                    const ObString &object_name,
                                    ObSchemaType &object_type,
                                    uint64_t &object_id,
                                    rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  uint64_t database_id = OB_INVALID_ID;
  const ObTableSchema *table_schema = NULL;
  if (TABLE_SCHEMA == object_type || VIEW_SCHEMA == object_type) {
    const ObTableSchema *table_schema = NULL;
    if (OB_FAIL(ddl_service.get_database_id(schema_guard, tenant_id, object_database, database_id))) {
      LOG_WARN("failed to get database id", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, database_id,
                                                    object_name, false, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(tenant_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_BAD_TABLE;
      LOG_WARN("table schema is invalid", K(ret), K(object_name), K(object_name));
    } else if (table_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("table is in recyclebin", K(ret), K(object_name), K(object_name));
    } else if (!table_schema->is_user_table() && !table_schema->is_user_view()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("trigger only support create on user table or user view now", K(ret));
    } else {
      object_type = table_schema->is_user_table() ? TABLE_SCHEMA : VIEW_SCHEMA;
      object_id = table_schema->get_table_id();
    }
  } else if (USER_SCHEMA == object_type || DATABASE_SCHEMA == object_type) {
    const ObUserInfo *user_info = NULL;
    ObString host_name("%");
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, object_name, host_name, user_info))) {
      LOG_WARN("get user info failed", K(ret), K(object_name), K(tenant_id));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_BAD_TABLE;
      LOG_WARN("user_info is NULL", K(ret), K(object_name), K(tenant_id));
    } else {
      object_id = user_info->get_user_id();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("object_type is invalid", K(ret), K(object_type));
  }
  return ret;
}
int ObPLDDLService::rebuild_triggers_on_hidden_table(const ObTableSchema &orig_table_schema,
                                                     const ObTableSchema &hidden_table_schema,
                                                     ObSchemaGetterGuard &src_tenant_schema_guard,
                                                     ObSchemaGetterGuard &dst_tenant_schema_guard,
                                                     ObDDLOperator &ddl_operator,
                                                     ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t src_tenant_id = orig_table_schema.get_tenant_id();
  const uint64_t dst_tenant_id = hidden_table_schema.get_tenant_id();
  const bool is_across_tenant = src_tenant_id != dst_tenant_id;
  const ObIArray<uint64_t> &trigger_list = orig_table_schema.get_trigger_list();
  const ObTriggerInfo *trigger_info = NULL;
  ObTriggerInfo new_trigger_info;
  ObErrorInfo error_info;
  ObPLDDLOperator pl_operator(ddl_operator.get_multi_schema_service(), ddl_operator.get_sql_proxy());
  for (int i = 0; OB_SUCC(ret) && i < trigger_list.count(); i++) {
    const ObTriggerInfo *check_exist_trigger = nullptr;
    OZ (src_tenant_schema_guard.get_trigger_info(src_tenant_id, trigger_list.at(i), trigger_info));
    OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_list.at(i));
    OZ (dst_tenant_schema_guard.get_trigger_info(dst_tenant_id, hidden_table_schema.get_database_id(),
        trigger_info->get_trigger_name(), check_exist_trigger));
    if (nullptr != check_exist_trigger && is_across_tenant) {
      LOG_INFO("duplicated trigger name, ignore to rebuild",
               K(dst_tenant_id), "db_id", hidden_table_schema.get_database_id(), KPC(trigger_info));
    } else {
      OZ (new_trigger_info.assign(*trigger_info));
      OX (new_trigger_info.set_base_object_id(hidden_table_schema.get_table_id()));
      OX (new_trigger_info.set_trigger_id(OB_INVALID_ID));
      OX (new_trigger_info.set_tenant_id(dst_tenant_id));
      OX (new_trigger_info.set_database_id(hidden_table_schema.get_database_id()));
      if (!is_across_tenant) {
        // triiger under source tenant will be dropped when drop tenant.
        OZ (pl_operator.drop_trigger(*trigger_info,
                                     trans,
                                     nullptr,
                                     false/*is_update_table_schema_version*/));
      }
      if (OB_SUCC(ret)) {
        ObSEArray<ObDependencyInfo, 1> dep_infos;
        int64_t table_schema_version = OB_INVALID_VERSION;
        OZ (pl_operator.create_trigger(new_trigger_info,
                                       trans,
                                       error_info,
                                       dep_infos,
                                       table_schema_version,
                                       nullptr,
                                       false/*is_update_table_schema_version*/));
      }
    }
  }
  return ret;
}

int ObPLDDLService::drop_trigger_in_drop_user(ObMySQLTransaction &trans,
                                            rootserver::ObDDLOperator &ddl_operator,
                                            ObSchemaGetterGuard &schema_guard,
                                            const uint64_t tenant_id,
                                            const uint64_t user_id)
{
  int ret = OB_SUCCESS;
  uint64_t trigger_id = OB_INVALID_ID;
  const ObTriggerInfo *trigger_info = NULL;
  const ObUserInfo *user_info = NULL;
  ObPLDDLOperator pl_operator(ddl_operator.get_multi_schema_service(), ddl_operator.get_sql_proxy());
  OZ (schema_guard.get_user_info(tenant_id, user_id, user_info));
  OV (OB_NOT_NULL(user_info));
  if (OB_SUCC(ret)) {
    const ObIArray<uint64_t> &trigger_id_list = user_info->get_trigger_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < trigger_id_list.count(); i++) {
      OX (trigger_id = trigger_id_list.at(i));
      OZ (schema_guard.get_trigger_info(tenant_id, trigger_id, trigger_info), trigger_id);
      OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_id);
      OV (!trigger_info->is_in_recyclebin(), OB_ERR_UNEXPECTED, trigger_id);
      OZ (pl_operator.drop_trigger(*trigger_info, trans, NULL));
    }
  }
  return ret;
}
//----End of functions for managing trigger----

template <typename ArgType>
int ObPLDDLService::check_env_before_ddl(share::schema::ObSchemaGetterGuard &schema_guard,
                                         const ArgType &arg,
                                         rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service.check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_FAIL(ddl_service.get_tenant_schema_guard_with_version_in_inner_table(arg.exec_tenant_id_,
                                                                                     schema_guard))) {
    LOG_WARN("get schema guard with version in inner table failed", K(ret));
  } else if (OB_FAIL(ddl_service.check_parallel_ddl_conflict(schema_guard, arg))) {
    LOG_WARN("check parallel ddl conflict failed", K(ret));
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
