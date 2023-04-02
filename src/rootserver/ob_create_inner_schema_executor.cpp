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
#include "ob_create_inner_schema_executor.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace oceanbase::sql;

int64_t ObCreateInnerSchemaTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask *ObCreateInnerSchemaTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObAsyncTask *task = nullptr;
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || buf_size < deep_copy_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(deep_copy_size), K(buf_size));
  } else {
    task = new (buf) ObCreateInnerSchemaTask(*executor_);
  }
  return task;
}

int ObCreateInnerSchemaTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] start to execute create inner schema task", K(start));
  if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, executor must not be NULL", K(ret));
  } else if (OB_FAIL(executor_->execute())) {
    LOG_WARN("fail to execute create inner schema task", K(ret));
  }
  LOG_INFO("[UPGRADE] finish create inner schema task",
           K(ret), "cost_time", ObTimeUtility::current_time() - start);
  return ret;
}

ObCreateInnerSchemaExecutor::ObCreateInnerSchemaExecutor()
  : is_inited_(false), is_stopped_(false), execute_(false),
    rwlock_(ObLatchIds::CREATE_INNER_SCHEMA_EXECUTOR_LOCK), schema_service_(nullptr), sql_proxy_(nullptr),
    rpc_proxy_(nullptr)
{
}

int ObCreateInnerSchemaExecutor::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObCommonRpcProxy &rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCreateInnerSchemaExecutor has been inited twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
    is_stopped_ = false;
    rpc_proxy_ = &rpc_proxy;
  }
  return ret;
}

int ObCreateInnerSchemaExecutor::stop()
{
  int ret = OB_SUCCESS;
  const uint64_t WAIT_US = 100 * 1000L; //100ms
  const uint64_t MAX_WAIT_US = 10 * 1000 * 1000L; //10s
  const int64_t start = ObTimeUtility::current_time();
  {
    SpinWLockGuard guard(rwlock_);
    is_stopped_ = true;
  }
  while (OB_SUCC(ret)) {
    if (ObTimeUtility::current_time() - start > MAX_WAIT_US) {
      ret = OB_TIMEOUT;
      LOG_WARN("use too much time", K(ret), "cost_us", ObTimeUtility::current_time() - start);
    } else if (!execute_) {
      break;
    } else {
      ob_usleep(WAIT_US);
    }
  }
  return ret;
}

int ObCreateInnerSchemaExecutor::check_stop()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("executor should stopped", K(ret));
  }
  return ret;
}

void ObCreateInnerSchemaExecutor::start()
{
  SpinWLockGuard guard(rwlock_);
  is_stopped_ = false;
}

int ObCreateInnerSchemaExecutor::set_execute_mark()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_stopped_ || execute_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't run job at the same time", K(ret), K(is_stopped_), K(execute_));
  } else {
    execute_ = true;
  }
  return ret;
}

int ObCreateInnerSchemaExecutor::execute()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCreateInnerSchemaExecutor has not been inited", K(ret));
  } else if (OB_FAIL(set_execute_mark())) {
    LOG_WARN("fail to set execute mark", K(ret));
  } else {
    int64_t job_id = OB_INVALID_ID;
    // unused
    // bool can_run_job = false;
    ObRsJobType job_type = ObRsJobType::JOB_TYPE_CREATE_INNER_SCHEMA;
    if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, *sql_proxy_, "tenant_id", 0))) {
      LOG_WARN("fail to create rs job", K(ret));
    } else if (job_id <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job is is invalid", K(ret), K(job_id));
    } else if (OB_FAIL(do_create_inner_schema())) {
      LOG_WARN("fail to do create inner schema job", K(ret));
    }
    if (job_id > 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, *sql_proxy_))) {
        LOG_ERROR("fail to complete job", K(tmp_ret), K(ret), K(job_id));
                ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
    // no need lock, because single-machine concurrency is prevented in the process
    execute_ = false;
  }
  return ret;
}

int ObCreateInnerSchemaExecutor::ur_exists(
    ObSchemaGetterGuard &schema_guard,
    uint64_t tenant_id,
    const uint64_t ur_id,
    bool &exists)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = NULL;

  OZ (schema_guard.get_user_info(tenant_id, ur_id, user_info));
  if (OB_SUCC(ret)) {
    exists = (user_info != NULL);
  }
  return ret;
}

int ObCreateInnerSchemaExecutor::add_inner_role(
    uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  // unused
  // int n;
  int64_t affected_rows = 0;
  bool exists;
  OZ (ur_exists(schema_guard, tenant_id, OB_ORA_CONNECT_ROLE_ID, exists));
  if (OB_SUCC(ret) && !exists) {
    OZ (sql.assign_fmt("create role \"CONNECT\""));
    CK (sql_proxy != NULL);
    OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));
  }
  OZ (sql.assign_fmt("grant create session to connect"));
  CK (sql_proxy != NULL);
  OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));

  OZ (ur_exists(schema_guard, tenant_id, OB_ORA_RESOURCE_ROLE_ID, exists));
  if (OB_SUCC(ret) && !exists) {
    OZ (sql.assign_fmt("create role \"RESOURCE\""));
    CK (sql_proxy != NULL);
    OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));
  }
  OZ (sql.assign_fmt("grant create table, create type, create trigger, "
                     "create procedure, create sequence to resource"));
  CK (sql_proxy != NULL);
  OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));

  OZ (ur_exists(schema_guard, tenant_id, OB_ORA_PUBLIC_ROLE_ID, exists));
  if (OB_SUCC(ret) && !exists) {
    OZ (sql.assign_fmt("create role \"PUBLIC\""));
    CK (sql_proxy != NULL);
    OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));
  }

  OZ (ur_exists(schema_guard, tenant_id, OB_ORA_DBA_ROLE_ID, exists));
  if (OB_SUCC(ret) && !exists) {
    OZ (sql.assign_fmt("create role \"DBA\""));
    CK (sql_proxy != NULL);
    OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));
  }
  OZ (sql.assign_fmt("grant create session, create table, create any table, "
                      " alter any table, backup any table, drop any table, "
                      " lock any table, comment any table, select any table, "
                      " insert any table, update any table, delete any table, "
                      " flashback any table, create role, drop any role, "
                      " grant any role, alter any role, grant any privilege, "
                      " grant any object privilege, create any index, "
                      " alter any index, drop any index, create any view, "
                      " drop any view, create view, select any dictionary, "
                      " create procedure, create any procedure, "
                      " alter any procedure, drop any procedure, "
                      " execute any procedure, create synonym, "
                      " create any synonym, drop any synonym, "
                      " create public synonym, drop public synonym, "
                      " create sequence, create any sequence, "
                      " alter any sequence, drop any sequence, "
                      " select any sequence, create trigger, "
                      " create any trigger, alter any trigger, "
                      " drop any trigger, create profile, "
                      " alter profile, drop profile, "
                      " create user,"
                      " alter user, drop user, "
                      " create type, create any type, "
                      " alter any type, drop any type, "
                      " execute any type, under any type, "
                      " purge dba_recyclebin, create any outline, "
                      " alter any outline, drop any outline, "
                      " syskm, create tablespace, "
                      " alter tablespace, drop tablespace, "
                      " show process, alter system, "
                      " create database link, create public database link, "
                      " drop database link, alter session, alter database, "
                      " create any directory, drop any directory, "
                      " debug connect session, debug any procedure, "
                      " create any context, drop any context to dba"));
  CK (sql_proxy != NULL);
  OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, ORACLE_MODE));
  
  return ret;
}

int ObCreateInnerSchemaExecutor::do_create_inner_schema_by_tenant(
    uint64_t tenant_id,
    oceanbase::lib::Worker::CompatMode compat_mode,
    ObSchemaGetterGuard &schema_guard,
    common::ObMySQLProxy *sql_proxy,
    obrpc::ObCommonRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  
  if (compat_mode == lib::Worker::CompatMode::ORACLE) {
    //at first, do profile
    OZ (check_and_create_default_profile(schema_guard, tenant_id, rpc_proxy));
    OZ (add_password_and_lock_security_sys_users(tenant_id, schema_guard, sql_proxy), 
        tenant_id);
    OZ (add_inner_role(tenant_id, schema_guard, sql_proxy), tenant_id);
  } else if (compat_mode == lib::Worker::CompatMode::MYSQL) {
    OZ (check_and_create_inner_keysore(schema_guard, tenant_id, rpc_proxy));
    OZ (add_audit_user(tenant_id, schema_guard, sql_proxy), tenant_id);
  }
  return ret;
}

int ObCreateInnerSchemaExecutor::do_create_inner_schema()
{
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("[UPGRADE] execute job create_inner_schema start", K(start));
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("executor should stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get tenant ids failed", K(ret));
  } else if (OB_ISNULL(sql_proxy_))  {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret));
  } else {
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    oceanbase::lib::Worker::CompatMode compat_mode;
    for (int64_t i = tenant_ids.size() - 1; i >= 0 && OB_SUCC(ret); i--) {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("create inner user or role is stopped", K(ret));
      } else {
        tenant_id = tenant_ids.at(i);
        if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, compat_mode))) {
          LOG_WARN("get tenant compat mode failed", K(ret));
        } else {
          OZ (schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
          OZ (do_create_inner_schema_by_tenant(tenant_id, compat_mode, schema_guard,
                                               sql_proxy_, rpc_proxy_));
        }
      }
    }
  }
  LOG_INFO("[UPGRADE] execute job create_inner_schema finish",
           K(ret), "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

int ObCreateInnerSchemaExecutor::check_and_create_inner_keysore(ObSchemaGetterGuard &schema_guard,
                                                                int64_t tenant_id,
                                                                obrpc::ObCommonRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  
  const ObKeystoreSchema *ks_schema = NULL;
  if (OB_SYS_TENANT_ID == tenant_id) {
    /*do nothing*/
  } else if (OB_FAIL(schema_guard.get_keystore_schema(tenant_id, ks_schema))) {
    LOG_WARN("fail to get keystore schema", K(ret));
  } else if (OB_NOT_NULL(ks_schema)) {
    /*do nothing*/
  } else {
    ObKeystoreDDLArg arg;
    ObKeystoreSchema &keystore_schema = arg.schema_;
    arg.exec_tenant_id_ = tenant_id;
    arg.type_ = ObKeystoreDDLArg::DDLType::CREATE_KEYSTORE;
    int64_t keystore_id = OB_MYSQL_TENANT_INNER_KEYSTORE_ID;
    keystore_schema.set_keystore_id(keystore_id);
    keystore_schema.set_tenant_id(tenant_id);
    keystore_schema.set_status(2);
    keystore_schema.set_keystore_name("mysql_keystore");
    if (OB_ISNULL(rpc_proxy)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get common rpc proxy failed");
    } else if (OB_FAIL(rpc_proxy->do_keystore_ddl(arg))) {
      LOG_WARN("alter keystore error", K(ret));
    }
  }
  return ret;
}

int ObCreateInnerSchemaExecutor::check_and_create_default_profile(ObSchemaGetterGuard &schema_guard,
                                                                  int64_t tenant_id,
                                                                  obrpc::ObCommonRpcProxy* rpc_proxy)
{
  int ret = OB_SUCCESS;
  const ObProfileSchema *profile_schema = NULL;
  int64_t profile_id = OB_ORACLE_TENANT_INNER_PROFILE_ID;
  if (OB_FAIL(schema_guard.get_profile_schema_by_id(tenant_id, profile_id, profile_schema))) {
    if (OB_OBJECT_NAME_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get profile schema", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(profile_schema)) {
    /*do nothing*/
  } else {
    obrpc::ObProfileDDLArg arg;
    ObProfileSchema &profile_schema = arg.schema_;
    arg.exec_tenant_id_ = tenant_id;
    arg.ddl_type_ = ObSchemaOperationType::OB_DDL_CREATE_PROFILE;
    profile_schema.set_tenant_id(tenant_id);
    profile_schema.set_profile_id(profile_id);
    profile_schema.set_password_lock_time(USECS_PER_DAY);
    profile_schema.set_password_life_time(ObProfileSchema::UNLIMITED_VALUE);
    profile_schema.set_password_grace_time(ObProfileSchema::UNLIMITED_VALUE);
    profile_schema.set_failed_login_attempts(ObProfileSchema::UNLIMITED_VALUE);
    profile_schema.set_password_verify_function("NULL");
    profile_schema.set_profile_name("DEFAULT");
    if (OB_ISNULL(rpc_proxy)) {
      ret = OB_NOT_INIT;
      LOG_WARN("get common rpc proxy failed");
    } else if (OB_FAIL(rpc_proxy->do_profile_ddl(arg))) {
      LOG_WARN("alter profile error", K(ret));
    }
  }
  return ret;
}

int ObCreateInnerSchemaExecutor::add_password_and_lock_security_sys_users(
    uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  CK (sql_proxy != NULL);
  const char* user_names[2] = {OB_ORA_LBACSYS_NAME, OB_ORA_AUDITOR_NAME};
  uint64_t user_id[2] = {OB_ORA_LBACSYS_USER_ID, OB_ORA_AUDITOR_USER_ID};
  for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
    bool exists = false;
    OZ (ur_exists(schema_guard, tenant_id, user_id[i], exists));
    //password
    OZ (sql.assign_fmt("%s user %s identified by %s",
                       !exists ? "create" : "alter",
                       user_names[i], user_names[i]));
    OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, 1));
    //lock
    OZ (sql.assign_fmt("alter user %s account lock", user_names[i]));
    OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, 1));
  }
  return ret;
}

int ObCreateInnerSchemaExecutor::can_execute()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_stopped_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("status not matched", K(ret),
             "stopped", is_stopped_ ? "true" : "false");
  }
  return ret;
}

int ObCreateInnerSchemaExecutor::add_audit_user(
    uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  CK (sql_proxy != NULL);
  const char* user_name = OB_ORA_AUDITOR_NAME;
  uint64_t user_id = OB_ORA_AUDITOR_USER_ID;
  bool exists = false;
  OZ (ur_exists(schema_guard, tenant_id, user_id, exists));
  if (exists) {
     /*do nothing*/
  } else {
    //password
    OZ (sql.assign_fmt("%s user %s identified by '%s'",
                       "create", user_name, user_name));
    OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, 0/*MySQL*/));
    //lock
    OZ (sql.assign_fmt("alter user %s account lock", user_name));
    OZ (sql_proxy->write(tenant_id, sql.ptr(), affected_rows, 0/*MySQL*/));
  }
  return ret;
}
