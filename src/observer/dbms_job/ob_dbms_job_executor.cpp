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

#define USING_LOG_PREFIX SERVER

#include "ob_dbms_job_utils.h"
#include "ob_dbms_job_executor.h"

#include "lib/oblog/ob_log.h"
#include "lib/mysqlclient/ob_isql_connection.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "share/schema/ob_schema_getter_guard.h"

#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql.h"
#ifdef OB_BUILD_AUDIT_SECURITY
#include "pl/sys_package/ob_dbms_audit_mgmt.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share::schema;
using namespace observer;
using namespace sql;

namespace dbms_job
{

int ObDBMSJobExecutor::init(
  common::ObMySQLProxy *sql_proxy, ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("job scheduler executor already init", K(inited_), K(ret));
  } else if (OB_ISNULL(sql_proxy_ = sql_proxy)
          || OB_ISNULL(schema_service_ = schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy or schema service is null", K(sql_proxy), K(ret));
  } else if (OB_FAIL(job_utils_.init(sql_proxy_))) {
    LOG_WARN("fail to init action record", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObDBMSJobExecutor::init_session(
  sql::ObSQLSessionInfo &session,
  ObSchemaGetterGuard &schema_guard,
  const ObString &tenant_name, uint64_t tenant_id,
  const ObString &database_name, uint64_t database_id,
  const ObUserInfo* user_info,
  ObExecEnv &exec_env)
{
  int ret = OB_SUCCESS;
  ObObj oracle_sql_mode;
  ObArenaAllocator *allocator = NULL;
  const bool print_info_log = true;
  const bool is_sys_tenant = true;
  ObPCMemPctConf pc_mem_conf;
  ObObj oracle_mode;
  oracle_mode.set_int(1);
  oracle_sql_mode.set_uint(ObUInt64Type, DEFAULT_ORACLE_MODE);
  OZ (session.init(1, 1, allocator));
  OX (session.set_inner_session());
  OZ (session.load_default_sys_variable(print_info_log, is_sys_tenant));
  OZ (session.update_max_packet_size());
  OZ (session.init_tenant(tenant_name.ptr(), tenant_id));
  OZ (session.load_all_sys_vars(schema_guard));
  OZ (session.update_sys_variable(share::SYS_VAR_SQL_MODE, oracle_sql_mode));
  OZ (session.update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, oracle_mode));
  OZ (session.update_sys_variable(share::SYS_VAR_NLS_DATE_FORMAT,
                                  ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT));
  OZ (session.update_sys_variable(share::SYS_VAR_NLS_TIMESTAMP_FORMAT,
                                  ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT));
  OZ (session.update_sys_variable(share::SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT,
                                  ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT));
  OZ (session.set_default_database(database_name));
  OZ (session.get_pc_mem_conf(pc_mem_conf));
  CK (OB_NOT_NULL(GCTX.sql_engine_));
  OX (session.set_database_id(database_id));
  OZ (session.set_user(
    user_info->get_user_name(), user_info->get_host_name_str(), user_info->get_user_id()));
  OX (session.set_user_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT));
  OX (session.init_use_rich_format());

  OZ (exec_env.store(session));
  return ret;
}

int ObDBMSJobExecutor::init_env(ObDBMSJobInfo &job_info, ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_info = NULL;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  ObSEArray<const ObUserInfo *, 1> user_infos;
  const ObUserInfo* user_info = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  share::schema::ObUserLoginInfo login_info;
  ObExecEnv exec_env;
  CK (OB_NOT_NULL(schema_service_));
  CK (job_info.valid());
  OZ (schema_service_->get_tenant_schema_guard(job_info.get_tenant_id(), schema_guard));
  OZ (schema_guard.get_tenant_info(job_info.get_tenant_id(), tenant_info));
  OZ (schema_guard.get_user_info(
    job_info.get_tenant_id(), job_info.get_lowner(), user_infos));
  OZ (schema_guard.get_database_schema(
    job_info.get_tenant_id(), job_info.get_lowner(), database_schema));
  OV (1 == user_infos.count(), OB_ERR_UNEXPECTED, K(job_info));
  CK (OB_NOT_NULL(user_info = user_infos.at(0)));
  CK (OB_NOT_NULL(user_info));
  CK (OB_NOT_NULL(tenant_info));
  CK (OB_NOT_NULL(database_schema));
  OZ (exec_env.init(job_info.get_exec_env()));
  OZ (init_session(session,
                   schema_guard,
                   tenant_info->get_tenant_name(),
                   job_info.get_tenant_id(),
                   database_schema->get_database_name(),
                   database_schema->get_database_id(),
                   user_info,
                   exec_env));
  return ret;
}

int ObDBMSJobExecutor::run_dbms_job(
  uint64_t tenant_id, ObDBMSJobInfo &job_info, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
    UNUSEDx(tenant_id, job_info, allocator);
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support", K(ret));
#else
  ObSqlString what;
  ObInnerSQLConnectionPool *pool = NULL;
  ObInnerSQLConnection *conn = NULL;
  SMART_VAR(ObSQLSessionInfo, session) {
    int64_t affected_rows = 0;
    CK (OB_LIKELY(inited_));
    CK (OB_NOT_NULL(sql_proxy_));
    CK (sql_proxy_->is_inited());
    if (OB_SUCC(ret)) {
      if (job_info.is_mysql_audit_job()) {
        pl::ObDbmsAuditMgmt::ObAuditParam audit_param;
        OZ (pl::ObDbmsAuditMgmt::parse_mysql_job_param(job_info.get_what(), audit_param));
        OZ (pl::ObDbmsAuditMgmt::clean_audit_trail_impl(audit_param, allocator, sql_proxy_, tenant_id), audit_param);
      } else {
        ObOracleSqlProxy oracle_proxy(*(static_cast<ObMySQLProxy *>(sql_proxy_)));
        CK (job_info.valid());
        CK (job_info.get_what().length() != 0);
        OZ (what.append_fmt("BEGIN %.*s END;",
                            job_info.get_what().length(), job_info.get_what().ptr()));
        CK (OB_NOT_NULL(pool = static_cast<ObInnerSQLConnectionPool *>(oracle_proxy.get_pool())));
        OZ (init_env(job_info, session));
        OZ (pool->acquire_spi_conn(&session, conn));
        OZ (conn->execute_write(tenant_id, what.string().ptr(), affected_rows));
        if (OB_NOT_NULL(conn)) {
          sql_proxy_->close(conn, ret);
        }
      }
    }
  }
#endif
  return ret;
}

int ObDBMSJobExecutor::run_dbms_job(uint64_t tenant_id, uint64_t job_id)
{
  int ret = OB_SUCCESS;
  ObDBMSJobInfo job_info;
  ObArenaAllocator allocator;

  THIS_WORKER.set_timeout_ts(INT64_MAX);

  OZ (job_utils_.get_dbms_job_info(tenant_id, job_id, allocator, job_info));

  if (OB_SUCC(ret)) {
    OZ (job_utils_.update_for_start(tenant_id, job_info));

    OZ (run_dbms_job(tenant_id, job_info, allocator));

    int tmp_ret = OB_SUCCESS;
    ObString errmsg = common::ob_get_tsi_err_msg(ret);
    if (errmsg.empty() && ret != OB_SUCCESS) {
      errmsg = ObString(strlen(ob_errpkt_strerror(ret, lib::is_oracle_mode())),
                        ob_errpkt_strerror(ret, lib::is_oracle_mode()));
    }
    if ((tmp_ret = job_utils_.update_for_end(tenant_id, job_info, ret, errmsg)) != OB_SUCCESS) {
      LOG_WARN("update dbms job failed", K(tmp_ret), K(ret));
    }
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

}
}
