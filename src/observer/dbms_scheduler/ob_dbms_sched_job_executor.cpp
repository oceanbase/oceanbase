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

#include "ob_dbms_sched_job_utils.h"
#include "ob_dbms_sched_job_executor.h"

#include "lib/oblog/ob_log.h"
#include "lib/mysqlclient/ob_isql_connection.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "share/schema/ob_schema_getter_guard.h"

#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql.h"
#include "sql/executor/ob_executor_rpc_processor.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share::schema;
using namespace share;
using namespace observer;
using namespace sql;

namespace dbms_scheduler
{

int ObDBMSSchedJobExecutor::init(
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
  } else if (OB_FAIL(table_operator_.init(sql_proxy_))) {
    LOG_WARN("fail to init action record", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObDBMSSchedJobExecutor::init_session(
  sql::ObSQLSessionInfo &session,
  ObSchemaGetterGuard &schema_guard,
  const ObString &tenant_name, uint64_t tenant_id,
  const ObString &database_name, uint64_t database_id,
  const ObUserInfo* user_info,
  ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObPrivSet db_priv_set = OB_PRIV_SET_EMPTY;
  ObArenaAllocator *allocator = NULL;
  const bool print_info_log = true;
  const bool is_sys_tenant = true;
  ObPCMemPctConf pc_mem_conf;
  ObObj compatibility_mode;
  ObObj sql_mode;
  if (job_info.is_oracle_tenant_) {
    compatibility_mode.set_int(1);
    sql_mode.set_uint(ObUInt64Type, DEFAULT_ORACLE_MODE);
  } else {
    compatibility_mode.set_int(0);
    sql_mode.set_uint(ObUInt64Type, DEFAULT_MYSQL_MODE);
  }
  OX (session.set_inner_session());
  OZ (session.load_default_sys_variable(print_info_log, is_sys_tenant));
  OZ (session.update_max_packet_size());
  OZ (session.init_tenant(tenant_name.ptr(), tenant_id));
  OZ (session.load_all_sys_vars(schema_guard));
  OZ (session.update_sys_variable(share::SYS_VAR_SQL_MODE, sql_mode));
  OZ (session.update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, compatibility_mode));
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
  OX (session.set_priv_user_id(user_info->get_user_id()));
  OX (session.set_user_priv_set(user_info->get_priv_set()));
  OZ (schema_guard.get_db_priv_set(tenant_id, user_info->get_user_id(), database_name, db_priv_set));
  OX (session.set_db_priv_set(db_priv_set));
  OX (session.get_enable_role_array().reuse());
  for (int i = 0; OB_SUCC(ret) && i < user_info->get_role_id_array().count(); ++i) {
    if (user_info->get_disable_option(user_info->get_role_id_option_array().at(i)) == 0) {
      OZ (session.get_enable_role_array().push_back(user_info->get_role_id_array().at(i)));
    }
  }
  OX (session.set_shadow(true));
  if (OB_SUCC(ret)) {
    if (job_info.is_date_expression_job_class()) {
      // set larger timeout for mview scheduler jobs
      const int64_t QUERY_TIMEOUT_US = (24 * 60 * 60 * 1000000L); // 24hours
      const int64_t TRX_TIMEOUT_US = (24 * 60 * 60 * 1000000L); // 24hours
      ObObj query_timeout_obj;
      ObObj trx_timeout_obj;
      query_timeout_obj.set_int(QUERY_TIMEOUT_US);
      trx_timeout_obj.set_int(TRX_TIMEOUT_US);
      OZ (session.update_sys_variable(SYS_VAR_OB_QUERY_TIMEOUT, query_timeout_obj));
      OZ (session.update_sys_variable(SYS_VAR_OB_TRX_TIMEOUT, trx_timeout_obj));
    } else if (job_info.is_olap_async_job_class()) {
      const int64_t QUERY_TIMEOUT_US = ((job_info.get_max_run_duration() - OLAP_ASYNC_JOB_DEVIATION_SECOND) * 1000000L);
      const int64_t TRX_TIMEOUT_US = ((job_info.get_max_run_duration() - OLAP_ASYNC_JOB_DEVIATION_SECOND) * 1000000L);
      ObObj query_timeout_obj;
      ObObj trx_timeout_obj;
      query_timeout_obj.set_int(QUERY_TIMEOUT_US);
      trx_timeout_obj.set_int(TRX_TIMEOUT_US);
      OZ (session.update_sys_variable(SYS_VAR_OB_QUERY_TIMEOUT, query_timeout_obj));
      OZ (session.update_sys_variable(SYS_VAR_OB_TRX_TIMEOUT, trx_timeout_obj));
    }
  }

  return ret;
}

int ObDBMSSchedJobExecutor::init_env(ObDBMSSchedJobInfo &job_info, ObSQLSessionInfo &session)
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
  OZ (schema_guard.get_database_schema(
    job_info.get_tenant_id(), job_info.get_cowner(), database_schema));
  if (OB_SUCC(ret)) {
    if (job_info.is_oracle_tenant()) {
      OZ (schema_guard.get_user_info(
        job_info.get_tenant_id(), job_info.get_powner(), user_infos));
      OV (1 == user_infos.count(), OB_ERR_UNEXPECTED, K(job_info), K(user_infos));
      CK (OB_NOT_NULL(user_info = user_infos.at(0)));
    } else if (job_info.get_user_id() != OB_INVALID_ID) {
      OZ (schema_guard.get_user_info(
        job_info.get_tenant_id(), job_info.get_user_id(), user_info));
    } else {
      ObString user = job_info.get_powner();
      if (OB_SUCC(ret)) {
        const char *c = user.reverse_find('@');
        if (OB_ISNULL(c)) {
          OZ (schema_guard.get_user_info(
            job_info.get_tenant_id(), user, user_infos));
          if (OB_SUCC(ret) && user_infos.count() > 1) {
            OZ(ObDBMSSchedJobUtils::reserve_user_with_minimun_id(user_infos));
          }
          OV (1 == user_infos.count(), 0 == user_infos.count() ? OB_USER_NOT_EXIST : OB_ERR_UNEXPECTED, K(job_info), K(user_infos));
          CK (OB_NOT_NULL(user_info = user_infos.at(0)));
        } else {
          ObString user_name;
          ObString host_name;
          user_name = user.split_on(c);
          host_name = user;
          OZ (schema_guard.get_user_info(
            job_info.get_tenant_id(), user_name, host_name, user_info));
        }
      }
    }
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
                    job_info));
    OZ (exec_env.store(session));
  }
  return ret;
}

int ObDBMSSchedJobExecutor::create_session(
    const uint64_t tenant_id,
    ObFreeSessionCtx &free_session_ctx,
    ObSQLSessionInfo *&session_info)
{
  int ret = OB_SUCCESS;
  uint32_t sid = sql::ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_mgr_ is null", KR(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("alloc session id failed", KR(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(
                tenant_id, sid, proxy_sid, ObTimeUtility::current_time(), session_info))) {
    LOG_WARN("create session failed", K(ret), K(sid));
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session_info = NULL;
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session info is null", K(ret));
  } else {
    free_session_ctx.sessid_ = sid;
    free_session_ctx.proxy_sessid_ = proxy_sid;
  }
  return ret;
}

int ObDBMSSchedJobExecutor::destroy_session(
    ObFreeSessionCtx &free_session_ctx,
    ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_mgr_ is null", KR(ret));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_info is null", KR(ret));
  } else {
    session_info->set_session_sleep();
    GCTX.session_mgr_->revert_session(session_info);
    GCTX.session_mgr_->free_session(free_session_ctx);
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
  }
  return ret;
}

int ObDBMSSchedJobExecutor::run_dbms_sched_job(
  uint64_t tenant_id, ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString what;
  ObInnerSQLConnectionPool *pool = NULL;
  ObInnerSQLConnection *conn = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObFreeSessionCtx free_session_ctx;
  int64_t affected_rows = 0;
  CK (OB_LIKELY(inited_));
  CK (OB_NOT_NULL(sql_proxy_));
  CK (sql_proxy_->is_inited());
  CK (job_info.valid());
  CK ((job_info.get_what().length() != 0) || (job_info.get_program_name().length() != 0));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObDBMSSchedJobExecutor::create_session(tenant_id, free_session_ctx, session_info))) {
    LOG_WARN("failed to create session", KR(ret), K(tenant_id));
  } else {
    if (job_info.get_what().length() != 0) { // action
      if (job_info.is_oracle_tenant_) {
        OZ (what.append_fmt("BEGIN %.*s; END;",
            job_info.get_what().length(), job_info.get_what().ptr()));
      } else if (job_info.is_olap_async_job_class()){
        OZ (what.append_fmt("%.*s",
            job_info.get_what().length(), job_info.get_what().ptr()));
      } else {
        //mysql mode not support anonymous block
        OZ (what.append_fmt("CALL %.*s;",
            job_info.get_what().length(), job_info.get_what().ptr()));
      }
    } else { // program
      ObSqlString sql;
      ObString program_action;
      uint64_t number_of_argument = 0;
      OZ (sql.assign_fmt("select program_action, number_of_argument from %s where program_name = \'%.*s\'",
        OB_ALL_TENANT_SCHEDULER_PROGRAM_TNAME,
        job_info.get_program_name().length(),
        job_info.get_program_name().ptr()));
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
          LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id), K(job_info.get_program_name().ptr()), K(job_info.get_job_name().ptr()));
        } else if (OB_NOT_NULL(result.get_result())) {
          if (OB_SUCCESS == (ret = result.get_result()->next())) {
            EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*(result.get_result()), "program_action", program_action);
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*(result.get_result()), "number_of_argument", number_of_argument, uint64_t);
            if (OB_SUCC(ret) && (result.get_result( )->next()) != OB_ITER_END) {
              LOG_ERROR("got more than one row for dbms sched program!", K(ret), K(tenant_id), K(job_info.get_job_name().ptr()), K(job_info.get_program_name().ptr()));
              ret = OB_ERR_UNEXPECTED;
            }
          } else if (OB_ITER_END == ret) {
            LOG_INFO("program not exists, may delete alreay!", K(ret), K(tenant_id), K(job_info.get_program_name().ptr()), K(job_info.get_program_name().ptr()));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get next", K(ret), K(tenant_id), K(job_info.get_job_name().ptr()), K(job_info.get_program_name().ptr()));
          }
        }
      }
      OZ (what.append_fmt("BEGIN %.*s(",
        program_action.length(), program_action.ptr()));
      if (OB_SUCC(ret) && (0 != number_of_argument)) {
        ObString argument_value;
        for (int i = 1; OB_SUCC(ret) && i <= number_of_argument; i++) {
          argument_value.reset();
          OZ (sql.assign_fmt("select default_value from %s where program_name = \'%.*s\' and job_name = \'%.*s\' and argument_position = %d and is_for_default = 0",
            OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TNAME,
            job_info.get_program_name().length(),
            job_info.get_program_name().ptr(),
            job_info.get_job_name().length(),
            job_info.get_job_name().ptr(),
            i));
          SMART_VAR(ObMySQLProxy::MySQLResult, result) {
            if (OB_FAIL(sql_proxy_->read(result, tenant_id, sql.ptr()))) {
              LOG_WARN("execute query failed", K(ret), K(sql), K(result.get_result()), K(tenant_id), K(job_info.get_job_name().ptr()));
            } else if (OB_NOT_NULL(result.get_result())) {
              if (OB_SUCCESS == (ret = result.get_result()->next())) {
                EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*(result.get_result()), "default_value", argument_value);
                if (OB_SUCC(ret) && (result.get_result()->next()) != OB_ITER_END) {
                  LOG_ERROR("got more than one row for argument!", K(ret), K(tenant_id), K(job_info.get_job_name().ptr()), K(job_info.get_program_name().ptr()));
                  ret = OB_ERR_UNEXPECTED;
                }
              } else if (OB_ITER_END == ret) {
                LOG_INFO("job argument not exists, use default");
                ret = OB_SUCCESS;
                OZ (sql.assign_fmt("select default_value from %s where program_name = \'%.*s\' and job_name = \'%s\' and argument_position = %d and is_for_default = 1",
                  OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TNAME,
                  job_info.get_program_name().length(),
                  job_info.get_program_name().ptr(),
                  "default",
                  i));
                SMART_VAR(ObMySQLProxy::MySQLResult, tmp_result) {
                  if (OB_FAIL(sql_proxy_->read(tmp_result, tenant_id, sql.ptr()))) {
                    LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id), K(job_info.get_job_name().ptr()));
                  } else if (OB_NOT_NULL(tmp_result.get_result())) {
                    if (OB_SUCCESS == (ret = tmp_result.get_result()->next())) {
                      EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*(tmp_result.get_result()), "default_value", argument_value);
                      if (OB_SUCC(ret) && (tmp_result.get_result()->next()) != OB_ITER_END) {
                        LOG_ERROR("got more than one row for argument!", K(ret), K(tenant_id), K(job_info.get_job_name().ptr()), K(job_info.get_program_name().ptr()));
                        ret = OB_ERR_UNEXPECTED;
                      }
                    } else if (OB_ITER_END == ret) {
                      LOG_ERROR("program default argument not exists", K(sql.ptr()), K(job_info.get_program_name().ptr()));
                    } else {
                      LOG_WARN("failed to get next", K(ret), K(tenant_id), K(job_info.get_job_name().ptr()));
                    }
                  }
                }
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to get next", K(ret), K(tenant_id), K(job_info.get_job_name().ptr()));
              }
            }
          }
          if (i == 1) {
            OZ (what.append_fmt("\'%.*s\'", argument_value.length(), argument_value.ptr()));
          } else {
            OZ (what.append_fmt(",\'%.*s\'", argument_value.length(), argument_value.ptr()));
          }
        }
        OZ (what.append_fmt("); END;"));
      } else {
        LOG_ERROR("number_of_argument not exist or not right", K(ret), K(number_of_argument));
      }
    }
    if (OB_SUCC(ret)) {
      ObWorkerSessionGuard worker_session_guard(session_info);
      OZ (ObDBMSSchedJobExecutor::init_env(job_info, *session_info));
      CK (OB_NOT_NULL(pool = static_cast<ObInnerSQLConnectionPool *>(sql_proxy_->get_pool())));
      OX (session_info->set_job_info(&job_info));
      OZ (pool->acquire_spi_conn(session_info, conn));
      OZ (conn->execute_write(tenant_id, what.string().ptr(), affected_rows));
      if (OB_NOT_NULL(conn)) {
        sql_proxy_->close(conn, ret);
      }
    }
  }
  if (NULL != session_info) {
    int tmp_ret = OB_SUCCESS;
    {
      ObSQLSessionInfo::LockGuard lock_guard(session_info->get_thread_data_lock());
      session_info->set_job_info(nullptr);
    }
    if (OB_TMP_FAIL(ObDBMSSchedJobExecutor::destroy_session(free_session_ctx, session_info))) {
      LOG_WARN("failed to destroy session", KR(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    } else {
      session_info = NULL;
    }
  }
  return ret;
}

int ObDBMSSchedJobExecutor::run_dbms_sched_job(uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, const ObString &job_name)
{
  int ret = OB_SUCCESS;
  ObDBMSSchedJobInfo job_info;
  ObArenaAllocator allocator("DBMSSchedTmp");

  THIS_WORKER.set_timeout_ts(INT64_MAX);

  OZ (table_operator_.get_dbms_sched_job_info(tenant_id, is_oracle_tenant, job_id, job_name, allocator, job_info));

  if (OB_SUCC(ret)) {
    if (job_info.is_killed()) { //Intercept user cancellation requests before the actual execution of the job
      OZ(table_operator_.update_for_kill(job_info));
    } else {
      OZ (run_dbms_sched_job(tenant_id, job_info));
      bool job_is_user_stop = false;
      if (OB_ERR_SESSION_INTERRUPTED == ret) { //It may have been the user interrupted, need to check.
        int tmp_user_stop_ret = OB_SUCCESS;
        bool job_is_killed = false;
        if ((tmp_user_stop_ret = table_operator_.get_dbms_sched_job_is_killed(job_info, job_is_killed)) != OB_SUCCESS) {
          LOG_WARN("double check get dbms sched job failed", K(tmp_user_stop_ret), K(ret));
        } else if (job_is_killed) {
          job_is_user_stop = true;
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (job_is_user_stop) {
        if ((OB_TMP_FAIL(table_operator_.update_for_kill(job_info)))) {
          LOG_WARN("update user stop dbms sched job failed", K(tmp_ret), K(ret));
        }
      } else {
        ObString errmsg = common::ob_get_tsi_err_msg(ret);
        if (errmsg.empty() && ret != OB_SUCCESS) {
          errmsg = ObString(strlen(ob_errpkt_strerror(ret, lib::is_oracle_mode())),
                            ob_errpkt_strerror(ret, lib::is_oracle_mode()));
        }
        if ((OB_TMP_FAIL(table_operator_.update_for_end(job_info, ret, errmsg)))) {
          LOG_WARN("update dbms sched job failed", K(tmp_ret), K(ret));
        }
      }
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

}
}
