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

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/ob_mview_sched_job_utils.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_executor.h"
#include "share/ob_global_stat_proxy.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/schema/ob_mview_info.h"
#include "share/schema/ob_mlog_info.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{
using namespace common;
using namespace dbms_scheduler;
using namespace share;
using namespace share::schema;
using namespace sql;
namespace storage
{
int ObMViewSchedJobUtils::generate_job_id(
    const uint64_t tenant_id,
    int64_t &job_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    ObCommonID raw_id;
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id_by_rpc(tenant_id, raw_id))) {
      LOG_WARN("failed to gen unique id by rpc", KR(ret), K(tenant_id));
    } else {
      job_id = raw_id.id() + ObDBMSSchedTableOperator::JOB_ID_OFFSET;
    }
  }
  return ret;
}

int ObMViewSchedJobUtils::generate_job_name(
    ObIAllocator &allocator,
    const int64_t job_id,
    const ObString &name_prefix,
    ObString &job_name)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = 128;  // job_name is varchar(128)
  int64_t pos = 0;
  char *name_buf = nullptr;
  if (OB_ISNULL(name_buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret));
  } else if (OB_FAIL(databuff_printf(name_buf,
                                     buf_len,
                                     pos,
                                     "%.*s%ld",
                                     name_prefix.length(),
                                     name_prefix.ptr(),
                                     job_id))) {
    LOG_WARN("failed to generate name buf", KR(ret));
  } else {
    job_name.assign_ptr(name_buf, static_cast<ObString::obstr_size_t>(pos));
  }
  return ret;
}

int ObMViewSchedJobUtils::generate_job_action(
    ObIAllocator &allocator,
    const ObString &job_action_func,
    const ObString &db_name,
    const ObString &table_name,
    ObString &job_action)
{
  int ret = OB_SUCCESS;
  char *job_action_buf = nullptr;
  int64_t misc_char_len = 10; // for ' ' and ( )
  int64_t buf_len = job_action_func.length() + db_name.length() + table_name.length() + misc_char_len;
  int64_t pos = 0;
  if (OB_ISNULL(job_action_buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret));
  } else if (OB_FAIL(databuff_printf(job_action_buf,
                              buf_len,
                              pos,
                              "%.*s('%.*s.%.*s')",
                              job_action_func.length(),
                              job_action_func.ptr(),
                              db_name.length(),
                              db_name.ptr(),
                              table_name.length(),
                              table_name.ptr()))) {
    LOG_WARN("failed to generate job action str", KR(ret));
  } else {
    job_action.assign_ptr(job_action_buf, static_cast<ObString::obstr_size_t>(pos));
  }
  return ret;
}

int ObMViewSchedJobUtils::add_scheduler_job(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t job_id,
    const ObString &job_name,
    const ObString &job_action,
    const ObObj &start_date,
    const ObString &repeat_interval,
    const ObString &exec_env)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    int64_t start_date_us = start_date.is_null() ? ObTimeUtility::current_time() : start_date.get_timestamp();
    int64_t end_date_us = 64060560000000000; // 4000-01-01
    HEAP_VAR(ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action;
      job_info.lowner_ = ObString("oceanbase");
      job_info.cowner_ = ObString("oceanbase");
      job_info.powner_ = lib::is_oracle_mode() ? ObString("SYS") : ObString("root@%");
      job_info.job_style_ = ObString("regular");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = start_date_us;
      job_info.end_date_ = end_date_us;
      job_info.repeat_interval_ = repeat_interval;
      job_info.enabled_ = 1;
      job_info.auto_drop_ = 0;
      job_info.max_run_duration_ = 24 * 60 * 60; // set to 1 day
      job_info.exec_env_ = exec_env;
      job_info.max_failures_ = 16;
      job_info.scheduler_flags_ = dbms_scheduler::ObDBMSSchedJobInfo::JOB_SCHEDULER_FLAG_DATE_EXPRESSION_JOB_CLASS; // for compat old version
      job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::MVIEW_JOB;

      if (OB_FAIL(ObDBMSSchedJobUtils::create_dbms_sched_job(
          sql_client, tenant_id, job_id, job_info))) {
        LOG_WARN("failed to create dbms scheduler job", KR(ret));
      }
    }
  }
  return ret;
}

int ObMViewSchedJobUtils::create_mview_scheduler_job(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t mview_id,
    const common::ObString &db_name,
    const common::ObString &table_name,
    const common::ObObj &start_date,
    const common::ObString &repeat_interval,
    const common::ObString &exec_env,
    ObArenaAllocator &allocator,
    common::ObString &job_name)
{
  int ret = OB_SUCCESS;
  ObString refresh_job;
  ObString job_prefix(ObMViewInfo::MVIEW_REFRESH_JOB_PREFIX);
  int64_t job_id = OB_INVALID_ID;
  job_name.reset();

  // job_name is generated as "job_prefix+job_id"
  if (OB_FAIL(ObMViewSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("failed to generate mview job id", KR(ret));
  } else if (OB_FAIL(ObMViewSchedJobUtils::generate_job_name(allocator, job_id, job_prefix,
                                                             refresh_job))) {
    LOG_WARN("failed to generate mview job name", KR(ret), K(tenant_id), K(job_id), K(job_prefix));
  } else {
    ObSqlString job_action;
    if (OB_FAIL(job_action.assign_fmt("DBMS_MVIEW.refresh('%.*s.%.*s')",
                                      static_cast<int>(db_name.length()), db_name.ptr(),
                                      static_cast<int>(table_name.length()), table_name.ptr()))) {
      LOG_WARN("fail to assign job action", KR(ret), K(db_name), K(table_name));
    } else if (OB_FAIL(ObMViewSchedJobUtils::add_scheduler_job(
                   sql_client, tenant_id, job_id, refresh_job, job_action.string(), start_date,
                   repeat_interval, exec_env))) {
      LOG_WARN("failed to add mview scheduler job", KR(ret), K(refresh_job), K(job_action),
               "start with", start_date, "next", repeat_interval);
    } else {
      job_name = refresh_job;
      LOG_INFO("succeed to add refresh mview scheduler job", K(refresh_job), K(job_action),
               "start with", start_date, "next", repeat_interval);
    }
  }

  return ret;
}

int ObMViewSchedJobUtils::create_mlog_scheduler_job(
    common::ObISQLClient &trans,
    const uint64_t tenant_id,
    const uint64_t mview_id,
    const common::ObString &db_name,
    const common::ObString &table_name,
    const common::ObObj &start_date,
    const common::ObString &repeat_interval,
    const common::ObString &exec_env,
    ObArenaAllocator &allocator,
    common::ObString &job_name)
{
  int ret = OB_SUCCESS;
  const ObString mlog_purge_func("DBMS_MVIEW.purge_log");
  ObString job_prefix(ObMLogInfo::MLOG_PURGE_JOB_PREFIX);
  int64_t job_id = OB_INVALID_ID;
  ObString job_action;
  ObString purge_job;
  if (OB_FAIL(generate_job_id(tenant_id, job_id))) {
    LOG_WARN("failed to generate mview job id", KR(ret));
  } else if (OB_FAIL(generate_job_name(allocator, job_id, job_prefix, purge_job))) {
    LOG_WARN("failed to generate mview job name", KR(ret), K(tenant_id), K(job_prefix));
  } else if (OB_FAIL(generate_job_action(allocator, mlog_purge_func, db_name, table_name, job_action))) {
    LOG_WARN("failed to generate mview job action", KR(ret));
  } else if (OB_FAIL(add_scheduler_job(trans, tenant_id, job_id, purge_job, job_action, start_date,
                                       repeat_interval, exec_env))) {
    LOG_WARN("failed to add mview scheduler job", KR(ret), K(purge_job), K(job_action),
             K(start_date), K(repeat_interval));
  } else {
    job_name = purge_job;
  }
  return ret;
}

int ObMViewSchedJobUtils::add_mview_info_and_refresh_job(ObISQLClient &sql_client,
                                                         const uint64_t tenant_id,
                                                         const uint64_t mview_id,
                                                         const ObString &db_name,
                                                         const ObString &table_name,
                                                         const ObMVRefreshInfo *refresh_info,
                                                         const int64_t schema_version,
                                                         ObMViewInfo &mview_info)
{
  int ret = OB_SUCCESS;
  ObString refresh_job;
  ObArenaAllocator allocator("CreateMVTmp");
  SCN curr_ts;
  mview_info.reset();
  share::ObGlobalStatProxy stat_proxy(sql_client, tenant_id);
  share::SCN major_refresh_mv_merge_scn;
  ObArray<share::ObBackupJobAttr> backup_jobs;
  uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (refresh_info == nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("refresh_info is null", KR(ret));
  } else if (ObMVRefreshMode::MAJOR_COMPACTION == refresh_info->refresh_mode_) {
    if (OB_FAIL(acquire_major_refresh_mv_merge_scn_(sql_client, tenant_id))) {
      LOG_WARN("failed to acquire major refresh mv merge scn", KR(ret), K(tenant_id));
    } else if (OB_FAIL(share::ObBackupJobOperator::get_jobs(
                   *GCTX.sql_proxy_, meta_tenant_id, false /*select for update*/, backup_jobs))) {
      LOG_WARN("failed to get backup jobs", KR(ret), K(tenant_id));
    } else if (!backup_jobs.empty()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("[MAJ_REF_MV] backup jobs exist, can not create materialized view", K(ret),
               K(tenant_id));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "backup jobs exist, can not create materialized view, please "
                                      "try again after backup jobs are finished");
    }
  }

  if (OB_SUCC(ret) &&
      (!refresh_info->start_time_.is_null() || !refresh_info->next_time_expr_.empty()) &&
      OB_FAIL(create_mview_scheduler_job(sql_client, tenant_id, mview_id, db_name, table_name,
                                         refresh_info->start_time_, refresh_info->next_time_expr_,
                                         refresh_info->exec_env_, allocator, refresh_job))) {
    LOG_WARN("failed to create scheduler job", KR(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id,
                                      GCONF.rpc_timeout,
                                      curr_ts))) {
      LOG_WARN("fail to get gts sync", K(ret), K(tenant_id));
    } else if (OB_UNLIKELY(!curr_ts.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected curr_scn", KR(ret), K(tenant_id), K(curr_ts));
    }
  }

  if (OB_SUCC(ret)) {
    mview_info.set_tenant_id(tenant_id);
    mview_info.set_mview_id(mview_id);
    mview_info.set_build_mode(ObMViewBuildMode::IMMEDIATE);
    mview_info.set_refresh_mode(refresh_info->refresh_mode_);
    mview_info.set_refresh_method(refresh_info->refresh_method_);
    mview_info.set_refresh_job(refresh_job);
    mview_info.set_refresh_dop(refresh_info->refresh_dop_);
    // TODO: we should set last_refresh_scn to 0 for all kind of mview, and fix the mlog recycle
    // problem later.
    if (ObMVRefreshMode::MAJOR_COMPACTION == refresh_info->refresh_mode_) {
      mview_info.set_last_refresh_scn(0);
    } else {
      mview_info.set_last_refresh_scn(curr_ts.get_val_for_inner_table_field());
    }
    mview_info.set_schema_version(schema_version);
    if (refresh_info->start_time_.is_timestamp()) {
      mview_info.set_refresh_start(refresh_info->start_time_.get_timestamp());
    }
    if (!refresh_info->next_time_expr_.empty() &&
        OB_FAIL(mview_info.set_refresh_next(refresh_info->next_time_expr_))) {
      LOG_WARN("fail to set refresh next", KR(ret));
    } else if (OB_FAIL(ObMViewInfo::insert_mview_info(sql_client, mview_info))) {
      LOG_WARN("fail to insert mview info", KR(ret), K(mview_info));
    }
  }

  return ret;
}

int ObMViewSchedJobUtils::remove_mview_refresh_job(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObMViewInfo mview_info;
  if (OB_FAIL(ObMViewInfo::fetch_mview_info(sql_client,
                                            tenant_id,
                                            table_id,
                                            mview_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("failed to fetch mview info", KR(ret), K(tenant_id), K(table_id));
  } else if (!mview_info.get_refresh_job().empty()
      && OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(
              sql_client, tenant_id, mview_info.get_refresh_job(), true/*if_exists*/))) {
    LOG_WARN("failed to remove dbms sched job",
        KR(ret), K(tenant_id), K(mview_info.get_refresh_job()));
  }
  return ret;
}

int ObMViewSchedJobUtils::remove_mlog_purge_job(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObMLogInfo mlog_info;
  if (OB_FAIL(ObMLogInfo::fetch_mlog_info(sql_client,
                                          tenant_id,
                                          table_id,
                                          mlog_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("failed to fetch mlog info", KR(ret), K(tenant_id), K(table_id));
  } else if (!mlog_info.get_purge_job().empty()
      && OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(
              sql_client, tenant_id, mlog_info.get_purge_job(), true/*if_exists*/))) {
    LOG_WARN("failed to remove dbms sched job",
        KR(ret), K(tenant_id), K(mlog_info.get_purge_job()));
  }
  return ret;
}

int ObMViewSchedJobUtils::calc_date_expr_from_str(
    sql::ObSQLSessionInfo &session,
    ObIAllocator &allocator,
    const uint64_t tenant_id,
    const ObString &interval_str,
    int64_t &timestamp)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSchemaChecker schema_checker;
  CK (OB_NOT_NULL(GCTX.schema_service_));
  OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
  OZ (schema_checker.init(schema_guard));
  SMART_VAR(ObResolverParams, resolver_ctx){
    const ParseNode *expr_node = nullptr;
    ObRawExprFactory expr_factory(allocator);
    resolver_ctx.allocator_  = &allocator;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &session;
    resolver_ctx.database_id_ = session.get_database_id();
    resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
    resolver_ctx.expr_factory_ = &expr_factory;
    if (OB_FAIL(sql::ObRawExprUtils::parse_default_expr_from_str(interval_str,
                                                                session.get_charsets4parser(),
                                                                allocator,
                                                                expr_node))) {
      LOG_WARN("failed to parse default expr from str", KR(ret), K(interval_str));
    } else if (OB_ISNULL(expr_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node is null", KR(ret));
    } else if (OB_FAIL(resolve_date_expr_to_timestamp(resolver_ctx,
                                                      session,
                                                      *expr_node,
                                                      allocator,
                                                      timestamp))) {
      LOG_WARN("failed to resolve data expr to timestamp", KR(ret));
    } else {
      LOG_INFO("mview_sched_job_utils calc_date_expr_from_str end",
          KR(ret), K(tenant_id), K(interval_str), K(timestamp));
    }
  }
  return ret;
}

int ObMViewSchedJobUtils::calc_date_expression(
    ObDBMSSchedJobInfo &job_info,
    int64_t &next_date_ts)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = job_info.get_tenant_id();
  if (!job_info.is_mview_job()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job is not date expression job class",
        KR(ret), K(job_info.is_mview_job()));
  } else {
    ContextParam ctx_param;
    ctx_param.set_mem_attr(common::OB_SERVER_TENANT_ID, "MVSchedTmp");
    CREATE_WITH_TEMP_CONTEXT(ctx_param) {
      ObIAllocator &tmp_allocator = CURRENT_CONTEXT->get_arena_allocator();
      SMART_VAR(ObSQLSessionInfo, session) {
        ObDBMSSchedJobExecutor executor;
        if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
          ret = OB_INVALID_ERROR;
          LOG_WARN("null ptr", K(ret), K(GCTX.sql_proxy_), K(GCTX.schema_service_));
        } else if (OB_FAIL(executor.init(GCTX.sql_proxy_,GCTX.schema_service_))) {
          LOG_WARN("fail to init dbms sched job executor", K(ret));
        } else if (OB_FAIL(session.init(1, 1, &tmp_allocator))) {
          LOG_WARN("failed to init session", KR(ret));
        } else if (OB_FAIL(executor.init_env(job_info, session))) {
          LOG_WARN("failed to init env", KR(ret), K(job_info));
        } else {
          bool is_oracle_mode = lib::is_oracle_mode();
          bool is_oracle_tenant = false;
          is_oracle_tenant = job_info.is_oracle_tenant_;
          if (is_oracle_tenant && !is_oracle_mode) {
            THIS_WORKER.set_compatibility_mode(Worker::CompatMode::ORACLE);
          }

          int64_t current_time = ObTimeUtility::current_time() / 1000000L * 1000000L; // ignore micro seconds
          int64_t next_time = 0;
          if (OB_FAIL(calc_date_expr_from_str(session, tmp_allocator,
              tenant_id, job_info.get_interval(), next_time))) {
            LOG_WARN("failed to calc date expression from str", KR(ret),
                K(tenant_id), K(job_info.get_interval()));
          } else if (next_time <= current_time) {
            ret = OB_ERR_TIME_EARLIER_THAN_SYSDATE;
            LOG_WARN("the parameter next date must evaluate to a time in the future",
                KR(ret), K(current_time), K(next_time));
            LOG_USER_ERROR(OB_ERR_TIME_EARLIER_THAN_SYSDATE, "next date");
          } else {
            next_date_ts = next_time;
          }

          if (is_oracle_tenant && !is_oracle_mode) {
            THIS_WORKER.set_compatibility_mode(Worker::CompatMode::MYSQL);
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewSchedJobUtils::resolve_date_expr_to_timestamp(
    ObResolverParams &params,
    ObSQLSessionInfo &session,
    const ParseNode &node,
    ObIAllocator &allocator,
    int64_t &timestamp)
{
  int ret = OB_SUCCESS;
  ObRawExpr *const_expr = nullptr;
  ParamStore params_array;
  ObObj obj;
  timestamp = 0;
  if (OB_FAIL(ObResolverUtils::resolve_const_expr(
      params, node, const_expr, nullptr))) {
    LOG_WARN("fail to resolve const expr", KR(ret));
  } else if (OB_FAIL(ObSQLUtils::calc_const_expr(
      &session, *const_expr, obj, allocator, params_array))) {
    LOG_WARN("fail to calc const expr", KR(ret));
  } else {
    if (obj.is_timestamp()) {
      timestamp = obj.get_timestamp();
    } else if (obj.is_timestamp_tz()) {
      timestamp = obj.get_otimestamp_value().time_us_;
    } else if (obj.is_datetime()) {
      int64_t tmp_timestamp = 0;
      if (OB_FAIL(ObTimeConverter::datetime_to_timestamp(
          obj.get_datetime(), TZ_INFO(&session), tmp_timestamp))) {
        LOG_WARN("failed to convert datetime to timestamp",
            KR(ret), K(obj.get_datetime()));
      } else {
        timestamp = tmp_timestamp;
      }
    } else if (obj.is_mysql_datetime()) {
      int64_t tmp_timestamp = 0;
      if (OB_FAIL(ObTimeConverter::mdatetime_to_timestamp(
          obj.get_mysql_datetime(), TZ_INFO(&session), tmp_timestamp))) {
        LOG_WARN("failed to convert datetime to timestamp",
            KR(ret), K(obj.get_mysql_datetime()));
      } else {
        timestamp = tmp_timestamp;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid datetime expression", KR(ret), K(obj));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "datetime expression");
    }
  }
  return ret;
}

int ObMViewSchedJobUtils::acquire_major_refresh_mv_merge_scn_(common::ObISQLClient &trans,
                                                              const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  share::SCN major_refresh_mv_merge_scn;
  ObMySQLTransaction tmp_trans;
  common::ObISQLClient *sql_proxy = GCTX.sql_proxy_;

  // firstly, check if major_refresh_mv_merge_scn has been set, if not, set it to 0
  if (OB_UNLIKELY(OB_ISNULL(sql_proxy))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(tmp_trans.start(sql_proxy, tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
  } else {
    share::ObGlobalStatProxy tmp_proxy(tmp_trans, tenant_id);
    if (OB_FAIL(tmp_proxy.get_major_refresh_mv_merge_scn(true /*select for update*/,
                                                         major_refresh_mv_merge_scn))) {
      if (OB_ERR_NULL_VALUE == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("[MAJ_REF_MV] major_refresh_mv_merge_scn has not been set");
        major_refresh_mv_merge_scn.set_min();
        if (OB_FAIL(tmp_proxy.update_major_refresh_mv_merge_scn(major_refresh_mv_merge_scn,
                                                                false /*is incremental*/))) {
          LOG_WARN("fail to update major_refresh_mv_merge_scn", KR(ret),
                   K(major_refresh_mv_merge_scn));
        } else {
          LOG_INFO("[MAJ_REF_MV] init major_refresh_mv_merge_scn", K(tenant_id),
                   K(major_refresh_mv_merge_scn));
        }
      } else {
        LOG_WARN("fail to get major_refresh_mv_merge_scn", KR(ret), K(tenant_id));
      }
    }
  }

  if (tmp_trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = tmp_trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  if (OB_SUCC(ret)) {
    share::ObGlobalStatProxy stat_proxy(trans, tenant_id);
    if (OB_FAIL(stat_proxy.get_major_refresh_mv_merge_scn(true /*select for update*/,
                                                          major_refresh_mv_merge_scn))) {
      LOG_WARN("fail to get major_refresh_mv_merge_scn", KR(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObMViewSchedJobUtils::replace_mview_refresh_job(common::ObISQLClient &sql_client,
                                                    share::schema::ObMViewInfo &mview_info,
                                                    const common::ObString &db_name,
                                                    const common::ObString &table_name,
                                                    const common::ObString &exec_env)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = mview_info.get_tenant_id();
  if (!mview_info.get_refresh_job().empty() &&
      OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(sql_client, tenant_id, mview_info.get_refresh_job(), true /*if_exists*/))) {
    LOG_WARN("failed to remove dbms sched job", KR(ret), K(tenant_id),
             K(mview_info.get_refresh_job()));
  } else {
    common::ObObj start_date;
    ObString refresh_job;
    ObArenaAllocator allocator("CreateMVTmp");
    start_date.set_timestamp(mview_info.get_refresh_start());
    if (OB_FAIL(create_mview_scheduler_job(
            sql_client, tenant_id, mview_info.get_mview_id(), db_name, table_name, start_date,
            mview_info.get_refresh_next(), exec_env, allocator, refresh_job))) {
      LOG_WARN("failed to create scheduler job", KR(ret));
    } else {
      mview_info.set_refresh_job(refresh_job);
    }
  }

  return ret;
}

int ObMViewSchedJobUtils::replace_mlog_purge_job(common::ObISQLClient &sql_client,
                                                 share::schema::ObMLogInfo &mlog_info,
                                                 const common::ObString &db_name,
                                                 const common::ObString &table_name,
                                                 const common::ObString &exec_env)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = mlog_info.get_tenant_id();
  if (!mlog_info.get_purge_job().empty() &&
      OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(
          sql_client, tenant_id, mlog_info.get_purge_job(), true /*if_exists*/))) {
    LOG_WARN("failed to remove dbms sched job", KR(ret), K(tenant_id),
             K(mlog_info.get_purge_job()));
  } else {
    common::ObObj start_date;
    ObString purge_job;
    ObArenaAllocator allocator("CreateMLogTmp");
    start_date.set_timestamp(mlog_info.get_purge_start());
    if (OB_FAIL(create_mlog_scheduler_job(sql_client, tenant_id, mlog_info.get_mlog_id(), db_name,
                                          table_name, start_date, mlog_info.get_purge_next(),
                                          exec_env, allocator, purge_job))) {
      LOG_WARN("failed to create scheduler job", KR(ret));
    } else {
      mlog_info.set_purge_job(purge_job);
    }
  }

  return ret;
}

} // end of storage
} // end of oceanbase
