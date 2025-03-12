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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_olap_async_job_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace dbms_scheduler;
namespace sql
{
ObOLAPAsyncJobResolver::ObOLAPAsyncJobResolver(ObResolverParams &params)
    : ObSelectResolver(params)
{
}

int ObOLAPAsyncJobResolver::resolve(const ParseNode &parse_tree)
{

  int ret = OB_SUCCESS;
  ObItemType stmt_type = parse_tree.type_;
  switch (stmt_type) {
  case T_OLAP_ASYNC_JOB_SUBMIT: {
    ObOLAPAsyncSubmitJobStmt job_stmt;
    OZ (resolve_submit_job_stmt(parse_tree, job_stmt));
    OZ (execute_submit_job(job_stmt));
    OZ (init_select_stmt(job_stmt));
    break;
  }
  case T_OLAP_ASYNC_JOB_CANCEL: {
    ObOLAPAsyncCancelJobStmt *stmt = create_stmt<ObOLAPAsyncCancelJobStmt>();
    OV (OB_NOT_NULL(stmt), OB_ALLOCATE_MEMORY_FAILED);
    OZ (resolve_cancel_job_stmt(parse_tree, stmt));
    break;
  }
  default:
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stmt type", K(ret), K(stmt_type));
  }
  return ret;
}

int ObOLAPAsyncJobResolver::resolve_submit_job_stmt(const ParseNode &parse_tree, ObOLAPAsyncSubmitJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  int64_t session_query_time_out_ts = 0;

  const ParseNode* sql_stmt_node =  parse_tree.children_[0];
  /* 解析的结构
  parse_tree->T_OLAP_ASYNC_JOB_SUBMIT
  |--[0] T_SQL_STMT
    |--[0] [T_SELECT/T_INSERT/T_CREATE_TABLE] user_sql
  */
  if (parse_tree.num_child_ != 1 || OB_ISNULL(sql_stmt_node) || sql_stmt_node->type_ != T_SQL_STMT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job name", K(ret));
  } else if (sql_stmt_node->num_child_ != 1 || OB_ISNULL(sql_stmt_node->children_[0]) || (
     sql_stmt_node->children_[0]->type_ != T_INSERT &&
     sql_stmt_node->children_[0]->type_ != T_LOAD_DATA &&
     sql_stmt_node->children_[0]->type_ != T_CREATE_TABLE)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sql type");
  } else if (OB_JOB_SQL_MAX_LENGTH - 1 < parse_tree.str_len_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sql too long", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sql length");
  } else if (OB_INVALID_ID == session_info_->get_database_id()) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("not select database", KR(ret));
  } else if (OB_FAIL(session_info_->get_query_timeout(session_query_time_out_ts))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session query timeout failed", KR(ret));
  } else {
    const int definer_buf_size = OB_MAX_USER_NAME_LENGTH + OB_MAX_HOST_NAME_LENGTH + 2; // @ + \0
    char *definer_buf = static_cast<char*>(allocator_->alloc(definer_buf_size));
    if (OB_ISNULL(definer_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      memset(definer_buf, 0, definer_buf_size);
      snprintf(definer_buf, definer_buf_size, "%.*s@%.*s", session_info_->get_user_name().length(), session_info_->get_user_name().ptr(),
                                                           session_info_->get_host_name().length(), session_info_->get_host_name().ptr());
      stmt.set_job_definer(definer_buf);
    }

    if (OB_SUCC(ret)) {
      const uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
      stmt.set_tenant_id(tenant_id);
      stmt.set_user_id(session_info_->get_user_id());
      stmt.set_job_database(session_info_->get_database_name());
      stmt.set_database_id(session_info_->get_database_id());
      stmt.set_query_time_out_second(session_query_time_out_ts / 1000 / 1000);
    }

    if (OB_SUCC(ret)) {
      char *sql_buf = static_cast<char*>(allocator_->alloc(OB_JOB_SQL_MAX_LENGTH));
      if (OB_ISNULL(sql_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        memset(sql_buf, 0, OB_JOB_SQL_MAX_LENGTH);
        snprintf(sql_buf, OB_JOB_SQL_MAX_LENGTH, "%.*s;", (int)parse_tree.str_len_, parse_tree.str_value_);
        stmt.set_job_action(sql_buf);
      }
    }

    if (OB_SUCC(ret)) {
      char *job_exec_buf = static_cast<char*>(allocator_->alloc(OB_MAX_PROC_ENV_LENGTH));
      if (OB_ISNULL(job_exec_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        memset(job_exec_buf, 0, OB_MAX_PROC_ENV_LENGTH);
        int64_t pos = 0;
        if (OB_FAIL(ObExecEnv::gen_exec_env(*session_info_, job_exec_buf, OB_MAX_PROC_ENV_LENGTH, pos))){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("generate exec env failed", K(ret), K(session_info_));
        } else {
          stmt.set_exec_env((ObString(pos, job_exec_buf)));
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t job_id = OB_INVALID_ID;
      if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::generate_job_id(stmt.get_tenant_id(), job_id))) {
          LOG_WARN("generate_job_id failed", KR(ret), K(stmt.get_tenant_id()));
      } else {
        stmt.set_job_id(job_id);
        char *job_name_buf = static_cast<char*>(allocator_->alloc(OB_JOB_NAME_MAX_LENGTH));
        if (OB_ISNULL(job_name_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          memset(job_name_buf, 0, OB_JOB_NAME_MAX_LENGTH);
          snprintf(job_name_buf, OB_JOB_NAME_MAX_LENGTH, "%"PRIu64"%"PRIu64,stmt.get_database_id(), stmt.get_job_id());
          stmt.set_job_name(job_name_buf);
        }
      }
    }
  }
  return ret;
}
int ObOLAPAsyncJobResolver::resolve_cancel_job_stmt(const ParseNode &parse_tree, ObOLAPAsyncCancelJobStmt *stmt)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
  stmt->set_tenant_id(tenant_id);
  stmt->set_user_id(session_info_->get_user_id());
  if (parse_tree.num_child_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job name", K(ret));
  } else {
    const ParseNode *job_name_node = parse_tree.children_[0];
    if (OB_ISNULL(job_name_node)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job name", K(ret));
    } else {
      char *job_name_buf = static_cast<char*>(allocator_->alloc(OB_JOB_NAME_MAX_LENGTH));
      if (OB_ISNULL(job_name_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        memset(job_name_buf, 0, OB_JOB_NAME_MAX_LENGTH);
        snprintf(job_name_buf, OB_JOB_NAME_MAX_LENGTH, "%.*s", (int)job_name_node->str_len_, job_name_node->str_value_);
        stmt->set_job_name(job_name_buf);
      }
    }
  }
  return ret;
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(ERRSIM_SUBMIT_ERR_JOB_NAME);
ERRSIM_POINT_DEF(ERRSIM_SUBMIT_ERR_JOB_START_TIME);
#endif

int ObOLAPAsyncJobResolver::execute_submit_job(ObOLAPAsyncSubmitJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == stmt.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(stmt.get_tenant_id()));
  } else {
    int64_t start_date_us = ObTimeUtility::current_time();
    int64_t end_date_us = 64060560000000000; // 4000-01-01
    HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = stmt.get_tenant_id();
      job_info.user_id_ = stmt.get_user_id();
      job_info.database_id_ = stmt.get_database_id();
      job_info.job_ = stmt.get_job_id();
      job_info.job_name_ = stmt.get_job_name();
      job_info.job_action_ = stmt.get_job_action();
      job_info.lowner_ = stmt.get_job_definer();
      job_info.powner_ = stmt.get_job_definer();
      job_info.cowner_ = stmt.get_job_database();
      job_info.job_style_ = ObString("regular");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("OLAP_ASYNC_JOB_CLASS"); // for compat old version
      job_info.start_date_ = start_date_us;
      job_info.end_date_ = end_date_us;
      job_info.repeat_interval_ = job_info.repeat_interval_;
      job_info.enabled_ = true;
      job_info.auto_drop_ = true;
      job_info.max_run_duration_ = stmt.get_query_time_out_second() + 60;
      job_info.interval_ts_ = 0;
      job_info.exec_env_ = stmt.get_exec_env();
      job_info.comments_ = ObString("olap async job");
      job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::OLAP_ASYNC_JOB;

      #ifdef ERRSIM
      if (OB_SUCCESS != ERRSIM_SUBMIT_ERR_JOB_NAME) { //注入一个错误的JOB NAME
        job_info.job_name_ = "ERRSIM_JOB";
      }
      if (OB_SUCCESS != ERRSIM_SUBMIT_ERR_JOB_START_TIME) { //注入一个错误的开始时间
        job_info.start_date_ = 64060560000000000;
      }
      #endif

      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(GCTX.sql_proxy_, stmt.get_tenant_id()))) {
        LOG_WARN("failed to start trans", KR(ret), K(stmt.get_tenant_id()));
      } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(
          trans, stmt.get_tenant_id(), stmt.get_job_id(), job_info))) {
        LOG_WARN("failed to create dbms scheduler job", KR(ret));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
      if (ret == OB_ERR_PRIMARY_KEY_DUPLICATE) {
        LOG_WARN("job is exist", KR(ret));
      }
    }
  }
  return ret;
}

int ObOLAPAsyncJobResolver::init_select_stmt(ObOLAPAsyncSubmitJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = create_stmt<ObSelectStmt>();
  OV (OB_NOT_NULL(select_stmt), OB_ALLOCATE_MEMORY_FAILED);
  if (OB_LIKELY(OB_SUCC(ret))) {
    ObSqlString select_sql;
    if (OB_FAIL(select_sql.assign_fmt(
        "SELECT '%.*s' as job_id",stmt.get_job_name().length(), stmt.get_job_name().ptr()))) {
      LOG_WARN("assign sql string failed", KR(ret), K(stmt.get_job_name()));
    } else if (OB_FAIL(parse_and_resolve_select_sql(select_sql.string()))) {
      LOG_WARN("fail to parse and resolve select sql", K(ret), K(select_sql));
    } else if (OB_UNLIKELY(stmt::T_SELECT != stmt_->get_stmt_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt type", K(stmt_->get_stmt_type()));
    } else {
      select_stmt->set_select_type(NOT_AFFECT_FOUND_ROWS);
      if(OB_FAIL(select_stmt->formalize_stmt(session_info_))) {
        LOG_WARN("pull select stmt all expr relation ids failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOLAPAsyncJobResolver::parse_and_resolve_select_sql(const ObString &select_sql)
{
  int ret = OB_SUCCESS;
  // 1. parse and resolve view defination
  if (OB_ISNULL(session_info_) || OB_ISNULL(params_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data member is not init", K(ret), K(session_info_), K(params_.allocator_));
  } else {
    ParseResult select_result;
    ObParser parser(*params_.allocator_, session_info_->get_sql_mode());
    if (OB_FAIL(parser.parse(select_sql, select_result))) {
      LOG_WARN("parse select sql failed", K(select_sql), K(ret));
    } else {
      // use alias to make all columns number continued
      if (OB_ISNULL(select_result.result_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is NULL", K(ret));
      } else if (OB_UNLIKELY(select_result.result_tree_->num_child_ != 1
                             || NULL == select_result.result_tree_->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is invalid",
                 K(ret), K(select_result.result_tree_->num_child_), K(select_result.result_tree_->children_));
      } else if (OB_UNLIKELY(NULL == select_result.result_tree_->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is invalid", K(ret), "child ptr", select_result.result_tree_->children_[0]);
      } else {
        ParseNode *select_stmt_node = select_result.result_tree_->children_[0];
        if (OB_FAIL(ObSelectResolver::resolve(*select_stmt_node))) {
          LOG_WARN("resolve select in view definition failed", K(ret), K(select_stmt_node));
        }
      }
    }
  }
  return ret;
}



} //namespace sql
} //namespace oceanbase