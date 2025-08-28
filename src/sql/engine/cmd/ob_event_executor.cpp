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
 
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_event_executor.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "sql/resolver/cmd/ob_event_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h"
namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{

int ObCreateEventExecutor::execute(ObExecContext &ctx, ObCreateEventStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else {
    int64_t job_id = OB_INVALID_ID;
    if (OB_INVALID_TENANT_ID == stmt.get_event_info().get_tenant_id()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant id", KR(ret), K(stmt.get_event_info().get_tenant_id()));
    } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::generate_job_id(stmt.get_event_info().get_tenant_id(), job_id))) {
      LOG_WARN("generate_job_id failed", KR(ret), K(stmt.get_event_info().get_tenant_id()));
    } else {
      int64_t start_date_us = stmt.get_event_info().get_start_time() == OB_INVALID_TIMESTAMP ? ObTimeUtility::current_time() : stmt.get_event_info().get_start_time();
      int64_t end_date_us = stmt.get_event_info().get_end_time() == OB_INVALID_TIMESTAMP ? dbms_scheduler::ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE : stmt.get_event_info().get_end_time(); // 4000-01-01
      HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
        job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::MYSQL_EVENT_JOB;
        job_info.tenant_id_ = stmt.get_event_info().get_tenant_id();
        job_info.user_id_ = stmt.get_event_info().get_user_id();
        job_info.database_id_ = stmt.get_event_info().get_database_id();
        job_info.job_ = job_id;
        job_info.job_name_ = stmt.get_event_info().get_event_name();
        job_info.job_action_ = stmt.get_event_info().get_event_body();
        job_info.lowner_ = stmt.get_event_info().get_event_definer();
        job_info.powner_ = stmt.get_event_info().get_event_definer();
        job_info.cowner_ = stmt.get_event_info().get_event_database();
        job_info.job_style_ = ObString("REGULAR");
        job_info.job_type_ = ObString("PLSQL_BLOCK");
        job_info.job_class_ = ObString("MYSQL_EVENT_JOB_CLASS");
        job_info.what_ = stmt.get_event_info().get_event_body();
        job_info.start_date_ = start_date_us;
        job_info.end_date_ = end_date_us;
        job_info.interval_ = job_info.repeat_interval_;
        job_info.repeat_interval_ = stmt.get_event_info().get_repeat_interval();
        job_info.enabled_ = stmt.get_event_info().get_is_enable() == ObEventInfo::ObEventBoolType::SET_FALSE ? false : true; //Not_Set = true
        job_info.auto_drop_ = stmt.get_event_info().get_auto_drop()  == ObEventInfo::ObEventBoolType::SET_FALSE ? false : true; //Not_Set = true
        job_info.max_run_duration_ = stmt.get_event_info().get_max_run_duration() > 0 ? stmt.get_event_info().get_max_run_duration() : 24 * 60 * 60;
        job_info.exec_env_ = stmt.get_event_info().get_exec_env();
        job_info.comments_ = stmt.get_event_info().get_event_comment();
        ObMySQLTransaction trans;
        if (OB_FAIL(trans.start(GCTX.sql_proxy_, stmt.get_event_info().get_tenant_id()))) {
          LOG_WARN("failed to start trans", KR(ret), K(stmt.get_event_info().get_tenant_id()));
        } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(
            trans, stmt.get_event_info().get_tenant_id(), job_id, job_info))) {
          LOG_WARN("failed to create dbms scheduler job", KR(ret), K(stmt.get_event_info().get_if_exist_or_if_not_exist()));
        }
        if (trans.is_started()) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
            LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
            ret = OB_SUCC(ret) ? tmp_ret : ret;
          }
        }
        if (ret == OB_ERR_PRIMARY_KEY_DUPLICATE) {
          if (stmt.get_event_info().get_if_exist_or_if_not_exist()) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_EVENT_EXIST;
          }
        }
      }
    }
  }
  return ret;
}


int ObAlterEventExecutor::execute(ObExecContext &ctx, ObAlterEventStmt &stmt)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = stmt.get_event_info().get_tenant_id();
  ObArenaAllocator allocator;
  dbms_scheduler::ObDBMSSchedJobInfo job_info;
  ObMySQLTransaction trans;
  if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(stmt.get_event_info().get_tenant_id()));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(*GCTX.sql_proxy_, 
                                                                              tenant_id, 
                                                                              false, // is_oracle_tenant
                                                                              stmt.get_event_info().get_event_name(),
                                                                              allocator,
                                                                              job_info))) {
    LOG_WARN("get job failed", K(ret));
    if (ret == OB_ENTRY_NOT_EXIST) {
      ret = OB_ERR_EVENT_NOT_EXIST;
    }
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_SUCC(ret) && !stmt.get_event_info().get_event_definer().empty()) {
      ObObj powner_obj, user_id_obj;
      powner_obj.set_char(stmt.get_event_info().get_event_definer());
      user_id_obj.set_int(stmt.get_event_info().get_user_id());
      OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("powner"), powner_obj));
      OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("user_id"), user_id_obj));
    }

    if (OB_SUCC(ret) && ObEventInfo::ObEventBoolType::NOT_SET != stmt.get_event_info().get_auto_drop()) {
      ObObj obj;
      if (ObEventInfo::ObEventBoolType::SET_TRUE == stmt.get_event_info().get_auto_drop()) {
        obj.set_bool(true);
      } else {
        obj.set_bool(false);
      }
      OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("auto_drop"), obj));
    } 
  
    if (OB_SUCC(ret) && ObEventInfo::ObEventBoolType::NOT_SET != stmt.get_event_info().get_is_enable()) {
      ObObj obj;
      if (ObEventInfo::ObEventBoolType::SET_TRUE == stmt.get_event_info().get_is_enable()) {
        obj.set_bool(true);
      } else {
        obj.set_bool(false);
      }
      OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("enabled"), obj));
    } 


    if (OB_SUCC(ret) && !stmt.get_event_info().get_event_comment().empty()) {
      ObObj obj;
      obj.set_char(stmt.get_event_info().get_event_comment());
      OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("comments"), obj));
    } 


    if (OB_SUCC(ret) && !stmt.get_event_info().get_event_body().empty()) {
      ObObj obj;
      obj.set_char(stmt.get_event_info().get_event_body());
      OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("job_action"), obj));
    } 

    if (OB_SUCC(ret) && OB_INVALID_TIMESTAMP != stmt.get_event_info().get_start_time()) {
      int64_t start_date_us = stmt.get_event_info().get_start_time();
      int64_t end_date_us = stmt.get_event_info().get_end_time() == OB_INVALID_TIMESTAMP ? dbms_scheduler::ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE : stmt.get_event_info().get_end_time();
      if (start_date_us > end_date_us) {
        ret = OB_ERR_EVENT_ENDS_BEFORE_STARTS;
        LOG_WARN("end time invalid", K(ret), K(start_date_us), K(end_date_us));
      } else {
        ObObj start_date_obj, end_date_obj, repeat_interval_obj, max_run_duration_obj;
        start_date_obj.set_time(start_date_us);
        end_date_obj.set_time(end_date_us);
        repeat_interval_obj.set_char(stmt.get_event_info().get_repeat_interval());
        max_run_duration_obj.set_int(stmt.get_event_info().get_max_run_duration() > 0 ? stmt.get_event_info().get_max_run_duration() : 24 * 60 * 60);
        OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("start_date"), start_date_obj));
        OX (job_info.start_date_ = start_date_us); //更新本地job_info防止计算错误
        OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("end_date"), end_date_obj));
        OX (job_info.end_date_ = end_date_us); //更新本地job_info防止计算错误
        OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("repeat_interval"), repeat_interval_obj));
        OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("max_run_duration"), max_run_duration_obj));
      }
    }

    //先更新 job_name 会导致后续的 update 执行错误，job name 的更新要放在最后。
    if (OB_SUCC(ret) && !stmt.get_event_info().get_event_rename().empty()) {
      if (job_info.get_start_date() > ObTimeUtility::current_time()) { //job开始运行了就不能改
        ObObj obj;
        obj.set_char(stmt.get_event_info().get_event_rename());
        OZ (dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job_info(trans, job_info, ObString("job_name"), obj));
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_ERR_EVENT_EXIST;
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "modify EVENT NAME that has already started");
      }
    } 

    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}


int ObDropEventExecutor::execute(ObExecContext &ctx, ObDropEventStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else {
    uint64_t tenant_id = stmt.get_event_info().get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::remove_dbms_sched_job(
        *GCTX.sql_proxy_,
        stmt.get_event_info().get_tenant_id(), 
        stmt.get_event_info().get_event_name(), 
        stmt.get_event_info().get_if_exist_or_if_not_exist())
    )) {
      if (ret == OB_INVALID_ARGUMENT) {
        ret = OB_ERR_EVENT_NOT_EXIST;
      }
      LOG_WARN("failed to remove dbms scheduler job", KR(ret));
    }
  }
  return ret;
}

}
}