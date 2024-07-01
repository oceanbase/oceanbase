#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_event_executor.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "sql/resolver/cmd/ob_create_event_stmt.h"
#include "sql/resolver/cmd/ob_drop_event_stmt.h"
#include "sql/resolver/cmd/ob_alter_event_stmt.h"
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
    ObString first_stmt;
    if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
      LOG_WARN("fail to get first stmt" , K(ret));
    } else {
      int64_t job_id = OB_INVALID_ID;
      if (OB_INVALID_TENANT_ID == stmt.get_tenant_id()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tenant id", KR(ret), K(stmt.get_tenant_id()));
      } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::generate_job_id(stmt.get_tenant_id(), job_id))) {
        LOG_WARN("generate_job_id failed", KR(ret), K(stmt.get_tenant_id()));
      } else {
        int64_t start_date_us = stmt.get_start_time() == OB_INVALID_TIMESTAMP ? ObTimeUtility::current_time() : stmt.get_start_time();
        int64_t end_date_us = stmt.get_end_time() == OB_INVALID_TIMESTAMP ? 64060560000000000 : stmt.get_end_time(); // 4000-01-01
        HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
          job_info.tenant_id_ = stmt.get_tenant_id();
          job_info.user_id_ = stmt.get_user_id();
          job_info.database_id_ = stmt.get_database_id();
          job_info.job_ = job_id;
          job_info.job_name_ = stmt.get_event_name();
          job_info.job_action_ = stmt.get_event_body();
          job_info.lowner_ = stmt.get_event_definer();
          job_info.powner_ = stmt.get_event_definer();
          job_info.cowner_ = stmt.get_event_database();
          job_info.job_style_ = ObString("regular");
          job_info.job_type_ = ObString("PLSQL_BLOCK");
          job_info.job_class_ = ObString("MYSQL_EVENT_JOB_CLASS");
          job_info.what_ = stmt.get_event_body();
          job_info.start_date_ = start_date_us;
          job_info.end_date_ = end_date_us;
          job_info.interval_ = job_info.repeat_interval_;
          job_info.repeat_interval_ = stmt.get_repeat_interval();
          job_info.enabled_ = stmt.get_is_enable();
          job_info.auto_drop_ = stmt.get_auto_drop();
          job_info.max_run_duration_ = stmt.get_repeat_ts() > 0 ? stmt.get_repeat_ts() / 1000000 : 24 * 60 * 60;
          job_info.interval_ts_ = stmt.get_repeat_ts();
          job_info.exec_env_ = stmt.get_exec_env();
          job_info.comments_ = stmt.get_event_comment();
          ObMySQLTransaction trans;
          if (OB_FAIL(trans.start(GCTX.sql_proxy_, stmt.get_tenant_id()))) {
            LOG_WARN("failed to start trans", KR(ret), K(stmt.get_tenant_id()));
          } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(
              trans, stmt.get_tenant_id(), job_id, job_info))) {
            LOG_WARN("failed to create dbms scheduler job", KR(ret), K(stmt.get_if_not_exists()));
          }
          if (trans.is_started()) {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
              LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
              ret = OB_SUCC(ret) ? tmp_ret : ret;
            }
          }
          if (ret == OB_ERR_PRIMARY_KEY_DUPLICATE) {
            if (stmt.get_if_not_exists()) {
              ret = OB_SUCCESS;
            } else {
              ret = OB_ERR_EVENT_EXIST;
            }
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
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else {
    uint64_t tenant_id = stmt.get_tenant_id();
    bool exist = false;
    bool start_time_on_past = false;
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, stmt.get_tenant_id()))) {
      LOG_WARN("failed to start trans", KR(ret), K(stmt.get_tenant_id()));
    } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::check_dbms_sched_job_exist_and_start_time_on_past(
      trans,
      stmt.get_tenant_id(),
      stmt.get_event_name(),
      exist,
      start_time_on_past
    ))) {
      LOG_WARN("check job exist failed", K(stmt.get_event_name()));
    } else if (exist) {
      if (stmt.get_start_time() != OB_INVALID_TIMESTAMP && start_time_on_past) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support change started event", K(ret), K(stmt.get_start_time()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "modify the scheduling time of a event that has already started");
      } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::update_dbms_sched_job(
          trans,
          stmt.get_tenant_id(),
          stmt.get_event_name(),
          stmt.get_auto_drop(),
          stmt.get_is_enable(),
          stmt.get_start_time(),
          stmt.get_end_time(),
          stmt.get_repeat_ts(),
          stmt.get_repeat_interval(),
          stmt.get_event_definer(),
          stmt.get_event_rename(),
          stmt.get_event_comment(),
          stmt.get_event_body()))) {
        LOG_WARN("failed to update dbms scheduler job", KR(ret));
        if (ret == OB_ERR_PRIMARY_KEY_DUPLICATE) {
            ret = OB_ERR_EVENT_EXIST;
        }
      }
    } else {
      ret = OB_ERR_EVENT_NOT_EXIST;
      LOG_WARN("alter not exist event", KR(ret));
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
    uint64_t tenant_id = stmt.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::remove_dbms_sched_job(
        *GCTX.sql_proxy_,
        stmt.get_tenant_id(),
        stmt.get_event_name(),
        stmt.get_if_exists())
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