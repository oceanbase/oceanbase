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

#include "ob_dbms_sched_job_rpc_processor.h"


#include "ob_dbms_sched_job_master.h"
#include "ob_dbms_sched_job_executor.h"

using namespace oceanbase::common;
using namespace oceanbase::dbms_scheduler;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace obrpc
{

int ObRpcAPDBMSSchedJobCB::process()
{
  int ret = OB_SUCCESS;
  ObDBMSSchedJobResult &result = result_;
  if (!result.is_valid()) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("dbms sched job result is invalid", K(ret), K(result));
  } else {
    LOG_INFO("dbms sched job run done!");
  }
  return ret;
}

int ObRpcRunDBMSSchedJobP::process()
{
  int ret = OB_SUCCESS;
  const ObDBMSSchedJobArg &arg = arg_;
  ObDBMSSchedJobResult &result = result_;
  ObDBMSSchedJobExecutor executor;
  result.set_tenant_id(arg.tenant_id_);
  result.set_job_id(arg.job_id_);
  result.set_server_addr(arg.server_addr_);

  LOG_INFO("dbms sched job run rpc process start", K(ret));

  if (!arg.is_valid()) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("fail to get dbms sched job arg", K(ret), K(arg));
  } else if (OB_ISNULL(gctx_.sql_proxy_) || OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("null ptr",
             K(ret), K(gctx_.sql_proxy_), K(gctx_.schema_service_));
  } else if (OB_FAIL(executor.init(gctx_.sql_proxy_, gctx_.schema_service_))) {
    LOG_WARN("fail to init dbms sched job executor", K(ret));
  } else if (OB_FAIL(executor.run_dbms_sched_job(arg.tenant_id_, arg.is_oracle_tenant_, arg.job_id_, arg.job_name_))) {
    LOG_WARN("fail to executor dbms sched job", K(ret), K(arg));
  }
  LOG_INFO("dbms sched job run rpc process end", K(ret), K(arg_));
  result.set_status_code(ret);

  return ret;
}

int ObRpcStopDBMSSchedJobP::process()
{
  int ret = OB_SUCCESS;
  const ObDBMSSchedStopJobArg &arg = arg_;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_mgr_ is null", K(ret));
  } else {
    sql::ObSessionGetterGuard guard(*GCTX.session_mgr_, arg.session_id_);
    if (OB_FAIL(guard.get_session(session))) {
      LOG_WARN("failed to get session", K(arg));
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else {
      {
        ObSQLSessionInfo::LockGuard lock_guard(session->get_thread_data_lock());
        ObDBMSSchedJobInfo *job_info = session->get_job_info();
        if (OB_ISNULL(job_info)) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("job_info is null, maybe job end", K(ret), K(arg));
        } else if (0 != job_info->get_job_name().case_compare(arg.job_name_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("job_info is not expected", K(ret), KPC(job_info), K(arg));
        }
      }
      if (OB_SUCC(ret)) {
        if (arg_.rpc_send_time_ <= session->get_sess_create_time()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session maybe reused by later round", K(ret), K(arg), K(session->get_sess_create_time()), KPC(session));
        } else if (OB_FAIL(GCTX.session_mgr_->kill_session(*session))) {
          LOG_WARN("failed to kill session", K(ret), K(arg), KPC(session));
        } else {
          LOG_INFO("stop job finish", K(arg));
        }
      }
    }
  }
  return ret;
}

int ObRpcDBMSSchedPurgeCB::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("dbms sched purge done!");
  return ret;
}

int ObRpcDBMSSchedPurgeP::process()
{
  int ret = OB_SUCCESS;
  const ObDBMSSchedPurgeArg &arg = arg_;
  int64_t tenant_id = arg.tenant_id_;
  const int64_t PURGE_RUN_DETAIL_TIMEOUT = 5 * 60 * 1000 * 1000L; // 5min
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret), K(tenant_id));
  } else {
    dbms_scheduler::ObDBMSSchedTableOperator table_operator;
    if (OB_FAIL(table_operator.init(GCTX.sql_proxy_))) {
      LOG_WARN("failed to init table_operator", K(ret), K(tenant_id));
    } else {
      bool is_tenant_standby = false;
      if (OB_FAIL(ObAllTenantInfoProxy::is_standby_tenant(GCTX.sql_proxy_, tenant_id, is_tenant_standby))) {
        LOG_WARN("check is standby tenant failed", K(ret), K(tenant_id));
      } else if (is_tenant_standby) {
        LOG_INFO("tenant is standby, not GC", K(tenant_id));
      } else {
        const int64_t save_timeout_ts = THIS_WORKER.get_timeout_ts();
        THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + PURGE_RUN_DETAIL_TIMEOUT);
        OZ (table_operator.purge_run_detail(tenant_id));
        THIS_WORKER.set_timeout_ts(save_timeout_ts);
      }
    }
    LOG_INFO("[DBMS_SCHED_GC] finish once", K(ret), K(tenant_id_));
  }
  return ret;
}

}
} // namespace oceanbase
