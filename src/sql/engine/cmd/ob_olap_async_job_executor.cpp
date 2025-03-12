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
#include "sql/engine/cmd/ob_olap_async_job_executor.h"
#include "sql/resolver/cmd/ob_olap_async_job_stmt.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{
int ObOLAPAsyncCancelJobExecutor::execute(ObExecContext &ctx, ObOLAPAsyncCancelJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = stmt.get_tenant_id();
  uint64_t user_id = stmt.get_user_id();
  const ObUserInfo *user_info = nullptr;
  ObArenaAllocator allocator("ASYNC_JOB_TMP");
  dbms_scheduler::ObDBMSSchedJobInfo job_info;
  schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if(OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
    LOG_WARN("fail to get user id", KR(ret), K(tenant_id), K(user_id));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user not exist", KR(ret), K(tenant_id), K(user_id));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::get_dbms_sched_job_info(
        *GCTX.sql_proxy_,
        tenant_id,
        false, // is_oracle_tenant
        stmt.get_job_name(),
        allocator,
        job_info))) {
    LOG_WARN("get job info failed", KR(ret), K(tenant_id), K(stmt.get_job_name()));
  } else if (!job_info.is_olap_async_job()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("cancel not olap async job", KR(ret), K(tenant_id), K(job_info));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::check_dbms_sched_job_priv(user_info, job_info))) {
    LOG_WARN("check user priv failed", KR(ret), K(tenant_id), K(job_info));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::stop_dbms_sched_job(*GCTX.sql_proxy_, job_info, true /* delete after stop */))) {
    if (OB_ENTRY_NOT_EXIST == ret) {//当前job_不在运行不需要报错
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to stop dbms scheduler job", KR(ret));
    }
  }
  return ret;
}

}
}