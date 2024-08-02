#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_olap_async_job_executor.h"
#include <chrono>
#include <ctime>
#include <iomanip>
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "sql/resolver/cmd/ob_olap_async_job_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h"
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
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const char ASYNC_JOB_CLASS[] = "ADB_ASYNC_JOB_CLASS";
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::stop_dbms_sched_job(
      stmt.get_user_name(),
      stmt.get_tenant_id(),
      stmt.get_job_name(),
      ASYNC_JOB_CLASS)
  )) {
    LOG_WARN("failed to stop dbms scheduler job", KR(tmp_ret));
    if (tmp_ret == OB_NOT_SUPPORTED) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("support cancel job", KR(ret));
    } else if (tmp_ret == OB_ERR_NO_PRIVILEGE){
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE,
          "insufficient privalige to cancel other user job");
    } else if (tmp_ret == OB_ENTRY_NOT_EXIST) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("olap not running job", KR(ret));
    }
  }
  //stop 失败不影响 remove
  if (OB_SUCC(ret) && OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::remove_dbms_sched_job(
      *GCTX.sql_proxy_,
      stmt.get_tenant_id(),
      stmt.get_job_name(),
      true,
      ASYNC_JOB_CLASS)
  )) {
    LOG_WARN("failed to remove dbms scheduler job", KR(ret));
  }
  return ret;
}

}
}