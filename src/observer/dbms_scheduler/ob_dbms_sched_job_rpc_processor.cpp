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

#include "lib/oblog/ob_log_module.h"

#include "ob_dbms_sched_job_master.h"
#include "ob_dbms_sched_job_executor.h"
#include "ob_dbms_sched_job_rpc_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::dbms_scheduler;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace obrpc
{

int ObRpcAPDBMSSchedJobCB::process()
{
  int ret = OB_SUCCESS;
  ObDBMSSchedJobResult &result = result_;
  ObDBMSSchedJobMaster &job_master = ObDBMSSchedJobMaster::get_instance();
  if (!job_master.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms sched job master not init", K(ret), K(job_master.is_inited()));
  } else if (!result.is_valid()) {
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

}
} // namespace oceanbase
