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

#include "ob_dbms_job_rpc_processor.h"

#include "lib/oblog/ob_log_module.h"

#include "ob_dbms_job_master.h"
#include "ob_dbms_job_executor.h"
#include "ob_dbms_job_rpc_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::dbms_job;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace obrpc
{

int ObRpcAPDBMSJobCB::process()
{
  int ret = OB_SUCCESS;
  ObDBMSJobResult &result = result_;
  ObDBMSJobMaster &job_master = ObDBMSJobMaster::get_instance();
  if (!job_master.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("dbms job master not init", K(ret), K(job_master.is_inited()));
  } else if (!result.is_valid()) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("dbms job result is invalid", K(ret), K(result));
  } else {
    LOG_INFO("dbms job run done!");
  }
  return ret;
}

int ObRpcRunDBMSJobP::process()
{
  int ret = OB_SUCCESS;
  const ObDBMSJobArg &arg = arg_;
  ObDBMSJobResult &result = result_;
  ObDBMSJobExecutor executor;
  result.set_tenant_id(arg.tenant_id_);
  result.set_job_id(arg.job_id_);
  result.set_server_addr(arg.server_addr_);

  LOG_INFO("dbms job run rpc process start", K(ret));

  if (!arg.is_valid()) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("fail to get dbms job arg", K(ret), K(arg));
  } else if (OB_ISNULL(gctx_.sql_proxy_) || OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("null ptr",
             K(ret), K(gctx_.sql_proxy_), K(gctx_.schema_service_));
  } else if (OB_FAIL(executor.init(gctx_.sql_proxy_, gctx_.schema_service_))) {
    LOG_WARN("fail to init dbms job executor", K(ret));
  } else if (OB_FAIL(executor.run_dbms_job(arg.tenant_id_, arg.job_id_))) {
    LOG_WARN("fail to executor dbms job", K(ret), K(arg));
  }
  LOG_INFO("dbms job run rpc process end", K(ret), K(arg_));
  result.set_status_code(ret);

  return ret;
}

}
} // namespace oceanbase
