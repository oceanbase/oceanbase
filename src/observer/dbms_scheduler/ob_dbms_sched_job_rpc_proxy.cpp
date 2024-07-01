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

#include "ob_dbms_sched_job_rpc_proxy.h"

#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server.h"

#include "ob_dbms_sched_job_rpc_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::observer;

namespace oceanbase
{
namespace obrpc
{

OB_SERIALIZE_MEMBER(ObDBMSSchedJobArg, tenant_id_, job_id_, server_addr_, master_addr_, is_oracle_tenant_, job_name_);
OB_SERIALIZE_MEMBER(ObDBMSSchedJobResult, tenant_id_, job_id_, server_addr_, status_code_);
OB_SERIALIZE_MEMBER(ObDBMSSchedStopJobArg, tenant_id_, job_name_, session_id_, rpc_send_time_);

int ObDBMSSchedJobRpcProxy::run_dbms_sched_job(
  uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, ObString &job_name, ObAddr server_addr, ObAddr master_addr)
{
  int ret = OB_SUCCESS;
  ObDBMSSchedJobArg arg(tenant_id, job_id, server_addr, master_addr, is_oracle_tenant, job_name);
  ObRpcAPDBMSSchedJobCB cb;
  CK (arg.is_valid());
  OZ (this->to(arg.server_addr_).by(arg.tenant_id_).run_dbms_sched_job(arg, &cb), arg);
  return ret;
}

int ObDBMSSchedJobRpcProxy::stop_dbms_sched_job(
  uint64_t tenant_id, ObString &job_name, ObAddr server_addr, uint64_t session_id)
{
  int ret = OB_SUCCESS;
  ObDBMSSchedStopJobArg arg(tenant_id, job_name, session_id, ObTimeUtility::current_time());
  CK (arg.is_valid());
  OZ (this->to(server_addr).by(arg.tenant_id_).stop_dbms_sched_job(arg));
  return ret;
}

}/* ns obrpc*/
}/* ns oceanbase */
