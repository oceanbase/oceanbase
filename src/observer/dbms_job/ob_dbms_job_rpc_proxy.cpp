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

#include "ob_dbms_job_rpc_proxy.h"

#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server.h"

#include "ob_dbms_job_rpc_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::observer;

namespace oceanbase
{
namespace obrpc
{

OB_SERIALIZE_MEMBER(ObDBMSJobArg, tenant_id_, job_id_, server_addr_, master_addr_);
OB_SERIALIZE_MEMBER(ObDBMSJobResult, tenant_id_, job_id_, server_addr_, status_code_);

int ObDBMSJobRpcProxy::run_dbms_job(
  uint64_t tenant_id, uint64_t job_id, ObAddr server_addr, ObAddr master_addr)
{
  int ret = OB_SUCCESS;
  ObDBMSJobArg arg(tenant_id, job_id, server_addr, master_addr);
  ObRpcAPDBMSJobCB cb;
  CK (arg.is_valid());
  OZ (this->to(arg.server_addr_).by(arg.tenant_id_).run_dbms_job(arg, &cb), arg);
  return ret;
}

}/* ns obrpc*/
}/* ns oceanbase */
