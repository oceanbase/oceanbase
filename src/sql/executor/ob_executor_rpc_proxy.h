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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROXY_
#define OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROXY_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "sql/executor/ob_task.h"
#include "sql/executor/ob_task_event.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace sql
{

struct ObEraseDtlIntermResultArg
{
  OB_UNIS_VERSION(1);
public:
  ObEraseDtlIntermResultArg() {}

  ObSEArray<uint64_t, 4> interm_result_ids_;

  TO_STRING_KV(K_(interm_result_ids));
};

}

namespace obrpc
{
class ObExecutorRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObExecutorRpcProxy);

  // For remote plan
  RPC_SS(@PR5 task_execute, obrpc::OB_REMOTE_EXECUTE, (sql::ObTask), common::ObScanner);
  RPC_SS(@PR5 remote_task_execute, obrpc::OB_REMOTE_SYNC_EXECUTE, (sql::ObRemoteTask), common::ObScanner);
  RPC_S(@PR5 task_kill, obrpc::OB_TASK_KILL, (sql::ObTaskID));

  // For DTL
  RPC_AP(@PR5 erase_dtl_interm_result, obrpc::OB_ERASE_DTL_INTERM_RESULT, (sql::ObEraseDtlIntermResultArg));
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROXY_ */
//// end of header file
