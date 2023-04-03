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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROCESSOR_
#define OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROCESSOR_

#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "sql/executor/ob_executor_rpc_proxy.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/ob_sql_trans_control.h"
#include "share/schema/ob_schema_getter_guard.h"

#define OB_DEFINE_SQL_PROCESSOR(cls, pcode, pname)                          \
  class pname : public obrpc::ObRpcProcessor<                           \
    obrpc::Ob ## cls ## RpcProxy::ObRpc<pcode> >


#define OB_DEFINE_SQL_CMD_PROCESSOR(cls, pcode, pname)        \
  OB_DEFINE_SQL_PROCESSOR(cls, obrpc::pcode, pname)         \
  {                                             \
  public:                                       \
    pname(const observer::ObGlobalContext &)    \
    {}                                          \
    virtual ~pname() {}                         \
  protected: int process();                     \
             int preprocess_arg();               \
  private:                                      \
    common::ObArenaAllocator alloc_;     \
  }

namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}
namespace share
{
namespace schema
{
}
}
namespace sql
{
class ObExecContext;
class ObPhysicalPlan;

class ObWorkerSessionGuard
{
public:
  ObWorkerSessionGuard(ObSQLSessionInfo *session);
  ~ObWorkerSessionGuard();
};

// For DTL
OB_DEFINE_SQL_CMD_PROCESSOR(Executor, OB_ERASE_DTL_INTERM_RESULT, ObRpcEraseIntermResultP);
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROCESSOR_ */
