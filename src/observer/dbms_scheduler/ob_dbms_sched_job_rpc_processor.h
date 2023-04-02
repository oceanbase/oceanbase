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

#ifndef SRC_OBSERVER_DBMS_SCHED_JOB_RPC_PROCESSOR_H_
#define SRC_OBSERVER_DBMS_SCHED_JOB_RPC_PROCESSOR_H_

#include "ob_dbms_sched_job_rpc_proxy.h"
#include "observer/ob_server_struct.h"


namespace oceanbase
{
namespace observer
{
class ObGlobalContext;
}
namespace obrpc
{

class ObRpcAPDBMSSchedJobCB
  : public obrpc::ObDBMSSchedJobRpcProxy::AsyncCB<obrpc::OB_RUN_DBMS_SCHED_JOB>
{
public:
  ObRpcAPDBMSSchedJobCB() {}
  virtual ~ObRpcAPDBMSSchedJobCB() {}

public:
  virtual int process();
  virtual void on_invalid() {}
  virtual void on_timeout()
  {
    SQL_EXE_LOG_RET(WARN, OB_TIMEOUT, "run dbms sched job timeout!");
  }

  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof(*this));
    rpc::frame::ObReqTransport::AsyncCB *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObRpcAPDBMSSchedJobCB();
    }
    return newcb;
  }

  virtual void set_args(const Request &arg) { UNUSED(arg); }

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcAPDBMSSchedJobCB);
};

class ObRpcRunDBMSSchedJobP
  : public obrpc::ObRpcProcessor<obrpc::ObDBMSSchedJobRpcProxy::ObRpc<obrpc::OB_RUN_DBMS_SCHED_JOB> >
{
public:
  ObRpcRunDBMSSchedJobP(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}

protected:
  int process();

private:
  const observer::ObGlobalContext &gctx_;
};

}
}
#endif /* SRC_OBSERVER_DBMS_SCHED_JOB_RPC_PROCESSOR_H_ */


