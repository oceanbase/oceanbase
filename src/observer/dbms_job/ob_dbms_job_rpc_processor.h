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

#ifndef SRC_OBSERVER_DBMS_JOB_RPC_PROCESSOR_H_
#define SRC_OBSERVER_DBMS_JOB_RPC_PROCESSOR_H_

#include "ob_dbms_job_rpc_proxy.h"
#include "observer/ob_server_struct.h"


namespace oceanbase
{
namespace observer
{
class ObGlobalContext;
}
namespace obrpc
{

class ObRpcAPDBMSJobCB
  : public obrpc::ObDBMSJobRpcProxy::AsyncCB<obrpc::OB_RUN_DBMS_JOB>
{
public:
  ObRpcAPDBMSJobCB() {}
  virtual ~ObRpcAPDBMSJobCB() {}

public:
  virtual int process();
  virtual void on_invalid() {}
  virtual void on_timeout()
  {
    SQL_EXE_LOG_RET(WARN, common::OB_TIMEOUT, "run dbms job timeout!");
  }

  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof(*this));
    rpc::frame::ObReqTransport::AsyncCB *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObRpcAPDBMSJobCB();
    }
    return newcb;
  }

  virtual void set_args(const Request &arg) { UNUSED(arg); }

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcAPDBMSJobCB);
};

class ObRpcRunDBMSJobP
  : public obrpc::ObRpcProcessor<obrpc::ObDBMSJobRpcProxy::ObRpc<obrpc::OB_RUN_DBMS_JOB> >
{
public:
  ObRpcRunDBMSJobP(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}

protected:
  int process();

private:
  const observer::ObGlobalContext &gctx_;
};

}
}
#endif /* SRC_OBSERVER_DBMS_JOB_RPC_PROCESSOR_H_ */


