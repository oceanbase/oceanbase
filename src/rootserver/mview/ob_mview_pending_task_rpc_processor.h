/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "share/ob_srv_rpc_proxy.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace obrpc
{

class ObRpcRunMViewPendingTaskCB
  : public obrpc::ObSrvRpcProxy::AsyncCB<obrpc::OB_RUN_MVIEW_PENDING_TASK>
{
public:
  ObRpcRunMViewPendingTaskCB() {}
  virtual ~ObRpcRunMViewPendingTaskCB() {}

public:
  virtual int process();
  virtual void on_invalid() override;
  virtual void on_timeout() override;

  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof(*this));
    rpc::frame::ObReqTransport::AsyncCB *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObRpcRunMViewPendingTaskCB();
      static_cast<ObRpcRunMViewPendingTaskCB *>(newcb)->arg_ = arg_;
    }
    return newcb;
  }

  virtual void set_args(const Request &arg) { arg_ = arg; }

private:
  Request arg_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcRunMViewPendingTaskCB);
};

class ObRpcRunMViewPendingTaskP
  : public obrpc::ObRpcProcessor<obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_RUN_MVIEW_PENDING_TASK>>
{
public:
  ObRpcRunMViewPendingTaskP(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}

protected:
  int process();

private:
  const observer::ObGlobalContext &gctx_;
};

class ObRpcScheduleMViewRefreshP
  : public obrpc::ObRpcProcessor<obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_SCHEDULE_MVIEW_REFRESH>>
{
public:
  ObRpcScheduleMViewRefreshP(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}

protected:
  int process();

private:
  const observer::ObGlobalContext &gctx_;
};

class ObRpcKillMViewRefreshP
  : public obrpc::ObRpcProcessor<obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_KILL_MVIEW_REFRESH>>
{
public:
  ObRpcKillMViewRefreshP(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}

protected:
  int process();

private:
  const observer::ObGlobalContext &gctx_;
};

} // namespace obrpc
} // namespace oceanbase
