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
#ifndef __SQL_ENG_P2P_DH_RPC_PROCESS_H__
#define __SQL_ENG_P2P_DH_RPC_PROCESS_H__
#include "rpc/obrpc/ob_rpc_processor.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "lib/ob_define.h"
namespace oceanbase {
namespace sql {

class ObPxP2pDhMsgP
    : public obrpc::ObRpcProcessor<obrpc::ObP2PDhRpcProxy::ObRpc<obrpc::OB_PX_P2P_DH_MSG>>
{
public:
  ObPxP2pDhMsgP(const observer::ObGlobalContext &gctx) { UNUSED(gctx);}
  virtual ~ObPxP2pDhMsgP() = default;
  //virtual int init() final;
  //virtual void destroy() final;
  virtual int process() final;
  DISALLOW_COPY_AND_ASSIGN(ObPxP2pDhMsgP);
};

class ObPxP2pDhMsgCB
      : public obrpc::ObP2PDhRpcProxy::AsyncCB<obrpc::OB_PX_P2P_DH_MSG>
{
public:
    ObPxP2pDhMsgCB(const common::ObAddr &server,
                   const common::ObCurTraceId::TraceId &trace_id,
                   int64_t start_time,
                   int64_t timeout_ts,
                   int64_t p2p_datahub_id)
        : addr_(server),
          start_time_(start_time),
          timeout_ts_(timeout_ts),
          p2p_datahub_id_(p2p_datahub_id)
  {
    trace_id_.set(trace_id);
  }
  virtual ~ObPxP2pDhMsgCB() {}
public:
  virtual int process() { return OB_SUCCESS; }
  virtual void on_invalid() {}
  virtual void on_timeout();
  rpc::frame::ObReqTransport::AsyncCB *clone(
      const rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof(*this));
    rpc::frame::ObReqTransport::AsyncCB *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObPxP2pDhMsgCB(addr_, trace_id_,
          start_time_, timeout_ts_, p2p_datahub_id_);
    }
    return newcb;
  }
  virtual void set_args(const Request &arg) { UNUSED(arg); }
private:
  common::ObCurTraceId::TraceId trace_id_;
  common::ObAddr addr_;
  int64_t start_time_;
  int64_t timeout_ts_;
  int64_t p2p_datahub_id_;
  DISALLOW_COPY_AND_ASSIGN(ObPxP2pDhMsgCB);
};


class ObPxP2pDhClearMsgP
    : public obrpc::ObRpcProcessor<obrpc::ObP2PDhRpcProxy::ObRpc<obrpc::OB_PX_CLAER_DH_MSG>>
{
public:
  ObPxP2pDhClearMsgP(const observer::ObGlobalContext &gctx) { UNUSED(gctx);}
  virtual ~ObPxP2pDhClearMsgP() = default;
  //virtual int init() final;
  //virtual void destroy() final;
  virtual int process() final;
  DISALLOW_COPY_AND_ASSIGN(ObPxP2pDhClearMsgP);
};


}
}

#endif
