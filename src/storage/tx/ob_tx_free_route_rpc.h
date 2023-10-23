/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

namespace obrpc {

class ObTxFreeRouteCheckAliveP : public ObRpcProcessor< ObTransRpcProxy::ObRpc<OB_TX_FREE_ROUTE_CHECK_ALIVE> >
{
public:
  ObTxFreeRouteCheckAliveP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObTxFreeRouteCheckAliveP);
};

class ObTxFreeRouteCheckAliveRespP : public ObRpcProcessor< ObTransRpcProxy::ObRpc<OB_TX_FREE_ROUTE_CHECK_ALIVE_RESP> >
{
public:
  ObTxFreeRouteCheckAliveRespP() {}
protected:
  int process();
private:
  int kill_session_();
  int release_session_tx_();
  DISALLOW_COPY_AND_ASSIGN(ObTxFreeRouteCheckAliveRespP);
};

class ObTxFreeRoutePushStateP : public ObRpcProcessor< ObTransRpcProxy::ObRpc<OB_TX_FREE_ROUTE_PUSH_STATE> >
{
protected:
  int process();
};

template<ObRpcPacketCode PC>
class ObTxFreeRouteRPCCB : public ObTransRpcProxy::AsyncCB<PC>
{
public:
  void set_args(const typename ObTransRpcProxy::AsyncCB<PC>::Request &args)
  {}
  int init() {}
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(const oceanbase::rpc::frame::SPAlloc &alloc) const {
    void *buf = alloc(sizeof (*this));
    return new (buf) ObTxFreeRouteRPCCB<PC>();
  }
  int process();
  void on_timeout();
};

}
//}
