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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_DETECTOR_RPC_
#define OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_DETECTOR_RPC_

#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "ob_deadlock_detector_common_define.h"
#include "ob_deadlock_parameters.h"
#include "ob_deadlock_detector_mgr.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_message.h"
#include "ob_deadlock_message.h"

namespace oceanbase
{
namespace obrpc
{
using share::detector::INVALID_VALUE;

class  ObDetectorRpcProxy : public ObRpcProxy
{
public:
  DEFINE_TO(ObDetectorRpcProxy);
  RPC_AP(PR9 post_lcl_message,
         OB_DETECTOR_LCL_MESSAGE,
         (share::detector::ObLCLMessage), Int64);
  RPC_AP(PR9 post_collect_info_message,
         OB_DETECTOR_COLLECT_INFO_MESSAGE,
         (share::detector::ObDeadLockCollectInfoMessage), Int64);
  RPC_AP(PR9 post_notify_parent_message,
         OB_DETECTOR_NOTIFY_PARENT_MESSAGE,
         (share::detector::ObDeadLockNotifyParentMessage), Int64)
};

class ObDetectorLCLMessageP : public
      ObRpcProcessor<ObDetectorRpcProxy::ObRpc<OB_DETECTOR_LCL_MESSAGE>>
{
public:
  explicit ObDetectorLCLMessageP(const observer::ObGlobalContext &global_ctx)
  { UNUSED(global_ctx); }
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObDetectorLCLMessageP);
};

class ObDeadLockCollectInfoMessageP : public
      ObRpcProcessor<ObDetectorRpcProxy::ObRpc<OB_DETECTOR_COLLECT_INFO_MESSAGE>>
{
public:
  explicit ObDeadLockCollectInfoMessageP(const observer::ObGlobalContext &global_ctx)
  { UNUSED(global_ctx); }
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObDeadLockCollectInfoMessageP);
};

class ObDeadLockNotifyParentMessageP : public
      ObRpcProcessor<ObDetectorRpcProxy::ObRpc<OB_DETECTOR_NOTIFY_PARENT_MESSAGE>>
{
public:
  explicit ObDeadLockNotifyParentMessageP(const observer::ObGlobalContext &global_ctx)
  { UNUSED(global_ctx); }
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObDeadLockNotifyParentMessageP);
};

template <ObRpcPacketCode PC>
class ObDetectorRPCCB : public ObDetectorRpcProxy::AsyncCB<PC>
{
public:
  ObDetectorRPCCB() {}
  ~ObDetectorRPCCB() {}
  void set_args(const typename ObDetectorRpcProxy::AsyncCB<PC>::Request &args) {
    UNUSED(args);
  }
  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const {
    void *buf = alloc(sizeof (*this));
    ObDetectorRPCCB<PC> *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObDetectorRPCCB<PC>();
    }
    return newcb;
  }
  int process() {
    const common::ObAddr &dst = ObDetectorRpcProxy::AsyncCB<PC>::dst_;
    const Int64 &remote_ret = ObDetectorRpcProxy::AsyncCB<PC>::result_;
    ObRpcResultCode &rcode = ObDetectorRpcProxy::AsyncCB<PC>::rcode_;

    if (OB_SUCCESS != rcode.rcode_) {
      DETECT_LOG_RET(WARN, rcode.rcode_, "detector rpc returns error code(rpc level)", K(rcode), K(dst));
    } else if (OB_SUCCESS != remote_ret) {
      const int32_t tmp_ret = static_cast<int32_t>(remote_ret);
      DETECT_LOG_RET(WARN, tmp_ret, "detector rpc returns error code(detector level)",
                       KR(tmp_ret), K(rcode.rcode_), K(dst));
    } else {
      // do nothing
    }

    return OB_SUCCESS;
  }
  void on_timeout() {
    const common::ObAddr &dst = ObDetectorRpcProxy::AsyncCB<PC>::dst_;
    const int ret = ((0 == this->get_error()) ? common::OB_TIMEOUT : common::OB_RPC_CONNECT_ERROR);
    DETECT_LOG(WARN, "ObDetector RPC timeout", KR(ret), K(dst));
  }
};
}// obrpc

namespace share
{
namespace detector
{

using obrpc::ObDetectorRpcProxy;

class ObDeadLockDetectorRpc
{
public:
  ObDeadLockDetectorRpc() :
    is_inited_(false),
    proxy_(nullptr) {};
  ~ObDeadLockDetectorRpc() = default;
  int init(obrpc::ObDetectorRpcProxy *proxy, const common::ObAddr &self);
  void destroy();
public:
  virtual int post_lcl_message(const ObAddr &dest_addr, const ObLCLMessage &lcl_msg);
  virtual int post_collect_info_message(const ObAddr &dest_addr,
                                        const ObDeadLockCollectInfoMessage &lcl_msg);
  virtual int post_notify_parent_message(const ObAddr &dest_addr,
                                         const ObDeadLockNotifyParentMessage &notify_msg);
private:
  bool is_inited_;
  obrpc::ObDetectorRpcProxy *proxy_;
  common::ObAddr self_;
  obrpc::ObDetectorRPCCB<obrpc::ObRpcPacketCode::OB_DETECTOR_LCL_MESSAGE> lcl_msg_cb_;
  obrpc::ObDetectorRPCCB<obrpc::ObRpcPacketCode::OB_DETECTOR_COLLECT_INFO_MESSAGE> collect_msg_cb_;
  obrpc::ObDetectorRPCCB<obrpc::ObRpcPacketCode::OB_DETECTOR_NOTIFY_PARENT_MESSAGE> notify_msg_cb_;
};

}// detector
}// share
}// oceanbase
#endif
