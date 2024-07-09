#ifndef OCEANBASE_LCL_TIME_SYNC_RPC_H
#define OCEANBASE_LCL_TIME_SYNC_RPC_H

#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "ob_lcl_time_sync_message.h"

namespace oceanbase
{
namespace obrpc
{
class  ObLCLTimeSyncRpcProxy : public ObRpcProxy
{
public:
  DEFINE_TO(ObLCLTimeSyncRpcProxy);
  RPC_S(PR5 update_local_time_message,
         OB_DETECTOR_TIME_SYNC_MESSAGE,
        (share::detector::ObDeadLockTimeSyncArg), share::detector::ObDeadLockTimeSyncResp);

};

class ObDeadLockTimeSyncMessageP : public
      ObRpcProcessor<ObLCLTimeSyncRpcProxy::ObRpc<OB_DETECTOR_TIME_SYNC_MESSAGE>>
{
public:
  explicit ObDeadLockTimeSyncMessageP(const observer::ObGlobalContext &global_ctx)
  { UNUSED(global_ctx); }
  static uint64_t ob_server_start_time;
protected:
  int process();
  int process_msg(share::detector::ObDeadLockTimeSyncResp &resp, const share::detector::ObDeadLockTimeSyncArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDeadLockTimeSyncMessageP);
};

}// obrpc

namespace share
{
namespace detector
{

using obrpc::ObLCLTimeSyncRpcProxy;

class ObLCLTimeSyncRpc
{
public:
  ObLCLTimeSyncRpc() :
    is_inited_(false),
    proxy_(nullptr) {};
  ~ObLCLTimeSyncRpc() = default;
  int init();
  void destroy();
public:
  virtual int
  update_local_time_message(const ObAddr &dest_addr,
                             const ObDeadLockTimeSyncArg &time_sync_arg, ObDeadLockTimeSyncResp &resp);

private:
  bool is_inited_;
  obrpc::ObLCLTimeSyncRpcProxy *proxy_;
};

}// detector
}// share
}// oceanbase
#endif
