#include "share/ob_occam_time_guard.h"
#include "ob_lcl_time_sync_rpc.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_lcl_time_sync_thread.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace detector;
extern const char * MEMORY_LABEL;
namespace obrpc
{

uint64_t obrpc::ObDeadLockTimeSyncMessageP::ob_server_start_time = ObLCLTimeSyncThread::get_lcl_real_local_time();
int ObDeadLockTimeSyncMessageP::process()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(process_msg(result_, arg_))) {
    DETECT_LOG(WARN, "time sync process msg failed",
               KR(ret), K(arg_));
  } else {
    // do nothing
  }
  return ret;
}

int ObDeadLockTimeSyncMessageP::process_msg(ObDeadLockTimeSyncResp &resp, const ObDeadLockTimeSyncArg &arg) {
  int ret = OB_SUCCESS;

  int64_t leader_real_ts = ObLCLTimeSyncThread::get_lcl_real_local_time();
  int64_t lcl_time_skew_rate = ObServerConfig::get_instance()._lcl_time_skew_rate;
  int64_t clock_skew_offset_for_test = 0;
  if (lcl_time_skew_rate != 0) {
    // every 100ms the offset is clock_skew_rate ms
    double clock_skew_rate = 1.0 * lcl_time_skew_rate / 100 * 1000;
    clock_skew_offset_for_test = (leader_real_ts - ob_server_start_time) * clock_skew_rate;
    leader_real_ts += clock_skew_offset_for_test;
  }
  DETECT_LOG(INFO, "rs leader receive time sync request", K(leader_real_ts), K(leader_real_ts), K(clock_skew_offset_for_test));
  resp.set_leader_current_time(leader_real_ts);
  return ret;
}

}// namespace obrpc

namespace share
{
namespace detector
{
  
constexpr const int64_t OB_LCL_RPC_TIMEOUT = 500 * 1000;

int ObLCLTimeSyncRpc::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(OB_SERVER_TENANT_ID, MEMORY_LABEL);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    DETECT_LOG(WARN, "init twice", KR(ret));
  } else if (nullptr ==
       (proxy_ =
       (obrpc::ObLCLTimeSyncRpcProxy *)ob_malloc(sizeof(obrpc::ObLCLTimeSyncRpcProxy), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "alloc proxy_ memory failed", KR(ret));
  } else {
    proxy_ = new (proxy_) obrpc::ObLCLTimeSyncRpcProxy();
    is_inited_ = true;
  }

  return ret;
}

void ObLCLTimeSyncRpc::destroy()
{
  if (is_inited_ == false) {
    DETECT_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "ObDeadLockDetectorRpc has been destroyed", K(lbt()));
  } else {
    if (nullptr != proxy_) {
      proxy_->destroy();
      ob_free(proxy_);
      proxy_ = nullptr;
    }
    is_inited_ = false;
    DETECT_LOG(INFO, "ObLCLTimeSyncRpc destroy success");
  }
}

int ObLCLTimeSyncRpc::update_local_time_message(
    const ObAddr &dest_addr, const ObDeadLockTimeSyncArg &time_sync_arg,
    ObDeadLockTimeSyncResp &resp) {
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(proxy_->to(dest_addr)
                  .timeout(OB_LCL_RPC_TIMEOUT)
                  .update_local_time_message(time_sync_arg, resp))) {
  } else {
    // do nothing
  }
  return ret;
}

}// namespace detector
}// namespace share
}// namespace oceanbase
