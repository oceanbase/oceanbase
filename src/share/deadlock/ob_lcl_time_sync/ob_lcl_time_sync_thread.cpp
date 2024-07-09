#include "ob_lcl_time_sync_thread.h"
#include "ob_lcl_time_sync_rpc.h"
#include "ob_lcl_time_sync_message.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "share/deadlock/ob_deadlock_arg_checker.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "share/deadlock/ob_deadlock_detector_rpc.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include <ctime>
#include <exception>
namespace oceanbase {
namespace share {
namespace detector {

using namespace common;

int64_t ObLCLTimeSyncThread::local_offset_ = 0;
double ObLCLTimeSyncThread::client_rand_per_for_test = 0;
extern const char * MEMORY_LABEL;

int ObLCLTimeSyncThread::start() {
  CHECK_INIT();
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_running_, true);
  if (OB_FAIL(ObThreadPool::start())) {
    ATOMIC_STORE(&is_running_, false);
    DETECT_LOG(ERROR, "satrt ObLCLTimeSyncThread failed", KR(ret));
  }
  return ret;
}

void ObLCLTimeSyncThread::run1() {
  lib::set_thread_name("TimeSyncThread");
  DETECT_LOG(INFO, "ObLCLTimeSyncThread start run");
  update_local_time_from_rs_leader_periodically_();
}

void ObLCLTimeSyncThread::stop() {
  ObThreadPool::stop();
  ATOMIC_STORE(&is_running_, false);
  DETECT_LOG(INFO, "ObLCLTimeSyncThread stop");
}

void ObLCLTimeSyncThread::destroy() {
  int ret = OB_SUCCESS;
  (void)stop();
  (void)wait();
  
  if (false == is_inited_) {
    DETECT_LOG(WARN, "ObLCLTimeSyncThread not init or has been destroyed");
  } else {
    if (nullptr != time_sync_rpc_) {
      time_sync_rpc_->destroy();
      ob_free(time_sync_rpc_);
      time_sync_rpc_ = nullptr;
    }
    is_inited_ = false;
    DETECT_LOG(INFO, "ObLCLTimeSyncThread destroy success");
  }
  DETECT_LOG(INFO, "ObLCLTimeSyncThread destroy called", K(lbt()));
}

void ObLCLTimeSyncThread::wait() {
  ObThreadPool::wait();
  DETECT_LOG(INFO, "ObLCLTimeSyncThread wait");
}

void ObLCLTimeSyncThread::update_local_time_from_rs_leader_periodically_() {
  while (ATOMIC_LOAD(&is_running_)) {
    int max_retry_time = 3;
    int retries = 0;
    DETECT_LOG(INFO,
               "update local time request: update local time periodically");
    while (retries < max_retry_time) {
      int ret = update_local_time_from_rs_leader_(true);
      if (ret == OB_SUCCESS) {
        break;
      }
      DETECT_LOG(INFO,
               "update local time request: retry", KR(ret));
      retries++;
    }
    usleep(auto_fit_update_time_ms * 1000);
  }
};

void ObLCLTimeSyncThread::try_update_local_time_from_rs_leader_now() {
  int64_t local_time = get_lcl_local_time();
  int64_t last_update_time_interval = abs(local_time - last_update_time_);
  if (last_update_time_interval > UPDATE_TIME_FROM_CLIENT_MIN_INTERVAL) {
    DETECT_LOG(INFO, "update local time request: update local time now");
    update_local_time_from_rs_leader_(false);
  }
};

int ObLCLTimeSyncThread::update_local_time_from_rs_leader_(
    bool change_update_local_time_interval) {
  int ret = OB_SUCCESS;
  uint64_t client_send_request_time;
  uint64_t client_recv_response_time;
  uint64_t network_delay;
  ObDeadLockTimeSyncArg arg;
  ObDeadLockTimeSyncResp resp;
  ObAddr rs_leader_addr;
  if (OB_FAIL(get_rs_leader_addr_(rs_leader_addr))) {
    DETECT_LOG(WARN, "update local time failed: get rs leader failed", KR(ret));
    return ret;
  } else if (OB_FAIL(get_global_time_from_rs_leader_(
                 rs_leader_addr, arg, resp, client_send_request_time,
                 client_recv_response_time))) {
    DETECT_LOG(WARN, "update local time failed: get time from rs leader failed",
               KR(ret), K(rs_leader_addr));
    return ret;
  } else if (resp_not_in_resonable_time_(
                 resp.get_leader_current_us(), client_send_request_time,
                 client_recv_response_time, network_delay)) {
    DETECT_LOG(WARN, "update local time failed: resp not in resonable time",
               KR(ret), K(resp.get_leader_current_us()),
               K(client_send_request_time), K(client_recv_response_time));
    return ret;
  }
  if (change_update_local_time_interval) {
    change_lcl_update_time_interval_(rs_leader_addr, resp.get_leader_current_us(), network_delay, client_recv_response_time);
  }
  set_lcl_local_time_offset_and_last_update_time_(
      network_delay, resp.get_leader_current_us(), client_recv_response_time);
  return ret;
}

void ObLCLTimeSyncThread::change_lcl_update_time_interval_(const ObAddr &rs_leader_addr, int64_t leader_current_us, int64_t network_delay, int64_t client_recv_response_time) {
  if (rs_leader_addr != last_rs_leader_addr_) {
      last_rs_leader_addr_ = rs_leader_addr;
      auto_fit_update_time_ms = UPDATE_TIME_FROM_CLIENT_MIN_INTERVAL;
    }
    if (time_in_expected_range_(network_delay, leader_current_us,
                               client_recv_response_time)) {
      if (auto_fit_update_time_ms < UPDATE_TIME_FROM_CLIENT_MAX_INTERVAL) {
        auto_fit_update_time_ms += ObLCLTimeSyncThread::INTERVAL_CHANGE_UNIT;
        DETECT_LOG(
            INFO,
            "update local time success: auto fit update time change add 10s",
            K(auto_fit_update_time_ms));
      }
    } else {
      if (auto_fit_update_time_ms > UPDATE_TIME_FROM_CLIENT_MIN_INTERVAL) {
        auto_fit_update_time_ms -= ObLCLTimeSyncThread::INTERVAL_CHANGE_UNIT;
        DETECT_LOG(INFO,
                   "update local time success: auto fit update time change "
                   "subtract 10s",
                   K(auto_fit_update_time_ms));
      }
    }
};

int ObLCLTimeSyncThread::get_rs_leader_addr_(ObAddr &rs_leader_addr) {
  return GCTX.location_service_->nonblock_get_leader(
      obrpc::ObRpcNetHandler::CLUSTER_ID, OB_SYS_TENANT_ID,
      ObLSID(share::ObLSID::SYS_LS_ID), rs_leader_addr);
};

int ObLCLTimeSyncThread::init() {
  int ret = OB_SUCCESS;
  if (is_inited_) {
    is_inited_ = true;
    return ret;
  }
  ObMemAttr attr(OB_SERVER_TENANT_ID, MEMORY_LABEL);
  if (nullptr == (time_sync_rpc_ = (ObLCLTimeSyncRpc *)ob_malloc(sizeof(ObLCLTimeSyncRpc), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "alloc rpc_ memory failed", KR(ret));
  } else {
    time_sync_rpc_ = new (time_sync_rpc_) ObLCLTimeSyncRpc();
    time_sync_rpc_->init();
  }
  if (OB_FAIL(share::ObThreadPool::init())) {
    DETECT_LOG(WARN, "init thread failed", K(ret), KP(this), K(MTL_ID()));
  } else {
    is_inited_ = true;
  }
  client_rand_per_for_test = get_rand_between_one_and_zero_();
  DETECT_LOG(INFO, "init thread rand_per", K(client_rand_per_for_test));
  return ret;
}

int ObLCLTimeSyncThread::get_global_time_from_rs_leader_(
    const ObAddr &rs_leader_addr, const ObDeadLockTimeSyncArg &arg,
    ObDeadLockTimeSyncResp &resp, uint64_t &client_send_request_time,
    uint64_t &client_recv_response_time) {
  int ret = OB_SUCCESS;
  client_send_request_time = get_lcl_local_time();
  if (OB_FAIL(time_sync_rpc_->update_local_time_message(
          rs_leader_addr, arg, resp))) {
    return ret;
  }
  client_recv_response_time = get_lcl_local_time();
  return ret;
}

bool ObLCLTimeSyncThread::resp_not_in_resonable_time_(
    uint64_t leader_current_us, uint64_t client_send_request_time,
    uint64_t client_recv_response_time, uint64_t &network_delay) {
  network_delay = (client_recv_response_time - client_send_request_time) / 2;
  return network_delay >= MAX_NETWORK_DELAY;
}

bool ObLCLTimeSyncThread::time_in_expected_range_(
    uint64_t delay_offset, uint64_t leader_current_us,
    uint64_t client_recv_response_time) {
  bool new_time_in_expected_delay_range =
      leader_current_us - delay_offset >=
          client_recv_response_time - expect_delay_offset_ &&
      leader_current_us + delay_offset <=
          client_recv_response_time + expect_delay_offset_;
  return new_time_in_expected_delay_range;
}

void ObLCLTimeSyncThread::set_lcl_local_time_offset_and_last_update_time_(
    uint64_t delay_offset, uint64_t leader_current_us,
    uint64_t client_recv_response_time) {
  uint64_t now_lcl_sync_real_time = leader_current_us + delay_offset;
  uint64_t local_real_time = ObLCLTimeSyncThread::get_lcl_real_local_time();
  local_offset_ =
      now_lcl_sync_real_time - local_real_time;
  last_update_time_ = now_lcl_sync_real_time;
  report_update_lcl_time_info_to_inner_table_(local_real_time, local_offset_, auto_fit_update_time_ms);
  DETECT_LOG(INFO, "update local time success: set local time offset",
             K(now_lcl_sync_real_time), K(delay_offset), K(local_offset_), K(local_real_time));
}

double ObLCLTimeSyncThread::get_rand_between_one_and_zero_() { 
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  int64_t randt = static_cast<int64_t>(ts.tv_sec) * 1000000000L +
         static_cast<int64_t>(ts.tv_nsec);
  srand(randt);
	int iRand = rand();
	iRand %= 10000; 
	return iRand / 10000.0;
}

int64_t ObLCLTimeSyncThread::get_lcl_real_local_time() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  int64_t lcl_time_client_offset = ObServerConfig::get_instance().lcl_time_client_offset;
  return static_cast<int64_t>(ts.tv_sec) * 1000L +
         static_cast<int64_t>(ts.tv_nsec / 1000000L) + (lcl_time_client_offset * client_rand_per_for_test);
}

int64_t ObLCLTimeSyncThread::get_lcl_local_time() {
  return local_offset_ + get_lcl_real_local_time();
}

int64_t ObLCLTimeSyncThread::get_lcl_local_offset() {
  return local_offset_;
}

void ObLCLTimeSyncThread::report_update_lcl_time_info_to_inner_table_(int64_t local_real_time, int64_t local_offset, int64_t update_interval) {
  int ret;
  int64_t report_ts = ObClockGenerator::getRealClock();
  if (OB_FAIL(SERVER_EVENT_SYNC_ADD("DEADLOCK",
                                    "TIME_SYNC",
                                    "LOCAL_REAL_TIME",
                                    local_real_time,
                                    "LOCAL_OFFSET",
                                    local_offset,
                                    "UPDATE_INTERVAL",
                                    update_interval,
                                    "UPDATE_TIME",
                                    ObTime2Str::ob_timestamp_str_range<YEAR, USECOND>(report_ts)))) {

  } else {}
};

} // namespace detector

} // namespace share
} // namespace oceanbase