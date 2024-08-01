#include "ob_lcl_time_sync_thread.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_lcl_time_sync_message.h"
#include "ob_lcl_time_sync_rpc.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
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

extern const char *MEMORY_LABEL;

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
    int ret = OB_SUCCESS;
    while (retries++ < max_retry_time && OB_FAIL(update_local_time_from_rs_leader_(need_update_time_from_rs_leader_now_)) && ATOMIC_LOAD(&is_running_)) {
      DETECT_LOG(INFO, "update local time request: retry", KR(ret));
    }
    ATOMIC_STORE(&need_update_time_from_rs_leader_now_, false);
    uint64_t start_time = ObClockGenerator::getRealClock();
    uint64_t next_update_time_intervel = ATOMIC_LOAD(&auto_fit_update_time);
    while (ATOMIC_LOAD(&is_running_) &&
           ObClockGenerator::getRealClock() - start_time < next_update_time_intervel && !ATOMIC_LOAD(&need_update_time_from_rs_leader_now_)) {
      usleep(10 * 1000);
    }
  }
};

void ObLCLTimeSyncThread::try_update_local_time_from_rs_leader_now() {
  int64_t local_time = get_lcl_local_time();
  int64_t last_update_time_interval = abs(local_time - last_update_time_);
  if (last_update_time_interval > UPDATE_TIME_FROM_CLIENT_MIN_INTERVAL_) {
    DETECT_LOG(INFO, "update local time request: update local time now");
    ATOMIC_STORE(&need_update_time_from_rs_leader_now_, true);
  }
};

int ObLCLTimeSyncThread::update_local_time_from_rs_leader_(
    bool need_update_time_from_rs_leader_now) {
  int ret = OB_SUCCESS;
  uint64_t client_send_request_time = 0;
  uint64_t client_recv_response_time = 0;
  uint64_t network_delay = 0;
  ObDeadLockTimeSyncArg arg;
  ObDeadLockTimeSyncResp resp;
  ObAddr rs_leader_addr;
  if (OB_FAIL(get_rs_leader_addr_(rs_leader_addr))) {
    DETECT_LOG(WARN, "update local time failed: get rs leader failed", KR(ret));
  } else if (is_rs_leader(rs_leader_addr)) {
    // do nothing
  }
  else if (OB_FAIL(get_global_time_from_rs_leader_(
                 rs_leader_addr, arg, resp, client_send_request_time,
                 client_recv_response_time))) {
    DETECT_LOG(WARN, "update local time failed: get time from rs leader failed",
               KR(ret), K(rs_leader_addr));
  } else if (resp_not_in_resonable_time_(
                 resp.get_leader_current_time(), client_send_request_time,
                 client_recv_response_time, network_delay)) {
    ret = OB_EAGAIN;
    DETECT_LOG(WARN, "update local time failed: resp not in resonable time",
               KR(ret), K(resp.get_leader_current_time()),
               K(client_send_request_time), K(client_recv_response_time));
  } else {
    DETECT_LOG(INFO, "update local time", K(need_update_time_from_rs_leader_now));
    if (!need_update_time_from_rs_leader_now) {
      change_lcl_update_time_interval_(rs_leader_addr,
                                      resp.get_leader_current_time(),
                                      network_delay, client_recv_response_time);
    }
    set_lcl_local_time_offset_and_last_update_time_(
        network_delay, resp.get_leader_current_time(), client_recv_response_time);
  }
  return ret;
}

void ObLCLTimeSyncThread::change_lcl_update_time_interval_(
    const ObAddr &rs_leader_addr, int64_t leader_current_time,
    int64_t network_delay, int64_t client_recv_response_time) {
  if (rs_leader_addr != last_rs_leader_addr_) {
    last_rs_leader_addr_ = rs_leader_addr;
    ATOMIC_STORE(&auto_fit_update_time, UPDATE_TIME_FROM_CLIENT_MIN_INTERVAL_);
  }
  if (time_in_expected_range_(network_delay, leader_current_time,
                              client_recv_response_time)) {
    if (auto_fit_update_time < UPDATE_TIME_FROM_CLIENT_MAX_INTERVAL_) {
      ATOMIC_FAA(&auto_fit_update_time, ObLCLTimeSyncThread::INTERVAL_CHANGE_UNIT_);
      DETECT_LOG(
          INFO,
          "update local time success: auto fit update time change add 10s",
          K(auto_fit_update_time));
    }
  } else {
    if (auto_fit_update_time > UPDATE_TIME_FROM_CLIENT_MIN_INTERVAL_) {
      ATOMIC_FAA(&auto_fit_update_time, -ObLCLTimeSyncThread::INTERVAL_CHANGE_UNIT_);
      DETECT_LOG(INFO,
                 "update local time success: auto fit update time change "
                 "subtract 10s",
                 K(auto_fit_update_time));
    }
  }
};

int ObLCLTimeSyncThread::get_rs_leader_addr_(ObAddr &rs_leader_addr) {
  return GCTX.location_service_->nonblock_get_leader(
      obrpc::ObRpcNetHandler::CLUSTER_ID, OB_SYS_TENANT_ID,
      ObLSID(share::ObLSID::SYS_LS_ID), rs_leader_addr);
};

bool ObLCLTimeSyncThread::is_rs_leader(ObAddr &rs_leader_addr) {
  return rs_leader_addr == GCTX.self_addr();
}

int ObLCLTimeSyncThread::init() {
  int ret = OB_SUCCESS;
  if (is_inited_) {
    return ret;
  }
  ObMemAttr attr(OB_SERVER_TENANT_ID, MEMORY_LABEL);
  if (nullptr == (time_sync_rpc_ = (ObLCLTimeSyncRpc *)ob_malloc(
                      sizeof(ObLCLTimeSyncRpc), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    DETECT_LOG(WARN, "alloc rpc_ memory failed", KR(ret));
  } else {
    time_sync_rpc_ = new (time_sync_rpc_) ObLCLTimeSyncRpc();
    time_sync_rpc_->init();
  }
  client_rand_per_for_test = get_rand_between_one_and_zero_();
  obrpc::ObDeadLockTimeSyncMessageP::ob_server_start_time = get_lcl_real_local_time();
  DETECT_LOG(INFO, "init thread rand_per", K(client_rand_per_for_test));
  if (OB_FAIL(share::ObThreadPool::init())) {
    DETECT_LOG(WARN, "init thread failed", K(ret), KP(this), K(MTL_ID()));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLCLTimeSyncThread::get_global_time_from_rs_leader_(
    const ObAddr &rs_leader_addr, const ObDeadLockTimeSyncArg &arg,
    ObDeadLockTimeSyncResp &resp, uint64_t &client_send_request_time,
    uint64_t &client_recv_response_time) {
  int ret = OB_SUCCESS;
  client_send_request_time = get_lcl_local_time();
  if (OB_FAIL(time_sync_rpc_->update_local_time_message(rs_leader_addr, arg,
                                                        resp))) {
    DETECT_LOG(WARN, "update local time failed", KR(ret), K(rs_leader_addr));
  }
  client_recv_response_time = get_lcl_local_time();
  return ret;
}

bool ObLCLTimeSyncThread::resp_not_in_resonable_time_(
    uint64_t leader_current_time, uint64_t client_send_request_time,
    uint64_t client_recv_response_time, uint64_t &network_delay) {
  network_delay = (client_recv_response_time - client_send_request_time) / 2;
  bool res = network_delay >= 2 * max_network_delay_;
  max_network_delay_ = max(static_cast<int>(max_network_delay_ * NETWORK_DELTA_RATIO_ + (1 - NETWORK_DELTA_RATIO_)) * network_delay, MIN_NETWORK_DELAY_);
  return res;
}

bool ObLCLTimeSyncThread::time_in_expected_range_(
    uint64_t delay_offset, uint64_t leader_current_time,
    uint64_t client_recv_response_time) {
  bool new_time_in_expected_delay_range =
      leader_current_time - delay_offset >=
          client_recv_response_time - expect_delay_offset_ &&
      leader_current_time + delay_offset <=
          client_recv_response_time + expect_delay_offset_;
  return new_time_in_expected_delay_range;
}

void ObLCLTimeSyncThread::set_lcl_local_time_offset_and_last_update_time_(
    uint64_t delay_offset, uint64_t leader_current_time,
    uint64_t client_recv_response_time) {
  uint64_t now_lcl_sync_real_time = leader_current_time + delay_offset;
  uint64_t local_real_time = ObLCLTimeSyncThread::get_lcl_real_local_time();
  uint64_t last_local_offset = local_offset_;
  ATOMIC_STORE(&local_offset_, now_lcl_sync_real_time - local_real_time);
  ATOMIC_STORE(&last_update_time_, now_lcl_sync_real_time);
  report_update_lcl_time_info_to_inner_table_(local_real_time, local_offset_, last_local_offset,
                                              auto_fit_update_time);
  DETECT_LOG(INFO, "update local time success: set local time offset",
             K(now_lcl_sync_real_time), K(delay_offset), K(local_offset_),
             K(local_real_time));
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
  int64_t lcl_time_client_offset =
      ObServerConfig::get_instance()._lcl_time_client_offset;
  return static_cast<int64_t>(ts.tv_sec) * 1000000L +
         static_cast<int64_t>(ts.tv_nsec / 1000L) +
         (lcl_time_client_offset * client_rand_per_for_test);
}

int64_t ObLCLTimeSyncThread::get_lcl_local_time() {
  return get_lcl_local_offset() + get_lcl_real_local_time();
}

int64_t ObLCLTimeSyncThread::get_lcl_local_offset() { return ATOMIC_LOAD(&local_offset_); }

void ObLCLTimeSyncThread::report_update_lcl_time_info_to_inner_table_(
    int64_t local_time, int64_t local_offset, int64_t last_local_offset,int64_t update_interval) {
  int ret = OB_SUCCESS;
  if (abs(local_offset - last_local_offset) > 10 * 1000) {
    if (OB_FAIL(SERVER_EVENT_SYNC_ADD(
            "DEADLOCK", "TIME_SYNC", "LOCAL_REAL_TIME", local_time,
            "LOCAL_OFFSET", local_offset, "LAST_LOCAL_OFFSET", last_local_offset,"UPDATE_INTERVAL", update_interval))) {

    } else {
    }
  }
};
ObLCLTimeSyncThread& ObLCLTimeSyncThread::get_time_sync_thread_instance() {
  static ObLCLTimeSyncThread time_sync_thread(100 * 1000);
  return time_sync_thread;
}

} // namespace detector
} // namespace share
} // namespace oceanbase