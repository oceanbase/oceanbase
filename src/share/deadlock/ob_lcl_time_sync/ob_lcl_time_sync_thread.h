#ifndef OCEANBASE_SHARE_DEADLOCK_OB_LCL_TIME_SYNC_THREAD_H
#define OCEANBASE_SHARE_DEADLOCK_OB_LCL_TIME_SYNC_THREAD_H

#include "ob_lcl_time_sync_message.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/net/ob_addr.h"
#include "share/ob_thread_pool.h"
#include "ob_lcl_time_sync_rpc.h"
#include <ctime>
#include <random>

namespace oceanbase {
namespace share {
namespace detector {
class ObDeadLockDetectorMgr;

class ObLCLTimeSyncThread : public share::ObThreadPool {
public:
  ObLCLTimeSyncThread(int64_t expectDelayOffset)
      : is_inited_(false), is_running_(false),
        expect_delay_offset_(expectDelayOffset) {}
  ~ObLCLTimeSyncThread() { destroy(); }
  int init();
  int start();
  void destroy();
  void stop();
  void wait();
  void run1();
  void try_update_local_time_from_rs_leader_now();
  static int64_t get_lcl_local_time();
  static int64_t get_lcl_real_local_time();
  static int64_t get_lcl_local_offset();

private:
  void update_local_time_from_rs_leader_periodically_();
  void change_lcl_update_time_interval_(const ObAddr &rs_leader_addr, int64_t leader_current_us, int64_t network_delay, int64_t client_recv_response_time);
  int update_local_time_from_rs_leader_(bool change_update_local_time_interval);
  int get_rs_leader_addr_(ObAddr &rs_leader_addr);
  int get_global_time_from_rs_leader_(const ObAddr &rs_leader_addr,
                                     const ObDeadLockTimeSyncArg &arg,
                                     ObDeadLockTimeSyncResp &resp,
                                     uint64_t &client_send_request_time,
                                     uint64_t &client_recv_response_time);
  bool time_in_expected_range_(uint64_t delay_offset, uint64_t leader_current_us,
                              uint64_t client_recv_response_time);
  bool resp_not_in_resonable_time_(uint64_t leader_current_us,
                                  uint64_t client_send_request_time,
                                  uint64_t client_recv_response_time,
                                  uint64_t &network_delay);
  void set_lcl_local_time_offset_and_last_update_time_(
      uint64_t delay_offset, uint64_t leader_current_us,
      uint64_t client_recv_response_time);

  double get_rand_between_one_and_zero_();

  void report_update_lcl_time_info_to_inner_table_(int64_t update_interval, int64_t local_time, int64_t local_offset);

private:
  bool is_inited_;
  bool is_running_;
  static int64_t local_offset_;
  int64_t last_update_time_;
  int64_t auto_fit_update_time = 10 * 1000 * 1000;
  
  int64_t expect_delay_offset_;
  static double client_rand_per_for_test;
  ObLCLTimeSyncRpc *time_sync_rpc_;
  ObAddr last_rs_leader_addr_;
  static constexpr uint64_t INTERVAL_CHANGE_UNIT = 1000 * 1000 * 10; // 10s
  static constexpr uint64_t UPDATE_TIME_FROM_CLIENT_MIN_INTERVAL =
      1000 * 1000 * 10;                                     // 10s
  static constexpr uint64_t UPDATE_TIME_FROM_CLIENT_MAX_INTERVAL =
      1000 * 1000 * 60 * 10; // 10min
  static constexpr uint64_t MAX_NETWORK_DELAY = 100 * 1000; // 100ms
};

} // namespace detector
} // namespace share
} // namespace oceanbase
#endif