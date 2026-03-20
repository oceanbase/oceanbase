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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_TASK_QUEUE_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_TASK_QUEUE_H_

#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "share/scn.h"
#include "logservice/palf/fixed_sliding_window.h"
#include "logservice/palf/lsn.h"
#include "logservice/transportservice/ob_log_transport_rpc_define.h"
#include "logservice/transportservice/ob_log_transport_task_owner_state.h"
#include "lib/atomic/atomic128.h"
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
#include <functional>
#endif

namespace oceanbase
{
namespace common
{
class ObILogAllocator;
}
namespace logservice
{

class ObLogRestoreHandler;

// TODO by qingxia: move queue into tansport worker
class ObLogTransportTaskQueue : public palf::ISlidingCallBack
{
public:
  static const int64_t MAX_QUEUE_SIZE = (1 << 11); // 2048
  static const int64_t DEFAULT_MAX_CACHED_BYTES = palf::FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE;

  ObLogTransportTaskQueue();
  ~ObLogTransportTaskQueue();

  int init(common::ObILogAllocator *alloc_mgr,
           ObLogRestoreHandler *restore_handler,
           const int64_t queue_size);
  void stop();
  void destroy();
  void clear();

  int push(ObLogTransportTaskHolder task_holder, const int64_t end_log_id);
  int process(const int64_t proposal_id,
              const int64_t end_log_id,
              int64_t &processed,
              const int64_t batch_size);

  void get_drop_stats(int64_t &drop_stale,
                      int64_t &drop_duplicate,
                      int64_t &reject_out_of_window,
                      int64_t &parse_fail) const;
  void get_drop_stats(int64_t &drop_stale,
                      int64_t &drop_duplicate,
                      int64_t &reject_out_of_window,
                      int64_t &parse_fail,
                      int64_t &early_drop_far_lsn,
                      int64_t &evict_trigger,
                      int64_t &evict_scan_slots,
                      int64_t &evicted_slot_cnt,
                      int64_t &evicted_task_bytes) const;
  void inc_early_drop_far_lsn();

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  typedef std::function<int(const int64_t,
                            const palf::LSN &,
                            const share::SCN &,
                            const char *,
                            const int64_t)> RawWriteTestHook;
  void set_raw_write_test_hook(const RawWriteTestHook &hook) { raw_write_test_hook_ = hook; }
  void reset_raw_write_test_hook() { raw_write_test_hook_ = nullptr; }
#endif

  TO_STRING_KV(K_(is_inited),
               K_(is_stopped),
               KP_(restore_handler),
               KP_(alloc_mgr),
               "sw_begin_sn", sw_.get_begin_sn(),
               "sw_end_sn", sw_.get_end_sn(),
               "first_slot", get_first_slot_info_(),
               K_(queue_size),
               K_(cached_bytes),
               K_(max_cached_bytes),
               K_(drop_stale_cnt),
               K_(drop_duplicate_cnt),
               K_(reject_out_of_window_cnt),
               K_(parse_fail_cnt),
               K_(early_drop_far_lsn_cnt),
               K_(evict_trigger_cnt),
               K_(evict_scan_slots),
               K_(evicted_slot_cnt),
               K_(evicted_task_bytes));

private:
  class TransportTaskSlot : public palf::FixedSlidingWindowSlot
  {
  public:
    TransportTaskSlot();
    ~TransportTaskSlot() override;
    bool can_be_slid() override;
    void reset() override;
    bool is_valid() const;
    ObLogTransportTaskHolder task_holder_;
    int64_t task_bytes_;
    int64_t *cached_bytes_ptr_;
    bool is_submitted_;
  };

  struct SwRevertGuard
  {
    SwRevertGuard();
    SwRevertGuard(palf::FixedSlidingWindow<TransportTaskSlot> &sw,
                  const int64_t log_id,
                  TransportTaskSlot *slot);
    SwRevertGuard(const SwRevertGuard &) = delete;
    SwRevertGuard &operator=(const SwRevertGuard &) = delete;
    SwRevertGuard(SwRevertGuard &&other);
    SwRevertGuard &operator=(SwRevertGuard &&other);
    ~SwRevertGuard();

    bool is_valid() const
    {
      return nullptr != sw_;
    }

    TransportTaskSlot *get_slot() const
    {
      return slot_;
    }

    void reset();

    palf::FixedSlidingWindow<TransportTaskSlot> *sw_;
    TransportTaskSlot *slot_;
    int64_t log_id_;
  };

  int sliding_cb(const int64_t sn, const palf::FixedSlidingWindowSlot *data) override;
  int submit_tasks_sequentially_(const int64_t proposal_id, const int64_t batch_size, int64_t &processed);

  int parse_group_entry_log_id_(const char *buf, const int64_t buf_size, int64_t &log_id) const;
  int get_slot_guard_for_push_(const int64_t log_id, SwRevertGuard &revert_guard);
  int try_init_or_advance_base_log_id_(const int64_t end_log_id);
  int self_heal_();
  void reserve_bytes_(const int64_t bytes);
  void release_bytes_(const int64_t bytes);
  int raw_write_(const int64_t proposal_id,
                 const palf::LSN &lsn,
                 const share::SCN &scn,
                 const char *buf,
                 const int64_t buf_size);

  struct FirstSlotInfo
  {
    bool has_task_ = false;
    const void *task_ptr_ = nullptr;
    int64_t task_bytes_ = 0;
    bool is_submitted_ = false;
    TO_STRING_KV(K(has_task_), KP(task_ptr_), K(task_bytes_), K(is_submitted_));
  };
  FirstSlotInfo get_first_slot_info_() const;

private:
  palf::FixedSlidingWindow<TransportTaskSlot> sw_;
  ObLogRestoreHandler *restore_handler_;
  bool is_inited_;
  int64_t is_stopped_;
  common::ObILogAllocator *alloc_mgr_;
  int64_t queue_size_;
  int64_t cached_bytes_;
  int64_t max_cached_bytes_;
  common::ObSpinLock process_lock_;
  mutable common::SpinRWLock lock_;
  common::ObSpinLock self_heal_lock_;

  // stats
  int64_t drop_stale_cnt_;
  int64_t drop_duplicate_cnt_;
  int64_t reject_out_of_window_cnt_;
  int64_t parse_fail_cnt_;
  int64_t early_drop_far_lsn_cnt_;
  int64_t evict_trigger_cnt_;
  int64_t evict_scan_slots_;
  int64_t evicted_slot_cnt_;
  int64_t evicted_task_bytes_;
  int64_t queue_stats_print_time_us_;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  RawWriteTestHook raw_write_test_hook_;
#endif
};

static_assert((ObLogTransportTaskQueue::MAX_QUEUE_SIZE & (ObLogTransportTaskQueue::MAX_QUEUE_SIZE - 1)) == 0,
              "MAX_QUEUE_SIZE must be power of two");

} // namespace logservice
} // namespace oceanbase

#endif /* OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_TASK_QUEUE_H_ */
