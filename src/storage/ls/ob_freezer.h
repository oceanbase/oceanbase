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

#ifndef OCEABASE_STORAGE_FREEZER_
#define OCEABASE_STORAGE_FREEZER_

#include "lib/utility/utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string_holder.h"
#include "lib/container/ob_array.h"
#include "share/ob_ls_id.h"
#include "storage/tx/wrs/ob_ls_wrs_handler.h"
#include "storage/checkpoint/ob_freeze_checkpoint.h"
#include "logservice/ob_log_handler.h"
#include "lib/container/ob_array_serialization.h"
#include "share/ob_occam_thread_pool.h"

namespace oceanbase
{
namespace common
{
class ObTabletID;
};
namespace logservice
{
class ObILogHandler;
};
namespace memtable
{
class ObMemtable;
}
namespace storage
{
class ObIMemtable;
class ObLSTxService;
class ObLSTabletService;
class ObTablet;
class ObLSWRSHandler;
class ObTableHandleV2;
struct AsyncFreezeFunctor;
namespace checkpoint
{
class ObDataCheckpoint;
}


class ObFreezeState
{
public:
  static const int64_t INVALID = -1;
  static const int64_t NOT_SET_FREEZE_FLAG = 0;
  static const int64_t NOT_SUBMIT_LOG = 1;
  static const int64_t WAIT_READY_FOR_FLUSH = 2;
  static const int64_t FINISH = 3;
};

class ObFrozenMemtableInfo
{
public:
  ObFrozenMemtableInfo();
  ObFrozenMemtableInfo(const ObTabletID &tablet_id,
                       const share::SCN &start_scn_,
                       const share::SCN &end_scn,
                       const int64_t write_ref_cnt,
                       const int64_t unsubmitted_cnt,
                       const int64_t current_right_boundary);
  ~ObFrozenMemtableInfo();

  void reset();
  void set(const ObTabletID &tablet_id,
           const share::SCN &start_scn,
           const share::SCN &end_scn,
           const int64_t write_ref_cnt,
           const int64_t unsubmitted_cnt,
           const int64_t current_right_boundary);
  bool is_valid();

public:
  ObTabletID tablet_id_;
  share::SCN start_scn_;
  share::SCN end_scn_;
  int64_t write_ref_cnt_;
  int64_t unsubmitted_cnt_;
  int64_t current_right_boundary_;
  TO_STRING_KV(K_(tablet_id), K_(start_scn), K_(end_scn), K_(write_ref_cnt),
               K_(unsubmitted_cnt), K_(current_right_boundary));
};

class ObFreezerStat
{
public:
  static const int64_t FROZEN_MEMTABLE_INFO_CNT = 1;

public:
  ObFreezerStat();
  ~ObFreezerStat();

  void reset();
  bool is_valid();

public:
  int add_memtable_info(const ObTabletID &tablet_id,
                        const share::SCN &start_scn,
                        const share::SCN &end_scn,
                        const int64_t write_ref_cnt,
                        const int64_t unsubmitted_cnt,
                        const int64_t current_right_boundary);
  int remove_memtable_info(const ObTabletID &tablet_id);
  int get_memtables_info(common::ObSArray<ObFrozenMemtableInfo> &memtables_info);
  int set_memtables_info(const common::ObSArray<ObFrozenMemtableInfo> &memtables_info);
  int add_diagnose_info(const ObString &str);
  int get_diagnose_info(ObStringHolder &diagnose_info);
  void set_tablet_id(const ObTabletID &tablet_id);
  ObTabletID get_tablet_id();
  void set_need_rewrite_meta(bool need_rewrite_meta);
  bool need_rewrite_meta();
  void set_state(int state);
  int get_state();
  void set_freeze_clock(const uint32_t freeze_clock);
  uint32_t get_freeze_clock();
  void set_start_time(int64_t start_time);
  int64_t get_start_time();
  void set_end_time(int64_t end_time);
  int64_t get_end_time();
  void set_ret_code(int ret_code);
  int get_ret_code();
  void set_freeze_snapshot_version(const share::SCN &freeze_snapshot_version);
  share::SCN get_freeze_snapshot_version();
  int deep_copy_to(ObFreezerStat &other);
  int begin_set_freeze_stat(const uint32_t freeze_clock,
                            const int64_t start_time,
                            const int state,
                            const share::SCN &freeze_snapshot_version,
                            const ObTabletID &tablet_id,
                            const bool need_rewrite_meta);
  int end_set_freeze_stat(const int state,
                          const int64_t end_time,
                          const int ret_code);

private:
  ObTabletID tablet_id_;
  bool need_rewrite_meta_;
  int state_;
  uint32_t freeze_clock_;
  int64_t start_time_;
  int64_t end_time_;
  int ret_code_;
  share::SCN freeze_snapshot_version_;
  ObStringHolder diagnose_info_;
  common::ObSArray<ObFrozenMemtableInfo> memtables_info_;
  ObSpinLock lock_;
};

struct AsyncFreezeTabletInfo {
  ObTabletID tablet_id_;
  int64_t epoch_;
  int64_t hash() const { return tablet_id_.id(); }

  AsyncFreezeTabletInfo() : tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)), epoch_(-1) {}

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  bool operator==(const AsyncFreezeTabletInfo &rhs) const
  {
    return (tablet_id_ == rhs.tablet_id_) && (epoch_ == rhs.epoch_);
  }

  TO_STRING_KV(K(tablet_id_), K(epoch_));
};

class ObFreezer
{
public:
  friend AsyncFreezeFunctor;
  static const int64_t SYNC_FREEZE_DEFAULT_RETRY_TIME = 10LL * 1000LL * 1000LL; // 10 senconds

public:
  ObFreezer();
  ObFreezer(ObLS *ls);
  ~ObFreezer();

  int init(ObLS *ls);
  void reset();
  void offline() { enable_ = false; }
  void online() { enable_ = true; }

public:
  /********************** freeze **********************/
  int logstream_freeze(const int64_t trace_id);
  int wait_ls_freeze_finish();
  int wait_tablet_freeze_finish(ObIArray<ObTableHandleV2> &frozen_memtable_handles,
                                ObIArray<ObTabletID> &freeze_failed_tablets);
  int ls_inner_tablet_freeze(const ObTabletID &tablet_id);
  int tablet_freeze(const int64_t trace_id,
                    const ObIArray<ObTabletID> &tablet_ids,
                    const bool need_rewrite_meta,
                    ObIArray<ObTableHandleV2> &frozen_memtable_handles,
                    ObIArray<ObTabletID> &freeze_failed_tablets);
  int get_all_async_freeze_tablets(const int64_t ls_epoch, ObIArray<ObTabletID> &tablet_ids);
  bool is_async_freeze_tablets_empty() const { return async_freeze_tablets_.empty(); }
  bool is_async_tablet_freeze_task_running() { return ATOMIC_LOAD(&is_async_tablet_freeze_task_running_); }
  bool set_async_tablet_freeze_task_start_succ() { return ATOMIC_BCAS(&is_async_tablet_freeze_task_running_, false, true); }
  void set_async_freeze_task_stop() { ATOMIC_STORE(&is_async_tablet_freeze_task_running_, false); }
  void record_async_freeze_tablet(const AsyncFreezeTabletInfo &async_freeze_tablet_info);
  void erase_async_freeze_tablet(const AsyncFreezeTabletInfo &async_freeze_tablet_info);
  void commit_an_async_freeze_task(const int64_t trace_id, const bool is_ls_freeze);
  void async_tablet_freeze_consumer(const int64_t trace_id);
  common::hash::ObHashSet<AsyncFreezeTabletInfo> &get_async_freeze_tablets() { return async_freeze_tablets_; }
  /********************** freeze **********************/

  /* freeze_flag */
  bool is_freeze(uint32_t freeze_flag) const;
  bool is_ls_freeze_running() const {  return 0 < ATOMIC_LOAD(&high_priority_freeze_cnt_); }
  uint32_t get_freeze_flag() const { return ATOMIC_LOAD(&freeze_flag_); };
  uint32_t get_freeze_clock() { return ATOMIC_LOAD(&freeze_flag_) & (~(1 << 31)); }

  /* ls info */
  share::ObLSID get_ls_id();
  checkpoint::ObDataCheckpoint *get_ls_data_checkpoint();
  ObLSTxService *get_ls_tx_svr();
  ObLSTabletService *get_ls_tablet_svr();
  logservice::ObILogHandler *get_ls_log_handler();
  ObLSWRSHandler *get_ls_wrs_handler();

  /* freeze_snapshot_version */
  share::SCN get_freeze_snapshot_version() { return freeze_snapshot_version_; }

  /* max_decided_scn */
  share::SCN get_max_decided_scn() { return max_decided_scn_; }

  /* statistics*/
  void inc_empty_memtable_cnt();
  void clear_empty_memtable_cnt();
  int64_t get_empty_memtable_cnt();
  void print_freezer_statistics();

  /* others */
  bool need_resubmit_log() { return ATOMIC_LOAD(&need_resubmit_log_); }
  void set_throttle_is_skipping() { throttle_is_skipping_ = true; }
  void unset_throttle_is_skipping() { throttle_is_skipping_ = false; }
  bool throttle_is_skipping() { return throttle_is_skipping_; }
  void set_tenant_replay_is_pending() { tenant_replay_is_pending_ = true; }
  void unset_tenant_replay_is_pending() { tenant_replay_is_pending_ = false; }
  bool tenant_replay_is_pending() const { return tenant_replay_is_pending_; }
  // get consequent callbacked log_ts right boundary
  virtual int get_max_consequent_callbacked_scn(share::SCN &max_consequent_callbacked_scn);
  // to set snapshot version when memtables meet ready_for_flush
  int get_ls_weak_read_scn(share::SCN &weak_read_scn);
  int decide_max_decided_scn(share::SCN &max_decided_scn);
  // to resolve concurrency problems about multi-version tablet
  int get_newest_clog_checkpoint_scn(const ObTabletID &tablet_id,
                                    share::SCN &clog_checkpoint_scn);
  int get_newest_snapshot_version(const ObTabletID &tablet_id,
                                  share::SCN &snapshot_version);
  ObFreezerStat& get_stat() { return stat_; }
  void set_need_resubmit_log(bool flag) { return ATOMIC_STORE(&need_resubmit_log_, flag); }
  int pend_ls_replay();
  int restore_ls_replay();

private:
  class ObLSFreezeGuard
  {
  public:
    ObLSFreezeGuard(ObFreezer &parent);
    ~ObLSFreezeGuard();
  private:
    ObFreezer &parent_;
  };
  class ObTabletFreezeGuard
  {
  public:
    ObTabletFreezeGuard(ObFreezer &parent, const bool try_guard);
    ~ObTabletFreezeGuard();
    int try_set_tablet_freeze_begin();
  private:
    bool need_release_;
    ObFreezer &parent_;
  };
  class PendTenantReplayHelper {
  public:
    PendTenantReplayHelper(ObFreezer &host, ObLS *current_freeze_ls)
        : host_(host), current_freeze_ls_(current_freeze_ls) {}
    ~PendTenantReplayHelper() { reset_pend_status_(); }
    void set_skip_throttle_flag();
    void check_pend_condition_once();
  private:
    bool current_ls_is_leader_();
    bool remain_memory_is_exhausting_();
    void pend_tenant_replay_();
    void restore_tenant_replay_();
    void reset_pend_status_()
    {
      (void)host_.unset_throttle_is_skipping();
      (void)restore_tenant_replay_();
    }
  private:
    ObFreezer &host_;
    ObLS *current_freeze_ls_;
    ObSEArray<ObLSHandle, 16> ls_handle_array_;
  };

private:
  /* freeze_flag */
  int set_freeze_flag();
  int set_freeze_flag_without_inc_freeze_clock();
  int loop_set_freeze_flag(const int64_t max_loop_time);
  void unset_freeze_();
  void try_freeze_tx_data_();

  /* inner subfunctions for freeze process */
  int submit_log_for_freeze(const bool is_tablet_freeze, const bool is_try);
  void submit_log_if_needed_(ObIArray<ObTableHandleV2> &frozen_memtable_handles);
  void try_submit_log_for_freeze_(const bool is_tablet_freeze);
  void resubmit_log_if_needed_(const int64_t start_time,
                               const bool is_tablet_freeze,
                               const bool is_try,
                               const ObITabletMemtable *freeze_memtable = nullptr /* used for tablet freeze */);
  int wait_data_memtable_freeze_finish_(ObITabletMemtable *tablet_memtable);
  int wait_direct_load_memtable_freeze_finish_(ObITabletMemtable *tablet_memtable);
  int set_tablet_freeze_flag_(const int64_t trace_id,
                              const ObTabletID tablet_id,
                              const bool need_rewrite_meta,
                              const SCN freeze_snapshot_version,
                              ObIArray<ObTableHandleV2> &frozen_memtable_handles);
  int handle_no_active_memtable_(const ObTabletID &tablet_id,
                                 const ObTablet *tablet,
                                 share::SCN freeze_snapshot_version);
  int decide_real_snapshot_version_(const ObTabletID &tablet_id,
                                    const ObTablet *tablet,
                                    const SCN freeze_snapshot_version,
                                    SCN &real_snapshot_version);
  void handle_set_tablet_freeze_failed(const bool need_rewrite_meta,
                                       const ObTabletID &tablet_id,
                                       const ObLSID &ls_id,
                                       const ObTablet *tablet,
                                       const share::SCN freeze_snapshot_version,
                                       int &ret);
  void init_tablet_freeze_param_(const ObIArray<ObTabletID> &tablet_ids,
                                 const bool need_rewrite_meta,
                                 int64_t &max_loop_time,
                                 ObTabletID &record_tablet_id,
                                 bool &try_guard);
  int submit_wait_freeze_finish_task_(const bool is_ls_freeze, ObFuture<int> *result, ObTableHandleV2 &handle);
  int wait_memtable_ready_for_flush_(ObITabletMemtable *tablet_memtable);
  int try_set_tablet_freeze_begin_();
  void set_tablet_freeze_begin_();
  void set_tablet_freeze_end_() { ATOMIC_DEC(&low_priority_freeze_cnt_); }
  void set_ls_freeze_begin_();
  void set_ls_freeze_end_() { ATOMIC_DEC(&high_priority_freeze_cnt_); }
  int check_ls_state(); // must be used under the protection of ls_lock
  int tablet_freeze_(const int64_t trace_id,
                     const ObIArray<ObTabletID> &tablet_ids,
                     const bool need_rewrite_meta,
                     const share::SCN freeze_snapshot_version,
                     ObIArray<ObTableHandleV2> &frozen_memtable_handles,
                     ObIArray<ObTabletID> &freeze_failed_tablets);
  int inner_wait_memtable_freeze_finish_(ObTableHandleV2 &memtable_handle);
  void submit_checkpoint_task();
private:
  // flag whether the logsteram is freezing
  // the first bit: 1, freeze; 0, not freeze
  // the last 31 bits: logstream freeze clock, inc the clock every freeze process
  uint32_t freeze_flag_;
  // weak read timestamp saved for memtable, which means the version before
  // which all transaction has been saved into the memtable and the memtables
  // before it.
  share::SCN freeze_snapshot_version_;
  // max_decided_scn saved for memtable when freeze happen, which means the
  // log ts before which will be smaller than the log ts in the latter memtables
  share::SCN max_decided_scn_;

  ObLS *ls_;
  ObFreezerStat stat_;
  int64_t empty_memtable_cnt_;

  // make sure ls freeze has higher priority than tablet freeze
  int64_t high_priority_freeze_cnt_; // waiting and freeze cnt
  int64_t low_priority_freeze_cnt_; // freeze tablet cnt
  int64_t pend_replay_cnt_;
  common::ObByteLock byte_lock_; // only used to control pend_replay_cnt_
  bool need_resubmit_log_;
  bool enable_;                     // whether we can do freeze now
  bool is_inited_;
  bool is_async_tablet_freeze_task_running_;
  bool throttle_is_skipping_;
  bool tenant_replay_is_pending_;
  common::hash::ObHashSet<AsyncFreezeTabletInfo> async_freeze_tablets_;
};

} // namespace storage
} // namespace oceanbase
#endif
