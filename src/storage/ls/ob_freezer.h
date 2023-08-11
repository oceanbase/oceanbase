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
class ObIMemtable;
class ObMemtable;
}
namespace storage
{
class ObLSTxService;
class ObLSTabletService;
class ObTablet;
class ObLSWRSHandler;
class ObTableHandleV2;
namespace checkpoint
{
class ObDataCheckpoint;
}

class ObReplaySubmitLogPendingWhenFreezeGuard
{
public:
  ObReplaySubmitLogPendingWhenFreezeGuard(logservice::ObILogHandler *loghandler,
                                          share::ObLSID &ls_id)
    : is_follower_(false),
      loghandler_(loghandler),
      ls_id_(ls_id) {
    int ret = OB_SUCCESS;
    int64_t proposal_id = 0;
    common::ObRole ls_role = common::ObRole::INVALID_ROLE;
    if (OB_FAIL(loghandler_->get_role(ls_role, proposal_id))) {
      STORAGE_LOG(WARN, "get ls role fail", K(ret), K(ls_id_));
    } else if (common::ObRole::FOLLOWER == ls_role) {
      is_follower_ = true;
    }

    if (is_follower_) {
      if (OB_ISNULL(loghandler_)) {
        STORAGE_LOG(WARN, "loghandler should not null", K(ls_id_));
      } else if (OB_FAIL(loghandler_->pend_submit_replay_log())) {
        STORAGE_LOG(ERROR, "pend_submit_replay_log failed", K(ls_id_));
      }
    }
  }

  ~ObReplaySubmitLogPendingWhenFreezeGuard() {
    int ret = OB_SUCCESS;
    if (is_follower_) {
      if (OB_ISNULL(loghandler_)) {
        STORAGE_LOG(WARN, "loghandler should not null", K(ls_id_));
      } else if (OB_FAIL(loghandler_->restore_submit_replay_log())) {
        STORAGE_LOG(ERROR, "restore_submit_replay_log failed", K(ls_id_));
      }
    }
  }

private:
  bool is_follower_;
  logservice::ObILogHandler *loghandler_;
  share::ObLSID ls_id_;
};

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
                       const int64_t unsynced_cnt,
                       const int64_t current_right_boundary);
  ~ObFrozenMemtableInfo();

  void reset();
  void set(const ObTabletID &tablet_id,
           const share::SCN &start_scn,
           const share::SCN &end_scn,
           const int64_t write_ref_cnt,
           const int64_t unsubmitted_cnt,
           const int64_t unsynced_cnt,
           const int64_t current_right_boundary);
  bool is_valid();

public:
  ObTabletID tablet_id_;
  share::SCN start_scn_;
  share::SCN end_scn_;
  int64_t write_ref_cnt_;
  int64_t unsubmitted_cnt_;
  int64_t unsynced_cnt_;
  int64_t current_right_boundary_;
  TO_STRING_KV(K_(tablet_id), K_(start_scn), K_(end_scn), K_(write_ref_cnt),
               K_(unsubmitted_cnt), K_(unsynced_cnt), K_(current_right_boundary));
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
                        const int64_t unsynced_cnt,
                        const int64_t current_right_boundary);
  int remove_memtable_info(const ObTabletID &tablet_id);
  int get_memtables_info(common::ObSArray<ObFrozenMemtableInfo> &memtables_info);
  int set_memtables_info(const common::ObSArray<ObFrozenMemtableInfo> &memtables_info);
  int add_diagnose_info(const ObString &str);
  int get_diagnose_info(ObStringHolder &diagnose_info);
  void set_tablet_id(const ObTabletID &tablet_id);
  ObTabletID get_tablet_id();
  void set_is_force(bool is_force);
  bool get_is_force();
  void set_state(int state);
  int get_state();
  void set_freeze_clock(const int64_t freeze_clock);
  int64_t get_freeze_clock();
  void set_start_time(int64_t start_time);
  int64_t get_start_time();
  void set_end_time(int64_t end_time);
  int64_t get_end_time();
  void set_ret_code(int ret_code);
  int get_ret_code();
  void set_freeze_snapshot_version(const share::SCN &freeze_snapshot_version);
  share::SCN get_freeze_snapshot_version();
  int deep_copy_to(ObFreezerStat &other);
  int begin_set_freeze_stat(const int64_t freeze_clock,
                            const int64_t start_time,
                            const int state,
                            const share::SCN &freeze_snapshot_version,
                            const ObTabletID &tablet_id,
                            const bool is_force);
  int end_set_freeze_stat(const int state,
                          const int64_t end_time,
                          const int ret_code);

private:
  ObTabletID tablet_id_;
  bool is_force_;
  int state_;
  int64_t freeze_clock_;
  int64_t start_time_;
  int64_t end_time_;
  int ret_code_;
  share::SCN freeze_snapshot_version_;
  ObStringHolder diagnose_info_;
  common::ObSArray<ObFrozenMemtableInfo> memtables_info_;
  ObSpinLock lock_;
};

class ObFreezer
{
public:
  static const int64_t MAX_WAIT_READY_FOR_FLUSH_TIME = 10 * 1000 * 1000; // 10_s
  typedef common::ObSEArray<ObTableHandleV2, OB_DEFAULT_TABLET_ID_COUNT> ObTableHandleArray;

public:
  ObFreezer();
  ObFreezer(ObLS *ls);
  ~ObFreezer();

  int init(ObLS *ls);
  void reset();
  void offline() { enable_ = false; }
  void online() { enable_ = true; }

public:
  /* freeze */
  int logstream_freeze(ObFuture<int> *result = nullptr);
  int tablet_freeze(const ObTabletID &tablet_id, ObFuture<int> *result = nullptr);
  int force_tablet_freeze(const ObTabletID &tablet_id);
  int tablet_freeze_for_replace_tablet_meta(const ObTabletID &tablet_id, ObTableHandleV2 &handle);
  int handle_frozen_memtable_for_replace_tablet_meta(const ObTabletID &tablet_id, ObTableHandleV2 &handle);
  int batch_tablet_freeze(const ObIArray<ObTabletID> &tablet_ids, ObFuture<int> *result = nullptr);

  /* freeze_flag */
  bool is_freeze(uint32_t is_freeze=UINT32_MAX) const;
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
  bool need_resubmit_log() { return ATOMIC_LOAD(&need_resubmit_log_); }
  void set_need_resubmit_log(bool flag) { return ATOMIC_STORE(&need_resubmit_log_, flag); }
  // only used after start freeze_task successfully
  int wait_freeze_finished(ObFuture<int> &result);

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
    ObTabletFreezeGuard(ObFreezer &parent, const bool try_guard = false);
    ~ObTabletFreezeGuard();
    int try_set_tablet_freeze_begin();
  private:
    bool need_release_;
    ObFreezer &parent_;
  };
private:
  /* freeze_flag */
  int set_freeze_flag();
  int set_freeze_flag_without_inc_freeze_clock();
  int loop_set_freeze_flag();
  int inc_freeze_clock();
  void unset_freeze_();
  void undo_freeze_();

  /* inner subfunctions for freeze process */
  int inner_logstream_freeze(ObFuture<int> *result);
  int submit_log_for_freeze(bool is_try);
  void try_submit_log_for_freeze_();
  int ls_freeze_task();
  int tablet_freeze_task(ObTableHandleV2 handle);
  int submit_freeze_task(const bool is_ls_freeze, ObFuture<int> *result, ObTableHandleV2 &handle);
  void wait_memtable_ready_for_flush(memtable::ObMemtable *memtable);
  int wait_memtable_ready_for_flush_with_ls_lock(memtable::ObMemtable *memtable);
  int handle_memtable_for_tablet_freeze(memtable::ObIMemtable *imemtable);
  int create_memtable_if_no_active_memtable(ObTablet *tablet);
  int try_set_tablet_freeze_begin_();
  void set_tablet_freeze_begin_();
  void set_tablet_freeze_end_();
  void set_ls_freeze_begin_();
  void set_ls_freeze_end_();
  int check_ls_state(); // must be used under the protection of ls_lock
  int freeze_normal_tablet_(const ObTabletID &tablet_id, ObFuture<int> *result = nullptr);
  int freeze_ls_inner_tablet_(const ObTabletID &tablet_id);
  int batch_tablet_freeze_(const ObIArray<ObTabletID> &tablet_ids, ObFuture<int> *result, bool &need_freeze);
  int submit_batch_tablet_freeze_task(const ObTableHandleArray &tables_array, ObFuture<int> *result);
  int batch_tablet_freeze_task(ObTableHandleArray tables_array);
  int finish_freeze_with_ls_lock(memtable::ObMemtable *memtable);
  int try_wait_memtable_ready_for_flush_with_ls_lock(memtable::ObMemtable *memtable, bool &ready_for_flush, const int64_t start);
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

  bool need_resubmit_log_;
  bool enable_;                     // whether we can do freeze now

  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
#endif
