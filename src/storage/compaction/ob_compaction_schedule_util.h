/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_COMPACTION_COMPACTION_SCHEDULER_UTIL_H_
#define OB_STORAGE_COMPACTION_COMPACTION_SCHEDULER_UTIL_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/literals/ob_literals.h"
#include "share/compaction/ob_compaction_time_guard.h"
#include "share/ob_zone_merge_info.h"
#include "storage/compaction/ob_tenant_status_cache.h"

namespace oceanbase
{
namespace compaction
{

enum ObCompactionScheduleMode : uint8_t
{
  COMPACTION_NORMAL_MODE = 0, // do both schedule new round medium for leader, and schedule merge dag execute
  COMPACTION_WINDOW_MODE = 1, // do both add tablet score into priority queue or candidate list, and schedule merge dag execute, used while medium loop in window compaction
  COMPACTION_MAX_MODE
};

inline bool is_window_compaction_mode(const ObCompactionScheduleMode mode) { return COMPACTION_WINDOW_MODE == mode; }
inline bool is_normal_compaction_mode(const ObCompactionScheduleMode mode) { return COMPACTION_NORMAL_MODE == mode; }

struct ObScheduleStatistics
{
public:
  ObScheduleStatistics() { reset(); }
  ~ObScheduleStatistics() {}
  OB_INLINE void reset()
  {
    all_ls_weak_read_ts_ready_ = false;
    add_weak_read_ts_event_flag_ = false;
    check_weak_read_ts_cnt_ = 0;
    start_timestamp_ = 0;
  }
  OB_INLINE void start_merge()
  {
    all_ls_weak_read_ts_ready_ = false;
    add_weak_read_ts_event_flag_ = true;
    check_weak_read_ts_cnt_ = 0;
    start_timestamp_ = ObTimeUtility::fast_current_time();
  }
  TO_STRING_KV(K_(all_ls_weak_read_ts_ready), K_(start_timestamp));

  bool all_ls_weak_read_ts_ready_;
  bool add_weak_read_ts_event_flag_;
  int64_t check_weak_read_ts_cnt_;
  int64_t start_timestamp_;
};

struct ObScheduleTabletCnt
{
  ObScheduleTabletCnt() { reset(); }
  ~ObScheduleTabletCnt() = default;
  void reset()
  {
    schedule_dag_cnt_ = 0;
    submit_clog_cnt_ = 0;
    finish_cnt_ = 0;
    loop_tablet_cnt_ = 0;
    force_freeze_cnt_ = 0;
    wait_rs_validate_cnt_ = 0;
  }
  TO_STRING_KV(K_(schedule_dag_cnt), K_(submit_clog_cnt), K_(finish_cnt),
               K_(loop_tablet_cnt), K_(force_freeze_cnt),
               K_(wait_rs_validate_cnt));
  int64_t schedule_dag_cnt_;
  int64_t submit_clog_cnt_;
  int64_t finish_cnt_;
  int64_t loop_tablet_cnt_;
  int64_t force_freeze_cnt_;
  int64_t wait_rs_validate_cnt_;
};

struct ObMergeInfo final // For atomic store and load
{
public:
  ObMergeInfo()
    : merge_mode_(share::ObGlobalMergeInfo::MERGE_MODE_TENANT),
      merge_status_(share::ObZoneMergeInfo::MERGE_STATUS_IDLE),
      merge_start_time_(0)
  {}
  virtual ~ObMergeInfo() { reset(); }
  void reset()
  {
    merge_mode_ = share::ObGlobalMergeInfo::MERGE_MODE_TENANT;
    merge_status_ = share::ObZoneMergeInfo::MERGE_STATUS_IDLE;
    merge_start_time_ = 0;
  }
  bool is_global_during_window_compaction() const
  {
    return share::ObGlobalMergeInfo::MERGE_MODE_WINDOW == merge_mode_
        && share::ObZoneMergeInfo::MERGE_STATUS_MERGING == merge_status_;
  }
  TO_STRING_KV(K_(merge_mode), K_(merge_status), K_(merge_start_time));
public:
  share::ObGlobalMergeInfo::MergeMode merge_mode_;
  share::ObZoneMergeInfo::MergeStatus merge_status_;
  int64_t merge_start_time_;
};

class ObBasicMergeScheduler
{
public:
  ObBasicMergeScheduler();
  virtual ~ObBasicMergeScheduler();
  void reset();
  static ObBasicMergeScheduler *get_merge_scheduler();
  static bool could_start_loop_task();
  // major merge status control
  OB_INLINE bool could_major_merge_start() const { return !is_stop_ && ATOMIC_LOAD(&major_merge_status_); }
  void stop_major_merge();
  void resume_major_merge();
  void set_inner_table_merged_scn(const int64_t merged_scn) { ATOMIC_STORE(&inner_table_merged_scn_, merged_scn); }
  int64_t get_inner_table_merged_scn() const { return ATOMIC_LOAD(&inner_table_merged_scn_); }
  int64_t get_frozen_version() const;
  bool is_compacting() const;
  int get_min_data_version(uint64_t &min_data_version) { return tenant_status_.get_min_data_version(min_data_version); }
  int get_window_schema_version(int64_t &window_schema_version) { return tenant_status_.get_window_schema_version(window_schema_version); }
  int during_restore(bool &during_restore) { return tenant_status_.during_restore(during_restore); }
  virtual int schedule_merge(const int64_t broadcast_version) = 0;
  void update_merged_version(const int64_t merged_version);
  int update_merge_info(const share::ObGlobalMergeInfo::MergeMode merge_mode, const share::ObZoneMergeInfo::MergeStatus merge_status, const int64_t merge_start_time);
  int64_t get_merged_version() const { return merged_version_; }
  bool enable_adaptive_compaction() const { return tenant_status_.enable_adaptive_compaction(); }
  bool enable_adaptive_merge_schedule() const { return tenant_status_.enable_adaptive_merge_schedule(); }
  bool is_global_during_window_compaction() const { return merge_info_.is_global_during_window_compaction(); }
  const ObTenantStatusCache &get_tenant_status() const { return tenant_status_; }
  static const int64_t INIT_COMPACTION_SCN = 1;
protected:
  void update_frozen_version_and_merge_progress(const int64_t broadcast_version);
  void try_finish_merge_progress(const int64_t merge_version);
protected:
  static const int64_t PRINT_SLOG_REPLAY_INVERVAL = 10_s;
  mutable obsys::ObRWLock<> frozen_version_lock_;
  int64_t frozen_version_;
  int64_t inner_table_merged_scn_;
  int64_t merged_version_; // the merged major version of the local server, may be not accurate after reboot
  ObTenantStatusCache tenant_status_;
  ObMergeInfo merge_info_;
  bool major_merge_status_;
  bool is_stop_;
};

#define MERGE_SCHEDULER_PTR (oceanbase::compaction::ObBasicMergeScheduler::get_merge_scheduler())

} // compaction
} // oceanbase

#endif // OB_STORAGE_COMPACTION_COMPACTION_SCHEDULER_UTIL_H_
