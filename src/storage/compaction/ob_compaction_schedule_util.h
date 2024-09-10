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

namespace oceanbase
{
namespace compaction
{

struct ObScheduleStatistics
{
public:
  ObScheduleStatistics() { reset(); }
  ~ObScheduleStatistics() {}
  OB_INLINE void reset()
  {
    add_weak_read_ts_event_flag_ = false;
    check_weak_read_ts_cnt_ = 0;
    start_timestamp_ = 0;
    clear_tablet_cnt();
  }
  OB_INLINE void clear_tablet_cnt()
  {
    schedule_dag_cnt_ = 0;
    submit_clog_cnt_ = 0;
    finish_cnt_ = 0;
    wait_rs_validate_cnt_ = 0;
  }
  OB_INLINE void start_merge()
  {
    add_weak_read_ts_event_flag_ = true;
    check_weak_read_ts_cnt_ = 0;
    start_timestamp_ = ObTimeUtility::fast_current_time();
    clear_tablet_cnt();
  }
  TO_STRING_KV(K_(schedule_dag_cnt), K_(submit_clog_cnt), K_(finish_cnt), K_(wait_rs_validate_cnt));
  bool add_weak_read_ts_event_flag_;
  int64_t check_weak_read_ts_cnt_;
  int64_t start_timestamp_;
  int64_t schedule_dag_cnt_;
  int64_t submit_clog_cnt_;
  int64_t finish_cnt_;
  int64_t wait_rs_validate_cnt_;
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
  OB_INLINE bool could_major_merge_start() const { return ATOMIC_LOAD(&major_merge_status_); }
  void stop_major_merge();
  void resume_major_merge();
  void set_inner_table_merged_scn(const int64_t merged_scn) { ATOMIC_STORE(&inner_table_merged_scn_, merged_scn); }
  int64_t get_inner_table_merged_scn() const { return ATOMIC_LOAD(&inner_table_merged_scn_); }
  int64_t get_frozen_version() const;
  bool is_compacting() const;
  int get_min_data_version(uint64_t &min_data_version);
  int refresh_data_version();
  virtual int schedule_merge(const int64_t broadcast_version) = 0;
  static const int64_t INIT_COMPACTION_SCN = 1;
protected:
  void update_frozen_version(const int64_t broadcast_version);
  void init_merge_progress(const int64_t broadcast_version);
  void update_merge_progress(const int64_t merge_version);
protected:
  static const int64_t PRINT_SLOG_REPLAY_INVERVAL = 10_s;
  mutable obsys::ObRWLock frozen_version_lock_;
  int64_t frozen_version_;
  int64_t inner_table_merged_scn_;
  int64_t merged_version_; // the merged major version of the local server, may be not accurate after reboot
  uint64_t min_data_version_;
  bool major_merge_status_;
};

#define MERGE_SCHEDULER_PTR (oceanbase::compaction::ObBasicMergeScheduler::get_merge_scheduler())

} // compaction
} // oceanbase

#endif // OB_STORAGE_COMPACTION_COMPACTION_SCHEDULER_UTIL_H_
