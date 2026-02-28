/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_COMPACTION_WINDOW_LOOP_H_
#define OB_STORAGE_COMPACTION_WINDOW_LOOP_H_

#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/compaction/ob_window_compaction_utils.h"
#include "storage/compaction/ob_window_schedule_tablet_func.h"

namespace oceanbase
{
namespace compaction
{

class ObWindowLoop
{
public:
  enum ObAdaptiveThreadStatus {
    THREAD_NOT_INITED = 0,
    THREAD_ADAPTIVE   = 1,
    THREAD_ASSIGNED   = 2,
    THREAD_MAX
  };
  struct LoopPriorityQueueRecord final
  {
  public:
    LoopPriorityQueueRecord() : min_score_(INT64_MAX), max_score_(0), moved_cnt_(0), failed_cnt_(0) {}
    ~LoopPriorityQueueRecord() = default;
    TO_STRING_KV(K_(min_score), K_(max_score), K_(moved_cnt), K_(failed_cnt));
    void finish(const int64_t score);
    int record_to_string(ObSqlString &loop_info) const;
  public:
    int64_t min_score_;
    int64_t max_score_;
    int64_t moved_cnt_;
    int64_t failed_cnt_;
  };
  struct LoopReadyListRecord final
  {
  public:
    LoopReadyListRecord() { MEMSET(this, 0, sizeof(LoopReadyListRecord)); }
    ~LoopReadyListRecord() = default;
    int record_to_string(ObSqlString &loop_info) const;
    TO_STRING_KV(K_(finish_cnt), K_(ready_cnt), K_(log_submitted_cnt));
  public:
    int64_t finish_cnt_;
    int64_t ready_cnt_;
    int64_t log_submitted_cnt_;
  };
public:
  ObWindowLoop();
  virtual ~ObWindowLoop();
  int try_schedule_window_compaction(
      const bool could_start_window_compaction,
      const int64_t merge_start_time);
  int start_window_compaction(const int64_t merge_start_time);
  int stop_window_compaction();
  int loop();
  bool is_active() const { return is_inited_; }
  OB_INLINE int process_tablet_stat(storage::ObTablet &tablet) { return score_prio_queue_.process_tablet_stat(tablet); }
  int process_ready_candidate(
      ObWindowScheduleTabletFunc &func,
      ObTabletCompactionScore *candidate,
      storage::ObTabletHandle &tablet_handle,
      LoopReadyListRecord &record);
  void update_adaptive_compaction_thread_cnt();
  int get_adaptive_compaction_thread_cnt(
      const int64_t compaction_low_thread_score,
      int64_t &adaptive_thread_cnt) const;
  // ATTENTION: Only for virtual table iterator
  OB_INLINE ObWindowCompactionPriorityQueue &get_score_prio_queue() { return score_prio_queue_; }
  OB_INLINE ObWindowCompactionReadyList &get_ready_list() { return ready_list_; }
private:
  int loop_tablet_stats();
  int loop_priority_queue();
  int loop_ready_list(const int64_t ts_threshold); // only loop the candidate which add_ts >= ts_threshold
  void dump_loop_priority_queue_record(
      const bool before_loop,
      LoopPriorityQueueRecord &loop_record);
  OB_INLINE void reset_adaptive_compaction_thread_cnt() { adaptive_status_ = THREAD_NOT_INITED; adaptive_compaction_thread_cnt_ = 0; }
private:
  bool is_inited_;
  ObWindowCompactionMemoryContext mem_ctx_;
  ObWindowCompactionScoreTracker score_tracker_;
  ObWindowCompactionReadyList ready_list_;
  ObWindowCompactionPriorityQueue score_prio_queue_;
  ObWindowCompactionTimeGuard time_guard_;
  ObAdaptiveThreadStatus adaptive_status_;
  int64_t adaptive_compaction_thread_cnt_;
  int64_t merge_start_time_;
  int64_t merge_loop_round_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObWindowLoop);
};

} // end namespace compaction
} // end namespace oceanbase

#endif // OB_STORAGE_COMPACTION_WINDOW_LOOP_H_