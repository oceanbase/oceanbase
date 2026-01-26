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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_window_loop.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_compaction_schedule_iterator.h"
#include "storage/compaction/ob_window_compaction_utils.h"
#include "storage/compaction/ob_schedule_tablet_func.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "rootserver/freeze/window/ob_window_compaction_helper.h"
#include "share/compaction/ob_schedule_daily_maintenance_window.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "share/compaction/ob_compaction_resource_manager.h"
#include "share/compaction/ob_compaction_time_guard.h"

ERRSIM_POINT_DEF(EN_WINDOW_COMPACTION_DO_NOT_ADD_READY_LIST);
ERRSIM_POINT_DEF(EN_WINDOW_COMPACTION_DO_NOT_PROCESS_READY_LIST);

namespace oceanbase
{
namespace compaction
{

/*-------------------------------- Loop PriorityQueue/ReadyList Record --------------------------------*/
void ObWindowLoop::LoopPriorityQueueRecord::finish(const int64_t score)
{
  moved_cnt_++;
  min_score_ = std::min(min_score_, score);
  max_score_ = std::max(max_score_, score);
}

int ObWindowLoop::LoopPriorityQueueRecord::record_to_string(ObSqlString &loop_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(loop_info.append_fmt("{\"min\":%ld,\"max\":%ld,\"moved\":%ld,\"failed\":%ld}",
                                       min_score_, max_score_, moved_cnt_, failed_cnt_))) {
    LOG_WARN("failed to append record", K(ret));
  }
  return ret;
}

int ObWindowLoop::LoopReadyListRecord::record_to_string(ObSqlString &loop_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(loop_info.append_fmt("{\"ready\":%ld,\"submited\":%ld,\"finish\":%ld}",
                                       ready_cnt_, log_submitted_cnt_, finish_cnt_))) {
    LOG_WARN("failed to append record", K(ret));
  }
  return ret;
}

/*-------------------------------- ObWindowLoop --------------------------------*/
ObWindowLoop::ObWindowLoop()
  : is_inited_(false),
    mem_ctx_(),
    score_tracker_(),
    ready_list_(mem_ctx_, score_tracker_),
    score_prio_queue_(mem_ctx_, score_tracker_),
    adaptive_status_(THREAD_NOT_INITED),
    adaptive_compaction_thread_cnt_(0),
    merge_start_time_(0),
    merge_loop_round_(0)
{
}

ObWindowLoop::~ObWindowLoop()
{
  int tmp_ret = OB_SUCCESS;
  if (IS_INIT) {
    merge_loop_round_ = 0;
    merge_start_time_ = 0;
    adaptive_compaction_thread_cnt_ = 0;
    adaptive_status_ = THREAD_NOT_INITED;
    if (OB_TMP_FAIL(score_prio_queue_.destroy())) {
      LOG_WARN_RET(tmp_ret, "failed to destroy score priority queue");
    }
    if (OB_TMP_FAIL(ready_list_.destroy())) {
      LOG_WARN_RET(tmp_ret, "failed to destroy ready list");
    }
    score_tracker_.reset();
    mem_ctx_.purge();
    is_inited_ = false;
  }
}

int ObWindowLoop::try_schedule_window_compaction(
    const bool could_start_window_compaction,
    const int64_t merge_start_time)
{
  int ret = OB_SUCCESS;
  if (could_start_window_compaction) {
    if (OB_FAIL(start_window_compaction(merge_start_time))) {
      LOG_WARN("failed to start window compaction", K(ret), K(merge_start_time));
    } else if (OB_FAIL(loop())) {
      LOG_WARN("failed to do window loop", K(ret));
    } else {
      LOG_INFO("[WIN-COMPACTION] Finish do window loop", K(ret), K(merge_start_time), K_(time_guard));
    }
  } else if (OB_FAIL(stop_window_compaction())) {
    LOG_WARN("failed to stop window compaction", K(ret));
  } else {
    LOG_INFO("[WIN-COMPACTION] Stop schedule all tablet window compaction", K(ret), K(merge_start_time));
  }
  return ret;
}

int ObWindowLoop::start_window_compaction(const int64_t merge_start_time)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_INIT) {
    LOG_INFO("window compaction has started");
  } else if (OB_FAIL(score_tracker_.init(merge_start_time))) {
    LOG_WARN("failed to init score tracker", K(ret), K(merge_start_time));
  } else if (OB_FAIL(ready_list_.init())) {
    score_tracker_.reset();
    LOG_WARN("failed to init ready list", K(ret));
  } else if (OB_FAIL(score_prio_queue_.init())) {
    if (OB_TMP_FAIL(ready_list_.destroy())) {
      // may cause window loop never start in this loop, it only failed when lock failed, rarely.
      // If it happens, disable window and re-enable it will be cured.
      LOG_WARN_RET(tmp_ret, "failed to destroy ready list");
    }
    score_tracker_.reset();
    LOG_WARN("failed to init score priority queue", K(ret));
  } else {
    merge_start_time_ = merge_start_time;
    merge_loop_round_ = 0;
    is_inited_ = true;
    LOG_INFO("[WIN-COMPACTION] Successfully start window compaction loop", K(ret));
    SERVER_EVENT_SYNC_ADD("window_compaction", "start_window_compaction", "tenant_id", MTL_ID(), "merge_start_time", merge_start_time);
  }
  return ret;
}

int ObWindowLoop::stop_window_compaction()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    LOG_INFO("window compaction has stopped");
  } else if (OB_FAIL(score_prio_queue_.destroy())) { // failed only when locked failed, rarely
    LOG_WARN("failed to destroy score priority queue", K(ret));
  } else if (OB_FAIL(ready_list_.destroy())) {
    LOG_WARN("failed to destroy ready list", K(ret));
  } else {
    score_tracker_.reset();
    mem_ctx_.purge(); // only purge, not destroy
    reset_adaptive_compaction_thread_cnt();
    merge_start_time_ = 0;
    merge_loop_round_ = 0;
    is_inited_ = false;
    LOG_INFO("[WIN-COMPACTION] Successfully stop window compaction loop", K(ret));
    SERVER_EVENT_SYNC_ADD("window_compaction", "stop_window_compaction", "tenant_id", MTL_ID());
  }
  return ret;
}

int ObWindowLoop::loop()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  time_guard_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("window compaction loop is not inited", K(ret));
  } else {
    // 0. update adaptive compaction thread cnt
    (void) update_adaptive_compaction_thread_cnt();

    // 1. loop and remove the candidates which are finished or invalid
    if (OB_TMP_FAIL(loop_ready_list(0 /*ts_threshold*/))) {
      LOG_WARN("failed to loop ready list", K(tmp_ret));
    }
    // 2. loop all tablet analyzers, calculate tablet score and push into priority queue
    if (OB_TMP_FAIL(loop_tablet_stats())) {
      LOG_WARN("failed to loop tablet stats", K(tmp_ret));
    }
    // 3. get the remaining size of candidate list, move the top k candidates from priority queue to candidate list
    if (OB_TMP_FAIL(loop_priority_queue())) {
      LOG_WARN("failed to loop priority queue", K(tmp_ret));
    }
    // 4. schedule medium compaction for candidate list
    if (OB_TMP_FAIL(loop_ready_list(time_guard_.add_time_))) {
      LOG_WARN("failed to loop ready list", K(tmp_ret));
    }

    merge_loop_round_++;
  }
  return ret;
}

struct ObTabletStatAnalyzerLSIDComparator final
{
public:
  ObTabletStatAnalyzerLSIDComparator(int &sort_ret) : result_code_(sort_ret) {}
  bool operator()(const storage::ObTabletStatAnalyzer &lhs, const storage::ObTabletStatAnalyzer &rhs) {
    return lhs.tablet_stat_.ls_id_ < rhs.tablet_stat_.ls_id_;
  }
public:
  int &result_code_;
};

struct TabletStatAnalyzerProcessor
{
public:
  TabletStatAnalyzerProcessor(ObWindowCompactionPriorityQueue &prio_queue) : prio_queue_(prio_queue) {}
  int operator()(ObScheduleTabletFunc &func, const storage::ObTabletStatAnalyzer &analyzer, ObTabletHandle &tablet_handle)
  {
    int ret = OB_SUCCESS;
    if (!func.get_ls_status().is_leader_) {
      // only ls leader can submit medium clog, so skip this
    } else if (OB_FAIL(prio_queue_.process_tablet_stat_analyzer(tablet_handle, analyzer))) {
      LOG_WARN("failed to process tablet stat analyzer", K(ret), K(analyzer));
    }
    return ret;
  }
private:
  ObWindowCompactionPriorityQueue &prio_queue_;
};

int ObWindowLoop::loop_tablet_stats()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<storage::ObTabletStatAnalyzer, 128> tablet_analyzers;
  tablet_analyzers.set_attr(ObMemAttr(MTL_ID(), "TbltAnlyrs"));
  ObTabletStatAnalyzerLSIDComparator cmp(ret);
  ObLSSortedIterator<storage::ObTabletStatAnalyzer> iterator;
  ObWindowScheduleTabletFunc func; // only used to filter tablets
  TabletStatAnalyzerProcessor processor(score_prio_queue_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObWindowLoop is not inited", K(ret));
  } else if (OB_FAIL(MTL(storage::ObTenantTabletStatMgr *)->get_all_tablet_analyzers(tablet_analyzers))) {
    LOG_WARN("failed to get all tablet analyzers", K(ret));
  } else if (tablet_analyzers.empty()) {
  } else if (FALSE_IT(lib::ob_sort(tablet_analyzers.begin(), tablet_analyzers.end(), cmp))) {
  } else if (OB_FAIL(ret)) {
    LOG_WARN("failed to sort tablet analyzers", K(ret));
  } else if (OB_FAIL(iterator.iterate(func, tablet_analyzers, processor, true /*skip_follower*/))) {
    LOG_WARN("failed to iterate tablet stat analyzers", K(ret));
  } else {
    LOG_INFO("[WIN-COMPACTION] Successfully loop tablet stats", K(ret), "tablet_cnt", tablet_analyzers.count(),
              K_(score_prio_queue), K_(ready_list));
  }
  time_guard_.click(ObWindowCompactionTimeGuard::LOOP_TABLET_STATS);
  return ret;
}

int ObWindowLoop::loop_priority_queue()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObWindowLoop is not inited", K(ret));
  } else if (score_prio_queue_.empty() || ready_list_.get_remaining_space() <= 0) {
    LOG_INFO("[WIN-COMPACTION] Priority queue is empty or ready list has no remaining space, skip loop priority queue", K(ret));
  } else {
    LoopPriorityQueueRecord record;
    ObSEArray<ObTabletCompactionScore *, 128> failed_scores;
    failed_scores.set_attr(ObMemAttr(MTL_ID(), "FailedScore"));
    (void) dump_loop_priority_queue_record(true /*before_loop*/, record);
    int64_t score_value = 0;
#ifdef ERRSIM
    if (OB_FAIL(EN_WINDOW_COMPACTION_DO_NOT_ADD_READY_LIST)) {
      LOG_INFO("ERRSIM EN_WINDOW_COMPACTION_DO_NOT_ADD_READY_LIST, do not add ready list", K(ret));
    }
#endif
    while (OB_SUCC(ret) && ready_list_.get_remaining_space() > 0) {
      ObTabletCompactionScore *score = nullptr;
      const int64_t remaining_space = ready_list_.get_remaining_space();
      if (OB_FAIL(score_prio_queue_.check_and_pop(remaining_space, score))) {
        if (OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to check and pop score from priority queue", K(ret), K(remaining_space));
        }
      } else if (OB_ISNULL(score)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("score is nullptr", K(ret));
      } else if (FALSE_IT(score->pos_at_priority_queue_ = -1)) {
      } else if (FALSE_IT(score->add_timestamp_ = time_guard_.add_time_)) {
      } else if (FALSE_IT(score_value = score->score_)) {
      } else if (OB_TMP_FAIL(ready_list_.add(score))) {
        LOG_WARN("failed to add candidate into ready list", K(tmp_ret), KPC(score));
        if (OB_NOT_NULL(score) && OB_TMP_FAIL(failed_scores.push_back(score))) {
          LOG_WARN("failed to push score into failed scores", K(tmp_ret));
          (void) mem_ctx_.free_score(score);
        }
      } else {
        record.finish(score_value);
      }
    }
    record.failed_cnt_ = failed_scores.count();
    for (int i = 0; i < failed_scores.count(); i++) {
      failed_scores.at(i)->pos_at_priority_queue_ = -1;
      if (OB_TMP_FAIL(score_prio_queue_.push(failed_scores.at(i)))) {
        LOG_WARN("failed to push score into priority queue", K(tmp_ret));
        (void) mem_ctx_.free_score(failed_scores.at(i));
      }
    }
    (void) dump_loop_priority_queue_record(false /*before_loop*/, record);
    LOG_INFO("[WIN-COMPACTION] Finish loop priority queue", K(ret), K(record), K_(merge_start_time), K_(merge_loop_round), K_(score_prio_queue), K_(ready_list));
  }
  time_guard_.click(ObWindowCompactionTimeGuard::LOOP_PRIORITY_QUEUE);
  return ret;
}

struct ObTabletCompactionScoreLSComparator final
{
public:
  ObTabletCompactionScoreLSComparator(int &sort_ret) : result_code_(sort_ret) {}
  bool operator()(const ObTabletCompactionScore *lhs, const ObTabletCompactionScore *rhs) const
  {
    return lhs->get_key().ls_id_ < rhs->get_key().ls_id_;
  }
public:
  int &result_code_;
};

class ReadyCandidateProcessor
{
public:
  ReadyCandidateProcessor(ObWindowLoop &window_loop, ObWindowLoop::LoopReadyListRecord &record)
    : window_loop_(window_loop),
      record_(record) {}
  int operator()(ObScheduleTabletFunc &func, ObTabletCompactionScore *candidate, ObTabletHandle &tablet_handle)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!func.is_window_compaction_func())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid func", K(ret), K(func));
    } else if (OB_FAIL(window_loop_.process_ready_candidate(static_cast<ObWindowScheduleTabletFunc &>(func), candidate, tablet_handle, record_))) {
      LOG_WARN("failed to process ready candidate", K(ret), KPC(candidate), K(tablet_handle));
    }
    return ret;
  }
private:
  ObWindowLoop &window_loop_;
  ObWindowLoop::LoopReadyListRecord &record_;
};

int ObWindowLoop::loop_ready_list(const int64_t ts_threshold)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletCompactionScore *, 4> candidates;  // capcity will be reserved when get_candidate_list
  ObLSSortedIterator<ObTabletCompactionScore *> iterator;
  ObWindowScheduleTabletFunc func;
  LoopReadyListRecord loop_record;
  ReadyCandidateProcessor processor(*this, loop_record);
  ObWindowCompactionBaseContainerGuard list_guard(ready_list_); // for thread-safely access ready list
  int64_t old_weighted_size = 0;
  ObSqlString loop_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObWindowLoop is not inited", K(ret));
  } else if (OB_FAIL(list_guard.acquire())) {
    LOG_WARN("failed to init list guard", K(ret));
  } else if (FALSE_IT(old_weighted_size = ready_list_.get_current_weighted_size())) {
  } else if (OB_FAIL(ready_list_.get_candidate_list(candidates, ts_threshold))) {
    LOG_WARN("failed to get candidate list", K(ret));
  } else if (candidates.empty()) {
    LOG_INFO("[WIN-COMPACTION] No candidate to process", K(ret));
  } else if (FALSE_IT(lib::ob_sort(candidates.begin(), candidates.end(), ObTabletCompactionScoreLSComparator(ret)))) {
  } else if (OB_FAIL(ret)) {
    LOG_WARN("failed to sort candidates", K(ret));
  } else if (OB_FAIL(iterator.iterate(func, candidates, processor, false /*skip_follower*/))) {
    LOG_WARN("failed to iterate ready candidates", K(ret));
  } else if (OB_FAIL(loop_record.record_to_string(loop_info))) {
    LOG_WARN("failed to convert loop info to string", K(ret));
  } else {
    LOG_INFO("[WIN-COMPACTION] Finish loop ready list", K(ret), "candidates_cnt", candidates.count(),
              K_(score_prio_queue), K_(ready_list));

    // first round loop in window loop, try to adjust the max capacity of ready list
    if (0 == ts_threshold) {
      const int64_t new_weighted_size = ready_list_.get_current_weighted_size();
      (void) ready_list_.update_max_capacity(old_weighted_size - new_weighted_size);
    }


    SERVER_EVENT_SYNC_ADD(
      "window_compaction", // module
      0 == ts_threshold ? "loop_history_list" : "loop_ready_list",   // event
      "tenant_id", MTL_ID(),
      "loop_info", loop_info.string(),
      "start_time", merge_start_time_,
      "round", merge_loop_round_,
      "capacity", ready_list_.get_max_capacity(),
      "size", ready_list_.get_current_weighted_size()
    );
  }
  time_guard_.click(ObWindowCompactionTimeGuard::LOOP_READY_LIST);
  return ret;
}

int ObWindowLoop::process_ready_candidate(
    ObWindowScheduleTabletFunc &func,
    ObTabletCompactionScore *candidate,
    storage::ObTabletHandle &tablet_handle,
    LoopReadyListRecord &record)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_ctx_.is_score_valid(candidate) || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(candidate), K(tablet_handle));
#ifdef ERRSIM
  } else if (OB_FAIL(EN_WINDOW_COMPACTION_DO_NOT_PROCESS_READY_LIST)) {
    LOG_INFO("ERRSIM EN_WINDOW_COMPACTION_DO_NOT_PROCESS_READY_LIST, do not process ready list", K(ret));
#endif
  } else if (OB_UNLIKELY(!func.get_ls_status().is_leader_)) {
    if (OB_FAIL(ready_list_.remove(candidate))) {
      LOG_WARN("failed to remove candidate from ready list", K(ret), KPC(candidate));
    }
  } else if (OB_FAIL(func.refresh_window_tablet(*candidate))) {
    LOG_WARN("failed to refresh window merge reason", K(ret), KPC(candidate));
  } else if (candidate->is_ready_status() && OB_FAIL(func.process_ready_candidate(*candidate, tablet_handle))) {
    LOG_WARN("failed to process ready candidate", K(ret), KPC(candidate));
  } else if (candidate->is_log_submitted_status() && OB_FAIL(func.process_log_submitted_candidate(*candidate, tablet_handle))) {
    LOG_WARN("failed to process log submitted candidate", K(ret), KPC(candidate));
  } else if (candidate->is_finished_status()) {
    const ObTabletID &tablet_id = candidate->get_key().tablet_id_;
    if (OB_FAIL(ready_list_.remove(candidate))) {
      LOG_WARN("failed to remove candidate from ready list", K(ret), KPC(candidate));
    } else {
      record.finish_cnt_++;
      LOG_INFO("[WIN-COMPACTION] Candidate is finished, remove it from ready list", K(tablet_id));
    }
  } else if (!candidate->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid candidate status", K(ret), KPC(candidate));
  } else {
    // summary for ready list statistics
    if (candidate->is_ready_status()) {
      record.ready_cnt_++;
    } else if (candidate->is_log_submitted_status()) {
      record.log_submitted_cnt_++;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid candidate status", K(ret), KPC(candidate));
    }
  }
  return ret;
}

void ObWindowLoop::update_adaptive_compaction_thread_cnt()
{
  int ret = OB_SUCCESS;
  bool from_job_config = false;
  if (OB_ISNULL(GCTX.cgroup_ctrl_) || !GCTX.cgroup_ctrl_->is_valid()) {
    // cgroup is invalid , set adaptive_task_limit_ as limits_
    adaptive_compaction_thread_cnt_ = 0;
    adaptive_status_ = THREAD_NOT_INITED;
    LOG_INFO("cgroup is invalid, don't need to adapt window thread cnt");
  } else if (THREAD_NOT_INITED == adaptive_status_ || REACH_THREAD_TIME_INTERVAL(5_min)) {
    // adaptive task limit for window compaction is affected by tenant max cpu, plan directive, and job_config, so it needs to be updated periodically.
    ObCompactionResourceManager resource_mgr;
    if (OB_FAIL(resource_mgr.get_window_compaction_thread_cnt(adaptive_compaction_thread_cnt_, from_job_config))) {
      LOG_WARN("failed to get window compaction thread cnt", K(ret));
    } else if (from_job_config) {
      adaptive_status_ = THREAD_ASSIGNED;
    } else {
      adaptive_status_ = THREAD_ADAPTIVE;
    }
  }
  time_guard_.click(ObWindowCompactionTimeGuard::UPDATE_ADAPTIVE_THREAD_CNT);
}

int ObWindowLoop::get_adaptive_compaction_thread_cnt(
    const int64_t compaction_low_thread_score,
    int64_t &adaptive_thread_cnt) const
{
  int ret = OB_SUCCESS;
  if (THREAD_NOT_INITED == adaptive_status_) {
    adaptive_thread_cnt = compaction_low_thread_score;
  } else if (THREAD_ADAPTIVE) {
    adaptive_thread_cnt = std::max(adaptive_compaction_thread_cnt_, compaction_low_thread_score);
  } else if (THREAD_ASSIGNED) {
    adaptive_thread_cnt = adaptive_compaction_thread_cnt_;
  } else {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid adaptive thread status", K(ret), K_(adaptive_status));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(adaptive_thread_cnt < 0 || adaptive_thread_cnt > ObDailyWindowJobConfig::MAX_THREAD_CNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid adaptive thread cnt", KR(ret), K(adaptive_thread_cnt), K(ObDailyWindowJobConfig::MAX_THREAD_CNT));
  }
  return ret;
}

void ObWindowLoop::dump_loop_priority_queue_record(
    const bool before_loop,
    LoopPriorityQueueRecord &loop_record)
{
  int ret = OB_SUCCESS;
  ObWindowCompactionQueueInfo queue_info;
  ObSqlString queue_distribution;
  ObSqlString loop_info;
  if (OB_FAIL(score_prio_queue_.fetch_info(queue_info))) {
    LOG_WARN("failed to fetch priority queue info", K(ret));
  } else if (OB_FAIL(queue_info.info_to_string(queue_distribution))) {
    LOG_WARN("failed to convert queue info to string", K(ret));
  } else if (before_loop) {
    SERVER_EVENT_ADD(
      "window_compaction",          // module
      "before_loop_priority_queue", // event
      "tenant_id", MTL_ID(),
      "distribution", queue_distribution.string(),
      "start_time", merge_start_time_,
      "round", merge_loop_round_
    );
  } else if (OB_FAIL(loop_record.record_to_string(loop_info))) {
    LOG_WARN("failed to convert loop info to string", K(ret));
  } else {
    SERVER_EVENT_ADD(
      "window_compaction",          // module
      "finish_loop_priority_queue", // event
      "tenant_id", MTL_ID(),
      "distribution", queue_distribution.string(),
      "start_time", merge_start_time_,
      "round", merge_loop_round_,
      "loop_info", loop_info.string()
    );
  }
}

} // end namespace compaction
} // end namespace oceanbase