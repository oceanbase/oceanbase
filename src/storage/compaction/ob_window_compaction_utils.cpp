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

#define USING_LOG_PREFIX STORAGE

#include "storage/compaction/ob_window_compaction_utils.h"
#include "storage/compaction/ob_schedule_tablet_func.h"
#include "storage/tablet/ob_tablet.h"
#include "share/ob_cluster_version.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
namespace compaction
{
ERRSIM_POINT_DEF(EN_LONG_TIME_NOT_MAJOR_FREEZE_SIMULATION_TABLET_ID);
ERRSIM_POINT_DEF(EN_LONG_TIME_NOT_MAJOR_FREEZE_SIMULATION_DAYS);

/*------------------------ ObTabletCompactionScoreDynamicInfo ----------------------*/
ObTabletCompactionScoreDynamicInfo::ObTabletCompactionScoreDynamicInfo()
  : info_(0),
    query_cnt_(0),
    read_amplification_factor_(1.0)
{
}

void ObTabletCompactionScoreDynamicInfo::reset()
{
  info_ = 0;
  query_cnt_ = 0;
  read_amplification_factor_ = 1.0;
}

int ObTabletCompactionScoreDynamicInfo::assign(const ObTabletCompactionScoreDynamicInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    info_ = other.info_;
    query_cnt_ = other.query_cnt_;
    read_amplification_factor_ = other.read_amplification_factor_;
  }
  return ret;
}

void ObTabletCompactionScoreDynamicInfo::gen_info(const int64_t compat_version, char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || pos >= buf_len) {
  } else {
    J_OBJ_START();
    BUF_PRINTF("query_cnt:%d, read_amp:%.2f, info:%u, ver:%u", query_cnt_, read_amplification_factor_, info_, compat_version);
    J_OBJ_END();
  }
}

OB_SERIALIZE_MEMBER(ObTabletCompactionScoreDynamicInfo, info_, query_cnt_, read_amplification_factor_);

/*------------------------ ObTabletCompactionScoreDecisionInfo ----------------------*/
ObTabletCompactionScoreDecisionInfo::ObTabletCompactionScoreDecisionInfo()
  : dynamic_info_(),
    base_inc_row_cnt_(0),
    tablet_snapshot_version_(0),
    major_snapshot_version_(0)
{
}

void ObTabletCompactionScoreDecisionInfo::reset()
{
  dynamic_info_.reset();
  base_inc_row_cnt_ = 0;
  tablet_snapshot_version_ = 0;
  major_snapshot_version_ = 0;
}

/*------------------------ ObTabletCompactionScore ----------------------*/
const char *ObTabletCompactionScore::CompactStatusStr[COMPACT_STATUS_MAX] = {
  "WAITING",
  "READY",
  "LOG_SUBMITTED",
  "FINISHED",
};

const char *ObTabletCompactionScore::get_compact_status_str(const CompactStatus compact_status)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACT_STATUS_MAX) == ARRAYSIZEOF(CompactStatusStr), "events str len is mismatch");
  const char *str = "";
  if (compact_status >= COMPACT_STATUS_MAX || compact_status < COMPACT_STATUS_WAITING) {
    str = "INVALID_STATUS";
  } else {
    str = CompactStatusStr[compact_status];
  }
  return str;
}

ObTabletCompactionScore::ObTabletCompactionScore()
  : key_(),
    decision_info_(),
    score_(0),
    pos_at_priority_queue_(-1),
    add_timestamp_(0),
    compact_status_(COMPACT_STATUS_WAITING),
    magic_(0)
{

}

void ObTabletCompactionScore::reset()
{
  key_.reset();
  decision_info_.reset();
  score_ = 0;
  pos_at_priority_queue_ = -1;
  add_timestamp_ = 0;
  compact_status_ = COMPACT_STATUS_WAITING;
  magic_ = 0;
}

void ObTabletCompactionScore::update(const ObTabletCompactionScoreKey &key,
                                     const ObTabletCompactionScoreDecisionInfo &decision_info,
                                     const int64_t score)
{
  key_ = key;
  decision_info_ = decision_info;
  score_ = score;
}

void ObTabletCompactionScore::update_with(const ObTabletCompactionScoreDecisionInfo &decision_info,
                                          const int64_t score)
{
  decision_info_ = decision_info;
  score_ = score;
}

/*------------------------ ObWindowCompactionMemoryContext ----------------------*/
ObWindowCompactionMemoryContext::ObWindowCompactionMemoryContext()
  : allocator_()
{
  allocator_.set_attr(ObMemAttr(MTL_ID(), "WinComAlloc"));
  allocator_.set_nway(2); // only window loop and medium loop will use this allocator
}

ObWindowCompactionMemoryContext::~ObWindowCompactionMemoryContext()
{
  allocator_.destroy();
}

int ObWindowCompactionMemoryContext::alloc_score(ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTabletCompactionScore)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for score", K(ret));
  } else {
    score = new (buf) ObTabletCompactionScore();
    score->magic_ = CONTAINER_SCORE_MAGIC;
  }
  return ret;
}

void ObWindowCompactionMemoryContext::free_score(ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(score)) {
  } else if (OB_UNLIKELY(score->magic_ != CONTAINER_SCORE_MAGIC)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid score", K(ret), KPC(score));
  } else {
    score->~ObTabletCompactionScore();
    allocator_.free(score);
    score = nullptr;
  }
}

/*------------------------ ObWindowCompactionScoreTracker ----------------------*/
const char* ObWindowCompactionScoreTracker::RECORD_SCHEMA_STR = "\"ts\",\"tablet_id\",\"inc_row_cnt\",\"info\", \"query_cnt\", \"read_amplification_factor\", \"score\"";

int ObWindowCompactionScoreTracker::UpdateCallback::operator()(hash::HashMapPair<ObTabletID, int64_t> &pair)
{
  int ret = OB_SUCCESS;
  if (new_val_ > pair.second) {
    pair.second = new_val_;
    res_ = UpsertResult::UPDATED;
  } else {
    res_ = UpsertResult::SKIPED;
  }
  return ret;
}

int ObWindowCompactionScoreTracker::TabletRecord::append_itself(ObSqlString &records) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(records.append_fmt("[%ld,%ld,%ld,%u,%u,%f,%ld]",
      timestamp_, tablet_id_, inc_row_cnt_, dynamic_info_.info_, dynamic_info_.query_cnt_, dynamic_info_.read_amplification_factor_, score_))) {
    LOG_WARN("failed to append record", K(ret));
  }
  return ret;
}
ObWindowCompactionScoreTracker::ObWindowCompactionScoreTracker()
  : is_inited_(false),
    merge_start_time_(0),
    allocator_(ObMemAttr(MTL_ID(), "TbltScrTkr")),
    hash_allocator_(allocator_),
    bucket_allocator_(allocator_),
    score_map_(),
    record_array_()
{
  const int64_t tenant_id = MTL_ID();
  hash_allocator_.set_attr(ObMemAttr(tenant_id, "TbltScrHash"));
  bucket_allocator_.set_attr(ObMemAttr(tenant_id, "TbltScrBukt"));
  record_array_.set_attr(ObMemAttr(tenant_id, "TbltDyRec"));
}

int ObWindowCompactionScoreTracker::init(const int64_t merge_start_time)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "inited twice", K(ret));
  } else if (OB_FAIL(score_map_.create(DEFAULT_BUCKET_NUM, &hash_allocator_, &bucket_allocator_))) {
    STORAGE_LOG(WARN, "failed to init score map", K(ret));
  } else {
    merge_start_time_ = merge_start_time;
    is_inited_ = true;
    STORAGE_LOG(INFO, "init window compaction score tracker success", K(ret), K(merge_start_time));
  }
  return ret;
}

void ObWindowCompactionScoreTracker::reset()
{
  if (IS_INIT) {
    (void) dump_records_();
    record_array_.reset();
    score_map_.destroy();
    allocator_.reset();
    merge_start_time_ = 0;
    is_inited_ = false;
  }
}

int ObWindowCompactionScoreTracker::upsert(
    const ObTabletID &tablet_id,
    const storage::ObTabletStatAnalyzer *analyzer,
    const ObTabletCompactionScoreDecisionInfo &decision_info,
    const int64_t new_score)
{
  int ret = OB_SUCCESS;
  UpsertResult result = UpsertResult::MAX_REULST;
  SetCallback set_callback(result);
  UpdateCallback update_callback(new_score, result);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("score tracker is not inited", K(ret));
  } else if (OB_FAIL(score_map_.set_or_update(tablet_id, new_score, set_callback, update_callback))) {
    LOG_WARN("failed to upsert score", K(ret), K(tablet_id), K(new_score));
  } else if (UpsertResult::MAX_REULST <= result) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to upsert score", K(ret), K(tablet_id), K(new_score));
  } else if (UpsertResult::INSERTED == result || UpsertResult::UPDATED == result) {
    (void) track_record_(tablet_id, analyzer, decision_info, new_score, result);
  }
  return ret;
}


int ObWindowCompactionScoreTracker::track_record_(
    const ObTabletID &tablet_id,
    const storage::ObTabletStatAnalyzer *analyzer,
    const ObTabletCompactionScoreDecisionInfo &decision_info,
    const int64_t new_score,
    const UpsertResult result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("score tracker is not inited", K(ret));
  } else {
    TabletRecord record;
    record.timestamp_ = ObTimeUtil::current_time();
    record.tablet_id_ = tablet_id.id();
    record.inc_row_cnt_ = decision_info.base_inc_row_cnt_;
    record.score_ = new_score;
    record.dynamic_info_ = decision_info.dynamic_info_;
    if (OB_FAIL(record_array_.push_back(record))) {
      LOG_WARN("failed to push back dynamic record", K(ret));
    } else if (record_array_.count() >= MAX_RECORD_NUM && OB_FAIL(dump_records_())) {
      LOG_WARN("failed to dump dynamic records", K(ret));
    }
    LOG_INFO("[WIN-COMPACTION] calc tablet compaction score", K(ret), K(tablet_id), K(result), KPC(analyzer),  K(decision_info), K(new_score));
  }
  return ret;
}

int ObWindowCompactionScoreTracker::dump_records_()
{
  int ret = OB_SUCCESS;
  ObSqlString records;
  if (OB_UNLIKELY(record_array_.empty())) {
  } else if (OB_FAIL(records.append_fmt("{\"schema\":[%s],\"records\":[", RECORD_SCHEMA_STR))) {
    STORAGE_LOG(WARN, "failed to append schema", K(ret));
  } else {
    const int64_t count = record_array_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      const bool is_last = (i == count - 1);
      const TabletRecord &record = record_array_.at(i);
      if (OB_FAIL(record.append_itself(records))) {
        STORAGE_LOG(WARN, "failed to append record", K(ret), K(record));
      } else if (OB_FAIL(records.append_fmt("%s", is_last ? "]}" : ","))) {
        STORAGE_LOG(WARN, "failed to end record", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      SERVER_EVENT_ADD("window_compaction", "record_complete_tablet_score", "tenant_id", MTL_ID(), "records", records.string(), "merge_start_time", merge_start_time_);
      record_array_.reuse();
    }
  }
  return ret;
}

int ObWindowCompactionScoreTracker::finish(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("score tracker is not inited", K(ret));
  } else if (OB_FAIL(score_map_.erase_refactored(tablet_id))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to erase score from map", K(ret), K(tablet_id));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

/*-------------------------------- ObWindowCompactionQueueInfo --------------------------------*/
int ObWindowCompactionQueueInfo::info_to_string(ObSqlString &queue_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue_info.append_fmt("{\"max\":%ld,\"min\":%ld,\"mid\":%ld,\"avg\":%.2f, \"len\":%ld, \"threshold\":%ld}",
                                        max_score_, min_score_, mid_score_, avg_score_, length_, threshold_))) {
    LOG_WARN("failed to append queue info", K(ret));
  }
  return ret;
}

/*------------------------ ObWindowCompactionPriorityQueue ----------------------*/
ObWindowCompactionPriorityQueue::ObWindowCompactionPriorityQueue(ObWindowCompactionMemoryContext &mem_ctx,
                                                                 ObWindowCompactionScoreTracker &score_tracker)
  : ObWindowCompactionBaseContainer(mem_ctx, score_tracker),
    is_inited_(false),
    max_heap_cmp_(),
    score_map_(),
    score_prio_queue_(max_heap_cmp_, &mem_ctx.get_allocator()),
    avg_score_(0.0)
{

}

int ObWindowCompactionPriorityQueue::destroy()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(lock_);
  if (OB_UNLIKELY(!guard.acquired())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to acquire lock, retry again", K(ret));
  } else if (IS_INIT) {
    score_prio_queue_.reset();
    TabletScoreMap::iterator iter = score_map_.begin();
    for ( ; iter != score_map_.end(); ++iter) {
      (void) mem_ctx_.free_score(iter->second);
    }
    score_map_.destroy();
    avg_score_ = 0.0;
    is_inited_ = false;
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::init()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(lock_);
  if (OB_UNLIKELY(!guard.acquired())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to acquire lock, retry again", K(ret));
  } else if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else if (OB_FAIL(score_map_.create(DEFAULT_BUCKET_NUM, ObMemAttr(MTL_ID(), "ScoreMap")))) {
    LOG_WARN("failed to init score map", K(ret));
  } else {
    avg_score_ = 0.0;
    is_inited_ = true;
    LOG_INFO("init window compaction priority queue success", K(ret));
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::process_tablet_stat_analyzer(
    storage::ObTabletHandle &tablet_handle,
    const storage::ObTabletStatAnalyzer &analyzer)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!analyzer.tablet_stat_.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(analyzer), K(tablet_handle));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(process_tablet_stat(*tablet, &analyzer))) {
    LOG_WARN("failed to process tablet stat", K(ret), K(analyzer), KPC(tablet));
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::process_tablet_stat(
    storage::ObTablet &tablet,
    const storage::ObTabletStatAnalyzer *analyzer)
{
  int ret = OB_SUCCESS;
  bool need_window_compaction = false;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObTabletCompactionScoreDecisionInfo info;
  int64_t score = 0;
  if (OB_FAIL(ObWindowCompactionUtils::calculate_tablet_compaction_score(analyzer, tablet, need_window_compaction, info, score))) {
    LOG_WARN("failed to calc tablet compaction score", K(ret), K(tablet));
  } else if (!need_window_compaction) {
  } else if (OB_FAIL(score_tracker_.upsert(tablet_id, analyzer, info, score))) {
    LOG_WARN("failed to upsert score into score tracker", K(ret), K(tablet_id), K(analyzer), K(info), K(score));
  } else if (OB_FAIL(try_insert_or_update_tablet_score(tablet, info, score))) {
    LOG_WARN("failed to update priority queue", K(ret), K(tablet));
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::try_insert_or_update_tablet_score(
    const storage::ObTablet &tablet,
    const ObTabletCompactionScoreDecisionInfo &info,
    const int64_t score)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletCompactionScoreKey key(ls_id, tablet_id);
  ObTabletCompactionScore *tablet_score = nullptr;
  bool exist = false;
  obsys::ObWLockGuard guard(lock_);
  if (OB_UNLIKELY(!guard.acquired())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to acquire lock, retry again", K(ret));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObWindowCompactionPriorityQueue is not inited", K(ret));
  } else if (OB_FAIL(inner_check_exist_and_get_score(key, exist, tablet_score))) {
    LOG_WARN("failed to check exist and get score", K(ret), K(key));
  } else if (exist) {
    if (OB_FAIL(inner_update_with_new_score(tablet_score, info, score))) {
      LOG_WARN("failed to update score in priority queue", K(ret), KPC(tablet_score));
    }
  } else {
    // check admission to prevent too many small scores
    if (OB_FAIL(check_admission(score))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to check admission for push new store", K(ret), K(score));
      }
    } else if (OB_FAIL(inner_push_new_score(key, info, score))) {
      LOG_WARN("failed to push new score into priority queue", K(ret), K(key));
    }
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::push(ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(lock_);
  if (OB_UNLIKELY(!guard.acquired())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to acquire lock, retry again", K(ret));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObWindowCompactionPriorityQueue is not inited", K(ret));
  } else if (OB_FAIL(inner_push(score))) {
    LOG_WARN("failed to push score into priority queue", K(ret), KPC(score));
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::inner_push(ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_score_valid(score))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(score));
  } else if (OB_FAIL(score_map_.set_refactored(score->key_, score))) {
    LOG_WARN("failed to set score into map", K(ret), KPC(score)); // recommend to check existence before push
  } else if (OB_FAIL(score_prio_queue_.push(score))) {
    LOG_WARN("failed to push score into priority queue", K(ret), KPC(score));
    if (OB_TMP_FAIL(score_map_.erase_refactored(score->key_))) {
      LOG_ERROR("failed to erase score from map", K(tmp_ret), K(score->key_));
    }
  } else {
    (void) update_avg_score_on_success(score->score_, true /* is_push */);
    score->add_timestamp_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::check_and_pop(
    const int64_t max_size,
    ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(lock_);
  ObTabletCompactionScore *top = nullptr;
  score = nullptr;
  if (OB_UNLIKELY(!guard.acquired())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to acquire lock, retry again", K(ret));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObWindowCompactionPriorityQueue is not inited", K(ret));
  } else if (max_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(max_size));
  } else if (score_prio_queue_.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_TRACE("priority queue is empty");
  } else if (OB_ISNULL(top = score_prio_queue_.top())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("top score is nullptr", K(ret));
  } else if (top->get_weighted_size() > max_size) {
    ret = OB_EAGAIN;
    LOG_WARN("top score is too large", K(ret), KPC(top), K(max_size));
  } else if (OB_FAIL(score_prio_queue_.pop())) {
    LOG_WARN("failed to pop score from priority queue", K(ret));
  } else if (FALSE_IT(update_avg_score_on_success(top->score_, false /* is_push */))) {
  } else if (OB_FAIL(score_map_.erase_refactored(top->key_))) {
    LOG_ERROR("failed to erase score from map", K(ret), KPC(top));
  } else {
    score = top;
  }
  return ret;
}

struct ObTabletCompactionScoreComparator final
{
public:
  ObTabletCompactionScoreComparator(int &sort_ret) : result_code_(sort_ret) {}
  bool operator()(const ObTabletCompactionScore *lhs, const ObTabletCompactionScore *rhs) const
  {
    return lhs->score_ < rhs->score_;
  }
public:
  int &result_code_;
};

int ObWindowCompactionPriorityQueue::fetch_info(ObWindowCompactionQueueInfo &info)
{
  int ret = OB_SUCCESS;
  obsys::ObRLockGuard guard(lock_);
  if (OB_UNLIKELY(!guard.acquired())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to acquire lock, retry again", K(ret));
  } else if (OB_FAIL(inner_fetch_info(info))) {
    LOG_WARN("failed to fetch info", K(ret));
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::inner_fetch_info(ObWindowCompactionQueueInfo &info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObWindowCompactionPriorityQueue is not inited", K(ret));
  } else if (score_prio_queue_.empty()) {
    // skip
  } else {
    ObSEArray<ObTabletCompactionScore *, 4> scores;
    if (OB_FAIL(scores.assign(score_prio_queue_.get_heap_data()))) {
      LOG_WARN("failed to assign scores", K(ret));
    } else if (FALSE_IT(lib::ob_sort(scores.begin(), scores.end(), ObTabletCompactionScoreComparator(ret)))) {
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to sort scores", K(ret));
    } else {
      info.length_ = scores.count();
      info.max_score_ = scores.at(scores.count() - 1)->score_;
      info.min_score_ = scores.at(0)->score_;
      info.mid_score_ = scores.at(scores.count() / 2)->score_;
      info.threshold_ = static_cast<int64_t>(0.5 * avg_score_ * (scores.count() / BASE_THRESHOLD));
      info.avg_score_ = avg_score_;
      if (info.max_score_ != score_prio_queue_.top()->score_) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "top score mismatch", K(info), KPC(score_prio_queue_.top()));
      }
    }
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::inner_check_exist_and_get_score(
    const ObTabletCompactionScoreKey &key,
    bool &exist,
    ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  exist = false;
  score = nullptr;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_TMP_FAIL(score_map_.get_refactored(key, score))) {
    if (OB_HASH_NOT_EXIST != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to get score from map", K(ret), K(key));
    }
  } else if (OB_UNLIKELY(!is_score_valid_in_queue(score))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score is invalid", K(ret), K(key), KPC(score));
  } else {
    exist = true;
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::inner_update_with_new_score(
    ObTabletCompactionScore *tablet_score,
    const ObTabletCompactionScoreDecisionInfo &decision_info,
    const int64_t score)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_score_valid_in_queue(tablet_score))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(tablet_score));
  } else if (tablet_score->score_ >= score) { // new score is not better, no need to update
  } else if (OB_FAIL(score_prio_queue_.remove(tablet_score))) {
    LOG_WARN("failed to remove score from priority queue", K(ret), KPC(tablet_score));
  } else if (FALSE_IT(update_avg_score_on_success(tablet_score->score_, false /* is_push */))) {
  } else if (FALSE_IT(tablet_score->update_with(decision_info, score))) {
  } else if (OB_FAIL(score_prio_queue_.push(tablet_score))) {
    LOG_WARN("failed to push score into priority queue", K(ret), KPC(tablet_score));
    // if push failed, the score is not in the priority queue, but in the map, so we need to remove it
    if (OB_TMP_FAIL(score_map_.erase_refactored(tablet_score->key_))) {
      LOG_ERROR("failed to erase score from map", K(tmp_ret), K(tablet_score->key_));
    } else {
      (void) mem_ctx_.free_score(tablet_score);
    }
  } else {
    (void) update_avg_score_on_success(score, true /* is_push */);
    LOG_INFO("[WINDOW_COMPACTION_DEBUG] update score in priority queue success", K(ret), KPC(tablet_score)); // TODO(chengkong): downgrade to TRACE when stable
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::inner_push_new_score(
    const ObTabletCompactionScoreKey &key,
    const ObTabletCompactionScoreDecisionInfo &decision_info,
    const int64_t score)
{
  int ret = OB_SUCCESS;
  ObTabletCompactionScore *tablet_score = nullptr;
  if (OB_FAIL(mem_ctx_.alloc_score(tablet_score))) {
    LOG_WARN("failed to alloc memory for score", K(ret));
  } else if (FALSE_IT(tablet_score->update(key, decision_info, score))) {
  } else if (OB_FAIL(inner_push(tablet_score))) {
    LOG_WARN("failed to push score into priority queue", K(ret), KPC(tablet_score)); // previous check has guaranteed that the score is not in the priority queue
  } else {
    LOG_INFO("[WINDOW_COMPACTION_DEBUG] push score into priority queue success", K(ret), KPC(tablet_score)); // TODO(chengkong): downgrade to TRACE when stable
  }

  if (OB_FAIL(ret)) {
    (void) mem_ctx_.free_score(tablet_score);
  }
  return ret;
}

int ObWindowCompactionPriorityQueue::check_admission(const int64_t score)
{
  int ret = OB_SUCCESS;
  const int64_t current_size = score_prio_queue_.count();
  if (current_size >= THRESHOLD_MAX /* 200000*/) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("priority queue size overflow", K(ret), K(current_size), K(score), K_(avg_score));
  } else if (2 * score <= avg_score_ * (current_size / BASE_THRESHOLD)) {
    ret = OB_EAGAIN;
    LOG_TRACE("score is too small, wait a moment", K(ret), K(current_size), K(score), K_(avg_score));
  }
  return ret;
}

void ObWindowCompactionPriorityQueue::update_avg_score_on_success(const int64_t score, const bool is_push)
{
  const double current_size = static_cast<double>(score_prio_queue_.count());
  if (is_push) {
    OB_ASSERT_MSG(current_size >= 1, "current size should be positive");
    if (1 == current_size) {
      avg_score_ = score;
    } else {
      const double old_size = current_size - 1;
      // (avg_score_ * old_size + score) / current_size;
      avg_score_ = avg_score_ * (old_size / current_size)  + (score / current_size);
    }
  } else /* is_pop*/ {
    OB_ASSERT_MSG(current_size >= 0, "current size should not be negative");
    if (0 == current_size) {
      avg_score_ = 0;
    } else {
      const double old_size = current_size + 1;
      // (avg_score_ * old_size - score) / current_size;
      avg_score_ = avg_score_ * (old_size / current_size)  - (score / current_size);
    }
  }
}

/*------------------------ ObWindowCompactionReadyList ----------------------*/
ObWindowCompactionReadyList::ObWindowCompactionReadyList(ObWindowCompactionMemoryContext &mem_ctx,
                                                         ObWindowCompactionScoreTracker &score_tracker)
  : ObWindowCompactionBaseContainer(mem_ctx, score_tracker),
    is_inited_(false),
    candidate_map_(),
    max_capacity_(DEFAULT_CANDIDATE_WINDOW_SIZE),
    weighted_size_(0),
    waiting_score_cnt_(0)
{

}

int ObWindowCompactionReadyList::destroy()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(lock_);
  if (OB_UNLIKELY(!guard.acquired())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to acquire lock, retry again", K(ret));
  } else if (IS_INIT) {
    waiting_score_cnt_ = 0;
    weighted_size_ = 0;
    max_capacity_ = DEFAULT_CANDIDATE_WINDOW_SIZE;
    TabletScoreMap::iterator iter = candidate_map_.begin();
    for ( ; iter != candidate_map_.end(); ++iter) {
      (void) mem_ctx_.free_score(iter->second);
    }
    candidate_map_.destroy();
    is_inited_ = false;
  }
  return ret;
}

int ObWindowCompactionReadyList::init()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(lock_);
  if (OB_UNLIKELY(!guard.acquired())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to acquire lock, retry again", K(ret));
  } else if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else if (OB_FAIL(candidate_map_.create(DEFAULT_BUCKET_NUM, ObMemAttr(MTL_ID(), "CandidateMap")))) {
    LOG_WARN("failed to init candidate map", K(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("init window compaction ready list success", K(ret));
  }
  return ret;
}

int ObWindowCompactionReadyList::get_candidate_list(
    ObIArray<ObTabletCompactionScore *> &candidates,
    const int64_t ts_threshold)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObWindowCompactionReadyList is not inited", K(ret));
  } else if (FALSE_IT(candidates.reset())) {
  } else if (OB_FAIL(candidates.reserve(candidate_map_.size()))) {
    LOG_WARN("failed to reserve memory for candidates", K(ret));
  } else {
    TabletScoreMap::iterator iter = candidate_map_.begin();
    for ( ; OB_SUCC(ret) && iter != candidate_map_.end(); iter++) {
      if (OB_ISNULL(iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("candidate is nullptr", K(ret));
      } else if (iter->second->add_timestamp_ < ts_threshold) {
        continue;
      } else if (OB_FAIL(candidates.push_back(iter->second))) {
        LOG_WARN("failed to push candidate into array", K(ret));
      }
    }
  }
  return ret;
}

int ObWindowCompactionReadyList::add(ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  ObTabletCompactionScore *candidate = nullptr;
  bool exist = false;
  obsys::ObWLockGuard guard(lock_);
  if (OB_UNLIKELY(!guard.acquired())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to acquire lock, retry again", K(ret));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObWindowCompactionReadyList is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_score_valid(score))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet score", K(ret), KPC(score));
  } else if (OB_FAIL(inner_check_exist_and_get_candidate(score->key_, exist, candidate))) {
    LOG_WARN("failed to check exist and get candidate", K(ret), KPC(score));
  } else if (exist) {
    if (OB_FAIL(inner_update_candidate_with_new_score(candidate, score))) {
      LOG_WARN("failed to update candidate score", K(ret), KPC(score));
    }
  } else {
    if (OB_FAIL(inner_add_candidate(score))) {
      LOG_WARN("failed to add candidate", K(ret), KPC(score));
    }
  }
  return ret;
}

// protected by ObWindowCompactionBaseContainerGuard, so do not need to acquire lock
int ObWindowCompactionReadyList::remove(ObTabletCompactionScore *&candidate)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObWindowCompactionReadyList is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_score_valid(candidate))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid candidate", K(ret), KPC(candidate));
  } else if (OB_FAIL(candidate_map_.erase_refactored(candidate->get_key()))) {
    LOG_WARN("failed to erase candidate from map", K(ret), KPC(candidate));
  } else if (OB_FAIL(score_tracker_.finish(candidate->get_key().tablet_id_))) {
    LOG_WARN("failed to finish tracking tablet score", K(ret), KPC(candidate));
  } else {
    weighted_size_ -= candidate->get_weighted_size();
    (void) mem_ctx_.free_score(candidate);
  }
  return ret;
}

void ObWindowCompactionReadyList::update_max_capacity(const int64_t finished_weighted_size)
{
  if (finished_weighted_size > 0 && finished_weighted_size > (max_capacity_ / 2) && max_capacity_ < MAX_CANDIDATE_WINDOW_SIZE) {
    const int64_t step_size = MIN(finished_weighted_size, DEFAULT_CANDIDATE_WINDOW_SIZE);
    max_capacity_ = MIN(max_capacity_ + step_size, MAX_CANDIDATE_WINDOW_SIZE);
    LOG_INFO("[WIN-COMPACTION] update max capacity for ready list", K(finished_weighted_size), K(step_size), K_(max_capacity));
  }
}

int ObWindowCompactionReadyList::inner_add_candidate(ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_score_valid(score) || !score->is_waiting_status())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(score));
  } else if (get_remaining_space() < score->get_weighted_size()) {
    ret = OB_EAGAIN;
    LOG_INFO("[WINDOW_COMPACTION_DEBUG] No enough space in ready list", K(ret),
             "remaining_space", get_remaining_space(), "score_size", score->get_weighted_size());
  } else if (OB_FAIL(candidate_map_.set_refactored(score->key_, score))) {
    LOG_WARN("failed to set candidate into map", K(ret), KPC(score));
  } else {
    weighted_size_ += score->get_weighted_size();
    score->set_ready_status();
    score = nullptr;
  }
  return ret;
}

int ObWindowCompactionReadyList::inner_check_exist_and_get_candidate(
    const ObTabletCompactionScoreKey &key,
    bool &exist,
    ObTabletCompactionScore *&candidate)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  exist = false;
  candidate = nullptr;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_TMP_FAIL(candidate_map_.get_refactored(key, candidate))) {
    if (OB_HASH_NOT_EXIST != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to get candidate from map", K(ret), K(key));
    }
  } else if (OB_UNLIKELY(!is_score_valid(candidate))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("candidate is invalid", K(ret), K(key), KPC(candidate));
  } else {
    exist = true;
  }
  return ret;
}

int ObWindowCompactionReadyList::inner_update_candidate_with_new_score(
    ObTabletCompactionScore *candidate,
    ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  int64_t size_diff = 0;
  if (OB_UNLIKELY(!is_score_valid(candidate) || !is_score_valid(score))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(candidate), KPC(score));
  } else if (!candidate->is_ready_status()) {
    ret = OB_EAGAIN;
    waiting_score_cnt_++;
    LOG_TRACE("[WINDOW_COMPACTION_DEBUG] need use waiting score", K(ret), K_(waiting_score_cnt), KPC(score));
  } else {
    weighted_size_ -= candidate->get_weighted_size();
    (void) candidate->update_with(score->decision_info_, score->score_);
    weighted_size_ += candidate->get_weighted_size();
    (void) mem_ctx_.free_score(score);
  }
  return ret;
}

/*------------------------ ObWindowCompactionUtils ----------------------*/
int ObWindowCompactionUtils::calculate_tablet_compaction_score(
    const storage::ObTabletStatAnalyzer *analyzer,
    storage::ObTablet &tablet,
    bool &need_window_compaction,
    ObTabletCompactionScoreDecisionInfo &decision_info,
    int64_t &score)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  bool need_recycle_truncate_info = false;
  need_window_compaction = false;
  decision_info.reset();
  score = 0;

  if (tablet_id.is_special_merge_tablet()) {
    // tablet does not need to do major compaction
  } else if (tablet.get_inc_row_cnt() >= INC_ROW_CNT_THRESHOLD) {
    need_window_compaction = true;
  } else if (OB_FAIL(check_need_recycle_truncate_info(tablet, need_recycle_truncate_info))) {
    LOG_WARN("failed to check need recycle truncate info", K(ret), K(tablet));
  } else if (tablet.get_inc_row_cnt() > 0 || need_recycle_truncate_info || tablet.need_progressive_merge()) {
    need_window_compaction = true;
  }

  if (need_window_compaction) {
    // column_group_cnt = tablet.get_last_major_column_count() - 2(multi-version column) + 1(rowkey or all cg) = tablet.get_last_major_column_count() - 1
    const int64_t cg_merge_batch_cnt = tablet.is_row_store() ? 1 : ObCOMajorMergePolicy::get_cg_merge_batch_cnt(tablet.get_last_major_column_count() - 1);
    int64_t inc_row_cnt_factor = 0;
    if (OB_FAIL(get_inc_row_cnt_factor(tablet, need_recycle_truncate_info, inc_row_cnt_factor))) {
      LOG_WARN("failed to get inc row cnt factor", K(ret), K(tablet));
    } else {
      double score_value = cg_merge_batch_cnt * inc_row_cnt_factor;
      if (OB_NOT_NULL(analyzer)) {
        multiply_tablet_compaction_score_with_analyzer(*analyzer, decision_info, score_value);
      }
      decision_info.dynamic_info_.need_recycle_mds_ = need_recycle_truncate_info;
      decision_info.dynamic_info_.need_progressive_merge_ = tablet.need_progressive_merge();
      decision_info.dynamic_info_.cg_merge_batch_cnt_ = cg_merge_batch_cnt;
      decision_info.base_inc_row_cnt_ = tablet.get_inc_row_cnt();
      decision_info.tablet_snapshot_version_ = tablet.get_snapshot_version();
      decision_info.major_snapshot_version_ = tablet.get_last_major_snapshot_version();

      if (score_value > INT64_MAX || std::isinf(score_value)) {
        score = INT64_MAX;
      } else {
        score = static_cast<int64_t>(score_value);
      }
    }
  }
  return ret;
}

int ObWindowCompactionUtils::check_need_recycle_truncate_info(
    storage::ObTablet &tablet,
    bool &need_recycle_truncate_info)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObAdaptiveMergePolicy::AdaptiveMergeReason reason = ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE;
  int64_t unused_least_medium_snapshot = 0;
  need_recycle_truncate_info = false;
  if (OB_FAIL(ObAdaptiveMergePolicy::check_truncate_info_reason(tablet, reason, unused_least_medium_snapshot))) {
    LOG_WARN("failed to check truncate info reason", K(ret), K(tablet_id));
  } else if (ObAdaptiveMergePolicy::AdaptiveMergeReason::RECYCLE_TRUNCATE_INFO == reason) {
    need_recycle_truncate_info = true;
  }
  return ret;
}

void ObWindowCompactionUtils::multiply_tablet_compaction_score_with_analyzer(
     const storage::ObTabletStatAnalyzer &analyzer,
     ObTabletCompactionScoreDecisionInfo &decision_info,
     double &score_value)
{
  // there are two kinds of tablet stats, one is latest stat(recent 16 minutes), another is total stat
  const ObTabletStat &total_stat = analyzer.total_tablet_stat_;
  double queuing_factor = 1 / (analyzer.boost_factor_ > 0 ? analyzer.boost_factor_ : 1);
  const uint32_t query_factor = MAX(total_stat.query_cnt_ + 1, 1);
  const double read_amplification_factor = analyzer.get_read_amplification();
  score_value *= queuing_factor * query_factor * read_amplification_factor * analyzer.get_satisfied_condition_number();
  ObTabletCompactionScoreDynamicInfo &dynamic_info = decision_info.dynamic_info_;
  dynamic_info.queuing_mode_ = analyzer.mode_;
  dynamic_info.is_hot_tablet_ = analyzer.is_hot_tablet();
  dynamic_info.is_insert_mostly_ = analyzer.is_insert_mostly();
  dynamic_info.is_update_or_delete_mostly_ = analyzer.is_update_or_delete_mostly();
  dynamic_info.has_slow_query_ = analyzer.has_frequent_slow_query();
  dynamic_info.has_accumulated_delete_ = analyzer.has_accumulated_delete();
  dynamic_info.query_cnt_ = total_stat.query_cnt_;
  dynamic_info.read_amplification_factor_ = read_amplification_factor;
}

int ObWindowCompactionUtils::get_inc_row_cnt_factor(
    storage::ObTablet &tablet,
    const bool need_recycle_truncate_info,
    int64_t &factor)
{
  int ret = OB_SUCCESS;
  int64_t score_row_cnt = tablet.get_inc_row_cnt();
  if (need_recycle_truncate_info || tablet.need_progressive_merge()) {
    score_row_cnt += tablet.get_last_major_row_count();
  }
  int64_t days_since_last_compaction = 0;
  const int64_t last_major_snapshot_version = tablet.get_last_major_snapshot_version();
  // Get time duration since last compaction.
  // If tablet never do major/medium since created, need to estimate it.
  if (OB_LIKELY(last_major_snapshot_version > 1)) {
#ifdef ERRSIM
    int64_t errsim_days_ns = 0;
    if (OB_SUCCESS != EN_LONG_TIME_NOT_MAJOR_FREEZE_SIMULATION_TABLET_ID &&
        OB_SUCCESS != EN_LONG_TIME_NOT_MAJOR_FREEZE_SIMULATION_DAYS) {
      const int64_t errsim_tablet_id = -EN_LONG_TIME_NOT_MAJOR_FREEZE_SIMULATION_TABLET_ID;
      const int64_t errsim_days = -EN_LONG_TIME_NOT_MAJOR_FREEZE_SIMULATION_DAYS;
      if (errsim_tablet_id == tablet.get_tablet_id().id()) {
        errsim_days_ns = errsim_days * NS_PER_DAY;
        LOG_INFO("ERRSIM EN_LONG_TIME_NOT_MAJOR_FREEZE_SIMULATION, set last major snapshot version to errsim timestamp",
                 K(errsim_tablet_id), K(errsim_days), K(errsim_days_ns));
      }
    }
    days_since_last_compaction = calculate_days_since_timestamp(last_major_snapshot_version - errsim_days_ns);
#else
    days_since_last_compaction = calculate_days_since_timestamp(last_major_snapshot_version);
#endif
  } else if (OB_INVALID_SCN_VAL != tablet.get_tablet_meta().create_scn_.get_val_for_gts()) {
    // only tablet created by replaying mds log with valid create_scn
    days_since_last_compaction = calculate_days_since_timestamp(tablet.get_tablet_meta().create_scn_.get_val_for_tx());
  } else if (tablet.get_inc_row_cnt() <= 0) {
    // tablet need recycle truncate info or need progressive merge may with 0 incremental row count, use clog checkpoint scn to estimate
    days_since_last_compaction = calculate_days_since_timestamp(tablet.get_clog_checkpoint_scn().get_val_for_tx());
  } else if (OB_FAIL(estimate_days_by_sstable(tablet, days_since_last_compaction))) {
    LOG_WARN("failed to estimate days by sstable", K(ret), K(tablet));
  }

  if (OB_SUCC(ret)) {
    factor = score_row_cnt * MAX(1, days_since_last_compaction / 2 - 1);
  }
  return ret;
}

int ObWindowCompactionUtils::estimate_days_by_sstable(
    storage::ObTablet &tablet,
    int64_t &days)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  days = 0;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), K(tablet));
  } else if (OB_ISNULL(table_store = table_store_wrapper.get_member()) || OB_UNLIKELY(!table_store->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is nullptr or invalid", K(ret), K(tablet));
  } else {
    // The first mini/minor/inc_major sstable, no matter whether it is covered by the base table
    // its end_scn is the oldest time that can be determined
    const ObSSTable *first_minor_sstable = table_store->get_minor_sstables().at(0);
    const ObSSTable *first_inc_major_sstable = table_store->get_inc_major_sstables().at(0);
    if (OB_NOT_NULL(first_minor_sstable)) {
      days = MAX(days, calculate_days_since_timestamp(first_minor_sstable->get_end_scn().get_val_for_tx()));
    }
    if (OB_NOT_NULL(first_inc_major_sstable)) {
      days = MAX(days, calculate_days_since_timestamp(first_inc_major_sstable->get_end_scn().get_val_for_tx()));
    }
  }
  return ret;
}

int64_t ObWindowCompactionUtils::calculate_days_since_timestamp(const int64_t timestamp)
{
  if (timestamp <= DEFENSIVE_TIMESTAMP) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "timestamp is too old, the calculation is not accurate", K(timestamp));
  }
  return (ObTimeUtility::current_time_ns() - timestamp) / NS_PER_DAY;
}

/*------------------------ ObWindowCompactionBaseContainerGuard ----------------------*/
ObWindowCompactionBaseContainerGuard::ObWindowCompactionBaseContainerGuard(ObWindowCompactionBaseContainer &container)
  : lock_(container.lock_),
    is_locked_(false)
{
}

int ObWindowCompactionBaseContainerGuard::acquire()
{
  int ret = OB_SUCCESS;
  if (is_locked_) {
    // already locked
  } else if (OB_FAIL(lock_.wlock()->lock())) {
    LOG_WARN("failed to lock lock", K(ret));
  } else {
    is_locked_ = true;
  }
  return ret;
}

void ObWindowCompactionBaseContainerGuard::release()
{
  if (is_locked_) {
    (void) lock_.wlock()->unlock();
    is_locked_ = false;
  }
}


/*------------------------ ObWindowCompactionPriorityQueueIterator ----------------------*/
ObWindowCompactionPriorityQueueIterator::ObWindowCompactionPriorityQueueIterator(ObWindowCompactionPriorityQueue &priority_queue)
  : ObWindowCompactionBaseContainerGuard(priority_queue),
    queue_(priority_queue),
    idx_(0),
    count_(0),
    is_inited_(false)
{
}

int ObWindowCompactionPriorityQueueIterator::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else if (OB_FAIL(acquire())) {
    LOG_WARN("failed to acquire lock", K(ret));
  } else {
    idx_ = 0;
    count_ = queue_.score_prio_queue_.count();
    is_inited_ = true;
  }
  return ret;
}

void ObWindowCompactionPriorityQueueIterator::destroy()
{
  idx_ = 0;
  count_ = 0;
  release();
  is_inited_ = false;
}

int ObWindowCompactionPriorityQueueIterator::get_next(ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("is not inited", K(ret));
  } else if (idx_ < count_) {
    score = queue_.score_prio_queue_.at(idx_);
    idx_++;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObWindowCompactionPriorityQueueIterator::get_queue_summary(ObSqlString &queue_summary) const
{
  int ret = OB_SUCCESS;
  ObWindowCompactionQueueInfo info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("is not inited", K(ret));
  } else if (OB_FAIL(queue_.inner_fetch_info(info))) {
    LOG_WARN("failed to fetch queue info", K(ret));
  } else if (FALSE_IT(queue_summary.reset())) {
  } else if (OB_FAIL(queue_summary.append_fmt("Queue Summary: len: %ld, max: %ld, min: %ld, mid: %ld, threshold: %ld, avg: %f",
                                              info.length_, info.max_score_, info.min_score_, info.mid_score_, info.threshold_, info.avg_score_))) {
    LOG_WARN("failed to append queue summary", K(ret));
  }
  return ret;
}

/*------------------------ ObWindowCompactionReadyListIterator ----------------------*/
ObWindowCompactionReadyListIterator::ObWindowCompactionReadyListIterator(ObWindowCompactionReadyList &ready_list)
  : ObWindowCompactionBaseContainerGuard(ready_list),
    list_(ready_list),
    iter_(),
    end_iter_(),
    is_inited_(false)
{
}

int ObWindowCompactionReadyListIterator::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else if (OB_FAIL(acquire())) {
    LOG_WARN("failed to acquire lock", K(ret));
  } else {
    iter_ = list_.candidate_map_.begin();
    end_iter_ = list_.candidate_map_.end();
    is_inited_ = true;
  }
  return ret;
}

void ObWindowCompactionReadyListIterator::destroy()
{
  release();
  // iter and end_iter don't support destroy
  is_inited_ = false;
}

int ObWindowCompactionReadyListIterator::get_next(ObTabletCompactionScore *&score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("is not inited", K(ret));
  } else if (iter_ != end_iter_) {
    score = iter_->second;
    iter_++;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObWindowCompactionReadyListIterator::get_list_summary(ObSqlString &list_summary) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("is not inited", K(ret));
  } else if (FALSE_IT(list_summary.reset())) {
  } else if (OB_FAIL(list_summary.append_fmt("List Summary: size :%ld, capacity: %ld",
                                             list_.get_current_weighted_size(), list_.get_max_capacity()))) {
    LOG_WARN("failed to append list summary", K(ret));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase