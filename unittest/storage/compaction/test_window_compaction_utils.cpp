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

#include <gmock/gmock.h>

#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/compaction/ob_window_compaction_utils.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace storage;
using namespace compaction;

namespace unittest
{
class TestWindowCompaction : public ::testing::Test
{
public:
  TestWindowCompaction();
  virtual ~TestWindowCompaction() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static int get_score_index(
    ObWindowCompactionPriorityQueue &priority_queue,
    const ObTabletCompactionScoreKey &key,
    int64_t &index);
  static ObTabletCompactionScoreDecisionInfo get_valid_decision_info(const int64_t cg_merge_batch_cnt = 3);
private:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
  ObTenantTabletStatMgr *stat_mgr_;
  ObTimerService *timer_service_;
};

TestWindowCompaction::TestWindowCompaction()
  : tenant_id_(1),
    tenant_base_(tenant_id_),
    stat_mgr_(nullptr),
    timer_service_(nullptr)
{
}

void TestWindowCompaction::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestWindowCompaction::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestWindowCompaction::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;

  timer_service_ = OB_NEW(ObTimerService, ObModIds::TEST, tenant_id_);
  ASSERT_NE(nullptr, timer_service_);
  ASSERT_EQ(OB_SUCCESS, timer_service_->start());
  tenant_base_.set(timer_service_);
  stat_mgr_ = OB_NEW(ObTenantTabletStatMgr, ObModIds::TEST);
  ret = stat_mgr_->init(tenant_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  tenant_base_.set(stat_mgr_);

  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  ASSERT_EQ(tenant_id_, MTL_ID());
  ASSERT_EQ(stat_mgr_, MTL(ObTenantTabletStatMgr *));
  ASSERT_EQ(timer_service_, MTL(ObTimerService *));
}

void TestWindowCompaction::TearDown()
{
  stat_mgr_->destroy();
  if (nullptr != timer_service_) {
    timer_service_->stop();
    timer_service_->wait();
    timer_service_->destroy();
    OB_DELETE(ObTimerService, ObModIds::TEST, timer_service_);
  }
  ObTenantEnv::set_tenant(nullptr);
}

int TestWindowCompaction::get_score_index(
    ObWindowCompactionPriorityQueue &priority_queue,
    const ObTabletCompactionScoreKey &key,
    int64_t &index)
{
  int ret = OB_SUCCESS;
  index = -1;
  bool exist = false;
  ObTabletCompactionScore *score = nullptr;
  obsys::ObRWLock<> &lock = priority_queue.lock_;
  obsys::ObWLockGuard guard(lock);
  if (OB_UNLIKELY(!guard.acquired())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to acquire lock, retry again", K(ret));
  } else if (OB_FAIL(priority_queue.inner_check_exist_and_get_score(key, exist, score))) {
    LOG_WARN("failed to check exist and get score", K(ret), K(key));
  } else if (exist) {
    index = score->pos_at_priority_queue_;
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

ObTabletCompactionScoreDecisionInfo TestWindowCompaction::get_valid_decision_info(const int64_t cg_merge_batch_cnt)
{
  ObTabletCompactionScoreDecisionInfo decision_info;
  decision_info.dynamic_info_.need_recycle_mds_ = false;
  decision_info.dynamic_info_.cg_merge_batch_cnt_ = cg_merge_batch_cnt;
  decision_info.base_inc_row_cnt_ = 1;
  decision_info.tablet_snapshot_version_ = 1;
  decision_info.major_snapshot_version_ = 1;
  return decision_info;
}

// Only used to observe the usage of ObTimeConverter
TEST_F(TestWindowCompaction, test_major_freeze_duty_time_zone)
{
  int ret = OB_SUCCESS;
  time_t now = time(NULL); // get current time
  struct tm utc_time;
  ASSERT_NE(gmtime_r(&now, &utc_time), nullptr);
  OB_LOG(INFO, "get current utc time", K(utc_time.tm_hour), K(utc_time.tm_min), K(utc_time.tm_sec), K(utc_time.tm_gmtoff));

  ObString tz_str("+8:00");
  ObString trimed_tz_str = tz_str.trim();
  int ret_more = OB_SUCCESS;
  int offset_sec = 0;
  if (OB_FAIL(ObTimeConverter::str_to_offset(trimed_tz_str, offset_sec, ret_more, false /*is_oracle_mode*/, true /*need_check_valid*/))) {
    if (ret != OB_ERR_UNKNOWN_TIME_ZONE) {
      OB_LOG(WARN, "fail to convert str_to_offset", K(trimed_tz_str), K(ret));
    } else if (OB_FAIL(ret_more)) {
      OB_LOG(WARN, "invalid time zone hour or minute", K(trimed_tz_str), K(ret));
    }
  }

  time_t adjusted_current_time = now + offset_sec;
  struct tm adjust_time;
  ASSERT_NE(gmtime_r(&adjusted_current_time, &adjust_time), nullptr);
  OB_LOG(INFO, "get current time in specific timezone", K(adjust_time.tm_hour), K(adjust_time.tm_min), K(adjust_time.tm_sec), K(adjust_time.tm_gmtoff));
}

TEST_F(TestWindowCompaction, test_window_compaction_priority_queue)
{
  ObWindowCompactionMemoryContext mem_ctx;
  ObWindowCompactionScoreTracker score_tracker;
  const int64_t start_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, score_tracker.init(start_time));
  ObWindowCompactionPriorityQueue priority_queue(mem_ctx, score_tracker);
  ASSERT_EQ(OB_SUCCESS, priority_queue.init());

  const int64_t ITER_CNT = 100;
  ObSEArray<ObTabletCompactionScore *, 128> pushed_scores;
  ObSEArray<ObTabletCompactionScore *, 128> poped_scores;

  for (int64_t i = 0; i < ITER_CNT; ++i) {
    ObTabletCompactionScore *score = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_ctx.alloc_score(score));
    ASSERT_NE(nullptr, score);
    ObTabletCompactionScore *back_score = score;
    score->key_.ls_id_ = 1001;
    score->key_.tablet_id_ = 200001 + i;
    score->score_ = rand() % 1000000 + 1;
    score->decision_info_ = get_valid_decision_info();
    ASSERT_EQ(OB_SUCCESS, priority_queue.push(score));
    ASSERT_EQ(OB_SUCCESS, pushed_scores.push_back(back_score));
    ASSERT_EQ(i + 1, priority_queue.score_prio_queue_.count());
    ASSERT_EQ(i + 1, priority_queue.score_map_.size());
    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, priority_queue.inner_check_exist_and_get_score(back_score->key_, exist, score));
    ASSERT_TRUE(exist);
    LOG_INFO("push score", K(i), KPC(back_score));

    score = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_ctx.alloc_score(score));
    ASSERT_NE(nullptr, score);
    score->key_.ls_id_ = 1001;
    score->key_.tablet_id_ = 200001 + i;
    score->score_ = rand() % 1000000 + 1;
    score->decision_info_ = get_valid_decision_info();
    ASSERT_EQ(OB_HASH_EXIST, priority_queue.push(score));
    mem_ctx.free_score(score);
    ASSERT_EQ(i + 1, priority_queue.score_prio_queue_.count());
    ASSERT_EQ(i + 1, priority_queue.score_map_.size());
  }

  ObSEArray<int64_t, 128> sorted_idxes;
  for (int64_t i = 0; i < ITER_CNT; ++i) {
    const ObTabletCompactionScore *score = pushed_scores[i];
    int64_t idx = -1;
    ASSERT_EQ(OB_SUCCESS, get_score_index(priority_queue, score->key_, idx));
    ASSERT_EQ(OB_SUCCESS, sorted_idxes.push_back(idx));
    ASSERT_EQ(idx, score->pos_at_priority_queue_);
    ASSERT_EQ(score->score_, priority_queue.score_prio_queue_.at(idx)->score_);
    ASSERT_EQ(score->key_.ls_id_, priority_queue.score_prio_queue_.at(idx)->key_.ls_id_);
    ASSERT_EQ(score->key_.tablet_id_, priority_queue.score_prio_queue_.at(idx)->key_.tablet_id_);
    LOG_INFO("get index", K(i), K(idx), K(score->score_), K(score->pos_at_priority_queue_));
  }

  for (int64_t i = 0; i < ITER_CNT; ++i) {
    ObTabletCompactionScore *score = nullptr;
    ASSERT_EQ(OB_SUCCESS, priority_queue.check_and_pop(INT64_MAX, score));
    ASSERT_NE(nullptr, score);
    ObTabletCompactionScore *check_score = nullptr;
    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, priority_queue.inner_check_exist_and_get_score(score->key_, exist, check_score));
    ASSERT_EQ(nullptr, check_score);
    ASSERT_FALSE(exist);
    ASSERT_EQ(ITER_CNT - i - 1, priority_queue.score_prio_queue_.count());
    ASSERT_EQ(ITER_CNT - i - 1, priority_queue.score_map_.size());
    ASSERT_EQ(OB_SUCCESS, poped_scores.push_back(score));
    LOG_INFO("pop score", K(i), KPC(score));
  }
  ASSERT_TRUE(priority_queue.score_prio_queue_.empty());

  // check the scores are sorted by score in descending order
  for (int64_t i = 0; i < ITER_CNT - 1; ++i) {
    ASSERT_GE(poped_scores[i]->score_, poped_scores[i + 1]->score_);
  }

  // free all scores
  for (int64_t i = 0; i < ITER_CNT; ++i) {
    mem_ctx.free_score(poped_scores[i]);
  }

  priority_queue.destroy();
}

TEST_F(TestWindowCompaction, test_window_compaction_ready_list)
{
  ObWindowCompactionMemoryContext mem_ctx;
  ObWindowCompactionScoreTracker score_tracker;
  const int64_t start_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, score_tracker.init(start_time));
  ObWindowCompactionReadyList ready_list(mem_ctx, score_tracker);
  ASSERT_EQ(OB_SUCCESS, ready_list.init());

  const int64_t ITER_CNT = 1000;
  ObSEArray<ObTabletCompactionScore *, 128> candidates;
  int64_t total_weighted_size = 0;

  for (int64_t i = 0; i < ITER_CNT; ++i) {
    ObTabletCompactionScore *score = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_ctx.alloc_score(score));
    ASSERT_NE(nullptr, score);
    score->key_.ls_id_ = 1001;
    score->key_.tablet_id_ = 200001 + i;
    ObTabletCompactionScoreKey key = score->key_;
    score->score_ = rand() % 1000000 + 1;
    bool is_row_store = (i % 2 == 0) ? true : false;
    const int64_t column_cnt = rand() % 100 + 1;
    const int64_t weighted_size = is_row_store ? 1 : MAX(1, ObCOMajorMergePolicy::get_cg_merge_batch_cnt(column_cnt + 1));
    score->decision_info_ = get_valid_decision_info(weighted_size);
    bool exist = false;
    ObTabletCompactionScore *candidate = nullptr;
    ASSERT_EQ(OB_SUCCESS, ready_list.inner_check_exist_and_get_candidate(score->key_, exist, candidate));
    ASSERT_FALSE(exist);
    // weighted_size is random, so the previous one may add failed, but the later one succeed
    if (ready_list.get_remaining_space() >= weighted_size) {
      ASSERT_EQ(OB_SUCCESS, ready_list.add(score));
      ASSERT_EQ(nullptr, score);
      total_weighted_size += weighted_size;
      ASSERT_EQ(candidates.count() + 1, ready_list.candidate_map_.size());
      ASSERT_EQ(total_weighted_size, ready_list.weighted_size_);
      candidate = nullptr;
      exist = false;
      ASSERT_EQ(OB_SUCCESS, ready_list.inner_check_exist_and_get_candidate(key, exist, candidate));
      ASSERT_TRUE(exist);
      ASSERT_NE(nullptr, candidate);
      ASSERT_EQ(OB_SUCCESS, candidates.push_back(candidate));
    } else {
      ASSERT_EQ(OB_EAGAIN, ready_list.add(score));
      mem_ctx.free_score(score);
    }
  }

  ASSERT_EQ(candidates.count(), ready_list.candidate_map_.size());

  for (int64_t i = 0; i < candidates.count(); ++i) {
    ObTabletCompactionScore *candidate = candidates[i];
    ASSERT_NE(nullptr, candidate);
    ObTabletCompactionScore *score = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_ctx.alloc_score(score));
    ASSERT_NE(nullptr, score);
    score->key_ = candidate->key_;
    score->decision_info_ = candidate->decision_info_;
    score->score_ = candidate->score_;
    bool exist = false;
    ObTabletCompactionScore *ready_candidate = nullptr;
    ASSERT_EQ(OB_SUCCESS, ready_list.inner_check_exist_and_get_candidate(score->key_, exist, ready_candidate));
    ASSERT_TRUE(exist);
    ASSERT_NE(nullptr, ready_candidate);
    ASSERT_EQ(candidate, ready_candidate);
    ASSERT_TRUE(candidate->is_ready_status());

    candidate->set_log_submitted_status();
    ASSERT_EQ(OB_EAGAIN, ready_list.add(score));
    ASSERT_NE(nullptr, score);

    candidate->compact_status_ = ObTabletCompactionScore::COMPACT_STATUS_READY;
    ASSERT_NE(score, candidate);
    int64_t old_total_weighted_size = ready_list.weighted_size_;
    int64_t new_score = candidate->score_ + 10;
    int64_t old_size = candidate->get_weighted_size();
    int64_t new_cg_merge_batch_cnt = candidate->decision_info_.dynamic_info_.cg_merge_batch_cnt_ + 3;
    const int64_t new_size = new_cg_merge_batch_cnt;
    score->decision_info_ = candidate->decision_info_;
    score->decision_info_.dynamic_info_.cg_merge_batch_cnt_ = new_cg_merge_batch_cnt;
    score->score_ = new_score;
    ASSERT_EQ(OB_SUCCESS, ready_list.add(score));
    ASSERT_EQ(nullptr, score);
    ASSERT_EQ(new_score, candidate->score_);
    ASSERT_EQ(new_size, candidate->get_weighted_size());
    ASSERT_EQ(old_total_weighted_size - old_size + new_size, ready_list.weighted_size_);
  }

  ready_list.destroy();
}

TEST_F(TestWindowCompaction, test_priority_queue_avg_score)
{
  ObWindowCompactionMemoryContext mem_ctx;
  ObWindowCompactionScoreTracker score_tracker;
  const int64_t start_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, score_tracker.init(start_time));
  ObWindowCompactionPriorityQueue priority_queue(mem_ctx, score_tracker);
  ASSERT_EQ(OB_SUCCESS, priority_queue.init());
  // 1. initial avg_score should be 0
  ASSERT_EQ(0, priority_queue.get_average_score());
  ASSERT_DOUBLE_EQ(0.0, priority_queue.avg_score_);

  // 2. test avg_score calculation
  const int64_t THRESHOLD_MIN = ObWindowCompactionPriorityQueue::BASE_THRESHOLD;
  const int64_t THRESHOLD_LOW = THRESHOLD_MIN + ObWindowCompactionPriorityQueue::BASE_THRESHOLD;
  const int64_t THRESHOLD_MID = THRESHOLD_LOW + ObWindowCompactionPriorityQueue::BASE_THRESHOLD;
  double total_score_value = 0.0;
  for (int64_t i = 0; i < THRESHOLD_MIN; ++i) {
    const int64_t score_value = rand() % 1000 + 1;
    total_score_value += score_value;
    const double avg_score = total_score_value / (i + 1);

    ObTabletCompactionScore *score = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_ctx.alloc_score(score));
    score->key_.ls_id_ = 1001;
    score->key_.tablet_id_ = 200001 + i;
    score->decision_info_ = get_valid_decision_info();
    score->score_ = score_value;
    ASSERT_EQ(OB_SUCCESS, priority_queue.check_admission(score->score_));
    ASSERT_EQ(OB_SUCCESS, priority_queue.push(score));
    double diff = std::abs(avg_score - priority_queue.avg_score_);
    ASSERT_LT(diff, 1e-5);
  }
  ASSERT_EQ(THRESHOLD_MIN, priority_queue.score_prio_queue_.count());

  // 3. check addmission
  // 3.1 when size in [THRESHOLD_MIN, THRESHOLD_LOW) 50000 ~ 100000, only score > 0.5 * avg_score can be pushed
  int64_t reject_score_value = std::max(1L, static_cast<int64_t>(priority_queue.avg_score_ / 2 - 1));
  ASSERT_EQ(OB_EAGAIN, priority_queue.check_admission(reject_score_value));
  for (int64_t i = THRESHOLD_MIN; i < THRESHOLD_LOW; ++i) {
    const int64_t score_value = static_cast<int64_t>(priority_queue.avg_score_ / 2) + 1;
    total_score_value += score_value;
    const double avg_score = total_score_value / (i + 1);

    ObTabletCompactionScore *score = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_ctx.alloc_score(score));
    score->key_.ls_id_ = 1001;
    score->key_.tablet_id_ = 200001 + i;
    score->decision_info_ = get_valid_decision_info();
    score->score_ = score_value;
    ASSERT_EQ(OB_SUCCESS, priority_queue.check_admission(score_value));
    ASSERT_EQ(OB_SUCCESS, priority_queue.push(score));
    double diff = std::abs(avg_score - priority_queue.avg_score_);
    ASSERT_LT(diff, 1e-5);
  }
  ASSERT_EQ(THRESHOLD_LOW, priority_queue.score_prio_queue_.count());

  // 3.2 when size in [THRESHOLD_LOW, THRESHOLD_MID) 100000 ~ 150000, only score > avg_score can be pushed
  reject_score_value = std::max(1L, static_cast<int64_t>(priority_queue.avg_score_ - 1));
  ASSERT_EQ(OB_EAGAIN, priority_queue.check_admission(reject_score_value));
  for (int64_t i = THRESHOLD_LOW; i < THRESHOLD_MID; ++i) {
    const int64_t score_value = static_cast<int64_t>(priority_queue.avg_score_) + 1;
    total_score_value += score_value;
    const double avg_score = total_score_value / (i + 1);

    ObTabletCompactionScore *score = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_ctx.alloc_score(score));
    score->key_.ls_id_ = 1001;
    score->key_.tablet_id_ = 200001 + i;
    score->decision_info_ = get_valid_decision_info();
    score->score_ = score_value;
    ASSERT_EQ(OB_SUCCESS, priority_queue.check_admission(score_value));
    ASSERT_EQ(OB_SUCCESS, priority_queue.push(score));
    double diff = std::abs(avg_score - priority_queue.avg_score_);
    ASSERT_LT(diff, 1e-5);
  }
  ASSERT_EQ(THRESHOLD_MID, priority_queue.score_prio_queue_.count());

  // 3.3 when size in [THRESHOLD_MID, THRESHOLD_MAX) 150000 ~ 200000, only score > 1.5 * avg_score can be pushed
  reject_score_value = std::max(1L, static_cast<int64_t>(priority_queue.avg_score_ * 3 / 2 - 1));
  ASSERT_EQ(OB_EAGAIN, priority_queue.check_admission(reject_score_value));
  for (int64_t i = THRESHOLD_MID; i < ObWindowCompactionPriorityQueue::THRESHOLD_MAX; ++i) {
    const int64_t score_value = static_cast<int64_t>(priority_queue.avg_score_ * 3 / 2) + 1;
    total_score_value += score_value;
    const double avg_score = total_score_value / (i + 1);

    ObTabletCompactionScore *score = nullptr;
    ASSERT_EQ(OB_SUCCESS, mem_ctx.alloc_score(score));
    score->key_.ls_id_ = 1001;
    score->key_.tablet_id_ = 200001 + i;
    score->decision_info_ = get_valid_decision_info();
    score->score_ = score_value;
    ASSERT_EQ(OB_SUCCESS, priority_queue.check_admission(score_value));
    ASSERT_EQ(OB_SUCCESS, priority_queue.push(score));
    double diff = std::abs(avg_score - priority_queue.avg_score_);
    ASSERT_LT(diff, 1e-5);
  }
  ASSERT_EQ(ObWindowCompactionPriorityQueue::THRESHOLD_MAX, priority_queue.score_prio_queue_.count());

  // 3.4 when size > THRESHOLD_MAX 200000, no score can be pushed
  reject_score_value = static_cast<int64_t>(priority_queue.avg_score_ * 3 / 2) + 1000;
  ASSERT_EQ(OB_SIZE_OVERFLOW, priority_queue.check_admission(reject_score_value));

  // 3.5 check avg score when inplace update
  for (int64_t i = 0; i < ObWindowCompactionPriorityQueue::THRESHOLD_MAX; ++i) {
    ObTabletCompactionScore *score = priority_queue.score_prio_queue_.at(i);
    const ObTabletCompactionScoreDecisionInfo &decision_info = score->decision_info_;
    const int64_t increment = rand() % 1000 + 1;
    const int64_t new_score = score->score_ + increment;
    total_score_value += increment;
    const double avg_score = total_score_value / (ObWindowCompactionPriorityQueue::THRESHOLD_MAX);
    ASSERT_EQ(OB_SUCCESS, priority_queue.inner_update_with_new_score(score, decision_info, new_score));
    ASSERT_EQ(new_score, score->score_);
    double diff = std::abs(avg_score - priority_queue.avg_score_);
    ASSERT_LT(diff, 1e-5);
  }

  // 3.6 check avg score when pop
  for (int64_t i = 0; i < ObWindowCompactionPriorityQueue::THRESHOLD_MAX; ++i) {
    ObTabletCompactionScore *score = nullptr;
    ASSERT_EQ(OB_SUCCESS, priority_queue.check_and_pop(INT64_MAX, score));
    ASSERT_NE(nullptr, score);
    const int64_t count = ObWindowCompactionPriorityQueue::THRESHOLD_MAX - i - 1;
    const int64_t score_value = score->score_;
    total_score_value -= score_value;
    const double avg_score = count > 0 ? total_score_value / count : 0.0;
    double ratio = count > 0 ? avg_score / priority_queue.avg_score_ : 1.0;
    double diff = std::abs(1 - ratio);
    LOG_INFO("check avg score when pop", K(i), K(count), K(score_value), K(total_score_value), K(avg_score), K(priority_queue.avg_score_), K(ratio), K(diff));
    ASSERT_LT(diff, 1e-2);
    mem_ctx.free_score(score);
  }
  priority_queue.destroy();
}

TEST_F(TestWindowCompaction, test_million_tablet_score_storage)
{
  ObWindowCompactionScoreTracker score_tracker;
  ObArenaAllocator &allocator = score_tracker.allocator_;
  const int64_t start_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, score_tracker.init(start_time));
  const int64_t ONE_MILLION = 1 * 1000 * 1000;
  const int64_t LOOP_CNT = 2 * ONE_MILLION;
  const int64_t STEP = 50000;
  for (int64_t i = 0; i < LOOP_CNT; ++i) {
    const ObTabletID tablet_id(200001 + i);
    const int64_t score = rand() % 1000000 + 1;
    ASSERT_EQ(OB_SUCCESS, score_tracker.score_map_.set_refactored(tablet_id, score));
    int64_t output_score = 0;
    ASSERT_EQ(OB_SUCCESS, score_tracker.score_map_.get_refactored(tablet_id, output_score));
    ASSERT_EQ(score, output_score);
    ASSERT_EQ(i + 1, score_tracker.score_map_.size());
    if ((i+1) % STEP == 0) {
      LOG_INFO("finish test million tablet score storage", "step", i+1, "total", allocator.total(), "used", allocator.used());
    }
  }
  score_tracker.reset();
  LOG_INFO("after reset", "total", allocator.total(), "used", allocator.used());
}

TEST_F(TestWindowCompaction, test_window_compaction_score_tracker)
{
  ObWindowCompactionScoreTracker score_tracker;
  const ObTabletID tablet_id(200001);
  ObTabletCompactionScoreDecisionInfo decision_info = get_valid_decision_info();

  // 1. Test without init
  ASSERT_EQ(OB_NOT_INIT, score_tracker.upsert(tablet_id, nullptr, decision_info, 1000));

  // 2. Test insert new tablet (INSERTED)
  const int64_t start_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, score_tracker.init(start_time));
  const int64_t score1 = 1000;
  ASSERT_EQ(OB_SUCCESS, score_tracker.upsert(tablet_id, nullptr, decision_info, score1));
  int64_t stored_score = 0;
  ASSERT_EQ(OB_SUCCESS, score_tracker.score_map_.get_refactored(tablet_id, stored_score));
  ASSERT_EQ(score1, stored_score);

  // 3. Test update with larger score (UPDATED)
  const int64_t score2 = 2000;
  ASSERT_EQ(OB_SUCCESS, score_tracker.upsert(tablet_id, nullptr, decision_info, score2));
  ASSERT_EQ(OB_SUCCESS, score_tracker.score_map_.get_refactored(tablet_id, stored_score));
  ASSERT_EQ(score2, stored_score);

  // 4. Test update with smaller score (SKIPED)
  const int64_t score3 = 1500;
  ASSERT_EQ(OB_SUCCESS, score_tracker.upsert(tablet_id, nullptr, decision_info, score3));
  ASSERT_EQ(OB_SUCCESS, score_tracker.score_map_.get_refactored(tablet_id, stored_score));
  ASSERT_EQ(score2, stored_score); // should still be score2 (2000)

  // 5. Test update with equal score (SKIPED)
  ASSERT_EQ(OB_SUCCESS, score_tracker.upsert(tablet_id, nullptr, decision_info, score2));
  ASSERT_EQ(OB_SUCCESS, score_tracker.score_map_.get_refactored(tablet_id, stored_score));
  ASSERT_EQ(score2, stored_score); // should still be score2 (2000)

  // 6. Test multiple tablets and verify only highest score is kept
  const int64_t TABLET_CNT = 1000;
  for (int64_t i = 1; i < TABLET_CNT; ++i) {
    const ObTabletID tablet_id(200001 + i);
    const int64_t initial_score = rand() % 10000 + 1;
    ASSERT_EQ(OB_SUCCESS, score_tracker.upsert(tablet_id, nullptr, decision_info, initial_score));

    // Try to update with smaller score, should be skipped
    const int64_t smaller_score = initial_score / 2;
    ASSERT_EQ(OB_SUCCESS, score_tracker.upsert(tablet_id, nullptr, decision_info, smaller_score));
    int64_t stored_score = 0;
    ASSERT_EQ(OB_SUCCESS, score_tracker.score_map_.get_refactored(tablet_id, stored_score));
    ASSERT_EQ(initial_score, stored_score); // should keep the larger score

    // Update with larger score, should succeed
    const int64_t larger_score = initial_score * 2;
    ASSERT_EQ(OB_SUCCESS, score_tracker.upsert(tablet_id, nullptr, decision_info, larger_score));
    ASSERT_EQ(OB_SUCCESS, score_tracker.score_map_.get_refactored(tablet_id, stored_score));
    ASSERT_EQ(larger_score, stored_score);
  }
  ASSERT_EQ(TABLET_CNT, score_tracker.score_map_.size());

  // 7. Test reset
  score_tracker.reset();
  ASSERT_FALSE(score_tracker.is_inited_);
  ASSERT_EQ(0, score_tracker.score_map_.size());

  // 8. Test re-init after reset
  const int64_t start_time2 = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, score_tracker.init(start_time2));
  const int64_t score4 = 5000;
  ASSERT_EQ(OB_SUCCESS, score_tracker.upsert(tablet_id, nullptr, decision_info, score4));
  ASSERT_EQ(OB_SUCCESS, score_tracker.score_map_.get_refactored(tablet_id, stored_score));
  ASSERT_EQ(score4, stored_score);
  ASSERT_EQ(1, score_tracker.score_map_.size());

  // 9. Test with analyzer (should log with analyzer info)
  storage::ObTabletStatAnalyzer analyzer;
  const int64_t score5 = 6000;
  ASSERT_EQ(OB_SUCCESS, score_tracker.upsert(tablet_id, &analyzer, decision_info, score5));
  ASSERT_EQ(OB_SUCCESS, score_tracker.score_map_.get_refactored(tablet_id, stored_score));
  ASSERT_EQ(score5, stored_score);

  score_tracker.reset();
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_window_compaction_utils.log*");
  OB_LOGGER.set_file_name("test_window_compaction_utils.log", true);
  OB_LOGGER.set_log_level("TRACE");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}