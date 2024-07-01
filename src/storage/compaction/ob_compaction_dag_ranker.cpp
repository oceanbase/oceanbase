/**
 * Copyright (c) 2022 OceanBase
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
#include "ob_compaction_dag_ranker.h"
#include "ob_tablet_merge_task.h"
#include "lib/container/ob_array_iterator.h"
#include "share/scheduler/ob_dag_scheduler_config.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace storage;

namespace compaction
{

// TODO(@DanLing) optimize compaction mem usage
int ObCompactionEstimator::estimate_compaction_memory(
    const int64_t priority,
    const ObCompactionParam &param,
    int64_t &estimate_mem_usage)
{
  int ret = OB_SUCCESS;
  estimate_mem_usage = 0;

  if (ObDagPrio::DAG_PRIO_COMPACTION_HIGH != priority &&
      ObDagPrio::DAG_PRIO_COMPACTION_MID != priority &&
      ObDagPrio::DAG_PRIO_COMPACTION_LOW != priority) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(priority));
  } else if (ObDagPrio::DAG_PRIO_COMPACTION_LOW == priority) {
    if (param.estimate_concurrent_cnt_ <= 0 || param.sstable_cnt_ <= 0 || param.batch_size_ <= 0) {
      // not ObCOMergeBatchExeDag, use default mem usage.
      estimate_mem_usage = DEFAULT_COMPACTION_MEM;
    } else {
      estimate_mem_usage = COMPACTION_RESERVED_MEM + COMPACTION_BLOCK_FIXED_MEM;
      estimate_mem_usage += COMPACTION_ITER_BASE_MEM * param.estimate_concurrent_cnt_ * param.sstable_cnt_;
      estimate_mem_usage += COMPACTION_CONCURRENT_MEM_FACTOR * param.estimate_concurrent_cnt_ * param.batch_size_;
    }
  } else { // mini && minor compaction
    estimate_mem_usage = COMPACTION_RESERVED_MEM + COMPACTION_BLOCK_FIXED_MEM;
    estimate_mem_usage += COMPACTION_CONCURRENT_MEM_FACTOR * MAX(1, param.estimate_concurrent_cnt_);
    if (ObDagPrio::DAG_PRIO_COMPACTION_MID == priority) {
      estimate_mem_usage += COMPACTION_ITER_BASE_MEM * MAX(1, param.estimate_concurrent_cnt_) * MAX(2, param.parallel_sstable_cnt_);
    }
  }
  return ret;
}

int64_t ObCompactionEstimator::estimate_compaction_batch_size(
    const compaction::ObMergeType merge_type,
    const int64_t compaction_mem_limit,
    const int64_t concurrent_cnt,
    const int64_t sstable_cnt)
{
  int64_t mem_allow_batch_size = DEFAULT_BATCH_SIZE;

  if (concurrent_cnt <= 0 || sstable_cnt <= 0) {
    // do nothing
  } else if (is_major_merge_type(merge_type)) {
    int64_t base_mem_usage = COMPACTION_RESERVED_MEM + COMPACTION_BLOCK_FIXED_MEM;
    base_mem_usage += COMPACTION_ITER_BASE_MEM * concurrent_cnt * sstable_cnt;

    mem_allow_batch_size = (compaction_mem_limit - base_mem_usage) / (COMPACTION_CONCURRENT_MEM_FACTOR * concurrent_cnt);
    mem_allow_batch_size = MAX(mem_allow_batch_size, 1);
  }
  return mem_allow_batch_size;
}


#define CALCULATE_NORMALIZED_RANK_SCORE(dimension, val, weight, score)             \
  ({                                                                               \
    int ret = OB_SUCCESS;                                                          \
    const int64_t min_val = min_ ## dimension ## _;                                \
    const int64_t max_val = max_ ## dimension ## _;                                \
    const int64_t cur_val = val;                                                   \
    if (min_val == max_val) {                                                      \
      /* do nothing */                                                             \
    } else if (cur_val < min_val || cur_val > max_val) {                           \
      ret = OB_ERR_UNEXPECTED;                                                     \
      LOG_WARN("get unexpected val", K(ret), K(cur_val), K(min_val), K(max_val));  \
    } else {                                                                       \
      uint64_t normalized_val = (cur_val - min_val) * 10000 / (max_val - min_val); \
      score += normalized_val * weight;                                            \
    }                                                                              \
    ret;                                                                           \
  })                                                                               \


ObCompactionRankHelper::ObCompactionRankHelper(const int64_t rank_time)
  : rank_time_(rank_time),
    max_occupy_size_(0),
    min_occupy_size_(INT64_MAX),
    max_wait_time_(0),
    min_wait_time_(INT64_MAX),
    max_sstable_cnt_(0),
    min_sstable_cnt_(INT16_MAX)
{
}

bool ObCompactionRankHelper::is_valid() const
{
  bool bret = true;
  if (max_occupy_size_ < min_occupy_size_ ||
      max_wait_time_ < min_wait_time_ ||
      max_sstable_cnt_ < min_sstable_cnt_) {
    bret = false;
  }
  return bret;
}

bool ObCompactionRankHelper::check_need_rank() const
{
  bool bret = true;
  if (OB_UNLIKELY(!is_valid())) {
    bret = false;
  } else if (max_occupy_size_ == min_occupy_size_ &&
      max_wait_time_ == min_wait_time_ &&
      max_sstable_cnt_ == min_sstable_cnt_) {
    bret = false;
  }
  return bret;
}

void ObCompactionRankHelper::update(
    const int64_t current_time,
    const ObCompactionParam &param)
{
  if (rank_time_ == current_time) {
    max_occupy_size_ = MAX(max_occupy_size_, param.occupy_size_);
    min_occupy_size_ = MIN(min_occupy_size_, param.occupy_size_);

    int64_t cur_wait_time = current_time - param.add_time_;
    max_wait_time_ = MAX(max_wait_time_, cur_wait_time);
    min_wait_time_ = MIN(min_wait_time_, cur_wait_time);

    max_sstable_cnt_ = MAX(max_sstable_cnt_, param.sstable_cnt_);
    min_sstable_cnt_ = MIN(min_sstable_cnt_, param.sstable_cnt_);
  }
}


ObMiniCompactionRankHelper::ObMiniCompactionRankHelper(const int64_t rank_time)
  : ObCompactionRankHelper(rank_time),
    max_replay_interval_(0),
    min_replay_interval_(INT64_MAX)
{
}

bool ObMiniCompactionRankHelper::is_valid() const
{
  bool bret = true;
  if (!ObCompactionRankHelper::is_valid() ||
      max_replay_interval_ < min_replay_interval_) {
    bret = false;
  }
  return bret;
}

bool ObMiniCompactionRankHelper::check_need_rank() const
{
  bool bret = true;
  if (ObCompactionRankHelper::check_need_rank()) {
  } else if (max_replay_interval_ <= min_replay_interval_) {
    bret = false;
  }
  return bret;
}

void ObMiniCompactionRankHelper::update(
    const int64_t current_time,
    const ObCompactionParam &param)
{
  if (rank_time_ == current_time) {
    ObCompactionRankHelper::update(current_time, param);

    max_replay_interval_ = MAX(max_replay_interval_, param.replay_interval_);
    min_replay_interval_ = MIN(min_replay_interval_, param.replay_interval_);
  }
}

int ObMiniCompactionRankHelper::get_rank_weighed_score(
    common::ObSEArray<compaction::ObTabletMergeDag *, 32> &dags) const
{
  int ret = OB_SUCCESS;
  const int8_t SIZE_WEIGHT = -5;
  const int8_t FIFO_WIEGHT = -2;
  const int8_t REPLAY_WEIGHT = 3;
  const int8_t SSTABLE_CNT_WEIGHT = 2;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), KPC(this));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dags.count(); ++i) {
    ObTabletMergeDag *cur_dag = dags.at(i);
    if (OB_ISNULL(cur_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null compaction dag", K(ret), K(i), K(dags));
    } else {
      ObCompactionParam &param = cur_dag->get_param().compaction_param_;
      param.score_ = 0; // the greater score, the lower priority

      if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(occupy_size, param.occupy_size_, SIZE_WEIGHT, param.score_))) {
        LOG_WARN("failed to calculate rank score for occupy size", K(ret), K(param));
      } else if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(replay_interval, param.replay_interval_, REPLAY_WEIGHT, param.score_))) {
        LOG_WARN("failed to calculate rank score for replay interval", K(ret), K(param));
      } else if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(wait_time, rank_time_ - param.add_time_, FIFO_WIEGHT, param.score_))) {
        LOG_WARN("failed to calculate rank score for wait time", K(ret), K(param));
      } else if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(sstable_cnt, param.sstable_cnt_, SSTABLE_CNT_WEIGHT, param.score_))) {
        LOG_WARN("failed to calculate rank score for sstable cnt", K(ret), K(param));
      }
    }
  }
  return ret;
}


ObMinorCompactionRankHelper::ObMinorCompactionRankHelper(const int64_t rank_time)
  : ObCompactionRankHelper(rank_time),
    max_parallel_dag_cnt_(0),
    min_parallel_dag_cnt_(INT64_MAX)
{
}

bool ObMinorCompactionRankHelper::is_valid() const
{
  bool bret = true;
  if (!ObCompactionRankHelper::is_valid() ||
      max_parallel_dag_cnt_ < min_parallel_dag_cnt_) {
    bret = false;
  }
  return bret;
}

bool ObMinorCompactionRankHelper::check_need_rank() const
{
  bool bret = true;
  if (ObCompactionRankHelper::check_need_rank()) {
  } else if (max_parallel_dag_cnt_ <= min_parallel_dag_cnt_) {
    bret = false;
  }
  return bret;
}

void ObMinorCompactionRankHelper::update(
    const int64_t current_time,
    const ObCompactionParam &param)
{
  if (rank_time_ == current_time) {
    ObCompactionRankHelper::update(current_time, param);

    max_parallel_dag_cnt_ = MAX(max_parallel_dag_cnt_, param.parallel_dag_cnt_);
    min_parallel_dag_cnt_ = MIN(min_parallel_dag_cnt_, param.parallel_dag_cnt_);
  }
}

int ObMinorCompactionRankHelper::get_rank_weighed_score(
    common::ObSEArray<compaction::ObTabletMergeDag *, 32> &dags) const
{
  int ret = OB_SUCCESS;
  const int8_t SIZE_WEIGHT = -4;
  const int8_t FIFO_WIEGHT = -2;
  const int8_t SSTABLE_CNT_WEIGHT = -5;
  const int8_t PARALLEL_DAG_CNT_WEIGHT = 3;
  const int8_t PARALLEL_SSTABLE_CNT_WEIGHT = -1;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), KPC(this));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dags.count(); ++i) {
    ObTabletMergeDag *cur_dag = dags.at(i);
    if (OB_ISNULL(cur_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null compaction dag", K(ret), K(i), K(dags));
    } else {
      ObCompactionParam &param = cur_dag->get_param().compaction_param_;
      param.score_ = 0; // the greater score, the lower priority

      if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(occupy_size, param.occupy_size_, SIZE_WEIGHT, param.score_))) {
        LOG_WARN("failed to calculate rank score for occupy size", K(ret));
      } else if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(sstable_cnt, param.sstable_cnt_, SSTABLE_CNT_WEIGHT, param.score_))) {
        LOG_WARN("failed to calculate rank score for sstable cnt", K(ret));
      } else if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(parallel_dag_cnt, param.parallel_dag_cnt_, PARALLEL_DAG_CNT_WEIGHT, param.score_))) {
        LOG_WARN("failed to calculate rank score for parallel dag cnt", K(ret));
      } else if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(wait_time, rank_time_ - param.add_time_, FIFO_WIEGHT, param.score_))) {
        LOG_WARN("failed to calculate rank score for wait time", K(ret));
      } else {
        param.score_ += param.parallel_sstable_cnt_ * 10000 / param.sstable_cnt_ * PARALLEL_SSTABLE_CNT_WEIGHT;
      }
    }
  }
  return ret;
}


bool ObCompactionDagRanker::ObCompactionRankScoreCompare::operator()(
     const compaction::ObTabletMergeDag *left,
     const compaction::ObTabletMergeDag *right) const
{
  bool bret = false;
  if (OB_SUCCESS != result_code_) {
  } else if (OB_SUCCESS != (result_code_ = compare_dags_with_score(left, right, bret))) {
    LOG_WARN_RET(result_code_, "failed to compare mini dag with score", KPC(left), KPC(right));
  }
  return bret;
}

int ObCompactionDagRanker::ObCompactionRankScoreCompare::compare_dags_with_score(
    const compaction::ObTabletMergeDag *left,
    const compaction::ObTabletMergeDag *right,
    bool &bret) const
{
  int ret = OB_SUCCESS;
  bret = false;

  if (NULL == left) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("left dag must not null", K(ret), KPC(left), KPC(right));
  } else if (NULL == right) {
    bret = true;
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("right dag must not null", K(ret), KPC(left), KPC(right));
  } else {
    const ObCompactionParam &left_param = left->get_param().compaction_param_;
    const ObCompactionParam &right_param = right->get_param().compaction_param_;
    bret = left_param.score_ < right_param.score_;
  }
  return ret;
}

ObCompactionDagRanker::ObCompactionDagRanker()
  : allocator_(),
    rank_helper_(nullptr),
    is_inited_(false)
{
}

ObCompactionDagRanker::~ObCompactionDagRanker()
{
  destroy();
}

void ObCompactionDagRanker::destroy()
{
  is_inited_ = false;
  if (OB_NOT_NULL(rank_helper_)) {
    rank_helper_->~ObCompactionRankHelper();
    allocator_.free(rank_helper_);
  }
  allocator_.reset();
}

int ObCompactionDagRanker::init(
    const int64_t priority,
    const int64_t rank_time)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dag ranker has been inited", K(ret), KPC(this));
  } else if (OB_UNLIKELY(nullptr != rank_helper_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rank helper is unexpected not null", K(ret), KPC(rank_helper_));
  } else if (ObDagPrio::DAG_PRIO_COMPACTION_HIGH != priority &&
      ObDagPrio::DAG_PRIO_COMPACTION_MID != priority &&
      ObDagPrio::DAG_PRIO_COMPACTION_LOW != priority) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(priority));
  } else if (ObDagPrio::DAG_PRIO_COMPACTION_HIGH == priority) {
    if (OB_FAIL(create_rank_helper<ObMiniCompactionRankHelper>(rank_time, rank_helper_))) {
      LOG_WARN("failed to create rank helper", K(ret), K(priority));
    }
  } else {
    if (OB_FAIL(create_rank_helper<ObMinorCompactionRankHelper>(rank_time, rank_helper_))) {
      LOG_WARN("failed to create rank helper", K(ret), K(priority));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    destroy();
  }
  return ret;
}

void ObCompactionDagRanker::update(
    const int64_t current_time,
    const ObCompactionParam &param)
{
  if (OB_NOT_NULL(rank_helper_)) {
    rank_helper_->update(current_time, param);
  }
}

template<typename T>
int ObCompactionDagRanker::create_rank_helper(
    const int64_t rank_time,
    ObCompactionRankHelper *&helper)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(T)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for helper", K(ret));
  } else {
    rank_helper_ = new (buf) T(rank_time);
  }
  return ret;
}

int ObCompactionDagRanker::sort(
    common::ObSEArray<compaction::ObTabletMergeDag *, 32> &dags)
{
  int ret = OB_SUCCESS;
  if (dags.empty()) {
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag ranker not inited", K(ret));
  } else if (!rank_helper_->check_need_rank()) {
    // no need to sort
  } else if (OB_FAIL(rank_helper_->get_rank_weighed_score(dags))) {
    LOG_WARN("failed to get rank weighed score", K(ret), KPC(rank_helper_), K(dags));
  } else {
    ObCompactionRankScoreCompare comp(ret);
    lib::ob_sort(dags.begin(), dags.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort compaction dags", K(ret), K(dags.count()));

      // dump sort failed dags for debugging
      for (int64_t i = 0; i < dags.count(); ++i) {
        LOG_WARN("dump failed sorted dag", K(ret), K(i), KPC(dags.at(i)));
      }
    }
  }
  return ret;
}


} //compaction
} //oceanbase
