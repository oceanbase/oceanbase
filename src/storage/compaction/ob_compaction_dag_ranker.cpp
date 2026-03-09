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
#include "storage/compaction/ob_tenant_tablet_scheduler.h"

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


// holder: the object that holds min_/max_ members (e.g. common_param_ or (*this))
#define CALCULATE_NORMALIZED_RANK_SCORE(holder, dimension, val, weight, score)     \
  ({                                                                               \
    int ret = OB_SUCCESS;                                                          \
    const int64_t min_val = holder.min_ ## dimension ## _;                         \
    const int64_t max_val = holder.max_ ## dimension ## _;                         \
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


ObCompactionRankCommonParam::ObCompactionRankCommonParam()
  : max_occupy_size_(0),
    min_occupy_size_(INT64_MAX),
    max_wait_time_(0),
    min_wait_time_(INT64_MAX),
    max_sstable_cnt_(0),
    min_sstable_cnt_(INT16_MAX)
{
}

void ObCompactionRankCommonParam::update(const ObTabletMergeDagParam &param, const int64_t rank_time)
{
  max_occupy_size_ = MAX(max_occupy_size_, param.compaction_param_.occupy_size_);
  min_occupy_size_ = MIN(min_occupy_size_, param.compaction_param_.occupy_size_);

  int64_t cur_wait_time = rank_time - param.compaction_param_.add_time_;
  max_wait_time_ = MAX(max_wait_time_, cur_wait_time);
  min_wait_time_ = MIN(min_wait_time_, cur_wait_time);

  max_sstable_cnt_ = MAX(max_sstable_cnt_, param.compaction_param_.sstable_cnt_);
  min_sstable_cnt_ = MIN(min_sstable_cnt_, param.compaction_param_.sstable_cnt_);
}


// common_param: the ObCompactionRankCommonParam object
// extra_max/extra_min: additional max/min values specific to each helper
#define CHECK_NEED_RANK(common_param, extra_max, extra_min)                           \
  ({                                                                                  \
    bool need_rank_dag = true;                                                        \
    if (OB_UNLIKELY((common_param).max_occupy_size_ < (common_param).min_occupy_size_ \
                 || (common_param).max_wait_time_ < (common_param).min_wait_time_     \
                 || (common_param).max_sstable_cnt_ < (common_param).min_sstable_cnt_ \
                 || (extra_max) < (extra_min))) {                                     \
      need_rank_dag = false;                                                          \
    } else if ((common_param).max_occupy_size_ == (common_param).min_occupy_size_     \
            && (common_param).max_wait_time_ == (common_param).min_wait_time_         \
            && (common_param).max_sstable_cnt_ == (common_param).min_sstable_cnt_     \
            && (extra_max) == (extra_min)) {                                          \
      need_rank_dag = false;                                                          \
    }                                                                                 \
    need_rank_dag;                                                                    \
  })

// Calculate common rank score for occupy_size, sstable_cnt, wait_time
#define CALCULATE_COMMON_RANK_SCORE(common_param, rank_time, param, size_weight, sstable_cnt_weight, fifo_weight)                                 \
  ({                                                                                                                                              \
    int ret = OB_SUCCESS;                                                                                                                         \
    if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(common_param, occupy_size, (param).occupy_size_, size_weight, (param).score_))) {                 \
      LOG_WARN("failed to calculate rank score for occupy size", K(ret));                                                                         \
    } else if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(common_param, sstable_cnt, (param).sstable_cnt_, sstable_cnt_weight, (param).score_))) {   \
      LOG_WARN("failed to calculate rank score for sstable cnt", K(ret));                                                                         \
    } else if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE(common_param, wait_time, (rank_time) - (param).add_time_, fifo_weight, (param).score_))) { \
      LOG_WARN("failed to calculate rank score for wait time", K(ret));                                                                           \
    }                                                                                                                                             \
    ret;                                                                                                                                          \
  })


ObMiniCompactionRankHelper::ObMiniCompactionRankHelper(const int64_t rank_time)
  : ObCompactionRankHelper(rank_time),
    common_param_(),
    max_replay_interval_(0),
    min_replay_interval_(INT64_MAX)
{
}

bool ObMiniCompactionRankHelper::need_rank() const
{
  return CHECK_NEED_RANK(common_param_, max_replay_interval_, min_replay_interval_);
}

void ObMiniCompactionRankHelper::update(const ObTabletMergeDagParam &param)
{
  common_param_.update(param, rank_time_);
  max_replay_interval_ = MAX(max_replay_interval_, param.compaction_param_.replay_interval_);
  min_replay_interval_ = MIN(min_replay_interval_, param.compaction_param_.replay_interval_);
}

int ObMiniCompactionRankHelper::get_rank_weighed_score(common::ObIArray<compaction::ObTabletMergeDag *> &dags) const
{
  int ret = OB_SUCCESS;
  const int8_t SIZE_WEIGHT = -5;
  const int8_t FIFO_WIEGHT = -2;
  const int8_t REPLAY_WEIGHT = 3;
  const int8_t SSTABLE_CNT_WEIGHT = 2;
  const bool need_calc_score = need_rank();

  for (int64_t i = 0; OB_SUCC(ret) && i < dags.count(); ++i) {
    ObTabletMergeDag *cur_dag = dags.at(i);
    if (OB_ISNULL(cur_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null compaction dag", K(ret), K(i), K(dags));
    } else if (need_calc_score) {
      ObCompactionParam &param = cur_dag->get_param().compaction_param_;
      param.score_ = 0; // the greater score, the lower priority

      if (OB_FAIL(CALCULATE_COMMON_RANK_SCORE(common_param_, rank_time_, param, SIZE_WEIGHT, SSTABLE_CNT_WEIGHT, FIFO_WIEGHT))) {
      } else if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE((*this), replay_interval, param.replay_interval_, REPLAY_WEIGHT, param.score_))) {
        LOG_WARN("failed to calculate rank score for replay interval", K(ret), K(param));
      }
    } else {
      cur_dag->get_param().compaction_param_.score_ = 0;
    }
  }
  return ret;
}


ObMinorCompactionRankHelper::ObMinorCompactionRankHelper(const int64_t rank_time)
  : ObCompactionRankHelper(rank_time),
    common_param_(),
    max_parallel_dag_cnt_(0),
    min_parallel_dag_cnt_(INT64_MAX)
{
}

bool ObMinorCompactionRankHelper::need_rank() const
{
  return CHECK_NEED_RANK(common_param_, max_parallel_dag_cnt_, min_parallel_dag_cnt_);
}

void ObMinorCompactionRankHelper::update(const ObTabletMergeDagParam &param)
{
  common_param_.update(param, rank_time_);
  max_parallel_dag_cnt_ = MAX(max_parallel_dag_cnt_, param.compaction_param_.parallel_dag_cnt_);
  min_parallel_dag_cnt_ = MIN(min_parallel_dag_cnt_, param.compaction_param_.parallel_dag_cnt_);
}

int ObMinorCompactionRankHelper::get_rank_weighed_score(common::ObIArray<compaction::ObTabletMergeDag *> &dags) const
{
  int ret = OB_SUCCESS;
  const int8_t SIZE_WEIGHT = -4;
  const int8_t FIFO_WIEGHT = -2;
  const int8_t SSTABLE_CNT_WEIGHT = -5;
  const int8_t PARALLEL_DAG_CNT_WEIGHT = 3;
  const int8_t PARALLEL_SSTABLE_CNT_WEIGHT = -1;
  const bool need_calc_score = need_rank();

  for (int64_t i = 0; OB_SUCC(ret) && i < dags.count(); ++i) {
    ObTabletMergeDag *cur_dag = dags.at(i);
    if (OB_ISNULL(cur_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null compaction dag", K(ret), K(i), K(dags));
    } else if (!need_calc_score || is_meta_major_merge(cur_dag->get_param().merge_type_)) {
      cur_dag->get_param().compaction_param_.score_ = 0;
    } else {
      ObCompactionParam &param = cur_dag->get_param().compaction_param_;
      param.score_ = 0;
      if (OB_UNLIKELY(param.sstable_cnt_ == 0)) {
        // just for defend
      } else if (OB_FAIL(CALCULATE_COMMON_RANK_SCORE(common_param_, rank_time_, param, SIZE_WEIGHT, SSTABLE_CNT_WEIGHT, FIFO_WIEGHT))) {
      } else if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE((*this), parallel_dag_cnt, param.parallel_dag_cnt_, PARALLEL_DAG_CNT_WEIGHT, param.score_))) {
        LOG_WARN("failed to calculate rank score for parallel dag cnt", K(ret));
      } else {
        param.score_ += param.parallel_sstable_cnt_ * 10000 / param.sstable_cnt_ * PARALLEL_SSTABLE_CNT_WEIGHT;
      }
    }
  }
  return ret;
}


ObMajorCompactionRankHelper::ObMajorCompactionRankHelper(const int64_t rank_time)
  : ObCompactionRankHelper(rank_time),
    max_compaction_scn_(0),
    min_compaction_scn_(INT64_MAX)
{
}

bool ObMajorCompactionRankHelper::need_rank() const
{
 return max_compaction_scn_ > min_compaction_scn_;
}

void ObMajorCompactionRankHelper::update(const ObTabletMergeDagParam &param)
{
  max_compaction_scn_ = MAX(max_compaction_scn_, param.merge_version_);
  min_compaction_scn_ = MIN(min_compaction_scn_, param.merge_version_);
}

int ObMajorCompactionRankHelper::get_rank_weighed_score(common::ObIArray<compaction::ObTabletMergeDag *> &dags) const
{
  int ret = OB_SUCCESS;
  const int8_t WEIGHT = 1;
  const bool need_calc_score = need_rank();

  for (int64_t i = 0; OB_SUCC(ret) && i < dags.count(); ++i) {
    ObTabletMergeDag *cur_dag = dags.at(i);
    if (OB_ISNULL(cur_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null compaction dag", K(ret), K(i), K(dags));
    } else if (need_calc_score) {
      ObTabletMergeDagParam &param = cur_dag->get_param();
      ObCompactionParam &compaction_param = param.compaction_param_;
      compaction_param.score_ = 0; // the greater score, the lower priority

      if (OB_FAIL(CALCULATE_NORMALIZED_RANK_SCORE((*this), compaction_scn, param.merge_version_, WEIGHT, compaction_param.score_))) {
        LOG_WARN("failed to calculate rank score for compaction scn", K(ret));
      }
    } else {
      cur_dag->get_param().compaction_param_.score_ = 0;
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

ObCompactionDagRanker::ObCompactionDagRanker(
    const int64_t rank_time,
    common::ObDList<ObIDag> &ready_dag_list,
    common::ObDList<ObIDag> &rank_dag_list)
  : ready_dag_list_(ready_dag_list),
    rank_dag_list_(rank_dag_list),
    rank_helper_(nullptr),
    mini_helper_(rank_time),
    minor_helper_(rank_time),
    major_helper_(rank_time),
    rank_dags_(),
    fetch_co_dag_limit_(0)
{
}

ObCompactionDagRanker::~ObCompactionDagRanker()
{
  rank_helper_ = nullptr;
  rank_dags_.reset();
}

int ObCompactionDagRanker::process(
    const int64_t priority,
    const int64_t batch_size,
    const int64_t limits)
{
  int ret = OB_SUCCESS;
  fetch_co_dag_limit_ = 0;

  if (ObDagPrio::DAG_PRIO_COMPACTION_HIGH == priority) {
    rank_helper_ = &mini_helper_;
  } else if (ObDagPrio::DAG_PRIO_COMPACTION_MID == priority) {
    rank_helper_ = &minor_helper_;
  } else if (ObDagPrio::DAG_PRIO_COMPACTION_LOW == priority) {
    rank_helper_ = &major_helper_;

    // should calculate fetch co dag limit even if the need_adaptive_schedule is false
    int64_t prepare_co_dag_cnt = 0;
    const ObIDag *head = ready_dag_list_.get_header();
    const ObIDag *cur = head->get_next();
    while (NULL != cur && head != cur) {
      if (ObDagType::DAG_TYPE_CO_MERGE_PREPARE == cur->get_type() ||
          ObDagType::DAG_TYPE_CO_MERGE_SCHEDULE == cur->get_type()) {
        ++prepare_co_dag_cnt;
      }
      cur = cur->get_next();
    }
    fetch_co_dag_limit_ = MAX(1, limits - prepare_co_dag_cnt);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(priority));
  }

  if (FAILEDx(prepare_rank_dags_(batch_size))) {
    LOG_WARN("failed to collect and sort dags", K(ret), K(priority));
  } else if (OB_FAIL(sort_())) {
    LOG_WARN("failed to sort dags", K(ret));
  } else if (OB_FAIL(move_dags_to_ready_dag_list_())) {
    LOG_WARN("failed to move dags to ready dag list", K(ret));
  }
  return ret;
}

int ObCompactionDagRanker::prepare_rank_dags_(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  compaction::ObTabletMergeDag *compaction_dag = nullptr;
  int64_t add_co_dag_cnt = 0;
  const ObIDag *head = rank_dag_list_.get_header();
  ObIDag *cur = const_cast<ObIDag *>(head->get_next());

  while (OB_SUCC(ret) && NULL != cur && head != cur) {
    const ObIDag::ObDagStatus &dag_status = cur->get_dag_status();
    if (OB_ISNULL(compaction_dag = static_cast<compaction::ObTabletMergeDag *>(cur))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null dag", K(ret), KPC(cur));
    } else if (ObIDag::DAG_STATUS_INITING == dag_status) {
      // do nothing
    } else if (ObIDag::DAG_STATUS_READY != dag_status) { // dag status must be INITING or READY
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag in rank list must be ready", K(ret), K(dag_status), KPC(compaction_dag));
    } else if (OB_FAIL(rank_dags_.push_back(compaction_dag))) {
      LOG_WARN("failed to add compaction dag", K(ret), KPC(compaction_dag));
    } else if (FALSE_IT(rank_helper_->update(compaction_dag->get_param()))) {
    } else if (ObDagType::DAG_TYPE_CO_MERGE_PREPARE == compaction_dag->get_type()) {
      ++add_co_dag_cnt;
    }

    if (OB_SUCC(ret)) {
      if (rank_dags_.count() >= batch_size || add_co_dag_cnt >= fetch_co_dag_limit_) {
        break;
      }
      cur = cur->get_next();
    }
  }

  if (OB_FAIL(ret)) {
    rank_dags_.reset();
  }
  return ret;
}

int ObCompactionDagRanker::sort_()
{
  int ret = OB_SUCCESS;
  const bool need_adaptive_schedule = (nullptr == ObBasicMergeScheduler::get_merge_scheduler())
                                    ? false
                                    : ObBasicMergeScheduler::get_merge_scheduler()->enable_adaptive_merge_schedule();

  if (OB_UNLIKELY(rank_dags_.empty() || nullptr == rank_helper_ || !rank_helper_->need_rank())) {
    // do nothing, no need to sort
  } else if (!need_adaptive_schedule) {
    // no need to sort
  } else if (OB_FAIL(rank_helper_->get_rank_weighed_score(rank_dags_))) {
    LOG_WARN("failed to get rank weighed score", K(ret), KPC(rank_helper_), K(rank_dags_));
  } else {
    ObCompactionRankScoreCompare comp(ret);
    lib::ob_sort(rank_dags_.begin(), rank_dags_.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort compaction dags", K(ret), K(rank_dags_.count()));

      // dump sort failed dags for debugging
      for (int64_t i = 0; i < rank_dags_.count(); ++i) {
        LOG_WARN("dump failed sorted dag", K(ret), K(i), KPC(rank_dags_.at(i)));
      }
    }
  }
  return ret;
}

int ObCompactionDagRanker::move_dags_to_ready_dag_list_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < rank_dags_.count(); ++i) {
    ObTabletMergeDag *cur_dag = rank_dags_.at(i);
    if (OB_ISNULL(cur_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null compaction dag", K(ret), K(i), K(rank_dags_));
    } else if (OB_UNLIKELY(RANK_DAG_LIST != cur_dag->get_list_idx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("dag is not in rank dag list", K(ret), K(i), KPC(cur_dag));
    } else if (OB_ISNULL(rank_dag_list_.remove(cur_dag))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to remove dag from rank dag list", K(ret), K(i), KPC(cur_dag));
    } else if (OB_UNLIKELY(!ready_dag_list_.add_last(cur_dag))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to add dag to ready dag list", K(ret), K(i), KPC(cur_dag));
      ob_abort();
    } else {
      cur_dag->set_list_idx(READY_DAG_LIST);
    }
  }
  return ret;
}


} //compaction
} //oceanbase
