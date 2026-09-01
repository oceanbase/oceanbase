/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PALF

#include <new>
#include "log_async_palf_ctx_diag.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace palf
{

void PhysicalWriteFragmentPoolStat::reset()
{
  free_count_ = 0;
  wait_parent_count_ = 0;
  ready_count_ = 0;
  submitted_count_ = 0;
  finished_count_ = 0;
  failed_count_ = 0;
}

int PhysicalWriteFragmentPoolStat::inc_state(const AsyncFragmentState state)
{
  int ret = OB_SUCCESS;
  if (AsyncFragmentState::FREE == state) {
    ++free_count_;
  } else if (AsyncFragmentState::WAIT_PARENT == state) {
    ++wait_parent_count_;
  } else if (AsyncFragmentState::READY == state) {
    ++ready_count_;
  } else if (AsyncFragmentState::SUBMITTED == state) {
    ++submitted_count_;
  } else if (AsyncFragmentState::FINISHED == state) {
    ++finished_count_;
  } else if (AsyncFragmentState::FAILED == state) {
    ++failed_count_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unknown async fragment state", K(ret), K(state));
  }
  return ret;
}

bool PhysicalWriteFragmentPoolStat::has_failed_fragment() const
{
  return failed_count_ > 0;
}

void AsyncCtxStatSnapshot::reset()
{
  palf_id_ = -1;
  task_queue_cnt_ = 0;
  pending_task_cnt_ = 0;
  inflight_aio_cnt_ = 0;
  fragment_pool_stat_.reset();
  submit_fail_cnt_ = 0;
  complete_fail_cnt_ = 0;
  block_switch_pending_ = false;
  planned_end_lsn_.reset();
  persisted_lsn_.reset();
}

void AsyncCtxStatSnapshot::init(const int64_t palf_id,
                                const int64_t task_queue_cnt,
                                const int64_t inflight_aio_cnt,
                                const int64_t pending_task_cnt,
                                const PhysicalWriteFragmentPoolStat &fragment_pool_stat,
                                const int64_t submit_fail_cnt,
                                const int64_t complete_fail_cnt,
                                const bool block_switch_pending,
                                const LSN &planned_end_lsn,
                                const LSN &persisted_lsn)
{
  palf_id_ = palf_id;
  task_queue_cnt_ = task_queue_cnt;
  inflight_aio_cnt_ = inflight_aio_cnt;
  pending_task_cnt_ = pending_task_cnt;
  fragment_pool_stat_ = fragment_pool_stat;
  submit_fail_cnt_ = submit_fail_cnt;
  complete_fail_cnt_ = complete_fail_cnt;
  block_switch_pending_ = block_switch_pending;
  planned_end_lsn_ = planned_end_lsn;
  persisted_lsn_ = persisted_lsn;
}

bool AsyncCtxStatSnapshot::looks_stuck() const
{
  static const int64_t STUCK_FAIL_HINT = 1000;
  bool stuck = false;
  if (fragment_pool_stat_.has_failed_fragment() && submit_fail_cnt_ >= STUCK_FAIL_HINT) {
    stuck = true;
  } else {
    // 启发式卡住判定: planner 仍持有 task, planned/persisted 位点无法推进,
    // 同时没有 inflight AIO 可以解除阻塞.
    stuck = pending_task_cnt_ > 0
        && planned_end_lsn_.is_valid()
        && persisted_lsn_.is_valid()
        && (planned_end_lsn_ == persisted_lsn_ || planned_end_lsn_ < persisted_lsn_)
        && inflight_aio_cnt_ == 0;
  }
  return stuck;
}

void PalfPerfCounterStat::reset()
{
  sum_ = 0;
  per_sec_ = 0;
}

void PalfPerfCounterStat::init(const int64_t sum_value, const int64_t interval_us)
{
  sum_ = sum_value;
  per_sec_ = interval_us > 0 ? sum_value * 1000 * 1000 / interval_us : 0;
}

void PalfPerfValueStat::reset()
{
  count_ = 0;
  sum_ = 0;
  per_sec_ = 0;
  avg_ = 0;
  p99_ = 0;
  max_ = 0;
}

void PalfPerfValueStat::init(const int64_t count,
                             const int64_t sum,
                             const int64_t interval_us,
                             const int64_t p99,
                             const int64_t max)
{
  count_ = count;
  sum_ = sum;
  per_sec_ = interval_us > 0 ? sum * 1000 * 1000 / interval_us : 0;
  avg_ = count > 0 ? sum / count : 0;
  p99_ = p99;
  max_ = max;
}

void PalfPerfHistogram::reset()
{
  count_ = 0;
  sum_ = 0;
  max_ = 0;
  MEMSET(buckets_, 0, sizeof(buckets_));
}

void PalfPerfHistogram::record(const int64_t value)
{
  if (value >= 0) {
    const int64_t bucket_idx = get_bucket_index_(value);
    count_++;
    sum_ += value;
    if (value > max_) {
      max_ = value;
    }
    buckets_[bucket_idx]++;
  }
}

void PalfPerfHistogram::build_print_item(const int64_t interval_us, PalfPerfValueStat &out) const
{
  out.init(count_, sum_, interval_us,
           get_percentile_(99, 100),
           max_);
}

int64_t PalfPerfHistogram::get_bucket_index_(const int64_t value) const
{
  int64_t idx = 0;
  if (value > 0) {
    idx = 1;
    int64_t upper_bound = 1;
    while (idx < BUCKET_CNT - 1 && value > upper_bound) {
      idx++;
      upper_bound <<= 1;
    }
  }
  return idx;
}

int64_t PalfPerfHistogram::get_bucket_upper_bound_(const int64_t bucket_idx) const
{
  int64_t upper_bound = 1;
  if (bucket_idx <= 0) {
    upper_bound = 0;
  } else if (bucket_idx >= BUCKET_CNT - 1) {
    upper_bound = INT64_MAX;
  } else {
    upper_bound = 1LL << (bucket_idx - 1);
  }
  return upper_bound;
}

int64_t PalfPerfHistogram::get_percentile_(const int64_t numerator, const int64_t denominator) const
{
  int64_t percentile = 0;
  if (count_ > 0 && numerator > 0 && denominator > 0) {
    const int64_t target = (count_ * numerator + denominator - 1) / denominator;
    int64_t curr = 0;
    for (int64_t i = 0; 0 == percentile && i < BUCKET_CNT; ++i) {
      curr += buckets_[i];
      if (curr >= target) {
        percentile = get_bucket_upper_bound_(i);
      }
    }
  }
  return percentile;
}

PalfPerfItem::PalfPerfItem(const bool is_counter)
  : is_counter_(is_counter),
    lock_(common::ObLatchIds::PALF_LOG_ENGINE_LOCK),
    last_print_ts_(OB_INVALID_TIMESTAMP),
    first_record_ts_(OB_INVALID_TIMESTAMP),
    counter_sum_(0),
    value_hist_()
{
  reset();
}

void PalfPerfItem::reset()
{
  common::ObSpinLockGuard guard(lock_);
  last_print_ts_ = OB_INVALID_TIMESTAMP;
  first_record_ts_ = OB_INVALID_TIMESTAMP;
  counter_sum_ = 0;
  value_hist_.reset();
}

void PalfPerfItem::record(const int64_t now_us, const int64_t value)
{
  common::ObSpinLockGuard guard(lock_);
  if (OB_INVALID_TIMESTAMP == first_record_ts_) {
    first_record_ts_ = now_us;
  }
  if (is_counter_) {
    if (value > 0) {
      counter_sum_ += value;
    }
  } else {
    value_hist_.record(value);
  }
}

void PalfPerfItem::print_item(const char *name, const int64_t now_us, char *buf, const int64_t buf_len, int64_t &pos)
{
  common::ObSpinLockGuard guard(lock_);
  PalfPerfCounterStat counter_stat;
  PalfPerfValueStat value_stat;
  const int64_t interval_begin_ts = OB_INVALID_TIMESTAMP != last_print_ts_ ? last_print_ts_ : first_record_ts_;
  const int64_t interval_us = interval_begin_ts >= 0 && now_us > interval_begin_ts
      ? now_us - interval_begin_ts
      : 1000 * 1000;
  J_NAME(OB_NOT_NULL(name) ? name : "NULL");
  J_COLON();
  if (is_counter_) {
    counter_stat.init(counter_sum_, interval_us);
    BUF_PRINTO(counter_stat);
  } else {
    value_hist_.build_print_item(interval_us, value_stat);
    BUF_PRINTO(value_stat);
  }
  counter_sum_ = 0;
  value_hist_.reset();
  last_print_ts_ = now_us;
  first_record_ts_ = OB_INVALID_TIMESTAMP;
}

PalfPerfReporter::PalfPerfReporter(const char *prefix)
  : prefix_(prefix),
    items_(),
    now_us_(0),
    oldest_task_age_us_(0)
{
}

void PalfPerfReporter::add_item(const char *name, PalfPerfItem *item)
{
  int ret = OB_SUCCESS;
  PalfPerfReportItem report_item = {name, item};
  if (OB_ISNULL(name) || OB_ISNULL(item)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid perf report item", KR(ret), KP(name), KP(item));
  } else if (OB_FAIL(items_.push_back(report_item))) {
    PALF_LOG(WARN, "push perf report item failed", KR(ret), KP(name), KP(item));
  }
}

void PalfPerfReporter::print(const int64_t now_us, const int64_t oldest_task_age_us)
{
  now_us_ = now_us;
  oldest_task_age_us_ = oldest_task_age_us;
  PALF_LOG(INFO, prefix_, KPC(this));
}

int64_t PalfPerfReporter::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t print_item_cnt = 0;
  J_OBJ_START();
  J_NAME("items");
  J_COLON();
  J_ARRAY_START();
  for (int64_t i = 0; i < items_.count(); ++i) {
    const PalfPerfReportItem &item = items_.at(i);
    if (OB_NOT_NULL(item.name) && OB_NOT_NULL(item.item)) {
      if (print_item_cnt > 0) {
        J_COMMA();
      }
      item.item->print_item(item.name, now_us_, buf, buf_len, pos);
      ++print_item_cnt;
    }
  }
  J_ARRAY_END();
  J_COMMA();
  J_KV(K_(oldest_task_age_us));
  J_OBJ_END();
  return pos;
}

} // end namespace palf
} // end namespace oceanbase
