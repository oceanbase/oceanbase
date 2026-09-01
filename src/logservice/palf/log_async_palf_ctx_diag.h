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

#ifndef OCEANBASE_LOGSERVICE_LOG_ASYNC_PALF_CTX_DIAG_
#define OCEANBASE_LOGSERVICE_LOG_ASYNC_PALF_CTX_DIAG_

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_print_utils.h"
#include "log_async_io_struct.h"
#include "log_define.h"
#include "lsn.h"

namespace oceanbase
{
namespace palf
{

// Fragment-pool control-plane snapshot.
struct PhysicalWriteFragmentPoolStat
{
  PhysicalWriteFragmentPoolStat() { reset(); }
  void reset();
  int inc_state(const AsyncFragmentState state);
  bool has_failed_fragment() const;

private:
  int64_t free_count_;
  int64_t wait_parent_count_;
  int64_t ready_count_;
  int64_t submitted_count_;
  int64_t finished_count_;
  int64_t failed_count_;

public:
  TO_STRING_KV(K_(free_count), K_(wait_parent_count),
               K_(ready_count), K_(submitted_count), K_(finished_count),
               K_(failed_count));
};

// 单次诊断打印使用的只读快照, 聚合 ctx、planner、fragment pool 的实时状态
// 和诊断计数. 这些字段是派生观测值, 不参与状态机所有权, 因此不保存在 ctx 中.
struct AsyncCtxStatSnapshot
{
  AsyncCtxStatSnapshot() { reset(); }
  void reset();
  void init(const int64_t palf_id,
            const int64_t task_queue_cnt,
            const int64_t inflight_aio_cnt,
            const int64_t pending_task_cnt,
            const PhysicalWriteFragmentPoolStat &fragment_pool_stat,
            const int64_t submit_fail_cnt,
            const int64_t complete_fail_cnt,
            const bool block_switch_pending,
            const LSN &planned_end_lsn,
            const LSN &persisted_lsn);

  // Return a heuristic signal for a pending pipeline with no visible progress source.
  bool looks_stuck() const;
  int64_t get_palf_id() const { return palf_id_; }
  int64_t get_inflight_aio_cnt() const { return inflight_aio_cnt_; }
  int64_t get_pending_task_cnt() const { return pending_task_cnt_; }
  int64_t get_submit_fail_cnt() const { return submit_fail_cnt_; }

private:
  int64_t palf_id_;
  int64_t task_queue_cnt_;
  int64_t inflight_aio_cnt_;
  int64_t pending_task_cnt_;
  PhysicalWriteFragmentPoolStat fragment_pool_stat_;
  int64_t submit_fail_cnt_;
  int64_t complete_fail_cnt_;
  bool block_switch_pending_;
  LSN planned_end_lsn_;
  LSN persisted_lsn_;

public:
  TO_STRING_KV(K_(palf_id),
               K_(task_queue_cnt), K_(pending_task_cnt),
               K_(inflight_aio_cnt),
               K_(fragment_pool_stat),
               K_(submit_fail_cnt), K_(complete_fail_cnt),
               K_(block_switch_pending),
               K_(planned_end_lsn), K_(persisted_lsn));
};

// Counter value for one print interval.
struct PalfPerfCounterStat
{
  PalfPerfCounterStat() { reset(); }
  void reset();
  void init(const int64_t sum_value, const int64_t interval_us);

private:
  int64_t sum_;       // 窗口内累计次数或累计量。
  int64_t per_sec_;   // 按窗口时长折算后的每秒值。

public:
  TO_STRING_KV(K_(sum), K_(per_sec));
};

// Value stats for one print interval. Percentiles are estimated by exponential
// histogram buckets.
struct PalfPerfValueStat
{
  PalfPerfValueStat() { reset(); }
  void reset();
  void init(const int64_t count,
            const int64_t sum,
            const int64_t interval_us,
            const int64_t p99,
            const int64_t max);

private:
  int64_t count_;     // 窗口内样本数量。
  int64_t sum_;       // 窗口内样本值总和。
  int64_t per_sec_;   // sum 按窗口时长折算后的每秒值。
  int64_t avg_;       // sum / count。
  int64_t p99_;       // 99 分位，来自指数桶近似。
  int64_t max_;       // 窗口内最大样本值。

public:
  TO_STRING_KV(K_(count), K_(sum), K_(per_sec), K_(avg),
               K_(p99), K_(max));
};

// Exponential histogram for one reporting interval. It stores no raw samples;
// percentile values are approximate bucket upper bounds.
class PalfPerfHistogram
{
public:
  PalfPerfHistogram() { reset(); }
  void reset();
  void record(const int64_t value);
  void build_print_item(const int64_t interval_us, PalfPerfValueStat &out) const;

private:
  static const int64_t BUCKET_CNT = 32;
  int64_t get_bucket_index_(const int64_t value) const;
  int64_t get_bucket_upper_bound_(const int64_t bucket_idx) const;
  int64_t get_percentile_(const int64_t numerator, const int64_t denominator) const;

private:
  int64_t count_;
  int64_t sum_;
  int64_t max_;
  int64_t buckets_[BUCKET_CNT];
};

// 线程安全的区间指标采集器. counter 模式累计次数, value 模式额外统计样本分布.
// record() 可由 worker 或 callback 线程调用; print_item() 输出并清空当前区间.
class PalfPerfItem
{
public:
  explicit PalfPerfItem(bool is_counter);
  void reset();
  void record(const int64_t now_us, const int64_t value);
  void print_item(const char *name, const int64_t now_us, char *buf, const int64_t buf_len, int64_t &pos);

private:
  bool is_counter_;
  common::ObSpinLock lock_;
  int64_t last_print_ts_;
  int64_t first_record_ts_;
  int64_t counter_sum_;
  PalfPerfHistogram value_hist_;
};

// Non-owning name-to-metric binding registered in PalfPerfReporter.
struct PalfPerfReportItem
{
  const char *name;
  PalfPerfItem *item;
  TO_STRING_KV(K(name), KP(item));
};

// 一次注册具名指标, 之后合并成一条 PALF 日志打印. reporter 借用 prefix、
// name 和 item; 每次序列化会取走并清空各 item 当前统计区间的数据.
class PalfPerfReporter
{
public:
  explicit PalfPerfReporter(const char *prefix);
  // Register one non-owning metric item. The item must outlive this reporter.
  void add_item(const char *name, PalfPerfItem *item);
  // Print and reset all registered interval metrics.
  void print(const int64_t now_us, const int64_t oldest_task_age_us);
  DECLARE_TO_STRING;

private:
  const char *prefix_;
  common::ObSEArray<PalfPerfReportItem, 16> items_;
  int64_t now_us_;
  int64_t oldest_task_age_us_;
};

} // end namespace palf
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_ASYNC_PALF_CTX_DIAG_
