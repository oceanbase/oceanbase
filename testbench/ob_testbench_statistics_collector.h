/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 *
 */

#ifndef _OCEANBASE_TESTBENCH_STATISTICS_COLLECTOR_H_
#define _OCEANBASE_TESTBENCH_STATISTICS_COLLECTOR_H_

#include "lib/queue/ob_link_queue.h"
#include "share/stat/ob_opt_column_stat.h"
#include "lib/thread/ob_thread_lease.h"
#include "common/object/ob_object.h"
#include "lib/allocator/page_arena.h"
#include "lib/thread/thread_mgr.h"

namespace oceanbase {
namespace testbench {

enum ObLatencyTaskType {
  INVALID_LATENCY_TASK = 0,
  RPC_LATENCY_TASK,
  DISTRIBUTED_TXN_LATENCY_TASK,
  CONTENTION_TXN_LATENCY_TASK,
  DEADLOCK_TXN_LATENCY_TASK,
  COMMIT_SQL_LATENCY_TASK,
  LOCK_SQL_LATENCY_TASK,
  ELECTION_LATENCY_TASK,
  LATENCY_TASK_TYPE_CNT
};

// const int64_t TASK_QUEUE_SIZE = ObLatencyTaskType::LATENCY_TASK_TYPE_CNT;
const int64_t TASK_QUEUE_SIZE = 8;

class ObLatencyTask {
public:
  ObLatencyTask(ObLatencyTaskType type, const common::ObObj &latency);
  ~ObLatencyTask();
  inline ObLatencyTaskType get_type() const { return type_; }
  inline const common::ObObj &get_latency() const { return latency_; }
  
  static ObLatencyTask *__get_class_address(common::ObLink *ptr);
  static common::ObLink *__get_member_address(ObLatencyTask *ptr);
  TO_STRING_KV(K(type_), K(latency_));

public:
  common::ObLink *__next_;

private:
  ObLatencyTaskType type_;
  common::ObObj latency_;
};

enum ObStatisticsTaskType {
  INVALID_STATISTICS_TASK = 0,
  STATISTICS_QUEUE_TASK,
  STATISTICS_SUBMIT_TASK
};

class ObStatisticsTask {
public:
  ObStatisticsTask();
  virtual ~ObStatisticsTask();
  bool acquire_lease();
  bool revoke_lease();
  inline ObStatisticsTaskType get_type() const { return type_; }
  VIRTUAL_TO_STRING_KV(K(type_));

protected:
  ObStatisticsTaskType type_;
  common::ObThreadLease lease_;
};

class ObStatisticsSubmitTask : public ObStatisticsTask {
public:
  ObStatisticsSubmitTask();
  ~ObStatisticsSubmitTask();
  int init();
};

class ObStatisticsQueueTask : public ObStatisticsTask {
public:
  ObStatisticsQueueTask();
  ~ObStatisticsQueueTask();
  int init(int64_t index);
  void inc_total_submit_cnt();
  void inc_total_apply_cnt();
  int64_t get_total_submit_cnt() const;
  int64_t get_total_apply_cnt() const;
  int64_t get_snapshot_queue_cnt() const;
  double_t get_min_value() const;
  double_t get_max_value() const;
  inline int64_t get_index() const { return index_; }
  inline bool is_histogram_inited() const { return histogram_inited_; }
  void set_histogram_inited();

  int top(ObLatencyTask *&task);
  int pop();
  int push(ObLatencyTask *task);
  INHERIT_TO_STRING_KV("ObStatisticsTask", ObStatisticsTask, 
    K(total_submit_cnt_), K(total_apply_cnt_), K(index_));

private:
  int64_t total_submit_cnt_;
  int64_t total_apply_cnt_;
  common::ObSpLinkQueue queue_;
  int64_t index_;
  // the minimum and maximum value in the queue are not dynamically maintained 
  // they are only used in ObHistogram initialization
  common::ObObj min_value_;
  common::ObObj max_value_;
  bool histogram_inited_;
  ObArenaAllocator inner_allocator_;
};

class ObTestbenchStatisticsCollector : public lib::TGTaskHandler {
public:
  ObTestbenchStatisticsCollector(int64_t bucket_capacity_, double_t bucket_min_ratio, double_t bucket_max_ratio);
  ~ObTestbenchStatisticsCollector();

public:
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void handle(void *task) override;
  int push_latency_task(ObLatencyTask *task);
  int sync_latency_task();
  int get_percentage_latency(ObLatencyTaskType type, double_t percentage, double_t &latency);
  const ObHistogram &get_histogram(ObLatencyTaskType type) const;
  const ObStatisticsQueueTask &get_queue_task(ObLatencyTaskType type) const;

  inline int get_tg_id() const { return tg_id_; }
  inline bool is_snapshot_ready() const { return 0 == snapshot_ready_; }

private:
  int handle_queue_task_(ObStatisticsQueueTask *task);
  int handle_submit_task_();
  int push_task_(ObStatisticsTask *task);

public:
  static constexpr int64_t THREAD_NUM = 1;

private:
  int tg_id_;
  bool is_inited_;
  int64_t snapshot_ready_;
  int64_t bucket_capacity_;
  double_t bucket_min_ratio_;
  double_t bucket_max_ratio_;
  ObStatisticsSubmitTask submit_;
  ObStatisticsQueueTask submit_queues_[TASK_QUEUE_SIZE];
  ObHistogram histograms_[TASK_QUEUE_SIZE];
  ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTestbenchStatisticsCollector);
};

} // namespace testbench
} // namespace oceanbase
#endif