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
 */

#include "ob_testbench_statistics_collector.h"
#include "share/ob_thread_define.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase {
namespace testbench {

/*
                              ObLatencyTask
*/
ObLatencyTask::ObLatencyTask(ObLatencyTaskType type, const common::ObObj &latency) : type_(type), latency_(latency) {}

ObLatencyTask::~ObLatencyTask() {}

ObLatencyTask *ObLatencyTask::__get_class_address(common::ObLink *ptr) {
  return nullptr != ptr ? CONTAINER_OF(ptr, ObLatencyTask, __next_) : nullptr;
}

common::ObLink *ObLatencyTask::__get_member_address(ObLatencyTask *ptr) {
  return nullptr != ptr ? reinterpret_cast<common::ObLink*>(ADDRESS_OF(ptr, ObLatencyTask, __next_)) : nullptr;
}

/*
                              ObStatisticsTask
*/
ObStatisticsTask::ObStatisticsTask() : lease_() {}

ObStatisticsTask::~ObStatisticsTask() {}

bool ObStatisticsTask::acquire_lease() {
  return lease_.acquire();
}

bool ObStatisticsTask::revoke_lease() {
  return lease_.revoke();
}

/*
                              ObStatisticsSubmitTask
*/
ObStatisticsSubmitTask::ObStatisticsSubmitTask() {}

ObStatisticsSubmitTask::~ObStatisticsSubmitTask() {}

int ObStatisticsSubmitTask::init() {
  int ret = OB_SUCCESS;
  type_ = ObStatisticsTaskType::STATISTICS_SUBMIT_TASK;
  TESTBENCH_LOG(INFO, "statistics submit task init succeed", K(type_));
  return ret;
}

/*
                              ObStatisticsQueueTask
*/
ObStatisticsQueueTask::ObStatisticsQueueTask()
  : total_submit_cnt_(0),
    total_apply_cnt_(0),
    index_(-1),
    min_value_(),
    max_value_(),
    histogram_inited_(false),
    inner_allocator_("StatCollect")
  {
    min_value_.set_null();
    max_value_.set_null();
  }

ObStatisticsQueueTask::~ObStatisticsQueueTask() {}

int ObStatisticsQueueTask::init(int64_t index) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TESTBENCH_LOG(WARN, "statistics queue task invalid argument", K(type_), K(index), K(ret));
  } else {
    type_ = ObStatisticsTaskType::STATISTICS_QUEUE_TASK;
    index_ = index;
    TESTBENCH_LOG(INFO, "statistics queue task init succeed", K(type_));
  }
  return ret;
}

int ObStatisticsQueueTask::top(ObLatencyTask *&task) {
  int ret = OB_SUCCESS;
  common::ObLink *link = nullptr;
  if (OB_FAIL(queue_.top(link))) {
    TESTBENCH_LOG(WARN, "get statistics queue link top failed", K(ret));
  } else {
    task = ObLatencyTask::__get_class_address(link);
  }
  return ret;
}

int ObStatisticsQueueTask::pop() {
  common::ObLink *link = nullptr;
  return queue_.pop(link);
}

int ObStatisticsQueueTask::push(ObLatencyTask *task) {
  int ret = OB_SUCCESS;
  common::ObObj latency = task->get_latency();
  common::ObLink *link = ObLatencyTask::__get_member_address(task);
  if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    TESTBENCH_LOG(WARN, "get oblink from latency task failed", K(ret), KPC(task));
  } else if (OB_UNLIKELY(latency.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    TESTBENCH_LOG(WARN, "get null latency value", K(ret), KPC(task));
  } else if (OB_FAIL(queue_.push(link))) {
    TESTBENCH_LOG(WARN, "push oblink to the queue failed", K(ret));
  } else {
    if (min_value_.is_null() || latency.get_double() < min_value_.get_double()) {
      min_value_ = latency;
    }
    if (max_value_.is_null() || latency.get_double() > max_value_.get_double()) {
      max_value_ = latency;
    }
  }
  return ret;
}

void ObStatisticsQueueTask::inc_total_submit_cnt() {
  ++total_submit_cnt_;
}

void ObStatisticsQueueTask::inc_total_apply_cnt() {
  ++total_apply_cnt_;
}

int64_t ObStatisticsQueueTask::get_total_submit_cnt() const {
  return total_submit_cnt_;
}

int64_t ObStatisticsQueueTask::get_total_apply_cnt() const {
  return total_apply_cnt_;
}

int64_t ObStatisticsQueueTask::get_snapshot_queue_cnt() const {
  return total_submit_cnt_ - total_apply_cnt_;
}

double_t ObStatisticsQueueTask::get_min_value() const {
  return min_value_.get_double();
}

double_t ObStatisticsQueueTask::get_max_value() const {
  return max_value_.get_double();
}

void ObStatisticsQueueTask::set_histogram_inited() {
  histogram_inited_ = true;
}

/*
                          ObTestbenchStatisticsCollector
*/
ObTestbenchStatisticsCollector::ObTestbenchStatisticsCollector(int64_t bucket_capacity, double_t bucket_min_ratio, double_t bucket_max_ratio)
  : is_inited_(false),
    snapshot_ready_(0),
    bucket_capacity_(bucket_capacity),
    bucket_min_ratio_(bucket_min_ratio),
    bucket_max_ratio_(bucket_max_ratio),
    submit_(),
    submit_queues_{},
    histograms_{},
    allocator_("StatCollect")
  {}

ObTestbenchStatisticsCollector::~ObTestbenchStatisticsCollector() {}

int ObTestbenchStatisticsCollector::init() {
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    TESTBENCH_LOG(WARN, "statistics collector has already been inited", K(ret));
  } else if (OB_FAIL(submit_.init())) {
    TESTBENCH_LOG(ERROR, "statistics submit task init failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < TASK_QUEUE_SIZE; ++i) {
      if (OB_FAIL(submit_queues_[i].init(i))) {
        TESTBENCH_LOG(ERROR, "statistics submit queue task init failed", K(ret), K(i));
      } else if (OB_FAIL(histograms_[i].prepare_allocate_buckets(allocator_, bucket_capacity_))) {
        TESTBENCH_LOG(ERROR, "histogram allocate new buckets failed", K(ret), K(bucket_capacity_));
      } else {
        histograms_[i].set_sample_size(0);
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else if (OB_INIT_TWICE != ret) {
    destroy();
    TESTBENCH_LOG(ERROR, "statistics collector init failed", K(ret));
  }
  return ret;
}

int ObTestbenchStatisticsCollector::start() {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TESTBENCH_LOG(ERROR, "statistics collector is not inited", K(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::StatisticsCollectorPool, tg_id_))) {
    TESTBENCH_LOG(ERROR, "statistics collector create threadpool failed", K(ret), K(tg_id_));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    TESTBENCH_LOG(ERROR, "start statistics collector failed", K(tg_id_), K(ret));
  } else {
    TESTBENCH_LOG(INFO, "start statistics collector succeed", K(tg_id_), K(ret));
  }
  return ret;
}

void ObTestbenchStatisticsCollector::stop() {
  TESTBENCH_LOG(INFO, "statistics collector stop start");
  TG_STOP(tg_id_);
  TESTBENCH_LOG(INFO, "statistics collector stop finish");
}

void ObTestbenchStatisticsCollector::wait() {
  TESTBENCH_LOG(INFO, "statistics collector wait start");
  int64_t num = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(TG_GET_QUEUE_NUM(tg_id_, num)) && num > 0) {
    PAUSE();
  }
  if (OB_FAIL(ret)) {
    TESTBENCH_LOG(WARN, "statistics collector get queue number failed");
  }
  TG_WAIT(tg_id_);
  TESTBENCH_LOG(INFO, "statistics collector wait finish");
}

void ObTestbenchStatisticsCollector::destroy() {
  is_inited_ = false;
  if (-1 != tg_id_) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  TESTBENCH_LOG(INFO, "statistics collector destroy");
}

int ObTestbenchStatisticsCollector::push_task_(ObStatisticsTask *task) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TESTBENCH_LOG(ERROR, "statistics collector is not inited", K(ret));
  } else if (OB_ISNULL(task)) {
    TESTBENCH_LOG(ERROR, "task is null", K(ret));
  } else {
    while (OB_FAIL(TG_PUSH_TASK(tg_id_, task)) && OB_EAGAIN == ret) {
      ob_usleep(1000);
      TESTBENCH_LOG(ERROR, "failed to push task", K(ret));
    }
  }
  return ret;
}

int ObTestbenchStatisticsCollector::push_latency_task(ObLatencyTask *task) {
  int ret = OB_SUCCESS;
  ObLatencyTaskType type = task->get_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TESTBENCH_LOG(ERROR, "statistics collector is not inited", K(ret));
  } else if (OB_UNLIKELY(!submit_queues_[type].acquire_lease())) {
    ret = OB_ERR_UNEXPECTED;
    submit_queues_[type].revoke_lease();
    TESTBENCH_LOG(ERROR, "acquire thread lease failed", K(ret));
  } else if (OB_FAIL(submit_queues_[type].push(task))) {
    TESTBENCH_LOG(ERROR, "push latency task failed", K(submit_queues_[type]), K(type), K(ret));
  } else {
    submit_queues_[type].inc_total_submit_cnt();
    submit_queues_[type].revoke_lease();
  }
  return ret;
}

int ObTestbenchStatisticsCollector::sync_latency_task() {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TESTBENCH_LOG(ERROR, "statistics collector is not inited", K(ret));
  } else if (OB_FAIL(push_task_(&submit_))) {
    TESTBENCH_LOG(ERROR, "sync submit task failed", K(ret), K(submit_));
  }
  return ret;
}

int ObTestbenchStatisticsCollector::get_percentage_latency(ObLatencyTaskType type, double_t percentage, double_t &latency) {
  int ret = OB_SUCCESS;
  common::ObObj value;
  if (OB_FAIL(histograms_[type].get_percentage_value(percentage, value))) {
    TESTBENCH_LOG(WARN, "get percentage value failed", K(ret));
  } else {
    latency = value.get_double();
  }
  return ret;
}

const ObHistogram &ObTestbenchStatisticsCollector::get_histogram(ObLatencyTaskType type) const {
  return histograms_[type];
}

const ObStatisticsQueueTask &ObTestbenchStatisticsCollector::get_queue_task(ObLatencyTaskType type) const {
  return submit_queues_[type];
}

void ObTestbenchStatisticsCollector::handle(void *task) {
  int ret = OB_SUCCESS;
  ObStatisticsTask *task_to_handle = static_cast<ObStatisticsTask *>(task);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TESTBENCH_LOG(ERROR, "statistics collector is not inited", K(ret));
  } else if (OB_ISNULL(task_to_handle)) {
    ret = OB_INVALID_ARGUMENT;
    TESTBENCH_LOG(ERROR, "task is null", K(ret));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    task_to_handle = nullptr;
    TESTBENCH_LOG(ERROR, "statistics collector is not inited", K(ret));
  } else {
    ObStatisticsTaskType task_type = task_to_handle->get_type();
    if (ObStatisticsTaskType::STATISTICS_QUEUE_TASK == task_type) {
      ObStatisticsQueueTask *queue_task = static_cast<ObStatisticsQueueTask*>(task_to_handle);
      ret = handle_queue_task_(queue_task);
    } else if (ObStatisticsTaskType::STATISTICS_SUBMIT_TASK == task_type) {
      ret = handle_submit_task_();
    } else {
      ret = OB_ERR_UNEXPECTED;
      TESTBENCH_LOG(ERROR, "invalid task type", K(ret), K(task_type));
    }
  }
  if (OB_FAIL(ret) && nullptr != task_to_handle) {
    if (OB_FAIL(push_task_(task_to_handle))) {
      TESTBENCH_LOG(ERROR, "push task back after handle failed", K(ret), KPC(task_to_handle));
    }
  }
}

int ObTestbenchStatisticsCollector::handle_queue_task_(ObStatisticsQueueTask *task) {
  int ret = OB_SUCCESS;
  int64_t index = task->get_index();
  ObHistogram &histogram = histograms_[index];
  if (OB_UNLIKELY(!task->is_histogram_inited())) {
    double_t task_min_value = task->get_min_value();
    double_t task_max_value = task->get_max_value();
    double_t bucket_width = (task_max_value - task_min_value) / (bucket_capacity_ * (bucket_max_ratio_ - bucket_min_ratio_));
    double_t bucket_min_value = std::max(static_cast<double_t>(0), task_min_value - bucket_min_ratio_ * bucket_capacity_ * bucket_width);
    histogram.set_bucket_width(bucket_width);
    for (int64_t i = 0; i < bucket_capacity_; ++i) {
      common::ObObj value;
      value.set_double(bucket_min_value + bucket_width * i);
      ObHistBucket bucket(value, 0, 0, 0);
      if (OB_FAIL(histogram.add_bucket(bucket))) {
        TESTBENCH_LOG(WARN, "histogram initialization add bucket failed", K(ret), K(bucket));
      }
    }
    task->set_histogram_inited();
  }

  int64_t task_to_merge = task->get_snapshot_queue_cnt();
  for (int64_t i = 0; i < task_to_merge; ++i) {
    ObLatencyTask *latency_task = nullptr;
    if (OB_FAIL(task->top(latency_task))) {
      TESTBENCH_LOG(WARN, "get top latency task failed", K(ret));
    } else {
      const common::ObObj &latency = latency_task->get_latency();
      if (OB_FAIL(histogram.inc_endpoint_repeat_count(latency))) {
        TESTBENCH_LOG(WARN, "increase latency count failed", K(ret), K(latency));
      }
      task->inc_total_apply_cnt();
      if (OB_FAIL(task->pop())) {
        TESTBENCH_LOG(WARN, "pop top latency task failed", K(ret));
      }
      // TODO: memory leak?
      allocator_.free(latency_task);
    }
  }
  if (OB_SUCC(ret)) {
    TESTBENCH_LOG(TRACE, "process queued tasks succeed");
  }
  snapshot_ready_--;
  return ret;
}

int ObTestbenchStatisticsCollector::handle_submit_task_() {
  int ret = OB_SUCCESS;
  int64_t submit_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < TASK_QUEUE_SIZE; i++) {
    if (submit_queues_[i].get_total_apply_cnt() >= submit_queues_[i].get_total_submit_cnt()) {
      // do nothing
    } else if (OB_FAIL(push_task_(&submit_queues_[i]))) {
      TESTBENCH_LOG(ERROR, "submit queued tasks failed", K(submit_queues_[i]), K(ret));
    } else {
      submit_count++;
    }
  }
  if (OB_SUCC(ret)) {
    snapshot_ready_ = submit_count;
    TESTBENCH_LOG(TRACE, "submit queued tasks succeed");
  }
  return ret;
}

} // namespace testbench
} // namespace oceanbase