/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * Partitioned transaction dispatcher, responsible for dispatching partitioned transaction tasks
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_part_trans_dispatcher.h"   // PartTransDispatcher

#include "share/ob_errno.h"                 // OB_SUCCESS
#include "lib/utility/ob_macro_utils.h"     // OB_UNLIKELY
#include "lib/oblog/ob_log_module.h"        // LOG_ERROR

#include "ob_log_fetcher_dispatcher.h"      // IObLogFetcherDispatcher
#include "ob_log_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
int64_t PartTransDispatcher::g_part_trans_task_count = 0;

//////////////////////////////////// TransCommitInfo ///////////////////////////////////

void TransCommitInfo::reset()
{
  log_lsn_.reset();
  log_ts_ = OB_INVALID_TIMESTAMP;
}

TransCommitInfo::TransCommitInfo(const palf::LSN &log_lsn, const int64_t log_ts) :
    log_lsn_(log_lsn),
    log_ts_(log_ts)
{
}

//////////////////////////////////// PartTransDispatcher ///////////////////////////////////

PartTransDispatcher::PartTransDispatcher(const char *tls_id_str,
    TaskPool &task_pool,
    PartTransTaskMap &task_map,
    IObLogFetcherDispatcher &dispatcher):
    tls_id_(),
    tls_id_str_(tls_id_str),
    task_pool_(task_pool),
    task_map_(task_map),
    dispatcher_(dispatcher),
    task_queue_(),
    init_dispatch_progress_(OB_INVALID_TIMESTAMP),
    last_dispatch_progress_(OB_INVALID_TIMESTAMP),
    last_dispatch_log_lsn_(),
    task_count_only_in_map_(0),
    checkpoint_(OB_INVALID_VERSION),
    created_trans_count_(0),
    last_created_trans_count_(0),
    last_stat_time_(get_timestamp()),
    dispatch_lock_()
{}

PartTransDispatcher::~PartTransDispatcher()
{
  // TODO: Consider cleaning up the data, but make sure to call clean_task to empty the task when the partition is offline
  // That is, task_queue and task_map should not have tasks from this partition in them
  // If want to clean tasks here, we need to save the tls_id, but for the time being, don't need to save the tls_id in order to optimise memory usage
  if (task_queue_.size() > 0) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "task_queue_ is not empty", K(task_queue_.size()), KPC(task_queue_.top()));
  }

  tls_id_.reset();
  tls_id_str_ = NULL;
  init_dispatch_progress_ = OB_INVALID_TIMESTAMP;
  last_dispatch_progress_ = OB_INVALID_TIMESTAMP;
  last_dispatch_log_lsn_.reset();
  task_count_only_in_map_ = 0;
  checkpoint_ = OB_INVALID_VERSION;
  created_trans_count_ = 0;
  last_created_trans_count_ = 0;
  last_stat_time_ = 0;
}

int PartTransDispatcher::init(const logservice::TenantLSID &tls_id,
    const int64_t start_tstamp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_tstamp)) {
    LOG_ERROR("invalid argument", K(start_tstamp), K(tls_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tls_id_ = tls_id;
    // Only the necessary fields are initialized here, all other fields are initialized during construction
    // 1. When there is a transaction in the partition with the same timestamp as the start timestamp and it has not been sent,
    // it is possible that the heartbeat will fall back because it takes the "pending task timestamp-1".
    // 2. so initialize progress to start timestamp-1
    init_dispatch_progress_ = start_tstamp - 1;
    last_dispatch_progress_ = start_tstamp - 1;
  }
  return ret;
}

// The heartbeat interface only produces heartbeat tasks, it does not consume them
// requires only one producer, no need to add production lock here
int PartTransDispatcher::heartbeat(const int64_t hb_tstamp)
{
  int ret = OB_SUCCESS;
  PartTransTask *task = NULL;

  if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == hb_tstamp)) {
    LOG_ERROR("invalid heartbeat timestamp", K(hb_tstamp), K(tls_id_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(task = task_pool_.get(tls_id_str_, tls_id_))) {
    LOG_ERROR("alloc part trans task fail", K(task));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(task->init_ls_heartbeat_info(tls_id_, hb_tstamp))) {
    LOG_ERROR("init_ls_heartbeat_info fail", KR(ret), K_(tls_id), K(hb_tstamp), KPC(task));
  } else {
    push_task_queue_(*task);
  }

  if (OB_SUCCESS != ret && NULL != task) {
    task->revert();
    task = NULL;
  }

  return ret;
}

void PartTransDispatcher::update_status_after_consume_task_(const int64_t trans_prepare_ts,
    const palf::LSN& prepare_log_lsn)
{
  if (OB_INVALID_TIMESTAMP != trans_prepare_ts) {
    last_dispatch_progress_ = std::max(last_dispatch_progress_, trans_prepare_ts);
  }

  if (prepare_log_lsn.is_valid()) {
    last_dispatch_log_lsn_ = prepare_log_lsn;
  }
  LOG_DEBUG("update part_dispatch_progress", K_(tls_id), K_(last_dispatch_progress), K_(last_dispatch_log_lsn));
}

int PartTransDispatcher::dispatch_part_trans_task_(PartTransTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  // Save the basic information of the task in advance, the output will not be accessible afterwards
  const int64_t prepare_ts = task.get_prepare_ts();
  const int64_t trans_commit_version = task.get_trans_commit_version();
  // Note:
  // 1. prepare_log_lsn is only valid for DDL and DML types
  // 2. should not use reference of prepare_log_lsn in case of usage of prepare_lsn after part_trans_task recycled
  const palf::LSN prepare_lsn = task.get_prepare_log_lsn();

  if (OB_INVALID_TIMESTAMP != prepare_ts) {
    // Check if progress is backed up
    // The progress here may indeed be backwards because the start time of the target partition is the maximum value
    // calculated based on the DDL partition transaction's prepare log timestamp and schema_version,
    // in order to prevent the heartbeat timestamp from being backwards; whereas the target partition's own transaction
    // timestamp is generated locally, and although the master guarantees that the prepare log timestamp is greater
    // than or equal to the GTS time, but only if the target partition and the DDL partition are the same tenant can
    // their own prepare timestamp be guaranteed by GTS to be greater than or equal to the DDL partition transaction's
    // prepare timestamp. However, if the DDL partition and the target partition are not the same tenant, GTS does
    // not have a corresponding guarantee. Therefore, it is normal for data progress to be rolled back here. But it is
    // dangerous. This problem can only be completely solved when the schema is split within the tenant.
    if (OB_UNLIKELY(prepare_ts < last_dispatch_progress_)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "partition dispatch progress is rollback, we should check it",
          K_(tls_id),
          "prepare_ts", NTS_TO_STR(prepare_ts),
          "prepare_lsn", prepare_lsn,
          "trans_commit_version", trans_commit_version,
          "last_dispatch_progress", NTS_TO_STR(last_dispatch_progress_),
          K(last_dispatch_progress_),
          K(last_dispatch_log_lsn_),
          K(task),
          K(stop_flag));
    }
  }

  if (OB_FAIL(handle_before_fetcher_dispatch_(task))) {
    LOG_ERROR("handle_before_fetcher_dispatch_ fail", KR(ret), K(task));
  } else if (OB_FAIL(dispatcher_.dispatch(task, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("fetcher dispatch task fail", KR(ret), K(task));
    }
  } else {
    // Task output successful, update progress values
    update_status_after_consume_task_(prepare_ts, prepare_lsn);
  }

  // Regardless of whether the dispatch task completes successfully or not, reset the task information recorded by the queue
  task_queue_.reset_dispatched_task_info();
  return ret;
}

int PartTransDispatcher::handle_before_fetcher_dispatch_(PartTransTask &task)
{
  int ret = OB_SUCCESS;

  if (task.is_dml_trans()) {
    if (OB_FAIL(handle_dml_trans_before_fetcher_dispatch_(task))) {
      LOG_ERROR("handle_dml_trans_before_fetcher_dispatch_ fail", KR(ret), K(task));
    }
  } else {
    // do nothing
  }

  return ret;
}

int PartTransDispatcher::handle_dml_trans_before_fetcher_dispatch_(PartTransTask &task)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(task.try_to_set_data_ready_status())) {
    LOG_ERROR("try_to_set_data_ready_status fail", KR(ret), K(task));
  }

  return ret;
}

void PartTransDispatcher::push_task_queue_(PartTransTask &task)
{
  task_queue_.push(&task);

  if (task.is_dml_trans() || task.is_ddl_trans()) {
    ATOMIC_INC(&created_trans_count_);
  }

  // Update the global task count when the task queue task count changes
  ATOMIC_INC(&g_part_trans_task_count);
}

PartTransTask *PartTransDispatcher::pop_task_queue_()
{
  PartTransTask *ret = task_queue_.pop();
  if (NULL != ret) {
    // Update the global task count when the task queue task count changes
    ATOMIC_DEC(&g_part_trans_task_count);
  }
  return ret;
}

int PartTransDispatcher::submit_task(PartTransTask &task)
{
  int ret = OB_SUCCESS;
  push_task_queue_(task);

  // into the queue, minus the number in the map only
  // Update the global number of tasks at the same time
  ATOMIC_DEC(&task_count_only_in_map_);
  ATOMIC_DEC(&g_part_trans_task_count);
  return ret;
}

int PartTransDispatcher::check_task_ready_(PartTransTask &task, bool &task_is_ready)
{
  int ret = OB_SUCCESS;

  if (task.is_dml_trans() || task.is_ddl_trans() || task.is_ls_op_trans() || task.is_trans_type_unknown()) {
    // trans type should be valid if committed.
    if (OB_SUCC(ret)) {
      task_is_ready = task.is_trans_committed();
      // If the task is ready for output, remove it from the map
      // Note: The logic for removing from map is placed here because the task in map must be a DDL
      // or DML type transaction, other types of task will not be placed in map.
      //
      // In order to be compatible with more types of tasks in the future and other tasks do not need to be
      // removed from the map, DML and DDL type tasks are removed from the map as soon as they are detected as ready.
      if (task_is_ready) {
        PartTransID part_trans_id(task.get_tls_id(), task.get_trans_id());
        // Requires that it must exist
        if (OB_FAIL(task_map_.erase(part_trans_id))) {
          LOG_ERROR("erase from task map fail", KR(ret), K(part_trans_id), K(task));
        }
      }
    }
  } else {
    // Other types of tasks are ready by default
    task_is_ready = true;
  }
  return ret;
}


int64_t PartTransDispatcher::get_total_task_count_()
{
  // in MAP only + in QUEUE
  return ATOMIC_LOAD(&task_count_only_in_map_) + task_queue_.size();
}

int PartTransDispatcher::dispatch_part_trans(volatile bool &stop_flag, int64_t &pending_task_count)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(dispatch_lock_);

  while (OB_SUCCESS == ret && ! stop_flag) {
    PartTransTask *task = task_queue_.top();
    bool task_is_ready = false;

    // If the task is not ready, exit the loop
    if (NULL == task) {
      ret = OB_EAGAIN;
    } else if (OB_FAIL(check_task_ready_(*task, task_is_ready))) {
      LOG_ERROR("check_task_ready_ fail", KR(ret), KPC(task));
    } else if (! task_is_ready) {
      ret = OB_EAGAIN;
    } else {
      // pop out data
      (void)pop_task_queue_();

      if (OB_UNLIKELY(! task->is_dml_trans()
            && ! task->is_ddl_trans()
            && ! task->is_ls_heartbeat()
            && ! task->is_ls_op_trans())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid task which is not DML or DDL task", KR(ret), K_(tls_id), K(task_is_ready), KPC(task));
      }
      // dispatch the current task
      else if (OB_FAIL(dispatch_part_trans_task_(*task, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("dispatch_part_trans_task_ fail", KR(ret), KPC(task));
        }
      } else {
        task = NULL;
      }
    }
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  if (OB_EAGAIN == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCCESS == ret) {
    pending_task_count = get_total_task_count_();
  }

  return ret;
}

int PartTransDispatcher::dispatch_offline_ls_task(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(dispatch_lock_);
  PartTransTask *task = NULL;

  // Because offline partitioned tasks are generated in real time and then sent down in real time,
  // it is important to ensure that the task queue is empty and there are no queued tasks ahead
  // Another purpose is to reclaim all memory and avoid memory leaks
  if (OB_UNLIKELY(task_queue_.size() > 0)) {
    LOG_ERROR("there are tasks not dispatched, cat not dispatch offline task",
        "task_queue_size", task_queue_.size());
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_ISNULL(task = task_pool_.get(tls_id_str_, tls_id_))) {
    LOG_ERROR("alloc part trans task fail", K(task));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(task->init_offline_ls_task(tls_id_))) {
    LOG_ERROR("init_offline_partition_task fail", KR(ret), K_(tls_id), KPC(task));
  } else if (OB_FAIL(dispatch_part_trans_task_(*task, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("dispatch task fail", KR(ret), KPC(task), K(task));
    }
  } else {
    task = NULL;
  }

  if (OB_FAIL(ret)) {
    if (NULL != task) {
      task->revert();
      task = NULL;
    }
  }

  return ret;
}

struct TaskMapCleaner
{
  bool operator()(const PartTransID &key, PartTransTask *&val)
  {
    bool bool_ret = false;
    const bool is_sys_ls = key.is_sys_ls();

    if (key.tls_id_ == tls_id_) {
      LOG_DEBUG("TaskMapCleaner operator", K(tls_id_), K(val), KPC(val));

      if (NULL != val && is_sys_ls) {
        val->revert();
      }

      val = NULL;
      bool_ret = true;
      count_++;
    }

    return bool_ret;
  }

  TaskMapCleaner(const logservice::TenantLSID &tls_id) : count_(0), tls_id_(tls_id) {}

  int64_t               count_;
  const logservice::TenantLSID      &tls_id_;
};

// Iterate through all tasks, ready or not, and revert them directly
// The aim is to clear all tasks
int PartTransDispatcher::clean_task()
{
  int ret = OB_SUCCESS;
  TaskMapCleaner map_cleaner(tls_id_);
  const bool is_sys_ls = tls_id_.is_sys_log_stream();

  // Clearance tasks are also a form of consumption and require the addition of a consumption lock
  ObByteLockGuard guard(dispatch_lock_);

  // First recycle all tasks in the queue, which may all be in the map, so delete the corresponding data items in the map at the same time
  // pop out the tasks in the queue, whether they are ready or not
  PartTransTask *task = NULL;
  int64_t task_queue_size = task_queue_.size();

  while (NULL != (task = pop_task_queue_()) && OB_SUCCESS == ret) {
    // Delete DML and DDL type transactions from map
    if (task->is_dml_trans()
        || task->is_ddl_trans()
        || task->is_ls_op_trans()
        || task->is_trans_type_unknown()) {
      PartTransID part_trans_id(task->get_tls_id(), task->get_trans_id());
      if (OB_FAIL(task_map_.erase(part_trans_id))) {
        // It is normal for Map not to exist–
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("erase from task map fail", KR(ret), K(part_trans_id), K(task));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      // Update state after consumption is complete
      update_status_after_consume_task_(task->get_prepare_ts(), task->get_prepare_log_lsn());
      // recycle task
      if (is_sys_ls) {
        task->revert();
      }
    }
  }

  if (OB_SUCCESS == ret) {
    // Clear all tasks in the Map that are not in the queue
    if (OB_FAIL(task_map_.remove_if(map_cleaner))) {
      LOG_ERROR("remove from map fail", KR(ret), K_(tls_id));
    } else {
      // Subtract the number of tasks that are only in the map
      (void)ATOMIC_AAF(&g_part_trans_task_count, -map_cleaner.count_);
      (void)ATOMIC_AAF(&task_count_only_in_map_, -map_cleaner.count_);
    }
  }

  if (OB_SUCCESS == ret) {
    if (task_count_only_in_map_ != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("task_count_only_in_map_ != 0 after clean all task, unexcepted error",
          K(task_count_only_in_map_), K_(tls_id), K(task_queue_size),
          "task_map_size", map_cleaner.count_);
    }
  }

  LOG_INFO("part trans resolver clean task", KR(ret), K_(tls_id), K(task_queue_size),
      "task_map_size", map_cleaner.count_,
      K(task_count_only_in_map_), K(g_part_trans_task_count),
      K(last_dispatch_progress_), K(last_dispatch_log_lsn_));

  return ret;
}

int PartTransDispatcher::remove_task(const bool is_sys_ls, const transaction::ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  // To remove a task from a Queue and therefore add a lock
  ObByteLockGuard guard(dispatch_lock_);
  PartTransTask *task = NULL;
  PartTransID part_trans_id(tls_id_, trans_id);

  // Remove the corresponding task from the Map and return the object value
  if (OB_FAIL(task_map_.erase(part_trans_id, task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("get task from map fail", KR(ret), K(part_trans_id));
    }
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid task", K(task), K(part_trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    bool exist_in_queue = false;
    // Picking tasks from a Queue's chain
    task_queue_.remove(task, exist_in_queue);

    // ddl revert task
    if (is_sys_ls) {
      LOG_TRACE("remove_task", K(is_sys_ls), "trans_id", task->get_trans_id());
      task->revert();
      task = NULL;
    } else {
      if (OB_FAIL(task->handle_unserved_trans())) {
        LOG_ERROR("handle_unserved_trans fail", KR(ret));
      } else {
        // Note, task may be recycled, we can not handle it
      }
    }

    if (! exist_in_queue) {
      ATOMIC_DEC(&task_count_only_in_map_);
    }

    ATOMIC_DEC(&g_part_trans_task_count);
  }

  return ret;
}

// No concurrent update of task queues involved, just application data structures, no locking required
int PartTransDispatcher::alloc_task(const PartTransID &part_trans_id, PartTransTask *&task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!part_trans_id.is_valid() || part_trans_id.get_tls_id() != tls_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid PartTransID to alloc PartTransTask", KR(ret), K(part_trans_id), K_(tls_id));
  } else if (OB_ISNULL(task = task_pool_.get(tls_id_str_, tls_id_))) {
    LOG_ERROR("alloc part trans task fail", K(task), K_(tls_id));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  // Inserted into Map for subsequent queries based on TransID
  } else if (OB_FAIL(task_map_.insert(part_trans_id, task))) {
    task->revert();
    task = NULL;
    LOG_ERROR("insert part trans task fail", KR(ret), K(part_trans_id));
  } else if (OB_FAIL(task->init_log_entry_task_allocator())) {
    LOG_ERROR("init log_entry_task base allocator failed", KR(ret), KPC(task));
  } else {
    task->set_trans_id(part_trans_id.get_tx_id());
    // This task is only in MAP, not in QUEUE
    ATOMIC_INC(&task_count_only_in_map_);
    ATOMIC_INC(&g_part_trans_task_count);
  }

  return ret;
}

int PartTransDispatcher::get_task(const PartTransID &trans_id, PartTransTask *&task)
{
  int ret = OB_SUCCESS;
  task = NULL;

  if (OB_FAIL(task_map_.get(trans_id, task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get part trans task fail", KR(ret), K(trans_id));
    }
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("part_trans_task is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (! task->is_served()) {
      ret = OB_INVALID_ERROR;
    }
  }

  return ret;
}

// To eliminate loop dependency issues, bug logging
// dispatch_progress information does not use dispatch locks, uses task_queue_ locks, risky when getting dispatch_info
//
// 1. get progress based on last dispatch progress
// 2. Update dispatch progress if there are tasks that are dispatching or ready to be dispatched
int PartTransDispatcher::get_dispatch_progress(int64_t &dispatch_progress,
    logfetcher::PartTransDispatchInfo &dispatch_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == last_dispatch_progress_)) {
    LOG_ERROR("invalid last_dispatch_progress_", K(last_dispatch_progress_));
    ret = OB_NOT_INIT;
  }
  // Get PartTransDispatcher member values without locking
  else {
    // The default is to take the progress of the last issue
    dispatch_progress = ATOMIC_LOAD(&last_dispatch_progress_);
    dispatch_info.last_dispatch_log_lsn_ = palf::LSN(ATOMIC_LOAD(&last_dispatch_log_lsn_.val_));
    dispatch_info.current_checkpoint_ = ATOMIC_LOAD(&checkpoint_);

    // Access is not atomic and may be biased
    dispatch_info.pending_task_count_ = get_total_task_count_();

    // Update dispatch progress based on the task being dispatched
    task_queue_.update_dispatch_progress_by_task_queue(dispatch_progress, dispatch_info);
  }

  return ret;
}

double PartTransDispatcher::get_tps()
{
  int64_t current_timestamp = get_timestamp();
  int64_t local_created_trans_count = ATOMIC_LOAD(&created_trans_count_);
  int64_t local_last_created_trans_count = ATOMIC_LOAD(&last_created_trans_count_);
  int64_t local_last_stat_time = last_stat_time_;
  int64_t delta_create_count = local_created_trans_count - local_last_created_trans_count;
  int64_t delta_time = current_timestamp - local_last_stat_time;
  double create_tps = 0.0;

  // Update the last statistics
  last_created_trans_count_ = local_created_trans_count;
  last_stat_time_ = current_timestamp;

  if (delta_time > 0) {
    create_tps = (double)(delta_create_count) * 1000000.0 / (double)delta_time;
  }

  return create_tps;
}

int PartTransDispatcher::update_checkpoint(int64_t new_checkpoint)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_VERSION == new_checkpoint)) {
    LOG_ERROR("invalid argument", K(new_checkpoint));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // prepare log records the checkpoint, which is not guaranteed to be incremented due to concurrent transaction scenarios
    // so checkpoint is only updated when it is bigger
    if (new_checkpoint > checkpoint_) {
      ATOMIC_STORE(&checkpoint_, new_checkpoint);
    }
  }

  return ret;
}

}
}
