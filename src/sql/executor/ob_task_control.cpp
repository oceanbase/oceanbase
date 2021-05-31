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
 */

#define USING_LOG_PREFIX SQL_EXE

#include "sql/ob_sql_partition_location_cache.h"
#include "sql/executor/ob_task_control.h"
#include "sql/executor/ob_task_event.h"
#include "sql/executor/ob_addrs_provider.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

int ObTaskResult::init(const ObTaskLocation& task_location, const common::ObIArray<ObSliceEvent>& slice_events)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", K(ret), K(task_location_), K(slice_events_));
  } else if (OB_UNLIKELY(!task_location.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(task_location));
  } else {
    task_location_ = task_location;
    slice_events_ = &slice_events;
  }
  return ret;
}

bool ObTaskResult::has_empty_data() const
{
  bool ret = false;
  if (NULL != slice_events_ && slice_events_->count() > 0) {
    ret = slice_events_->at(0).has_empty_data();
  }
  return ret;
}

int64_t ObTaskResult::get_found_rows() const
{
  int64_t found_rows = 0;
  if (NULL != slice_events_ && slice_events_->count() > 0) {
    found_rows = slice_events_->at(0).get_found_rows();
  }
  return found_rows;
}

int64_t ObTaskResult::get_affected_rows() const
{
  int64_t affected_rows = 0;
  if (NULL != slice_events_ && slice_events_->count() > 0) {
    affected_rows = slice_events_->at(0).get_affected_rows();
  }
  return affected_rows;
}

int64_t ObTaskResult::get_duplicated_rows() const
{
  int64_t duplicated_rows = 0;
  if (NULL != slice_events_ && slice_events_->count() > 0) {
    duplicated_rows = slice_events_->at(0).get_duplicated_rows();
  }
  return duplicated_rows;
}

int64_t ObTaskResult::get_matched_rows() const
{
  int64_t matched_rows = 0;
  if (NULL != slice_events_ && slice_events_->count() > 0) {
    matched_rows = slice_events_->at(0).get_matched_rows();
  }
  return matched_rows;
}

ObTaskControl::ObTaskControl()
    : tasks_(),
      //    is_scan_job_(false),
      is_root_job_(false),
      finish_queue_(),
      is_select_plan_(false)
{}

int ObTaskControl::init_finish_queue(int64_t dop)
{
  int ret = OB_SUCCESS;
  if (dop < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid dop", K(dop), K(ret));
  } else {
    ret = finish_queue_.init(dop);
  }
  return ret;
}

ObTaskControl::~ObTaskControl()
{}

void ObTaskControl::reset()
{
  tasks_.reset();
  //  is_scan_job_ = false;
  is_root_job_ = false;
  finish_queue_.reset();
  is_select_plan_ = false;
}

int ObTaskControl::find_task(uint64_t task_id, ObTaskInfo*& task) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < tasks_.count(); ++i) {
    ObTaskInfo* task_info = tasks_.at(i);
    if (OB_ISNULL(task_info)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (task_info->get_task_location().get_task_id() == task_id) {
      task = task_info;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTaskControl::get_task_result(uint64_t task_id, ObTaskResult& task_result) const
{
  int ret = OB_SUCCESS;
  ObTaskInfo* task_info = NULL;
  if (OB_FAIL(find_task(task_id, task_info))) {
    LOG_WARN("fail to find task", K(ret), K(task_id));
  } else if (OB_ISNULL(task_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("succ to find task info, but task info is null", K(ret));
  } else if (OB_FAIL(task_result.init(task_info->get_task_location(), task_info->get_slice_events()))) {
    LOG_WARN("fail to init task result", K(ret), K(task_info->get_task_location()), K(task_info->get_slice_events()));
  }
  return ret;
}

int ObTaskControl::get_task_location(uint64_t task_id, ObTaskLocation& task_location) const
{
  int ret = OB_SUCCESS;
  ObTaskInfo* task_info = NULL;
  if (OB_FAIL(find_task(task_id, task_info))) {
    LOG_WARN("fail to find task", K(ret), K(task_id));
  } else if (OB_ISNULL(task_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("succ to find task info, but task info is null", K(ret));
  } else {
    task_location = task_info->get_task_location();
  }
  return ret;
}

int ObTaskControl::prepare(int64_t job_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (!is_root_job_) {
    int64_t parallel_degree = MIN(tasks_.count(), job_parallel_degree);
    for (int64_t i = 0; OB_SUCC(ret) && i < parallel_degree; ++i) {
      ObTaskInfo* task = tasks_.at(i);
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("task is NULL", K(ret), K(tasks_.count()), K(parallel_degree), K(job_parallel_degree));
      } else {
        task->set_state(OB_TASK_STATE_INITED);
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks_.count(); i++) {
      ObTaskInfo* task = tasks_.at(i);
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task is NULL", K(ret));
      } else {
        ObTaskLocation dummy_task_loc;
        task->set_task_location(dummy_task_loc);
        task->set_state(OB_TASK_STATE_INITED);
      }
    }
  }
  return ret;
}

int ObTaskControl::update_task_state(ObExecContext& ctx, ObTaskEvent& evt)
{
  int ret = OB_SUCCESS;
  ObTaskInfo* task_info = NULL;
  ObTaskLocation task_loc;
  bool retry_curr_task = false;

  if (!evt.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task event is invalid", K(ret), K(evt));
  } else if (!(task_loc = evt.get_task_location()).is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task location is invalid", K(ret), K(evt));
  } else if (OB_FAIL(find_task(task_loc.get_task_id(), task_info))) {
    LOG_WARN("fail to find task", K(task_loc.get_task_id()), K(ret));
  } else {
    int err_code = static_cast<int>(evt.get_err_code());
    switch (err_code) {
      case OB_ERR_TASK_SKIPPED: {
        task_info->set_state(OB_TASK_STATE_SKIPPED);
        LOG_WARN("task is skipped", K(ret), K(evt));
        break;
      }
      case OB_SUCCESS: {
        task_info->set_state(OB_TASK_STATE_FINISHED);
        // Deep copy first and then push into the queue to inform the main thread,
        // otherwise there will be concurrency problems.
        // The sche_allocator of the scheduling thread is used here,
        // otherwise there will be the problem of concurrent alloc memory
        if (OB_FAIL(task_info->deep_copy_slice_events(ctx.get_sche_allocator(), evt.get_slice_events()))) {
          LOG_WARN("fail to set small result list", K(ret));
        } else if (OB_FAIL(finish_queue_.push(id_to_ptr(task_loc.get_task_id())))) {
          LOG_WARN("fail to push task id into finish queue", K(ret), K(finish_queue_.size()));
        } else {
          task_info->set_task_recv_done(evt.get_task_recv_done());
          task_info->set_result_send_begin(evt.get_result_send_begin());
          task_info->set_result_recv_done(ObTimeUtility::current_time());
        }
        break;
      }
      default: {
        static const int MAX_TASK_RETRY_COUNT = 3;
        // Background task is for index building plan, can not do task retry,
        // because the task may still executing in background (doing notification).
        if (!task_info->is_background() &&
            (is_master_changed_error(err_code) || is_partition_change_error(err_code) ||
                is_snapshot_discarded_err(err_code) || is_data_not_readable_err(err_code)) &&
            is_select_plan_ && task_info->get_range_location().part_locs_.count() == 1 &&
            task_info->retry_times() < MAX_TASK_RETRY_COUNT) {
          // In order to retry the NOT_MASTER partition, the location cache of the corresponding partition
          // is updated directionally here.
          // Then mark the task status as INIT and notify the scheduler to reschedule the task
          if (OB_FAIL(update_location_cache_and_retry_task(ctx, *task_info, evt))) {
            LOG_WARN("fail update location cache and retry task", K(*task_info), K(ret));
          } else {
            // Do not generate the next task this time,
            // because the current task performs a retry operation will occupy a parallel slot
            retry_curr_task = true;
          }
        }

        if (!retry_curr_task) {
          // Task execution failed, state is set as OB_TASK_STATE_FAILED, ret returns evt.get_err_code()
          task_info->set_state(OB_TASK_STATE_FAILED);
          ret = err_code;
          LOG_WARN("task is not successfully executed", K(ret), K(evt));
          if (is_data_not_readable_err(err_code)) {
            int add_ret = OB_SUCCESS;
            if (OB_UNLIKELY(
                    OB_SUCCESS != (add_ret = ctx.get_scheduler_thread_ctx()
                                                 .get_scheduler_retry_info_for_update()
                                                 .add_invalid_server_distinctly(task_loc.get_server(), true)))) {
              LOG_WARN("fail to add dist addr to invalid servers distinctly",
                  K(ret),
                  K(add_ret),
                  K(err_code),
                  K(task_loc.get_server()));
            }
          }
        }
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_root_job_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root job should not got update task state signal", K(evt));
    } else if (!retry_curr_task) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tasks_.count(); ++i) {
        ObTaskInfo* task = tasks_.at(i);
        if (OB_ISNULL(task)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_TASK_STATE_NOT_INIT == task->get_state()) {
          // Every time a task is completed, the next task is driven to execute
          // This ensures that there are always parallel tasks in the pipeline
          task->set_state(OB_TASK_STATE_INITED);
          break;
        }
      }
    }
  }
  return ret;
}

int ObTaskControl::update_location_cache_and_retry_task(
    ObExecContext& exec_ctx, ObTaskInfo& task_info, ObTaskEvent& evt)
{
  int ret = OB_SUCCESS;
  ObTaskInfo::ObRangeLocation& range_loc = task_info.get_range_location();
  ObTaskExecutorCtx& task_exec_ctx = exec_ctx.get_task_exec_ctx();
  ObSqlPartitionLocationCache* cache = nullptr;
  if (range_loc.part_locs_.count() > 1) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(
                 cache = static_cast<ObSqlPartitionLocationCache*>(task_exec_ctx.get_partition_location_cache()))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    share::schema::ObSchemaGetterGuard* old_guard = cache->get_schema_guard();
    ObTaskInfo::ObPartLoc& part_loc = range_loc.part_locs_.at(0);
    ObPartitionKey& part_key = part_loc.partition_key_;
    ObPartitionLocation dummy_loc;
    ObReplicaLocation replica_leader_loc;
    // For the get interface, you need to pass a maximum value,
    // which means you need to get the latest location cache and invalidate the old one
    const int64_t expire_renew_time = INT64_MAX;
    bool is_cache_hit = false;
    share::schema::ObSchemaGetterGuard schema_guard;
    const uint64_t tenant_id = part_key.get_tenant_id();
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(part_key));
    } else if (FALSE_IT(cache->set_schema_guard(&schema_guard))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be here", K(ret));
    } else if (OB_FAIL(cache->get(part_key, dummy_loc, expire_renew_time, is_cache_hit))) {
      LOG_WARN("LOCATION: refresh cache failed", K(ret), K(part_key), K(expire_renew_time));
    } else if (OB_FAIL(dummy_loc.get_strong_leader(replica_leader_loc))) {
      LOG_WARN("fail get leader", K(part_key), K(ret));
    } else {
      task_info.get_task_location().set_server(replica_leader_loc.server_);
      task_info.get_range_location().server_ = replica_leader_loc.server_;
      evt.rewrite_err_code(OB_SUCCESS);  // Mandatory set to SUCC, let the upper layer continue to retry
      LOG_INFO("LOCATION: refresh partition location cache for single partition retry succ",
          K(part_key),
          K(dummy_loc),
          K(replica_leader_loc),
          K(is_cache_hit));
    }
    cache->set_schema_guard(old_guard);
  }

  if (OB_SUCC(ret)) {
    task_info.set_state(OB_TASK_STATE_INITED);
    task_info.inc_retry_times();
  }
  return ret;
}

bool ObTaskControl::all_tasks_finished_or_skipped_or_failed() const
{
  bool is_finished = true;
  for (int64_t i = 0; true == is_finished && i < tasks_.count(); ++i) {
    if (OB_TASK_STATE_FINISHED != tasks_.at(i)->get_state() && OB_TASK_STATE_SKIPPED != tasks_.at(i)->get_state() &&
        OB_TASK_STATE_FAILED != tasks_.at(i)->get_state()) {
      is_finished = false;
    }
  }
  return is_finished;
}

bool ObTaskControl::all_tasks_run() const
{
  bool is_run = true;
  for (int64_t i = 0; true == is_run && i < tasks_.count(); ++i) {
    ObTaskInfo* task = tasks_.at(i);
    if (OB_ISNULL(task)) {
      is_run = false;
      LOG_ERROR("task is null", K(i), K(tasks_.count()));
    } else if (OB_TASK_STATE_FINISHED != task->get_state() && OB_TASK_STATE_RUNNING != task->get_state() &&
               OB_TASK_STATE_SKIPPED != task->get_state() && OB_TASK_STATE_FAILED != task->get_state()) {
      is_run = false;
    }
  }
  return is_run;
}

int ObTaskControl::get_ready_tasks(common::ObIArray<ObTaskInfo*>& tasks) const
{
  int ret = OB_SUCCESS;
  bool iter_end = true;
  const int64_t count = tasks_.count();
  tasks.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObTaskInfo* task = tasks_.at(i);
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_TASK_STATE_NOT_INIT == task->get_state()) {
      iter_end = false;
    } else if (OB_TASK_STATE_INITED == task->get_state()) {
      iter_end = false;
      ret = tasks.push_back(tasks_.at(i));
    }
  }
  if (iter_end && OB_SUCC(ret)) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObTaskControl::get_running_tasks(common::ObIArray<ObTaskInfo*>& tasks) const
{
  return get_task_by_state(tasks, OB_TASK_STATE_RUNNING);
}

int ObTaskControl::get_finished_tasks(common::ObIArray<ObTaskInfo*>& tasks) const
{
  return get_task_by_state(tasks, OB_TASK_STATE_FINISHED);
}

int ObTaskControl::get_skipped_tasks(common::ObIArray<ObTaskInfo*>& tasks) const
{
  return get_task_by_state(tasks, OB_TASK_STATE_SKIPPED);
}

int ObTaskControl::get_failed_tasks(common::ObIArray<ObTaskInfo*>& tasks) const
{
  return get_task_by_state(tasks, OB_TASK_STATE_FAILED);
}

int ObTaskControl::get_begin_running_tasks(common::ObIArray<ObTaskInfo*>& tasks) const
{
  int ret = OB_SUCCESS;
  const int64_t count = tasks_.count();
  tasks.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (tasks_.at(i)->get_state() >= OB_TASK_STATE_RUNNING) {
      ret = tasks.push_back(tasks_.at(i));
    }
  }
  return ret;
}

int ObTaskControl::get_all_tasks(common::ObIArray<ObTaskInfo*>& tasks) const
{
  int ret = OB_SUCCESS;
  tasks.reset();
  if (OB_FAIL(tasks.assign(tasks_))) {
    LOG_WARN("fail to assign task array", K(ret), K(tasks_.count()));
  }
  return ret;
}

int ObTaskControl::get_task_by_state(common::ObIArray<ObTaskInfo*>& tasks_out, int state) const
{
  int ret = OB_SUCCESS;
  const int64_t count = tasks_.count();
  tasks_out.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (state == tasks_.at(i)->get_state()) {
      ret = tasks_out.push_back(tasks_.at(i));
    }
  }
  return ret;
}

int ObTaskControl::get_finished_slice_events(common::ObIArray<const ObSliceEvent*>& out_slice_events)
{
  int ret = OB_SUCCESS;
  int64_t task_count = tasks_.count();
  out_slice_events.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < task_count; i++) {
    if (OB_TASK_STATE_FINISHED == tasks_.at(i)->get_state()) {
      const common::ObIArray<ObSliceEvent>& slice_events = tasks_.at(i)->get_slice_events();
      int64_t slice_count = slice_events.count();
      for (int64_t j = 0; OB_SUCC(ret) && j < slice_count; j++) {
        const ObSliceEvent& slice_event = slice_events.at(j);
        if (slice_event.has_empty_data()) {
          // nothing.
        } else if (OB_FAIL(out_slice_events.push_back(&slice_events.at(j)))) {
          LOG_WARN("fail to push back slice event", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTaskControl::append_finished_slice_events(ObIArray<const ObSliceEvent*>& slice_events, bool skip_empty)
{
  int ret = OB_SUCCESS;
  int64_t task_count = tasks_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < task_count; i++) {
    if (OB_TASK_STATE_FINISHED == tasks_.at(i)->get_state()) {
      const common::ObIArray<ObSliceEvent>& task_slice_events = tasks_.at(i)->get_slice_events();
      int64_t slice_count = task_slice_events.count();
      for (int64_t j = 0; OB_SUCC(ret) && j < slice_count; j++) {
        const ObSliceEvent& slice_event = task_slice_events.at(j);
        if (slice_event.has_empty_data() && skip_empty) {
          // nothing.
        } else if (OB_FAIL(slice_events.push_back(&task_slice_events.at(j)))) {
          LOG_WARN("fail to push back slice event", K(ret));
        }
      }
    } else if (!skip_empty) {
      // In this case, it will cause ObTaskOrderReceive to die and wait other data,
      // so an error will be reported temporarily
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("can not skip failed task now", K(ret), K(*tasks_.at(i)));
    }
  }
  return ret;
}
