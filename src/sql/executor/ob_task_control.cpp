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

#include "sql/executor/ob_task_control.h"
#include "sql/executor/ob_task_event.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

int ObTaskResult::init(const ObTaskLocation &task_location,
                       const common::ObIArray<ObSliceEvent> &slice_events)
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
    is_select_plan_(false)
{
}

ObTaskControl::~ObTaskControl()
{}

void ObTaskControl::reset()
{
  tasks_.reset();
//  is_scan_job_ = false;
  is_root_job_ = false;
  is_select_plan_ = false;
}

int ObTaskControl::find_task(uint64_t task_id, ObTaskInfo *&task) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < tasks_.count(); ++i) {
    ObTaskInfo *task_info = tasks_.at(i);
    if (OB_ISNULL(task_info)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (task_info->get_task_location().get_task_id() == task_id) {
      task = task_info;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTaskControl::get_task_location(uint64_t task_id, ObTaskLocation &task_location) const
{
  int ret = OB_SUCCESS;
  ObTaskInfo *task_info = NULL;
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
      ObTaskInfo *task = tasks_.at(i);
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("task is NULL", K(ret), K(tasks_.count()),
                  K(parallel_degree), K(job_parallel_degree));
      } else {
        task->set_state(OB_TASK_STATE_INITED);
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks_.count(); i++) {
      ObTaskInfo *task = tasks_.at(i);
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

int ObTaskControl::get_ready_tasks(common::ObIArray<ObTaskInfo *> &tasks) const
{
  int ret = OB_SUCCESS;
  bool iter_end = true;
  const int64_t count = tasks_.count();
  tasks.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObTaskInfo *task = tasks_.at(i);
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

int ObTaskControl::get_running_tasks(common::ObIArray<ObTaskInfo *> &tasks) const
{
  return get_task_by_state(tasks, OB_TASK_STATE_RUNNING);
}

int ObTaskControl::get_finished_tasks(common::ObIArray<ObTaskInfo *> &tasks) const
{
  return get_task_by_state(tasks, OB_TASK_STATE_FINISHED);
}

int ObTaskControl::get_skipped_tasks(common::ObIArray<ObTaskInfo *> &tasks) const
{
  return get_task_by_state(tasks, OB_TASK_STATE_SKIPPED);
}

int ObTaskControl::get_failed_tasks(common::ObIArray<ObTaskInfo *> &tasks) const
{
  return get_task_by_state(tasks, OB_TASK_STATE_FAILED);
}

int ObTaskControl::get_begin_running_tasks(common::ObIArray<ObTaskInfo *> &tasks) const
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

int ObTaskControl::get_all_tasks(common::ObIArray<ObTaskInfo *> &tasks) const
{
  int ret = OB_SUCCESS;
  tasks.reset();
  if (OB_FAIL(tasks.assign(tasks_))) {
    LOG_WARN("fail to assign task array", K(ret), K(tasks_.count()));
  }
  return ret;
}

int ObTaskControl::get_task_by_state(common::ObIArray<ObTaskInfo *> &tasks_out, int state) const
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
