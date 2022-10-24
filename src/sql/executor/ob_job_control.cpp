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

#include "sql/executor/ob_job_control.h"
#include "sql/executor/ob_task_event.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/exchange/ob_transmit_op.h"

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;

volatile uint64_t ObJobControl::global_job_id_ = 0;

ObJobControl::ObJobControl() : jobs_()
{
}

ObJobControl::~ObJobControl()
{
  for (int64_t i = 0; i < jobs_.count(); ++i) {
    ObJob *job = jobs_.at(i);
    if (NULL != job) {
      job->~ObJob();
    }
  }
}

void ObJobControl::reset()
{
  for (int64_t i = 0; i < jobs_.count(); ++i) {
    ObJob *job = jobs_.at(i);
    if (NULL != job) {
      job->reset();
    }
  }
  jobs_.reset();
}

int ObJobControl::all_jobs_finished(bool &is_finished) const
{
  int ret = OB_SUCCESS;
  bool finished = true;
  for (int64_t i = 0; OB_SUCC(ret) && true == finished
       && i < jobs_.count(); ++i) {
    ObJob *job = jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      finished = false;
      LOG_ERROR("job is NULL", K(i), K(ret));
    } else if (OB_JOB_STATE_FINISHED != job->get_state()) {
      finished = false;
    } else {
      //empty
    }
  }
  is_finished = finished;
  return ret;
}

int ObJobControl::all_jobs_finished_except_root_job(bool &is_finished) const
{
  int ret = OB_SUCCESS;
  bool finished = true;
  if (jobs_.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    finished = false;
    LOG_ERROR("count of jobs is less than 1",
              K(ret), "job_count", jobs_.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && true == finished
         && i < jobs_.count() - 1; ++i) {
      ObJob *job = jobs_.at(i);
      if (OB_I(t1) OB_ISNULL(job)) {
        ret = OB_ERR_UNEXPECTED;
        finished = false;
        LOG_ERROR("job is NULL", K(i), K(ret));
      } else if (OB_JOB_STATE_FINISHED != job->get_state()) {
        finished = false;
      } else {
        //empty
      }
    }
  }
  is_finished = finished;
  return ret;
}

int ObJobControl::create_job(ObIAllocator &allocator,
                             const ObExecutionID &ob_execution_id,
                             uint64_t root_op_id,
                             ObJob *&job) const
{
  int ret = OB_SUCCESS;
  void *tmp = NULL;
  job = NULL;
  if (OB_I(t1) OB_ISNULL(tmp = allocator.alloc(sizeof(ObJob)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObJob", K(ret), K(ob_execution_id));
  } else if (OB_I(t2) OB_ISNULL(job = new(tmp) ObJob)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new ObJob", K(ret), K(ob_execution_id));
  } else {
    // job_id之所以全局递增，是因为execution_id是从ObIDMap的assign函数中获取的，
    // 有可能会重复，所以job_id在本进程内必须不能重复。
    ObJobID ob_job_id;
    uint64_t job_id = ATOMIC_FAA(&global_job_id_, 1);
    ob_job_id.set_ob_execution_id(ob_execution_id);
    ob_job_id.set_job_id(job_id);
    ob_job_id.set_root_op_id(root_op_id);
    job->set_ob_job_id(ob_job_id);
  }
  return ret;
}

int ObJobControl::find_job_by_job_id(uint64_t job_id, ObJob *&job) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret
       && i < jobs_.count(); ++i) {
    ObJob *tmp_job = jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(tmp_job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("job is NULL", K(ret));
    } else if (tmp_job->get_job_id() == job_id) {
      job = tmp_job;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObJobControl::find_job_by_root_op_id(uint64_t root_op_id, ObJob *&job) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret
       && i < jobs_.count(); ++i) {
    ObJob *tmp_job = jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(tmp_job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("job is NULL", K(ret));
    } else if (tmp_job->get_root_op_id() == root_op_id) {
      job = tmp_job;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObJobControl::get_running_jobs(ObIArray<ObJob *> &jobs) const
{
  int ret = OB_SUCCESS;
  jobs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob *job = jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("job is NULL", K(ret));
    } else if (OB_JOB_STATE_RUNNING == job->get_state() && OB_FAIL(jobs.push_back(job))) {
      LOG_WARN("fail to push back job", K(ret), K(*job));
    }
  }
  return ret;
}

int ObJobControl::get_all_jobs(ObIArray<ObJob *> &jobs) const
{
  int ret = OB_SUCCESS;
  jobs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob *job = jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("job is NULL", K(ret));
    } else if (OB_FAIL(jobs.push_back(job))) {
      LOG_WARN("fail to push back job", K(ret), K(*job));
    }
  }
  return ret;
}

int ObJobControl::get_all_jobs_except_root_job(ObIArray<ObJob *> &jobs) const
{
  int ret = OB_SUCCESS;
  jobs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob *job = jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("job is NULL", K(ret));
    } else if (!job->is_root_job() && OB_FAIL(jobs.push_back(job))) {
      LOG_WARN("fail to push back job", K(ret), K(*job));
    }
  }
  return ret;
}

int ObJobControl::sort_job_scan_part_locs(ObExecContext &ctx)
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObJobControl::init_job_finish_queue(ObExecContext &ctx)
{
  UNUSED(ctx);
  return OB_SUCCESS;
}
//int ObJobControl::arrange_jobs()
//{
//  return jobs_quick_sort(jobs_, 0, jobs_.count() - 1);
//}
//
//int ObJobControl::jobs_quick_sort(ObIArray<ObJob *> &jobs,
//                                  int64_t low,
//                                  int64_t high)
//{
//  int ret = OB_SUCCESS;
//  if (low < high) {
//    int64_t i = low;
//    int64_t j = high;
//    ObJob *temp = jobs.at(i);
//    if (OB_ISNULL(temp)) {
//      ret = OB_ERR_UNEXPECTED;
//      LOG_WARN("job is NULL", K(ret), K(i), K(low), K(high));
//    }
//    while (OB_SUCC(ret) && i < j) {
//      while ((jobs.at(j)->get_priority() <= temp->get_priority()) && (i < j)) {
//        j--;
//      }
//      jobs.at(i) = jobs.at(j);
//      while ((jobs.at(i)->get_priority() >= temp->get_priority()) && (i < j)) {
//        i++;
//      }
//      jobs.at(j) = jobs.at(i);
//    }
//    if (OB_SUCC(ret)) {
//      jobs.at(i) = temp;
//      if (OB_FAIL(jobs_quick_sort(jobs, low, i - 1))) {
//        LOG_WARN("fail to jobs quick sort left", K(ret),
//                 K(low), K(high), K(i), K(j));
//      } else if (OB_FAIL(jobs_quick_sort(jobs, j + 1, high))) {
//        LOG_WARN("fail to jobs quick sort right", K(ret),
//                 K(low), K(high), K(i), K(j));
//      }
//    }
//  }
//  return ret;
//}

// 如果超过buf_len则会被截断
int ObJobControl::print_status(char *buf, int64_t buf_len,
                               bool ignore_normal_state/* = false*/) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(J_OBJ_START())) {
    LOG_WARN("fail to print obj start", K(ret));
  } else {
    J_KV(N_JOB_COUNT, jobs_.count());
    if (OB_FAIL(J_COMMA())) {
      LOG_WARN("fail to print comma", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob *job = jobs_.at(i);
    if (OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job is NULL", K(ret), K(i));
    } else if (OB_FAIL(job->print_status(buf, buf_len, pos, ignore_normal_state))) {
      LOG_WARN("fail to print job status", K(ret), K(i), K(*job));
    } else if (i < jobs_.count() - 1 && OB_FAIL(J_COMMA())) {
      LOG_WARN("fail to print comma", K(ret), K(i), K(*job));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(J_OBJ_END())) {
    LOG_WARN("fail to print obj end", K(ret));
  }
  if (OB_SIZE_OVERFLOW == ret) {
    LOG_WARN("buf overflow, truncate it", K(ret), K(buf_len), K(pos));
    ret = OB_SUCCESS;
  }
  return ret;
}

DEF_TO_STRING(ObJobControl)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME(N_JOB_TREE);
  J_COLON();
  print_job_tree(buf, buf_len, pos, jobs_.at(0));
  J_OBJ_END();
  return pos;
}

void ObJobControl::print_job_tree(char *buf, const int64_t buf_len, int64_t &pos, ObJob *job) const
{
  J_OBJ_START();
  J_KV(N_JOB, job);
  int64_t child_count = job->get_child_count();
  if (child_count > 0) {
    J_COMMA();
    J_NAME(N_CHILD_JOB);
    J_COLON();
    J_ARRAY_START();
    ObJob *child_job = NULL;
    for (int64_t i = 0; i < child_count; i++) {
      if (i > 0) {
        J_COMMA();
      }
      (void)job->get_child_job(i, child_job);
      print_job_tree(buf, buf_len, pos, child_job);
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
}

}
}
