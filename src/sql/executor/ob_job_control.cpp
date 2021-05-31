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
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_fifo_receive.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {
using namespace oceanbase::common;

volatile uint64_t ObJobControl::global_job_id_ = 0;

ObJobControl::ObJobControl() : jobs_(), local_job_id_(0)
{}

ObJobControl::~ObJobControl()
{
  for (int64_t i = 0; i < jobs_.count(); ++i) {
    ObJob* job = jobs_.at(i);
    if (NULL != job) {
      job->~ObJob();
    }
  }
}

void ObJobControl::reset()
{
  local_job_id_ = 0;
  for (int64_t i = 0; i < jobs_.count(); ++i) {
    ObJob* job = jobs_.at(i);
    if (NULL != job) {
      job->reset();
    }
  }
  jobs_.reset();
}

int ObJobControl::all_jobs_finished(bool& is_finished) const
{
  int ret = OB_SUCCESS;
  bool finished = true;
  for (int64_t i = 0; OB_SUCC(ret) && true == finished && i < jobs_.count(); ++i) {
    ObJob* job = jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      finished = false;
      LOG_ERROR("job is NULL", K(i), K(ret));
    } else if (OB_JOB_STATE_FINISHED != job->get_state()) {
      finished = false;
    } else {
      // empty
    }
  }
  is_finished = finished;
  return ret;
}

int ObJobControl::all_jobs_finished_except_root_job(bool& is_finished) const
{
  int ret = OB_SUCCESS;
  bool finished = true;
  if (jobs_.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    finished = false;
    LOG_ERROR("count of jobs is less than 1", K(ret), "job_count", jobs_.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && true == finished && i < jobs_.count() - 1; ++i) {
      ObJob* job = jobs_.at(i);
      if (OB_I(t1) OB_ISNULL(job)) {
        ret = OB_ERR_UNEXPECTED;
        finished = false;
        LOG_ERROR("job is NULL", K(i), K(ret));
      } else if (OB_JOB_STATE_FINISHED != job->get_state()) {
        finished = false;
      } else {
        // empty
      }
    }
  }
  is_finished = finished;
  return ret;
}

int ObJobControl::create_job(
    ObIAllocator& allocator, const ObExecutionID& ob_execution_id, uint64_t root_op_id, ObJob*& job) const
{
  int ret = OB_SUCCESS;
  void* tmp = NULL;
  job = NULL;
  if (OB_I(t1) OB_ISNULL(tmp = allocator.alloc(sizeof(ObJob)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObJob", K(ret), K(ob_execution_id));
  } else if (OB_I(t2) OB_ISNULL(job = new (tmp) ObJob)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new ObJob", K(ret), K(ob_execution_id));
  } else {
    uint64_t job_id = 0;
    if (ObSqlExecutionIDMap::is_outer_id(ob_execution_id.get_execution_id())) {
      local_job_id_ += 1;
      job_id = local_job_id_;
    } else {
      job_id = ATOMIC_FAA(&global_job_id_, 1);
    }
    ObJobID ob_job_id;
    ob_job_id.set_ob_execution_id(ob_execution_id);
    ob_job_id.set_job_id(job_id);
    ob_job_id.set_root_op_id(root_op_id);
    job->set_ob_job_id(ob_job_id);
  }
  return ret;
}

int ObJobControl::find_job_by_job_id(uint64_t job_id, ObJob*& job) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < jobs_.count(); ++i) {
    ObJob* tmp_job = jobs_.at(i);
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

int ObJobControl::find_job_by_root_op_id(uint64_t root_op_id, ObJob*& job) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < jobs_.count(); ++i) {
    ObJob* tmp_job = jobs_.at(i);
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

int ObJobControl::get_running_jobs(ObIArray<ObJob*>& jobs) const
{
  int ret = OB_SUCCESS;
  jobs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob* job = jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("job is NULL", K(ret));
    } else if (OB_JOB_STATE_RUNNING == job->get_state() && OB_FAIL(jobs.push_back(job))) {
      LOG_WARN("fail to push back job", K(ret), K(*job));
    }
  }
  return ret;
}

int ObJobControl::get_all_jobs(ObIArray<ObJob*>& jobs) const
{
  int ret = OB_SUCCESS;
  jobs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob* job = jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("job is NULL", K(ret));
    } else if (OB_FAIL(jobs.push_back(job))) {
      LOG_WARN("fail to push back job", K(ret), K(*job));
    }
  }
  return ret;
}

int ObJobControl::get_all_jobs_except_root_job(ObIArray<ObJob*>& jobs) const
{
  int ret = OB_SUCCESS;
  jobs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob* job = jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("job is NULL", K(ret));
    } else if (!job->is_root_job() && OB_FAIL(jobs.push_back(job))) {
      LOG_WARN("fail to push back job", K(ret), K(*job));
    }
  }
  return ret;
}

int ObJobControl::sort_job_scan_part_locs(ObExecContext& ctx)
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObJobControl::init_job_finish_queue(ObExecContext& ctx)
{
  UNUSED(ctx);
  return OB_SUCCESS;
}
// int ObJobControl::arrange_jobs()
//{
//  return jobs_quick_sort(jobs_, 0, jobs_.count() - 1);
//}
//
// int ObJobControl::jobs_quick_sort(ObIArray<ObJob *> &jobs,
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

int ObJobControl::build_jobs_ctx(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  int64_t job_count = jobs_.count();
  for (int64_t i = job_count - 1; OB_SUCC(ret) && i >= 0; i--) {
    if (OB_ISNULL(jobs_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("jobs_[i] is NULL", K(ret), K(i), K(job_count));
    } else if (OB_FAIL(build_job_ctx(ctx, *jobs_[i]))) {
      LOG_WARN("fail build job op input");
    }
  }
  return ret;
}

int ObJobControl::get_last_failed_task_infos(ObIArray<ObTaskInfo*>& last_failed_task_infos) const
{
  int ret = OB_SUCCESS;
  last_failed_task_infos.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < jobs_.count(); ++i) {
    ObJob* job = jobs_.at(i);
    if (OB_ISNULL(job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job is NULL", K(ret), K(i));
    } else if (OB_FAIL(job->append_to_last_failed_task_infos(last_failed_task_infos))) {
      LOG_WARN("fail to append last failed task infos", K(ret), K(i), K(*job));
    }
  }
  return ret;
}

int ObJobControl::print_status(char* buf, int64_t buf_len, bool ignore_normal_state /* = false*/) const
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
    ObJob* job = jobs_.at(i);
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

int ObJobControl::build_job_ctx(ObExecContext& query_ctx, ObJob& job)
{
  int ret = OB_SUCCESS;
  ObTransmitInput* transmit_input = NULL;
  ObPhyOperator* root_op = job.get_root_op();
  if (OB_ISNULL(root_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root op is NULL", K(ret), K(job));
  } else if (IS_TRANSMIT(root_op->get_type()) || job.is_root_job()) {
    ObTransmit* transmit_op = static_cast<ObTransmit*>(root_op);
    if (OB_FAIL(build_phy_op_input(query_ctx, root_op, &job))) {
      LOG_WARN("fail to build physical operator input", K(ret));
    } else if (!job.is_root_job()) {
      if (OB_I(t2) OB_ISNULL(transmit_input = GET_PHY_OP_INPUT(ObTransmitInput, query_ctx, transmit_op->get_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("transmit input is NULL", K(ret), "op_id", transmit_op->get_id());
      } else {
        transmit_input->set_job(&job);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("root op must be transmit operator except root job", K(ret), "type", root_op->get_type(), K(job));
  }
  return ret;
}

// recursively build op input for current job
int ObJobControl::alloc_phy_op_input(ObExecContext& ctx, ObPhyOperator* op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is NULL", K(ret));
  } else if (OB_FAIL(op->create_operator_input(ctx))) {
    LOG_WARN("fail create operator input", K(ret));
  }

  for (int32_t i = 0; OB_SUCC(ret) && i < op->get_child_num(); ++i) {
    ObPhyOperator* child_op = op->get_child(i);
    // no more search if reach the begining of next job
    if (OB_ISNULL(child_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is NULL", K(ret));
    } else if (OB_FAIL(alloc_phy_op_input(ctx, child_op))) {
      LOG_WARN("fail to alloc child op input", K(ret), K(i));
    }
  }
  return ret;
}

// recursively build op input for current job
int ObJobControl::build_phy_op_input(ObExecContext& job_ctx, ObPhyOperator* op, ObJob* job)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is NULL", K(ret));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < op->get_child_num(); ++i) {
    ObPhyOperator* child_op = op->get_child(i);
    // no more search if reach the begining of next job
    if (OB_ISNULL(child_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is NULL", K(ret));
    } else if (!IS_TRANSMIT(child_op->get_type())) {
      if (OB_FAIL(build_phy_op_input(job_ctx, op->get_child(i), job))) {
        LOG_WARN("fail to build child op input", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (IS_ASYNC_RECEIVE(op->get_type()) && NULL != job && !job->is_root_job()) {
      ObPhyOperator* child_op = NULL;
      ObJob* child_job = NULL;
      ObDistributedReceiveInput* receive_input = NULL;
      if (1 != op->get_child_num()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("receive child count is not 1", K(ret), K(op->get_child_num()));
      } else if (OB_ISNULL(child_op = op->get_child(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("receive child is NULL", K(ret));
      } else if (!IS_TRANSMIT(child_op->get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("receive child is not transmit", K(ret), K(op->get_type()));
      } else if (OB_FAIL(job->find_child_job(child_op->get_id(), child_job))) {
        LOG_WARN("fail to find child job", K(ret), K(child_op->get_id()));
      } else if (OB_ISNULL(child_job)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child job is NULL", K(ret));
      } else if (OB_ISNULL(receive_input = GET_PHY_OP_INPUT(ObDistributedReceiveInput, job_ctx, op->get_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("receive input is NULL", K(ret), "op_id", op->get_id());
      } else {
        receive_input->set_child_job_id(child_job->get_job_id());
      }
    } else {
      // nothing.
    }
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

void ObJobControl::print_job_tree(char* buf, const int64_t buf_len, int64_t& pos, ObJob* job) const
{
  J_OBJ_START();
  J_KV(N_JOB, job);
  int64_t child_count = job->get_child_count();
  if (child_count > 0) {
    J_COMMA();
    J_NAME(N_CHILD_JOB);
    J_COLON();
    J_ARRAY_START();
    ObJob* child_job = NULL;
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

}  // namespace sql
}  // namespace oceanbase
