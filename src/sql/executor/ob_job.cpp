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

#include "sql/executor/ob_job.h"
#include "sql/executor/ob_task_event.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/executor/ob_addrs_provider.h"
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_receive.h"
#include "share/ob_define.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

int64_t ObMiniJob::to_string(char* buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("root_op");
  J_COLON();
  ObPhysicalPlan::print_tree(buf, buf_len, pos, root_op_);
  J_COMMA();
  J_NAME("extend_op");
  J_COLON();
  ObPhysicalPlan::print_tree(buf, buf_len, pos, extend_op_);
  J_OBJ_END();
  return pos;
}

ObJob::ObJob()
    : ob_job_id_(),
      is_root_job_(false),
      phy_plan_(NULL),
      root_op_(NULL),
      state_(OB_JOB_STATE_NOT_INIT),
      task_spliter_(NULL),
      task_splited_(false),
      task_control_(),
      is_outer_join_child_job_(false),
      has_outer_join_child_scan_(false),
      has_scan_(false)
{}

ObJob::~ObJob()
{}

// Ideal strategy:
//-The finish-queue size of all jobs is allocated according to the actual number of tasks
// But the difficulty is that the number of tasks in the middle job of this framework
// can only be determined when it is running.
//
// Compromise strategy:
//-leaf job gives small value
//-non-leaf job uses the default large value
int ObJob::init_finish_queue(const ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  int64_t finish_queue_size = 0;
  if (is_root_job()) {
    // do nothing
  } else {
    if (child_jobs_.count() > 0) {
      // Not a leaf job, the size of finish_queue is set to 16K
      // The reason why the number of tasks is not analyzed like leaf job is because in job init
      // The stage has not really scheduled tasks, how many tasks will a non-leaf job have?
      // It is impossible to know, and cannot give an exact value.
      const static int64_t TASK_FINISH_QUEUE_MAX_LEN = 1024 * 16;
      finish_queue_size = TASK_FINISH_QUEUE_MAX_LEN;
    } else {
      // leaf job, you can call prepare task control in advance
      if (OB_FAIL(prepare_task_control(exec_ctx))) {
        LOG_WARN("fail prepare task control", K(ret));
      } else {
        // + 2 means possible NOP_EVENT and SCHE_ITER_END two special messages
        // Need to reserve 2 spaces for them
        finish_queue_size = task_control_.get_task_count() + 2;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(task_control_.init_finish_queue(finish_queue_size))) {
        // The finish queue in task control must be initialized in advance,
        // otherwise before scheduling
        // The pop event from finish queue will fail, causing the main thread to fail on the queue
        // Wait for the remote end to return the result.
        LOG_WARN("fail init task control", K(ret));
      }
    }
    LOG_TRACE("job finish queue init", "job_id", get_job_id(), K(finish_queue_size), K(ret));
  }
  return ret;
}

int ObJob::prepare_task_control(const ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  ObTaskInfo* task = NULL;
  if (OB_I(t1) OB_ISNULL(task_spliter_) || OB_ISNULL(root_op_) || OB_ISNULL(phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("job not init", K_(task_spliter), K_(root_op), K_(phy_plan));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(task_spliter_->get_next_task(task))) {
      if (OB_FAIL(OB_I(t2) task_control_.add_task(task))) {
        task_control_.reset();
        LOG_WARN("fail add task to taskq", K(ret), "task", to_cstring(task));
      }
      LOG_DEBUG("add task", K(task), "task", to_cstring(task));
    }
    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
      if (OB_SUCC(ret)) {
        int64_t stmt_parallel_degree = 0;
        if (OB_FAIL(get_parallel_degree(exec_ctx, stmt_parallel_degree))) {
          LOG_WARN("fail get parallel degree", K(ret));
        } else if (OB_FAIL(task_control_.prepare(stmt_parallel_degree))) {
          LOG_WARN("fail to prepare task control", K(ret), K(stmt_parallel_degree));
        } else {
          task_splited_ = true;
        }
      }
    }
  }
  return ret;
}

int ObJob::get_parallel_degree(const ObExecContext& exec_ctx, int64_t& stmt_parallel_degree)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(session = exec_ctx.get_my_session()) || OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is NULL", K(ret), KP(phy_plan_), K(session));
  } else {
    const ObQueryHint& query_hint = phy_plan_->get_query_hint();
    stmt_parallel_degree = query_hint.parallel_;
    if (ObStmtHint::UNSET_PARALLEL == stmt_parallel_degree) {
      // dop not specified in hint. use system variable
      if (OB_FAIL(session->get_ob_stmt_parallel_degree(stmt_parallel_degree))) {
        LOG_WARN("fail to get ob_stmt_parallel_degree from session", K(ret));
      }
    }
    // When the need_serial_execute is identified in the physical plan,
    // the task in the job needs to be executed serially.
    if (OB_SUCC(ret)) {
      const ObPhysicalPlan* physical_plan = NULL;
      if (OB_ISNULL(exec_ctx.get_physical_plan_ctx()) ||
          OB_ISNULL(physical_plan = exec_ctx.get_physical_plan_ctx()->get_phy_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("physical plan is null", K(ret));
      } else if (physical_plan->get_need_serial_exec() || session->need_serial_exec()) {
        stmt_parallel_degree = 1;
      }
    }

    /**
     * the system variables in nested session are serialized from other server,
     * we can not get min or max value here because the serialize operation handle
     * current value only.
     * see: OB_DEF_SERIALIZE(ObBasicSessionInfo).
     */
    if (OB_SUCC(ret) && !session->is_nested_session() && !session->is_fast_select()) {
      if (OB_UNLIKELY(stmt_parallel_degree < 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("stmt_parallel_degree is invalid", K(ret), K(stmt_parallel_degree));
      }
    }
  }
  return ret;
}

void ObJob::reset()
{
  ob_job_id_.reset();
  is_root_job_ = false;
  phy_plan_ = NULL;
  root_op_ = NULL;
  state_ = OB_JOB_STATE_NOT_INIT;
  task_spliter_ = NULL;
  task_splited_ = false;
  task_control_.reset();
  is_outer_join_child_job_ = false;
  has_outer_join_child_scan_ = false;
}

int ObJob::sort_scan_partition_locations(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObTableScan*, 16> scan_ops;
  if (OB_ISNULL(root_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("root op is NULL");
  } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op_))) {
    LOG_WARN("fail to find scan ops", K(ret), "root_op_id", root_op_->get_id());
  } else if (OB_UNLIKELY(1 > scan_ops.count())) {
  } else {
    ObPhyTableLocation* table_loc = NULL;
    int64_t base_part_loc_count = -1;
    int64_t base_part_order = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_ops.count(); ++i) {
      const ObTableScan* scan_op = scan_ops.at(i);
      if (OB_ISNULL(scan_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scan op can't be null", K(ret));
      } else {
        table_loc = NULL;
        uint64_t table_location_key = scan_op->get_table_location_key();
        if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location_for_update(
                ctx, table_location_key, scan_op->get_location_table_id(), table_loc))) {
          LOG_WARN("fail to get phy table location", K(ret));
        } else if (OB_ISNULL(table_loc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get phy table location", K(ret));
        } else {
          if (0 == i) {
            const ObPartitionReplicaLocationIArray& base_part_locs = table_loc->get_partition_location_list();
            base_part_loc_count = base_part_locs.count();
            if (base_part_loc_count > 1) {
              base_part_order = base_part_locs.at(1).get_partition_id() - base_part_locs.at(0).get_partition_id();
            } else {
              base_part_order = 1;
            }
            if (OB_UNLIKELY(0 == base_part_order)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("partition id in same scan can not be equal", K(ret), K(base_part_order), K(base_part_locs));
            }
          } else {
            ObPartitionReplicaLocationIArray& part_locs = table_loc->get_partition_location_list();
            if (OB_UNLIKELY(part_locs.count() != base_part_loc_count)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("part loc count not equal, can not wise join",
                  K(ret),
                  K(i),
                  K(part_locs.count()),
                  K(base_part_loc_count),
                  K(part_locs));
            } else {
              if (part_locs.count() > 1) {
                int64_t part_order = part_locs.at(1).get_partition_id() - part_locs.at(0).get_partition_id();
                if (OB_UNLIKELY(0 == base_part_order || 0 == part_order)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_ERROR("partition id in same scan can not be equal",
                      K(ret),
                      K(base_part_order),
                      K(part_order),
                      K(part_locs));
                } else {
                  // The order of the bottom left scan is different from the order of the current scan,
                  // then the current scan is arranged in the order of the bottom left scan
                  if (base_part_order * part_order < 0) {
                    // At present, the copy efficiency is relatively low.
                    // If it becomes a performance bottleneck in the future, change it
                    if (base_part_order > 0) {
                      std::sort(&part_locs.at(0),
                          &part_locs.at(0) + part_locs.count(),
                          ObPartitionReplicaLocation::compare_part_loc_asc);
                    } else {
                      std::sort(&part_locs.at(0),
                          &part_locs.at(0) + part_locs.count(),
                          ObPartitionReplicaLocation::compare_part_loc_desc);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObJob::get_task_control(const ObExecContext& exec_ctx, ObTaskControl*& task_control)
{
  int ret = OB_SUCCESS;
  if (!task_splited_) {
    if (OB_FAIL(prepare_task_control(exec_ctx))) {
      LOG_WARN("fail prepare task control", K(ret));
    }
  }
  task_control = &task_control_;
  return ret;
}

int ObJob::get_finished_task_locations(ObSArray<ObTaskLocation>& task_locs) const
{
  int ret = OB_SUCCESS;
  bool is_valid_finished_tasks = false;
  ObSEArray<ObTaskInfo*, 8> task_infos;
  if (OB_FAIL(OB_I(t1) task_control_.get_finished_tasks(task_infos))) {
    LOG_WARN("fail to get finished tasks from task control", K(ret));
  } else if (OB_FAIL(is_valid_finished_task_infos(task_infos, is_valid_finished_tasks))) {
    LOG_WARN("invalid finished task infos", K(ret), K(task_infos), K(task_control_));
  } else if (true == is_valid_finished_tasks) {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      ObTaskInfo* task_info = task_infos.at(i);
      if (OB_ISNULL(task_info)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const ObTaskLocation& loc = task_info->get_task_location();
        if (OB_FAIL(OB_I(t4) task_locs.push_back(loc))) {
          LOG_WARN("fail to push to task location array", K(i), K(ret));
        }
      }
    }
  } else {
  }
  return ret;
}

int ObJob::update_job_state(ObExecContext& ctx, ObTaskEvent& evt, bool& job_finished)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_control_.update_task_state(ctx, evt))) {
    LOG_WARN("fail to update task state", K(ret));
  } else if (task_control_.all_tasks_finished_or_skipped_or_failed()) {
    // this job is finished
    set_state(OB_JOB_STATE_FINISHED);
    job_finished = true;
  } else {
    job_finished = false;
  }
  return ret;
}

int ObJob::get_task_result(uint64_t task_id, ObTaskResult& task_result) const
{
  return task_control_.get_task_result(task_id, task_result);
}

int ObJob::append_to_last_failed_task_infos(ObIArray<ObTaskInfo*>& last_failed_task_infos) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTaskInfo*, 32> all_task_infos;
  if (OB_FAIL(task_control_.get_all_tasks(all_task_infos))) {
    LOG_WARN("fail to get all task infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_task_infos.count(); ++i) {
      ObTaskInfo* task_info = all_task_infos.at(i);
      if (OB_ISNULL(task_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info is NULL", K(ret), K(i));
      } else {
        switch (task_info->get_state()) {
          case OB_TASK_STATE_FINISHED: {
            // When the execution of the task fails and the location cache needs to be refreshed
            // Only successfully executed participants do not need to be refreshed
            // The location cache needs to be refreshed if it is not executed or fails to execute
            // The purpose of this is to prevent that after a task fails,
            // the partitions involved in other unexecuted tasks may also have location cache changes
            // If you only re-flash the failed task each time,
            // it will cause the retry to repeatedly encounter a new failed task
            break;
          }
          case OB_TASK_STATE_NOT_INIT:
          case OB_TASK_STATE_INITED:
          case OB_TASK_STATE_RUNNING:
          case OB_TASK_STATE_SKIPPED:
          case OB_TASK_STATE_FAILED: {
            if (OB_FAIL(last_failed_task_infos.push_back(task_info))) {
              LOG_WARN("fail to push back task info into last_failed_task_infos", K(ret), K(*task_info));
            }
            break;
          }
          default: {
            LOG_ERROR("invalid state", K(*task_info));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObJob::print_status(char* buf, int64_t buf_len, int64_t& pos, bool ignore_normal_state /* = false*/) const
{
  int ret = OB_SUCCESS;
  ObArray<ObTaskInfo*> all_task_infos;
  if (OB_FAIL(task_control_.get_all_tasks(all_task_infos))) {
    LOG_WARN("fail to get all task infos", K(ret));
  } else {
    int64_t state_not_init_count = 0;
    int64_t state_inited_count = 0;
    int64_t state_running_count = 0;
    int64_t state_finished_count = 0;
    int64_t state_skipped_count = 0;
    int64_t state_failed_count = 0;
    bool is_normal_state = false;
    if (OB_FAIL(J_OBJ_START())) {
      LOG_WARN("fail to print obj start", K(ret));
    } else {
      J_KV(N_TASK_COUNT, all_task_infos.count());
      if (OB_FAIL(J_COMMA())) {
        LOG_WARN("fail to print comma", K(ret));
      }
    }
    for (int64_t i = 0, print_count = 0; OB_SUCC(ret) && i < all_task_infos.count(); ++i) {
      ObTaskInfo* task_info = all_task_infos.at(i);
      if (OB_ISNULL(task_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info is NULL", K(ret), K(i));
      } else {
        switch (task_info->get_state()) {
          case OB_TASK_STATE_NOT_INIT: {
            state_not_init_count++;
            is_normal_state = true;
            break;
          }
          case OB_TASK_STATE_INITED: {
            state_inited_count++;
            is_normal_state = true;
            break;
          }
          case OB_TASK_STATE_RUNNING: {
            state_running_count++;
            is_normal_state = false;
            break;
          }
          case OB_TASK_STATE_FINISHED: {
            state_finished_count++;
            is_normal_state = true;
            break;
          }
          case OB_TASK_STATE_SKIPPED: {
            state_skipped_count++;
            is_normal_state = false;
            break;
          }
          case OB_TASK_STATE_FAILED: {
            state_failed_count++;
            is_normal_state = false;
            break;
          }
          default: {
            LOG_ERROR("invalid state", K(task_info->get_state()));
            break;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (ignore_normal_state && is_normal_state) {
        } else if (print_count > 0 && OB_FAIL(J_COMMA())) {
          LOG_WARN("fail to print comma", K(ret), K(i), K(*task_info));
        } else {
          const ObTaskLocation& task_loc = task_info->get_task_location();
          BUF_PRINTF("task_info:{");
          J_KV("loc", task_loc.get_server());
          J_KV("ctrl", task_loc.get_ctrl_server());
          J_KV("eid", task_loc.get_execution_id());
          J_KV("jid", task_loc.get_job_id());
          J_KV("tid", task_loc.get_task_id());
          J_KV("pull_sid", task_info->get_pull_slice_id());
          J_KV("state", task_info->get_state());
          BUF_PRINTF("}");
          print_count++;
        }
      }
    }
    if (OB_SUCC(ret)) {
      BUF_PRINTF("state statistics:{");
      if (0 != state_not_init_count) {
        J_KV("not_init", state_not_init_count);
      }
      if (0 != state_inited_count) {
        J_KV("inited", state_inited_count);
      }
      if (0 != state_running_count) {
        J_KV("running", state_running_count);
      }
      if (0 != state_finished_count) {
        J_KV("finished", state_finished_count);
      }
      if (0 != state_skipped_count) {
        J_KV("skipped", state_skipped_count);
      }
      if (0 != state_failed_count) {
        J_KV("failed", state_failed_count);
      }
      BUF_PRINTF("}");
      if (OB_FAIL(J_OBJ_END())) {
        LOG_WARN("fail to print obj end", K(ret));
      }
    }
  }
  return ret;
}

int ObJob::find_child_job(uint64_t root_op_id, ObJob*& job) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < child_jobs_.count(); ++i) {
    ObJob* child_job = child_jobs_.at(i);
    if (OB_I(t1) OB_ISNULL(child_job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child job is NULL", K(ret));
    } else if (child_job->get_root_op_id() == root_op_id) {
      job = child_job;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObJob::is_valid_finished_task_infos(const ObIArray<ObTaskInfo*>& finished_tasks, bool& is_valid) const
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (finished_tasks.count() != task_control_.get_task_count()) {
    ObArray<ObTaskInfo*> all_task_infos;
    if (OB_FAIL(task_control_.get_all_tasks(all_task_infos))) {
      LOG_WARN("fail to get all task infos", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && true == is_valid && i < all_task_infos.count(); ++i) {
      ObTaskInfo* task_info = all_task_infos.at(i);
      if (OB_ISNULL(task_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("task info is NULL", K(ret), K(i), K(all_task_infos.count()));
      } else if (OB_UNLIKELY(OB_TASK_STATE_FINISHED != task_info->get_state() &&
                             OB_TASK_STATE_SKIPPED != task_info->get_state())) {
        is_valid = false;
        LOG_WARN("some task fail",
            "finished_task_count",
            finished_tasks.count(),
            "total_task_count",
            task_control_.get_task_count(),
            K(*task_info),
            "task_control",
            to_cstring(task_control_));
      }
    }
  }
  return ret;
}

int ObJob::job_can_exec(bool& can_exec)
{
  int ret = OB_SUCCESS;
  int64_t child_count = child_jobs_.count();
  can_exec = true;
  for (int64_t i = 0; OB_SUCC(ret) && can_exec && i < child_count; i++) {
    ObJob* child_job = NULL;
    if (OB_FAIL(child_jobs_.at(i, child_job))) {
      LOG_WARN("fail to get child job", K(ret));
    } else if (OB_ISNULL(child_job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child_job is NULL", K(ret));
    } else {
      can_exec = child_job->parent_can_exec();
    }
  }
  return ret;
}

int ObJob::append_finished_slice_events(common::ObIArray<const ObSliceEvent*>& slice_events, bool skip_empty)
{
  return task_control_.append_finished_slice_events(slice_events, skip_empty);
}

int ObJob::need_skip_empty_result(bool& skip_empty) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_op_) || OB_ISNULL(root_op_->get_parent())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root op or root parent op is NULL", K(ret));
  } else {
    skip_empty = (PHY_TASK_ORDER_RECEIVE != root_op_->get_parent()->get_type());
  }
  return ret;
}

int ObJob::child_need_repart(bool& need_repart_part, bool& need_repart_subpart) const
{
  int ret = OB_SUCCESS;
  if (child_jobs_.count() > 0) {
    ObJob* child_job = child_jobs_.at(0);
    if (OB_ISNULL(child_job) || OB_ISNULL(child_job->root_op_) || !IS_TRANSMIT(child_job->root_op_->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child job or child root op is NULL or child root op is not transmit", K(ret));
    } else {
      static_cast<ObTransmit*>(child_job->root_op_)->need_repart(need_repart_part, need_repart_subpart);
    }
  } else {
    need_repart_part = false;
    need_repart_subpart = false;
  }
  return ret;
}

DEF_TO_STRING(ObJob)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ob_job_id), K_(is_root_job), K_(state));
  J_COMMA();
  J_NAME(N_PLAN_TREE);
  J_COLON();
  print_plan_tree(buf, buf_len, pos, root_op_);
  J_OBJ_END();
  return pos;
}

void ObJob::print_plan_tree(char* buf, const int64_t buf_len, int64_t& pos, const ObPhyOperator* phy_op) const
{
  if (!OB_ISNULL(phy_op)) {
    J_OBJ_START();
    J_KV(N_OP, ob_phy_operator_type_str(phy_op->get_type()));
    J_COMMA();
    J_KV(N_OP_ID, phy_op->get_id());
    int64_t child_num = phy_op->get_child_num();
    if (child_num > 0 && !IS_RECEIVE(phy_op->get_type())) {
      J_COMMA();
      J_NAME("child_op");
      J_COLON();
      J_ARRAY_START();
      for (int32_t i = 0; i < child_num; i++) {
        if (i > 0) {
          J_COMMA();
        }
        print_plan_tree(buf, buf_len, pos, phy_op->get_child(i));
      }
      J_ARRAY_END();
    }
    J_OBJ_END();
  }
}
