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
#include "share/ob_define.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

int64_t ObMiniJob::to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("root_op");
  J_COLON();
  J_COMMA();
  J_NAME("extend_op");
  J_COLON();
  J_OBJ_END();
  return pos;
}

ObJob::ObJob()
  : ob_job_id_(),
    is_root_job_(false),
    phy_plan_(NULL),
    state_(OB_JOB_STATE_NOT_INIT),
    task_spliter_(NULL),
    task_splited_(false),
    task_control_(),
    has_scan_(false),
    root_spec_(NULL)
{
}

ObJob::~ObJob()
{
}

int ObJob::prepare_task_control(const ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObTaskInfo *task = NULL;
  if (OB_I(t1) OB_ISNULL(task_spliter_)
             || OB_ISNULL(phy_plan_)
             || OB_ISNULL(root_spec_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("job not init", K_(task_spliter), K(root_spec_), K_(phy_plan));
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
        int64_t stmt_parallel_degree = 1;
        if (OB_FAIL(task_control_.prepare(stmt_parallel_degree))) {
          LOG_WARN("fail to prepare task control", K(ret), K(stmt_parallel_degree));
        } else {
          task_splited_ = true;
        }
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
  state_ = OB_JOB_STATE_NOT_INIT;
  task_spliter_ = NULL;
  task_splited_ = false;
  task_control_.reset();
}

int ObJob::get_task_control(const ObExecContext &exec_ctx, ObTaskControl *&task_control)
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


int ObJob::print_status(char *buf, int64_t buf_len, int64_t &pos,
                        bool ignore_normal_state/* = false*/) const
{
  int ret = OB_SUCCESS;
  ObArray<ObTaskInfo *> all_task_infos;
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
      ObTaskInfo *task_info = all_task_infos.at(i);
      if (OB_ISNULL(task_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info is NULL", K(ret), K(i));
      } else {
        switch(task_info->get_state()) {
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
          // 正常状态的task，忽略，不打印
        } else if (print_count > 0 && OB_FAIL(J_COMMA())) {
          LOG_WARN("fail to print comma", K(ret), K(i), K(*task_info));
        } else {
          // ObTaskInfo默认的to_string函数打出来的字符串太长，这里简化一下
          const ObTaskLocation &task_loc = task_info->get_task_location();
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

int ObJob::find_child_job(uint64_t root_op_id, ObJob *&job) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < child_jobs_.count(); ++i) {
    ObJob *child_job = child_jobs_.at(i);
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

int ObJob::job_can_exec(bool &can_exec)
{
  int ret = OB_SUCCESS;
  int64_t child_count = child_jobs_.count();
  can_exec = true;
  for (int64_t i = 0; OB_SUCC(ret) && can_exec && i < child_count; i++) {
    ObJob *child_job = NULL;
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

DEF_TO_STRING(ObJob)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ob_job_id),
       K_(is_root_job),
       K_(state));
  J_COMMA();
  J_NAME(N_PLAN_TREE);
  return pos;
}

