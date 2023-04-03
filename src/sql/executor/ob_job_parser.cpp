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

#include "sql/ob_sql_define.h"
#include "sql/executor/ob_job_parser.h"
#include "sql/engine/px/exchange/ob_px_transmit_op.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/json/ob_json_print_utils.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
ObJobParser::ObJobParser()
{
}

ObJobParser::~ObJobParser()
{
}

/* 本文件的入口函数，将plan里面的op tree切分成多个Job
 * 切割点为每一对ObReceive和ObTransmit之间
 * @input ObPhysicalPlan 内包含operator tree
 * @input ob_execution_id 当前execution的id，用以区分不同execution
 * @input ObTaskSpliterFactory 负责构造TaskSpliter，构造的结果保存在ObJob中.
 *        创建出的每一个Spliter都已经被初始化好, 拥有一个指向ObJob的引用
 * @output ObJobControl 切分好的job都填充到ObJobControl内管理起来
 */
int ObJobParser::parse_job(ObExecContext &exec_ctx,
                           ObPhysicalPlan *phy_plan,
                           const ObExecutionID &exec_id,
                           ObTaskSpliterFactory &spfactory,
                           ObJobControl &job_ctrl) const
{
  NG_TRACE(parse_job_begin);
  int ret = OB_SUCCESS;
  ObOpSpec *root_spec = NULL;

  if (OB_ISNULL(phy_plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plan is NULL", K(ret));
  } else if (OB_ISNULL(root_spec =
                                           const_cast<ObOpSpec *>(phy_plan->get_root_op_spec()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root op spec of plan is NULL", K(ret));
  } else {
    int task_split_type = ObTaskSpliter::LOCAL_IDENTITY_SPLIT;
    ObJob *root_job = NULL;
    if (OB_FAIL(create_job(exec_ctx, phy_plan, root_spec, exec_id, job_ctrl,
                           task_split_type, spfactory, root_job))) {
      LOG_WARN("fail to create job", K(ret));
    } else if (OB_ISNULL(root_job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root_job is NULL", K(ret));
    } else if (FALSE_IT(root_job->set_root_job())) {
    } else if (OB_FAIL(split_jobs(exec_ctx, phy_plan, root_spec, exec_id,
                                  job_ctrl, spfactory, *root_job))) {
      LOG_WARN("fail to split jobs", K(ret));
    } else {}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(root_spec->create_op_input(exec_ctx))) {
      LOG_WARN("fail create root_spec's input", K(ret));
    } else if (OB_FAIL(job_ctrl.sort_job_scan_part_locs(exec_ctx))) {
      LOG_WARN("fail to sort job scan partition locations", K(ret));
    } else if (OB_FAIL(job_ctrl.init_job_finish_queue(exec_ctx))) {
      LOG_WARN("fail init job", K(ret));
    } else {
      // sanity check for early stage debug, can be removed after code stabilized
      if (OB_UNLIKELY(job_ctrl.get_job_count() <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("job count should > 0", K(ret), K(job_ctrl.get_job_count()));
      }
    }
  }
  NG_TRACE(parse_job_end);
  return ret;
}

int ObJobParser::split_jobs(ObExecContext &exec_ctx,
                            ObPhysicalPlan *phy_plan,
                            ObOpSpec *op_spec,
                            const ObExecutionID &exec_id,
                            ObJobControl &job_ctrl,
                            ObTaskSpliterFactory &spfactory,
                            ObJob &cur_job) const
{
  int ret = OB_SUCCESS;
  ObTransmitSpec *transmit_op_spec = NULL;
  ObJob *job = NULL;
  if (OB_ISNULL(op_spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_op and op_spec are NULL ptrs", K(ret));
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("phy_plan is NULL", K(ret));
  } else {
    if (!IS_TRANSMIT(op_spec->get_type())) {
      if (0 == op_spec->get_child_num()) {
        cur_job.set_scan(true);
      }
    } else if (OB_ISNULL(transmit_op_spec = static_cast<ObTransmitSpec *>(op_spec))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transmit op is NULL", K(ret), K(op_spec));
    } else if (OB_FAIL(create_job(exec_ctx, phy_plan, op_spec, exec_id, job_ctrl,
                       ObTaskSpliter::REMOTE_IDENTITY_SPLIT, spfactory, job))) {
      LOG_WARN("fail to create job", K(ret), K(exec_id));
    } else if (OB_FAIL(cur_job.append_child_job(job))) {
      LOG_WARN("fail to add child job", K(ret), K(exec_id));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < op_spec->get_child_num(); ++i) {
      if (OB_FAIL(split_jobs(exec_ctx, phy_plan, op_spec->get_child(i), exec_id,
                             job_ctrl, spfactory, NULL != job ? *job : cur_job))) {
        LOG_WARN("fail to split jobs for child op", K(ret), K(exec_id), K(i));
      } else {}
    }
  }
  return ret;
}

int ObJobParser::create_job(
    ObExecContext &exec_ctx,
    ObPhysicalPlan *phy_plan,
    ObOpSpec *op_spec,
    const ObExecutionID &exec_id,
    ObJobControl &job_ctrl,
    const int task_split_type,
    ObTaskSpliterFactory &spfactory,
    ObJob *&job) const
{
  int ret = OB_SUCCESS;
  job = NULL;

  if (OB_ISNULL(phy_plan) || OB_ISNULL(op_spec)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL ptr is unexpected", K(ret), K(phy_plan), K(op_spec));
  } else if (OB_FAIL(job_ctrl.create_job(exec_ctx.get_allocator(), exec_id,
                     op_spec->get_id(), job))) {
    LOG_WARN("fail to create job", K(ret));
  } else if (OB_ISNULL(job)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job is NULL", K(ret), K(exec_id));
  } else {
    job->set_phy_plan(phy_plan);
    job->set_state(OB_JOB_STATE_INITED);
    job->set_root_spec(op_spec);
    if (OB_FAIL(job_ctrl.add_job(job))) {
      job->~ObJob();
      job = NULL;
      LOG_WARN("fail add job", K(ret));
    } else {
      // 设置Spliter和ServersProvider到Job中
      ObTaskSpliter *task_spliter = NULL;
      if (OB_FAIL(spfactory.create(exec_ctx, *job, task_split_type, task_spliter))) {
        LOG_WARN("fail create task spliter", "type", task_split_type, K(ret));
      } else if (OB_ISNULL(task_spliter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task_spliter is NULL", K(ret));
      } else {
        job->set_task_spliter(task_spliter);
      }
    }
  }
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */
