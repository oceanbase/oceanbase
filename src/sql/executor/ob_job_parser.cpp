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
#include "sql/executor/ob_addrs_provider.h"
#include "sql/executor/ob_job_parser.h"
#include "sql/executor/ob_transmit.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/join/ob_join.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/json/ob_json_print_utils.h"
using namespace oceanbase::common;
namespace oceanbase {
namespace sql {
ObJobParser::ObJobParser()
{}

ObJobParser::~ObJobParser()
{}

/* entry function
 * @input ObPhysicalPlan include operator tree
 * @input ob_execution_id current execution id
 * @input ObTaskSpliterFactory Responsible for construct TaskSpliter
 * @input ObAddrsProviderFactory Responsible for construct AddrsProvider
 * @output ObJobControl
 */
int ObJobParser::parse_job(ObExecContext& exec_ctx, ObPhysicalPlan* phy_plan, const ObExecutionID& exec_id,
    ObTaskSpliterFactory& spfactory, ObJobControl& job_ctrl) const
{
  NG_TRACE(parse_job_begin);
  int ret = OB_SUCCESS;
  ObPhyOperator* root = NULL;
  if (OB_ISNULL(phy_plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plan is NULL", K(ret));
  } else if (OB_ISNULL(root = phy_plan->get_main_query())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root op of plan is NULL", K(ret));
  } else {
    int task_split_type = ObTaskSpliter::LOCAL_IDENTITY_SPLIT;
    ObJob* root_job = NULL;
    if (OB_FAIL(create_job(exec_ctx, phy_plan, root, exec_id, job_ctrl, task_split_type, spfactory, root_job))) {
      LOG_WARN("fail to create job", K(ret));
    } else if (OB_ISNULL(root_job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root_job is NULL", K(ret));
    } else if (FALSE_IT(root_job->set_root_job())) {
    } else if (OB_FAIL(split_jobs(exec_ctx, phy_plan, root, exec_id, job_ctrl, spfactory, *root_job))) {
      LOG_WARN("fail to split jobs", K(ret));
    } else {
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObJobControl::alloc_phy_op_input(exec_ctx, root))) {
      LOG_WARN("fail alloc phy ops input", K(ret));
    } else if (OB_FAIL(job_ctrl.build_jobs_ctx(exec_ctx))) {
      LOG_WARN("fail build job input", K(ret));
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

int ObJobParser::split_jobs(ObExecContext& exec_ctx, ObPhysicalPlan* phy_plan, ObPhyOperator* phy_op,
    const ObExecutionID& exec_id, ObJobControl& job_ctrl, ObTaskSpliterFactory& spfactory, ObJob& cur_job) const
{
  int ret = OB_SUCCESS;
  ObTransmit* transmit_op = NULL;
  ObJob* job = NULL;
  if (NULL == phy_op) {
    // op is NULL, do nothing
  } else if (OB_ISNULL(phy_plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("phy_plan is NULL", K(ret));
  } else if (!IS_TRANSMIT(phy_op->get_type())) {
    if (0 == phy_op->get_child_num()) {
      cur_job.set_scan(true);
      if (is_outer_join_child(*phy_op)) {
        cur_job.set_outer_join_child_scan(true);
      }
    }
  } else if (OB_ISNULL(transmit_op = static_cast<ObTransmit*>(phy_op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transmit op is NULL", K(ret), K(phy_op));
  } else {
    int task_split_type = transmit_op->get_job_conf().get_task_split_type();
    if (OB_FAIL(create_job(exec_ctx, phy_plan, phy_op, exec_id, job_ctrl, task_split_type, spfactory, job))) {
      LOG_WARN("fail to create job", K(ret), K(exec_id));
    } else if (OB_FAIL(cur_job.append_child_job(job))) {
      LOG_WARN("fail to add child job", K(ret), K(exec_id));
    } else if (is_outer_join_child(*phy_op)) {
      job->set_outer_join_child_job(true);
    }
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < phy_op->get_child_num(); ++i) {
    if (OB_FAIL(split_jobs(
            exec_ctx, phy_plan, phy_op->get_child(i), exec_id, job_ctrl, spfactory, NULL != job ? *job : cur_job))) {
      LOG_WARN("fail to split jobs for child op", K(ret), K(exec_id), K(i));
    } else {
    }
  }
  return ret;
}

int ObJobParser::create_job(ObExecContext& exec_ctx, ObPhysicalPlan* phy_plan, ObPhyOperator* phy_op,
    const ObExecutionID& exec_id, ObJobControl& job_ctrl, const int task_split_type, ObTaskSpliterFactory& spfactory,
    ObJob*& job) const
{
  int ret = OB_SUCCESS;
  job = NULL;
  if (OB_ISNULL(phy_op) || OB_ISNULL(phy_plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("op or phy_plan is NULL", K(ret), K(phy_op), K(phy_plan));
  } else if (OB_FAIL(job_ctrl.create_job(exec_ctx.get_allocator(), exec_id, phy_op->get_id(), job))) {
    LOG_WARN("fail to create job", K(ret));
  } else if (OB_ISNULL(job)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job is NULL", K(ret), K(exec_id));
  } else {
    job->set_phy_plan(phy_plan);
    job->set_root_op(phy_op);
    job->set_state(OB_JOB_STATE_INITED);
    if (OB_FAIL(job_ctrl.add_job(job))) {
      job->~ObJob();
      job = NULL;
      LOG_WARN("fail add job", K(ret));
    } else {
      // set Spliter && ServersProvider into Jobs
      ObTaskSpliter* task_spliter = NULL;
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

bool ObJobParser::is_outer_join_child(const ObPhyOperator& phy_op) const
{
  bool is_outer_join_child_ret = false;
  const ObPhyOperator* cur_op = &phy_op;
  const ObPhyOperator* parent_op = phy_op.get_parent();
  while (!is_outer_join_child_ret && !OB_ISNULL(parent_op) && !IS_TRANSMIT(parent_op->get_type())) {
    if (IS_JOIN(parent_op->get_type())) {
      const ObJoin* join = static_cast<const ObJoin*>(parent_op);
      ObJoinType join_type = join->get_join_type();
      switch (join_type) {
        case LEFT_OUTER_JOIN:
          /*no break*/
        case LEFT_ANTI_JOIN:
          if (cur_op == join->get_child(0)) {
            is_outer_join_child_ret = true;
          }
          break;
        case RIGHT_OUTER_JOIN:
          /*no break*/
        case RIGHT_ANTI_JOIN:
          if (cur_op == join->get_child(1)) {
            is_outer_join_child_ret = true;
          }
          break;
        case FULL_OUTER_JOIN:
          is_outer_join_child_ret = true;
          break;
        default:
          break;
      }
      break;
    } else if (IS_SET_PHY_OP(parent_op->get_type())) {
      is_outer_join_child_ret = true;
    }
    cur_op = parent_op;
    parent_op = parent_op->get_parent();
  }
  return is_outer_join_child_ret;
}

}  // namespace sql
}  // namespace oceanbase
