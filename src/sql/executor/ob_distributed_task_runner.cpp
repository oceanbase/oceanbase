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

#include "sql/executor/ob_distributed_task_runner.h"
#include "sql/executor/ob_distributed_transmit.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObDistributedTaskRunner::ObDistributedTaskRunner()
{}

ObDistributedTaskRunner::~ObDistributedTaskRunner()
{}
/*
 * Implementation process:
 * 1. The Server receives the OB_DISTRIBUTE_TASK message and immediately responds to
      the Scheduler receiving the Task message
 * 2. Construct three objects of ObTask, ObExecContext and ObPhysicalPlan on the stack
 * 3. Initialize ObTask: ObTask.init(ObExecContext &ctx, ObPhysicalPlan &plan)
 * 4. Deserialize ObTask from Packet
 * 5. (*) Call ObDistributedTaskRunner::execute() to execute the Task
 * 6. Report execution results to Scheduler
 */
int ObDistributedTaskRunner::execute(ObExecContext& ctx, ObPhysicalPlan& phy_plan, ObIArray<ObSliceEvent>& slice_events)
{
  int ret = OB_SUCCESS;
  int close_ret = OB_SUCCESS;
  ObPhyOperator* root_op = NULL;
  ObDistributedTransmitInput* trans_input = NULL;
  if (OB_I(t1)(OB_ISNULL(root_op = phy_plan.get_main_query()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail execute task. no root op", K(root_op), K(ret));
  } else if (OB_I(t2) OB_UNLIKELY(!IS_DIST_TRANSMIT(root_op->get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("valid check fail. root op type must be ObDistributedTransmit", K(ret), K(*root_op));
  } else if (OB_ISNULL(trans_input = GET_PHY_OP_INPUT(ObDistributedTransmitInput, ctx, root_op->get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get op ctx", K(ret), "op_id", root_op->get_id());
  } else if (FALSE_IT(trans_input->set_slice_events(&slice_events))) {
  } else {
    // root_op no needs to call get_next_row()
    // ObTransmit will help with open/get_next_row/send_result/close
    if (OB_FAIL(OB_I(t3) root_op->open(ctx))) {
      LOG_WARN("fail open root op.", K(ret));
    }

    if (OB_SUCCESS != (close_ret = root_op->close(ctx))) {
      LOG_WARN("fail close root op.", K(ret), K(close_ret));
    }
    ret = (OB_SUCCESS == ret) ? close_ret : ret;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
