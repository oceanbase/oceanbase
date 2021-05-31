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

#include "sql/executor/ob_receive.h"
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_job.h"
#include "share/ob_define.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
//
//
//////////////// ObReceiveInput ////////////////////
//
//

ObReceiveInput::ObReceiveInput()
    : pull_slice_id_(common::OB_INVALID_ID), child_job_id_(common::OB_INVALID_ID), child_op_id_(common::OB_INVALID_ID)
{}

ObReceiveInput::~ObReceiveInput()
{}

void ObReceiveInput::reset()
{
  pull_slice_id_ = OB_INVALID_ID;
  child_job_id_ = OB_INVALID_ID;
  child_op_id_ = OB_INVALID_ID;
}

int ObReceiveInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& cur_op)
{
  int ret = OB_SUCCESS;
  ObTransmitInput* transmit_input = NULL;
  ObJob* child_job = NULL;

  // meta data
  pull_slice_id_ = task_info.get_pull_slice_id();

  // That's a long way to get the child job of cur_op:
  // cur_op -> child_op -> child_op_input -> job
  task_locs_.reset();
  for (int32_t i = 0; OB_SUCC(ret) && i < cur_op.get_child_num(); ++i) {
    ObPhyOperator* trans_op = cur_op.get_child(i);
    if (OB_ISNULL(trans_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail get child", K(ret));
    } else if (!IS_TRANSMIT(trans_op->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is not ObTransmit", K(ret), K(cur_op.get_id()), K(trans_op->get_type()));
    } else if (OB_ISNULL(transmit_input = GET_PHY_OP_INPUT(ObTransmitInput, ctx, trans_op->get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tranmit op ctx is NULL", K(ret), K(cur_op.get_id()));
    } else if (OB_ISNULL(child_job = transmit_input->get_job())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("child op's job is NULL", K(ret));
    } else if (OB_FAIL(child_job->get_finished_task_locations(task_locs_))) {
      LOG_WARN("fail to get finished task locations", K(ret));
    } else {
      child_job_id_ = child_job->get_job_id();
      child_op_id_ = trans_op->get_id();
    }
  }
  return ret;
}

int ObReceiveInput::get_result_location(const int64_t child_job_id, const int64_t child_task_id, ObAddr& svr) const
{
  UNUSED(child_job_id);
  UNUSED(child_task_id);
  UNUSED(svr);
  // find data in task_locs_
  return OB_NOT_IMPLEMENT;
}

OB_SERIALIZE_MEMBER(ObReceiveInput, pull_slice_id_, child_job_id_, task_locs_);

//
//
//////////////// ObReceiveCtx ////////////////////
//
//

ObReceive::ObReceiveCtx::ObReceiveCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx)
{}
ObReceive::ObReceiveCtx::~ObReceiveCtx()
{}

//
//
//////////////// ObReceive ////////////////////
//
//
ObReceive::ObReceive(ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc),
      partition_order_specified_(false),
      need_set_affected_row_(false),
      is_merge_sort_(false)
{}

ObReceive::~ObReceive()
{}

int ObReceive::switch_iterator(ObExecContext& ctx) const
{
  UNUSED(ctx);
  // exchange operator not support switch iterator, return OB_ITER_END directly
  return OB_ITER_END;
}

OB_SERIALIZE_MEMBER((ObReceive, ObSingleChildPhyOperator), partition_order_specified_, need_set_affected_row_);

}  // namespace sql
}  // namespace oceanbase
