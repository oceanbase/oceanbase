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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_receive_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/exchange/ob_transmit_op.h"
#include "sql/engine/px/ob_px_basic_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObReceiveOpInput::ObReceiveOpInput(ObExecContext &ctx, const ObOpSpec &spec)
  : ObOpInput(ctx, spec),
    pull_slice_id_(common::OB_INVALID_ID),
    child_job_id_(common::OB_INVALID_ID),
    child_op_id_(common::OB_INVALID_ID)
{
}

ObReceiveOpInput::~ObReceiveOpInput()
{
}

void ObReceiveOpInput::reset()
{
  pull_slice_id_ = OB_INVALID_ID;
  child_job_id_ = OB_INVALID_ID;
  child_op_id_ = OB_INVALID_ID;
}

int ObReceiveOpInput::init(ObTaskInfo &task_info)
{
  int ret = OB_SUCCESS;

  // meta data
  pull_slice_id_ = task_info.get_pull_slice_id();

  // That's a long way to get the child job of cur_op:
  // cur_op -> child_op -> child_op_input -> job
  task_locs_.reset();
  for (int32_t i = 0; OB_SUCC(ret) && i < spec_.get_child_cnt(); ++i) {
    const ObOpSpec *trans_op = spec_.get_child(i);
    if (!IS_TRANSMIT(trans_op->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is not ObTransmit",
                K(ret), K(spec_.id_), K(trans_op->get_type()));
    } else {
      // child_job_id_ = child_job->get_job_id();
      child_op_id_ = trans_op->id_;
    }
  }
  return ret;
}

int ObReceiveOpInput::get_result_location(const int64_t child_job_id,
                                        const int64_t child_task_id,
                                        ObAddr &svr)
{
  UNUSED(child_job_id);
  UNUSED(child_task_id);
  UNUSED(svr);
  // find data in task_locs_
  return OB_NOT_IMPLEMENT;
}

OB_SERIALIZE_MEMBER(ObReceiveOpInput, pull_slice_id_, child_job_id_, task_locs_);

ObReceiveSpec::ObReceiveSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
    partition_order_specified_(false),
    need_set_affected_row_(false),
    is_merge_sort_(false)
{
}

OB_SERIALIZE_MEMBER((ObReceiveSpec, ObOpSpec),
                    partition_order_specified_,
                    need_set_affected_row_,
                    is_merge_sort_);

ObReceiveOp::ObReceiveOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input)
{
}

} // end namespace sql
} // end namespace oceanbase
