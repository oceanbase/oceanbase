/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_basic_nested_loop_join_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObBasicNestedLoopJoinSpec, ObJoinSpec),
                    rescan_params_,
                    gi_partition_id_expr_,
                    enable_gi_partition_pruning_,
                    enable_px_batch_rescan_);

ObBasicNestedLoopJoinOp::ObBasicNestedLoopJoinOp(ObExecContext &exec_ctx,
                                                 const ObOpSpec &spec,
                                                 ObOpInput *input)
  : ObJoinOp(exec_ctx, spec, input)
  {}

int ObBasicNestedLoopJoinOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJoinOp::inner_open())) {
    LOG_WARN("failed to inner open join", K(ret));
  }
  return ret;
}

int ObBasicNestedLoopJoinOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJoinOp::inner_rescan())) {
    LOG_WARN("failed to call parent rescan", K(ret));
  }
  return ret;
}

int ObBasicNestedLoopJoinOp::inner_close()
{
  return OB_SUCCESS;
}


int ObBasicNestedLoopJoinOp::get_next_left_row()
{
  if (!get_spec().rescan_params_.empty()) {
    // Reset exec param before get left row, because the exec param still reference
    // to the previous row, when get next left row, it may become wild pointer.
    // The exec parameter may be accessed by the under PX execution by serialization, which
    // serialize whole parameters store.
    set_param_null();
  }
  return ObJoinOp::get_next_left_row();
}

void ObBasicNestedLoopJoinOp::set_param_null()
{
  set_pushdown_param_null(get_spec().rescan_params_);
}

} // end namespace sql
} // end namespace oceanbase
