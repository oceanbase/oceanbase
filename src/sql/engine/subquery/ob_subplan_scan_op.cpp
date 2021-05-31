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

#include "ob_subplan_scan_op.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObSubPlanScanSpec::ObSubPlanScanSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), projector_(alloc)
{}

OB_SERIALIZE_MEMBER((ObSubPlanScanSpec, ObOpSpec), projector_);

ObSubPlanScanOp::ObSubPlanScanOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input)
{}

int ObSubPlanScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no child", K(ret));
  } else if (OB_UNLIKELY(MY_SPEC.projector_.count() % 2 != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("projector array size should be multiples of 2", K(ret));
  }
  return ret;
}

int ObSubPlanScanOp::rescan()
{
  return ObOperator::rescan();
}

int ObSubPlanScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get row from child failed", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.projector_.count(); i += 2) {
      ObExpr* from = MY_SPEC.projector_[i];
      ObExpr* to = MY_SPEC.projector_[i + 1];
      ObDatum* datum = NULL;
      if (OB_FAIL(from->eval(eval_ctx_, datum))) {
        LOG_WARN("expr evaluate failed", K(ret), K(*from));
      } else {
        to->locate_expr_datum(eval_ctx_) = *datum;
        to->get_eval_info(eval_ctx_).evaluated_ = true;
      }
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
