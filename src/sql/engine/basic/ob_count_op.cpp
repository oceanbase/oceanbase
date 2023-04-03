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

#include "ob_count_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{

ObCountSpec::ObCountSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
    rownum_limit_(NULL),
    anti_monotone_filters_(alloc)

{
}

OB_SERIALIZE_MEMBER((ObCountSpec, ObOpSpec),
                    rownum_limit_,
                    anti_monotone_filters_);


ObCountOp::ObCountOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input)
{
  reset_default();
}

void ObCountOp::reset_default()
{
  cur_rownum_ = 0;
  rownum_limit_ = INT64_MAX;
}


int ObCountOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count operator has no child", K(ret));
  } else if (OB_FAIL(get_rownum_limit())) {
    LOG_WARN("get rownum limit failed", K(ret));
  }
  return ret;
}

int ObCountOp::get_rownum_limit()
{
  int ret = OB_SUCCESS;
  ObExpr *expr = MY_SPEC.rownum_limit_;
  if (NULL != expr) {
    ObDatum *datum = NULL;
    if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
      LOG_WARN("limit expr evaluate failed", K(ret));
    } else {
      if (datum->null_) {
        rownum_limit_ = 0;
      } else {
        OB_ASSERT(ob_is_int_tc(expr->datum_meta_.type_));
        rownum_limit_ = *datum->int_;
      }
    }
  }
  return ret;
}

int ObCountOp::inner_rescan()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObOperator::inner_rescan())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("operator rescan fail", K(ret));
    }
  } else {
    reset_default();
    OZ(get_rownum_limit());
  }
  return ret;
}

int ObCountOp::inner_switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
    LOG_WARN("operator switch iterator fail", K(ret));
  } else {
    reset_default();
    OZ(get_rownum_limit());
  }
  return ret;
}

int ObCountOp::get_next_row()
{
  int ret = OB_SUCCESS;
  if (cur_rownum_ >= rownum_limit_) {
    ret = OB_ITER_END;
  } else {
    cur_rownum_ += 1;
    if (OB_FAIL(ObOperator::get_next_row())) {
      cur_rownum_ -= 1;
      if (OB_ITER_END != ret) {
        LOG_WARN("operator get next row failed", K(ret));
      }
    }
  }
  return ret;
}

int ObCountOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get row from child failed", K(ret));
    }
  } else {
    clear_evaluated_flag();
    if (!MY_SPEC.anti_monotone_filters_.empty()) {
      bool filtered = false;
      if (OB_FAIL(filter(MY_SPEC.anti_monotone_filters_, filtered))) {
        LOG_WARN("filter row failed", K(ret));
      } else if (filtered) {
        // Set rownum limit to zero make it return OB_ITER_END directly on next call.
        rownum_limit_ = 0;
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
