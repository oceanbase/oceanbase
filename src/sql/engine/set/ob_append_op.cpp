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
#include "sql/engine/set/ob_append_op.h"
#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER((ObAppendSpec, ObOpSpec));

int ObAppendOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (current_child_op_idx_ >= MY_SPEC.get_child_cnt() || current_child_op_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. current child op idx is out of range", K(ret), K(current_child_op_idx_));
  } else if (OB_ISNULL(MY_SPEC.get_child(current_child_op_idx_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("child_op_ is null", K(ret));
  } else if (MY_SPEC.get_child(current_child_op_idx_)->is_dml_operator()) {
    ret = OB_ITER_END;
  } else {
    const ObOpSpec* spec = MY_SPEC.get_child(current_child_op_idx_);
    ObOperatorKit* kit = ctx_.get_operator_kit(spec->id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(*spec));
    } else {
      ret = kit->op_->get_next_row();
    }
    if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  return ret;
}

int ObAppendOp::get_next_row()
{
  int ret = OB_SUCCESS;
  while (true) {
    ret = inner_get_next_row();
    if (OB_SUCC(ret)) {
      LOG_DEBUG("append op output", "output", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
      break;
    } else if (OB_ITER_END == ret) {
      if (current_child_op_idx_ < MY_SPEC.get_child_cnt() - 1) {
        ++current_child_op_idx_;
        // go on iterating
      } else {
        // iterate ended. really.
        break;
      }
    } else {
      LOG_WARN("inner get next row failed", K(ret));
      break;
    }
  }
  return ret;
}

int ObAppendOp::inner_open()
{
  // do nothing
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
