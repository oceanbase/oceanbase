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

#include "ob_values_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

ObValuesSpec::ObValuesSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), row_store_(alloc)
{
}

OB_SERIALIZE_MEMBER((ObValuesSpec, ObOpSpec),
                    row_store_);


ObValuesOp::ObValuesOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input)
{
}

int ObValuesOp::inner_open()
{
  int ret = OB_SUCCESS;
  row_store_it_ = MY_SPEC.row_store_.begin();
  ObObj *cells = static_cast<ObObj *>(ctx_.get_allocator().alloc(
          sizeof(ObObj) * MY_SPEC.output_.count()));
  if (OB_ISNULL(cells)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else {
    for (int64_t i = 0; i < MY_SPEC.output_.count(); i++) {
      new (&cells[i]) ObObj();
    }
    cur_row_.cells_ = cells;
    cur_row_.count_ = MY_SPEC.output_.count();
  }
  return ret;
}

int ObValuesOp::inner_rescan()
{
  row_store_it_ = MY_SPEC.row_store_.begin();
  return ObOperator::inner_rescan();
}

int ObValuesOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_store_it_.get_next_row(cur_row_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get row from row store failed", K(ret));
    }
  } else {
    clear_evaluated_flag();
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_.count(); i++) {
      const ObObj &cell = cur_row_.cells_[i];
      ObDatum &datum = MY_SPEC.output_.at(i)->locate_datum_for_write(eval_ctx_);
      ObExpr *expr = MY_SPEC.output_.at(i);
      if (cell.is_null()) {
        datum.set_null();
      } else if (cell.get_type() != expr->datum_meta_.type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type mismatch", K(ret), K(i), K(cell.get_type()), K(*expr));
      } else if (OB_FAIL(datum.from_obj(cell, expr->obj_datum_map_))) {
        LOG_WARN("convert obj to datum failed", K(ret));
      } else if (is_lob_storage(cell.get_type()) &&
                 OB_FAIL(ob_adjust_lob_datum(cell, expr->obj_meta_, expr->obj_datum_map_,
                                             get_exec_ctx().get_allocator(), datum))) {
        LOG_WARN("adjust lob datum failed", K(ret), K(cell.get_meta()), K(expr->obj_meta_));
      } else {
        expr->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
