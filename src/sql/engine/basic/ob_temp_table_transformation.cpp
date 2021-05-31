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

#include "sql/engine/basic/ob_temp_table_transformation.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
namespace sql {

void ObTempTableTransformation::reset()
{
  ObMultiChildrenPhyOperator::reset();
}

void ObTempTableTransformation::reuse()
{
  ObMultiChildrenPhyOperator::reuse();
}

int ObTempTableTransformation::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultiChildrenPhyOperator::rescan(ctx))) {
    LOG_WARN("failed to rescan transformation.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTempTableTransformation::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  const common::ObNewRow* row = NULL;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("failed to init op ctx.", K(ret));
  } else if (OB_FAIL(handle_op_ctx(ctx))) {
    LOG_WARN("failed to handle operator context.", K(ret));
  } else if (OB_ISNULL(child_array_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if (OB_FAIL(child_array_[0]->get_next_row(ctx, row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row.", K(ret));
    } else {
      LOG_DEBUG("all rows are fetched");
      ret = OB_SUCCESS;
    }
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTempTableTransformation::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTempTableTransformationCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create phy operator ctx.", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("create current row failed", K(ret));
  } else { /* do nothing. */
  }
  return ret;
}

int ObTempTableTransformation::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* ttt_ctx = NULL;
  const ObNewRow* input_row = NULL;
  if (OB_ISNULL(child_array_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if (child_array_[0]->is_dml_operator()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(ttt_ctx = GET_PHY_OPERATOR_CTX(ObTempTableTransformationCtx, ctx, get_id()))) {
    LOG_WARN("failed to create phy operator ctx.", K(ret));
  } else if (OB_FAIL(child_array_[1]->get_next_row(ctx, input_row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row from child array.", K(ret));
    } else { /*do nothing.*/
    }
  } else {
    for (int64_t i = 0; i < input_row->get_count(); i++) {
      ttt_ctx->get_cur_row().cells_[i] = input_row->get_cell(i);
    }
  }
  if (OB_SUCC(ret)) {
    row = &ttt_ctx->get_cur_row();
  }
  return ret;
}

int ObTempTableTransformation::get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ret = ObPhyOperator::get_next_row(ctx, row);
  return ret;
}

int ObTempTableTransformation::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

OB_SERIALIZE_MEMBER((ObTempTableTransformation, ObMultiChildrenPhyOperator));

}  // end namespace sql
}  // end namespace oceanbase
