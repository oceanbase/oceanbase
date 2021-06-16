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

#include "ob_uk_row_transform.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/ob_exec_context.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "share/ob_unique_index_row_transformer.h"
#include "share/ob_worker.h"
#include "share/ob_get_compat_mode.h"

namespace oceanbase {
namespace sql {

using namespace common;

class ObUKRowTransform::ObUKRowTransformCtx : public ObPhyOperatorCtx {
public:
  explicit ObUKRowTransformCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx)
  {}
  virtual void destroy() override
  {
    ObPhyOperatorCtx::destroy_base();
  }
};

ObUKRowTransform::ObUKRowTransform(ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc), uk_col_cnt_(0), shadow_pk_cnt_(0), columns_(alloc)
{}

void ObUKRowTransform::reset()
{
  uk_col_cnt_ = 0;
  shadow_pk_cnt_ = 0;
  columns_.reset();
  ObSingleChildPhyOperator::reset();
}

void ObUKRowTransform::reuse()
{
  reset();
}

int ObUKRowTransform::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unique row transform operator is invalid", K(ret));
  } else if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init operator ctx failed", K(ret));
  }
  return ret;
}

int ObUKRowTransform::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  const bool alloc_cells = true;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unique row transform operator is invalid", K(ret));
  } else if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObUKRowTransformCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create ctx failed", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, alloc_cells))) {
    LOG_WARN("init current row failed", K(ret));
  }
  return ret;
}

int ObUKRowTransform::rescan(ObExecContext&) const
{
  return OB_SUCCESS;
}

int ObUKRowTransform::inner_close(ObExecContext&) const
{
  return OB_SUCCESS;
}

int ObUKRowTransform::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* input_row = NULL;
  ObUKRowTransformCtx* op_ctx = NULL;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unique row transform operator is invalid", K(ret));
  } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObUKRowTransformCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get operator ctx failed", K(ret));
  } else if (OB_FAIL(child_op_->get_next_row(ctx, input_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("get next row from child failed", K(ret));
    }
  } else {
    share::dag_yield();
    ObNewRow& r = op_ctx->get_cur_row();
    const ObCompatibilityMode sql_mode = ctx.get_my_session()->get_compatibility_mode();
    if (OB_FAIL(share::ObUniqueIndexRowTransformer::convert_to_unique_index_row(
            *input_row, sql_mode, uk_col_cnt_, shadow_pk_cnt_, &columns_, r, true /*need copy cells*/))) {
      LOG_WARN("fail to convert to unique index row", K(ret));
    } else {
      row = &r;
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObUKRowTransform, ObSingleChildPhyOperator), uk_col_cnt_, shadow_pk_cnt_, columns_);

}  // end namespace sql
}  // end namespace oceanbase
