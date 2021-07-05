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
#include "sql/engine/subquery/ob_subplan_scan.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
using namespace common;
namespace sql {
class ObSubPlanScan::ObSubPlanScanCtx : public ObPhyOperatorCtx {
public:
  explicit ObSubPlanScanCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx)
  {}
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }
  friend class ObSubPlanScan;
};

ObSubPlanScan::ObSubPlanScan(ObIAllocator& alloc) : ObSingleChildPhyOperator(alloc), output_indexs_(alloc)
{}

ObSubPlanScan::~ObSubPlanScan()
{}

OB_SERIALIZE_MEMBER((ObSubPlanScan, ObSingleChildPhyOperator), output_indexs_);

void ObSubPlanScan::reset()
{
  output_indexs_.reset();
  ObSingleChildPhyOperator::reset();
}

int ObSubPlanScan::add_output_index(int64_t index)
{
  return output_indexs_.push_back(index);
}

int ObSubPlanScan::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObSubPlanScanCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create SubQueryCtx", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
    LOG_WARN("create current row failed", K(ret));
  }
  return ret;
}

int ObSubPlanScan::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init subquery scan context failed", K(ret));
  }
  return ret;
}

int ObSubPlanScan::rescan(ObExecContext& ctx) const
{
  // do noting, just rescan child operator
  return ObSingleChildPhyOperator::rescan(ctx);
}

int ObSubPlanScan::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObSubPlanScan::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  // The role of ObSubQueryScan is similar to TableScan,
  // which generates new rows from the results of the iteration
  // in the subquery according to the data required by the upper query
  int ret = OB_SUCCESS;
  const ObNewRow* input_row = NULL;
  ObSubPlanScanCtx* subquery_ctx = NULL;
  if (OB_ISNULL(subquery_ctx = GET_PHY_OPERATOR_CTX(ObSubPlanScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get subquery scan context failed", K(ret));
  } else if (OB_FAIL(child_op_->get_next_row(ctx, input_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from child operator failed", K(ret));
    } else { /*do nothing*/
    }
  } else if (OB_ISNULL(input_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input_row is null");
  } else {
    // generate new row in subquery scan
    ObNewRow& output_row = subquery_ctx->get_cur_row();
    if (OB_ISNULL(output_row.cells_) || OB_UNLIKELY(output_row.count_ <= 0)) {
      ret = OB_NOT_INIT;
      LOG_WARN("current row not init");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < output_indexs_.count(); ++i) {
      int64_t col_idx = output_indexs_.at(i);
      if (OB_UNLIKELY(i >= output_row.count_) || OB_UNLIKELY(col_idx >= input_row->get_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row not match", K(i), K(output_row), K(*input_row));
      } else {
        output_row.cells_[i] = input_row->get_cell(col_idx);
      }
    }
    if (OB_SUCC(ret)) {
      row = &output_row;
    }
  }
  return ret;
}

int64_t ObSubPlanScan::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(output_indexs));
  return pos;
}
}  // namespace sql
}  // namespace oceanbase
