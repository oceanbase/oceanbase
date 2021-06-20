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
#include "sql/engine/basic/ob_values.h"

namespace oceanbase {
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER((ObValues, ObNoChildrenPhyOperator), row_store_);

class ObValues::ObValuesCtx : public ObPhyOperatorCtx {
public:
  explicit ObValuesCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx), row_store_it_()
  {}
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }

private:
  ObRowStore::Iterator row_store_it_;
  friend class ObValues;
};

void ObValues::reset()
{
  row_store_.reset();
  ObNoChildrenPhyOperator::reset();
}

void ObValues::reuse()
{
  row_store_.reuse();
  ObNoChildrenPhyOperator::reuse();
}
// not used so far, may be use later
int ObValues::add_row(const ObNewRow& row)
{
  return row_store_.add_row(row);
}

int ObValues::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObValuesCtx* values_ctx = NULL;

  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("fail to init operator context", K(ret));
  } else if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObValuesCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator context");
  } else {
    values_ctx->row_store_it_ = row_store_.begin();
  }
  return ret;
}

// not used so far, may be use later
int ObValues::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObValuesCtx* values_ctx = NULL;
  if (OB_FAIL(ObNoChildrenPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan ObNoChildrenPhyOperator failed", K(ret));
  } else if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObValuesCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("values_ctx is null");
  } else {
    values_ctx->row_store_it_ = row_store_.begin();
  }
  return ret;
}

int ObValues::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObValues::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObValuesCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
    LOG_WARN("create current row failed", K(ret), "#columns", get_column_count());
  }
  return ret;
}

int ObValues::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObValuesCtx* values_ctx = NULL;

  if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObValuesCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator context");
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(values_ctx->row_store_it_.get_next_row(values_ctx->get_cur_row()))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else {
    row = &values_ctx->get_cur_row();
  }
  return ret;
}

int64_t ObValues::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_ROW_STORE, row_store_);
  return pos;
}

int ObValues::set_row_store(const common::ObRowStore& row_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_store_.assign(row_store))) {
    LOG_WARN("fail to assign row store", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
