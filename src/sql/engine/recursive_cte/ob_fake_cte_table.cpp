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
#include "sql/engine/recursive_cte/ob_fake_cte_table.h"

namespace oceanbase {
using namespace common;
namespace sql {

int ObFakeCTETable::ObFakeCTETableOperatorCtx::get_next_row(const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!has_valid_data()) {
    ret = OB_ITER_END;
  } else {
    row = pump_row_;
    empty_ = true;
  }
  return ret;
}

void ObFakeCTETable::ObFakeCTETableOperatorCtx::reuse()
{
  pump_row_ = nullptr;
  empty_ = true;
}

int ObFakeCTETable::ObFakeCTETableOperatorCtx::add_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  common::ObNewRow* new_row = nullptr;
  common::ObNewRow* old_row = nullptr;
  if (has_valid_data()) {
    LOG_DEBUG("Cur row count may be 0", K(lbt()), KPC(row), KPC(pump_row_));
  }
  common::ObNewRow& row_template = get_cur_row();
  for (int64_t i = 0; i < column_involved_offset_.count(); ++i) {
    int64_t offset = column_involved_offset_.at(i);
    if (offset != OB_INVALID_INDEX) {
      ObObj& fresh_obj = row->get_cell(offset);
      row_template.cells_[i] = fresh_obj;
    } else {
      // do nothing
    }
  }
  if (OB_FAIL(ObPhyOperator::deep_copy_row(row_template, new_row, alloc_))) {
    LOG_WARN("Failed to deep copy new row", K(row_template), K(ret));
  } else {
    old_row = pump_row_;
    pump_row_ = new_row;
    empty_ = false;
    if (nullptr != old_row) {
      alloc_.free(old_row);
    }
  }
  return ret;
}

int ObFakeCTETable::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;

  ObFakeCTETableOperatorCtx* cte_table_ctx = nullptr;
  if (OB_ISNULL(cte_table_ctx = GET_PHY_OPERATOR_CTX(ObFakeCTETableOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get physical operator context failed", K(ret), K_(id));
  } else if (nullptr != cte_table_ctx->pump_row_) {
    cte_table_ctx->empty_ = false;
  }

  return ret;
}

void ObFakeCTETable::reset()
{
  ObNoChildrenPhyOperator::reset();
}

void ObFakeCTETable::reuse()
{
  ObNoChildrenPhyOperator::reuse();
}

int ObFakeCTETable::has_valid_data(ObExecContext& ctx, bool& result) const
{
  int ret = OB_SUCCESS;
  ObFakeCTETableOperatorCtx* cte_table_ctx = nullptr;
  if (OB_ISNULL(cte_table_ctx = GET_PHY_OPERATOR_CTX(ObFakeCTETableOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get physical operator context failed", K(ret), K_(id));
  } else {
    result = cte_table_ctx->has_valid_data();
  }
  return ret;
}

int64_t ObFakeCTETable::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV("pump table", column_involved_offset_);
  return pos;
}

int ObFakeCTETable::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = nullptr;
  if (OB_FAIL(inner_create_operator_ctx(ctx, op_ctx))) {
    LOG_WARN("Inner create operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Op ctx is null", K(ret));
  }
  return ret;
}

int ObFakeCTETable::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = nullptr;
  if (OB_NOT_NULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, ctx, get_id()))) {
    // do nothing
  } else if (OB_FAIL(open_self(ctx))) {
    LOG_WARN("Open fake cte table failed", K(ret));
  }
  return ret;
}

int ObFakeCTETable::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObFakeCTETableOperatorCtx* cte_fake_table_ctx = nullptr;
  if (OB_ISNULL(cte_fake_table_ctx = GET_PHY_OPERATOR_CTX(ObFakeCTETableOperatorCtx, ctx, get_id()))) {
    LOG_DEBUG("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    cte_fake_table_ctx->reuse();
  }
  return ret;
}

int ObFakeCTETable::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObFakeCTETableOperatorCtx, ctx, get_id(), get_type(), op_ctx);
}

int ObFakeCTETable::inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObFakeCTETableOperatorCtx* cte_ctx = nullptr;
  if (OB_FAIL(try_check_status(exec_ctx))) {
    LOG_WARN("Failed to check physical plan status", K(ret));
  } else if ((OB_ISNULL(cte_ctx = GET_PHY_OPERATOR_CTX(ObFakeCTETableOperatorCtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get fake cte ctx", K(ret));
  } else if (OB_FAIL(cte_ctx->get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("Failed to get next sort row from recursive inner data", K(ret));
    }
  }
  return ret;
}

int ObFakeCTETable::add_row(ObExecContext& exec_ctx, common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObFakeCTETableOperatorCtx* cte_ctx = nullptr;
  ObPhyOperatorCtx* op_ctx = nullptr;
  if (OB_FAIL(try_open_and_get_operator_ctx(exec_ctx, op_ctx))) {
    LOG_WARN("Failed to init operator context", K(ret));
  } else if (OB_ISNULL(cte_ctx = static_cast<ObFakeCTETableOperatorCtx*>(op_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get fake ctx", K(ret));
  } else if (OB_FAIL(try_check_status(exec_ctx))) {
    LOG_WARN("Failed to check physical plan status", K(ret));
  } else if (OB_FAIL(cte_ctx->add_row(row))) {
    LOG_WARN("Add row to fake cte table op failed", K(ret));
  }
  return ret;
}

int ObFakeCTETable::set_empty(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObFakeCTETableOperatorCtx* cte_table_ctx = nullptr;
  if (OB_ISNULL(cte_table_ctx = GET_PHY_OPERATOR_CTX(ObFakeCTETableOperatorCtx, exec_ctx, get_id()))) {
    // still not open
  } else {
    cte_table_ctx->empty_ = true;
  }
  return ret;
}

int ObFakeCTETable::open_self(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObFakeCTETableOperatorCtx* cte_fake_table_ctx = nullptr;
  if (OB_FAIL(init_op_ctx(exec_ctx))) {
    LOG_WARN("Failed to init operator context", K(ret));
  } else if (OB_ISNULL(cte_fake_table_ctx = GET_PHY_OPERATOR_CTX(ObFakeCTETableOperatorCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get physical operator context failed", K(ret), K_(id));
  } else if (OB_FAIL(init_cur_row(*cte_fake_table_ctx, true))) {
    LOG_WARN("Init current row failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_involved_offset_.count(); ++i) {
      ret = cte_fake_table_ctx->column_involved_offset_.push_back(column_involved_offset_.at(i));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObFakeCTETable, ObNoChildrenPhyOperator), column_involved_offset_);

}  // end namespace sql
}  // end namespace oceanbase
