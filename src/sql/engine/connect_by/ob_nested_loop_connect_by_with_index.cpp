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

#include "sql/engine/connect_by/ob_nested_loop_connect_by_with_index.h"
#include "common/rowkey/ob_rowkey.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
int ObConnectByWithIndex::ObConnectByWithIndexCtx::init(const ObConnectByWithIndex& connect_by, ObExprCtx* expr_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(connect_by_pump_.init(connect_by, expr_ctx))) {
    LOG_WARN("fail to init Connect by Ctx", K(ret));
  } else if (OB_FAIL(create_null_cell_row(connect_by))) {
    LOG_WARN("fail to create null cell row", K(ret));
  } else if (OB_FAIL(create_mock_right_row(connect_by))) {
    LOG_WARN("fail to create mock right row", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObConnectByWithIndex::ObConnectByWithIndexCtx::reset()
{
  state_ = JS_READ_LEFT;
  connect_by_pump_.reset();
  is_match_ = false;
  is_cycle_ = false;
  root_row_ = NULL;
  output_row_ = NULL;
}

ObConnectByWithIndex::ObConnectByWithIndex(ObIAllocator& alloc) : ObConnectByBase(alloc), need_sort_siblings_(false)
{
  state_operation_func_[JS_JOIN_END] = &ObConnectByWithIndex::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObConnectByWithIndex::join_end_func_end;

  state_operation_func_[JS_READ_OUTPUT] = &ObConnectByWithIndex::read_output_operate;
  state_function_func_[JS_READ_OUTPUT][FT_ITER_GOING] = &ObConnectByWithIndex::read_output_func_going;
  state_function_func_[JS_READ_OUTPUT][FT_ITER_END] = &ObConnectByWithIndex::read_output_func_end;

  state_operation_func_[JS_READ_PUMP] = &ObConnectByWithIndex::read_pump_operate;
  state_function_func_[JS_READ_PUMP][FT_ITER_GOING] = &ObConnectByWithIndex::read_pump_func_going;
  state_function_func_[JS_READ_PUMP][FT_ITER_END] = &ObConnectByWithIndex::read_pump_func_end;

  state_operation_func_[JS_READ_LEFT] = &ObConnectByWithIndex::read_left_operate;
  state_function_func_[JS_READ_LEFT][FT_ITER_GOING] = &ObConnectByWithIndex::read_left_func_going;
  state_function_func_[JS_READ_LEFT][FT_ITER_END] = &ObConnectByWithIndex::read_left_func_end;

  state_operation_func_[JS_READ_RIGHT] = &ObConnectByWithIndex::read_right_operate;
  state_function_func_[JS_READ_RIGHT][FT_ITER_GOING] = &ObConnectByWithIndex::read_right_func_going;
  state_function_func_[JS_READ_RIGHT][FT_ITER_END] = &ObConnectByWithIndex::read_right_func_end;
}

ObConnectByWithIndex::~ObConnectByWithIndex()
{}

int ObConnectByWithIndex::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObConnectByWithIndexCtx* join_ctx = NULL;
  if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("my phy plan is null", K(ret));
  } else if (OB_FAIL(ObBasicNestedLoopJoin::inner_open(exec_ctx))) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObConnectByWithIndexCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get nested loop connect by ctx", K(ret));
  } else if (OB_FAIL(wrap_expr_ctx(exec_ctx, join_ctx->expr_ctx_))) {
    LOG_WARN("fail to wrap expr ctx", K(ret));
  } else if (OB_FAIL(join_ctx->init(*this, &join_ctx->expr_ctx_))) {
    LOG_WARN("fail to init Connect by Ctx", K(ret));
  } else {
    join_ctx->expr_ctx_.phy_plan_ctx_ = exec_ctx.get_physical_plan_ctx();
    join_ctx->expr_ctx_.calc_buf_ = &join_ctx->get_calc_buf();
  }
  return ret;
}

int ObConnectByWithIndex::inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObConnectByWithIndexCtx* join_ctx = NULL;
  if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObConnectByWithIndexCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get nested loop connect by ctx", K(ret));
  } else {
    state_operation_func_type state_operation = NULL;
    state_function_func_type state_function = NULL;
    int func = -1;
    row = NULL;
    ObJoinState& state = join_ctx->state_;
    while (OB_SUCC(ret) && NULL == row) {
      state_operation = state_operation_func_[state];
      if (OB_ISNULL(state_operation)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("state operation is null ", K(ret));
      } else if (OB_ITER_END == (ret = (this->*state_operation)(*join_ctx))) {
        func = FT_ITER_END;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed state operation", K(ret), K(state));
      } else {
        func = FT_ITER_GOING;
      }
      if (OB_SUCC(ret)) {
        state_function = state_function_func_[state][func];
        if (OB_ISNULL(state_function)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("state operation is null ", K(ret));
        } else if (OB_FAIL((this->*state_function)(*join_ctx, row)) && OB_ITER_END != ret) {
          LOG_WARN("failed state function", K(ret), K(state), K(func));
        }
      }
    }  // while
    join_ctx->connect_by_pump_.cur_level_--;
  }

  return ret;
}

int ObConnectByWithIndex::rescan(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObConnectByWithIndexCtx* join_ctx = GET_PHY_OPERATOR_CTX(ObConnectByWithIndexCtx, exec_ctx, get_id());
  if (OB_ISNULL(join_ctx)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("join ctx is null", K(ret));
  } else {
    join_ctx->reset();
    if (OB_FAIL(ObBasicNestedLoopJoin::rescan(exec_ctx))) {
      LOG_WARN("failed to rescan", K(ret));
    }
  }
  return ret;
}

int ObConnectByWithIndex::inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObConnectByWithIndexCtx* join_ctx = NULL;
  if (OB_FAIL(OB_I(t1) CREATE_PHY_OPERATOR_CTX(ObConnectByWithIndexCtx, exec_ctx, get_id(), get_type(), join_ctx))) {
    LOG_WARN("failed to create nested loop join ctx", K(ret));
  } else {
    op_ctx = join_ctx;
  }
  return ret;
}

int ObConnectByWithIndex::read_output_operate(ObConnectByWithIndexCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (NULL == join_ctx.output_row_) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObConnectByWithIndex::read_output_func_going(ObConnectByWithIndexCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_ctx.output_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid join ctx", K(ret));
  } else if (OB_FAIL(add_pseudo_column(join_ctx, const_cast<ObNewRow*>(join_ctx.output_row_), CONNECT_BY_ISLEAF))) {
    LOG_WARN("fail to add is_leaf pseudo column", K(ret));
  } else if (OB_FAIL(add_pseudo_column(join_ctx, const_cast<ObNewRow*>(join_ctx.output_row_), CONNECT_BY_ISCYCLE))) {
    LOG_WARN("fail to add pseudo column", K(ret));
  } else if (OB_FAIL(join_ctx.connect_by_pump_.set_connect_by_root_row(join_ctx.output_row_))) {
    LOG_WARN("fail to set connect by root", K(ret));
  } else if (OB_FAIL(calc_connect_by_root_exprs(join_ctx,
                 const_cast<ObNewRow*>(join_ctx.connect_by_pump_.connect_by_root_row_),
                 const_cast<ObNewRow*>(join_ctx.output_row_)))) {
    LOG_WARN("fail to calc connect_by_root_exprs", K(ret));
  } else if (OB_FAIL(calc_sys_connect_by_path(join_ctx, const_cast<ObNewRow*>(join_ctx.output_row_)))) {
    LOG_WARN("Failed to calc sys connect by paht", K(ret));
  } else {
    row = join_ctx.output_row_;
    join_ctx.output_row_ = NULL;
    join_ctx.is_match_ = false;
    join_ctx.is_cycle_ = false;
    join_ctx.state_ = JS_READ_PUMP;
    LOG_DEBUG("connect by output going", KPC(row), K(ret));
  }
  return ret;
}

int ObConnectByWithIndex::read_output_func_end(ObConnectByWithIndexCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  //  join_ctx.state_ = JS_READ_LEFT;
  join_ctx.state_ = JS_READ_PUMP;
  return ret;
}

int ObConnectByWithIndex::read_pump_operate(ObConnectByWithIndexCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(join_ctx.connect_by_pump_.get_next_row(join_ctx.left_row_, join_ctx.output_row_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get pump next row", K(ret));
    }
  }
  return ret;
}

int ObConnectByWithIndex::read_pump_func_going(ObConnectByWithIndexCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  ObExecContext& exec_ctx = join_ctx.exec_ctx_;
  if (OB_ISNULL(right_op_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("right op is null ", K(ret));
  } else if (OB_FAIL(prepare_rescan_params(join_ctx))) {
    LOG_WARN("failed to prepare rescan params", K(ret));
  } else if (OB_FAIL(right_op_->rescan(exec_ctx))) {
    LOG_WARN("failed to rescan right op", K(ret));
  } else {
    join_ctx.state_ = JS_READ_RIGHT;
    //    row = join_ctx.output_row_;
  }
  return ret;
}

int ObConnectByWithIndex::read_pump_func_end(ObConnectByWithIndexCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  //  join_ctx.connect_by_pump_.reset();
  //  join_ctx.state_ = JS_READ_LEFT;
  join_ctx.state_ = JS_JOIN_END;
  return OB_ITER_END;
}

int ObConnectByWithIndex::read_left_operate(ObConnectByWithIndexCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_op_->get_next_row(join_ctx.exec_ctx_, join_ctx.root_row_)) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next row", K(ret));
  } else {
    LOG_DEBUG("connect by read left", KPC(join_ctx.root_row_), K(ret), K(lbt()));
  }
  return ret;
}

int ObConnectByWithIndex::add_pseudo_column(
    ObConnectByWithIndexCtx& join_ctx, ObNewRow* output_row, ObConnectByPseudoColumn column_type) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(output_row) || OB_UNLIKELY(!output_row->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", KPC(output_row), K(ret));
  } else if (OB_UNLIKELY(CONNECT_BY_PSEUDO_COLUMN_CNT != pseudo_column_row_desc_.count()) ||
             OB_UNLIKELY(CONNECT_BY_PSEUDO_COLUMN_CNT == column_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pseudo_column_row_desc", K(ret));
  } else if (UNUSED_POS != pseudo_column_row_desc_[column_type]) {
    int64_t cell_idx = pseudo_column_row_desc_[column_type];
    if (cell_idx > output_row->count_ - 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid cell idx", K(cell_idx), K(output_row->count_), K(ret));
    } else {
      common::number::ObNumber num;
      ObObj val;
      switch (column_type) {
        case LEVEL: {
          int64_t cur_level = join_ctx.connect_by_pump_.get_current_level();
          if (OB_FAIL(num.from(cur_level, *join_ctx.calc_buf_))) {
            LOG_WARN("Failed to generate LEVEL values", K(ret));
          } else {
            val.set_number(num);
          }
          output_row->cells_[cell_idx] = val;
          break;
        }
        case CONNECT_BY_ISLEAF: {
          bool is_leaf = (false == join_ctx.is_match_);
          int64_t num_value = is_leaf ? 1 : 0;
          if (OB_FAIL(num.from(num_value, *join_ctx.calc_buf_))) {
            LOG_WARN("Failed to generate CONNECT_BY_ISLEAF values", K(ret));
          } else {
            val.set_number(num);
          }
          output_row->cells_[cell_idx] = val;
          break;
        }
        case CONNECT_BY_ISCYCLE: {
          int64_t num_value = join_ctx.is_cycle_ ? 1 : 0;
          if (OB_FAIL(num.from(num_value, *join_ctx.calc_buf_))) {
            LOG_WARN("Failed to generate CONNECT_BY_ISCYCLE values", K(ret));
          } else {
            val.set_number(num);
          }
          output_row->cells_[cell_idx] = val;
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invali pseudo column type", K(column_type), K(ret));
      }
    }
    LOG_DEBUG("add output row", KPC(output_row));
  }
  return ret;
}

int ObConnectByWithIndex::read_left_func_going(ObConnectByWithIndexCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  const ObNewRow* output_row = NULL;
  if (OB_FAIL(construct_root_output_row(join_ctx, output_row))) {
    LOG_WARN("fail to construct root output row", K(ret));
  } else if (OB_FAIL(add_pseudo_column(join_ctx, const_cast<ObNewRow*>(output_row), LEVEL))) {
    LOG_WARN("fail to add pseudo column", K(ret));
  } else if (OB_FAIL(calc_sort_siblings_expr(join_ctx, const_cast<ObNewRow*>(output_row)))) {
    LOG_WARN("fail to calc sort siblings expr", KPC(output_row), K(ret));
  } else if (OB_FAIL(join_ctx.connect_by_pump_.add_root_row(join_ctx.root_row_, output_row))) {
    LOG_WARN("fail to set root row", K(ret));
  } else {
    join_ctx.root_row_ = NULL;
  }
  return ret;
}

int ObConnectByWithIndex::read_left_func_end(ObConnectByWithIndexCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (OB_FAIL(join_ctx.connect_by_pump_.sort_sibling_rows())) {
    LOG_WARN("fail to sort sibling rows", K(ret));
  }
  join_ctx.state_ = JS_READ_OUTPUT;
  return ret;
}

int ObConnectByWithIndex::join_end_operate(ObConnectByWithIndexCtx& join_ctx) const
{
  UNUSED(join_ctx);
  return OB_ITER_END;
}

int ObConnectByWithIndex::join_end_func_end(ObConnectByWithIndexCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(join_ctx);
  UNUSED(row);
  return OB_ITER_END;
}

int ObConnectByWithIndex::read_right_operate(ObConnectByWithIndexCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(get_next_right_row(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right row", K(ret));
  }
  return ret;
}

int ObConnectByWithIndex::read_right_func_going(ObConnectByWithIndexCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  const ObNewRow* joined_row = NULL;
  UNUSED(row);
  if (OB_ISNULL(join_ctx.left_row_) || OB_ISNULL(join_ctx.right_row_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("left row or right row is null", K(ret));
  } else if (OB_FAIL(set_level_as_param(join_ctx, join_ctx.connect_by_pump_.get_current_level()))) {
    LOG_WARN("failed to set current level", K(ret));
  } else if (OB_FAIL(calc_other_conds(join_ctx, is_match))) {
    LOG_WARN("failed to compare left and right row on other join conds", K(ret));
  } else if (is_match) {
    LOG_DEBUG("read right row going", K(*join_ctx.left_row_), K(*join_ctx.right_row_), K(ret));
    if (OB_FAIL(join_rows(join_ctx, joined_row))) {
      LOG_WARN("failed to join rows", K(ret));
    } else if (OB_FAIL(add_pseudo_column(join_ctx, const_cast<ObNewRow*>(joined_row), LEVEL))) {
      LOG_WARN("fail to add pseudo column", K(ret));
    } else if (OB_FAIL(calc_sort_siblings_expr(join_ctx, const_cast<ObNewRow*>(joined_row)))) {
      LOG_WARN("fail to calc sort siblings expr", KPC(joined_row), K(ret));
    } else if (OB_FAIL(join_ctx.connect_by_pump_.append_row(join_ctx.right_row_, joined_row))) {
      if (OB_ERR_CBY_LOOP == ret && is_nocycle_) {
        ret = OB_SUCCESS;
        join_ctx.is_cycle_ = true;
      } else {
        LOG_WARN("fail to append row", K(ret));
      }
    } else {
      join_ctx.is_match_ = true;
    }
  } else {
    LOG_DEBUG("read right row going", KPC(join_ctx.left_row_), KPC(join_ctx.right_row_), K(ret));
  }
  UNUSED(row);
  return ret;
}

int ObConnectByWithIndex::read_right_func_end(ObConnectByWithIndexCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  //  join_ctx.state_ = JS_READ_PUMP;
  if (OB_FAIL(join_ctx.connect_by_pump_.sort_sibling_rows())) {
    LOG_WARN("fail to sort siblings", K(ret));
  } else {
    join_ctx.state_ = JS_READ_OUTPUT;
    join_ctx.left_row_ = NULL;
  }
  return ret;
}

int ObConnectByWithIndex::calc_sort_siblings_expr(ObConnectByWithIndexCtx& join_ctx, ObNewRow* output_row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(output_row) || OB_UNLIKELY(output_row->is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid output_row", KPC(output_row), K(ret));
  }
  DLIST_FOREACH(node, sort_siblings_exprs_)
  {
    const ObColumnExpression* calc_expr = static_cast<const ObColumnExpression*>(node);
    if (OB_ISNULL(calc_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("calc expr is NULL", K(ret));
    } else if (OB_FAIL(calc_expr->calc_and_project(join_ctx.expr_ctx_, *output_row))) {
      LOG_WARN("fail to calc and project", KPC(output_row), K(ret));
    }
  }
  LOG_DEBUG("calc sort siblings expr)", KPC(output_row));
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObConnectByWithIndex)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObConnectByWithIndex, ObConnectByBase));
  LST_DO_CODE(OB_UNIS_ADD_LEN, need_sort_siblings_);
  return len;
}

OB_DEF_SERIALIZE(ObConnectByWithIndex)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObConnectByWithIndex, ObConnectByBase));
  LST_DO_CODE(OB_UNIS_ENCODE, need_sort_siblings_);
  return ret;
}

OB_DEF_DESERIALIZE(ObConnectByWithIndex)
{
  int ret = OB_SUCCESS;
  bool need_sort_siblings = true;
  BASE_DESER((ObConnectByWithIndex, ObConnectByBase));
  LST_DO_CODE(OB_UNIS_DECODE, need_sort_siblings);
  need_sort_siblings_ = need_sort_siblings;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
