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

#include "common/rowkey/ob_rowkey.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/connect_by/ob_nested_loop_connect_by.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
using namespace common;
namespace sql {
ObConnectByBase::ObConnectByBase(ObIAllocator& alloc) : ObBasicNestedLoopJoin(alloc), level_params_(alloc)
{}

ObConnectByBase::~ObConnectByBase()
{}

int ObConnectByBase::ObConnectByBaseCtx::create_null_cell_row(const ObConnectByBase& connect_by)
{
  int ret = OB_SUCCESS;
  const ObPhyOperator* left_op = NULL;
  if (OB_UNLIKELY(2 != connect_by.get_child_num())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child num", K(ret));
  } else if (OB_ISNULL(left_op = connect_by.get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left operator is NULL", K(ret));
  } else if (OB_FAIL(alloc_row_cells(left_op->get_column_count(), null_cell_row_))) {
    LOG_WARN("fail to alloc row cells", K(ret));
  } else {
    null_cell_row_.projector_ = const_cast<int32_t*>(left_op->get_projector());
    null_cell_row_.projector_size_ = left_op->get_projector_size();
  }

  ObObj null_val;
  null_val.set_null();
  for (int64_t i = 0; i < null_cell_row_.count_; ++i) {
    null_cell_row_.cells_[i] = null_val;
  }

  return ret;
}

int ObConnectByBase::ObConnectByBaseCtx::create_mock_right_row(const ObConnectByBase& connect_by)
{
  int ret = OB_SUCCESS;
  const ObPhyOperator* right_op = NULL;
  if (OB_UNLIKELY(2 != connect_by.get_child_num())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child num", K(ret));
  } else if (OB_ISNULL(right_op = connect_by.get_child(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left operator is NULL", K(ret));
  } else if (OB_FAIL(alloc_row_cells(right_op->get_column_count(), mock_right_row_))) {
    LOG_WARN("fail to alloc row cells", K(ret));
  } else {
    mock_right_row_.projector_ = const_cast<int32_t*>(right_op->get_projector());
    mock_right_row_.projector_size_ = right_op->get_projector_size();
  }
  return ret;
}

void ObConnectByBase::reset()
{
  ObBasicNestedLoopJoin::reset();
}

void ObConnectByBase::reuse()
{
  ObBasicNestedLoopJoin::reuse();
}

// copy root_row to mock_right_row
int ObConnectByBase::construct_mock_right_row(const ObNewRow& root_row, ObNewRow& mock_right_row) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(root_row.is_invalid()) || OB_UNLIKELY(mock_right_row.is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(root_row), K(mock_right_row), K(ret));
  } else if (root_row_desc_.count() != mock_right_row.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", K(root_row_desc_.count()), K(mock_right_row.count_));
  } else {
    int64_t cell_count = root_row_desc_.count();
    ObObj val;
    val.set_int(1);
    ObObj invalid_val(ObMaxType);
    for (int64_t i = 0; OB_SUCC(ret) && i < cell_count; ++i) {
      if (root_row_desc_.at(i) == ObJoin::DUMMY_OUPUT) {
        mock_right_row.cells_[i] = val;
      } else if (root_row_desc_.at(i) == ObJoin::UNUSED_POS) {
        mock_right_row.cells_[i] = invalid_val;
      } else if (OB_UNLIKELY(root_row_desc_.at(i) + 1 > root_row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid count", K(root_row_desc_.at(i)), K(root_row.count_));
      } else {
        mock_right_row.cells_[i] = root_row.cells_[root_row_desc_.at(i)];
      }
    }
    LOG_DEBUG("connect by mock right row", K(mock_right_row), K(root_row));
  }
  return ret;
}

/* root output:
-----------------------------
|null, null, ... | left_row |
-----------------------------
*/
int ObConnectByBase::construct_root_output_row(ObConnectByBaseCtx& join_ctx, const ObNewRow*& output_row) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* joined_result_row = NULL;
  if (OB_ISNULL(join_ctx.root_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid join ctx", K(ret));
  } else if (OB_FAIL(construct_mock_right_row(*join_ctx.root_row_, join_ctx.mock_right_row_))) {
    LOG_WARN("fail to construct mock right row", K(ret));
  } else {
    join_ctx.left_row_ = &join_ctx.null_cell_row_;
    join_ctx.right_row_ = &join_ctx.mock_right_row_;
    if (OB_FAIL(join_rows(join_ctx, joined_result_row))) {
      LOG_WARN("fail to join rows", K(ret));
    } else {
      output_row = joined_result_row;
      join_ctx.left_row_ = NULL;
      join_ctx.right_row_ = NULL;
    }
  }
  return ret;
}

int ObConnectByBase::calc_connect_by_root_exprs(
    ObConnectByBaseCtx& join_ctx, ObNewRow* root_row, const ObNewRow* output_row) const
{
  int ret = OB_SUCCESS;
  const ObColumnExpression* calc_expr = NULL;
  if (OB_ISNULL(root_row) || OB_ISNULL(output_row) || OB_UNLIKELY(root_row->is_invalid()) ||
      OB_UNLIKELY(output_row->is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid root row", KPC(root_row), KPC(output_row), K(ret));
  } else if (false == connect_by_root_exprs_.is_empty()) {
    int64_t result_idx = OB_INVALID_INDEX;
    DLIST_FOREACH(calc_node, connect_by_root_exprs_)
    {
      calc_expr = static_cast<const ObColumnExpression*>(calc_node);
      if (OB_ISNULL(calc_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid calc expr", K(ret));
      } else if (OB_FAIL(calc_expr->calc_and_project(join_ctx.expr_ctx_, *root_row))) {
        LOG_WARN("fail to calc and project", KPC(root_row), KPC(calc_expr), K(ret));
      } else if (FALSE_IT(result_idx = calc_expr->get_result_index())) {
      } else if (OB_UNLIKELY(root_row->count_ != output_row->count_) ||
                 OB_UNLIKELY(root_row->count_ < result_idx + 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parameter", KPC(root_row), KPC(output_row), KPC(calc_expr), K(ret));
      } else {
        output_row->cells_[result_idx] = root_row->cells_[result_idx];
      }
    }
  }
  return ret;
}

int ObConnectByBase::set_level_as_param(ObConnectByBaseCtx& join_ctx, int64_t level) const
{
  int ret = OB_SUCCESS;
  ObObjParam res_obj;
  res_obj.set_int(level);
  res_obj.set_param_meta();
  ObPhysicalPlanCtx* plan_ctx = join_ctx.exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("plan ctx or left row is null", K(ret));
  } else {
    for (int64_t i = 0; i < level_params_.count() && OB_SUCC(ret); ++i) {
      int64_t idx = level_params_.at(i);
      plan_ctx->get_param_store_for_update().at(idx) = res_obj;
      LOG_DEBUG("prepare exec params", K(ret), K(i), K(res_obj), K(idx));
    }
  }
  return ret;
}

int ObConnectByBase::add_sys_connect_by_path(ObColumnExpression* expr)
{
  return ObSqlExpressionUtil::add_expr_to_list(sys_connect_exprs_, expr);
}

int ObConnectByBase::calc_sys_connect_by_path(ObConnectByBaseCtx& join_ctx, ObNewRow* output_row) const
{
  int ret = OB_SUCCESS;
  const ObColumnExpression* calc_expr = NULL;
  if (OB_ISNULL(output_row) || OB_UNLIKELY(output_row->is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid root row", KPC(output_row), K(ret));
  } else if (false == sys_connect_exprs_.is_empty()) {
    int64_t idx = 0;
    DLIST_FOREACH(calc_node, sys_connect_exprs_)
    {
      calc_expr = static_cast<const ObColumnExpression*>(calc_node);
      join_ctx.expr_ctx_.sys_connect_by_path_id_ = idx;
      if (OB_ISNULL(calc_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid calc expr", K(ret));
      } else if (OB_FAIL(calc_expr->calc_and_project(join_ctx.expr_ctx_, *output_row))) {
        LOG_WARN("fail to calc and project", KPC(output_row), KPC(calc_expr), K(ret));
      } else {
        LOG_DEBUG("after connect by path", K(output_row), KPC(output_row));
      }
      ++idx;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObConnectByBase)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObConnectByBase, ObBasicNestedLoopJoin));
  OB_UNIS_ENCODE(level_params_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_dlist(sys_connect_exprs_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize calc_exprs_", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObConnectByBase)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObConnectByBase, ObBasicNestedLoopJoin));
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(level_params_);
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE_EXPR_DLIST(ObColumnExpression, sys_connect_exprs_, my_phy_plan_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObConnectByBase)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObConnectByBase, ObBasicNestedLoopJoin));
  OB_UNIS_ADD_LEN(level_params_);
  len += get_dlist_serialize_size(sys_connect_exprs_);
  return len;
}

int ObConnectBy::ObConnectByCtx::init(const ObConnectBy& connect_by, ObExprCtx* expr_ctx)
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

void ObConnectBy::ObConnectByCtx::reset()
{
  state_ = JS_READ_LEFT;
  connect_by_pump_.reset();
  root_row_ = NULL;
}

ObConnectBy::ObConnectBy(ObIAllocator& alloc)
    : ObConnectByBase(alloc), hash_key_exprs_(), hash_probe_exprs_(), equal_cond_infos_()

{
  state_operation_func_[JS_READ_LEFT] = &ObConnectBy::read_left_operate;
  state_function_func_[JS_READ_LEFT][FT_ITER_GOING] = &ObConnectBy::read_left_func_going;
  state_function_func_[JS_READ_LEFT][FT_ITER_END] = &ObConnectBy::read_left_func_end;

  state_operation_func_[JS_READ_RIGHT] = &ObConnectBy::read_right_operate;
  state_function_func_[JS_READ_RIGHT][FT_ITER_GOING] = &ObConnectBy::read_right_func_going;
  state_function_func_[JS_READ_RIGHT][FT_ITER_END] = &ObConnectBy::read_right_func_end;

  state_operation_func_[JS_READ_OUTPUT] = &ObConnectBy::read_output_operate;
  state_function_func_[JS_READ_OUTPUT][FT_ITER_GOING] = &ObConnectBy::read_output_func_going;
  state_function_func_[JS_READ_OUTPUT][FT_ITER_END] = &ObConnectBy::read_output_func_end;

  state_operation_func_[JS_READ_PUMP] = &ObConnectBy::read_pump_operate;
  state_function_func_[JS_READ_PUMP][FT_ITER_GOING] = &ObConnectBy::read_pump_func_going;
  state_function_func_[JS_READ_PUMP][FT_ITER_END] = &ObConnectBy::read_pump_func_end;

  state_operation_func_[JS_JOIN_END] = &ObConnectBy::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObConnectBy::join_end_func_end;
}

ObConnectBy::~ObConnectBy()
{}

int ObConnectBy::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObConnectByCtx* join_ctx = NULL;
  lib::ContextParam param;
  int64_t tenant_id = OB_INVALID_ID;
  if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("my phy plan is null", K(ret));
  } else if (OB_FAIL(ObJoin::inner_open(exec_ctx))) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObConnectByCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get nested loop connect by ctx", K(ret));
  } else if (OB_FAIL(wrap_expr_ctx(exec_ctx, join_ctx->expr_ctx_))) {
    LOG_WARN("fail to wrap expr ctx", K(ret));
  } else if (OB_FAIL(join_ctx->init(*this, &join_ctx->expr_ctx_))) {
    LOG_WARN("fail to init Connect by Ctx", K(ret));
  } else if (OB_ISNULL(exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (hash_key_exprs_.count() != hash_probe_exprs_.count() ||
             hash_key_exprs_.count() != equal_cond_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash key exprs count != hash probe exprs count", K(ret));
  } else {
    tenant_id = exec_ctx.get_my_session()->get_effective_tenant_id();
    param.set_mem_attr(lib::ObMemAttr(tenant_id, ObModIds::OB_CONNECT_BY_PUMP, ObCtxIds::WORK_AREA));
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(join_ctx->mem_context_, param))) {
    LOG_WARN("create entity failed", K(ret));
  } else if (OB_ISNULL(join_ctx->mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null memory entity returned", K(ret));
  } else if (OB_FAIL(join_ctx->connect_by_pump_.row_store_.init(INT64_MAX,
                 tenant_id,
                 ObCtxIds::WORK_AREA,
                 ObModIds::OB_CONNECT_BY_PUMP,
                 true /* enable dump */,
                 ObChunkRowStore::FULL,
                 0))) {
    LOG_WARN("init chunk row store failed", K(ret));
  } else {
    join_ctx->connect_by_pump_.row_store_.set_allocator(join_ctx->mem_context_->get_malloc_allocator());
    join_ctx->expr_ctx_.phy_plan_ctx_ = exec_ctx.get_physical_plan_ctx();
    join_ctx->expr_ctx_.calc_buf_ = &join_ctx->get_calc_buf();
  }
  return ret;
}

int ObConnectBy::rescan(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObConnectByCtx* join_ctx = GET_PHY_OPERATOR_CTX(ObConnectByCtx, exec_ctx, get_id());
  if (OB_ISNULL(join_ctx)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("join ctx is null", K(ret));
  } else {
    join_ctx->left_row_ = NULL;
    join_ctx->right_row_ = NULL;
    join_ctx->reset();
    ObPhyOperator* left_op = NULL;
    ObPhyOperator* right_op = NULL;
    if (OB_ISNULL(left_op = get_child(0))) {
      LOG_WARN("failed to get left child", K(left_op), K(get_child_num()), K(ret));
    } else if (OB_ISNULL(right_op = get_child(1))) {
      LOG_WARN("failed to get right child", K(right_op), K(get_child_num()), K(ret));
    } else if (OB_FAIL(left_op->rescan(exec_ctx))) {
      LOG_WARN("rescan left child operator failed",
          K(ret),
          "op_type",
          ob_phy_operator_type_str(get_type()),
          "child op_type",
          ob_phy_operator_type_str(left_op->get_type()));
      // TODO:shanting not need to rescan right_op if there is no push_down parameters
    } else if (OB_FAIL(right_op->rescan(exec_ctx))) {
      LOG_WARN("rescan right child operator failed",
          K(ret),
          "op_type",
          ob_phy_operator_type_str(get_type()),
          "child op_type",
          ob_phy_operator_type_str(right_op->get_type()));
    } else {
      join_ctx->is_filtered_has_set_ = false;
      join_ctx->op_monitor_info_.rescan_times_++;
    }
  }
  return ret;
}

int ObConnectBy::inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObConnectByCtx* join_ctx = NULL;
  if (OB_FAIL(OB_I(t1) CREATE_PHY_OPERATOR_CTX(ObConnectByCtx, exec_ctx, get_id(), get_type(), join_ctx))) {
    LOG_WARN("failed to create nested loop join ctx", K(ret));
  } else {
    op_ctx = join_ctx;
  }
  return ret;
}

int ObConnectBy::inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObConnectByCtx* join_ctx = NULL;
  if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObConnectByCtx, exec_ctx, get_id()))) {
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
  }

  return ret;
}

int ObConnectBy::inner_close(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObConnectByCtx* join_ctx = NULL;
  if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObConnectByCtx, exec_ctx, get_id()))) {
    LOG_DEBUG("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    join_ctx->connect_by_pump_.close(
        OB_ISNULL(join_ctx->mem_context_) ? NULL : &join_ctx->mem_context_->get_malloc_allocator());
    join_ctx->sql_mem_processor_.unregister_profile();
    if (nullptr != join_ctx->mem_context_) {
      join_ctx->mem_context_->reuse();
    }
  }
  return ret;
}

int ObConnectBy::read_left_operate(ObConnectByCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_op_->get_next_row(join_ctx.exec_ctx_, join_ctx.root_row_)) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next row", K(ret));
  }
  return ret;
}

int ObConnectBy::read_left_func_going(ObConnectByCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  const ObNewRow* output_row = NULL;
  if (OB_FAIL(construct_root_output_row(join_ctx, output_row))) {
    LOG_WARN("fail to construct root output row", K(ret));
  } else if (OB_FAIL(
                 join_ctx.connect_by_pump_.add_root_row(join_ctx.root_row_, join_ctx.mock_right_row_, output_row))) {
    LOG_WARN("fail to set root row", K(ret));
  } else {
    join_ctx.root_row_ = NULL;
    if (join_ctx.connect_by_pump_.get_row_store_constructed()) {
      join_ctx.state_ = JS_READ_OUTPUT;
    } else {
      join_ctx.state_ = JS_READ_RIGHT;
    }
  }
  return ret;
}

int ObConnectBy::read_left_func_end(ObConnectByCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  join_ctx.state_ = JS_JOIN_END;
  return ret;
}

int ObConnectBy::read_right_operate(ObConnectByCtx& join_ctx) const
{
  UNUSED(join_ctx);
  int ret = OB_SUCCESS;
  return ret;
}

int ObConnectBy::process_dump(ObConnectByCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  if (OB_FAIL(join_ctx.sql_mem_processor_.update_max_available_mem_size_periodically(
          &join_ctx.mem_context_->get_malloc_allocator(),
          [&](int64_t cur_cnt) { return join_ctx.connect_by_pump_.row_store_.get_row_cnt_in_memory() > cur_cnt; },
          updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (join_ctx.need_dump() && GCONF.is_sql_operator_dump_enabled() &&
             OB_FAIL(join_ctx.sql_mem_processor_.extend_max_memory_size(
                 &join_ctx.mem_context_->get_malloc_allocator(),
                 [&](int64_t max_memory_size) { return join_ctx.sql_mem_processor_.get_data_size() > max_memory_size; },
                 dumped,
                 join_ctx.sql_mem_processor_.get_data_size()))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped) {
    if (OB_FAIL(join_ctx.connect_by_pump_.row_store_.dump(false, true))) {
      LOG_WARN("failed to dump row store", K(ret));
    } else {
      join_ctx.sql_mem_processor_.reset();
      join_ctx.sql_mem_processor_.set_number_pass(1);
      LOG_TRACE("trace material dump",
          K(join_ctx.sql_mem_processor_.get_data_size()),
          K(join_ctx.connect_by_pump_.row_store_.get_row_cnt_in_memory()),
          K(join_ctx.sql_mem_processor_.get_mem_bound()));
    }
  }
  return ret;
}

int ObConnectBy::read_right_func_going(ObConnectByCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_ID;
  if (OB_ISNULL(join_ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    tenant_id = join_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();
  }

  bool first_row = true;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_right_row(join_ctx))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next right row failed", K(ret));
      }
    } else if (first_row) {
      int64_t row_count = 0;
      if (OB_ISNULL(right_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("right op is null", K(ret));
      } else if (FALSE_IT(row_count = right_op_->get_rows())) {
      } else if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
                     &join_ctx.exec_ctx_, px_est_size_factor_, row_count, row_count))) {
        LOG_WARN("failed to get px size", K(ret));
      } else if (OB_FAIL(join_ctx.sql_mem_processor_.init(&join_ctx.mem_context_->get_malloc_allocator(),
                     tenant_id,
                     row_count * get_width(),
                     get_type(),
                     get_id(),
                     &join_ctx.exec_ctx_))) {
        LOG_WARN("failed to init sql memory manager processor", K(ret));
      } else {
        join_ctx.connect_by_pump_.row_store_.set_dir_id(join_ctx.sql_mem_processor_.get_dir_id());
        join_ctx.connect_by_pump_.row_store_.set_callback(&join_ctx.sql_mem_processor_);
        LOG_TRACE("trace init sql mem mgr for material",
            K(get_rows()),
            K(row_count),
            K(get_width()),
            K(join_ctx.profile_.get_cache_size()),
            K(join_ctx.profile_.get_expect_size()));
      }
      first_row = false;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(join_ctx.right_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right row is null");
    } else if (OB_FAIL(process_dump(join_ctx))) {
      LOG_WARN("failed to process dump", K(ret));
    } else if (OB_FAIL(join_ctx.connect_by_pump_.push_back_store_row(*join_ctx.right_row_))) {
      LOG_WARN("add row to row store failed", K(ret));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(join_ctx.connect_by_pump_.row_store_.finish_add_row(true))) {
      LOG_WARN("failed to finish add row to row store", K(ret));
    }
  }
  LOG_TRACE("push all row in row_store",
      K(join_ctx.connect_by_pump_.row_store_.get_row_cnt()),
      K(join_ctx.connect_by_pump_.row_store_.has_dumped()));
  if (OB_SUCC(ret) && hash_key_exprs_.count() != 0 && !join_ctx.connect_by_pump_.row_store_.has_dumped() &&
      OB_FAIL(join_ctx.connect_by_pump_.build_hash_table(join_ctx.mem_context_->get_malloc_allocator()))) {
    LOG_WARN("build hash table failed", K(ret));
  }
  join_ctx.connect_by_pump_.set_row_store_constructed();
  join_ctx.state_ = JS_READ_OUTPUT;
  return ret;
}

int ObConnectBy::read_right_func_end(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const
{
  UNUSED(join_ctx);
  UNUSED(row);
  int ret = OB_SUCCESS;
  return ret;
}

int ObConnectBy::read_output_operate(ObConnectByCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (join_ctx.connect_by_pump_.pump_stack_.empty()) {
    ret = OB_ITER_END;
  }
  return ret;
}

/*calculate values of pseudo columns.
1. Search for children nodes, if meet a loop, means child is same as an ancestor, node.is_cycle_ = true.
2. Search for children nodes, if no children or all children make up a loop, node.is_cycle_ = true.
*/
int ObConnectBy::calc_pseudo_flags(ObConnectByCtx& join_ctx, ObConnectByPump::PumpNode& node) const
{
  int ret = OB_SUCCESS;
  bool output_cycle = join_ctx.connect_by_pump_.is_output_cycle_;
  bool output_leaf = join_ctx.connect_by_pump_.is_output_leaf_;
  node.is_cycle_ = false;
  node.is_leaf_ = true;
  if (output_cycle || output_leaf) {
    ObConnectByPump::RowFetcher row_fetcher;
    ObChunkRowStore::Iterator iterator;
    row_fetcher.iterator_ = &iterator;
    join_ctx.left_row_ = node.pump_row_;
    if (OB_FAIL(set_level_as_param(join_ctx, node.level_ + 1))) {
      LOG_WARN("set level as param failed", K(ret));
    } else if (OB_FAIL(row_fetcher.init(join_ctx.connect_by_pump_, hash_probe_exprs_, join_ctx.left_row_))) {
      LOG_WARN("begin row store failed", K(ret));
    }

    bool finished = false;
    bool matched = false;
    ObConnectByPump::PumpNode next_node;
    next_node.level_ = node.level_ + 1;
    join_ctx.connect_by_pump_.cur_level_ = node.level_ + 1;
    while (OB_SUCC(ret) && false == finished) {
      if (OB_FAIL(row_fetcher.get_next_row(const_cast<ObNewRow*&>(join_ctx.right_row_)))) {
      } else if (OB_FAIL(calc_other_conds(join_ctx, matched))) {
        LOG_WARN("fail to calc other conds", K(ret));
      } else if (matched) {
        if (OB_FAIL(join_ctx.connect_by_pump_.check_child_cycle(*join_ctx.right_row_, next_node))) {
          if (OB_ERR_CBY_LOOP == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("check child cycle failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (next_node.is_cycle_) {
            node.is_cycle_ = true;
          } else {
            node.is_leaf_ = false;
          }
          finished = (false == output_cycle || node.is_cycle_) && (false == output_leaf || false == node.is_leaf_);
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObConnectBy::read_output_func_going(ObConnectByCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObConnectByPump::PumpNode* top_node = NULL;
  if (OB_FAIL(join_ctx.connect_by_pump_.get_top_pump_node(top_node))) {
    LOG_WARN("connect by pump get top node failed", K(ret));
  } else if (OB_ISNULL(top_node->output_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output row of node is null", K(ret));
  } else if (OB_FAIL(top_node->row_fetcher_.init(join_ctx.connect_by_pump_, hash_probe_exprs_, top_node->pump_row_))) {
    LOG_WARN("fail to begin iterator for chunk row store", K(ret));
  } else if (OB_FAIL(calc_pseudo_flags(join_ctx, *top_node))) {
    LOG_WARN("calc pseudo flags failed", K(ret));
  } else if (OB_FAIL(add_pseudo_column(join_ctx, *top_node))) {
    LOG_WARN("fail to add pseudo column", K(ret));
  } else if (1 == top_node->level_ &&
             OB_FAIL(join_ctx.connect_by_pump_.set_connect_by_root_row(top_node->output_row_))) {
    LOG_WARN("fail to set connect by root", K(ret));
  } else if (OB_FAIL(calc_connect_by_root_exprs(join_ctx,
                 const_cast<ObNewRow*>(join_ctx.connect_by_pump_.connect_by_root_row_),
                 top_node->output_row_))) {
    LOG_WARN("fail to calc connect_by_root_exprs", K(ret));
  } else if (OB_FAIL(calc_sys_connect_by_path(join_ctx, const_cast<ObNewRow*>(top_node->output_row_)))) {
    LOG_WARN("Failed to calc sys connect by paht", K(ret));
  } else {
    row = top_node->output_row_;
    join_ctx.connect_by_pump_.cur_level_ = top_node->level_;
    join_ctx.state_ = JS_READ_PUMP;
  }
  return ret;
}

int ObConnectBy::read_output_func_end(ObConnectByCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  join_ctx.state_ = JS_READ_LEFT;
  return ret;
}

int ObConnectBy::read_pump_operate(ObConnectByCtx& join_ctx) const
{
  UNUSED(join_ctx);
  int ret = OB_SUCCESS;
  return ret;
}

int ObConnectBy::read_pump_func_going(ObConnectByCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (OB_FAIL(join_ctx.connect_by_pump_.get_next_row(&join_ctx))) {
    if (OB_ITER_END == ret) {  // finish search a tree
      ret = OB_SUCCESS;
      join_ctx.state_ = JS_READ_LEFT;
    } else {
      LOG_WARN("connect by pump get next row failed", K(ret));
    }
  } else {
    join_ctx.state_ = JS_READ_OUTPUT;
  }
  return ret;
}

int ObConnectBy::read_pump_func_end(ObConnectByCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  UNUSED(join_ctx);
  return OB_ITER_END;
}

int ObConnectBy::add_pseudo_column(ObConnectByCtx& join_ctx, ObConnectByPump::PumpNode& node) const
{
  int ret = OB_SUCCESS;
  ObNewRow* output_row = const_cast<ObNewRow*>(node.output_row_);
  if (OB_ISNULL(output_row) || OB_UNLIKELY(!output_row->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", KPC(output_row), K(ret));
  } else if (OB_UNLIKELY(CONNECT_BY_PSEUDO_COLUMN_CNT != pseudo_column_row_desc_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pseudo_column_row_desc", K(ret));
  } else {
    if (UNUSED_POS != pseudo_column_row_desc_[LEVEL]) {
      int64_t cell_idx = pseudo_column_row_desc_[LEVEL];
      if (cell_idx > output_row->count_ - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid cell idx", K(cell_idx), K(output_row->count_), K(ret));
      } else {
        common::number::ObNumber num;
        ObObj val;
        int64_t level = node.level_;
        if (OB_FAIL(num.from(level, *join_ctx.calc_buf_))) {
          LOG_WARN("Failed to generate LEVEL values", K(ret));
        } else {
          val.set_number(num);
        }
        output_row->cells_[cell_idx] = val;
      }
    }

    if (OB_SUCC(ret) && UNUSED_POS != pseudo_column_row_desc_[CONNECT_BY_ISLEAF]) {
      int64_t cell_idx = pseudo_column_row_desc_[CONNECT_BY_ISLEAF];
      if (cell_idx > output_row->count_ - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid cell idx", K(cell_idx), K(output_row->count_), K(ret));
      } else {
        common::number::ObNumber num;
        ObObj val;
        int64_t num_value = node.is_leaf_ ? 1 : 0;
        if (OB_FAIL(num.from(num_value, *join_ctx.calc_buf_))) {
          LOG_WARN("Failed to generate CONNECT_BY_ISLEAF values", K(ret));
        } else {
          val.set_number(num);
        }
        output_row->cells_[cell_idx] = val;
      }
    }

    if (OB_SUCC(ret) && UNUSED_POS != pseudo_column_row_desc_[CONNECT_BY_ISCYCLE]) {
      int64_t cell_idx = pseudo_column_row_desc_[CONNECT_BY_ISCYCLE];
      if (cell_idx > output_row->count_ - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid cell idx", K(cell_idx), K(output_row->count_), K(ret));
      } else {
        common::number::ObNumber num;
        ObObj val;
        int64_t num_value = node.is_cycle_ ? 1 : 0;
        if (OB_FAIL(num.from(num_value, *join_ctx.calc_buf_))) {
          LOG_WARN("Failed to generate CONNECT_BY_ISCYCLE values", K(ret));
        } else {
          val.set_number(num);
        }
        output_row->cells_[cell_idx] = val;
      }
    }
    LOG_DEBUG("add output row", KPC(output_row));
  }
  return ret;
}

int ObConnectBy::join_end_operate(ObConnectByCtx& join_ctx) const
{
  UNUSED(join_ctx);
  return OB_ITER_END;
}

int ObConnectBy::join_end_func_end(ObConnectByCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(join_ctx);
  UNUSED(row);
  return OB_ITER_END;
}

OB_SERIALIZE_MEMBER((ObConnectBy, ObConnectByBase));

}  // namespace sql
}  // namespace oceanbase
