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

#include "sql/engine/ob_phy_operator.h"
#include "share/ob_worker.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/object/ob_obj_compare.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_job_conf.h"
#include "sql/executor/ob_receive.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
namespace oceanbase {
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER(ColumnContent, projector_index_, auto_filled_timestamp_, column_name_, is_nullable_, is_implicit_,
    column_type_, coll_type_);
OB_SERIALIZE_MEMBER(ObForeignKeyColumn, name_, idx_, name_idx_);
OB_SERIALIZE_MEMBER(ObForeignKeyArg, ref_action_, table_name_, columns_, database_name_, is_self_ref_);
OB_SERIALIZE_MEMBER(ObOpSchemaObj, obj_type_, is_not_null_, order_type_);

ObPhyOperator::ObPhyOperator(ObIAllocator& alloc)
    : sql_expression_factory_(alloc),
      my_phy_plan_(NULL),
      parent_op_(NULL),
      id_(OB_INVALID_ID),
      column_count_(0),
      projector_size_(0),
      filter_exprs_(),
      calc_exprs_(),
      startup_exprs_(),
      virtual_column_exprs_(),
      rows_(0),
      cost_(0),
      width_(0),
      projector_(NULL),
      is_exact_rows_(false),
      type_(PHY_INVALID),
      plan_depth_(0),
      op_schema_objs_(alloc)
{}

ObPhyOperator::~ObPhyOperator()
{
  reset();
}

void ObPhyOperator::reset()
{
  type_ = PHY_INVALID;
  my_phy_plan_ = NULL;
  parent_op_ = NULL;
  id_ = OB_INVALID_ID;
  column_count_ = 0;
  projector_ = NULL;
  projector_size_ = 0;
  // Embedded linked list, no memory management is involved, only clear() method, reset state back and forth
  filter_exprs_.reset();
  calc_exprs_.reset();
  startup_exprs_.reset();
  virtual_column_exprs_.reset();
  op_schema_objs_.reset();
}

void ObPhyOperator::reuse()
{
  type_ = PHY_INVALID;
  my_phy_plan_ = NULL;
  parent_op_ = NULL;
  id_ = OB_INVALID_ID;
  column_count_ = 0;
  projector_ = NULL;
  projector_size_ = 0;
  filter_exprs_.reset();
  calc_exprs_.reset();
  startup_exprs_.reset();
  virtual_column_exprs_.reset();
  op_schema_objs_.reset();
}
int ObPhyOperator::handle_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get operator ctx failed", K(ret));
  } else if (OB_FAIL(wrap_expr_ctx(ctx, op_ctx->expr_ctx_))) {
    LOG_WARN("wrap_expr_ctx failed", K(ret));
  }
  return ret;
}

int ObPhyOperator::open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  OperatorOpenOrder open_order = get_operator_open_order(ctx);
  while (OB_SUCC(ret) && open_order != OPEN_EXIT) {
    switch (open_order) {
      case OPEN_CHILDREN_FIRST:
      case OPEN_CHILDREN_LATER: {
        ObPhyOperator* child_ptr = NULL;
        for (int32_t i = 0; OB_SUCC(ret) && i < get_child_num(); ++i) {
          if (OB_ISNULL(child_ptr = get_child(i))) {
            ret = OB_NOT_INIT;
            LOG_WARN("failed to get child", K(i), K(child_ptr), K(get_child_num()), K(ret));
          } else if (OB_FAIL(child_ptr->open(ctx))) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
              LOG_WARN("Open child operator failed", K(ret), "op_type", ob_phy_operator_type_str(get_type()));
            }
          } else {
          }
        }
        open_order = (OPEN_CHILDREN_FIRST == open_order) ? OPEN_SELF_LATER : OPEN_EXIT;
        break;
      }
      case OPEN_SELF_FIRST:
      case OPEN_SELF_LATER:
      case OPEN_SELF_ONLY: {
        if (OB_FAIL(inner_open(ctx))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("Open this operator failed", K(ret), "op_type", ob_phy_operator_type_str(get_type()));
          }
        } else if (OB_FAIL(handle_op_ctx(ctx))) {
          LOG_WARN("handle op ctx failed ", K(ret));
        }
        open_order = (OPEN_SELF_FIRST == open_order) ? OPEN_CHILDREN_LATER : OPEN_EXIT;
        break;
      }
      case OPEN_NONE:
      case OPEN_EXIT: {
        open_order = OPEN_EXIT;
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected open order type", K(open_order));
        break;
    }
    if (OB_FAIL(ret)) {
      if (OB_FAIL(process_expect_error(ctx, ret))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("fail to process error", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(after_open(ctx))) {
      LOG_WARN("after open operator failed", K(ret), "op_type", ob_phy_operator_type_str(get_type()), K(id_));
    }
  }
  LOG_DEBUG("open op", K(ret), "op_type", ob_phy_operator_type_str(get_type()), "op_id", get_id());
  return ret;
}

int ObPhyOperator::process_expect_error(ObExecContext& ctx, int errcode) const
{
  UNUSED(ctx);
  return errcode;
}

int ObPhyOperator::close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  int child_ret = OB_SUCCESS;
  OperatorOpenOrder open_order = get_operator_open_order(ctx);

  if (OPEN_SELF_ONLY != open_order && OPEN_NONE != open_order) {
    // first call close of all children
    ObPhyOperator* child_ptr = NULL;
    for (int32_t i = 0; i < get_child_num(); ++i) {
      // For the physical operator, when closing, regardless of whether the child's clos succeeds or fails,
      // all child closes must be called and not need to check ret
      if (OB_ISNULL(child_ptr = get_child(i))) {
        ret = OB_NOT_INIT;
        LOG_WARN("failed to get child",
            K(i),
            K(child_ptr),
            K(get_child_num()),
            K(ret),
            "op_type",
            ob_phy_operator_type_str(get_type()));
      } else {
        int tmp_ret = child_ptr->close(ctx);
        if (OB_SUCCESS != tmp_ret) {
          child_ret = tmp_ret;
          LOG_WARN("Close child operator failed", K(child_ret), "op_type", ob_phy_operator_type_str(get_type()));
        }
      }
    }
  }

  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, ctx, get_id()))) {
    LOG_DEBUG("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  }

  if (OPEN_NONE != open_order) {
    // no matter what, must call operatoris close function, then close this operator
    if (OB_FAIL(inner_close(ctx))) {
      LOG_WARN("Close this operator failed", K(ret), "op_type", ob_phy_operator_type_str(get_type()));
    } else if (op_ctx) {
      op_ctx->op_monitor_info_.close_time_ = oceanbase::common::ObClockGenerator::getClock();
    }
  }

  if (OB_SUCC(ret)) {
    // Can only preserve one error code
    ret = child_ret;
  }

  if (GCONF.enable_sql_audit) {
    // Record current operator statistics to sql_plan_monitor, inner_sql will not be counted
    ObPlanMonitorNodeList* list = MTL_GET(ObPlanMonitorNodeList*);
    if (op_ctx && list && my_phy_plan_ && ctx.get_my_session()->is_user_session() &&
        OB_PHY_PLAN_LOCAL != my_phy_plan_->get_plan_type() && OB_PHY_PLAN_REMOTE != my_phy_plan_->get_plan_type()) {
      op_ctx->op_monitor_info_.set_plan_depth(plan_depth_);
      IGNORE_RETURN list->submit_node(op_ctx->op_monitor_info_);
    }
  }

  return ret;
}

int ObPhyOperator::rescan(ObExecContext& ctx) const
{
  // rescan must reset the oeprator context to the state after call operator open()
  // for the general non-terminal operator, function rescan() is to call children rescan()
  // if you want to do more, you must rewrite this function
  // for the general terminal operator, function rescan() does nothing
  // you can rewrite it to complete special function
  int ret = OB_SUCCESS;
  ObPhyOperator* child_op = NULL;
  for (int32_t i = 0; OB_SUCC(ret) && i < get_child_num(); ++i) {
    if (OB_ISNULL(child_op = get_child(i))) {
      LOG_WARN("failed to get child", K(i), K(child_op), K(get_child_num()), K(ret));
    } else if (OB_FAIL(child_op->rescan(ctx))) {
      LOG_WARN("rescan child operator failed",
          K(ret),
          K(i),
          "op_type",
          ob_phy_operator_type_str(get_type()),
          "child op_type",
          ob_phy_operator_type_str(child_op->get_type()));
    } else {
    }
  }
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    op_ctx->is_filtered_has_set_ = false;
    op_ctx->op_monitor_info_.rescan_times_++;
  }
  return ret;
}

int ObPhyOperator::switch_iterator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperator* child_op = NULL;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx is null", K(ret));
  } else if (plan_ctx->get_bind_array_count() <= 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    op_ctx->is_filtered_has_set_ = false;
    // op_ctx->op_monitor_info_.increase_value(RESCAN_TIMES);
    ++op_ctx->expr_ctx_.cur_array_index_;
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < get_child_num(); ++i) {
    if (OB_ISNULL(child_op = get_child(i))) {
      LOG_WARN("failed to get child", K(i), K(child_op), K(get_child_num()), K(ret));
    } else if (OB_FAIL(child_op->switch_iterator(ctx))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("switch child operator iterator failed",
            K(ret),
            "op_type",
            ob_phy_operator_type_str(get_type()),
            "child op_type",
            ob_phy_operator_type_str(child_op->get_type()));
      }
    } else {
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObPhyOperator)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialize_dlist(calc_exprs_, buf, buf_len, pos))) {
    LOG_WARN("failed to serialize calc_exprs_", K(ret));
  } else if (OB_FAIL(serialize_dlist(filter_exprs_, buf, buf_len, pos))) {
    LOG_WARN("failed to serialize filter_exprs_", K(ret));
  } else if (OB_FAIL(serialize_dlist(startup_exprs_, buf, buf_len, pos))) {
    LOG_WARN("failed to serialize startup_exprs_", K(ret));
  }

  OB_UNIS_ENCODE(static_cast<int64_t>(id_));
  OB_UNIS_ENCODE(column_count_);
  OB_UNIS_ENCODE_ARRAY(projector_, projector_size_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_dlist(virtual_column_exprs_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize startup_exprs_", K(ret));
    }
  }

  OB_UNIS_ENCODE(op_schema_objs_);
  OB_UNIS_ENCODE(is_exact_rows_);
  OB_UNIS_ENCODE(rows_);
  OB_UNIS_ENCODE(width_);
  OB_UNIS_ENCODE(px_est_size_factor_);
  OB_UNIS_ENCODE(plan_depth_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPhyOperator)
{
  int ret = OB_SUCCESS;
  int64_t id_int64 = 0;

  if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan is null");
  }
  OB_UNIS_DECODE_EXPR_DLIST(ObColumnExpression, calc_exprs_, my_phy_plan_);
  OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, filter_exprs_, my_phy_plan_);
  OB_UNIS_DECODE_EXPR_DLIST(ObSqlExpression, startup_exprs_, my_phy_plan_);
  OB_UNIS_DECODE(id_int64);
  OB_UNIS_DECODE(column_count_);
  if (OB_SUCC(ret)) {
    id_ = static_cast<uint64_t>(id_int64);
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(projector_size_);
    if (projector_size_ > 0) {
      ObIAllocator& alloc = my_phy_plan_->get_allocator();
      if (OB_ISNULL(projector_ = static_cast<int32_t*>(alloc.alloc(sizeof(int32_t) * projector_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("no memory", K_(projector_size));
      } else {
        OB_UNIS_DECODE_ARRAY(projector_, projector_size_);
      }
    } else {
      projector_ = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE_EXPR_DLIST(ObColumnExpression, virtual_column_exprs_, my_phy_plan_);
    OB_UNIS_DECODE(op_schema_objs_);
    OB_UNIS_DECODE(is_exact_rows_);
    OB_UNIS_DECODE(rows_);
    OB_UNIS_DECODE(width_);
    OB_UNIS_DECODE(px_est_size_factor_);
    OB_UNIS_DECODE(plan_depth_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPhyOperator)
{
  int64_t len = 0;
  len += get_dlist_serialize_size(calc_exprs_);
  len += get_dlist_serialize_size(filter_exprs_);
  len += get_dlist_serialize_size(startup_exprs_);
  OB_UNIS_ADD_LEN(static_cast<int64_t>(id_));
  OB_UNIS_ADD_LEN(column_count_);
  OB_UNIS_ADD_LEN_ARRAY(projector_, projector_size_);
  len += get_dlist_serialize_size(virtual_column_exprs_);
  OB_UNIS_ADD_LEN(op_schema_objs_);
  OB_UNIS_ADD_LEN(is_exact_rows_);
  OB_UNIS_ADD_LEN(rows_);
  OB_UNIS_ADD_LEN(width_);
  OB_UNIS_ADD_LEN(px_est_size_factor_);
  OB_UNIS_ADD_LEN(plan_depth_);
  return len;
}

int ObPhyOperator::add_filter(ObSqlExpression* expr, bool startup)
{
  int ret = OB_SUCCESS;
  if (startup) {
    if (OB_FAIL(ObSqlExpressionUtil::add_expr_to_list(startup_exprs_, expr))) {
      OB_LOG(WARN, "fail to add expr to list", K(ret));
    }
  } else {
    if (OB_FAIL(ObSqlExpressionUtil::add_expr_to_list(filter_exprs_, expr))) {
      OB_LOG(WARN, "fail to ad expr to list", K(ret));
    }
  }
  return ret;
}

int ObPhyOperator::add_virtual_column_expr(ObColumnExpression* expr)
{
  return ObSqlExpressionUtil::add_expr_to_list(virtual_column_exprs_, expr);
}

int ObPhyOperator::add_compute(ObColumnExpression* expr)
{
  return ObSqlExpressionUtil::add_expr_to_list(calc_exprs_, expr);
}

int ObPhyOperator::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* input_row = NULL;
  bool is_filtered = false;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else if (OB_ISNULL(op_ctx->expr_ctx_.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc buffer is null", K(ret), K(get_id()));
  } else if (!op_ctx->is_filtered_has_set_) {
    if (OB_FAIL(startup_filter(op_ctx->expr_ctx_, op_ctx->is_filtered_))) {
      LOG_WARN("call startup_filter failed", K(ret), K(op_ctx->is_filtered_));
    } else {
      op_ctx->is_filtered_has_set_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (op_ctx->is_filtered_) {
      ret = OB_ITER_END;
    }
  }
  while (OB_SUCC(ret)) {
    op_ctx->expr_ctx_.calc_buf_->reuse();
    op_ctx->expr_ctx_.row_ctx_.reset();
    if (OB_FAIL(inner_get_next_row(ctx, input_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed", K(ret), K_(type), K_(id), "op", ob_phy_operator_type_str(get_type()));
      }
    } else {
      if (need_filter_row() && OB_FAIL(filter_row(op_ctx->expr_ctx_, *input_row, is_filtered))) {  // filter the row
        LOG_WARN("call filter_row failed", K(ret), K_(type), K_(id), "op", ob_phy_operator_type_str(get_type()));
      } else if (!is_filtered) {
        // not be filtered, break
        break;
      } else {
      }
    }
    // be filtered, continue to get next row
  }
  // calculate
  if (OB_SUCC(ret)) {
    ObNewRow* calc_row = const_cast<ObNewRow*>(input_row);
    if (OB_FAIL(calculate_row(op_ctx->expr_ctx_, *calc_row))) {
      LOG_WARN("calculate row failed", K(ret));
    } else {
      row = calc_row;
      if (!op_ctx->got_first_row_) {
        op_ctx->op_monitor_info_.first_row_time_ = oceanbase::common::ObClockGenerator::getClock();
        op_ctx->got_first_row_ = true;
      }
      op_ctx->op_monitor_info_.output_row_count_++;
    }
  }
  if (OB_ITER_END == ret && NULL != op_ctx) {
    int tmp_ret = drain_exch(ctx);
    if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("drain exchange data failed", K(tmp_ret));
    }
    if (op_ctx->got_first_row_) {
      op_ctx->op_monitor_info_.last_row_time_ = oceanbase::common::ObClockGenerator::getClock();
    }
  }
  return ret;
}

int ObPhyOperator::get_next_rows(ObExecContext& ctx, const ObNewRow*& rows, int64_t& row_count) const
{
  int ret = get_next_row(ctx, rows);
  if (OB_SUCCESS == ret) {
    row_count = 1;
  } else {
    row_count = 0;
  }

  return ret;
}

int ObPhyOperator::inner_open(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObPhyOperator::after_open(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObPhyOperator::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObPhyOperator::startup_filter(ObExprCtx& expr_ctx, bool& is_filtered) const
{
  int ret = OB_SUCCESS;

  bool is_true = false;
  is_filtered = false;
  ObObj result;
  ObNewRow empty_row;
  if (!startup_exprs_.is_empty()) {
    DLIST_FOREACH(p, startup_exprs_)
    {
      result.reset();
      if (OB_ISNULL(p)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null");
      } else if (OB_FAIL(p->calc(expr_ctx, empty_row, result))) {
        LOG_WARN("failed to calc expression", K(ret), K(*p));
      } else if (OB_FAIL(ObObjEvaluator::is_true(result, is_true))) {
        LOG_WARN("failed to call is true", K(ret));
      } else if (!is_true) {
        // the row is filtered, break to get next row
        is_filtered = true;
        break;
      } else {
      }
    }  // end for
  }
  return ret;
}

inline int ObPhyOperator::filter_row(ObExprCtx& expr_ctx, const ObNewRow& row, bool& is_filtered) const
{
  int ret = OB_SUCCESS;
  is_filtered = false;
  if (OB_FAIL(filter_row(expr_ctx, row, filter_exprs_, is_filtered))) {
    LOG_WARN("failed to filter row", K(ret));
  } else {
    // now some cells in row maybe wild pointer, these cells will be set in calculate_row later,
    // so we can't print row here.
    LOG_DEBUG("got 1 row from child", "op_type", ob_phy_operator_type_str(get_type()), /*K(row),*/ K(is_filtered));
  }
  return ret;
}

int ObPhyOperator::filter_row(
    ObExprCtx& expr_ctx, const ObNewRow& row, const ObDList<ObSqlExpression>& filters, bool& is_filtered) const
{
  int ret = OB_SUCCESS;
  is_filtered = false;
  if (OB_UNLIKELY(NULL == row.cells_ || row.count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is not init", K(ret));
  } else if (!filters.is_empty()) {
    DLIST_FOREACH(p, filters)
    {
      if (OB_FAIL(filter_row_inner(expr_ctx, row, p, is_filtered))) {
        LOG_WARN("fail to filter row", K(ret));
      } else if (is_filtered) {
        break;
      }
    }  // end for
  }
  return ret;
}

int ObPhyOperator::filter_row_for_check_cst(
    ObExprCtx& expr_ctx, const ObNewRow& row, const ObDList<ObSqlExpression>& filters, bool& is_filtered) const
{
  int ret = OB_SUCCESS;
  is_filtered = false;

  if (OB_UNLIKELY(NULL == row.cells_ || row.count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is not init", K(ret));
  } else if (!filters.is_empty()) {
    DLIST_FOREACH(p, filters)
    {
      if (OB_FAIL(filter_row_inner_for_check_cst(expr_ctx, row, p, is_filtered))) {
        LOG_WARN("fail to filter row", K(ret));
      } else if (is_filtered) {
        break;
      }
    }  // end for
  }

  return ret;
}

int ObPhyOperator::filter_row(
    ObExprCtx& expr_ctx, const ObNewRow& row, const ObIArray<ObISqlExpression*>& filters, bool& is_filtered)
{
  int ret = OB_SUCCESS;
  is_filtered = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    if (OB_FAIL(filter_row_inner(expr_ctx, row, filters.at(i), is_filtered))) {
      LOG_WARN("fail to filter row", K(ret));
    } else if (is_filtered) {
      break;
    }
  }

  return ret;
}

int ObPhyOperator::filter_row_inner_for_check_cst(
    ObExprCtx& expr_ctx, const ObNewRow& row, const ObISqlExpression* expr, bool& is_filtered)
{
  int ret = OB_SUCCESS;
  ObObj result;
  bool is_false = false;

  if (OB_ISNULL(expr) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(expr_ctx.calc_buf_), K(ret));
  } else if (OB_FAIL(expr->calc(expr_ctx, row, result))) {
    LOG_WARN("failed to calc expression", K(ret), K(*expr));
  } else if (OB_FAIL(ObObjEvaluator::is_false(result, is_false))) {
    LOG_WARN("failed to call is true", K(ret));
  } else {
    if (is_false) {
      expr_ctx.calc_buf_->reuse();
      // the row is filtered, break to get next row
      is_filtered = true;
    }
  }

  return ret;
}

int ObPhyOperator::filter_row_inner(
    ObExprCtx& expr_ctx, const ObNewRow& row, const ObISqlExpression* expr, bool& is_filtered)
{
  int ret = OB_SUCCESS;
  ObObj result;
  bool is_true = false;
  if (OB_ISNULL(expr) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(expr_ctx.calc_buf_), K(ret));
  } else if (OB_FAIL(expr->calc(expr_ctx, row, result))) {
    LOG_WARN("failed to calc expression", K(ret), K(*expr));
  } else if (OB_FAIL(ObObjEvaluator::is_true(result, is_true))) {
    LOG_WARN("failed to call is true", K(ret));
  } else {
    // PLEASE...DO NOT DELETE ME...JUST LEAVE ME ALONE 'CAUSE ONE DAY I WOULD
    if (!is_true) {
      expr_ctx.calc_buf_->reuse();
      // the row is filtered, break to get next row
      is_filtered = true;
    }
  }

  return ret;
}

int ObPhyOperator::calculate_row(common::ObExprCtx& expr_ctx, common::ObNewRow& calc_row) const
{
  return calculate_row_inner(expr_ctx, calc_row, calc_exprs_);
}

int ObPhyOperator::calculate_virtual_column(common::ObExprCtx& expr_ctx, common::ObNewRow& calc_row) const
{
  return calculate_row_inner(expr_ctx, calc_row, virtual_column_exprs_);
}

int ObPhyOperator::calculate_row_inner(
    ObExprCtx& expr_ctx, ObNewRow& calc_row, const ObDList<ObSqlExpression>& calc_exprs) const
{
  int ret = OB_SUCCESS;
  const ObColumnExpression* p = NULL;
  if (OB_UNLIKELY(NULL == calc_row.cells_ || calc_row.count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is not init");
  } else if (!calc_exprs.is_empty()) {
    DLIST_FOREACH(node, calc_exprs)
    {
      p = static_cast<const ObColumnExpression*>(node);
      if (OB_UNLIKELY(NULL == node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null");
      } else if (OB_FAIL(p->calc_and_project(expr_ctx, calc_row))) {
        LOG_WARN("calculate column expression failed", K(ret), K(*p), "op_type", ob_phy_operator_type_str(get_type()));
        expr_ctx.err_col_idx_ = calc_row.get_project_idx(p->get_result_index());
      } else {
      }
    }
    LOG_DEBUG("calculate row", K(ret), "op_type", ob_phy_operator_type_str(get_type()), K(calc_row));
  }
  return ret;
}

int ObPhyOperator::init_cur_row(ObPhyOperatorCtx& op_ctx, bool need_create_cells) const
{
  int ret = OB_SUCCESS;
  if (need_create_cells && get_column_count() > 0) {
    // Need to create a cells array for the current row, this time the child row will be actually copied to the current
    // row
    if (OB_FAIL(op_ctx.create_cur_row(get_column_count(), projector_, projector_size_))) {
      LOG_WARN("create current row failed", K(ret));
    }
  } else {
    ObNewRow& cur_row = op_ctx.get_cur_row();
    cur_row.cells_ = NULL;
    cur_row.count_ = get_column_count();
    cur_row.projector_ = projector_;
    cur_row.projector_size_ = projector_size_;
  }
  return ret;
}

int ObPhyOperator::init_cur_rows(int64_t row_count, ObPhyOperatorCtx& op_ctx, bool need_create_cells) const
{
  int ret = OB_SUCCESS;
  if (need_create_cells && get_column_count() > 0) {
    // Need to create a cells array for the current row, this time the child row will be actually copied to the current
    // row
    if (OB_FAIL(op_ctx.create_cur_rows(row_count, get_column_count(), projector_, projector_size_))) {
      LOG_WARN("create current rows failed", K(ret));
    }

  } else {
    ObNewRow* cur_rows = op_ctx.get_cur_rows();
    int64_t row_count = op_ctx.get_row_count();
    for (int i = 0; i < row_count; i++) {
      cur_rows[i].cells_ = NULL;
      cur_rows[i].count_ = get_column_count();
      cur_rows[i].projector_ = projector_;
      cur_rows[i].projector_size_ = projector_size_;
    }
  }
  return ret;
}

int ObPhyOperator::ObPhyOperatorCtx::alloc_row_cells(const int64_t column_count, common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (column_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_count));
  } else if (OB_ISNULL(ptr = exec_ctx_.get_allocator().alloc(column_count * sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for row failed", "size", column_count * sizeof(ObObj));
  } else {
    // This is not initializing, but assigning a value to the row object. What is passed
    // here is a reference, not a pointer. The initialization of all members should be managed by the caller
    row.cells_ = new (ptr) common::ObObj[column_count];
    row.count_ = column_count;
  }
  return ret;
}

int ObPhyOperator::ObPhyOperatorCtx::alloc_rows_cells(
    const int64_t row_count, const int64_t column_count, common::ObNewRow* rows)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (row_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_count));
  } else if (column_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_count));
  } else if (OB_ISNULL(ptr = exec_ctx_.get_allocator().alloc(row_count * column_count * sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for row failed", "size", row_count * column_count * sizeof(ObObj));
  } else {
    // This is not initializing, but assigning a value to the row object. What is passed
    // here is a reference, not a pointer. The initialization of all members should be managed by the caller
    for (int64_t i = 0; i < row_count; i++) {
      long unsigned offset = i * column_count * sizeof(ObObj);
      rows[i].cells_ = new (static_cast<char*>(ptr) + offset) common::ObObj[column_count];
      rows[i].count_ = column_count;
    }
  }
  return ret;
}

int ObPhyOperator::ObPhyOperatorCtx::create_cur_rows(
    int64_t row_count, int64_t column_count, int32_t* projector, int64_t projector_size)
{
  int ret = OB_SUCCESS;

  if (row_count <= 0 || column_count <= 0 || (projector == NULL && projector_size > 0) ||
      (projector != NULL && projector_size == 0)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(row_count), K(column_count), K(projector), K(projector_size));
  } else if (row_count > BULK_COUNT) {
    row_count = BULK_COUNT;
  }
  row_count_ = row_count;
  void* ptr = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ptr = exec_ctx_.get_allocator().alloc(row_count_ * sizeof(ObNewRow)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(ERROR, "alloc memory for row failed", "size", row_count_ * sizeof(ObNewRow));
  } else {
    cur_rows_ = new (ptr) ObNewRow[row_count_];

    if (OB_SUCCESS == (ret = alloc_rows_cells(row_count_, column_count, cur_rows_))) {
      for (int64_t i = 0; i < row_count_; i++) {
        cur_rows_[i].projector_ = projector;
        cur_rows_[i].projector_size_ = projector_size;
      }
      column_count_ = column_count;
      projector_ = projector;
      projector_size_ = projector_size;
    }
  }
  return ret;
}

// Copy the row cells to the current row in the physical operator context and keep the physical location of the original
// row cells
int ObPhyOperator::copy_cur_row(ObPhyOperatorCtx& op_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_cur_row(op_ctx.get_cur_row(), row, need_copy_row_for_compute()))) {
    LOG_WARN("copy current row failed", K(ret));
  }
  return ret;
}

int ObPhyOperator::copy_cur_row(ObNewRow& cur_row, const ObNewRow*& row, bool need_copy_cell) const
{
  int ret = OB_SUCCESS;
  cur_row.count_ = column_count_;
  cur_row.projector_ = projector_;
  cur_row.projector_size_ = projector_size_;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(need_copy_cell)) {
    int64_t real_count = row->get_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < real_count; ++i) {
      int64_t real_idx = row->projector_size_ > 0 ? row->projector_[i] : i;
      // In order to minimize copying, only copy the columns projected by the child operator
      // Because the materialization operations for compute are special, The row_desc of
      // this layer and the row_desc of the child operator are exactly the same and so
      // it needs to be copied to its own corresponding position, and the physical position
      // of the child operator must be one-to-one correspondence. The cells filtered by
      // projection do not need to be copied, and these cells will not be used in this layer
      if (OB_UNLIKELY(real_idx >= row->count_ || real_idx >= cur_row.count_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid row count", K(real_idx), K_(row->count), K_(cur_row.count));
      } else {
        cur_row.cells_[real_idx] = row->cells_[real_idx];
      }
    }
  } else if (OB_UNLIKELY(cur_row.count_ != row->count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current row is not equal", K_(cur_row.count), K_(row->count));
  } else {
    // No need to copy the cell, only need to reproject the current row of the child operator to the current row
    cur_row.cells_ = row->cells_;
  }
  if (OB_SUCC(ret)) {
    row = &cur_row;
  }
  return ret;
}

int ObPhyOperator::copy_cur_row_by_projector(ObNewRow& cur_row, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  int64_t col_cnt = 0;
  if (OB_ISNULL(row) || OB_UNLIKELY(cur_row.is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is null", K(row), "cur_row", cur_row);
  } else if (OB_UNLIKELY(&cur_row == row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("self assignment. current row has same address with row", K(ret));
  } else {
    col_cnt = min(cur_row.count_, row->get_count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
    cur_row.cells_[i] = row->get_cell(i);
  }
  // set rest objs to be nulltype in case of prone bugs
  for (int64_t i = row->get_count(); OB_SUCC(ret) && i < cur_row.count_; ++i) {
    cur_row.cells_[i].set_null();
  }
  row = &cur_row;
  return ret;
}

int ObPhyOperator::copy_cur_row_by_projector(ObPhyOperatorCtx& op_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObNewRow& cur_row = op_ctx.get_cur_row();
  if (OB_FAIL(copy_cur_row_by_projector(cur_row, row))) {
    LOG_WARN("fail to copy row by projector", K(row), K(cur_row), K(ret));
  }
  return ret;
}

int ObPhyOperator::copy_cur_rows_by_projector(ObPhyOperatorCtx& op_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObNewRow* cur_row = NULL;
  if (op_ctx.index_ >= op_ctx.row_count_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index is not valid", K(op_ctx.index_), K(ret));
  } else {
    cur_row = &op_ctx.get_cur_rows()[op_ctx.index_];
    if (OB_FAIL(copy_cur_row_by_projector(*cur_row, row))) {
      LOG_WARN("fail to copy row by projector", K(op_ctx.row_count_), K(op_ctx.index_), K(row), K(*cur_row), K(ret));
    } else {
      op_ctx.index_++;
    }
  }
  return ret;
}

int ObPhyOperator::copy_cur_rows(ObPhyOperatorCtx& op_ctx, const ObNewRow*& row, bool need_copy_cell) const
{
  int ret = OB_SUCCESS;
  ObNewRow* cur_row = NULL;
  if (op_ctx.index_ >= op_ctx.row_count_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index is not valid", K(op_ctx.index_), K(ret));
  } else {
    cur_row = &op_ctx.get_cur_rows()[op_ctx.index_];
    if (OB_FAIL(copy_cur_row(*cur_row, row, need_copy_cell))) {
      LOG_WARN("fail to copy row failed", K(row), K(*cur_row), K(ret));
    } else {
      op_ctx.index_++;
    }
  }
  return ret;
}

int ObPhyOperator::wrap_expr_ctx(ObExecContext& exec_ctx, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObPhyOperatorCtx* phy_op_ctx = NULL;
  const ObTimeZoneInfo* tz_info = NULL;
  int64_t tz_offset = 0;
  if (OB_ISNULL(phy_plan_ctx = exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_physical_plan_ctx failed", K(ret));
  } else if (OB_ISNULL(my_session = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed", K(ret));
  } else if (OB_ISNULL(phy_op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else if (OB_ISNULL(tz_info = get_timezone_info(my_session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tz info pointer failed", K(ret));
  } else if (OB_FAIL(get_tz_offset(tz_info, tz_offset))) {
    LOG_WARN("get tz offset failed", K(ret));
  } else {
    ObMemAttr mem_attr(my_session->get_effective_tenant_id(), ObModIds::OB_SQL_EXPR_CALC);
    phy_op_ctx->get_calc_buf().set_attr(mem_attr);
    expr_ctx.phy_plan_ctx_ = phy_plan_ctx;
    expr_ctx.my_session_ = my_session;
    expr_ctx.exec_ctx_ = &exec_ctx;
    expr_ctx.calc_buf_ = &(phy_op_ctx->get_calc_buf());
    expr_ctx.tz_offset_ = tz_offset;
    expr_ctx.phy_operator_ctx_id_ = get_id();
    if (OB_FAIL(ObSQLUtils::get_default_cast_mode(my_phy_plan_->get_stmt_type(), my_session, expr_ctx.cast_mode_))) {
      LOG_WARN("get default cast mode failed", K(ret));
    } else if (OB_FAIL(ObSQLUtils::wrap_column_convert_ctx(expr_ctx, expr_ctx.column_conv_ctx_))) {
      LOG_WARN("wrap column convert ctx failed", K(ret));
    }
  }
  return ret;
}

int ObPhyOperator::create_operator_input(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

DEF_TO_STRING(ObPhyOperator)
{
  int64_t pos = 0;
  int64_t parent_pos = 0;
  int64_t child_len = 0;
  J_OBJ_START();
  J_KV(N_ID,
      id_,
      N_TYPE,
      type_,
      N_COLUMN_COUNT,
      column_count_,
      N_PROJECTOR,
      ObArrayWrap<int32_t>(projector_, projector_size_),
      N_FILTER_EXPRS,
      filter_exprs_,
      N_CALC_EXPRS,
      calc_exprs_);
  parent_pos = pos;
  J_COMMA();
  child_len = to_string_kv(buf + pos, buf_len - pos);
  if (child_len <= 0) {
    pos = parent_pos;
  } else {
    pos += child_len;
  }
  J_OBJ_END();
  return pos;
}

int ObPhyOperator::check_status(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx.check_status())) {
    LOG_WARN("ctx status check failed", "op_type", ob_phy_operator_type_str(get_type()));
  }
  return ret;
}

int ObPhyOperator::try_check_status(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, ctx, get_id());
  if (OB_ISNULL(op_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("op ctx is null", K(ret));
  } else {
    ++op_ctx->check_times_;
    if (0 == (CHECK_STATUS_MASK & op_ctx->check_times_)) {
      ret = check_status(ctx);
    }
  }
  return ret;
}

int ObPhyOperator::drain_exch(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = nullptr;
  /**
   * 1. try to open this operator
   * 2. try to drain all children
   */
  if (OB_FAIL(try_open_and_get_operator_ctx(ctx, op_ctx))) {
    LOG_WARN("fail to get operator ctx", K(ret));
  } else if (!op_ctx->exch_drained_) {
    op_ctx->exch_drained_ = true;
    const int32_t child_num = get_child_num();
    for (int32_t i = 0; i < child_num && OB_SUCC(ret); i++) {
      const ObPhyOperator* child = get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL child found", K(ret));
      } else if (OB_FAIL(child->drain_exch(ctx))) {
        LOG_WARN("drain exch failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPhyOperator::register_to_datahub(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObPhyOperator::open_child(ObExecContext& ctx, const int32_t child_idx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperator* child_ptr = nullptr;
  if (OB_ISNULL(child_ptr = get_child(child_idx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get child", K(ret), K(child_ptr), K(get_child_num()), K(child_idx));
  } else if (OB_FAIL(child_ptr->open(ctx))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("Open child operator failed", K(ret), "op_type", ob_phy_operator_type_str(get_type()));
    }
  }
  return ret;
}

int ObPhyOperator::get_real_child(ObPhyOperator*& child, const int32_t child_idx) const
{
  int ret = OB_SUCCESS;
  const int32_t first_child_idx = 0;
  ObPhyOperator* first_child = nullptr;
  if (first_child_idx >= get_child_num()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid child idx", K(ret), K(get_child_num()));
  } else if (OB_ISNULL(first_child = get_child(first_child_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null child", K(ret));
  } else if (IS_DUMMY_PHY_OPERATOR(first_child->get_type())) {
    if (OB_FAIL(first_child->get_real_child(child, child_idx))) {
      LOG_WARN("Failed to get real child", K(ret), K(first_child->get_type()));
    }
  } else {
    child = get_child(child_idx);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get a null child", K(ret));
    }
  }
  return ret;
}

int ObPhyOperator::deep_copy_row(const ObNewRow& src_row, ObNewRow*& dst_row, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  char* buf = nullptr;
  int64_t pos = sizeof(ObNewRow);
  ObNewRow* new_row = nullptr;
  int64_t buf_len = src_row.get_deep_copy_size() + sizeof(ObNewRow);
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc new row", K(ret), K(buf_len));
  } else if (OB_ISNULL(new_row = new (buf) ObNewRow())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null new row", K(ret), K(buf_len));
  } else if (OB_FAIL(new_row->deep_copy(src_row, buf, buf_len, pos))) {
    LOG_WARN("Failed to deep copy row", K(ret), K(buf_len), K(pos));
  } else {
    dst_row = new_row;
  }
  return ret;
}

int ObPhyOperator::try_open_and_get_operator_ctx(
    ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx, bool* has_been_opened) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, ctx, get_id()))) {
    // we should open this operator first
    if (OB_NOT_NULL(has_been_opened)) {
      *has_been_opened = false;
    } else if (OB_FAIL(open(ctx))) {
      LOG_WARN("fail to open this operator", K(ret), K(type_));
    } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObPhyOperatorCtx, ctx, get_id()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("get operator ctx failed", K(ret), K(get_id()), K(get_name()));
    }
  } else if (OB_NOT_NULL(has_been_opened)) {
    *has_been_opened = true;
  }
  return ret;
}

int ObPhyOperator::init_op_schema_obj(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(op_schema_objs_.init(count))) {
    LOG_WARN("fail to init op schema obj", K(ret), K(count));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
