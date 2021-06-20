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
#include "sql/engine/subquery/ob_subplan_filter.h"
#include "common/row/ob_row_iterator.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase {
using namespace common;
namespace sql {
class ObSubPlanFilter::ObSubPlanIterator : public ObNewRowIterator {
public:
  ObSubPlanIterator(ObExecContext& ctx, const ObPhyOperator& op)
      : ObNewRowIterator(),
        ctx_(ctx),
        op_(op),
        onetime_plan_(false),
        init_plan_(false),
        inited_(false),
        row_store_(),
        row_store_it_(),
        cur_row_()
  {}
  ~ObSubPlanIterator()
  {}
  void set_tenant_id(uint64_t tenant_id)
  {
    row_store_.set_tenant_id(tenant_id);
  }
  int get_next_row(ObNewRow*& row)
  {
    int ret = OB_SUCCESS;
    const ObNewRow* tmp_row = NULL;
    if (init_plan_ && inited_) {
      if (OB_SUCC(row_store_it_.get_next_row(cur_row_))) {
        row = &cur_row_;
      }
    } else {
      if (OB_SUCC(op_.get_next_row(ctx_, tmp_row))) {
        row = const_cast<ObNewRow*>(tmp_row);
      }
    }
    return ret;
  }
  void reset()
  {
    if (onetime_plan_) {
      // for onetime expr
    } else if (init_plan_) {
      // for init plan
      row_store_it_ = row_store_.begin();
    } else {
      int ret = OB_SUCCESS;
      ObExecContext::ObPlanRestartGuard restart_plan(ctx_);
      if (OB_FAIL(op_.rescan(ctx_))) {
        BACKTRACE(ERROR, true, "failed to do rescan");
      }
    }
  }
  void reuse()
  {
    inited_ = false;
    row_store_.reuse();
    row_store_it_.reset();
  }
  void set_init_plan()
  {
    init_plan_ = true;
  }
  bool is_init_plan()
  {
    return init_plan_;
  }
  void set_onetime_plan()
  {
    onetime_plan_ = true;
  }
  bool is_onetime_plan()
  {
    return onetime_plan_;
  }
  int prepare_init_plan()
  {
    int ret = OB_SUCCESS;
    if (!inited_) {
      if (NULL == cur_row_.cells_) {
        if (OB_FAIL(init_cur_row())) {
          LOG_WARN("failed to init cur_row", K(ret));
        } else { /*do nothing*/
        }
      } else { /*do nothing*/
      }
      ObNewRow* row = NULL;
      while (OB_SUCC(ret) && OB_SUCC(get_next_row(row))) {
        if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row is null");
        } else if (OB_FAIL(row_store_.add_row(*row))) {
          LOG_WARN("failed to add row to row store", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        inited_ = true;
        row_store_it_ = row_store_.begin();
      }
    } else { /*do nothing*/
    }
    return ret;
  }
  int init_cur_row()
  {
    int ret = common::OB_SUCCESS;
    void* ptr = NULL;
    int64_t column_count = op_.get_projector_size();
    if (OB_UNLIKELY(column_count <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(column_count));
    } else if (OB_UNLIKELY(NULL == (ptr = ctx_.get_allocator().alloc(column_count * sizeof(common::ObObj))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory for row failed", K(column_count * sizeof(common::ObObj)));
    } else {
      cur_row_.cells_ = new (ptr) ObObj[column_count];
      cur_row_.count_ = column_count;
    }
    return ret;
  }

private:
  ObExecContext& ctx_;
  const ObPhyOperator& op_;
  bool onetime_plan_;
  bool init_plan_;
  bool inited_;
  common::ObRowStore row_store_;
  ObRowStore::Iterator row_store_it_;
  common::ObNewRow cur_row_;
};

class ObSubPlanFilter::ObSubPlanFilterCtx : public ObPhyOperatorCtx {
  explicit ObSubPlanFilterCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx), subplan_iters_()
  {}
  ~ObSubPlanFilterCtx()
  {
    for (int64_t i = 0; i < subplan_iters_.count(); ++i) {
      ObNewRowIterator*& iter = subplan_iters_.at(i);
      if (iter != NULL) {
        iter->~ObNewRowIterator();
        iter = NULL;
      }
    }
  }
  virtual void destroy()
  {
    subplan_iters_.~ObSEArray<ObNewRowIterator*, 16>();
    ObPhyOperatorCtx::destroy_base();
  }

private:
  ObSEArray<ObNewRowIterator*, 16> subplan_iters_;
  friend class ObSubPlanFilter;
};

ObSubPlanFilter::ObSubPlanFilter(ObIAllocator& alloc)
    : ObMultiChildrenPhyOperator(alloc),
      rescan_params_(alloc),
      onetime_exprs_(alloc),
      init_plan_idxs_(ModulePageAllocator(alloc)),
      one_time_idxs_(ModulePageAllocator(alloc)),
      update_set_(false)
{}

ObSubPlanFilter::~ObSubPlanFilter()
{}

void ObSubPlanFilter::reset()
{
  rescan_params_.reset();
  onetime_exprs_.reset();
  init_plan_idxs_.reset();
  one_time_idxs_.reset();
  ObMultiChildrenPhyOperator::reset();
}

void ObSubPlanFilter::reuse()
{
  rescan_params_.reuse();
  onetime_exprs_.reuse();
  init_plan_idxs_.reuse();
  one_time_idxs_.reuse();
  ObMultiChildrenPhyOperator::reuse();
}

int ObSubPlanFilter::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperator* child_op = NULL;
  ObSubPlanFilterCtx* subplan_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(subplan_ctx = GET_PHY_OPERATOR_CTX(ObSubPlanFilterCtx, ctx, get_id())) ||
      OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else { /*do nothing*/
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rescan_params_.count(); ++i) {
    int64_t idx = rescan_params_.at(i).second;
    plan_ctx->get_param_store_for_update().at(idx).set_null();
    LOG_DEBUG("prepare_rescan_params", K(ret), K(i), K(idx));
  }

  for (int32_t i = 1; OB_SUCC(ret) && i < get_child_num(); ++i) {
    if (OB_ISNULL(child_op = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get child", K(i), K(child_op), K(get_child_num()), K(ret));
    } else if (OB_FAIL(child_op->rescan(ctx))) {
      LOG_WARN("rescan child operator failed",
          K(ret),
          "op_type",
          ob_phy_operator_type_str(get_type()),
          "child op_type",
          ob_phy_operator_type_str(child_op->get_type()));
    } else { /*do nothing*/
    }
  }
  for (int32_t i = 1; OB_SUCC(ret) && i < get_child_num(); ++i) {
    ObSubPlanIterator* subplan_iter = NULL;
    subplan_iter = static_cast<ObSubPlanIterator*>(subplan_ctx->subplan_iters_.at(i - 1));
    if (OB_ISNULL(subplan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subplan_iter is null");
    } else if (init_plan_idxs_.has_member(i)) {
      subplan_iter->reuse();
      if (OB_FAIL(subplan_iter->prepare_init_plan())) {
        LOG_WARN("prepare init plan failed", K(ret), K(i));
      } else { /*do nothing*/
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prepare_onetime_exprs(ctx))) {
      LOG_WARN("prepare onetime exprs failed", K(ret));
    } else if (OB_ISNULL(child_op = get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get child", K(0), K(child_op), K(get_child_num()), K(ret));
    } else if (OB_FAIL(child_op->rescan(ctx))) {
      LOG_WARN("failed to do rescan", K(ret));
    } else {
      subplan_ctx->is_filtered_has_set_ = false;
      // subplan_ctx->op_monitor_info_.increase_value(RESCAN_TIMES);
    }
  }
  return ret;
}

int ObSubPlanFilter::switch_iterator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperator* child_op = NULL;
  ObSubPlanFilterCtx* subplan_ctx = NULL;
  if (OB_ISNULL(subplan_ctx = GET_PHY_OPERATOR_CTX(ObSubPlanFilterCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else if (OB_ISNULL(child_op = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("main query child operator is null", K(ret));
  } else if (OB_FAIL(child_op->switch_iterator(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("swtich child operator iterator failed", K(ret));
    }
  } else {
    subplan_ctx->is_filtered_has_set_ = false;
    ++subplan_ctx->expr_ctx_.cur_array_index_;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObSubPlanFilter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultiChildrenPhyOperator::serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize child phy operator failed", K(ret));
  } else {
    // serialize rescan_params_
    OB_UNIS_ENCODE(rescan_params_.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < rescan_params_.count(); ++i) {
      ObSqlExpression* sql_expr = rescan_params_.at(i).first;
      int64_t param_idx = rescan_params_.at(i).second;
      if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_expr is null");
      }
      OB_UNIS_ENCODE(*sql_expr);
      OB_UNIS_ENCODE(param_idx);
    }
    // serialize onetime_exprs_
    if (OB_SUCC(ret)) {
      OB_UNIS_ENCODE(onetime_exprs_.count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < onetime_exprs_.count(); ++i) {
      ObSqlExpression* sql_expr = onetime_exprs_.at(i).first;
      int64_t param_idx = onetime_exprs_.at(i).second;
      if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_expr is null");
      }
      OB_UNIS_ENCODE(*sql_expr);
      OB_UNIS_ENCODE(param_idx);
    }
    // serialize init_plan_idxs_ and one_time_idxs_
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_plan_idxs_.serialize(buf, buf_len, pos))) {
        LOG_WARN("serialize init plan idxs failed", K(ret));
      } else if (OB_FAIL(one_time_idxs_.serialize(buf, buf_len, pos))) {
        LOG_WARN("serialize one time idxs failed", K(ret));
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
    OB_UNIS_ENCODE(update_set_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObSubPlanFilter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultiChildrenPhyOperator::deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize child phy operator failed", K(ret));
  } else if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("my_phy_plan_ is null");
  } else {
    // deserialize rescan_params_
    int64_t param_count = 0;
    OB_UNIS_DECODE(param_count);
    if (OB_FAIL(init_rescan_param(param_count))) {
      LOG_WARN("fail to init rescan_param", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      ObSqlExpression* sql_expr = NULL;
      int64_t param_idx = OB_INVALID_INDEX;
      if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, sql_expr))) {
        LOG_WARN("make sql expression failed", K(ret));
      } else if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_expr is null");
      } else {
        OB_UNIS_DECODE(*sql_expr);
        OB_UNIS_DECODE(param_idx);
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(rescan_params_.push_back(std::pair<ObSqlExpression*, int64_t>(sql_expr, param_idx)))) {
          LOG_WARN("push back rescan params failed", K(ret));
        }
      }
    }
    // deserialize onetime_exprs_
    OB_UNIS_DECODE(param_count);
    if (OB_SUCC(ret) && OB_FAIL(init_onetime_exprs(param_count))) {
      LOG_WARN("fail to init onetime expr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      ObSqlExpression* sql_expr = NULL;
      int64_t param_idx = OB_INVALID_INDEX;
      if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, sql_expr))) {
        LOG_WARN("make sql expression failed", K(ret));
      } else if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_expr is null");
      } else {
        OB_UNIS_DECODE(*sql_expr);
        OB_UNIS_DECODE(param_idx);
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(onetime_exprs_.push_back(std::pair<ObSqlExpression*, int64_t>(sql_expr, param_idx)))) {
          LOG_WARN("push back rescan params failed", K(ret));
        }
      }
    }
    // serialize init_plan_idxs_ and one_time_idxs_
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_plan_idxs_.deserialize(buf, data_len, pos))) {
        LOG_WARN("serialize init plan idxs failed", K(ret));
      } else if (OB_FAIL(one_time_idxs_.deserialize(buf, data_len, pos))) {
        LOG_WARN("serialize init plan idxs failed", K(ret));
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
    OB_UNIS_DECODE(update_set_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSubPlanFilter)
{
  int64_t len = 0;
  len = ObMultiChildrenPhyOperator::get_serialize_size();
  OB_UNIS_ADD_LEN(rescan_params_.count());
  for (int64_t i = 0; i < rescan_params_.count(); ++i) {
    ObSqlExpression* sql_expr = rescan_params_.at(i).first;
    int64_t param_idx = rescan_params_.at(i).second;
    if (sql_expr != NULL) {
      OB_UNIS_ADD_LEN(*sql_expr);
      OB_UNIS_ADD_LEN(param_idx);
    }
  }
  OB_UNIS_ADD_LEN(onetime_exprs_.count());
  for (int64_t i = 0; i < onetime_exprs_.count(); ++i) {
    ObSqlExpression* sql_expr = onetime_exprs_.at(i).first;
    int64_t param_idx = onetime_exprs_.at(i).second;
    if (sql_expr != NULL) {
      OB_UNIS_ADD_LEN(*sql_expr);
      OB_UNIS_ADD_LEN(param_idx);
    }
  }
  len += init_plan_idxs_.get_serialize_size();
  len += one_time_idxs_.get_serialize_size();
  OB_UNIS_ADD_LEN(update_set_);
  return len;
}

int ObSubPlanFilter::open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperator* child_ptr = NULL;
  for (int32_t i = 1; OB_SUCC(ret) && i < get_child_num(); ++i) {
    if (OB_ISNULL(child_ptr = get_child(i))) {
      ret = OB_NOT_INIT;
      LOG_WARN("failed to get child", K(i), K(child_ptr), K(get_child_num()), K(ret));
    } else if (OB_FAIL(child_ptr->open(ctx))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("Open child operator failed", K(ret));
      }
    } else {
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(inner_open(ctx))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("Open this operator failed", K(ret));
      }
    } else if (OB_FAIL(handle_op_ctx(ctx))) {
      LOG_WARN("handle op ctx failed ", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(child_ptr = get_child(0))) {
      ret = OB_NOT_INIT;
      LOG_WARN("failed to get child", K(0), K(child_ptr), K(get_child_num()), K(ret));
    } else if (OB_FAIL(child_ptr->open(ctx))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("Open child operator failed", K(ret));
      }
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObSubPlanFilter::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObSubPlanFilterCtx* subplan_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init operator context failed", K(ret));
  } else if (OB_ISNULL(subplan_ctx = GET_PHY_OPERATOR_CTX(ObSubPlanFilterCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get session", K(ret), K(session));
  } else {
    void* ptr = NULL;
    ObSubPlanIterator* subplan_iter = NULL;
    for (int32_t i = 1; OB_SUCC(ret) && i < get_child_num(); ++i) {
      if (OB_UNLIKELY(NULL == (ptr = ctx.get_allocator().alloc(sizeof(ObSubPlanIterator))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc subplan iterator failed", "size", sizeof(ObSubPlanIterator));
      } else {
        subplan_iter = new (ptr) ObSubPlanIterator(ctx, *get_child(i));
        subplan_iter->set_tenant_id(session->get_effective_tenant_id());
        if (OB_FAIL(subplan_ctx->subplan_iters_.push_back(subplan_iter))) {
          LOG_WARN("push back subplan iter failed", K(ret), K(i));
          subplan_iter->~ObSubPlanIterator();
          subplan_iter = NULL;
        } else {
          if (init_plan_idxs_.has_member(i)) {
            subplan_iter->set_init_plan();
            if (OB_FAIL(subplan_iter->prepare_init_plan())) {
              LOG_WARN("prepare init plan failed", K(ret), K(i));
            } else { /*do nothing*/
            }
          } else if (one_time_idxs_.has_member(i)) {
            subplan_iter->set_onetime_plan();
          } else { /*do nothing*/
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prepare_onetime_exprs(ctx))) {
        LOG_WARN("prepare onetime exprs failed", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSubPlanFilter::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObSubPlanFilterCtx* subplan_ctx = NULL;
  if (OB_ISNULL(subplan_ctx = GET_PHY_OPERATOR_CTX(ObSubPlanFilterCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < subplan_ctx->subplan_iters_.count(); ++i) {
      ObNewRowIterator*& subplan_iter = subplan_ctx->subplan_iters_.at(i);
      if (subplan_iter != NULL) {
        subplan_iter->~ObNewRowIterator();
        subplan_iter = NULL;
      }
    }
  }
  return ret;
}

int ObSubPlanFilter::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObSubPlanFilterCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, update_set_ || need_copy_row_for_compute()))) {
    LOG_WARN("create current row failed", K(ret));
  }
  return ret;
}

int ObSubPlanFilter::wrap_expr_ctx(ObExecContext& exec_ctx, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObSubPlanFilterCtx* subplan_ctx = NULL;
  if (OB_FAIL(ObPhyOperator::wrap_expr_ctx(exec_ctx, expr_ctx))) {
    LOG_WARN("wrap_expr_ctx failed", K(ret));
  } else if (OB_ISNULL(subplan_ctx = GET_PHY_OPERATOR_CTX(ObSubPlanFilterCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    expr_ctx.subplan_iters_ = &(subplan_ctx->subplan_iters_);
  }
  return ret;
}

int ObSubPlanFilter::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObPhyOperator* child_op = NULL;
  ObSubPlanFilterCtx* subplan_ctx = NULL;
  if (OB_ISNULL(child_op = get_child(0))) {
    ret = OB_NOT_INIT;
    LOG_WARN("child operator is null");
  } else if (OB_FAIL(child_op->get_next_row(ctx, row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from child operator failed", K(ret));
    }
  } else if (OB_ISNULL(row)) {
    LOG_WARN("row is null");
  } else if (OB_FAIL(prepare_rescan_params(ctx, *row))) {
    LOG_WARN("prepare rescan params failed", K(ret));
  } else {
    ObNewRowIterator* subplan_iter = NULL;
    if (OB_ISNULL(subplan_ctx = GET_PHY_OPERATOR_CTX(ObSubPlanFilterCtx, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get physical operator context failed");
    }
    ObExecContext::ObPlanRestartGuard restart_plan(ctx);
    for (int32_t i = 1; OB_SUCC(ret) && i < get_child_num(); ++i) {
      if (one_time_idxs_.has_member(i)) {
      } else if (init_plan_idxs_.has_member(i)) {
        if (OB_ISNULL(subplan_iter = subplan_ctx->subplan_iters_.at(i - 1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subplan iter is null");
        } else {
          subplan_iter->reset();
        }
      } else if (OB_ISNULL(child_op = get_child(i))) {
        ret = OB_NOT_INIT;
        LOG_WARN("child operator is null", K(i));
      } else if (OB_FAIL(child_op->rescan(ctx))) {
        LOG_WARN("rescan child operator failed", K(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (update_set_) {
      OZ(handle_update_set(subplan_ctx, row));
    } else {
      OZ(copy_cur_row(*subplan_ctx, row));
    }
  }

  return ret;
}

int ObSubPlanFilter::prepare_rescan_params(ObExecContext& ctx, const ObNewRow& row) const
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context is null");
  } else if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
    LOG_WARN("wrap expr ctx failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rescan_params_.count(); ++i) {
      ObObjParam result;
      ObSqlExpression* sql_expr = rescan_params_.at(i).first;
      int64_t param_idx = rescan_params_.at(i).second;
      if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_expr is null");
      } else if (OB_FAIL(sql_expr->calc(expr_ctx, row, result))) {
        LOG_WARN("sql expr calc failed", K(ret), K(*sql_expr), K(row));
      } else {
        result.set_param_meta();
        plan_ctx->get_param_store_for_update().at(param_idx) = result;
      }
    }
  }
  return ret;
}

int ObSubPlanFilter::add_rescan_param(ObSqlExpression* sql_expr, int64_t param_idx)
{
  return rescan_params_.push_back(std::pair<ObSqlExpression*, int64_t>(sql_expr, param_idx));
}

int ObSubPlanFilter::construct_array_params(ObExecContext& ctx) const
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("not supported", K(ctx));
  return ret;
}

int ObSubPlanFilter::prepare_onetime_exprs(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context is null");
  } else if (OB_LIKELY(plan_ctx->get_bind_array_count() > 0)) {
    if (OB_FAIL(construct_array_params(ctx))) {
      LOG_WARN("construct array params failed", K(ret));
    }
  } else if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
    LOG_WARN("wrap expr ctx failed", K(ret));
  } else {
    ObNewRow row;
    for (int64_t i = 0; OB_SUCC(ret) && i < onetime_exprs_.count(); ++i) {
      ObObj tmp;
      ObObjParam result;
      ObSqlExpression* sql_expr = onetime_exprs_.at(i).first;
      int64_t param_idx = onetime_exprs_.at(i).second;
      if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_expr is null");
      } else if (OB_FAIL(sql_expr->calc(expr_ctx, row, tmp))) {
        LOG_WARN("sql expr calc failed", K(ret), K(*sql_expr), K(row));
      } else if (OB_FAIL(ob_write_obj(ctx.get_allocator(), tmp, result))) {
        LOG_WARN("deep copy obj failed", K(ret));
      } else {
        result.set_param_meta();
        plan_ctx->get_param_store_for_update().at(param_idx) = result;
      }
    }
  }
  return ret;
}

int ObSubPlanFilter::add_onetime_expr(ObSqlExpression* sql_expr, int64_t param_idx)
{
  return onetime_exprs_.push_back(std::pair<ObSqlExpression*, int64_t>(sql_expr, param_idx));
}

int64_t ObSubPlanFilter::to_string_kv(char* buf, int64_t buf_len) const
{
  int64_t pos = 0;
  // rescan_params_
  J_NAME(N_RESCAN_PARAM);
  J_COLON();
  J_ARRAY_START();
  for (int64_t i = 0; i < rescan_params_.count() - 1; ++i) {
    int64_t index = rescan_params_.at(i).second;
    ObSqlExpression* expr = rescan_params_.at(i).first;
    if (expr != NULL) {
      J_OBJ_START();
      J_KV(K(index), N_EXPR, expr);
      J_OBJ_END();
      J_COMMA();
    }
  }
  if (rescan_params_.count() > 0) {
    int64_t index = rescan_params_.at(rescan_params_.count() - 1).second;
    ObSqlExpression* expr = rescan_params_.at(rescan_params_.count() - 1).first;
    if (expr != NULL) {
      J_OBJ_START();
      J_KV(K(index), N_EXPR, expr);
      J_OBJ_END();
    }
  }
  J_ARRAY_END();
  J_COMMA();
  // onetime_exprs_
  J_NAME(N_ONETIME_FILTER);
  J_COLON();
  J_ARRAY_START();
  for (int64_t i = 0; i < onetime_exprs_.count() - 1; ++i) {
    int64_t index = onetime_exprs_.at(i).second;
    ObSqlExpression* expr = onetime_exprs_.at(i).first;
    if (expr != NULL) {
      J_OBJ_START();
      J_KV(K(index), N_EXPR, expr);
      J_OBJ_END();
      J_COMMA();
    }
  }
  if (onetime_exprs_.count() > 0) {
    int64_t index = onetime_exprs_.at(onetime_exprs_.count() - 1).second;
    ObSqlExpression* expr = onetime_exprs_.at(onetime_exprs_.count() - 1).first;
    if (expr != NULL) {
      J_OBJ_START();
      J_KV(K(index), N_EXPR, expr);
      J_OBJ_END();
    }
  }
  J_ARRAY_END();
  J_COMMA();
  // init_plan_idxs_ and onetime_idxs_
  J_KV(K_(init_plan_idxs), K_(one_time_idxs));
  return pos;
}

int ObSubPlanFilter::handle_update_set(ObSubPlanFilterCtx* subplan_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row) || OB_ISNULL(subplan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null unexpected", K(ret));
  } else {
    bool no_row = false;
    ObNewRow* subquery_row = NULL;
    ObNewRowIterator* row_iter = nullptr;
    ObSEArray<ObNewRowIterator*, 16>& row_iters = subplan_ctx->subplan_iters_;
    if (1 != row_iters.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("too many subplan unexpected", K(ret), K(row_iters.count()));
    } else {
      row_iter = row_iters.at(0);
      if (OB_ISNULL(row_iter) || OB_FAIL(row_iter->get_next_row(subquery_row))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          no_row = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObNewRow& cur_row = subplan_ctx->cur_row_;
      int64_t real_count = row->get_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < real_count; ++i) {
        int64_t real_idx = row->projector_size_ > 0 ? row->projector_[i] : i;
        if (OB_UNLIKELY(real_idx >= row->count_ || real_idx >= cur_row.count_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid row count", K(real_idx), K_(row->count), K_(cur_row.count));
        } else {
          cur_row.cells_[real_idx] = row->cells_[real_idx];
        }
      }
      if (OB_SUCC(ret)) {
        if (!no_row) {
          for (int64_t i = 0; OB_SUCC(ret) && i < subquery_row->get_count(); i++) {
            // subquery row's memory will be release after asign the obj to current row
            // so need to call ob_write_obj to deep copy this obj at here
            if (OB_UNLIKELY(row->count_ + i >= cur_row.count_)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid row count", K(row->count_ + i), K_(cur_row.count), K(ret));
            } else if (OB_FAIL(ob_write_obj(*subplan_ctx->expr_ctx_.calc_buf_,
                           subquery_row->get_cell(i),
                           cur_row.cells_[row->count_ + i]))) {
              LOG_WARN("write obj failed", K(ret));
            }
          }
        } else {
          ObObj null_obj;
          for (int64_t i = row->count_; i < cur_row.count_; i++) {
            cur_row.cells_[i] = null_obj;
          }
        }
        row = &cur_row;
      }
    }
    if (OB_SUCC(ret)) {
      ObNewRow* tmp_row = NULL;
      if (OB_UNLIKELY(OB_SUCCESS == (ret = row_iter->get_next_row(tmp_row)))) {
        ret = OB_ERR_MORE_THAN_ONE_ROW;
        LOG_WARN("subquery too many rows", K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
