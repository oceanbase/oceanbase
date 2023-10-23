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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_execute_result.h"
#include "lib/ash/ob_active_session_guard.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

int ObExecuteResult::open(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ret = open();
  return ret;
}

int ObExecuteResult::get_next_row(ObExecContext &ctx, const common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = &row_;
  const ObOpSpec &spec = static_engine_root_->get_spec();
  if (spec.output_.count() > 0 && NULL == row_.cells_) {
    if (OB_ISNULL(row_.cells_ = static_cast<ObObj *>(
                ctx.get_allocator().alloc(sizeof(ObObj) * spec.output_.count())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      for (int64_t i = 0; i < spec.output_.count(); i++) {
        new (&row_.cells_[i]) ObObj();
      }
      row_.count_ = spec.output_.count();
      row_.projector_size_ = 0;
      row_.projector_ = NULL;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!spec.is_vectorized()) {
    ret = get_next_row();
    // convert datum to obj
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < spec.output_.count(); i++) {
        ObDatum *datum = NULL;
        ObExpr *expr = spec.output_.at(i);
        if (OB_FAIL(expr->eval(static_engine_root_->get_eval_ctx(), datum))) {
          LOG_WARN("expr evaluate failed", K(ret));
        } else if (OB_FAIL(datum->to_obj(
                    row_.cells_[i], expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        }
      }
    }
  } else {
    ret = br_it_.get_next_row();
    if (OB_SUCC(ret)) {
      const int64_t idx = br_it_.cur_idx();
      for (int64_t i = 0; OB_SUCC(ret) && i < spec.output_.count(); i++) {
        ObExpr *expr = spec.output_.at(i);
        // expressions are evaluated in get_next_batch(), get datum value directly
        const ObDatum *datum = expr->locate_batch_datums(
            static_engine_root_->get_eval_ctx()) + (expr->is_batch_result() ? idx : 0);
        if (OB_FAIL(datum->to_obj(
                    row_.cells_[i], expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExecuteResult::close(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ret = close();
  return ret;
}

int ObExecuteResult::open() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(static_engine_root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(static_engine_root_->open())) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("open operator failed", K(ret));
    }
  } else if (!static_engine_root_->get_spec().plan_->var_init_exprs_.empty()) {
    // Evaluate the var init expr in generated table, This is to be compatible with some of mysql's uses of variables
    // Such as "select c1,(@rownum:= @rownum+1) as CCBH from t1,(SELECT@rownum:=0) B"
    const ExprFixedArray &var_init_exprs = static_engine_root_->get_spec().plan_->var_init_exprs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < var_init_exprs.count(); i++) {
      ObDatum *datum = NULL;
      ObExpr *expr = var_init_exprs.at(i);
      if (OB_FAIL(expr->eval(static_engine_root_->get_eval_ctx(), datum))) {
        LOG_WARN("expr evaluate failed", K(ret));
      }
    }
  }
  return ret;
}

int ObExecuteResult::get_next_row() const
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  if (OB_ISNULL(static_engine_root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(static_engine_root_));
  }
  // switch bind array iterator in DML returning plan
  while (OB_SUCC(ret) && !got_row) {
    if (OB_FAIL(static_engine_root_->get_next_row())) {
      if (OB_ITER_END == ret) {
        ObPhysicalPlanCtx *plan_ctx = static_engine_root_->get_exec_ctx().get_physical_plan_ctx();
        if (plan_ctx->get_bind_array_count() <= 0
            || plan_ctx->get_bind_array_idx() >= plan_ctx->get_bind_array_count()) {
          // no bind array or reach binding array end, do nothing
        } else {
          plan_ctx->inc_bind_array_idx();
          if (OB_FAIL(static_engine_root_->switch_iterator())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("switch op iterator failed",
                       K(ret), "op_type", static_engine_root_->op_name());
            }
          }
        }
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("get next row from operator failed", K(ret));
      }
    } else {
      got_row = true;
    }
  }
  return ret;
}

int ObExecuteResult::close() const
{
  int ret = OB_SUCCESS;
  if (NULL != static_engine_root_) {
    if (OB_FAIL(static_engine_root_->close())) {
      LOG_WARN("close failed", K(ret));
    }
  }
  return ret;
}

ObAsyncExecuteResult::ObAsyncExecuteResult()
  : field_count_(0),
    scanner_(nullptr),
    cur_row_(nullptr),
    spec_(nullptr)
{
}

int ObAsyncExecuteResult::open(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  ObSQLSessionInfo *session = ctx.get_my_session();
  if (OB_ISNULL(scanner_) || OB_ISNULL(plan_ctx) || OB_ISNULL(session)) {
    ret = OB_NOT_INIT;
    LOG_WARN("scanner is invalid", K(ret), K(scanner_), K(plan_ctx), K(session));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::merge_task_result_meta(*plan_ctx, *scanner_))) {
    LOG_WARN("merge task result meta failed", K(ret), KPC_(scanner));
  } else if (OB_FAIL(session->replace_user_variables(ctx, scanner_->get_session_var_map()))) {
    LOG_WARN("replace user variables failed", K(ret));
  } else if (field_count_ <= 0) {
    //远端没有数据返回，因此不需要创建row buffer
  } else if (OB_FAIL(ob_create_row(ctx.get_allocator(), field_count_, cur_row_))) {
    LOG_WARN("create current row failed", K(ret), K(field_count_));
  } else {
    if (nullptr == spec_) {
      row_iter_ = scanner_->begin();
    } else {
      if (OB_FAIL(scanner_->get_datum_store().begin(datum_iter_))) {
        LOG_WARN("fail to init datum iter", K(ret));
      }
    }
  }
  return ret;
}

int ObAsyncExecuteResult::get_next_row(ObExecContext &ctx, const ObNewRow *&row)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (field_count_ <= 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(cur_row_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("scanner is invalid", K(ret));
  } else if (nullptr == spec_) {
    if (OB_FAIL(row_iter_.get_next_row(*cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from row iterator failed", K(ret));
      }
    }
  } else {
    // Static engine.
    // For async execute result, ObExecContext::eval_ctx_ is destroyed, can not be used.
    const ObChunkDatumStore::StoredRow *sr = NULL;
    if (OB_FAIL(datum_iter_.get_next_row(sr))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from datum iterator failed", K(ret));
      }
    } else if (OB_ISNULL(sr) || spec_->output_.count() != sr->cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("store row is NULL or datum count mismatch",
               K(ret), KP(sr), K(spec_->output_.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < spec_->output_.count(); i++) {
        const sql::ObExpr *e = spec_->output_.at(i);
        if (OB_FAIL(sr->cells()[i].to_obj(cur_row_->cells_[i],
                                          e->obj_meta_,
                                          e->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = cur_row_;
  }
  return ret;
}

int ObAsyncExecuteResult::close(ObExecContext &ctx)
{
  UNUSED(ctx);
  return common::OB_SUCCESS;
}

}/* ns sql*/
}/* ns oceanbase */
