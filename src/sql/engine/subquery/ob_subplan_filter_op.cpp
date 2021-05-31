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

#include "ob_subplan_filter_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObSubQueryIterator::ObSubQueryIterator(ObOperator& op)
    : op_(op), onetime_plan_(false), init_plan_(false), inited_(false), iterated_(false)
{}

int ObSubQueryIterator::start()
{
  int ret = OB_SUCCESS;
  if (iterated_) {
    const bool reset_onetime_plan = true;
    reset(reset_onetime_plan);
  }
  return ret;
}

int ObSubQueryIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  iterated_ = true;
  if (init_plan_ && inited_) {
    ret = store_it_.get_next_row(get_output(), op_.get_eval_ctx());
  } else {
    ret = op_.get_next_row();
  }
  return ret;
}

void ObSubQueryIterator::reset(const bool reset_onetime_plan /* = false */)
{
  int ret = OB_SUCCESS;
  if (onetime_plan_ && !reset_onetime_plan) {
    // for onetime expr
  } else if (init_plan_) {
    // for init plan
    if (OB_FAIL(store_.begin(store_it_, ObChunkDatumStore::BLOCK_SIZE))) {
      BACKTRACE(ERROR, true, "failed to rewind iterator");
    }
  } else {
    ObExecContext::ObPlanRestartGuard restart_plan(op_.get_exec_ctx());
    if (OB_FAIL(op_.rescan())) {
      BACKTRACE(ERROR, true, "failed to do rescan");
    }
  }
  iterated_ = false;
}

void ObSubQueryIterator::reuse()
{
  inited_ = false;
  iterated_ = false;
  store_it_.reset();
  store_.reset();
}

int ObSubQueryIterator::prepare_init_plan()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    if (!store_.is_inited()) {
      // TODO : use auto memory management
      OZ(store_.init(1L << 20,  // 1MB memory limit
          GET_MY_SESSION(op_.get_exec_ctx())->get_effective_tenant_id()));
    }
    while (OB_SUCC(ret) && OB_SUCC(get_next_row())) {
      OZ(store_.add_row(get_output(), &op_.get_eval_ctx()));
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      inited_ = true;
      iterated_ = false;
      OZ(store_it_.init(&store_, ObChunkDatumStore::BLOCK_SIZE));
    }
  }
  return ret;
}

ObSubPlanFilterSpec::ObSubPlanFilterSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      rescan_params_(alloc),
      onetime_exprs_(alloc),
      init_plan_idxs_(ModulePageAllocator(alloc)),
      one_time_idxs_(ModulePageAllocator(alloc)),
      update_set_(alloc)

{}

OB_SERIALIZE_MEMBER(
    (ObSubPlanFilterSpec, ObOpSpec), rescan_params_, onetime_exprs_, init_plan_idxs_, one_time_idxs_, update_set_);

DEF_TO_STRING(ObSubPlanFilterSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(rescan_params), K_(onetime_exprs), K_(init_plan_idxs), K_(one_time_idxs), K_(update_set));
  J_OBJ_END();
  return pos;
}

ObSubPlanFilterOp::ObSubPlanFilterOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input), update_set_mem_(NULL)
{}

ObSubPlanFilterOp::~ObSubPlanFilterOp()
{
  destroy_subplan_iters();
  destroy_update_set_mem();
}

void ObSubPlanFilterOp::destroy_subplan_iters()
{
  FOREACH_CNT(it, subplan_iters_)
  {
    if (NULL != *it) {
      (*it)->~Iterator();
      *it = NULL;
    }
  }
  subplan_iters_.reset();
}

void ObSubPlanFilterOp::destroy()
{
  destroy_subplan_iters();
  destroy_update_set_mem();
  ObOperator::destroy();
}

int ObSubPlanFilterOp::set_param_null()
{
  int ret = OB_SUCCESS;
  ObDatum null_datum;
  null_datum.set_null();
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.rescan_params_.count(); ++i) {
    OZ(MY_SPEC.rescan_params_.at(i).update_dynamic_param(eval_ctx_, null_datum));
    LOG_DEBUG("prepare_rescan_params", K(ret), K(i));
  }
  return ret;
}

int ObSubPlanFilterOp::rescan()
{
  int ret = OB_SUCCESS;

  clear_evaluated_flag();
  if (OB_FAIL(set_param_null())) {
    LOG_WARN("failed to set param null", K(ret));
  }

  for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
    if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("rescan child operator failed", K(ret), "op", op_name(), "child", children_[i]->op_name());
    }
  }

  for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
    Iterator* iter = subplan_iters_.at(i - 1);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subplan_iter is null", K(ret));
    } else if (MY_SPEC.init_plan_idxs_.has_member(i)) {
      iter->reuse();
      if (OB_FAIL(iter->prepare_init_plan())) {
        LOG_WARN("prepare init plan failed", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prepare_onetime_exprs())) {
      LOG_WARN("prepare onetime exprs failed", K(ret));
    } else if (OB_FAIL(child_->rescan())) {
      LOG_WARN("failed to do rescan", K(ret));
    } else {
      startup_passed_ = spec_.startup_filters_.empty();
    }
  }
  return ret;
}

int ObSubPlanFilterOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_->switch_iterator())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("swtich child operator iterator failed", K(ret));
    }
  } else {
    startup_passed_ = spec_.startup_filters_.empty();
  }
  return ret;
}

int ObSubPlanFilterOp::open()
{
  int ret = OB_SUCCESS;
  opened_ = true;
  CK(child_cnt_ >= 2);
  // reset param:
  if (OB_FAIL(set_param_null())) {
    LOG_WARN("failed to set param null", K(ret));
  }
  for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_NOT_INIT;
      LOG_WARN("failed to get child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->open())) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("Open child operator failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_evaluated_flags())) {
      LOG_WARN("init evaluated flags failed", K(ret));
    } else if (OB_FAIL(inner_open())) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("Open this operator failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(child_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("failed to get child", K(ret));
    } else if (OB_FAIL(child_->open())) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("Open child operator failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSubPlanFilterOp::inner_open()
{
  int ret = OB_SUCCESS;
  CK(child_cnt_ >= 2);
  if (OB_SUCC(ret)) {
    // generate row iterator for each subquery
    OZ(subplan_iters_.prepare_allocate(child_cnt_ - 1));
    for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
      void* ptr = ctx_.get_allocator().alloc(sizeof(Iterator));
      Iterator*& iter = subplan_iters_.at(i - 1);
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc subplan iterator failed", K(ret), "size", sizeof(Iterator));
      } else {
        iter = new (ptr) Iterator(*children_[i]);
        if (MY_SPEC.init_plan_idxs_.has_member(i)) {
          iter->set_init_plan();
          OZ(iter->prepare_init_plan());
        } else if (MY_SPEC.one_time_idxs_.has_member(i)) {
          iter->set_onetime_plan();
        }
      }
    }
    OZ(prepare_onetime_exprs());
  }
  return ret;
}

int ObSubPlanFilterOp::inner_close()
{
  destroy_subplan_iters();
  destroy_update_set_mem();
  return OB_SUCCESS;
}

int ObSubPlanFilterOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from child operator failed", K(ret));
    }
  } else if (OB_FAIL(prepare_rescan_params())) {
    LOG_WARN("prepare rescan params failed", K(ret));
  } else {
    ObExecContext::ObPlanRestartGuard restart_plan(ctx_);
    for (int32_t i = 1; OB_SUCC(ret) && i < child_cnt_; ++i) {
      //// rescan for each subquery
      if (MY_SPEC.one_time_idxs_.has_member(i)) {
        // need no rescan, skip
      } else if (MY_SPEC.init_plan_idxs_.has_member(i)) {
        Iterator* iter = subplan_iters_.at(i - 1);
        if (OB_ISNULL(iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subplan iter is null");
        } else {
          iter->reset();
        }
      } else if (OB_FAIL(children_[i]->rescan())) {
        LOG_WARN("rescan child operator failed", K(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!MY_SPEC.update_set_.empty()) {
      OZ(handle_update_set());
    }
  }
  return ret;
}

int ObSubPlanFilterOp::prepare_rescan_params()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.rescan_params_.count(); ++i) {
    OZ(MY_SPEC.rescan_params_.at(i).set_dynamic_param(eval_ctx_));
  }
  return ret;
}

// int ObSubPlanFilter::construct_array_params(ObExecContext &ctx) const
// {
//   int ret = OB_SUCCESS;
//   ObExprCtx expr_ctx;
//   ObPhysicalPlanCtx *plan_ctx = NULL;
//   if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("physical plan context is null");
//   } else if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
//     LOG_WARN("wrap expr ctx failed", K(ret));
//   } else if (OB_UNLIKELY(onetime_exprs_.count() != get_child_num() - 1)) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("onetime exprs isn't match with child operator count",
//              K(ret), K(onetime_exprs_.count()), K(get_child_num()));
//   } else {
//     ObNewRow empty_row;
//     ObObj tmp_obj;
//     int64_t array_binding_count = plan_ctx->get_bind_array_count();
//     ObIAllocator &allocator = ctx.get_allocator();
//     for (int64_t i = 0; OB_SUCC(ret) && i < onetime_exprs_.count(); ++i) {
//       ObSqlExpression *sql_expr = onetime_exprs_.at(i).first;
//       int64_t param_idx = onetime_exprs_.at(i).second;
//       pl::ObPLNestedTable *binding_array = NULL;
//       void *ba_buf = NULL;
//       void *data_buf = NULL;
//       if (OB_ISNULL(sql_expr)) {
//         ret = OB_ERR_UNEXPECTED;
//         LOG_WARN("sql_expr is null");
//       } else {
//         int64_t ba_buf_size = static_cast<int64_t>(sizeof(pl::ObPLNestedTable));
//         int64_t data_buf_size = static_cast<int64_t>(sizeof(ObObj) * array_binding_count);
//         ObObj tmp_obj;
//         if (OB_ISNULL(ba_buf = allocator.alloc(ba_buf_size))) {
//           ret = OB_ALLOCATE_MEMORY_FAILED;
//           LOG_WARN("allocate nested table failed", K(ret), K(ba_buf_size));
//         } else if (OB_ISNULL(data_buf = allocator.alloc(data_buf_size))) {
//           ret = OB_ALLOCATE_MEMORY_FAILED;
//           LOG_WARN("allocate data buffer failed", K(ret), K(data_buf_size));
//         } else {
//           binding_array = new(ba_buf) pl::ObPLNestedTable();
//           binding_array->set_data(data_buf);
//           binding_array->set_count(array_binding_count);
//         }
//       }
//       expr_ctx.cur_array_index_ = 0;
//       while (OB_SUCC(ret)) {
//         tmp_obj.reset();
//         if (OB_FAIL(sql_expr->calc(expr_ctx, empty_row, tmp_obj))) {
//           LOG_WARN("calc sql expression failed", K(ret));
//         } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj,
//         static_cast<ObObj*>(data_buf)[expr_ctx.cur_array_index_]))) {
//           LOG_WARN("write obj failed", K(ret), K(tmp_obj));
//         } else if (OB_FAIL(get_child(static_cast<int32_t>(i + 1))->switch_iterator(ctx))) {
//           if (OB_ITER_END != ret) {
//             LOG_WARN("swtich child operator iterator failed", K(ret));
//           }
//         } else {
//           ++expr_ctx.cur_array_index_;
//         }
//       }
//       if (OB_ITER_END == ret) {
//         ret = OB_SUCCESS;
//         if (OB_UNLIKELY(0 == expr_ctx.cur_array_index_)) {
//           //subquery does not contain bind array param
//           plan_ctx->get_param_store_for_update().at(param_idx) =
//           static_cast<ObObj*>(data_buf)[expr_ctx.cur_array_index_];
//         } else if (OB_UNLIKELY(expr_ctx.cur_array_index_ != plan_ctx->get_bind_array_count() - 1)) {
//           ret = OB_ERR_UNEXPECTED;
//           LOG_WARN("current array index is invalid", K(ret), K_(expr_ctx.cur_array_index),
//           K(plan_ctx->get_bind_array_count()));
//         } else {
//           plan_ctx->get_param_store_for_update().at(param_idx).set_ext(reinterpret_cast<int64_t>(binding_array));
//         }
//       }
//     }
//   }
//   return ret;
// }

int ObSubPlanFilterOp::prepare_onetime_exprs()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_LIKELY(plan_ctx->get_bind_array_count() > 0)) {
    // TODO : support array binding
    // if (OB_FAIL(construct_array_params(ctx))) {
    //   LOG_WARN("construct array params failed", K(ret));
    // }
  } else {
    ObDatum copyed;
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.onetime_exprs_.count(); ++i) {
      const ObDynamicParamSetter& setter = MY_SPEC.onetime_exprs_.at(i);
      ObDatum* datum = NULL;
      if (OB_FAIL(setter.src_->eval(eval_ctx_, datum))) {
        LOG_WARN("expression evaluate failed", K(ret));
      } else if (OB_FAIL(copyed.deep_copy(*datum, ctx_.get_allocator()))) {
        LOG_WARN("datum deep copy failed", K(ret));
      } else if (OB_FAIL(setter.update_dynamic_param(eval_ctx_, copyed))) {
        LOG_WARN("update dynamic param store failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSubPlanFilterOp::handle_update_set()
{
  int ret = OB_SUCCESS;
  bool no_row = false;
  const int64_t extra_size = 0;
  if (NULL == update_set_mem_) {
    lib::ContextParam param;
    param.set_mem_attr(ctx_.get_my_session()->get_effective_tenant_id(), ObModIds::OB_SQL_EXECUTOR, ObCtxIds::WORK_AREA)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(update_set_mem_, param))) {
      LOG_WARN("create memory entity failed", K(ret));
    }
  } else {
    update_set_mem_->get_arena_allocator().reuse();
  }

  if (OB_FAIL(ret)) {
  } else if (1 != subplan_iters_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too many subplan unexpected", K(ret), K(subplan_iters_.count()));
  } else {
    ObChunkDatumStore::LastStoredRow<> row_val(update_set_mem_->get_arena_allocator());
    Iterator* iter = subplan_iters_.at(0);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null iterator", K(ret));
    } else if (OB_FAIL(iter->get_next_row())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        no_row = true;
      }
    } else if (OB_FAIL(row_val.save_store_row(MY_SPEC.update_set_, eval_ctx_, extra_size))) {
      LOG_WARN("deep copy row failed", K(ret));
    } else {
      if (OB_UNLIKELY(OB_SUCCESS == (ret = iter->get_next_row()))) {
        ret = OB_ERR_MORE_THAN_ONE_ROW;
        LOG_WARN("subquery too many rows", K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(ret)) {
      if (no_row) {
        FOREACH_CNT(expr, MY_SPEC.update_set_)
        {
          (*expr)->locate_expr_datum(eval_ctx_).set_null();
          (*expr)->get_eval_info(eval_ctx_).evaluated_ = true;
        }
      } else {
        OZ(row_val.store_row_->to_expr(MY_SPEC.update_set_, eval_ctx_));
      }
    }
  }

  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
