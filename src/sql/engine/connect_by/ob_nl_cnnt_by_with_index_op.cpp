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

#include "sql/engine/connect_by/ob_nl_cnnt_by_with_index_op.h"
#include "common/rowkey/ob_rowkey.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/basic/ob_material_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObNLConnectByWithIndexSpec, ObNLConnectBySpecBase));

ObNLConnectByWithIndexOp::ObNLConnectByWithIndexOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObNLConnectByOpBase(exec_ctx, spec, input),
    connect_by_pump_(),
    state_(CNTB_STATE_READ_LEFT),
    root_row_(NULL),
    cur_output_row_(NULL),
    is_match_(false),
    is_cycle_(false),
    is_inited_(false),
    need_return_(false),
    mem_context_(NULL),
    connect_by_root_row_(NULL)
{
  state_operation_func_[CNTB_STATE_JOIN_END] = &ObNLConnectByWithIndexOp::join_end_operate;
  state_function_func_[CNTB_STATE_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[CNTB_STATE_JOIN_END][FT_ITER_END] = &ObNLConnectByWithIndexOp::join_end_func_end;

  state_operation_func_[CNTB_STATE_READ_OUTPUT] = &ObNLConnectByWithIndexOp::read_output_operate;
  state_function_func_[CNTB_STATE_READ_OUTPUT][FT_ITER_GOING] =  &ObNLConnectByWithIndexOp::read_output_func_going;
  state_function_func_[CNTB_STATE_READ_OUTPUT][FT_ITER_END] = &ObNLConnectByWithIndexOp::read_output_func_end;

  state_operation_func_[CNTB_STATE_READ_PUMP] = &ObNLConnectByWithIndexOp::read_pump_operate;
  state_function_func_[CNTB_STATE_READ_PUMP][FT_ITER_GOING] = &ObNLConnectByWithIndexOp::read_pump_func_going;
  state_function_func_[CNTB_STATE_READ_PUMP][FT_ITER_END] = &ObNLConnectByWithIndexOp::read_pump_func_end;

  state_operation_func_[CNTB_STATE_READ_LEFT] = &ObNLConnectByWithIndexOp::read_left_operate;
  state_function_func_[CNTB_STATE_READ_LEFT][FT_ITER_GOING] = &ObNLConnectByWithIndexOp::read_left_func_going;
  state_function_func_[CNTB_STATE_READ_LEFT][FT_ITER_END] = &ObNLConnectByWithIndexOp::read_left_func_end;

  state_operation_func_[CNTB_STATE_READ_RIGHT] = &ObNLConnectByWithIndexOp::read_right_operate;
  state_function_func_[CNTB_STATE_READ_RIGHT][FT_ITER_GOING] = &ObNLConnectByWithIndexOp::read_right_func_going;
  state_function_func_[CNTB_STATE_READ_RIGHT][FT_ITER_END] = &ObNLConnectByWithIndexOp::read_right_func_end;
}

ObNLConnectByWithIndexOp::~ObNLConnectByWithIndexOp()
{
  destroy();
}

void ObNLConnectByWithIndexOp::destroy()
{
  connect_by_pump_.~ObConnectByOpBFSPump();//must be call
  ObOperator::destroy();
}

int ObNLConnectByWithIndexOp::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(connect_by_pump_.init(
      *const_cast<ObNLConnectByWithIndexSpec*>(&MY_SPEC), *this, eval_ctx_))) {
    LOG_WARN("fail to init Connect by Ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObNLConnectByWithIndexOp::reset()
{
  state_ = CNTB_STATE_READ_LEFT;
  connect_by_pump_.reset();
  is_match_ = false;
  is_cycle_ = false;
  root_row_ = NULL;
  cur_output_row_ = NULL;
  is_inited_ = false;
  sys_connect_by_path_id_ = INT64_MAX;
  need_return_ = false;
  if (OB_NOT_NULL(connect_by_root_row_)) {
    mem_context_->get_malloc_allocator().free(connect_by_root_row_);
    connect_by_root_row_ = NULL;
  }
}

int ObNLConnectByWithIndexOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_FAIL(init())) {
    LOG_WARN("fail to init Connect by Ctx", K(ret));
  } else if (MY_SPEC.left_prior_exprs_.count() != MY_SPEC.right_prior_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: exprs is not match",
      "left prior exprs", MY_SPEC.left_prior_exprs_.count(),
      "right prior exprs", MY_SPEC.right_prior_exprs_.count());
  } else if (MY_SPEC.cmp_funcs_.count() != MY_SPEC.connect_by_prior_exprs_.count()
      && MY_SPEC.cmp_funcs_.count() != MY_SPEC.left_prior_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: cmp func is not match with prior exprs", K(ret));
  } else {
    lib::ContextParam param;
    int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    param.set_mem_attr(tenant_id, ObModIds::OB_CONNECT_BY_PUMP, ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    } else {
      ObIAllocator &alloc = mem_context_->get_malloc_allocator();
      connect_by_pump_.set_allocator(alloc);
      connect_by_pump_.pump_stack_.set_block_allocator(ModulePageAllocator(alloc, "CnntArrays"));
      connect_by_pump_.path_stack_.set_block_allocator(ModulePageAllocator(alloc, "CnntArrays"));
      connect_by_pump_.sort_stack_.set_block_allocator(ModulePageAllocator(alloc, "CnntArrays"));
    }
  }
  return ret;
}


int ObNLConnectByWithIndexOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

int ObNLConnectByWithIndexOp::inner_close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

int ObNLConnectByWithIndexOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  state_operation_func_type state_operation = NULL;
  state_function_func_type state_function = NULL;
  int func = -1;
  ObCnntByOpState &state = state_;
  need_return_ = false;
  clear_evaluated_flag();
  while (OB_SUCC(ret) && !need_return_) {
    state_operation = state_operation_func_[state];
    if (OB_ISNULL(state_operation)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("state operation is null ", K(ret));
    } else if (OB_ITER_END == (ret = (this->*state_operation)())) {
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
      } else if (OB_FAIL((this->*state_function)()) && OB_ITER_END != ret) {
        LOG_WARN("failed state function", K(ret), K(state), K(func));
      }
    }
  } // while
  connect_by_pump_.cur_level_--;
  if (OB_SUCC(ret)) {
    LOG_DEBUG("debug get next", K(ObToStringExprRow(eval_ctx_, MY_SPEC.right_prior_exprs_)),
        K(ObToStringExprRow(eval_ctx_, MY_SPEC.cur_row_exprs_)));
  }
  return ret;
}

int ObNLConnectByWithIndexOp::read_output_operate()
{
  int ret = OB_SUCCESS;
  if (NULL == cur_output_row_) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObNLConnectByWithIndexOp::read_output_func_going()
{
  int ret = OB_SUCCESS;
  // 先构造ObExpr
  if (OB_FAIL(to_expr())) {
    LOG_WARN("fail to convert expr", K(ret));
  } else if (OB_FAIL(add_pseudo_column(MY_SPEC.is_leaf_expr_, CONNECT_BY_ISLEAF))) {
    LOG_WARN("fail to add is_leaf pseudo column", K(ret));
  } else if (OB_FAIL(add_pseudo_column(MY_SPEC.is_cycle_expr_, CONNECT_BY_ISCYCLE))) {
    LOG_WARN("fail to add pseudo column", K(ret));
  } else if (OB_FAIL(calc_connect_by_root_exprs(connect_by_pump_.is_root_node()))) {
    LOG_WARN("fail to calc connect_by_root_exprs", K(ret));
  } else if (OB_FAIL(calc_sys_connect_by_path())) {
    LOG_WARN("Failed to calc sys connect by path", K(ret));
  } else {
    cur_output_row_ = nullptr;
    root_row_ = nullptr;
    is_match_ = false;
    is_cycle_ = false;
    need_return_ = true;
    state_ = CNTB_STATE_READ_PUMP;
    LOG_DEBUG("connect by output going", K(ret));
  }
  return ret;
}

int ObNLConnectByWithIndexOp::read_output_func_end()
{
  int ret = OB_SUCCESS;
  state_ = CNTB_STATE_READ_PUMP;
  return ret;
}

// restore prior row to calc other and so on
int ObNLConnectByWithIndexOp::restore_prior_expr()
{
  int ret = OB_SUCCESS;
  if (nullptr != root_row_) {
    if (OB_FAIL(root_row_->to_expr(MY_SPEC.left_prior_exprs_, eval_ctx_))) {
      LOG_WARN("failed to to expr from prior row", K(ret));
    }
  }
  return ret;
}

// with index 场景下无法保证 connect_by_root_exprs_ 依赖的 datum 一只有效，提前缓存。
int ObNLConnectByWithIndexOp::calc_connect_by_root_exprs(bool is_root)
{
  int ret = OB_SUCCESS;
  const ObNLConnectBySpecBase &spec = static_cast<const ObNLConnectBySpecBase &>(spec_);
  if (is_root) {
    if (0 != spec.connect_by_root_exprs_.count()) {
      ObExpr *expr = nullptr;
      ObExpr *param = nullptr;
      ObDatum *datum = nullptr;
      for (int64_t i = 0; i < spec.connect_by_root_exprs_.count() && OB_SUCC(ret); ++i) {
        if (OB_ISNULL(expr = spec.connect_by_root_exprs_.at(i))
            || OB_UNLIKELY(1 != expr->arg_cnt_)
            || OB_ISNULL(param = expr->args_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid expr", K(ret), K(expr));
        } else if (OB_FAIL(param->eval(eval_ctx_, datum))) {
          LOG_WARN("expr eval failed");
        } else {
          expr->locate_expr_datum(eval_ctx_) = *datum;
          expr->set_evaluated_projected(eval_ctx_);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_NOT_NULL(connect_by_root_row_)) {
          mem_context_->get_malloc_allocator().free(connect_by_root_row_);
          connect_by_root_row_ = NULL;
        }
        if (OB_FAIL(ObStoredDatumRow::build(connect_by_root_row_, spec.connect_by_root_exprs_,
                                            eval_ctx_, mem_context_->get_malloc_allocator()))) {
          LOG_WARN("failed to build store row", K(ret));
        }
      }
    }
  } else {
    if (0 != spec.connect_by_root_exprs_.count()) {
      if (OB_ISNULL(connect_by_root_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: connect by root row is null", K(ret));
      } else if (OB_FAIL(connect_by_root_row_->to_expr(spec.connect_by_root_exprs_, eval_ctx_))) {
        LOG_WARN("failed to to expr from prior row", K(ret));
      }
    }
  }
  return ret;
}

int ObNLConnectByWithIndexOp::to_expr()
{
  int ret = OB_SUCCESS;
  if (nullptr != root_row_) {
    if (OB_FAIL(root_row_->to_expr(MY_SPEC.right_prior_exprs_, eval_ctx_))) {
      LOG_WARN("failed to to expr from prior row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (nullptr == cur_output_row_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: cur output row is null", K(ret));
    } else if (OB_FAIL(cur_output_row_->to_expr(MY_SPEC.cur_row_exprs_, eval_ctx_))) {
      LOG_WARN("failed to to expr from prior row", K(ret));
    }
  }
  return ret;
}

int ObNLConnectByWithIndexOp::read_pump_operate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(connect_by_pump_.get_next_row(root_row_, cur_output_row_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get pump next row", K(ret));
    }
  } else if (OB_FAIL(restore_prior_expr())) {
    LOG_WARN("fail to convert expr", K(ret));
  }
  return ret;
}

int ObNLConnectByWithIndexOp::prepare_rescan_params()
{
  int ret = OB_SUCCESS;
  int64_t param_cnt = MY_SPEC.rescan_params_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
    const ObDynamicParamSetter &rescan_param = MY_SPEC.rescan_params_.at(i);
    if (OB_FAIL(rescan_param.set_dynamic_param(eval_ctx_))) {
      LOG_WARN("fail to set dynamic param", K(ret));
    }
  }
  return ret;
}

int ObNLConnectByWithIndexOp::read_pump_func_going()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_level_as_param(connect_by_pump_.get_current_level()))) {
    /* LEVEL is allowed to be a dynamic param, so it needs to be set before
     * preparing rescan params.
     * set_level_as_param is currently repeatedly called in multiple places,
     * which does not make sense and can be optimized later.
     */
    LOG_WARN("failed to set current level", K(ret));
  } else if (OB_LIKELY(!MY_SPEC.rescan_params_.empty())) {
    if (OB_FAIL(prepare_rescan_params())) {
      LOG_WARN("failed to prepare rescan params", K(ret));
    } else if (OB_FAIL(right_->rescan())) {
      LOG_WARN("failed to rescan right op", K(ret));
    } else {
      state_ = CNTB_STATE_READ_RIGHT;
    }
  } else {
    if (OB_FAIL(prepare_rescan_params())) {
      LOG_WARN("failed to prepare rescan params", K(ret));
    } else if (OB_FAIL(rescan_right())) {
      LOG_WARN("failed to rescan right", K(ret));
    } else {
      state_ = CNTB_STATE_READ_RIGHT;
    }
  }
  LOG_DEBUG("debug pump going", K(ObToStringExprRow(eval_ctx_, MY_SPEC.right_prior_exprs_)),
      K(ObToStringExprRow(eval_ctx_, MY_SPEC.cur_row_exprs_)));
  return ret;
}

int ObNLConnectByWithIndexOp::read_pump_func_end()
{
  state_ = CNTB_STATE_JOIN_END;
  return OB_ITER_END;
}

int ObNLConnectByWithIndexOp::read_left_operate()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(left_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next row", K(ret));
  } else {
    LOG_DEBUG("connect by read left", K(ret), K(lbt()));
  }
  return ret;
}

int ObNLConnectByWithIndexOp::add_pseudo_column(ObExpr *pseudo_expr, ObConnectByPseudoColumn column_type)
{
  int ret = OB_SUCCESS;
  if (nullptr != pseudo_expr) {
    common::number::ObNumber num;
    ObNumStackOnceAlloc tmp_alloc;
    ObDatum *expr_datum = &pseudo_expr->locate_datum_for_write(eval_ctx_);
    switch (column_type) {
      case LEVEL: {
        int64_t cur_level = connect_by_pump_.get_current_level();
        if (OB_FAIL(num.from(cur_level, tmp_alloc))) {
          LOG_WARN("failed to from number", K(ret));
        } else {
          expr_datum->set_number(num);
        }
        break;
      }
      case CONNECT_BY_ISLEAF: {
        bool is_leaf = (false == is_match_);
        int64_t num_value = is_leaf ? 1 : 0;
        if (OB_FAIL(num.from(num_value, tmp_alloc))) {
          LOG_WARN("failed to from number", K(ret));
        } else {
          expr_datum->set_number(num);
        }
        break;
      }
      case CONNECT_BY_ISCYCLE: {
        int64_t num_value = is_cycle_ ? 1 : 0;
        if (OB_FAIL(num.from(num_value, tmp_alloc))) {
          LOG_WARN("failed to from number", K(ret));
        } else {
          expr_datum->set_number(num);
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invali pseudo column type", K(column_type), K(ret));
    }
    pseudo_expr->set_evaluated_projected(eval_ctx_);

    LOG_DEBUG("add pseudo column", KP(cur_output_row_),
      K(ObToStringExprRow(eval_ctx_, MY_SPEC.right_prior_exprs_)),
      K(ObToStringExprRow(eval_ctx_, MY_SPEC.cur_row_exprs_)));
  }
  return ret;
}

int ObNLConnectByWithIndexOp::read_left_func_going()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(construct_root_output_row())) {
    LOG_WARN("fail to construct root output row", K(ret));
  } else if (OB_FAIL(add_pseudo_column(MY_SPEC.level_expr_, LEVEL))) {
    LOG_WARN("fail to add pseudo column", K(ret));
  } else if (OB_FAIL(connect_by_pump_.add_root_row(MY_SPEC.right_prior_exprs_, MY_SPEC.cur_row_exprs_))) {
    LOG_WARN("fail to set root row", K(ret));
  } else {
    LOG_DEBUG("trace left going row", K(ObToStringExprRow(eval_ctx_, MY_SPEC.right_prior_exprs_)),
      K(ObToStringExprRow(eval_ctx_, MY_SPEC.cur_row_exprs_)));
  }
  return ret;
}

int ObNLConnectByWithIndexOp::read_left_func_end()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(connect_by_pump_.sort_sibling_rows())) {
    LOG_WARN("fail to sort sibling rows", K(ret));
  } else {
    // clean right：由于之前为了构造出[prior row, cur_row]，将right expr设置了值
    // 这个时候需要将right expr清理掉，否则可能会污染下一次right表达式里面的值
    for (int64_t i = 0; i < MY_SPEC.right_prior_exprs_.count() && OB_SUCC(ret); ++i) {
      MY_SPEC.right_prior_exprs_.at(i)->get_eval_info(eval_ctx_).clear_evaluated_flag();
    }
  }
  state_ = CNTB_STATE_READ_OUTPUT;
  return ret;
}

int ObNLConnectByWithIndexOp::join_end_operate()
{
  return OB_ITER_END;
}

int ObNLConnectByWithIndexOp::join_end_func_end()
{
  return OB_ITER_END;
}

int ObNLConnectByWithIndexOp::read_right_operate()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(right_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right row", K(ret));
  }
  return ret;
}

int ObNLConnectByWithIndexOp::read_right_func_going()
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  LOG_DEBUG("trace output row after get right", K(ObToStringExprRow(eval_ctx_, MY_SPEC.right_prior_exprs_)),
      K(ObToStringExprRow(eval_ctx_, MY_SPEC.cur_row_exprs_)));
  // 这里设置level是为了方便计算calc_other_conds中有level表达式
  // 与老框架不同的地方在于，目前ObExpr就是一个当前全局可见的一块内存，相当于param
  // 只是生命周期是每迭代一行可能会改变不同的值
  // 但是这里暂时没有那样做，因为老框架会将condition中的level转化成parameter，所以这里一定是parameter
  if (OB_FAIL(set_level_as_param(connect_by_pump_.get_current_level()))) {
    LOG_WARN("failed to set current level", K(ret));
  } else if (OB_FAIL(calc_other_conds(is_match))) {
    LOG_WARN("failed to compare left and right row on other join conds", K(ret));
  } else if (is_match) {
    if (OB_FAIL(add_pseudo_column(MY_SPEC.level_expr_, LEVEL))) {
      LOG_WARN("failed to add pseudo column level", K(ret));
    } else if (OB_FAIL(connect_by_pump_.append_row(MY_SPEC.right_prior_exprs_, MY_SPEC.cur_row_exprs_))) {
      if (OB_ERR_CBY_LOOP == ret) {
        ret = OB_SUCCESS;
        if (MY_SPEC.is_nocycle_) {
          is_cycle_ = true;
        }
      } else {
        LOG_WARN("fail to append row", K(ret));
      }
    } else {
      is_match_ = true;
    }
    LOG_DEBUG("trace output row after get right", K(ObToStringExprRow(eval_ctx_, MY_SPEC.right_prior_exprs_)),
      K(ObToStringExprRow(eval_ctx_, MY_SPEC.cur_row_exprs_)),
      K(is_match_));
  }
  return ret;
}

int ObNLConnectByWithIndexOp::read_right_func_end()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(connect_by_pump_.sort_sibling_rows())) {
    LOG_WARN("fail to sort siblings", K(ret));
  } else {
    state_ = CNTB_STATE_READ_OUTPUT;
  }
  return ret;
}

int ObNLConnectByWithIndexOp::rescan_right()
{
  int ret = OB_SUCCESS;
  if (PHY_MATERIAL != right_->get_spec().type_ && OB_FAIL(right_->rescan())) {
    LOG_WARN("rescan right child failed", K(ret));
  } else if (PHY_MATERIAL == right_->get_spec().type_ 
              && OB_FAIL(static_cast<ObMaterialOp *> (right_)->rewind())) {
    LOG_WARN("rescan right child failed", K(ret));
  }
  return ret;
}

}//sql
}//oceanbase
