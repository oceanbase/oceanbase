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

#include "sql/engine/connect_by/ob_nl_cnnt_by_op.h"
#include "common/rowkey/ob_rowkey.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_sys_connect_by_path.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObNLConnectBySpecBase::ObNLConnectBySpecBase(ObIAllocator &alloc, const ObPhyOperatorType type)
: ObOpSpec(alloc, type),
  cmp_funcs_(alloc),
  sort_siblings_exprs_(alloc),
  connect_by_root_exprs_(alloc),
  sys_connect_exprs_(alloc),
  cond_exprs_(alloc),
  connect_by_prior_exprs_(alloc),
  level_expr_(nullptr),
  is_leaf_expr_(nullptr),
  is_cycle_expr_(nullptr),
  cur_row_exprs_(alloc),
  left_prior_exprs_(alloc),
  right_prior_exprs_(alloc),
  sort_collations_(alloc),
  sort_cmp_funs_(alloc),
  rescan_params_(alloc),
  is_nocycle_(false),
  has_prior_(false)
{
}

OB_SERIALIZE_MEMBER((ObNLConnectBySpecBase, ObOpSpec),
                    cmp_funcs_,
                    sort_siblings_exprs_,
                    connect_by_root_exprs_,
                    sys_connect_exprs_,
                    cond_exprs_,
                    connect_by_prior_exprs_,
                    level_expr_,
                    is_leaf_expr_,
                    is_cycle_expr_,
                    cur_row_exprs_,
                    left_prior_exprs_,
                    right_prior_exprs_,
                    sort_collations_,
                    sort_cmp_funs_,
                    rescan_params_,
                    is_nocycle_,
                    has_prior_);

OB_SERIALIZE_MEMBER((ObNLConnectBySpec, ObNLConnectBySpecBase));

// swap left and right, so left row is prior row, all is null, and right row is cur_row
// if left_root_exprs has duplicate row, so set null after assign all right row are assigned
int ObNLConnectByOpBase::construct_root_output_row()
{
  int ret = OB_SUCCESS;
  ObDatum *left_expr_datum = nullptr;
  const ObNLConnectBySpecBase &spec = static_cast<const ObNLConnectBySpecBase &>(spec_);
  for (int64_t i = 0; i < spec.left_prior_exprs_.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(spec.left_prior_exprs_.at(i)->eval(eval_ctx_, left_expr_datum))) {
      LOG_WARN("failed to eval expr", K(ret));
    } else {
      spec.right_prior_exprs_.at(i)->locate_expr_datum(eval_ctx_) = *left_expr_datum;
      spec.right_prior_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
    }
  }
  for (int64_t i = 0; i < spec.left_prior_exprs_.count() && OB_SUCC(ret); ++i) {
    spec.left_prior_exprs_.at(i)->locate_expr_datum(eval_ctx_).set_null();
    spec.left_prior_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}

// 这里与老框架不一样，老框架根据root row直接计算，而新框架都是绑定到当前的ObExpr
// 当然这里也可以将root row 与当前ObIArray<ObExpr*>进行swap后做处理，比较繁琐，暂时不按照这种方式处理
int ObNLConnectByOpBase::calc_connect_by_root_exprs(bool is_root)
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
    }
  } else {
    // 这里假设上层不会修改connect_by_root，或者到了connect_by算子，connect_by_root还是原来的值
    for (int64_t i = 0; i < spec.connect_by_root_exprs_.count() && OB_SUCC(ret); ++i) {
      spec.connect_by_root_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

int ObNLConnectByOpBase::calc_other_conds(bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  const ObNLConnectBySpecBase &spec = static_cast<const ObNLConnectBySpecBase &>(spec_);
  const ObIArray<ObExpr *> &conds = spec.cond_exprs_;
  ObDatum *cmp_res = NULL;
  ARRAY_FOREACH(conds, i) {
    if (OB_FAIL(conds.at(i)->eval(eval_ctx_, cmp_res))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*conds.at(i)));
    } else if (cmp_res->is_null() || 0 == cmp_res->get_int()) {
      is_match = false;
      break;
    }
  }
  return ret;
}

int ObNLConnectByOpBase::calc_sys_connect_by_path()
{
  int ret = OB_SUCCESS;
  const ObNLConnectBySpecBase &spec = static_cast<const ObNLConnectBySpecBase &>(spec_);
  if (0 != spec.sys_connect_exprs_.count()) {
    ObExpr *expr = nullptr;
    for (int64_t i = 0; i < spec.sys_connect_exprs_.count() && OB_SUCC(ret); ++i) {
      expr = spec.sys_connect_exprs_.at(i);
      sys_connect_by_path_id_ = i;
      ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
      if (OB_FAIL(ObExprSysConnectByPath::eval_sys_connect_by_path(*expr, eval_ctx_,
                                                datum, this))) {
        LOG_WARN("failed to eval connect by path", K(ret));
        datum.set_null();
      } else {
        expr->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObNLConnectByOpBase::set_level_as_param(int64_t level)
{
  int ret = OB_SUCCESS;
  const ObNLConnectBySpecBase &spec = static_cast<const ObNLConnectBySpecBase &>(spec_);
  if (NULL != spec.level_expr_) {
    common::number::ObNumber num;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(num.from(level, tmp_alloc))) {
      LOG_WARN("failed to from number", K(ret));
    } else {
      spec.level_expr_->locate_datum_for_write(eval_ctx_).set_number(num);
      spec.level_expr_->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

ObNLConnectByOp::ObNLConnectByOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObNLConnectByOpBase(exec_ctx, spec, input),
    connect_by_pump_(),
    state_(CNTB_STATE_READ_LEFT),
    is_inited_(false),
    output_generated_(false),
    mem_context_(NULL),
    profile_(ObSqlWorkAreaType::SORT_WORK_AREA),
    sql_mem_processor_(profile_, op_monitor_info_)
{
  state_operation_func_[CNTB_STATE_JOIN_END] = &ObNLConnectByOp::join_end_operate;
  state_function_func_[CNTB_STATE_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[CNTB_STATE_JOIN_END][FT_ITER_END] = &ObNLConnectByOp::join_end_func_end;

  state_operation_func_[CNTB_STATE_READ_OUTPUT] = &ObNLConnectByOp::read_output_operate;
  state_function_func_[CNTB_STATE_READ_OUTPUT][FT_ITER_GOING] =  &ObNLConnectByOp::read_output_func_going;
  state_function_func_[CNTB_STATE_READ_OUTPUT][FT_ITER_END] = &ObNLConnectByOp::read_output_func_end;

  state_operation_func_[CNTB_STATE_READ_PUMP] = &ObNLConnectByOp::read_pump_operate;
  state_function_func_[CNTB_STATE_READ_PUMP][FT_ITER_GOING] = &ObNLConnectByOp::read_pump_func_going;
  state_function_func_[CNTB_STATE_READ_PUMP][FT_ITER_END] = &ObNLConnectByOp::read_pump_func_end;

  state_operation_func_[CNTB_STATE_READ_LEFT] = &ObNLConnectByOp::read_left_operate;
  state_function_func_[CNTB_STATE_READ_LEFT][FT_ITER_GOING] = &ObNLConnectByOp::read_left_func_going;
  state_function_func_[CNTB_STATE_READ_LEFT][FT_ITER_END] = &ObNLConnectByOp::read_left_func_end;

  state_operation_func_[CNTB_STATE_READ_RIGHT] = &ObNLConnectByOp::read_right_operate;
  state_function_func_[CNTB_STATE_READ_RIGHT][FT_ITER_GOING] = &ObNLConnectByOp::read_right_func_going;
  state_function_func_[CNTB_STATE_READ_RIGHT][FT_ITER_END] = &ObNLConnectByOp::read_right_func_end;
}

ObNLConnectByOp::~ObNLConnectByOp()
{
  destroy();
}

void ObNLConnectByOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  connect_by_pump_.~ObConnectByOpPump();//must be call
  ObNLConnectByOpBase::destroy();
}

int ObNLConnectByOp::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(connect_by_pump_.init(
      *const_cast<ObNLConnectBySpec*>(&MY_SPEC), *this, eval_ctx_))) {
    LOG_WARN("fail to init Connect by Ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObNLConnectByOp::reset()
{
  state_ = CNTB_STATE_READ_LEFT;
  connect_by_pump_.reset();
  sys_connect_by_path_id_ = INT64_MAX;
  output_generated_ = false;
}

int ObNLConnectByOp::inner_open()
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  int64_t tenant_id = OB_INVALID_ID;
  if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_FAIL(init())) {
    LOG_WARN("fail to init Connect by Ctx", K(ret));
  } else if (MY_SPEC.left_prior_exprs_.count() > MY_SPEC.right_prior_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: exprs is not match",
      "left prior exprs", MY_SPEC.left_prior_exprs_.count(),
      "right prior exprs", MY_SPEC.right_prior_exprs_.count());
  } else if (MY_SPEC.cmp_funcs_.count() != MY_SPEC.connect_by_prior_exprs_.count()
      && MY_SPEC.cmp_funcs_.count() != MY_SPEC.left_prior_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: cmp func is not match with prior exprs", K(ret));
  } else if (MY_SPEC.hash_key_exprs_.count() != MY_SPEC.hash_probe_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash key exprs count != hash probe exprs count", K(ret), K(MY_SPEC.hash_key_exprs_),
             K(MY_SPEC.hash_probe_exprs_));
  } else {
    tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    param.set_mem_attr(tenant_id, ObModIds::OB_CONNECT_BY_PUMP, ObCtxIds::WORK_AREA)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL)
         .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    } else if (OB_FAIL(connect_by_pump_.datum_store_.init(INT64_MAX,
              tenant_id,
              ObCtxIds::WORK_AREA, ObModIds::OB_CONNECT_BY_PUMP,
              true /* enable dump */, 0))) {
      LOG_WARN("init chunk row store failed", K(ret));
    } else {
      ObIAllocator &alloc = mem_context_->get_malloc_allocator();
      connect_by_pump_.set_allocator(alloc);
      connect_by_pump_.datum_store_.set_allocator(alloc);
      connect_by_pump_.pump_stack_.set_block_allocator(ModulePageAllocator(alloc, "CnntArrays"));
    }
  }
  return ret;
}

int ObNLConnectByOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

int ObNLConnectByOp::inner_close()
{
  int ret = OB_SUCCESS;
  connect_by_pump_.close(OB_ISNULL(mem_context_) ? NULL : &mem_context_->get_malloc_allocator());
  sql_mem_processor_.unregister_profile();
  if (nullptr != mem_context_) {
    mem_context_->reuse();
  }
  connect_by_pump_.sys_path_buffer_.reset();
  return ret;
}

int ObNLConnectByOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  state_operation_func_type state_operation = NULL;
  state_function_func_type state_function = NULL;
  int func = -1;
  ObCnntByOpState &state = state_;
  output_generated_ = false;
  clear_evaluated_flag();
  while (OB_SUCC(ret) && !output_generated_) {
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
  if (OB_SUCC(ret)) {
    LOG_DEBUG("debug get next", K(ObToStringExprRow(eval_ctx_, MY_SPEC.cur_row_exprs_)));
  }
  return ret;
}

int ObNLConnectByOp::read_left_operate()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(left_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("fail to get next row", K(ret));
  } else {
    LOG_DEBUG("connect by read left", K(ret));
  }
  return ret;
}

int ObNLConnectByOp::read_left_func_going()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(construct_root_output_row())) {
    LOG_WARN("fail to construct root output row", K(ret));
  } else if (OB_FAIL(connect_by_pump_.add_root_row())) {
    LOG_WARN("fail to set root row", K(ret));
  } else {
    if (connect_by_pump_.get_row_store_constructed()) {
      state_ = CNTB_STATE_READ_OUTPUT;
    } else {
      state_ = CNTB_STATE_READ_RIGHT;
    }
    LOG_DEBUG("trace left going row",
      K(ObToStringExprRow(eval_ctx_, MY_SPEC.left_prior_exprs_)),
      K(ObToStringExprRow(eval_ctx_, MY_SPEC.cur_row_exprs_)));
  }
  return ret;
}

int ObNLConnectByOp::read_left_func_end()
{
  int ret = OB_SUCCESS;
  // clean right：由于之前为了构造出[prior row, cur_row]，将right expr设置了值
  // 这个时候需要将right expr清理掉，否则可能会污染下一次right表达式里面的值
  //maybe no need
  for (int64_t i = 0; i < MY_SPEC.right_prior_exprs_.count() && OB_SUCC(ret); ++i) {
    MY_SPEC.right_prior_exprs_.at(i)->get_eval_info(eval_ctx_).clear_evaluated_flag();
  }
  state_ = CNTB_STATE_JOIN_END;
  return ret;
}

int ObNLConnectByOp::read_right_operate()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObNLConnectByOp::read_right_func_going()
{
  int ret = OB_SUCCESS;
  bool first_row = true;
  int64_t tenant_id = OB_INVALID_ID;
	if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  }
  while(OB_SUCC(ret)) {
    if (OB_FAIL(right_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next right row failed", K(ret));
      }
    } else if (first_row) {
		  int64_t row_count = 0;
      if (OB_ISNULL(spec_.get_right())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("right child is null", K(ret));
      } else if (FALSE_IT(row_count = spec_.get_right()->get_rows())) {
      } else if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
          &ctx_, MY_SPEC.px_est_size_factor_, row_count, row_count))) {
        LOG_WARN("failed to get px size", K(ret));
      } else if (OB_FAIL(sql_mem_processor_.init(
          &mem_context_->get_malloc_allocator(),
          tenant_id,
          row_count * MY_SPEC.width_, MY_SPEC.type_, MY_SPEC.id_, &ctx_))) {
        LOG_WARN("failed to init sql memory manager processor", K(ret));
      } else {
        connect_by_pump_.datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
        connect_by_pump_.datum_store_.set_callback(&sql_mem_processor_);
        connect_by_pump_.datum_store_.set_io_event_observer(&io_event_observer_);
        LOG_TRACE("trace init sql mem mgr for material", K(row_count),
                  K(profile_.get_cache_size()), K(profile_.get_expect_size()));
      }
      first_row = false;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(process_dump())) {
      LOG_WARN("failed to process dump", K(ret));
    } else if (OB_FAIL(connect_by_pump_.push_back_store_row())) {
      LOG_WARN("add row to row store failed", K(ret));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
	LOG_TRACE("push all row in row_store", K(connect_by_pump_.datum_store_.get_row_cnt()),
    K(connect_by_pump_.datum_store_.has_dumped()));
  if (OB_SUCC(ret) && MY_SPEC.hash_key_exprs_.count() != 0
      && !connect_by_pump_.datum_store_.has_dumped()
      && OB_FAIL(connect_by_pump_.build_hash_table(mem_context_->get_malloc_allocator()))) {
    LOG_WARN("build hash table failed", K(ret));
  }
  connect_by_pump_.set_row_store_constructed();
  state_ = CNTB_STATE_READ_OUTPUT;
  return ret;
}

int ObNLConnectByOp::read_right_func_end()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObNLConnectByOp::read_output_operate()
{
  int ret = OB_SUCCESS;
  if (connect_by_pump_.pump_stack_.empty()) {
    ret = OB_ITER_END;
  }
  return ret;
}

/*进行伪列计算
1.查找子节点，如果子节点构成环，也就是与某个祖先节点相同，那么node.is_cycle_ = true;
2.查找子节点，如果子节点不构成环，那么node.is_leaf_ = false;
*/
int ObNLConnectByOp::calc_pseudo_flags(ObConnectByOpPump::PumpNode &node)
{
  int ret = OB_SUCCESS;
  bool output_cycle = (NULL != MY_SPEC.is_cycle_expr_);
  bool output_leaf = (NULL != MY_SPEC.is_leaf_expr_);
  node.is_cycle_ = false;
  node.is_leaf_ = true;
  if (output_cycle || output_leaf) {
    ObConnectByOpPump::RowFetcher row_fetcher;
     ObChunkDatumStore::Iterator iterator;
     row_fetcher.iterator_ = &iterator;
    if (OB_FAIL(node.pump_row_->to_expr(MY_SPEC.left_prior_exprs_, eval_ctx_,
                                        MY_SPEC.left_prior_exprs_.count()))) {
      LOG_WARN("to expr failed", K(ret));
    } else if (OB_FAIL(set_level_as_param(node.level_ + 1))) {
      LOG_WARN("set level as param failed", K(ret));
    } else if (OB_FAIL(row_fetcher.init(connect_by_pump_, MY_SPEC.hash_probe_exprs_))) {
      LOG_WARN("begin row store failed", K(ret));
    }

    bool finished = false;
    bool matched = false;
    ObConnectByOpPump::PumpNode next_node;
    next_node.level_ = node.level_ + 1;
    connect_by_pump_.cur_level_ = next_node.level_;
    while(OB_SUCC(ret) && false == finished) {
      clear_evaluated_flag();
      OZ(row_fetcher.get_next_row(next_node.pump_row_));
      OZ(next_node.pump_row_->to_expr(MY_SPEC.right_prior_exprs_, eval_ctx_));

      if (OB_FAIL(ret)) {
      } else if(OB_FAIL(calc_other_conds(matched))) {
        LOG_WARN("fail to calc other conds", K(ret));
      } else if (matched) {
        if (OB_FAIL(connect_by_pump_.check_child_cycle(next_node, &node))) {
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
          finished = (false == output_cycle || node.is_cycle_)
                      && (false == output_leaf || false == node.is_leaf_);
        }
        if (OB_NOT_NULL(next_node.prior_exprs_result_)) {
          connect_by_pump_.allocator_.free(
              const_cast<ObChunkDatumStore::StoredRow *>(next_node.prior_exprs_result_));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObNLConnectByOp::read_output_func_going()
{
  int ret = OB_SUCCESS;
  ObConnectByOpPump::PumpNode *top_node = NULL;
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("check physical plan status faild", K(ret));
  } else if (OB_FAIL(connect_by_pump_.get_top_pump_node(top_node))) {
    LOG_WARN("connect by pump get top node failed", K(ret));
  } else if (OB_ISNULL(top_node->output_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output row of node is null", K(ret));
  } else if (OB_FAIL(calc_pseudo_flags(*top_node))) {
    LOG_WARN("calc pseudo flags failed", K(ret));
  } else if (OB_FAIL(add_pseudo_column(*top_node))) {
    LOG_WARN("fail to add pseudo column", K(ret));
  } else if (OB_FAIL(top_node->pump_row_->to_expr(MY_SPEC.left_prior_exprs_, eval_ctx_))) {
    LOG_WARN("to expr failed", K(ret));
  } else if (OB_FAIL(top_node->row_fetcher_.init(connect_by_pump_, MY_SPEC.hash_probe_exprs_))) {
    LOG_WARN("fail to begin iterator for chunk row store", K(ret));
  } else if (OB_FAIL(top_node->pump_row_->to_expr(MY_SPEC.right_prior_exprs_, eval_ctx_))) {
    LOG_WARN("to_expr failed", K(ret));
  } else if (OB_FAIL(top_node->output_row_->to_expr(MY_SPEC.cur_row_exprs_, eval_ctx_))) {
    LOG_WARN("to_expr failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_connect_by_root_exprs(1 == top_node->level_))) {
    LOG_WARN("fail to calc connect_by_root_exprs", K(ret));
  } else if (OB_FAIL(calc_sys_connect_by_path())) {
    LOG_WARN("Failed to calc sys connect by paht", K(ret));
  } else {
    connect_by_pump_.cur_level_ = top_node->level_;
    state_ = CNTB_STATE_READ_PUMP;
    output_generated_ = true;
  }
  return ret;
}

int ObNLConnectByOp::read_output_func_end()
{
  int ret = OB_SUCCESS;
  state_ = CNTB_STATE_READ_LEFT;
  return ret;
}

int ObNLConnectByOp::read_pump_operate()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObNLConnectByOp::read_pump_func_going()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(connect_by_pump_.get_next_row())) {
    if (OB_ITER_END == ret) { //搜索完一棵树
      ret = OB_SUCCESS;
      state_ = CNTB_STATE_READ_LEFT;
    } else {
      LOG_WARN("connect by pump get next row failed", K(ret));
    }
  } else {
    state_ = CNTB_STATE_READ_OUTPUT;
  }
  return ret;
}

int ObNLConnectByOp::read_pump_func_end()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObNLConnectByOp::add_pseudo_column(ObConnectByOpPump::PumpNode &node)
{
  int ret = OB_SUCCESS;
  common::number::ObNumber num;
  ObNumStackAllocator<3> tmp_alloc;
  const ObExpr *level_expr = MY_SPEC.level_expr_;
  const ObExpr *is_leaf_expr = MY_SPEC.is_leaf_expr_;
  const ObExpr *is_cycle_expr = MY_SPEC.is_cycle_expr_;

  if (NULL != level_expr) {
    int64_t cur_level = node.level_;
    if (OB_FAIL(num.from(cur_level, tmp_alloc))) {
      LOG_WARN("failed to from number", K(ret));
    } else {
      level_expr->locate_datum_for_write(eval_ctx_).set_number(num);
      level_expr->set_evaluated_projected(eval_ctx_);
    }
  }
  if (NULL != is_leaf_expr) {
    int64_t num_value = node.is_leaf_ ? 1 : 0;
    if (OB_FAIL(num.from(num_value, tmp_alloc))) {
      LOG_WARN("failed to from number", K(ret));
    } else {
      is_leaf_expr->locate_datum_for_write(eval_ctx_).set_number(num);
      is_leaf_expr->set_evaluated_projected(eval_ctx_);
    }
  }
  if (NULL != is_cycle_expr) {
    int64_t num_value = node.is_cycle_ ? 1 : 0;
    if (OB_FAIL(num.from(num_value, tmp_alloc))) {
      LOG_WARN("failed to from number", K(ret));
    } else {
      is_cycle_expr->locate_datum_for_write(eval_ctx_).set_number(num);
      is_cycle_expr->set_evaluated_projected(eval_ctx_);
    }
  }

  LOG_DEBUG("add pseudo column",
    K(ObToStringExprRow(eval_ctx_, MY_SPEC.cur_row_exprs_)));
  return ret;
}

int ObNLConnectByOp::join_end_operate()
{
  return OB_ITER_END;
}

int ObNLConnectByOp::join_end_func_end()
{
  return OB_ITER_END;
}

int ObNLConnectByOp::process_dump()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
      &mem_context_->get_malloc_allocator(),
      [&](int64_t cur_cnt){ return connect_by_pump_.datum_store_.get_row_cnt_in_memory() > cur_cnt; },
      updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (need_dump() && GCONF.is_sql_operator_dump_enabled()
          && OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t max_memory_size) {
              return sql_mem_processor_.get_data_size() > max_memory_size;
            },
            dumped, sql_mem_processor_.get_data_size()))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped) {
    if (OB_FAIL(connect_by_pump_.datum_store_.dump(false, true))) {
      LOG_WARN("failed to dump row store", K(ret));
    } else {
      sql_mem_processor_.reset();
      sql_mem_processor_.set_number_pass(1);
      LOG_TRACE("trace material dump",
        K(sql_mem_processor_.get_data_size()),
        K(connect_by_pump_.datum_store_.get_row_cnt_in_memory()),
        K(sql_mem_processor_.get_mem_bound()));
    }
  }
  return ret;
}

}//sql
}//oceanbase
