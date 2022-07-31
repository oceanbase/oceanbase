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

#include "ob_operator.h"
#include "ob_operator_factory.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_transmit.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER(ObDynamicParamSetter, param_idx_, src_, dst_);

int ObDynamicParamSetter::set_dynamic_param(ObEvalCtx& eval_ctx) const
{
  int ret = OB_SUCCESS;
  ObDatum* res = NULL;
  if (OB_ISNULL(src_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr not init", K(ret), KP(src_));
  } else if (OB_FAIL(src_->eval(eval_ctx, res))) {
    LOG_WARN("fail to calc rescan params", K(ret), K(*this));
  } else if (OB_FAIL(update_dynamic_param(eval_ctx, *res))) {
    LOG_WARN("update dynamic param store failed", K(ret));
  }
  return ret;
}

int ObDynamicParamSetter::update_dynamic_param(ObEvalCtx& eval_ctx, ObDatum& datum) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_ctx = eval_ctx.exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(phy_ctx) || OB_ISNULL(dst_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(phy_ctx), KP(dst_));
  } else {
    ParamStore& param_store = phy_ctx->get_param_store_for_update();
    clear_parent_evaluated_flag(eval_ctx, *dst_);
    ObDatum& param_datum = dst_->locate_expr_datum(eval_ctx);
    param_datum.set_datum(datum);
    dst_->get_eval_info(eval_ctx).evaluated_ = true;
    // init param store, for calculating query range.
    if (OB_FAIL(param_datum.to_obj(param_store.at(param_idx_), dst_->obj_meta_, dst_->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret), "datum", DATUM2STR(*dst_, param_datum));
    } else {
      param_store.at(param_idx_).set_param_meta();
    }
  }

  return ret;
}

void ObDynamicParamSetter::clear_parent_evaluated_flag(ObEvalCtx& eval_ctx, ObExpr& expr) const
{
  for (int64_t i = 0; i < expr.parent_cnt_; i++) {
    clear_parent_evaluated_flag(eval_ctx, *expr.parents_[i]);
  }
  expr.get_eval_info(eval_ctx).clear_evaluated_flag();
}

ObOpSpec::ObOpSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : type_(type),
      id_(OB_INVALID_ID),
      plan_(NULL),
      parent_(NULL),
      children_(NULL),
      child_cnt_(0),
      left_(NULL),
      right_(NULL),
      output_(&alloc),
      startup_filters_(&alloc),
      filters_(&alloc),
      calc_exprs_(&alloc),
      cost_(0),
      rows_(0),
      width_(0),
      px_est_size_factor_(),
      plan_depth_(0)
{}

ObOpSpec::~ObOpSpec()
{}

OB_SERIALIZE_MEMBER(ObOpSpec, id_, output_, startup_filters_, filters_, calc_exprs_, cost_, rows_, width_,
    px_est_size_factor_, plan_depth_);

DEF_TO_STRING(ObOpSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("name",
      op_name(),
      K_(type),
      K_(id),
      K_(child_cnt),
      "output_cnt",
      output_.count(),
      "startup_filters_cnt",
      startup_filters_.count(),
      "calc_exprs_cnt",
      calc_exprs_.count(),
      K_(rows));
  J_OBJ_END();
  return pos;
}

int ObOpSpec::set_children_pointer(ObOpSpec** children, const uint32_t child_cnt)
{
  int ret = OB_SUCCESS;
  if (child_cnt > 0 && NULL == children) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(child_cnt), KP(children));
  } else {
    children_ = children;
    child_cnt_ = child_cnt;
    if (child_cnt > 0) {
      child_ = children[0];
    } else {
      child_ = NULL;
    }
    if (child_cnt > 1) {
      right_ = children[1];
    } else {
      right_ = NULL;
    }
  }
  return ret;
}

int ObOpSpec::set_child(const uint32_t idx, ObOpSpec* child)
{
  int ret = OB_SUCCESS;
  if (idx >= child_cnt_ || OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), K(child_cnt_), KP(child));
  } else {
    children_[idx] = child;
    if (0 == idx) {
      child_ = child;
    }
    if (1 == idx) {
      right_ = child;
    }
    child->parent_ = this;
  }
  return ret;
}

int ObOpSpec::create_op_input(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  // Do some sanity check,
  // we no longer need to check the validity of those pointers in ObOperator.
  if (OB_ISNULL(GET_MY_SESSION(exec_ctx)) || OB_ISNULL(GET_PHY_PLAN_CTX(exec_ctx)) ||
      OB_ISNULL(GET_TASK_EXECUTOR_CTX(exec_ctx))) {
  } else if (OB_FAIL(create_op_input_recursive(exec_ctx))) {
    LOG_WARN("create operator recursive failed", K(ret));
  }
  LOG_TRACE("trace create input", K(ret), K(lbt()));
  return ret;
}

int ObOpSpec::create_op_input_recursive(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObOperatorKit* kit = exec_ctx.get_operator_kit(id_);
  int64_t create_child_cnt = child_cnt_;
  if (OB_ISNULL(kit) || (child_cnt_ > 0 && NULL == children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator kit should be created before create operator "
             "and children must be valid",
        K(ret),
        KP(kit),
        K(id_),
        KP(children_),
        K(create_child_cnt),
        K(type_));
  } else {
    kit->spec_ = this;
    LOG_TRACE("trace create input", K(ret), K(id_), K(type_), K(lbt()));
  }

  // create operator input
  if (OB_SUCC(ret)) {
    // Operator input may created in scheduler, no need to create again.
    if (NULL == kit->input_ && ObOperatorFactory::has_op_input(type_)) {
      if (OB_FAIL(ObOperatorFactory::alloc_op_input(exec_ctx.get_allocator(), exec_ctx, *this, kit->input_))) {
        LOG_WARN("create operator input failed", K(ret), K(*this));
      } else if (OB_ISNULL(kit->input_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL input returned", K(ret));
      } else {
        kit->input_->set_deserialize_allocator(&exec_ctx.get_allocator());
        LOG_TRACE("trace create input", K(ret), K(id_), K(type_));
      }
    }
  }

  // create child operator
  if (OB_SUCC(ret) && create_child_cnt > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < create_child_cnt; i++) {
      if (nullptr == children_[i]) {
        // if child is nullptr, it means it's receive operator
        if (!IS_PX_RECEIVE(type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("only receive is leaf in px", K(ret), K(type_), K(id_));
        }
      } else if (OB_FAIL(children_[i]->create_op_input_recursive(exec_ctx))) {
        LOG_WARN("create operator failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOpSpec::create_operator(ObExecContext& exec_ctx, ObOperator*& op) const
{
  int ret = OB_SUCCESS;
  // Do some sanity check,
  // we no longer need to check the validity of those pointers in ObOperator.
  if (OB_ISNULL(GET_MY_SESSION(exec_ctx)) || OB_ISNULL(GET_PHY_PLAN_CTX(exec_ctx)) ||
      OB_ISNULL(GET_TASK_EXECUTOR_CTX(exec_ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(create_operator_recursive(exec_ctx, op))) {
    LOG_WARN("create operator recursive failed", K(ret));
  }
  LOG_TRACE("trace create operator", K(ret), K(lbt()));
  return ret;
}

int ObOpSpec::create_operator_recursive(ObExecContext& exec_ctx, ObOperator*& op) const
{
  int ret = OB_SUCCESS;
  ObOperatorKit* kit = exec_ctx.get_operator_kit(id_);
  int64_t create_child_cnt = child_cnt_;
  if (OB_ISNULL(kit) || (child_cnt_ > 0 && NULL == children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator kit should be created before create operator "
             "and children must be valid",
        K(ret),
        K(id_),
        KP(kit),
        KP(children_),
        K(create_child_cnt),
        K(type_));
  } else {
    kit->spec_ = this;
    LOG_TRACE("trace create spec", K(ret), K(id_), K(type_));
    for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; i++) {
      if (NULL == children_[i]) {
        // if child is nullptr, it means it's receive operator
        if (!IS_PX_RECEIVE(type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("only receive is leaf in px", K(ret), K(type_), K(id_));
        } else {
          create_child_cnt = 0;
        }
      } else if (IS_TRANSMIT(children_[i]->type_)) {
        OB_ASSERT(1 == child_cnt_);
        // only create context for current DFO
        create_child_cnt = 0;
      }
    }
  }

  // create operator input
  if (OB_SUCC(ret)) {
    // Operator input may created in scheduler, no need to create again.
    if (NULL == kit->input_ && ObOperatorFactory::has_op_input(type_)) {
      if (OB_FAIL(ObOperatorFactory::alloc_op_input(exec_ctx.get_allocator(), exec_ctx, *this, kit->input_))) {
        LOG_WARN("create operator input failed", K(ret), K(*this));
      } else if (OB_ISNULL(kit->input_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL input returned", K(ret));
      } else {
        LOG_TRACE("trace create input", K(ret), K(id_), K(type_));
      }
    }
  }

  // create operator
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOperatorFactory::alloc_operator(
            exec_ctx.get_allocator(), exec_ctx, *this, kit->input_, create_child_cnt, kit->op_)) ||
        OB_ISNULL(kit->op_)) {
      ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      LOG_WARN("create operator failed", K(ret), KP(kit->op_), K(*this));
    } else {
      op = kit->op_;
      op->get_monitor_info().set_operator_id(id_);
      op->get_monitor_info().set_operator_type(type_);
      op->get_monitor_info().set_plan_depth(plan_depth_);
      op->get_monitor_info().set_tenant_id(GET_MY_SESSION(exec_ctx)->get_effective_tenant_id());
      op->get_monitor_info().open_time_ = oceanbase::common::ObClockGenerator::getClock();
    }
  }

  // create child operator
  if (OB_SUCC(ret) && create_child_cnt > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < create_child_cnt; i++) {
      ObOperator* child_op = NULL;
      if (nullptr == children_[i]) {
        // if child is nullptr, it means it's receive operator
        if (!IS_PX_RECEIVE(type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("only receive is leaf in px", K(ret), K(type_), K(id_));
        }
      } else if (OB_FAIL(children_[i]->create_operator_recursive(exec_ctx, child_op))) {
        LOG_WARN("create operator failed", K(ret));
      } else if (OB_FAIL(op->set_child(i, child_op))) {
        LOG_WARN("set child operator failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOpSpec::accept(ObOpSpecVisitor& visitor) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(visitor.pre_visit(*this))) {
    LOG_WARN("failed to pre visit", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothingn
  } else if (OB_ISNULL(children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null children", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < child_cnt_; i++) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null children", K(ret));
    } else if (OB_FAIL(children_[i]->accept(visitor))) {
      LOG_WARN("failed to visit", K(ret));
    }
  }  // end for
  if (OB_SUCC(ret) && OB_FAIL(visitor.post_visit(*this))) {
    LOG_WARN("failed to post visit", K(ret));
  }
  return ret;
}

ObOperator::ObOperator(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : spec_(spec),
      ctx_(exec_ctx),
      eval_ctx_(*exec_ctx.get_eval_ctx()),
      eval_infos_(exec_ctx.get_allocator()),
      input_(input),
      parent_(NULL),
      children_(NULL),
      child_cnt_(0),
      left_(NULL),
      right_(NULL),
      try_check_times_(0),
      opened_(false),
      startup_passed_(spec_.startup_filters_.empty()),
      exch_drained_(false),
      got_first_row_(false)
{}

ObOperator::~ObOperator()
{
  ObOperator::destroy();
}

int ObOperator::set_children_pointer(ObOperator** children, const uint32_t child_cnt)
{
  int ret = OB_SUCCESS;
  if (child_cnt > 0 && NULL == children) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(child_cnt), KP(children));
  } else {
    children_ = children;
    child_cnt_ = child_cnt;
    if (child_cnt > 0) {
      child_ = children[0];
    } else {
      child_ = NULL;
    }
    if (child_cnt > 1) {
      right_ = children[1];
    } else {
      right_ = NULL;
    }
  }
  return ret;
}

int ObOperator::set_child(const uint32_t idx, ObOperator* child)
{
  int ret = OB_SUCCESS;
  if (idx >= child_cnt_ || OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), K(child_cnt_), KP(child));
  } else {
    children_[idx] = child;
    if (0 == idx) {
      child_ = child;
    }
    if (1 == idx) {
      right_ = child;
    }
    child_->parent_ = this;
  }
  return ret;
}

int ObOperator::init()
{
  return OB_SUCCESS;
}

// copy from ob_phy_operator.cpp
int ObOperator::open()
{
  int ret = OB_SUCCESS;
  OperatorOpenOrder open_order = get_operator_open_order();
  while (OB_SUCC(ret) && open_order != OPEN_EXIT) {
    switch (open_order) {
      case OPEN_CHILDREN_FIRST:
      case OPEN_CHILDREN_LATER: {
        for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; ++i) {
          // children_ pointer is checked before operator open, no need check again.
          if (OB_FAIL(children_[i]->open())) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
              LOG_WARN("Open child operator failed", K(ret), "op_type", op_name());
            }
          }
        }
        open_order = (OPEN_CHILDREN_FIRST == open_order) ? OPEN_SELF_LATER : OPEN_EXIT;
        break;
      }
      case OPEN_SELF_FIRST:
      case OPEN_SELF_LATER:
      case OPEN_SELF_ONLY: {
        if (OB_FAIL(init_evaluated_flags())) {
          LOG_WARN("init evaluate flags failed", K(ret));
        } else if (OB_FAIL(inner_open())) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("Open this operator failed", K(ret), "op_type", op_name());
          }
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
  }
  opened_ = true;
  LOG_DEBUG("open op", K(ret), "op_type", op_name(), "op_id", spec_.id_, K(open_order));
  return ret;
}

int ObOperator::init_evaluated_flags()
{
  int ret = OB_SUCCESS;
  if (!spec_.calc_exprs_.empty()) {
    if (OB_FAIL(eval_infos_.prepare_allocate(spec_.calc_exprs_.count()))) {
      LOG_WARN("init fixed array failed", K(ret));
    } else {
      for (int64_t i = 0; i < spec_.calc_exprs_.count(); i++) {
        eval_infos_.at(i) = &spec_.calc_exprs_.at(i)->get_eval_info(eval_ctx_);
      }
    }
  }
  return ret;
}

// copy from ob_phy_operator.cpp
int ObOperator::rescan()
{
  // rescan must reset the oeprator context to the state after call operator open()
  // for the general non-terminal operator, function rescan() is to call children rescan()
  // if you want to do more, you must rewrite this function
  // for the general terminal operator, function rescan() does nothing
  // you can rewrite it to complete special function
  int ret = OB_SUCCESS;

  startup_passed_ = spec_.startup_filters_.empty();

  for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; ++i) {
    if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN(
          "rescan child operator failed", K(ret), K(i), "op_type", op_name(), "child op_type", children_[i]->op_name());
    }
  }
  op_monitor_info_.rescan_times_++;
  return ret;
}

// copy from ob_phy_operator.cpp
int ObOperator::switch_iterator()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx is null", K(ret));
  } else if (plan_ctx->get_bind_array_count() <= 0) {
    ret = OB_ITER_END;
  } else {
    startup_passed_ = spec_.startup_filters_.empty();

    // Differ from ObPhyOperator::switch_iterator(), current binding array index is moved from
    // ObExprCtx to ObPhysicalPlanCtx, can not increase in Operator.
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; i++) {
    if (OB_FAIL(children_[i]->switch_iterator())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("switch child operator iterator failed", K(ret), K(op_name()), K(children_[i]->op_name()));
      }
    }
  }
  return ret;
}

// copy from ob_phy_operator.cpp
int ObOperator::close()
{
  int ret = OB_SUCCESS;
  int child_ret = OB_SUCCESS;
  OperatorOpenOrder open_order = get_operator_open_order();
  if (OPEN_SELF_ONLY != open_order && OPEN_NONE != open_order) {
    // first call close of children
    for (int64_t i = 0; i < child_cnt_; ++i) {
      // children_ pointer is checked before operator open, no need check again.
      int tmp_ret = children_[i]->close();
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("Close child operator failed", K(child_ret), "op_type", op_name());
      }
    }
  }

  if (OPEN_NONE != open_order) {
    // no matter what, must call operator's close function
    int tmp_ret = inner_close();
    if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;  // overwrite child's error code.
      LOG_WARN("Close this operator failed", K(ret), "op_type", op_name());
    }
    if (GCONF.enable_sql_audit) {
      op_monitor_info_.close_time_ = oceanbase::common::ObClockGenerator::getClock();
      ObPlanMonitorNodeList* list = MTL_GET(ObPlanMonitorNodeList*);
      if (OB_LIKELY(nullptr != list && ctx_.get_my_session()->is_user_session() &&
                    OB_PHY_PLAN_LOCAL != spec_.plan_->get_plan_type() &&
                    OB_PHY_PLAN_REMOTE != spec_.plan_->get_plan_type())) {
        IGNORE_RETURN list->submit_node(op_monitor_info_);
        LOG_DEBUG("debug monitor", K(spec_.id_));
      }
    }
  }

  return ret;
}

int ObOperator::get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!startup_passed_)) {
    bool filtered = false;
    if (OB_FAIL(startup_filter(filtered))) {
      LOG_WARN("do startup filter failed", K(ret), "op", op_name());
    } else {
      if (filtered) {
        ret = OB_ITER_END;
      } else {
        startup_passed_ = true;
      }
    }
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(inner_get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("inner get next row failed", K(ret), "type", spec_.type_, "op", op_name());
      }
    } else {
      if (!spec_.filters_.empty()) {
        bool filtered = false;
        if (OB_FAIL(filter_row(filtered))) {
          LOG_WARN("filter row failed", K(ret), "type", spec_.type_, "op", op_name());
        } else {
          if (filtered) {
            continue;
          }
        }
      }
    }
    break;
  }

  if (OB_SUCCESS == ret) {
    op_monitor_info_.output_row_count_++;
    if (!got_first_row_) {
      op_monitor_info_.first_row_time_ = oceanbase::common::ObClockGenerator::getClock();
      ;
      got_first_row_ = true;
    }
  } else if (OB_ITER_END == ret) {
    int tmp_ret = drain_exch();
    if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("drain exchange data failed", K(tmp_ret));
    }
    if (got_first_row_) {
      op_monitor_info_.last_row_time_ = oceanbase::common::ObClockGenerator::getClock();
    }
  }
  return ret;
}

int ObOperator::filter(const common::ObIArray<ObExpr*>& exprs, bool& filtered)
{
  ObDatum* datum = NULL;
  int ret = OB_SUCCESS;
  filtered = false;
  FOREACH_CNT_X(e, exprs, OB_SUCC(ret))
  {
    OB_ASSERT(NULL != *e);
    if (OB_FAIL((*e)->eval(eval_ctx_, datum))) {
      LOG_WARN("expr evaluate failed", K(ret), "expr", *e);
    } else {
      OB_ASSERT(ob_is_int_tc((*e)->datum_meta_.type_));
      if (datum->null_ || 0 == *datum->int_) {
        filtered = true;
        break;
      }
    }
  }
  return ret;
}

// copy ObPhyOperator::drain_exch
int ObOperator::drain_exch()
{
  int ret = OB_SUCCESS;
  /**
   * 1. try to open this operator
   * 2. try to drain all children
   */
  if (OB_FAIL(try_open())) {
    LOG_WARN("fail to open operator", K(ret));
  } else if (!exch_drained_) {
    exch_drained_ = true;
    for (int64_t i = 0; i < child_cnt_ && OB_SUCC(ret); i++) {
      if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL child found", K(ret), K(i));
      } else if (OB_FAIL(children_[i]->drain_exch())) {
        LOG_WARN("drain exch failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOperator::get_real_child(ObOperator*& child, const int32_t child_idx)
{
  int ret = OB_SUCCESS;
  const int32_t first_child_idx = 0;
  ObOperator* first_child = nullptr;
  if (first_child_idx >= child_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid child idx", K(ret), K(child_cnt_));
  } else if (OB_ISNULL(first_child = get_child(first_child_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null child", K(ret));
  } else if (IS_DUMMY_PHY_OPERATOR(first_child->get_spec().get_type())) {
    if (OB_FAIL(first_child->get_real_child(child, child_idx))) {
      LOG_WARN("Failed to get real child", K(ret), K(first_child->get_spec().get_type()));
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

int ObOperator::check_status()
{
  return ctx_.check_status();
}

}  // end namespace sql
}  // end namespace oceanbase
