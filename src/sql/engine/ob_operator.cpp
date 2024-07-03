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

#include "share/rc/ob_tenant_base.h"
#include "ob_operator.h"
#include "ob_operator_factory.h"
#include "sql/engine/ob_exec_context.h"
#include "common/ob_smart_call.h"
#include "sql/engine/ob_exec_feedback_info.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER(ObDynamicParamSetter, param_idx_, src_, dst_);
OB_SERIALIZE_MEMBER(ObOpSchemaObj, obj_type_, is_not_null_, order_type_);

int ObDynamicParamSetter::set_dynamic_param(ObEvalCtx &eval_ctx) const
{
  int ret = OB_SUCCESS;
  ObDatum *res = NULL;
  if (OB_ISNULL(src_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr not init", K(ret), KP(src_));
  } else if (OB_FAIL(src_->eval(eval_ctx, res))) {
    LOG_WARN("fail to calc rescan params", K(ret), K(*this));
  } else if (OB_FAIL(update_dynamic_param(eval_ctx,*res))) {
    LOG_WARN("update dynamic param store failed", K(ret));
  }
  return ret;
}

int ObDynamicParamSetter::set_dynamic_param(ObEvalCtx &eval_ctx, ObObjParam *&param) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_ctx = eval_ctx.exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(phy_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null phy ctx", K(ret), KP(phy_ctx));
  } else if (OB_FAIL(set_dynamic_param(eval_ctx))) {
    LOG_WARN("update dynamic param store failed", K(ret));
  } else {
    ParamStore &param_store = phy_ctx->get_param_store_for_update();
    param = &param_store.at(param_idx_);
  }
  return ret;
}

int ObDynamicParamSetter::update_dynamic_param(ObEvalCtx &eval_ctx, ObDatum &datum) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_ctx = eval_ctx.exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(phy_ctx) || OB_ISNULL(dst_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(phy_ctx), KP(dst_));
  } else {
    clear_parent_evaluated_flag(eval_ctx, *dst_);
    ObDatum &param_datum = dst_->locate_expr_datum(eval_ctx);
    dst_->get_eval_info(eval_ctx).evaluated_ = true;
    if (0 == dst_->res_buf_off_) {
      // for compat, old server don't have ref buf for dynamic expr,
      // so keep shallow copy
      param_datum.set_datum(datum);
    } else {
      if (OB_FAIL(dst_->deep_copy_datum(eval_ctx, datum))) {
        LOG_WARN("fail to deep copy datum", K(ret), K(eval_ctx), K(*dst_));
      }
    }
    //初始化param store, 用于query range计算
    ParamStore &param_store = phy_ctx->get_param_store_for_update();
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(param_idx_ < 0 || param_idx_ >= param_store.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(param_idx_), K(param_store.count()));
    } else if (OB_FAIL(param_datum.to_obj(param_store.at(param_idx_),
                                   dst_->obj_meta_,
                                   dst_->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret), "datum",
               DATUM2STR(*dst_, param_datum));
    } else {
      param_store.at(param_idx_).set_param_meta();
    }
  }

  return ret;
}

void ObDynamicParamSetter::clear_parent_evaluated_flag(ObEvalCtx &eval_ctx, ObExpr &expr)
{
  for (int64_t i = 0; i < expr.parent_cnt_; i++) {
    clear_parent_evaluated_flag(eval_ctx, *expr.parents_[i]);
  }
  expr.get_eval_info(eval_ctx).clear_evaluated_flag();
}

ObOpSpec::ObOpSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
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
    plan_depth_(0),
    max_batch_size_(0),
    need_check_output_datum_(false),
    use_rich_format_(false),
    compress_type_(NONE_COMPRESSOR)
{
}

ObOpSpec::~ObOpSpec()
{
}

OB_SERIALIZE_MEMBER(ObOpSpec,
                    id_,
                    output_,
                    startup_filters_,
                    filters_,
                    calc_exprs_,
                    cost_,
                    rows_,
                    width_,
                    px_est_size_factor_,
                    plan_depth_,
                    max_batch_size_,
                    need_check_output_datum_,
                    use_rich_format_,
                    compress_type_);

DEF_TO_STRING(ObOpSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("name", op_name(),
       K_(type),
       K_(id),
       K_(child_cnt),
       "output_cnt", output_.count(),
       "startup_filters_cnt", startup_filters_.count(),
       "calc_exprs_cnt", calc_exprs_.count(),
       K_(rows),
       K_(max_batch_size),
       K_(filters),
       K_(use_rich_format),
       K_(compress_type));
  J_OBJ_END();
  return pos;
}

int ObOpSpec::set_children_pointer(ObOpSpec **children, const uint32_t child_cnt)
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

int ObOpSpec::set_child(const uint32_t idx, ObOpSpec *child)
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

int ObOpSpec::create_op_input(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  // Do some sanity check,
  // we no longer need to check the validity of those pointers in ObOperator.
  if (OB_ISNULL(GET_MY_SESSION(exec_ctx))
      || OB_ISNULL(GET_PHY_PLAN_CTX(exec_ctx))
      || OB_ISNULL(GET_TASK_EXECUTOR_CTX(exec_ctx))) {
  } else if (OB_FAIL(create_op_input_recursive(exec_ctx))) {
    LOG_WARN("create operator recursive failed", K(ret));
  }
  LOG_TRACE("trace create input", K(ret), K(lbt()));
  return ret;
}

int ObOpSpec::create_op_input_recursive(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObOperatorKit *kit = exec_ctx.get_operator_kit(id_);
  int64_t create_child_cnt = child_cnt_;
  if (OB_ISNULL(kit) || (child_cnt_ > 0 && NULL == children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator kit should be created before create operator "
             "and children must be valid",
             K(ret), KP(kit), K(id_), KP(children_), K(create_child_cnt), K(type_));
  } else {
    kit->spec_ = this;
    LOG_TRACE("trace create input", K(ret), K(id_), K(type_), K(lbt()));
  }

  // create operator input
  if (OB_SUCC(ret)) {
    // Operator input may created in scheduler, no need to create again.
    if (NULL == kit->input_ && ObOperatorFactory::has_op_input(type_)) {
      if (OB_FAIL(ObOperatorFactory::alloc_op_input(
                  exec_ctx.get_allocator(), exec_ctx, *this, kit->input_))) {
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
        // 这里如果有child但为nullptr，说明是receive算子
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

int ObOpSpec::create_operator(ObExecContext &exec_ctx, ObOperator *&op) const
{
  int ret = OB_SUCCESS;
  ObMonitorNode *pre_node = nullptr;
  // Do some sanity check,
  // we no longer need to check the validity of those pointers in ObOperator.
  if (OB_ISNULL(GET_MY_SESSION(exec_ctx))
      || OB_ISNULL(GET_PHY_PLAN_CTX(exec_ctx))
      || OB_ISNULL(GET_TASK_EXECUTOR_CTX(exec_ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(create_operator_recursive(exec_ctx, op))) {
    LOG_WARN("create operator recursive failed", K(ret));
  } else if (OB_FAIL(link_sql_plan_monitor_node_recursive(exec_ctx, pre_node))) {
    LOG_WARN("fail to link sql plan monitor node recursive", K(ret));
  } else if (OB_FAIL(create_exec_feedback_node_recursive(exec_ctx))) {
    LOG_WARN("fail to create exec feedback node", K(ret));
  }
  LOG_DEBUG("trace create operator", K(ret), K(lbt()));
  return ret;
}

int ObOpSpec::create_operator_recursive(ObExecContext &exec_ctx, ObOperator *&op) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else {
    ObOperatorKit *kit = exec_ctx.get_operator_kit(id_);
    int64_t create_child_cnt = child_cnt_;
    if (OB_ISNULL(kit) || (child_cnt_ > 0 && NULL == children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator kit should be created before create operator "
              "and children must be valid",
              K(ret), K(id_), KP(kit), KP(children_), K(create_child_cnt), K(type_));
    } else {
      kit->spec_ = this;
      LOG_DEBUG("trace create spec", K(ret), K(id_), K(type_));
      for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; i++) {
        if (NULL == children_[i]) {
          // 这里如果有child但为nullptr，说明是receive算子
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
        if (OB_FAIL(ObOperatorFactory::alloc_op_input(
                    exec_ctx.get_allocator(), exec_ctx, *this, kit->input_))) {
          LOG_WARN("create operator input failed", K(ret), K(*this));
        } else if (OB_ISNULL(kit->input_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL input returned", K(ret));
        } else {
          LOG_DEBUG("trace create input", K(ret), K(id_), K(type_));
        }
      }
    }

    // create operator
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOperatorFactory::alloc_operator(
                  exec_ctx.get_allocator(), exec_ctx, *this,
                  kit->input_, create_child_cnt, kit->op_))
          || OB_ISNULL(kit->op_)) {
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
        LOG_WARN("create operator failed", K(ret), KP(kit->op_), K(*this));
      } else {
        op = kit->op_;
        op->get_monitor_info().set_operator_id(id_);
        op->get_monitor_info().set_operator_type(type_);
        op->get_monitor_info().set_plan_depth(plan_depth_);
        op->get_monitor_info().set_tenant_id(GET_MY_SESSION(exec_ctx)->get_effective_tenant_id());
        op->get_monitor_info().open_time_ = oceanbase::common::ObClockGenerator::getClock();
        op->get_monitor_info().set_rich_format(use_rich_format_);
      }
    }

    // create child operator
    if (OB_SUCC(ret)) {
      if (create_child_cnt > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < create_child_cnt; i++) {
          ObOperator *child_op = NULL;
          if (nullptr == children_[i]) {
            // 这里如果有child但为nullptr，说明是receive算子
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
      } else if (child_cnt_ > 0) {
        if (OB_FAIL(assign_spec_ptr_recursive(exec_ctx))) {
          LOG_WARN("assign spec ptr failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObOpSpec::link_sql_plan_monitor_node_recursive(ObExecContext &exec_ctx, ObMonitorNode *&pre_node) const
{
  int ret = OB_SUCCESS;
  ObOperatorKit *kit = exec_ctx.get_operator_kit(id_);
  if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
    LOG_TRACE("operator kit is NULL", K(ret));
  } else if (OB_NOT_NULL(kit->op_->get_monitor_info().get_next()) ||
             OB_NOT_NULL(kit->op_->get_monitor_info().get_prev())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur monitor info is unexpected", K(ret));
  } else if (OB_ISNULL(pre_node)) {
    pre_node = &(kit->op_->get_monitor_info());
  } else {
    pre_node->add_rt_monitor_node(&(kit->op_->get_monitor_info()));
    pre_node = &(kit->op_->get_monitor_info());
  }
  for (int i = 0; OB_SUCC(ret) && i < child_cnt_; ++i) {
    if (nullptr == children_[i]) {
      continue;
    } else if (OB_FAIL(SMART_CALL(children_[i]->link_sql_plan_monitor_node_recursive(
        exec_ctx, pre_node)))) {
      LOG_WARN("fail to link sql plan monitor", K(ret));
    }
  }
  return ret;
}

int ObOpSpec::create_exec_feedback_node_recursive(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObOperatorKit *kit = exec_ctx.get_operator_kit(id_);
  if (OB_ISNULL(plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan is null", K(ret));
  } else if (!plan_->need_record_plan_info()) {
  } else if (OB_ISNULL(kit)) {
    LOG_TRACE("operator kit is NULL", K(ret));
  } else {
    ObExecFeedbackInfo &fb_info = exec_ctx.get_feedback_info();
    ObExecFeedbackNode node(id_);
    if (OB_FAIL(fb_info.add_feedback_node(node))) {
      LOG_WARN("fail to add feedback node", K(ret));
    } else if (OB_NOT_NULL(kit->op_)) {
      common::ObIArray<ObExecFeedbackNode> &nodes = fb_info.get_feedback_nodes();
      kit->op_->set_feedback_node_idx(nodes.count() - 1);
    }
    for (int i = 0; OB_SUCC(ret) && i < child_cnt_; ++i) {
      if (nullptr == children_[i]) {
        continue;
      } else if (OB_FAIL(SMART_CALL(children_[i]->create_exec_feedback_node_recursive(
            exec_ctx)))) {
        LOG_WARN("fail to link sql plan monitor", K(ret));
      }
    }
  }

  return ret;
}

int ObOpSpec::assign_spec_ptr_recursive(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObOperatorKit *kit = exec_ctx.get_operator_kit(id_);
  if (NULL == kit) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator kit is NULL", K(ret));
  } else {
    kit->spec_ = this;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; i++) {
      if (OB_FAIL(children_[i]->assign_spec_ptr_recursive(exec_ctx))) {
        LOG_WARN("assign spec ptr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOpSpec::accept(ObOpSpecVisitor &visitor) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(visitor.pre_visit(*this))) {
    LOG_WARN("failed to pre visit", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
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
  } // end for
  if (OB_SUCC(ret) && OB_FAIL(visitor.post_visit(*this))) {
    LOG_WARN("failed to post visit", K(ret));
  }
  return ret;
}

ObOperator::ObOperator(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : spec_(spec),
    ctx_(exec_ctx),
    eval_ctx_(exec_ctx),
    eval_infos_(exec_ctx.get_allocator()),
    input_(input),
    parent_(NULL),
    children_(NULL),
    child_cnt_(0),
    left_(NULL),
    right_(NULL),
    try_check_tick_(0),
    try_monitor_tick_(0),
    opened_(false),
    startup_passed_(spec_.startup_filters_.empty()),
    exch_drained_(false),
    got_first_row_(false),
    need_init_before_get_row_(true),
    fb_node_idx_(OB_INVALID_INDEX),
    io_event_observer_(op_monitor_info_),
    cpu_begin_time_(0),
    total_time_(0),
    batch_reach_end_(false),
    row_reach_end_(false),
    output_batches_b4_rescan_(0),
    #ifdef ENABLE_DEBUG_LOG
    dummy_mem_context_(nullptr),
    dummy_ptr_(nullptr),
    #endif
    check_stack_overflow_(false)
{
  eval_ctx_.max_batch_size_ = spec.max_batch_size_;
  eval_ctx_.batch_size_ = spec.max_batch_size_;
}

ObOperator::~ObOperator()
{
  ObOperator::destroy();
}

int ObOperator::set_children_pointer(ObOperator **children, const uint32_t child_cnt)
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

int ObOperator::set_child(const uint32_t idx, ObOperator *child)
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

int ObOperator::check_stack_once()
{
  int ret = OB_SUCCESS;
  if (check_stack_overflow_) {
  } else if (OB_FAIL(common::check_stack_overflow())) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else {
    check_stack_overflow_ = true;
  }
  return ret;
}

int ObOperator::output_expr_sanity_check()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < spec_.output_.count(); ++i) {
    ObDatum *datum = NULL;
    const ObExpr *expr = spec_.output_[i];
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, expr is nullptr", K(ret));
    } else if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
      LOG_WARN("evaluate expression failed", K(ret));
    } else {
      SANITY_CHECK_RANGE(datum->ptr_, datum->len_);
    }
  }
  return ret;
}

int ObOperator::output_expr_sanity_check_batch()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < spec_.output_.count(); ++i) {
    const ObExpr *expr = spec_.output_[i];
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, expr is nullptr", K(ret));
    } else if (OB_FAIL(expr->eval_vector(eval_ctx_, brs_))) {
      LOG_WARN("eval vector failed", K(ret));
    } else {
      VectorFormat vec_fmt = expr->get_format(eval_ctx_);
      ObIVector *ivec = expr->get_vector(eval_ctx_);
      if (vec_fmt == VEC_UNIFORM || vec_fmt == VEC_UNIFORM_CONST) {
        ObUniformBase *uni_data = static_cast<ObUniformBase *>(ivec);
        if (vec_fmt == VEC_UNIFORM_CONST) {
          if (brs_.skip_->accumulate_bit_cnt(brs_.size_) < brs_.size_) {
            ObDatum &datum = uni_data->get_datums()[0];
            SANITY_CHECK_RANGE(datum.ptr_, datum.len_);
          }
        } else {
          ObDatum *datums = uni_data->get_datums();
          for (int j = 0; j < brs_.size_; j++) {
            if (!brs_.skip_->at(j)) {
              SANITY_CHECK_RANGE(datums[j].ptr_, datums[j].len_);
            }
          }
        }
      } else if (vec_fmt == VEC_FIXED) {
        ObFixedLengthBase *fixed_data = static_cast<ObFixedLengthBase *>(ivec);
        ObBitmapNullVectorBase *nulls = static_cast<ObBitmapNullVectorBase *>(ivec);
        int32_t len = fixed_data->get_length();
        for (int j = 0; j < brs_.size_; j++) {
          if (!brs_.skip_->at(j) && !nulls->is_null(j)) {
            SANITY_CHECK_RANGE(fixed_data->get_data() + j * len, len);
          }
        }
      } else if (vec_fmt == VEC_DISCRETE) {
        ObDiscreteBase *dis_data = static_cast<ObDiscreteBase *>(ivec);
        ObBitmapNullVectorBase *nulls = static_cast<ObBitmapNullVectorBase *>(ivec);
        char **ptrs = dis_data->get_ptrs();
        ObLength *lens = dis_data->get_lens();
        for (int j = 0; j < brs_.size_; j++) {
          if (!brs_.skip_->at(j) && !nulls->is_null(j)) {
            SANITY_CHECK_RANGE(ptrs[j], lens[j]);
          }
        }
      } else if (vec_fmt == VEC_CONTINUOUS) {
        ObContinuousBase *cont_base = static_cast<ObContinuousBase *>(ivec);
        ObBitmapNullVectorBase *nulls = static_cast<ObBitmapNullVectorBase *>(ivec);
        uint32_t *offsets = cont_base->get_offsets();
        char *data = cont_base->get_data();
        for (int j = 0; j < brs_.size_; j++) {
          if (!brs_.skip_->at(j) && !nulls->is_null(j)) {
            SANITY_CHECK_RANGE(data + offsets[j], offsets[j + 1] - offsets[j]);
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected format", K(ret), K(vec_fmt));
      }
    }
  }
  return ret;
}

int ObOperator::output_expr_decint_datum_len_check()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < spec_.output_.count(); ++i) {
    ObDatum *datum = NULL;
    const ObExpr *expr = spec_.output_[i];
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, expr is nullptr", K(ret));
    } else if (!ob_is_decimal_int(expr->datum_meta_.get_type())) {
      // do nothing
    } else if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
      LOG_WARN("evaluate expression failed", K(ret));
    } else {
      const int16_t precision = expr->datum_meta_.precision_;
      if (OB_UNLIKELY(precision < 0)) {
        LOG_WARN("the precision of decimal int expr is unknown", K(ret), K(precision), K(*expr));
      } else if (!datum->is_null() && datum->len_ != 0) {
        const int len = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
        OB_ASSERT (len == datum->len_);
      }
    }
  }
  return ret;
}

int ObOperator::output_expr_decint_datum_len_check_batch()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < spec_.output_.count(); ++i) {
    const ObExpr *expr = spec_.output_[i];
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, expr is nullptr", K(ret));
    } else if (!ob_is_decimal_int(expr->datum_meta_.get_type())) {
      // do nothing
    } else {
      const int16_t precision = expr->datum_meta_.precision_;
      const int len = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
      if (OB_UNLIKELY(precision < 0)) {
        LOG_WARN("the precision of decimal int expr is unknown", K(ret), K(precision), K(*expr));
      } else if (OB_FAIL(expr->eval_batch(eval_ctx_, *brs_.skip_, brs_.size_))) {
        LOG_WARN("evaluate expression failed", K(ret));
      } else if (!expr->is_batch_result()) {
        const ObDatum &datum = expr->locate_expr_datum(eval_ctx_);
        if (!datum.is_null() && datum.len_ != 0) {
          OB_ASSERT (len == datum.len_);
        }
      } else {
        const ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
        for (int64_t j = 0; j < brs_.size_; j++) {
          if (!brs_.skip_->at(j) && !datums[j].is_null()) {
            OB_ASSERT (len == datums[j].len_);
          }
        }
      }
    }
  }
  return ret;
}

// copy from ob_phy_operator.cpp
int ObOperator::open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else {
    OperatorOpenOrder open_order = get_operator_open_order();
    if (!spec_.is_vectorized()) {
    /*
        for non-vectorized operator, need set batch size 1;

        case: vectorize.select_basic_vec_oracle
        OceanBase(TEST@TEST)>explain insert into dlt6 select * from dlt4\G
        *************************** 1. row ***************************
        Query Plan: =======================================
        |ID|OPERATOR     |NAME |EST. ROWS|COST|
        ---------------------------------------
        |0 |INSERT       |     |7        |97  |
        |1 | SUBPLAN SCAN|VIEW1|7        |46  |
        |2 |  TABLE SCAN |DLT4 |7        |46  |
        =======================================
        when vectorizition enable,  batch_result_ of column_conv expr in insert_op is true,
        if batch size of insert eval_ctx  is 0, when column_conv eval, reset_ptr will do nothing,
        caused datum ptr is null, and core when execution;
      */
      eval_ctx_.set_batch_size(1);
      eval_ctx_.set_batch_idx(0);
    }
    if (ctx_.get_my_session()->is_user_session() || spec_.plan_->get_phy_plan_hint().monitor_) {
      IGNORE_RETURN try_register_rt_monitor_node(0);
    }
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
        } else if (OB_FAIL(init_skip_vector())) {
          LOG_WARN("init skip vector failed", K(ret));
        }
        #ifdef ENABLE_DEBUG_LOG
        else if (OB_FAIL(init_dummy_mem_context(ctx_.get_my_session()->get_effective_tenant_id()))) {
          LOG_WARN("failed to get mem context", K(ret));
        } else if (OB_LIKELY(nullptr == dummy_ptr_)
                && OB_ISNULL(dummy_ptr_ = static_cast<char *>(dummy_mem_context_->get_malloc_allocator().alloc(sizeof(char))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        }
        #endif
        else if (OB_FAIL(inner_open())) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("Open this operator failed", K(ret), "op_type", op_name());
          }
        }
        open_order = (OPEN_SELF_FIRST == open_order) ? OPEN_CHILDREN_LATER : OPEN_EXIT;
        break;
      }
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
    if (OB_SUCC(ret)) {
      opened_ = true;
      clear_batch_end_flag();
      if (spec_.need_check_output_datum_) {
        void * ptr = ctx_.get_allocator().alloc(sizeof(ObBatchResultHolder));
        if (OB_ISNULL(ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocation failed for brs_checker_", K(ret), K(sizeof(ObBatchResultHolder)));
        } else {
          // replace new the object
          brs_checker_ = new(ptr) ObBatchResultHolder();
          if (OB_FAIL(brs_checker_->init(spec_.output_, eval_ctx_))) {
            LOG_WARN("brs_holder init failed", K(ret));
          }
        }
      }
    }

    LOG_DEBUG("open op", K(ret), "op_type", op_name(), "op_id", spec_.id_, K(open_order));
  }
  return ret;
}

void ObOperator::reset_output_format()
{
  if (spec_.plan_->get_use_rich_format() && !spec_.use_rich_format_) {
    FOREACH_CNT(e, spec_.output_) {
      (*e)->get_vector_header(eval_ctx_).format_ = VEC_INVALID;
    }
  }
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

// default batch_size is 1
int ObOperator::init_skip_vector()
{
  int ret = OB_SUCCESS;
  if (spec_.get_phy_plan()->is_vectorized() && NULL == brs_.skip_) {
    int batch_size = 0;
    if (spec_.max_batch_size_ > 0) {
      batch_size = spec_.max_batch_size_;
    } else {
      batch_size = 1;
    }
    void *mem = ctx_.get_allocator().alloc(ObBitVector::memory_size(batch_size));
    if (OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      brs_.skip_ = to_bit_vector(mem);
      brs_.skip_->init(batch_size);
    }
  }
  return ret;
}

// copy from ob_phy_operator.cpp
int ObOperator::rescan()
{
  //rescan must reset the operator context to the state after call operator open()
  //for the general non-terminal operator, function rescan() is to call children rescan()
  //if you want to do more, you must rewrite this function
  //for the general terminal operator, function rescan() does nothing
  //you can rewrite it to complete special function
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; ++i) {
    if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("rescan child operator failed",
               K(ret), K(i), "op_type", op_name(),
               "child op_type", children_[i]->op_name());
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(inner_rescan())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to inner rescan", K(ret));
    }
  } else {
#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif
  }

  return ret;
}

int ObOperator::inner_rescan()
{
  int ret = OB_SUCCESS;
  startup_passed_ = spec_.startup_filters_.empty();
  if (br_it_) {
    br_it_->rescan();
  }
  // If an operator rescan after drained, the exch_drained_ must be reset
  // so it can be drained again.
  exch_drained_ = false;
  batch_reach_end_ = false;
  row_reach_end_ = false;
  clear_batch_end_flag();
  // For the vectorization 2.0, when rescan, upper operator may have changed expr format,
  // but it has not been changed back, and the old operator will not initialize the format.
  // When calling cast to uniform again, The current format may unexpected, and caused cast error;
  // so when rescan, for operator which not support rich format, we reset the output format
  reset_output_format();
  op_monitor_info_.rescan_times_++;
  output_batches_b4_rescan_ = op_monitor_info_.output_batches_;
  if (spec_.need_check_output_datum_ && brs_checker_) {
    brs_checker_->reset();
  }
  return ret;
}

// copy from ob_phy_operator.cpp
int ObOperator::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_switch_iterator())) {
    LOG_WARN("failed to inner switch iterator", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; i++) {
    if (OB_FAIL(children_[i]->switch_iterator())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("switch child operator iterator failed",
                 K(ret), K(op_name()), K(children_[i]->op_name()));
      }
    }
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObOperator::inner_switch_iterator()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
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
  clear_batch_end_flag();
  return ret;
}

bool ObOperator::match_rt_monitor_condition(int64_t rows)
{
  bool ret = false;
  if (OB_ISNULL(spec_.plan_)) {
  } else if (spec_.plan_->get_phy_plan_hint().monitor_) {
    ret = true;
  } else if (spec_.plan_->get_px_dop() > 1) {
    ret = true;
  } else {
    try_monitor_tick_ += rows;
    if (try_monitor_tick_ > REAL_TIME_MONITOR_TRY_TIMES) {
      try_monitor_tick_ = 0;
      int64_t cur_time = oceanbase::common::ObClockGenerator::getClock();
      if (cur_time - ctx_.get_plan_start_time() > REAL_TIME_MONITOR_THRESHOLD) {
        ret = true;
      }
    }
  }
  return ret;
}

int ObOperator::try_register_rt_monitor_node(int64_t rows)
{
  int ret = OB_SUCCESS;
  if (!GCONF.enable_sql_audit) {
    // do nothing
  } else if (!ctx_.is_rt_monitor_node_registered() &&
              match_rt_monitor_condition(rows)) {
    ObPlanMonitorNodeList *list = MTL(ObPlanMonitorNodeList*);
    const ObOpSpec *root_spec = spec_.plan_->get_root_op_spec();
    if (OB_ISNULL(list) || OB_ISNULL(root_spec)) {
      /*do nothing*/
    } else {
      ObOperatorKit *kit = ctx_.get_operator_kit(root_spec->id_);
      if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
        /*do nothing*/
      } else if (OB_FAIL(list->register_monitor_node(kit->op_->get_monitor_info()))) {
        LOG_WARN("fail to register monitor node", K(ret));
      } else {
        ctx_.set_register_op_id(root_spec->id_);
      }
    }
  }
  return ret;
}

int ObOperator::try_deregister_rt_monitor_node()
{
  int ret = OB_SUCCESS;
  if (spec_.id_ == ctx_.get_register_op_id()
      && ctx_.is_rt_monitor_node_registered()) {
    ObPlanMonitorNodeList *list = MTL(ObPlanMonitorNodeList*);
    if (OB_ISNULL(list)) {
      // ignore ret
      LOG_WARN("fail to revert monitor node", K(list));
    } else if (OB_FAIL(list->revert_monitor_node(op_monitor_info_))) {
      LOG_ERROR("fail to revert monitor node", K(ret), K(op_monitor_info_));
    } else {
      ctx_.set_register_op_id(OB_INVALID_ID);
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
  if (OPEN_SELF_ONLY != open_order) {
    //first call close of children
    for (int64_t i = 0; i < child_cnt_; ++i) {
      // children_ pointer is checked before operator open, no need check again.
      int tmp_ret = SMART_CALL(children_[i]->close());
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("Close child operator failed", K(child_ret), "op_type", op_name());
      }
    }
  }

  // no matter what, must call operator's close function
  int tmp_ret = inner_close();
  if (OB_SUCCESS != tmp_ret) {
    ret = tmp_ret; // overwrite child's error code.
    LOG_WARN("Close this operator failed", K(ret), "op_type", op_name());
  }
  IGNORE_RETURN submit_op_monitor_node();
  IGNORE_RETURN setup_op_feedback_info();
  #ifdef ENABLE_DEBUG_LOG
    if (nullptr != dummy_mem_context_) {
      if (nullptr != dummy_ptr_) {
        dummy_mem_context_->get_malloc_allocator().free(dummy_ptr_);
        dummy_ptr_ = nullptr;
      }
    }
  #endif
  return ret;
}

int ObOperator::setup_op_feedback_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(spec_.plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan is null", K(ret));
  } else if (!spec_.plan_->need_record_plan_info() ||
             OB_INVALID_INDEX == fb_node_idx_) {
  } else {
    ObExecFeedbackInfo &fb_info = ctx_.get_feedback_info();
    common::ObIArray<ObExecFeedbackNode> &nodes = fb_info.get_feedback_nodes();
    int64_t &total_db_time = fb_info.get_total_db_time();
    total_db_time +=  op_monitor_info_.db_time_;
    if (fb_node_idx_ >= 0 && fb_node_idx_ < nodes.count()) {
      uint64_t cpu_khz = OBSERVER.get_cpu_frequency_khz();
      ObExecFeedbackNode &node = nodes.at(fb_node_idx_);
      node.block_time_ = op_monitor_info_.block_time_;
      node.block_time_ /= cpu_khz;
      node.db_time_ = op_monitor_info_.db_time_;
      node.db_time_ /= cpu_khz;
      node.op_close_time_ = op_monitor_info_.close_time_;
      node.op_first_row_time_ = op_monitor_info_.first_row_time_;
      node.op_last_row_time_ = op_monitor_info_.last_row_time_;
      node.op_open_time_ = op_monitor_info_.open_time_;
      node.output_row_count_ = op_monitor_info_.output_row_count_;
      node.worker_count_ = 1;
    }
  }
  return ret;
}

int ObOperator::submit_op_monitor_node()
{
  int ret = OB_SUCCESS;
  if (GCONF.enable_sql_audit) {
    // Record monitor info in sql_plan_monitor
    // Some records that meets the conditions needs to be archived
    // Reference document:
    op_monitor_info_.close_time_ = oceanbase::common::ObClockGenerator::getClock();
    ObPlanMonitorNodeList *list = MTL(ObPlanMonitorNodeList*);

    // exclude time cost in children, but px receive have no real children in exec view
    int64_t db_time = total_time_; // use temp var to avoid dis-order close
    if (!spec_.is_receive()) {
      for (int64_t i = 0; i < child_cnt_; i++) {
        db_time -= children_[i]->total_time_;
      }
    }
    if (db_time < 0) {
      db_time = 0;
    }
    // exclude io time cost
    // Change to divide by cpu_khz when generating the virtual table.
    // Otherwise, the unit of this field is inconsistent during SQL execution and after SQL execution is completed.
    op_monitor_info_.db_time_ = 1000 * db_time;
    op_monitor_info_.block_time_ = 1000 * op_monitor_info_.block_time_;

    if (list && spec_.plan_) {
      if (spec_.plan_->get_phy_plan_hint().monitor_
          || (ctx_.get_my_session()->is_user_session()
              && (spec_.plan_->get_px_dop() > 1
                  || (op_monitor_info_.close_time_
                      - ctx_.get_plan_start_time()
                      > MONITOR_RUNNING_TIME_THRESHOLD)))) {
        IGNORE_RETURN list->submit_node(op_monitor_info_);
        LOG_DEBUG("debug monitor", K(spec_.id_));
      }
    }
  }
  IGNORE_RETURN try_deregister_rt_monitor_node();
  return ret;
}

int ObOperator::get_next_row()
{
  int ret = OB_SUCCESS;
  begin_cpu_time_counting();
  begin_ash_line_id_reg();
  if (OB_FAIL(check_stack_once())) {
    LOG_WARN("too deep recursive", K(ret));
  } else {
    if (ctx_.get_my_session()->is_user_session() || spec_.plan_->get_phy_plan_hint().monitor_) {
      IGNORE_RETURN try_register_rt_monitor_node(1);
    }
    if (row_reach_end_) {
      ret = OB_ITER_END;
    } else if (OB_UNLIKELY(get_spec().is_vectorized())) {
      // Operator itself supports vectorization, while parent operator does NOT.
      // Use vectorize method to get next row.
      end_cpu_time_counting();
      if (OB_FAIL(get_next_row_vectorizely())) {
        // do nothing
      }
      begin_cpu_time_counting();
    } else {
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
            LOG_WARN("inner get next row failed", K(ret), "type", spec_.type_, "op", op_name(),
              "op_id", spec_.id_);
          }
        } else if (OB_FAIL(try_check_status())) {
          LOG_WARN("check status failed", K(ret));
        } else {
          bool filtered = false;
          if (!spec_.filters_.empty()) {
            if (OB_FAIL(filter_row(filtered))) {
              LOG_WARN("filter row failed", K(ret), "type", spec_.type_, "op", op_name());
            } else {
              if (filtered) {
                continue;
              }
            }
          }
#ifdef ENABLE_SANITY
          if (OB_SUCC(ret) && !filtered) {
            if (OB_FAIL(output_expr_sanity_check())) {
              LOG_WARN("output expr sanity check failed", K(ret));
            }
          }
#endif
#ifndef NDEBUG
          if (OB_SUCC(ret) && !filtered) {
            if (OB_FAIL(output_expr_decint_datum_len_check())) {
              LOG_WARN("output expr sanity check failed", K(ret));
            }
          }
#endif
        }
        break;
      }

      if (OB_SUCCESS == ret) {
        op_monitor_info_.output_row_count_++;
        if (!got_first_row_) {
          op_monitor_info_.first_row_time_ = oceanbase::common::ObClockGenerator::getClock();
          got_first_row_ = true;
        }
      } else if (OB_ITER_END == ret) {
        row_reach_end_ = true;
        int tmp_ret = do_drain_exch();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("drain exchange data failed", K(tmp_ret));
        }
        if (got_first_row_) {
          op_monitor_info_.last_row_time_ = oceanbase::common::ObClockGenerator::getClock();
        }
      }
    }
    if (OB_ITER_END == ret) {
      row_reach_end_ = true;
    } else if (OB_SUCC(ret)) {
      if (spec_.get_phy_plan()->is_vectorized()) {
        ObDatum *res = NULL;
        FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
          if (OB_FAIL((*e)->eval(eval_ctx_, res))) {
            LOG_WARN("expr evaluate failed", K(ret), KPC(*e), K_(eval_ctx));
          } else {
            (*e)->get_eval_info(eval_ctx_).projected_ = true;
          }
        }
      }
    }
  }
  end_ash_line_id_reg();
  end_cpu_time_counting();
  return ret;
}

int ObOperator::get_next_batch(const int64_t max_row_cnt, const ObBatchRows *&batch_rows)
{
  int ret = OB_SUCCESS;
  begin_cpu_time_counting();
  begin_ash_line_id_reg();

  if (OB_FAIL(check_stack_once())) {
    LOG_WARN("too deep recursive", K(ret));
  } else {
    if (OB_UNLIKELY(!spec_.plan_->get_use_rich_format()
                    && spec_.need_check_output_datum_
                    && brs_checker_)) {
      if (OB_FAIL(brs_checker_->check_datum_modified())) {
        LOG_WARN("check output datum failed", K(ret), "id", spec_.get_id(), "op_name", op_name());
      }
    }
    reset_batchrows();
    int64_t skipped_rows_count = 0;
    batch_rows = &brs_;
    if (batch_reach_end_) {
      // generated all data at last batch, just drain exch in this iterate
      brs_.end_ = true;
      brs_.size_ = 0;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_LIKELY(get_spec().is_vectorized())) {
      if (OB_UNLIKELY(!startup_passed_) && !brs_.end_) {
        ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
        guard.set_batch_size(1);
        guard.set_batch_idx(0);
        bool filtered = false;
        // TODO bin.lb: vectorized startup filter? no ObExpr::eval() in vectorization?
        if (OB_FAIL(startup_filter(filtered))) {
          LOG_WARN("do startup filter failed", K(ret), K_(eval_ctx), "op", op_name());
        } else {
          if (filtered) {
            brs_.end_ = true;
          } else {
            startup_passed_ = true;
          }
        }
      }
      while (OB_SUCC(ret) && !brs_.end_) {
        if (OB_FAIL(inner_get_next_batch(min(max_row_cnt, get_spec().max_batch_size_)))) {
          LOG_WARN("get next batch failed", K(ret),  K_(eval_ctx), "id", spec_.get_id(), "op_name", op_name());
        } else {
          LOG_DEBUG("inner get next batch", "id", spec_.get_id(), "op_name", op_name(), K(brs_));
        }
        if (OB_SUCC(ret)) {
          // FIXME bin.lb: accumulate bit count is CPU consuming, disable in perf mode?
          skipped_rows_count = brs_.skip_->accumulate_bit_cnt(brs_.size_);
          if (OB_UNLIKELY(brs_.size_ == skipped_rows_count)) {
            reset_batchrows();
            continue;
          }
        }
        if (OB_SUCC(ret) && (ctx_.get_my_session()->is_user_session() || spec_.plan_->get_phy_plan_hint().monitor_)) {
          IGNORE_RETURN try_register_rt_monitor_node(brs_.size_);
        }
        bool all_filtered = false;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(try_check_status_by_rows(brs_.size_))) {
          LOG_WARN("check status failed", K(ret));
        } else if (!spec_.filters_.empty()) {
          if (OB_FAIL(filter_rows(spec_.filters_,
                                  *brs_.skip_,
                                  brs_.size_,
                                  all_filtered,
                                  brs_.all_rows_active_))) {
            LOG_WARN("filter batch rows failed", K(ret), K_(eval_ctx));
          } else if (all_filtered) {
            brs_.skip_->reset(brs_.size_);
            brs_.size_ = 0;
            // keep brs_.end_ unchanged.
            continue;
          }
        }
#ifdef ENABLE_SANITY
        if (OB_SUCC(ret) && spec_.use_rich_format_ && !all_filtered) {
          if (OB_FAIL(output_expr_sanity_check_batch())) {
            LOG_WARN("output expr sanity check batch failed", K(ret));
          }
        }
#endif
#ifndef NDEBUG
        if (OB_SUCC(ret) && !all_filtered) {
          if (OB_FAIL(output_expr_decint_datum_len_check_batch())) {
            LOG_WARN("output expr sanity check batch failed", K(ret));
          }
        }
#endif
        break;
      }

      // do project
      if (OB_SUCC(ret) && brs_.size_ > 0) {
        FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
          ret = spec_.use_rich_format_ ? (*e)->eval_vector(eval_ctx_, brs_)
                                       : (*e)->eval_batch(eval_ctx_, *brs_.skip_, brs_.size_);
          (*e)->get_eval_info(eval_ctx_).projected_ = true;
        }
      }

      if (brs_.end_ && 0 == brs_.size_) {
        FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
          if (UINT32_MAX != (*e)->vector_header_off_) {
            if (OB_FAIL((*e)->init_vector(eval_ctx_, (*e)->is_batch_result()
                                          ? VEC_UNIFORM : VEC_UNIFORM_CONST, brs_.size_))) {
              LOG_WARN("failed to init vector", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (brs_.end_ && brs_.size_ >= 0) {
          batch_reach_end_ = true; // prepare to be end
          // if no data in batch, end iterate immediately, otherwise wait for next iterate
          brs_.end_ = !brs_.size_;
        }
        skipped_rows_count = brs_.skip_->accumulate_bit_cnt(brs_.size_);
        op_monitor_info_.output_row_count_ += brs_.size_ - skipped_rows_count;
        op_monitor_info_.skipped_rows_count_ += skipped_rows_count; // for batch
        ++op_monitor_info_.output_batches_; // for batch
        if (!got_first_row_ && !brs_.end_) {
          op_monitor_info_.first_row_time_ = ObClockGenerator::getClock();;
          got_first_row_ = true;
        }
        if (brs_.end_) {
          int tmp_ret = do_drain_exch();
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("drain exchange data failed", K(tmp_ret));
          }
          op_monitor_info_.last_row_time_ = ObClockGenerator::getClock();
        }
      }
    } else {
      end_cpu_time_counting();
      // Operator does NOT support vectorization, while its parent does. Return
      // the batch with only 1 row
      if (OB_FAIL(get_next_batch_with_onlyone_row())) {
        // do nothing
      }
      begin_cpu_time_counting();
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(spec_.need_check_output_datum_) && brs_checker_ && !brs_.end_ && brs_.size_ > 0) {
        OZ(brs_checker_->save(brs_.size_));
      }
      LOG_DEBUG("get next batch", "id", spec_.get_id(), "op_name", op_name(), K(brs_));
    }
  }

  // for operator which not use_rich_format, not maintain all_rows_active flag;
  OZ (convert_vector_format());
  if (!spec_.use_rich_format_ && PHY_TABLE_SCAN != spec_.type_) {
    brs_.set_all_rows_active(false);
  }

  end_ash_line_id_reg();
  end_cpu_time_counting();
  return ret;
}

// for old -> new(parent) operator, need init_vector for output exprs
// for new -> old(parent) operator, need cast format of output exprs to uniform
int ObOperator::convert_vector_format()
{
  int ret = OB_SUCCESS;
  if (NULL != spec_.get_parent() &&
       spec_.get_parent()->use_rich_format_ && !spec_.use_rich_format_) {
    // old operator -> new operator
    FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
      VectorFormat format = (*e)->is_batch_result() ? VEC_UNIFORM : VEC_UNIFORM_CONST;
      LOG_TRACE("init vector", K(format), K(*e));
      if (OB_FAIL((*e)->init_vector(eval_ctx_, format, brs_.size_))) {
        LOG_WARN("expr evaluate failed", K(ret), KPC(*e), K_(eval_ctx));
      }
    }
  } else if ((spec_.use_rich_format_ && (&spec_ == spec_.plan_->get_root_op_spec()
                                         && !IS_TRANSMIT(spec_.type_)))
             || (spec_.use_rich_format_ &&
                   NULL != spec_.get_parent() && !spec_.get_parent()->use_rich_format_)) {
    // new operator -> old operator
    FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
      LOG_TRACE("cast to uniform", K(*e));
      if (OB_FAIL((*e)->cast_to_uniform(brs_.size_, eval_ctx_))) {
        LOG_WARN("expr evaluate failed", K(ret), KPC(*e), K_(eval_ctx));
      }
    }
  }

  return ret;
}

int ObOperator::filter_row(ObEvalCtx &eval_ctx, const ObIArray<ObExpr *> &exprs, bool &filtered)
{
  ObDatum *datum = NULL;
  int ret = OB_SUCCESS;
  filtered = false;
  FOREACH_CNT_X(e, exprs, OB_SUCC(ret)) {
    OB_ASSERT(NULL != *e);
    if (OB_FAIL((*e)->eval(eval_ctx, datum))) {
      LOG_WARN("expr evaluate failed", K(ret), K(eval_ctx), "expr", *e);
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

int ObOperator::filter_row_vector(ObEvalCtx &eval_ctx,
                                  const common::ObIArray<ObExpr *> &exprs,
                                  const sql::ObBitVector &skip_bit,
                                  bool &filtered)
{
  int ret = OB_SUCCESS;
  filtered = false;
  const int64_t batch_idx = eval_ctx.get_batch_idx();
  EvalBound eval_bound(eval_ctx.get_batch_size(), batch_idx, batch_idx + 1, false);
  FOREACH_CNT_X(e, exprs, OB_SUCC(ret) && !filtered) {
    if (OB_FAIL((*e)->eval_vector(eval_ctx, skip_bit, eval_bound))) {
      LOG_WARN("Failed to evaluate vector", K(ret));
    } else {
      ObIVector *res = (*e)->get_vector(eval_ctx);
      filtered = !res->is_true(batch_idx);
    }
  }
  return ret;
}

int ObOperator::filter(const common::ObIArray<ObExpr *> &exprs, bool &filtered)
{
  return filter_row(eval_ctx_, exprs, filtered);
}

int ObOperator::filter_rows(const ObExprPtrIArray &exprs,
                            ObBitVector &skip,
                            const int64_t bsize,
                            bool &all_filtered,
                            bool &all_active)
{
  return spec_.use_rich_format_
         ? filter_vector_rows(exprs, skip, bsize, all_filtered, all_active)
         : filter_batch_rows(exprs, skip, bsize, all_filtered, all_active);

}

int ObOperator::filter_vector_rows(const ObExprPtrIArray &exprs,
                                   ObBitVector &skip,
                                   const int64_t bsize,
                                   bool &all_filtered,
                                   bool &all_active)
{
  int ret = OB_SUCCESS;
  all_filtered = false;
  bool tmp_all_active = true;
  FOREACH_CNT_X(e, exprs, OB_SUCC(ret) && !all_filtered) {
    int64_t output_rows = 0;
    OB_ASSERT(ob_is_int_tc((*e)->datum_meta_.type_));
    ObIVector *vec = NULL;
    if (OB_FAIL((*e)->eval_vector(eval_ctx_, skip, bsize, all_active))) {
      LOG_WARN("evaluate batch failed", K(ret), K_(eval_ctx));
    } else if (FALSE_IT(vec = (*e)->get_vector(eval_ctx_))) {
      // do nothing
    } else if (!(*e)->is_batch_result()) {
      ObUniformBase *const_vec = static_cast<ObUniformBase*>(vec);
      ObDatum &d = const_vec->get_datums()[0];
      if (d.null_ || 0 == *d.int_) {
        skip.set_all(bsize);
        tmp_all_active = false;
      } else {
        output_rows++;
      }
      LOG_DEBUG("const vector filter", K(bsize));
    } else if (OB_LIKELY(VEC_FIXED == vec->get_format())) {
      ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vec);
      ObBitVector *nulls = fixed_vec->get_nulls();
      int64_t *int_arr = reinterpret_cast<int64_t *>(fixed_vec->get_data());
      ObBitVector::flip_foreach(skip, bsize,
        [&](int64_t idx) __attribute__((always_inline)) {
          if (0 == int_arr[idx] || nulls->at(idx)) {
            skip.set(idx);
            tmp_all_active = false;
          } else {
            output_rows++;
          }
          return OB_SUCCESS;
        }
      );
      LOG_DEBUG("fixed vector filter", K(bsize));
    } else if (VEC_UNIFORM == vec->get_format()) {
      ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vec);
      const ObDatum *datums = uniform_vec->get_datums();
      for (int64_t i = 0; i < bsize; i++) {
        if (!skip.at(i)) {
          if (datums[i].null_ || 0 == *datums[i].int_) {
            skip.set(i);
            tmp_all_active = false;
          } else {
            output_rows++;
          }
        }
      }
      // FIXME bin.lb: add output_rows to ObBatchRows?
      LOG_DEBUG("uniform vector filter", K(bsize));
    }
    all_filtered = (0 == output_rows);
    all_active &= tmp_all_active;
  }

  return ret;
}

int ObOperator::filter_batch_rows(const ObExprPtrIArray &exprs,
                                  ObBitVector &skip,
                                  const int64_t bsize,
                                  bool &all_filtered,
                                  bool &all_active)
{
  int ret = OB_SUCCESS;
  all_filtered = false;
  bool tmp_all_active = true;
  FOREACH_CNT_X(e, exprs, OB_SUCC(ret) && !all_filtered) {
    OB_ASSERT(ob_is_int_tc((*e)->datum_meta_.type_));
    if (OB_FAIL((*e)->eval_batch(eval_ctx_, skip, bsize))) {
      LOG_WARN("evaluate batch failed", K(ret), K_(eval_ctx));
    } else if (!(*e)->is_batch_result()) {
      const ObDatum &d = (*e)->locate_expr_datum(eval_ctx_);
      if (d.null_ || 0 == *d.int_) {
        all_filtered = true;
        skip.set_all(bsize);
        tmp_all_active = false;
      }
    } else {
      int64_t output_rows = 0;
      const ObDatum *datums = (*e)->locate_batch_datums(eval_ctx_);
      for (int64_t i = 0; i < bsize; i++) {
        if (!skip.at(i)) {
          if (datums[i].null_ || 0 == *datums[i].int_) {
            skip.set(i);
            tmp_all_active = false;
          } else {
            output_rows += 1;
          }
        }
      }
      // FIXME bin.lb: add output_rows to ObBatchRows?
      all_filtered = (0 == output_rows);
    }

    all_active &= tmp_all_active;
  }
  return ret;
}

// copy ObPhyOperator::drain_exch
int ObOperator::drain_exch()
{
  int ret = OB_SUCCESS;
  uint64_t cpu_begin_time = rdtsc();
  ret = do_drain_exch();
  total_time_ += (rdtsc() - cpu_begin_time);
  return ret;
}

int ObOperator::do_drain_exch()
{
  int ret = OB_SUCCESS;
  /**
   * 1. try to open this operator
   * 2. try to drain all children
   */
  if (OB_FAIL(try_open())) {
    LOG_WARN("fail to open operator", K(ret));
  } else if (!exch_drained_) {
    int tmp_ret = inner_drain_exch();
    exch_drained_ = true;
    // If an operator is drained, it means that the parent operator will never call its
    // get_next_batch function again theoretically. However, we cannot guarantee that there won't be
    // any bugs that call get_next_batch again after drain. To prevent this situation, we set the
    // all iter end flags here.
    // For specific case, refer to issue:
    brs_.end_ = true;
    batch_reach_end_ = true;
    row_reach_end_ = true;
    if (!spec_.is_receive()) {
      for (int64_t i = 0; i < child_cnt_ && OB_SUCC(ret); i++) {
        if (OB_ISNULL(children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL child found", K(ret), K(i));
        } else if (OB_FAIL(children_[i]->drain_exch())) {
          LOG_WARN("drain exch failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObOperator::check_status()
{
  return ctx_.check_status();
}

void ObOperator::do_clear_datum_eval_flag()
{
  FOREACH_CNT(e, spec_.calc_exprs_) {
    if ((*e)->is_batch_result()) {
      (*e)->get_evaluated_flags(eval_ctx_).unset(eval_ctx_.get_batch_idx());
    } else {
      (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
    }
  }
}
void ObOperator::set_pushdown_param_null(const ObIArray<ObDynamicParamSetter> &rescan_params)
{
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ParamStore &param_store = plan_ctx->get_param_store_for_update();
  FOREACH_CNT(param, rescan_params) {
    param_store.at(param->param_idx_).set_null();
    param->dst_->locate_expr_datum(eval_ctx_, 0).set_null();
  }
}

inline int ObOperator::get_next_row_vectorizely()
{
  int ret = OB_SUCCESS;
  //init br_it_ at the 1st time
  if (br_it_ == nullptr) {
    void * ptr = ctx_.get_allocator().alloc(sizeof(ObBatchRowIter));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocation failed for br_it_", K(ret),
                  K(sizeof(ObBatchRowIter)));
    } else {
      br_it_ = new(ptr) ObBatchRowIter();
      br_it_->set_operator(this);
      if (OB_FAIL(br_it_->brs_holder_.init(spec_.output_, eval_ctx_))){
        SQL_ENG_LOG(WARN, "brs_holder init failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(br_it_->get_next_row(eval_ctx_, spec_))) {
    if (ret != OB_ITER_END) {
      SQL_ENG_LOG(WARN, "Failed to get_next_row vectorizedly", K(ret));
    }
  }
  // update eval_ctx_ so that upper operator can find the correct datum when
  // evoking ObExpr::eval_one_datum_of_batch(ctx, datum)
  eval_ctx_.set_batch_size(1);
  eval_ctx_.set_batch_idx(0);
  return ret;
}

#ifdef ENABLE_DEBUG_LOG
inline int ObOperator::init_dummy_mem_context(uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL == dummy_mem_context_)) {
    lib::ContextParam param;
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_mem_attr(tenant_id, "ObSqlOp",
                     common::ObCtxIds::DEFAULT_CTX_ID);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(dummy_mem_context_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(dummy_mem_context_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "mem entity is null", K(ret));
    }
  }
  return ret;
}
#endif

int ObBatchRowIter::get_next_row()
{
  const int64_t max_row_cnt = INT64_MAX;
  int ret = OB_SUCCESS;
  if (NULL == brs_) {
    if (OB_FAIL(op_->get_next_batch(max_row_cnt, brs_))) {
      LOG_WARN("get next batch failed", K(ret));
    }
  }
  while (OB_SUCC(ret)) {
    if (idx_ >= brs_->size_ && brs_->end_) {
      ret = OB_ITER_END;
      break;
    }
    // skip the filtered row
    while (idx_ < brs_->size_ && brs_->skip_->at(idx_)) {
      idx_++;
    }
    if (idx_ >= brs_->size_) {
      if (!brs_->end_) {
        if (OB_FAIL(op_->get_next_batch(max_row_cnt, brs_))) {
          LOG_WARN("get next batch failed", K(ret));
        }
        idx_ = 0;
      }
      continue;
    } else {
      // got row, increase the index
      idx_ += 1;
      break;
    }
  }
  return ret;
}

int ObBatchRowIter::get_next_row(ObEvalCtx &eval_ctx, const ObOpSpec &spec)
{
  const int64_t max_row_cnt = INT64_MAX;
  int ret = OB_SUCCESS;
  LOG_DEBUG("debug batch to row transform ", K(idx_));
  if (NULL == brs_) {
    if (OB_FAIL(op_->get_next_batch(max_row_cnt, brs_))) {
      LOG_WARN("get next batch failed", K(ret));
    } else if (OB_FAIL(brs_holder_.save(1))) {
      LOG_WARN("backup datumss[0] failed", K(ret));
    }
    // backup datums[0]
    LOG_DEBUG("batch to row transform ", K(idx_), KPC(brs_));
  }
  while (OB_SUCC(ret)) {
    if (idx_ >= brs_->size_ && brs_->end_) {
      ret = OB_ITER_END;
      break;
    }
    // skip the filtered row
    while (idx_ < brs_->size_ && brs_->skip_->at(idx_)) {
      idx_++;
    }
    if (idx_ >= brs_->size_) {
      if (!brs_->end_) {
        brs_holder_.restore();
        if (OB_FAIL(op_->get_next_batch(max_row_cnt, brs_))) {
          LOG_WARN("get next batch failed", K(ret));
        } else {
          idx_ = 0;
          if (0 == brs_->size_ && brs_->end_) {
            LOG_DEBUG("get empty batch ", K(brs_));
            ret = OB_ITER_END;
            break;
          } else if (OB_FAIL(brs_holder_.save(1))) {
            LOG_WARN("backup datumss[0] failed", K(ret));
          }
        }
      }
      continue;
    } else {
      // got row, increase the index to next row
      idx_ += 1;
      break;
    }
  }
  // overwrite datums[0]: shallow copy
  for (auto i = 0; OB_SUCC(ret) && i < spec.output_.count(); i++) {
    ObExpr *expr = spec.output_.at(i);
    if (expr->is_batch_result() && 0 != cur_idx()) {
      ObDatum *datums = expr->locate_batch_datums(eval_ctx);
      datums[0] = datums[cur_idx()];
      LOG_DEBUG("copy datum to datum[0], details: ", K(cur_idx()), K(datums[0]),
               K(expr->locate_batch_datums(eval_ctx)),
               KPC(expr->locate_batch_datums(eval_ctx)), K(expr), KPC(expr));
    } // non batch expr always point to offset 0, do nothing for else brach
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObBatchRescanParams, params_, param_idxs_, param_expr_idxs_);

int ObBatchRescanParams::assign(const ObBatchRescanParams &other)
{
  int ret = OB_SUCCESS;
  OZ(params_.assign(other.params_));
  OZ(param_idxs_.assign(other.param_idxs_));
  OZ(param_expr_idxs_.assign(other.param_expr_idxs_));
  return ret;
}

int ObBatchRescanParams::deep_copy_param(const common::ObObjParam &org_param,
    common::ObObjParam &new_param)
{
  int ret = OB_SUCCESS;
  int64_t obj_size = org_param.get_deep_copy_size();
  char *buf = NULL;
  int64_t pos = 0;
  new_param = org_param;
  if (obj_size <= 0) {
    /*do nothing*/
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(obj_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate new obj failed", K(ret), K(obj_size), K(org_param));
  } else if (OB_FAIL(new_param.deep_copy(org_param, buf, obj_size, pos))) {
    LOG_WARN("fail to deep copy new param", K(ret));
  }
  return ret;
}

int ObBatchRescanParams::append_batch_rescan_param(const ObIArray<int64_t> &param_idxs,
    const ObTMArray<common::ObObjParam> &res_objs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(params_.push_back(res_objs))) {
    LOG_WARN("fail to rescan batch param", K(ret));
  } else if (param_idxs.count() != param_idxs_.count()) {
    if (param_idxs_.count() != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid count", K(ret), K(param_idxs_.count()));
    } else if (OB_FAIL(param_idxs_.assign(param_idxs))) {
      LOG_WARN("fail to assign param_idxs", K(ret));
    }
  }
  return ret;
}

int ObBatchRescanParams::append_batch_rescan_param(const ObIArray<int64_t> &param_idxs,
    const ObTMArray<ObObjParam> &res_objs,
    const common::ObIArray<int64_t> &param_expr_idxs)
{
  int ret = OB_SUCCESS;
  OZ(append_batch_rescan_param(param_idxs, res_objs));
  OZ(param_expr_idxs_.assign(param_expr_idxs));
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
