/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_monitoring_dump_op.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

struct MonitorDumpLogGuard
{
  MonitorDumpLogGuard()
  {
    int tmp_ret = OB_E(EventTable::EN_FORCE_MONITORING_DUMP_ROWS) OB_SUCCESS;
    if (tmp_ret != OB_SUCCESS) {
      // NOTE: the free function common::allow_next_syslog(int64_t) only has a weak
      // empty impl; call ObTaskController directly so force-allow actually takes effect.
      ObTaskController::get().allow_next_syslog();
    }
  }
};

ObMonitoringDumpSpec::ObMonitoringDumpSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
    flags_(0),
    dst_op_id_(-1)
{
}

OB_SERIALIZE_MEMBER((ObMonitoringDumpSpec, ObOpSpec), flags_, dst_op_id_);

ObMonitoringDumpOp::ObMonitoringDumpOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    op_name_(),
    tracefile_identifier_(),
    open_time_(0),
    rows_(0),
    first_row_time_(0),
    last_row_time_(0),
    first_row_fetched_(false),
    output_hash_(exec_ctx.get_allocator()),
    batch_hash_values_(exec_ctx.get_allocator())
{
}

int ObMonitoringDumpOp::inner_open()
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &output = MY_SPEC.output_;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: child is null", K(ret));
  } else {
    ObObj val;
    const char* name = get_phy_op_name(spec_.get_left()->type_, spec_.use_rich_format_);
    op_name_.set_string(name, strlen(name));
    if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(SYS_VAR_TRACEFILE_IDENTIFIER, val))) {
      LOG_WARN("Get sys variable error", K(ret));
    } else if (OB_FAIL(tracefile_identifier_.from_obj(val))) {
      LOG_WARN("failed to convert datum from obj", K(ret));
    } else if (OB_FAIL(output_hash_.init(output.count()))) {
      LOG_WARN("init output hash array failed", K(ret));
    } else if (OB_FAIL(batch_hash_values_.init(MY_SPEC.max_batch_size_))) {
      LOG_WARN("init batch hash array failed", K(ret), K(MY_SPEC.max_batch_size_));
    } else {
      for (int64_t i = 0; i < output.count() && OB_SUCC(ret); i++) {
        if (OB_FAIL(output_hash_.push_back(0))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
      for (int64_t i = 0; i < MY_SPEC.max_batch_size_ && OB_SUCC(ret); i++) {
        if (OB_FAIL(batch_hash_values_.push_back(0))) {
          LOG_WARN("push back batch hash value failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        open_time_ = ObTimeUtility::current_time();
      }
    }
  }
  return ret;
}

int ObMonitoringDumpOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.flags_ & ObAllocOpHint::OB_MONITOR_STAT) {
    uint64_t CLOSE_TIME = ObTimeUtility::current_time();
    if (!tracefile_identifier_.null_ && tracefile_identifier_.len_ > 0) {
      LOG_INFO("", K(tracefile_identifier_), K(op_name_), K(rows_), K(open_time_),
        K(CLOSE_TIME), K(first_row_time_), K(last_row_time_), K(MY_SPEC.dst_op_id_));
    } else {
      LOG_INFO("", K(op_name_), K(rows_), K(open_time_), K(CLOSE_TIME),
        K(first_row_time_), K(last_row_time_), K(MY_SPEC.dst_op_id_));
    }
  }
  uint64_t id = MY_SPEC.id_;
  uint64_t sum_hash = 0;
  const ExprFixedArray &output = MY_SPEC.output_;
  for (int64_t i = 0; i < output_hash_.count(); i++) {
    uint64_t output_hash = output_hash_.at(i);
    LOG_INFO("monitoring dump output hash value", K(id), K(i), K(output_hash),
      K(MY_SPEC.dst_op_id_));
    sum_hash += output_hash;
  }
  LOG_INFO("monitoring dump sum output hash value", K(id), K(sum_hash), K(MY_SPEC.dst_op_id_));
  op_monitor_info_.otherstat_1_value_ += static_cast<int64_t>(sum_hash);
  op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::MONITORING_DUMP_SUM_OUTPUT_HASH;
  return ret;
}

int ObMonitoringDumpOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  // 这里每次rescan后，重新记录时间，之前没有这样做
  // 其他变量可以不用reset，这些变量都是一次性赋值
  first_row_fetched_ = false;
  LOG_INFO("do rescan",
           K(tracefile_identifier_.get_string()),
           K(op_name_.get_string()),
           K(MY_SPEC.dst_op_id_));
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

int ObMonitoringDumpOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END == ret) {
      LOG_DEBUG("OB_ITER_END", K(op_name_.get_string()), K(MY_SPEC.dst_op_id_));
      last_row_time_ = ObTimeUtility::current_time();
    } else {
      LOG_WARN("Failed to get next row", K(ret));
    }
  } else {
    rows_++;
    if (OB_FAIL(calc_hash_value())) {
      LOG_WARN("calc hash value failed", K(ret));
    } else  {
      if (MY_SPEC.flags_ & ObAllocOpHint::OB_MONITOR_TRACING) {
        if (!tracefile_identifier_.null_ && tracefile_identifier_.len_ > 0) {
          LOG_INFO("", K(tracefile_identifier_.get_string()), K(op_name_.get_string()),
            K(ObToStringExprRow(eval_ctx_, MY_SPEC.output_)), K(MY_SPEC.dst_op_id_));
        } else {
          LOG_INFO("", K(op_name_.get_string()), K(ObToStringExprRow(eval_ctx_, MY_SPEC.output_)),
            K(MY_SPEC.dst_op_id_));
        }
      }
      if (!first_row_fetched_) {
        first_row_fetched_ = true;
        first_row_time_ = ObTimeUtility::current_time();
      }
    }
  }
  return ret;
}

int ObMonitoringDumpOp::calc_hash_value()
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &output = MY_SPEC.output_;
  for (int64_t i = 0; i < output.count() && OB_SUCC(ret); i++) {
    ObExpr *expr = output.at(i);
    ObDatum *datum = NULL;
    if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
      LOG_WARN("eval expr failed", K(ret));
    } else {
      uint64_t ori_hash_value = output_hash_.at(i);
      uint64_t hash_value = 0;
      if (OB_FAIL(expr->basic_funcs_->default_hash_(*datum, 0, hash_value))) {
        LOG_WARN("do hash failed", K(ret));
      } else {
        output_hash_.at(i) = ori_hash_value + hash_value;
      }
    }
  }
  return ret;
}

int ObMonitoringDumpOp::calc_hash_value_batch(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &output = MY_SPEC.output_;
  const int64_t batch_size = brs.size_;
  const uint64_t seed = 0;
  if (OB_UNLIKELY(batch_size > batch_hash_values_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch hash array is not enough", K(ret), K(batch_size), K(batch_hash_values_.count()));
  } else if (batch_size > 0) {
    EvalBound bound(batch_size, brs.all_rows_active_);
    uint64_t *batch_hash_values = &batch_hash_values_.at(0);
    for (int64_t i = 0; i < output.count() && OB_SUCC(ret); i++) {
      ObExpr *expr = output.at(i);
      ObIVector *vec = NULL;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(i));
      } else if (OB_FAIL(expr->eval_vector(eval_ctx_, *brs.skip_, bound))) {
        LOG_WARN("eval vector failed", K(ret), K(i));
      } else if (OB_ISNULL(vec = expr->get_vector(eval_ctx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vector is null", K(ret), K(i));
      } else if (OB_FAIL(vec->default_hash(*expr, batch_hash_values, *brs.skip_, bound,
                                           &seed, false /* is_batch_seed */))) {
        LOG_WARN("do vector hash failed", K(ret), K(i));
      } else {
        uint64_t output_hash = output_hash_.at(i);
        for (int64_t row_idx = 0; row_idx < batch_size; row_idx++) {
          if (!brs.skip_->at(row_idx)) {
            output_hash += batch_hash_values[row_idx];
          }
        }
        output_hash_.at(i) = output_hash;
      }
    }
  }
  return ret;
}

void ObMonitoringDumpOp::print_batch_vector_headers(const ObBatchRows &brs)
{
  const ExprFixedArray &output = MY_SPEC.output_;
  const EvalBound bound(brs.size_, brs.all_rows_active_);
  MonitorDumpLogGuard guard;
  ToStrExprVecFmt expr_fmts_row(output, eval_ctx_, *brs.skip_, bound);
  if (!tracefile_identifier_.null_ && tracefile_identifier_.len_ > 0) {
    LOG_INFO("monitoring dump vector header", K(tracefile_identifier_.get_string()),
             K(op_name_.get_string()), K(expr_fmts_row),
             K(MY_SPEC.dst_op_id_));
  } else {
    LOG_INFO("monitoring dump vector header", K(op_name_.get_string()),
             K(expr_fmts_row), K(MY_SPEC.dst_op_id_));
  }
}

int ObMonitoringDumpOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  LOG_DEBUG("MonitoringDumpOp get_next_batch start");
  int ret = OB_SUCCESS;
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  const ObBatchRows *child_brs = nullptr;
  int64_t skip_cnt = 0;
  clear_evaluated_flag();

  if (OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
    LOG_WARN("child_op failed to get next row", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    batch_info_guard.set_batch_size(child_brs->size_);
    if (MY_SPEC.use_rich_format_
        && child_brs->size_ > 0
        && OB_FAIL(calc_hash_value_batch(*child_brs))) {
      LOG_WARN("calc hash value batch failed", K(ret));
    } else if (MY_SPEC.use_rich_format_
               && child_brs->size_ > 0
               && (MY_SPEC.flags_ & ObAllocOpHint::OB_MONITOR_TRACING)) {
      print_batch_vector_headers(*child_brs);
    }
    for (int64_t i = 0; i < child_brs->size_ && OB_SUCC(ret); ++i) {
      if (child_brs->skip_->exist(i)) {
        ++skip_cnt;
      } else {
        ++rows_;
        batch_info_guard.set_batch_idx(i);
        if (!MY_SPEC.use_rich_format_ && OB_FAIL(calc_hash_value())) {
          LOG_WARN("calc hash value failed", K(ret));
        } else {
          MonitorDumpLogGuard guard;
          VEC_ROWEXPR2STR row(eval_ctx_, MY_SPEC.output_);
          if (MY_SPEC.flags_ & ObAllocOpHint::OB_MONITOR_TRACING) {
            if (MY_SPEC.use_rich_format_) {
              if (!tracefile_identifier_.null_ && tracefile_identifier_.len_ > 0) {
                LOG_INFO("", K(tracefile_identifier_.get_string()), K(op_name_.get_string()),
                         K(row));
              } else {
                LOG_INFO("", K(op_name_.get_string()), K(row), K(MY_SPEC.dst_op_id_));
              }
            } else {
              if (!tracefile_identifier_.null_ && tracefile_identifier_.len_ > 0) {
                LOG_INFO("", K(tracefile_identifier_.get_string()), K(op_name_.get_string()),
                  K(ObToStringExprRow(eval_ctx_, MY_SPEC.output_)), K(MY_SPEC.dst_op_id_));
              } else {
                LOG_INFO("", K(op_name_.get_string()), K(ObToStringExprRow(eval_ctx_, MY_SPEC.output_)),
                  K(MY_SPEC.dst_op_id_));
              }
            }
          }
          if (!first_row_fetched_) {
            first_row_fetched_ = true;
            first_row_time_ = ObTimeUtility::current_time();
          }
        }
      }
    } // end for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(brs_.copy(child_brs))) {
        LOG_WARN("copy child_brs to brs_ failed", K(ret));
      }
    }
    LOG_INFO("", K(op_name_.get_string()), K(MY_SPEC.dst_op_id_),
             K(child_brs->size_), K(skip_cnt), K(child_brs->end_));
  }

  return ret;
}

}
}
