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
#include "ob_monitoring_dump_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

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
    output_hash_(exec_ctx.get_allocator())
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
    const char* name = get_phy_op_name(spec_.get_left()->type_);
    op_name_.set_string(name, strlen(name));
    if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(SYS_VAR_TRACEFILE_IDENTIFIER, val))) {
      LOG_WARN("Get sys variable error", K(ret));
    } else if (OB_FAIL(tracefile_identifier_.from_obj(val))) {
      LOG_WARN("failed to convert datum from obj", K(ret));
    } else if (OB_FAIL(output_hash_.init(output.count()))) {
      LOG_WARN("init output hash array failed", K(ret));
    } else {
      for (int64_t i = 0; i < output.count() && OB_SUCC(ret); i++) {
        if (OB_FAIL(output_hash_.push_back(0))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
      open_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObMonitoringDumpOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.flags_ & ObMonitorHint::OB_MONITOR_STAT) {
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
      if (MY_SPEC.flags_ & ObMonitorHint::OB_MONITOR_TRACING) {
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
    batch_info_guard.set_batch_size(batch_cnt);
    for (int64_t i = 0; i < child_brs->size_ && OB_SUCC(ret); ++i) {
      if (child_brs->skip_->exist(i)) {
        ++skip_cnt;
      } else {
        ++rows_;
        batch_info_guard.set_batch_idx(i);
        if (OB_FAIL(calc_hash_value())) {
          LOG_WARN("calc hash value failed", K(ret));
        } else {
          if (MY_SPEC.flags_ & ObMonitorHint::OB_MONITOR_TRACING) {
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
