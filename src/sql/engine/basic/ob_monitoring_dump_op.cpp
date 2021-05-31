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

namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {

ObMonitoringDumpSpec::ObMonitoringDumpSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), flags_(0), dst_op_id_(-1)
{}

OB_SERIALIZE_MEMBER((ObMonitoringDumpSpec, ObOpSpec), flags_, dst_op_id_);

ObMonitoringDumpOp::ObMonitoringDumpOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input),
      op_name_(),
      tracefile_identifier_(),
      open_time_(0),
      rows_(0),
      first_row_time_(0),
      last_row_time_(0),
      first_row_fetched_(false)
{}

int ObMonitoringDumpOp::inner_open()
{
  int ret = OB_SUCCESS;
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
    } else {
      open_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObMonitoringDumpOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.flags_ & OB_MONITOR_STAT) {
    uint64_t CLOSE_TIME = ObTimeUtility::current_time();
    if (!tracefile_identifier_.null_ && tracefile_identifier_.len_ > 0) {
      LOG_INFO("",
          K(tracefile_identifier_),
          K(op_name_),
          K(rows_),
          K(open_time_),
          K(CLOSE_TIME),
          K(first_row_time_),
          K(last_row_time_),
          K(MY_SPEC.dst_op_id_));
    } else {
      LOG_INFO("",
          K(op_name_),
          K(rows_),
          K(open_time_),
          K(CLOSE_TIME),
          K(first_row_time_),
          K(last_row_time_),
          K(MY_SPEC.dst_op_id_));
    }
  }
  return ret;
}

int ObMonitoringDumpOp::rescan()
{
  int ret = OB_SUCCESS;
  first_row_fetched_ = false;
  if (OB_FAIL(ObOperator::rescan())) {
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
      LOG_DEBUG("OB_ITER_END");
      last_row_time_ = ObTimeUtility::current_time();
    } else {
      LOG_WARN("Failed to get next row", K(ret));
    }
  } else {
    rows_++;
    if (MY_SPEC.flags_ & OB_MONITOR_TRACING) {
      if (!tracefile_identifier_.null_ && tracefile_identifier_.len_ > 0) {
        LOG_INFO("",
            K(tracefile_identifier_.get_string()),
            K(op_name_.get_string()),
            K(ObToStringExprRow(eval_ctx_, MY_SPEC.output_)),
            K(MY_SPEC.dst_op_id_));
      } else {
        LOG_INFO("", K(op_name_.get_string()), K(ObToStringExprRow(eval_ctx_, MY_SPEC.output_)), K(MY_SPEC.dst_op_id_));
      }
    }
    if (!first_row_fetched_) {
      first_row_fetched_ = true;
      first_row_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
