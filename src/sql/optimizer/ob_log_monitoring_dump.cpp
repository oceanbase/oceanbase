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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_monitoring_dump.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "ob_opt_est_cost.h"
#include "ob_select_log_plan.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

const char* ObLogMonitoringDump::get_name() const
{
  static const char* monitoring_name_[1] = {
      "MONITORING DUMP",
  };
  return monitoring_name_[0];
}

int ObLogMonitoringDump::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  ObLogMonitoringDump* monitoring_dump = NULL;
  if (OB_FAIL(clone(op))) {
    SQL_OPT_LOG(WARN, "Failed to clone ObLogMonitoringDump", K(ret));
  } else if (OB_ISNULL(monitoring_dump = static_cast<ObLogMonitoringDump*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_OPT_LOG(WARN, "Failed to cast ObLogicalOperator * to ObLogMonitoringDump *", K(ret));
  } else {
    monitoring_dump->flags_ = flags_;
    out = monitoring_dump;
  }
  return ret;
}

int ObLogMonitoringDump::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  return ret;
}

int ObLogMonitoringDump::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  // print access
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  UNUSED(type);
  return ret;
}

int ObLogMonitoringDump::transmit_op_ordering()
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("Monitoring dump never got there", K(ret));
  return ret;
}

int ObLogMonitoringDump::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(parent);
  UNUSED(need_row_count);
  UNUSED(re_est);
  LOG_WARN("Monitoring dump never got there", K(ret));
  return ret;
}

int ObLogMonitoringDump::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Child is null", K(ret));
  } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
    LOG_WARN("Failed to set op ordering", K(ret));
  }
  return ret;
}

int ObLogMonitoringDump::print_outline(planText& plan_text)
{
  int ret = OB_SUCCESS;
  if (is_added_outline_) {  // do nothing.
  } else if (OB_FAIL(print_tracing(plan_text))) {
    LOG_WARN("fail to print tracing", K(ret));
  }
  return ret;
}

int ObLogMonitoringDump::print_tracing(planText& plan_text)
{
  int ret = OB_SUCCESS;
  bool is_oneline = plan_text.is_oneline_;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  if (flags_ & OB_MONITOR_TRACING) {
    if (OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::TRACING_HINT))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%lu", dst_op_line_id_))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else {
    }
  }
  if (OB_SUCC(ret) && (flags_ & OB_MONITOR_STAT)) {
    if (OB_FAIL(BUF_PRINTF("\n"))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::STAT_HINT))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%lu", dst_op_line_id_))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("failed to do BUF PRINF", K(ret));
    } else {
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
