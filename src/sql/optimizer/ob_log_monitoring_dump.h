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

#ifndef OB_LOG_TRACING_H_
#define OB_LOG_TRACING_H_

#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase {
namespace sql {

class ObLogMonitoringDump : public ObLogicalOperator {
public:
  ObLogMonitoringDump(ObLogPlan& plan) : ObLogicalOperator(plan), flags_(0), dst_op_line_id_(0)
  {}
  virtual ~ObLogMonitoringDump() = default;
  const char* get_name() const;
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type) override;
  virtual int copy_without_child(ObLogicalOperator*& out) override;
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int compute_op_ordering() override;
  virtual int transmit_op_ordering() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est);
  virtual int print_outline(planText& plan_text) override;
  int print_tracing(planText& plan_text);
  inline void set_flags(uint64_t flags)
  {
    flags_ = flags;
  }
  uint64_t get_flags()
  {
    return flags_;
  }
  void set_dst_op_id(uint64_t dst_op_id)
  {
    dst_op_line_id_ = dst_op_id;
  }
  uint64_t get_dst_op_id()
  {
    return dst_op_line_id_;
  }

private:
  uint64_t flags_;
  uint64_t dst_op_line_id_;
  DISALLOW_COPY_AND_ASSIGN(ObLogMonitoringDump);
};

}  // namespace sql
}  // namespace oceanbase

#endif
