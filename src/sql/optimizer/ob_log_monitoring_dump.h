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

namespace oceanbase
{
namespace sql
{

class ObLogMonitoringDump : public ObLogicalOperator
{
public:
  ObLogMonitoringDump(ObLogPlan &plan) :
  ObLogicalOperator(plan), flags_(0), dst_op_line_id_(0)
  { }
  virtual ~ObLogMonitoringDump() = default;
  const char *get_name() const;
  inline void set_flags(uint64_t flags) { flags_ = flags; }
  uint64_t get_flags() { return flags_; }
  void set_dst_op_id(uint64_t dst_op_id) { dst_op_line_id_ = dst_op_id; }
  uint64_t get_dst_op_id() { return dst_op_line_id_; }
  virtual int est_cost() override;
private:
  uint64_t flags_;
  // 这里的id只是的算子的line id，也就是explain看到的line编号
  uint64_t dst_op_line_id_;
  DISALLOW_COPY_AND_ASSIGN(ObLogMonitoringDump);
};

}
}

#endif
