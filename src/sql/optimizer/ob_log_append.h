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

#ifndef OCEANBASE_SQL_OB_LOG_APPEND_H
#define OCEANBASE_SQL_OB_LOG_APPEND_H
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "ob_logical_operator.h"
#include "ob_log_plan.h"
namespace oceanbase {
namespace sql {
class ObLogAppend : public ObLogicalOperator {
public:
  ObLogAppend(ObLogPlan& plan);
  virtual ~ObLogAppend()
  {}
  int add_sub_plan(ObLogPlan* sub_plan);
  ObLogPlan* get_sub_plan(int64_t idx) const
  {
    return idx < 0 || idx >= sub_plan_num_ ? NULL : sub_plan_[idx];
  }
  int64_t get_sub_plan_count()
  {
    return sub_plan_num_;
  }
  void set_type(log_op_def::ObLogOpType type)
  {
    type_ = type;
  }
  virtual int copy_without_child(ObLogicalOperator*& out);
  virtual int allocate_exchange_post(AllocExchContext* ctx) override
  {
    UNUSED(ctx);
    return common::OB_NOT_SUPPORTED;
  }
  virtual bool is_consume_child_1by1() const
  {
    return true;
  }
  VIRTUAL_TO_STRING_KV(K_(sub_plan_num));

private:
  ObLogPlan* sub_plan_[OB_SQL_MAX_CHILD_OPERATOR_NUM];
  int64_t sub_plan_num_;
};

}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_APPEND_H
