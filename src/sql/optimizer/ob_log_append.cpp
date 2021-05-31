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
#include "ob_log_append.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

ObLogAppend::ObLogAppend(ObLogPlan& plan) : ObLogicalOperator(plan)
{
  sub_plan_num_ = 0;
  for (int64_t i = 0; i < OB_SQL_MAX_CHILD_OPERATOR_NUM; ++i) {
    sub_plan_[i] = NULL;
  }
  type_ = log_op_def::LOG_APPEND;
}

int ObLogAppend::add_sub_plan(ObLogPlan* sub_plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sub plan is null", K(sub_plan), K(ret));
  } else if (OB_UNLIKELY(sub_plan_num_ >= OB_SQL_MAX_CHILD_OPERATOR_NUM)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow. too many sub plans", K(sub_plan_num_), K(ret));
  } else {
    sub_plan_[sub_plan_num_] = sub_plan;
    ++sub_plan_num_;
  }
  return ret;
}

int ObLogAppend::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogAppend* append = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogAppend op", K(ret));
  } else if (OB_ISNULL(append = static_cast<ObLogAppend*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperator to ObLogAppend", K(ret));
  } else {
    append->sub_plan_num_ = sub_plan_num_;
    for (int64_t i = 0; i < sub_plan_num_; ++i) {
      append->sub_plan_[i] = sub_plan_[i];
    }
    out = append;
  }
  return ret;
}
