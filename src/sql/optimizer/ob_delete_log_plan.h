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

#ifndef _OB_DELETE_LOG_PLAN_H
#define _OB_DELETE_LOG_PLAN_H
#include "lib/container/ob_array.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase {
namespace sql {
/**
 *  Logical Plan for 'delete' statement
 */
class ObLogDelete;
class ObDeleteLogPlan : public ObLogPlan {
public:
  ObDeleteLogPlan(ObOptimizerContext& ctx, const ObDeleteStmt* delete_stmt)
      : ObLogPlan(ctx, delete_stmt), delete_op_(NULL)
  {}
  virtual ~ObDeleteLogPlan(){};

  int generate_raw_plan();
  virtual int generate_plan();

private:
  DISALLOW_COPY_AND_ASSIGN(ObDeleteLogPlan);
  int allocate_delete_as_top(ObLogicalOperator*& top);
  int allocate_pdml_delete_as_top(ObLogicalOperator*& top);

private:
  ObLogDelete* delete_op_;
};
}  // namespace sql
}  // namespace oceanbase
#endif
