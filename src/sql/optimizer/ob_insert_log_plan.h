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

#ifndef _OB_INSERT_LOG_PLAN_H
#define _OB_INSERT_LOG_PLAN_H
#include "ob_log_plan.h"

namespace oceanbase {
namespace sql {
class ObDMLStmt;
class ObLogInsert;
class ObInsertStmt;
struct ObDupKeyScanInfo;
typedef common::ObSEArray<common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true>, 1,
    common::ModulePageAllocator, true>
    RowParamMap;
class ObInsertLogPlan : public ObLogPlan {
public:
  ObInsertLogPlan(ObOptimizerContext& ctx, const ObDMLStmt* insert_stmt) : ObLogPlan(ctx, insert_stmt), insert_op_(NULL)
  {}
  virtual ~ObInsertLogPlan()
  {}
  int generate_raw_plan();
  virtual int generate_plan();
  inline const RowParamMap& get_row_params_map() const
  {
    return row_params_map_;
  }

protected:
  int generate_values_op_as_child(ObLogicalOperator*& expr_values);
  bool is_self_part_insert();
  int map_value_param_index();
  int generate_duplicate_key_checker(ObLogInsert& insert_op);
  int generate_dupkey_scan_info(
      const ObDupKeyScanInfo& dupkey_info, ObLogicalOperator*& scan_op, ObRawExpr*& calc_part_expr);
  int allocate_insert_op(ObInsertStmt& insert_stmt, ObLogInsert*& insert_op);
  int allocate_insert_all_op(ObInsertStmt& insert_stmt, ObLogInsert*& insert_op);
  int allocate_pdml_insert_as_top(ObLogicalOperator*& top_op);

private:
  DISALLOW_COPY_AND_ASSIGN(ObInsertLogPlan);

private:
  RowParamMap row_params_map_;
  ObLogInsert* insert_op_;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // _OB_INSERT_LOG_PLAN_H
