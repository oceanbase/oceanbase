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
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/optimizer/ob_del_upd_log_plan.h"

namespace oceanbase
{
namespace sql
{
/**
 *  Logical Plan for 'delete' statement
 */
class ObDeleteLogPlan : public ObDelUpdLogPlan
{
public:
  ObDeleteLogPlan(ObOptimizerContext &ctx, const ObDeleteStmt *delete_stmt)
    : ObDelUpdLogPlan(ctx, delete_stmt)
  {}
  virtual ~ObDeleteLogPlan() {}
  const ObDeleteStmt *get_stmt() const override
  { return reinterpret_cast<const ObDeleteStmt*>(stmt_); }

protected:
  virtual int generate_normal_raw_plan() override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDeleteLogPlan);
  // 分配普通delete计划的delete log operator
  int candi_allocate_delete();
  int create_delete_plan(ObLogicalOperator *&top);
  int create_delete_plans(ObIArray<CandidatePlan> &candi_plans,
                          const bool force_no_multi_part,
                          const bool force_multi_part,
                          ObIArray<CandidatePlan> &delete_plans);
  int allocate_delete_as_top(ObLogicalOperator *&top, bool is_multi_part_dml);
  // 分配pdml delete计划中的delete log operator
  int candi_allocate_pdml_delete();
  virtual int prepare_dml_infos() override;
  virtual int prepare_table_dml_info_special(const ObDmlTableInfo& table_info,
                                             IndexDMLInfo* table_dml_info,
                                             ObIArray<IndexDMLInfo*> &index_dml_infos,
                                             ObIArray<IndexDMLInfo*> &all_index_dml_infos) override;
};
}
}
#endif
