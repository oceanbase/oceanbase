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

#ifndef _OB_UPDATE_LOG_PLAN_H
#define _OB_UPDATE_LOG_PLAN_H 1
#include "sql/optimizer/ob_del_upd_log_plan.h"
#include "sql/resolver/dml/ob_update_stmt.h"

namespace oceanbase
{
namespace sql
{
  class ObLogUpdate;
  class ObUpdateLogPlan : public ObDelUpdLogPlan
  {
  public:
    ObUpdateLogPlan(ObOptimizerContext &ctx, const ObUpdateStmt *update_stmt)
      : ObDelUpdLogPlan(ctx, update_stmt)
    {}
    virtual ~ObUpdateLogPlan() {}

    const ObUpdateStmt *get_stmt() const override
    { return reinterpret_cast<const ObUpdateStmt*>(stmt_); }

    int perform_vector_assign_expr_replacement(ObUpdateStmt *stmt);

  protected:
    virtual int generate_normal_raw_plan() override;

  private:
    int candi_allocate_update();
    int candi_allocate_pdml_update();
    int create_update_plans(ObIArray<CandidatePlan> &candi_plans,
                            ObConstRawExpr *lock_row_flag_expr,
                            const bool force_no_multi_part,
                            const bool force_multi_part,
                            ObIArray<CandidatePlan> &update_plans);
    int allocate_update_as_top(ObLogicalOperator *&top,
                               ObConstRawExpr *lock_row_flag_expr,
                               bool is_multi_part_dml);

    int replace_alias_ref_expr(ObRawExpr *&expr, bool &replace_happened);
    int extract_assignment_subqueries(ObIArray<ObRawExpr*> &normal_query_refs,
                                      ObIArray<ObRawExpr*> &alias_query_refs);
    int extract_assignment_subqueries(ObRawExpr *expr,
                                      ObIArray<ObRawExpr*> &normal_query_refs,
                                      ObIArray<ObRawExpr*> &alias_query_refs);
    virtual int prepare_dml_infos() override;
    virtual int prepare_table_dml_info_special(const ObDmlTableInfo& table_info,
                                               IndexDMLInfo* table_dml_info,
                                               ObIArray<IndexDMLInfo*> &index_dml_infos,
                                               ObIArray<IndexDMLInfo*> &all_index_dml_infos) override;

    DISALLOW_COPY_AND_ASSIGN(ObUpdateLogPlan);

  private:
  };
}
}
#endif
