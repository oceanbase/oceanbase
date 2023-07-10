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

#ifndef _OB_MERGE_LOG_PLAN_H
#define _OB_MERGE_LOG_PLAN_H
#include "sql/optimizer/ob_insert_log_plan.h"
#include "sql/resolver/dml/ob_merge_stmt.h"

namespace oceanbase
{
namespace sql
{
struct ObStmtCompareContext;

class ObMergeLogPlan : public ObDelUpdLogPlan
{
public:
  ObMergeLogPlan(ObOptimizerContext &ctx, const ObMergeStmt *merge_stmt)
      : ObDelUpdLogPlan(ctx, merge_stmt)
      {
      }
  virtual ~ObMergeLogPlan() {}

  const ObMergeStmt *get_stmt() const
  { return reinterpret_cast<const ObMergeStmt*>(stmt_); }

  ObIArray<ObRawExpr *>& get_insert_condition() { return insert_condition_exprs_; }
  const ObIArray<ObRawExpr *>& get_insert_condition() const { return insert_condition_exprs_; }
  ObIArray<ObRawExpr *>& get_update_condition() { return update_condition_exprs_; }
  const ObIArray<ObRawExpr *>& get_update_condition() const { return update_condition_exprs_; }
  ObIArray<ObRawExpr *>& get_delete_condition() { return delete_condition_exprs_; }
  const ObIArray<ObRawExpr *>& get_delete_condition() const { return delete_condition_exprs_; }
  virtual int prepare_dml_infos() override;
private:
  virtual int generate_normal_raw_plan() override;
  int candi_allocate_merge();
  int create_merge_plans(ObIArray<CandidatePlan> &candi_plans,
                         ObTablePartitionInfo *insert_table_part,
                         ObShardingInfo *insert_sharding,
                         const bool force_no_multi_part,
                         const bool force_multi_part,
                         ObIArray<CandidatePlan> &update_plans);
  int allocate_merge_as_top(ObLogicalOperator *&top,
                            ObTablePartitionInfo *table_partition_info,
                            bool is_multi_part_dml,
                            const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> *equal_pairs);
  int candi_allocate_subplan_filter_for_merge();
  int candi_allocate_pdml_merge();
  int create_pdml_merge_plan(ObLogicalOperator *&top,
                             ObTablePartitionInfo *insert_table_part,
                             ObExchangeInfo &exch_info);

  int check_merge_need_multi_partition_dml(ObLogicalOperator &top,
                                           ObTablePartitionInfo *insert_table_partition,
                                           ObShardingInfo *insert_sharding,
                                           bool &is_multi_part_dml,
                                           ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &equal_pairs);
  int check_update_insert_sharding_basic(ObLogicalOperator &top,
                                         ObShardingInfo *insert_sharding,
                                         ObShardingInfo *update_sharding,
                                         bool &is_basic,
                                         ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &equal_pairs);
  bool match_same_partition(const ObShardingInfo &l_sharding_info,
                            const ObShardingInfo &r_sharding_info);
  int generate_equal_constraint(ObLogicalOperator &top,
                                ObShardingInfo &insert_sharding,
                                bool &can_gen_cons,
                                ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &equal_pairs);
  int has_equal_values(const ObIArray<ObRawExpr*> &l_const_exprs,
                       const ObIArray<ObRawExpr*> &r_const_exprs,
                       bool &has_equal,
                       ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &equal_pairs);
  int get_const_expr_values(const ObRawExpr *part_expr,
                            const ObIArray<ObRawExpr*> &conds,
                            ObIArray<ObRawExpr*> &const_exprs);
  int get_target_table_scan(const uint64_t target_table_id,
                            ObLogicalOperator *cur_op,
                            ObLogTableScan *&target_table_scan);
  int check_update_insert_sharding_partition_wise(ObLogicalOperator &top,
                                                  ObShardingInfo *insert_sharding,
                                                  bool &is_partition_wise);
  int prepare_table_dml_info_insert(const ObMergeTableInfo& merge_info,
                                    IndexDMLInfo* table_dml_info,
                                    ObIArray<IndexDMLInfo*> &index_dml_infos,
                                    ObIArray<IndexDMLInfo*> &all_index_dml_infos);
  int prepare_table_dml_info_update(const ObMergeTableInfo& merge_info,
                                    IndexDMLInfo* table_dml_info,
                                    ObIArray<IndexDMLInfo*> &index_dml_infos,
                                    ObIArray<IndexDMLInfo*> &all_index_dml_infos);
  int prepare_table_dml_info_delete(const ObMergeTableInfo& merge_info,
                                    IndexDMLInfo* table_dml_info,
                                    ObIArray<IndexDMLInfo*> &index_dml_infos,
                                    ObIArray<IndexDMLInfo*> &all_index_dml_infos);

  int check_merge_stmt_need_multi_partition_dml(bool &is_multi_part_dml, bool &is_one_part_table);


  DISALLOW_COPY_AND_ASSIGN(ObMergeLogPlan);

private:
  common::ObSEArray<IndexDMLInfo *, 4, common::ModulePageAllocator, true> index_update_infos_;
  common::ObSEArray<IndexDMLInfo *, 4, common::ModulePageAllocator, true> index_delete_infos_;

  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> insert_condition_exprs_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> update_condition_exprs_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> delete_condition_exprs_;
};
}//sql
}//oceanbase
#endif
