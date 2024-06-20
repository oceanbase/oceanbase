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
#include "sql/optimizer/ob_del_upd_log_plan.h"
#include "sql/ob_sql_define.h"

namespace oceanbase
{
namespace sql
{
class ObDMLStmt;
class ObLogInsert;
class ObInsertStmt;
typedef common::ObSEArray<common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true>, 1, common::ModulePageAllocator, true> RowParamMap;
class ObInsertLogPlan: public ObDelUpdLogPlan
{
public:
  ObInsertLogPlan(ObOptimizerContext &ctx, const ObInsertStmt *insert_stmt)
      : ObDelUpdLogPlan(ctx, insert_stmt),
        is_direct_insert_(false),
        is_insert_overwrite_(false)
  { }
  virtual ~ObInsertLogPlan()
  { }
  virtual int generate_normal_raw_plan() override;

  const ObInsertStmt *get_stmt() const override
  { return reinterpret_cast<const ObInsertStmt*>(stmt_); }

  virtual int prepare_dml_infos() override;

  common::ObIArray<IndexDMLInfo *> &get_replace_del_index_del_infos()
  { return replace_del_index_del_infos_; }
  const common::ObIArray<IndexDMLInfo *> &get_replace_del_index_del_infos() const
  { return replace_del_index_del_infos_; }

  common::ObIArray<IndexDMLInfo *> &get_insert_up_index_upd_infos()
  { return insert_up_index_upd_infos_; }
  const common::ObIArray<IndexDMLInfo *> &get_insert_up_index_upd_infos() const
  { return insert_up_index_upd_infos_; }

  bool is_direct_insert() const { return is_direct_insert_; }
  void set_is_insert_overwrite(const bool is_insert_overwrite) { is_insert_overwrite_ = is_insert_overwrite; }
  bool is_insert_overwrite() const { return is_insert_overwrite_; }
protected:
  int allocate_insert_values_as_top(ObLogicalOperator *&top);
  int candi_allocate_insert(OSGShareInfo *osg_info);
  int build_lock_row_flag_expr(ObConstRawExpr *&lock_row_flag_expr);
  int create_insert_plans(ObIArray<CandidatePlan> &candi_plans,
                          ObTablePartitionInfo *insert_table_part,
                          ObShardingInfo *insert_table_sharding,
                          ObConstRawExpr *lock_row_flag_expr,
                          const bool force_no_multi_part,
                          const bool force_multi_part,
                          ObIArray<CandidatePlan> &insert_plans,
                          OSGShareInfo *osg_info);
  int allocate_insert_as_top(ObLogicalOperator *&top,
                             ObRawExpr *lock_row_flag_expr,
                             ObTablePartitionInfo *table_partition_info,
                             ObShardingInfo *insert_op_sharding,
                             bool is_multi_part,
                             bool is_partition_wise);
  int candi_allocate_pdml_insert(OSGShareInfo *osg_info);
  int candi_allocate_optimizer_stats_merge(OSGShareInfo *osg_info);

  int get_osg_type(bool is_multi_part_dml,
                   ObShardingInfo *insert_table_sharding,
                   int64_t distributed_method,
                   OSG_TYPE &type);

  virtual int get_best_insert_dist_method(ObLogicalOperator &top,
                                          ObTablePartitionInfo *insert_table_partition,
                                          ObShardingInfo *insert_table_sharding,
                                          const bool force_no_multi_part,
                                          const bool force_multi_part,
                                          int64_t &distributed_methods,
                                          bool &is_multi_part_dml);
  virtual int check_insert_plan_need_multi_partition_dml(ObTablePartitionInfo *insert_table_partition,
                                                        ObShardingInfo *insert_table_sharding,
                                                        bool &is_multi_part_dml);
  int check_basic_sharding_for_insert_stmt(ObShardingInfo &target_sharding,
                                           ObLogicalOperator &child,
                                           bool &is_basic);
  int check_if_match_partition_wise_insert(ObShardingInfo &target_sharding,
                                           ObLogicalOperator &child,
                                           bool &is_partition_wise);

  virtual int prepare_table_dml_info_special(const ObDmlTableInfo& table_info,
                                             IndexDMLInfo* table_dml_info,
                                             ObIArray<IndexDMLInfo*> &index_dml_infos,
                                             ObIArray<IndexDMLInfo*> &all_index_dml_infos) override;

  int copy_index_dml_infos_for_replace(ObIArray<IndexDMLInfo*> &src_dml_infos,
                                       ObIArray<IndexDMLInfo*> &dst_dml_infos);
  int copy_index_dml_infos_for_insert_up(const ObInsertTableInfo& table_info,
                                         IndexDMLInfo* table_dml_info,
                                         ObIArray<IndexDMLInfo*> &index_dml_infos,
                                         ObIArray<IndexDMLInfo*> &dst_dml_infos);
  int prepare_unique_constraint_infos(const ObDmlTableInfo& table_info);
  int prepare_unique_constraint_info(const ObTableSchema &index_schema,
                                     const uint64_t table_id,
                                     ObUniqueConstraintInfo &constraint_info);
  int prepare_table_dml_info_for_ddl(const ObInsertTableInfo& table_info,
                                     ObIArray<IndexDMLInfo*> &all_index_dml_infos);

  int get_all_rowkey_columns_for_ddl(const ObInsertTableInfo& table_info,
                                     const ObTableSchema* ddl_table_schema,
                                     ObIArray<ObColumnRefRawExpr*> &column_exprs);
  int get_all_columns_for_ddl(const ObInsertTableInfo& table_info,
                              const ObTableSchema* ddl_table_schema,
                              ObIArray<ObColumnRefRawExpr*> &column_exprs);
  int get_all_part_columns_for_ddl(const ObInsertTableInfo& table_info,
                                   const ObTableSchema* data_table_schema,
                                   ObIArray<ObColumnRefRawExpr*> &column_exprs);
  int build_column_conv_for_shadow_pk(const ObInsertTableInfo& table_info,
                                      ObColumnRefRawExpr *column_expr,
                                      ObRawExpr *&column_conv_expr);

  int check_contain_non_onetime_expr(const ObRawExpr *expr, bool &contain);
  int check_contain_non_onetime_expr(const ObIArray<ObRawExpr *> &exprs, bool &contain);
private:
  int get_index_part_ids(const ObInsertTableInfo& table_info, const ObTableSchema *&data_table_schema, const ObTableSchema *&index_schema, ObIArray<uint64_t> &index_part_ids);
  int generate_osg_share_info(OSGShareInfo *&info);
  int check_need_online_stats_gather(bool &need_osg);
  int set_is_direct_insert();
  DISALLOW_COPY_AND_ASSIGN(ObInsertLogPlan);
private:
  common::ObSEArray<IndexDMLInfo *, 1, common::ModulePageAllocator, true> replace_del_index_del_infos_;
  common::ObSEArray<IndexDMLInfo *, 1, common::ModulePageAllocator, true> insert_up_index_upd_infos_;
  common::ObSEArray<ObUniqueConstraintInfo, 8, common::ModulePageAllocator, true> uk_constraint_infos_;
  bool is_direct_insert_;
  bool is_insert_overwrite_;
};
}
}
#endif // _OB_INSERT_LOG_PLAN_H
