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

#ifndef _OB_DEL_UPD_LOG_PLAN_H
#define _OB_DEL_UPD_LOG_PLAN_H

#include "ob_log_plan.h"
#include "ob_log_del_upd.h"

namespace oceanbase
{
namespace sql
{
class ObDelUpdLogPlan: public ObLogPlan
{
public:
  ObDelUpdLogPlan(ObOptimizerContext &ctx, const ObDelUpdStmt *del_upd_stmt)
      : ObLogPlan(ctx, del_upd_stmt),
        max_dml_parallel_(ObGlobalHint::UNSET_PARALLEL),
        use_pdml_(false)
    {}
  virtual ~ObDelUpdLogPlan() {}

  inline virtual const ObDelUpdStmt *get_stmt() const override
  { return static_cast<const ObDelUpdStmt *>(stmt_); }

  int check_table_rowkey_distinct(const ObIArray<IndexDMLInfo *> &index_dml_infos);

  int check_fullfill_safe_update_mode(ObLogicalOperator *op);

  int do_check_fullfill_safe_update_mode(ObLogicalOperator *top, bool &is_not_fullfill);

  int calculate_insert_table_location_and_sharding(ObTablePartitionInfo *&insert_table_part,
                                                   ObShardingInfo *&insert_sharding);
  int calculate_table_location_and_sharding(const ObDelUpdStmt &stmt,
                                            const ObIArray<ObRawExpr*> &filters,
                                            const uint64_t table_id,
                                            const uint64_t ref_table_id,
                                            const ObIArray<common::ObObjectID> *part_ids,
                                            ObShardingInfo *&sharding_info,
                                            ObTablePartitionInfo *&table_partition_info);

  int calculate_table_location(const ObDelUpdStmt &stmt,
                               const ObIArray<ObRawExpr*> &filters,
                               const uint64_t table_id,
                               const uint64_t ref_table_id,
                               const common::ObIArray<common::ObObjectID> *part_ids,
                               ObTablePartitionInfo &table_partition_info);

  int compute_exchange_info_for_pdml_del_upd(const ObShardingInfo &source_sharding,
                                             const ObTablePartitionInfo &target_table_partition,
                                             const IndexDMLInfo &index_dml_info,
                                             bool is_index_maintenance,
                                             ObExchangeInfo &exch_info);

  int compute_hash_dist_exprs_for_pdml_del_upd(ObExchangeInfo &exch_info,
                                               const IndexDMLInfo &dml_info);

  int compute_exchange_info_for_pdml_insert(const ObShardingInfo &source_sharding,
                                            const ObTablePartitionInfo &target_table_partition,
                                            const IndexDMLInfo &index_dml_info,
                                            bool is_index_maintenance,
                                            ObExchangeInfo &exch_info);

  int compute_repartition_info_for_pdml_insert(const IndexDMLInfo &index_dml_info,
                                               const ObShardingInfo &target_sharding,
                                               ObRawExprFactory &expr_factory,
                                               ObExchangeInfo &exch_info);

  int build_merge_stmt_repartition_hash_key(const IndexDMLInfo &index_dml_info,
                                            ObRawExprFactory &expr_factory,
                                            ObIArray<ObRawExpr*> &case_when_exprs);

  int compute_hash_dist_exprs_for_pdml_insert(ObExchangeInfo &exch_info,
                                              const IndexDMLInfo &index_dml_info);

  int build_merge_stmt_hash_dist_exprs(const IndexDMLInfo &index_dml_info,
                                       ObIArray<ObRawExpr*> &rowkey_exprs);

  int replace_assignment_expr_from_dml_info(const IndexDMLInfo &index_dml_info,
                                            ObRawExpr *&expr);

  int candi_allocate_one_pdml_delete(bool is_index_maintenance,
                                     bool is_last_dml_op,
                                     bool is_pdml_update_split,
                                     IndexDMLInfo *index_dml_info);

  int create_pdml_delete_plan(ObLogicalOperator *&top,
                              const ObExchangeInfo &exch_info,
                              ObTablePartitionInfo *table_location,
                              bool is_index_maintenance,
                              bool is_last_dml_op,
                              bool need_partition_id,
                              bool is_pdml_update_split,
                              IndexDMLInfo *index_dml_info);

  int allocate_pdml_delete_as_top(ObLogicalOperator *&top,
                                  bool is_index_maintenance,
                                  bool is_last_dml_op,
                                  bool need_partition_id,
                                  bool is_pdml_update_split,
                                  ObTablePartitionInfo *table_location,
                                  IndexDMLInfo *index_dml_info);

  int candi_allocate_one_pdml_insert(bool is_index_maintenance,
                                     bool is_last_dml_op,
                                     bool is_pdml_update_split,
                                     IndexDMLInfo *index_dml_info,
                                     OSGShareInfo *osg_info = NULL);

  int create_pdml_insert_plan(ObLogicalOperator *&top,
                              const ObExchangeInfo &exch_info,
                              ObTablePartitionInfo *table_location,
                              bool is_index_maintenance,
                              bool is_last_dml_op,
                              bool need_partition_id,
                              bool is_pdml_update_split,
                              IndexDMLInfo *index_dml_info,
                              OSGShareInfo *osg_info);

  int create_online_ddl_plan(ObLogicalOperator *&top,
                             const ObExchangeInfo &exch_info,
                             ObTablePartitionInfo *table_location,
                             const ObIArray<OrderItem> &sort_keys,
                             const ObIArray<OrderItem> &sample_sort_keys,
                             const ObIArray<OrderItem> &px_coord_sort_keys,
                             bool is_index_maintenance,
                             bool is_last_dml_op,
                             bool need_partition_id,
                             bool is_pdml_update_split,
                             IndexDMLInfo *index_dml_info);

  int get_ddl_sample_sort_column_count(int64_t &sample_sort_column_count);

  int get_ddl_sort_keys_with_part_expr(ObExchangeInfo &exch_info,
                                       common::ObIArray<OrderItem> &sort_keys,
                                       common::ObIArray<OrderItem> &sample_sort_keys);

  int replace_exch_info_exprs(ObExchangeInfo &exch_info);

  int allocate_pdml_insert_as_top(ObLogicalOperator *&top,
                                  bool is_index_maintenance,
                                  bool is_last_dml_op,
                                  bool need_partition_id,
                                  bool is_pdml_update_split,
                                  ObTablePartitionInfo *table_location,
                                  IndexDMLInfo *dml_info);

  int candi_allocate_one_pdml_update(bool is_index_maintenance,
                                     bool is_last_dml_op,
                                     IndexDMLInfo *index_dml_info);
  int create_pdml_update_plan(ObLogicalOperator *&top,
                              const ObExchangeInfo &exch_info,
                              ObTablePartitionInfo *table_location,
                              bool is_index_maintenance,
                              bool is_last_dml_op,
                              bool need_partition_id,
                              IndexDMLInfo *index_dml_info);

  int allocate_pdml_update_as_top(ObLogicalOperator *&top,
                                  bool is_index_maintenance,
                                  bool is_last_dml_op,
                                  bool need_partition_id,
                                  ObTablePartitionInfo *table_location,
                                  IndexDMLInfo *index_dml_info);

  int check_need_exchange_for_pdml_del_upd(ObLogicalOperator *top,
                                           const ObExchangeInfo &exch_info,
                                           uint64_t table_id,
                                           bool &need_exchange);

  int create_index_dml_info(const IndexDMLInfo &orgi_dml_info,
                            IndexDMLInfo *&opt_dml_info);
  //split update index dml info with delete and insert
  int split_update_index_dml_info(const IndexDMLInfo &upd_dml_info,
                                  IndexDMLInfo *&del_dml_info,
                                  IndexDMLInfo *&ins_dml_info);
  int collect_related_local_index_ids(IndexDMLInfo &primary_dml_info);

  // PX range sort need the coordinator to help sampling.
  // The original sort expr can not used in PX coord or it will failed to in ALLOC_EXPR.
  // We replace the expr with  pseudo column (ObOpPseudoColumnRawExpr) here.
  int gen_px_coord_sampling_sort_keys(const ObIArray<OrderItem> &src,
                                      ObIArray<OrderItem> &dst);

  virtual int prepare_dml_infos();
  int prune_virtual_column(IndexDMLInfo &index_dml_info);

  int prepare_table_dml_info_basic(const ObDmlTableInfo& table_info,
                                   IndexDMLInfo*& table_dml_info,
                                   ObIArray<IndexDMLInfo*> &index_dml_infos,
                                   const bool has_tg);
  virtual int prepare_table_dml_info_special(const ObDmlTableInfo& table_info,
                                             IndexDMLInfo* table_dml_info,
                                             ObIArray<IndexDMLInfo*> &index_dml_infos,
                                             ObIArray<IndexDMLInfo*> &all_index_dml_infos);
  int generate_part_key_ids(const ObTableSchema &index_schema,
                            ObIArray<uint64_t> &array) const;
  int generate_index_column_exprs(const uint64_t table_id,
                                  const ObTableSchema &index_schema,
                                  const ObAssignments &assignments,
                                  ObIArray<ObColumnRefRawExpr*> &column_exprs);

  int generate_index_column_exprs(uint64_t table_id,
                                  const ObTableSchema &index_schema,
                                  ObIArray<ObColumnRefRawExpr*> &column_exprs);

  int generate_index_rowkey_exprs(uint64_t table_id,
                                  const ObTableSchema &index_schema,
                                  ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                  bool need_spk);

  int check_index_update(ObAssignments assigns,
                         const ObTableSchema& index_schema,
                         const bool is_update_view,
                         bool& need_update);

  int fill_index_column_convert_exprs(ObRawExprCopier &copier,
                                      const ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                      ObIArray<ObRawExpr *> &column_convert_exprs);
  
  virtual int add_extra_dependency_table() const override;
  int check_update_unique_key(const ObTableSchema* index_schema,
                                IndexDMLInfo*& index_dml_info) const;
  int check_update_part_key(const ObTableSchema* index_schema,
                            IndexDMLInfo*& index_dml_info) const;
  int check_update_primary_key(const ObTableSchema* index_schema,
                               IndexDMLInfo*& index_dml_info) const;
  int allocate_link_dml_as_top(ObLogicalOperator *&old_top);
  bool use_pdml() const { return use_pdml_; }
  int compute_dml_parallel();
  int get_parallel_info_from_candidate_plans(int64_t &dop) const;
  int get_pdml_parallel_degree(const int64_t target_part_cnt, int64_t &dop) const;

protected:
  virtual int generate_normal_raw_plan() override;
  virtual int generate_dblink_raw_plan() override;
  int allocate_optimizer_stats_gathering_as_top(ObLogicalOperator *&old_top,
                                                OSGShareInfo &info,
                                                OSG_TYPE type);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDelUpdLogPlan);

protected:
  ObSEArray<IndexDMLInfo *, 1, common::ModulePageAllocator, true> index_dml_infos_;
  ObSEArray<share::schema::ObSchemaObjVersion, 4, common::ModulePageAllocator, true> extra_dependency_tables_;
  int64_t max_dml_parallel_;
  int64_t use_pdml_;
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* _OB_DEL_UPD_LOG_PLAN_H_ */
