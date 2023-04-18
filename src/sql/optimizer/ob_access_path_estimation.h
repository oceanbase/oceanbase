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

#ifndef OB_ACCESS_PATH_ESTIMATION_H
#define OB_ACCESS_PATH_ESTIMATION_H

#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_join_order.h"
namespace oceanbase {
namespace sql {
class AccessPath;

struct ObBatchEstTasks
{
  ObAddr addr_;
  obrpc::ObEstPartArg arg_;
  obrpc::ObEstPartRes res_;
  ObArray<AccessPath *> paths_;

  bool check_result_reliable() const;

  TO_STRING_KV(K_(addr));
};

class ObAccessPathEstimation
{
public:
  static int estimate_rowcount(ObOptimizerContext &ctx,
                               ObTableMetaInfo &meta,
                               common::ObIArray<AccessPath*> &paths);

  static int estimate_full_table_rowcount(ObOptimizerContext &ctx,
                                          const ObTablePartitionInfo &table_part_info,
                                          ObTableMetaInfo &meta);
private:

  static int64_t get_get_range_count(const ObIArray<ObNewRange> &ranges);

  static int64_t get_scan_range_count(const ObIArray<ObNewRange> &ranges);

  static int choose_best_estimation_method(const AccessPath *path,
                                           const ObTableMetaInfo &meta,
                                           bool &use_storage_stat,
                                           bool &is_vt);
  
  static int choose_leader_replica(const ObCandiTabletLoc &part_loc_info,
                                   const bool can_use_remote,
                                   const ObAddr &local_addr,
                                   EstimatedPartition &best_partition);

  static int process_vtable_estimation(AccessPath *path);

  /// following functions are mainly uesd by statistics estimation
  static int process_statistics_estimation(const ObTableMetaInfo &meta,
                                           AccessPath *paths);

  /// following functions are mainly used by storage estimation
  static int process_storage_estimation(ObOptimizerContext &ctx,
                                        ObTableMetaInfo &meta,
                                        ObIArray<AccessPath *>& paths);

  static int calc_skip_scan_prefix_ndv(AccessPath &ap, double &prefix_ndv);

  static int get_skip_scan_prefix_exprs(ObIArray<ColumnItem> &column_items,
                                        int64_t skip_scan_offset,
                                        ObIArray<ObRawExpr*> &prefix_exprs);

  static int update_use_skip_scan(ObCostTableScanInfo &est_cost_info,
                                  ObIArray<ObExprSelPair> &all_predicate_sel,
                                  OptSkipScanState &use_skip_scan);

  static int do_storage_estimation(ObOptimizerContext &ctx,
                                   ObBatchEstTasks &tasks);

  static int get_task(ObIArray<ObBatchEstTasks *>& tasks,
                      const ObAddr &addr,
                      ObBatchEstTasks *&task);

  static int create_task(ObIAllocator &allocator,
                         const ObAddr &addr,
                         ObBatchEstTasks *&task);


  static int add_index_info(ObOptimizerContext &ctx,
                            ObIAllocator &allocator,
                            ObBatchEstTasks *task,
                            const EstimatedPartition &part,
                            AccessPath *ap);

  static int construct_scan_range_batch(ObIAllocator &allocator,
                                        const ObIArray<ObNewRange> &scan_ranges,
                                        ObSimpleBatch &batch);

  static int construct_geo_scan_range_batch(ObIAllocator &allocator,
                                            const ObIArray<ObNewRange> &scan_ranges,
                                            ObSimpleBatch &batch);

  static bool is_multi_geo_range(const ObNewRange &range);

  static int estimate_prefix_range_rowcount(
      const obrpc::ObEstPartResElement &result,
      ObCostTableScanInfo &est_cost_info,
      double &logical_row_count,
      double &physical_row_count);

  static int fill_cost_table_scan_info(ObCostTableScanInfo &est_cost_info,
                                       const RowCountEstMethod est_method,
                                       double &output_row_count,
                                       double &logical_row_count,
                                       double &physical_row_count,
                                       double &index_back_row_count);

  static int get_key_ranges(ObOptimizerContext &ctx,
                            ObIAllocator &allocator,
                            const ObTabletID &tablet_id,
                            AccessPath *ap,
                            ObIArray<common::ObNewRange> &new_ranges);

  static int convert_agent_vt_key_ranges(ObOptimizerContext &ctx,
                                         ObIAllocator &allocator,
                                         AccessPath *ap,
                                         ObIArray<common::ObNewRange> &new_ranges);

  static int gen_agent_vt_table_convert_info(const uint64_t vt_table_id,
                                             const ObTableSchema *vt_table_schema,
                                             const ObTableSchema *real_table_schema,
                                             const ObIArray<ColumnItem> &range_columns,
                                             bool &has_tenant_id_col,
                                             ObIArray<ObObjMeta> &key_types);

  static int convert_physical_rowid_ranges(ObOptimizerContext &ctx,
                                           ObIAllocator &allocator,
                                           const ObTabletID &tablet_id,
                                           const uint64_t index_id,
                                           ObIArray<common::ObNewRange> &new_ranges);

  static int storage_estimate_full_table_rowcount(ObOptimizerContext &ctx,
                                                  const ObCandiTabletLoc &part_loc_info,
                                                  ObTableMetaInfo &meta);

  static int estimate_full_table_rowcount_by_meta_table(ObOptimizerContext &ctx,
                                                        const ObIArray<ObTabletID> &all_tablet_ids,
                                                        const ObIArray<ObLSID> &all_ls_ids,
                                                        ObTableMetaInfo &meta);

};

}
}

#endif // OB_ACCESS_PATH_ESTIMATION_H
