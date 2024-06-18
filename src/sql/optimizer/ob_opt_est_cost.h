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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_
#define OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_
#include "ob_opt_est_cost_model.h"

namespace oceanbase
{
namespace sql
{
class ObDMLStmt;
class JoinPath;
struct OrderItem;
struct ObExprSelPair;
struct JoinFilterInfo;
class OptTableMetas;
class OptSelectivityCtx;
class ObOptimizerContext;

class ObOptEstCost
{
public:
  const static int64_t MAX_STORAGE_RANGE_ESTIMATION_NUM;
  enum MODEL_TYPE {
      NORMAL_MODEL = 0,
      VECTOR_MODEL
  };

  static int cost_nestloop(const ObCostNLJoinInfo &est_cost_info,
                           double &cost,
                           double &filter_selectivity,
                           common::ObIArray<ObExprSelPair> &all_predicate_sel,
                           const ObOptimizerContext &opt_ctx);

  static int cost_mergejoin(const ObCostMergeJoinInfo &est_cost_info,
                            double &cost,
                            const ObOptimizerContext &opt_ctx);

  static int cost_hashjoin(const ObCostHashJoinInfo &est_cost_info,
                           double &cost,
                           const ObOptimizerContext &opt_ctx);

  static int cost_sort_and_exchange(OptTableMetas *table_metas,
                                    OptSelectivityCtx *sel_ctx,
                                    const ObPQDistributeMethod::Type dist_method,
                                    const bool is_distributed,
                                    const bool input_local_order,
                                    const double input_card,
                                    const double input_width,
                                    const double input_cost,
                                    const int64_t out_parallel,
                                    const int64_t in_server_cnt,
                                    const int64_t in_parallel,
                                    const ObIArray<OrderItem> &expected_ordering,
                                    const bool need_sort,
                                    const int64_t prefix_pos,
                                    double &cost,
                                    const ObOptimizerContext &opt_ctx);

  static int cost_sort(const ObSortCostInfo &cost_info,
                       double &cost,
                       const ObOptimizerContext &opt_ctx);

  static int cost_exchange(const ObExchCostInfo &cost_info,
                           double &ex_cost,
                           const ObOptimizerContext &opt_ctx);

  static int cost_exchange_in(const ObExchInCostInfo &cost_info,
                              double &cost,
                              const ObOptimizerContext &opt_ctx);

  static int cost_exchange_out(const ObExchOutCostInfo &cost_info,
                               double &cost,
                               const ObOptimizerContext &opt_ctx);

  static double cost_merge_group(double rows,
                                 double res_rows,
                                 double width,
                                 const ObIArray<ObRawExpr *> &group_columns,
                                 int64_t agg_col_count,
                                 const ObOptimizerContext &opt_ctx);

  static double cost_hash_group(double rows,
                                double res_rows,
                                double width,
                                const ObIArray<ObRawExpr *> &group_columns,
                                int64_t agg_col_count,
                                const ObOptimizerContext &opt_ctx);

  static double cost_scalar_group(double rows,
                                  int64_t agg_col_count,
                                  const ObOptimizerContext &opt_ctx);

  static double cost_merge_distinct(double rows,
                                    double res_rows,
                                    double width,
                                    const ObIArray<ObRawExpr *> &distinct_columns,
                                    const ObOptimizerContext &opt_ctx);

  static double cost_hash_distinct(double rows,
                                   double res_rows,
                                   double width,
                                   const ObIArray<ObRawExpr *> &disinct_columns,
                                   const ObOptimizerContext &opt_ctx);

  static double cost_get_rows(double rows,
                              const ObOptimizerContext &opt_ctx);

  static double cost_sequence(double rows,
                              double uniq_sequence_cnt,
                              const ObOptimizerContext &opt_ctx);

  static double cost_material(const double rows,
                              const double average_row_size,
                              const ObOptimizerContext &opt_ctx);

  static double cost_read_materialized(const double rows,
                                       const ObOptimizerContext &opt_ctx);

  static double cost_filter_rows(double rows,
                                 ObIArray<ObRawExpr*> &filters,
                                 const ObOptimizerContext &opt_ctx);

  static int cost_subplan_filter(const ObSubplanFilterCostInfo &info,
                                 double &cost,
                                 const ObOptimizerContext &opt_ctx);

  static int cost_union_all(const ObCostMergeSetInfo &info,
                            double &cost,
                            const ObOptimizerContext &opt_ctx);

  static int cost_merge_set(const ObCostMergeSetInfo &info,
                            double &cost,
                            const ObOptimizerContext &opt_ctx);

  static int cost_hash_set(const ObCostHashSetInfo &info,
                           double &cost,
                           const ObOptimizerContext &opt_ctx);

  static double cost_quals(double rows, 
                           const ObIArray<ObRawExpr *> &quals, 
                           const ObOptimizerContext &opt_ctx,
                           bool need_scale = true);
  /*
   * entry point for estimating table access cost
   */
  static int cost_table(const ObCostTableScanInfo &est_cost_info,
                        int64_t parallel,
                        double &cost,
                        const ObOptimizerContext &opt_ctx);

  static int cost_table_for_parallel(const ObCostTableScanInfo &est_cost_info,
                                     const int64_t parallel,
                                     const double part_cnt_per_dop,
                                     double &px_cost,
                                     double &cost,
                                     const ObOptimizerContext &opt_ctx);

  static double cost_late_materialization_table_get(int64_t column_cnt,
                                                    const ObOptimizerContext &opt_ctx);

  static void cost_late_materialization_table_join(double left_card,
                                                   double left_cost,
                                                   double right_card,
                                                   double right_cost,
                                                   double &op_cost,
                                                   double &cost,
                                                   const ObOptimizerContext &opt_ctx);

  static void cost_late_materialization(double left_card,
                                        double left_cost,
                                        int64_t column_count,
                                        double &cost,
                                        const ObOptimizerContext &opt_ctx);

  static int cost_window_function(double rows, 
                                  double width, 
                                  double win_func_cnt, 
                                  double &cost,
                                  const ObOptimizerContext &opt_ctx);

  static int cost_insert(ObDelUpCostInfo& cost_info,
                         double &cost,
                         const ObOptimizerContext &opt_ctx);

  static int cost_update(ObDelUpCostInfo& cost_info,
                         double &cost,
                         const ObOptimizerContext &opt_ctx);

  static int cost_delete(ObDelUpCostInfo& cost_info,
                         double &cost,
                         const ObOptimizerContext &opt_ctx);

  static int calc_range_cost(const ObTableMetaInfo& table_meta_info,
                            const ObIArray<ObRawExpr *> &filters,
                            int64_t index_column_count,
                            int64_t range_count,
                            double range_sel,
                            double &cost,
                            const ObOptimizerContext &opt_ctx);

  static int estimate_width_for_table(const OptTableMetas &table_metas,
                                      const OptSelectivityCtx &ctx,
                                      const ObIArray<ColumnItem> &columns,
                                      int64_t table_id,
                                      double &width);

  static int estimate_width_for_exprs(const OptTableMetas &table_metas,
                                      const OptSelectivityCtx &ctx,
                                      const ObIArray<ObRawExpr *> &exprs,
                                      double &width);

   //将scan ranges转换为ObSimpleBatch
  //@param[in] scan_ranges :抽取出来的query scan range信息
  //@param[out] batch: 存储层估行需要的query range集合
  //@param[out] range: T_SCAN batch需要的range
  //@param[out] range_array: T_MULTI_SCAN batch需要的range
  static int construct_scan_range_batch(const ObIArray<ObNewRange> &scan_ranges,
                                        common::ObSimpleBatch &batch,
                                        common::SQLScanRange &range,
                                        common::SQLScanRangeArray &range_array);

  static int stat_estimate_partition_batch_rowcount(const ObCostTableScanInfo &est_cost_info,
                                                    const ObIArray<ObNewRange> &scan_ranges,
                                                    double &row_count);

  static int calculate_filter_selectivity(ObCostTableScanInfo &est_cost_info,
                                          common::ObIArray<ObExprSelPair> &all_predicate_sel);

  static int stat_estimate_single_range_rc(const ObCostTableScanInfo &est_cost_info,
                                           const ObNewRange &range,
                                           double &count);

  static double get_estimate_width_from_type(const ObExprResType &type);
  static double calc_pred_cost_per_row(const ObRawExpr *expr,
                                      double card,
                                      double &cost,
                                      const ObOptimizerContext &opt_ctx);
  static double cost_values_table(double rows,
                                  ObIArray<ObRawExpr*> &filters,
                                  const ObOptimizerContext &opt_ctx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObOptEstCost);
};

}
}

#endif /* OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_ */
