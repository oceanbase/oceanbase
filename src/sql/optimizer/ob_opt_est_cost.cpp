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

#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_optimizer.h"
#include "storage/access/ob_table_scan_range.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "ob_opt_est_parameter_normal.h"
#include "ob_opt_est_parameter_vector.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "ob_opt_est_cost_model_vector.h"
#include "ob_opt_est_parameter_normal.h"
#include "ob_opt_est_parameter_vector.h"

#define GET_COST_MODEL()                                                \
      ObOptEstCostModel normal_model(cost_params_normal,                \
                                     opt_ctx.get_system_stat());        \
      ObOptEstVectorCostModel vector_model(cost_params_vector,          \
                                           opt_ctx.get_system_stat());  \
      ObOptEstCostModel *model = &normal_model;                 \
      if (VECTOR_MODEL == opt_ctx.get_cost_model_type()) {      \
        model = &vector_model;                                  \
      }                                                         \


using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::storage;
using namespace oceanbase::jit::expr;
// using share::schema::ObSchemaGetterGuard;

const int64_t ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM = 10;

int ObOptEstCost::cost_nestloop(const ObCostNLJoinInfo &est_cost_info,
                                double &cost,
                                double &filter_selectivity,
                                ObIArray<ObExprSelPair> &all_predicate_sel,
                                const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_nestloop(est_cost_info,
                                  cost,
                                  filter_selectivity,
                                  all_predicate_sel))) {
    LOG_WARN("failed to est cost for nestloop join", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_mergejoin(const ObCostMergeJoinInfo &est_cost_info,
                                 double &cost,
                                 const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_mergejoin(est_cost_info,
                                   cost))) {
    LOG_WARN("failed to est cost for merge join", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_hashjoin(const ObCostHashJoinInfo &est_cost_info,
                                double &cost,
                                const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_hashjoin(est_cost_info,
                                   cost))) {
    LOG_WARN("failed to est cost for hash join", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_sort_and_exchange(OptTableMetas *table_metas,
                                         OptSelectivityCtx *sel_ctx,
                                         const ObPQDistributeMethod::Type dist_method,
                                         const bool is_distributed,
                                         const bool is_local_order,
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
                                         const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_sort_and_exchange(table_metas,
                                          sel_ctx,
                                          dist_method,
                                          is_distributed,
                                          is_local_order,
                                          input_card,
                                          input_width,
                                          input_cost,
                                          out_parallel,
                                          in_server_cnt,
                                          in_parallel,
                                          expected_ordering,
                                          need_sort,
                                          prefix_pos,
                                          cost))) {
    LOG_WARN("failed to est cost for sort and exchange", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_sort(const ObSortCostInfo &cost_info,
                            double &cost,
                            const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_sort(cost_info,
                              cost))) {
    LOG_WARN("failed to est cost for sort", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_exchange(const ObExchCostInfo &cost_info,
                                double &cost,
                                const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_exchange(cost_info,
                                  cost))) {
    LOG_WARN("failed to est cost for exchange", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_exchange_in(const ObExchInCostInfo &cost_info,
                                   double &cost,
                                   const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_exchange_in(cost_info,
                                     cost))) {
    LOG_WARN("failed to est cost for exchange in", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_exchange_out(const ObExchOutCostInfo &cost_info,
                                    double &cost,
                                    const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_exchange_out(cost_info,
                                      cost))) {
    LOG_WARN("failed to est cost for exchange out", K(ret));
  }
  return ret;
}

double ObOptEstCost::cost_merge_group(double rows,
                                      double res_rows,
                                      double row_width,
                                      const ObIArray<ObRawExpr *> &group_columns,
                                      int64_t agg_col_count,
                                      const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_merge_group(rows,
                                  res_rows,
                                  row_width,
                                  group_columns,
                                  agg_col_count);
}

double ObOptEstCost::cost_hash_group(double rows,
                                     double res_rows,
                                     double row_width,
                                     const ObIArray<ObRawExpr *> &group_columns,
                                     int64_t agg_col_count,
                                     const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_hash_group(rows,
                                res_rows,
                                row_width,
                                group_columns,
                                agg_col_count);
}

double ObOptEstCost::cost_scalar_group(double rows,
                                       int64_t agg_col_count,
                                       const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_scalar_group(rows,
                                  agg_col_count);
}

double ObOptEstCost::cost_merge_distinct(double rows,
                                         double res_rows,
                                         double row_width,
                                         const ObIArray<ObRawExpr *> &distinct_columns,
                                         const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_merge_distinct(rows,
                                    res_rows,
                                    row_width,
                                    distinct_columns);
}

double ObOptEstCost::cost_hash_distinct(double rows,
                                        double res_rows,
                                        double row_width,
                                        const ObIArray<ObRawExpr *> &distinct_columns,
                                        const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_hash_distinct(rows,
                                  res_rows,
                                  row_width,
                                  distinct_columns);
}

double ObOptEstCost::cost_sequence(double rows,
                                   double uniq_sequence_cnt,
                                   const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_sequence(rows,
                              uniq_sequence_cnt);
}

double ObOptEstCost::cost_get_rows(double rows, const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_get_rows(rows);
}

double ObOptEstCost::cost_read_materialized(double rows, const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_read_materialized(rows);
}

double ObOptEstCost::cost_material(const double rows,
                                   const double average_row_size,
                                   const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_material(rows,
                              average_row_size);
}

double ObOptEstCost::cost_filter_rows(double rows,
                                      ObIArray<ObRawExpr*> &filters,
                                      const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_filter_rows(rows,
                                 filters);
}

int ObOptEstCost::cost_subplan_filter(const ObSubplanFilterCostInfo &info,
                                      double &cost,
                                      const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_subplan_filter(info, cost))) {
    LOG_WARN("failed to est cost for subplan filter", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_union_all(const ObCostMergeSetInfo &info,
                                 double &cost,
                                 const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_union_all(info,
                                   cost))) {
    LOG_WARN("failed to est cost for union all", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_merge_set(const ObCostMergeSetInfo &info,
                                 double &cost,
                                 const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_merge_set(info,
                                   cost))) {
    LOG_WARN("failed to est cost for merge set", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_hash_set(const ObCostHashSetInfo &info,
                                double &cost,
                                const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_hash_set(info,
                                  cost))) {
    LOG_WARN("failed to est cost for hash set", K(ret));
  }
  return ret;
}

double ObOptEstCost::cost_quals(double rows,
                                const ObIArray<ObRawExpr *> &quals,
                                const ObOptimizerContext &opt_ctx,
                                bool need_scale)
{
  GET_COST_MODEL();
  return model->cost_quals(rows, quals, need_scale);
}

int ObOptEstCost::cost_table(const ObCostTableScanInfo &est_cost_info,
                             int64_t parallel,
                             double &cost,
                             const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_table(est_cost_info,
                                parallel,
                                cost))) {
    LOG_WARN("failed to est cost for table scan", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_table_for_parallel(const ObCostTableScanInfo &est_cost_info,
                                          const int64_t parallel,
                                          const double part_cnt_per_dop,
                                          double &px_cost,
                                          double &cost,
                                          const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_table_for_parallel(est_cost_info,
                                                            parallel,
                                                            part_cnt_per_dop,
                                                            px_cost,
                                                            cost))) {
    LOG_WARN("failed to est cost for table scan parallel", K(ret));
  }
  return ret;
}

double ObOptEstCost::cost_late_materialization_table_get(int64_t column_cnt, const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->cost_late_materialization_table_get(column_cnt);
}

void ObOptEstCost::cost_late_materialization_table_join(double left_card,
                                                        double left_cost,
                                                        double right_card,
                                                        double right_cost,
                                                        double &op_cost,
                                                        double &cost,
                                                        const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  model->cost_late_materialization_table_join(left_card,
                                              left_cost,
                                              right_card,
                                              right_cost,
                                              op_cost,
                                              cost);
}

void ObOptEstCost::cost_late_materialization(double left_card,
                                             double left_cost,
                                             int64_t column_count,
                                             double &cost,
                                             const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  model->cost_late_materialization(left_card,
                                  left_cost,
                                  column_count,
                                  cost);
}

int ObOptEstCost::cost_window_function(double rows,
                                       double width,
                                       double win_func_cnt,
                                       double &cost,
                                       const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_window_function(rows,
                                        width,
                                        win_func_cnt,
                                        cost))) {
    LOG_WARN("failed to est cost for window function", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_insert(ObDelUpCostInfo& cost_info,
                              double &cost,
                              const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_insert(cost_info,
                                cost))) {
    LOG_WARN("failed to est cost for insert", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_update(ObDelUpCostInfo& cost_info,
                              double &cost,
                              const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_update(cost_info,
                                cost))) {
    LOG_WARN("failed to est cost for update", K(ret));
  }
  return ret;
}

int ObOptEstCost::cost_delete(ObDelUpCostInfo& cost_info,
                              double &cost,
                              const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->cost_delete(cost_info,
                                cost))) {
    LOG_WARN("failed to est cost for delete", K(ret));
  }
  return ret;
}

int ObOptEstCost::calc_range_cost(const ObTableMetaInfo& table_meta_info,
                                  const ObIArray<ObRawExpr *> &filters,
                                  int64_t index_column_count,
                                  int64_t range_count,
                                  double range_sel,
                                  double &cost,
                                  const ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  GET_COST_MODEL();
  if (OB_FAIL(model->calc_range_cost(table_meta_info,
                                    filters,
                                    index_column_count,
                                    range_count,
                                    range_sel,
                                    cost))) {
    LOG_WARN("failed to est cost for range scan", K(ret));
  }
  return ret;
}

int ObOptEstCost::estimate_width_for_table(const OptTableMetas &table_metas,
                                           const OptSelectivityCtx &ctx,
                                           const ObIArray<ColumnItem> &columns,
                                           int64_t table_id,
                                           double &width)
{
  int ret = OB_SUCCESS;
  width = 0.0;
  ObArray<ObRawExpr *> column_exprs;
  const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_id);
  if (OB_ISNULL(ctx.get_opt_stat_manager()) ||
      OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const ColumnItem &column_item = columns.at(i);
      ObColumnRefRawExpr *column_expr = column_item.expr_;
      if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (column_item.get_column_type() == NULL ||
                 column_item.table_id_ != table_id ||
                 !column_expr->is_explicited_reference() ||
                 column_expr->is_hidden_column()) {
        // do nothing
      } else {
        ObGlobalColumnStat stat;
        if (OB_NOT_NULL(table_meta) && table_meta->use_opt_stat() &&
            OB_FAIL(ctx.get_opt_stat_manager()->get_column_stat(ctx.get_session_info()->get_effective_tenant_id(),
                                                                table_meta->get_ref_table_id(),
                                                                table_meta->get_all_used_parts(),
                                                                column_expr->get_column_id(),
                                                                table_meta->get_all_used_global_parts(),
                                                                table_meta->get_rows(),
                                                                table_meta->get_scale_ratio(),
                                                                stat))) {
          LOG_WARN("failed to get column stat", K(ret));
        } else if (stat.avglen_val_ != 0) {
          width += stat.avglen_val_;
        } else {
          width += get_estimate_width_from_type(column_expr->get_result_type());
        }
      }
    }
  }
  return ret;
}

int ObOptEstCost::estimate_width_for_exprs(const OptTableMetas &table_metas,
                                           const OptSelectivityCtx &ctx,
                                           const ObIArray<ObRawExpr *> &exprs,
                                           double &width)
{
  int ret = OB_SUCCESS;
  width = 0.0;
  if (OB_ISNULL(ctx.get_opt_stat_manager()) ||
      OB_ISNULL(ctx.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      const ObRawExpr *expr = exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr", K(ret));
      } else if (expr->is_column_ref_expr() &&
                 OB_INVALID_ID != static_cast<const ObColumnRefRawExpr*>(expr)->get_table_id()) {
        // column expr
        const ObColumnRefRawExpr* column_expr = static_cast<const ObColumnRefRawExpr*>(expr);
        uint64_t table_id = column_expr->get_table_id();
        ObGlobalColumnStat stat;
        const OptTableMeta *table_meta = table_metas.get_table_meta_by_table_id(table_id);
        // base table column expr use statistic
        if (OB_NOT_NULL(table_meta) && table_meta->use_opt_stat() &&
            OB_FAIL(ctx.get_opt_stat_manager()->get_column_stat(ctx.get_session_info()->get_effective_tenant_id(),
                                                                table_meta->get_ref_table_id(),
                                                                table_meta->get_all_used_parts(),
                                                                column_expr->get_column_id(),
                                                                table_meta->get_all_used_global_parts(),
                                                                table_meta->get_rows(),
                                                                table_meta->get_scale_ratio(),
                                                                stat))) {
          LOG_WARN("failed to get column stat", K(ret));
        } else if (stat.avglen_val_ != 0) {
          width += stat.avglen_val_;
        } else {
          // non base table column expr use estimation
          width += get_estimate_width_from_type(column_expr->get_result_type());
        }
      } else {
        // common expr, e.g, compositive expr or aggr expr
        width += get_estimate_width_from_type(expr->get_result_type());
      }
    }
  }
  // set minimal width as size of integer
  width = std::max(width, 4.0);
  return ret;
}

double ObOptEstCost::get_estimate_width_from_type(const ObExprResType &type)
{
  double width = ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH;
  if (type.is_integer_type()) {
    // int
    width += 4;
  } else if (type.get_accuracy().get_length() > 0) {
    // ObStringTC
    // 我们使用字符串定义的最大长度的一半估算字符串的字节，这样估计是不太准确的
    // 实际场景下字符串通常比实际定义的最大长度要小的多，这里我们调整其大小使它
    // 不超过MAX_STRING_WIDTH
    int64_t string_width = type.get_accuracy().get_length() / 2;
    width += static_cast<double>(std::min(string_width, ObOptEstCostModel::DEFAULT_MAX_STRING_WIDTH));
  } else if (type.get_accuracy().get_precision() > 0) {
    // number, time
    width += type.get_accuracy().get_precision() / 2;
  } else if (type.is_oracle_integer()) {
    width += OB_MAX_NUMBER_PRECISION / 2;
  } else {
    // default for DEFAULT PK
    width += sizeof(uint64_t);
  }
  return width;
}

int ObOptEstCost::construct_scan_range_batch(const ObIArray<ObNewRange> &scan_ranges,
                                             ObSimpleBatch &batch,
                                             SQLScanRange &range,
                                             SQLScanRangeArray &range_array)
{
  int ret = OB_SUCCESS;
  if (scan_ranges.count() < 1) {
    batch.type_ = ObSimpleBatch::T_NONE;
  } else if (scan_ranges.count() == 1) {
    range = scan_ranges.at(0);
    batch.type_ = ObSimpleBatch::T_SCAN;
    batch.range_ = &range;
  } else {
    const int64_t max_range_count = ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM;
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_ranges.count() && i < max_range_count; i++) {
      if (OB_FAIL(range_array.push_back(scan_ranges.at(i)))) {
        LOG_WARN("failed to push back array", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      batch.type_ = ObSimpleBatch::T_MULTI_SCAN;
      batch.ranges_ = &range_array;
    }
  }
  return ret;
}

int ObOptEstCost::stat_estimate_partition_batch_rowcount(const ObCostTableScanInfo &est_cost_info,
                                                         const ObIArray<ObNewRange> &scan_ranges,
                                                         double &row_count)
{
  int ret = OB_SUCCESS;
  double temp_row_count = 0;
  row_count = 0;
  if (scan_ranges.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Batch type error", K(ret), K(scan_ranges));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_ranges.count(); i++) {
    if (OB_FAIL(stat_estimate_single_range_rc(est_cost_info,
                                              scan_ranges.at(i),
                                              temp_row_count))) {
      LOG_WARN("failed to estimate row count", K(est_cost_info),
               K(scan_ranges.at(i)), K(ret));
    } else {
      row_count += temp_row_count;
      LOG_TRACE("OPT:[STAT EST MULTI SCAN]", K(temp_row_count),
                K(row_count));
    }
  }
  return ret;
}

int ObOptEstCost::calculate_filter_selectivity(ObCostTableScanInfo &est_cost_info,
                                               ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(est_cost_info.table_metas_) || OB_ISNULL(est_cost_info.sel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(est_cost_info.table_metas_), K(est_cost_info.sel_ctx_), K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(*est_cost_info.table_metas_,
                                                             *est_cost_info.sel_ctx_,
                                                             est_cost_info.prefix_filters_,
                                                             est_cost_info.prefix_filter_sel_,
                                                             all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(est_cost_info.postfix_filters_), K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(*est_cost_info.table_metas_,
                                                             *est_cost_info.sel_ctx_,
                                                             est_cost_info.pushdown_prefix_filters_,
                                                             est_cost_info.pushdown_prefix_filter_sel_,
                                                             all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(est_cost_info.pushdown_prefix_filters_), K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(*est_cost_info.table_metas_,
                                                             *est_cost_info.sel_ctx_,
                                                             est_cost_info.ss_postfix_range_filters_,
                                                             est_cost_info.ss_postfix_range_filters_sel_,
                                                             all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(est_cost_info.ss_postfix_range_filters_), K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(*est_cost_info.table_metas_,
                                                             *est_cost_info.sel_ctx_,
                                                             est_cost_info.postfix_filters_,
                                                             est_cost_info.postfix_filter_sel_,
                                                             all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(est_cost_info.postfix_filters_), K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(*est_cost_info.table_metas_,
                                                             *est_cost_info.sel_ctx_,
                                                             est_cost_info.table_filters_,
                                                             est_cost_info.table_filter_sel_,
                                                             all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(est_cost_info.table_filters_), K(ret));
  } else {
    LOG_TRACE("table filter info", K(est_cost_info.ref_table_id_), K(est_cost_info.index_id_),
        K(est_cost_info.prefix_filters_), K(est_cost_info.pushdown_prefix_filters_),
        K(est_cost_info.postfix_filters_), K(est_cost_info.table_filters_),
        K(est_cost_info.prefix_filter_sel_), K(est_cost_info.pushdown_prefix_filter_sel_),
        K(est_cost_info.ss_postfix_range_filters_), K(est_cost_info.ss_postfix_range_filters_sel_),
        K(est_cost_info.postfix_filter_sel_), K(est_cost_info.table_filter_sel_));
  }
  return ret;
}

int ObOptEstCost::stat_estimate_single_range_rc(const ObCostTableScanInfo &est_cost_info,
                                                const ObNewRange &range,
                                                double &count)
{
  int ret = OB_SUCCESS;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  const ObIndexMetaInfo &index_meta_info = est_cost_info.index_meta_info_;
  double range_selectivity = 1.0;
  count = -1;
  if (OB_ISNULL(est_cost_info.table_metas_) || OB_ISNULL(est_cost_info.sel_ctx_) ||
      OB_ISNULL(table_meta_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(est_cost_info.table_metas_), K(est_cost_info.sel_ctx_), K(ret));
  } else if (0 == index_meta_info.index_part_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition count is 0", K(index_meta_info.index_part_count_), K(ret));
  } else if (OB_FAIL(ObOptSelectivity::get_single_newrange_selectivity(*est_cost_info.table_metas_,
                                                                       *est_cost_info.sel_ctx_,
                                                                       est_cost_info.range_columns_,
                                                                       range,
                                                                       range_selectivity))) {
    LOG_WARN("failed to calculate single newrange selectivity", K(est_cost_info), K(ret));
  } else {
    if (range.empty() && fabs(1.0 - est_cost_info.prefix_filter_sel_) < OB_DOUBLE_EPSINON) {
      //defend code: user may enter a query without predicates on index prefix, but with
      //empty range on index postfix(such as when they are experimenting), leading optimizer to
      //think that this index has a very small scan cost. such plan will cause following
      //query with correct ranges to timeout.
      range_selectivity = 1.0;
      LOG_TRACE("OPT:[STAT EST RANGE] range is empty and prefix_filter_sel is 1");
    }
    count = static_cast<double>(table_meta_info->table_row_count_) * range_selectivity;
    LOG_TRACE("OPT:[STAT EST RANGE]", K(range), K(range_selectivity), K(count));
  }
  return ret;
}

double ObOptEstCost::calc_pred_cost_per_row(const ObRawExpr *expr,
                                            double card,
                                            double &cost,
                                            const ObOptimizerContext &opt_ctx)
{
  GET_COST_MODEL();
  return model->calc_pred_cost_per_row(expr, card, cost);
}

double ObOptEstCost::cost_values_table(double rows,
                                       ObIArray<ObRawExpr*> &filters,
                                       const ObOptimizerContext &opt_ctx)
{
  return cost_get_rows(rows, opt_ctx) + cost_quals(rows, filters, opt_ctx);
}