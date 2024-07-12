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

#include "sql/optimizer/ob_opt_est_cost_model.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "ob_opt_cost_model_parameter.h"
#include <math.h>
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::jit::expr;

const int64_t ObOptEstCostModel::DEFAULT_LOCAL_ORDER_DEGREE = 32;
const int64_t ObOptEstCostModel::DEFAULT_MAX_STRING_WIDTH = 64;
const int64_t ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH = 12;

int ObCostColumnGroupInfo::assign(const ObCostColumnGroupInfo& info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(filters_.assign(info.filters_))) {
    LOG_WARN("failed to assign filters", K(ret));
  } else if (OB_FAIL(access_column_items_.assign(info.access_column_items_))) {
    LOG_WARN("failed to assign column", K(ret));
  } else {
    column_id_ = info.column_id_;
    micro_block_count_ = info.micro_block_count_;
    filter_sel_ = info.filter_sel_;
    skip_rate_ = info.skip_rate_;
    skip_filter_sel_ = info.skip_filter_sel_;
  }
  return ret;
}

int ObCostTableScanInfo::assign(const ObCostTableScanInfo &est_cost_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ranges_.assign(est_cost_info.ranges_))) {
    LOG_WARN("failed to assign range", K(ret));
  } else if (OB_FAIL(ss_ranges_.assign(est_cost_info.ss_ranges_))) {
    LOG_WARN("failed to assign range", K(ret));
  } else if (OB_FAIL(range_columns_.assign(est_cost_info.range_columns_))) {
    LOG_WARN("failed to assign range columns", K(ret));
  } else if (OB_FAIL(access_column_items_.assign(est_cost_info.access_column_items_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else if (OB_FAIL(index_access_column_items_.assign(est_cost_info.index_access_column_items_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else if (OB_FAIL(prefix_filters_.assign(est_cost_info.prefix_filters_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else if (OB_FAIL(pushdown_prefix_filters_.assign(est_cost_info.pushdown_prefix_filters_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else if (OB_FAIL(ss_postfix_range_filters_.assign(est_cost_info.ss_postfix_range_filters_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else if (OB_FAIL(postfix_filters_.assign(est_cost_info.postfix_filters_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else if (OB_FAIL(table_filters_.assign(est_cost_info.table_filters_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else if (OB_FAIL(access_columns_.assign(est_cost_info.access_columns_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else if (OB_FAIL(index_scan_column_group_infos_.assign(est_cost_info.index_scan_column_group_infos_))) {
    LOG_WARN("failed to to assign column group infos", K(ret));
  } else if (OB_FAIL(index_back_column_group_infos_.assign(est_cost_info.index_back_column_group_infos_))) {
    LOG_WARN("failed to to assign column group infos", K(ret));
  } else {
    table_id_ = est_cost_info.table_id_;
    ref_table_id_ = est_cost_info.ref_table_id_;
    index_id_ = est_cost_info.index_id_;
    table_meta_info_ = est_cost_info.table_meta_info_;
    index_meta_info_.assign(est_cost_info.index_meta_info_);
    is_virtual_table_ = est_cost_info.is_virtual_table_;
    is_unique_ = est_cost_info.is_unique_;
    is_inner_path_ = est_cost_info.is_inner_path_;
    can_use_batch_nlj_ = est_cost_info.can_use_batch_nlj_;
    table_metas_ = est_cost_info.table_metas_;
    sel_ctx_ = est_cost_info.sel_ctx_;
    est_method_ = est_cost_info.est_method_;
    prefix_filter_sel_ = est_cost_info.prefix_filter_sel_;
    pushdown_prefix_filter_sel_ = est_cost_info.pushdown_prefix_filter_sel_;
    postfix_filter_sel_ = est_cost_info.postfix_filter_sel_;
    table_filter_sel_ = est_cost_info.table_filter_sel_;
    join_filter_sel_ = est_cost_info.join_filter_sel_;
    ss_prefix_ndv_ = est_cost_info.ss_prefix_ndv_;
    ss_postfix_range_filters_sel_ = est_cost_info.ss_postfix_range_filters_sel_;
    logical_query_range_row_count_ = est_cost_info.logical_query_range_row_count_;
    phy_query_range_row_count_ = est_cost_info.phy_query_range_row_count_;
    index_back_row_count_ = est_cost_info.index_back_row_count_;
    output_row_count_ = est_cost_info.output_row_count_;
    batch_type_ = est_cost_info.batch_type_;
    sample_info_ = est_cost_info.sample_info_;
    use_column_store_ = est_cost_info.use_column_store_;
    at_most_one_range_ = est_cost_info.at_most_one_range_;
    index_back_with_column_store_ = est_cost_info.index_back_with_column_store_;
    // no need to copy table scan param
  }
  return ret;
}

void ObTableMetaInfo::assign(const ObTableMetaInfo &table_meta_info)
{
  ref_table_id_ = table_meta_info.ref_table_id_;
  schema_version_ = table_meta_info.schema_version_;
  part_count_ = table_meta_info.part_count_;
  micro_block_size_ = table_meta_info.micro_block_size_;
  part_size_ = table_meta_info.part_size_;
  average_row_size_ = table_meta_info.average_row_size_;
  table_column_count_ = table_meta_info.table_column_count_;
  table_rowkey_count_ = table_meta_info.table_rowkey_count_;
  table_row_count_ = table_meta_info.table_row_count_;
  row_count_ = table_meta_info.row_count_;
  has_opt_stat_ = table_meta_info.has_opt_stat_;
  micro_block_count_ = table_meta_info.micro_block_count_;
  table_type_ = table_meta_info.table_type_;
}

int ObCostTableScanInfo::has_exec_param(const ObIArray<ObRawExpr *> &exprs, bool &bool_ret) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret) && !bool_ret; ++i) {
    if (OB_FAIL(exprs.at(i)->has_exec_param(bool_ret))) {
      LOG_WARN("failed to has_exec_param");
    }
  }
  return ret;
}

int ObCostTableScanInfo::has_exec_param(bool &bool_ret) const
{
  int ret = OB_SUCCESS;
  bool_ret = false;
  if (OB_FAIL(has_exec_param(prefix_filters_, bool_ret))) {
  } else if (OB_FAIL(has_exec_param(pushdown_prefix_filters_, bool_ret))) {
    LOG_WARN("failed to has_exec_param");
  } else if (OB_FAIL(has_exec_param(ss_postfix_range_filters_, bool_ret))) {
    LOG_WARN("failed to has_exec_param");
  } else if (OB_FAIL(has_exec_param(postfix_filters_, bool_ret))) {
    LOG_WARN("failed to has_exec_param");
  } else if (OB_FAIL(has_exec_param(table_filters_, bool_ret))) {
    LOG_WARN("failed to has_exec_param");
  }
  return ret;
}

double ObTableMetaInfo::get_micro_block_numbers() const
{
  double ret = 0.0;
  if (micro_block_count_ <= 0) {
    // calculate micro block count use storage statistics
    ret = 0;
  } else {
    // get micro block count from optimizer statistics
    ret = static_cast<double>(micro_block_count_);
  }
  return ret;
}

void ObIndexMetaInfo::assign(const ObIndexMetaInfo &index_meta_info)
{
  ref_table_id_ = index_meta_info.ref_table_id_;
  index_id_ = index_meta_info.index_id_;
  index_micro_block_size_ = index_meta_info.index_micro_block_size_;
  index_part_count_ = index_meta_info.index_part_count_;
  index_part_size_ = index_meta_info.index_part_size_;
  index_part_count_ = index_meta_info.index_part_count_;
  index_column_count_ = index_meta_info.index_column_count_;
  is_index_back_ = index_meta_info.is_index_back_;
  is_unique_index_ = index_meta_info.is_unique_index_;
  is_global_index_ = index_meta_info.is_global_index_;
  index_micro_block_count_ = index_meta_info.index_micro_block_count_;
}

double ObIndexMetaInfo::get_micro_block_numbers() const
{
  double ret = 0.0;
  if (index_micro_block_count_ <= 0) {
    // calculate micore block count use storage statistics
    ret = 0;
  } else {
    // get micro block count from optimizer statistics
    ret = static_cast<double>(index_micro_block_count_);
  }
  return ret;
}

/**
 * @brief    估算Nested Loop Join的代价
 * @formula  cost(总代价) = get_next_row_cost
 *                         + left_cost + right_cost
 *                         + left_rows * rescan_cost
 *                         + JOIN_PER_ROW_COST * output_rows
 *                         + qual_cost
 */
int ObOptEstCostModel::cost_nestloop(const ObCostNLJoinInfo &est_cost_info,
                                    double &cost,
                                    double &filter_selectivity,
                                    ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_ISNULL(est_cost_info.table_metas_) || OB_ISNULL(est_cost_info.sel_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point", K(est_cost_info.table_metas_), K(est_cost_info.sel_ctx_));
  } else {
    double left_rows = est_cost_info.left_rows_;
    double right_rows = est_cost_info.right_rows_;
    double cart_tuples = left_rows * right_rows; // tuples of Cartesian product
    double out_tuples = 0.0;
    filter_selectivity = 0.0;
    double material_cost = 0.0;
    //selectivity for equal conds
    if (OB_FAIL(ObOptSelectivity::calculate_selectivity(*est_cost_info.table_metas_,
                                                        *est_cost_info.sel_ctx_,
                                                        est_cost_info.other_join_conditions_,
                                                        filter_selectivity,
                                                        all_predicate_sel))) {
      LOG_WARN("Failed to calculate filter selectivity", K(ret));
    } else {
      out_tuples = cart_tuples * filter_selectivity;

      // 再次扫描右表全表的代价。如果不使用物化，就是读取一次右表和本层get_next_row的代价；
      // 如果物化，则为读取物化后的行的代价。
      double once_rescan_cost = 0.0;
      if (est_cost_info.need_mat_) {
        once_rescan_cost = cost_read_materialized(right_rows);
      } else {
        double rescan_cost = 0.0;
        if (est_cost_info.right_has_px_rescan_) {
          if (est_cost_info.parallel_ > 1) {
            rescan_cost = cost_params_.get_px_rescan_per_row_cost(sys_stat_);
          } else {
            rescan_cost = cost_params_.get_px_batch_rescan_per_row_cost(sys_stat_);
          }
        } else {
          rescan_cost = cost_params_.get_rescan_cost(sys_stat_);
        }
        once_rescan_cost = est_cost_info.right_cost_ + rescan_cost
                           + right_rows * cost_params_.get_cpu_tuple_cost(sys_stat_);
      }
      // total rescan cost
      if (LEFT_SEMI_JOIN == est_cost_info.join_type_
          || LEFT_ANTI_JOIN == est_cost_info.join_type_) {
        double match_sel = (est_cost_info.anti_or_semi_match_sel_ < OB_DOUBLE_EPSINON) ?
                            OB_DOUBLE_EPSINON : est_cost_info.anti_or_semi_match_sel_;
        out_tuples = left_rows * match_sel;
      }
      cost += left_rows * once_rescan_cost;
      //qual cost
      double qual_cost = cost_quals(left_rows * right_rows, est_cost_info.equal_join_conditions_) +
                         cost_quals(left_rows * right_rows, est_cost_info.other_join_conditions_);

      cost += qual_cost;

      double join_cost = cost_params_.get_join_per_row_cost(sys_stat_) * out_tuples;
      cost += join_cost;

      LOG_TRACE("OPT: [COST NESTLOOP JOIN]",
                K(cost), K(qual_cost), K(join_cost),K(once_rescan_cost),
                K(est_cost_info.left_cost_), K(est_cost_info.right_cost_),
                K(left_rows), K(right_rows), K(est_cost_info.right_width_),
                K(filter_selectivity), K(cart_tuples), K(material_cost));
    }
  }
  return ret;
}

/**
 * @brief    估算Merge Join的代价
 * @formula  cost(总代价) = left_cost + right_cost
 *                         + get_next_row_cost
 *                         + qual_cost
 *                         + COST_JOIN_PER_ROW * output_rows
 *
 * @param[in]  est_cost_info       用于计算merge join代价的一些参数
 * @param[out] merge_cost          merge join算子的总代价
 */
int ObOptEstCostModel::cost_mergejoin(const ObCostMergeJoinInfo &est_cost_info,
                                 			double &cost)
{
  int ret = OB_SUCCESS;
  double left_selectivity = 0.0;
  double right_selectivity = 0.0;
  cost = 0.0;
  double left_rows = est_cost_info.left_rows_;
  double right_rows = est_cost_info.right_rows_;
  double left_width = est_cost_info.left_width_;
  double cond_tuples = 0.0;
  double out_tuples = 0.0;
  double cond_sel = est_cost_info.equal_cond_sel_;
  double filter_sel = est_cost_info.other_cond_sel_;
  if (IS_SEMI_ANTI_JOIN(est_cost_info.join_type_)) {
    if (LEFT_SEMI_JOIN == est_cost_info.join_type_) {
      cond_tuples = left_rows * cond_sel;
    } else if (LEFT_ANTI_JOIN == est_cost_info.join_type_) {
      cond_tuples = left_rows * (1 - cond_sel);
    } else if (RIGHT_SEMI_JOIN == est_cost_info.join_type_) {
      cond_tuples = right_rows * cond_sel;
    } else if (RIGHT_ANTI_JOIN == est_cost_info.join_type_) {
      cond_tuples = right_rows * (1 - cond_sel);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected join type", K(est_cost_info.join_type_), K(ret));
    }
  } else {
    cond_tuples = left_rows * right_rows * cond_sel;
  }
  out_tuples = cond_tuples * filter_sel;
  // get_next_row()获取左表和右表所有行的代价
  cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * (left_rows + right_rows);
  // 谓词代价
  cost += cost_quals(cond_tuples, est_cost_info.equal_join_conditions_) +
          cost_quals(cond_tuples, est_cost_info.other_join_conditions_);
  // JOIN连接的代价
  cost += cost_params_.get_join_per_row_cost(sys_stat_)  * out_tuples;
  cost += cost_material(left_rows, left_width);
  cost += cost_read_materialized(left_rows);
  LOG_TRACE("OPT: [COST MERGE JOIN]",
                  K(left_rows), K(right_rows),
                  K(cond_sel), K(filter_sel),
                  K(cond_tuples), K(out_tuples),
                  K(cost));


  return ret;
}

/**
 * @brief    估算Hash Join的代价
 * @formula  cost(总代价) = left_cost + right_cost
 *                         + left_rows * BUILD_HASH_PER_ROW_COST
 *                         + material_cost
 *                         + right_rows * PROBE_HASH_PER_ROW_COST
 *                         + (left_rows + right_rows) * HASH_COST
 *                         + qual_cost
 *                         + JOIN_PER_ROW_COST * output_rows
 * @param[in]  est_cost_info       用于计算hash join代价的一些参数
 * @param[out] hash_cost           hash join算子的总代价
 * @param[in]  all_predicate_sel   各个谓词的选择率
 */
int ObOptEstCostModel::cost_hashjoin(const ObCostHashJoinInfo &est_cost_info,
                                		 double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double build_hash_cost = 0.0;
  double left_rows = est_cost_info.left_rows_;
  double right_rows = est_cost_info.right_rows_;
  double cond_sel = est_cost_info.equal_cond_sel_;
  double filter_sel = est_cost_info.other_cond_sel_;
  // number of tuples satisfying join-condition
  double cond_tuples = 0.0;
  // number of tuples satisfying filters, which is also the number of output tuples
  double out_tuples = 0.0;

  if (IS_SEMI_ANTI_JOIN(est_cost_info.join_type_)) {
    if (LEFT_SEMI_JOIN == est_cost_info.join_type_) {
      cond_tuples = left_rows * cond_sel;
    } else if (LEFT_ANTI_JOIN == est_cost_info.join_type_) {
      cond_tuples = left_rows * (1 - cond_sel);
    } else if (RIGHT_SEMI_JOIN == est_cost_info.join_type_) {
      cond_tuples = right_rows * cond_sel;
    } else if (RIGHT_ANTI_JOIN == est_cost_info.join_type_) {
      cond_tuples = right_rows * (1 - cond_sel);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected join type", K(est_cost_info.join_type_), K(ret));
    }
  } else {
    cond_tuples = left_rows * right_rows * cond_sel;
  }
  out_tuples = cond_tuples * filter_sel;
  double join_filter_cost = 0.0;
  for (int i = 0; i < est_cost_info.join_filter_infos_.count(); ++i) {
    const JoinFilterInfo& info = est_cost_info.join_filter_infos_.at(i);
    //bloom filter构建、使用代价
    join_filter_cost += cost_hash(left_rows, info.lexprs_) + cost_hash(right_rows, info.rexprs_);
    if (info.need_partition_join_filter_) {
      //partition join filter代价
      join_filter_cost += cost_hash(left_rows, info.lexprs_);
    }
    right_rows *= info.join_filter_selectivity_;
  }
  cost += join_filter_cost;
  // build hash cost for left table
  build_hash_cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * left_rows;
  build_hash_cost += cost_material(left_rows, est_cost_info.left_width_);
  build_hash_cost += cost_hash(left_rows, est_cost_info.equal_join_conditions_);
  build_hash_cost += cost_params_.get_build_hash_per_row_cost(sys_stat_) * left_rows;
  // probe cost for right table
  cost += build_hash_cost;
  cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * right_rows;
  cost += cost_hash(right_rows, est_cost_info.equal_join_conditions_);
  cost += cost_params_.get_probe_hash_per_row_cost(sys_stat_) * right_rows;
  cost += cost_quals(cond_tuples, est_cost_info.equal_join_conditions_)
                     + cost_quals(cond_tuples, est_cost_info.other_join_conditions_);
  cost += cost_params_.get_join_per_row_cost(sys_stat_)  * out_tuples;
  LOG_TRACE("OPT: [COST HASH JOIN]",
            K(left_rows), K(right_rows),
            K(cond_sel), K(filter_sel),
            K(cond_tuples), K(out_tuples),
            K(join_filter_cost),
            K(cost), K(build_hash_cost));

  return ret;
}

int ObOptEstCostModel::cost_sort_and_exchange(OptTableMetas *table_metas,
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
																							double &cost)
{
  int ret = OB_SUCCESS;
  double exch_cost = 0.0;
  double sort_cost = 0.0;
  bool need_exchange = (dist_method != ObPQDistributeMethod::NONE);
  bool exchange_need_merge_sort = need_exchange && (is_distributed || is_local_order) &&
                                  (!need_sort || ObPQDistributeMethod::LOCAL == dist_method);
  bool exchange_sort_local_order = need_exchange && !need_sort && is_local_order;
  bool need_exchange_down_sort = (ObPQDistributeMethod::LOCAL == dist_method ||
                                  ObPQDistributeMethod::NONE == dist_method) &&
                                  (need_sort || is_local_order);
  bool need_exchange_up_sort = need_sort && need_exchange && ObPQDistributeMethod::LOCAL != dist_method;

  cost = 0.0;
  if (need_exchange) {
    ObSEArray<OrderItem, 8> exchange_sort_keys;
    if (exchange_need_merge_sort && OB_FAIL(exchange_sort_keys.assign(expected_ordering))) {
      LOG_WARN("failed to assign sort keys", K(ret));
    } else {
      ObExchCostInfo exch_info(input_card,
                               input_width,
                               dist_method,
                               out_parallel,
                               in_parallel,
                               exchange_sort_local_order,
                               exchange_sort_keys,
                               in_server_cnt);
      if (OB_FAIL(ObOptEstCostModel::cost_exchange(exch_info, exch_cost))) {
        LOG_WARN("failed to cost exchange", K(ret));
      } else { /*do nothing*/ }
    }
  }

  if (OB_SUCC(ret) && (need_exchange_down_sort || need_exchange_up_sort)) {
    double card = input_card;
    double width = input_width;
    bool real_local_order = false;
    int64_t real_prefix_pos = 0;
    if (need_exchange_down_sort) {
      card /= out_parallel;
      real_prefix_pos = need_sort && !is_local_order ? prefix_pos : 0;
      real_local_order = need_sort ? false : is_local_order;
    } else {
      real_prefix_pos = need_exchange ? 0 : prefix_pos;
      if (ObPQDistributeMethod::BROADCAST != dist_method) {
        card /= in_parallel;
      }
    }
    ObSortCostInfo cost_info(card,
                             width,
                             real_prefix_pos,
                             expected_ordering,
                             real_local_order,
                             table_metas,
                             sel_ctx);
    if (OB_FAIL(ObOptEstCostModel::cost_sort(cost_info, sort_cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    cost = input_cost + exch_cost + sort_cost;
    LOG_TRACE("succeed to compute distributed sort cost", K(input_cost), K(exch_cost), K(sort_cost),
        K(need_sort), K(prefix_pos), K(is_local_order));
  }
  return ret;
}

int ObOptEstCostModel::cost_sort(const ObSortCostInfo &cost_info,
                            		 double &cost)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> order_exprs;
  ObSEArray<ObExprResType, 4> order_types;
  // top-n排序不会进行前缀排序
  // 如果获取不到est_sel_info，也回退到普通的排序代价估算
  cost = 0.0;
  if (OB_FAIL(ObOptimizerUtil::get_expr_and_types(cost_info.order_items_,
                                                         order_exprs,
                                                         order_types))) {
    LOG_WARN("failed to get expr types", K(ret));
  } else if (order_exprs.empty()) {
    /*do nothing*/
  } else if (cost_info.is_local_merge_sort_) {
    if (OB_FAIL(cost_local_order_sort(cost_info, order_types, cost))) {
      LOG_WARN("failed to cost local order sort", K(ret));
    } else {
      // get_next_row获取下层算子行的代价
      cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * cost_info.rows_;
    }
  } else if (cost_info.prefix_pos_ > 0) {
    // prefix sort
    if (OB_FAIL(cost_prefix_sort(cost_info, order_exprs, cost_info.topn_, cost))) {
      LOG_WARN("failed to calc prefix cost", K(ret));
    } else {
      // get_next_row获取下层算子行的代价
      cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * cost_info.rows_;
    }
  } else if (cost_info.part_cnt_ > 0 && cost_info.topn_ >= 0) {
    //part topn sort/part topn limit
    if (OB_FAIL(cost_part_topn_sort(cost_info, order_exprs, order_types, cost))) {
      LOG_WARN("failed to calc part cost", K(ret));
    } else {
      // get_next_row获取下层算子行的代价
      cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * cost_info.rows_;
    }
  } else if (cost_info.topn_ >= 0) {
    //top-n sort
    if (OB_FAIL(cost_topn_sort(cost_info, order_types, cost))) {
      LOG_WARN("failed to calc topn sort cost", K(ret));
    } else {
      // get_next_row获取下层算子行的代价
      cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * cost_info.rows_;
    }
  } else if (cost_info.part_cnt_ > 0) {
    // part sort
    if (OB_FAIL(cost_part_sort(cost_info, order_exprs, order_types, cost))) {
      LOG_WARN("failed to calc part cost", K(ret));
    } else {
      // get_next_row获取下层算子行的代价
      cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * cost_info.rows_;
    }
  } else {
    // normal sort
    if (OB_FAIL(cost_sort(cost_info, order_types, cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else {
      // get_next_row获取下层算子行的代价
      cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * cost_info.rows_;
    }
  }
  LOG_TRACE("succeed to compute sort cost", K(cost_info), K(cost));
  return ret;
}

/**
 * @brief      估算Sort算子代价的函数。
 * @formula    cost = material_cost + sort_cost
 *             material_cost = cost_material(...) + cost_read_materialized(...)
 *             sort_cost = cost_cmp_per_row * N * logN
 * @param[in]  cost_info   估算排序代价的一些参数
 *                         row         待排序的行数
 *                         width       平均行长
 * @param[in]  order_cols  排序列
 * @param[out] cost        排序算子自身的代价
 */
int ObOptEstCostModel::cost_sort(const ObSortCostInfo &cost_info,
																const ObIArray<ObExprResType> &order_col_types,
																double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double real_sort_cost = 0.0;
  double material_cost = 0.0;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  if (rows < 1.0) {
    material_cost = 0;
  } else {
    material_cost = cost_material(rows, width) + cost_read_materialized(rows * LOG2(rows));
  }
  if (OB_FAIL(cost_sort_inner(order_col_types, rows, real_sort_cost))) {
    LOG_WARN("failed to calc cost", K(ret));
  } else {
    cost = material_cost + real_sort_cost;
    LOG_TRACE("OPT: [COST SORT]", K(cost), K(material_cost), K(real_sort_cost),
              K(rows), K(width), K(order_col_types), "is_prefix_sort", cost_info.prefix_pos_ > 0);
  }
  return ret;
}

/**
 * 理想假设：行数为2的指数倍（保证桶数量和行数量相等），输入数据具有任意性，桶内所有数据哈希值不同。
 * @brief      估算 PART_SORT Sort算子代价的函数，与窗口函数连用。
 * @formula    cost = material_cost + hash_cost + sort_cost
 *             material_cost = cost_material(...) + cost_read_materialized(...)
 *             hash_cost = calc_hash * part_expr * rows + build_hash * rows
 *             sort_cost = cost_cmp_per_row * rows * theoretical_cmp_times
 * @param[in]  cost_info          估算排序代价的一些参数
 *                                rows        待排序的行数
 *                                width       平均行长
 * @param[in]  order_exprs        需要排序的表达式，前半部分是 part by，后半部分是 order by
 * @param[in]  order_col_types    需要排序的列的类型，order by 部分用作桶内排序
 * @param[in]  part_cnt          表达式组中 part by 部分所占个数，用作桶间排序
 * @param[out] cost               排序算子自身的代价
 */
int ObOptEstCostModel::cost_part_sort(const ObSortCostInfo &cost_info,
                                      const ObIArray<ObRawExpr *> &order_exprs,
                                      const ObIArray<ObExprResType> &order_col_types,
                                      double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double real_sort_cost = 0.0;
  double material_cost = 0.0;
  double calc_hash_cost = 0.0;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  double distinct_parts = rows;

  ObSEArray<ObRawExpr*, 4> part_exprs;
  ObSEArray<ObExprResType, 4> sort_types;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_exprs.count(); ++i) {
    if (i < cost_info.part_cnt_) {
      if (OB_FAIL(part_exprs.push_back(order_exprs.at(i)))) {
        LOG_WARN("fail to push back expr", K(ret));
      }
    } else {
      if (OB_FAIL(sort_types.push_back(order_col_types.at(i)))) {
        LOG_WARN("fail to push back type", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptSelectivity::calculate_distinct(*cost_info.table_metas_,
                                                      *cost_info.sel_ctx_,
                                                      part_exprs,
                                                      rows,
                                                      distinct_parts))) {
      LOG_WARN("failed to calculate distinct", K(ret));
    } else if (OB_UNLIKELY(distinct_parts < 1.0 || distinct_parts > rows)) {
      distinct_parts = rows;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(rows < 0.0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid row count", K(rows), K(ret));
    } else if (rows < 1.0) {
      // do nothing
    } else {
      double comp_cost = 0.0;
      if (sort_types.count() > 0 && OB_FAIL(get_sort_cmp_cost(sort_types, comp_cost))) {
        LOG_WARN("failed to get cmp cost", K(ret));
      } else if (OB_UNLIKELY(comp_cost < 0.0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("negative cost", K(comp_cost), K(ret));
      } else {
        real_sort_cost = rows * LOG2(rows / distinct_parts) * comp_cost;
        material_cost = cost_material(rows, width) + cost_read_materialized(rows);
        calc_hash_cost = cost_hash(rows, part_exprs) + rows * cost_params_.get_build_hash_per_row_cost(sys_stat_) / 2.0;
        cost = real_sort_cost + material_cost + calc_hash_cost;
        LOG_TRACE("OPT: [COST HASH SORT]", K(cost), K(real_sort_cost), K(calc_hash_cost),
                  K(material_cost), K(rows), K(width), K(cost_info.part_cnt_));
      }
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_part_topn_sort(const ObSortCostInfo &cost_info,
                                          const ObIArray<ObRawExpr *> &order_exprs,
                                          const ObIArray<ObExprResType> &order_col_types,
                                          double &cost)
{
    int ret = OB_SUCCESS;
  cost = 0.0;
  double real_sort_cost = 0.0;
  double material_cost = 0.0;
  double calc_hash_cost = 0.0;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  double distinct_parts = rows;

  ObSEArray<ObRawExpr*, 4> part_exprs;
  ObSEArray<ObExprResType, 4> sort_types;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_exprs.count(); ++i) {
    if (i < cost_info.part_cnt_) {
      if (OB_FAIL(part_exprs.push_back(order_exprs.at(i)))) {
        LOG_WARN("fail to push back expr", K(ret));
      }
    } else {
      if (OB_FAIL(sort_types.push_back(order_col_types.at(i)))) {
        LOG_WARN("fail to push back type", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptSelectivity::calculate_distinct(*cost_info.table_metas_,
                                                      *cost_info.sel_ctx_,
                                                      part_exprs,
                                                      rows,
                                                      distinct_parts))) {
      LOG_WARN("failed to calculate distinct", K(ret));
    } else if (OB_UNLIKELY(distinct_parts < 1.0 || distinct_parts > rows)) {
      distinct_parts = rows;
    }
  }

  if (OB_SUCC(ret)) {
    //partition topn sort
    double topn = cost_info.topn_;
    double one_part_rows = rows;
    if (distinct_parts != 0) {
      one_part_rows = rows / distinct_parts;
    }
    if (topn > one_part_rows) {
      topn = one_part_rows;
    }
    material_cost = cost_material(topn, width) * distinct_parts;
    if (sort_types.count() > 0 && OB_FAIL(cost_topn_sort_inner(sort_types, one_part_rows, topn, real_sort_cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else {
      real_sort_cost = real_sort_cost * distinct_parts;
      calc_hash_cost = cost_hash(rows, part_exprs) + rows * cost_params_.get_build_hash_per_row_cost(sys_stat_) / 2.0;
      cost = material_cost + real_sort_cost + calc_hash_cost;
      LOG_TRACE("OPT: [COST PARTITION TOPN SORT]", K(cost), K(calc_hash_cost), K(material_cost),
                K(real_sort_cost), K(rows), K(width), K(topn), K(cost_info.part_cnt_));
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_prefix_sort(const ObSortCostInfo &cost_info,
																				const ObIArray<ObRawExpr *> &order_exprs,
																				const int64_t topn_count,
																				double &cost)
{
  int ret = OB_SUCCESS;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  double cost_per_group = 0.0;
  if (OB_ISNULL(cost_info.table_metas_) || OB_ISNULL(cost_info.sel_ctx_) ||
      OB_UNLIKELY(cost_info.prefix_pos_ <= 0 || cost_info.prefix_pos_ >= order_exprs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(cost_info.table_metas_), K(cost_info.sel_ctx_),
        K(cost_info.prefix_pos_), K(order_exprs.count()), K(ret));
  } else {
    ObSEArray<ObRawExpr*, 4> prefix_ordering;
    ObSEArray<OrderItem, 4> ordering_per_group;
    for (int64_t i = 0; OB_SUCC(ret) && i < cost_info.prefix_pos_; i++) {
      if (OB_ISNULL(order_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(prefix_ordering.push_back(order_exprs.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
    for (int64_t i = cost_info.prefix_pos_; OB_SUCC(ret) && i < order_exprs.count(); ++i) {
      if (OB_ISNULL(order_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ordering_per_group.push_back(OrderItem(order_exprs.at(i))))) {
        LOG_WARN("failed to push array", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      // 前缀排序的每个部分不会进行前缀排序，也不会进行topn排序
      int64_t prefix_pos = 0;
      double num_rows_per_group = 0;
      double num_distinct_rows = rows;
      if (OB_FAIL(ObOptSelectivity::calculate_distinct(*cost_info.table_metas_,
                                                       *cost_info.sel_ctx_,
                                                       prefix_ordering,
                                                       rows,
                                                       num_distinct_rows))) {
        LOG_WARN("failed to calculate distinct", K(ret));
      } else if (OB_UNLIKELY(std::fabs(num_distinct_rows) < OB_DOUBLE_EPSINON)) {
        num_rows_per_group = rows;
      } else {
        num_rows_per_group = rows / num_distinct_rows;
      }
      if (OB_SUCC(ret)) {
        ObSortCostInfo cost_info_per_group(num_rows_per_group,
                                           width,
                                           prefix_pos,
                                           ordering_per_group,
                                           false,
                                           cost_info.table_metas_,
                                           cost_info.sel_ctx_);
        if (OB_FAIL(cost_sort(cost_info_per_group, cost_per_group))) {
          LOG_WARN("failed to calc cost", K(ret));
        } else if (topn_count >= 0 && num_rows_per_group > 0) {
          // topn prefix sort
          cost = cost_per_group * (topn_count / num_rows_per_group);
          LOG_TRACE("OPT: [COST PREFIX TOPN SORT]", K(cost), K(cost_per_group), K(topn_count), K(num_rows_per_group));
        } else {
          // normal prefix sort
          cost = cost_per_group * num_distinct_rows;
          LOG_TRACE("OPT: [COST PREFIX SORT]", K(cost), K(cost_per_group), K(num_distinct_rows));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief 计算排序算子实际排序部分的代价
 *
 *    cost = cost_cmp * rows * log(row_count)
 */
int ObOptEstCostModel::cost_sort_inner(const ObIArray<ObExprResType> &types,
																			double row_count,
																			double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_UNLIKELY(0.0 > row_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", K(row_count), K(ret));
  } else if (row_count < 1.0) {
    // LOG2(x) 在x小于1时为负数，这里需要特殊处理
    cost = 0.0;
  } else {
    double cost_cmp = 0.0;
    if (OB_FAIL(get_sort_cmp_cost(types, cost_cmp))) {
      LOG_WARN("failed to get cmp cost", K(ret));
    } else if (OB_UNLIKELY(0.0 > cost_cmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("negative cost", K(cost_cmp), K(ret));
    } else {
      cost = cost_cmp * row_count * LOG2(row_count);
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_local_order_sort_inner(const common::ObIArray<sql::ObExprResType> &types,
																									double row_count,
																									double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_UNLIKELY(0.0 > row_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", K(row_count), K(ret));
  } else if (row_count < 1.0) {
    // LOG2(x) 在x小于1时为负数，这里需要特殊处理
    cost = 0.0;
  } else {
    double cost_cmp = 0.0;
    if (OB_FAIL(get_sort_cmp_cost(types, cost_cmp))) {
      LOG_WARN("failed to get cmp cost", K(ret));
    } else if (OB_UNLIKELY(0.0 > cost_cmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("negative cost", K(cost_cmp), K(ret));
    } else {
      cost = cost_cmp * row_count * LOG2(ObOptEstCostModel::DEFAULT_LOCAL_ORDER_DEGREE);
    }
  }
  return ret;
}

/**
 * @brief      估算TOP-N Sort算子代价的函数。
 * @formula    cost = material_cost + sort_cost
 *             material_cost = cost_material(...)
 *             sort_cost = cost_cmp_per_row * rows * logN
 * @param[in]  cost_info   估算排序代价的一些参数
 *                         rows        待排序的行数
 *                         topn        TOP-N
 *                         width       平均行长
 * @param[in]  store_cols  需要进行物化的所有列
 * @param[in]  order_cols  排序列
 * @param[out] cost        排序算子自身的代价
 */
int ObOptEstCostModel::cost_topn_sort(const ObSortCostInfo &cost_info,
																			const ObIArray<ObExprResType> &types,
																			double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  double topn = cost_info.topn_;
  double real_sort_cost = 0.0;
  double material_cost = 0.0;
  if (0 == types.count() || topn < 0) {
    // do nothing
  } else {
    if (topn > rows) {
      topn = rows;
    }
    // top-n sort至少物化n行，至多物化rows行
    // 我们认为topn sort大约需要物化两者的平均数(n + rows) / 2
    material_cost = cost_material(topn, width);
    if (OB_FAIL(cost_topn_sort_inner(types, rows, topn, real_sort_cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else {
      cost = material_cost + real_sort_cost;
      LOG_TRACE("OPT: [COST TOPN SORT]", K(cost), K(material_cost),
                K(real_sort_cost), K(rows), K(width), K(topn));
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_local_order_sort(const ObSortCostInfo &cost_info,
																						const ObIArray<ObExprResType> &types,
																						double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double real_sort_cost = 0.0;
  double material_cost = 0.0;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  material_cost = cost_material(rows, width) + cost_read_materialized(rows);
  if (OB_FAIL(cost_local_order_sort_inner(types, rows, real_sort_cost))) {
    LOG_WARN("failed to calc cost", K(ret));
  } else {
    cost = material_cost + real_sort_cost;
    LOG_TRACE("OPT: [COST LOCAL ORDER SORT]", K(cost), K(material_cost), K(real_sort_cost),
              K(rows), K(width), K(types));
  }
  return ret;
}

/**
 * @brief        计算topn排序算子实际排序部分的代价
 *
 *    cost = cost_cmp * rows * log(n)
 */
int ObOptEstCostModel::cost_topn_sort_inner(const ObIArray<ObExprResType> &types,
																						double rows,
																						double n,
																						double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_UNLIKELY(0.0 > rows)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid number of rows", K(rows), K(ret));
  } else if (n < 1.0) {
    // LOG2(x) 在x小于1时为负数，这里需要特殊处理
    cost = 0.0;
  } else {
    double cost_cmp = 0.0;
    if (OB_FAIL(get_sort_cmp_cost(types, cost_cmp))) {
      LOG_WARN("failed to get cmp cost", K(ret));
    } else if (OB_UNLIKELY(0.0 > cost_cmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("negative cost", K(cost_cmp), K(ret));
    } else {
        cost = cost_cmp * rows * LOG2(n);
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_exchange(const ObExchCostInfo &cost_info,
                                		 double &ex_cost)
{
  int ret = OB_SUCCESS;
  double ex_out_cost = 0.0;
  double ex_in_cost = 0.0;
  ObExchOutCostInfo out_est_cost_info(cost_info.rows_,
                                      cost_info.width_,
                                      cost_info.dist_method_,
                                      cost_info.out_parallel_,
                                      cost_info.in_server_cnt_);
  ObExchInCostInfo in_est_cost_info(cost_info.rows_,
                                    cost_info.width_,
                                    cost_info.dist_method_,
                                    cost_info.in_parallel_,
                                    cost_info.in_server_cnt_,
                                    cost_info.is_local_order_,
                                    cost_info.sort_keys_);
  if (OB_FAIL(ObOptEstCostModel::cost_exchange_out(out_est_cost_info, ex_out_cost))) {
    LOG_WARN("failed to cost exchange in output", K(ret));
  } else if (OB_FAIL(ObOptEstCostModel::cost_exchange_in(in_est_cost_info, ex_in_cost))) {
    LOG_WARN("failed to cost exchange in", K(ret));
  } else {
    ex_cost = ex_out_cost + ex_in_cost;
  }
  return ret;
}

int ObOptEstCostModel::cost_exchange_in(const ObExchInCostInfo &cost_info,
                                   			double &cost)
{
  int ret = OB_SUCCESS;
  double per_dop_rows = 0.0;
  ObSEArray<ObRawExpr*, 4> order_exprs;
  ObSEArray<ObExprResType, 4> order_types;
  cost = 0;
  if (OB_UNLIKELY(cost_info.parallel_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_and_types(cost_info.sort_keys_,
                                                         order_exprs,
                                                         order_types))) {
    LOG_WARN("failed to get order expr and order types", K(ret));
  } else if (ObPQDistributeMethod::BC2HOST == cost_info.dist_method_) {
    per_dop_rows = cost_info.rows_ * cost_info.server_cnt_ / cost_info.parallel_;
  } else if (ObPQDistributeMethod::BROADCAST == cost_info.dist_method_) {
    per_dop_rows = cost_info.rows_;
  } else {
    per_dop_rows = cost_info.rows_ / cost_info.parallel_;
  }
  if (OB_SUCC(ret)) {
    cost = cost_params_.get_cpu_tuple_cost(sys_stat_) * per_dop_rows;
    cost += cost_params_.get_network_deser_per_byte_cost(sys_stat_) * per_dop_rows * cost_info.width_;
    LOG_TRACE("OPT: [COST EXCHANGE IN]", K(cost_info.rows_), K(cost_info.width_),
                              K(cost_info.dist_method_), K(cost_info.parallel_), K(cost));
    if (ObPQDistributeMethod::BROADCAST == cost_info.dist_method_) {
      //每个线程都需要拷贝一份当前机器收到的数据
      cost += ObOptEstCostModel::cost_material(per_dop_rows, cost_info.width_);
    }
    if (!cost_info.sort_keys_.empty() && per_dop_rows > 0) {
      double merge_degree = 0;
      double cmp_cost = 0.0;
      if (cost_info.is_local_order_) {
        cost += ObOptEstCostModel::cost_material(per_dop_rows, cost_info.width_);
        merge_degree = ObOptEstCostModel::DEFAULT_LOCAL_ORDER_DEGREE * cost_info.parallel_;
      } else {
        merge_degree = cost_info.parallel_;
      }
      if (merge_degree > per_dop_rows) {
        merge_degree = per_dop_rows;
      }
      if (OB_FAIL(get_sort_cmp_cost(order_types, cmp_cost))) {
        LOG_WARN("failed to get sort cmp cost", K(ret));
      } else {
        cost += per_dop_rows * LOG2(merge_degree) * cmp_cost;
      }
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_exchange_out(const ObExchOutCostInfo &cost_info,
                                    		 double &cost)
{
  int ret = OB_SUCCESS;
  double per_dop_ser_rows = 0.0;
  double per_dop_trans_rows = 0.0;
  cost = 0.0;
  if (OB_UNLIKELY(cost_info.parallel_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(cost_info.parallel_), K(ret));
  } else if (ObPQDistributeMethod::BC2HOST == cost_info.dist_method_ ||
             ObPQDistributeMethod::BROADCAST == cost_info.dist_method_) {
    per_dop_ser_rows = cost_info.rows_ / cost_info.parallel_;
    per_dop_trans_rows = cost_info.rows_ * cost_info.server_cnt_ / cost_info.parallel_;
  } else {
    per_dop_ser_rows = cost_info.rows_ / cost_info.parallel_;
    per_dop_trans_rows = per_dop_ser_rows;
  }
  if (OB_SUCC(ret)) {
    // add repartition cost, hash-hash cost ?
    cost = cost_params_.get_cpu_tuple_cost(sys_stat_) * per_dop_ser_rows;
    cost += cost_params_.get_network_ser_per_byte_cost(sys_stat_) * per_dop_ser_rows * cost_info.width_;
    cost += cost_params_.get_network_trans_per_byte_cost(sys_stat_) * per_dop_trans_rows * cost_info.width_;
    LOG_TRACE("OPT: [COST EXCHANGE OUT]", K(cost_info.rows_), K(cost_info.width_),
                              K(cost_info.dist_method_), K(cost_info.parallel_), K(cost));
  }
  return ret;
}
/**
 * @brief      估算Merge Group By算子代价的函数。
 * @note       我们假设group by比较时几乎都需要比较所有列
 *
 * @formula    cost = CPU_TUPLE_COST *rows
 *                    + qual_cost(CMP_DEFAULT * num_group_columns * rows)
 *                    + PER_AGGR_FUNC_COST * num_aggr_columns * rows
 *
 * @param[in]  rows            待排序的行数
 * @param[in]  group_columns   group by的列数
 * @param[in]  agg_col_count   聚合函数的个数
 * @return                     算子自身的代价
 */
double ObOptEstCostModel::cost_merge_group(double rows,
																					double res_rows,
																					double row_width,
																					const ObIArray<ObRawExpr *> &group_columns,
																					int64_t agg_col_count)
{
  double cost = 0.0;
  cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * rows;
  //material cost
  cost += cost_material(res_rows, row_width);
  cost += cost_quals(rows, group_columns);
  cost += cost_params_.get_per_aggr_func_cost(sys_stat_) * static_cast<double>(agg_col_count) * rows;
  LOG_TRACE("OPT: [COST MERGE GROUP BY]", K(cost), K(agg_col_count),
            K(rows), K(res_rows));
  return cost;
}

/**
 * @brief      估算Hash Group By算子代价的函数。
 * @formula    cost = CPU_TUPLE_COST * rows
 *                    + BUILD_HASH_COST * res_rows
 *                    + PROBE_HASH_COST * rows
 *                    + hash_calculation_cost
 *                    + PER_AGGR_FUNC_COST * num_aggr_columns * rows
 * @param[in]  rows            输入行数
 * @param[in]  group_columns   group by的列
 * @param[in]  res_rows        输出行数
 * @param[in]  agg_col_count   聚合函数的个数
 * @return                     算子自身的代价
 */
double ObOptEstCostModel::cost_hash_group(double rows,
																					double res_rows,
																					double row_width,
																					const ObIArray<ObRawExpr *> &group_columns,
																					int64_t agg_col_count)
{
  double cost = 0;
  cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * rows;
  cost += cost_material(res_rows, row_width);
  cost += cost_params_.get_build_hash_per_row_cost(sys_stat_) * res_rows;
  cost += cost_params_.get_probe_hash_per_row_cost(sys_stat_) * rows;
  cost += cost_hash(rows, group_columns);
  cost += cost_params_.get_per_aggr_func_cost(sys_stat_) * static_cast<double>(agg_col_count) * rows;
  LOG_TRACE("OPT: [HASH GROUP BY]", K(cost), K(agg_col_count), K(rows), K(res_rows));
  return cost;
}

/**
 * @brief      估算Scalar Group By算子代价的函数。
 * @formula    cost = PER_AGGR_FUNC_COST * num_aggr_columns * rows
 * @param[in]  rows            待排序的行数
 * @param[in]  agg_col_count   聚合函数的个数
 * @return                     算子自身的代价
 */
double ObOptEstCostModel::cost_scalar_group(double rows, int64_t agg_col_count)
{
  double cost = 0.0;
  cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * rows;
  cost += cost_params_.get_per_aggr_func_cost(sys_stat_) * static_cast<double>(agg_col_count) * rows;
  LOG_TRACE("OPT: [SCALAR GROUP BY]", K(cost), K(agg_col_count), K(rows));
  return cost;
}

/**
 * @brief      估算Merge Distinct 算子代价的函数。
 * @formula    cost = get_next_row_cost
 *                  + cost_quals
 * @param[in]  rows               输入行数
 * @param[in]  distinct_columns   distinct的列
 * @return                        算子自身的代价
 */
double ObOptEstCostModel::cost_merge_distinct(double rows,
																							double res_rows,
																							double width,
																							const ObIArray<ObRawExpr *> &distinct_columns)
{
  double cost = 0.0;
  cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * rows;
  cost += cost_quals(rows, distinct_columns);
  LOG_TRACE("OPT: [COST MERGE DISTINCT]", K(cost), K(rows), K(res_rows));
  return cost;
}

/**
 * @brief        估计Hash Distinct算子代价的函数。
 * @formula      cost = get_next_row_cost
 *                    + HASH_BUILD_COST * res_rows
 *                    + HASH_PROBE_COST * rows
 *                    + hash_calculation_cost
 * @param[in]    rows               输入行数
 * @param[in]    res_rows           输出行数，也即distinct数
 * @param[in]    distinct_columns   distinct列
 */
double ObOptEstCostModel::cost_hash_distinct(double rows,
																						double res_rows,
																						double width,
																						const ObIArray<ObRawExpr *> &distinct_columns)
{
  double cost = 0.0;
  // get_next_row()的代价
  cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * rows;
  //material cost
  cost += cost_material(res_rows, width);
  // 构建hash table的代价
  cost += cost_params_.get_build_hash_per_row_cost(sys_stat_) * res_rows;
  // probe的代价
  cost += cost_params_.get_probe_hash_per_row_cost(sys_stat_) * rows;
  // 计算hash值代价
  cost += cost_hash(rows, distinct_columns);

  LOG_TRACE("OPT: [COST HASH DISTINCT]", K(cost), K(rows), K(res_rows));
  return cost;
}

/**
 * @brief     估算 Select 下的 Sequence 算子的代价函数
 */
double ObOptEstCostModel::cost_sequence(double rows, double uniq_sequence_cnt)
{
  return cost_params_.get_cpu_tuple_cost(sys_stat_) * rows +
          cost_params_.get_cpu_operator_cost(sys_stat_) * uniq_sequence_cnt;
}
/**
 * @brief      估算Limit算子代价的函数。
 * @formula    cost = rows * CPU_TUPLE_COST
 * @return     算子自身的代价
 */
double ObOptEstCostModel::cost_get_rows(double rows)
{
  return rows * cost_params_.get_cpu_tuple_cost(sys_stat_);
}

/**
 * @brief      估算读取物化后的数据代价的函数。
 */
double ObOptEstCostModel::cost_read_materialized(double rows)
{
  return rows * cost_params_.get_read_materialized_per_row_cost(sys_stat_);
}

/**
 * @brief      估算Material算子代价的函数。
 * @formula    cost = MATERIALZE_PER_BYTE_COST * average_row_size * rows
 * @param[in]  rows             需要物化的行数
 * @param[in]  average_row_size 每行的平均长度（字节）
 * @return     算子自身的代价
 */
double ObOptEstCostModel::cost_material(const double rows, const double average_row_size)
{
  double cost = cost_params_.get_materialize_per_byte_write_cost(sys_stat_) * average_row_size * rows;
  LOG_TRACE("OPT: [COST MATERIAL]", K(cost), K(rows), K(average_row_size));
  return cost;
}


double ObOptEstCostModel::cost_late_materialization_table_get(int64_t column_cnt)
{
  double op_cost = 0.0;
  double io_cost = cost_params_.get_micro_block_seq_cost(sys_stat_);
  double cpu_cost = (cost_params_.get_cpu_tuple_cost(sys_stat_)
                         + cost_params_.get_project_column_cost(sys_stat_, PROJECT_INT, true, false) * column_cnt);
  op_cost = io_cost + cpu_cost;
  return op_cost;
}

void ObOptEstCostModel::cost_late_materialization_table_join(double left_card,
																														double left_cost,
																														double right_card,
																														double right_cost,
																														double &op_cost,
																														double &cost)
{
  op_cost = 0.0;
  cost = 0.0;
  // 再次扫描右表全表的代价。如果不使用物化，就是读取一次右表和本层get_next_row的代价；
  // 如果物化，则为读取物化后的行的代价。
  double once_rescan_cost = right_cost + right_card * cost_params_.get_cpu_tuple_cost(sys_stat_);
  op_cost += left_card * once_rescan_cost + left_card * cost_params_.get_join_per_row_cost(sys_stat_);
  // 读取左表和本层get_next_row的代价
  cost += left_cost + cost_params_.get_cpu_tuple_cost(sys_stat_) * left_card;
  cost += op_cost;
}

void ObOptEstCostModel::cost_late_materialization(double left_card,
																									double left_cost,
																									int64_t column_count,
																									double &cost)
{
  double op_cost = 0.0;
  double right_card = 1.0;
  double right_cost = cost_late_materialization_table_get(column_count);
  cost_late_materialization_table_join(left_card,
                                       left_cost,
                                       right_card,
                                       right_cost,
                                       op_cost,
                                       cost);
}

// entry point to estimate table cost
int ObOptEstCostModel::cost_table(const ObCostTableScanInfo &est_cost_info,
																	int64_t parallel,
																	double &cost)
{
  int ret = OB_SUCCESS;
  const double part_cnt = static_cast<double>(est_cost_info.index_meta_info_.index_part_count_);
  if (OB_UNLIKELY(parallel < 1 || part_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(parallel), K(part_cnt), K(ret));
  } else if (OB_NOT_NULL(est_cost_info.table_meta_info_)
             && EXTERNAL_TABLE == est_cost_info.table_meta_info_->table_type_) {
    //TODO [ExternalTable] need refine
    double phy_query_range_row_count = est_cost_info.phy_query_range_row_count_;
    cost = 4.0 * phy_query_range_row_count;
    OPT_TRACE_COST_MODEL(KV(cost),"=","4.0 * ", KV(phy_query_range_row_count));
  } else if (OB_FAIL(cost_basic_table(est_cost_info,
                                      part_cnt / parallel,
                                      cost))) {
    LOG_WARN("failed to estimate table cost", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptEstCostModel::cost_table_for_parallel(const ObCostTableScanInfo &est_cost_info,
                                               const int64_t parallel,
                                               const double part_cnt_per_dop,
                                               double &px_cost,
                                               double &cost)
{
  int ret = OB_SUCCESS;
  px_cost = 0.0;
  cost = 0.0;
  double table_cost = 0.0;
  if (OB_UNLIKELY(is_virtual_table(est_cost_info.ref_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected virtual table", K(ret), K(est_cost_info.ref_table_id_));
  } else if (OB_FAIL(cost_basic_table(est_cost_info,
                                      part_cnt_per_dop,
                                      table_cost))) {
    LOG_WARN("Failed to estimate cost", K(ret), K(est_cost_info));
  } else if (OB_FAIL(ObOptEstCostModel::cost_px(parallel, px_cost))) {
    LOG_WARN("Failed to estimate px cost", K(ret), K(parallel));
  } else {
    cost = table_cost + px_cost;
    LOG_TRACE("OPT:[ESTIMATE TABLE PARALLEL FINISH]", K(cost), K(table_cost), K(px_cost),
              K(parallel), K(part_cnt_per_dop),
              K(est_cost_info));
  }
  return ret;
}

int ObOptEstCostModel::cost_px(int64_t parallel, double &px_cost)
{
  int ret = OB_SUCCESS;
  px_cost = 0.0;
  if (parallel <= 1) {
    /* do nothing */
  } else {
    px_cost = 0.1 * parallel * parallel;
  }
  return ret;
}

// estimate cost for real table
// 1. 计算filter选择率
// 2. 判断使用哪种估行方式
// 3. 遍历key ranges, 循环获取ObBatch
// 4. 估算每一个ObBatch行数
// 5. 计算每一个ObBatch代价
// 6. 处理相关输出信息
int ObOptEstCostModel::cost_basic_table(const ObCostTableScanInfo &est_cost_info,
                                        const double part_cnt_per_dop,
																				double &cost)

{
  int ret = OB_SUCCESS;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  if (OB_ISNULL(table_meta_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret));
  } else {
    double row_count = est_cost_info.phy_query_range_row_count_;
    // revise number of output row if is row sample scan
    if (est_cost_info.sample_info_.is_row_sample()) {
      row_count *= 0.01 * est_cost_info.sample_info_.percent_;
    }
    // calc row count for one partition
    int64_t part_count = table_meta_info->part_count_;
    part_count = part_count > 0 ? part_count : 1;
    double row_count_per_part = row_count / part_count;
    double index_scan_cost = 0;
    double index_back_cost = 0;
    double prefix_filter_sel = 1.0;
    // calc scan one partition cost
    if (OB_FAIL(cost_index_scan(est_cost_info,
                                row_count_per_part,
                                prefix_filter_sel,
                                index_scan_cost))) {
      LOG_WARN("failed to calc index scan cost", K(ret));
    } else if (est_cost_info.index_meta_info_.is_index_back_ &&
               OB_FAIL(cost_index_back(est_cost_info,
                                      row_count_per_part,
                                      prefix_filter_sel,
                                      index_back_cost))) {
      LOG_WARN("failed to calc index back cost", K(ret));
    } else {
      cost += index_scan_cost;
      OPT_TRACE_COST_MODEL(KV(cost), "+=", KV(index_scan_cost));
      cost += index_back_cost;
      OPT_TRACE_COST_MODEL(KV(cost), "+=", KV(index_back_cost));
      // calc one parallel scan cost
      cost *= part_cnt_per_dop;
      OPT_TRACE_COST_MODEL(KV(cost), "*=", KV(part_cnt_per_dop));
      LOG_TRACE("OPT:[ESTIMATE FINISH]", K(cost), K(part_cnt_per_dop), K(est_cost_info));
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_index_scan(const ObCostTableScanInfo &est_cost_info,
                                      double row_count,
                                      double &prefix_filter_sel,
                                      double &index_scan_cost)
{
  int ret = OB_SUCCESS;
  if (est_cost_info.use_column_store_ &&
      OB_FAIL(cost_column_store_index_scan(est_cost_info,
                                          row_count,
                                          prefix_filter_sel,
                                          index_scan_cost))) {
    LOG_WARN("failed to calc column store index scan cost", K(ret));
  } else if (!est_cost_info.use_column_store_ &&
             OB_FAIL(cost_row_store_index_scan(est_cost_info,
                                              row_count,
                                              index_scan_cost))) {
    LOG_WARN("failed to calc row store index scan cost", K(ret));
  }
  return ret;
}

int ObOptEstCostModel::cost_index_back(const ObCostTableScanInfo &est_cost_info,
                                      double row_count,
                                      double &prefix_filter_sel,
                                      double &index_back_cost)
{
  int ret = OB_SUCCESS;
  if (est_cost_info.index_back_with_column_store_ &&
      OB_FAIL(cost_column_store_index_back(est_cost_info,
                                          row_count,
                                          prefix_filter_sel,
                                          index_back_cost))) {
    LOG_WARN("failed to calc column store index back cost", K(ret));
  } else if (!est_cost_info.index_back_with_column_store_ &&
             OB_FAIL(cost_row_store_index_back(est_cost_info,
                                              row_count,
                                              index_back_cost))) {
    LOG_WARN("failed to calc row store index back cost", K(ret));
  }
  return ret;
}

int ObOptEstCostModel::cost_column_store_index_scan(const ObCostTableScanInfo &est_cost_info,
                                                    double row_count,
                                                    double &prefix_filter_sel,
                                                    double &index_scan_cost)
{
  int ret = OB_SUCCESS;
  double runtime_filter_sel = est_cost_info.join_filter_sel_;
  SMART_VAR(ObCostTableScanInfo, column_group_est_cost_info, OB_INVALID_ID, OB_INVALID_ID, OB_INVALID_ID) {
    if (OB_FAIL(column_group_est_cost_info.assign(est_cost_info))) {
      LOG_WARN("failed to assign est cost info", K(ret));
    } else {
      column_group_est_cost_info.access_column_items_.reuse();
      column_group_est_cost_info.prefix_filters_.reuse();
      column_group_est_cost_info.postfix_filters_.reuse();
      column_group_est_cost_info.use_column_store_ = true;
      column_group_est_cost_info.join_filter_sel_ = 1.0;
    }
    // calc scan cost for each column group
    for (int64_t i = 0; OB_SUCC(ret) && i < est_cost_info.index_scan_column_group_infos_.count(); ++i) {
      // prepare est cost info for column group
      const ObCostColumnGroupInfo &cg_info = est_cost_info.index_scan_column_group_infos_.at(i);
      double cg_row_count = row_count * prefix_filter_sel * cg_info.skip_filter_sel_ * cg_info.skip_rate_;
      column_group_est_cost_info.index_meta_info_.index_micro_block_count_ = cg_info.micro_block_count_;
      column_group_est_cost_info.table_filters_.reuse();
      double column_group_cost = 0.0;
      if (OB_FAIL(column_group_est_cost_info.postfix_filters_.assign(cg_info.filters_))) {
        LOG_WARN("failed to assign filters", K(ret));
      } else if (!est_cost_info.index_meta_info_.is_index_back_ &&
                OB_FAIL(column_group_est_cost_info.access_column_items_.assign(cg_info.access_column_items_))) {
        LOG_WARN("failed to assign filters", K(ret));
      } else if (est_cost_info.index_meta_info_.is_index_back_ &&
                OB_FAIL(column_group_est_cost_info.index_access_column_items_.assign(cg_info.access_column_items_))) {
        LOG_WARN("failed to assign filters", K(ret));
      } else if (OB_FAIL(cost_row_store_index_scan(column_group_est_cost_info,
                                                  row_count * prefix_filter_sel,
                                                  column_group_cost))) {
        LOG_WARN("failed to calc index scan cost", K(ret), K(cg_row_count), K(column_group_est_cost_info));
      } else {
        index_scan_cost += column_group_cost;
        OPT_TRACE_COST_MODEL(KV(index_scan_cost), "+=", KV(column_group_cost));
        prefix_filter_sel *= cg_info.filter_sel_;
        if (cg_info.filters_.empty() && runtime_filter_sel < 1.0) {
          prefix_filter_sel *= runtime_filter_sel;
          runtime_filter_sel = 1.0;
        }
        LOG_TRACE("OPT:[COST ONE COLUMN GROUP]", K(row_count), K(prefix_filter_sel), K(column_group_cost));
      }
    }
  }
  LOG_TRACE("OPT:[COST INDEX SCAN WITH COLUMN STORE]", K(row_count), K(index_scan_cost));
  return ret;
}

int ObOptEstCostModel::cost_column_store_index_back(const ObCostTableScanInfo &est_cost_info,
                                                    double row_count,
                                                    double &prefix_filter_sel,
                                                    double &index_back_cost)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObCostTableScanInfo, column_group_est_cost_info, OB_INVALID_ID, OB_INVALID_ID, OB_INVALID_ID) {
    double network_cost = 0.0;
    index_back_cost = 0.0;
    if (OB_FAIL(column_group_est_cost_info.assign(est_cost_info))) {
      LOG_WARN("failed to assign est cost info", K(ret));
    } else {
      column_group_est_cost_info.access_column_items_.reuse();
      column_group_est_cost_info.prefix_filters_.reuse();
      column_group_est_cost_info.postfix_filters_.reuse();
      column_group_est_cost_info.use_column_store_ = true;
      column_group_est_cost_info.join_filter_sel_ = 1.0;
    }
    // calc scan cost for each column group
    for (int64_t i = 0; OB_SUCC(ret) && i < est_cost_info.index_back_column_group_infos_.count(); ++i) {
      // prepare est cost info for column group
      const ObCostColumnGroupInfo &cg_info = est_cost_info.index_back_column_group_infos_.at(i);
      double cg_row_count = row_count * prefix_filter_sel * cg_info.skip_filter_sel_ * cg_info.skip_rate_;
      column_group_est_cost_info.index_meta_info_.index_micro_block_count_ = cg_info.micro_block_count_;
      column_group_est_cost_info.table_filters_.reuse();
      double column_group_cost = 0.0;
      if (OB_FAIL(column_group_est_cost_info.table_filters_.assign(cg_info.filters_))) {
        LOG_WARN("failed to assign filters", K(ret));
      } else if (OB_FAIL(column_group_est_cost_info.access_column_items_.assign(cg_info.access_column_items_))) {
        LOG_WARN("failed to assign filters", K(ret));
      } else if (OB_FAIL(cost_range_get(column_group_est_cost_info,
                                        false,
                                        row_count * prefix_filter_sel,
                                        column_group_cost))) {
        LOG_WARN("failed to calc index scan cost", K(ret), K(cg_row_count), K(column_group_est_cost_info));
      } else {
        index_back_cost += column_group_cost;
        OPT_TRACE_COST_MODEL(KV(index_back_cost), "+=", KV(column_group_cost));
        prefix_filter_sel *= cg_info.filter_sel_;
        LOG_TRACE("OPT:[COST ONE COLUMN GROUP]", K(row_count), K(prefix_filter_sel), K(column_group_cost));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (est_cost_info.index_meta_info_.is_global_index_ &&
              OB_FAIL(cost_global_index_back_with_rp(row_count * prefix_filter_sel,
                                                      est_cost_info,
                                                      network_cost))) {
      LOG_WARN("failed to get newwork transform cost for global index", K(ret));
    } else {
      index_back_cost += network_cost;
      OPT_TRACE_COST_MODEL(KV(index_back_cost), "+=", KV(network_cost));
    }
    LOG_TRACE("OPT:[COST INDEX BACK WITH COLUMN STORE]", K(row_count), K(network_cost), K(index_back_cost));
  }
  return ret;
}

int ObOptEstCostModel::cost_row_store_index_scan(const ObCostTableScanInfo &est_cost_info,
                                                double row_count,
                                                double &index_scan_cost)
{
  int ret = OB_SUCCESS;
  //refine row count for index skip scan
  if (!est_cost_info.ss_ranges_.empty() && est_cost_info.ss_prefix_ndv_ > 0) {
    row_count /= est_cost_info.ss_prefix_ndv_;
  }
  if (ObSimpleBatch::T_GET == est_cost_info.batch_type_ ||
      ObSimpleBatch::T_MULTI_GET == est_cost_info.batch_type_) {
    if (OB_FAIL(cost_range_get(est_cost_info,
                               true,
                               row_count,
                               index_scan_cost))) {
      LOG_WARN("Failed to estimate get cost", K(ret));
    }
  } else if (ObSimpleBatch::T_SCAN == est_cost_info.batch_type_ ||
             ObSimpleBatch::T_MULTI_SCAN == est_cost_info.batch_type_) {
    if (OB_FAIL(cost_range_scan(est_cost_info,
                                true,
                                row_count,
                                index_scan_cost))) {
      LOG_WARN("Failed to estimate scan cost", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid batch type", K(ret), K(est_cost_info.batch_type_));
  }
  //add spatial index scan cost
  if (OB_FAIL(ret)) {
  } else if (est_cost_info.index_meta_info_.is_geo_index_) {
    double spatial_cost = row_count *  cost_params_.get_spatial_per_row_cost(sys_stat_);
    index_scan_cost += spatial_cost;
    OPT_TRACE_COST_MODEL(KV(index_scan_cost), "+=", KV(spatial_cost));
    LOG_TRACE("OPT::[COST SPATIAL INDEX SCAN]", K(spatial_cost), K(ret));
  } else if (est_cost_info.index_meta_info_.is_fulltext_index_) {
    // 全文索引一期：对于每一个 token，都需要:
    // 1. 以 [token, token] 为 range 扫描 inv_index 两次，计算一个聚合函数；
    // 2. 全表扫描 doc_id_rowkey_index, 计算一个聚合函数；
    // 3. 用过滤后的 doc_id 对 doc_id_rowkey_index 做回表
    int token_count = 1;  // 此处先假设 search query 只有一个 token，后续要调整
    double token_sel = DEFAULT_SEL;
    double inv_index_range_scan_cost = 0;
    double doc_id_full_scan_cost = 0;
    double doc_id_index_back_cost = 0;
    if (OB_FAIL(cost_range_scan(est_cost_info,
                                true,
                                row_count * token_sel,
                                inv_index_range_scan_cost))) {
      LOG_WARN("Failed to estimate scan cost", K(ret));
    } else if (OB_FAIL(cost_range_scan(est_cost_info,
                                       true,
                                       row_count,
                                       doc_id_full_scan_cost))) {
      LOG_WARN("Failed to estimate scan cost", K(ret));
    } else if (OB_FAIL(cost_range_get(est_cost_info,
                                      true,
                                      row_count * token_sel,
                                      doc_id_index_back_cost))) {
      LOG_WARN("Failed to estimate get cost", K(ret));
    }
    double aggregation_cost = (row_count * token_sel + row_count) * cost_params_.get_per_aggr_func_cost(sys_stat_);
    double fulltext_scan_cost = 2 * inv_index_range_scan_cost + doc_id_full_scan_cost +
                                aggregation_cost + doc_id_index_back_cost;
    index_scan_cost = token_count * fulltext_scan_cost;
    LOG_TRACE("OPT::[COST FULLTEXT INDEX SCAN]", K(fulltext_scan_cost), K(ret));
  }
  //add index skip scan cost
  if (OB_FAIL(ret)) {
  } else if (!est_cost_info.ss_ranges_.empty()) {
    index_scan_cost *= est_cost_info.ss_prefix_ndv_;
    OPT_TRACE_COST_MODEL(KV(index_scan_cost), "*=", KV_(est_cost_info.ss_prefix_ndv));
    LOG_TRACE("OPT::[COST INDEX SKIP SCAN]", K(est_cost_info.ss_prefix_ndv_), K(index_scan_cost));
  }
  return ret;
}

int ObOptEstCostModel::cost_row_store_index_back(const ObCostTableScanInfo &est_cost_info,
                                                double row_count,
                                                double &index_back_cost)
{
  int ret = OB_SUCCESS;
  double network_cost = 0.0;
  // calc real index back row count
  double index_back_row_count = row_count * est_cost_info.postfix_filter_sel_;
  if (OB_FAIL(cost_range_get(est_cost_info,
                             false,
                             index_back_row_count,
                             index_back_cost))) {
    LOG_WARN("Failed to estimate get cost", K(ret));
  } else if (est_cost_info.index_meta_info_.is_global_index_ &&
             OB_FAIL(cost_global_index_back_with_rp(index_back_row_count,
                                                    est_cost_info,
                                                    network_cost))) {
    LOG_WARN("failed to get newwork transform cost for global index", K(ret));
  } else {
    index_back_cost += network_cost;
    OPT_TRACE_COST_MODEL(KV(index_back_cost), "+=", KV(network_cost));
    LOG_TRACE("OPT:[COST ROW STORE INDEX BACK]", K(index_back_row_count),
                                       K(network_cost), K(index_back_cost));
  }
  return ret;
}

/*
 * estimate the network transform and rpc cost for global index,
 * so far, this cost model should be revised by banliu
 */
int ObOptEstCostModel::cost_global_index_back_with_rp(double row_count,
                                                      const ObCostTableScanInfo &est_cost_info,
                                                      double &network_cost)
{
  int ret = OB_SUCCESS;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  network_cost = 0.0;
  if (OB_ISNULL(table_meta_info) ||
      OB_UNLIKELY(table_meta_info->table_column_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table column count should not be 0", K(table_meta_info->table_column_count_), K(ret));
  } else {
    double column_count = est_cost_info.access_column_items_.count();
    double transform_size = (table_meta_info->average_row_size_ * row_count * column_count)
                            /static_cast<double>(table_meta_info->table_column_count_);
    network_cost = transform_size * cost_params_.get_network_trans_per_byte_cost(sys_stat_) +
            row_count * cost_params_.get_table_loopup_per_row_rpc_cost(sys_stat_);
    OPT_TRACE_COST_MODEL(KV(network_cost), "=", KV(transform_size), "*",
                         cost_params_.get_network_trans_per_byte_cost(sys_stat_), "+",
                         KV(row_count), "*", cost_params_.get_table_loopup_per_row_rpc_cost(sys_stat_));
    LOG_TRACE("OPT::[COST GLOBAL INDEX BACK WITH RPC]", K(network_cost), K(table_meta_info->average_row_size_),
            K(row_count), K(table_meta_info->table_column_count_));
  }
  return ret;
}

int ObOptEstCostModel::cost_range_scan(const ObCostTableScanInfo &est_cost_info,
																			 bool is_scan_index,
                                       double row_count,
                                       double &range_scan_cost)
{
  int ret = OB_SUCCESS;
  // 从memtable读取数据的代价，待提供
  double memtable_cost = 0;
  // memtable数据和基线数据合并的代价，待提供
  double memtable_merge_cost = 0;
  double io_cost = 0.0;
  double cpu_cost = 0.0;
  if (OB_FAIL(range_scan_io_cost(est_cost_info,
                                 is_scan_index,
                                 row_count,
                                 io_cost))) {
    LOG_WARN("failed to calc table scan io cost", K(ret));
  } else if (OB_FAIL(range_scan_cpu_cost(est_cost_info,
                                         is_scan_index,
                                         row_count,
                                         false,
                                         cpu_cost))) {
    LOG_WARN("failed to calc table scan cpu cost", K(ret));
  } else {
    if (io_cost > cpu_cost) {
        range_scan_cost = io_cost + memtable_cost + memtable_merge_cost;
        OPT_TRACE_COST_MODEL(KV(range_scan_cost), "=", KV(io_cost), "+",
                             KV(memtable_cost), "+", KV(memtable_merge_cost));
    } else {
        range_scan_cost = cpu_cost + memtable_cost + memtable_merge_cost;
        OPT_TRACE_COST_MODEL(KV(range_scan_cost), "=", KV(cpu_cost), "+",
                             KV(memtable_cost), "+", KV(memtable_merge_cost));
    }
    LOG_TRACE("OPT:[COST RANGE SCAN]", K(is_scan_index), K(row_count), K(range_scan_cost),
            K(io_cost), K(cpu_cost), K(memtable_cost), K(memtable_merge_cost));
  }
  return ret;
}

int ObOptEstCostModel::cost_range_get(const ObCostTableScanInfo &est_cost_info,
																			 bool is_scan_index,
                                       double row_count,
                                       double &range_get_cost)
{
  int ret = OB_SUCCESS;
  // 从memtable读取数据的代价，待提供
  double memtable_cost = 0;
  // memtable数据和基线数据合并的代价，待提供
  double memtable_merge_cost = 0;
  double io_cost = 0.0;
  double cpu_cost = 0.0;
  if (OB_FAIL(range_get_io_cost(est_cost_info,
                                is_scan_index,
                                row_count,
                                io_cost))) {
    LOG_WARN("failed to calc table get io cost", K(ret));
  } else if (OB_FAIL(range_scan_cpu_cost(est_cost_info,
                                         is_scan_index,
                                         row_count,
                                         true,
                                         cpu_cost))) {
    LOG_WARN("failed to calc table scan cpu cost", K(ret));
  } else {
    double fetch_row_cost = cost_params_.get_fetch_row_rnd_cost(sys_stat_) * row_count;
    OPT_TRACE_COST_MODEL(KV(fetch_row_cost), "=", cost_params_.get_fetch_row_rnd_cost(sys_stat_), "*", KV(row_count));
    range_get_cost = cpu_cost + io_cost + fetch_row_cost + memtable_cost + memtable_merge_cost;
    OPT_TRACE_COST_MODEL(KV(range_get_cost), "=", KV(cpu_cost), "+", KV(io_cost), "+", KV(fetch_row_cost),
                         KV(memtable_cost), "+", KV(memtable_merge_cost));
    LOG_TRACE("OPT:[COST RANGE GET]", K(is_scan_index), K(row_count), K(range_get_cost),
            K(io_cost), K(cpu_cost), K(fetch_row_cost), K(memtable_cost), K(memtable_merge_cost));
  }
  return ret;
}

int ObOptEstCostModel::range_get_io_cost(const ObCostTableScanInfo &est_cost_info,
                                        bool is_scan_index,
                                        double row_count,
                                        double &io_cost)
{
  int ret = OB_SUCCESS;
  io_cost = 0.0;
  const ObIndexMetaInfo &index_meta_info = est_cost_info.index_meta_info_;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  if (OB_ISNULL(table_meta_info) || row_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret));
  } else {
    //索引总的微块数 = 总大小/微块大小
    //计算涉及的微块数
    double num_micro_blocks = 0;
    if (is_scan_index) {
      num_micro_blocks = index_meta_info.get_micro_block_numbers();
    } else if (est_cost_info.index_back_with_column_store_) {
      num_micro_blocks = index_meta_info.get_micro_block_numbers();
    } else {
      num_micro_blocks = table_meta_info->get_micro_block_numbers();
    }
    double num_micro_blocks_read = 0;
    const double table_row_count = static_cast<double>(table_meta_info->table_row_count_);
    if (OB_LIKELY(table_row_count > 0 && row_count <= table_row_count)) {
      num_micro_blocks_read = num_micro_blocks * (1.0 - std::pow((1.0 - row_count / table_row_count), table_row_count / num_micro_blocks));
      num_micro_blocks_read = std::ceil(num_micro_blocks_read);
    } else {
      num_micro_blocks_read = num_micro_blocks;
    }
    // IO代价，包括读取整个微块及反序列化的代价和每行定位微块的代价
    double first_block_cost = cost_params_.get_micro_block_rnd_cost(sys_stat_);
    if (est_cost_info.is_inner_path_) {
      if (est_cost_info.can_use_batch_nlj_) {
        first_block_cost = cost_params_.get_batch_nl_get_cost(sys_stat_);
      } else {
        first_block_cost = cost_params_.get_nl_get_cost(sys_stat_);
      }
    }
    if (num_micro_blocks_read < 1) {
      io_cost = 0;
      OPT_TRACE_COST_MODEL(KV(io_cost), "= 0");
    } else {
      io_cost = first_block_cost + cost_params_.get_micro_block_rnd_cost(sys_stat_) * (num_micro_blocks_read-1);
      OPT_TRACE_COST_MODEL(KV(io_cost), "=", KV(first_block_cost), "+",
                           cost_params_.get_micro_block_rnd_cost(sys_stat_), "* (", KV(num_micro_blocks_read), "-1)");
    }
    LOG_TRACE("OPT:[COST RANGE GET IO]", K(is_scan_index), K(row_count), K(io_cost), K(num_micro_blocks),
                                          K(num_micro_blocks_read), K(first_block_cost));
  }
  return ret;
}

int ObOptEstCostModel::range_scan_io_cost(const ObCostTableScanInfo &est_cost_info,
                                          bool is_scan_index,
                                          double row_count,
                                          double &io_cost)
{
  int ret = OB_SUCCESS;
  io_cost = 0.0;
  const ObIndexMetaInfo &index_meta_info = est_cost_info.index_meta_info_;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  if (OB_ISNULL(table_meta_info) || row_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(row_count), KP(table_meta_info));
  } else {
    //索引总的微块数 = 总大小/微块大小
    //计算涉及的微块数
    double num_micro_blocks = 0;
    if (!is_scan_index) {
      num_micro_blocks = table_meta_info->get_micro_block_numbers();
    } else {
      num_micro_blocks = index_meta_info.get_micro_block_numbers();
    }
    //读微块数 = 总微块数 * 读行比例
    double num_micro_blocks_read = 0;
    const double table_row_count = static_cast<double>(table_meta_info->table_row_count_);
    if (OB_LIKELY(table_row_count > 0 && row_count <= table_row_count)) {
      num_micro_blocks_read = std::ceil(num_micro_blocks * row_count / table_row_count);
    } else {
      num_micro_blocks_read = num_micro_blocks;
    }

    // IO代价，主要包括读取微块、反序列化的代价的代价
    double first_block_cost = cost_params_.get_micro_block_rnd_cost(sys_stat_);
    if (!est_cost_info.pushdown_prefix_filters_.empty()) {
      if (est_cost_info.can_use_batch_nlj_) {
        first_block_cost = cost_params_.get_batch_nl_scan_cost(sys_stat_);
      } else {
        first_block_cost = cost_params_.get_nl_scan_cost(sys_stat_);
      }
    }
    if (num_micro_blocks_read < 1) {
      io_cost = first_block_cost;
      OPT_TRACE_COST_MODEL(KV(io_cost), "=", KV(first_block_cost));
    } else {
      io_cost = first_block_cost + cost_params_.get_micro_block_seq_cost(sys_stat_) * (num_micro_blocks_read-1);
      OPT_TRACE_COST_MODEL(KV(io_cost), "=", KV(first_block_cost), "+",
                           cost_params_.get_micro_block_seq_cost(sys_stat_), "* (", KV(num_micro_blocks_read), "-1)");
    }
    LOG_TRACE("OPT:[COST RANGE SCAN IO]", K(is_scan_index), K(row_count), K(io_cost), K(num_micro_blocks),
                                          K(num_micro_blocks_read), K(first_block_cost));
  }
  return ret;
}

int ObOptEstCostModel::range_scan_cpu_cost(const ObCostTableScanInfo &est_cost_info,
                                          bool is_scan_index,
                                          double row_count,
                                          bool is_get,
                                          double &cpu_cost)
{
  int ret = OB_SUCCESS;
  double project_cost = 0.0;
  const ObIndexMetaInfo &index_meta_info = est_cost_info.index_meta_info_;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  bool is_index_back = index_meta_info.is_index_back_;
  if (OB_ISNULL(table_meta_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret));
  } else if (is_scan_index && is_index_back) {
    if (OB_FAIL(cost_project(row_count,
                            est_cost_info.index_access_column_items_,
                            is_get,
                            est_cost_info.use_column_store_,
                            project_cost))) {
      LOG_WARN("failed to cost project", K(ret));
    }
  } else if (est_cost_info.use_column_store_) {
    if (OB_FAIL(cost_project(row_count,
                            est_cost_info.access_column_items_,
                            is_get,
                            est_cost_info.use_column_store_,
                            project_cost))) {
      LOG_WARN("failed to cost project", K(ret));
    }
  } else {
    if (OB_FAIL(cost_full_table_scan_project(row_count,
                                             est_cost_info,
                                             is_get,
                                             project_cost))) {
      LOG_WARN("failed to cost project", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    // 谓词代价，主要指filter的代价
    double qual_cost = 0.0;
    if (!is_index_back) {
      // 全表扫描
      qual_cost += cost_quals(row_count, est_cost_info.postfix_filters_);
      qual_cost += cost_quals(row_count, est_cost_info.table_filters_);
    } else if (is_scan_index) {
      // 索引扫描
      qual_cost += cost_quals(row_count, est_cost_info.postfix_filters_);
    } else {
      // 回表扫描
      qual_cost += cost_quals(row_count, est_cost_info.table_filters_);
    }
    // CPU代价，包括get_next_row调用的代价和谓词代价
    double range_cost = 0;
    double range_count = est_cost_info.ranges_.count();
    if (range_count > 1 && est_cost_info.at_most_one_range_) {
      range_count = 1;
    } else if (range_count > 1 && est_cost_info.index_meta_info_.is_multivalue_index_) {
      // json contains/json overlaps may extract any equal pred
      // range cost needn't grow liner with range-count
      // alse do a discount with param 0.25
      range_count = 1 + (range_count) * 0.25;
    }
    range_cost = range_count * cost_params_.get_range_cost(sys_stat_);
    cpu_cost = row_count * cost_params_.get_cpu_tuple_cost(sys_stat_);
    OPT_TRACE_COST_MODEL(KV(cpu_cost), "=", KV(row_count), "*", cost_params_.get_cpu_tuple_cost(sys_stat_));
    cpu_cost += range_cost + qual_cost + project_cost;
    OPT_TRACE_COST_MODEL(KV(cpu_cost), "+=", KV(range_cost), "+", KV(qual_cost), "+", KV(project_cost));
    LOG_TRACE("OPT: [RANGE SCAN CPU COST]", K(is_scan_index), K(is_get),
            K(cpu_cost), K(qual_cost), K(project_cost), K(range_cost), K(row_count));
  }
  return ret;
}

int ObOptEstCostModel::get_sort_cmp_cost(const common::ObIArray<sql::ObExprResType> &types,
                                   			 double &cost)
{
  int ret = OB_SUCCESS;
  double cost_ret = 0.0;
  if (OB_UNLIKELY(types.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid col count", "col count", types.count(), K(ret));
  } else {
    double factor = 1.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < types.count(); ++i) {
      ObObjTypeClass tc = types.at(i).get_type_class();
      if (OB_UNLIKELY(tc >= ObMaxTC)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("not supported type class", K(tc), K(ret));
      } else {
        //Correctly estimating cmp cost need NDVs of each sort col:
        //  if first col is identical, then we needn't compare the second col and so on.
        //But now we cannot get hand on NDV easily, just use
        //  cmp_cost_col0 + cmp_cost_col1 / DEF_NDV + cmp_cost_col2 / DEF_NDV^2 ...
        double cost_for_col = cost_params_.get_comparison_cost(sys_stat_, tc);;
        if (OB_UNLIKELY(cost_for_col < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not supported type class", K(tc), K(ret));
        } else {
          cost_ret += cost_for_col * factor;
          factor /= 10.0;
        }
      }
    }
    if (OB_SUCC(ret)) {
      cost = cost_ret;
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_window_function(double rows, double width, double win_func_cnt, double &cost)
{
  int ret = OB_SUCCESS;
  cost += rows * cost_params_.get_cpu_tuple_cost(sys_stat_);
  cost += ObOptEstCostModel::cost_material(rows, width) + ObOptEstCostModel::cost_read_materialized(rows);
  cost += rows * cost_params_.get_per_win_func_cost(sys_stat_) * win_func_cnt;
  return ret;
}

/**
 * @brief     计算filter的代价
 * @formula   cost = rows * CPU_TUPLE_COST + cost_quals
 * @param[in] rows        输入行数
 * @param[in] filters filter数量
 * @return    算子代价
 */
double ObOptEstCostModel::cost_filter_rows(double rows, ObIArray<ObRawExpr*> &filters)
{
  return rows * cost_params_.get_cpu_tuple_cost(sys_stat_) + cost_quals(rows, filters);
}

/**
 *  @brief   估算SubplanFilter的代价
 *
 *  @formula 除最左的子节点以外，其它的节点都是一个filter，filter分成3种类型：
 *           1. onetime expr：这种类型的filter只需要计算一次，而且不需要物化
 *           2. initplan    : 这种类型的filter只需要计算一次，之后物化，从物化的数据中读取
 *           3. 其它         : 剩下的所有filter每次都要重新计算
 */
int ObOptEstCostModel::cost_subplan_filter(const ObSubplanFilterCostInfo &info,
                                           double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double onetime_cost = 0.0;
  if (info.children_.count() > 0) {
    cost += info.children_.at(0).rows_ * cost_params_.get_cpu_tuple_cost(sys_stat_);
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < info.children_.count(); ++i) {
    const ObBasicCostInfo &child = info.children_.at(i);
    //判断是否为onetime expr;
    if (info.onetime_idxs_.has_member(i)) { // onetime cost
      // 这个子节点是一个onetime expr
      // 则只需要进行一次右表计算，且不物化
      onetime_cost += child.cost_;
    } else if (info.initplan_idxs_.has_member(i)) { // init plan cost
      // 这个子节点是一个initplan
      // 对右表进行物化，之后只需读取物化后的行
      onetime_cost += child.cost_ + child.rows_ * cost_params_.get_cpu_tuple_cost(sys_stat_)
          + cost_material(child.rows_, child.width_);
      cost += info.children_.at(0).rows_ * cost_read_materialized(child.rows_);
    } else { // other cost
      // 一般情况，每一次都要扫描右表
      cost += info.children_.at(0).rows_ * (child.cost_ + child.rows_ * cost_params_.get_cpu_tuple_cost(sys_stat_));
      if (child.exchange_allocated_) {
        cost += cost_params_.get_px_rescan_per_row_cost(sys_stat_) * info.children_.at(0).rows_;
      }
    }
  } // for info_childs end

  if (OB_SUCC(ret)) {
    cost += onetime_cost;
    LOG_TRACE("OPT: [COST SUBPLAN FILTER]", K(cost), K(onetime_cost), K(info));
  }
  return ret;
}

int ObOptEstCostModel::cost_union_all(const ObCostMergeSetInfo &info, double &cost)
{
  int ret = OB_SUCCESS;
  double total_rows = 0.0;
  for (int64_t i = 0; i < info.children_.count(); ++i) {
    total_rows += info.children_.at(i).rows_;
  }
  cost = total_rows * cost_params_.get_cpu_tuple_cost(sys_stat_);
  return ret;
}

/**
 * @brief 计算集合运算的代价（包括union / except / intersect）
 * @param[in]  info    估算集合运算代价所需要的一些参数
 * @param[out] cost 估算出的集合运算算子本身的代价
 * 对于merge set，可能出现set op展平的情况，所以需要考虑多个孩子节点
 */
int ObOptEstCostModel::cost_merge_set(const ObCostMergeSetInfo &info, double &cost)
{
  int ret = OB_SUCCESS;
  double sum_rows = 0;
  double width = 0.0;
  for (int64_t i = 0; i < info.children_.count(); ++i) {
    sum_rows += info.children_.at(i).rows_;
    width = info.children_.at(i).width_;
  }
  cost = 0.0;
  //get next row cost
  cost += sum_rows * cost_params_.get_cpu_tuple_cost(sys_stat_);
  cost += cost_material(sum_rows, width);
  //operator cost：cmp_cost + cpu_cost
  LOG_TRACE("OPT: [COST MERGE SET]", K(cost), K(sum_rows), K(width));
  return ret;
}

/**
 * @brief 计算集合运算的代价（包括union / except / intersect）
 * @param[in]  info    估算集合运算代价所需要的一些参数
 * @param[out] cost    估算出的集合运算算子本身的代价
 * 对于hash set，不会出现set op展平的情况，所以只需要考虑两个孩子节点
 */
int ObOptEstCostModel::cost_hash_set(const ObCostHashSetInfo &info, double &cost)
{
  int ret = OB_SUCCESS;
  double build_rows = 0.0;
  double probe_rows = 0.0;
  if (ObSelectStmt::UNION == info.op_) {
    build_rows = info.left_rows_ + info.right_rows_;
    probe_rows = info.left_rows_ + info.right_rows_;
  } else if (ObSelectStmt::INTERSECT == info.op_) {
    build_rows = info.left_rows_;
    probe_rows = info.left_rows_ + info.right_rows_;
  } else if (ObSelectStmt::EXCEPT == info.op_) {
    build_rows = info.left_rows_;
    probe_rows = info.left_rows_ + info.right_rows_;
  }

  cost = 0.0;
  //get_next_row() 代价
  cost += cost_params_.get_cpu_tuple_cost(sys_stat_) * (info.left_rows_ + info.right_rows_);
  //material cost
  cost += cost_material(info.left_rows_, info.left_width_) +
             cost_material(info.right_rows_, info.right_width_);
  //build hash table cost
  cost += cost_params_.get_build_hash_per_row_cost(sys_stat_) * build_rows;
  //probe hash table cost
  cost += cost_params_.get_probe_hash_per_row_cost(sys_stat_) * probe_rows;
  //计算 hash 的代价
  cost += cost_hash(info.left_rows_ + info.right_rows_, info.hash_columns_);

  LOG_TRACE("OPT: [COST HASH SET]", K(cost));
  return ret;
}


/**
 * @brief                 计算hash值的代价
 * @note(@ banliu.zyd)    这个函数用于估算hash计算的代价，为了使代码简洁不侵入，这个函数
 *                        直接以计算hash的代价为返回值，对于发现的某个谓词为空直接跳过，认为
 *                        在其它地方有对谓词是否存在错误的判断，检测的逻辑不应在这里
 * @param[in] rows        数据行数
 * @param[in] hash_exprs  hash列数组
 *
 */
double ObOptEstCostModel::cost_hash(double rows, const ObIArray<ObRawExpr *> &hash_exprs)
{
  double cost_per_row = 0.0;
  for (int64_t i = 0; i < hash_exprs.count(); ++i) {
    const ObRawExpr *expr = hash_exprs.at(i);
    if (OB_ISNULL(expr)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "qual should not be NULL, but we don't set error return code here, just skip it");
    } else {
      ObObjTypeClass calc_type = expr->get_result_type().get_calc_type_class();
      cost_per_row += cost_params_.get_hash_cost(sys_stat_,calc_type);
    }
  }
  return rows * cost_per_row;
}

int ObOptEstCostModel::cost_project(double rows,
																		const ObIArray<ColumnItem> &columns,
																		bool is_get,
                                    bool use_column_store,
																		double &cost)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> project_columns;
  for (int i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ColumnItem &column_item = columns.at(i);
    ObRawExpr *expr = column_item.expr_;
    if (OB_FAIL(project_columns.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret) &&
      OB_FAIL(cost_project(rows,
                           project_columns,
                           is_get,
                           use_column_store,
                           cost))) {
    LOG_WARN("failed to calc project cost", K(ret));
  }
  return ret;
}

int ObOptEstCostModel::cost_project(double rows,
																		const ObIArray<ObRawExpr*> &columns,
																		bool is_get,
                                    bool use_column_store,
																		double &cost)
{
  int ret = OB_SUCCESS;
  double project_one_row_cost = 0.0;
  for (int i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ObRawExpr *expr = columns.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->get_ref_count() <= 0) {
      //do nothing
    } else {
      const ObExprResType &type = expr->get_result_type();
      if (type.is_integer_type()) {
        // int
        project_one_row_cost += cost_params_.get_project_column_cost(sys_stat_,
                                                                     PROJECT_INT,
                                                                     is_get,
                                                                     use_column_store);
      } else if (type.get_accuracy().get_length() > 0) {
        // ObStringTC
        int64_t string_width = type.get_accuracy().get_length();
        string_width = std::min(string_width, ObOptEstCostModel::DEFAULT_MAX_STRING_WIDTH);
        project_one_row_cost += cost_params_.get_project_column_cost(sys_stat_,
                                                                     PROJECT_CHAR,
                                                                     is_get,
                                                                     use_column_store) * string_width;
      } else if (type.get_accuracy().get_precision() > 0 || type.is_oracle_integer()) {
        // number, time
        project_one_row_cost += cost_params_.get_project_column_cost(sys_stat_,
                                                                     PROJECT_NUMBER,
                                                                     is_get,
                                                                     use_column_store);
      } else {
        // default for DEFAULT PK
        project_one_row_cost += cost_params_.get_project_column_cost(sys_stat_,
                                                                     PROJECT_INT,
                                                                     is_get,
                                                                     use_column_store);
      }
    }
  }
  cost = project_one_row_cost * rows;
  LOG_TRACE("COST PROJECT:", K(cost), K(rows), K(columns));
  return ret;
}

int ObOptEstCostModel::cost_full_table_scan_project(double rows,
                                                    const ObCostTableScanInfo &est_cost_info,
                                                    bool is_get,
                                                    double &cost)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> filter_columns;
  double cost_project_filter_column = 0;
  double project_one_row_cost = 0;
  double project_full_row_count = rows * est_cost_info.table_filter_sel_
                                       * est_cost_info.join_filter_sel_;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(est_cost_info.postfix_filters_,
                                                  filter_columns))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(est_cost_info.table_filters_,
                                                         filter_columns))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_FAIL(cost_project(project_full_row_count,
                                  est_cost_info.access_column_items_,
                                  is_get,
                                  est_cost_info.use_column_store_,
                                  cost))) {
    LOG_WARN("failed to calc project cost", K(ret));
  } else if (OB_FAIL(cost_project(rows,
                                  filter_columns,
                                  is_get,
                                  est_cost_info.use_column_store_,
                                  cost_project_filter_column))) {
    LOG_WARN("failed to calc project cost", K(ret));
  } else {
    cost += cost_project_filter_column;
    LOG_TRACE("COST TABLE SCAN PROJECT:", K(rows), K(project_full_row_count),
                                      K(cost_project_filter_column), K(cost));
  }
  return ret;
}

/**
 * @brief              计算谓词部分的代价
 * @note(@ banliu.zyd) 这个函数用于估算谓词计算的代价，为了使代码简洁不侵入，这个函数
 *                     直接以谓词代价为返回值，对于发现的某个谓词为空直接跳过，认为
 *                     在其它地方有对谓词是否存在错误的判断，检测的逻辑不应在这里
 * @param[in] rows     数据行数
 * @param[in] quals    谓词数组
 *
 */
// 谓词代价 = 行数 * sum(不同谓词类型比较的代价)
double ObOptEstCostModel::cost_quals(double rows, const ObIArray<ObRawExpr *> &quals, bool need_scale)
{
  double factor = 1.0;
  double cost_per_row = 0.0;
  for (int64_t i = 0; i < quals.count(); ++i) {
    const ObRawExpr *qual = quals.at(i);
    if (OB_ISNULL(qual)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "qual should not be NULL, but we don't set error return code here, just skip it");
    } else if (qual->is_spatial_expr()) {
      cost_per_row +=  cost_params_.get_cmp_spatial_cost(sys_stat_) * factor;
      if (need_scale) {
        factor /= 10.0;
      }
    } else if (qual->is_multivalue_expr()) {
      cost_per_row += cost_params_.get_comparison_cost(sys_stat_, ObJsonTC) * factor;
      if (need_scale) {
        factor /= 25.0;
      }
    } else {
      ObObjTypeClass calc_type = qual->get_result_type().get_calc_type_class();
      cost_per_row += cost_params_.get_comparison_cost(sys_stat_, calc_type) * factor;
      if (need_scale) {
        factor /= 10.0;
      }
    }
  }
  return rows * cost_per_row;
}

int ObOptEstCostModel::cost_insert(ObDelUpCostInfo& cost_info, double &cost)
{
  int ret = OB_SUCCESS;
  cost = cost_params_.get_cpu_tuple_cost(sys_stat_) * cost_info.affect_rows_ +
         cost_params_.get_insert_per_row_cost(sys_stat_) * cost_info.affect_rows_ +
         cost_params_.get_insert_index_per_row_cost(sys_stat_) * cost_info.index_count_ +
         cost_params_.get_insert_check_per_row_cost(sys_stat_) * cost_info.constraint_count_;
  return ret;
}

int ObOptEstCostModel::cost_update(ObDelUpCostInfo& cost_info, double &cost)
{
  int ret = OB_SUCCESS;
  cost = cost_params_.get_cpu_tuple_cost(sys_stat_) * cost_info.affect_rows_ +
         cost_params_.get_update_per_row_cost(sys_stat_) * cost_info.affect_rows_ +
         cost_params_.get_update_index_per_row_cost(sys_stat_) * cost_info.index_count_ +
         cost_params_.get_update_check_per_row_cost(sys_stat_) * cost_info.constraint_count_;
  return ret;
}

int ObOptEstCostModel::cost_delete(ObDelUpCostInfo& cost_info, double &cost)
{
  int ret = OB_SUCCESS;
  cost = cost_params_.get_cpu_tuple_cost(sys_stat_) * cost_info.affect_rows_ +
         cost_params_.get_delete_per_row_cost(sys_stat_) * cost_info.affect_rows_ +
         cost_params_.get_delete_index_per_row_cost(sys_stat_) * cost_info.index_count_ +
         cost_params_.get_delete_check_per_row_cost(sys_stat_) * cost_info.constraint_count_;
  return ret;
}

int ObOptEstCostModel::calc_range_cost(const ObTableMetaInfo& table_meta_info,
                                      const ObIArray<ObRawExpr *> &filters,
                                      int64_t index_column_count,
                                      int64_t range_count,
                                      double range_sel,
                                      double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0;
  int64_t row_count = table_meta_info.table_row_count_ * range_sel;
  double num_micro_blocks = -1;
  if (table_meta_info.has_opt_stat_) {
    num_micro_blocks = table_meta_info.micro_block_count_;
    num_micro_blocks *= index_column_count * 1.0 / table_meta_info.table_column_count_;
  }
  double num_micro_blocks_read = 0;
  if (OB_LIKELY(table_meta_info.table_row_count_ > 0)) {
    num_micro_blocks_read = std::ceil(num_micro_blocks
                                      * row_count
                                      / static_cast<double> (table_meta_info.table_row_count_));
  }
  double io_cost = cost_params_.get_micro_block_seq_cost(sys_stat_) * num_micro_blocks_read;
  double qual_cost = cost_quals(row_count, filters);
  double cpu_cost = row_count * cost_params_.get_cpu_tuple_cost(sys_stat_)
                      + range_count * cost_params_.get_range_cost(sys_stat_) + qual_cost;
  cpu_cost += row_count * cost_params_.get_table_scan_cpu_tuple_cost(sys_stat_);
  cost = io_cost + cpu_cost;
  return ret;
}

int ObOptEstCostModel::calc_pred_cost_per_row(const ObRawExpr *expr,
                                              double card,
                                              double &cost)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    double rows = expr->is_const_expr() && card > 0 ? card : 1;
    bool need_calc_child_cost = true;
    if (IS_SPATIAL_OP(expr->get_expr_type())
       || IS_GEO_OP(expr->get_expr_type())
       || expr->is_json_expr()
       || expr->is_xml_expr()) {
      cost += cost_params_.get_cmp_spatial_cost(sys_stat_) / rows;
    } else if (expr->is_udf_expr()) {
      cost += cost_params_.get_cmp_udf_cost(sys_stat_) / rows;
    } else if (ob_is_lob_locator(expr->get_result_type().get_type())) {
      cost += cost_params_.get_cmp_lob_cost(sys_stat_) / rows;
    } else if (T_OP_DIV == expr->get_expr_type()) {
      cost += cost_params_.get_cmp_err_handle_expr_cost(sys_stat_) / rows;
    } else if (T_FUN_SYS_CAST == expr->get_expr_type()) {
      if (OB_ISNULL(expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        ObObjType src = expr->get_param_expr(0)->get_result_type().get_type();
        ObObjType dst = expr->get_result_type().get_type();
        if (ob_is_string_type(src) &&
            (ob_is_numeric_type(dst) || ob_is_temporal_type(dst))) {
          cost += cost_params_.get_cmp_err_handle_expr_cost(sys_stat_) / rows;
        } else {
          cost += cost_params_.get_comparison_cost(sys_stat_,ObIntTC) / rows;
        }
      }
    } else if (T_OP_IN == expr->get_expr_type()) {
      if (expr->get_param_count() != 2 || OB_ISNULL(expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid in params", K(ret));
      } else {
        cost += (expr->get_param_expr(1)->get_param_count() + 1) * cost_params_.get_comparison_cost(sys_stat_,ObIntTC) / rows;
      }
      need_calc_child_cost = false;
    } else {
      cost += cost_params_.get_comparison_cost(sys_stat_,ObIntTC) / rows;
    }
    if (need_calc_child_cost) {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (OB_FAIL(SMART_CALL(calc_pred_cost_per_row(expr->get_param_expr(i), card, cost)))) {
          LOG_WARN("calc cost per tuple failed", K(ret), KPC(expr));
        }
      }
    }
  }
  return ret;
}
