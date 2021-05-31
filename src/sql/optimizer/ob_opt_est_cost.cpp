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
#include <math.h>
#include "share/stat/ob_table_stat.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_optimizer.h"
#include "storage/ob_range_iterator.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_partition.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::storage;
using share::schema::ObSchemaGetterGuard;

const ObOptEstCost::ObCostParams ObOptEstCost::cost_params_;
const double ObOptEstCost::ObCostParams::DEFAULT_CPU_TUPLE_COST = 0.138021936096;
const double ObOptEstCost::ObCostParams::DEFAULT_MICRO_BLOCK_SEQ_COST = 35.745839897756;
const double ObOptEstCost::ObCostParams::DEFAULT_MICRO_BLOCK_RND_COST = 46.915803714286;
const double ObOptEstCost::ObCostParams::DEFAULT_PROJECT_COLUMN_SEQ_COST = 0.022058289557;
const double ObOptEstCost::ObCostParams::DEFAULT_PROJECT_COLUMN_RND_COST = 0.091368088578;
const double ObOptEstCost::ObCostParams::DEFAULT_FETCH_ROW_RND_COST = 4.717976971326;
const double ObOptEstCost::ObCostParams::DEFAULT_CMP_INT_COST = 0.083724216640;
const double ObOptEstCost::ObCostParams::DEFAULT_CMP_NUMBER_COST = 0.16744963328;
const double ObOptEstCost::ObCostParams::DEFAULT_CMP_CHAR_COST = 0.46836853027344;
const double ObOptEstCost::ObCostParams::INVALID_CMP_COST = -1;
const double ObOptEstCost::ObCostParams::DEFAULT_HASH_INT_COST = 0.05813460227;
const double ObOptEstCost::ObCostParams::DEFAULT_HASH_NUMBER_COST = 0.08414224927;
const double ObOptEstCost::ObCostParams::DEFAULT_HASH_CHAR_COST = 0.45419188363;
const double ObOptEstCost::ObCostParams::INVALID_HASH_COST = -1;
const double ObOptEstCost::ObCostParams::DEFAULT_MATERIALIZE_PER_BYTE_COST = 0.04593843865002;
const double ObOptEstCost::ObCostParams::DEFAULT_MATERIALIZED_ROW_COST = 0.187837063;
const double ObOptEstCost::ObCostParams::DEFAULT_MATERIALIZED_BYTE_READ_COST = 0.0026111726;
const double ObOptEstCost::ObCostParams::DEFAULT_PER_AGGR_FUNC_COST = 0.052990468;
const double ObOptEstCost::ObCostParams::DEFAULT_CPU_OPERATOR_COST = 0.052990468;
const double ObOptEstCost::ObCostParams::DEFAULT_JOIN_PER_ROW_COST = 0.50935757;
const double ObOptEstCost::ObCostParams::DEFAULT_BUILD_HASH_PER_ROW_COST = 0.715711487380;
const double ObOptEstCost::ObCostParams::DEFAULT_PROBE_HASH_PER_ROW_COST = 0.132096735636;
const double ObOptEstCost::ObCostParams::DEFAULT_NETWORK_PER_BYTE_COST = 0.011832508338;
const double ObOptEstCost::ObCostParams::DEFAULT_NET_PER_ROW_COST = 0.4;

const double ObOptEstCost::comparison_params_[ObMaxTC + 1] = {
    ObCostParams::DEFAULT_CMP_INT_COST,     // null
    ObCostParams::DEFAULT_CMP_INT_COST,     // int8, int16, int24, int32, int64.
    ObCostParams::DEFAULT_CMP_INT_COST,     // uint8, uint16, uint24, uint32, uint64.
    ObCostParams::DEFAULT_CMP_INT_COST,     // float, ufloat.
    ObCostParams::DEFAULT_CMP_INT_COST,     // double, udouble.
    ObCostParams::DEFAULT_CMP_NUMBER_COST,  // number, unumber.
    ObCostParams::DEFAULT_CMP_INT_COST,     // datetime, timestamp.
    ObCostParams::DEFAULT_CMP_INT_COST,     // date
    ObCostParams::DEFAULT_CMP_INT_COST,     // time
    ObCostParams::DEFAULT_CMP_INT_COST,     // year
    ObCostParams::DEFAULT_CMP_CHAR_COST,    // varchar, char, varbinary, binary.
    ObCostParams::DEFAULT_CMP_INT_COST,     // extend
    ObCostParams::INVALID_CMP_COST,         // unknown
    ObCostParams::DEFAULT_CMP_CHAR_COST,    // TinyText,MediumText, Text ,LongText
};

const double ObOptEstCost::hash_params_[ObMaxTC + 1] = {
    ObCostParams::DEFAULT_HASH_INT_COST,     // null
    ObCostParams::DEFAULT_HASH_INT_COST,     // int8, int16, int24, int32, int64.
    ObCostParams::DEFAULT_HASH_INT_COST,     // uint8, uint16, uint24, uint32, uint64.
    ObCostParams::DEFAULT_HASH_INT_COST,     // float, ufloat.
    ObCostParams::DEFAULT_HASH_INT_COST,     // double, udouble.
    ObCostParams::DEFAULT_HASH_NUMBER_COST,  // number, unumber.
    ObCostParams::DEFAULT_HASH_INT_COST,     // datetime, timestamp.
    ObCostParams::DEFAULT_HASH_INT_COST,     // date
    ObCostParams::DEFAULT_HASH_INT_COST,     // time
    ObCostParams::DEFAULT_HASH_INT_COST,     // year
    ObCostParams::DEFAULT_HASH_CHAR_COST,    // varchar, char, varbinary, binary.
    ObCostParams::DEFAULT_HASH_INT_COST,     // extend
    ObCostParams::INVALID_HASH_COST,         // unknown
    ObCostParams::DEFAULT_HASH_CHAR_COST,    // TinyText,MediumText, Text ,LongText
};

const int64_t ObOptEstCost::MAX_STORAGE_RANGE_ESTIMATION_NUM = 5;

int ObCostTableScanInfo::assign(const ObCostTableScanInfo& est_cost_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(range_columns_.assign(est_cost_info.range_columns_))) {
    LOG_WARN("failed to assign range columns", K(ret));
  } else if (OB_FAIL(table_scan_param_.column_ids_.assign(est_cost_info.table_scan_param_.column_ids_))) {
    LOG_WARN("failed to assign table scan param column ids", K(ret));
  } else {
    table_id_ = est_cost_info.table_id_;
    ref_table_id_ = est_cost_info.ref_table_id_;
    index_id_ = est_cost_info.index_id_;
    table_meta_info_.assign(est_cost_info.table_meta_info_);
    index_meta_info_.assign(est_cost_info.index_meta_info_);
    is_virtual_table_ = est_cost_info.is_virtual_table_;
    is_unique_ = est_cost_info.is_unique_;
    est_sel_info_ = est_cost_info.est_sel_info_;
    row_est_method_ = est_cost_info.row_est_method_;
    prefix_filter_sel_ = est_cost_info.prefix_filter_sel_;
    pushdown_prefix_filter_sel_ = est_cost_info.pushdown_prefix_filter_sel_;
    postfix_filter_sel_ = est_cost_info.postfix_filter_sel_;
    table_filter_sel_ = est_cost_info.table_filter_sel_;
    batch_type_ = est_cost_info.batch_type_;
    sample_info_ = est_cost_info.sample_info_;
    // no need to copy table scan param
  }
  return ret;
}

void ObTableMetaInfo::assign(const ObTableMetaInfo& table_meta_info)
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
  is_only_memtable_data_ = table_meta_info.is_only_memtable_data_;
  cost_est_type_ = table_meta_info.cost_est_type_;
}

void ObIndexMetaInfo::assign(const ObIndexMetaInfo& index_meta_info)
{
  ref_table_id_ = index_meta_info.ref_table_id_;
  index_id_ = index_meta_info.index_id_;
  index_micro_block_size_ = index_meta_info.index_micro_block_size_;
  index_part_size_ = index_meta_info.index_part_size_;
  index_column_count_ = index_meta_info.index_column_count_;
  is_index_back_ = index_meta_info.is_index_back_;
  is_unique_index_ = index_meta_info.is_unique_index_;
  is_global_index_ = index_meta_info.is_global_index_;
}

int ObOptEstCost::extract_rel_vars(
    const ObDMLStmt* stmt, const ObRelIds& relids, const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& vars)
{
  int ret = OB_SUCCESS;
  int64_t N = exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    ret = ObOptEstCost::extract_rel_vars(stmt, relids, exprs.at(i), vars);
  }
  return ret;
}

int ObOptEstCost::extract_rel_vars(
    const ObDMLStmt* stmt, const ObRelIds& relids, const ObRawExpr* expr, ObIArray<ObRawExpr*>& vars)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> columns;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, columns))) {
    LOG_WARN("failed to extract_column_exprs", K(expr), K(ret));
  } else {
    int64_t N = columns.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_ISNULL(columns.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(columns.at(i)), K(i), K(ret));
      } else if (ObRawExpr::EXPR_COLUMN_REF != columns.at(i)->get_expr_class()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extract_column_exprs returns invalid expr", K(columns.at(i)->get_expr_class()), K(i), K(ret));
      } else {
        uint64_t relid = static_cast<ObColumnRefRawExpr*>(columns.at(i))->get_table_id();
        if (relids.has_member(stmt->get_table_bit_index(relid))) {
          ret = vars.push_back(columns.at(i));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObOptEstCost::cost_subplan_filter(const ObSubplanFilterCostInfo& info, double& op_cost, double& cost)
{
  int ret = OB_SUCCESS;
  op_cost = 0.0;
  cost = 0.0;
  if (info.children_.count() > 0) {
    cost = info.children_.at(0).cost_ + info.children_.at(0).rows_ * cost_params_.CPU_TUPLE_COST;
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < info.children_.count(); ++i) {
    const ObBasicCostInfo& child = info.children_.at(i);
    bool is_onetime_expr = false;
    for (int64_t j = 0; OB_SUCC(ret) && !is_onetime_expr && j < info.onetime_exprs_.count(); ++j) {
      if (OB_FAIL(is_subquery_one_time(info.onetime_exprs_.at(j).second, i, is_onetime_expr))) {
        LOG_WARN("failed to check if subquery is one time expr", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_onetime_expr) {  // onetime cost
      op_cost = child.cost_;
      cost += op_cost;
      LOG_TRACE("OPT: [COST SUBPLAN ONETIME COST]", K(op_cost), K(cost));
    } else if (info.initplans_.has_member(i)) {  // init plan cost
      op_cost = cost_material(child.rows_, child.width_);
      op_cost += info.children_.at(0).rows_ * cost_read_materialized(child.rows_, child.width_);
      op_cost += child.cost_ + child.rows_ * cost_params_.CPU_TUPLE_COST;
      cost += op_cost;
      LOG_TRACE("OPT: [COST SUBPLAN INIT_PLAN COST]", K(op_cost), K(cost));
    } else {  // other cost
      op_cost = info.children_.at(0).rows_ * (child.cost_ + child.rows_ * cost_params_.CPU_TUPLE_COST);
      cost += op_cost;
      LOG_TRACE("OPT: [COST SUBPLAN OTHER COST]", K(op_cost), K(cost));
    }
  }  // for info_childs end

  LOG_TRACE("OPT: [COST SUBPLAN FILTER]", K(cost), K(op_cost), K(info));

  return ret;
}

int ObOptEstCost::is_subquery_one_time(const ObRawExpr* expr, int64_t query_ref_id, bool& is_onetime_expr)
{
  int ret = OB_SUCCESS;
  is_onetime_expr = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(expr), K(ret));
  } else if (expr->is_query_ref_expr()) {
    const ObQueryRefRawExpr* subquery = static_cast<const ObQueryRefRawExpr*>(expr);
    if (subquery->get_ref_id() == query_ref_id) {
      is_onetime_expr = true;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_onetime_expr && i < expr->get_param_count(); i++) {
      if (OB_FAIL(is_subquery_one_time(expr->get_param_expr(i), query_ref_id, is_onetime_expr))) {
        LOG_WARN("failed to check if subquery is one time expr", K(ret));
      }
    }
  }
  return ret;
}

int ObOptEstCost::cost_nestloop(
    const ObCostNLJoinInfo& est_cost_info, double& op_cost, double& nl_cost, ObIArray<ObExprSelPair>* all_predicate_sel)
{
  int ret = OB_SUCCESS;
  op_cost = 0.0;
  nl_cost = 0.0;
  if (OB_ISNULL(est_cost_info.get_est_sel_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point", K(est_cost_info.get_est_sel_info()));
  } else {
    double left_rows = est_cost_info.left_rows_;
    double right_rows = est_cost_info.right_rows_;
    double cart_tuples = left_rows * right_rows;  // tuples of Cartesian product
    double out_tuples = 0.0;
    double filter_selectivity = 0.0;
    double material_cost = 0.0;
    // selectivity for equal conds
    if (OB_FAIL(ObOptEstSel::calculate_selectivity(*est_cost_info.get_est_sel_info(),
            est_cost_info.other_join_condition_,
            filter_selectivity,
            all_predicate_sel))) {
      LOG_WARN("Failed to calculate filter selectivity", K(ret));
    } else {
      out_tuples = cart_tuples * filter_selectivity;
      nl_cost += est_cost_info.left_cost_ + cost_params_.CPU_TUPLE_COST * left_rows;
      double once_rescan_cost = est_cost_info.right_cost_ + right_rows * cost_params_.CPU_TUPLE_COST;
      if (est_cost_info.need_mat_) {
        material_cost = ObOptEstCost::cost_material(right_rows, est_cost_info.right_width_);
        nl_cost += material_cost + est_cost_info.right_cost_ + right_rows * cost_params_.CPU_TUPLE_COST;
        once_rescan_cost = cost_read_materialized(right_rows, est_cost_info.right_width_);
      }
      // total rescan cost
      if (LEFT_SEMI_JOIN == est_cost_info.join_type_ || LEFT_ANTI_JOIN == est_cost_info.join_type_) {
        double match_sel = (est_cost_info.anti_or_semi_match_sel_ < OB_DOUBLE_EPSINON)
                               ? OB_DOUBLE_EPSINON
                               : est_cost_info.anti_or_semi_match_sel_;
        // As left_rows * match_sel * agv_match_count = left_rows * right_rows * cond_selectivity
        double avg_match_count = (right_rows * filter_selectivity) / match_sel;
        avg_match_count = std::min(avg_match_count, right_rows);
        // TODO for future, optimizer can get whole path first(include exchange),
        // so we can calc and record startup cost of every operator.
        //
        // cost += once_rescan_cost;//cannot guarantee all rows found matching in right path
        // double outer_matched_rows = static_cast<double>(left_rows) * match_sel;
        // cost += outer_matched_rows * once_rescan_cost * inner_scan_frac;
        // if (c_info.with_nl_param_) {
        //  cost += (left_rows - outer_matched_rows) * once_rescan_cost / right_rows;
        //} else {
        //  cost += (left_rows - outer_matched_rows) * once_rescan_cost;
        //}
        out_tuples = left_rows * match_sel;
        op_cost += left_rows * once_rescan_cost;
      } else {
        op_cost += left_rows * once_rescan_cost;
      }
      // qual cost
      double qual_cost = cost_quals(left_rows * right_rows, est_cost_info.equal_join_condition_) +
                         cost_quals(left_rows * right_rows, est_cost_info.other_join_condition_);
      op_cost += qual_cost;

      double join_cost = cost_params_.JOIN_PER_ROW_COST * out_tuples;
      op_cost += join_cost;

      nl_cost += op_cost;

      LOG_TRACE("OPT: [COST NESTLOOP JOIN]",
          K(nl_cost),
          K(op_cost),
          K(qual_cost),
          K(join_cost),
          K(est_cost_info.left_cost_),
          K(est_cost_info.right_cost_),
          K(left_rows),
          K(right_rows),
          K(est_cost_info.right_width_),
          K(filter_selectivity),
          K(cart_tuples),
          K(material_cost));
    }
  }
  return ret;
}

int ObOptEstCost::cost_mergejoin(const ObCostMergeJoinInfo& est_cost_info, double& op_cost, double& mj_cost,
    ObIArray<ObExprSelPair>* all_predicate_sel)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(est_cost_info.get_est_sel_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point", K(est_cost_info.get_est_sel_info()));
  } else {
    double left_rows = est_cost_info.left_rows_;
    double left_cost = est_cost_info.left_cost_;
    double left_width = est_cost_info.left_width_;
    double right_rows = est_cost_info.right_rows_;
    double right_cost = est_cost_info.right_cost_;
    double right_width = est_cost_info.right_width_;
    double cart_tuples = left_rows * right_rows;  // tuples of Cartesian product
    double cond_tuples = 0.0;
    double out_tuples = 0.0;
    double cond_selectivity = 0.0;
    double filter_selectivity = 0.0;
    double left_selectivity = 0.0;
    double right_selectivity = 0.0;
    double left_sort_cost = 0.0;
    double right_sort_cost = 0.0;

    if (IS_SEMI_ANTI_JOIN(est_cost_info.join_type_)) {
      if (IS_LEFT_SEMI_ANTI_JOIN(est_cost_info.join_type_)) {
        cart_tuples = left_rows;
      } else {
        cart_tuples = right_rows;
      }
    }

    // selectivity for equal conds
    if (OB_FAIL(ObOptEstSel::calculate_selectivity(*est_cost_info.get_est_sel_info(),
            est_cost_info.equal_join_condition_,
            cond_selectivity,
            all_predicate_sel,
            est_cost_info.join_type_,
            &est_cost_info.left_ids_,
            &est_cost_info.right_ids_,
            left_rows,
            right_rows))) {
      LOG_WARN("failed to calculate selectivity", K(est_cost_info.equal_join_condition_), K(ret));
    } else {
      cond_tuples = cart_tuples * cond_selectivity;
    }

    // selectivity for full conds
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOptEstSel::calculate_selectivity(*est_cost_info.get_est_sel_info(),
              est_cost_info.other_join_condition_,
              filter_selectivity,
              all_predicate_sel,
              est_cost_info.join_type_,
              &est_cost_info.left_ids_,
              &est_cost_info.right_ids_,
              left_rows,
              right_rows))) {
        LOG_WARN("failed to calculate selectivity", K(est_cost_info.other_join_condition_), K(ret));
      } else {
        // estimate out tuples
        out_tuples = cond_tuples * filter_selectivity;
      }
    }
    // estimate used rows of right table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOptEstSel::calc_sel_for_equal_join_cond(*est_cost_info.get_est_sel_info(),
              est_cost_info.equal_join_condition_,
              left_selectivity,
              right_selectivity))) {
        LOG_WARN("failed to calculate selectivity", K(est_cost_info.equal_join_condition_));
      } else {
        // do real calculation
        mj_cost += left_cost + right_cost;

        mj_cost += cost_params_.CPU_TUPLE_COST * (left_rows + right_rows);

        double qual_cost = cost_quals(cond_tuples, est_cost_info.equal_join_condition_) +
                           cost_quals(cond_tuples, est_cost_info.other_join_condition_);
        mj_cost += qual_cost;

        double join_cost = cost_params_.JOIN_PER_ROW_COST * out_tuples;
        mj_cost += join_cost;
        op_cost = mj_cost - left_cost - right_cost;

        if (est_cost_info.left_need_ordering_.count() > 0) {
          double sort_cost = 0.0;
          ObSortCostInfo sort_cost_info(left_rows, left_width, 0, NULL);
          if (OB_FAIL(cost_sort(sort_cost_info, est_cost_info.left_need_ordering_, sort_cost))) {
            LOG_WARN("failed to calc cost", K(ret));
          } else {
            left_sort_cost = sort_cost;
            mj_cost += left_sort_cost;
          }
        } else { /*do nothing*/
        }

        if (OB_SUCC(ret)) {
          if (est_cost_info.right_need_ordering_.count() > 0) {
            double sort_cost = 0;
            ObSortCostInfo sort_cost_info(right_rows, right_width, 0, NULL);
            if (OB_FAIL(cost_sort(sort_cost_info, est_cost_info.right_need_ordering_, sort_cost))) {
              LOG_WARN("failed to calc cost", K(ret));
            } else {
              right_sort_cost = sort_cost;
              mj_cost += right_sort_cost;
            }
          } else { /*do nothing*/
          }

          LOG_TRACE("OPT: [COST MERGE JOIN]",
              K(cond_selectivity),
              K(filter_selectivity),
              K(cart_tuples),
              K(cond_tuples),
              K(out_tuples),
              K(mj_cost),
              K(op_cost),
              K(qual_cost),
              K(left_sort_cost),
              K(right_sort_cost),
              K(left_cost),
              K(right_cost),
              K(join_cost));
        }
      }
    }
  }
  return ret;
}

int ObOptEstCost::cost_hashjoin(const ObCostHashJoinInfo& est_cost_info, double& op_cost, double& hash_cost,
    ObIArray<ObExprSelPair>* all_predicate_sel)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(est_cost_info.get_est_sel_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point", K(est_cost_info.get_est_sel_info()));
  } else {
    double left_rows = est_cost_info.left_rows_;
    double right_rows = est_cost_info.right_rows_;
    double left_width = est_cost_info.left_width_;
    double cond_sel = 0.0;
    double filter_sel = 0.0;
    // number of tuples satisfying join-condition
    double cond_tuples = 0.0;
    // number of tuples satisfying filters, which is also the number of output tuples
    double out_tuples = 0.0;
    if (OB_FAIL(ObOptEstSel::calculate_selectivity(*est_cost_info.get_est_sel_info(),
            est_cost_info.equal_join_condition_,
            cond_sel,
            all_predicate_sel,
            est_cost_info.join_type_,
            &est_cost_info.left_ids_,
            &est_cost_info.right_ids_,
            left_rows,
            right_rows))) {
      LOG_WARN("failed to calculate join selectivity", K(est_cost_info.equal_join_condition_), K(ret));
    } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(*est_cost_info.get_est_sel_info(),
                   est_cost_info.other_join_condition_,
                   filter_sel,
                   all_predicate_sel,
                   est_cost_info.join_type_,
                   &est_cost_info.left_ids_,
                   &est_cost_info.right_ids_,
                   left_rows,
                   right_rows))) {
      LOG_WARN("failed to calculate filter selectivity", K(est_cost_info.other_join_condition_), K(ret));
    } else {
      if (IS_SEMI_ANTI_JOIN(est_cost_info.join_type_)) {
        if (IS_LEFT_SEMI_ANTI_JOIN(est_cost_info.join_type_)) {
          cond_tuples = left_rows * cond_sel;
        } else {
          cond_tuples = right_rows * cond_sel;
        }
      } else {
        cond_tuples = left_rows * right_rows * cond_sel;
      }

      out_tuples = cond_tuples * filter_sel;

      hash_cost += cost_params_.CPU_TUPLE_COST * (left_rows + right_rows);
      double material_cost = cost_material(left_rows, left_width);
      hash_cost += material_cost;
      double build_cost = cost_params_.BUILD_HASH_PER_ROW_COST * left_rows;
      double probe_cost = cost_params_.PROBE_HASH_PER_ROW_COST * right_rows;
      hash_cost += build_cost + probe_cost;
      double hash_calc_cost = cost_hash(left_rows + right_rows, est_cost_info.equal_join_condition_);
      hash_cost += hash_calc_cost;
      double qual_cost = cost_quals(cond_tuples, est_cost_info.equal_join_condition_) +
                         cost_quals(cond_tuples, est_cost_info.other_join_condition_);
      hash_cost += qual_cost;
      double join_cost = cost_params_.JOIN_PER_ROW_COST * out_tuples;
      hash_cost += join_cost;

      op_cost = hash_cost;
      hash_cost += est_cost_info.left_cost_ + est_cost_info.right_cost_;
      LOG_TRACE("OPT: [COST HASH JOIN]",
          K(hash_cost),
          K(material_cost),
          K(qual_cost),
          K(build_cost),
          K(probe_cost),
          K(hash_calc_cost),
          K(join_cost),
          K(left_rows),
          K(right_rows),
          K(cond_tuples),
          K(out_tuples),
          K(op_cost),
          K(left_width));
    }
  }

  return ret;
}

int ObOptEstCost::cost_sort(const ObSortCostInfo& cost_info, const ObIArray<ObRawExpr*>& order_exprs, double& cost)
{
  int ret = OB_SUCCESS;
  if (cost_info.prefix_pos_ > 0 && cost_info.prefix_pos_ < order_exprs.count() &&
      cost_info.get_est_sel_info() != NULL && cost_info.topn_ < 0) {
    if (OB_FAIL(cost_prefix_sort(cost_info, order_exprs, cost))) {  // prefix sort
      LOG_WARN("failed to calc cost", K(ret));
    }
  } else {
    ObSEArray<ObExprResType, 5> types;
    for (int64_t i = 0; OB_SUCC(ret) && i < order_exprs.count(); ++i) {
      const ObRawExpr* expr = order_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("order_exprs expr is null", K(ret));
      } else if (OB_FAIL(types.push_back(expr->get_result_type()))) {
        LOG_WARN("failed to push array", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (cost_info.topn_ >= 0) {  // topn sort
        if (OB_FAIL(cost_topn_sort(cost_info, types, cost))) {
          LOG_WARN("failed to calc topn sort cost", K(ret));
        } else {
          cost += cost_params_.CPU_TUPLE_COST * cost_info.rows_;
        }
      } else {  // normal sort
        if (OB_FAIL(cost_sort(cost_info, types, cost))) {
          LOG_WARN("failed to calc cost", K(ret));
        } else {
          cost += cost_params_.CPU_TUPLE_COST * cost_info.rows_;
        }
      }
    }
  }
  return ret;
}

int ObOptEstCost::cost_sort(const ObSortCostInfo& cost_info, const ObIArray<OrderItem>& order_items, double& cost)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 5> ordering_exprs;
  if (0 == order_items.count()) {
    // order by 'a', no true sort key
    cost = 0;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
      ObRawExpr* expr = order_items.at(i).expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("order_items expr is null", K(ret));
      } else if (OB_FAIL(ordering_exprs.push_back(expr))) {
        LOG_WARN("failed to push array", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cost_sort(cost_info, ordering_exprs, cost))) {
        LOG_WARN("failed to calc cost", K(ret));
      }
    }
  }
  return ret;
}

int ObOptEstCost::cost_sort(
    const ObSortCostInfo& cost_info, const ObIArray<ObExprResType>& order_col_types, double& cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double real_sort_cost = 0.0;
  double material_cost = 0.0;
  double rows = cost_info.rows_;
  double width = cost_info.width_;

  material_cost = cost_material(rows, width) + cost_read_materialized(rows, width);
  if (OB_FAIL(cost_sort_inner(order_col_types, rows, real_sort_cost))) {
    LOG_WARN("failed to calc cost", K(ret));
  } else {
    cost = material_cost + real_sort_cost;
    LOG_TRACE("OPT: [COST SORT]",
        K(cost),
        K(material_cost),
        K(real_sort_cost),
        K(rows),
        K(width),
        K(order_col_types),
        "is_prefix_sort",
        cost_info.prefix_pos_ > 0);
  }
  return ret;
}

int ObOptEstCost::cost_prefix_sort(
    const ObSortCostInfo& cost_info, const ObIArray<ObRawExpr*>& order_exprs, double& cost)
{
  int ret = OB_SUCCESS;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  double num_distinct_rows = rows;
  double cost_per_group = 0.0;
  const ObEstSelInfo* est_sel_info = cost_info.get_est_sel_info();
  if (OB_ISNULL(est_sel_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("est_sel_info should not be null when estimating prefix-sort cost", K(ret));
  } else if (OB_FAIL(ObOptEstSel::calculate_distinct(rows, *est_sel_info, order_exprs, num_distinct_rows))) {
    LOG_WARN("calculate distinct failed", K(ret));
  } else if (cost_info.prefix_pos_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prefix position should be at least 1 in prefix-sort", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 5> ordering_per_group;
    for (int64_t i = cost_info.prefix_pos_; OB_SUCC(ret) && i < order_exprs.count(); ++i) {
      ObRawExpr* expr = order_exprs.at(i);
      if (OB_FAIL(ordering_per_group.push_back(expr))) {
        LOG_WARN("failed to push array", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      double num_rows_per_group = 0;
      if (OB_UNLIKELY(std::fabs(num_distinct_rows) < OB_DOUBLE_EPSINON)) {
        num_rows_per_group = 0;
      } else {
        num_rows_per_group = rows / num_distinct_rows;
      }
      // use normal sort cost
      ObSortCostInfo cost_info_per_group(num_rows_per_group, width, 0, NULL, -1);
      if (OB_FAIL(cost_sort(cost_info_per_group, ordering_per_group, cost_per_group))) {
        LOG_WARN("failed to calc cost", K(ret));
      } else {
        cost = cost_per_group * num_distinct_rows;
        LOG_TRACE("OPT: [COST PREFIX SORT]", K(cost), K(cost_per_group), K(num_distinct_rows));
      }
    }
  }
  return ret;
}

int ObOptEstCost::cost_sort_inner(const ObIArray<ObExprResType>& types, double row_count, double& cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_UNLIKELY(0.0 > row_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", K(row_count), K(ret));
  } else if (row_count < 1.0) {
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

int ObOptEstCost::cost_topn_sort(const ObSortCostInfo& cost_info, const ObIArray<ObExprResType>& types, double& cost)
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
    material_cost = cost_material((topn + rows) / 2, width);
    if (OB_FAIL(cost_topn_sort_inner(types, rows, topn, real_sort_cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else {
      cost = material_cost + real_sort_cost;
      LOG_TRACE("OPT: [COST TOPN SORT]", K(cost), K(material_cost), K(real_sort_cost), K(rows), K(width), K(topn));
    }
  }
  return ret;
}

int ObOptEstCost::cost_topn_sort_inner(const ObIArray<ObExprResType>& types, double rows, double n, double& cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_UNLIKELY(0.0 > rows)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid number of rows", K(rows), K(ret));
  } else if (n < 1.0) {
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

double ObOptEstCost::cost_merge_group(double rows, double res_rows, const ObIArray<ObRawExpr*>& group_columns,
    int64_t agg_col_count, int64_t input_col_count)
{
  UNUSED(input_col_count);
  UNUSED(res_rows);
  double cost = 0.0;
  cost += cost_params_.CPU_TUPLE_COST * rows;
  cost += cost_quals(rows, group_columns);
  cost += cost_params_.PER_AGGR_FUNC_COST * static_cast<double>(agg_col_count) * rows;
  LOG_TRACE("OPT: [COST MERGE GROUP BY]", K(cost), K(agg_col_count), K(input_col_count), K(rows), K(res_rows));
  return cost;
}

double ObOptEstCost::cost_hash_group(
    double rows, double res_rows, const ObIArray<ObRawExpr*>& group_columns, int64_t agg_col_count)
{
  double cost = 0;
  cost += cost_params_.CPU_TUPLE_COST * rows;
  cost += cost_params_.BUILD_HASH_PER_ROW_COST * res_rows;
  cost += cost_params_.PROBE_HASH_PER_ROW_COST * rows;
  cost += cost_hash(rows, group_columns);
  cost += cost_params_.PER_AGGR_FUNC_COST * static_cast<double>(agg_col_count) * rows;
  LOG_TRACE("OPT: [HASH GROUP BY]", K(cost), K(agg_col_count), K(rows), K(res_rows));
  return cost;
}

double ObOptEstCost::cost_scalar_group(double rows, int64_t agg_col_count)
{
  double cost = 0.0;
  cost += cost_params_.CPU_TUPLE_COST * rows;
  cost += cost_params_.PER_AGGR_FUNC_COST * static_cast<double>(agg_col_count) * rows;
  LOG_TRACE("OPT: [SCALAR GROUP BY]", K(cost), K(agg_col_count), K(rows));
  return cost;
}

double ObOptEstCost::cost_merge_distinct(
    const double rows, const double res_rows, const ObIArray<ObRawExpr*>& distinct_columns)
{
  UNUSED(res_rows);
  double cost = 0.0;
  cost += cost_params_.CPU_TUPLE_COST * rows;
  cost += cost_quals(rows, distinct_columns);
  LOG_TRACE("OPT: [COST MERGE DISTINCT]", K(cost), K(rows), K(res_rows));
  return cost;
}

double ObOptEstCost::cost_hash_distinct(
    const double rows, const double res_rows, const ObIArray<ObRawExpr*>& distinct_columns)
{
  double cost = 0.0;
  cost += cost_params_.CPU_TUPLE_COST * rows;
  cost += cost_params_.BUILD_HASH_PER_ROW_COST * res_rows;
  cost += cost_params_.PROBE_HASH_PER_ROW_COST * rows;
  cost += cost_hash(rows, distinct_columns);
  LOG_TRACE("OPT: [COST HASH DISTINCT]", K(cost), K(rows), K(res_rows));
  return cost;
}

double ObOptEstCost::cost_sequence(double rows, double uniq_sequence_cnt)
{
  return cost_params_.CPU_TUPLE_COST * rows + cost_params_.CPU_OPERATOR_COST * uniq_sequence_cnt;
}

double ObOptEstCost::cost_limit(double rows)
{
  return rows * cost_params_.CPU_TUPLE_COST;
}

double ObOptEstCost::cost_read_materialized(double rows, double width)
{
  return rows * (cost_params_.MATERIALIZED_ROW_COST + width * cost_params_.MATERIALIZED_BYTE_READ_COST);
}

double ObOptEstCost::cost_material(const double rows, const double average_row_size)
{
  double cost = cost_params_.MATERIALIZE_PER_BYTE_COST * average_row_size * rows;
  LOG_TRACE("OPT: [COST MATERIAL]", K(cost), K(rows), K(average_row_size));
  return cost;
}

double ObOptEstCost::cost_transmit(const double rows, const double average_row_size)
{
  double cost = cost_params_.NETWORK_PER_BYTE_COST * average_row_size * rows;
  LOG_TRACE("OPT: [COST TRANSMIT]", K(cost), K(rows), K(average_row_size));
  return cost;
}

int64_t ObOptEstCost::revise_ge_1(int64_t num)
{
  return num < 1 ? 1 : num;
}

double ObOptEstCost::revise_ge_1(double num)
{
  return num < 1.0 ? 1.0 : num;
}

int64_t ObOptEstCost::revise_by_sel(int64_t num, double sel)
{
  double res = static_cast<double>(num) * sel;
  res = revise_ge_1(res);
  return static_cast<int64_t>(res);
}

double ObOptEstCost::cost_temp_table(double rows, double width, const ObIArray<ObRawExpr*>& filters)
{
  double op_cost = 0.0;
  op_cost = cost_read_materialized(rows, width) + cost_quals(rows, filters);
  return op_cost;
}

// entry point to estimate table cost
int ObOptEstCost::cost_table(ObCostTableScanInfo& est_cost_info, double& query_range_row_count,
    double& phy_query_range_row_count, double& cost, double& index_back_cost)
{
  int ret = OB_SUCCESS;
  if (is_virtual_table(est_cost_info.ref_table_id_)) {
    if (OB_FAIL(cost_virtual_table(est_cost_info, cost))) {
      LOG_WARN("failed to estimate virtual table cost", K(ret));
    } else {
      index_back_cost = 0;
    }
  } else {
    if (OB_FAIL(cost_normal_table(
            est_cost_info, query_range_row_count, phy_query_range_row_count, cost, index_back_cost))) {
      LOG_WARN("failed to estimate table cost", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

// estimate cost for virtual table
int ObOptEstCost::cost_virtual_table(ObCostTableScanInfo& est_cost_info, double& cost)
{
  int ret = OB_SUCCESS;
  if (0 == est_cost_info.ranges_.count()) {
    cost = VIRTUAL_INDEX_GET_COST * static_cast<double>(OB_EST_DEFAULT_ROW_COUNT);
    LOG_TRACE("OPT:[VT] virtual table without range, use default stat", K(cost));
  } else {
    cost = VIRTUAL_INDEX_GET_COST * static_cast<double>(est_cost_info.ranges_.count());
    // refine the cost if it is not exact match
    if (!est_cost_info.is_unique_) {
      cost *= 100.0;
    }
    LOG_TRACE("OPT:[VT] virtual table with range, init est", K(cost), K(est_cost_info.ranges_.count()));
  }
  return ret;
}

int ObOptEstCost::cost_normal_table(ObCostTableScanInfo& est_cost_info, const double query_range_row_count,
    const double phy_query_range_row_count, double& cost, double& index_back_cost)

{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOptEstCost::cost_table_one_batch(est_cost_info,
          est_cost_info.batch_type_,
          est_cost_info.index_meta_info_.is_index_back_,
          est_cost_info.table_scan_param_.column_ids_.count(),
          query_range_row_count,
          phy_query_range_row_count,
          cost,
          index_back_cost))) {
    LOG_WARN("Failed to estimate cost", K(ret), K(est_cost_info));
  } else {
    LOG_TRACE("OPT:[ESTIMATE FINISH]",
        K(cost),
        K(index_back_cost),
        K(phy_query_range_row_count),
        K(query_range_row_count),
        K(est_cost_info));
  }
  return ret;
}

int ObOptEstCost::estimate_row_count(const obrpc::ObEstPartResElement& result, ObCostTableScanInfo& est_cost_info,
    ObIArray<ObExprSelPair>* all_predicate_sel, double& output_row_count, double& query_range_row_count,
    double& phy_query_range_row_count, double& index_back_row_count)
{
  int ret = OB_SUCCESS;
  output_row_count = 0.0;
  if (is_virtual_table(est_cost_info.ref_table_id_) &&
      !is_oracle_mapping_real_virtual_table(est_cost_info.ref_table_id_)) {
    if (0 == est_cost_info.ranges_.count()) {
      output_row_count = static_cast<double>(OB_EST_DEFAULT_ROW_COUNT);
      LOG_TRACE("OPT:[VT] virtual table without range, use default stat", K(output_row_count));
    } else {
      output_row_count = static_cast<double>(est_cost_info.ranges_.count());
      if (!est_cost_info.is_unique_) {
        output_row_count *= 100.0;
      }
    }
    query_range_row_count = output_row_count;
    phy_query_range_row_count = output_row_count;
    index_back_row_count = 0;
  } else {
    double logical_row_count = 0.0;
    double physical_row_count = 0.0;
    if (OB_FAIL(calculate_filter_selectivity(est_cost_info, all_predicate_sel))) {
      LOG_WARN("failed to calculate filter selectivity", K(ret));
    } else if (OB_FAIL(
                   estimate_partition_range_rowcount(result, est_cost_info, logical_row_count, physical_row_count))) {
      LOG_WARN("failed to get query range row count", K(ret));
    } else {
      // logical/phy row count
      if (!est_cost_info.is_unique_) {
        logical_row_count *= static_cast<double>(est_cost_info.table_meta_info_.part_count_);
        physical_row_count *= static_cast<double>(est_cost_info.table_meta_info_.part_count_);
      }
      logical_row_count = std::max(logical_row_count, 1.0);
      physical_row_count = std::max(physical_row_count, 1.0);
      query_range_row_count = logical_row_count;
      phy_query_range_row_count = physical_row_count;
      // index back row count
      if (est_cost_info.index_meta_info_.is_index_back_) {
        index_back_row_count = logical_row_count * est_cost_info.postfix_filter_sel_;
      }
      // access row count
      output_row_count = query_range_row_count;
      if (est_cost_info.sample_info_.is_row_sample()) {
        output_row_count *= 0.01 * est_cost_info.sample_info_.percent_;
      }
      output_row_count = output_row_count * est_cost_info.postfix_filter_sel_ * est_cost_info.table_filter_sel_;
    }
  }
  LOG_TRACE("OPT:[ESTIMATE ROW COUNT END]",
      K(est_cost_info.ref_table_id_),
      K(query_range_row_count),
      K(phy_query_range_row_count),
      K(index_back_row_count),
      K(output_row_count),
      K(est_cost_info.postfix_filter_sel_),
      K(est_cost_info.table_filter_sel_),
      "is_index_back",
      est_cost_info.index_meta_info_.is_index_back_,
      "part_count",
      est_cost_info.table_meta_info_.part_count_);

  return ret;
}

int ObOptEstCost::estimate_partition_range_rowcount(const obrpc::ObEstPartResElement& result,
    ObCostTableScanInfo& est_cost_info, double& logical_row_count, double& physical_row_count)
{
  int ret = OB_SUCCESS;
  logical_row_count = 0;
  physical_row_count = 0;
  ObSEArray<ObNewRange, 4> get_ranges;
  ObSEArray<ObNewRange, 4> scan_ranges;
  ObArenaAllocator allocator;
  if (OB_FAIL(ObOptimizerUtil::classify_get_scan_ranges(
          est_cost_info.table_scan_param_.key_ranges_, allocator, get_ranges, scan_ranges))) {
    LOG_WARN("failed to classify get scan ranges", K(ret));
  } else {
    logical_row_count = static_cast<double>(get_ranges.count()) * est_cost_info.pushdown_prefix_filter_sel_;
    physical_row_count = logical_row_count;
    if (scan_ranges.count() > 0) {
      double scan_logical_rc = 0.0;
      double scan_physical_rc = 0.0;
      if (OB_FAIL(estimate_partition_scan_batch_rowcount(
              est_cost_info, scan_ranges, result, scan_logical_rc, scan_physical_rc))) {
        LOG_WARN("failed to estimate partition scan batch rowcount", K(ret));
      } else {
        logical_row_count += scan_logical_rc;
        physical_row_count += scan_physical_rc;
        LOG_TRACE("OPT:[EST ROW COUNT]",
            K(get_ranges.count()),
            K(scan_ranges.count()),
            K(scan_logical_rc),
            K(logical_row_count),
            K(scan_physical_rc),
            K(physical_row_count));
      }
    } else {
      est_cost_info.row_est_method_ = RowCountEstMethod::LOCAL_STORAGE;
      LOG_TRACE(
          "OPT:EST ROW COUNT, no scan range]", K(logical_row_count), K(physical_row_count), K(get_ranges.count()));
    }
  }
  if (OB_SUCC(ret)) {
    if (est_cost_info.sample_info_.is_block_sample()) {
      logical_row_count *= 0.01 * est_cost_info.sample_info_.percent_;
      physical_row_count *= 0.01 * est_cost_info.sample_info_.percent_;
    }
  }
  if (OB_SUCC(ret)) {
    if (get_ranges.count() + scan_ranges.count() > 1) {
      if (scan_ranges.count() >= 1) {
        est_cost_info.batch_type_ = ObSimpleBatch::T_MULTI_SCAN;
      } else {
        est_cost_info.batch_type_ = ObSimpleBatch::T_MULTI_GET;
      }
    } else {
      if (scan_ranges.count() == 1) {
        est_cost_info.batch_type_ = ObSimpleBatch::T_SCAN;
      } else {
        est_cost_info.batch_type_ = ObSimpleBatch::T_GET;
      }
    }
  }
  return ret;
}

int ObOptEstCost::estimate_acs_partition_rowcount(
    const ObTableScanParam& table_scan_param, storage::ObPartitionService* partition_service, double& row_count)
{
  int ret = OB_SUCCESS;
  ObSimpleBatch batch;
  SQLScanRange range;
  SQLScanRangeArray range_array;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  ObSEArray<ObNewRange, 4> get_ranges;
  ObSEArray<ObNewRange, 4> scan_ranges;
  ObArenaAllocator allocator;
  if (OB_ISNULL(partition_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(ret));
  } else if (OB_FAIL(partition_service->get_partition(table_scan_param.pkey_, guard))) {
    LOG_WARN("failed to get partition guard", K(ret));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(table_scan_param.pkey_, pg_partition_guard))) {
    LOG_WARN("failed to get pg partition guard", K(ret));
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::classify_get_scan_ranges(
                 table_scan_param.key_ranges_, allocator, get_ranges, scan_ranges))) {
    LOG_WARN("failed to push back classify get scan ranges", K(ret));
  } else if (FALSE_IT(row_count = static_cast<double>(get_ranges.count()))) {
    /*do nothing*/
  } else if (OB_FAIL(ObOptEstCost::construct_scan_range_batch(scan_ranges, batch, range, range_array))) {
    LOG_WARN("failed to construct scan range", K(ret));
  } else if (batch.is_valid()) {
    // get ObBatch to call storage esetimate interface
    ObBatch storage_batch;
    ObSEArray<ObEstRowCountRecord, 64> est_records;

    if (OB_FAIL(ObBatch::get_storage_batch(batch, allocator, storage_batch))) {
      LOG_WARN("Failed to get storage batch", K(ret), K(batch));
    } else {
      int64_t rc_logical = 0;
      int64_t rc_physical = 0;
      if (OB_ISNULL(pg_partition_guard.get_pg_partition()->get_storage())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null point error", K(ret));
      } else if (OB_FAIL(pg_partition_guard.get_pg_partition()->get_storage()->get_batch_rows(
                     table_scan_param, storage_batch, rc_logical, rc_physical, est_records))) {
        LOG_TRACE("OPT:[STORAGE EST FAILED]", "storage_ret", ret);
      } else if (scan_ranges.count() > MAX_STORAGE_RANGE_ESTIMATION_NUM) {
        row_count += static_cast<double>(rc_logical * scan_ranges.count()) /
                     static_cast<double>(MAX_STORAGE_RANGE_ESTIMATION_NUM);
        LOG_TRACE("OPT:[STORAGE EST ROW COUNT, more ranges]",
            K(get_ranges.count()),
            K(scan_ranges.count()),
            K(row_count),
            K(ret));
      } else {
        row_count += static_cast<double>(rc_logical);
        LOG_TRACE("OPT:[STORAGE EST ROW COUNT]", K(get_ranges.count()), K(scan_ranges.count()), K(row_count), K(ret));
      }
    }
  } else {
    LOG_TRACE("OPT:STORAGE EST ROW COUNT, no scan range]", K(row_count), K(get_ranges.count()));
  }
  return ret;
}

int ObOptEstCost::construct_scan_range_batch(
    const ObIArray<ObNewRange>& scan_ranges, ObSimpleBatch& batch, SQLScanRange& range, SQLScanRangeArray& range_array)
{
  int ret = OB_SUCCESS;
  if (scan_ranges.count() < 1) {
    batch.type_ = ObSimpleBatch::T_NONE;
  } else if (scan_ranges.count() == 1) {
    range = scan_ranges.at(0);
    batch.type_ = ObSimpleBatch::T_SCAN;
    batch.range_ = &range;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_ranges.count() && i < MAX_STORAGE_RANGE_ESTIMATION_NUM; i++) {
      if (OB_FAIL(range_array.push_back(scan_ranges.at(i)))) {
        LOG_WARN("failed to push back array", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      batch.type_ = ObSimpleBatch::T_MULTI_SCAN;
      batch.ranges_ = &range_array;
    }
  }
  return ret;
}

int ObOptEstCost::estimate_partition_scan_batch_rowcount(ObCostTableScanInfo& est_cost_info,
    const ObIArray<ObNewRange>& scan_ranges, const obrpc::ObEstPartResElement& result, double& scan_logical_rc,
    double& scan_physical_rc)
{
  int ret = OB_SUCCESS;
  scan_logical_rc = 0.0;
  scan_physical_rc = 0.0;
  if (OB_UNLIKELY(scan_ranges.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan range is empty", K(ret));
  } else if (result.reliable_) {
    // fill storage estimation results
    scan_logical_rc = static_cast<double>(result.logical_row_count_);
    scan_physical_rc = static_cast<double>(result.physical_row_count_);
    if (scan_ranges.count() > MAX_STORAGE_RANGE_ESTIMATION_NUM) {
      scan_logical_rc *=
          static_cast<double>(scan_ranges.count()) / static_cast<double>(MAX_STORAGE_RANGE_ESTIMATION_NUM);
      scan_physical_rc *=
          static_cast<double>(scan_ranges.count()) / static_cast<double>(MAX_STORAGE_RANGE_ESTIMATION_NUM);
    }
  } else if (OB_FAIL(stat_estimate_partition_batch_rowcount(est_cost_info, scan_ranges, scan_logical_rc))) {
    LOG_WARN("failed to estimate rowcount with stat", K(ret));
  } else {
    scan_physical_rc = scan_logical_rc;
  }

  if (OB_SUCC(ret)) {
    scan_logical_rc = scan_logical_rc * est_cost_info.pushdown_prefix_filter_sel_;
    scan_physical_rc = scan_physical_rc * est_cost_info.pushdown_prefix_filter_sel_;
    if (est_cost_info.is_unique_) {
      scan_logical_rc = static_cast<double>(scan_ranges.count());
      scan_physical_rc = static_cast<double>(scan_ranges.count());
    }
    LOG_TRACE("[Estimated partition rowcount]",
        K(est_cost_info.prefix_filter_sel_),
        K(est_cost_info.pushdown_prefix_filter_sel_),
        K(scan_logical_rc),
        K(scan_physical_rc));
  }
  return ret;
}

int ObOptEstCost::storage_estimate_rowcount(const ObPartitionService* part_service, ObStatManager& stat_manager,
    const obrpc::ObEstPartArg& arg, obrpc::ObEstPartRes& res)
{
  int ret = OB_SUCCESS;
  int64_t part_row_count = 0;
  int64_t part_size = 0;
  double avg_row_size = 0.0;
  // est part rowcount and part
  if (!arg.is_valid_table_pkey()) {
    // do nothing
  } else if (OB_FAIL(ObOptEstCost::compute_partition_rowcount_and_size(
                 arg, stat_manager, part_service, part_row_count, part_size, avg_row_size))) {
    LOG_WARN("fail to compute partition rowcount and size", K(arg), K(ret));
    res.part_rowcount_size_res_.reset();
    ret = OB_SUCCESS;
  } else {
    res.part_rowcount_size_res_.row_count_ = part_row_count;
    res.part_rowcount_size_res_.part_size_ = part_size;
    res.part_rowcount_size_res_.avg_row_size_ = avg_row_size;
    res.part_rowcount_size_res_.reliable_ = (part_row_count > 0);
  }

  // est path rows
  if (OB_SUCC(ret)) {
    // init common param
    ObTableScanParam param;
    param.schema_version_ = arg.schema_version_;
    if (OB_UNLIKELY(arg.index_pkeys_.count() != 0 && arg.index_pkeys_.count() != arg.index_params_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN(
          "the number of index pkey is invalid", K(ret), K(arg.index_pkeys_.count()), K(arg.index_params_.count()));
    } else if (OB_FAIL(append(param.column_ids_, arg.column_ids_))) {
      LOG_WARN("failed to append column ids", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_params_.count(); i++) {
      obrpc::ObEstPartResElement est_res;
      param.pkey_ = (arg.index_pkeys_.count() == 0 ? arg.pkey_ : arg.index_pkeys_.at(i));
      param.index_id_ = arg.index_params_.at(i).index_id_;
      param.scan_flag_ = arg.index_params_.at(i).scan_flag_;
      if (OB_FAIL(storage_estimate_rowcount(part_service,
              param,
              arg.index_params_.at(i).batch_,
              arg.index_params_.at(i).range_columns_count_,
              est_res))) {
        LOG_WARN("failed to estimate index row count", K(ret));
      } else if (OB_FAIL(res.index_param_res_.push_back(est_res))) {
        LOG_WARN("failed to push back result", K(ret));
      } else {
        LOG_TRACE("[OPT EST]: row count stat", K(est_res), K(i), K(param));
      }
    }
  }
#if !defined(NDEBUG)
  if (OB_SUCC(ret)) {
    LOG_INFO("[OPT EST] rowcount estimation result", K(arg), K(res));
  }
#endif
  return ret;
}

int ObOptEstCost::compute_partition_rowcount_and_size(const obrpc::ObEstPartArg& arg, ObStatManager& stat_manager,
    const storage::ObPartitionService* part_service, int64_t& logical_row_count, int64_t& part_size,
    double& avg_row_size)
{
  int ret = OB_SUCCESS;
  int64_t sstable_row_count = 0;
  int64_t sstable_part_size = 0;
  double sstable_avg_row_size = 0.0;
  LOG_TRACE("OPT : start compute partition rowcount and size", K(arg));
  if (OB_FAIL(ObOptEstCost::get_sstable_rowcount_and_size(
          stat_manager, arg.partition_keys_, sstable_row_count, sstable_part_size, sstable_avg_row_size))) {
    LOG_WARN("get partition row count failed", K(ret));
  }
  LOG_TRACE(
      "OPT: storage sstable stat  estimation", K(sstable_row_count), K(sstable_part_size), K(sstable_avg_row_size));
  if (OB_SUCC(ret)) {
    if (0 == sstable_row_count) {  // use memtable
      // construct dummy scan param to estimate memtable row count
      storage::ObTableScanParam table_scan_param;
      table_scan_param.pkey_ = arg.pkey_;
      table_scan_param.index_id_ = arg.pkey_.table_id_;
      table_scan_param.scan_flag_.index_back_ = 0;
      table_scan_param.schema_version_ = arg.schema_version_;
      int64_t tmp_phy_row_count = 0;
      if (OB_FAIL(append(table_scan_param.column_ids_, arg.column_ids_))) {
        LOG_WARN("failed to append column ids", K(ret));
      } else if (OB_FAIL(estimate_memtable_row_count(table_scan_param,
                     part_service,
                     arg.scan_param_.range_columns_count_,
                     logical_row_count,
                     tmp_phy_row_count))) {
        LOG_WARN("failed to estimate memtable row count", K(ret));
      } else {
        common::ObTableStat tstat = ObStatManager::get_default_table_stat();
        avg_row_size = static_cast<double>(tstat.get_average_row_size());
        part_size = static_cast<int64_t>(avg_row_size * static_cast<double>(logical_row_count));
        logical_row_count = arg.partition_keys_.count() * logical_row_count;
      }
    } else {  // use sstable
      logical_row_count = sstable_row_count;
      part_size = sstable_part_size;
      avg_row_size = sstable_avg_row_size;
    }
  }
  LOG_TRACE("OPT: after storage stat estimation", K(logical_row_count), K(part_size), K(avg_row_size));

  return ret;
}

int ObOptEstCost::estimate_memtable_row_count(const storage::ObTableScanParam& param,
    const storage::ObPartitionService* part_service, const int64_t rowkey_count, int64_t& logical_row_count,
    int64_t& physical_row_count)
{
  int ret = OB_SUCCESS;
  double tmp_logical_row_count = 0;
  double tmp_physical_row_count = 0;
  ObArenaAllocator allocator;
  ObNewRange* cur_range = NULL;
  if (OB_FAIL(ObSQLUtils::make_whole_range(allocator, param.pkey_.table_id_, rowkey_count, cur_range))) {
    LOG_WARN("failed to make whole range", K(ret));
  } else if (OB_ISNULL(cur_range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null query range", K(ret));
  } else {
    // estimate memtable row count here
    ObSEArray<ObEstRowCountRecord, MAX_SSTABLE_CNT_IN_STORAGE> est_records;
    ObSimpleBatch batch;
    batch.type_ = ObSimpleBatch::T_SCAN;
    batch.range_ = cur_range;
    LOG_TRACE("OPT:[STORAGE START TO ESTIMATE MEMTABLE ROWCOUNT]", K(rowkey_count));
    if (OB_FAIL(ObOptEstCost::storage_estimate_partition_batch_rowcount(param.pkey_,
            batch,
            param,
            rowkey_count,
            part_service,
            est_records,
            tmp_logical_row_count,
            tmp_physical_row_count))) {
      LOG_WARN("fail to get memtable rowcount", K(ret), K(param), K(batch));
    }
  }

  if (OB_FAIL(ret)) {
    logical_row_count = 0;
    physical_row_count = 0;
    ret = OB_SUCCESS;
  } else {
    logical_row_count = static_cast<int64_t>(tmp_logical_row_count);
    physical_row_count = static_cast<int64_t>(tmp_physical_row_count);
  }

  return ret;
}

// estimate scan rowcount
int ObOptEstCost::storage_estimate_rowcount(const ObPartitionService* part_service, const ObTableScanParam& param,
    const ObSimpleBatch& batch, const int64_t range_columns_count, obrpc::ObEstPartResElement& res)
{
  int ret = OB_SUCCESS;
  double rc_logical = 0;
  double rc_physical = 0;
  if (!batch.is_valid()) {
    // do nothing when there is no scan range
    res.logical_row_count_ = static_cast<int64_t>(rc_logical);
    res.physical_row_count_ = static_cast<int64_t>(rc_physical);
    res.reliable_ = true;
  } else if (ObOptEstCost::storage_estimate_partition_batch_rowcount(param.pkey_,
                 batch,
                 param,
                 range_columns_count,
                 part_service,
                 res.est_records_,
                 rc_logical,
                 rc_physical)) {
    LOG_WARN("fail to get partition batch rowcount", K(param.pkey_), K(batch), K(ret));
    res.reset();
    ret = OB_SUCCESS;
  } else {
    res.logical_row_count_ = static_cast<int64_t>(rc_logical);
    res.physical_row_count_ = static_cast<int64_t>(rc_physical);
    res.reliable_ = true;
  }
  LOG_TRACE("[OPT EST]:estimate partition scan batch rowcount", K(res), K(batch), K(ret));
  return ret;
}

int ObOptEstCost::storage_estimate_partition_batch_rowcount(const ObPartitionKey& pkey, const ObSimpleBatch& batch,
    const storage::ObTableScanParam& table_scan_param, const int64_t range_columns_count,
    const storage::ObPartitionService* part_service, ObIArray<ObEstRowCountRecord>& est_records,
    double& logical_row_count, double& physical_row_count)
{
  int ret = OB_SUCCESS;
  int64_t rc_logical = 0;
  int64_t rc_physical = 0;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  ObBatch storage_batch;
  ObArenaAllocator allocator;
  UNUSED(range_columns_count);
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_normal_replica(pkey, part_service, guard)) || OB_ISNULL(guard.get_partition_group())) {
    LOG_WARN("fail to get partition replica", K(ret));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(pkey, pg_partition_guard))) {
    LOG_WARN("Invalid argument of NULL", K(ret), K(pkey));
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition()) ||
             OB_ISNULL(pg_partition_guard.get_pg_partition()->get_storage())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pg partition or partition storage is null, unexpected error",
        K(ret),
        KP(pg_partition_guard.get_pg_partition()),
        K(pkey));
  } else if (OB_FAIL(ObBatch::get_storage_batch(batch, allocator, storage_batch))) {
    LOG_WARN("Failed to get storage batch", K(ret), K(batch));
  } else {
    if (OB_FAIL(pg_partition_guard.get_pg_partition()->get_storage()->get_batch_rows(
            table_scan_param, storage_batch, rc_logical, rc_physical, est_records))) {
      LOG_TRACE("OPT:[STORAGE EST FAILED, USE STAT EST]", "storage_ret", ret);
    } else {
      LOG_TRACE("storage estimate row count result",
          K(rc_logical),
          K(rc_physical),
          K(table_scan_param),
          K(storage_batch),
          K(ret));
      logical_row_count = rc_logical < 0 ? 1.0 : static_cast<double>(rc_logical);
      physical_row_count = rc_physical < 0 ? 1.0 : static_cast<double>(rc_physical);
    }
  }

  return ret;
}

int ObOptEstCost::get_normal_replica(
    const ObPartitionKey& pkey, const storage::ObPartitionService* part_service, ObIPartitionGroupGuard& guard)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(part_service), K(ret));
  } else {
    storage::ObPartitionReplicaState replica_stat = storage::OB_NORMAL_REPLICA;
    // back-up partition may not exist on this server as some server error.
    if (OB_UNLIKELY(OB_FAIL(part_service->get_partition(pkey, guard)))) {
      LOG_TRACE("Failed to get partition", K(ret), K(pkey));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_PARTITION_NOT_EXIST;
      LOG_WARN("Partition not exist", K(ret), K(pkey));
    } else if (OB_UNLIKELY(OB_FAIL(guard.get_partition_group()->get_replica_state(replica_stat)))) {
      LOG_WARN("Get partition replicate state error", K(ret));
    } else if (storage::OB_NORMAL_REPLICA != replica_stat) {
      ret = OB_NO_REPLICA_VALID;
      LOG_WARN("The partition not normal replica", K(replica_stat), K(ret));
    }  // do nothing
  }

  return ret;
}

int ObOptEstCost::get_sstable_rowcount_and_size(ObStatManager& stat_manager, const ObIArray<ObPartitionKey>& part_keys,
    int64_t& row_count, int64_t& part_size, double& avg_row_size)
{
  int ret = OB_SUCCESS;
  // calc partition info using partition key
  row_count = 0;
  part_size = 0;
  avg_row_size = 0;
  int64_t part_count = 0;
  common::ObTableStat tstat;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_keys.count(); ++i) {
    if (OB_FAIL(stat_manager.get_table_stat(part_keys.at(i), tstat))) {
      LOG_WARN("get table stat failed", K(ret));
    } else if (0 == tstat.get_row_count()) {
      // do nothing
    } else {
      ++part_count;
      row_count += tstat.get_row_count();
      part_size += tstat.get_data_size();
      avg_row_size += static_cast<double>(tstat.get_average_row_size());
    }
    LOG_TRACE("get_partition_rc result", K(part_keys.at(i)), K(tstat));
  }
  if (OB_SUCC(ret)) {
    if (0 != part_count) {
      part_size = part_size / part_count;
      avg_row_size = avg_row_size / static_cast<double>(part_count);
    }
    LOG_TRACE("[PRTITION_INFO] get partition sstable rowcount and size",
        K(row_count),
        K(part_size),
        K(avg_row_size),
        K(part_count));
  }

  return ret;
}

int ObOptEstCost::stat_estimate_partition_batch_rowcount(
    const ObCostTableScanInfo& est_cost_info, const ObIArray<ObNewRange>& scan_ranges, double& row_count)
{
  int ret = OB_SUCCESS;
  double temp_row_count = 0;
  row_count = 0;
  if (scan_ranges.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Batch type error", K(ret), K(scan_ranges));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_ranges.count(); i++) {
    if (OB_FAIL(stat_estimate_single_range_rc(est_cost_info, scan_ranges.at(i), temp_row_count))) {
      LOG_WARN("failed to estimate row count", K(est_cost_info), K(scan_ranges.at(i)), K(ret));
    } else {
      row_count += temp_row_count;
      LOG_TRACE("OPT:[STAT EST MULTI SCAN]", K(temp_row_count), K(row_count));
    }
  }
  return ret;
}

int ObOptEstCost::stat_estimate_single_range_rc(
    const ObCostTableScanInfo& est_cost_info, const ObNewRange& range, double& count)
{
  int ret = OB_SUCCESS;
  const ObEstSelInfo* est_sel_info = est_cost_info.get_est_sel_info();
  const ObTableMetaInfo& table_meta_info = est_cost_info.table_meta_info_;
  double range_selectivity = 1.0;
  count = -1;
  if (OB_ISNULL(est_sel_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(est_sel_info), K(ret));
  } else if (0 == table_meta_info.part_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition count is 0", K(table_meta_info.part_count_), K(ret));
  } else if (OB_FAIL(ObOptEstSel::get_single_newrange_selectivity(*est_sel_info,
                 est_cost_info.range_columns_,
                 range,
                 table_meta_info.cost_est_type_,
                 false,
                 range_selectivity))) {
    LOG_WARN("failed to calculate single newrange selectivity", K(est_cost_info), K(ret));
  } else {
    if (range.empty() && fabs(1.0 - est_cost_info.prefix_filter_sel_) < OB_DOUBLE_EPSINON) {
      // defend code: user may enter a query without predicates on index prefix, but with
      // empty range on index postfix(such as when they are experimenting), leading optimizer to
      // think that this index has a very small scan cost. such plan will cause following
      // query with correct ranges to timeout.
      range_selectivity = 1.0;
      LOG_TRACE("OPT:[STAT EST RANGE] range is empty and prefix_filter_sel is 1");
    }
    count = static_cast<double>(table_meta_info.table_row_count_) / static_cast<double>(table_meta_info.part_count_) *
            range_selectivity;
    LOG_TRACE("OPT:[STAT EST RANGE]", K(range), K(range_selectivity), K(count));
  }
  return ret;
}

int ObOptEstCost::calculate_filter_selectivity(
    ObCostTableScanInfo& est_cost_info, ObIArray<ObExprSelPair>* all_predicate_sel)
{
  int ret = OB_SUCCESS;
  const ObEstSelInfo* est_sel_info = est_cost_info.get_est_sel_info();
  if (OB_ISNULL(est_sel_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point error", K(est_sel_info), K(ret));
  } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(
                 *est_sel_info, est_cost_info.prefix_filters_, est_cost_info.prefix_filter_sel_, all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(est_cost_info.postfix_filters_), K(ret));
  } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(*est_sel_info,
                 est_cost_info.pushdown_prefix_filters_,
                 est_cost_info.pushdown_prefix_filter_sel_,
                 all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(est_cost_info.pushdown_prefix_filters_), K(ret));
  } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(*est_sel_info,
                 est_cost_info.postfix_filters_,
                 est_cost_info.postfix_filter_sel_,
                 all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(est_cost_info.postfix_filters_), K(ret));
  } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(
                 *est_sel_info, est_cost_info.table_filters_, est_cost_info.table_filter_sel_, all_predicate_sel))) {
    LOG_WARN("failed to calculate selectivity", K(est_cost_info.table_filters_), K(ret));
  } else {
    LOG_TRACE("table filter info",
        K(est_cost_info.ref_table_id_),
        K(est_cost_info.index_id_),
        K(est_cost_info.prefix_filters_),
        K(est_cost_info.pushdown_prefix_filters_),
        K(est_cost_info.postfix_filters_),
        K(est_cost_info.table_filters_),
        K(est_cost_info.prefix_filter_sel_),
        K(est_cost_info.pushdown_prefix_filter_sel_),
        K(est_cost_info.postfix_filter_sel_),
        K(est_cost_info.table_filter_sel_));
  }
  return ret;
}

int ObOptEstCost::cost_table_one_batch(const ObCostTableScanInfo& est_cost_info, const ObSimpleBatch::ObBatchType& type,
    const bool index_back, const int64_t column_count, const double logical_output_row_count,
    const double physical_output_row_count, double& cost, double& index_back_cost)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(logical_output_row_count < 0.0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(logical_output_row_count), K(ret));
  } else {
    // It's possible to get a query like 'select 1 from t1', whose column count would be 0.
    int64_t min_column_count = (column_count > 1) ? column_count : 1;
    if (ObSimpleBatch::T_GET == type || ObSimpleBatch::T_MULTI_GET == type) {
      if (OB_FAIL(cost_table_get_one_batch(
              est_cost_info, index_back, min_column_count, logical_output_row_count, cost, index_back_cost))) {
        LOG_WARN("Failed to estimate get cost", K(ret));
      }
    } else if (ObSimpleBatch::T_SCAN == type || ObSimpleBatch::T_MULTI_SCAN == type) {
      if (OB_FAIL(cost_table_scan_one_batch(est_cost_info,
              index_back,
              min_column_count,
              logical_output_row_count,
              physical_output_row_count,
              cost,
              index_back_cost))) {
        LOG_WARN("Failed to estimate scan cost", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid batch type", K(ret), K(type));
    }
  }
  return ret;
}

int ObOptEstCost::cost_table_get_one_batch(const ObCostTableScanInfo& est_cost_info, const bool is_index_back,
    const int64_t column_count, const double output_row_count, double& cost, double& index_back_cost)
{
  int ret = OB_SUCCESS;
  const ObTableMetaInfo& table_meta_info = est_cost_info.table_meta_info_;
  if (OB_UNLIKELY(output_row_count < 0) || OB_UNLIKELY(column_count <= 0) ||
      OB_UNLIKELY(est_cost_info.postfix_filter_sel_ < 0) || OB_UNLIKELY(est_cost_info.postfix_filter_sel_ > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(column_count), K(output_row_count), K_(est_cost_info.postfix_filter_sel), K(ret));
  } else {
    double get_cost = 0.0;
    double get_index_back_cost = 0.0;
    if (is_index_back) {
      double base_cost = 0.0;
      double ib_cost = 0.0;
      double network_cost = 0.0;
      if (OB_FAIL(cost_table_get_one_batch_inner(output_row_count,
              static_cast<double>(table_meta_info.table_rowkey_count_),
              est_cost_info,
              false,
              base_cost))) {
        LOG_WARN("failed to calc cost", K(output_row_count), K(ret));
      } else {
        double index_back_row_count = output_row_count;
        // revise number of output row if is row sample scan
        if (est_cost_info.sample_info_.is_row_sample()) {
          index_back_row_count *= 0.01 * est_cost_info.sample_info_.percent_;
        }
        LOG_TRACE("OPT:[COST GET]", K(index_back_row_count));

        index_back_row_count = index_back_row_count * est_cost_info.postfix_filter_sel_;
        if (OB_FAIL(cost_table_get_one_batch_inner(
                index_back_row_count, static_cast<double>(column_count), est_cost_info, true, ib_cost))) {
          LOG_WARN("failed to calc cost", K(index_back_row_count), K(column_count), K(ret));
        } else if (OB_FAIL(cost_table_get_one_batch_network(
                       index_back_row_count, static_cast<double>(column_count), est_cost_info, network_cost))) {
          LOG_WARN("failed to get newwork transform cost for global index", K(ret));
        } else {
          get_cost = base_cost + ib_cost + network_cost;
          get_index_back_cost = ib_cost + network_cost;
          LOG_TRACE("OPT:[COST GET]",
              K(output_row_count),
              K(index_back_row_count),
              K(get_cost),
              K(base_cost),
              K(ib_cost),
              K(network_cost),
              K_(est_cost_info.postfix_filter_sel));
        }
      }
    } else {
      if (OB_FAIL(cost_table_get_one_batch_inner(
              output_row_count, static_cast<double>(column_count), est_cost_info, false, get_cost))) {
        LOG_WARN("failed to calc cost", K(output_row_count), K(column_count), K(ret));
      } else {
        LOG_TRACE("OPT:[COST GET]", K(output_row_count), K(get_cost));
      }
    }
    cost = get_cost;
    index_back_cost = get_index_back_cost;
  }
  return ret;
}

double ObOptEstCost::cost_late_materialization_table_get(int64_t column_cnt)
{
  double op_cost = 0.0;
  double io_cost = cost_params_.MICRO_BLOCK_SEQ_COST;
  double cpu_cost = (cost_params_.CPU_TUPLE_COST + cost_params_.PROJECT_COLUMN_SEQ_COST * column_cnt);
  op_cost = io_cost + cpu_cost;
  return op_cost;
}

void ObOptEstCost::cost_late_materialization_table_join(
    double left_card, double left_cost, double right_card, double right_cost, double& op_cost, double& cost)
{
  op_cost = 0.0;
  cost = 0.0;
  double once_rescan_cost = right_cost + right_card * cost_params_.CPU_TUPLE_COST;
  op_cost += left_card * once_rescan_cost + left_card * cost_params_.JOIN_PER_ROW_COST;
  cost += left_cost + ObOptEstCost::cost_params_.CPU_TUPLE_COST * left_card;
  cost += op_cost;
}

int ObOptEstCost::cost_table_scan_one_batch(const ObCostTableScanInfo& est_cost_info, const bool is_index_back,
    const int64_t column_count, const double logical_output_row_count, const double physical_output_row_count,
    double& cost, double& index_back_cost)
{
  int ret = OB_SUCCESS;
  const ObTableMetaInfo& table_meta_info = est_cost_info.table_meta_info_;
  const ObIndexMetaInfo& index_meta_info = est_cost_info.index_meta_info_;
  if (OB_UNLIKELY(index_meta_info.index_column_count_ <= 0) || OB_UNLIKELY(logical_output_row_count < 0) ||
      OB_UNLIKELY(physical_output_row_count < 0) || OB_UNLIKELY(column_count <= 0) ||
      OB_UNLIKELY(est_cost_info.postfix_filter_sel_ < 0) || OB_UNLIKELY(est_cost_info.postfix_filter_sel_ > 1) ||
      OB_UNLIKELY(table_meta_info.table_rowkey_count_ < 0) || OB_UNLIKELY(table_meta_info.table_row_count_ <= 0) ||
      OB_UNLIKELY(table_meta_info.part_count_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
        K_(index_meta_info.index_column_count),
        K(logical_output_row_count),
        K(column_count),
        K_(est_cost_info.postfix_filter_sel),
        K(table_meta_info.table_rowkey_count_),
        K(table_meta_info.table_row_count_),
        K(table_meta_info.part_count_),
        K(physical_output_row_count),
        K(ret));
  } else {
    double rowkey_count_double = static_cast<double>(table_meta_info.table_rowkey_count_);
    double column_count_double = static_cast<double>(column_count);
    double scan_cost = 0.0;
    double scan_index_back_cost = 0.0;
    if (is_index_back) {
      double base_cost = 0.0;
      double ib_cost = 0.0;
      double network_cost = 0.0;
      if (OB_FAIL(cost_table_scan_one_batch_inner(physical_output_row_count,
              rowkey_count_double,
              TYPICAL_BLOCK_CACHE_HIT_RATE,
              est_cost_info,
              false,
              base_cost))) {
        LOG_WARN("Failed to calc scan scan_cost", K(ret));
      } else {
        double index_back_row_count = logical_output_row_count;
        // revise number of output row if is row sample scan
        if (est_cost_info.sample_info_.is_row_sample()) {
          index_back_row_count *= 0.01 * est_cost_info.sample_info_.percent_;
        }
        LOG_TRACE("OPT:[COST SCAN SIMPLE ROW COUNT]", K(index_back_row_count));
        index_back_row_count = index_back_row_count * est_cost_info.postfix_filter_sel_;
        if (OB_FAIL(cost_table_get_one_batch_inner(
                index_back_row_count, column_count_double, est_cost_info, true, ib_cost))) {
          LOG_WARN("Failed to calc get scan_cost", K(ret));
        } else if (OB_FAIL(cost_table_get_one_batch_network(
                       index_back_row_count, column_count_double, est_cost_info, network_cost))) {
          LOG_WARN("failed to get newwork transform scan_cost for global index", K(ret));
        } else {
          scan_cost = base_cost + ib_cost + network_cost;
          scan_index_back_cost = ib_cost + network_cost;
          LOG_TRACE("OPT:[COST SCAN]",
              K(logical_output_row_count),
              K(index_back_row_count),
              K(scan_cost),
              K(base_cost),
              K(ib_cost),
              K(network_cost),
              "postfix_sel",
              est_cost_info.postfix_filter_sel_);
        }
      }
    } else {
      if (OB_FAIL(cost_table_scan_one_batch_inner(physical_output_row_count,
              column_count_double,
              TYPICAL_BLOCK_CACHE_HIT_RATE,
              est_cost_info,
              false,
              scan_cost))) {
        LOG_WARN("Failed to calc scan cost", K(ret));
      } else {
        LOG_TRACE("OPT:[COST SCAN]", K(logical_output_row_count), K(scan_cost));
      }
    }
    if (OB_SUCC(ret)) {
      cost = scan_cost;
      index_back_cost = scan_index_back_cost;
    }
  }
  return ret;
}

int ObOptEstCost::cost_table_scan_one_batch_inner(double row_count, double column_count, double block_cache_rate,
    const ObCostTableScanInfo& est_cost_info, bool is_index_back, double& cost)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0) || OB_UNLIKELY(column_count < 1) || OB_UNLIKELY(block_cache_rate < 0) ||
      OB_UNLIKELY(block_cache_rate > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(row_count), K(column_count), K(block_cache_rate), K(ret));
  } else {
    const ObIndexMetaInfo& index_meta_info = est_cost_info.index_meta_info_;
    const ObTableMetaInfo& table_meta_info = est_cost_info.table_meta_info_;
    double num_micro_blocks = static_cast<double>(table_meta_info.part_count_) * index_meta_info.index_part_size_ /
                              static_cast<double>(index_meta_info.index_micro_block_size_);
    double num_micro_blocks_read = 0;
    if (OB_LIKELY(table_meta_info.table_row_count_ > 0)) {
      num_micro_blocks_read =
          std::ceil(num_micro_blocks * row_count / static_cast<double>(table_meta_info.table_row_count_));
    }
    if (est_cost_info.sample_info_.is_row_sample()) {
      row_count *= 0.01 * est_cost_info.sample_info_.percent_;
    }
    double io_cost = cost_params_.MICRO_BLOCK_SEQ_COST * num_micro_blocks_read;

    double qual_cost = 0.0;
    if (est_cost_info.index_meta_info_.index_id_ == est_cost_info.index_meta_info_.ref_table_id_) {
      qual_cost +=
          cost_quals(row_count, est_cost_info.postfix_filters_) + cost_quals(row_count, est_cost_info.table_filters_);
    } else {
      if (is_index_back) {
        qual_cost += cost_quals(row_count, est_cost_info.table_filters_);
      } else {
        qual_cost += cost_quals(row_count, est_cost_info.postfix_filters_);
      }
    }
    double project_cost = (cost_params_.PROJECT_COLUMN_SEQ_COST * column_count) * row_count;
    double cpu_cost = row_count * cost_params_.CPU_TUPLE_COST + project_cost + qual_cost;
    double memtable_cost = 0;
    double memtable_merge_cost = 0;
    cost = io_cost + cpu_cost + memtable_cost + memtable_merge_cost;
    LOG_TRACE("OPT:[COST TABLE SCAN INNER]",
        K(cost),
        K(io_cost),
        K(cpu_cost),
        K(memtable_cost),
        K(memtable_merge_cost),
        K(qual_cost),
        K(project_cost),
        K(num_micro_blocks_read),
        K(row_count),
        K(column_count));
  }
  return ret;
}

/*
 * estimate the network transform and rpc cost for global index,
 * so far, this cost model should be revised by banliu
 */
int ObOptEstCost::cost_table_get_one_batch_network(
    double row_count, double column_count, const ObCostTableScanInfo& est_cost_info, double& cost)
{
  int ret = OB_SUCCESS;
  const ObTableMetaInfo& table_meta_info = est_cost_info.table_meta_info_;
  cost = 0.0;
  if (OB_UNLIKELY(table_meta_info.table_column_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table column count should not be 0", K(table_meta_info.table_column_count_), K(ret));
  } else if (est_cost_info.index_meta_info_.is_global_index_) {
    double transform_size = (table_meta_info.average_row_size_ * row_count * column_count) /
                            static_cast<double>(table_meta_info.table_column_count_);
    cost = transform_size * ObOptEstCost::ObCostParams::DEFAULT_NETWORK_PER_BYTE_COST;
  } else { /*do nothing*/
  }
  LOG_TRACE("OPT::[COST_TABLE_GET_NETWORK]", K(cost), K(ret));
  return ret;
}

int ObOptEstCost::cost_table_get_one_batch_inner(
    double row_count, double column_count, const ObCostTableScanInfo& est_cost_info, bool is_index_back, double& cost)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0 || column_count < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(row_count), K(column_count), K(ret));
  } else {
    const ObIndexMetaInfo& index_meta_info = est_cost_info.index_meta_info_;
    const ObTableMetaInfo& table_meta_info = est_cost_info.table_meta_info_;

    double num_micro_blocks = static_cast<double>(table_meta_info.part_count_) * index_meta_info.index_part_size_ /
                              static_cast<double>(index_meta_info.index_micro_block_size_);
    double num_micro_blocks_read = 0;
    if (OB_LIKELY(table_meta_info.table_row_count_ > 0)) {
      num_micro_blocks_read =
          std::ceil(num_micro_blocks * row_count / static_cast<double>(table_meta_info.table_row_count_));
    }

    double io_cost = cost_params_.MICRO_BLOCK_RND_COST * num_micro_blocks_read;
    double fetch_row_cost = cost_params_.FETCH_ROW_RND_COST * row_count;
    io_cost += fetch_row_cost;

    if (est_cost_info.sample_info_.is_row_sample()) {
      row_count *= 0.01 * est_cost_info.sample_info_.percent_;
    }
    double qual_cost = 0.0;
    if (est_cost_info.index_meta_info_.index_id_ == est_cost_info.index_meta_info_.ref_table_id_) {
      qual_cost +=
          cost_quals(row_count, est_cost_info.postfix_filters_) + cost_quals(row_count, est_cost_info.table_filters_);
    } else {
      if (is_index_back) {
        qual_cost += cost_quals(row_count, est_cost_info.table_filters_);
      } else {
        qual_cost += cost_quals(row_count, est_cost_info.postfix_filters_);
      }
    }
    double cpu_cost =
        (cost_params_.CPU_TUPLE_COST + cost_params_.PROJECT_COLUMN_RND_COST * column_count) * row_count + qual_cost;
    double memtable_cost = 0;
    double memtable_merge_cost = 0;
    cost = io_cost + cpu_cost + memtable_cost + memtable_merge_cost;
    LOG_TRACE("OPT:[COST TABLE GET INNER]",
        K(cost),
        K(io_cost),
        K(cpu_cost),
        K(fetch_row_cost),
        K(qual_cost),
        K(memtable_cost),
        K(memtable_merge_cost),
        K(num_micro_blocks_read),
        K(row_count),
        K(column_count));
  }
  return ret;
}

int ObOptEstCost::estimate_width_for_columns(const ObIArray<ObRawExpr*>& columns, double& width)
{
  int ret = OB_SUCCESS;
  width = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObRawExpr* column = columns.at(i);
    if (OB_ISNULL(column)) {
      // skip
    } else {
      const ObExprResType& type = column->get_result_type();
      if (type.is_integer_type()) {
        // int
        width += 4;
      } else if (type.get_accuracy().get_length() > 0) {
        int64_t string_width = type.get_accuracy().get_length() / 2;
        width += static_cast<double>(std::min(string_width, cost_params_.MAX_STRING_WIDTH));
      } else if (type.get_accuracy().get_precision() > 0) {
        // number, time
        width += type.get_accuracy().get_precision() / 2;
      } else {
        // default for DEFAULT PK
        width += sizeof(uint64_t);
      }
    }
  }
  // set minimal width as size of integer
  width = std::max(width, 4.0);

  LOG_TRACE("estimate width for table", K(columns), K(width));

  return ret;
}

// FIXME () seems that width of dummy columns is added as well (i.e. ref_count = 0)
int ObOptEstCost::estimate_width_for_table(const ObIArray<ColumnItem>& columns, int64_t table_id, double& width)
{
  int ret = OB_SUCCESS;
  width = 0;
  ObArray<ObRawExpr*> column_exprs;
  for (int i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ColumnItem& column_item = columns.at(i);
    if (OB_UNLIKELY(column_item.get_column_type() == NULL || column_item.table_id_ != table_id)) {
      // skip
    } else if (OB_FAIL(column_exprs.push_back(column_item.expr_))) {
      LOG_WARN("push back column expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(estimate_width_for_columns(column_exprs, width))) {
      LOG_WARN("estimate width for tables failed", K(ret));
    }
  }
  return ret;
}

int ObOptEstCost::get_sort_cmp_cost(const common::ObIArray<sql::ObExprResType>& types, double& cost)
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
        double cost_for_col = comparison_params_[tc];
        if (OB_UNLIKELY(cost_for_col < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not supported type class", K(tc), K(ret));
        } else {
          cost_ret += cost_for_col * factor;
          factor /= static_cast<double>(EST_DEF_COL_NUM_DISTINCT);
        }
      }
    }
    if (OB_SUCC(ret)) {
      cost = cost_ret;
    }
  }
  return ret;
}

// to be revised
double ObOptEstCost::cost_count(double rows)
{
  return rows * cost_params_.CPU_TUPLE_COST;
}

int ObOptEstCost::cost_window_function(double rows, double& cost)
{
  int ret = OB_SUCCESS;
  cost = rows * cost_params_.PER_AGGR_FUNC_COST;
  cost += rows * cost_params_.CPU_TUPLE_COST;
  return ret;
}

double ObOptEstCost::cost_subplan_scan(double rows, int64_t num_filters)
{
  return rows * cost_params_.CPU_TUPLE_COST + cost_quals(rows, num_filters);
}

int ObOptEstCost::cost_merge_set(const ObCostMergeSetInfo& info, double& rows, double& op_cost, double& cost)
{
  int ret = OB_SUCCESS;
  double sum_rows = 0;
  double sum_cost = 0;
  for (int64_t i = 0; i < info.children_.count(); ++i) {
    sum_rows += info.children_.at(i).rows_;
    sum_cost += info.children_.at(i).cost_;
  }
  if (ObSelectStmt::UNION == info.op_) {
    rows = sum_rows;
  } else if (2 != info.children_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected set op child num", K(ret), K(info.op_));
  } else if (ObSelectStmt::INTERSECT == info.op_) {
    rows = std::min(info.children_.at(0).rows_, info.children_.at(1).rows_);
  } else if (ObSelectStmt::EXCEPT == info.op_) {
    rows = info.children_.at(0).rows_;
  }
  op_cost = 0.0;
  op_cost += sum_rows * cost_params_.CPU_TUPLE_COST;
  op_cost += cost_params_.CPU_OPERATOR_COST * static_cast<double>(info.num_select_items_) * sum_rows +
             cost_params_.CPU_TUPLE_COST * rows;
  cost = sum_cost + op_cost;
  LOG_TRACE("OPT: [COST MERGE SET]", K(cost), K(op_cost), K(rows));
  return ret;
}

int ObOptEstCost::cost_hash_set(const ObCostHashSetInfo& info, double& rows, double& op_cost, double& cost)
{
  int ret = OB_SUCCESS;
  if (ObSelectStmt::UNION == info.op_) {
    rows = info.left_rows_ + info.right_rows_;
  } else if (ObSelectStmt::INTERSECT == info.op_) {
    rows = std::min(info.left_rows_, info.right_rows_);
  } else if (ObSelectStmt::EXCEPT == info.op_) {
    rows = info.left_rows_;
  }

  op_cost = 0.0;
  cost = 0.0;
  op_cost += cost_params_.CPU_TUPLE_COST * (info.left_rows_ + info.right_rows_);
  if (ObSelectStmt::UNION == info.op_) {
    // build cost, probe cost
    op_cost += cost_params_.BUILD_HASH_PER_ROW_COST * rows;
    op_cost += cost_params_.PROBE_HASH_PER_ROW_COST * (info.left_rows_ + info.right_rows_);
  } else {
    op_cost += cost_params_.BUILD_HASH_PER_ROW_COST * (info.right_rows_ + rows);
    op_cost += cost_params_.PROBE_HASH_PER_ROW_COST * info.left_rows_;
  }

  op_cost += cost_hash(info.left_rows_ + info.right_rows_, info.hash_columns_);

  cost = info.left_cost_ + info.right_cost_ + op_cost;
  LOG_TRACE("OPT: [COST HASH SET]", K(cost), K(op_cost), K(rows));
  return ret;
}

double ObOptEstCost::cost_hash(double rows, const ObIArray<ObRawExpr*>& hash_exprs)
{
  double cost_per_row = 0.0;
  for (int64_t i = 0; i < hash_exprs.count(); ++i) {
    const ObRawExpr* expr = hash_exprs.at(i);
    if (OB_ISNULL(expr)) {
      LOG_WARN("qual should not be NULL, but we don't set error return code here, just skip it");
    } else {
      ObObjTypeClass calc_type = expr->get_result_type().get_calc_type_class();
      if (OB_UNLIKELY(hash_params_[calc_type] < 0)) {
        LOG_WARN("hash type not supported, skipped", K(calc_type));
      } else {
        cost_per_row += hash_params_[calc_type];
      }
    }
  }
  return rows * cost_per_row;
}

double ObOptEstCost::cost_hash(double rows, int64_t num_hash_columns)
{
  return cost_params_.HASH_DEFAULT_COST * static_cast<double>(num_hash_columns) * rows;
}

double ObOptEstCost::cost_quals(double rows, const ObIArray<ObRawExpr*>& quals)
{
  double cost_per_row = 0.0;
  for (int64_t i = 0; i < quals.count(); ++i) {
    const ObRawExpr* qual = quals.at(i);
    if (OB_ISNULL(qual)) {
      LOG_WARN("qual should not be NULL, but we don't set error return code here, just skip it");
    } else {
      ObObjTypeClass calc_type = qual->get_result_type().get_calc_type_class();
      if (OB_UNLIKELY(comparison_params_[calc_type] < 0)) {
        LOG_WARN("comparison type not supported, skipped", K(calc_type));
      } else {
        cost_per_row += comparison_params_[calc_type];
      }
    }
  }
  return rows * cost_per_row;
}

double ObOptEstCost::cost_quals(double rows, int64_t num_quals)
{
  return cost_params_.CMP_DEFAULT_COST * static_cast<double>(num_quals) * rows;
}
