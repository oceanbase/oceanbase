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
#include "sql/optimizer/ob_log_table_scan.h"

#include "lib/container/ob_se_array_iterator.h"
#include "storage/ob_partition_service.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/code_generator/ob_code_generator_impl.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using schema::ObSchemaGetterGuard;
using schema::ObTableSchema;

int ObLogTableScan::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogTableScan* table_scan = NULL;

  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogTableScan", K(ret));
  } else if (OB_ISNULL(table_scan = static_cast<ObLogTableScan*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOpertor* to ObLogTableScan*", K(ret));
  } else if (OB_FAIL(table_scan->set_range_columns(get_range_columns()))) {
    LOG_WARN("failed to set_range_columns", K(ret));
  } else if (OB_FAIL(table_scan->parts_sortkey_.assign(parts_sortkey_))) {
    LOG_WARN("Failed to assign parts sortkey", K(ret));
  } else if (OB_FAIL(table_scan->access_exprs_.assign(access_exprs_))) {
    LOG_WARN("failed to assign access_exprs_", K(ret));
  } else if (OB_FAIL(table_scan->idx_columns_.assign(idx_columns_))) {
    LOG_WARN("Failed to assign idx columns", K(ret));
  } else if (OB_FAIL(table_scan->filter_before_index_back_.assign(filter_before_index_back_))) {
    LOG_WARN("failed to assign filter_before_index_back_", K(ret));
  } else if (OB_FAIL(table_scan->ranges_.assign(ranges_))) {
    LOG_WARN("failed to assign ranges_", K(ret));
  } else if (OB_FAIL(table_scan->set_est_row_count_record(est_records_))) {
    LOG_WARN("failed to set est row count records", K(ret));
  } else if (OB_FAIL(table_scan->set_query_range_exprs(range_conds_))) {
    LOG_WARN("failed to set query range exprs", K(ret));
  } else if (OB_FAIL(table_scan->set_extra_access_exprs_for_lob_col(extra_access_exprs_for_lob_col_))) {
    LOG_WARN("failed to set extra access exprs", K(ret));
  } else {
    table_scan->set_part_expr(get_part_expr());
    table_scan->set_subpart_expr(get_subpart_expr());
    table_scan->set_est_cost_info(est_cost_info_);
    table_scan->set_table_id(get_table_id());
    table_scan->set_ref_table_id(get_ref_table_id());
    table_scan->set_index_table_id(get_index_table_id());
    table_scan->set_is_index_global(get_is_index_global());
    table_scan->set_is_global_index_back(get_is_global_index_back());
    table_scan->get_table_name().assign_ptr(table_name_.ptr(), table_name_.length());
    table_scan->set_index_name(index_name_);
    table_scan->set_scan_direction(get_scan_direction());
    table_scan->set_for_update(is_for_update(), get_for_update_wait_us());
    table_scan->set_sample_info(get_sample_info());

    table_scan->expected_part_id_ = expected_part_id_;
    table_scan->part_filter_ = part_filter_;
    table_scan->gi_charged_ = gi_charged_;
    table_scan->gi_alloc_post_state_forbidden_ = gi_alloc_post_state_forbidden_;
    // filtering exprs are generated during join order generation, we must carry
    // it over to the new node
    table_scan->hint_ = hint_;
    table_scan->exist_hint_ = exist_hint_;
    table_scan->set_pre_query_range(get_pre_query_range());
    table_scan->part_hint_ = part_hint_;
    table_scan->index_back_ = index_back_;
    table_scan->table_partition_info_ = table_partition_info_;
    table_scan->set_limit_offset(limit_count_expr_, limit_offset_expr_);
    table_scan->set_is_fake_cte_table(is_fake_cte_table_);

    table_scan->set_table_row_count(table_row_count_);
    table_scan->set_output_row_count(output_row_count_);
    table_scan->set_phy_query_range_row_count(phy_query_range_row_count_);
    table_scan->set_query_range_row_count(query_range_row_count_);
    table_scan->set_index_back_row_count(index_back_row_count_);
    table_scan->set_estimate_method(estimate_method_);
    table_scan->set_table_opt_info(table_opt_info_);
    table_scan->set_index_back_cost(index_back_cost_);
    table_scan->set_is_global_index_back(is_global_index_back_);
    table_scan->set_is_index_global(is_index_global_);
    table_scan->set_diverse_path_count(diverse_path_count_);

    out = table_scan;
  }
  return ret;
}

const char* ObLogTableScan::get_name() const
{
  bool is_get = false;
  const char* name = NULL;
  int ret = OB_SUCCESS;
  if (NULL != pre_query_range_ && OB_FAIL(get_pre_query_range()->is_get(is_get))) {
    // is_get always return true
    LOG_WARN("failed to get is_get");
    is_get = false;
  }
  name = is_get ? "TABLE GET" : "TABLE SCAN";
  SampleInfo::SampleMethod sample_method = get_sample_info().method_;
  if (sample_method != SampleInfo::NO_SAMPLE) {
    name = (sample_method == SampleInfo::ROW_SAMPLE) ? "ROW SAMPLE SCAN" : "BLOCK SAMPLE SCAN";
  }
  return name;
}

int ObLogTableScan::transmit_local_ordering()
{
  int ret = OB_SUCCESS;
  // The op_ordering of table scan has be set
  reset_local_ordering();
  if (OB_FAIL(set_local_ordering(get_op_ordering()))) {
    LOG_WARN("fail to set local ordering", K(ret));
  }
  return ret;
}

int ObLogTableScan::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = get_child(first_child);
  if (OB_NOT_NULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan has no child", K(ret));
  } else if (OB_FAIL(transmit_local_ordering())) {
    LOG_WARN("failed to set op local ordering", K(ret));
  }
  return ret;
}

int ObLogTableScan::set_range_columns(const ObIArray<ColumnItem>& range_columns)
{
  int ret = OB_SUCCESS;
  range_columns_.reset();
  if (OB_FAIL(range_columns_.assign(range_columns))) {
    LOG_WARN("failed to assign range columns", K(ret));
  } else if (!is_virtual_table(ref_table_id_)) {
    OrderItem item;
    int64_t N = range_columns.count();
    reset_op_ordering();
    common::ObIArray<OrderItem>& op_ordering = get_op_ordering();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      item.expr_ = range_columns.at(i).expr_;
      item.order_type_ = scan_direction_;
      if (OB_FAIL(op_ordering.push_back(item))) {
        LOG_WARN("failed to push back item", K(ret));
      }
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogTableScan::set_update_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_plan() or get_stmt() returns NULL");
  } else if (!is_index_global_) {
    if (share::is_oracle_mode() && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260) {
      // do nothing
    } else {
      ObDMLStmt* stmt = static_cast<ObDMLStmt*>(get_plan()->get_stmt());
      const TableItem* table_item = stmt->get_table_item_by_id(get_table_id());
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to get table item", K(ret));
      } else {
        for_update_ = table_item->for_update_;
        for_update_wait_us_ = table_item->for_update_wait_us_;
      }
    }
  } else {
  }  // do nothing
  return ret;
}

int ObLogTableScan::compute_property(Path* path)
{
  int ret = OB_SUCCESS;
  const ObJoinOrder* join_order = NULL;

  if (OB_ISNULL(path) || path->path_type_ != ACCESS || OB_ISNULL(join_order = path->parent_) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path is invalid", K(ret), K(path), K(join_order), K(get_plan()));
  } else if (OB_FAIL(ObLogicalOperator::compute_property(path))) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    // process cost, card and width
    AccessPath* ap = static_cast<AccessPath*>(path);
    bool is_index_back = ap->est_cost_info_.index_meta_info_.is_index_back_;
    if (ap->is_global_index_ && is_index_back) {
      set_cost(ap->cost_ - ap->index_back_cost_);
      set_op_cost(get_cost());
      set_card(index_back_row_count_);
    } else if (ap->is_cte_path()) {
      set_card(1);
      set_cost(1);
    } else {
      set_cost(ap->cost_);
      set_op_cost(ap->op_cost_);
      set_index_back_cost(ap->index_back_cost_);
      if (path->is_inner_path()) {
        set_card(path->inner_row_count_);
      } else {
        set_card(path->parent_->get_output_rows());
      }
    }
    set_width(join_order->get_average_output_row_size());
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("table scan:", K(get_card()), K(get_width()), K(get_cost()));
  }
  return ret;
}

int ObLogTableScan::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  UNUSED(parent);
  re_est = false;
  bool need_all = false;
  double need_count = 0.0;
  if (OB_UNLIKELY(need_row_count < 0.0) || OB_ISNULL(est_cost_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Need row count is less than 0", K(ret), K(need_row_count), K(est_cost_info_));
  } else if (need_row_count >= get_card()) {
    need_all = true;
  } else if (OB_UNLIKELY(est_cost_info_->table_filter_sel_ <= 0.0)) {
    need_all = true;
  } else if (is_virtual_table(ref_table_id_)) {
    need_count = static_cast<double>(need_row_count) / est_cost_info_->table_filter_sel_;
  } else {
    need_count = static_cast<double>(need_row_count) / est_cost_info_->table_filter_sel_;
    if (OB_UNLIKELY(est_cost_info_->postfix_filter_sel_ <= 0.0)) {
      need_all = true;
    } else {
      need_count = static_cast<double>(need_count) / est_cost_info_->postfix_filter_sel_;
    }
  }
  if (get_sample_info().is_row_sample() && get_sample_info().percent_ > 0.0) {
    need_count = static_cast<double>(need_count) / get_sample_info().percent_;
  }
  if (need_count < 0.0) {  // too big, overflow
    need_all = true;
  }
  if (!need_all && query_range_row_count_ >= OB_DOUBLE_EPSINON) {
    re_est = true;
    double cost = 0;
    double index_back_cost = 0;
    double physical_need_count = need_count * phy_query_range_row_count_ / query_range_row_count_;
    if (is_virtual_table(ref_table_id_)) {
      cost = VIRTUAL_INDEX_GET_COST * need_count;
    } else if (OB_FAIL(ObOptEstCost::cost_table_one_batch(*est_cost_info_,
                   est_cost_info_->batch_type_,
                   is_index_global_ ? false : index_back_,
                   est_cost_info_->table_scan_param_.column_ids_.count(),
                   need_count,
                   physical_need_count,
                   cost,
                   index_back_cost))) {
      LOG_WARN("failed to estimate cost", K(ret));
    } else {
    }  // do nothing

    if (OB_SUCC(ret)) {
      cost_ = cost;
      card_ = need_row_count;
      index_back_cost_ = index_back_cost;
      LOG_TRACE("succeed to do re-estimation",
          K_(cost),
          K_(index_back_cost),
          K_(card),
          K(need_count),
          K(physical_need_count),
          K(need_row_count),
          K_(phy_query_range_row_count),
          K_(query_range_row_count),
          K_(est_cost_info));
    }
  }
  return ret;
}

int ObLogTableScan::get_index_cost(int64_t column_count, bool index_back, double& cost)
{
  int ret = OB_SUCCESS;
  double logical_row_count = query_range_row_count_;
  double physical_row_count = phy_query_range_row_count_;
  double index_back_cost = 0;
  if (is_virtual_table(ref_table_id_)) {
    cost = VIRTUAL_INDEX_GET_COST * static_cast<double>(logical_row_count);
  } else if (OB_ISNULL(est_cost_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("est cost info is null", K(ret), K(est_cost_info_));
  } else if (OB_FAIL(ObOptEstCost::cost_table_one_batch(*est_cost_info_,
                 est_cost_info_->batch_type_,
                 index_back,
                 column_count,
                 logical_row_count,
                 physical_row_count,
                 cost,
                 index_back_cost))) {
    LOG_WARN("failed to estimate cost", K(ret));
  } else {
    LOG_TRACE("succeed to estimate get index cost", K(cost), K(query_range_row_count_), K(phy_query_range_row_count_));
  }
  return ret;
}

int ObLogTableScan::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
      LOG_WARN("failed to allocate expr pre", K(ret));
    }
  }
  ObIArray<ExprProducer>& exprs = ctx.expr_producers_;
  bool found = false;
  for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret) && !found; i++) {
    if (T_PDML_PARTITION_ID == exprs.at(i).expr_->get_expr_type()) {
      if (exprs.at(i).producer_id_ == OB_INVALID_ID) {
        ObPseudoColumnRawExpr* pseudo_expr = (ObPseudoColumnRawExpr*)(exprs.at(i).expr_);
        if (pseudo_expr->get_table_id() == get_table_id()) {
          LOG_TRACE("find pdml partition id expr producer", K(get_table_id()), K(*pseudo_expr));
          exprs.at(i).producer_id_ = id_;
          found = true;
        } else {
          LOG_TRACE("the table id of partition id expr is not equal to table id",
              K(get_table_id()),
              K(pseudo_expr->get_table_id()));
        }
      } else {
        LOG_TRACE("pdml partition id expr has be produced", K(id_), K(exprs.at(i)));
      }
    }
  }

  return ret;
}

int ObLogTableScan::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> temp_access_exprs;
  if (is_for_update() || (is_index_global_ && is_global_index_back_)) {
    if (OB_FAIL(generate_rowkey_and_partkey_exprs(temp_access_exprs))) {
      LOG_WARN("failed to generate rowkey access expr", K(ret));
    } else if (OB_FAIL(append(access_exprs_, temp_access_exprs))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (!is_for_update() && is_index_global_ && is_global_index_back_) {
      if (OB_FAIL(append(output_exprs_, temp_access_exprs))) {
        LOG_WARN("failed to append output exprs", K(ret));
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }

  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(generate_access_exprs(temp_access_exprs))) {
    LOG_WARN("failed to generate access expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_access_exprs.count(); i++) {
      bool expr_is_required = false;
      ObRawExpr* expr = temp_access_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs_, expr))) {
        LOG_WARN("failed to add expr", K(ret));
      } else if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx, expr_is_required))) {
        LOG_WARN("failed to mark expr as produced", K(*expr), K(branch_id_), K(id_), K(ret));
      } else {
        if (!is_plan_root() && !(!is_for_update() && is_index_global_ && is_global_index_back_)) {
          if (OB_FAIL(add_var_to_array_no_dup(output_exprs_, expr))) {
            LOG_PRINT_EXPR(WARN, "failed to add expr to output_exprs_", expr, K(ret));
          } else {
            LOG_PRINT_EXPR(DEBUG, "succ to add expr to output_exprs_", expr, K(ret));
          }
        } else {
          LOG_PRINT_EXPR(DEBUG, "expr is produced but no one has requested it", expr, K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_add_extra_access_exprs_for_lob_col())) {
      LOG_WARN("failed to add extra access exprs", K(ret));
    }
  }

  // check if we can produce some more exprs, such as 1 + 'c1' after we have produced 'c1'
  if (OB_SUCC(ret)) {
    if (!(is_index_global_ && is_global_index_back_)) {
      if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
        LOG_WARN("failed to allocate_expr_post", K(ret));
      }
    } else {
      if (OB_FAIL(ObLogicalOperator::append_not_produced_exprs(ctx.not_produced_exprs_))) {
        LOG_WARN("fail to append not produced exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObLogTableScan::generate_access_exprs(ObIArray<ObRawExpr*>& access_exprs)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point", K(ret));
  } else if (!is_for_update() && is_index_global_ && is_global_index_back_) {
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filter_exprs_, access_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else { /*do nothing*/
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); i++) {
      const ColumnItem* col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_item is null", K(i), K(ret));
      } else if (OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (col_item->table_id_ == table_id_ && col_item->expr_->is_explicited_reference()) {
        if (OB_FAIL(access_exprs.push_back(col_item->expr_))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObRawExpr* part_filter = NULL;
    if (OB_FAIL(generate_part_filter(part_filter))) {
      LOG_WARN("fail to generate table location expr", K(ret));
    } else if (NULL == part_filter) {
      // do nothing
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_filter, access_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(get_filter_exprs().push_back(part_filter))) {
      LOG_WARN("fail to push part filter", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_pseudo_column_like_exprs().count(); i++) {
    ObRawExpr* expr = stmt->get_pseudo_column_like_exprs().at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (T_ORA_ROWSCN == expr->get_expr_type() &&
               static_cast<ObPseudoColumnRawExpr*>(expr)->get_table_id() == table_id_) {
      OZ(access_exprs.push_back(expr));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(add_mapping_columns_for_vt(access_exprs))) {
    LOG_WARN("failed to add mapping columns for virtual table", K(ret));
  }
  LOG_TRACE("generated access exprs", K(is_global_index_back_), K(index_back_), K(access_exprs));
  return ret;
}

// eg: calc_partition_id(part_expr, subpart_expr) == expected_part_id
int ObLogTableScan::generate_part_filter(ObRawExpr*& part_filter_expr)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  const ObTableSchema* index_schema = NULL;
  ObLogPlan* log_plan = NULL;
  ObSQLSessionInfo* session = NULL;
  bool is_get = false;
  if (OB_FAIL(is_table_get(is_get))) {
    LOG_WARN("check is table get failed", K(ret));
  } else if (is_get) {
    // do nothing
  } else if (OB_ISNULL(log_plan = get_plan()) || OB_ISNULL(stmt = log_plan->get_stmt()) ||
             OB_INVALID_ID == ref_table_id_ || OB_INVALID_ID == index_table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id", K(ref_table_id_), K(index_table_id_), K(ret));
  } else if (OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_ISNULL(session = log_plan->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null");
  } else if (!session->use_static_typing_engine() || is_fake_cte_table_) {
    // do nothing
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ref_table_id_), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_table_id_, index_schema))) {
    LOG_WARN("get table schema failed", K(index_table_id_), K(ret));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(table_schema), K(index_schema));
  } else if (table_schema->need_part_filter()) {
    ObRawExprFactory& expr_factory = log_plan->get_optimizer_context().get_expr_factory();
    uint64_t filter_table_id = is_index_scan() ? index_table_id_ : ref_table_id_;
    const ObTableSchema* filter_table_schema = is_index_scan() ? index_schema : table_schema;
    schema::ObPartitionLevel part_level = filter_table_schema->get_part_level();
    ObRawExpr* part_expr = stmt->get_part_expr(table_id_, filter_table_id);
    ObRawExpr* subpart_expr = stmt->get_subpart_expr(table_id_, filter_table_id);
    ObRawExpr* calc_part_id_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_calc_part_id_expr(
            expr_factory, *session, filter_table_id, part_level, part_expr, subpart_expr, calc_part_id_expr))) {
      LOG_WARN("fail to build table location expr", K(ret));
    }
    if (NULL != calc_part_id_expr) {
      ObConstRawExpr* part_id_const_expr = NULL;
      ObRawExpr* part_id_expr = NULL;
      std::pair<int64_t, ObRawExpr*> idx_expr_pair;
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, 0, part_id_const_expr))) {
        LOG_WARN("fail to build const int expr", K(ret));
      } else if (FALSE_IT(part_id_expr = part_id_const_expr)) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::create_exec_param_expr(
                     stmt, expr_factory, part_id_expr, session, idx_expr_pair))) {
        LOG_WARN("fail to create param expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(
                     expr_factory, session, part_id_expr, calc_part_id_expr, part_filter_expr))) {
        LOG_WARN("fail to create equal expr", K(ret));
      } else {
        expected_part_id_ = part_id_expr;
        part_filter_ = part_filter_expr;
      }
    }
  }

  return ret;
}

int ObLogTableScan::generate_rowkey_and_partkey_exprs(ObIArray<ObRawExpr*>& rowkey_exprs, bool has_lob_col /* false */)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id_, table_schema)) || OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is null", K(ref_table_id_), K(ret));
  } else if (OB_FAIL(extract_access_exprs(table_schema, table_schema->get_rowkey_info(), rowkey_exprs))) {
    LOG_WARN("failed to extract expr from rowkey info", K(ret));
  } else if ((is_index_global_ && is_global_index_back_ && table_schema->is_partitioned_table() &&
                 table_schema->is_old_no_pk_table()) ||
             has_lob_col) {
    if (OB_FAIL(extract_access_exprs(table_schema, table_schema->get_partition_key_info(), rowkey_exprs))) {
      LOG_WARN("failed to extract expr from rowkey info", K(ret));
    } else if (share::schema::PARTITION_LEVEL_TWO == table_schema->get_part_level() &&
               OB_FAIL(extract_access_exprs(table_schema, table_schema->get_subpartition_key_info(), rowkey_exprs))) {
      LOG_WARN("failed to extract expr from rowkey info", K(ret));
    }
  }
  return ret;
}

int ObLogTableScan::add_mapping_columns_for_vt(ObIArray<ObRawExpr*>& access_exprs)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("debug mapping columns for virtual table", K(ref_table_id_));
  if (0 < access_exprs.count()) {
    ObColumnRefRawExpr* first_col_expr = static_cast<ObColumnRefRawExpr*>(access_exprs.at(0));
    LOG_DEBUG("debug mapping columns for virtual table", K(first_col_expr->get_table_id()), K(ref_table_id_));
    if (share::is_oracle_mapping_real_virtual_table(ref_table_id_)) {
      ARRAY_FOREACH(access_exprs, i)
      {
        OZ(add_mapping_column_for_vt(static_cast<ObColumnRefRawExpr*>(access_exprs.at(i))));
      }
    }
  }
  return ret;
}

int ObLogTableScan::add_mapping_column_for_vt(ObColumnRefRawExpr* col_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_expr->get_real_expr())) {
    int64_t tenant_id = extract_tenant_id(ref_table_id_);
    int64_t mapping_table_id = share::schema::ObSchemaUtils::get_real_table_mappings_tid(ref_table_id_);
    if (OB_INVALID_ID == mapping_table_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: failed to get real table id", K(ret));
    }
    const schema::ObColumnSchemaV2* col_schema = NULL;
    const ObTableSchema* table_schema = NULL;
    int64_t column_id = col_expr->get_column_id();
    ObColumnRefRawExpr* mapping_col_expr = NULL;
    LOG_DEBUG("debug mapping column", K(column_id), K(col_expr->get_column_name()));
    ObOptimizerContext* opt_ctx = NULL;
    ObSqlSchemaGuard* schema_guard = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
               OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get unexpected null", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(mapping_table_id, table_schema))) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("get table schema failed", K(mapping_table_id), K(ret));
    } else if (OB_ISNULL(col_schema = table_schema->get_column_schema(col_expr->get_column_name()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(col_expr->get_column_name()), K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(opt_ctx->get_expr_factory(), *col_schema, mapping_col_expr))) {
      LOG_WARN("build column expr failed", K(ret));
    } else {
      mapping_col_expr->set_synonym_db_name(col_expr->get_synonym_db_name());
      mapping_col_expr->set_synonym_name(col_expr->get_synonym_name());
      mapping_col_expr->set_column_attr(table_schema->get_table_name_str(), col_schema->get_column_name_str());
      mapping_col_expr->set_database_name(col_expr->get_database_name());
      // column maybe from alias table, so must reset ref id by table id from table_item
      mapping_col_expr->set_ref_id(mapping_table_id, col_schema->get_column_id());
      mapping_col_expr->set_lob_column(col_expr->is_lob_column());
      col_expr->set_real_expr(mapping_col_expr);
    }
  }
  return ret;
}

int ObLogTableScan::extract_access_exprs(
    const ObTableSchema* table_schema, const ObRowkeyInfo& rowkey_info, ObIArray<ObRawExpr*>& rowkey_exprs)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObOptimizerContext* opt_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret));
  } else {
    const schema::ObColumnSchemaV2* column_schema = NULL;
    uint64_t column_id = OB_INVALID_ID;
    for (int col_idx = 0; OB_SUCC(ret) && col_idx < rowkey_info.get_size(); ++col_idx) {
      if (OB_FAIL(rowkey_info.get_column_id(col_idx, column_id))) {
        LOG_WARN("Failed to get column id", K(ret));
      } else if (OB_ISNULL(column_schema = (table_schema->get_column_schema(column_id)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(column_id), K(ret));
      } else {
        ObColumnRefRawExpr* expr = NULL;
        ObRawExpr* raw_expr = stmt->get_column_expr_by_id(get_table_id(), column_id);
        if (NULL != raw_expr) {
          expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
        } else if (OB_FAIL(ObRawExprUtils::build_column_expr(opt_ctx->get_expr_factory(), *column_schema, expr))) {
          LOG_WARN("build column expr failed", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(col_idx), K(ret));
        } else if (OB_FAIL(expr->formalize(opt_ctx->get_session_info()))) {
          LOG_WARN("failed to formalize the new expr", K(ret));
        } else { /*do nothing*/
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(rowkey_exprs.push_back(expr))) {
            LOG_WARN("failed to add row key expr", K(ret));
          } else { /* do nothing */
          }
        } else { /* do nothing */
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::index_back_check()
{
  int ret = OB_SUCCESS;
  bool column_found = true;
  if (!is_index_scan()) {
    column_found = true;
  } else {
    for (int64_t access_idx = 0; OB_SUCC(ret) && column_found && access_idx < access_exprs_.count(); ++access_idx) {
      const ObColumnRefRawExpr* expr = NULL;
      if (OB_ISNULL(expr = static_cast<const ObColumnRefRawExpr*>(access_exprs_.at(access_idx)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("access expr is null", K(access_idx), K(ret));
      } else {
        const uint64_t column_id = expr->get_column_id();
        column_found = false;
        for (int64_t col_idx = 0; OB_SUCC(ret) && !column_found && col_idx < idx_columns_.count(); ++col_idx) {
          if (column_id == idx_columns_.at(col_idx)) {
            column_found = true;
          } else { /* Do nothing */
          }
        }
      }
    }  // end for
  }

  if (OB_SUCC(ret)) {
    index_back_ = !column_found;
    if (OB_FAIL(filter_before_index_back_set())) {
      LOG_WARN("Failed to filter_before_index_back_set", K(ret));
    } else { /*Do nothing*/
    }
  } else { /* Do nothing */
  }

  return ret;
}

int ObLogTableScan::filter_before_index_back_set()
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  filter_before_index_back_.reset();
  if (index_back_) {
    if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "get_plan is NULL or get_plan()->get_optimizer_context() returns null", K(get_plan()), K(opt_ctx), K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::check_filter_before_indexback(get_index_table_id(),
                   opt_ctx->get_schema_guard(),
                   get_filter_exprs(),
                   false,
                   OB_INVALID_ID,
                   filter_before_index_back_))) {
      LOG_WARN("Failed to check filter before index back", K(ret));
    } else { /*do nothing*/
    }
  } else {
    // Not index back, mark all filter not pulled
    for (int64_t i = 0; OB_SUCC(ret) && i < get_filter_exprs().count(); ++i) {
      if (OB_FAIL(filter_before_index_back_.push_back(false))) {
        LOG_WARN("Failed to add bool to filter_before_index_back_", K(i), K(ret));
      } else { /* Do nothing */
      }
    }
  }
  LOG_TRACE("succeed to check filter before index back", K(filter_before_index_back_), K(index_back_), K(ret));
  return ret;
}

int ObLogTableScan::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(stmt_) || OB_ISNULL(table_partition_info_) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(get_plan()), K(session_info), K(stmt_), K(table_partition_info_), K(ret));
  } else if (is_fake_cte_table_) {
    sharding_info_.set_location_type(ObTableLocationType::OB_TBL_LOCATION_ALL);
  } else {
    bool is_intra_part_parallel = false;
    ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
    ObOptimizerContext& opt_ctx = get_plan()->get_optimizer_context();
    if (OB_FAIL(init_table_location_info())) {
      LOG_WARN("failed to calculate table location", K(ret));
    } else if (OB_FAIL(table_partition_info_->get_all_servers(ctx->servers_))) {
      LOG_WARN("failed to get all servers", K(ret));
    } else if (OB_FAIL(table_partition_info_->get_location_type(opt_ctx.get_local_server_addr(), location_type))) {
      LOG_WARN("get table location type error", K(ret));
    } else {
      table_phy_location_type_ = location_type;

      if ((OB_TBL_LOCATION_LOCAL == location_type || OB_TBL_LOCATION_REMOTE == location_type) &&
          get_plan()->get_optimizer_context().use_intra_parallel()) {
        location_type = OB_TBL_LOCATION_DISTRIBUTED;
        is_intra_part_parallel = true;
        ctx->plan_type_ = AllocExchContext::DistrStat::DISTRIBUTED;
      } else if (OB_TBL_LOCATION_DISTRIBUTED == location_type) {
        ctx->plan_type_ = AllocExchContext::DistrStat::DISTRIBUTED;
      } else { /*do nothing*/
      }

      if (is_intra_part_parallel) {
        sharding_info_.set_ref_table_id(is_index_global_ ? index_table_id_ : ref_table_id_);
        sharding_info_.set_location_type(location_type);
        LOG_DEBUG("intra part parallel for tsc", K(sharding_info_), K(location_type));
      } else if (OB_FAIL(sharding_info_.init_partition_info(get_plan()->get_optimizer_context(),
                     *stmt_,
                     table_id_,
                     is_index_global_ ? index_table_id_ : ref_table_id_,
                     table_partition_info_->get_phy_tbl_location_info()))) {
        LOG_WARN("failed to set partition key", K(ret), K(ref_table_id_));
      } else {
        sharding_info_.set_location_type(location_type);
        LOG_DEBUG("non-intra part parallel for tsc", K(sharding_info_), K(location_type));
      }
    }
  }
  return ret;
}

int ObLogTableScan::refine_query_range()
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  const ParamStore* params = NULL;
  void* tmp_ptr = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(params = opt_ctx->get_params())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(get_plan()), K(opt_ctx), K(params), K(ret));
  } else if (get_is_fake_cte_table() || get_range_columns().empty()) {
    /*do nothing*/
  } else if (OB_FAIL(append(get_filter_exprs(), range_conds_))) {
    LOG_WARN("failed to append expr", K(ret));
  } else if (OB_ISNULL(tmp_ptr = get_plan()->get_allocator().alloc(sizeof(ObQueryRange)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory", K(ret));
  } else {
    ObQueryRange* tmp_qr = new (tmp_ptr) ObQueryRange(get_plan()->get_allocator());
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(opt_ctx->get_session_info());
    if (OB_FAIL(tmp_qr->preliminary_extract_query_range(get_range_columns(), get_filter_exprs(), dtc_params, params))) {
      LOG_WARN("failed to preliminary extract query range", K(ret));
      if (NULL != tmp_qr) {
        tmp_qr->reset();
        tmp_qr = NULL;
      }
    } else {
      range_conds_.reset();
      set_pre_query_range(tmp_qr);
      if (OB_FAIL(pick_out_query_range_exprs())) {
        LOG_WARN("failed to pit our range expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogTableScan::set_filters(const common::ObIArray<ObRawExpr*>& filters)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_filter_exprs().assign(filters))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else if (OB_FAIL(pick_out_query_range_exprs())) {
    LOG_WARN("failed to pick out query range exprs", K(ret));
  } else if (OB_FAIL(pick_out_startup_filters())) {
    LOG_WARN("failed to pick out startup filters", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogTableScan::pick_out_query_range_exprs()
{
  int ret = OB_SUCCESS;
  bool is_get = false;
  ObOptimizerContext* opt_ctx = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  const share::schema::ObTableSchema* index_schema = NULL;
  /*
   * virtual table may have hash index,
   * for hash index, if it is a get, we should still extract the range condition
   */
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL pointer error", K(get_plan()), K(opt_ctx), K(schema_guard), K(ret));
  } else if (get_is_fake_cte_table()) {
    // do nothing
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id_, index_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(is_table_get(is_get))) {
    LOG_WARN("failed to check is table get", K(ret));
  } else if ((index_schema->is_ordered() || is_get) && NULL != pre_query_range_) {
    const ObIArray<ObRawExpr*>& range_exprs = pre_query_range_->get_range_exprs();
    ObArray<ObRawExpr*> filter_exprs;
    if (OB_FAIL(filter_exprs.assign(filter_exprs_))) {
      LOG_WARN("assign filter exprs failed", K(ret));
    } else {
      filter_exprs_.reset();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); ++i) {
      bool found_expr = false;
      for (int64_t j = 0; OB_SUCC(ret) && !found_expr && j < range_exprs.count(); ++j) {
        if (filter_exprs.at(i) == range_exprs.at(j)) {
          found_expr = true;
        } else { /* Do nothing */
        }
      }
      // for virtual table, even if we extract query range, we need to maintain the condition into the filter
      if (OB_SUCC(ret) &&
          (!found_expr || (is_virtual_table(ref_table_id_) && !is_oracle_mapping_real_virtual_table(ref_table_id_)))) {
        if (OB_FAIL(filter_exprs_.push_back(filter_exprs.at(i)))) {
          LOG_WARN("add filter expr failed", K(i), K(ret));
        } else { /* Do nothing */
        }
      }
      if (OB_SUCC(ret) && found_expr) {
        if (OB_FAIL(range_conds_.push_back(filter_exprs.at(i)))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }      // end for
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogTableScan::pick_out_startup_filters()
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> filter_exprs;
  if (OB_FAIL(filter_exprs.assign(filter_exprs_))) {
    LOG_WARN("assign filter exprs failed", K(ret));
  } else {
    filter_exprs_.reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); ++i) {
    ObRawExpr* expr = filter_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->get_relation_ids().is_empty() && !expr->has_flag(CNT_EXEC_PARAM)) {
      ret = get_startup_exprs().push_back(expr);
    } else {
      ret = get_filter_exprs().push_back(expr);
    }
  }
  return ret;
}

uint64_t ObLogTableScan::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = do_hash(table_id_, hash_value);
  hash_value = do_hash(ref_table_id_, hash_value);
  hash_value = do_hash(index_table_id_, hash_value);
  hash_value = do_hash(is_global_index_back_, hash_value);
  hash_value = do_hash(is_fake_cte_table_, hash_value);
  hash_value = do_hash(index_back_, hash_value);
  hash_value = do_hash(scan_direction_, hash_value);
  hash_value = do_hash(for_update_, hash_value);
  hash_value = do_hash(for_update_wait_us_, hash_value);
  hash_value = do_hash(hint_, hash_value);
  hash_value = do_hash(exist_hint_, hash_value);
  if (NULL != part_hint_) {
    hash_value = do_hash(*part_hint_, hash_value);
  }
  HASH_PTR_ARRAY(range_conds_, hash_value);
  HASH_ARRAY(range_columns_, hash_value);
  HASH_ARRAY(idx_columns_, hash_value);
  HASH_ARRAY(parts_sortkey_, hash_value);
  HASH_PTR_ARRAY(access_exprs_, hash_value);
  HASH_ARRAY(filter_before_index_back_, hash_value);
  hash_value = ObOptimizerUtil::hash_expr(limit_count_expr_, hash_value);
  hash_value = ObOptimizerUtil::hash_expr(limit_offset_expr_, hash_value);
  hash_value = do_hash(sample_info_, hash_value);
  hash_value = do_hash(is_parallel_, hash_value);

  LOG_TRACE("TABLE SCAN hash value", K(hash_value), K(limit_count_expr_), K(part_hint_));
  hash_value = ObLogicalOperator::hash(hash_value);
  return hash_value;
}

int ObLogTableScan::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  // print access
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }

  if (OB_SUCC(ret)) {
    const ObIArray<ObRawExpr*>& access = get_access_exprs();
    EXPLAIN_PRINT_EXPRS(access, type);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }

  // print partitions
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(table_partition_info_)) {
      if (OB_FAIL(BUF_PRINTF("partitions is NULL, "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    } else {
      const ObPhyPartitionLocationInfoIArray& partitions =
          table_partition_info_->get_phy_tbl_location_info().get_phy_part_loc_info_list();
      const bool two_level = (schema::PARTITION_LEVEL_TWO == table_partition_info_->get_part_level());
      ObSEArray<int64_t, 128> pids;
      int64_t N = partitions.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        const ObOptPartLoc& part_loc = partitions.at(i).get_partition_location();
        int64_t pid = part_loc.get_partition_id();
        if (OB_FAIL(pids.push_back(pid))) {
          LOG_WARN("failed to add partition id", K(pid), K(i), K(ret));
        } else {
          std::sort(pids.begin(), pids.end(), compare_partition_id);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObLogicalOperator::explain_print_partitions(pids, two_level, buf, buf_len, pos))) {
          LOG_WARN("Failed to print partitions");
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(print_limit_offset_annotation(buf, buf_len, pos, type))) {
      LOG_WARN("print limit offset annotation failed", K(ret), K(buf_len), K(pos), K(type));
    }
  }

  if (OB_SUCC(ret) && (EXPLAIN_EXTENDED == type || EXPLAIN_EXTENDED_NOADDR == type || EXPLAIN_PLANREGRESS == type)) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("is_index_back=%s", index_back_ ? "true" : "false"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */
    }

    if (OB_SUCC(ret) && (0 != filter_before_index_back_.count())) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(print_filter_before_indexback_annotation(buf, buf_len, pos))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */
      }
    }
  }

  if (OB_SUCC(ret) && exist_hint_) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("hint("))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (FALSE_IT(hint_.hint_to_string(buf, buf_len, pos))) {
      LOG_WARN("hint_to_string() fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */
    }
  }

  // Print ranges
  if (OB_SUCC(ret) && (EXPLAIN_EXTENDED == type || EXPLAIN_EXTENDED_NOADDR == type || EXPLAIN_PLANREGRESS == type) &&
      range_columns_.count() > 0) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(print_range_annotation(buf, buf_len, pos, type))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(print_split_range_annotation(buf, buf_len, pos, type))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }

  return ret;
}

int ObLogTableScan::explain_index_selection_info(char* buf, int64_t& buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObString index_selection_method_name;
  ObString heuristic_rule_name;
  if (OB_NOT_NULL(table_opt_info_)) {
    switch (table_opt_info_->optimization_method_) {
      case OptimizationMethod::MAX_METHOD:
        index_selection_method_name = "max_method";
        break;
      case OptimizationMethod::COST_BASED:
        index_selection_method_name = "cost_based";
        break;
      case OptimizationMethod::RULE_BASED:
        index_selection_method_name = "rule_based";
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown index selection method", K(table_opt_info_->optimization_method_));
        break;
    }
    switch (table_opt_info_->heuristic_rule_) {
      case HeuristicRule::MAX_RULE:
        heuristic_rule_name = "max_rule";
        break;
      case HeuristicRule::UNIQUE_INDEX_WITHOUT_INDEXBACK:
        heuristic_rule_name = "unique_index_without_indexback";
        break;
      case HeuristicRule::UNIQUE_INDEX_WITH_INDEXBACK:
        heuristic_rule_name = "unique_index_with_indexback";
        break;
      case HeuristicRule::VIRTUAL_TABLE_HEURISTIC:
        heuristic_rule_name = "virtual_table_prefix";
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown heuristic rule", K(table_opt_info_->heuristic_rule_));
    }
    // print detail info of index selection method
    if (OB_SUCC(ret)) {
      const char* method[5] = {"invalid_method", "remote_storage", "local_storage", "basic_stat", "histogram"};
      if (OB_FAIL(BUF_PRINTF("%.*s:", table_name_.length(), table_name_.ptr()))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_UNLIKELY(estimate_method_ < 0 || estimate_method_ >= 5)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid array pos", K(estimate_method_), K(ret));
      } else if (OB_FAIL(BUF_PRINTF("table_rows:%ld, physical_range_rows:%ld, logical_range_rows:%ld, "
                                    "index_back_rows:%ld, output_rows:%ld, est_method:%s",
                     static_cast<int64_t>(table_row_count_),
                     static_cast<int64_t>(phy_query_range_row_count_),
                     static_cast<int64_t>(query_range_row_count_),
                     static_cast<int64_t>(index_back_row_count_),
                     static_cast<int64_t>(output_row_count_),
                     method[estimate_method_]))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("optimization_method=%.*s",
                     index_selection_method_name.length(),
                     index_selection_method_name.ptr()))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OptimizationMethod::MAX_METHOD == table_opt_info_->optimization_method_) {
        // do nothing
      } else if (OptimizationMethod::RULE_BASED == table_opt_info_->optimization_method_) {
        if (OB_FAIL(BUF_PRINTF("heuristic_rule=%.*s\n", heuristic_rule_name.length(), heuristic_rule_name.ptr()))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* do nothing*/
        }
      } else {
        // print available index id
        if (OB_FAIL(BUF_PRINTF("avaiable_index_name["))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < table_opt_info_->available_index_name_.count(); ++i) {
          if (OB_FAIL(BUF_PRINTF("%.*s",
                  table_opt_info_->available_index_name_.at(i).length(),
                  table_opt_info_->available_index_name_.at(i).ptr()))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else if (i != table_opt_info_->available_index_name_.count() - 1) {
            if (OB_FAIL(BUF_PRINTF(","))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            } else { /* do nothing*/
            }
          } else { /* do nothing*/
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(BUF_PRINTF("]"))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else { /* Do nothing */
          }
        } else { /* Do nothing */
        }

        // print pruned index name
        if (OB_SUCC(ret) && table_opt_info_->pruned_index_name_.count() > 0) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else if (OB_FAIL(BUF_PRINTF("pruned_index_name["))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < table_opt_info_->pruned_index_name_.count(); ++i) {
              if (OB_FAIL(BUF_PRINTF("%.*s",
                      table_opt_info_->pruned_index_name_.at(i).length(),
                      table_opt_info_->pruned_index_name_.at(i).ptr()))) {
                LOG_WARN("BUF_PRINTF fails", K(ret));
              } else if (i != table_opt_info_->pruned_index_name_.count() - 1) {
                if (OB_FAIL(BUF_PRINTF(","))) {
                  LOG_WARN("BUF_PRINTF fails", K(ret));
                } else { /* do nothing*/
                }
              } else { /* do nothing*/
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(BUF_PRINTF("]"))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            } else { /* Do nothing */
            }
          } else { /* Do nothing */
          }
        }
        // print est row count infos
        if (OB_SUCC(ret) && est_records_.count() > 0) {
          if (OB_FAIL(BUF_PRINTF(", estimation info[table_id:%ld,", est_records_.at(0).table_id_))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < est_records_.count(); ++i) {
            const ObEstRowCountRecord& record = est_records_.at(i);
            if (OB_FAIL(BUF_PRINTF(" (table_type:%ld, version:%ld-%ld-%ld, logical_rc:%ld, physical_rc:%ld)%c",
                    record.table_type_,
                    record.version_range_.base_version_,
                    record.version_range_.multi_version_start_,
                    record.version_range_.snapshot_version_,
                    record.logical_row_count_,
                    record.physical_row_count_,
                    i == est_records_.count() - 1 ? ']' : ','))) {
              LOG_WARN("BUF PRINTF fails", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(BUF_PRINTF("\n"))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::print_split_range_annotation(char* buf, int64_t buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;

  UNUSED(type);
  if (NULL != table_partition_info_) {
    ObSEArray<ObPhyTableLocationInfo, 1> phy_table_location_array;
    ObPhyPlanType plan_type = OB_PHY_PLAN_UNINITIALIZED;
    const ObPhyTableLocationInfo& phy_table_location_info = table_partition_info_->get_phy_tbl_location_info();

    if (OB_FAIL(phy_table_location_array.push_back(phy_table_location_info))) {
    } else if (OB_FAIL(ObSqlPlanSet::calc_phy_plan_type(phy_table_location_array, plan_type))) {
    } else if (OB_PHY_PLAN_DISTRIBUTED == plan_type) {
      if (phy_table_location_info.get_splitted_ranges().count() > 0) {
        // const ObSplittedRanges &split_ranges = phy_table_location_info.get_splitted_ranges().at(0);
        const common::ObIArray<ObSplittedRanges>& split_ranges_array = phy_table_location_info.get_splitted_ranges();
        for (int64_t j = 0; j < split_ranges_array.count() && OB_SUCC(ret); ++j) {
          const ObSplittedRanges& split_ranges = split_ranges_array.at(j);
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
          } else if (OB_FAIL(BUF_PRINTF("split_ranges("))) {
          } else if (OB_FAIL(print_ranges(buf, buf_len, pos, split_ranges.get_ranges()))) {
          } else if (OB_FAIL(BUF_PRINTF("), split_pos=("))) {
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < split_ranges.get_offsets().count(); ++i) {
              if (OB_FAIL(BUF_PRINTF("%ld", split_ranges.get_offset(i)))) {
              } else if (i != split_ranges.get_offsets().count() - 1) {
                ret = BUF_PRINTF(", ");
              } else {
                ret = BUF_PRINTF(")");
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::print_filter_before_indexback_annotation(char* buf, int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF("filter_before_indexback["))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < filter_before_index_back_.count(); ++i) {
    if (filter_before_index_back_.at(i)) {
      if (OB_FAIL(BUF_PRINTF("true"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */
      }
    } else {
      if (OB_FAIL(BUF_PRINTF("false"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */
      }
    }
    if ((filter_before_index_back_.count() - 1) != i) {
      if (OB_FAIL(BUF_PRINTF(","))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */
      }
    } else { /* Do nothing */
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(BUF_PRINTF("]"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }

  return ret;
}

int ObLogTableScan::print_ranges(char* buf, int64_t buf_len, int64_t& pos, const ObIArray<ObNewRange>& ranges)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    if (i >= 1) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */
      }
    } else { /* Do nothing */
    }

    if (OB_SUCC(ret)) {
      pos += ranges.at(i).to_plain_string(buf + pos, buf_len - pos);
    }
  }
  return ret;
}

int ObLogTableScan::print_range_annotation(char* buf, int64_t buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> range_key;
  for (int64_t i = 0; OB_SUCC(ret) && i < range_columns_.count(); ++i) {
    if (OB_ISNULL(range_columns_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Expr in column item should not be NULL", K(i), K(ret));
    } else if (OB_FAIL(range_key.push_back(range_columns_.at(i).expr_))) {
      LOG_WARN("Fail to add range expr", K(i), K(ret));
    } else { /* Do nothing */
    }
  }
  if (OB_SUCC(ret)) {
    EXPLAIN_PRINT_EXPRS(range_key, type);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("range"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */
      }
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }
  // When range is empty. Range is always true.
  if (OB_SUCC(ret) && 0 >= ranges_.count()) {
    if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("MIN"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(" ; "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("MAX"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }

  if (OB_SUCC(ret)) {
    ret = print_ranges(buf, buf_len, pos, ranges_);
  }

  if (OB_SUCC(ret)) {
    if (!range_conds_.empty()) {
      // print range condition
      const ObIArray<ObRawExpr*>& range_cond = range_conds_;
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
      EXPLAIN_PRINT_EXPRS(range_cond, type);
    }
  }

  return ret;
}

int ObLogTableScan::print_limit_offset_annotation(char* buf, int64_t buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (NULL != limit_count_expr_ || NULL != limit_offset_expr_) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      ObRawExpr* limit = limit_count_expr_;
      ObRawExpr* offset = limit_offset_expr_;
      EXPLAIN_PRINT_EXPR(limit, type);
      BUF_PRINTF(", ");
      EXPLAIN_PRINT_EXPR(offset, type);
    }
  }

  return ret;
}

int ObLogTableScan::is_prefix_of_partition_key(ObIArray<OrderItem>& ordering, bool& is_prefix)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObSEArray<ObRawExpr*, 5> part_order;
  is_prefix = false;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt())) ||
      OB_ISNULL(table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_plan()), K(stmt), K(table_partition_info_));
  } else {
    if (OB_FAIL(table_partition_info_->get_not_insert_dml_part_sort_expr(*stmt, &part_order))) {
      LOG_WARN("fail to get_not_insert_dml_part_sort_expr", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (part_order.count() <= 0) {
      LOG_TRACE("no partitions order");
    } else {
      ObArray<OrderItem> part_order_with_directions;
      ObArray<ObRawExpr*> raw_exprs;
      ObArray<ObOrderDirection> directions;
      EqualSets dummy_input_esets;
      if (OB_FAIL(ObOptimizerUtil::split_expr_direction(ordering, raw_exprs, directions))) {
        LOG_WARN("failed to split order items", K(ret));
      } else if (0 == directions.count()) {
        ret = OB_SUCCESS;
        LOG_TRACE("directions is empty", K(ordering), K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(part_order, directions.at(0), part_order_with_directions))) {
        LOG_WARN("fail to make sort keys", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(part_order_with_directions,
                     raw_exprs,
                     dummy_input_esets,
                     get_output_const_exprs(),
                     is_prefix,
                     &directions))) {
        LOG_WARN("Failed to check prefix ordering", K(ret));
      } else if (!is_prefix) {
        ObArray<ObOrderDirection> part_directions;
        ObArray<ObRawExpr*> part_keys;
        if (OB_FAIL(ObOptimizerUtil::split_expr_direction(part_order_with_directions, part_keys, part_directions))) {
          LOG_WARN("failed to split order items", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(ordering,
                       part_keys,
                       dummy_input_esets,
                       get_output_const_exprs(),
                       is_prefix,
                       &part_directions))) {
          LOG_WARN("Failed to check prefix ordering", K(ret));
        } else {
          LOG_TRACE("is prefix ordering", K(is_prefix));
        }
      } else {
        LOG_TRACE("is prefix ordering", K(is_prefix));
      }
    }
  }
  return ret;
}

int ObLogTableScan::init_table_location_info()
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObSEArray<ObRawExpr*, 5> part_order;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt())) ||
      OB_ISNULL(table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(stmt), K(table_partition_info_));
  } else if (OB_FAIL(table_partition_info_->set_log_op_infos(index_table_id_, scan_direction_))) {
    LOG_WARN("fail to set log op infos", K(ret));
  } else if (OB_FAIL(table_partition_info_->get_not_insert_dml_part_sort_expr(*stmt, &part_order))) {
    LOG_WARN("fail to get_not_insert_dml_part_sort_expr", K(ret));
  } else if (is_fake_cte_table_) {
    /*do nothing*/
  } else if (OB_FAIL(get_plan()->add_global_table_partition_info(table_partition_info_))) {
    LOG_WARN("Failed to add global table partition info", K(ret));
  } else {
    LOG_DEBUG("debug partition info", K(*table_partition_info_));
  }

  // construct part order, if px is used, we ignore the part order */
  if (OB_SUCC(ret) && part_order.count() > 0) {
    ObSEArray<ObRawExpr*, 5> index_ordering;
    EqualSets dummy_input_esets;
    bool is_prefix = false;
    if (OB_FAIL(get_path_ordering(index_ordering))) {
      LOG_WARN("Failed to construct ordering", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::prefix_subset_exprs(
                   part_order, index_ordering, dummy_input_esets, get_plan()->get_const_exprs(), is_prefix, NULL))) {
      LOG_WARN("failed to check prefix ordering", K(ret));
    } else if (is_prefix) {
      if (OB_FAIL(ObOptimizerUtil::make_sort_keys(index_ordering, scan_direction_, parts_sortkey_))) {
        LOG_WARN("Failed to make sort keys", K(ret));
      }
    } else {
      LOG_TRACE("partition order is prefix of index ordering", K(is_prefix));
    }
  }
  return ret;
}

int ObLogTableScan::set_query_ranges(ObRangesArray ranges)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(ranges_, ranges))) {
    LOG_WARN("Failed to do append to ranges_ in set_query_ranges()");
  } else { /* Do nothing =*/
  }
  return ret;
}

int ObLogTableScan::inner_replace_generated_agg_expr(
    const ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(to_replace_exprs, access_exprs_))) {
    LOG_WARN("failed to replace_expr_action", K(ret));
  }
  return ret;
}

int ObLogTableScan::print_operator_for_outline(planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  OutlineType type = plan_text.outline_type_;
  TableItem* table_item = NULL;
  ObString stmt_name;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL", K(ret));
  } else if (OB_ISNULL(table_item = stmt_->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table item", K(ret), "table_id", table_id_);
  } else if (USED_HINT == type && OB_FAIL(stmt_->get_stmt_name(stmt_name))) {
    LOG_WARN("fail to get stmt_name", K(ret), K(stmt_->get_stmt_id()));
  } else if (OUTLINE_DATA == type && OB_FAIL(stmt_->get_stmt_org_name(stmt_name))) {
    LOG_WARN("fail to get stmt_name", K(ret), K(stmt_->get_stmt_id()));
  } else if (OB_FAIL(BUF_PRINTF("\"%.*s.%.*s\"@\"%.*s\"",
                 table_item->database_name_.length(),
                 table_item->database_name_.ptr(),
                 table_item->get_object_name().length(),
                 table_item->get_object_name().ptr(),
                 stmt_name.length(),
                 stmt_name.ptr()))) {
    LOG_WARN("fail to print buffer", K(ret), K(buf), K(buf_len), K(pos));
  } else { /*do nohting*/
  }
  return ret;
}

int ObLogTableScan::print_single_no_index(const ObString& index_name, planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_one_line = plan_text.is_oneline_;
  OutlineType type = plan_text.outline_type_;
  TableItem* table_item = NULL;
  ObString stmt_name;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULl", K(ret));
  } else if (OB_ISNULL(table_item = stmt_->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table item", K(ret), "table_id", table_id_);
  } else if (USED_HINT == type && OB_FAIL(stmt_->get_stmt_name(stmt_name))) {
    LOG_WARN("fail to get stmt_name", K(ret), K(stmt_->get_stmt_id()));
  } else if (OUTLINE_DATA == type && OB_FAIL(stmt_->get_stmt_org_name(stmt_name))) {
    LOG_WARN("fail to get org stmt_name", K(ret), K(stmt_->get_stmt_id()));
  } else if (!is_one_line && OB_FAIL(BUF_PRINTF("\n"))) {
  } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_one_line)))) {
  } else if (OB_FAIL(BUF_PRINTF("%s(@\"%.*s\" \"%.*s.%.*s\"@\"%.*s\" \"%.*s\")",
                 ObStmtHint::NO_INDEX_HINT,
                 stmt_name.length(),
                 stmt_name.ptr(),
                 table_item->database_name_.length(),
                 table_item->database_name_.ptr(),
                 table_name_.length(),
                 table_name_.ptr(),
                 stmt_name.length(),
                 stmt_name.ptr(),
                 index_name.length(),
                 index_name.ptr()))) {
    LOG_WARN("fail to table scan hint", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogTableScan::print_all_no_index(const ObIndexHint& index_hint, planText& plan_text)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  ObString index_name;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is NULL", K(ret));
  } else if (OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("opt ctx is NULL", K(ret));
  } else if (OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is NULL", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < index_hint.valid_index_ids_.count(); ++i) {
    uint64_t index_id = index_hint.valid_index_ids_.at(i);
    const ObTableSchema* index_schema = NULL;
    index_name.reset();
    if (index_id == ref_table_id_) {  // primary key
      index_name.assign_ptr(ObStmtHint::PRIMARY_KEY, static_cast<int32_t>(strlen(ObStmtHint::PRIMARY_KEY)));
      if (OB_FAIL(print_single_no_index(index_name, plan_text))) {
        LOG_WARN("fail to print single_no_index", K(ret), K(index_name));
      }
    } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {  // index
      LOG_WARN("fail to get index schema", K(ret), K(index_id));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index schema is null", K(index_id), K(ret));
    } else if (OB_FAIL(index_schema->get_index_name(index_name))) {
      LOG_WARN("fail to get index name", K(ret));
    } else if (OB_FAIL(print_single_no_index(index_name, plan_text))) {
      LOG_WARN("fail to print single_no_index", K(ret), K(index_name));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogTableScan::print_used_index(planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_used_hint = false;
  ObIndexHint::HintType hint_type = ObIndexHint::UNINIT;
  const ObIndexHint* index_hint = NULL;
  ;
  if (OB_FAIL(get_index_hint(is_used_hint, index_hint))) {
    LOG_WARN("fail to get hint_type", K(ret), K(is_used_hint));
  } else if (is_used_hint) {
    if (OB_ISNULL(index_hint)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index hint is NULL", K(ret));
    } else {
      switch (index_hint->type_) {
        case ObIndexHint::USE:
        case ObIndexHint::FORCE: {
          if (OB_FAIL(print_outline_data(plan_text))) {
            LOG_WARN("fail to print index", K(ret), K(buf), K(buf_len), K(pos));
          }
          break;
        }
        case ObIndexHint::IGNORE: {
          if (OB_FAIL(print_all_no_index(*index_hint, plan_text))) {
            LOG_WARN("fail to print index", K(ret));
          }
          break;
        }
        default: {
          LOG_WARN("unexpected index hint type", K(hint_type));
        }
      }
    }
  } else { /*do not print*/
  }

  return ret;
}

int ObLogTableScan::get_index_hint(bool& is_used_hint, const ObIndexHint*& index_hint)
{
  int ret = OB_SUCCESS;
  bool is_found_table = false;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else {
    const ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    is_used_hint = false;

    for (int64_t i = 0; OB_SUCC(ret) && !is_found_table && i < stmt_hint.indexes_.count(); ++i) {
      const ObIndexHint& cur_index_hint = stmt_hint.indexes_.at(i);
      if (table_id_ == cur_index_hint.table_id_) {
        is_found_table = true;
        if (cur_index_hint.valid_index_ids_.count() != 0) {
          index_hint = &cur_index_hint;
          is_used_hint = true;
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::print_no_use_late_materialization(planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_one_line = plan_text.is_oneline_;
  OutlineType outline_type = plan_text.outline_type_;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULl", K(ret));
  } else {
    ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    ObLogicalOperator* cur_op = get_parent();
    if (NULL != cur_op && log_op_def::LOG_SORT == cur_op->get_type() && NULL != (cur_op = cur_op->get_parent()) &&
        log_op_def::LOG_LIMIT == cur_op->get_type() && NULL == cur_op->get_parent()) {
      if (OUTLINE_DATA == outline_type ||
          (USED_HINT == outline_type && stmt_hint.use_late_mat_ == OB_NO_USE_LATE_MATERIALIZATION)) {
        if (!is_one_line && OB_FAIL(BUF_PRINTF("\n"))) {
        } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_one_line)))) {
        } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::NO_USE_LATE_MATERIALIZATION))) {
          LOG_WARN("fail to prinit outline", K(ret));
        } else { /*do nothing*/
        }
      }
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogTableScan::print_outline_data(planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_one_line = plan_text.is_oneline_;
  OutlineType type = plan_text.outline_type_;
  TableItem* table_item = NULL;
  ObString stmt_name;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULl", K(ret));
  } else if (OB_ISNULL(table_item = stmt_->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table item", K(ret), "table_id", table_id_);
  } else if (USED_HINT == type && OB_FAIL(stmt_->get_stmt_name(stmt_name))) {
    LOG_WARN("fail to get stmt_name", K(ret), K(stmt_->get_stmt_id()));
  } else if (OUTLINE_DATA == type && OB_FAIL(stmt_->get_stmt_org_name(stmt_name))) {
    LOG_WARN("fail to get stmt_name", K(ret), K(stmt_->get_stmt_id()));
  } else {
    if (ref_table_id_ == index_table_id_) {  // FULL_HINT
      if (NULL != get_parent() && log_op_def::LOG_JOIN == get_parent()->get_type() &&
          static_cast<ObLogJoin*>(get_parent())->is_late_mat()) {
      } else if (!is_one_line && OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_one_line)))) {
      } else if (OB_FAIL(BUF_PRINTF("%s(@\"%.*s\" \"%.*s.%.*s\"@\"%.*s\")",
                     ObStmtHint::FULL_HINT,
                     stmt_name.length(),
                     stmt_name.ptr(),
                     table_item->database_name_.length(),
                     table_item->database_name_.ptr(),
                     table_item->get_object_name().length(),
                     table_item->get_object_name().ptr(),
                     stmt_name.length(),
                     stmt_name.ptr()))) {
        LOG_WARN("fail to table scan hint", K(ret));
      }
    } else {  // INDEX_HINT
      // print no use late materialization
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100 && OB_FAIL(print_no_use_late_materialization(plan_text))) {
        LOG_WARN("fail to print hint", K(ret));
      }

      // print index
      if (OB_SUCC(ret)) {
        if (!is_one_line && OB_FAIL(BUF_PRINTF("\n"))) {
        } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_one_line)))) {
        } else if (OB_FAIL(BUF_PRINTF("%s(@\"%.*s\" \"%.*s.%.*s\"@\"%.*s\" \"%.*s\")",
                       ObStmtHint::INDEX_HINT,
                       stmt_name.length(),
                       stmt_name.ptr(),
                       table_item->database_name_.length(),
                       table_item->database_name_.ptr(),
                       table_name_.length(),
                       table_name_.ptr(),
                       stmt_name.length(),
                       stmt_name.ptr(),
                       get_index_name().length(),
                       get_index_name().ptr()))) {
          LOG_WARN("fail to table scan hint", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::print_outline(planText& plan_text)
{
  int ret = OB_SUCCESS;
  OutlineType outline_type = plan_text.outline_type_;
  switch (outline_type) {
    case OUTLINE_DATA: {
      if (OB_FAIL(print_outline_data(plan_text))) {
        LOG_WARN("fail to print index", K(ret));
      }
      break;
    }
    case USED_HINT: {
      if (OB_FAIL(print_used_index(plan_text))) {
        LOG_WARN("fail to print used_index", K(ret));
      }
      break;
    }
    default: {
      LOG_WARN("invalid outline_type", K(outline_type));
      break;
    }
  }
  return ret;
}

int ObLogTableScan::set_limit_size()
{
  int ret = OB_SUCCESS;
  int64_t limit_count = 0;
  int64_t offset_count = 0;
  bool re_est = false;
  bool is_null_value = false;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpeced null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_limit_value(limit_count_expr_,
                 get_plan()->get_stmt(),
                 get_plan()->get_optimizer_context().get_params(),
                 get_plan()->get_optimizer_context().get_session_info(),
                 &get_plan()->get_optimizer_context().get_allocator(),
                 limit_count,
                 is_null_value))) {
    LOG_WARN("Get limit count num error", K(ret));
  } else if (!is_null_value && OB_FAIL(ObTransformUtils::get_limit_value(limit_offset_expr_,
                                   get_plan()->get_stmt(),
                                   get_plan()->get_optimizer_context().get_params(),
                                   get_plan()->get_optimizer_context().get_session_info(),
                                   &get_plan()->get_optimizer_context().get_allocator(),
                                   offset_count,
                                   is_null_value))) {
    LOG_WARN("Get limit offset num error", K(ret));
  } else if (is_null_value) {
    set_card(0.0);
  } else {
    double limit_count_double = static_cast<double>(limit_count);
    double offset_count_double = static_cast<double>(offset_count);
    double scan_count_double = limit_count_double + offset_count_double;
    if (OB_FAIL(re_est_cost(NULL, scan_count_double, re_est))) {
      LOG_WARN("Re est cost error", K(ret));
    } else if (scan_count_double <= get_card()) {
      set_card(limit_count_double);
    } else {
      limit_count_double = get_card() - offset_count_double;
      set_card(limit_count_double >= 0.0 ? limit_count_double : 0.0);
    }
  }
  return ret;
}

int ObLogTableScan::is_used_join_type_hint(JoinAlgo join_algo, bool& is_used)
{
  int ret = OB_SUCCESS;
  is_used = false;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULl", K(ret));
  } else {
    const ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    int32_t table_id_idx = stmt_->get_table_bit_index(table_id_);
    if (OB_FAIL(stmt_hint.is_used_join_type(join_algo, table_id_idx, is_used))) {
      LOG_WARN("fail to judge whether use join type", K(ret), K(join_algo), K(table_id_idx));
    }
  }
  return ret;
}

int ObLogTableScan::is_used_in_leading_hint(bool& is_used)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULl", K(ret));
  } else {
    const ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    if (OB_FAIL(stmt_hint.is_used_in_leading(table_id_, is_used))) {
      LOG_WARN("fail to judge whether use leading", K(ret), K(table_id_), K(is_used));
    }
  }
  return ret;
}

int ObLogTableScan::get_path_ordering(ObIArray<ObRawExpr*>& order_exprs)
{
  int ret = OB_SUCCESS;
  if (range_columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Range columns count should not less than 1", K(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < range_columns_.count(); ++idx) {
      if (OB_FAIL(order_exprs.push_back(range_columns_.at(idx).expr_))) {
        LOG_WARN("Failed to add order expr", K(ret));
      }
    }
  }
  return ret;
}

int ObLogTableScan::allocate_granule_pre(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  if (ctx.managed_by_gi()) {
    gi_alloc_post_state_forbidden_ = true;
    LOG_TRACE("tsc in the partition wise state, forbidden table scan allocate gi", K(ref_table_id_));
  }
  return ret;
}

int ObLogTableScan::allocate_granule_post(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(table_partition_info_), K(ret));
  } else {
    ObTableLocation& table_location = table_partition_info_->get_table_location();
    if (gi_alloc_post_state_forbidden_) {
      /**
       * this tsc in a partition wise join/union sub plan,
       * so do not alloc a gi above.
       * but we still sign this table scan been charged by a granule iterator.
       * tsc will try to get scan task from gi operator if the gi_charged_ is true.
       */
      gi_charged_ = true;
      LOG_TRACE("set tsc been charged by gi");
    } else if (ctx.parallel_ >= 2 || OB_TBL_LOCATION_DISTRIBUTED == table_phy_location_type_ ||
               get_plan()->get_optimizer_context().is_batched_multi_stmt()) {
      /**
       * at px framework, if there is exchange above this tsc, we should alloc a granule iterator.
       */
      ctx.alloc_gi_ = true;
      ctx.partition_count_ = table_partition_info_->get_phy_tbl_location_info().get_phy_part_loc_info_list().count();
      gi_charged_ = true;

      ObDMLStmt* stmt = NULL;
      ObOptimizerContext* opt_ctx = NULL;
      ObSqlSchemaGuard* schema_guard = NULL;
      const ObTableSchema* table_schema = NULL;

      if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
          OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
          OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Get unexpected null", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id_, table_schema)) || OB_ISNULL(table_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("table schema is null", K(ref_table_id_), K(ret));
      } else {
        ctx.hash_part_ = table_schema->is_hash_part() || table_schema->is_hash_subpart() ||
                         table_schema->is_key_subpart() || table_schema->is_key_subpart();
      }
    } else {
      LOG_TRACE("do not alloc a granule iterator",
          K(stmt_->get_stmt_hint().get_parallel_hint()),
          K(sharding_info_),
          K(table_phy_location_type_));
    }
    ctx.tablet_size_ = table_location.get_tablet_size();
  }
  return ret;
}

int ObLogTableScan::is_table_get(bool& is_get) const
{
  int ret = OB_SUCCESS;
  if (pre_query_range_ != NULL) {
    if (OB_FAIL(pre_query_range_->is_get(is_get))) {
      LOG_WARN("check query range is table get", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObLogTableScan::is_need_feedback
 * in the following cases, we need to feedback table scan execution stats
 * 1. whole range scan, or very bad selectivity
 *
 * is_multi_partition_scan = true: table scan wrapped in table look up
 * @return
 */
bool ObLogTableScan::is_need_feedback() const
{
  bool ret = false;
  const int64_t SELECTION_THRESHOLD = 80;
  int64_t sel = (is_whole_range_scan() || table_row_count_ == 0)
                    ? 100
                    : static_cast<int64_t>(query_range_row_count_) * 100 / table_row_count_;

  ret = sel >= SELECTION_THRESHOLD && !is_multi_part_table_scan_;

  LOG_TRACE("is_need_feedback",
      K(estimate_method_),
      K(table_row_count_),
      K(query_range_row_count_),
      K(table_row_count_),
      K(sel),
      K(ret));
  return ret;
}

int ObLogTableScan::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(raw_exprs.append(access_exprs_));
  OZ(raw_exprs.append(limit_count_expr_));
  OZ(raw_exprs.append(limit_offset_expr_));
  OZ(raw_exprs.append(range_conds_));
  OZ(raw_exprs.append(part_filter_));
  OZ(raw_exprs.append(part_expr_));
  OZ(raw_exprs.append(subpart_expr_));
  if (is_multi_part_table_scan_) {
    OZ(raw_exprs.append(filter_exprs_));
  }
  for (int64_t i = 0; i < access_exprs_.count() && OB_SUCC(ret); ++i) {
    ObRawExpr* raw_expr = access_exprs_.at(i);
    if (raw_expr->get_expr_class() == ObRawExpr::EXPR_COLUMN_REF) {
      ObColumnRefRawExpr* col_ref = static_cast<ObColumnRefRawExpr*>(access_exprs_.at(i));
      if (OB_NOT_NULL(col_ref->get_real_expr())) {
        OZ(raw_exprs.append(col_ref->get_real_expr()));
      }
    }
  }
  return ret;
}

int ObLogTableScan::get_duplicate_table_replica(ObIArray<ObAddr>& valid_addrs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_partition_info_) || OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else {
    const ObPhyTableLocationInfo& phy_loc = table_partition_info_->get_phy_tbl_location_info();
    if (phy_loc.get_partition_cnt() < 1) {
      // do nothing
    } else if (!phy_loc.is_duplicate_table_not_in_dml()) {
      const ObPhyPartitionLocationInfo& phy_part_loc = phy_loc.get_phy_part_loc_info_list().at(0);
      ObReplicaLocation replica_location;
      if (OB_FAIL(phy_part_loc.get_selected_replica(replica_location))) {
        LOG_WARN("failed to get replica location", K(ret));
      } else if (OB_FAIL(valid_addrs.push_back(replica_location.server_))) {
        LOG_WARN("failed to push back server", K(ret));
      }
    } else {
      const ObPhyPartitionLocationInfo& phy_part_loc = phy_loc.get_phy_part_loc_info_list().at(0);
      const ObIArray<ObRoutePolicy::CandidateReplica>& replicas =
          phy_part_loc.get_partition_location().get_replica_locations();
      for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
        const ObAddr& addr = replicas.at(i).server_;
        bool valid_for_all_parts = true;
        for (int64_t j = 1; OB_SUCC(ret) && valid_for_all_parts && j < phy_loc.get_partition_cnt(); ++j) {
          int64_t index = OB_INVALID_INDEX;
          valid_for_all_parts = phy_loc.get_phy_part_loc_info_list().at(j).is_server_in_replica(addr, index);
        }
        if (OB_SUCC(ret) && valid_for_all_parts) {
          if (OB_FAIL(valid_addrs.push_back(addr))) {
            LOG_WARN("failed to push back server", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::set_duplicate_table_replica(const ObAddr& addr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_partition_info_) || OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt_));
  } else {
    ObPhyTableLocationInfo& phy_loc = table_partition_info_->get_phy_tbl_location_info_for_update();
    if (!phy_loc.is_duplicate_table_not_in_dml()) {
      // do nothing
    } else {
      int64_t replica_index = OB_INVALID_INDEX;
      for (int64_t i = 0; OB_SUCC(ret) && i < phy_loc.get_partition_cnt(); ++i) {
        ObPhyPartitionLocationInfo& phy_part_loc = phy_loc.get_phy_part_loc_info_list_for_update().at(i);
        if (!phy_part_loc.is_server_in_replica(addr, replica_index)) {
          // do nothing
        } else if (OB_UNLIKELY(replica_index == OB_INVALID_INDEX)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid replica index", K(ret));
        } else {
          phy_part_loc.set_selected_replica_idx(replica_index);
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::generate_link_sql_pre(GenLinkStmtContext& link_ctx)
{
  int ret = OB_SUCCESS;
  ObLinkStmt* link_stmt = link_ctx.link_stmt_;
  TableItem* table_item = NULL;
  if (OB_ISNULL(link_stmt) || !link_stmt->is_inited()) {
    // do nothing.
  } else if (dblink_id_ != link_ctx.dblink_id_) {
    link_ctx.dblink_id_ = OB_INVALID_ID;
    link_ctx.link_stmt_ = NULL;
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULl", K(ret));
  } else if (OB_ISNULL(table_item = stmt_->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table item", K(ret), "table_id", table_id_);
  } else if (link_stmt->is_select_strs_empty()) {
    if (OB_FAIL(fill_link_stmt(output_exprs_, *table_item, *link_stmt))) {
      LOG_WARN("failed to fill link stmt", K(ret));
    }
  } else {
    ObLinkStmt sub_link_stmt(link_stmt->alloc_, access_exprs_);
    const ObString& alias_name = (table_item->alias_name_.empty() ? table_item->table_name_ : table_item->alias_name_);
    if (OB_FAIL(sub_link_stmt.init(&link_ctx))) {
      LOG_WARN("failed to init sub link stmt", K(ret), K(dblink_id_));
    } else if (OB_FAIL(fill_link_stmt(access_exprs_, *table_item, sub_link_stmt))) {
      LOG_WARN("failed to fill sub link stmt", K(ret));
    } else if (OB_FAIL(link_stmt->fill_from_strs(sub_link_stmt, alias_name))) {
      LOG_WARN("failed to fill link stmt from strs with sub stmt", K(ret));
    }
  }
  return ret;
}

int ObLogTableScan::fill_link_stmt(
    const ObIArray<ObRawExpr*>& select_strs, const TableItem& table_item, ObLinkStmt& link_stmt)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> param_columns;
  if (OB_FAIL(link_stmt.force_fill_select_strs())) {
    LOG_WARN("failed to fill link stmt select strs", K(ret), K(select_strs));
  } else if (OB_FAIL(link_stmt.fill_from_strs(table_item))) {
    LOG_WARN("failed to fill sub link stmt from strs with table item", K(ret));
  } else if (OB_FAIL(link_stmt.fill_where_strs(range_conds_, true))) {
    LOG_WARN("failed to fill sub link stmt where strs with range conds", K(ret));
  } else if (OB_FAIL(link_stmt.fill_where_strs(filter_exprs_, true))) {
    LOG_WARN("failed to fill sub link stmt where strs with filters", K(ret));
  } else if (OB_FAIL(link_stmt.fill_where_rownum(limit_count_expr_))) {
    LOG_WARN("failed to fill sub link stmt where rownum with limit count expr", K(ret));
  } else if (OB_FAIL(link_stmt.get_nl_param_columns(range_conds_, access_exprs_, param_columns))) {
    LOG_WARN("failed to get nl param columns", K(ret), K(range_conds_));
  } else if (OB_FAIL(link_stmt.get_nl_param_columns(filter_exprs_, access_exprs_, param_columns))) {
    LOG_WARN("failed to get nl param columns", K(ret), K(filter_exprs_));
  } else if (OB_FAIL(link_stmt.append_select_strs(param_columns))) {
    LOG_WARN("failed to append param columns", K(ret), K(param_columns));
  }
  return ret;
}

int ObLogTableScan::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < extra_access_exprs_for_lob_col_.count(); ++i) {
    if (OB_ISNULL(expr = extra_access_exprs_for_lob_col_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(checker.check(*expr))) {
      LOG_WARN("failed to expr", K(ret), KPC(expr));
    }
  }
  return ret;
}

// if we got lob type column in output expr, we need to project all columns that
// rowid pseudo column need(pk and part key)(lob locator need rowid)
int ObLogTableScan::try_add_extra_access_exprs_for_lob_col()
{
  int ret = OB_SUCCESS;
  bool has_lob_col = false;
  ObSEArray<ObRawExpr*, 4> temp_access_exprs;
  ObRawExpr* expr = NULL;
  if (lib::is_oracle_mode()) {
    ObSEArray<ObIArray<ObRawExpr*>*, 2> arrs;
    if (OB_FAIL(arrs.push_back(&output_exprs_)) || OB_FAIL(arrs.push_back(&access_exprs_))) {
      LOG_WARN("failed to push arr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < arrs.count(); ++i) {
      ObIArray<ObRawExpr*>* arr = arrs.at(i);
      CK(OB_NOT_NULL(arr));
      for (int64_t i = 0; OB_SUCC(ret) && i < arr->count() && !has_lob_col; ++i) {
        CK(OB_NOT_NULL(expr = arr->at(i)));
        if (T_REF_COLUMN == expr->get_expr_type()) {
          if (expr->get_result_type().is_lob_locator() && !has_exist_in_array(extra_access_exprs_for_lob_col_, expr)) {
            has_lob_col = true;
          }
        }
      }
    }
    if (OB_SUCC(ret) && has_lob_col) {
      if (OB_FAIL(generate_rowkey_and_partkey_exprs(temp_access_exprs, has_lob_col))) {
        LOG_WARN("failed to generate rowkey access expr", K(ret));
      } else {
        extra_access_exprs_for_lob_col_.assign(temp_access_exprs);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_access_exprs.count(); ++i) {
      if (OB_ISNULL(expr = temp_access_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(i));
      } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs_, expr))) {
        LOG_WARN("failed to add expr to array", K(ret), KPC(expr));
      }
    }
  }
  return ret;
}
