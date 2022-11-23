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
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/dblink/ob_dblink_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using oceanbase::share::schema::ObTableSchema;
using oceanbase::share::schema::ObSchemaGetterGuard;

const char *ObLogTableScan::get_name() const
{
  bool is_get = false;
  const char *name = NULL;
  int ret = OB_SUCCESS;
  SampleInfo::SampleMethod sample_method = get_sample_info().method_;
  if (NULL != pre_query_range_ && OB_FAIL(get_pre_query_range()->is_get(is_get))) {
    // is_get always return true
    LOG_WARN("failed to get is_get", K(ret));
    is_get = false;
  }
  if (sample_method != SampleInfo::NO_SAMPLE) {
    name = (sample_method == SampleInfo::ROW_SAMPLE) ? "ROW SAMPLE SCAN" : "BLOCK SAMPLE SCAN";
  } else if (use_das()) {
    name = is_get ? "DISTRIBUTED TABLE GET" : "DISTRIBUTED TABLE SCAN";
  } else {
    name = is_get ? "TABLE GET" : "TABLE SCAN";
  }
  return name;
}

void ObLogTableScan::set_ref_table_id(uint64_t ref_table_id)
{ 
  ref_table_id_ = ref_table_id;
}

int ObLogTableScan::set_range_columns(const ObIArray<ColumnItem> &range_columns)
{
  int ret = OB_SUCCESS;
  range_columns_.reset();
  if (OB_FAIL(range_columns_.assign(range_columns))) {
    LOG_WARN("failed to assign range columns", K(ret));
  } else if (!is_virtual_table(ref_table_id_)) {
    // banliu.zyd: 虚拟表不保序，仅对非虚拟表做处理
    OrderItem item;
    int64_t N = range_columns.count();
    reset_op_ordering();
    common::ObIArray<OrderItem> &op_ordering = get_op_ordering();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      item.expr_ = range_columns.at(i).expr_;
      item.order_type_ = scan_direction_;
      if (OB_FAIL(op_ordering.push_back(item))) {
        LOG_WARN("failed to push back item", K(ret));
      }
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObLogTableScan::compute_property(Path *path)
{
  int ret = OB_SUCCESS;
  const ObJoinOrder *join_order = NULL;
  if (OB_ISNULL(path) || OB_ISNULL(join_order = path->parent_) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path is invalid", K(ret), K(path), K(join_order), K(get_plan()));
  } else if (OB_FAIL(ObLogicalOperator::compute_property(path))) {
    LOG_WARN("failed to compute property", K(ret));
  } else if (path->is_access_path()) {
    // process cost, card and width
    AccessPath *ap = static_cast<AccessPath *>(path);
    bool is_index_back = ap->est_cost_info_.index_meta_info_.is_index_back_;
    if (ap->is_global_index_ && is_index_back) {
      set_cost(ap->cost_ - ap->index_back_cost_);
      set_op_cost(get_cost());
      set_card(index_back_row_count_);
    } else {
      set_cost(ap->cost_);
      set_op_cost(ap->op_cost_);
      set_card(path->get_path_output_rows());
    }
    set_width(join_order->get_output_row_size());
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("table scan:", K(get_card()), K(get_width()), K(get_cost()));
  }
  return ret;
}

int ObLogTableScan::re_est_cost(EstimateCostInfo &param, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  double index_back_cost = 0.0;
  if (OB_FAIL(re_est_cost(param, card, index_back_cost, cost))) {
    LOG_WARN("failed re est table scan cost", K(ret));
  }
  return ret;
}

int ObLogTableScan::re_est_cost(EstimateCostInfo &param, double &card, double &index_back_cost, double &cost)
{
  int ret = OB_SUCCESS;
  int64_t limit_count = 0;
  int64_t offset_count = 0;
  bool is_null_value = false;
  index_back_cost = 0.0;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(access_path_)) {
    card = get_card();
    cost = get_cost();
  } else if (NULL != limit_count_expr_ &&
             OB_FAIL(ObTransformUtils::get_limit_value(limit_count_expr_,
                                                       get_stmt(),
                                                       get_plan()->get_optimizer_context().get_params(),
                                                       get_plan()->get_optimizer_context().get_exec_ctx(),
                                                       &get_plan()->get_optimizer_context().get_allocator(),
                                                       limit_count,
                                                       is_null_value))) {
    LOG_WARN("Get limit count num error", K(ret));
  } else if (!is_null_value &&
             NULL != limit_offset_expr_ &&
             OB_FAIL(ObTransformUtils::get_limit_value(limit_offset_expr_,
                                                       get_stmt(),
                                                       get_plan()->get_optimizer_context().get_params(),
                                                       get_plan()->get_optimizer_context().get_exec_ctx(),
                                                       &get_plan()->get_optimizer_context().get_allocator(),
                                                       offset_count,
                                                       is_null_value))) {
    LOG_WARN("Get limit offset num error", K(ret));
  } else {
    double limit_count_double = static_cast<double>(limit_count);
    double offset_count_double = static_cast<double>(offset_count);
    if (NULL != limit_count_expr_) {
      if (param.need_row_count_ < 0 || limit_count_double < param.need_row_count_) {
        param.need_row_count_ = limit_count_double + offset_count_double;
      } else {
        param.need_row_count_ += offset_count_double;
      }
    }
    if (OB_FAIL(access_path_->re_estimate_cost(param, card, index_back_cost, cost))) {
      LOG_WARN("failed to re est cost", K(ret));
    } else {
      bool is_index_back = access_path_->est_cost_info_.index_meta_info_.is_index_back_;
      //全局索引回表的代价由table look up算子展示
      if (access_path_->is_global_index_ && is_index_back) {
        cost -= index_back_cost;
      }
      if (NULL != limit_count_expr_) {
        card = limit_count_double < card ? limit_count_double : card;
      }
      if (param.override_) {
        set_card(card);
        set_op_cost(cost);
        set_cost(cost);
      }
    }
  }
  return ret;
}

int ObLogTableScan::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(generate_access_exprs())) {
    LOG_WARN("failed to generate acess exprs", K(ret));
  } else if (NULL != limit_count_expr_ &&
             OB_FAIL(all_exprs.push_back(limit_count_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (NULL != limit_offset_expr_ &&
             OB_FAIL(all_exprs.push_back(limit_offset_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (NULL != fq_expr_ && OB_FAIL(all_exprs.push_back(fq_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (NULL != tablet_id_expr_ && OB_FAIL(all_exprs.push_back(tablet_id_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (NULL != calc_part_id_expr_ && OB_FAIL(all_exprs.push_back(calc_part_id_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(append(all_exprs, access_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, pushdown_aggr_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogTableScan::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs_.count(); i++) {
    ObRawExpr *expr = access_exprs_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx))) {
      LOG_WARN("failed to mark expr as produced", K(*expr), K(branch_id_), K(id_), K(ret));
    } else { /*do nothing*/ }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_aggr_exprs_.count(); i++) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = pushdown_aggr_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx))) {
      LOG_WARN("failed to mark expr as produced", K(*expr), K(branch_id_), K(id_), K(ret));
    } else { /*do nothing*/ }
  }

  // check if we can produce some more exprs, such as 1 + 'c1' after we have produced 'c1'
  if (OB_SUCC(ret)) {
    if (is_index_global_ && is_global_index_back_) {
      if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
        LOG_WARN("failed to allocate expr post", K(ret));
      }
    } else {
      if (!is_plan_root() && OB_FAIL(append(output_exprs_, access_exprs_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(append(output_exprs_, pushdown_aggr_exprs_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
        LOG_WARN("failed to allocate expr post", K(ret));
      } else { /*do nothing*/ }
    }
  }
   // add special exprs to all exprs
  if (OB_SUCC(ret)) {
    ObRawExprUniqueSet &all_exprs = get_plan()->get_optimizer_context().get_all_exprs();
    if (NULL != part_expr_ && OB_FAIL(all_exprs.append(part_expr_))) {
      LOG_WARN("failed to get part expr", K(ret));
    } else if (NULL != subpart_expr_ && OB_FAIL(all_exprs.append(subpart_expr_))) {
      LOG_WARN("failed to get subpart expr", K(ret));
    } else if (OB_FAIL(all_exprs.append(range_conds_))) {
      LOG_WARN("failed to append range exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < real_expr_map_.count(); ++i) {
        if (OB_FAIL(all_exprs.append(real_expr_map_.at(i).second))) {
          LOG_WARN("failed to append real expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::check_output_dependance(common::ObIArray<ObRawExpr *> &child_output, PPDeps &deps)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> exprs;
  LOG_TRACE("start to check output exprs", K(type_), K(child_output), K(deps));
  ObRawExprCheckDep dep_checker(child_output, deps, true);
  if (OB_FAIL(append(exprs, filter_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(exprs, output_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(exprs, rowkey_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(exprs, part_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (nullptr != group_id_expr_
             && OB_FAIL(add_var_to_array_no_dup(exprs, group_id_expr_))) {
    LOG_WARN("failed to push back group id expr", K(ret));
  } else if (OB_FAIL(dep_checker.check(exprs))) {
    LOG_WARN("failed to check op_exprs", K(ret));
  } else {
    LOG_TRACE("succeed to check output exprs", K(exprs), K(type_), K(deps));
  }
  return ret;
}

int ObLogTableScan::generate_access_exprs()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(get_stmt()), K(ret));
  } else if (OB_FAIL(generate_necessary_rowkey_and_partkey_exprs())) {
    LOG_WARN("failed to generate rowkey and part exprs", K(ret));
  } else if (use_batch()
    && OB_FAIL(ObOptimizerUtil::allocate_group_id_expr(get_plan(), group_id_expr_))) {
    LOG_WARN("allocate group id expr failed", K(ret));
  } else if (nullptr != group_id_expr_ && OB_FAIL(access_exprs_.push_back(group_id_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(append_array_no_dup(access_exprs_, rowkey_exprs_))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(access_exprs_, part_exprs_))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (is_index_global_ && is_global_index_back_) {
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filter_exprs_, temp_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(access_exprs_, temp_exprs))) {
      LOG_WARN("failed to append array no dup", K(ret));
    } else { /*do nothing*/}
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); i++) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(col_item), K(ret));
      } else if (col_item->table_id_ == table_id_ && col_item->expr_->is_explicited_reference() &&
                 OB_FAIL(temp_exprs.push_back(col_item->expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/}
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(append_array_no_dup(access_exprs_, temp_exprs))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else { /*do nothing*/ }

    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_pseudo_column_like_exprs().count(); i++) {
      ObRawExpr *expr = stmt->get_pseudo_column_like_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (T_ORA_ROWSCN == expr->get_expr_type() &&
          static_cast<ObPseudoColumnRawExpr*>(expr)->get_table_id() == table_id_ &&
          OB_FAIL(access_exprs_.push_back(expr))) {
        LOG_WARN("failed to get next row", K(ret));
      } else { /*do nothing*/}
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_mapping_columns_for_vt(access_exprs_))) {
        LOG_WARN("failed to add mapping columns for vt", K(ret));
      } else {
        LOG_TRACE("succeed to generate access exprs", K(access_exprs_));
      }
    }
  }
  return ret;
}

int ObLogTableScan::generate_necessary_rowkey_and_partkey_exprs()
{
  int ret = OB_SUCCESS;
  bool has_lob_column = false;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  bool is_heap_table = false;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id_, table_schema,
                                         ObSqlSchemaGuard::is_link_table(get_stmt(), table_id_)))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (table_schema != NULL && FALSE_IT(is_heap_table = table_schema->is_heap_table())) {
  } else if (OB_FAIL(get_stmt()->has_lob_column(table_id_, has_lob_column))) {
    LOG_WARN("failed to check whether stmt has lob column", K(ret));
  } else if (has_lob_column || (is_index_global_ && is_global_index_back_) || get_index_back()) {
    if (is_heap_table && is_index_global_ && is_global_index_back_) {
      if (OB_FAIL(get_part_column_exprs(table_id_, ref_table_id_, part_exprs_))) {
        LOG_WARN("failed to get part column exprs", K(ret));
      }
    } else if (has_lob_column) {
      ObSEArray<ObRawExpr*, 8> tmp_part_exprs;
      if (OB_FAIL(get_part_column_exprs(table_id_, ref_table_id_, tmp_part_exprs))) {
        LOG_WARN("failed to get part column exprs", K(ret));
      } else if ((is_index_global_ && is_global_index_back_) || get_index_back()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_part_exprs.count(); ++i) {
          ObRawExpr *expr = tmp_part_exprs.at(i);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (expr->is_column_ref_expr() &&
                     static_cast<ObColumnRefRawExpr *>(expr)->is_virtual_generated_column()) {
            // do nothing
          } else if (OB_FAIL(part_exprs_.push_back(expr))) {
            LOG_WARN("failed to push back part expr", K(ret));
          }
        }
      } else if (OB_FAIL(append(part_exprs_, tmp_part_exprs))) {
        LOG_WARN("failed to appen part exprs", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_plan()->get_rowkey_exprs(table_id_, ref_table_id_, rowkey_exprs_))) {
      LOG_WARN("failed to generate rowkey exprs", K(ret));
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }
  return ret;
}

int ObLogTableScan::add_mapping_columns_for_vt(ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("debug mapping columns for virtual table", K(ref_table_id_));
  if (0 < access_exprs.count()) {
    ObColumnRefRawExpr *first_col_expr = static_cast<ObColumnRefRawExpr*>(access_exprs.at(0));
    LOG_DEBUG("debug mapping columns for virtual table", K(first_col_expr->get_table_id()), K(ref_table_id_));
    if (share::is_oracle_mapping_real_virtual_table(ref_table_id_)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); ++i) {
        ObRawExpr *real_expr = NULL;
        if (OB_FAIL(add_mapping_column_for_vt(static_cast<ObColumnRefRawExpr *>(access_exprs.at(i)),
                                              real_expr))) {
          LOG_WARN("failed to add mapping column for vt", K(ret));
        } else if (OB_FAIL(real_expr_map_.push_back(
                             std::pair<ObRawExpr *, ObRawExpr *>(access_exprs.at(i), real_expr)))) {
          LOG_WARN("failed to push back real expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::add_mapping_column_for_vt(ObColumnRefRawExpr *col_expr,
                                              ObRawExpr *&real_expr)
{
  int ret = OB_SUCCESS;
  int64_t mapping_table_id = share::schema::ObSchemaUtils::get_real_table_mappings_tid(ref_table_id_);
  if (OB_INVALID_ID == mapping_table_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: failed to get real table id", K(ret));
  }
  const share::schema::ObColumnSchemaV2 *col_schema = NULL;
  const ObTableSchema *table_schema = NULL;
  ObColumnRefRawExpr *mapping_col_expr = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(get_plan()) ||
             OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
             OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id_, mapping_table_id, get_stmt(), table_schema))) {
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
    //column maybe from alias table, so must reset ref id by table id from table_item
    mapping_col_expr->set_ref_id(mapping_table_id, col_schema->get_column_id());
    mapping_col_expr->set_lob_column(col_expr->is_lob_column());
    real_expr = mapping_col_expr;
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
    for (int64_t i = 0; OB_SUCC(ret) && column_found && i < access_exprs_.count(); ++i) {
      const ObColumnRefRawExpr *expr = NULL;
      if (OB_ISNULL(expr = static_cast<const ObColumnRefRawExpr*>(access_exprs_.at(i)))) {
    	  ret = OB_ERR_UNEXPECTED;
    	  LOG_WARN("get unexpected null", K(ret));
      } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
        column_found = false;
      } else if (T_PSEUDO_GROUP_ID == expr->get_expr_type()) {
        // do nothing
      } else if (OB_UNLIKELY(!expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr type", K(expr->get_expr_type()), K(ret));
      } else {
        const uint64_t column_id = expr->get_column_id();
        column_found = false;
        if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == column_id) {
          // rowid can be computed directly from index table.
          column_found = true;
        }
        for (int64_t col_idx = 0;
             OB_SUCC(ret) && !column_found && col_idx < idx_columns_.count();
             ++col_idx) {
          if (column_id == idx_columns_.at(col_idx)) {
            column_found = true;
          } else { /* Do nothing */ }
        }
        column_found = ObOptimizerUtil::find_item(idx_columns_,
            static_cast<const ObColumnRefRawExpr*>(expr)->get_column_id());
      }
    } //end for
  }

  if (OB_SUCC(ret)) {
    index_back_ = !column_found;
    if (OB_FAIL(filter_before_index_back_set())) {
      LOG_WARN("Failed to filter_before_index_back_set", K(ret));
    } else {/*Do nothing*/}
  } else { /* Do nothing */ }

  return ret;
}

int ObLogTableScan::filter_before_index_back_set()
{
  int ret = OB_SUCCESS;
  filter_before_index_back_.reset();
  if (index_back_) {
    if (OB_FAIL(ObOptimizerUtil::check_filter_before_indexback(filter_exprs_,
                                                               idx_columns_,
                                                               filter_before_index_back_))) {
      LOG_WARN("failed to check filter before index back", K(ret));
    } else { /*do nothing*/ }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs_.count(); ++i) {
      if (OB_FAIL(filter_before_index_back_.push_back(false))) {
        LOG_WARN("failed to add bool to filter_before_index_back_", K(ret));
      } else { /* Do nothing */ }
    }
  }
  LOG_TRACE("succeed to check filter before index back", K(filter_before_index_back_),
      K(index_back_), K(ret));
  return ret;
}

int ObLogTableScan::set_table_scan_filters(const common::ObIArray<ObRawExpr *> &filters)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_filter_exprs().assign(filters))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else if (OB_FAIL(pick_out_query_range_exprs())) {
    LOG_WARN("failed to pick out query range exprs", K(ret));
  } else if (OB_FAIL(pick_out_startup_filters())) {
    LOG_WARN("failed to pick out startup filters", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogTableScan::pick_out_query_range_exprs()
{
  int ret = OB_SUCCESS;
  bool is_get = false;
  ObOptimizerContext *opt_ctx = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  const share::schema::ObTableSchema *index_schema = NULL;
  /*
  * virtual table may have hash index,
  * for hash index, if it is a get, we should still extract the range condition
  */
  if (OB_ISNULL(get_plan())
      || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())
      || OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("NULL pointer error", K(get_plan()), K(opt_ctx), K(schema_guard), K(ret));
  } else if (get_contains_fake_cte()) {
    // do nothing
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id_, ref_table_id_, get_stmt(), index_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(is_table_get(is_get))) {
    LOG_WARN("failed to check is table get", K(ret));
  } else if ((index_schema->is_ordered() || is_get) && NULL != pre_query_range_) {
    const ObIArray<ObRawExpr *> &range_exprs = pre_query_range_->get_range_exprs();
    ObArray<ObRawExpr *> filter_exprs;
    if (OB_FAIL(filter_exprs.assign(filter_exprs_))) {
      LOG_WARN("assign filter exprs failed", K(ret));
    } else {
      filter_exprs_.reset();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); ++i) {
      bool found_expr = false;
      for (int64_t j = 0; OB_SUCC(ret) && !found_expr && j < range_exprs.count(); ++j) {
        if (filter_exprs.at(i) == range_exprs.at(j)) {
          //有重复表达式，忽略掉
          found_expr = true;
        } else { /* Do nothing */ }
      }
      // for virtual table, even if we extract query range, we need to maintain the condition into the filter
      if (OB_SUCC(ret) && (!found_expr || (is_virtual_table(ref_table_id_)))) {
        if (OB_FAIL(filter_exprs_.push_back(filter_exprs.at(i)))) {
          LOG_WARN("add filter expr failed", K(i), K(ret));
        } else { /* Do nothing */ }
      }
      if (OB_SUCC(ret) && found_expr) {
        if (OB_FAIL(range_conds_.push_back(filter_exprs.at(i)))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/}
      }
    } //end for
  } else { /*do nothing*/ }
  return ret;
}

int ObLogTableScan::pick_out_startup_filters()
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = get_plan();
  const ParamStore *params = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  ObArray<ObRawExpr *> filter_exprs;
  if (OB_ISNULL(plan)
      || OB_ISNULL(opt_ctx = &plan->get_optimizer_context())
      || OB_ISNULL(params = opt_ctx->get_params())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("NULL pointer error", K(plan), K(opt_ctx), K(ret));
  } else if (OB_FAIL(filter_exprs.assign(filter_exprs_))) {
    LOG_WARN("assign filter exprs failed", K(ret));
  } else {
    filter_exprs_.reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); ++i) {
    ObRawExpr *qual = filter_exprs.at(i);
    if (OB_ISNULL(qual)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (qual->is_static_const_expr()) {
      if (OB_FAIL(startup_exprs_.push_back(qual))) {
        LOG_WARN("add filter expr failed", K(i), K(ret));
      } else { /* Do nothing */ }
    } else if (OB_FAIL(filter_exprs_.push_back(qual))) {
      LOG_WARN("add filter expr failed", K(i), K(ret));
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObLogTableScan::init_calc_part_id_expr()
{
  int ret = OB_SUCCESS;
  calc_part_id_expr_ = NULL;
  share::schema::ObPartitionLevel part_level = share::schema::PARTITION_LEVEL_MAX;
  ObSQLSessionInfo *session = NULL;
  ObRawExpr *part_expr = NULL;
  ObRawExpr *subpart_expr = NULL;
  ObRawExpr *new_part_expr = NULL;
  ObRawExpr *new_subpart_expr = NULL;
  if (OB_ISNULL(get_plan()) || OB_UNLIKELY(OB_INVALID_ID == ref_table_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(ret), K(ref_table_id_));
  } else if (OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    share::schema::ObSchemaGetterGuard *schema_guard = NULL;
    const share::schema::ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(
               session->get_effective_tenant_id(),
               ref_table_id_, table_schema))) {
      LOG_WARN("get table schema failed", K(ref_table_id_), K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret), K(table_schema));
    } else if (OB_FAIL(get_plan()->gen_calc_part_id_expr(table_id_,
                                                         ref_table_id_,
                                                         CALC_PARTITION_TABLET_ID,
                                                         calc_part_id_expr_))) {
      LOG_WARN("failed to build calc part id expr", K(ret));
    } else if (!table_schema->is_heap_table() &&
               OB_NOT_NULL(calc_part_id_expr_) &&
               OB_FAIL(replace_gen_column(calc_part_id_expr_, calc_part_id_expr_))) {
      LOG_WARN("failed to replace gen column", K(ret));
    } else {
      // For no-pk table partitioned by generated column, it is no need to replace generated
      // column as dependent exprs, because index table scan will add dependant columns
      // into access_exprs_
      // eg:
      // create table t1(c1 int, c2 int, c3 int generated always as (c1 + 1)) partition by hash(c3);
      // create index idx on t1(c2) global;
      // select /*+ index(t1 idx) */ * from t1\G
      // TLU  
      //  TSC // output([pk_inc],[calc_part_id_expr(c3)], [c1]), access([pk_inc], [c1])

      // For pk table partitioned by generated column, we can replace generated column by
      // dependant exprs, because pk must be a superset of partition columns
      // eg:
      // create table t1(c1 int primary key, c2 int, c3 int generated always as (c1 + 1)) partition by hash(c3);
      // create index idx on t1(c2) global;
      // select /*+ index(t1 idx) */ * from t1\G
      // TLU  
      //  TSC // output([c1],[calc_part_id_expr(c1 + 1)]), access([c1])
      LOG_TRACE("log table scan init calc part id expr", KPC(calc_part_id_expr_));
    }
  }

  return ret;
}

int ObLogTableScan::replace_gen_column(ObRawExpr *part_expr, ObRawExpr *&new_part_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> column_exprs;
  new_part_expr = part_expr;
  if (OB_ISNULL(part_expr)) {
    // do nothing
  } else if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, column_exprs))) {
    LOG_WARN("fail to extract column exprs", K(part_expr), K(ret));
  } else {
    ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
    bool cnt_gen_columns = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
      if (OB_ISNULL(column_exprs.at(i)) ||
          OB_UNLIKELY(!column_exprs.at(i)->is_column_ref_expr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else {
        ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr *>(column_exprs.at(i));
        if (!col->is_generated_column()) {
          // do nothing
        } else if (OB_ISNULL(col->get_dependant_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dependant expr is null", K(ret), K(*col));
        } else if (OB_FAIL(copier.add_replaced_expr(col, col->get_dependant_expr()))) {
          LOG_WARN("failed to add replace pair", K(ret));
        } else {
          cnt_gen_columns = true;
        }
      }
    }
    if (OB_SUCC(ret) && cnt_gen_columns) {
      if (OB_FAIL(copier.copy_on_replace(part_expr, new_part_expr))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      }
    }
  }
  return ret;
}

uint64_t ObLogTableScan::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = do_hash(table_name_, hash_value);
  if (!index_name_.empty()) {
    hash_value = do_hash(index_name_, hash_value);
  }
  hash_value = do_hash(sample_info_.method_, hash_value);
  hash_value = do_hash(use_das_, hash_value);
  LOG_TRACE("TABLE SCAN hash value", K(hash_value), K(table_name_), K(index_name_), K(get_name()));
  hash_value = ObLogicalOperator::hash(hash_value);
  return hash_value;
}

int ObLogTableScan::print_my_plan_annotation(char *buf,
                                             int64_t &buf_len,
                                             int64_t &pos,
                                             ExplainType type)
{
  int ret = OB_SUCCESS;
  // print access
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }

  if (OB_SUCC(ret)) {
    const ObIArray<ObRawExpr*> &access = get_access_exprs();
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
      if (OB_FAIL(explain_print_partitions(*table_partition_info_, buf, buf_len, pos))) {
        LOG_WARN("Failed to print partitions");
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(print_limit_offset_annotation(buf, buf_len, pos, type))) {
      LOG_WARN("print limit offset annotation failed", K(ret), K(buf_len), K(pos), K(type));
    }
  }

  if (OB_SUCC(ret)
      && (EXPLAIN_EXTENDED == type
          || EXPLAIN_EXTENDED_NOADDR == type
          || EXPLAIN_PLANREGRESS == type)) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("is_index_back=%s", index_back_ ? "true" : "false"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */ }

    if (OB_SUCC(ret) && (0 != filter_before_index_back_.count())) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(print_filter_before_indexback_annotation(buf, buf_len, pos))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    }
  }

  //Print ranges
  if (OB_SUCC(ret)
      && (EXPLAIN_EXTENDED == type
          || EXPLAIN_EXTENDED_NOADDR == type
          || EXPLAIN_PLANREGRESS == type)
      && range_columns_.count() > 0) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(print_range_annotation(buf, buf_len, pos, type))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }

  return ret;
}

int ObLogTableScan::explain_index_selection_info(char *buf,
                                                 int64_t &buf_len,
                                                 int64_t &pos)
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
      if (OB_FAIL(BUF_PRINTF("%.*s:", table_name_.length(), table_name_.ptr()))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_UNLIKELY(estimate_method_ < 0 || estimate_method_ > 5)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid array pos", K(estimate_method_), K(ret));
      } else if (OB_FAIL(BUF_PRINTF("table_rows:%ld, physical_range_rows:%ld, logical_range_rows:%ld, index_back_rows:%ld, output_rows:%ld, est_method:%s",
                              static_cast<int64_t>(table_row_count_),
                              static_cast<int64_t>(phy_query_range_row_count_),
                              static_cast<int64_t>(query_range_row_count_),
                              static_cast<int64_t>(index_back_row_count_),
                              static_cast<int64_t>(output_row_count_),
                              ObOptEstCost::get_method_name(estimate_method_)))) {
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
        if (OB_FAIL(BUF_PRINTF("heuristic_rule=%.*s\n", heuristic_rule_name.length(),
                                heuristic_rule_name.ptr()))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* do nothing*/ }
      } else {
        // print available index id
        if (OB_FAIL(BUF_PRINTF("avaiable_index_name["))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < table_opt_info_->available_index_name_.count(); ++i) {
          if (OB_FAIL(BUF_PRINTF("%.*s", table_opt_info_->available_index_name_.at(i).length(),
                      table_opt_info_->available_index_name_.at(i).ptr()))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else if (i != table_opt_info_->available_index_name_.count() - 1) {
            if (OB_FAIL(BUF_PRINTF(","))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            } else { /* do nothing*/ }
          } else { /* do nothing*/ }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(BUF_PRINTF("]"))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else { /* Do nothing */ }
        } else { /* Do nothing */ }

        // print pruned index name
        if (OB_SUCC(ret) && table_opt_info_->pruned_index_name_.count() > 0) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else if (OB_FAIL(BUF_PRINTF("pruned_index_name["))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < table_opt_info_->pruned_index_name_.count(); ++i) {
              if (OB_FAIL(BUF_PRINTF("%.*s", table_opt_info_->pruned_index_name_.at(i).length(),
                                    table_opt_info_->pruned_index_name_.at(i).ptr()))) {
                LOG_WARN("BUF_PRINTF fails", K(ret));
              } else if (i != table_opt_info_->pruned_index_name_.count() - 1) {
                if (OB_FAIL(BUF_PRINTF(","))) {
                  LOG_WARN("BUF_PRINTF fails", K(ret));
                } else { /* do nothing*/ }
              } else { /* do nothing*/ }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(BUF_PRINTF("]"))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            } else { /* Do nothing */ }
          } else { /* Do nothing */ }
        }
        // print unstable index name
        if (OB_SUCC(ret) && table_opt_info_->unstable_index_name_.count() > 0) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else if (OB_FAIL(BUF_PRINTF("unstable_index_name["))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < table_opt_info_->unstable_index_name_.count(); ++i) {
              if (OB_FAIL(BUF_PRINTF("%.*s", table_opt_info_->unstable_index_name_.at(i).length(),
                                    table_opt_info_->unstable_index_name_.at(i).ptr()))) {
                LOG_WARN("BUF_PRINTF fails", K(ret));
              } else if (i != table_opt_info_->unstable_index_name_.count() - 1) {
                if (OB_FAIL(BUF_PRINTF(","))) {
                  LOG_WARN("BUF_PRINTF fails", K(ret));
                } else { /* do nothing*/ }
              } else { /* do nothing*/ }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(BUF_PRINTF("]"))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            } else { /* Do nothing */ }
          } else { /* Do nothing */ }
        }

        // print est row count infos
        if (OB_SUCC(ret) && est_records_.count() > 0) {
          if (OB_FAIL(BUF_PRINTF(", estimation info[table_id:%ld,", est_records_.at(0).table_id_))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < est_records_.count(); ++i) {
            const ObEstRowCountRecord &record = est_records_.at(i);
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
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObLogTableScan::print_filter_before_indexback_annotation(char *buf,
                                                             int64_t buf_len,
                                                             int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF("filter_before_indexback["))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */ }

  for (int64_t i = 0; OB_SUCC(ret) && i < filter_before_index_back_.count(); ++i) {
    if (filter_before_index_back_.at(i)) {
      if (OB_FAIL(BUF_PRINTF("true"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    } else {
      if (OB_FAIL(BUF_PRINTF("false"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    }
    if ((filter_before_index_back_.count() - 1) != i) {
      if (OB_FAIL(BUF_PRINTF(","))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    } else { /* Do nothing */ }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(BUF_PRINTF("]"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */ }
  } else { /* Do nothing */ }

  return ret;
}

int ObLogTableScan::print_ranges(char *buf,
                                 int64_t buf_len,
                                 int64_t &pos,
                                 const ObIArray<ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    if (i >= 1) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    } else { /* Do nothing */ }

    if (OB_SUCC(ret)) {
      pos += ranges.at(i).to_plain_string(buf + pos, buf_len - pos);
    }
  }
  return ret;
}

int ObLogTableScan::print_range_annotation(char *buf,
                                           int64_t buf_len,
                                           int64_t &pos,
                                           ExplainType type)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> range_key;
  for (int64_t i = 0; OB_SUCC(ret) && i < range_columns_.count(); ++i) {
    if (OB_ISNULL(range_columns_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Expr in column item should not be NULL", K(i), K(ret));
    } else if (OB_FAIL(range_key.push_back(range_columns_.at(i).expr_))) {
      LOG_WARN("Fail to add range expr", K(i), K(ret));
    } else { /* Do nothing */ }
  }
  if (OB_SUCC(ret)) {
    EXPLAIN_PRINT_EXPRS(range_key, type);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("range"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    } else { /* Do nothing */ }
  } else { /* Do nothing */ }
  //When range is empty. Range is always true.
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
    } else { /* Do nothing */ }
  } else { /* Do nothing */ }

  if (OB_SUCC(ret)) {
    ret = print_ranges(buf, buf_len, pos, ranges_);
  }

  if (OB_SUCC(ret)) {
    if (!range_conds_.empty()) {
      //print range condition
      const ObIArray<ObRawExpr*> &range_cond = range_conds_;
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

int ObLogTableScan::print_limit_offset_annotation(char *buf,
                                                  int64_t buf_len,
                                                  int64_t &pos,
                                                  ExplainType type)
{
  int ret = OB_SUCCESS;
  if (NULL != limit_count_expr_ || NULL != limit_offset_expr_) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      ObRawExpr *limit = limit_count_expr_;
      ObRawExpr *offset = limit_offset_expr_;
      EXPLAIN_PRINT_EXPR(limit, type);
      BUF_PRINTF(", ");
      EXPLAIN_PRINT_EXPR(offset, type);
    }
  }

  return ret;
}

int ObLogTableScan::set_query_ranges(ObRangesArray ranges)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(ranges_, ranges))) {
    LOG_WARN("Failed to do append to ranges_ in set_query_ranges()");
  } else { /* Do nothing =*/ }
  return ret;
}

int ObLogTableScan::inner_replace_generated_agg_expr(
        const ObIArray<std::pair<ObRawExpr *, ObRawExpr *> > &to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(to_replace_exprs, access_exprs_))) {
    LOG_WARN("failed to replace_expr_action", K(ret));
  }
  return ret;
}

int ObLogTableScan::print_used_hint(planText &plan_text)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()));
  } else {
    const ObLogPlanHint &plan_hint = get_plan()->get_log_plan_hint();
    const LogTableHint *table_hint = plan_hint.get_log_table_hint(table_id_);
    const ObHint *hint = plan_hint.get_normal_hint(T_USE_LATE_MATERIALIZATION);
    if (NULL != hint
        && ((need_late_materialization() && hint->is_enable_hint()) ||
            (!need_late_materialization() && hint->is_disable_hint()))
        && OB_FAIL(hint->print_hint(plan_text))) {
      LOG_WARN("failed to print late material hint", K(ret));
    } else if (NULL == table_hint) {
      /*do nothing*/
    } else if (NULL != table_hint->parallel_hint_ && table_hint->parallel_hint_->get_parallel() > 1
               && OB_FAIL(table_hint->parallel_hint_->print_hint(plan_text))) {
      LOG_WARN("failed to print table parallel hint", K(ret));
    } else if (NULL != table_hint->use_das_hint_ && (table_hint->use_das_hint_->is_enable_hint() ? use_das() : !use_das())
               && OB_FAIL(table_hint->use_das_hint_->print_hint(plan_text))) {
      LOG_WARN("failed to print table parallel hint", K(ret));
    } else if (table_hint->index_list_.empty()) {
      /*do nothing*/
    } else if (OB_UNLIKELY(table_hint->index_list_.count() != table_hint->index_hints_.count()
                           || (!table_hint->is_index_hint() && !table_hint->is_no_index_hint()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected log index hint", K(ret), K(*table_hint));
    } else {
      int64_t idx = OB_INVALID_INDEX;
      if (ObOptimizerUtil::find_item(table_hint->index_list_, index_table_id_, &idx)) {
        if (OB_UNLIKELY(idx < 0 || idx >= table_hint->index_list_.count())
            || OB_ISNULL(hint = table_hint->index_hints_.at(idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected idx", K(ret), K(idx), K(table_hint->index_list_));
        } else if (table_hint->is_index_hint() &&
                  OB_FAIL(hint->print_hint(plan_text))) {
          LOG_WARN("failed to print indedx hint", K(ret), K(*hint));
        }
      }

      // print all no index
      if (OB_SUCC(ret) && table_hint->is_no_index_hint()) {
        for (int64_t i = 0 ; OB_SUCC(ret) && i < table_hint->index_list_.count(); ++i) {
          if (idx == i) {
            /*do nothing*/
          } else if (OB_ISNULL(hint = table_hint->index_hints_.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected NULL", K(ret), K(hint));
          } else if (OB_FAIL(hint->print_hint(plan_text))) {
            LOG_WARN("failed to print indedx hint", K(ret), K(*hint));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::print_outline_data(planText &plan_text)
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf;
  int64_t &buf_len = plan_text.buf_len;
  int64_t &pos = plan_text.pos;
  TableItem *table_item = NULL;
  ObString qb_name;
  const ObDMLStmt *stmt = NULL;
  const ObTableParallelHint *parallel_hint = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULl", K(ret), K(get_plan()), K(stmt));
  } else if (OB_FAIL(stmt->get_qb_name(qb_name))) {
    LOG_WARN("fail to get qb_name", K(ret), K(stmt->get_stmt_id()));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table item", K(ret), "table_id", table_id_);
  } else if (NULL != (parallel_hint = get_plan()->get_log_plan_hint().get_parallel_hint(table_id_)) &&
             parallel_hint->get_parallel() > 1) { // parallel hint
    ObTableParallelHint temp_hint;
    temp_hint.set_parallel(parallel_hint->get_parallel());
    temp_hint.set_qb_name(qb_name);
    temp_hint.get_table().set_table(*table_item);
    if (OB_FAIL(temp_hint.print_hint(plan_text))) {
      LOG_WARN("failed to print table parallel hint", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (need_late_materialization() &&
             OB_FAIL(BUF_PRINTF("%s%s(@\"%.*s\")",
                                ObQueryHint::get_outline_indent(plan_text.is_oneline_),
                                ObHint::get_hint_name(T_USE_LATE_MATERIALIZATION),
                                qb_name.length(),
                                qb_name.ptr()))) {
    LOG_WARN("fail to print late materialization hint", K(ret));
  } else if (ref_table_id_ == index_table_id_ && NULL != get_parent()
             && log_op_def::LOG_JOIN == get_parent()->get_type()
             && static_cast<ObLogJoin*>(get_parent())->is_late_mat()) {
    // late materialization right table, do not print index hint.
  } else {
    ObIndexHint index_hint(ref_table_id_ == index_table_id_ ? T_FULL_HINT: T_INDEX_HINT);
    index_hint.set_qb_name(qb_name);
    index_hint.get_table().set_table(*table_item);
    if (T_INDEX_HINT == index_hint.get_hint_type()) {
      index_hint.get_index_name().assign(get_index_name().ptr(), get_index_name().length());
    }
    if (OB_FAIL(index_hint.print_hint(plan_text))) {
      LOG_WARN("failed to print index hint", K(ret));
    } else if (use_das()) {
      ObIndexHint use_das_hint(T_USE_DAS_HINT);
      use_das_hint.set_qb_name(qb_name);
      use_das_hint.get_table().set_table(*table_item);
      if (OB_FAIL(use_das_hint.print_hint(plan_text))) {
        LOG_WARN("failed to print use das hint", K(ret));
      }
    }
  }

  return ret;
}

int ObLogTableScan::print_outline(planText &plan_text)
{
  int ret = OB_SUCCESS;
  if (USED_HINT == plan_text.outline_type_ && OB_FAIL(print_used_hint(plan_text))) {
    LOG_WARN("fail to print used hint", K(ret));
  } else if (OUTLINE_DATA == plan_text.outline_type_ && OB_FAIL(print_outline_data(plan_text))) {
    LOG_WARN("fail to print outline data", K(ret));
  }
  return ret;
}

int ObLogTableScan::set_limit_offset(ObRawExpr *limit, ObRawExpr *offset)
{
  int ret = OB_SUCCESS;
  double card = 0.0;
  double cost = 0.0;
  limit_count_expr_ = limit;
  limit_offset_expr_ = offset;
  EstimateCostInfo param;
  param.override_ = true;
  if (OB_FAIL(re_est_cost(param, card, cost))) {
    LOG_WARN("failed to re est cost error", K(ret));
  }
  return ret;
}

int ObLogTableScan::get_path_ordering(ObIArray<ObRawExpr *> &order_exprs)
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

int ObLogTableScan::allocate_granule_pre(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  if (ctx.managed_by_gi()) {
    gi_alloc_post_state_forbidden_ = true;
    LOG_TRACE("tsc in the partition wise state, forbidden table scan allocate gi", K(ref_table_id_));
  }
  return ret;
}

int ObLogTableScan::allocate_granule_post(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  ctx.tablet_size_ = 0;
  if (OB_ISNULL(get_plan())
      || OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id_,
                                                    ref_table_id_,
                                                    get_stmt(),
                                                    table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_UNLIKELY(NULL == table_schema)) {
    // may be fake table, skip
  } else {
    ctx.tablet_size_ = (NULL == table_partition_info_) ? 0 : table_schema->get_tablet_size();
  }

  if (OB_FAIL(ret)) {
  } else if (use_das()) {
    // do nothing
  } else if (gi_alloc_post_state_forbidden_) {
    gi_charged_ = true;
  } else if (get_contains_fake_cte()) {
    /*do nothing*/
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema), K(table_partition_info_));
  } else if (is_distributed()) {
    gi_charged_ = true;
    ctx.alloc_gi_ = true;
    ctx.partition_count_ = table_partition_info_->get_phy_tbl_location_info().get_phy_part_loc_info_list().count();
    ctx.hash_part_ = table_schema->is_hash_part() || table_schema->is_hash_subpart()
                     || table_schema->is_key_subpart() || table_schema->is_key_subpart();
  } else { /*do nothing*/ }

  return ret;
}

int ObLogTableScan::is_table_get(bool &is_get) const
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
  int64_t sel = (is_whole_range_scan() || table_row_count_ == 0) ?
                  100 : static_cast<int64_t>(query_range_row_count_) * 100 / table_row_count_;

  ret = sel >= SELECTION_THRESHOLD && !is_multi_part_table_scan_;

  LOG_TRACE("is_need_feedback", K(estimate_method_), K(table_row_count_),
            K(query_range_row_count_), K(table_row_count_), K(sel), K(ret));
  return ret;
}

int ObLogTableScan::generate_link_sql_post(GenLinkStmtPostContext &link_ctx)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  const ObDMLStmt *stmt = NULL;
  if (0 == dblink_id_) {
    // do nothing
  } else if (FALSE_IT(link_ctx.check_dblink_id(dblink_id_))) {
    // do nothing
  } else if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULl", K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table item", K(table_id_), K(dblink_id_), K(ret));
  } else if (OB_FAIL(link_ctx.spell_table_scan(table_item, filter_exprs_, startup_exprs_,
                                               range_conds_, pushdown_filter_exprs_,
                                               limit_count_expr_))) {
    LOG_WARN("dblink fail to reverse spell table scan", K(dblink_id_), K(ret));
  }
  return ret;
}

int ObLogTableScan::get_phy_location_type(ObTableLocationType &location_type)
{
  int ret = OB_SUCCESS;
  ObShardingInfo *sharding = get_sharding();
  ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
  if (OB_ISNULL(sharding)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(sharding), K(ret));
  } else if (NULL == table_partition_info_) {
    // fake_cte_table, function_table, ...
    location_type = sharding->get_location_type();
  } else if (OB_FAIL(table_partition_info_->get_location_type(opt_ctx.get_local_server_addr(),
                                                              location_type))) {
    LOG_WARN("get table location type error", K(ret));
  }
  return ret;
}

int ObLogTableScan::extract_bnlj_param_idxs(ObIArray<int64_t> &bnlj_params)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> param_exprs;
  if (OB_FAIL(ObRawExprUtils::extract_params(range_conds_, param_exprs))) {
    LOG_WARN("extract params failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); ++i) {
    ObRawExpr *expr = param_exprs.at(i);
    if (expr->has_flag(IS_DYNAMIC_PARAM)) {
      ObConstRawExpr *exec_param = static_cast<ObConstRawExpr*>(expr);
      if (OB_FAIL(add_var_to_array_no_dup(bnlj_params, exec_param->get_value().get_unknown()))) {
        LOG_WARN("add var to array no dup failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("extract bnlj params", K(param_exprs), K(bnlj_params));
  }
  return ret;
}

ObRawExpr * ObLogTableScan::get_real_expr(const ObRawExpr *col) const
{
  ObRawExpr * ret = NULL;
  for (int64_t i = 0; NULL == ret && i < real_expr_map_.count(); ++i) {
    if (real_expr_map_.at(i).first == col) {
      ret = real_expr_map_.at(i).second;
      break;
    }
  }
  return ret;
}

int ObLogTableScan::copy_part_expr_pre(CopyPartExprCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (NULL != calc_part_id_expr_) {
    if (OB_FAIL(copy_part_expr(ctx, calc_part_id_expr_))) {
      LOG_WARN("failed to copy part expr", K(ret));
    }
    LOG_TRACE("succeed to deep copy calc_part_id_expr_ in index scan",
              K(ret), K(calc_part_id_expr_), K(*calc_part_id_expr_));
  }
  return ret;
}
