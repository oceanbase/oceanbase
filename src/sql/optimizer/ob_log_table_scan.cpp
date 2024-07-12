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
  bool is_range = false;
  const char *name = NULL;
  int ret = OB_SUCCESS;
  SampleInfo::SampleMethod sample_method = get_sample_info().method_;
  if (NULL != pre_query_range_) {
    if (OB_FAIL(get_pre_query_range()->is_get(is_get))) {
      // is_get always return true
      LOG_WARN("failed to get is_get", K(ret));
    } else if (range_conds_.count() > 0) {
      is_range = true;
    }
  }
  if (sample_method != SampleInfo::NO_SAMPLE) {
    name = (sample_method == SampleInfo::ROW_SAMPLE) ? "TABLE ROW SAMPLE SCAN" : "TABLE BLOCK SAMPLE SCAN";
  } else if (is_text_retrieval_scan()) {
    name = use_das() ? "DISTRIBUTED TEXT RETRIEVAL SCAN" : "TEXT RETRIEVAL SCAN";
  } else if (is_skip_scan()) {
    name = use_das() ? "DISTRIBUTED TABLE SKIP SCAN" : "TABLE SKIP SCAN";
  } else if (EXTERNAL_TABLE == get_table_type()) {
    name = "EXTERNAL TABLE SCAN";
  } else if (use_das()) {
    if (is_get) {
      name = "DISTRIBUTED TABLE GET";
    } else if (is_range) {
      name = "DISTRIBUTED TABLE RANGE SCAN";
    } else {
      name = "DISTRIBUTED TABLE FULL SCAN";
    }
  } else if (use_column_store()) {
    if (is_get) {
      name = "COLUMN TABLE GET";
    } else if (is_range) {
      name = "COLUMN TABLE RANGE SCAN";
    } else {
      name = "COLUMN TABLE FULL SCAN";
    }
  } else {
    if (is_get) {
      name = "TABLE GET";
    } else if (is_range) {
      name = "TABLE RANGE SCAN";
    } else {
      name = "TABLE FULL SCAN";
    }
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

int ObLogTableScan::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  double limit_percent = -1.0;
  int64_t limit_count = -1;
  int64_t offset_count = 0;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(access_path_)) {  // table scan create from CteTablePath
    card = get_card();
    op_cost = get_op_cost();
    cost = get_cost();
  } else if (OB_ISNULL(get_plan()) || OB_ISNULL(est_cost_info_) ||
             OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(stmt->get_query_ctx()) ||
            OB_UNLIKELY(1 > param.need_parallel_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(est_cost_info_), K(param));
  } else if (OB_FAIL(get_limit_offset_value(NULL, limit_count_expr_, limit_offset_expr_,
                                            limit_percent, limit_count, offset_count))) {
    LOG_WARN("failed to get limit offset value", K(ret));
  } else {
    card = get_card();
    int64_t part_count = est_cost_info_->index_meta_info_.index_part_count_;
    if (0 <= limit_count) {
      if (!use_das()) {
        limit_count *= part_count;
        offset_count *= part_count;
      }
      double need_row_count = limit_count + offset_count;
      need_row_count = std::min(need_row_count, card);
      if (param.need_row_count_ < 0) {
        param.need_row_count_ = need_row_count;
      } else {
        param.need_row_count_ = std::min(param.need_row_count_, need_row_count);
      }
    }
    if (((stmt->get_query_ctx()->optimizer_features_enable_version_ >= COMPAT_VERSION_4_2_3 &&
              stmt->get_query_ctx()->optimizer_features_enable_version_ < COMPAT_VERSION_4_3_0) ||
              stmt->get_query_ctx()->optimizer_features_enable_version_ >= COMPAT_VERSION_4_3_2) &&
        range_conds_.empty() &&
        (!est_cost_info_->postfix_filters_.empty() ||
        !est_cost_info_->table_filters_.empty() ||
        !est_cost_info_->ss_postfix_range_filters_.empty())) {
      //full scan with table filters
      param.need_row_count_ = -1;
    }
    if (OB_FAIL(AccessPath::re_estimate_cost(param,
                                            *est_cost_info_,
                                            sample_info_,
                                            get_plan()->get_optimizer_context(),
                                            card,
                                            op_cost))) {
      LOG_WARN("failed to re estimate cost", K(ret));
    } else {
      cost = op_cost;
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
    LOG_WARN("failed to generate access exprs", K(ret));
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
  } else if (OB_FAIL(allocate_lookup_trans_info_expr())) {
    LOG_WARN("failed to add lookup trans expr", K(ret));
  } else if (NULL != trans_info_expr_ && OB_FAIL(all_exprs.push_back(trans_info_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (NULL != group_id_expr_ && OB_FAIL(all_exprs.push_back(group_id_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (is_text_retrieval_scan() && OB_FAIL(get_text_retrieval_calc_exprs(all_exprs))) {
    LOG_WARN("failed to get text retrieval exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, access_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, pushdown_aggr_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(generate_filter_monotonicity())) {
    LOG_WARN("failed to analyze filter monotonicity", K(ret));
  } else if (OB_FAIL(get_filter_assist_exprs(all_exprs))) {
    LOG_WARN("failed to get filter assist expr", K(ret));
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
  if (OB_SUCC(ret) && is_text_retrieval_scan()) {
    // match against relevance expr will be calculated in storage
    ObSEArray<ObRawExpr *, 8> tmp_exprs;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(get_text_retrieval_info().relevance_expr_, tmp_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(tmp_exprs.push_back(get_text_retrieval_info().doc_token_cnt_))) {
      LOG_WARN("failed to append tmp exprs", K(ret));
    } else if (OB_FAIL(tmp_exprs.push_back(get_text_retrieval_info().total_doc_cnt_))) {
      LOG_WARN("failed to append tmp exprs", K(ret));
    } else if (OB_FAIL(tmp_exprs.push_back(get_text_retrieval_info().related_doc_cnt_))) {
      LOG_WARN("failed to append tmp exprs", K(ret));
    } else if (OB_FAIL(tmp_exprs.push_back(get_text_retrieval_info().match_expr_))) {
      LOG_WARN("failed to append tmp exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_exprs.count(); ++i) {
      ObRawExpr *expr = tmp_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx))) {
        LOG_WARN("failed to mark expr as produced", K(*expr), K(branch_id_), K(id_), K(ret));
      } else { /*do nothing*/ }
    }
  }

  // check if we can produce some more exprs, such as 1 + 'c1' after we have produced 'c1'
  if (OB_SUCC(ret)) {
    if (!is_plan_root() && OB_FAIL(append(output_exprs_, access_exprs_))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(append(output_exprs_, pushdown_aggr_exprs_))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate expr post", K(ret));
    } else { /*do nothing*/ }
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
  } else if (OB_FAIL(append_array_no_dup(exprs, spatial_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (use_group_id() && nullptr != group_id_expr_
             && OB_FAIL(add_var_to_array_no_dup(exprs, group_id_expr_))) {
    LOG_WARN("failed to push back group id expr", K(ret));
  } else if (index_back_ &&
      nullptr != trans_info_expr_ &&
      OB_FAIL(add_var_to_array_no_dup(exprs, trans_info_expr_))) {
    LOG_WARN("fail to add lookup trans info expr", K(ret));
  } else if (OB_FAIL(dep_checker.check(exprs))) {
    LOG_WARN("failed to check op_exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_output.count(); i++) {
      if (OB_ISNULL(child_output.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else if (child_output.at(i)->get_expr_type() == T_PSEUDO_EXTERNAL_FILE_URL) {
        if (OB_FAIL(deps.del_member(i))) {
          LOG_WARN("del member failed", K(ret));
        }
      }
    }
    LOG_TRACE("succeed to check output exprs", K(exprs), K(type_), K(deps));
  }
  return ret;
}

int ObLogTableScan::extract_file_column_exprs_recursively(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (T_PSEUDO_EXTERNAL_FILE_COL == expr->get_expr_type() ||
             T_PSEUDO_PARTITION_LIST_COL == expr->get_expr_type() ||
             T_PSEUDO_EXTERNAL_FILE_URL == expr->get_expr_type()) {
    auto pseudo_col_expr = static_cast<ObPseudoColumnRawExpr*>(expr);
    if (pseudo_col_expr->get_table_id() != table_id_) {
      //table id may be changed because of rewrite
      pseudo_col_expr->set_table_id(table_id_);
    }
    if (OB_FAIL(add_var_to_array_no_dup(ext_file_column_exprs_, expr))) {
      LOG_WARN("failed to add var to array", K(ret));
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_children_count(); ++i) {
      if (OB_FAIL(extract_file_column_exprs_recursively(expr->get_param_expr(i)))) {
        LOG_WARN("fail to extract file column expr", K(ret));
      }
    }
  }
  return ret;
}

// If the filter is indexed before returning to the table,
// and there are generated columns, deep copy is required
int ObLogTableScan::copy_filter_before_index_back()
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr*> &filters = get_filter_exprs();
  const auto &flags = get_filter_before_index_flags();
  if (OB_FAIL(filter_before_index_back_set())) {
    LOG_WARN("Failed to filter_before_index_back_set", K(ret));
  } else if (get_index_back() && !flags.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
      if (filters.at(i)->has_flag(CNT_PL_UDF)) {
        // do nothing.
      } else if (flags.at(i)) {
        if (get_index_back() && get_is_index_global() && filters.at(i)->has_flag(CNT_SUB_QUERY)) {
          // do nothing.
        } else {
          bool is_contain_vir_gen_column = false;
          // ObArray<ObRawExpr *> column_exprs;
          // scan_pushdown before index back conclude virtual generated column
          // need copy for avoiding shared expression.
          if (OB_FAIL(ObRawExprUtils::contain_virtual_generated_column(filters.at(i), is_contain_vir_gen_column))) {
            LOG_WARN("fail to contain virtual gen column", K(ret));
          } else if (is_contain_vir_gen_column) {
            ObArray<ObRawExpr *> vir_gen_par_exprs;
            if (OB_FAIL(ObRawExprUtils::extract_virtual_generated_column_parents(filters.at(i), filters.at(i), vir_gen_par_exprs))) {
              LOG_WARN("failed to extract virtual generated column parents", K(ret), K(i));
            } else {
              ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
              for (int64_t j = 0; OB_SUCC(ret) && j < vir_gen_par_exprs.count(); ++j) {
                ObRawExpr *copied_expr = NULL;
                if (OB_FAIL(get_plan()->get_optimizer_context().get_expr_factory().create_raw_expr(
                                                  vir_gen_par_exprs.at(j)->get_expr_class(),
                                                  vir_gen_par_exprs.at(j)->get_expr_type(),
                                                  copied_expr))) {
                  LOG_WARN("failed to create raw expr", K(ret));
                } else if (OB_FAIL(copied_expr->deep_copy(copier, *vir_gen_par_exprs.at(j)))) {
                  LOG_WARN("failed to assign old expr", K(ret));
                } else if (OB_FAIL(copier.add_replaced_expr(vir_gen_par_exprs.at(j), copied_expr))) {
                  LOG_WARN("failed to add replaced expr", K(ret));
                } else if (OB_FAIL(copier.copy_on_replace(filters.at(i), filters.at(i)))) {
                  LOG_WARN("failed to copy exprs", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(copier.copy_on_replace(filters.at(i), filters.at(i)))) {
                  LOG_WARN("failed to copy exprs", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObLogTableScan::find_nearest_rcte_op(ObLogSet *&log_set)
{
  int ret = OB_SUCCESS;
  log_set = nullptr;
  ObLogicalOperator *op = this->get_parent();
  while (OB_NOT_NULL(op)) {
    if (log_op_def::LOG_SET == op->get_type()) {
      ObLogSet *set_op = static_cast<ObLogSet *>(op);
      if (set_op->is_recursive_union()) {
        log_set = set_op;
        break;
      }
    }
    op = op->get_parent();
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
  } else if (OB_FAIL(copy_filter_before_index_back())) {
    LOG_WARN("failed to copy filter before index back", K(ret));
  } else if (is_text_retrieval_scan() && OB_FAIL(prepare_text_retrieval_dep_exprs())) {
    LOG_WARN("failed to copy text retrieval aggr exprs", K(ret));
  } else if (OB_FAIL(generate_necessary_rowkey_and_partkey_exprs())) {
    LOG_WARN("failed to generate rowkey and part exprs", K(ret));
  } else if (OB_FAIL(allocate_group_id_expr())) {
    LOG_WARN("failed to allocate group id expr", K(ret));
  } else if (NULL != group_id_expr_ && use_batch_ && OB_FAIL(access_exprs_.push_back(group_id_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(append_array_no_dup(access_exprs_, rowkey_exprs_))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(access_exprs_, part_exprs_))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (is_spatial_index_ && OB_FAIL(append_array_no_dup(access_exprs_, spatial_exprs_))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(access_exprs_, domain_exprs_))) {
    LOG_WARN("failed to append domain exprs", K(ret));
  } else if (is_index_global_ && index_back_) {
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filter_exprs_, temp_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(access_exprs_, temp_exprs))) {
      LOG_WARN("failed to append array no dup", K(ret));
    } else { /*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); i++) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(col_item), K(ret));
      } else if (col_item->table_id_ != table_id_ || !col_item->expr_->is_explicited_reference()) {
        //do nothing
      } else if (col_item->expr_->is_only_referred_by_stored_gen_col()) {
        //skip if is only referred by stored generated columns which don't need to be recalculated
      } else if (is_index_scan() && !get_index_back() && !col_item->expr_->is_referred_by_normal()) {
        //skip the dependant columns of partkeys and generated columns if index_back is false in index scan
      } else if (OB_FAIL(temp_exprs.push_back(col_item->expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/}
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(append_array_no_dup(access_exprs_, temp_exprs))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else { /*do nothing*/ }

    if (OB_SUCC(ret) && NULL != identify_seq_expr_) {
      if (OB_FAIL(access_exprs_.push_back(identify_seq_expr_))) {
        LOG_WARN("fail to add identify seq expr", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_pseudo_column_like_exprs().count(); i++) {
      ObRawExpr *expr = stmt->get_pseudo_column_like_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if ((T_ORA_ROWSCN == expr->get_expr_type())
                 && static_cast<ObPseudoColumnRawExpr*>(expr)->get_table_id() == table_id_) {
        if (OB_FAIL(access_exprs_.push_back(expr))) {
          LOG_WARN("fail to push back expr", K(ret));
        }
      } else if ((T_PSEUDO_EXTERNAL_FILE_URL == expr->get_expr_type())
                 && static_cast<ObPseudoColumnRawExpr*>(expr)->get_table_id() == table_id_) {
        if (OB_FAIL(add_var_to_array_no_dup(ext_file_column_exprs_, expr))) {
          LOG_WARN("fail to push back expr", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(output_exprs_, expr))) {
          LOG_WARN("fail to push back expr", K(ret));
        } else if (OB_FAIL(access_exprs_.push_back(expr))) { //add access expr temp
          LOG_WARN("fail to push back expr", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_mapping_columns_for_vt(access_exprs_))) {
        LOG_WARN("failed to add mapping columns for vt", K(ret));
      } else {
        LOG_TRACE("succeed to generate access exprs", K(access_exprs_), K(ext_file_column_exprs_));
      }
    }
  }
  return ret;
}


int ObLogTableScan::replace_gen_col_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (is_index_scan() && !(get_index_back())) {
    // do nothing.
  } else if (!replacer.empty()) {
    FOREACH_CNT_X(it, get_op_ordering(), OB_SUCC(ret)) {
      if (OB_FAIL(replace_expr_action(replacer, it->expr_))) {
        LOG_WARN("replace agg expr failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(replace_exprs_action(replacer, get_output_exprs()))) {
      LOG_WARN("failed to replace agg expr", K(ret));
    } else if (NULL != limit_offset_expr_ &&
        OB_FAIL(replace_expr_action(replacer, limit_count_expr_))) {
      LOG_WARN("failed to replace limit count expr", K(ret));
    } else if (NULL != limit_offset_expr_  &&
              OB_FAIL(replace_expr_action(replacer, limit_offset_expr_))) {
      LOG_WARN("failed to replace limit offset expr ", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_aggr_exprs_.count(); ++i) {
        ObAggFunRawExpr *pushdown_aggr_expr = pushdown_aggr_exprs_.at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < pushdown_aggr_expr->get_param_count(); j++) {
          if (OB_ISNULL(pushdown_aggr_expr->get_param_expr(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param_expr is NULL", K(j), K(ret));
          } else if (OB_FAIL(replace_expr_action(replacer,
                              pushdown_aggr_expr->get_param_expr(j)))) {
            LOG_WARN("Fail to replace push down aggr expr", K(i), K(j), K(ret));
          }
        }
      }
    }
    // Scenario processing without index table.
    if (OB_SUCC(ret) && !get_index_back()) {
      if (NULL != part_expr_  &&
              OB_FAIL(replace_expr_action(replacer, part_expr_))) {
        LOG_WARN("failed to replace part expr ", K(ret));
      } else if (NULL != subpart_expr_ && OB_FAIL(replace_expr_action(replacer,
                subpart_expr_))) {
        LOG_WARN("failed to replace subpart expr ", K(ret));
      } else if (OB_FAIL(replace_exprs_action(replacer, get_filter_exprs()))) {
        LOG_WARN("failed to replace agg expr", K(ret));
      }
    }
    // Index back to table scene processing
    if (OB_SUCC(ret) && get_index_back()) {
      if (OB_FAIL(replace_index_back_pushdown_filters(replacer))) {
        LOG_WARN("failed to replace pushdown exprs", K(ret));
      }
    }
  } else { /* Do nothing */ }
  return ret;
}

int ObLogTableScan::replace_index_back_pushdown_filters(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> non_pushdown_expr;
  ObArray<ObRawExpr*> scan_pushdown_filters;
  ObArray<ObRawExpr*> lookup_pushdown_filters;
  if (OB_UNLIKELY(!get_index_back())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("go wrong way", K(ret));
  } else if (OB_FAIL(extract_pushdown_filters(non_pushdown_expr,
                                       scan_pushdown_filters,
                                       lookup_pushdown_filters))) {
    LOG_WARN("extract pushdown filters failed", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, non_pushdown_expr))) {
    LOG_WARN("failed to replace non pushdown expr", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, lookup_pushdown_filters))) {
    LOG_WARN("failed to replace lookup pushdown expr", K(ret));
  } else { /* do nothing */ }
  return ret;
}

int ObLogTableScan::has_nonpushdown_filter(bool &has_npd_filter)
{
  int ret = OB_SUCCESS;
  has_npd_filter = false;
  ObArray<ObRawExpr*> nonpushdown_filters;
  ObArray<ObRawExpr*> scan_pushdown_filters;
  ObArray<ObRawExpr*> lookup_pushdown_filters;
  if (OB_FAIL(extract_pushdown_filters(nonpushdown_filters,
                                       scan_pushdown_filters,
                                       lookup_pushdown_filters,
                                       true /*ignore pushdown filters*/))) {
    LOG_WARN("extract pushdnow filters failed", K(ret));
  } else if (!nonpushdown_filters.empty()) {
    has_npd_filter = true;
  }
  return ret;
}

int ObLogTableScan::extract_pushdown_filters(ObIArray<ObRawExpr*> &nonpushdown_filters,
                                             ObIArray<ObRawExpr*> &scan_pushdown_filters,
                                             ObIArray<ObRawExpr*> &lookup_pushdown_filters,
                                             bool ignore_pd_filter /*= false */) const
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr*> &filters = get_filter_exprs();
  const auto &flags = get_filter_before_index_flags();
  if (get_contains_fake_cte() ||
      is_virtual_table(get_ref_table_id()) ||
      EXTERNAL_TABLE == get_table_type()) {
    //all filters can not push down to storage
    if (OB_FAIL(nonpushdown_filters.assign(filters))) {
      LOG_WARN("store non-pushdown filters failed", K(ret));
    }
  } else {
    //part of filters can push down to storage
    //scan_pushdown_filters means that:
    //1. index scan filter when TSC use index scan directly or
    //(TSC use index scan and lookup the data table)
    //2. data table scan filter when TSC use the data table scan directly
    //lookup_pushdown_filters means that the data table filter when
    //TSC use index scan and lookup the data table
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
      if (use_batch() && filters.at(i)->has_flag(CNT_DYNAMIC_PARAM)) {
        //In Batch table scan the dynamic param filter do not push down to storage
        if (OB_FAIL(nonpushdown_filters.push_back(filters.at(i)))) {
          LOG_WARN("push dynamic filter to store non-pushdown filter failed", K(ret), K(i));
        }
      } else if (filters.at(i)->has_flag(CNT_PL_UDF) ||
                 filters.at(i)->has_flag(CNT_OBJ_ACCESS_EXPR)) {
        //User Define Function/obj access expr filter do not push down to storage
        if (OB_FAIL(nonpushdown_filters.push_back(filters.at(i)))) {
          LOG_WARN("push UDF/obj access filter store non-pushdown filter failed", K(ret), K(i));
        }
      } else if (filters.at(i)->has_flag(CNT_DYNAMIC_USER_VARIABLE)
              || filters.at(i)->has_flag(CNT_ASSIGN_EXPR)) {
        if (OB_FAIL(nonpushdown_filters.push_back(filters.at(i)))) {
          LOG_WARN("push variable assign filter store non-pushdown filter failed", K(ret), K(i));
        }
      } else if (ignore_pd_filter) {
        //ignore_pd_filter: only extract non-pushdown filters, ignore others
      } else if (!get_index_back()) {
        if (OB_FAIL(scan_pushdown_filters.push_back(filters.at(i)))) {
          LOG_WARN("store pushdown filter failed", K(ret));
        }
      } else if (flags.empty() || i >= flags.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter before index flag is invalid", K(ret), K(i), K(flags), K(filters));
      } else if (flags.at(i)) {
        if (get_index_back() && get_is_index_global() && filters.at(i)->has_flag(CNT_SUB_QUERY)) {
          if (OB_FAIL(lookup_pushdown_filters.push_back(filters.at(i)))) {
            LOG_WARN("store lookup pushdown filter failed", K(ret), K(i));
          }
        } else if (OB_FAIL(scan_pushdown_filters.push_back(filters.at(i)))) {
          LOG_WARN("store scan pushdown filter failed", K(ret), K(i));
        }
      } else if (OB_FAIL(lookup_pushdown_filters.push_back(filters.at(i)))) {
        LOG_WARN("store lookup pushdown filter failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObLogTableScan::extract_virtual_gen_access_exprs(
                              ObIArray<ObRawExpr*> &access_exprs,
                              uint64_t scan_table_id)
{
  int ret = OB_SUCCESS;
  if (get_index_back() && scan_table_id == get_real_index_table_id()) {
    //this das scan is index scan and will lookup the data table later
    //index scan + lookup data table: the index scan only need access
    //range condition columns + index filter columns + the data table rowkeys
    const ObIArray<ObRawExpr*> &range_conditions = get_range_conditions();
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(range_conditions, access_exprs))) {
      LOG_WARN("extract column exprs failed", K(ret));
    }
    //store index filter columns
    if (OB_SUCC(ret)) {
      ObArray<ObRawExpr *> filter_columns; // the column in scan pushdown filters
      ObArray<ObRawExpr *> nonpushdown_filters;
      ObArray<ObRawExpr *> scan_pushdown_filters;
      ObArray<ObRawExpr *> lookup_pushdown_filters;
      if (OB_FAIL(extract_pushdown_filters(
                                  nonpushdown_filters,
                                  scan_pushdown_filters,
                                  lookup_pushdown_filters))) {
        LOG_WARN("extract pushdown filter failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(scan_pushdown_filters,
                                                              filter_columns))) {
        LOG_WARN("extract column exprs failed", K(ret));
      } else if (OB_FAIL(append_array_no_dup(access_exprs, filter_columns))) {
        LOG_WARN("append filter column to access exprs failed", K(ret));
      }
    }
    //store data table rowkeys
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append_array_no_dup(access_exprs, get_rowkey_exprs()))) {
        LOG_WARN("append the data table rowkey expr failed", K(ret), K(get_rowkey_exprs()));
      } else if (OB_FAIL(append_array_no_dup(access_exprs, get_part_exprs()))) {
        LOG_WARN("append the data table part expr failed", K(ret), K(get_part_exprs()));
      } else if (NULL != get_group_id_expr()
                 && OB_FAIL(add_var_to_array_no_dup(access_exprs,
                               const_cast<ObRawExpr *>(get_group_id_expr())))) {
        LOG_WARN("fail to add group id", K(ret));
      }
    }
  } else if (OB_FAIL(access_exprs.assign(get_access_exprs()))) {
    LOG_WARN("assign access exprs failed", K(ret));
  }
  if (OB_SUCC(ret) && is_oracle_mapping_real_virtual_table(get_ref_table_id())) {
    //the access exprs are the agent virtual table columns, but das need the real table columns
    //now to replace the real table column
    for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); ++i) {
      ObRawExpr *expr = access_exprs.at(i);
      ObRawExpr *mapping_expr = nullptr;
      uint64_t column_id = UINT64_MAX;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(expr));
      } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
        // keep orign expr
      } else if (OB_ISNULL(mapping_expr = get_real_expr(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mapping expr is null", K(ret), KPC(expr));
      } else {
        //replace the agent virtual table column expr
        access_exprs.at(i) = mapping_expr;
      }
    }
  }

  ObArray<ObRawExpr*> tmp_access_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); ++i) {
    ObRawExpr *expr = access_exprs.at(i);
    if (expr->is_column_ref_expr() &&
      static_cast<ObColumnRefRawExpr *>(expr)->is_virtual_generated_column()) {
      if (OB_FAIL(add_var_to_array_no_dup(tmp_access_exprs, expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    } else {
      //do nothing.
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(access_exprs.assign(tmp_access_exprs))) {
    LOG_WARN("failed to remove generated column exprs", K(ret));
  }
  return ret;
}

int ObLogTableScan::adjust_print_access_info(ObIArray<ObRawExpr*> &access)
{
  int ret = OB_SUCCESS;
  if (!is_index_scan() && !get_index_back()) {
    ObArray<ObRawExpr *> main_table_virtual_gen_exprs;
    ObArray<ObRawExpr *> tmp_access_exprs;
    if (OB_FAIL(extract_virtual_gen_access_exprs(
            main_table_virtual_gen_exprs, get_real_ref_table_id()))) {
      LOG_WARN("failed to extract das access exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::except_exprs(access,
              main_table_virtual_gen_exprs, tmp_access_exprs))) {
      LOG_WARN("failed to except virtual generated column exprs", K(ret));
    } else if (OB_FAIL(access.assign(tmp_access_exprs))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  } else if (!get_index_back()) {
    // do nothing.
  } else {
    ObArray<ObRawExpr *> main_table_virtual_gen_exprs;
    ObArray<ObRawExpr *> index_table_virtual_gen_exprs;
    ObArray<ObRawExpr *> tmp_virtual_gen_exprs;
    ObArray<ObRawExpr *> tmp_access_exprs;
    if (OB_FAIL(extract_virtual_gen_access_exprs(
                  main_table_virtual_gen_exprs, get_real_ref_table_id()))) {
      LOG_WARN("failed to extract das access exprs", K(ret));
    } else if (OB_FAIL(extract_virtual_gen_access_exprs(
                  index_table_virtual_gen_exprs, get_real_index_table_id()))) {
      LOG_WARN("failed to extract das access exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::except_exprs(main_table_virtual_gen_exprs,
              index_table_virtual_gen_exprs, tmp_virtual_gen_exprs))) {
      LOG_WARN("failed to except virtual generated column exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::except_exprs(access,
              tmp_virtual_gen_exprs, tmp_access_exprs))) {
      LOG_WARN("failed to except virtual generated column exprs", K(ret));
    } else if (OB_FAIL(access.assign(tmp_access_exprs))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

// for ddl scene.
int ObLogTableScan::generate_ddl_output_column_ids()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    if (opt_ctx.is_online_ddl() &&
        stmt::T_INSERT == opt_ctx.get_session_info()->get_stmt_type() &&
        !opt_ctx.get_session_info()->get_ddl_info().is_mview_complete_refresh()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_output_exprs().count(); ++i) {
        const ObRawExpr *output_expr = get_output_exprs().at(i);
        if (OB_ISNULL(output_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("output_expr is nullptr", K(ret));
        } else if (!output_expr->is_column_ref_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("output expr is not column ref", K(ret), KPC(output_expr));
        } else {
          const ObColumnRefRawExpr *output_col = static_cast<const ObColumnRefRawExpr*>(
                                                  output_expr);
          if (OB_FAIL(ddl_output_column_ids_.push_back(output_col->get_column_id()))) {
            LOG_WARN("store ddl output column id failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::get_mbr_column_exprs(const uint64_t table_id,
                                         ObIArray<ObRawExpr *> &mbr_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  const ObDMLStmt *stmt = NULL;
  ObSEArray<ObRawExpr*, 8> temp_exprs;

  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); i++) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(col_item), K(ret));
      } else if (table_id == col_item->table_id_ &&
                 OB_NOT_NULL(col_item->expr_->get_dependant_expr()) &&
                 col_item->expr_->get_dependant_expr()->get_expr_type() == T_FUN_SYS_SPATIAL_MBR &&
                 OB_FAIL(temp_exprs.push_back(col_item->expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/}
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_array_no_dup(mbr_exprs, temp_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  }

  return ret;
}

int ObLogTableScan::allocate_lookup_trans_info_expr()
{
  int ret = OB_SUCCESS;
  // Is strict defensive check mode
  // Is index_back (contain local lookup and global lookup)
  // There is no trans_info_expr on the current table_scan operator
  // Satisfy the three conditions, add trans_info_expr for lookup
  // The result of Index_scan will contain the transaction information corresponding to each row
  // The result of the lookup in the data table will also include the trans_info
  // of the current row in the data table, But the trans_info will not be output to the upper operator
  ObOptimizerContext *opt_ctx = nullptr;
  ObOpPseudoColumnRawExpr *tmp_trans_info_expr = nullptr;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_ISNULL(opt_ctx = &(get_plan()->get_optimizer_context()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (index_back_ &&
      opt_ctx->is_strict_defensive_check() &&
      GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0 &&
      nullptr == trans_info_expr_) {
    if (OB_FAIL(ObOptimizerUtil::generate_pseudo_trans_info_expr(*opt_ctx,
                                                                 index_name_,
                                                                 tmp_trans_info_expr))) {
      LOG_WARN("fail to generate pseudo trans info expr", K(ret), K(index_name_));
    } else {
      trans_info_expr_ = tmp_trans_info_expr;
    }
  }
  return ret;
}

int ObLogTableScan::allocate_group_id_expr()
{
  int ret = OB_SUCCESS;
  // [GROUP_ID] expr is now used for group rescan and global lookup keep order, it is handled
  // by DAS layer and transparent to TSC operator.
  ObRawExpr *group_id_expr = nullptr;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (use_group_id() && OB_FAIL(ObOptimizerUtil::allocate_group_id_expr(get_plan(), group_id_expr))) {
    LOG_WARN("failed to allocate group id expr", K(ret));
  } else {
    group_id_expr_ = group_id_expr;
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
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (table_schema != NULL && FALSE_IT(is_heap_table = table_schema->is_heap_table())) {
  } else if (OB_FAIL(get_stmt()->has_lob_column(table_id_, has_lob_column))) {
    LOG_WARN("failed to check whether stmt has lob column", K(ret));
  } else if (OB_FAIL(get_mbr_column_exprs(table_id_, spatial_exprs_))) {
    LOG_WARN("failed to check whether stmt has mbr column", K(ret));
  } else if (need_doc_id_index_back() && OB_FAIL(extract_doc_id_index_back_expr(domain_exprs_))) {
    LOG_WARN("failed to extract doc id index back exprs", K(ret));
  } else if (is_text_retrieval_scan() && OB_FAIL(extract_text_retrieval_access_expr(domain_exprs_))) {
    LOG_WARN("failed to extract text retrieval access exprs", K(ret));
  } else if (is_heap_table && is_index_global_ && index_back_ &&
             OB_FAIL(get_part_column_exprs(table_id_, ref_table_id_, part_exprs_))) {
    LOG_WARN("failed to get part column exprs", K(ret));
  } else if ((has_lob_column || index_back_) &&
             OB_FAIL(get_plan()->get_rowkey_exprs(table_id_, ref_table_id_, rowkey_exprs_))) {
    LOG_WARN("failed to generate rowkey exprs", K(ret));
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
        ObRawExpr *real_expr = access_exprs.at(i);
        if (T_ORA_ROWSCN != access_exprs.at(i)->get_expr_type()
            && OB_FAIL(add_mapping_column_for_vt(static_cast<ObColumnRefRawExpr *>(access_exprs.at(i)),
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
  real_expr = nullptr;
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
      } else if (ob_is_geometry_tc(expr->get_data_type())) { // 在此处先标记为需要index_back，具体是否需要需要结合谓词来判断。
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
  }
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

int ObLogTableScan::init_calc_part_id_expr()
{
  int ret = OB_SUCCESS;
  calc_part_id_expr_ = NULL;
  ObSQLSessionInfo *session = NULL;
  ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
  ObArray<ObRawExpr *> column_exprs;
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
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(calc_part_id_expr_, column_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(copier.add_skipped_expr(column_exprs))) {
      LOG_WARN("failed to add skipped exprs", K(ret));
    } else if (OB_FAIL(copier.copy(calc_part_id_expr_, calc_part_id_expr_))) {
      LOG_WARN("failed to copy exprs", K(ret));
    } else if (!table_schema->is_heap_table() &&
               OB_NOT_NULL(calc_part_id_expr_) &&
               OB_FAIL(replace_gen_column(get_plan(), calc_part_id_expr_, calc_part_id_expr_))) {
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

int ObLogTableScan::replace_gen_column(ObLogPlan *plan, ObRawExpr *part_expr, ObRawExpr *&new_part_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> column_exprs;
  new_part_expr = part_expr;
  if (OB_ISNULL(part_expr)) {
    // do nothing
  } else if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, column_exprs))) {
    LOG_WARN("fail to extract column exprs", K(part_expr), K(ret));
  } else {
    ObRawExprCopier copier(plan->get_optimizer_context().get_expr_factory());
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

int ObLogTableScan::get_plan_item_info(PlanText &plan_text,
                                       ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else if (OB_FAIL(get_plan_object_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan object info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    // print access
    ObIArray<ObRawExpr*> &access = get_access_exprs();
    if (OB_FAIL(adjust_print_access_info(access))) {
      ret = OB_SUCCESS;
      //ignore error code for explain
      EXPLAIN_PRINT_EXPRS(access, type);
    } else {
      EXPLAIN_PRINT_EXPRS(access, type);
    }
    if (OB_SUCC(ret) && EXTERNAL_TABLE == get_table_type() && EXPLAIN_EXTENDED == type) {
      if(OB_FAIL(BUF_PRINTF(NEW_LINE))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else {
        ObIArray<ObRawExpr*> &column_values = get_ext_column_convert_exprs();
        EXPLAIN_PRINT_EXPRS(column_values, type);
      }
    }
    END_BUF_PRINT(plan_item.access_predicates_,
                  plan_item.access_predicates_len_);
  }
  if (OB_SUCC(ret)) {
    //print index selection and stats version
    BEGIN_BUF_PRINT;
    ObLogPlan *plan = get_plan();
    OptTableMeta *table_meta = NULL;
    if (OB_ISNULL(plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param", K(ret));
    } else if (OB_FAIL(explain_index_selection_info(buf, buf_len, pos))) {
      LOG_WARN("failed to explain index selection info", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_ISNULL(table_meta =
      plan->get_basic_table_metas().get_table_meta_by_table_id(table_id_))) {
      //do nothing
    } else if (OB_FAIL(BUF_PRINTF("stats version:%ld", table_meta->get_version()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("dynamic sampling level:%ld", table_meta->get_ds_level()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_NOT_NULL(est_cost_info_) &&
               OB_FAIL(print_est_method(est_cost_info_->est_method_, buf, buf_len, pos))) {
      LOG_WARN("failed to print est method", K(ret));
    }
    END_BUF_PRINT(plan_item.optimizer_, plan_item.optimizer_len_);
  }
  // print partitions
  if (OB_SUCC(ret)) {
    if (NULL != table_partition_info_) {
      BEGIN_BUF_PRINT;
      if (OB_FAIL(explain_print_partitions(*table_partition_info_, buf, buf_len, pos))) {
        LOG_WARN("Failed to print partitions");
      }
      END_BUF_PRINT(plan_item.partition_start_,
                    plan_item.partition_start_len_);
    }
  }
  if (OB_SUCC(ret)) {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(print_limit_offset_annotation(buf, buf_len, pos, type))) {
      LOG_WARN("print limit offset annotation failed", K(ret), K(buf_len), K(pos), K(type));
    } else if (OB_FAIL(BUF_PRINTF("is_index_back=%s", index_back_ ? "true" : "false"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("is_global_index=%s", is_index_global_? "true" : "false"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (!das_keep_ordering_) {
      //do nothing
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("keep_ordering=%s", das_keep_ordering_ ? "true" : "false"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */ }

    if (OB_SUCC(ret) && (0 != filter_before_index_back_.count())) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(print_filter_before_indexback_annotation(buf, buf_len, pos))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    }
    //Print ranges
    if (OB_FAIL(ret) || is_text_retrieval_scan()) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(print_range_annotation(buf, buf_len, pos, type))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }

    if (OB_SUCC(ret) && (!pushdown_groupby_columns_.empty() ||
                         !pushdown_aggr_exprs_.empty())) {
      ObIArray<ObAggFunRawExpr*> &pushdown_aggregation = pushdown_aggr_exprs_;
      ObIArray<ObRawExpr*> &pushdown_groupby = pushdown_groupby_columns_;
      if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (!pushdown_groupby.empty() &&
                 OB_FALSE_IT(EXPLAIN_PRINT_EXPRS(pushdown_groupby, type))) {
      } else if (!pushdown_groupby.empty() &&
                 !pushdown_aggregation.empty() &&
                  OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (!pushdown_aggregation.empty()) {
        EXPLAIN_PRINT_EXPRS(pushdown_aggregation, type);
      }
    }

    if (OB_SUCC(ret) && is_text_retrieval_scan()) {
      // print match against related exprs
      if (OB_FAIL(print_text_retrieval_annotation(buf, buf_len, pos, type))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }

  return ret;
}

int ObLogTableScan::print_est_method(ObBaseTableEstMethod method, char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (method == EST_INVALID) {
    // do nothing
  } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("estimation method:["))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if ((EST_DEFAULT & method) &&
             OB_FAIL(BUF_PRINTF("DEFAULT, "))) {
    LOG_WARN("BUF_PRINTF fails");
  } else if ((EST_STAT & method) &&
             OB_FAIL(BUF_PRINTF("OPTIMIZER STATISTICS, "))) {
    LOG_WARN("BUF_PRINTF fails");
  } else if ((EST_STORAGE & method) &&
             OB_FAIL(BUF_PRINTF("STORAGE, "))) {
    LOG_WARN("BUF_PRINTF fails");
  } else if (((EST_DS_BASIC) & method) &&
             OB_FAIL(BUF_PRINTF("DYNAMIC SAMPLING BASIC, "))) {
    LOG_WARN("BUF_PRINTF fails");
  } else if (((EST_DS_FULL) & method) &&
             OB_FAIL(BUF_PRINTF("DYNAMIC SAMPLING FULL, "))) {
    LOG_WARN("BUF_PRINTF fails");
  } else {
    pos -= 2;
    if (OB_FAIL(BUF_PRINTF("]"))) {
      LOG_WARN("BUF_PRINTF fails");
    }
  }
  return ret;
}

int ObLogTableScan::get_plan_object_info(PlanText &plan_text,
                                         ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    //print object alias
    const ObString &name = get_table_name();
    const ObString &index_name = get_index_name();
    BEGIN_BUF_PRINT;
    if (OB_FAIL(BUF_PRINTF("%.*s", name.length(), name.ptr()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (is_index_scan()) {
      if (OB_FAIL(BUF_PRINTF("%s", LEFT_BRACKET))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("%.*s", index_name.length(), index_name.ptr()))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (is_descending_direction(get_scan_direction()) &&
                 OB_FAIL(BUF_PRINTF("%s", COMMA_REVERSE))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("%s", RIGHT_BRACKET))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    } else {
      if (is_descending_direction(get_scan_direction()) &&
                 OB_FAIL(BUF_PRINTF("%s", BRACKET_REVERSE))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    }
    END_BUF_PRINT(plan_item.object_alias_,
                  plan_item.object_alias_len_);
  }
  if (OB_SUCC(ret)) {
    //print object node、name、owner、type
    ObLogPlan *plan = get_plan();
    const ObDMLStmt *stmt = NULL;
    TableItem *table_item = NULL;
    BEGIN_BUF_PRINT;
    if (OB_ISNULL(plan) || OB_ISNULL(stmt=plan->get_stmt()) ||
        OB_ISNULL(table_item=stmt->get_table_item_by_id(table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param", K(ret));
    } else if (table_item->is_synonym()) {
      BUF_PRINT_OB_STR(table_item->synonym_db_name_.ptr(),
                      table_item->synonym_db_name_.length(),
                      plan_item.object_owner_,
                      plan_item.object_owner_len_);
      BUF_PRINT_OB_STR(table_item->synonym_name_.ptr(),
                      table_item->synonym_name_.length(),
                      plan_item.object_name_,
                      plan_item.object_name_len_);
      BUF_PRINT_STR("SYNONYM",
                    plan_item.object_type_,
                    plan_item.object_type_len_);
      plan_item.object_id_ = ref_table_id_;
    } else if (table_item->is_link_table()) {
      BUF_PRINT_OB_STR(table_item->dblink_name_.ptr(),
                      table_item->dblink_name_.length(),
                      plan_item.object_node_,
                      plan_item.object_node_len_);
      BUF_PRINT_OB_STR(table_item->database_name_.ptr(),
                      table_item->database_name_.length(),
                      plan_item.object_owner_,
                      plan_item.object_owner_len_);
      BUF_PRINT_OB_STR(table_item->table_name_.ptr(),
                      table_item->table_name_.length(),
                      plan_item.object_name_,
                      plan_item.object_name_len_);
      BUF_PRINT_STR("DBLINK",
                    plan_item.object_type_,
                    plan_item.object_type_len_);
      plan_item.object_id_ = ref_table_id_;
    } else if (table_item->is_fake_cte_table()) {
      BUF_PRINT_OB_STR(table_item->table_name_.ptr(),
                      table_item->table_name_.length(),
                      plan_item.object_name_,
                      plan_item.object_name_len_);
      BUF_PRINT_STR("FAKE CTE",
                    plan_item.object_type_,
                    plan_item.object_type_len_);
      plan_item.object_id_ = ref_table_id_;
    } else {
      BUF_PRINT_OB_STR(table_item->database_name_.ptr(),
                      table_item->database_name_.length(),
                      plan_item.object_owner_,
                      plan_item.object_owner_len_);
      BUF_PRINT_OB_STR(table_item->table_name_.ptr(),
                      table_item->table_name_.length(),
                      plan_item.object_name_,
                      plan_item.object_name_len_);
      BUF_PRINT_STR("BASIC TABLE",
                    plan_item.object_type_,
                    plan_item.object_type_len_);
      plan_item.object_id_ = ref_table_id_;
    }
  }
  return ret;
}

int ObLogTableScan::explain_index_selection_info(char *buf,
                                                 int64_t &buf_len,
                                                 int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObString op_parallel_rule_name;
  if (OB_NOT_NULL(table_opt_info_)) {
    switch (get_op_parallel_rule()) {
      case OpParallelRule::OP_GLOBAL_DOP:
        op_parallel_rule_name = "Global DOP";
        break;
      case OpParallelRule::OP_DAS_DOP:
        op_parallel_rule_name = "DAS DOP";
        break;
      case OpParallelRule::OP_HINT_DOP:
        op_parallel_rule_name = "Table Parallel Hint";
        break;
      case OpParallelRule::OP_TABLE_DOP:
        op_parallel_rule_name = "Table DOP";
        break;
      case OpParallelRule::OP_AUTO_DOP:
        op_parallel_rule_name = "Auto DOP";
        break;
      case OpParallelRule::OP_INHERIT_DOP:
        op_parallel_rule_name = "Inherited";
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown op parallel rule", K(get_op_parallel_rule()));
    }
    // print detail info of index selection method
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF("  %.*s:", table_name_.length(), table_name_.ptr()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("table_rows:%ld",
                            static_cast<int64_t>(get_table_row_count())))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("physical_range_rows:%ld",
                            static_cast<int64_t>(get_phy_query_range_row_count())))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("logical_range_rows:%ld",
                            static_cast<int64_t>(get_logical_query_range_row_count())))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("index_back_rows:%ld",
                            static_cast<int64_t>(get_index_back_row_count())))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("output_rows:%ld",
                            static_cast<int64_t>(get_output_row_count())))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("table_dop:%ld", get_parallel()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("dop_method:%.*s", op_parallel_rule_name.length(),
                                                     op_parallel_rule_name.ptr()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      // print available index id
      if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("avaiable_index_name:["))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < table_opt_info_->available_index_name_.count(); ++i) {
        if (OB_FAIL(BUF_PRINTF("%.*s", table_opt_info_->available_index_name_.at(i).length(),
                    table_opt_info_->available_index_name_.at(i).ptr()))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else if (i != table_opt_info_->available_index_name_.count() - 1) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else { /* do nothing*/ }
        } else { /* do nothing*/ }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(BUF_PRINTF("]"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }

      // print pruned index name
      if (OB_FAIL(ret) || table_opt_info_->pruned_index_name_.count() <= 0) {
      } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("pruned_index_name:["))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_opt_info_->pruned_index_name_.count(); ++i) {
          if (OB_FAIL(BUF_PRINTF("%.*s", table_opt_info_->pruned_index_name_.at(i).length(),
                                table_opt_info_->pruned_index_name_.at(i).ptr()))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else if (i != table_opt_info_->pruned_index_name_.count() - 1) {
            if (OB_FAIL(BUF_PRINTF(", "))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            } else { /* do nothing*/ }
          } else { /* do nothing*/ }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(BUF_PRINTF("]"))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* Do nothing */ }
      }
      // print unstable index name
      if (OB_FAIL(ret) || table_opt_info_->unstable_index_name_.count() <= 0) {
      } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("unstable_index_name:["))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_opt_info_->unstable_index_name_.count(); ++i) {
          if (OB_FAIL(BUF_PRINTF("%.*s", table_opt_info_->unstable_index_name_.at(i).length(),
                                table_opt_info_->unstable_index_name_.at(i).ptr()))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else if (i != table_opt_info_->unstable_index_name_.count() - 1) {
            if (OB_FAIL(BUF_PRINTF(", "))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            } else { /* do nothing*/ }
          } else { /* do nothing*/ }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(BUF_PRINTF("]"))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* Do nothing */ }
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
  int64_t ori_pos = pos;
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
  if (OB_FAIL(ret)) {
    pos = ori_pos;
    BUF_PRINTF("(too many ranges)");
    ret = OB_SUCCESS;
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

  if (OB_SUCC(ret) && is_skip_scan()) {
    int64_t skip_scan_offset = get_pre_query_range()->get_skip_scan_offset();
    if (OB_FAIL(BUF_PRINTF("\n      prefix_columns_cnt = %ld , skip_scan_range", skip_scan_offset))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (ss_ranges_.empty() && OB_FAIL(BUF_PRINTF("(MIN ; MAX)"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(print_ranges(buf, buf_len, pos, ss_ranges_))) {
      LOG_WARN("failed to print index skip ranges", K(ret));
    } else { /* Do nothing */ }
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
    ObRawExpr *limit = limit_count_expr_;
    ObRawExpr *offset = limit_offset_expr_;
    EXPLAIN_PRINT_EXPR(limit, type);
    BUF_PRINTF(", ");
    EXPLAIN_PRINT_EXPR(offset, type);
    BUF_PRINTF(", ");
  }

  return ret;
}

int ObLogTableScan::set_query_ranges(ObIArray<ObNewRange> &ranges,
                                     ObIArray<ObNewRange> &ss_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(ranges_, ranges))) {
    LOG_WARN("Failed to do append to ranges_ in set_query_ranges()", K(ret));
  } else if (OB_FAIL(append(ss_ranges_, ss_ranges))) {
    LOG_WARN("Failed to do append to ranges_ in set_query_ranges()", K(ret));
  } else { /* Do nothing =*/ }
  return ret;
}

int ObLogTableScan::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, access_exprs_))) {
    LOG_WARN("failed to replace_expr_action", K(ret));
  } else if (calc_part_id_expr_ != NULL &&
             OB_FAIL(replace_expr_action(replacer, calc_part_id_expr_))) {
    LOG_WARN("failed to replace calc part id expr", K(ret));
  }
  return ret;
}

int ObLogTableScan::print_outline_data(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  TableItem *table_item = NULL;
  ObString qb_name;
  const ObString *index_name = NULL;
  int64_t index_prefix = -1;
  ObItemType index_type = T_INDEX_HINT;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULl", K(ret), K(get_plan()), K(stmt));
  } else if (OB_FAIL(stmt->get_qb_name(qb_name))) {
    LOG_WARN("fail to get qb_name", K(ret), K(stmt->get_stmt_id()));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table item", K(ret), "table_id", table_id_);
  } else if (get_parallel() > ObGlobalHint::DEFAULT_PARALLEL) { // parallel hint
    ObTableParallelHint temp_hint;
    temp_hint.set_parallel(get_parallel());
    temp_hint.set_qb_name(qb_name);
    temp_hint.get_table().set_table(*table_item);
    if (OB_FAIL(temp_hint.print_hint(plan_text))) {
      LOG_WARN("failed to print table parallel hint", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_skip_scan()) {
    index_type = T_INDEX_SS_HINT;
    if (ref_table_id_ == index_table_id_) {
      index_name = &ObIndexHint::PRIMARY_KEY;
    } else {
      index_name = &get_index_name();
    }
  } else if (OB_FAIL(get_plan()->get_log_plan_hint().get_index_prefix(table_id_,
                                                  index_table_id_,
                                                  index_prefix))) {
    LOG_WARN("failed to get index prefix", K(table_id_), K(index_table_id_), K(index_prefix), K(ret));
  } else if (ref_table_id_ == index_table_id_ && index_prefix < 0) {
    index_type = T_FULL_HINT;
    index_name = &ObIndexHint::PRIMARY_KEY;
  } else {
    index_type = T_INDEX_HINT;
    if (ref_table_id_ == index_table_id_) {
      index_name = &ObIndexHint::PRIMARY_KEY;
    } else {
      index_name = &get_index_name();
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
    ObIndexHint index_hint(index_type);
    index_hint.set_qb_name(qb_name);
    index_hint.get_table().set_table(*table_item);
    index_hint.get_index_prefix() = index_prefix;
    if (NULL != index_name) {
      index_hint.get_index_name().assign_ptr(index_name->ptr(), index_name->length());
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
    if (OB_SUCC(ret) && use_column_store()) {
      ObIndexHint use_column_store_hint(T_USE_COLUMN_STORE_HINT);
      use_column_store_hint.set_qb_name(qb_name);
      use_column_store_hint.get_table().set_table(*table_item);
      if (OB_FAIL(use_column_store_hint.print_hint(plan_text))) {
        LOG_WARN("failed to print use column store hint", K(ret));
      }
    }
  }

  return ret;
}

int ObLogTableScan::print_used_hint(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()));
  } else {
    const ObLogPlanHint &plan_hint = get_plan()->get_log_plan_hint();
    const LogTableHint *table_hint = plan_hint.get_log_table_hint(table_id_);
    const ObHint *hint = plan_hint.get_normal_hint(T_USE_LATE_MATERIALIZATION);
    int64_t idx = OB_INVALID_INDEX;
    if (NULL != hint
        && ((need_late_materialization() && hint->is_enable_hint()) ||
            (!need_late_materialization() && hint->is_disable_hint()))
        && OB_FAIL(hint->print_hint(plan_text))) {
      LOG_WARN("failed to print late material hint", K(ret));
    } else if (NULL == table_hint) {
      /*do nothing*/
    } else if (NULL != table_hint->parallel_hint_ && get_parallel() == table_hint->parallel_hint_->get_parallel()
               && OpParallelRule::OP_HINT_DOP == get_op_parallel_rule()
               && OB_FAIL(table_hint->parallel_hint_->print_hint(plan_text))) {
      LOG_WARN("failed to print table parallel hint", K(ret));
    } else if (NULL != table_hint->dynamic_sampling_hint_ &&
               table_hint->dynamic_sampling_hint_->get_dynamic_sampling() != ObGlobalHint::UNSET_DYNAMIC_SAMPLING &&
               OB_FAIL(table_hint->dynamic_sampling_hint_->print_hint(plan_text))) {
      LOG_WARN("failed to print dynamic sampling hint", K(ret));
    } else if (NULL != table_hint->use_das_hint_
               && use_das() == table_hint->use_das_hint_->is_enable_hint()
               && OB_FAIL(table_hint->use_das_hint_->print_hint(plan_text))) {
      LOG_WARN("failed to print use das hint", K(ret));
    } else if (NULL != table_hint->use_column_store_hint_
               && use_column_store() == table_hint->use_column_store_hint_->is_enable_hint()
               && OB_FAIL(table_hint->use_column_store_hint_->print_hint(plan_text))) {
      LOG_WARN("failed to print use column_store hint", K(ret));
    } else if (table_hint->index_list_.empty()) {
      /*do nothing*/
    } else if (OB_UNLIKELY(table_hint->index_list_.count() != table_hint->index_hints_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected log index hint", K(ret), K(*table_hint));
    } else if (table_hint->is_use_index_hint()) {// print used use index hint
      if (ObOptimizerUtil::find_item(table_hint->index_list_, index_table_id_, &idx)) {
        if (OB_UNLIKELY(idx < 0 || idx >= table_hint->index_list_.count())
            || OB_ISNULL(hint = table_hint->index_hints_.at(idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected idx", K(ret), K(idx), K(table_hint->index_list_));
        } else if (!is_skip_scan() && T_INDEX_SS_HINT == hint->get_hint_type()) {
          /* is not index skip scan but exist index_ss hint */
        } else if (OB_FAIL(hint->print_hint(plan_text))) {
          LOG_WARN("failed to print index hint", K(ret), K(*hint));
        }
      }
    } else {// print all no index
      for (int64_t i = 0 ; OB_SUCC(ret) && i < table_hint->index_list_.count(); ++i) {
        if (idx == i) {
          /*do nothing*/
        } else if (OB_ISNULL(hint = table_hint->index_hints_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL", K(ret), K(hint));
        } else if (OB_FAIL(hint->print_hint(plan_text))) {
          LOG_WARN("failed to print index hint", K(ret), K(*hint));
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::set_limit_offset(ObRawExpr *limit, ObRawExpr *offset)
{
  int ret = OB_SUCCESS;
  double card = 0.0;
  double op_cost = 0.0;
  double cost = 0.0;
  limit_count_expr_ = limit;
  limit_offset_expr_ = offset;
  EstimateCostInfo param;
  param.need_parallel_ = get_parallel();

  double limit_percent = -1.0;
  int64_t limit_count = -1;
  int64_t offset_count = 0;
  double index_back_cost = 0.0;
  if (OB_ISNULL(est_cost_info_)) {
    //fake cte path
  } else if (OB_FAIL(get_limit_offset_value(NULL, limit_count_expr_, limit_offset_expr_,
                                            limit_percent, limit_count, offset_count))) {
    LOG_WARN("failed to get limit offset value", K(ret));
  } else {
    int64_t part_count = est_cost_info_->index_meta_info_.index_part_count_;
    if (0 <= limit_count) {
      if (!use_das()) {
        limit_count *= part_count;
        offset_count *= part_count;
      }
      param.need_row_count_ = limit_count + offset_count;
      param.need_row_count_ = std::min(param.need_row_count_, get_card());
    }
    ENABLE_OPT_TRACE_COST_MODEL;
    if (OB_FAIL(do_re_est_cost(param, card, op_cost, cost))) {
      LOG_WARN("failed to re est cost error", K(ret));
    } else {
      card = std::min(card, static_cast<double>(limit_count));
      set_op_cost(op_cost);
      set_cost(cost);
      set_card(card);
      LOG_TRACE("push limit into table scan", K(param), K(limit_count), K(part_count), K(card));
    }
    DISABLE_OPT_TRACE_COST_MODEL;
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
    //Before GI is adapted to the real agent table, block gi cannot be assigned to it
    if (share::is_oracle_mapping_real_virtual_table(table_schema->get_table_id())
        || table_schema->is_spatial_index()) {
      ctx.set_force_partition();
    }
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
  double table_row_count = get_table_row_count();
  double logical_query_range_row_count = get_logical_query_range_row_count();
  int64_t sel = (is_whole_range_scan() || table_row_count == 0) ?
                  100 : static_cast<int64_t>(logical_query_range_row_count) * 100 / table_row_count;

  ret = sel >= SELECTION_THRESHOLD && !is_multi_part_table_scan_;

  LOG_TRACE("is_need_feedback", K(table_row_count),
            K(logical_query_range_row_count), K(sel), K(ret));
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

bool ObLogTableScan::is_duplicate_table()
{
  bool bret = false;
  if (NULL != table_partition_info_) {
    bret = table_partition_info_->get_phy_tbl_location_info().is_duplicate_table_not_in_dml();
  }
  return bret;
}

int ObLogTableScan::extract_bnlj_param_idxs(ObIArray<int64_t> &bnlj_params)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> range_param_exprs;
  ObArray<ObRawExpr*> filter_param_exprs;
  if (OB_FAIL(ObRawExprUtils::extract_params(range_conds_, range_param_exprs))) {
    LOG_WARN("extract range params failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_params(filter_exprs_, filter_param_exprs))) {
    LOG_WARN("extract filter params failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_param_exprs.count(); ++i) {
    ObRawExpr *expr = range_param_exprs.at(i);
    if (expr->has_flag(IS_DYNAMIC_PARAM)) {
      ObConstRawExpr *exec_param = static_cast<ObConstRawExpr*>(expr);
      if (OB_FAIL(add_var_to_array_no_dup(bnlj_params, exec_param->get_value().get_unknown()))) {
        LOG_WARN("add var to array no dup failed", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_param_exprs.count(); ++i) {
    ObRawExpr *expr = filter_param_exprs.at(i);
    if (expr->has_flag(IS_DYNAMIC_PARAM)) {
      ObConstRawExpr *exec_param = static_cast<ObConstRawExpr*>(expr);
      if (OB_FAIL(add_var_to_array_no_dup(bnlj_params, exec_param->get_value().get_unknown()))) {
        LOG_WARN("add var to array no dup failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("extract bnlj params", K(range_param_exprs), K(filter_param_exprs), K(bnlj_params));
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

int ObLogTableScan::extract_doc_id_index_back_expr(ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  uint64_t doc_id_rowkey_tid = OB_INVALID_ID;
  ObColumnRefRawExpr *doc_id_col_expr = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  const ObTableSchema *table_schema = nullptr;
  const ObColumnSchemaV2 *doc_id_col_schema = nullptr;
  ObSEArray<ColumnItem, 4> col_items;
  if (!need_doc_id_index_back()) {
    //skip
  } else if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan())  ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(get_stmt()), KP(get_plan()),  KP(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); ++i) {
      const ObColumnSchemaV2 *col_schema = nullptr;
      if (OB_ISNULL(col_schema = table_schema->get_column_schema_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema by index", K(ret));
      } else if (col_schema->is_doc_id_column()) {
        doc_id_col_schema = col_schema;
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(doc_id_col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected doc id column schema not found", K(ret), KPC(table_schema));
  } else if (OB_FAIL(ObRawExprUtils::build_column_expr(
      get_plan()->get_optimizer_context().get_expr_factory(), *doc_id_col_schema, doc_id_col_expr))) {
    LOG_WARN("failed to create doc id column expr", K(ret), KPC(doc_id_col_schema));
  } else if (OB_FAIL(exprs.push_back(doc_id_col_expr))) {
    LOG_WARN("failed to append doc id col expr", K(ret));
  } else if (OB_FAIL(get_stmt()->get_column_items(table_id_, col_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_items.count(); ++i) {
      const ColumnItem &col_item = col_items.at(i);
      bool is_rowkey = false;
      if (OB_FAIL(table_schema->get_rowkey_info().is_rowkey_column(col_item.column_id_, is_rowkey))) {
        LOG_WARN("failed to check if column item is rowkey", K(ret));
      } else if (is_rowkey) {
        exprs.push_back(col_item.expr_);
      }
    }
  }

  return ret;
}

int ObLogTableScan::extract_text_retrieval_access_expr(ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  ObTextRetrievalInfo &tr_info = get_text_retrieval_info();
  if (OB_ISNULL(tr_info.match_expr_) || OB_ISNULL(tr_info.total_doc_cnt_) ||
      OB_ISNULL(tr_info.doc_token_cnt_) || OB_ISNULL(tr_info.related_doc_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match against expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(tr_info.token_column_))) {
    LOG_WARN("failed to append token column to access exprs", K(ret));
  } else if (OB_FAIL(exprs.push_back(tr_info.token_cnt_column_))) {
    LOG_WARN("failed to append token count column to access exprs", K(ret));
  } else if (OB_FAIL(exprs.push_back(tr_info.doc_id_column_))) {
    LOG_WARN("failed to append doc id column to access exprs", K(ret));
  } else if (OB_FAIL(exprs.push_back(tr_info.doc_length_column_))) {
    LOG_WARN("failed to append doc length column to access exprs", K(ret));
  } else if (OB_FAIL(exprs.push_back(tr_info.total_doc_cnt_->get_param_expr(0)))) {
    LOG_WARN("failed to append total doc cnt access col to access exprs", K(ret));
  } else if (OB_FAIL(exprs.push_back(tr_info.doc_token_cnt_->get_param_expr(0)))) {
    LOG_WARN("failed to append doc token cnt access col to access exprs", K(ret));
  } else if (OB_FAIL(exprs.push_back(tr_info.related_doc_cnt_->get_param_expr(0)))) {
    LOG_WARN("failed to append relater doc cnt access col to access exprs", K(ret));
  }
  return ret;
}

int ObLogTableScan::get_text_retrieval_calc_exprs(ObIArray<ObRawExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_text_retrieval_info().match_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match against expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(get_text_retrieval_info().related_doc_cnt_))) {
    LOG_WARN("failed to append relevanced doc cnt expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(get_text_retrieval_info().doc_token_cnt_))) {
    LOG_WARN("failed to append doc token cnt expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(get_text_retrieval_info().total_doc_cnt_))) {
    LOG_WARN("failed to append total doc cnt expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(get_text_retrieval_info().relevance_expr_))) {
    LOG_WARN("failed to append relevance expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(get_text_retrieval_info().match_expr_))) {
    LOG_WARN("failed to append text retrieval expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(get_text_retrieval_info().pushdown_match_filter_))) {
    LOG_WARN("failed to append match filter", K(ret));
  } else if (OB_NOT_NULL(get_text_retrieval_info().topk_limit_expr_) &&
             OB_FAIL(all_exprs.push_back(get_text_retrieval_info().topk_limit_expr_))) {
    LOG_WARN("failed to append limit expr", K(ret));
  } else if (OB_NOT_NULL(get_text_retrieval_info().topk_offset_expr_) &&
             OB_FAIL(all_exprs.push_back(get_text_retrieval_info().topk_offset_expr_))) {
    LOG_WARN("failed to append offset expr", K(ret));
  }
  return ret;
}

int ObLogTableScan::print_text_retrieval_annotation(char *buf, int64_t buf_len, int64_t &pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  ObTextRetrievalInfo &tr_info = get_text_retrieval_info();
  ObMatchFunRawExpr *match_expr = tr_info.match_expr_;
  ObRawExpr *pushdown_match_filter = tr_info.pushdown_match_filter_;
  ObRawExpr *limit = tr_info.topk_limit_expr_;
  ObRawExpr *offset = tr_info.topk_offset_expr_;
  ObSEArray<OrderItem, 1> sort_keys;
  bool calc_relevance = tr_info.need_calc_relevance_;
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("calc_relevance=%s", calc_relevance ? "true" : "false"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (FALSE_IT(EXPLAIN_PRINT_EXPR(match_expr, type))) {
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(pushdown_match_filter)) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (FALSE_IT(EXPLAIN_PRINT_EXPR(pushdown_match_filter, type))) {
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(tr_info.sort_key_.expr_)) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(sort_keys.push_back(tr_info.sort_key_))) {
      LOG_WARN("failed to push back order item", K(ret));
    } else if (FALSE_IT(EXPLAIN_PRINT_SORT_ITEMS(sort_keys, type))) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (FALSE_IT(EXPLAIN_PRINT_EXPR(limit, type))) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (FALSE_IT(EXPLAIN_PRINT_EXPR(offset, type))) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("with_ties("))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (tr_info.with_ties_ && OB_FAIL(BUF_PRINTF("true"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (!tr_info.with_ties_ && OB_FAIL(BUF_PRINTF("false"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  return ret;
}

int ObLogTableScan::prepare_text_retrieval_dep_exprs()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema;
  const ObTableSchema *inv_index_schema;
  ObSqlSchemaGuard *schema_guard = NULL;
  TableItem *table_item = nullptr;
  ObRawExprFactory *expr_factory = nullptr;
  ObSQLSessionInfo *session_info = nullptr;
  uint64_t token_col_id = OB_INVALID_ID;
  ObColumnRefRawExpr *token_column = nullptr;
  uint64_t token_cnt_col_id = OB_INVALID_ID;
  ObColumnRefRawExpr *token_cnt_column = nullptr;
  uint64_t doc_length_col_id = OB_INVALID_ID;
  ObColumnRefRawExpr *doc_length_column = nullptr;
  ObColumnRefRawExpr *doc_id_column = nullptr;
  ObAggFunRawExpr *related_doc_cnt = nullptr;
  ObAggFunRawExpr *total_doc_cnt = nullptr;
  ObAggFunRawExpr *doc_token_cnt = nullptr;
  ObOpRawExpr *relevance_expr = nullptr;
  ObTextRetrievalInfo &tr_info = get_text_retrieval_info();
  if (OB_NOT_NULL(tr_info.doc_id_column_) && OB_NOT_NULL(tr_info.doc_length_column_) &&
      OB_NOT_NULL(tr_info.token_column_) && OB_NOT_NULL(tr_info.token_cnt_column_) &&
      OB_NOT_NULL(tr_info.doc_token_cnt_) && OB_NOT_NULL(tr_info.total_doc_cnt_) &&
      OB_NOT_NULL(tr_info.related_doc_cnt_) && OB_NOT_NULL(tr_info.relevance_expr_)) {
    // do nothing, exprs already generated
  } else if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan()) ||
             OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory()) ||
             OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
             OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(get_real_ref_table_id(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(tr_info.inv_idx_tid_, inv_index_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
    for (int64_t i = 0; OB_SUCC(ret) && i < inv_index_schema->get_column_count(); ++i) {
      const ObColumnSchemaV2 *col_schema = inv_index_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else {
        const ObColumnSchemaV2 *col_schema_in_data_table = table_schema->get_column_schema(col_schema->get_column_id());
        if (OB_ISNULL(col_schema_in_data_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, column schema is nullptr in data table", K(ret), KPC(col_schema), KPC(table_schema));
        } else if (col_schema_in_data_table->is_doc_id_column()) {
          // create doc id expr later
          // Since currently, doc id column on main table schema is a special "virtual generated" column,
          // which can not be calculated by its expr record on schema
          // So we use its column ref expr on index table for index back / projection instead
          if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *col_schema, doc_id_column))) {
            LOG_WARN("failed to build doc id column expr", K(ret));
          }
        } else if (col_schema_in_data_table->is_word_count_column()) {
          token_cnt_col_id = col_schema->get_column_id();
        } else if (col_schema_in_data_table->is_word_segment_column()) {
          token_col_id = col_schema->get_column_id();
        } else if (col_schema_in_data_table->is_doc_length_column()) {
          doc_length_col_id = col_schema->get_column_id();
        } else {}
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); ++i) {
      const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (col_schema->get_column_id() == token_cnt_col_id) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *col_schema, token_cnt_column))) {
          LOG_WARN("failed to build doc id column expr", K(ret));
        } else if (OB_NOT_NULL(token_cnt_column)) {
          token_cnt_column->set_ref_id(get_table_id(), col_schema->get_column_id());
          token_cnt_column->set_column_attr(get_table_name(), col_schema->get_column_name_str());
          token_cnt_column->set_database_name(table_item->database_name_);
        }
      } else if (col_schema->get_column_id() == token_col_id) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *col_schema, token_column))) {
          LOG_WARN("failed to build doc id column expr", K(ret));
        } else if (OB_NOT_NULL(token_column)) {
          token_column->set_ref_id(get_table_id(), col_schema->get_column_id());
          token_column->set_column_attr(get_table_name(), col_schema->get_column_name_str());
          token_column->set_database_name(table_item->database_name_);
        }
      } else if (col_schema->get_column_id() == doc_length_col_id) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *col_schema, doc_length_column))) {
          LOG_WARN("failed to build doc id column expr", K(ret));
        } else if (OB_NOT_NULL(doc_length_column)) {
          doc_length_column->set_ref_id(get_table_id(), col_schema->get_column_id());
          doc_length_column->set_column_attr(get_table_name(), col_schema->get_column_name_str());
          doc_length_column->set_database_name(table_item->database_name_);
        }
      } else {}
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(token_cnt_column) || OB_ISNULL(token_column) || OB_ISNULL(doc_id_column) ||
               OB_ISNULL(doc_length_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null fulltext generated column", K(ret),
          KP(token_cnt_column), KP(token_column), KP(doc_id_column));
    } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_COUNT, related_doc_cnt))) {
      LOG_WARN("failed to create related doc cnt agg expr", K(ret));
    } else if (OB_FAIL(related_doc_cnt->add_real_param_expr(token_cnt_column))) {
      LOG_WARN("failed to set agg param", K(ret));
    } else if (OB_FAIL(related_doc_cnt->formalize(session_info))) {
      LOG_WARN("failed to formalize related doc cnt expr", K(ret));
    } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_COUNT, total_doc_cnt))) {
      LOG_WARN("failed to create related doc cnt agg expr", K(ret));
    } else if (OB_FAIL(total_doc_cnt->add_real_param_expr(doc_id_column))) {
      LOG_WARN("failed to set agg param", K(ret));
    } else if (OB_FAIL(total_doc_cnt->formalize(session_info))) {
      LOG_WARN("failed to formalize total doc cnt expr", K(ret));
    } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SUM, doc_token_cnt))) {
      LOG_WARN("failed to create document token count sum agg expr", K(ret));
    } else if (OB_FAIL(doc_token_cnt->add_real_param_expr(token_cnt_column))) {
      LOG_WARN("failed to set agg param", K(ret));
    } else if (OB_FAIL(doc_token_cnt->formalize(session_info))) {
      LOG_WARN("failed to formalize document token count expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_bm25_expr(*expr_factory, related_doc_cnt,
                                                      token_cnt_column, total_doc_cnt,
                                                      doc_token_cnt, relevance_expr,
                                                      session_info))) {
      LOG_WARN("failed to build bm25 expr", K(ret));
    } else if (OB_FAIL(relevance_expr->formalize(session_info))) {
      LOG_WARN("failed to formalize bm25 expr", K(ret));
    // Copy column ref expr referenced by aggregation in different index table scan
    // to avoid share expression
    } else if (OB_FAIL(copier.copy(related_doc_cnt->get_param_expr(0)))) {
    } else if (OB_FAIL(copier.copy(total_doc_cnt->get_param_expr(0)))) {
    } else if (OB_FAIL(copier.copy(doc_token_cnt->get_param_expr(0)))) {
    } else {
      tr_info.token_column_ = token_column;
      tr_info.token_cnt_column_ = token_cnt_column;
      tr_info.doc_id_column_ = doc_id_column;
      tr_info.doc_length_column_ = doc_length_column;
      tr_info.related_doc_cnt_ = related_doc_cnt;
      tr_info.doc_token_cnt_ = doc_token_cnt;
      tr_info.total_doc_cnt_ = total_doc_cnt;
      tr_info.relevance_expr_ = relevance_expr;
    }
  }
  return ret;
}

int ObLogTableScan::get_card_without_filter(double &card)
{
  int ret = OB_SUCCESS;
  card = NULL != est_cost_info_ ? est_cost_info_->phy_query_range_row_count_ : 1.0;
  return ret;
}

int ObLogTableScan::check_das_need_keep_ordering()
{
  int ret = OB_SUCCESS;
  das_keep_ordering_ = true;
  bool ordering_be_used = true;
  if (!use_das_ && !(is_index_global_ && index_back_)) {
    das_keep_ordering_ = false;
  } else if (OB_FAIL(check_op_orderding_used_by_parent(ordering_be_used))) {
    LOG_WARN("failed to check op ordering used by parent", K(ret));
  } else if (!ordering_be_used) {
    das_keep_ordering_ = false;
  }
  return ret;
}
int ObLogTableScan::generate_filter_monotonicity()
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  const ParamStore *param_store = NULL;
  ObRawExpr * filter_expr = NULL;
  ObSEArray<ObRawExpr *, 2> col_exprs;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_stmt()) || OB_ISNULL(get_stmt()->get_query_ctx()) ||
      OB_ISNULL(exec_ctx = get_plan()->get_optimizer_context().get_exec_ctx()) ||
      OB_ISNULL(param_store = get_plan()->get_optimizer_context().get_params())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected NULL ptr", K(ret));
  } else if (get_stmt()->get_query_ctx()->optimizer_features_enable_version_ < COMPAT_VERSION_4_3_2) {
    filter_monotonicity_.reset();
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_filter_exprs().count(); ++i) {
      col_exprs.reuse();
      if (OB_ISNULL(filter_expr = get_filter_exprs().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got unexpected NULL ptr", K(ret));
      } else if (T_OP_GT != filter_expr->get_expr_type() &&
                 T_OP_GE != filter_expr->get_expr_type() &&
                 T_OP_LT != filter_expr->get_expr_type() &&
                 T_OP_LE != filter_expr->get_expr_type() &&
                 T_OP_EQ != filter_expr->get_expr_type()) {
        /* do nothing */
      } else if (OB_UNLIKELY(2 != filter_expr->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got unexpected param", K(ret), K(*filter_expr));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filter_expr, col_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else if (1 == col_exprs.count()) {
        Monotonicity mono = Monotonicity::NONE_MONO;
        Monotonicity left_mono = Monotonicity::NONE_MONO;
        Monotonicity right_mono = Monotonicity::NONE_MONO;
        bool left_dummy_bool = true;
        bool right_dummy_bool = true;
        bool is_left_func_expr = true;
        ObPCConstParamInfo left_const_param_info;
        ObPCConstParamInfo right_const_param_info;
        ObRawFilterMonotonicity *filter_mono = NULL;
        ObOpRawExpr *assist_expr = NULL;
        ObRawExpr *func_expr = NULL;
        ObRawExpr *const_expr = NULL;
        if (OB_FAIL(ObOptimizerUtil::get_expr_monotonicity(filter_expr->get_param_expr(0), col_exprs.at(0),
                                                           *exec_ctx, left_mono, left_dummy_bool,
                                                           *param_store, left_const_param_info))) {
          LOG_WARN("failed to get expr monotonicity", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::get_expr_monotonicity(filter_expr->get_param_expr(1),
                                                           col_exprs.at(0), *exec_ctx, right_mono,
                                                           right_dummy_bool, *param_store,
                                                           right_const_param_info))) {
          LOG_WARN("failed to get expr monotonicity", K(ret));
        } else {
          if (Monotonicity::NONE_MONO == left_mono) {
            /* do nothing */
          } else if (Monotonicity::CONST == left_mono) {
            const_expr = filter_expr->get_param_expr(0);
          } else if (Monotonicity::ASC == left_mono || Monotonicity::DESC == left_mono) {
            func_expr = filter_expr->get_param_expr(0);
            mono = left_mono;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("got unknow monotonicity type", K(ret), K(left_mono));
          }
          if (OB_FAIL(ret)) {
          } else if (Monotonicity::NONE_MONO == right_mono) {
            /* do nothing */
          } else if (Monotonicity::CONST == right_mono) {
            const_expr = filter_expr->get_param_expr(1);
          } else if (Monotonicity::ASC == right_mono || Monotonicity::DESC == right_mono) {
            func_expr = filter_expr->get_param_expr(1);
            mono = right_mono;
            is_left_func_expr = false;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("got unknow monotonicity type", K(ret), K(right_mono));
          }
        }
        if (OB_SUCC(ret)) {
          if (NULL == func_expr || NULL == const_expr ||
              !(Monotonicity::ASC == mono || Monotonicity::DESC == mono)) {
            /* do nothing */
          } else if (OB_ISNULL(filter_mono = filter_monotonicity_.alloc_place_holder())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("alloc failed", K(ret));
          } else if (!left_const_param_info.const_idx_.empty() &&
                     OB_FAIL(const_param_constraints_.push_back(left_const_param_info))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (!right_const_param_info.const_idx_.empty() &&
                     OB_FAIL(const_param_constraints_.push_back(right_const_param_info))) {
            LOG_WARN("failed to push back", K(ret));
          } else {
            filter_mono->filter_expr_ = filter_expr;
            filter_mono->col_expr_ = static_cast<ObColumnRefRawExpr*>(col_exprs.at(0));
            if (T_OP_EQ != filter_expr->get_expr_type()) {
              /* asc  && f(x) > const  --> mon_asc
               * asc  && const < f(x)  --> mon_asc
               * asc  && f(x) < const  --> mon_desc
               * asc  && const > f(x)  --> mon_desc
               *
               * desc && f(x) > const  --> mon_desc
               * desc && const < f(x)  --> mon_desc
               * desc && f(x) < const  --> mon_asc
               * desc && const > f(x)  --> mon_asc
              */
              if (Monotonicity::ASC == mono) {
                if ((is_left_func_expr && (T_OP_GT == filter_expr->get_expr_type() ||
                                           T_OP_GE == filter_expr->get_expr_type())) ||
                    (!is_left_func_expr && (T_OP_LT == filter_expr->get_expr_type() ||
                                            T_OP_LE == filter_expr->get_expr_type()))) {
                  filter_mono->mono_ = PushdownFilterMonotonicity::MON_ASC;
                } else {
                  filter_mono->mono_ = PushdownFilterMonotonicity::MON_DESC;
                }
              } else {
                if ((is_left_func_expr && (T_OP_GT == filter_expr->get_expr_type() ||
                                           T_OP_GE == filter_expr->get_expr_type())) ||
                    (!is_left_func_expr && (T_OP_LT == filter_expr->get_expr_type() ||
                                            T_OP_LE == filter_expr->get_expr_type()))) {
                  filter_mono->mono_ = PushdownFilterMonotonicity::MON_DESC;
                } else {
                  filter_mono->mono_ = PushdownFilterMonotonicity::MON_ASC;
                }
              }
            } else {
              /* asc  && f(x) = const --> mon_eq_asc  + f(x) > const + f(x) < const
               * desc && f(x) = const --> mon_eq_desc + f(x) > const + f(x) < const
               *
               * asc  && const = f(x) --> mon_eq_asc  + f(x) > const + f(x) < const
               * desc && const = f(x) --> mon_eq_desc + f(x) > const + f(x) < const
              */
              ObRawExprFactory &expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
              ObIAllocator &allocator = get_plan()->get_allocator();
              filter_mono->mono_ = Monotonicity::ASC == mono ? PushdownFilterMonotonicity::MON_EQ_ASC :
                                                               PushdownFilterMonotonicity::MON_EQ_DESC;
              filter_mono->assist_exprs_.set_allocator(&allocator);
              filter_mono->assist_exprs_.set_capacity(2);
              if (OB_FAIL(expr_factory.create_raw_expr(T_OP_GT, assist_expr))) {
                LOG_WARN("failed to create gt raw expr", K(ret));
              } else if (OB_ISNULL(assist_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("alloc failed", K(ret));
              } else if (OB_FAIL(assist_expr->set_param_exprs(func_expr, const_expr))) {
                LOG_WARN("failed to set param exprs", K(ret));
              } else if (OB_FAIL(assist_expr->formalize(get_plan()->get_optimizer_context().get_session_info()))) {
                LOG_WARN("failed to get formalize expr", K(ret));
              } else if (OB_FAIL(filter_mono->assist_exprs_.push_back(assist_expr))) {
                LOG_WARN("failed to push back", K(ret));
              } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_LT, assist_expr))) {
                LOG_WARN("failed to create gt raw expr", K(ret));
              } else if (OB_ISNULL(assist_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("alloc failed", K(ret));
              } else if (OB_FAIL(assist_expr->set_param_exprs(func_expr, const_expr))) {
                LOG_WARN("failed to set param exprs", K(ret));
              } else if (OB_FAIL(assist_expr->formalize(get_plan()->get_optimizer_context().get_session_info()))) {
                LOG_WARN("failed to get formalize expr", K(ret));
              } else if (OB_FAIL(filter_mono->assist_exprs_.push_back(assist_expr))) {
                LOG_WARN("failed to push back", K(ret));
              }
            }
          }
        }
      }
    } // end for
  }
  return ret;
}

int ObLogTableScan::get_filter_monotonicity(const ObRawExpr *filter,
                                            const ObColumnRefRawExpr *col_expr,
                                            PushdownFilterMonotonicity &mono,
                                            ObIArray<ObRawExpr*> &assist_exprs) const
{
  int ret = OB_SUCCESS;
  mono = PushdownFilterMonotonicity::MON_NON;
  assist_exprs.reuse();
  if (OB_ISNULL(filter) || OB_ISNULL(col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected NULL ptr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_monotonicity_.count(); ++i) {
    if (filter == filter_monotonicity_.at(i).filter_expr_ &&
        col_expr == filter_monotonicity_.at(i).col_expr_ &&
        PushdownFilterMonotonicity::MON_NON != filter_monotonicity_.at(i).mono_) {
      mono = filter_monotonicity_.at(i).mono_;
      if (OB_FAIL(append(assist_exprs, filter_monotonicity_.at(i).assist_exprs_))) {
        LOG_WARN("failed to append");
      }
      break;
    }
  }
  return ret;
}

int ObLogTableScan::get_filter_assist_exprs(ObIArray<ObRawExpr *> &assist_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_monotonicity_.count(); ++i) {
    if (OB_FAIL(append(assist_exprs, filter_monotonicity_.at(i).assist_exprs_))) {
      LOG_WARN("failed to append");
    }
  }
  return ret;
}
