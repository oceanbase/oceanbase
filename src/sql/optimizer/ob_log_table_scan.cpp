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
#include "sql/optimizer/ob_log_join.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/domain_id/ob_domain_id.h"

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
  const ObQueryRangeProvider *pre_range = get_pre_graph();
  if (NULL != pre_range) {
    if (OB_FAIL(pre_range->is_get(is_get))) {
      // is_get always return true
      LOG_WARN("failed to get is_get", K(ret));
    } else if (use_query_range()) {
      is_range = true;
    }
  }
  if (sample_method != SampleInfo::NO_SAMPLE) {
    name = (sample_method == SampleInfo::ROW_SAMPLE) ? "TABLE ROW SAMPLE SCAN" : "TABLE BLOCK SAMPLE SCAN";
  } else if (is_text_retrieval_scan()) {
    name = use_das() ? "DISTRIBUTED TEXT RETRIEVAL SCAN" : "TEXT RETRIEVAL SCAN";
  } else if (is_pre_vec_idx_scan()) {
    name = use_das() ? "DISTRIBUTED VECTOR INDEX PRE-FILTER SCAN" : "VECTOR INDEX PRE-FILTER SCAN";
  } else if (is_post_vec_idx_scan()) {
    name = use_das() ? "DISTRIBUTED VECTOR INDEX SCAN" : "VECTOR INDEX SCAN";
  } else if (is_skip_scan()) {
    name = use_das() ? "DISTRIBUTED TABLE SKIP SCAN" : "TABLE SKIP SCAN";
  } else if (EXTERNAL_TABLE == get_table_type()) {
    name = "EXTERNAL TABLE SCAN";
  } else if (use_index_merge()) {
    name = use_das() ? "DISTRIBUTED INDEX MERGE SCAN" : "INDEX MERGE SCAN";
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

bool ObLogTableScan::use_query_range() const
{
  bool res = false;
  const ObRangeNode *head = NULL;
  bool cnt_dynamic_param = false;
  if (range_conds_.count() > 0) {
    // can extract precise range (for old query range)
    res = true;
  } else {
    if (!ranges_.empty()) {
      bool valid_range = false;
      for (int64_t i = 0; !valid_range && i < ranges_.count(); ++i) {
        if (!ranges_.at(i).is_whole_range()) {
          valid_range = true;
        }
      }
      res = valid_range;
    }
    // check pre range graph head for dynamic param
    if (!res && OB_NOT_NULL(get_pre_range_graph()) &&
                OB_NOT_NULL(head = get_pre_range_graph()->get_range_head())) {
      for (int64_t i = 0; !cnt_dynamic_param && i < filter_exprs_.count(); ++i) {
        cnt_dynamic_param = OB_NOT_NULL(filter_exprs_.at(i)) &&
                            filter_exprs_.at(i)->has_flag(CNT_DYNAMIC_PARAM);
      }
      res = cnt_dynamic_param &&
            get_pre_range_graph()->get_skip_scan_offset() == -1 &&
            head->min_offset_ == 0 && !head->always_true_;
    }
  }
  return res;
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
  if (NULL == access_path_) {  // table scan create from CteTablePath
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
    est_cost_info_->rescan_left_server_list_ = param.rescan_left_server_list_;
    card = get_output_row_count();
    int64_t part_count = est_cost_info_->index_meta_info_.index_part_count_;
    double limit_count_double = static_cast<double>(limit_count);
    double offset_count_double = static_cast<double>(offset_count);
    if (0 <= limit_count) {
      if (!use_das()) {
        limit_count_double *= part_count;
        offset_count_double *= part_count;
      }
      double need_row_count = limit_count_double + offset_count_double;
      if (param.need_row_count_ < 0) {
        param.need_row_count_ = need_row_count;
      } else {
        param.need_row_count_ = std::min(param.need_row_count_ + offset_count_double, need_row_count);
      }
      est_cost_info_->limit_rows_ = limit_count;
    }
    if (ObEnableOptRowGoal::OFF == get_plan()->get_optimizer_context().get_enable_opt_row_goal()) {
      param.need_row_count_ = -1;
      est_cost_info_->limit_rows_ = -1;
    } else if (stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_3, COMPAT_VERSION_4_3_0,
                                                               COMPAT_VERSION_4_3_2) &&
        range_conds_.empty() &&
        ObEnableOptRowGoal::AUTO == get_plan()->get_optimizer_context().get_enable_opt_row_goal() &&
        (!est_cost_info_->postfix_filters_.empty() ||
        !est_cost_info_->table_filters_.empty() ||
        !est_cost_info_->ss_postfix_range_filters_.empty())) {
      // full scan with table filters
      param.need_row_count_ = -1;
    }
    if (param.need_row_count_ > card) {
      param.need_row_count_ = -1;
    }
    if (access_path_->is_index_merge_path()) {
      card = param.need_row_count_ < 0 ? get_card() : std::min(param.need_row_count_, get_card());
      op_cost = get_op_cost();
      cost = get_cost();
    } else if (OB_FAIL(AccessPath::re_estimate_cost(param,
                                                    *est_cost_info_,
                                                    sample_info_,
                                                    get_plan()->get_optimizer_context(),
                                                    access_path_->can_batch_rescan_,
                                                    card,
                                                    op_cost))) {
      LOG_WARN("failed to re estimate cost", K(ret));
    } else {
      est_cost_info_->rescan_left_server_list_ = NULL;
      cost = op_cost;
      if (0 <= limit_count && param.need_row_count_ == -1) {
        // full scan with table filters
        card = std::min(limit_count_double + offset_count_double, card);
      }
      card = std::max(card - offset_count_double, 0.0);
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
  } else if (OB_FAIL(generate_auto_split_filter())) {
    LOG_WARN("failed to generate split filter", K(ret));
  } else if (OB_FAIL(generate_access_exprs())) {
    LOG_WARN("failed to generate acess exprs", K(ret));
  } else if (NULL != auto_split_filter_ &&
             OB_FAIL(all_exprs.push_back(auto_split_filter_))) {
    LOG_WARN("failed to push back expr", K(ret));
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
  } else if (is_text_retrieval_scan()
      && OB_FAIL(get_text_retrieval_calc_exprs(get_text_retrieval_info(), all_exprs))) {
    LOG_WARN("failed to get text retrieval exprs", K(ret));
  } else if ((is_post_vec_idx_scan() || is_pre_vec_idx_scan()) && OB_FAIL(get_vec_idx_calc_exprs(all_exprs))) {
    LOG_WARN("failed to get text retrieval exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, rowkey_id_exprs_))) {
    LOG_WARN("failed to append rowkey doc exprs", K(ret));
  } else if (has_func_lookup() && OB_FAIL(get_func_lookup_calc_exprs(all_exprs))) {
    LOG_WARN("failed to get functional lookup exprs", K(ret));
  } else if (use_index_merge() && OB_FAIL(get_index_merge_calc_exprs(all_exprs))) {
    LOG_WARN("failed to get index merge calc exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, access_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, pushdown_aggr_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(generate_filter_monotonicity())) {
    LOG_WARN("failed to analyze filter monotonicity", K(ret));
  } else if (OB_FAIL(get_filter_assist_exprs(all_exprs))) {
    LOG_WARN("failed to get filter assist expr", K(ret));
  } else if (use_index_merge() && OB_FAIL(append_array_no_dup(all_exprs, full_filters_))) {
    LOG_WARN("failed to append index merge full filters", K(ret));
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
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_id_exprs_.count(); ++i) {
    ObRawExpr *expr = rowkey_id_exprs_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (OB_FAIL(mark_expr_produced(expr, branch_id_, id_, ctx))) {
      LOG_WARN("failed to mark expr as produced", K(*expr), K(branch_id_), K(id_), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // match against relevance expr will be calculated in storage
    ObSEArray<ObRawExpr *, 8> tmp_exprs;
    if (is_text_retrieval_scan()
        && OB_FAIL(get_text_retrieval_calc_exprs(get_text_retrieval_info(), tmp_exprs))) {
      LOG_WARN("failed to get text retrieval calc exprs", K(ret));
    } else if (has_func_lookup()
        && OB_FAIL(get_func_lookup_calc_exprs(tmp_exprs))) {
      LOG_WARN("failed to get func lookup exprs", K(ret));
    } else if (use_index_merge()
        && OB_FAIL(get_index_merge_calc_exprs(tmp_exprs))) {
      LOG_WARN("failed to get index merge calc exprs", K(ret));
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
  if (use_index_merge()) {
    // need to copy filter conclude virtual generated column for each index scan in index merge
    if (OB_FAIL(copy_filter_for_index_merge())) {
      LOG_WARN("failed to copy filter for index merge", K(ret));
    }
  } else if (OB_FAIL(filter_before_index_back_set())) {
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
                ObRawExpr *old_expr = filters.at(i);
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
                } else if (filters.at(i)->get_expr_type() == T_OP_RUNTIME_FILTER) {
                  // record runtime filter, also replace it in join filter use operator
                  get_plan()->gen_col_replacer().add_replace_expr(old_expr, filters.at(i));
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

int ObLogTableScan::copy_filter_for_index_merge()
{
  int ret = OB_SUCCESS;
  // each index scan only involves range conds for now in index merge
  for (int64_t i = 0; OB_SUCC(ret) && i < index_range_conds_.count(); i++) {
    common::ObIArray<ObRawExpr*> &range_conds = index_range_conds_.at(i);
    for (int64_t i = 0; OB_SUCC(ret) && i < range_conds.count(); ++i) {
      if (range_conds.at(i)->has_flag(CNT_PL_UDF)) {
        // do nothing.
      } else {
        bool contain_vir_gen_column = false;
        if (OB_FAIL(ObRawExprUtils::contain_virtual_generated_column(range_conds.at(i), contain_vir_gen_column))) {
          LOG_WARN("fail to check contain virtual gen column", K(ret));
        } else if (contain_vir_gen_column) {
          ObArray<ObRawExpr *> vir_gen_par_exprs;
          if (OB_FAIL(ObRawExprUtils::extract_virtual_generated_column_parents(range_conds.at(i), range_conds.at(i), vir_gen_par_exprs))) {
            LOG_WARN("failed to extract virtual generated column parents", K(ret), K(i));
          } else {
            ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
            for (int64_t j = 0; OB_SUCC(ret) && j < vir_gen_par_exprs.count(); ++j) {
              ObRawExpr *copied_expr = NULL;
              ObRawExpr *old_expr = range_conds.at(i);
              if (OB_FAIL(get_plan()->get_optimizer_context().get_expr_factory().create_raw_expr(
                                                vir_gen_par_exprs.at(j)->get_expr_class(),
                                                vir_gen_par_exprs.at(j)->get_expr_type(),
                                                copied_expr))) {
                LOG_WARN("failed to create raw expr", K(ret));
              } else if (OB_FAIL(copied_expr->deep_copy(copier, *vir_gen_par_exprs.at(j)))) {
                LOG_WARN("failed to assign old expr", K(ret));
              } else if (OB_FAIL(copier.add_replaced_expr(vir_gen_par_exprs.at(j), copied_expr))) {
                LOG_WARN("failed to add replaced expr", K(ret));
              } else if (OB_FAIL(copier.copy_on_replace(range_conds.at(i), range_conds.at(i)))) {
                LOG_WARN("failed to copy exprs", K(ret));
              } else if (range_conds.at(i)->get_expr_type() == T_OP_RUNTIME_FILTER) {
                // record runtime filter, also replace it in join filter use operator
                get_plan()->gen_col_replacer().add_replace_expr(old_expr, range_conds.at(i));
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(copier.copy_on_replace(range_conds.at(i), range_conds.at(i)))) {
                LOG_WARN("failed to copy exprs", K(ret));
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
  } else if (is_text_retrieval_scan() && OB_FAIL(prepare_text_retrieval_dep_exprs(get_text_retrieval_info()))) {
    LOG_WARN("failed to copy text retrieval aggr exprs", K(ret));
  } else if ((is_post_vec_idx_scan() || is_pre_vec_idx_scan()) && OB_FAIL(prepare_vector_access_exprs())) {
    LOG_WARN("failed to copy vec idx scan exprs", K(ret));
  } else if (is_tsc_with_domain_id() && OB_FAIL(prepare_rowkey_domain_id_dep_exprs())) {
    LOG_WARN("failed to prepare table scan with doc id info", K(ret));
  } else if (has_func_lookup() && OB_FAIL(prepare_func_lookup_dep_exprs())) {
    LOG_WARN("failed to prepare functional lookup dependent exprs", K(ret));
  } else if (use_index_merge() && OB_FAIL(prepare_index_merge_dep_exprs())) {
    LOG_WARN("failed to prepare index merge dependent exprs", K(ret));
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
  if (OB_SUCC(ret) && nullptr != auto_split_filter_) {
    ObSEArray<ObRawExpr*, 8> temp_col_exprs;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(auto_split_filter_, temp_col_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(access_exprs_, temp_col_exprs))) {
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

    if (OB_SUCC(ret) && use_index_merge() && !full_filters_.empty()) {
      ObArray<ObRawExpr*> column_exprs;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(full_filters_, column_exprs))) {
        LOG_WARN("failed to extract column exprs", K(full_filters_), K(ret));
      } else if (OB_FAIL(append_array_no_dup(access_exprs_, column_exprs))) {
        LOG_WARN("failed to append array no dup", K(column_exprs), K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_pseudo_column_like_exprs().count(); i++) {
      ObRawExpr *expr = stmt->get_pseudo_column_like_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (static_cast<ObPseudoColumnRawExpr*>(expr)->get_table_id() != table_id_) {
        /* do nothing */
      } else if (T_ORA_ROWSCN != expr->get_expr_type()
                 && T_PSEUDO_EXTERNAL_FILE_URL != expr->get_expr_type()
                 && T_PSEUDO_OLD_NEW_COL != expr->get_expr_type()) {
        /* do nothing */
      } else if (OB_FAIL(access_exprs_.push_back(expr))) {
        LOG_WARN("fail to push back expr", K(ret));
      } else if (T_PSEUDO_EXTERNAL_FILE_URL == expr->get_expr_type()) {
        if (OB_FAIL(add_var_to_array_no_dup(ext_file_column_exprs_, expr))) {
          LOG_WARN("fail to push back expr", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(output_exprs_, expr))) {
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
  if (!need_replace_gen_column()) {
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
  } else if (OB_UNLIKELY(use_index_merge())) {
    // when use index merge, we need to replace the filter exprs related to virtual generated columns
    // for those main table participates as a branch of merge.
    IndexMergePath *path = static_cast<IndexMergePath*>(access_path_);
    if (OB_ISNULL(path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null index merge path", K(ret));
    } else if (OB_FAIL(replace_index_merge_pushdown_filters(path->root_, replacer))) {
      LOG_WARN("failed to replace index merge pushdown filters", K(ret));
    }
  } else { /* do nothing */ }
  return ret;
}

int ObLogTableScan::replace_index_merge_pushdown_filters(ObIndexMergeNode *node, ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null index merge node", K(ret), KPC(node));
  } else if (node->is_scan_node()) {
    if (OB_ISNULL(node->ap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null index merge access path", K(ret), KPC(node));
    } else if (node->ap_->index_id_ == ref_table_id_) {
      // main table participates as a branch of merge
      ObArray<ObRawExpr*> scan_pushdown_filters;
      if (OB_FAIL(get_index_filters(node->scan_node_idx_, scan_pushdown_filters))) {
        LOG_WARN("failed to get index scan pushdown filters", K(node->ap_->index_id_), K(ret));
      } else if (OB_FAIL(replace_exprs_action(replacer, scan_pushdown_filters))) {
        LOG_WARN("failed to replace index scan pushdown filters", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->children_.count(); ++i) {
      if (OB_FAIL(SMART_CALL(replace_index_merge_pushdown_filters(node->children_.at(i), replacer)))) {
        LOG_WARN("failed to replace index merge pushdown filters", K(ret));
      }
    }
  }
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
      } else if (has_func_lookup() &&
          (filters.at(i)->has_flag(CNT_MATCH_EXPR) || !flags.at(i))) {
        // for filter with match expr in functional lookup, need to be evaluated after func lookup
        // push-down filter on main-table lookup with functional lookup not supported by executor
        if (OB_FAIL(nonpushdown_filters.push_back(filters.at(i)))) {
          LOG_WARN("push func-lookup match filter to non-pushdown array failed", K(ret), K(i));
        }
      } else if (is_text_retrieval_scan() && need_text_retrieval_calc_relevance()) {
        if (OB_FAIL(nonpushdown_filters.push_back(filters.at(i)))) {
          LOG_WARN("push text retrieval scan store non-pushdown filter failed", K(ret), K(i));
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

int ObLogTableScan::extract_nonpushdown_filters(const ObIArray<ObRawExpr*> &filters,
                                                ObIArray<ObRawExpr*> &nonpushdown_filters,
                                                ObIArray<ObRawExpr*> &pushdown_filters) const
{
  int ret = OB_SUCCESS;
  if (get_contains_fake_cte() ||
      is_virtual_table(get_ref_table_id()) ||
      EXTERNAL_TABLE == get_table_type()) {
    // all filters can not push down to storage
    if (OB_FAIL(nonpushdown_filters.assign(filters))) {
      LOG_WARN("failed to assign full filter to non-pushdown filters", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
      if (use_batch() && filters.at(i)->has_flag(CNT_DYNAMIC_PARAM)) {
        //In Batch table scan the dynamic param filter do not push down to storage
        if (OB_FAIL(nonpushdown_filters.push_back(filters.at(i)))) {
          LOG_WARN("failed to push dynamic filter to non-pushdown filters", K(ret), K(i));
        }
      } else if (filters.at(i)->has_flag(CNT_PL_UDF) ||
                 filters.at(i)->has_flag(CNT_OBJ_ACCESS_EXPR)) {
        //User Define Function/obj access expr filter do not push down to storage
        if (OB_FAIL(nonpushdown_filters.push_back(filters.at(i)))) {
          LOG_WARN("failed to push UDF/obj access filter to non-pushdown filters", K(ret), K(i));
        }
      } else if (filters.at(i)->has_flag(CNT_DYNAMIC_USER_VARIABLE) ||
                 filters.at(i)->has_flag(CNT_ASSIGN_EXPR)) {
        if (OB_FAIL(nonpushdown_filters.push_back(filters.at(i)))) {
          LOG_WARN("failed to push variable assign filter to non-pushdown filters", K(ret), K(i));
        }
      } else if (OB_FAIL(pushdown_filters.push_back(filters.at(i)))) {
        LOG_WARN("failed to push back pushdown filter", K(ret));
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
  } else if (need_doc_id_index_back() && OB_FAIL(extract_doc_id_index_back_expr(domain_exprs_, (is_post_vec_idx_scan() || is_pre_vec_idx_scan())))) {
    LOG_WARN("failed to extract doc id index back exprs", K(ret));
  } else if (is_text_retrieval_scan()
      && OB_FAIL(extract_text_retrieval_access_expr(get_text_retrieval_info(), domain_exprs_))) {
    LOG_WARN("failed to extract text retrieval access exprs", K(ret));
  } else if ((is_post_vec_idx_scan() || is_pre_vec_idx_scan()) && OB_FAIL(extract_vec_idx_access_expr(domain_exprs_))) {
    LOG_WARN("failed to extract vector index access exprs", K(ret));
  } else if (has_func_lookup()
      && OB_FAIL(extract_func_lookup_access_exprs(domain_exprs_))) {
    LOG_WARN("failed to extract functional lookup access exprs", K(ret));
  } else if (use_index_merge()
      && OB_FAIL(extract_index_merge_access_exprs(domain_exprs_))) {
    LOG_WARN("failed to extract index merge access exprs", K(ret));
  } else if (is_heap_table && is_index_global_ && index_back_ &&
             OB_FAIL(get_part_column_exprs(table_id_, ref_table_id_, part_exprs_))) {
    LOG_WARN("failed to get part column exprs", K(ret));
  } else if ((has_lob_column || index_back_ || has_func_lookup()) &&
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
  if (use_index_merge()) {
    // force set index back when index merge
    column_found = false;
  } else if (!is_index_scan()) {
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
      } else if (T_PSEUDO_GROUP_ID == expr->get_expr_type() ||
                 T_PSEUDO_OLD_NEW_COL == expr->get_expr_type()) {
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
    // now, vec index hnsw scan must index back
    if (vector_index_info_.need_index_back() && OB_FALSE_IT(index_back_ = true)) {
    } else if (OB_FAIL(filter_before_index_back_set())) {
      LOG_WARN("Failed to filter_before_index_back_set", K(ret));
    } else {/*Do nothing*/}
  } else { /* Do nothing */ }

  return ret;
}

int ObLogTableScan::filter_before_index_back_set()
{
  int ret = OB_SUCCESS;
  filter_before_index_back_.reset();
  if (index_back_ && !is_post_vec_idx_scan()) {
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

int ObLogTableScan::set_index_merge_scan_filters(const AccessPath *path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_UNLIKELY(!path->is_index_merge_path())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected index merge path", K(ret), KPC(path));
  } else {
    const IndexMergePath *index_merge_path = static_cast<const IndexMergePath*>(path);
    if (OB_ISNULL(index_merge_path->root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null index merge node", K(ret), KPC(index_merge_path));
    } else if (OB_FAIL(get_filter_exprs().assign(index_merge_path->est_cost_info_.table_filters_))) {
      LOG_WARN("failed to assign filters array", K(ret));
    } else if (OB_FAIL(full_filters_.assign(index_merge_path->est_cost_info_.table_filters_))) {
      LOG_WARN("failed to assign filters array", K(ret));
    } else if (OB_FAIL(index_range_conds_.prepare_allocate(index_merge_path->index_cnt_)) ||
               OB_FAIL(index_filters_.prepare_allocate(index_merge_path->index_cnt_))) {
      LOG_WARN("failed to prepare allocate index range filters", K(ret));
    } else if (OB_FAIL(set_index_table_scan_filters(index_merge_path->root_))) {
      LOG_WARN("failed to set index table scan filters", K(ret));
    }
  }
  LOG_TRACE("index merge set range conds and filters", K(index_range_conds_), K(index_filters_));
  return ret;
}

int ObLogTableScan::set_index_table_scan_filters(ObIndexMergeNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KPC(node));
  } else if (node->is_scan_node()) {
    bool is_get = false;
    ObOptimizerContext *opt_ctx = nullptr;
    ObSqlSchemaGuard *schema_guard = nullptr;
    const share::schema::ObTableSchema *index_schema = nullptr;
    AccessPath *ap = nullptr;
    /*
    * virtual table may have hash index,
    * for hash index, if it is a get, we should still extract the range condition
    */
    if (OB_ISNULL(get_plan())
        || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())
        || OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())
        || OB_ISNULL(ap = node->ap_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected nullptr", K(get_plan()), K(opt_ctx), K(schema_guard), K(ap), K(ret));
    } else if (get_contains_fake_cte()) {
      // do nothing
    } else if (OB_FAIL(schema_guard->get_table_schema(table_id_, ap->index_id_, get_stmt(), index_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(is_table_get(is_get))) {
      LOG_WARN("failed to check is table get", K(ret));
    } else if ((index_schema->is_ordered() || is_get) && NULL != ap->get_query_range_provider()) {
      const ObIArray<ObRawExpr *> &range_exprs = ap->get_query_range_provider()->get_range_exprs();
      ObArray<ObRawExpr *> scan_pushdown_filters;
      ObArray<bool> filter_before_index_back;
      ObArray<uint64_t> index_column_ids;
      // extract the filters can be pushed down to index table scan
      for (ObTableSchema::const_column_iterator iter = index_schema->column_begin();
          OB_SUCC(ret) && iter != index_schema->column_end(); ++iter) {
        if (OB_ISNULL(iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr column iter", K(iter), K(ret));
        } else {
          const ObColumnSchemaV2 *column_schema = *iter;
          if (OB_ISNULL(column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr column schema", K(ret));
          } else if (OB_FAIL(index_column_ids.push_back(column_schema->get_column_id()))) {
            LOG_WARN("failed to push back column id", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObOptimizerUtil::check_filter_before_indexback(ap->filter_,
                                                                        index_column_ids,
                                                                        filter_before_index_back))) {
        LOG_WARN("failed to check filter before index back", K(ap->filter_), K(ret));
      } else {
        OB_ASSERT(ap->filter_.count() == filter_before_index_back.count());
        for (int64_t i = 0; OB_SUCC(ret) && i < ap->filter_.count(); i++) {
          if (filter_before_index_back.at(i) && OB_FAIL(scan_pushdown_filters.push_back(ap->filter_.at(i)))) {
            LOG_WARN("failed to push back filter", K(ret));
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_pushdown_filters.count(); i++) {
        bool found_expr = false;
        for (int64_t j = 0; OB_SUCC(ret) && !found_expr && j < range_exprs.count(); j++) {
          if (scan_pushdown_filters.at(i) == range_exprs.at(j)) {
            //有重复表达式，忽略掉
            found_expr = true;
          } else { /* do nothing */ }
        }
        // for virtual table, even if we extract query range, we need to maintain the condition into the filter
        if (OB_SUCC(ret) && (!found_expr || (is_virtual_table(ref_table_id_)))) {
          if (OB_FAIL(index_filters_.at(node->scan_node_idx_).push_back(scan_pushdown_filters.at(i)))) {
            LOG_WARN("add filter expr failed", KPC(node), K(ret));
          } else { /* do nothing */ }
        }
        if (OB_SUCC(ret) && found_expr) {
          if (OB_FAIL(index_range_conds_.at(node->scan_node_idx_).push_back(scan_pushdown_filters.at(i)))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else { /* do nothing */}
        }
      } //end for
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->children_.count(); ++i) {
      if (OB_FAIL(SMART_CALL(set_index_table_scan_filters(node->children_.at(i))))) {
        LOG_WARN("failed to set index table filters", K(ret));
      }
    }
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
  const ObQueryRangeProvider *pre_range = get_pre_graph();
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
  } else if ((index_schema->is_ordered() || is_get) && NULL != pre_range) {
    const ObIArray<ObRawExpr *> &range_exprs = pre_range->get_range_exprs();
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
    } else if (OB_ISNULL(table_meta =
      plan->get_basic_table_metas().get_table_meta_by_table_id(table_id_))) {
      //do nothing
    } else if (OB_FAIL(print_stats_version(*table_meta, buf, buf_len, pos))) {
      LOG_WARN("failed to print stats version", K(ret));
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

    if (OB_SUCC(ret) && use_index_merge()) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("use_index_merge=true"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    }

    if (OB_SUCC(ret) && with_domain_types_.size() > 0) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("with_domain_id("))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < with_domain_types_.size(); i++) {
          ObDomainIdUtils::ObDomainIDType cur_type = static_cast<ObDomainIdUtils::ObDomainIDType>(with_domain_types_[i]);
          if (OB_FAIL(BUF_PRINTF("%s", ObDomainIdUtils::get_domain_str_by_id(cur_type)))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else if ((i != with_domain_types_.size() - 1) && OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(BUF_PRINTF(")"))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          }
        }
      }
    }

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

    if (OB_FAIL(ret) || OB_ISNULL(auto_split_filter_)) {
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      ObRawExpr *auto_split_filter = auto_split_filter_;
      EXPLAIN_PRINT_EXPR(auto_split_filter, type);
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

    if (OB_SUCC(ret) && has_func_lookup()) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF failed", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("has_functional_lookup=true"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    }

    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }

  return ret;
}

int ObLogTableScan::print_stats_version(OptTableMeta &table_meta, char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const ObTimeZoneInfo *cur_tz_info = NULL;
  if (OB_ISNULL(plan = get_plan()) ||
      OB_ISNULL(session_info = plan->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(cur_tz_info = session_info->get_tz_info_wrap().get_time_zone_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret));
  } else {
    char date[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t date_len = 0;
    const ObDataTypeCastParams dtc_params(cur_tz_info);
    ObOTimestampData in_val;
    in_val.time_us_ = table_meta.get_version();
    if (OB_FAIL(ObTimeConverter::otimestamp_to_str(in_val, dtc_params, 6, ObTimestampLTZType, date,
                                                   sizeof(date), date_len))) {
      LOG_WARN("failed to convert otimestamp to string", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(OUTPUT_PREFIX))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("stats info:[version=%.*s", int32_t(date_len), date))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(", is_locked=%d", table_meta.is_stat_locked()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(", is_expired=%d", table_meta.is_opt_stat_expired()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("]"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
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
    } else if (use_index_merge()) {
      if (OB_FAIL(BUF_PRINTF("%s", LEFT_BRACKET))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        ObArray<ObString> index_name_list;
        if (OB_FAIL(get_index_name_list(index_name_list))) {
          LOG_WARN("failed to get index name list", K(ret));
        } else {
          int64_t N = index_name_list.count();
          for (int64_t i = 0; OB_SUCC(ret) && i < N - 1; i++) {
            if (OB_FAIL(BUF_PRINTF("%.*s", index_name_list.at(i).length(), index_name_list.at(i).ptr()))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            } else if (OB_FAIL(BUF_PRINTF(","))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(BUF_PRINTF("%.*s", index_name_list.at(N-1).length(), index_name_list.at(N-1).ptr()))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            } else if (is_descending_direction(get_scan_direction()) &&
                OB_FAIL(BUF_PRINTF("%s", COMMA_REVERSE))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            } else if (OB_FAIL(BUF_PRINTF("%s", RIGHT_BRACKET))) {
              LOG_WARN("BUF_PRINTF fails", K(ret));
            }
          }
        }
      }
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
  if (use_index_merge()) {
    ObArray<ObString> index_name_list;
    if (OB_FAIL(get_index_name_list(index_name_list))) {
      LOG_WARN("failed to get index name list", K(ret));
    } else {
      OB_ASSERT(index_name_list.count() == index_range_conds_.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < index_range_conds_.count(); ++i) {
        const ObString &index_name = index_name_list.at(i);
        const ObIArray<ObRawExpr*> &range_cond = index_range_conds_.at(i);
        const ObIArray<ObRawExpr*> &filter = index_filters_.at(i);
        if (OB_FAIL(BUF_PRINTF("index_name: %.*s, ", index_name.length(), index_name.ptr()))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        }
        EXPLAIN_PRINT_EXPRS(range_cond, type);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else { /* Do nothing */ }
        }
        EXPLAIN_PRINT_EXPRS(filter, type);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(BUF_PRINTF("\n      "))) {
            LOG_WARN("BUF_PRINTF fails", K(ret));
          } else { /* Do nothing */ }
        }
      }
      const ObIArray<ObRawExpr*> &lookup_filter = full_filters_;
      EXPLAIN_PRINT_EXPRS(lookup_filter, type);
    }
  } else {
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
      int64_t skip_scan_offset = get_pre_graph()->get_skip_scan_offset();
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
  }

  if (OB_SUCC(ret) && EXPLAIN_EXTENDED == type) {
    if (pre_range_graph_ != nullptr && pre_range_graph_->is_fast_nlj_range()) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(" is_fast_range = true"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
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
  int64_t index_prefix = index_prefix_;
  ObItemType index_type = T_INDEX_HINT;
  const ObDMLStmt *stmt = NULL;
  bool use_desc_hint = get_scan_direction() == default_desc_direction();
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(stmt->get_query_ctx())) {
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
  if (OB_SUCC(ret)) {
    use_desc_hint &= stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_3_5);
  }
  if (OB_FAIL(ret)) {
  } else if (is_skip_scan()) {
    index_type = use_desc_hint ? T_INDEX_SS_DESC_HINT : T_INDEX_SS_HINT;
    if (ref_table_id_ == index_table_id_) {
      index_name = &ObIndexHint::PRIMARY_KEY;
    } else {
      index_name = &get_index_name();
    }
  } else if (ref_table_id_ == index_table_id_ && index_prefix < 0 && !use_desc_hint) {
    index_type = T_FULL_HINT;
    index_name = &ObIndexHint::PRIMARY_KEY;
  } else {
    index_type = use_desc_hint ? T_INDEX_DESC_HINT : T_INDEX_HINT;
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
    }
    if (OB_SUCC(ret) && use_das()) {
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
    if (OB_SUCC(ret) && use_index_merge()) {
      ObUnionMergeHint union_merge_hint;
      union_merge_hint.set_qb_name(qb_name);
      union_merge_hint.get_table().set_table(*table_item);
      ObIArray<ObString> &index_name_list = union_merge_hint.get_index_name_list();
      if (OB_FAIL(get_index_name_list(index_name_list))) {
        LOG_WARN("failed to get index name list", K(ret));
      } else if (OB_FAIL(union_merge_hint.print_hint(plan_text))) {
        LOG_WARN("failed to print index merge hint", K(ret));
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
    bool is_match_union_match_hint = false;
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
    } else if (OB_FAIL(check_match_union_merge_hint(table_hint, is_match_union_match_hint))) {
      LOG_WARN("failed to check match union merge hint", K(ret));
    } else if (is_match_union_match_hint
               && OB_FAIL(table_hint->union_merge_hint_->print_hint(plan_text))) {
      LOG_WARN("failed to print use union merge hint", K(ret));
    } else if (table_hint->index_list_.empty()) {
      /*do nothing*/
    } else if (OB_UNLIKELY(table_hint->index_list_.count() != table_hint->index_hints_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected log index hint", K(ret), K(*table_hint));
    } else if (table_hint->is_use_index_hint()) {// print used use index hint
      const ObIndexHint *index_hint = NULL;
      if (ObOptimizerUtil::find_item(table_hint->index_list_, index_table_id_, &idx)) {
        if (OB_UNLIKELY(idx < 0 || idx >= table_hint->index_list_.count())
            || OB_ISNULL(index_hint = static_cast<const ObIndexHint *>(table_hint->index_hints_.at(idx)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected idx", K(ret), K(idx), K(table_hint->index_list_));
        } else if (!is_skip_scan() && index_hint->use_skip_scan()) {
          /* is not index skip scan but exist index_ss hint */
        } else if (index_hint->is_trans_added()) {
          //do nothing
        } else if (OB_FAIL(index_hint->print_hint(plan_text))) {
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

  ENABLE_OPT_TRACE_COST_MODEL;
  if (NULL == est_cost_info_) {
    //fake cte path
  } else if (OB_FAIL(do_re_est_cost(param, card, op_cost, cost))) {
    LOG_WARN("failed to re est cost error", K(ret));
  } else {
    set_op_cost(op_cost);
    set_cost(cost);
    set_card(card);
    LOG_TRACE("push limit into table scan", K(param), K(op_cost), K(cost), K(card));
  }
  DISABLE_OPT_TRACE_COST_MODEL;
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
        || table_schema->is_spatial_index() || table_schema->is_vec_index()) {
      ctx.set_force_partition();
    }
  } else { /*do nothing*/ }

  return ret;
}

int ObLogTableScan::is_table_get(bool &is_get) const
{
  int ret = OB_SUCCESS;
  const ObQueryRangeProvider *pre_range = get_pre_graph();
  if (pre_range != NULL) {
    if (OB_FAIL(pre_range->is_get(is_get))) {
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
  if (use_batch()) {
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
  }
  LOG_TRACE("extract bnlj params", K(use_batch_), K(bnlj_params), K(ret));
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

int ObLogTableScan::create_exec_param_for_auto_split(const ObExprResType &type, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObExecParamRawExpr *exec_param = NULL;
  ObRawExprFactory *expr_factory = NULL;
  expr = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(ret), K(ref_table_id_));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_QUESTIONMARK, exec_param))) {
    LOG_WARN("failed to create exec param expr", K(ret));
  } else if (OB_ISNULL(exec_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec param is null", K(ret), K(exec_param));
  } else {
    exec_param->set_ref_expr(NULL);
    exec_param->set_result_type(type);
    exec_param->set_eval_by_storage(true);
    expr = exec_param;
  }
  return ret;
}

int ObLogTableScan::construct_table_split_range_filter(ObSQLSessionInfo *session, const int64_t filter_type)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *lower_bound_vec = NULL;
  ObOpRawExpr *upper_bound_vec = NULL;
  ObRawExpr *filter_type_expr = NULL;
  ObRawExpr *pass_by_expr = NULL;
  ObRawExpr *lower_bound_expr = NULL;
  ObRawExpr *upper_bound_expr = NULL;
  ObRawExpr *lower_bound_filter = NULL;
  ObRawExpr *upper_bound_filter = NULL;
  ObRawExpr *and_expr = NULL;
  ObRawExpr *or_expr = NULL;
  ObSEArray<ObRawExpr*, 2> or_param_exprs;
  ObSEArray<ObRawExpr*, 2> and_param_exprs;

  ObExprResType res_type;
  res_type.set_type(ObIntType);
  res_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);

  share::schema::ObPartitionLevel part_level = share::schema::PARTITION_LEVEL_MAX;
  ObRawExpr *part_expr = NULL;
  ObRawExpr *subpart_expr = NULL;
  ObRawExprFactory *expr_factory = NULL;

  // construct auto split filter
  // :0 and (:1, :2) <= (partkey) and (partkey) < (:3, :4)
  // The order in auto_split_params_ should not change
  // construct auto split params: filter_type - filter_struct
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session", K(ret), KP(session));
  } else if (OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(ret), K(ref_table_id_));
  } else if (OB_FAIL(create_exec_param_for_auto_split(res_type, pass_by_expr))) {
    LOG_WARN("failed to create exec param", K(ret));
  } else if (OB_FAIL(auto_split_params_.push_back(pass_by_expr))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(get_plan()->get_part_exprs(table_id_,
                                                ref_table_id_,
                                                part_level,
                                                part_expr,
                                                subpart_expr))) {
    LOG_WARN("fail to get part exprs", K(ret));
  } else if (OB_ISNULL(part_expr) ||
             OB_UNLIKELY(PARTITION_LEVEL_ONE != part_level)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet", K(table_id_), K(ref_table_id_), K(part_level), KPC(part_expr));
  } else if (T_OP_ROW == part_expr->get_expr_type()) {
    int64_t row_length = part_expr->get_param_count();
    if (OB_FAIL(expr_factory->create_raw_expr(T_OP_ROW, lower_bound_vec))) {
      LOG_WARN("create to_type expr failed", K(ret));
    } else if (OB_FAIL(expr_factory->create_raw_expr(T_OP_ROW, upper_bound_vec))) {
      LOG_WARN("create to_type expr failed", K(ret));
    } else if (OB_ISNULL(lower_bound_vec) || OB_ISNULL(upper_bound_vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(upper_bound_vec), K(lower_bound_vec));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_length; i ++) {
      ObRawExpr *lower_exec_param = NULL;
      ObRawExpr *part_key = NULL;
      if (OB_ISNULL(part_key = part_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", KPC(part_expr), K(i));
      } else if (OB_FAIL(create_exec_param_for_auto_split(part_key->get_result_type(), lower_exec_param))) {
        LOG_WARN("failed to create exec param", K(ret));
      } else if (OB_FAIL(lower_bound_vec->add_param_expr(lower_exec_param))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (OB_FAIL(auto_split_params_.push_back(lower_exec_param))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_length; i ++) {
      ObRawExpr *upper_exec_param = NULL;
      ObRawExpr *part_key = NULL;
      if (OB_ISNULL(part_key = part_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", KPC(part_expr), K(i));
      } else if (OB_FAIL(create_exec_param_for_auto_split(part_key->get_result_type(), upper_exec_param))) {
        LOG_WARN("failed to create exec param", K(ret));
      } else if (OB_FAIL(upper_bound_vec->add_param_expr(upper_exec_param))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (OB_FAIL(auto_split_params_.push_back(upper_exec_param))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      lower_bound_expr = lower_bound_vec;
      upper_bound_expr = upper_bound_vec;
    }
  } else {
    if (OB_FAIL(create_exec_param_for_auto_split(part_expr->get_result_type(), lower_bound_expr))) {
      LOG_WARN("failed to create exec param", K(ret));
    } else if (OB_FAIL(create_exec_param_for_auto_split(part_expr->get_result_type(), upper_bound_expr))) {
      LOG_WARN("failed to create exec param", K(ret));
    } else if (OB_FAIL(auto_split_params_.push_back(lower_bound_expr))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(auto_split_params_.push_back(upper_bound_expr))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory,
                                                                 T_OP_LE,
                                                                 lower_bound_expr,
                                                                 part_expr,
                                                                 lower_bound_filter))) {
    LOG_WARN("failed to build filter", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory,
                                                                 T_OP_LT,
                                                                 part_expr,
                                                                 upper_bound_expr,
                                                                 upper_bound_filter))) {
    LOG_WARN("failed to build filter", K(ret));
  } else if (OB_ISNULL(lower_bound_filter) || OB_ISNULL(upper_bound_filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(lower_bound_filter), K(upper_bound_filter));
  } else if (OB_FAIL(lower_bound_filter->add_flag(IS_AUTO_PART_EXPR))) {
    LOG_WARN("failed to add flag", K(ret));
  } else if (OB_FAIL(upper_bound_filter->add_flag(IS_AUTO_PART_EXPR))) {
    LOG_WARN("failed to add flag", K(ret));
  } else if (OB_FAIL(and_param_exprs.push_back(lower_bound_filter))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(and_param_exprs.push_back(upper_bound_filter))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory,
                                                    and_param_exprs,
                                                    and_expr))) {
    LOG_WARN("failed to build and expr", K(ret));
  } else if (OB_ISNULL(and_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr null pointer", K(ret), K(and_expr));
  } else if (OB_FAIL(or_param_exprs.push_back(pass_by_expr))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(or_param_exprs.push_back(and_expr))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_or_exprs(*expr_factory,
                                                    or_param_exprs,
                                                    or_expr))) {
    LOG_WARN("failed to build or expr", K(ret));
  } else if (OB_ISNULL(or_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr null pointer", K(ret), K(or_expr));
  } else if (OB_FAIL(or_expr->formalize(session))) {
    LOG_WARN("failed to formalize", K(ret));
  } else {
    auto_split_filter_type_ = filter_type;
    auto_split_filter_ = or_expr;
  }

  return ret;
}

int ObLogTableScan::check_need_table_split_range_filter(ObSchemaGetterGuard &schema_guard,
                                                        const ObTableSchema &table_schema,
                                                        bool &need_filter)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const int64_t table_id = table_schema.get_table_id();
  const int64_t data_table_id = table_schema.get_data_table_id();
  const ObTableSchema *data_table_schema = nullptr;
  if (OB_UNLIKELY(!(table_schema.is_range_part() && !table_schema.is_interval_part()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition type", K(ret), K(table_id), K(table_schema.get_part_option()));
  } else if (table_schema.is_index_local_storage()) {
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("failed to get data table schema", K(ret), K(table_id), K(data_table_id));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table schema is null", K(ret), K(table_id), K(data_table_id));
    } else {
      const ObPartitionKeyInfo &partkey_info = data_table_schema->get_partition_key_info();
      const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
      bool partkey_is_rowkey_prefix = true;
      if (OB_UNLIKELY(!partkey_info.is_valid() || !rowkey_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid partkey info or rowkey info", K(ret), K(partkey_info), K(rowkey_info));
      }
      for (int64_t i = 0; OB_SUCC(ret) && partkey_is_rowkey_prefix && i < partkey_info.get_size(); i++) {
        uint64_t partkey_column_id = OB_INVALID_ID;
        uint64_t rowkey_column_id = OB_INVALID_ID;
        if (OB_FAIL(partkey_info.get_column_id(i, partkey_column_id))) {
          LOG_WARN("failed to get partkey column id", K(ret), K(partkey_info), K(i));
        } else if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
          LOG_WARN("failed to get rowkey column id", K(ret), K(rowkey_info), K(i));
        } else if (partkey_column_id != rowkey_column_id) {
          partkey_is_rowkey_prefix = false;
        }
      }
      if (OB_SUCC(ret)) {
        need_filter = !partkey_is_rowkey_prefix;
      }
    }
  } else {
    need_filter = false;
  }
  return ret;
}

int ObLogTableScan::extract_doc_id_index_back_expr(ObIArray<ObRawExpr *> &exprs, bool is_vec_scan)
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
      } else if ((!is_vec_scan && col_schema->is_doc_id_column()) || (is_vec_scan && col_schema->is_vec_hnsw_vid_column())) {
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

int ObLogTableScan::extract_text_retrieval_access_expr(ObTextRetrievalInfo &tr_info,
                                                       ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
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

int ObLogTableScan::extract_vec_idx_access_expr(ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  ObVecIndexInfo &vec_info = get_vector_index_info();
  ObSqlSchemaGuard *schema_guard = nullptr;
  const ObTableSchema *table_schema = nullptr;
  ObSEArray<ColumnItem, 4> col_items;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan())  ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(get_stmt()), KP(get_plan()),  KP(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
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
    if (OB_FAIL(ret)) {
    } else if (vec_info.is_hnsw_vec_scan()) {
      if (OB_FAIL(exprs.push_back(vec_info.vec_id_column_))) {
        LOG_WARN("failed to append vid column to access exprs", K(ret));
      } else if (OB_FAIL(exprs.push_back(vec_info.target_vec_column_))) {
        LOG_WARN("failed to append target vec column to access exprs", K(ret));
      } else {
        for (int i = 0; i < ObVectorHNSWColumnIdx::HNSW_MAX_COL_CNT && OB_SUCC(ret); ++i) {
          if (i < vec_info.aux_table_column_.count()) {
            if (OB_FAIL(exprs.push_back(vec_info.aux_table_column_.at(i)))) {
              LOG_WARN("failed to append aux column to access exprs", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected hnsw aux column count", K(ret));
          } // for each col
        }// end for
      }
    } else if (vec_info.is_ivf_vec_scan()) {
      int aux_table_column_cnt = 0;
      if (vec_info.is_ivf_flat_scan()) {
        aux_table_column_cnt = ObVectorIVFFlatColumnIdx::IVF_FLAT_ROWKEY_START;
      } else if (vec_info.is_ivf_sq_scan()) {
        aux_table_column_cnt = ObVectorIVFSQColumnIdx::IVF_SQ_ROWKEY_START;
      } else if (vec_info.is_ivf_pq_scan()) {
        aux_table_column_cnt = ObVectorIVFPQColumnIdx::IVF_PQ_ROWKEY_START;
      }
      for (int i = 0; i < aux_table_column_cnt && OB_SUCC(ret); ++i) {
        if (i < vec_info.aux_table_column_.count()) {
          if (OB_FAIL(exprs.push_back(vec_info.aux_table_column_.at(i)))) {
            LOG_WARN("failed to append aux column to access exprs", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected hnsw aux column count", K(ret));
        }  // for each col
      }    // end for
    }
  }

  return ret;
}

int ObLogTableScan::get_vec_idx_calc_exprs(ObIArray<ObRawExpr *> &all_exprs) // check all expr
{
  int ret = OB_SUCCESS;
  ObVecIndexInfo &vec_info = get_vector_index_info();
  if (OB_ISNULL(vec_info.sort_key_.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null vector sort expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(vec_info.sort_key_.expr_))) {
    LOG_WARN("failed to append vector sort expr", K(ret));
  } else if (OB_NOT_NULL(vec_info.topk_limit_expr_) &&
             OB_FAIL(all_exprs.push_back(vec_info.topk_limit_expr_))) {
    LOG_WARN("failed to append limit expr", K(ret));
  } else if (OB_NOT_NULL(vec_info.topk_offset_expr_) &&
             OB_FAIL(all_exprs.push_back(vec_info.topk_offset_expr_))) {
    LOG_WARN("failed to append offset expr", K(ret));
  } else if (is_pre_vec_idx_scan()) {
    if (vec_info.is_hnsw_vec_scan()) {
      if (OB_FAIL(all_exprs.push_back(vec_info.vec_id_column_))) {
        LOG_WARN("failed to append vid column to access exprs", K(ret));
      } else if (OB_FAIL(all_exprs.push_back(vec_info.target_vec_column_))) {
        LOG_WARN("failed to append snapshot_data column to access exprs", K(ret));
      } else {
        for (int i = 0; i < ObVectorHNSWColumnIdx::HNSW_MAX_COL_CNT && OB_SUCC(ret); ++i) {
          if (i < vec_info.aux_table_column_.count()) {
            if (OB_FAIL(all_exprs.push_back(vec_info.aux_table_column_.at(i)))) {
              LOG_WARN("failed to append aux column to access exprs", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected hnsw aux column count", K(ret));
          } // for each col expr
        }// end for
      }
    } else {
      if (vec_info.is_ivf_vec_scan()) {
        for (int i = 0; i < vec_info.aux_table_column_.count() && OB_SUCC(ret); ++i) {
          if (OB_FAIL(all_exprs.push_back(vec_info.aux_table_column_.at(i)))) {
            LOG_WARN("failed to append aux column to access exprs", K(ret));
          }
        } // end for
      }
    }
  }
  return ret;
}

int ObLogTableScan::get_text_retrieval_calc_exprs(ObTextRetrievalInfo &tr_info,
                                                  ObIArray<ObRawExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tr_info.match_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match against expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info.related_doc_cnt_))) {
    LOG_WARN("failed to append relevanced doc cnt expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info.doc_token_cnt_))) {
    LOG_WARN("failed to append doc token cnt expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info.total_doc_cnt_))) {
    LOG_WARN("failed to append total doc cnt expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info.relevance_expr_))) {
    LOG_WARN("failed to append relevance expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info.match_expr_))) {
    LOG_WARN("failed to append text retrieval expr", K(ret));
  } else if (nullptr != tr_info.pushdown_match_filter_
      && OB_FAIL(all_exprs.push_back(tr_info.pushdown_match_filter_))) {
    LOG_WARN("failed to append match filter", K(ret));
  } else if (nullptr != tr_info.topk_limit_expr_
      && OB_FAIL(all_exprs.push_back(tr_info.topk_limit_expr_))) {
    LOG_WARN("failed to append limit expr", K(ret));
  } else if (nullptr != tr_info.topk_offset_expr_
      && OB_FAIL(all_exprs.push_back(tr_info.topk_offset_expr_))) {
    LOG_WARN("failed to append offset expr", K(ret));
  }
  return ret;
}

int ObLogTableScan::extract_func_lookup_access_exprs(ObIArray<ObRawExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < lookup_tr_infos_.count(); ++i) {
    if (OB_FAIL(extract_text_retrieval_access_expr(lookup_tr_infos_.at(i), all_exprs))) {
      LOG_WARN("failed to extract text retrieval access expr", K(ret), K(i), K(lookup_tr_infos_.at(i)));
    }
  }

  return ret;
}

int ObLogTableScan::get_func_lookup_calc_exprs(ObIArray<ObRawExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < lookup_tr_infos_.count(); ++i) {
    if (OB_FAIL(get_text_retrieval_calc_exprs(lookup_tr_infos_.at(i), all_exprs))) {
      LOG_WARN("failed to get text retrieval calc expr", K(ret), K(i), K(lookup_tr_infos_.at(i)));
    }
  }

  return ret;
}

int ObLogTableScan::extract_index_merge_access_exprs(ObIArray<ObRawExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_tr_infos_.count(); ++i) {
    if (OB_FAIL(extract_text_retrieval_access_expr(merge_tr_infos_.at(i), all_exprs))) {
      LOG_WARN("failed to extract text retrieval access expr", K(ret), K(i), K(merge_tr_infos_.at(i)));
    }
  }

  return ret;
}

int ObLogTableScan::get_index_merge_calc_exprs(ObIArray<ObRawExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_tr_infos_.count(); ++i) {
    if (OB_FAIL(get_text_retrieval_calc_exprs(merge_tr_infos_.at(i), all_exprs))) {
      LOG_WARN("failed to get text retrieval calc expr", K(ret), K(i), K(merge_tr_infos_.at(i)));
    }
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

int ObLogTableScan::generate_auto_split_filter()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  const share::schema::ObTableSchema *table_schema = NULL;
  bool need_filter = false;
  ObDMLStmt *stmt = NULL;
  const int64_t table_id = is_index_scan() ? index_table_id_ : ref_table_id_;

  if (OB_ISNULL(get_plan())
      || OB_UNLIKELY(OB_INVALID_ID == table_id)
      || OB_ISNULL(stmt = const_cast<ObDMLStmt *>(get_stmt()))
      || OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())
      || OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(ret), K(ref_table_id_));
  } else if (get_contains_fake_cte() || is_virtual_table(table_id)) {
    // skip mock table and virtual table
  } else if (OB_FAIL(schema_guard->get_table_schema(
      session->get_effective_tenant_id(),
      table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", K(ret), K(table_id), K(is_index_scan()), K(index_table_id_), K(ref_table_id_), K(table_id_));
  } else if (table_schema->get_hidden_partition_num() > 0) {
    bool need_filter = false;
    if (table_schema->is_range_part() && !table_schema->is_interval_part()) {
      if (OB_FAIL(check_need_table_split_range_filter(*schema_guard, *table_schema, need_filter))) {
        LOG_WARN("failed to check need filter", K(ret));
      } else if (need_filter && OB_FAIL(construct_table_split_range_filter(session, static_cast<int64_t>(ObTabletSplitType::RANGE)))) {
        LOG_WARN("fail to construct table split range filter", K(ret));
      }
    } else if (table_schema->is_key_part()) {
    } else if (table_schema->is_hash_part()) {
    } else if (table_schema->is_list_part()) {
    } else {
    }
    if (OB_SUCC(ret) && need_filter) {
      LOG_INFO("generate filter for splitting table", K(need_filter), K(ref_table_id_), K(table_id_),
          K(get_index_back()), K(is_index_scan()), K(table_schema->get_table_id()), K(table_schema->get_schema_version()));
    }
  }
  return ret;
}

int ObLogTableScan::prepare_vector_access_exprs()
{
  int ret = OB_SUCCESS;
  bool is_all_inited = false;
  ObVecIndexInfo &vc_info = get_vector_index_info();
  if (OB_FAIL(vc_info.check_vec_aux_column_is_all_inited(is_all_inited))) {
    LOG_WARN("failed to check hnsw aux column is all inited", K(ret));
  } else if (is_all_inited) {
    // do nothing, exprs already generated
  } else if (get_vector_index_info().is_hnsw_vec_scan()) {
    if (OB_FAIL(prepare_hnsw_vector_access_exprs())) {
      LOG_WARN("fail to prepare hnsw vector access exprs", K(ret));
    }
  } else if (get_vector_index_info().is_ivf_vec_scan()) {
    if (OB_FAIL(prepare_ivf_vector_access_exprs())) {
      LOG_WARN("fail to prepare hnsw vector access exprs", K(ret));
    }
  }
  return ret;
}

int ObLogTableScan::prepare_ivf_pq_access_exprs(const ObTableSchema *table_schema,
                                                ObSqlSchemaGuard *schema_guard,
                                                TableItem *table_item,
                                                ObRawExprFactory *expr_factory,
                                                ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *ivf_pq_id_tbl = nullptr;
  const ObTableSchema *ivf_pq_code_tbl = nullptr;
  const ObTableSchema *ivf_pq_rowkey_cid_tbl = nullptr;
  ObVecIndexInfo &vc_info = get_vector_index_info();
  ObColumnRefRawExpr *ivf_pq_id_pid_column = nullptr;
  ObColumnRefRawExpr *ivf_pq_id_center_column = nullptr;
  ObColumnRefRawExpr *ivf_pq_code_cid_column = nullptr;
  ObColumnRefRawExpr *ivf_pq_code_pids_column = nullptr;
  ObColumnRefRawExpr *ivf_rowkey_cid_cid_column = nullptr;
  ObColumnRefRawExpr *ivf_rowkey_cid_pids_column = nullptr;
  ObArray<uint64_t> rowkey_cids;
  if (OB_ISNULL(table_schema) || OB_ISNULL(schema_guard) || OB_ISNULL(table_item)
     || OB_ISNULL(expr_factory) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP(table_schema), KP(schema_guard), KP(table_item), KP(expr_factory), KP(session_info));
  } else if (vc_info.aux_table_column_.count() != (ObVectorIVFFlatColumnIdx::IVF_CENTROID_CENTER_COL + 1)) {
     ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected aux column cnt", K(ret), K(vc_info.aux_table_id_.count()));
  } else if (OB_FAIL(schema_guard->get_table_schema(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_FOURTH_AUX_TBL_IDX), ivf_pq_id_tbl))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(ivf_pq_id_tbl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), K(vc_info.aux_table_id_.count()), K(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_FOURTH_AUX_TBL_IDX)));
  } else if (OB_FAIL(schema_guard->get_table_schema(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX), ivf_pq_code_tbl))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(ivf_pq_code_tbl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), K(vc_info.aux_table_id_.count()), K(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX)));
  } else if (OB_FAIL(schema_guard->get_table_schema(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX), ivf_pq_rowkey_cid_tbl))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(ivf_pq_rowkey_cid_tbl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), K(vc_info.aux_table_id_.count()), K(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX)));
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_cids))) {
    LOG_WARN("fail to get rowkey column ids in rowkey vid", K(ret), KPC(ivf_pq_code_tbl));
  } else if (OB_FAIL(prepare_ivf_aux_tbl_cid_and_center_col_access_exprs(ivf_pq_id_tbl, table_schema, expr_factory, table_item,
                                                                        ivf_pq_id_pid_column, ivf_pq_id_center_column, false/*is_cid*/, true/*is_center*/))) {
    LOG_WARN("fail to prepare ivf cid vec tbl access exprs", K(ret));
  } else if (OB_FAIL(prepare_ivf_aux_tbl_cid_and_pids_col_access_exprs(ivf_pq_code_tbl, table_schema, expr_factory, table_item,
                                                                      ivf_pq_code_cid_column, ivf_pq_code_pids_column))) {
    LOG_WARN("fail to prepare_ivf_aux_tbl_cid_and_pids_col_access_exprs", K(ret));
  } else if (OB_FAIL(prepare_ivf_aux_tbl_cid_and_pids_col_access_exprs(ivf_pq_rowkey_cid_tbl, table_schema, expr_factory, table_item,
                                                                      ivf_rowkey_cid_cid_column, ivf_rowkey_cid_pids_column))) {
    LOG_WARN("fail to prepare_ivf_aux_tbl_cid_and_pids_col_access_exprs", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if ( OB_ISNULL(ivf_pq_id_pid_column) || OB_ISNULL(ivf_pq_id_center_column)
            || OB_ISNULL(ivf_pq_code_cid_column) || OB_ISNULL(ivf_pq_code_pids_column)
            || OB_ISNULL(ivf_rowkey_cid_cid_column) || OB_ISNULL(ivf_rowkey_cid_pids_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null vetor index generated column", K(ret),
        KP(ivf_pq_id_tbl), KP(ivf_pq_id_pid_column), KP(ivf_pq_id_center_column), KP(ivf_pq_code_cid_column),
        KP(ivf_pq_code_pids_column), KP(ivf_rowkey_cid_cid_column), KP(ivf_rowkey_cid_pids_column));
  /* column must add in order, same as ObVectorIVFPQColumnIdx*/
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(ivf_pq_id_pid_column))) {
    LOG_WARN("fail to push back aux column", K(ret));
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(ivf_pq_id_center_column))) {
    LOG_WARN("fail to push back aux column", K(ret));
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(ivf_rowkey_cid_cid_column))) {
    LOG_WARN("fail to push back aux column", K(ret));
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(ivf_rowkey_cid_pids_column))) {
    LOG_WARN("fail to push back aux column", K(ret));
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(ivf_pq_code_cid_column))) {
    LOG_WARN("fail to push back aux column", K(ret));
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(ivf_pq_code_pids_column))) {
    LOG_WARN("fail to push back aux column", K(ret));
  }
  return ret;
}

int ObLogTableScan::prepare_ivf_vector_access_exprs()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  ObSqlSchemaGuard *schema_guard  = nullptr;
  TableItem *table_item = nullptr;
  ObRawExprFactory *expr_factory = nullptr;
  ObSQLSessionInfo *session_info = nullptr;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(get_real_ref_table_id(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (OB_FAIL(prepare_ivf_common_tbl_access_exprs(table_schema, schema_guard, table_item, expr_factory, session_info))) {
    LOG_WARN("prepare_ivf_rowkey_access_exprs", K(ret));
  } else if (get_vector_index_info().is_ivf_flat_scan() || get_vector_index_info().is_ivf_sq_scan()) {
    if (OB_FAIL(prepare_ivf_flat_and_sq_access_exprs(table_schema, schema_guard, table_item, expr_factory, session_info))) {
      LOG_WARN("fail to prepare ivf flat and sq vector access exprs", K(ret));
    }
  } else if (get_vector_index_info().is_ivf_pq_scan()) {
    if (OB_FAIL(prepare_ivf_pq_access_exprs(table_schema, schema_guard, table_item, expr_factory, session_info))) {
      LOG_WARN("fail to prepare ivf pq vector access exprs", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected IVF type", K(ret));
  }
  return ret;
}

int ObLogTableScan::prepare_ivf_common_tbl_access_exprs(const ObTableSchema *table_schema,
                                                        ObSqlSchemaGuard *schema_guard,
                                                        TableItem *table_item,
                                                        ObRawExprFactory *expr_factory,
                                                        ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *ivf_center_id_tbl = nullptr;
  ObVecIndexInfo &vc_info = get_vector_index_info();
  ObColumnRefRawExpr *cid_column = nullptr;
  ObColumnRefRawExpr *center_column = nullptr;
  if (OB_ISNULL(table_schema) || OB_ISNULL(schema_guard) || OB_ISNULL(table_item)
     || OB_ISNULL(expr_factory) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP(table_schema), KP(schema_guard), KP(table_item), KP(expr_factory), KP(session_info));
  } else if (OB_FAIL(schema_guard->get_table_schema(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX), ivf_center_id_tbl))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(ivf_center_id_tbl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), K(vc_info.aux_table_id_.count()), K(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX)));
  } else if (OB_FAIL(prepare_ivf_aux_tbl_cid_and_center_col_access_exprs(ivf_center_id_tbl, table_schema, expr_factory, table_item,
                                                                        cid_column, center_column, true, true))) {
    LOG_WARN("fail to prepare ivf cid vec tbl access exprs", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(cid_column) || OB_ISNULL(center_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null vetor index generated column", K(ret),
        KP(cid_column), KP(center_column));
  /* column must add in order, same as ObVectorIVFFlatColumnIdx*/
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(cid_column))) {
    LOG_WARN("fail to push back delta vid column", K(ret));
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(center_column))) {
    LOG_WARN("fail to push back delta type column", K(ret));
  }
  return ret;
}

int ObLogTableScan::prepare_ivf_rowkey_cid_tbl_access_exprs(const ObTableSchema *ivf_rowkey_cid_tbl,
                                                            const ObTableSchema *table_schema,
                                                            ObRawExprFactory *expr_factory,
                                                            TableItem *table_item,
                                                            ObColumnRefRawExpr *&rowkey_cid_cid_column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ivf_rowkey_cid_tbl) || OB_ISNULL(table_schema)
      || OB_ISNULL(expr_factory) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is null", K(ret), KP(ivf_rowkey_cid_tbl), KP(table_schema), KP(expr_factory));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ivf_rowkey_cid_tbl->get_column_count() && OB_ISNULL(rowkey_cid_cid_column); ++i) {
      const ObColumnSchemaV2 *data_col_schema = nullptr;
      const ObColumnSchemaV2 *col_schema = ivf_rowkey_cid_tbl->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (OB_ISNULL(data_col_schema = table_schema->get_column_schema(col_schema->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (data_col_schema->is_vec_ivf_center_id_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, rowkey_cid_cid_column))) {
          LOG_WARN("failed to build vec aux column expr", K(ret));
        } else if (OB_NOT_NULL(rowkey_cid_cid_column)) {
          rowkey_cid_cid_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          rowkey_cid_cid_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          rowkey_cid_cid_column->set_database_name(table_item->database_name_);
        }
      }
    } // end for
  }
  return ret;
}

int ObLogTableScan::prepare_ivf_aux_tbl_cid_and_center_col_access_exprs(const ObTableSchema *ivf_cid_vec_tbl,
                                                                        const ObTableSchema *table_schema,
                                                                        ObRawExprFactory *expr_factory,
                                                                        TableItem *table_item,
                                                                        ObColumnRefRawExpr *&id_column,
                                                                        ObColumnRefRawExpr *&center_column,
                                                                        bool is_cid, bool is_center)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ivf_cid_vec_tbl) || OB_ISNULL(table_schema)
      || OB_ISNULL(expr_factory) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is null", K(ret), KP(ivf_cid_vec_tbl), KP(table_schema), KP(expr_factory));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ivf_cid_vec_tbl->get_column_count() && (OB_ISNULL(id_column) || OB_ISNULL(center_column)); ++i) {
      const ObColumnSchemaV2 *data_col_schema = nullptr;
      const ObColumnSchemaV2 *col_schema = ivf_cid_vec_tbl->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (OB_ISNULL(data_col_schema = table_schema->get_column_schema(col_schema->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if ((is_cid && data_col_schema->is_vec_ivf_center_id_column())
      || (!is_cid && data_col_schema->is_vec_ivf_pq_center_id_column())) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, id_column))) {
          LOG_WARN("failed to build vec aux column expr", K(ret));
        } else if (OB_NOT_NULL(id_column)) {
          id_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          id_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          id_column->set_database_name(table_item->database_name_);
        }
      } else if ((is_center && data_col_schema->is_vec_ivf_center_vector_column())
        || (!is_center && data_col_schema->is_vec_ivf_data_vector_column())) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, center_column))) {
          LOG_WARN("failed to build vec type column expr", K(ret));
        } else if (OB_NOT_NULL(center_column)) {
          center_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          center_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          center_column->set_database_name(table_item->database_name_);
        }
      }
    } // end for
  }
  return ret;
}

int ObLogTableScan::prepare_ivf_aux_tbl_cid_and_pids_col_access_exprs(const ObTableSchema *aux_tbl,
                                                                    const ObTableSchema *table_schema,
                                                                    ObRawExprFactory *expr_factory,
                                                                    TableItem *table_item,
                                                                    ObColumnRefRawExpr *&cid_column,
                                                                    ObColumnRefRawExpr *&pids_column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(aux_tbl) || OB_ISNULL(table_schema)
      || OB_ISNULL(expr_factory) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is null", K(ret), KP(aux_tbl), KP(table_schema), KP(expr_factory));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < aux_tbl->get_column_count() && (OB_ISNULL(cid_column) || OB_ISNULL(pids_column)); ++i) {
      const ObColumnSchemaV2 *data_col_schema = nullptr;
      const ObColumnSchemaV2 *col_schema = aux_tbl->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (OB_ISNULL(data_col_schema = table_schema->get_column_schema(col_schema->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      }else if (data_col_schema->is_vec_ivf_center_id_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, cid_column))) {
          LOG_WARN("failed to build vec aux column expr", K(ret));
        } else if (OB_NOT_NULL(cid_column)) {
          cid_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          cid_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          cid_column->set_database_name(table_item->database_name_);
        }
      } else if (data_col_schema->is_vec_ivf_pq_center_ids_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, pids_column))) {
          LOG_WARN("failed to build vec aux column expr", K(ret));
        } else if (OB_NOT_NULL(pids_column)) {
          pids_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          pids_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          pids_column->set_database_name(table_item->database_name_);
        }
      }
    } // end for
  }
  return ret;
}

int ObLogTableScan::prepare_ivf_sq_meta_tbl_access_exprs(const ObTableSchema *ivf_sq_meta_tbl,
                                                        const ObTableSchema *table_schema,
                                                        ObRawExprFactory *expr_factory,
                                                        TableItem *table_item,
                                                        ObColumnRefRawExpr *&sq_meta_id_column,
                                                        ObColumnRefRawExpr *&sq_meta_vec_column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ivf_sq_meta_tbl) || OB_ISNULL(table_schema)
      || OB_ISNULL(expr_factory) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is null", K(ret), KP(ivf_sq_meta_tbl), KP(table_schema), KP(expr_factory));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ivf_sq_meta_tbl->get_column_count() && (OB_ISNULL(sq_meta_id_column) || OB_ISNULL(sq_meta_vec_column)); ++i) {
      const ObColumnSchemaV2 *data_col_schema = nullptr;
      const ObColumnSchemaV2 *col_schema = ivf_sq_meta_tbl->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (OB_ISNULL(data_col_schema = table_schema->get_column_schema(col_schema->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (data_col_schema->is_vec_ivf_meta_id_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, sq_meta_id_column))) {
          LOG_WARN("failed to build vec aux column expr", K(ret));
        } else if (OB_NOT_NULL(sq_meta_id_column)) {
          sq_meta_id_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          sq_meta_id_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          sq_meta_id_column->set_database_name(table_item->database_name_);
        }
      } else if (data_col_schema->is_vec_ivf_meta_vector_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, sq_meta_vec_column))) {
          LOG_WARN("failed to build vec type column expr", K(ret));
        } else if (OB_NOT_NULL(sq_meta_vec_column)) {
          sq_meta_vec_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          sq_meta_vec_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          sq_meta_vec_column->set_database_name(table_item->database_name_);
        }
      }
    } // end for
  }
  return ret;
}

int ObLogTableScan::prepare_ivf_flat_and_sq_access_exprs(const ObTableSchema *table_schema,
                                                        ObSqlSchemaGuard *schema_guard,
                                                        TableItem *table_item,
                                                        ObRawExprFactory *expr_factory,
                                                        ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *ivf_cid_vec_tbl = nullptr;
  const ObTableSchema *ivf_rowkey_cid_tbl = nullptr;
  const ObTableSchema *ivf_sq_meta_tbl = nullptr;
  ObVecIndexInfo &vc_info = get_vector_index_info();
  ObColumnRefRawExpr *cid_vec_cid_column = nullptr;
  ObColumnRefRawExpr *cid_vec_vec_column = nullptr;
  ObColumnRefRawExpr *rowkey_cid_cid_column = nullptr;
  ObColumnRefRawExpr *sq_meta_id_column = nullptr;
  ObColumnRefRawExpr *sq_meta_vec_column = nullptr;
  ObArray<uint64_t> rowkey_cids;
  if (OB_ISNULL(table_schema) || OB_ISNULL(schema_guard) || OB_ISNULL(table_item)
     || OB_ISNULL(expr_factory) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP(table_schema), KP(schema_guard), KP(table_item), KP(expr_factory), KP(session_info));
  } else if (vc_info.aux_table_column_.count() != (ObVectorIVFFlatColumnIdx::IVF_CENTROID_CENTER_COL + 1)) {
     ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected aux column cnt", K(ret), K(vc_info.aux_table_id_.count()));
  } else if (OB_FAIL(schema_guard->get_table_schema(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX), ivf_cid_vec_tbl))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(ivf_cid_vec_tbl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), K(vc_info.aux_table_id_.count()), K(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX)));
  } else if (OB_FAIL(schema_guard->get_table_schema(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX), ivf_rowkey_cid_tbl))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(ivf_rowkey_cid_tbl)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), K(vc_info.aux_table_id_.count()), K(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX)));
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_cids))) {
    LOG_WARN("fail to get rowkey column ids in rowkey vid", K(ret), KPC(ivf_cid_vec_tbl));
  } else if (OB_FAIL(prepare_ivf_aux_tbl_cid_and_center_col_access_exprs(ivf_cid_vec_tbl, table_schema, expr_factory, table_item,
                                                                        cid_vec_cid_column, cid_vec_vec_column, true, false))) {
    LOG_WARN("fail to prepare ivf cid vec tbl access exprs", K(ret));
  } else if (OB_FAIL(prepare_ivf_rowkey_cid_tbl_access_exprs(ivf_rowkey_cid_tbl, table_schema, expr_factory, table_item,
                                                            rowkey_cid_cid_column))) {
    LOG_WARN("fail to prepare_ivf_rowkey_cid_tbl_access_exprs", K(ret));
  } else if (vc_info.is_ivf_sq_scan()) {
    if (OB_FAIL(schema_guard->get_table_schema(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_FOURTH_AUX_TBL_IDX), ivf_sq_meta_tbl))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(ivf_sq_meta_tbl)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null pointer", K(ret), K(vc_info.aux_table_id_.count()), K(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_FOURTH_AUX_TBL_IDX)));
    } else if (OB_FAIL(prepare_ivf_sq_meta_tbl_access_exprs(ivf_sq_meta_tbl, table_schema, expr_factory, table_item,
                                                           sq_meta_id_column, sq_meta_vec_column))) {
      LOG_WARN("fail to prepare ivf sq meta tbl access exprs", K(ret));
    }
  }


  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(cid_vec_cid_column) || OB_ISNULL(cid_vec_vec_column)
            || OB_ISNULL(rowkey_cid_cid_column)
            || (vc_info.is_ivf_sq_scan() && (OB_ISNULL(sq_meta_id_column) || OB_ISNULL(sq_meta_vec_column)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null vetor index generated column", K(ret),
        KP(cid_vec_cid_column), KP(cid_vec_vec_column), KP(rowkey_cid_cid_column),
        K(ivf_cid_vec_tbl->get_rowkey_column_num()), K(rowkey_cids.count()),
        K( ivf_rowkey_cid_tbl->get_rowkey_column_num()),
        K(vc_info.is_ivf_sq_scan()), KP(sq_meta_id_column), KP(sq_meta_vec_column));
  /* column must add in order, same as ObVectorIVFFlatColumnIdx and ObVectorIVFSQColumnIdx*/
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(cid_vec_cid_column))) {
    LOG_WARN("fail to push back aux column", K(ret));
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(cid_vec_vec_column))) {
    LOG_WARN("fail to push back aux column", K(ret));
  } else if (OB_FAIL(vc_info.aux_table_column_.push_back(rowkey_cid_cid_column))) {
    LOG_WARN("fail to push back aux column", K(ret));
  } else if (vc_info.is_ivf_sq_scan()) {
    if (OB_FAIL(vc_info.aux_table_column_.push_back(sq_meta_id_column))) {
      LOG_WARN("fail to push back aux column", K(ret));
    } else if (OB_FAIL(vc_info.aux_table_column_.push_back(sq_meta_vec_column))) {
      LOG_WARN("fail to push back aux column", K(ret));
    }
  }

  return ret;
}

int ObLogTableScan::prepare_rowkey_vid_dep_exprs()
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = nullptr;
  const ObTableSchema *table_schema = nullptr;
  const ObTableSchema *rowkey_vid_schema = nullptr;
  const ObTableSchema *rowkey_doc_schema = nullptr;
  ObArray<uint64_t> rowkey_cids;
  uint64_t rowkey_vid_tid = get_vector_index_info().get_aux_table_id(ObVectorAuxTableIdx::VEC_FOURTH_AUX_TBL_IDX);
  if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, schema guard or get_plan() is nullptr", K(ret), KP(get_plan()), KP(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(get_real_ref_table_id(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is nullptr", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(rowkey_vid_tid, rowkey_vid_schema))) {
    LOG_WARN("fail toprint_ranges get rowkey vid table schema", K(ret), K(rowkey_vid_tid));
  } else if (OB_ISNULL(rowkey_vid_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, rowkey vid schema is nullptr", K(ret), KPC(rowkey_vid_schema));
  } else if (OB_FAIL(rowkey_vid_schema->get_rowkey_column_ids(rowkey_cids))) {
    LOG_WARN("fail to get rowkey column ids in rowkey vid", K(ret), KPC(rowkey_vid_schema));
  } else {
    const ObColumnSchemaV2 *col_schema = nullptr;
    ObColumnRefRawExpr *column_expr = nullptr;
    uint64_t vec_vid_col_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cids.count(); ++i) {
      if (OB_ISNULL(col_schema = rowkey_vid_schema->get_column_schema(rowkey_cids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_column_expr(get_plan()->get_optimizer_context().get_expr_factory(),
              *col_schema, column_expr))) {
        LOG_WARN("failed to build rowkey doc id column expr", K(ret), K(i), KPC(col_schema));
      } else if (OB_FAIL(rowkey_id_exprs_.push_back(column_expr))) {
        LOG_WARN("fail to push back column expr", K(ret));
      }
    }
    if (FAILEDx(rowkey_vid_schema->get_vec_index_vid_col_id(vec_vid_col_id))) {
      LOG_WARN("fail to get vec index column ids", K(ret), KPC(rowkey_vid_schema));
    } else if (OB_ISNULL(col_schema = rowkey_vid_schema->get_column_schema(vec_vid_col_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null column schema ptr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(get_plan()->get_optimizer_context().get_expr_factory(),
            *col_schema, column_expr))) {
      LOG_WARN("failed to build rowkey vec vid column expr", K(ret), K(vec_vid_col_id), KPC(col_schema));
    } else if (OB_FAIL(rowkey_id_exprs_.push_back(column_expr))) {
      LOG_WARN("failed to build rowkey doc id column expr", K(ret), K(vec_vid_col_id), KPC(col_schema));
    }
  }
  return ret;
}

bool ObVecIndexInfo::is_vec_aux_table_id(uint64_t tid) const
{
  bool ret_bool = false;
  if (is_hnsw_vec_scan() || is_ivf_sq_scan() || is_ivf_pq_scan()) {
    ret_bool = tid == get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX)
            || tid == get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX)
            || tid == get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX)
            || tid == get_aux_table_id(ObVectorAuxTableIdx::VEC_FOURTH_AUX_TBL_IDX);
  } else if (is_ivf_flat_scan()) {
    ret_bool = tid == get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX) ||
               tid == get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX) ||
               tid == get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX);
  }
  return ret_bool;
}

int ObVecIndexInfo::check_vec_aux_column_is_all_inited(bool& is_all_inited) const
{
  int ret = OB_SUCCESS;
  is_all_inited = true;
  int aux_table_column_cnt = 0;
  if (is_hnsw_vec_scan()) {
    aux_table_column_cnt = ObVectorHNSWColumnIdx::HNSW_MAX_COL_CNT;
  } else if (is_ivf_flat_scan()) {
    aux_table_column_cnt = ObVectorIVFFlatColumnIdx::IVF_FLAT_ROWKEY_START;
  } else if (is_ivf_sq_scan()) {
    aux_table_column_cnt = ObVectorIVFSQColumnIdx::IVF_SQ_ROWKEY_START;
  } else if (is_ivf_pq_scan()) {
    aux_table_column_cnt = ObVectorIVFPQColumnIdx::IVF_PQ_ROWKEY_START;
  }

  if (aux_table_column_.count() < aux_table_column_cnt) {
    is_all_inited = false;
  } else {
    for (int i = 0; i < aux_table_column_cnt && is_all_inited == true && OB_SUCC(ret); ++i) {
      if (i >= aux_table_column_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, too many hnsw aux column", K(ret), K(i), K(aux_table_column_.count()));
      } else if (OB_ISNULL(aux_table_column_.at(i))) {
        is_all_inited = false;
      }
    }
  }
  return ret;
}

int ObVecIndexInfo::check_vec_aux_table_is_all_inited(bool& is_all_inited) const
{
  int ret = OB_SUCCESS;
  is_all_inited = true;
  int aux_table_cnt = 0;
  if (is_hnsw_vec_scan()) {
    aux_table_cnt = ObVectorAuxTableIdx:: VEC_MAX_AUX_TBL_IDX;
  } else if (is_ivf_flat_scan()) {
    aux_table_cnt = ObVectorAuxTableIdx::VEC_FOURTH_AUX_TBL_IDX;
  } else if (is_ivf_sq_scan()) {
    aux_table_cnt = ObVectorAuxTableIdx::VEC_MAX_AUX_TBL_IDX;
  } else if (is_ivf_pq_scan()) {
    aux_table_cnt = ObVectorAuxTableIdx::VEC_MAX_AUX_TBL_IDX;
  }

  if (aux_table_id_.count() < aux_table_cnt) {
    is_all_inited = false;
  } else {
    for (int i = 0; i < aux_table_cnt && is_all_inited == true && OB_SUCC(ret); ++i) {
      if (i >= aux_table_id_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, too many hnsw aux column", K(ret), K(i), K(aux_table_column_.count()));
      } else if (aux_table_id_[i] == OB_INVALID_ID) {
        is_all_inited = false;
      }
    }
  }
  return ret;
}

int ObLogTableScan::prepare_hnsw_index_id_tbl_access_exprs(const ObTableSchema *index_id_table,
                                                          const ObTableSchema *table_schema,
                                                          ObRawExprFactory *expr_factory,
                                                          TableItem *table_item,
                                                          ObColumnRefRawExpr *&index_id_vid_column,
                                                          ObColumnRefRawExpr *&index_id_scn_column,
                                                          ObColumnRefRawExpr *&index_id_type_column,
                                                          ObColumnRefRawExpr *&index_id_vector_column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_id_table) || OB_ISNULL(table_schema)
      || OB_ISNULL(expr_factory) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is null", K(ret), KP(index_id_table), KP(table_schema), KP(expr_factory));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_id_table->get_column_count(); ++i) {
      const ObColumnSchemaV2 *data_col_schema = nullptr;
      const ObColumnSchemaV2 *col_schema = index_id_table->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (OB_ISNULL(data_col_schema = table_schema->get_column_schema(col_schema->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (data_col_schema->is_vec_hnsw_vid_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, index_id_vid_column))) {
          LOG_WARN("failed to build vec vid column expr", K(ret));
        }
      } else if (data_col_schema->is_vec_hnsw_type_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, index_id_type_column))) {
          LOG_WARN("failed to build vec type column expr", K(ret));
        } else if (OB_NOT_NULL(index_id_type_column)) {
          index_id_type_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          index_id_type_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          index_id_type_column->set_database_name(table_item->database_name_);
        }
      } else if (data_col_schema->is_vec_hnsw_vector_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, index_id_vector_column))) {
          LOG_WARN("failed to build vec type column expr", K(ret));
        } else if (OB_NOT_NULL(index_id_vector_column)) {
          index_id_vector_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          index_id_vector_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          index_id_vector_column->set_database_name(table_item->database_name_);
        }
      } else if (data_col_schema->is_vec_hnsw_scn_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, index_id_scn_column))) {
          LOG_WARN("failed to build vec type column expr", K(ret));
        } else if (OB_NOT_NULL(index_id_scn_column)) {
          index_id_scn_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          index_id_scn_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          index_id_scn_column->set_database_name(table_item->database_name_);
        }
      }
    }// end for
  }
  return ret;
}

int ObLogTableScan::prepare_hnsw_delta_buf_tbl_access_exprs(const ObTableSchema *delta_buf_table,
                                                            const ObTableSchema *table_schema,
                                                            ObRawExprFactory *expr_factory,
                                                            TableItem *table_item,
                                                            ObColumnRefRawExpr *&delta_vid_column,
                                                            ObColumnRefRawExpr *&delta_type_column,
                                                            ObColumnRefRawExpr *&delta_vector_column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(delta_buf_table) || OB_ISNULL(table_schema)
      || OB_ISNULL(expr_factory) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is null", K(ret), KP(delta_buf_table), KP(table_schema), KP(expr_factory));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < delta_buf_table->get_column_count(); ++i) {
      const ObColumnSchemaV2 *data_col_schema = nullptr;
      const ObColumnSchemaV2 *col_schema = delta_buf_table->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (OB_ISNULL(data_col_schema = table_schema->get_column_schema(col_schema->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (data_col_schema->is_vec_hnsw_vid_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, delta_vid_column))) {
          LOG_WARN("failed to build vec vid column expr", K(ret));
        }
      } else if (data_col_schema->is_vec_hnsw_type_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, delta_type_column))) {
          LOG_WARN("failed to build vec type column expr", K(ret));
        } else if (OB_NOT_NULL(delta_type_column)) {
          delta_type_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          delta_type_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          delta_type_column->set_database_name(table_item->database_name_);
        }
      } else if (data_col_schema->is_vec_hnsw_vector_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, delta_vector_column))) {
          LOG_WARN("failed to build vec type column expr", K(ret));
        } else if (OB_NOT_NULL(delta_vector_column)) {
          delta_vector_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          delta_vector_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          delta_vector_column->set_database_name(table_item->database_name_);
        }
      }
    } // end for
  }
  return ret;
}

int ObLogTableScan::prepare_hnsw_snapshot_tbl_access_exprs(const ObTableSchema *snapshot_table,
                                                          const ObTableSchema *table_schema,
                                                          ObRawExprFactory *expr_factory,
                                                          TableItem *table_item,
                                                          ObColumnRefRawExpr *&snapshot_key_column,
                                                          ObColumnRefRawExpr *&snapshot_data_column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(snapshot_table) || OB_ISNULL(table_schema)
      || OB_ISNULL(expr_factory) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is null", K(ret), KP(snapshot_table), KP(table_schema), KP(expr_factory));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < snapshot_table->get_column_count(); ++i) {
      const ObColumnSchemaV2 *data_col_schema = nullptr;
      const ObColumnSchemaV2 *col_schema = snapshot_table->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (OB_ISNULL(data_col_schema = table_schema->get_column_schema(col_schema->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (data_col_schema->is_vec_hnsw_key_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, snapshot_key_column))) {
          LOG_WARN("failed to build vec vid column expr", K(ret));
        } else if (OB_NOT_NULL(snapshot_key_column)) {
          snapshot_key_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          snapshot_key_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          snapshot_key_column->set_database_name(table_item->database_name_);
        }
      } else if (data_col_schema->is_vec_hnsw_data_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *data_col_schema, snapshot_data_column))) {
          LOG_WARN("failed to build vec type column expr", K(ret));
        } else if (OB_NOT_NULL(snapshot_data_column)) {
          snapshot_data_column->set_ref_id(get_table_id(), data_col_schema->get_column_id());
          snapshot_data_column->set_column_attr(get_table_name(), data_col_schema->get_column_name_str());
          snapshot_data_column->set_database_name(table_item->database_name_);
        }
      }
    } // end for
  }
  return ret;
}

int ObLogTableScan::prepare_hnsw_vector_access_exprs()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  const ObTableSchema *delta_buf_table = nullptr;
  const ObTableSchema *index_id_table = nullptr;
  const ObTableSchema *snapshot_table = nullptr;
  ObVecIndexInfo &vc_info = get_vector_index_info();
  ObSqlSchemaGuard *schema_guard  = nullptr;
  TableItem *table_item = nullptr;
  ObRawExprFactory *expr_factory = nullptr;
  ObSQLSessionInfo *session_info = nullptr;
  ObColumnRefRawExpr *vec_vid_column = nullptr;
  ObColumnRefRawExpr *target_vec_column = nullptr;
  ObColumnRefRawExpr *delta_vid_column = nullptr;
  ObColumnRefRawExpr *delta_type_column = nullptr;
  ObColumnRefRawExpr *delta_vector_column = nullptr;
  ObColumnRefRawExpr *index_id_vid_column = nullptr;
  ObColumnRefRawExpr *index_id_scn_column = nullptr;
  ObColumnRefRawExpr *index_id_type_column = nullptr;
  ObColumnRefRawExpr *index_id_vector_column = nullptr;
  ObColumnRefRawExpr *snapshot_key_column = nullptr;
  ObColumnRefRawExpr *snapshot_data_column = nullptr;
  ObSEArray<uint64_t , 1> col_ids;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(get_real_ref_table_id(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX), delta_buf_table))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX), index_id_table))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX), snapshot_table))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(delta_buf_table) || OB_ISNULL(index_id_table) || OB_ISNULL(snapshot_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    const ObColumnSchemaV2 *vec_column_schema = nullptr;
    if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_id(*table_schema, *delta_buf_table, col_ids))) { // todo 考虑下要不要改这个函数
      LOG_WARN("failed to get vector index column.", K(ret));
    } else if (col_ids.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid vector col counts.", K(ret), K(col_ids.count()));
    } else if (OB_ISNULL(vec_column_schema = table_schema->get_column_schema(col_ids.at(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid vector col column.", K(ret), K(col_ids.at(0)));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *vec_column_schema, target_vec_column))) {
      LOG_WARN("failed to build target vector column expr", K(ret));
    } else if (OB_NOT_NULL(target_vec_column)) {
      target_vec_column->set_ref_id(get_table_id(), vec_column_schema->get_column_id());
      target_vec_column->set_column_attr(get_table_name(), vec_column_schema->get_column_name_str());
      target_vec_column->set_database_name(table_item->database_name_);
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count() && OB_ISNULL(vec_vid_column); ++i) {
      const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (col_schema->is_vec_hnsw_vid_column()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *col_schema, vec_vid_column))) {
          LOG_WARN("failed to build vec vid column expr", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prepare_hnsw_delta_buf_tbl_access_exprs(delta_buf_table, table_schema, expr_factory, table_item,
                      delta_vid_column, delta_type_column, delta_vector_column))) {
      LOG_WARN("failed to prepare hnsw delta buf table access exprs", K(ret));
    } else if (OB_FAIL(prepare_hnsw_index_id_tbl_access_exprs(index_id_table, table_schema, expr_factory, table_item,
                      index_id_vid_column, index_id_scn_column, index_id_type_column, index_id_vector_column))) {
      LOG_WARN("failed to prepare hnsw index id table access exprs", K(ret));
    } else if (OB_FAIL(prepare_hnsw_snapshot_tbl_access_exprs(snapshot_table, table_schema, expr_factory, table_item,
                      snapshot_key_column, snapshot_data_column))) {
      LOG_WARN("failed to prepare hnsw snapshot table access exprs", K(ret));
    } else if (OB_ISNULL(vec_vid_column) || OB_ISNULL(target_vec_column)
              || OB_ISNULL(delta_vid_column) || OB_ISNULL(delta_type_column)
              || OB_ISNULL(delta_vector_column) || OB_ISNULL(index_id_vid_column) || OB_ISNULL(index_id_type_column)
              || OB_ISNULL(index_id_scn_column) || OB_ISNULL(index_id_vector_column)
              || OB_ISNULL(snapshot_key_column) || OB_ISNULL(snapshot_data_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null vetor index generated column", K(ret),
          KP(vec_vid_column), KP(delta_vid_column),
          KP(delta_type_column), KP(delta_vector_column),
          KP(index_id_vid_column), KP(index_id_type_column),
          KP(index_id_scn_column), KP(index_id_vector_column),
          KP(snapshot_key_column), KP(snapshot_data_column));
    } else if (OB_FAIL(prepare_rowkey_vid_dep_exprs())) {
      LOG_WARN("fail to prepare rowkey vid dep exprs", K(ret));
    /* column must add in order, same as ObVectorHNSWColumnIdx*/
    } else if (OB_FAIL(vc_info.aux_table_column_.push_back(delta_vid_column))) {
      LOG_WARN("fail to push back delta vid column", K(ret));
    } else if (OB_FAIL(vc_info.aux_table_column_.push_back(delta_type_column))) {
      LOG_WARN("fail to push back delta type column", K(ret));
    } else if (OB_FAIL(vc_info.aux_table_column_.push_back(delta_vector_column))) {
      LOG_WARN("fail to push back delta vector column", K(ret));
    } else if (OB_FAIL(vc_info.aux_table_column_.push_back(index_id_vid_column))) {
      LOG_WARN("fail to push back aux column", K(ret));
    } else if (OB_FAIL(vc_info.aux_table_column_.push_back(index_id_type_column))) {
      LOG_WARN("fail to push back aux column", K(ret));
    } else if (OB_FAIL(vc_info.aux_table_column_.push_back(index_id_vector_column))) {
      LOG_WARN("fail to push back aux column", K(ret));
    } else if (OB_FAIL(vc_info.aux_table_column_.push_back(index_id_scn_column))) {
      LOG_WARN("fail to push back aux column", K(ret));
    } else if (OB_FAIL(vc_info.aux_table_column_.push_back(snapshot_key_column))) {
      LOG_WARN("fail to push back aux column", K(ret));
    } else if (OB_FAIL(vc_info.aux_table_column_.push_back(snapshot_data_column))) {
      LOG_WARN("fail to push back aux column", K(ret));
    } else {
      vc_info.target_vec_column_ = target_vec_column;
      vc_info.vec_id_column_ = vec_vid_column;
    }
  }
  return ret;
}

int ObLogTableScan::prepare_text_retrieval_dep_exprs(ObTextRetrievalInfo &tr_info)
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
      LOG_WARN("failed to copy related_doc_cnt expr", K(ret));
    } else if (OB_FAIL(copier.copy(total_doc_cnt->get_param_expr(0)))) {
      LOG_WARN("failed to copy total_doc_cnt expr", K(ret));
    } else if (OB_FAIL(copier.copy(doc_token_cnt->get_param_expr(0)))) {
      LOG_WARN("failed to copy doc_token_cnt expr", K(ret));
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

int ObLogTableScan::prepare_func_lookup_dep_exprs()
{
  int ret = OB_SUCCESS;

  ObSqlSchemaGuard *schema_guard = nullptr;
  const ObTableSchema *table_schema = nullptr;
  ObArray<uint64_t> rowkey_cids;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, schema guard or get_plan() is nullptr", K(ret), KP(get_plan()), KP(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(get_real_ref_table_id(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is nullptr", K(ret));
  } else {
    const ObTableSchema *rowkey_doc_schema = nullptr;
    if (OB_FAIL(schema_guard->get_table_schema(rowkey_doc_tid_, rowkey_doc_schema))) {
      LOG_WARN("fail toprint_ranges get rowkey doc table schema", K(ret), K(rowkey_doc_tid_));
    } else if (OB_ISNULL(rowkey_doc_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, rowkey doc schema is nullptr", K(ret), KPC(rowkey_doc_schema));
    } else if (OB_FAIL(rowkey_doc_schema->get_rowkey_column_ids(rowkey_cids))) {
      LOG_WARN("fail to get rowkey column ids in rowkey doc", K(ret), KPC(rowkey_doc_schema));
    } else {
      const ObColumnSchemaV2 *col_schema = nullptr;
      ObColumnRefRawExpr *column_expr = nullptr;
      uint64_t doc_id_col_id = OB_INVALID_ID;
      uint64_t ft_col_id = OB_INVALID_ID;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cids.count(); ++i) {
        if (OB_ISNULL(col_schema = rowkey_doc_schema->get_column_schema(rowkey_cids.at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_column_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                *col_schema, column_expr))) {
          LOG_WARN("failed to build rowkey doc id column expr", K(ret), K(i), KPC(col_schema));
        } else if (OB_FAIL(rowkey_id_exprs_.push_back(column_expr))) {
          LOG_WARN("fail to push back column expr", K(ret));
        }
      }
      if (FAILEDx(rowkey_doc_schema->get_fulltext_column_ids(doc_id_col_id, ft_col_id))) {
        LOG_WARN("fail to get fulltext column ids", K(ret), KPC(rowkey_doc_schema));
      } else if (OB_ISNULL(col_schema = rowkey_doc_schema->get_column_schema(doc_id_col_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_column_expr(get_plan()->get_optimizer_context().get_expr_factory(),
              *col_schema, column_expr))) {
        LOG_WARN("failed to build rowkey doc id column expr", K(ret), K(doc_id_col_id), KPC(col_schema));
      } else if (OB_FAIL(rowkey_id_exprs_.push_back(column_expr))) {
        LOG_WARN("fail to push back column expr", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < lookup_tr_infos_.count(); ++i) {
    if (OB_FAIL(prepare_text_retrieval_dep_exprs(lookup_tr_infos_.at(i)))) {
      LOG_WARN("failed to prepare text retrieval dependent exprs",
          K(ret), K(i), K(lookup_tr_infos_.at(i)));
    }
  }

  return ret;
}

int ObLogTableScan::prepare_index_merge_dep_exprs()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_tr_infos_.count(); ++i) {
    if (OB_FAIL(prepare_text_retrieval_dep_exprs(merge_tr_infos_.at(i)))) {
      LOG_WARN("failed to prepare text retrieval dependent exprs",
          K(ret), K(i), K(merge_tr_infos_.at(i)));
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

bool ObLogTableScan::use_index_merge() const
{
  bool bret = false;
  if (OB_NOT_NULL(access_path_)) {
    bret = access_path_->is_index_merge_path();
  }
  return bret;
}

int ObLogTableScan::check_match_union_merge_hint(const LogTableHint *table_hint,
                                                 bool &is_match) const
{
  int ret = OB_SUCCESS;
  const ObIndexMergeNode *root = NULL;
  is_match = false;
  if (OB_ISNULL(access_path_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null access path", K(ret), K(access_path_));
  } else if (NULL == table_hint || NULL == table_hint->union_merge_hint_
             || !access_path_->is_index_merge_path()) {
    // do nothing
  } else if (OB_ISNULL(root = static_cast<const IndexMergePath*>(access_path_)->root_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null index merge node", K(ret), KPC(access_path_));
  } else if (root->children_.count() != table_hint->union_merge_list_.count()) {
    // do nothing
  } else {
    is_match = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < root->children_.count(); ++i) {
      const ObIndexMergeNode *child = root->children_.at(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null index merge node", K(ret), K(i), KPC(root));
      } else if (!child->is_scan_node()) {
        is_match = false;
      } else if (OB_ISNULL(child->ap_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null access path", K(ret), KPC(child));
      } else if (child->ap_->index_id_ != table_hint->union_merge_list_.at(i)) {
        is_match = false;
      }
    }
  }

  return ret;
}


int ObLogTableScan::get_index_range_conds(int64_t idx, ObIArray<ObRawExpr *> &index_range_conds) const
{
  int ret = OB_SUCCESS;
  index_range_conds.reuse();
  if (OB_UNLIKELY(idx < 0 || idx >= index_range_conds_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx of index range conds", K(idx), K(index_range_conds_.count()));
  } else if (OB_FAIL(index_range_conds.assign(index_range_conds_.at(idx)))) {
    LOG_WARN("failed to assign index range conds", K(ret));
  }
  return ret;
}

int ObLogTableScan::get_index_filters(int64_t idx, ObIArray<ObRawExpr *> &index_filters) const
{
  int ret = OB_SUCCESS;
  index_filters.reuse();
  if (OB_UNLIKELY(idx < 0 || idx >= index_filters_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx of index filters", K(idx), K(index_filters_.count()));
  } else if (OB_FAIL(index_filters.assign(index_filters_.at(idx)))) {
    LOG_WARN("failed to assign index filters", K(ret));
  }
  return ret;
}

int ObLogTableScan::get_index_tids(ObIArray<ObTableID> &index_tids) const
{
  int ret = OB_SUCCESS;
  const ObIndexMergeNode* root_node = NULL;
  index_tids.reuse();
  if (OB_ISNULL(access_path_) || OB_UNLIKELY(!access_path_->is_index_merge_path())
      || OB_ISNULL(root_node = static_cast<IndexMergePath*>(access_path_)->root_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null index merge path", K(ret), KPC(access_path_));
  } else if (OB_FAIL(root_node->get_all_index_ids(index_tids))) {
    LOG_WARN("failed to get all index ids", K(ret));
  }
  return ret;
}

int ObLogTableScan::get_index_name_list(ObIArray<ObString> &index_name_list) const
{
  int ret = OB_SUCCESS;
  const ObIndexMergeNode *root_node = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *index_schema = NULL;
  index_name_list.reuse();
  if (OB_ISNULL(access_path_) || OB_UNLIKELY(!access_path_->is_index_merge_path())
      || OB_ISNULL(root_node = static_cast<const IndexMergePath*>(access_path_)->root_)
      || OB_ISNULL(get_plan()) || OB_ISNULL(get_stmt())
      || OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected NULL",K(ret), KPC(access_path_), KPC(root_node), K(get_plan()), K(get_stmt()), K(schema_guard));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < root_node->children_.count(); ++i) {
    const ObIndexMergeNode *child_node = root_node->children_.at(i);
    ObString index_name;
    if (OB_ISNULL(child_node) || OB_ISNULL(child_node->ap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected NULL",K(ret), KPC(child_node), K(i));
    } else if (ref_table_id_ == child_node->ap_->index_id_) {
      index_name = ObIndexHint::PRIMARY_KEY;
    } else if (OB_FAIL(schema_guard->get_table_schema(table_id_,
                                                      child_node->ap_->index_id_,
                                                      get_stmt(),
                                                      index_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(child_node->ap_->index_id_), K(i));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null schema",K(ret), K(child_node->ap_->index_id_), K(i));
    } else if (OB_FAIL(index_schema->get_index_name(index_name))) {
      LOG_WARN("failed to get index name", K(ret), K(child_node->ap_->index_id_), K(i));
    }
    if (OB_SUCC(ret) && OB_FAIL(index_name_list.push_back(index_name))) {
      LOG_WARN("failed to push back index name", K(ret));
    }
  }
  return ret;
}

int ObLogTableScan::check_das_need_scan_with_domain_id()
{
  int ret = OB_SUCCESS;
  const ObLogPlan *plan = nullptr;
  const ObDMLStmt *stmt = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  const ObTableSchema *table_schema = nullptr;
  with_domain_types_.reset();
  domain_table_ids_.reset();
  ObSEArray<uint64_t, 4> vec_id_cols; // only for get ivfflat index table id
  ObOptimizerContext *opt_ctx = nullptr;

  if (OB_ISNULL(plan = get_plan()) || OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect error, plan or stmt is nullptr", K(ret), KP(plan), KP(stmt));
  } else if (!(stmt->is_delete_stmt() || stmt->is_update_stmt() || stmt->is_select_stmt())) {
    // just skip, nothing to do
  } else if (get_contains_fake_cte() || is_virtual_table(get_ref_table_id())) {
    // just skip, nothing to do;
  } else if (OB_ISNULL(schema_guard = plan->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, schema guard or get_plan() is nullptr", K(ret), KP(plan), KP(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id_, ref_table_id_, get_stmt(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is nullptr", K(ret), K(get_real_ref_table_id()), K(table_id_), K(ref_table_id_));
  } else if (ObDomainIdUtils::is_domain_id_index_table(table_schema)) {
    // just skip, nothing to do.
    LOG_TRACE("skip full-text index or multi-value index or vector index", K(ret), KPC(table_schema));
  } else if (plan->get_optimizer_context().is_insert_stmt_in_online_ddl()) {
    const TableItem *insert_table_item = plan->get_optimizer_context().get_root_stmt()->get_table_item(0);
    if (OB_ISNULL(insert_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect error, insert table item is nullptr", K(ret), K(plan->get_optimizer_context().get_root_stmt()->get_table_items()));
    } else {
      const uint64_t ddl_table_id = insert_table_item->ddl_table_id_;
      const schema::ObTableSchema *ddl_table_schema = nullptr;
      if (OB_FAIL(schema_guard->get_table_schema(ddl_table_id, ddl_table_schema))) {
        LOG_WARN("fail to get ddl table id", K(ret), K(ddl_table_id));
      } else if (OB_ISNULL(ddl_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, ddl table schema is nullptr", K(ret), KP(ddl_table_schema));
      } else {
        // multivalue and doc index use same domain type,
        // so we need to save extra multivalue_col_idx_ and multivalue_type_
        if (ddl_table_schema->is_multivalue_index_aux()) {
          if (OB_FAIL(ddl_table_schema->get_multivalue_column_id(multivalue_col_idx_))) {
            LOG_WARN("failed to get col idx", K(ret), K(ddl_table_id));
          } else {
            multivalue_type_ = static_cast<int32_t>(ddl_table_schema->get_index_type());
          }
        }
        bool res = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < ObDomainIdUtils::ObDomainIDType::MAX && !res; i++) {
          ObDomainIdUtils::ObDomainIDType cur_type = static_cast<ObDomainIdUtils::ObDomainIDType>(i);
          if (OB_FAIL(ObDomainIdUtils::check_table_need_domain_id_merge(cur_type, ddl_table_schema, res))) {
            LOG_WARN("fail to check table need domain id merge", K(ret), K(cur_type), KPC(ddl_table_schema));
          } else if (res) {
            if (OB_FAIL(with_domain_types_.push_back(i))) {
              LOG_WARN("fail to push back domain types", K(ret), K(i));
            } else if (ddl_table_schema->is_vec_ivfflat_cid_vector_index()) {
              // TODO(@liyao): 使用merge_iter补cid_vector
              // uint64_t vec_cid_col_id = OB_INVALID_ID;
              // for (int64_t i = 0; OB_SUCC(ret) && i < ddl_table_schema->get_column_count() && OB_INVALID_ID == vec_cid_col_id; ++i) {
              //   const ObColumnSchemaV2 *col_schema = nullptr;
              //   if (OB_ISNULL(col_schema = ddl_table_schema->get_column_schema_by_idx(i))) {
              //     ret = OB_ERR_UNEXPECTED;
              //     LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(i), KPC(ddl_table_schema));
              //   } else if (col_schema->is_vec_ivf_center_id_column()) {
              //     vec_cid_col_id = col_schema->get_column_id();
              //   }
              // }
              // if (OB_SUCC(ret)) {
              //   if (OB_INVALID_ID == vec_cid_col_id) {
              //     ret = OB_ERR_UNEXPECTED;
              //     LOG_WARN("invalid cid col in centriod table", K(ret));
              //   } else if (OB_FAIL(vec_id_cols.push_back(vec_cid_col_id))) {
              //     LOG_WARN("failed to push back array", K(ret));
              //   }
              // }
            } else if (OB_FAIL(vec_id_cols.push_back(OB_INVALID_ID))) { // place holder for doc id/vid
              LOG_WARN("failed to push back col id", K(ret));
            }
          }
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); i++) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(col_item), K(ret));
      } else if (col_item->table_id_ != table_id_ || !col_item->expr_->is_explicited_reference()) {
        // do nothing
      } else {
        bool res = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < ObDomainIdUtils::ObDomainIDType::MAX && !res; j++) {
          ObDomainIdUtils::ObDomainIDType cur_type = static_cast<ObDomainIdUtils::ObDomainIDType>(j);
          ObIndexType index_type = ObIndexType::INDEX_TYPE_MAX;
          if (col_item->expr_->is_vec_cid_column()) {
            if (col_item->expr_->is_vec_pq_cids_column() || col_item->expr_->is_vec_cid_column()) {
              if (OB_FAIL(ObVectorIndexUtil::get_vector_index_type(
                  schema_guard->get_schema_guard(), *table_schema, col_item->expr_->get_column_id(), index_type))) {
                LOG_WARN("fail to get vector index type", K(ret), KPC(col_item->expr_));
              }
            }
          }
          if (OB_FAIL(ObDomainIdUtils::check_column_need_domain_id_merge(cur_type, col_item->expr_, index_type, res))) {
            LOG_WARN("fail to check column need domain id merge", K(ret), K(cur_type), KPC(col_item));
          } else if (res) {
            if (OB_FAIL(with_domain_types_.push_back(j))) {
              LOG_WARN("fail to push back domain types", K(ret), K(j));
            } else if (OB_FAIL(vec_id_cols.push_back(col_item->expr_->get_column_id()))) {
              LOG_WARN("failed to push back col id", K(ret), K(col_item->expr_->get_column_id()));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < with_domain_types_.size(); i++) {
      uint64_t domain_table_id = common::OB_INVALID_ID;
      ObDomainIdUtils::ObDomainIDType cur_type = static_cast<ObDomainIdUtils::ObDomainIDType>(with_domain_types_[i]);
      if (OB_FAIL(ObDomainIdUtils::get_domain_tid_table_by_cid(cur_type, schema_guard, table_schema, vec_id_cols.at(i), domain_table_id))) {
        LOG_WARN("failed to get domain_tid", K(ret));
      } else if (OB_FAIL(domain_table_ids_.push_back(domain_table_id))) {
        LOG_WARN("fail to push back domain table id", K(ret), K(cur_type), K(domain_table_id));
      } else if (cur_type == ObDomainIdUtils::ObDomainIDType::DOC_ID) { // for function lookup
        set_rowkey_doc_table_id(domain_table_id);
      }
    }
  }
  LOG_TRACE("check_table_scan_with_domain_id", K(ret), K(with_domain_types_), K(domain_table_ids_), KPC(table_schema));
  return ret;
}

int ObLogTableScan::prepare_rowkey_domain_id_dep_exprs()
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = nullptr;
  const ObTableSchema *table_schema = nullptr;
  ObArray<uint64_t> rowkey_cids;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, schema guard or get_plan() is nullptr", K(ret), KP(get_plan()), KP(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(get_real_ref_table_id(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, table schema is nullptr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < with_domain_types_.size(); i++) {
      const ObTableSchema *rowkey_domain_id_schema = nullptr;
      ObDomainIdUtils::ObDomainIDType cur_type = static_cast<ObDomainIdUtils::ObDomainIDType>(with_domain_types_[i]);
      uint64_t domain_id_tid = domain_table_ids_[i];
      rowkey_cids.reset();
      if (OB_FAIL(schema_guard->get_table_schema(domain_id_tid, rowkey_domain_id_schema))) {
        LOG_WARN("fail toprint_ranges get rowkey domain id table schema", K(ret), K(domain_id_tid));
      } else if (OB_ISNULL(rowkey_domain_id_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, rowkey domain id schema is nullptr", K(ret), KPC(rowkey_domain_id_schema));
      } else if (OB_FAIL(rowkey_domain_id_schema->get_rowkey_column_ids(rowkey_cids))) {
        LOG_WARN("fail to get rowkey column ids in rowkey domain id", K(ret), KPC(rowkey_domain_id_schema));
      } else if (OB_FAIL(ObDomainIdUtils::get_domain_id_cols(cur_type, rowkey_domain_id_schema, rowkey_cids, schema_guard))) {
        LOG_WARN("fail to get domain id cols in table", K(ret));
      } else {
        const ObColumnSchemaV2 *col_schema = nullptr;
        const ObColumnSchemaV2 *data_col_schema = nullptr;
        ObColumnRefRawExpr *column_expr = nullptr;
        bool is_pq_index = rowkey_domain_id_schema->get_index_type() == INDEX_TYPE_VEC_IVFPQ_ROWKEY_CID_LOCAL;
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cids.count(); ++i) {
          if (OB_ISNULL(col_schema = rowkey_domain_id_schema->get_column_schema(rowkey_cids.at(i)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null column schema ptr", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::build_column_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                  *col_schema, column_expr))) {
            LOG_WARN("failed to build rowkey doc id column expr", K(ret), K(i), KPC(col_schema));
          } else if (is_pq_index) {
            if (OB_ISNULL(data_col_schema = table_schema->get_column_schema(rowkey_cids.at(i)))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null column schema ptr", K(ret));
            } else if (data_col_schema->is_vec_ivf_pq_center_ids_column()) {
              column_expr->set_vec_pq_cids_column();
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(rowkey_id_exprs_.push_back(column_expr))) {
              LOG_WARN("fail to push back column expr", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogTableScan::copy_gen_col_range_exprs()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> columns;
  bool need_copy = false;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!need_replace_gen_column()) {
    //no need replace in index table non-return table scenario.
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < range_conds_.count(); ++i) {
      columns.reuse();
      if (OB_ISNULL(range_conds_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(range_conds_.at(i),
                                                              columns, true))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else {
        need_copy = false;
        for (int64_t j = 0; OB_SUCC(ret) && !need_copy && j < columns.count(); ++j) {
          ObRawExpr *expr = columns.at(j);
          if (OB_ISNULL(expr) ||
              OB_UNLIKELY(!expr->is_column_ref_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (!static_cast<ObColumnRefRawExpr*>(expr)->is_generalized_column()) {
            // do nothing
          } else {
            need_copy = true;
          }
        }
        if (OB_SUCC(ret) && need_copy) {
          ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
          ObRawExpr *old_expr = range_conds_.at(i);
          if (OB_FAIL(copier.add_skipped_expr(columns))) {
            LOG_WARN("failed to add skipper expr", K(ret));
          } else if (OB_FAIL(copier.copy(old_expr, range_conds_.at(i)))) {
            LOG_WARN("failed to copy expr node", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

uint64_t ObLogTableScan::get_rowkey_domain_id_tid(int64_t domain_type) const
{
  uint64_t table_id = OB_INVALID_ID;
  for (int i = 0; i < with_domain_types_.size(); i++) {
    if (with_domain_types_[i] == domain_type) {
      table_id = domain_table_ids_[i];
    }
  }
  return table_id;
}

bool ObLogTableScan::is_scan_domain_id_table(uint64 table_id) const
{
  bool bret = false;
  for (int i = 0; i < domain_table_ids_.size() && !bret; i++) {
    if (domain_table_ids_[i] == table_id) {
      bret = true;
    }
  }
  return bret;
}

int ObLogTableScan::try_adjust_scan_direction(const ObIArray<OrderItem> &sort_keys)
{
  int ret = OB_SUCCESS;
  bool order_used = false;
  const AccessPath *path = NULL;
  if (OB_ISNULL(path = get_access_path())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(path));
  } else if (sort_keys.empty() || path->ordering_.empty() ||
             path->force_direction_ || use_batch()) {
    // do nothing
  } else if (OB_FAIL(check_op_orderding_used_by_parent(order_used))) {
    LOG_WARN("failed to check op ordering", K(ret));
  } else if (!order_used) {
    const OrderItem &first_sortkey = sort_keys.at(0);
    bool found = false;
    bool need_reverse = false;
    for (int64_t i = 0; !found && i < path->ordering_.count(); i ++) {
      const OrderItem &path_order = path->ordering_.at(i);
      if (path_order.expr_ == first_sortkey.expr_) {
        found = true;
      }
    }
    if (OB_SUCC(ret) && found) {
      set_scan_direction(first_sortkey.order_type_);
    }
  }
  return ret;
}
